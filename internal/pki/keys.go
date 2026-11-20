// Package pki 实现内嵌的证书颁发与生命周期管理。
//
// 几条贯穿本包的决策：
//
//   - 默认 ECDSA P-256。TLS 握手是 CPU 墙，RSA-2048 的服务端开销约为
//     P-256 的十倍，默认用 RSA 会让接入能力下降一个量级（PRD NET-011）。
//   - 设备私钥只在签发响应中出现一次，不留存（PKI-010）。
//   - CA 私钥经 KeyStore 接口访问，永不进入复制日志、WAL 或快照
//     （CLU-028）。签发是特权角色，不是所有节点都有的能力。
//   - 签发策略约束 CSR。CSR 可以请求任意主题与扩展，CA 盲从等于让
//     申请者自己决定自己的权限（PKI-012）。
package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

// KeyAlgorithm 是密钥算法。
type KeyAlgorithm uint8

const (
	// ECDSAP256 是默认算法。
	//
	// 选它而非 RSA 是因为握手开销：服务端侧 P-256 的签名验证远快于
	// RSA-2048，而 broker 要在接入风暴中承受每秒数千次握手。
	ECDSAP256 KeyAlgorithm = iota
	ECDSAP384
	// RSA2048 仅为兼容只支持 RSA 的老设备保留。
	RSA2048
	RSA4096
	// Ed25519 在握手开销上更优，但许多嵌入式 TLS 栈尚不支持，
	// 因此不作为默认。
	Ed25519
)

var algNames = [...]string{"ECDSA-P256", "ECDSA-P384", "RSA-2048", "RSA-4096", "Ed25519"}

func (a KeyAlgorithm) String() string {
	if int(a) >= len(algNames) {
		return "unknown"
	}
	return algNames[a]
}

// ParseAlgorithm 解析算法名。
func ParseAlgorithm(s string) (KeyAlgorithm, bool) {
	for i, n := range algNames {
		if s == n {
			return KeyAlgorithm(i), true
		}
	}
	return 0, false
}

// ErrUnsupportedAlgorithm 表示算法不被支持。
var ErrUnsupportedAlgorithm = errors.New("pki: 不支持的密钥算法")

// GenerateKey 生成一对密钥。
func GenerateKey(alg KeyAlgorithm) (crypto.Signer, error) {
	switch alg {
	case ECDSAP256:
		return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case ECDSAP384:
		return ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case RSA2048:
		return rsa.GenerateKey(rand.Reader, 2048)
	case RSA4096:
		return rsa.GenerateKey(rand.Reader, 4096)
	case Ed25519:
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		return priv, err
	default:
		return nil, ErrUnsupportedAlgorithm
	}
}

// AlgorithmOf 判定一个公钥的算法。
func AlgorithmOf(pub crypto.PublicKey) (KeyAlgorithm, bool) {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		switch k.Curve {
		case elliptic.P256():
			return ECDSAP256, true
		case elliptic.P384():
			return ECDSAP384, true
		}
	case *rsa.PublicKey:
		switch k.N.BitLen() {
		case 2048:
			return RSA2048, true
		case 4096:
			return RSA4096, true
		}
	case ed25519.PublicKey:
		return Ed25519, true
	}
	return 0, false
}

// PEM 块类型。
const (
	pemTypeCertificate = "CERTIFICATE"
	pemTypePrivateKey  = "PRIVATE KEY"
	pemTypeECKey       = "EC PRIVATE KEY"
	pemTypeRSAKey      = "RSA PRIVATE KEY"
	pemTypeCSR         = "CERTIFICATE REQUEST"
)

// EncodeCertPEM 把证书 DER 编码为 PEM。
func EncodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: der})
}

// EncodePrivateKeyPEM 把私钥编码为 PEM。
//
// ECDSA 用 SEC1（EC PRIVATE KEY），RSA 用 PKCS#1（RSA PRIVATE KEY），
// 其余（Ed25519 没有传统格式）用 PKCS#8。
//
// 这里原先一律用 PKCS#8，理由记的是"设备 SDK 对 PKCS#8 的支持最普遍"。
// 真机验证推翻了它：AWS IoT Device SDK（awscrt）在 Apple 平台上读不了
// PKCS#8 的 EC 私钥，报 AWS_IO_FILE_VALIDATION_FAILURE——一条完全指不到
// "密钥编码"的错误，而同一把钥匙转成 SEC1 立刻就能连上。AWS IoT Core
// 自己签发的私钥也是传统格式（CreateKeysAndCertificate 返回
// RSA PRIVATE KEY），所以照着设备 SDK 实际吃得下的格式发，
// 既是兼容性要求，也是对用户最省事的默认。
//
// 读取侧不受影响：ParsePrivateKeyPEM 一直同时接受这三种格式，
// 已经发出去的 PKCS#8 密钥仍然能用。
func EncodePrivateKeyPEM(key crypto.Signer) ([]byte, error) {
	switch k := key.(type) {
	case *ecdsa.PrivateKey:
		der, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, fmt.Errorf("pki: 编码私钥失败: %w", err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: pemTypeECKey, Bytes: der}), nil
	case *rsa.PrivateKey:
		der := x509.MarshalPKCS1PrivateKey(k)
		return pem.EncodeToMemory(&pem.Block{Type: pemTypeRSAKey, Bytes: der}), nil
	default:
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("pki: 编码私钥失败: %w", err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: der}), nil
	}
}

// EncodeCSRPEM 把证书请求 DER 编码为 PEM。
func EncodeCSRPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCSR, Bytes: der})
}

// ParseCertPEM 解析一张 PEM 证书。
func ParseCertPEM(data []byte) (*x509.Certificate, error) {
	blk, err := singleBlock(data, pemTypeCertificate)
	if err != nil {
		return nil, err
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析证书失败: %w", err)
	}
	return c, nil
}

// ParseCertChainPEM 解析一条 PEM 证书链，按出现顺序返回。
func ParseCertChainPEM(data []byte) ([]*x509.Certificate, error) {
	var out []*x509.Certificate
	rest := data
	for {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			break
		}
		if blk.Type != pemTypeCertificate {
			continue
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, fmt.Errorf("pki: 解析证书链失败: %w", err)
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("pki: PEM 中没有证书")
	}
	return out, nil
}

// ParsePrivateKeyPEM 解析一个 PEM 私钥。
//
// 同时接受 PKCS#8、SEC1（EC PRIVATE KEY）与 PKCS#1（RSA PRIVATE KEY）：
// 用户手上的既有密钥文件格式不一，拒绝解析只会把兼容负担推给用户。
func ParsePrivateKeyPEM(data []byte) (crypto.Signer, error) {
	blk, rest := pem.Decode(data)
	for blk != nil {
		if signer, err := parseKeyBlock(blk); err == nil {
			return signer, nil
		}
		blk, rest = pem.Decode(rest)
	}
	return nil, errors.New("pki: PEM 中没有可识别的私钥")
}

func parseKeyBlock(blk *pem.Block) (crypto.Signer, error) {
	if k, err := x509.ParsePKCS8PrivateKey(blk.Bytes); err == nil {
		return asSigner(k)
	}
	if k, err := x509.ParseECPrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS1PrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("pki: 无法识别的私钥格式")
}

func asSigner(k any) (crypto.Signer, error) {
	s, ok := k.(crypto.Signer)
	if !ok {
		return nil, ErrUnsupportedAlgorithm
	}
	return s, nil
}

// ParseCSRPEM 解析一个 PEM 证书请求并校验其自签名。
//
// 校验签名是必须的：CSR 的签名证明申请者持有对应私钥，
// 不验等于允许任何人用别人的公钥申请证书。
func ParseCSRPEM(data []byte) (*x509.CertificateRequest, error) {
	blk, err := singleBlock(data, pemTypeCSR)
	if err != nil {
		return nil, err
	}
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析证书请求失败: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("pki: 证书请求的签名无效（申请者未证明持有私钥）: %w", err)
	}
	return csr, nil
}

func singleBlock(data []byte, want string) (*pem.Block, error) {
	blk, _ := pem.Decode(data)
	if blk == nil {
		return nil, fmt.Errorf("pki: 不是合法的 PEM 数据")
	}
	if blk.Type != want {
		return nil, fmt.Errorf("pki: PEM 块类型为 %q，期望 %q", blk.Type, want)
	}
	return blk, nil
}
