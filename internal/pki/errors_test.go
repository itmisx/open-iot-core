package pki

import (
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 这个文件专测错误路径。
//
// 对 CA 而言，错误路径比正常路径更要紧：一个在正常输入下工作、在畸形输入
// 下静默接受的解析器，等于没有校验。所有"应当拒绝"的分支都必须被执行过
// 一次，否则无从知道它究竟拒绝了什么。

// fakeSigner 是一个不被 PKCS#8 支持的签名器，用于触发编码失败分支。
type fakeSigner struct{}

func (fakeSigner) Public() crypto.PublicKey { return struct{}{} }
func (fakeSigner) Sign(io.Reader, []byte, crypto.SignerOpts) ([]byte, error) {
	return nil, errors.New("不支持")
}

func TestAlgorithmErrors(t *testing.T) {
	if _, err := GenerateKey(KeyAlgorithm(99)); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Errorf("未知算法应返回 ErrUnsupportedAlgorithm，得到 %v", err)
	}
	if got := KeyAlgorithm(99).String(); got != "unknown" {
		t.Errorf("越界算法名 = %q，期望 unknown", got)
	}
	if _, ok := ParseAlgorithm("ECDSA-P521"); ok {
		t.Error("不支持的算法名不应被解析")
	}
	if a, ok := ParseAlgorithm("Ed25519"); !ok || a != Ed25519 {
		t.Errorf("Ed25519 应被解析，得到 %v/%v", a, ok)
	}
	if _, ok := AlgorithmOf(struct{}{}); ok {
		t.Error("非公钥类型不应被识别为算法")
	}
}

func TestEncodePrivateKeyPEMRejectsUnsupported(t *testing.T) {
	if _, err := EncodePrivateKeyPEM(fakeSigner{}); err == nil {
		t.Error("无法用 PKCS#8 表达的私钥应编码失败")
	}
}

func TestParseCertPEMErrors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"空输入", nil},
		{"非 PEM", []byte("这不是 PEM")},
		{"块类型不符", pem.EncodeToMemory(&pem.Block{
			Type: "PRIVATE KEY", Bytes: []byte{1, 2, 3}})},
		{"DER 非法", pem.EncodeToMemory(&pem.Block{
			Type: pemTypeCertificate, Bytes: []byte{1, 2, 3}})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := ParseCertPEM(c.in); err == nil {
				t.Error("应解析失败")
			}
		})
	}
}

func TestParseCertChainPEMErrors(t *testing.T) {
	if _, err := ParseCertChainPEM(nil); err == nil {
		t.Error("空输入应报错")
	}
	// 只含非证书块：等同于没有证书
	onlyKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte{1}})
	if _, err := ParseCertChainPEM(onlyKey); err == nil {
		t.Error("不含证书的 PEM 应报错")
	}
	// 含一个畸形证书块：必须整条链失败，不能只跳过坏的那张。
	// 悄悄跳过会让调用方以为拿到了完整链。
	ca, _, _ := newTestCA(t, DefaultPolicy())
	bad := append([]byte(nil), ca.ChainPEM()...)
	bad = append(bad, pem.EncodeToMemory(&pem.Block{
		Type: pemTypeCertificate, Bytes: []byte{9, 9, 9}})...)
	if _, err := ParseCertChainPEM(bad); err == nil {
		t.Error("链中含畸形证书时应整体失败")
	}
	// 正常链应解析出两张
	chain, err := ParseCertChainPEM(ca.ChainPEM())
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 2 {
		t.Errorf("链长度 = %d，期望 2", len(chain))
	}
}

func TestParsePrivateKeyPEMErrors(t *testing.T) {
	if _, err := ParsePrivateKeyPEM(nil); err == nil {
		t.Error("空输入应报错")
	}
	if _, err := ParsePrivateKeyPEM([]byte("垃圾数据")); err == nil {
		t.Error("非 PEM 应报错")
	}
	garbage := pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: []byte{1, 2, 3}})
	if _, err := ParsePrivateKeyPEM(garbage); err == nil {
		t.Error("畸形私钥应报错")
	}
	// 多块输入：前面是证书，后面才是私钥，应当跳过前者找到后者
	key, _ := GenerateKey(ECDSAP256)
	keyPEM, _ := EncodePrivateKeyPEM(key)
	ca, _, _ := newTestCA(t, DefaultPolicy())
	mixed := append(append([]byte(nil), ca.ChainPEM()...), keyPEM...)
	if _, err := ParsePrivateKeyPEM(mixed); err != nil {
		t.Errorf("应跳过非私钥块找到私钥: %v", err)
	}
}

// TestParsePrivateKeyPEMAcceptsLegacyFormats 覆盖 SEC1 与 PKCS#1 两条兼容路径。
//
// 用户手上的既有 CA 密钥常是这两种格式，拒绝解析只会把兼容负担推给用户。
func TestParsePrivateKeyPEMAcceptsLegacyFormats(t *testing.T) {
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sec1, err := x509.MarshalECPrivateKey(ec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePrivateKeyPEM(pem.EncodeToMemory(&pem.Block{
		Type: "EC PRIVATE KEY", Bytes: sec1}))
	if err != nil {
		t.Fatalf("SEC1 格式应被接受: %v", err)
	}
	if !ec.PublicKey.Equal(got.Public()) {
		t.Error("SEC1 解析出的私钥与原始不符")
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pkcs1 := x509.MarshalPKCS1PrivateKey(rsaKey)
	got, err = ParsePrivateKeyPEM(pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: pkcs1}))
	if err != nil {
		t.Fatalf("PKCS#1 格式应被接受: %v", err)
	}
	if !rsaKey.PublicKey.Equal(got.Public()) {
		t.Error("PKCS#1 解析出的私钥与原始不符")
	}
}

func TestParseCSRPEMErrors(t *testing.T) {
	if _, err := ParseCSRPEM(nil); err == nil {
		t.Error("空输入应报错")
	}
	if _, err := ParseCSRPEM(pem.EncodeToMemory(&pem.Block{
		Type: pemTypeCertificate, Bytes: []byte{1}})); err == nil {
		t.Error("块类型不符应报错")
	}
	if _, err := ParseCSRPEM(pem.EncodeToMemory(&pem.Block{
		Type: pemTypeCSR, Bytes: []byte{1, 2, 3}})); err == nil {
		t.Error("畸形 CSR 应报错")
	}
}

func TestNewCAValidation(t *testing.T) {
	root, err := CreateRootCA(CARequest{CommonName: "V Root", MaxPathLen: 1}, testNowMs)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, _ := GenerateKey(ECDSAP256)

	// 构造一张 IsCA 但缺少 certSign 用途的证书
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(42),
		Subject:               pkix.Name{CommonName: "No CertSign"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, root.Cert, leafKey.Public(), root.Key)
	if err != nil {
		t.Fatal(err)
	}
	noCertSign, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	// 一张普通终端实体证书（IsCA 为假）
	ca, _, _ := newTestCA(t, DefaultPolicy())
	leaf, err := ca.IssueKeyPair(IssueRequest{CommonName: "leaf"})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		issuer *Bundle
		now    func() int64
	}{
		{"缺少签发者", nil, testClock()},
		{"缺少证书", &Bundle{Key: root.Key}, testClock()},
		{"缺少私钥", &Bundle{Cert: root.Cert}, testClock()},
		{"非 CA 证书", &Bundle{Cert: leaf.Cert, Key: leafKey}, testClock()},
		{"缺少 certSign 用途", &Bundle{Cert: noCertSign, Key: leafKey}, testClock()},
		{"缺少时钟", &Bundle{Cert: root.Cert, Key: root.Key}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.issuer, nil, DefaultPolicy(), c.now); err == nil {
				t.Error("应报错")
			}
		})
	}
}

func TestCreateCARejectsBadInput(t *testing.T) {
	if _, err := CreateRootCA(CARequest{}, testNowMs); !errors.Is(err, ErrCommonNameRequired) {
		t.Errorf("缺少通用名应返回 ErrCommonNameRequired，得到 %v", err)
	}
	if _, err := CreateRootCA(CARequest{
		CommonName: "X", Algorithm: KeyAlgorithm(99),
	}, testNowMs); err == nil {
		t.Error("不支持的算法应报错")
	}
	root, err := CreateRootCA(CARequest{CommonName: "C Root", MaxPathLen: 1}, testNowMs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateIssuingCA(nil, CARequest{CommonName: "X"}, testNowMs); err == nil {
		t.Error("缺少根 CA 应报错")
	}
	if _, err := CreateIssuingCA(&Bundle{Cert: root.Cert}, CARequest{
		CommonName: "X",
	}, testNowMs); err == nil {
		t.Error("根 CA 缺少私钥应报错")
	}
	if _, err := CreateIssuingCA(root, CARequest{}, testNowMs); !errors.Is(err, ErrCommonNameRequired) {
		t.Errorf("缺少通用名应返回 ErrCommonNameRequired，得到 %v", err)
	}
	if _, err := CreateIssuingCA(root, CARequest{
		CommonName: "X", Algorithm: KeyAlgorithm(99),
	}, testNowMs); err == nil {
		t.Error("不支持的算法应报错")
	}
}

func TestIssueValidation(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())

	if _, err := ca.IssueKeyPair(IssueRequest{}); !errors.Is(err, ErrCommonNameRequired) {
		t.Errorf("缺少通用名应返回 ErrCommonNameRequired，得到 %v", err)
	}
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: strings.Repeat("a", 200),
	}); err == nil {
		t.Error("通用名超长应报错")
	}
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", Algorithm: KeyAlgorithm(99),
	}); err == nil {
		t.Error("不支持的算法应报错")
	}
	// 请求中带 SAN 而策略不允许
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", DNSNames: []string{"a.example"},
	}); !errors.Is(err, ErrSANsNotAllowed) {
		t.Errorf("DNS SAN 应被拒，得到 %v", err)
	}
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", IPAddresses: []string{"10.0.0.1"},
	}); !errors.Is(err, ErrSANsNotAllowed) {
		t.Errorf("IP SAN 应被拒，得到 %v", err)
	}
}

func TestIssueSANsLimits(t *testing.T) {
	p := DefaultPolicy()
	p.AllowDNSSANs = true
	p.AllowIPSANs = true
	p.MaxSANs = 1
	ca, _, _ := newTestCA(t, p)

	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", DNSNames: []string{"a.example", "b.example"},
	}); err == nil {
		t.Error("SAN 条目数超限应报错")
	}
	// 非法 IP 字面量必须报错，而不是写入一个空 IP
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", IPAddresses: []string{"不是地址"},
	}); err == nil {
		t.Error("非法 IP 应报错")
	}
	// 合法且在限内
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "x", DNSNames: []string{"a.example"},
	}); err != nil {
		t.Errorf("限内的 SAN 应通过: %v", err)
	}
}

func TestIssueServerCertValidation(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	if _, err := ca.IssueServerCert("", nil, nil, time.Hour); !errors.Is(err, ErrCommonNameRequired) {
		t.Errorf("缺少通用名应返回 ErrCommonNameRequired，得到 %v", err)
	}
	if _, err := ca.IssueServerCert("h", nil, []string{"不是地址"}, time.Hour); err == nil {
		t.Error("非法 IP 应报错")
	}
	// 服务端证书不受设备 SAN 策略限制：它本来就需要主机名
	srv, err := ca.IssueServerCert("h.example", []string{"h.example"},
		[]string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatalf("服务端证书应允许 SAN: %v", err)
	}
	if len(srv.Cert.DNSNames) != 1 || len(srv.Cert.IPAddresses) != 1 {
		t.Error("服务端证书的 SAN 未写入")
	}
}

func TestPolicyAccessor(t *testing.T) {
	p := DefaultPolicy()
	p.MaxCNLen = 7
	ca, _, _ := newTestCA(t, p)
	if ca.Policy().MaxCNLen != 7 {
		t.Error("Policy 应返回当前策略")
	}
	if ca.Issuer() == nil {
		t.Error("Issuer 不应为 nil")
	}
}

func TestSignCSRErrors(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	if _, err := ca.SignCSR(nil, IssueRequest{CommonName: "x"}); err == nil {
		t.Error("空 CSR 应报错")
	}
	if _, err := ca.SignCSR(pem.EncodeToMemory(&pem.Block{
		Type: pemTypeCSR, Bytes: []byte{1, 2},
	}), IssueRequest{CommonName: "x"}); err == nil {
		t.Error("畸形 CSR 应报错")
	}
}

func TestMemoryStoreErrors(t *testing.T) {
	s := NewMemoryStore(nil)
	if _, err := s.Signer(t.Context()); err == nil {
		t.Error("无密钥的内存后端应报错")
	}
	key, _ := GenerateKey(ECDSAP256)
	s = NewMemoryStore(key)
	if _, err := s.Signer(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.Kind() != "memory" {
		t.Errorf("Kind = %q", s.Kind())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Close 之后密钥必须真的不见了：否则"关闭"只是个说法
	if _, err := s.Signer(t.Context()); err == nil {
		t.Error("Close 之后不应仍能取到密钥")
	}
}

func TestFileStoreErrors(t *testing.T) {
	dir := t.TempDir()

	if _, err := NewFileStore(""); err == nil {
		t.Error("缺少路径应报错")
	}
	if err := WriteKeyFile(filepath.Join(dir, "k"), fakeSigner{}); err == nil {
		t.Error("无法编码的私钥应报错")
	}

	// 文件不存在
	st, err := NewFileStore(filepath.Join(dir, "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Signer(t.Context()); err == nil {
		t.Error("文件不存在应报错")
	}

	// 写入到不存在的多级目录：应自动创建
	key, _ := GenerateKey(ECDSAP256)
	nested := filepath.Join(dir, "a", "b", "ca.key")
	if err := WriteKeyFile(nested, key); err != nil {
		t.Fatalf("应自动创建目录: %v", err)
	}
	if fi, err := os.Stat(filepath.Dir(nested)); err != nil {
		t.Fatal(err)
	} else if fi.Mode().Perm() != 0o700 {
		t.Errorf("密钥目录权限 = %o，期望 700", fi.Mode().Perm())
	}
}

// TestFileStoreCaches 验证签名器只从盘上读一次。
func TestFileStoreCaches(t *testing.T) {
	dir := t.TempDir()
	key, _ := GenerateKey(ECDSAP256)
	path := filepath.Join(dir, "ca.key")
	if err := WriteKeyFile(path, key); err != nil {
		t.Fatal(err)
	}
	st, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.Signer(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// 删掉文件后仍能取到：说明走的是缓存，而不是每次读盘
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	second, err := st.Signer(t.Context())
	if err != nil {
		t.Fatalf("应命中缓存: %v", err)
	}
	if first != second {
		t.Error("两次应返回同一个签名器")
	}
	if st.Kind() != "file" {
		t.Errorf("Kind = %q", st.Kind())
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestParsePrivateKeyRejectsNonSigningKey 验证合法但不能签名的私钥被拒。
//
// X25519 的 PKCS#8 文件是完全合法的 PKCS#8，解析也成功，但得到的是
// 密钥协商密钥而非签名密钥。若放过它，错误会推迟到第一次签发时才以
// 类型断言失败或 panic 的形式出现——那时已经分不清是配置错了还是实现错了。
func TestParsePrivateKeyRejectsNonSigningKey(t *testing.T) {
	x := ecdh.X25519()
	key, err := x.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	blob := pem.EncodeToMemory(&pem.Block{Type: pemTypePrivateKey, Bytes: der})
	if _, err := ParsePrivateKeyPEM(blob); err == nil {
		t.Error("不能用于签名的私钥应被拒绝")
	}
}
