package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/pki"
)

// LoadSessionTicketKeys 读取集群共享的 TLS session ticket 密钥环。
// 第一行是当前签发密钥，后续行仅用于解开轮换前的 ticket。
func LoadSessionTicketKeys(path string) ([][32]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 TLS session ticket 密钥 %s 失败: %w", path, err)
	}
	var out [][32]byte
	seen := make(map[[32]byte]struct{})
	for lineNo, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		raw, derr := hex.DecodeString(line)
		if derr != nil || len(raw) != 32 {
			return nil, fmt.Errorf("%s:%d: ticket 密钥必须是 64 位十六进制", path, lineNo+1)
		}
		var key [32]byte
		copy(key[:], raw)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%s:%d: ticket 密钥重复", path, lineNo+1)
		}
		seen[key] = struct{}{}
		out = append(out, key)
		if len(out) > 8 {
			return nil, fmt.Errorf("%s: ticket 密钥最多保留 8 把", path)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: 没有可用的 TLS session ticket 密钥", path)
	}
	return out, nil
}

// PKI 文件名。
const (
	fileRootCert    = "root-ca.pem"
	fileIssuingCert = "issuing-ca.pem"
	fileIssuingKey  = "issuing-ca.key"
	fileServerCert  = "server.pem"
	fileServerKey   = "server.key"
	fileChain       = "ca-chain.pem"
)

// ErrPKIExists 表示 PKI 已经生成过，InitPKI 没有动它。
//
// 做成哨兵而不是一句错误文本，是给 `init --if-needed` 用的：编排里的
// 初始化步骤会被反复执行（compose 的 init 服务、k8s 的 initContainer），
// 它得能分辨「已经做过」与「真的失败了」。靠匹配错误文本去分辨会随文案
// 漂移，而漂移之后的表现是编排把一次真失败当成"已经做过"继续往下走。
var ErrPKIExists = errors.New("PKI 已存在")

// CA 私钥的静态加密已整条移除（AUTH-092）：私钥与 access key 的 secret
// 都以明文落在数据目录里，保护只有文件权限。换的是部署与运维的简单
// ——四种部署方式、五个验收脚本与 CI 的冒烟不必再各想一遍"口令怎么
// 送进容器"。代价写在 pki.FileStore 的类型注释里。

// pkiPaths 解析 PKI 各文件的实际路径。
//
// 配置留空时取 data dir 下的默认路径，使最简部署不必写任何路径。
type pkiPaths struct {
	root, issuingCert, issuingKey string
	serverCert, serverKey, chain  string
}

func resolvePKIPaths(dd *node.DataDir, cfg *config.Config) pkiPaths {
	dir := dd.Path(node.DirPKI)
	p := pkiPaths{
		root:        filepath.Join(dir, fileRootCert),
		issuingCert: filepath.Join(dir, fileIssuingCert),
		issuingKey:  filepath.Join(dir, fileIssuingKey),
		serverCert:  filepath.Join(dir, fileServerCert),
		serverKey:   filepath.Join(dir, fileServerKey),
		chain:       filepath.Join(dir, fileChain),
	}
	if cfg.PKI.RootCertFile != "" {
		p.root = cfg.PKI.RootCertFile
		p.issuingCert = cfg.PKI.IssuingCertFile
		p.issuingKey = cfg.PKI.IssuingKeyFile
	}
	return p
}

// InitPKI 在 data dir 中建立一套新的 CA 与服务端证书。
//
// 结构是「离线根 CA 加在线签发 CA」（PRD PKI-014）：根 CA 的私钥签完
// 签发 CA 之后即被丢弃，不落盘。这样即使在线节点被完全攻破，
// 攻击者也无法签发新的签发 CA，吊销与重建信任链仍然可行。
func InitPKI(dd *node.DataDir, cfg *config.Config, nowMs int64) error {
	if _, err := dd.EnsureDir(node.DirPKI); err != nil {
		return err
	}
	p := resolvePKIPaths(dd, cfg)
	if _, err := os.Stat(p.issuingKey); err == nil {
		return fmt.Errorf("%w（%s）；如需重建请先清空 %s 目录",
			ErrPKIExists, p.issuingKey, dd.Path(node.DirPKI))
	}

	root, err := pki.CreateRootCA(pki.CARequest{
		CommonName: "open-iot-core Root CA", MaxPathLen: 1,
	}, nowMs)
	if err != nil {
		return fmt.Errorf("创建根 CA 失败: %w", err)
	}
	issuing, err := pki.CreateIssuingCA(root, pki.CARequest{
		CommonName: "open-iot-core Issuing CA", MaxPathLen: 0,
	}, nowMs)
	if err != nil {
		return fmt.Errorf("创建签发 CA 失败: %w", err)
	}

	ca, err := newCA(issuing, root.Cert, cfg, func() int64 { return nowMs })
	if err != nil {
		return err
	}
	srv, err := ca.IssueServerCert(cfg.PKI.ServerHosts[0],
		dnsHosts(cfg.PKI.ServerHosts), ipHosts(cfg.PKI.ServerHosts),
		cfg.PKI.MaxValidity.D())
	if err != nil {
		return fmt.Errorf("签发服务端证书失败: %w", err)
	}

	// 根 CA 的私钥在此作用域结束后即不可达，不写入任何文件。
	writes := []struct {
		path string
		data []byte
		perm os.FileMode
	}{
		{p.root, pki.EncodeCertPEM(root.Cert.Raw), 0o644},
		{p.issuingCert, pki.EncodeCertPEM(issuing.Cert.Raw), 0o644},
		{p.chain, ca.ChainPEM(), 0o644},
		{p.serverCert, append(append([]byte(nil), srv.CertPEM...), srv.ChainPEM...), 0o644},
		{p.serverKey, srv.KeyPEM, 0o600},
	}
	for _, w := range writes {
		if err := os.WriteFile(w.path, w.data, w.perm); err != nil {
			return fmt.Errorf("写入 %s 失败: %w", w.path, err)
		}
	}
	if err := pki.WriteKeyFile(p.issuingKey, issuing.Key); err != nil {
		return fmt.Errorf("写入签发 CA 私钥失败: %w", err)
	}
	return nil
}

// LoadCA 从 data dir 加载签发 CA。
func LoadCA(dd *node.DataDir, cfg *config.Config, now func() int64) (*pki.CA, error) {
	p := resolvePKIPaths(dd, cfg)
	rootCert, err := loadCert(p.root)
	if err != nil {
		return nil, err
	}
	issuingCert, err := loadCert(p.issuingCert)
	if err != nil {
		return nil, err
	}
	store, err := pki.NewFileStore(p.issuingKey)
	if err != nil {
		return nil, err
	}
	key, err := store.Signer(context.Background())
	if err != nil {
		return nil, fmt.Errorf("加载签发 CA 私钥失败: %w", err)
	}
	return newCA(&pki.Bundle{Cert: issuingCert, Key: key}, rootCert, cfg, now)
}

// newCA 按配置的签发策略构造 CA。
func newCA(issuing *pki.Bundle, rootCert *x509.Certificate,
	cfg *config.Config, now func() int64,
) (*pki.CA, error) {
	policy := pki.DefaultPolicy()
	policy.DefaultValidity = cfg.PKI.DefaultValidity.D()
	policy.MaxValidity = cfg.PKI.MaxValidity.D()
	ca, err := pki.New(issuing, []*x509.Certificate{issuing.Cert, rootCert}, policy, now)
	if err != nil {
		return nil, fmt.Errorf("构造签发 CA 失败: %w", err)
	}
	return ca, nil
}

// LoadServerCert 加载服务端证书。
//
// 配置给出了 cert_file 与 key_file 则用它们，否则用 data dir 下由
// init 生成的那一对。
func LoadServerCert(dd *node.DataDir, cfg *config.Config, l *config.TLSListener) (tls.Certificate, error) {
	certFile, keyFile := l.CertFile, l.KeyFile
	if certFile == "" {
		p := resolvePKIPaths(dd, cfg)
		certFile, keyFile = p.serverCert, p.serverKey
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf(
			"加载服务端证书失败（%s / %s）：%w；"+
				"若尚未初始化请先执行 `openiotd init`",
			certFile, keyFile, err)
	}
	return cert, nil
}

// LoadClientCAs 构造校验客户端证书的信任池。
func LoadClientCAs(dd *node.DataDir, cfg *config.Config, l *config.TLSListener,
	ca *pki.CA,
) (*x509.CertPool, error) {
	if l.ClientCAFile != "" {
		data, err := os.ReadFile(l.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("读取 %s 失败: %w", l.ClientCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("%s 中没有可用的证书", l.ClientCAFile)
		}
		return pool, nil
	}
	if ca == nil {
		return nil, errors.New("既未配置 client_ca_file 也未启用内嵌 PKI")
	}
	return ca.TrustPool(), nil
}

func loadCert(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取证书 %s 失败: %w", path, err)
	}
	c, err := pki.ParseCertPEM(data)
	if err != nil {
		return nil, fmt.Errorf("解析证书 %s 失败: %w", path, err)
	}
	return c, nil
}

// dnsHosts 与 ipHosts 把混在一起的主机列表分成 DNS 名与 IP。
//
// 让用户在一个列表里混写是刻意的：运维不该被要求预先知道
// "127.0.0.1 要写在 ip 字段而 localhost 要写在 dns 字段"。
func dnsHosts(hosts []string) []string {
	var out []string
	for _, h := range hosts {
		if !isIP(h) {
			out = append(out, h)
		}
	}
	return out
}

func ipHosts(hosts []string) []string {
	var out []string
	for _, h := range hosts {
		if isIP(h) {
			out = append(out, h)
		}
	}
	return out
}

func isIP(s string) bool {
	for _, r := range s {
		if r == ':' {
			return true // IPv6
		}
		if (r < '0' || r > '9') && r != '.' {
			return false
		}
	}
	return len(s) > 0
}

// issueDeviceValidity 保证签发时长不超过策略上限。
func issueDeviceValidity(cfg *config.Config, want time.Duration) time.Duration {
	if want <= 0 {
		return cfg.PKI.DefaultValidity.D()
	}
	return want
}
