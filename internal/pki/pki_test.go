package pki

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testNowMs = 1_700_000_000_000

func testClock() func() int64 { return func() int64 { return testNowMs } }

// newTestCA 建立一套根 CA 加签发 CA 的层级。
func newTestCA(t *testing.T, policy Policy) (*CA, *Bundle, *Bundle) {
	t.Helper()
	root, err := CreateRootCA(CARequest{
		CommonName: "Test Root CA", Organization: "Acme", MaxPathLen: 1,
	}, testNowMs)
	if err != nil {
		t.Fatalf("创建根 CA 失败: %v", err)
	}
	issuing, err := CreateIssuingCA(root, CARequest{
		CommonName: "Test Issuing CA", Organization: "Acme", MaxPathLen: 0,
	}, testNowMs)
	if err != nil {
		t.Fatalf("创建签发 CA 失败: %v", err)
	}
	ca, err := New(issuing, []*x509.Certificate{issuing.Cert, root.Cert}, policy, testClock())
	if err != nil {
		t.Fatalf("创建 CA 失败: %v", err)
	}
	return ca, root, issuing
}

func TestKeyGenerationAndAlgorithmDetection(t *testing.T) {
	for _, alg := range []KeyAlgorithm{ECDSAP256, ECDSAP384, RSA2048, Ed25519} {
		t.Run(alg.String(), func(t *testing.T) {
			key, err := GenerateKey(alg)
			if err != nil {
				t.Fatalf("生成密钥失败: %v", err)
			}
			got, ok := AlgorithmOf(key.Public())
			if !ok || got != alg {
				t.Errorf("算法识别 = %s/%v，期望 %s", got, ok, alg)
			}
			// PEM 往返
			pemKey, err := EncodePrivateKeyPEM(key)
			if err != nil {
				t.Fatal(err)
			}
			back, err := ParsePrivateKeyPEM(pemKey)
			if err != nil {
				t.Fatalf("解析私钥失败: %v", err)
			}
			if _, ok := AlgorithmOf(back.Public()); !ok {
				t.Error("往返后无法识别算法")
			}
		})
	}
	if _, err := GenerateKey(KeyAlgorithm(99)); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Errorf("未知算法应报 ErrUnsupportedAlgorithm，得到 %v", err)
	}
	if _, ok := AlgorithmOf("not a key"); ok {
		t.Error("非密钥不应被识别")
	}
	// RSA-4096 单独测，生成较慢
	if testing.Short() {
		t.Skip()
	}
	if k, err := GenerateKey(RSA4096); err != nil {
		t.Errorf("RSA-4096 生成失败: %v", err)
	} else if a, ok := AlgorithmOf(k.Public()); !ok || a != RSA4096 {
		t.Errorf("RSA-4096 识别为 %s", a)
	}
}

func TestAlgorithmNames(t *testing.T) {
	for _, alg := range []KeyAlgorithm{ECDSAP256, ECDSAP384, RSA2048, RSA4096, Ed25519} {
		got, ok := ParseAlgorithm(alg.String())
		if !ok || got != alg {
			t.Errorf("%s 往返失败", alg)
		}
	}
	if _, ok := ParseAlgorithm("nope"); ok {
		t.Error("未知算法名应被拒绝")
	}
	if KeyAlgorithm(99).String() != "unknown" {
		t.Error("越界算法应返回 unknown")
	}
}

func TestCAHierarchyVerifies(t *testing.T) {
	ca, root, issuing := newTestCA(t, DefaultPolicy())

	// 签发 CA 必须能被根 CA 校验
	pool := x509.NewCertPool()
	pool.AddCert(root.Cert)
	if _, err := issuing.Cert.Verify(x509.VerifyOptions{
		Roots:       pool,
		CurrentTime: time.UnixMilli(testNowMs),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}); err != nil {
		t.Fatalf("签发 CA 无法被根校验: %v", err)
	}

	// 签发出的设备证书必须能经链校验到根
	issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatalf("签发失败: %v", err)
	}
	inter := x509.NewCertPool()
	inter.AddCert(issuing.Cert)
	chains, err := issued.Cert.Verify(x509.VerifyOptions{
		Roots:         pool,
		Intermediates: inter,
		CurrentTime:   time.UnixMilli(testNowMs),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		t.Fatalf("设备证书无法校验到根: %v", err)
	}
	if len(chains) == 0 || len(chains[0]) != 3 {
		t.Errorf("证书链长度 = %v，期望 3（设备、签发 CA、根）", len(chains[0]))
	}

	// 签发 CA 的 MaxPathLen 为 0，其下不得再有中间 CA
	if issuing.Cert.MaxPathLen != 0 || !issuing.Cert.MaxPathLenZero {
		t.Error("签发 CA 的路径长度应限制为 0")
	}
	// 下发的链应含两张证书
	if n := len(bytes.Split(ca.ChainPEM(), []byte("-----BEGIN CERTIFICATE-----"))) - 1; n != 2 {
		t.Errorf("下发链含 %d 张证书，期望 2", n)
	}
	if ca.Issuer() != issuing.Cert {
		t.Error("Issuer 应返回签发证书")
	}
	if ca.TrustPool() == nil {
		t.Error("信任池不应为空")
	}
}

func TestClockSkewGrace(t *testing.T) {
	// 设备时钟不准是 IoT 的常态：NotBefore 必须回退一段，
	// 否则慢几秒的设备会判定刚签发的证书"尚未生效"。
	ca, _, _ := newTestCA(t, DefaultPolicy())
	issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(testNowMs)
	if !issued.Cert.NotBefore.Before(now) {
		t.Errorf("NotBefore = %v，应早于当前时刻 %v", issued.Cert.NotBefore, now)
	}
	if now.Sub(issued.Cert.NotBefore) < time.Minute {
		t.Errorf("NotBefore 的回退量只有 %v，不足以容忍设备时钟偏差",
			now.Sub(issued.Cert.NotBefore))
	}
}

// TestIssuedCertIsClientOnly 固定住一条安全性质：
// 签发出的设备证书只能用于客户端认证。
//
// 一个同时具备 serverAuth 的客户端证书在某些配置下可以冒充服务端，
// 而设备证书没有任何理由需要它。
func TestIssuedCertIsClientOnly(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	if issued.Cert.IsCA {
		t.Error("设备证书不应是 CA 证书")
	}
	if len(issued.Cert.ExtKeyUsage) != 1 ||
		issued.Cert.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth {
		t.Errorf("扩展密钥用途 = %v，期望仅 clientAuth", issued.Cert.ExtKeyUsage)
	}
	if issued.Cert.KeyUsage&x509.KeyUsageCertSign != 0 {
		t.Error("设备证书不应具备 certSign 用途")
	}
}

// TestPrivateKeyReturnedOnceOnly 固定住 PRD PKI-010：
// 设备私钥只在签发响应中出现一次，CA 不留存。
func TestPrivateKeyReturnedOnceOnly(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())

	withKey, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	if len(withKey.KeyPEM) == 0 {
		t.Fatal("IssueKeyPair 应返回私钥")
	}
	if _, err := ParsePrivateKeyPEM(withKey.KeyPEM); err != nil {
		t.Errorf("返回的私钥无法解析: %v", err)
	}

	// 签 CSR 的路径下 CA 从不接触私钥
	key, _ := GenerateKey(ECDSAP256)
	csrPEM := makeCSR(t, key, "dev-002", nil, nil)
	fromCSR, err := ca.SignCSR(csrPEM, IssueRequest{CommonName: "dev-002"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fromCSR.KeyPEM) != 0 {
		t.Error("SignCSR 不应返回私钥——设备自持私钥是推荐路径")
	}
}

// TestPolicyIgnoresCSRSubjectByDefault 固定住 PRD PKI-012 最关键的一条。
//
// CSR 由设备生成，其中的主题是设备自己声明的，而证书主题会被策略变量
// ${iot:Certificate.Subject.CommonName} 引用。采纳设备自报的主题
// 等于让设备自己决定自己的权限。
func TestPolicyIgnoresCSRSubjectByDefault(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	key, _ := GenerateKey(ECDSAP256)
	// 设备在 CSR 里声称自己是 admin
	csrPEM := makeCSR(t, key, "admin", nil, nil)

	issued, err := ca.SignCSR(csrPEM, IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	if issued.Cert.Subject.CommonName != "dev-001" {
		t.Errorf("证书通用名 = %q，期望采用控制面给出的 dev-001 而非 CSR 自报的 admin",
			issued.Cert.Subject.CommonName)
	}

	// 显式允许采纳 CSR 主题时才生效
	p := DefaultPolicy()
	p.SubjectSource = SubjectFromCSR
	ca2, _, _ := newTestCA(t, p)
	issued2, err := ca2.SignCSR(csrPEM, IssueRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if issued2.Cert.Subject.CommonName != "admin" {
		t.Errorf("显式允许后通用名 = %q，期望 admin", issued2.Cert.Subject.CommonName)
	}
}

// TestPolicyRejectsSANsByDefault 固定住 SAN 的默认拒绝。
//
// 设备客户端证书不需要 SAN，而一个带任意 DNS SAN 的客户端证书
// 在某些服务端配置下可以被当作服务端证书使用。
func TestPolicyRejectsSANsByDefault(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())

	// 请求中带 SAN
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", DNSNames: []string{"evil.example"},
	}); !errors.Is(err, ErrSANsNotAllowed) {
		t.Errorf("带 DNS 名的请求应被拒绝，得到 %v", err)
	}
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", IPAddresses: []string{"10.0.0.1"},
	}); !errors.Is(err, ErrSANsNotAllowed) {
		t.Errorf("带 IP 的请求应被拒绝，得到 %v", err)
	}

	// CSR 中带 SAN，且策略采纳 CSR 主题时也必须被拒
	p := DefaultPolicy()
	p.SubjectSource = SubjectFromCSR
	ca2, _, _ := newTestCA(t, p)
	key, _ := GenerateKey(ECDSAP256)
	csrPEM := makeCSR(t, key, "dev-001", []string{"evil.example"}, nil)
	if _, err := ca2.SignCSR(csrPEM, IssueRequest{}); !errors.Is(err, ErrSANsNotAllowed) {
		t.Errorf("CSR 中的 SAN 应被拒绝，得到 %v", err)
	}
	// 即便策略不采纳 CSR 主题，CSR 里的 SAN 也不应被写入证书
	issued, err := ca.SignCSR(csrPEM, IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	if len(issued.Cert.DNSNames) != 0 {
		t.Errorf("证书不应含 CSR 里的 DNS 名，得到 %v", issued.Cert.DNSNames)
	}

	// 显式允许后生效，并受数量上限约束
	p2 := DefaultPolicy()
	p2.AllowDNSSANs = true
	p2.MaxSANs = 1
	ca3, _, _ := newTestCA(t, p2)
	if _, err := ca3.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", DNSNames: []string{"a.example"},
	}); err != nil {
		t.Errorf("显式允许后应放行，得到 %v", err)
	}
	if _, err := ca3.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", DNSNames: []string{"a.example", "b.example"},
	}); err == nil {
		t.Error("超出 SAN 数量上限应被拒绝")
	}
}

func TestPolicyAlgorithmRestriction(t *testing.T) {
	p := DefaultPolicy()
	p.AllowedAlgorithms = []KeyAlgorithm{ECDSAP256}
	ca, _, _ := newTestCA(t, p)

	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", Algorithm: RSA2048,
	}); !errors.Is(err, ErrAlgorithmNotAllowed) {
		t.Errorf("策略外的算法应被拒绝，得到 %v", err)
	}
	// CSR 的公钥算法同样受约束
	rsaKey, _ := GenerateKey(RSA2048)
	csrPEM := makeCSR(t, rsaKey, "dev-001", nil, nil)
	if _, err := ca.SignCSR(csrPEM, IssueRequest{CommonName: "dev-001"}); !errors.Is(err, ErrAlgorithmNotAllowed) {
		t.Errorf("CSR 的算法应受约束，得到 %v", err)
	}
	// 允许的算法放行
	if _, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"}); err != nil {
		t.Errorf("默认算法应放行，得到 %v", err)
	}
}

// TestPolicyClampsValidity 验证超长有效期被截断而非报错。
//
// 截断是安全的方向；报错只会让调用方去试探上限。
func TestPolicyClampsValidity(t *testing.T) {
	p := DefaultPolicy()
	p.MaxValidity = 24 * time.Hour
	ca, _, _ := newTestCA(t, p)

	issued, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", Validity: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("超长有效期应被截断而非报错，得到 %v", err)
	}
	span := issued.Cert.NotAfter.Sub(time.UnixMilli(testNowMs))
	if span > 24*time.Hour {
		t.Errorf("有效期 = %v，应被截断到 24 小时", span)
	}
	// 未指定时取默认值
	issued2, _ := ca.IssueKeyPair(IssueRequest{CommonName: "dev-002"})
	if issued2.Cert.NotAfter.Sub(time.UnixMilli(testNowMs)) > 24*time.Hour {
		t.Error("默认有效期也应受上限约束")
	}
}

// TestIssuedValidityNeverExceedsIssuer 验证终端证书不会比签发 CA 活得久。
func TestIssuedValidityNeverExceedsIssuer(t *testing.T) {
	root, err := CreateRootCA(CARequest{CommonName: "R", Validity: 48 * time.Hour}, testNowMs)
	if err != nil {
		t.Fatal(err)
	}
	// 请求的签发 CA 有效期超过根，应被截断
	issuing, err := CreateIssuingCA(root, CARequest{
		CommonName: "I", Validity: 10 * 365 * 24 * time.Hour,
	}, testNowMs)
	if err != nil {
		t.Fatal(err)
	}
	if issuing.Cert.NotAfter.After(root.Cert.NotAfter) {
		t.Error("签发 CA 的有效期不应超出根 CA")
	}

	p := DefaultPolicy()
	p.MaxValidity = 10 * 365 * 24 * time.Hour
	ca, err := New(issuing, []*x509.Certificate{issuing.Cert, root.Cert}, p, testClock())
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", Validity: 10 * 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if issued.Cert.NotAfter.After(issuing.Cert.NotAfter) {
		t.Error("设备证书的有效期不应超出签发 CA")
	}
}

func TestCommonNameRequired(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	if _, err := ca.IssueKeyPair(IssueRequest{}); !errors.Is(err, ErrCommonNameRequired) {
		t.Errorf("缺少通用名应被拒绝，得到 %v", err)
	}
	p := DefaultPolicy()
	p.MaxCNLen = 4
	ca2, _, _ := newTestCA(t, p)
	if _, err := ca2.IssueKeyPair(IssueRequest{CommonName: "toolong"}); err == nil {
		t.Error("超长通用名应被拒绝")
	}
	if _, err := ca.IssueServerCert("", nil, nil, 0); !errors.Is(err, ErrCommonNameRequired) {
		t.Error("服务端证书也需要通用名")
	}
}

// TestCSRSignatureVerified 固定住一条安全性质：
// CSR 的签名证明申请者持有对应私钥，不验等于允许任何人用别人的公钥申请证书。
func TestCSRSignatureVerified(t *testing.T) {
	key, _ := GenerateKey(ECDSAP256)
	csrPEM := makeCSR(t, key, "dev-001", nil, nil)
	if _, err := ParseCSRPEM(csrPEM); err != nil {
		t.Fatalf("合法 CSR 应通过: %v", err)
	}

	// 篡改 CSR 的内容使签名失效
	blk := []byte(csrPEM)
	der, err := decodePEMBytes(blk, "CERTIFICATE REQUEST")
	if err != nil {
		t.Fatal(err)
	}
	// 改动 DER 中部的一个字节
	tampered := append([]byte(nil), der...)
	tampered[len(tampered)/2] ^= 0xFF
	if _, err := ParseCSRPEM(EncodeCSRPEM(tampered)); err == nil {
		t.Error("被篡改的 CSR 应被拒绝")
	}

	ca, _, _ := newTestCA(t, DefaultPolicy())
	if _, err := ca.SignCSR([]byte("not pem"), IssueRequest{CommonName: "x"}); err == nil {
		t.Error("非 PEM 输入应被拒绝")
	}
}

func TestFingerprintStability(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	// 指纹是 DER 的 SHA-256，必须可从 PEM 重算出同一值
	parsed, err := ParseCertPEM(issued.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if FingerprintOf(parsed) != issued.Fingerprint {
		t.Error("从 PEM 重算的指纹与签发时不一致")
	}
	if len(issued.Fingerprint.String()) != 64 {
		t.Errorf("指纹十六进制长度 = %d，期望 64", len(issued.Fingerprint.String()))
	}
	// 两张不同的证书指纹必须不同
	other, _ := ca.IssueKeyPair(IssueRequest{CommonName: "dev-002"})
	if other.Fingerprint == issued.Fingerprint {
		t.Error("不同证书的指纹不应相同")
	}
}

func TestNewValidatesIssuer(t *testing.T) {
	ca, root, issuing := newTestCA(t, DefaultPolicy())
	_ = ca
	cases := map[string]struct {
		b   *Bundle
		now func() int64
	}{
		"缺少 Bundle": {nil, testClock()},
		"缺少证书":      {&Bundle{Key: issuing.Key}, testClock()},
		"缺少私钥":      {&Bundle{Cert: issuing.Cert}, testClock()},
		"缺少时钟":      {issuing, nil},
	}
	for name, tc := range cases {
		if _, err := New(tc.b, nil, DefaultPolicy(), tc.now); err == nil {
			t.Errorf("%s 应报错", name)
		}
	}
	// 非 CA 证书不能作为签发者
	issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})
	if err != nil {
		t.Fatal(err)
	}
	key, _ := GenerateKey(ECDSAP256)
	if _, err := New(&Bundle{Cert: issued.Cert, Key: key}, nil, DefaultPolicy(), testClock()); err == nil {
		t.Error("非 CA 证书不应能作为签发者")
	}
	_ = root
}

func TestCreateCAValidation(t *testing.T) {
	if _, err := CreateRootCA(CARequest{}, testNowMs); !errors.Is(err, ErrCommonNameRequired) {
		t.Error("根 CA 需要通用名")
	}
	root, _ := CreateRootCA(CARequest{CommonName: "R"}, testNowMs)
	if _, err := CreateIssuingCA(root, CARequest{}, testNowMs); !errors.Is(err, ErrCommonNameRequired) {
		t.Error("签发 CA 需要通用名")
	}
	if _, err := CreateIssuingCA(nil, CARequest{CommonName: "I"}, testNowMs); err == nil {
		t.Error("缺少根 CA 应报错")
	}
	if _, err := CreateRootCA(CARequest{
		CommonName: "R", Algorithm: KeyAlgorithm(99),
	}, testNowMs); err == nil {
		t.Error("非法算法应报错")
	}
}

func TestServerCertHasServerAuth(t *testing.T) {
	ca, root, issuing := newTestCA(t, DefaultPolicy())
	srv, err := ca.IssueServerCert("broker.local", []string{"broker.local"},
		[]string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatalf("签发服务端证书失败: %v", err)
	}
	if len(srv.ExtKeyUsageOf()) != 1 || srv.ExtKeyUsageOf()[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("扩展密钥用途 = %v，期望仅 serverAuth", srv.ExtKeyUsageOf())
	}
	if len(srv.KeyPEM) == 0 {
		t.Error("服务端证书应返回私钥")
	}
	// 应能作为服务端证书被校验
	pool := x509.NewCertPool()
	pool.AddCert(root.Cert)
	inter := x509.NewCertPool()
	inter.AddCert(issuing.Cert)
	if _, err := srv.Cert.Verify(x509.VerifyOptions{
		Roots: pool, Intermediates: inter,
		CurrentTime: time.UnixMilli(testNowMs),
		DNSName:     "broker.local",
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("服务端证书校验失败: %v", err)
	}
	if _, err := ca.IssueServerCert("x", nil, []string{"not-an-ip"}, 0); err == nil {
		t.Error("非法 IP 应报错")
	}
}

func TestIssueRejectsBadIP(t *testing.T) {
	p := DefaultPolicy()
	p.AllowIPSANs = true
	ca, _, _ := newTestCA(t, p)
	if _, err := ca.IssueKeyPair(IssueRequest{
		CommonName: "dev-001", IPAddresses: []string{"nonsense"},
	}); err == nil {
		t.Error("非法 IP 应报错")
	}
}

func TestSerialNumbersAreUniqueAndPositive(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		issued, err := ca.IssueKeyPair(IssueRequest{CommonName: "dev"})
		if err != nil {
			t.Fatal(err)
		}
		s := issued.Cert.SerialNumber
		if s.Sign() <= 0 {
			t.Fatalf("序列号必须是正数（RFC 5280 4.1.2.2），得到 %v", s)
		}
		if seen[s.String()] {
			t.Fatalf("序列号 %v 重复", s)
		}
		seen[s.String()] = true
	}
}

func TestParseHelpers(t *testing.T) {
	ca, _, _ := newTestCA(t, DefaultPolicy())
	issued, _ := ca.IssueKeyPair(IssueRequest{CommonName: "dev-001"})

	// 证书链解析
	chain, err := ParseCertChainPEM(ca.ChainPEM())
	if err != nil || len(chain) != 2 {
		t.Errorf("链解析 = %d 张, %v", len(chain), err)
	}
	if _, err := ParseCertChainPEM([]byte("no pem here")); err == nil {
		t.Error("非 PEM 应报错")
	}
	// 类型不符
	if _, err := ParseCertPEM(issued.KeyPEM); err == nil {
		t.Error("私钥 PEM 不应被当作证书解析")
	}
	if _, err := ParseCSRPEM(issued.CertPEM); err == nil {
		t.Error("证书 PEM 不应被当作 CSR 解析")
	}
	if _, err := ParsePrivateKeyPEM(issued.CertPEM); err == nil {
		t.Error("证书 PEM 不应被当作私钥解析")
	}
	if _, err := ParsePrivateKeyPEM([]byte("garbage")); err == nil {
		t.Error("垃圾输入应报错")
	}
}

// --- 密钥后端 ---

func TestMemoryStore(t *testing.T) {
	key, _ := GenerateKey(ECDSAP256)
	s := NewMemoryStore(key)
	got, err := s.Signer(nil)
	if err != nil || got != key {
		t.Errorf("Signer = %v, %v", got, err)
	}
	if s.Kind() != "memory" {
		t.Errorf("Kind = %q", s.Kind())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Signer(nil); err == nil {
		t.Error("关闭后应报错")
	}
}

// TestFileStoreRoundTrip 往返一次，并钉住落盘形态（AUTH-092）。
//
// 静态加密去掉之后，文件权限是私钥唯一的保护——此前这里断言权限的
// 理由是"口令强度未知"，现在是"除它之外什么都没有"，所以权限与
// PEM 形态两条都要断言，松一位不再有东西兜着。
func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "ca.key")
	key, _ := GenerateKey(ECDSAP256)

	if err := WriteKeyFile(path, key); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(blob, []byte("-----BEGIN")) {
		t.Errorf("应是普通 PEM，开头为 %q", blob[:min(16, len(blob))])
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != keyFilePerm {
		t.Errorf("密钥文件权限 = %o，期望 %o", fi.Mode().Perm(), keyFilePerm)
	}
	// 目录同样要收紧：私钥所在目录可列等于把文件名送出去
	if di, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	} else if di.Mode().Perm()&0o077 != 0 {
		t.Errorf("密钥目录权限 = %o，其他用户不该有任何位", di.Mode().Perm())
	}

	st, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.Signer(nil)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if !sameKey(got, key) {
		t.Error("读出的密钥与写入的不一致")
	}
	// 第二次读取走缓存
	if _, err := st.Signer(nil); err != nil {
		t.Errorf("缓存读取失败: %v", err)
	}
	if st.Kind() != "file" {
		t.Errorf("Kind = %q", st.Kind())
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 不是 PEM 的文件要报错，而不是把垃圾当成私钥往下传
	bad := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(bad, []byte("not a pem at all"), keyFilePerm); err != nil {
		t.Fatal(err)
	}
	st2, _ := NewFileStore(bad)
	if _, err := st2.Signer(t.Context()); err == nil {
		t.Error("非 PEM 应报错")
	}

	// 旧版口令加密的文件要被认出来（AUTH-092 的升级路径）。
	//
	// 不认它的话报的是"PEM 中没有可识别的私钥"——那句话指向文件坏了，
	// 而真因是本版本不再读这种文件，两者的处置完全不同。升级时每一个
	// 既有数据目录都会撞上这一条。
	legacy := filepath.Join(dir, "legacy.key")
	if err := os.WriteFile(legacy,
		append([]byte(legacyKeyFileMagic), 0x00, 0x09, 0x27, 0xc0), keyFilePerm); err != nil {
		t.Fatal(err)
	}
	st3, _ := NewFileStore(legacy)
	if _, err := st3.Signer(t.Context()); !errors.Is(err, ErrLegacyEncryptedKey) {
		t.Errorf("旧版加密文件应报 ErrLegacyEncryptedKey，得到 %v", err)
	}
}

// --- 辅助 ---

// ExtKeyUsageOf 便于测试读取扩展密钥用途。
func (i *Issued) ExtKeyUsageOf() []x509.ExtKeyUsage { return i.Cert.ExtKeyUsage }

func makeCSR(t *testing.T, key crypto.Signer, cn string, dns []string, ips []net.IP) []byte {
	t.Helper()
	tmpl := &x509.CertificateRequest{
		Subject:     pkix.Name{CommonName: cn, Organization: []string{"SelfDeclared"}},
		DNSNames:    dns,
		IPAddresses: ips,
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, tmpl, key)
	if err != nil {
		t.Fatalf("构造 CSR 失败: %v", err)
	}
	return EncodeCSRPEM(der)
}

func decodePEMBytes(data []byte, want string) ([]byte, error) {
	blk, err := singleBlock(data, want)
	if err != nil {
		return nil, err
	}
	return blk.Bytes, nil
}

func sameKey(a, b crypto.Signer) bool {
	ap, err := x509.MarshalPKIXPublicKey(a.Public())
	if err != nil {
		return false
	}
	bp, err := x509.MarshalPKIXPublicKey(b.Public())
	if err != nil {
		return false
	}
	return bytes.Equal(ap, bp)
}

func TestErrorMessagesMentionReason(t *testing.T) {
	// 错误信息要能让人直接知道怎么办，而不只是「失败了」
	ca, _, _ := newTestCA(t, DefaultPolicy())
	key, _ := GenerateKey(ECDSAP256)
	csrPEM := makeCSR(t, key, "x", []string{"a.example"}, nil)
	p := DefaultPolicy()
	p.SubjectSource = SubjectFromCSR
	ca2, _, _ := newTestCA(t, p)
	_, err := ca2.SignCSR(csrPEM, IssueRequest{})
	if err == nil || !strings.Contains(err.Error(), "DNS") {
		t.Errorf("SAN 拒绝的错误应指出是什么 SAN: %v", err)
	}
	_ = ca
}

// TestPrivateKeyPEMFormatIsSDKReadable 钉住私钥的 PEM 外层编码。
//
// 由来：原先一律发 PKCS#8，而 AWS IoT Device SDK（awscrt）在 Apple 平台上
// 读不了 PKCS#8 的 EC 私钥——报的是 AWS_IO_FILE_VALIDATION_FAILURE，
// 一条完全看不出是"密钥编码"的错误。签发侧没有任何报错，Go 这边的测试
// 也全绿，因为它们只检查"能不能被自己解析回来"。
//
// 所以这里断言的是外层 PEM 类型本身，不只是能不能回解析：
// 回解析永远会过（ParsePrivateKeyPEM 三种格式都收），钉不住这件事。
func TestPrivateKeyPEMFormatIsSDKReadable(t *testing.T) {
	for _, tc := range []struct {
		alg      KeyAlgorithm
		wantType string
	}{
		{ECDSAP256, "EC PRIVATE KEY"},
		{ECDSAP384, "EC PRIVATE KEY"},
		{RSA2048, "RSA PRIVATE KEY"},
		// Ed25519 没有传统格式，PKCS#8 是唯一选择
		{Ed25519, "PRIVATE KEY"},
	} {
		t.Run(tc.alg.String(), func(t *testing.T) {
			key, err := GenerateKey(tc.alg)
			if err != nil {
				t.Fatal(err)
			}
			data, err := EncodePrivateKeyPEM(key)
			if err != nil {
				t.Fatal(err)
			}
			blk, _ := pem.Decode(data)
			if blk == nil {
				t.Fatal("输出不是合法 PEM")
			}
			if blk.Type != tc.wantType {
				t.Errorf("PEM 类型 = %q，想要 %q", blk.Type, tc.wantType)
			}
			// 自家解析仍须接受
			if _, err := ParsePrivateKeyPEM(data); err != nil {
				t.Errorf("自己发出的密钥自己解析不了: %v", err)
			}
		})
	}
}

// TestParsePrivateKeyPEMStillAcceptsPKCS8 确认换格式不会让旧密钥失效。
//
// 已经发到设备上的 PKCS#8 私钥不能因为这次改动就读不了——
// 那会把一个兼容性修复变成一次破坏性变更。
func TestParsePrivateKeyPEMStillAcceptsPKCS8(t *testing.T) {
	key, err := GenerateKey(ECDSAP256)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	old := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if _, err := ParsePrivateKeyPEM(old); err != nil {
		t.Errorf("旧的 PKCS#8 私钥应当仍可解析: %v", err)
	}
}
