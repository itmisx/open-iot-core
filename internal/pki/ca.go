package pki

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

// Fingerprint 是证书 DER 编码的 SHA-256 摘要，用作证书的主键。
//
// 本包不引用注册表的类型：pki 关心的是证书本身，而指纹到状态、到设备、
// 到策略的映射属于元数据层。两者都是 [32]byte，转换是零成本的。
type Fingerprint [32]byte

// String 返回十六进制表示。
func (f Fingerprint) String() string { return hex.EncodeToString(f[:]) }

// FingerprintOf 计算一张证书的指纹。
func FingerprintOf(cert *x509.Certificate) Fingerprint {
	return Fingerprint(sha256.Sum256(cert.Raw))
}

// Bundle 是一张证书及其私钥。
type Bundle struct {
	Cert *x509.Certificate
	Key  crypto.Signer
}

// CARequest 是创建 CA 证书的参数。
type CARequest struct {
	CommonName   string
	Organization string
	Country      string
	Validity     time.Duration
	Algorithm    KeyAlgorithm
	// MaxPathLen 限制其下还能有多少层中间 CA。
	// 签发 CA 应设为 0，即其下只能签发终端实体证书。
	MaxPathLen int
}

// 默认的 CA 有效期。
//
// 根 CA 取 10 年、签发 CA 取 2 年，是为了让签发 CA 的轮换成为常规操作
// 而不是十年一遇的事件——轮换路径只有被定期走过才是可靠的（PRD PKI-014）。
const (
	defaultRootValidity    = 10 * 365 * 24 * time.Hour
	defaultIssuingValidity = 2 * 365 * 24 * time.Hour
)

// serialNumber 生成一个唯一且不可预测的序列号。
//
// 用 128 位随机数而不是递增计数器：递增序列号会泄露签发量，
// 且在多个签发节点之间需要协调（CLU-029 下签发是可以有多个节点的）。
func serialNumber() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 127)
	n, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("pki: 生成序列号失败: %w", err)
	}
	// 序列号必须是正数（RFC 5280 4.1.2.2）。
	return n.Add(n, big.NewInt(1)), nil
}

// CreateRootCA 创建一个自签名的根 CA。
//
// 根 CA 的私钥应当离线保管：本函数把它交给调用方，由其决定存放方式
// （PRD PKI-014 的离线根加在线签发 CA 结构）。
func CreateRootCA(req CARequest, nowMs int64) (*Bundle, error) {
	if req.CommonName == "" {
		return nil, ErrCommonNameRequired
	}
	if req.Validity <= 0 {
		req.Validity = defaultRootValidity
	}
	key, err := GenerateKey(req.Algorithm)
	if err != nil {
		return nil, err
	}
	tmpl, err := caTemplate(req, nowMs)
	if err != nil {
		return nil, err
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("pki: 签发根证书失败: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析根证书失败: %w", err)
	}
	return &Bundle{Cert: cert, Key: key}, nil
}

// CreateIssuingCA 用根 CA 签发一个在线签发 CA。
func CreateIssuingCA(root *Bundle, req CARequest, nowMs int64) (*Bundle, error) {
	if root == nil || root.Cert == nil || root.Key == nil {
		return nil, errors.New("pki: 缺少根 CA")
	}
	if req.CommonName == "" {
		return nil, ErrCommonNameRequired
	}
	if req.Validity <= 0 {
		req.Validity = defaultIssuingValidity
	}
	key, err := GenerateKey(req.Algorithm)
	if err != nil {
		return nil, err
	}
	tmpl, err := caTemplate(req, nowMs)
	if err != nil {
		return nil, err
	}
	// 签发 CA 的有效期不得超出根 CA。
	if tmpl.NotAfter.After(root.Cert.NotAfter) {
		tmpl.NotAfter = root.Cert.NotAfter
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, root.Cert, key.Public(), root.Key)
	if err != nil {
		return nil, fmt.Errorf("pki: 签发中间证书失败: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析中间证书失败: %w", err)
	}
	return &Bundle{Cert: cert, Key: key}, nil
}

func caTemplate(req CARequest, nowMs int64) (*x509.Certificate, error) {
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	notBefore := time.UnixMilli(nowMs).Add(-clockSkewGrace)
	return &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   req.CommonName,
			Organization: nonEmpty(req.Organization),
			Country:      nonEmpty(req.Country),
		},
		NotBefore:             notBefore,
		NotAfter:              time.UnixMilli(nowMs).Add(req.Validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            req.MaxPathLen,
		MaxPathLenZero:        req.MaxPathLen == 0,
	}, nil
}

// clockSkewGrace 是 NotBefore 的回退量。
//
// 设备时钟不准是 IoT 的常态：刚签发的证书若 NotBefore 正好是当前时刻，
// 一个慢几秒的设备会判定它"尚未生效"而拒绝连接。
const clockSkewGrace = 5 * time.Minute

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// IssueRequest 是签发终端实体证书的参数。
type IssueRequest struct {
	// CommonName 是证书主题的通用名。它会被策略变量
	// ${iot:Certificate.Subject.CommonName} 引用。
	CommonName   string
	Organization string
	// Validity 为 0 时取策略默认值。
	Validity time.Duration
	// Algorithm 仅在生成密钥时有意义；签 CSR 时由 CSR 中的公钥决定。
	Algorithm KeyAlgorithm
	// DNSNames 与 IPAddresses 受策略约束，默认不允许。
	DNSNames    []string
	IPAddresses []string
}

// Issued 是一次签发的结果。
type Issued struct {
	Cert        *x509.Certificate
	CertPEM     []byte
	ChainPEM    []byte
	Fingerprint Fingerprint
	Algorithm   KeyAlgorithm
	// KeyPEM 仅在 IssueKeyPair 的结果中非空。
	//
	// 它只在这一次响应中出现，CA 不留存设备私钥（PRD PKI-010）：
	// 留存等于把全部设备的身份集中到一处，一次泄露即全网失陷。
	KeyPEM   []byte
	NotAfter time.Time
}

// CA 是一个在线签发 CA。
type CA struct {
	issuer *Bundle
	// chain 是签发 CA 到根的证书链（不含根自身的私钥）。
	chain    []*x509.Certificate
	chainPEM []byte
	policy   Policy
	// now 是注入的时钟（契约 08）。证书有效期只用它取绝对时间，
	// 不参与任何排序或版本比较。
	now func() int64
}

// New 创建一个签发 CA。
//
// chain 应为「签发 CA、…、根 CA」的顺序，用于下发给设备以校验服务端。
func New(issuer *Bundle, chain []*x509.Certificate, policy Policy, now func() int64) (*CA, error) {
	switch {
	case issuer == nil || issuer.Cert == nil:
		return nil, errors.New("pki: 缺少签发证书")
	case issuer.Key == nil:
		return nil, errors.New("pki: 缺少签发私钥")
	case !issuer.Cert.IsCA:
		return nil, errors.New("pki: 签发证书不是 CA 证书")
	case issuer.Cert.KeyUsage&x509.KeyUsageCertSign == 0:
		return nil, errors.New("pki: 签发证书缺少 certSign 用途")
	case now == nil:
		return nil, errors.New("pki: 缺少时钟（契约 08 要求注入）")
	}
	ca := &CA{issuer: issuer, chain: chain, policy: policy, now: now}
	for _, c := range chain {
		ca.chainPEM = append(ca.chainPEM, EncodeCertPEM(c.Raw)...)
	}
	return ca, nil
}

// Issuer 返回签发 CA 的证书。
func (ca *CA) Issuer() *x509.Certificate { return ca.issuer.Cert }

// ChainPEM 返回下发给设备的证书链。
func (ca *CA) ChainPEM() []byte { return ca.chainPEM }

// Policy 返回当前签发策略。
func (ca *CA) Policy() Policy { return ca.policy }

// IssueKeyPair 生成密钥对并签发证书，对应 AWS 的
// CreateKeysAndCertificate（PRD PKI-010）。
//
// 私钥只在返回值中出现一次，不被 CA 留存。
func (ca *CA) IssueKeyPair(req IssueRequest) (*Issued, error) {
	alg := req.Algorithm
	if alg == 0 && ca.policy.DefaultAlgorithm != 0 {
		alg = ca.policy.DefaultAlgorithm
	}
	if err := ca.policy.checkAlgorithm(alg, true); err != nil {
		return nil, err
	}
	key, err := GenerateKey(alg)
	if err != nil {
		return nil, err
	}
	out, err := ca.sign(&req, key.Public(), nil)
	if err != nil {
		return nil, err
	}
	keyPEM, err := EncodePrivateKeyPEM(key)
	if err != nil {
		return nil, err
	}
	out.KeyPEM = keyPEM
	return out, nil
}

// SignCSR 依据证书请求签发证书，对应 AWS 的
// CreateCertificateFromCsr（PRD PKI-011）。
//
// 这是推荐路径：设备自持私钥，私钥从不离开设备。
func (ca *CA) SignCSR(csrPEM []byte, req IssueRequest) (*Issued, error) {
	csr, err := ParseCSRPEM(csrPEM)
	if err != nil {
		return nil, err
	}
	alg, known := AlgorithmOf(csr.PublicKey)
	if err := ca.policy.checkAlgorithm(alg, known); err != nil {
		return nil, err
	}
	// 主题的来源由策略决定，而不是由 CSR 决定（PRD PKI-012）。
	if ca.policy.SubjectSource == SubjectFromCSR && req.CommonName == "" {
		req.CommonName = csr.Subject.CommonName
		if len(csr.Subject.Organization) > 0 {
			req.Organization = csr.Subject.Organization[0]
		}
	}
	return ca.sign(&req, csr.PublicKey, csr)
}

// sign 是两条签发路径共用的实现。
func (ca *CA) sign(req *IssueRequest, pub crypto.PublicKey,
	csr *x509.CertificateRequest,
) (*Issued, error) {
	if err := ca.policy.checkCommonName(req.CommonName); err != nil {
		return nil, err
	}
	dns, ipStrs, err := ca.policy.applySANs(req, csr)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(ipStrs))
	for _, s := range ipStrs {
		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("pki: 无法解析 IP 地址 %q", s)
		}
		ips = append(ips, ip)
	}

	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	nowMs := ca.now()
	notAfter := time.UnixMilli(nowMs).Add(ca.policy.validity(req.Validity))
	// 终端实体证书的有效期不得超出签发 CA。
	if notAfter.After(ca.issuer.Cert.NotAfter) {
		notAfter = ca.issuer.Cert.NotAfter
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   req.CommonName,
			Organization: nonEmpty(req.Organization),
		},
		NotBefore: time.UnixMilli(nowMs).Add(-clockSkewGrace),
		NotAfter:  notAfter,
		// 用途固定为客户端认证。
		//
		// 不采纳 CSR 中请求的用途：一个同时具备 serverAuth 的客户端证书
		// 在某些配置下可以冒充服务端，而设备证书没有任何理由需要它。
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              dns,
		IPAddresses:           ips,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.issuer.Cert, pub, ca.issuer.Key)
	if err != nil {
		return nil, fmt.Errorf("pki: 签发证书失败: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析签发出的证书失败: %w", err)
	}
	alg, _ := AlgorithmOf(pub)
	return &Issued{
		Cert:        cert,
		CertPEM:     EncodeCertPEM(der),
		ChainPEM:    ca.chainPEM,
		Fingerprint: FingerprintOf(cert),
		Algorithm:   alg,
		NotAfter:    notAfter,
	}, nil
}

// IssueServerCert 签发一张服务端证书。
//
// broker 自己的 TLS 证书需要 serverAuth 与主机名 SAN，与设备证书的
// 约束不同，因此走单独的入口而不是放宽设备签发策略。
func (ca *CA) IssueServerCert(cn string, dnsNames []string, ips []string,
	validity time.Duration,
) (*Issued, error) {
	if cn == "" {
		return nil, ErrCommonNameRequired
	}
	key, err := GenerateKey(ca.policy.DefaultAlgorithm)
	if err != nil {
		return nil, err
	}
	parsedIPs := make([]net.IP, 0, len(ips))
	for _, s := range ips {
		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("pki: 无法解析 IP 地址 %q", s)
		}
		parsedIPs = append(parsedIPs, ip)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	nowMs := ca.now()
	notAfter := time.UnixMilli(nowMs).Add(ca.policy.validity(validity))
	if notAfter.After(ca.issuer.Cert.NotAfter) {
		notAfter = ca.issuer.Cert.NotAfter
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.UnixMilli(nowMs).Add(-clockSkewGrace),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           parsedIPs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.issuer.Cert, key.Public(), ca.issuer.Key)
	if err != nil {
		return nil, fmt.Errorf("pki: 签发服务端证书失败: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("pki: 解析服务端证书失败: %w", err)
	}
	keyPEM, err := EncodePrivateKeyPEM(key)
	if err != nil {
		return nil, err
	}
	return &Issued{
		Cert:        cert,
		CertPEM:     EncodeCertPEM(der),
		ChainPEM:    ca.chainPEM,
		Fingerprint: FingerprintOf(cert),
		KeyPEM:      keyPEM,
		NotAfter:    notAfter,
	}, nil
}

// TrustPool 返回用于校验客户端证书的信任池。
func (ca *CA) TrustPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.issuer.Cert)
	for _, c := range ca.chain {
		pool.AddCert(c)
	}
	return pool
}
