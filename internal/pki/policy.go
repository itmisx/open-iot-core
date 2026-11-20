package pki

import (
	"crypto/x509"
	"errors"
	"fmt"
	"time"
)

// SubjectSource 决定签发出的证书主题从哪里来。
type SubjectSource uint8

const (
	// SubjectFromRequest 使用调用方（控制面）给出的主题，忽略 CSR 中的主题。
	//
	// 这是默认且推荐的做法：CSR 由设备生成，其中的主题是设备自己声明的，
	// 而证书主题会被策略变量引用（${iot:Certificate.Subject.CommonName}）。
	// 采纳设备自报的主题等于让设备自己决定自己的权限。
	SubjectFromRequest SubjectSource = iota
	// SubjectFromCSR 采纳 CSR 中的主题。
	//
	// 仅当控制面已另行校验过该主题时才可用——例如可信用户代设备开通，
	// 主题由已认证的第三方指定。
	SubjectFromCSR
)

// Policy 约束签发行为（PRD PKI-012）。
//
// 它存在的理由很直接：CSR 可以请求任意主题、任意 SAN、任意扩展，
// CA 若盲从，申请者就自己决定了自己的身份与权限。
type Policy struct {
	// DefaultValidity 是未指定有效期时使用的时长。
	DefaultValidity time.Duration
	// MaxValidity 是允许的最长有效期。请求超出时按它截断而非报错——
	// 截断是安全的方向，报错只会让调用方去试探上限。
	MaxValidity time.Duration

	// AllowedAlgorithms 限制可接受的密钥算法。为空表示接受全部支持的算法。
	AllowedAlgorithms []KeyAlgorithm
	// DefaultAlgorithm 是生成密钥时的算法。
	DefaultAlgorithm KeyAlgorithm

	SubjectSource SubjectSource

	// AllowDNSSANs 与 AllowIPSANs 控制是否采纳 CSR 或请求中的 SAN。
	//
	// 默认都为 false：设备客户端证书不需要 SAN，而一个带任意 DNS SAN 的
	// 客户端证书在某些服务端配置下可以被当作服务端证书使用。
	AllowDNSSANs bool
	AllowIPSANs  bool
	// MaxSANs 是 SAN 条目总数上限。
	MaxSANs int

	// MaxCNLen 是通用名长度上限。
	MaxCNLen int
}

// DefaultPolicy 返回默认签发策略。
//
// 默认有效期取 90 天，显著短于 AWS 的默认值：短有效期把"必须有轮换流程"
// 从可选变成必须，而轮换缺失是 IoT 部署里最常见的长期隐患
// （PRD PKI-013、PKI-080）。
func DefaultPolicy() Policy {
	return Policy{
		DefaultValidity:  90 * 24 * time.Hour,
		MaxValidity:      397 * 24 * time.Hour,
		DefaultAlgorithm: ECDSAP256,
		AllowedAlgorithms: []KeyAlgorithm{
			ECDSAP256, ECDSAP384, RSA2048, RSA4096, Ed25519,
		},
		SubjectSource: SubjectFromRequest,
		AllowDNSSANs:  false,
		AllowIPSANs:   false,
		MaxSANs:       0,
		MaxCNLen:      128,
	}
}

// 签发策略相关的错误。
var (
	ErrAlgorithmNotAllowed = errors.New("pki: 策略不允许该密钥算法")
	ErrSANsNotAllowed      = errors.New("pki: 策略不允许 SAN")
	ErrCommonNameRequired  = errors.New("pki: 缺少通用名")
)

// validity 依策略裁剪请求的有效期。
func (p *Policy) validity(requested time.Duration) time.Duration {
	d := requested
	if d <= 0 {
		d = p.DefaultValidity
	}
	if p.MaxValidity > 0 && d > p.MaxValidity {
		d = p.MaxValidity
	}
	return d
}

// checkAlgorithm 校验密钥算法。
func (p *Policy) checkAlgorithm(alg KeyAlgorithm, known bool) error {
	if !known {
		return fmt.Errorf("%w: 无法识别的公钥", ErrAlgorithmNotAllowed)
	}
	if len(p.AllowedAlgorithms) == 0 {
		return nil
	}
	for _, a := range p.AllowedAlgorithms {
		if a == alg {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrAlgorithmNotAllowed, alg)
}

// checkCommonName 校验通用名。
func (p *Policy) checkCommonName(cn string) error {
	if cn == "" {
		return ErrCommonNameRequired
	}
	if p.MaxCNLen > 0 && len(cn) > p.MaxCNLen {
		return fmt.Errorf("pki: 通用名长度 %d 超出上限 %d", len(cn), p.MaxCNLen)
	}
	return nil
}

// applySANs 依策略筛选 SAN，返回最终写入证书的条目。
func (p *Policy) applySANs(req *IssueRequest, csr *x509.CertificateRequest) ([]string, []string, error) {
	dns := req.DNSNames
	ips := req.IPAddresses
	if csr != nil && p.SubjectSource == SubjectFromCSR {
		dns = append(dns, csr.DNSNames...)
		for _, ip := range csr.IPAddresses {
			ips = append(ips, ip.String())
		}
	}
	if len(dns) > 0 && !p.AllowDNSSANs {
		return nil, nil, fmt.Errorf("%w: 请求含 DNS 名", ErrSANsNotAllowed)
	}
	if len(ips) > 0 && !p.AllowIPSANs {
		return nil, nil, fmt.Errorf("%w: 请求含 IP 地址", ErrSANsNotAllowed)
	}
	if p.MaxSANs > 0 && len(dns)+len(ips) > p.MaxSANs {
		return nil, nil, fmt.Errorf("pki: SAN 条目数 %d 超出上限 %d",
			len(dns)+len(ips), p.MaxSANs)
	}
	return dns, ips, nil
}
