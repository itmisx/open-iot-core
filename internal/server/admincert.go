package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/itmisx/open-iot-core/internal/pki"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/trace"
)

// 证书签发的 HTTP 入口（PRD PKI-010、PKI-011）。
//
// 两条路径，分成两个端点而不是一个端点按字段分支：
//
//   - sign-csr：设备自持私钥，私钥不经过网络。这是 PKI-011 的推荐路径。
//   - issue：服务端生成密钥对，私钥在响应里出现**一次**。
//
// 为什么不合成一个端点：两者的安全性质完全不同，而"少传一个字段就
// 从不传私钥变成传私钥"是最容易出事的那种隐式行为。路径里写明做的
// 是哪一件事，调用方不会搞错。
//
// 签发之后立即生效，不需要重启：走 registry.PutCertificate，
// 它先写热视图再写冷层，与吊销（PKI-030）是对称的两个方向。

// issueCertRequest 是两条签发路径共用的入参。
type issueCertRequest struct {
	// Thing 是绑定的设备名。
	Thing string `json:"thing"`
	// PolicyName 是绑定的策略。
	PolicyName string `json:"policyName"`
	// CommonName 为空时取 Thing。
	CommonName string `json:"commonName,omitempty"`
	// ValiditySeconds 为 0 时取签发策略的默认值。
	ValiditySeconds int64 `json:"validitySeconds,omitempty"`
	// CSR 仅 sign-csr 端点使用。
	CSR string `json:"csr,omitempty"`
}

// issueCertResponse 是签发结果。
type issueCertResponse struct {
	Fingerprint    string `json:"fingerprint"`
	CertificatePem string `json:"certificatePem"`
	ChainPem       string `json:"chainPem"`
	NotAfter       int64  `json:"notAfter"`
	Thing          string `json:"thing"`
	PolicyName     string `json:"policyName"`
	// PrivateKeyPem 只在 issue 端点的响应里出现，且只出现这一次。
	//
	// CA 不留存设备私钥（PKI-010）：留存等于把全部设备的身份集中到
	// 一处，一次泄露即全网失陷。所以这个字段的内容在本次响应之后
	// 任何地方都取不到，调用方必须当场保存。
	PrivateKeyPem string `json:"privateKeyPem,omitempty"`
	// Warning 在私钥随响应返回时给出，提醒它不可再次获取。
	Warning string `json:"warning,omitempty"`
}

// handleIssueCert 生成密钥对并签发证书。
func (a *adminServer) handleIssueCert(w http.ResponseWriter, r *http.Request) {
	var in issueCertRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.CSR != "" {
		writeAPIError(w, http.StatusBadRequest,
			"本端点不接受 CSR",
			"要用自持私钥的 CSR 签发，请改用 POST /certificates/sign-csr")
		return
	}
	// 私钥要经明文 HTTP 回传，所以先确认这条链路不出本机。
	if !a.plaintextKeyExportAllowed() {
		writeAPIError(w, http.StatusForbidden,
			"管理端点绑在非回环地址，拒绝经明文 HTTP 返回私钥",
			"改用 POST /certificates/sign-csr（设备自持私钥，私钥不经网络）；"+
				"或在前置 TLS 终止的部署下设 listeners.admin.acknowledge_plaintext_key_export")
		return
	}
	issued, ok := a.signWith(r.Context(), w, &in,
		func(req pki.IssueRequest) (*pki.Issued, error) {
			return a.s.ca.IssueKeyPair(req)
		})
	if !ok {
		return
	}
	resp := issueCertResponse{
		Fingerprint:    issued.Fingerprint.String(),
		CertificatePem: string(issued.CertPEM),
		ChainPem:       string(issued.ChainPEM),
		NotAfter:       issued.NotAfter.UnixMilli(),
		Thing:          in.Thing,
		PolicyName:     in.PolicyName,
		PrivateKeyPem:  string(issued.KeyPEM),
		Warning:        "私钥只在本次响应中出现，服务端不留存，请立即保存",
	}
	// 审计记指纹，不记私钥也不记证书内容（NFR-041、OBS-054）。
	a.audit(r, "IssueCertificate", resp.Fingerprint+" thing="+in.Thing)
	writeJSON(w, http.StatusCreated, resp)
}

// handleSignCSR 用设备提交的 CSR 签发证书。
//
// 这是推荐路径（PKI-011）：私钥从不离开设备，因此没有传输与留存的
// 问题，也不受 plaintextKeyExportAllowed 的限制。
func (a *adminServer) handleSignCSR(w http.ResponseWriter, r *http.Request) {
	var in issueCertRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.CSR == "" {
		writeAPIError(w, http.StatusBadRequest, "须给出 csr",
			"PEM 格式的证书签名请求；要让服务端生成密钥请改用 POST /certificates/issue")
		return
	}
	issued, ok := a.signWith(r.Context(), w, &in,
		func(req pki.IssueRequest) (*pki.Issued, error) {
			// CSR 自带的字段不被直接采纳，主题与 SAN 由签发策略
			// 决定（PKI-012）。该保证在 pki.SignCSR 里，此处只传请求。
			return a.s.ca.SignCSR([]byte(in.CSR), req)
		})
	if !ok {
		return
	}
	resp := issueCertResponse{
		Fingerprint:    issued.Fingerprint.String(),
		CertificatePem: string(issued.CertPEM),
		ChainPem:       string(issued.ChainPEM),
		NotAfter:       issued.NotAfter.UnixMilli(),
		Thing:          in.Thing,
		PolicyName:     in.PolicyName,
	}
	a.audit(r, "SignCSR", resp.Fingerprint+" thing="+in.Thing)
	writeJSON(w, http.StatusCreated, resp)
}

// signWith 是两条路径共用的校验、签发与登记。
//
// 返回 ok 为 false 时响应已写出，调用方直接返回。
func (a *adminServer) signWith(ctx context.Context, w http.ResponseWriter,
	in *issueCertRequest, sign func(pki.IssueRequest) (*pki.Issued, error),
) (*pki.Issued, bool) {
	if a.s.ca == nil {
		writeAPIError(w, http.StatusServiceUnavailable,
			"未启用内嵌 PKI，无法签发",
			"在配置里启用 pki，或改用外部 CA 并经 PUT /certificates/{fp} 登记证书")
		return nil, false
	}
	if in.Thing == "" {
		writeAPIError(w, http.StatusBadRequest, "须给出 thing", "绑定的设备名")
		return nil, false
	}
	if in.PolicyName == "" {
		// 未绑定策略的证书连不上。在签发时拒绝，使错误落在真正的
		// 原因上，而不是等设备第一次连接时以一条含义模糊的拒绝出现。
		writeAPIError(w, http.StatusBadRequest, "须给出 policyName",
			"未绑定策略的证书无法建立连接")
		return nil, false
	}

	cn := in.CommonName
	if cn == "" {
		cn = in.Thing
	}
	req := pki.IssueRequest{CommonName: cn}
	if in.ValiditySeconds > 0 {
		req.Validity = time.Duration(in.ValiditySeconds) * time.Second
	}

	ctx, sp := trace.Start(ctx, "pki.issue")
	defer sp.End()
	sp.Set("thing", in.Thing)
	issued, err := sign(req)
	if err != nil {
		// 签发失败多半是策略约束不满足（CN、SAN、算法），属于入参问题。
		sp.SetError()
		writeAPIError(w, http.StatusBadRequest, "签发失败: "+err.Error(),
			issueFailureHint(err))
		return nil, false
	}
	sp.Set("cert.fingerprint", issued.Fingerprint.String())

	// 登记走热视图，签发后立即可用，不需要重启节点。
	//
	// 登记失败时不把证书返回给调用方：一张签出来但没登记的证书
	// 连不上，而调用方会以为它能用，于是去排查设备侧——真正的
	// 问题却在服务端。宁可让这次签发整体失败。
	cert := registry.Certificate{
		Fingerprint:    issued.Fingerprint.String(),
		Status:         registry.StatusActive,
		Thing:          in.Thing,
		PolicyName:     in.PolicyName,
		Subject:        cn,
		Issuer:         issued.Cert.Issuer.String(),
		NotAfterUnixMs: issued.NotAfter.UnixMilli(),
		PEM:            string(issued.CertPEM),
	}
	if err := a.s.reg2.PutCertificate(ctx, cert); err != nil {
		sp.SetError()
		writeRegistryError(w, err)
		return nil, false
	}
	return issued, true
}

// issueFailureHint 给出针对性的修复建议。
//
// 默认提示指向签发策略，但有一类失败与策略无关却很容易被它误导：
// EC 密钥的 CSR 若以"显式参数"编码椭圆曲线，Go 的 x509 解析不了，
// 报出来的是 "invalid ECDSA parameters"。而最自然的 openssl 命令
// 恰恰可能产出这种编码。把提示指向策略会让人去翻一份完全无关的
// 配置——这类误导比没有提示更费时间。
func issueFailureHint(err error) string {
	if strings.Contains(err.Error(), "invalid ECDSA parameters") {
		return "EC 的曲线参数须以 named_curve 编码。重新生成 CSR：" +
			"openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 " +
			"-pkeyopt ec_param_enc:named_curve -nodes -keyout dev.key -out dev.csr " +
			"-subj /CN=<设备名>"
	}
	return "签发策略约束了 CN、SAN、有效期与密钥算法，见 pki.issue_policy"
}

// plaintextKeyExportAllowed 报告是否允许经本端点回传私钥。
//
// 管理监听器是明文 HTTP（没有 TLS 选项）。绑在回环地址时，私钥不出
// 本机，与 CLI 把它写到本地文件没有区别；绑在非回环地址时，私钥会
// 以明文穿过网络，而令牌保护的是访问权限，不是传输机密性。
//
// 这与 acknowledge_public_exposure 是两件事，所以用两道闸：前者确认
// "愿意把管理面暴露出去"，这里确认"该链路的传输机密性由外部保证"
// （典型是前置一个 TLS 终止的反向代理）。把两者合并会让一个只想开
// 管理面的人顺带获得明文导出私钥的能力。
func (a *adminServer) plaintextKeyExportAllowed() bool {
	if a.s.cfg.Listeners.Admin.AcknowledgePlaintextKeyExport {
		return true
	}
	return isLoopbackAddr(a.s.cfg.Listeners.Admin.Addr)
}

// isLoopbackAddr 判断监听地址是否只接受本机连接。
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	switch host {
	case "localhost":
		return true
	case "", "0.0.0.0", "::", "[::]":
		// 空 host 表示监听全部地址，不是回环。
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// ---------- 证书与设备的关联（PRD REG-060 至 REG-065） ----------

// handleAttachPrincipal 把证书绑定到设备。
func (a *adminServer) handleAttachPrincipal(w http.ResponseWriter, r *http.Request) {
	thing, fp := r.PathValue("name"), r.PathValue("fp")
	if err := a.s.reg2.AttachThingPrincipal(r.Context(), thing, fp); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "AttachThingPrincipal", fp+" -> "+thing)
	w.WriteHeader(http.StatusNoContent)
}

// handleDetachPrincipal 解除证书与设备的绑定。
func (a *adminServer) handleDetachPrincipal(w http.ResponseWriter, r *http.Request) {
	thing, fp := r.PathValue("name"), r.PathValue("fp")
	if err := a.s.reg2.DetachThingPrincipal(r.Context(), thing, fp); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "DetachThingPrincipal", fp+" <- "+thing)
	w.WriteHeader(http.StatusNoContent)
}

// handleListThingPrincipals 列出绑定某设备的证书。
func (a *adminServer) handleListThingPrincipals(w http.ResponseWriter, r *http.Request) {
	fps, err := a.s.reg2.ListThingPrincipals(r.PathValue("name"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"principals": nonNil(fps)})
}

// handleListPrincipalThings 列出某证书绑定的设备。
func (a *adminServer) handleListPrincipalThings(w http.ResponseWriter, r *http.Request) {
	things, err := a.s.reg2.ListPrincipalThings(r.PathValue("fp"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"things": nonNil(things)})
}

// nonNil 把 nil 切片换成空切片。
//
// 否则 JSON 里出现的是 null 而不是 []，而客户端对二者的处理往往不同
// ——一个 .map() 在 null 上会抛错，在 [] 上不会。
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
