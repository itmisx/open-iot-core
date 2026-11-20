package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// certRig 建好策略与设备，使签发请求只差证书本身。
func certRig(t *testing.T, mut func(*config.Config)) *testRig {
	t.Helper()
	r := newAdminRig(t, mut)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device-scoped.json"),
		[]byte(`{"Statement":[{"Effect":"Allow","Action":["iot:Connect"],`+
			`"Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	r.start()
	code, body := r.adminReq(t, http.MethodPut, "/api/admin/v1/things/dev-x",
		map[string]any{"policyName": "device-scoped"})
	if code != http.StatusOK && code != http.StatusNoContent && code != http.StatusCreated {
		t.Fatalf("建设备失败 %d: %s", code, body)
	}
	return r
}

func issueResp(t *testing.T, body []byte) issueCertResponse {
	t.Helper()
	var out issueCertResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("解析响应失败: %v\n%s", err, body)
	}
	return out
}

// 签发出的证书须立即可用，不需要重启节点。
//
// 这是 PKI-030（吊销即时生效）的对称方向：登记走热视图，所以签发
// 与吊销都不需要重启。
func TestIssueCertificateUsableImmediately(t *testing.T) {
	r := certRig(t, nil)
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"})
	if code != http.StatusCreated {
		t.Fatalf("签发失败 %d: %s", code, body)
	}
	out := issueResp(t, body)
	if out.PrivateKeyPem == "" {
		t.Fatal("issue 端点必须返回私钥")
	}
	if out.Warning == "" {
		t.Error("返回私钥时必须给出不可再次获取的提示")
	}

	cert := tlsPairFromPEM(t, out.CertificatePem, out.PrivateKeyPem, out.ChainPem)
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("用新签发的证书握手失败: %v", err)
	}
	if ack := c.connect("dev-x"); ack.Code != reason.Success {
		t.Fatalf("新签发的证书连接被拒 %s —— 签发应即时生效，不需重启", ack.Code)
	}
}

// CSR 路径不得返回私钥。
//
// 这是 PKI-011 的推荐路径：私钥从不离开设备。若响应里出现私钥字段，
// 说明服务端自己生成了密钥，整条路径的意义就没了。
func TestSignCSRNeverReturnsPrivateKey(t *testing.T) {
	r := certRig(t, nil)
	csrPEM, key := makeCSR(t, "dev-x")

	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/sign-csr",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped", "csr": csrPEM})
	if code != http.StatusCreated {
		t.Fatalf("CSR 签发失败 %d: %s", code, body)
	}
	out := issueResp(t, body)
	if out.PrivateKeyPem != "" {
		t.Error("CSR 路径不得返回私钥")
	}
	if out.Warning != "" {
		t.Error("CSR 路径没有一次性私钥，不该给出该提示")
	}

	// 用设备自己的私钥 + 签回来的证书连接
	cert := tlsPairFromPEM(t, out.CertificatePem, key, out.ChainPem)
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-x"); ack.Code != reason.Success {
		t.Fatalf("CSR 签出的证书连接被拒: %s", ack.Code)
	}
}

// 两个端点不得互相替代。
//
// 合成一个端点按字段分支的话，"少传一个字段"就会从不传私钥变成
// 传私钥，这是最容易出事的隐式行为。
func TestIssueAndSignCSREndpointsAreDistinct(t *testing.T) {
	r := certRig(t, nil)
	csrPEM, _ := makeCSR(t, "dev-x")

	// 往 issue 塞 CSR：必须拒绝并指向正确的端点
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped", "csr": csrPEM})
	if code != http.StatusBadRequest {
		t.Errorf("issue 收到 CSR 应拒绝，得到 %d: %s", code, body)
	}
	if !strings.Contains(string(body), "sign-csr") {
		t.Errorf("错误信息应指向 sign-csr 端点: %s", body)
	}

	// sign-csr 不带 CSR：必须拒绝并指向正确的端点
	code, body = r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/sign-csr",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"})
	if code != http.StatusBadRequest {
		t.Errorf("sign-csr 缺 CSR 应拒绝，得到 %d", code)
	}
	if !strings.Contains(string(body), "issue") {
		t.Errorf("错误信息应指向 issue 端点: %s", body)
	}
}

// 未绑定策略的证书签不出来。
//
// 允许签发一张没有策略的证书，等于签发一张注定连不上的证书，
// 而失败要等到设备第一次连接时才暴露。
func TestIssueRequiresPolicyAndThing(t *testing.T) {
	r := certRig(t, nil)
	for _, c := range []struct {
		name string
		body map[string]any
	}{
		{"缺策略", map[string]any{"thing": "dev-x"}},
		{"缺设备", map[string]any{"policyName": "device-scoped"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, body := r.adminReq(t, http.MethodPost,
				"/api/admin/v1/certificates/issue", c.body)
			if code != http.StatusBadRequest {
				t.Errorf("%s 应被拒，得到 %d: %s", c.name, code, body)
			}
		})
	}
	// 不存在的策略同样要在签发时就失败
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "no-such-policy"})
	if code == http.StatusCreated {
		t.Errorf("绑定不存在的策略不应签发成功: %s", body)
	}
}

// 非回环绑定时拒绝经明文 HTTP 返回私钥。
//
// 管理监听器没有 TLS 选项。令牌保护的是访问权限，不是传输机密性，
// 所以私钥不能就这么穿过网络。CSR 路径不受影响——它本来就不传私钥。
func TestIssueRefusesPlaintextKeyExportOffLoopback(t *testing.T) {
	r := certRig(t, func(c *config.Config) {
		c.Listeners.Admin.Addr = "0.0.0.0:0"
		c.Listeners.Admin.AcknowledgePublicExposure = true
	})
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"})
	if code != http.StatusForbidden {
		t.Fatalf("非回环绑定应拒绝返回私钥，得到 %d: %s", code, body)
	}
	// 必须给出可行的替代方案，而不是只说"不行"
	if !strings.Contains(string(body), "sign-csr") {
		t.Errorf("拒绝时应指向 CSR 路径: %s", body)
	}

	// 同样的部署下 CSR 路径必须仍然可用
	csrPEM, _ := makeCSR(t, "dev-x")
	code, body = r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/sign-csr",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped", "csr": csrPEM})
	if code != http.StatusCreated {
		t.Errorf("CSR 路径不传私钥，不应被这道闸拦住，得到 %d: %s", code, body)
	}
}

// 显式确认之后允许明文导出。
func TestIssueAllowsPlaintextKeyExportWhenAcknowledged(t *testing.T) {
	r := certRig(t, func(c *config.Config) {
		c.Listeners.Admin.Addr = "0.0.0.0:0"
		c.Listeners.Admin.AcknowledgePublicExposure = true
		c.Listeners.Admin.AcknowledgePlaintextKeyExport = true
	})
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"})
	if code != http.StatusCreated {
		t.Fatalf("已确认后应允许，得到 %d: %s", code, body)
	}
	if issueResp(t, body).PrivateKeyPem == "" {
		t.Error("应返回私钥")
	}
}

// 签发必须要令牌。
func TestIssueRequiresToken(t *testing.T) {
	r := certRig(t, nil)
	code, _ := r.adminReqToken(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"}, "")
	if code != http.StatusUnauthorized {
		t.Errorf("无令牌签发 = %d，想要 401", code)
	}
}

// 审计与日志不得出现私钥（NFR-041、OBS-054）。
func TestIssueDoesNotLogPrivateKey(t *testing.T) {
	var buf safeBuffer
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device-scoped.json"),
		[]byte(`{"Statement":[{"Effect":"Allow","Action":["iot:Connect"],`+
			`"Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	r.logTo(&buf)
	r.start()
	if code, body := r.adminReq(t, http.MethodPut, "/api/admin/v1/things/dev-x",
		map[string]any{"policyName": "device-scoped"}); code >= 300 {
		t.Fatalf("建设备失败 %d: %s", code, body)
	}
	code, body := r.adminReq(t, http.MethodPost, "/api/admin/v1/certificates/issue",
		map[string]any{"thing": "dev-x", "policyName": "device-scoped"})
	if code != http.StatusCreated {
		t.Fatalf("签发失败 %d: %s", code, body)
	}
	out := issueResp(t, body)

	logs := buf.String()
	// 指纹在审计文件里。运行日志不再承担这件事，所以这里要单独去读那份文件。
	audit := waitAudit(t, filepath.Join(r.dd.Root(), auditLogName), out.Fingerprint)
	for _, text := range []string{logs, audit} {
		if strings.Contains(text, "PRIVATE KEY") {
			t.Error("日志或审计里出现了私钥 PEM 头")
		}
		// 取私钥正文的一段来找，PEM 头之外的内容同样不能出现
		if lines := strings.Split(strings.TrimSpace(out.PrivateKeyPem), "\n"); len(lines) > 2 {
			if strings.Contains(text, lines[1]) {
				t.Error("日志或审计里出现了私钥正文")
			}
		}
	}
}

// EC 的显式参数编码须给出针对性提示。
//
// 这类失败与签发策略无关，而默认提示指向策略，会让人去翻一份完全
// 无关的配置——这种误导比没有提示更费时间。
func TestExplicitECParamsHintIsSpecific(t *testing.T) {
	hint := issueFailureHint(errTest("pki: 解析证书请求失败: x509: invalid ECDSA parameters"))
	if !strings.Contains(hint, "named_curve") {
		t.Errorf("提示应指出曲线编码问题，得到: %s", hint)
	}
	if strings.Contains(hint, "issue_policy") {
		t.Errorf("不应把这类失败指向签发策略: %s", hint)
	}
	// 其他失败仍走默认提示
	if h := issueFailureHint(errTest("CN 不被允许")); !strings.Contains(h, "issue_policy") {
		t.Errorf("默认提示应指向签发策略，得到: %s", h)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }

// makeCSR 生成一个 EC 的 CSR，返回 PEM 与对应私钥 PEM。
//
// 用 Go 生成而不是调 openssl：曲线参数的编码由 Go 保证是 named_curve，
// 测试因此不依赖本机 openssl 的默认行为。
func makeCSR(t *testing.T, cn string) (csrPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: cn}}, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder}))
}

// tlsPairFromPEM 组出客户端证书。
//
// 必须拼上链（PKI-103）：只给叶子证书会让服务端报 "unknown authority"，
// 而真实原因是链不完整，不是证书有问题。
func tlsPairFromPEM(t *testing.T, certPEM, keyPEM, chainPEM string) tls.Certificate {
	t.Helper()
	cert, err := tls.X509KeyPair([]byte(certPEM+chainPEM), []byte(keyPEM))
	if err != nil {
		t.Fatalf("组装客户端证书失败: %v", err)
	}
	return cert
}
