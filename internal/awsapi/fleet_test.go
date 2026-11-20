package awsapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

// TestRegisterCARejectsAutoRegistration 验证自动注册字段会被拒绝，而不是悄悄忽略。
//
// 这些字段来自已经删掉的开通路径。忽略它们会让调用方以为配置生效了。
func TestRegisterCARejectsAutoRegistration(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, "POST", "/api/aws/cacertificate",
		`{"caCertificate":"x","verificationCertificate":"y","allowAutoRegistration":true}`)
	if resp.StatusCode != 400 || !strings.Contains(string(raw), "不支持自动注册") {
		t.Fatalf("自动注册 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, "PUT", "/api/aws/cacertificate/nope",
		`{"newStatus":"ACTIVE","removeAutoRegistration":true}`)
	if resp.StatusCode != 400 || !strings.Contains(string(raw), "不支持自动注册") {
		t.Fatalf("更新自动注册 = %d %s", resp.StatusCode, raw)
	}
}

// TestSearchAndRoleAlias 验证检索默认关闭，以及角色别名不签发云凭证。
//
// 检索若在关闭时仍返回设备，列举和检索就变成两条会分叉的路径。
// 凭证接口若返回一组编出来的密钥，调用方会拿去当真实凭证用。
func TestSearchAndRoleAlias(t *testing.T) {
	_, srv := newHarnessFull(t, nil, func(h *Handler) {
		h.Shadow = func(thing string) (map[string]any, map[string]any, bool) {
			if thing == "lamp" {
				return map[string]any{"temp": float64(21)}, map[string]any{"temp": float64(20)}, true
			}
			return nil, nil, false
		}
		h.Online = func(thing string) bool { return thing == "lamp" }
	})

	resp, raw := do(t, srv, "POST", "/api/aws/indices/search", `{"queryString":"thingName:lamp"}`)
	if resp.StatusCode != 400 {
		t.Fatalf("关闭时检索 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, "PUT", "/api/aws/indexing/config",
		`{"thingIndexingConfiguration":{"thingIndexingMode":"REGISTRY_AND_SHADOW","thingConnectivityIndexingMode":"STATUS"}}`)
	if resp.StatusCode != 200 {
		t.Fatalf("打开检索 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, "POST", "/api/aws/things/lamp",
		`{"attributePayload":{"attributes":{"site":"a"}}}`)
	if resp.StatusCode != 200 {
		t.Fatalf("创建设备 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, "POST", "/api/aws/things/other", `{}`)
	if resp.StatusCode != 200 {
		t.Fatalf("创建设备 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, "POST", "/api/aws/indices/search",
		`{"queryString":"attributes.site:a shadow.reported.temp:21 connectivity.connected:true"}`)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), `"thingName":"lamp"`) || strings.Contains(string(raw), "other") {
		t.Fatalf("检索 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, "POST", "/api/aws/role-aliases/ops",
		`{"roleArn":"arn:aws:iam::000000000000:role/ops"}`)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), `"credentialDurationSeconds":3600`) {
		t.Fatalf("创建别名 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, "GET", "/api/aws/role-aliases/ops/credentials", "")
	errBody := decodeErr(t, raw)
	if resp.StatusCode != 503 || errBody.Type != "ServiceUnavailableException" {
		t.Fatalf("凭证 = %d %+v", resp.StatusCode, errBody)
	}
	if strings.Contains(string(raw), "SecretAccessKey") || strings.Contains(string(raw), "SessionToken") {
		t.Fatalf("拒绝里不该有凭证: %s", raw)
	}
}

// TestRegisterCAHTTP 验证兼容面能用注册码证明私钥并登记设备证书。
func TestRegisterCAHTTP(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, "GET", "/api/aws/registrationcode", "")
	if resp.StatusCode != 200 {
		t.Fatalf("注册码 = %d %s", resp.StatusCode, raw)
	}
	var code struct {
		RegistrationCode string `json:"registrationCode"`
	}
	if err := json.Unmarshal(raw, &code); err != nil || code.RegistrationCode == "" {
		t.Fatal(raw)
	}
	caPEM, caCert, caKey := issueCAHTTP(t, "factory")
	bad, _ := json.Marshal(map[string]string{
		"caCertificate": caPEM, "verificationCertificate": issueLeafHTTP(t, caCert, caKey, "nope"),
	})
	resp, raw = do(t, srv, "POST", "/api/aws/cacertificate", string(bad))
	if resp.StatusCode != 400 {
		t.Fatalf("错误的验证证书 = %d %s", resp.StatusCode, raw)
	}
	good, _ := json.Marshal(map[string]any{
		"caCertificate": caPEM, "verificationCertificate": issueLeafHTTP(t, caCert, caKey, code.RegistrationCode),
		"setAsActive": true,
	})
	resp, raw = do(t, srv, "POST", "/api/aws/cacertificate", string(good))
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "certificateId") {
		t.Fatalf("登记 CA = %d %s", resp.StatusCode, raw)
	}
	leaf := issueLeafHTTP(t, caCert, caKey, "device-1")
	body, _ := json.Marshal(map[string]string{"certificatePem": leaf, "caCertificatePem": caPEM})
	resp, raw = do(t, srv, "POST", "/api/aws/certificate/register", string(body))
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "certificateId") {
		t.Fatalf("注册设备证书 = %d %s", resp.StatusCode, raw)
	}
	solo := issueLeafHTTP(t, caCert, caKey, "solo")
	body, _ = json.Marshal(map[string]string{"certificatePem": solo})
	resp, raw = do(t, srv, "POST", "/api/aws/certificate/register-no-ca", string(body))
	if resp.StatusCode != 200 {
		t.Fatalf("无 CA 注册 = %d %s", resp.StatusCode, raw)
	}
}

func issueCAHTTP(t *testing.T, cn string) (string, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), cert, key
}

func issueLeafHTTP(t *testing.T, parent *x509.Certificate, key *ecdsa.PrivateKey, cn string) string {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
