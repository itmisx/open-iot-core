package awsapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/registry"
)

const allowDoc = `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
const connectDoc = `{"Statement":[{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}]}`

func TestPolicyVersionsAndAttach(t *testing.T) {
	reg, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/policies/p1",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("创建 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/p1",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusConflict || decodeErr(t, raw).Type != "ResourceAlreadyExistsException" {
		t.Fatalf("重复创建 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/bad", `{"policyDocument":"{"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("坏文档 = %d %s，编译失败应是请求问题", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/tagged",
		`{"policyDocument":`+jsonString(allowDoc)+`,"tags":[{"Key":"a","Value":"b"}]}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("标签 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies/p1", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"defaultVersionId":"1"`) {
		t.Fatalf("读取 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/p1/version",
		`{"policyDocument":`+jsonString(connectDoc)+`}`)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"policyVersionId":"2"`) ||
		!strings.Contains(string(raw), `"isDefaultVersion":false`) {
		t.Fatalf("新版本 = %d %s", resp.StatusCode, raw)
	}
	p, err := reg.GetPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	if string(p.Document) != allowDoc || p.DefaultVersion != "1" {
		t.Fatalf("未设为默认时生效文档被换成了 %s 版本 %s", p.Document, p.DefaultVersion)
	}

	resp, raw = do(t, srv, http.MethodPatch, Prefix+"/policies/p1/version/2", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("切换默认 = %d %s", resp.StatusCode, raw)
	}
	p, err = reg.GetPolicy("p1")
	if err != nil || string(p.Document) != connectDoc || p.DefaultVersion != "2" {
		t.Fatalf("切换后 = %s %s %v", p.Document, p.DefaultVersion, err)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/policies/p1/version/2", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("删除默认版本 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/policies/p1/version/1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删除旧版本 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies?isAscendingOrder=false", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("降序 = %d %s", resp.StatusCode, raw)
	}

	if err := reg.PutThing(t.Context(), registry.Thing{Name: "lamp"}); err != nil {
		t.Fatal(err)
	}
	fp := strings.Repeat("ab", 32)
	if err := reg.RegisterCertificate(t.Context(), registry.Certificate{
		Fingerprint: fp, PEM: "CERT",
	}); err != nil {
		t.Fatal(err)
	}
	certARN := "arn:aws:iot:local:000000000000:cert/" + fp
	thingARN := "arn:aws:iot:local:000000000000:thing/lamp"
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/target-policies/p1",
		`{"target":"`+certARN+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("附加证书 = %d %s", resp.StatusCode, raw)
	}
	if err := reg.CreatePolicy(t.Context(), "p2", []byte(allowDoc)); err != nil {
		t.Fatal(err)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies?pageSize=1", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"nextMarker"`) {
		t.Fatalf("策略分页 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/target-policies/p2",
		`{"target":"`+certARN+`"}`)
	if resp.StatusCode != http.StatusGone || decodeErr(t, raw).Type != "LimitExceededException" {
		t.Fatalf("第二份策略 = %d %s", resp.StatusCode, raw)
	}
	c, err := reg.GetCertificate(fp)
	if err != nil || c.PolicyName != "p1" {
		t.Fatalf("证书策略 = %+v %v", c, err)
	}

	resp, raw = do(t, srv, http.MethodPut, Prefix+"/target-policies/p1",
		`{"target":"`+thingARN+`"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("附加设备 = %d %s", resp.StatusCode, raw)
	}
	th, err := reg.GetThing("lamp")
	if err != nil || th.PolicyName != "p1" || th.Version != 2 {
		t.Fatalf("设备 = %+v %v", th, err)
	}

	esc := url.PathEscape(certARN)
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/attached-policies/"+esc, "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"policyName":"p1"`) {
		t.Fatalf("已附加策略 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policy-targets/p1", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), certARN) ||
		!strings.Contains(string(raw), thingARN) {
		t.Fatalf("目标 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/attached-policies/"+esc+"?recursive=true", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("继承 = %d %s", resp.StatusCode, raw)
	}

	req, err := http.NewRequest(http.MethodPut, srv.URL+Prefix+"/things/lamp/principals", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amzn-principal", certARN)
	sign(t, req, nil)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := readResp(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("绑定 = %d %s", resp.StatusCode, body)
	}
	got, err := reg.ListPrincipalThings(fp)
	if err != nil || len(got) != 1 || got[0] != "lamp" {
		t.Fatalf("绑定结果 = %v %v", got, err)
	}

	req, err = http.NewRequest(http.MethodPut, srv.URL+Prefix+"/things/lamp/principals?thingPrincipalType=NON_EXCLUSIVE_THING", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amzn-principal", certARN)
	sign(t, req, nil)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body = readResp(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("非独占 = %d %s", resp.StatusCode, body)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things/lamp/principals", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), certARN) {
		t.Fatalf("设备的证书 = %d %s", resp.StatusCode, raw)
	}

	// 删掉版本 1 之后还剩默认版本 2。再加四个就到五个上限。
	for i := 0; i < 4; i++ {
		resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/p1/version",
			`{"policyDocument":`+jsonString(allowDoc)+`}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("追加版本 = %d %s", resp.StatusCode, raw)
		}
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/p1/version",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("第六个版本 = %d %s", resp.StatusCode, raw)
	}
}

func TestCreateKeysAndCertificate(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/keys-and-certificate", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("没有 CA = %d %s", resp.StatusCode, raw)
	}

	reg, srv := newHarnessIssuer(t, fakeIssuer{})
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/keys-and-certificate?setAsActive=false", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("签发 = %d %s", resp.StatusCode, raw)
	}
	var created struct {
		CertificateID  string `json:"certificateId"`
		CertificatePem string `json:"certificatePem"`
		KeyPair        struct {
			PrivateKey string `json:"PrivateKey"`
			PublicKey  string `json:"PublicKey"`
		} `json:"keyPair"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	if created.CertificateID != strings.Repeat("ab", 32) || created.CertificatePem != "CERTPEM" ||
		created.KeyPair.PrivateKey != "PRIVATEPEM" || created.KeyPair.PublicKey != "PUBLICPEM" {
		t.Fatalf("签发结果 = %+v", created)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/certificates/"+created.CertificateID, "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), "CERTPEM") ||
		strings.Contains(string(raw), "PRIVATEPEM") {
		t.Fatalf("查询 = %d %s", resp.StatusCode, raw)
	}
	c, err := reg.GetCertificate(created.CertificateID)
	if err != nil || c.Status != registry.StatusInactive || c.PolicyName != "" {
		t.Fatalf("登记 = %+v %v", c, err)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/keys-and-certificate", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("重复指纹 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/certificates/"+created.CertificateID+"?forceDelete=false", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("停用且未关联的证书删除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/keys-and-certificate", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("再次签发 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/certificates/"+created.CertificateID, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("活动证书删除 = %d %s", resp.StatusCode, raw)
	}
}

type fakeIssuer struct{}

func (fakeIssuer) IssueKeyPair(cn string) (IssuedCert, error) {
	return IssuedCert{
		Fingerprint: strings.Repeat("ab", 32),
		CertPEM:     []byte("CERTPEM"), KeyPEM: []byte("PRIVATEPEM"),
		PublicPEM: []byte("PUBLICPEM"), Subject: cn,
		NotAfterUnixMs: 1_800_000_000_000,
	}, nil
}

func (fakeIssuer) SignCSR(csr []byte, cn string) (IssuedCert, error) {
	if len(csr) == 0 {
		return IssuedCert{}, errors.New("empty csr")
	}
	return IssuedCert{
		Fingerprint: strings.Repeat("cd", 32),
		CertPEM:     []byte("CSRPEM"), Subject: cn,
	}, nil
}

func TestCreateCertificateFromCSR(t *testing.T) {
	_, srv := newHarnessIssuer(t, fakeIssuer{})
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/certificates",
		`{"certificateSigningRequest":"-----BEGIN CERTIFICATE REQUEST-----\nMII\n-----END CERTIFICATE REQUEST-----"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CSR = %d %s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), strings.Repeat("cd", 32)) ||
		!strings.Contains(string(raw), "CSRPEM") ||
		strings.Contains(string(raw), "PrivateKey") {
		t.Fatalf("CSR 响应 = %s", raw)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func readResp(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	raw := make([]byte, 0, 256)
	buf := make([]byte, 256)
	for {
		n, err := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if err != nil {
			return raw
		}
	}
}
