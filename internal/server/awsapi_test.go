package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/pki"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// TestAWSAPIDisabledIs404 验证兼容面默认不挂载。
//
// 挂上了就会在没有密钥的情况下露出一组路径。关掉开关后这些路径必须是 404。
func TestAWSAPIDisabledIs404(t *testing.T) {
	r := newTestRig(t, nil)
	r.start()
	code, _ := r.adminReqToken(t, "GET", awsapi.Prefix+"/endpoint", nil, "")
	if code != http.StatusNotFound {
		t.Fatalf("未启用时 = %d，期望 404", code)
	}
}

// TestAWSAPIIgnoresAdminToken 验证兼容面不依赖管理令牌。
//
// 管理令牌为空时 mount 会提前返回。兼容面如果挂在那次返回之后，
// 这里会得到 404 而不是 401。签名正确时端点地址来自配置，
// 密钥没装上则会停在 401。
func TestAWSAPIIgnoresAdminToken(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	cred := r.newAWSKey("admin")

	code, _ := r.adminReqToken(t, "GET", adminAPIPrefix+"/node", nil, "")
	if code != http.StatusNotFound {
		t.Fatalf("管理面 = %d，没有令牌时不应挂载", code)
	}

	code, raw := r.adminReqToken(t, "GET", awsapi.Prefix+"/endpoint", nil, "")
	if code != http.StatusUnauthorized {
		t.Fatalf("未签名 = %d %s，期望 401", code, raw)
	}

	req, err := http.NewRequest(http.MethodGet,
		"http://"+r.s.AdminAddr()+awsapi.Prefix+"/endpoint", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	signControlPlane(t, req, cred)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("已签名 = %d", resp.StatusCode)
	}
	var ep struct {
		EndpointAddress string `json:"endpointAddress"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		t.Fatal(err)
	}
	if ep.EndpointAddress != "127.0.0.1:8883" {
		t.Fatalf("端点 = %q", ep.EndpointAddress)
	}
}

// TestAWSCreateKeysUsesCA 验证兼容面的签发走本节点的 CA。
//
// 签发器没挂上时，这个请求是 503，私钥也不会出现在响应里。
func TestAWSCreateKeysUsesCA(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	cred := r.newAWSKey("admin")

	req, err := http.NewRequest(http.MethodPost,
		"http://"+r.s.AdminAddr()+awsapi.Prefix+"/keys-and-certificate", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	signControlPlane(t, req, cred)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("签发 = %d %s", resp.StatusCode, raw)
	}
	var created struct {
		CertificateID  string `json:"certificateId"`
		CertificatePem string `json:"certificatePem"`
		KeyPair        struct {
			PrivateKey string `json:"PrivateKey"`
		} `json:"keyPair"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	cert, err := pki.ParseCertPEM([]byte(created.CertificatePem))
	if err != nil {
		t.Fatal(err)
	}
	if pki.FingerprintOf(cert).String() != created.CertificateID {
		t.Fatalf("证书编号 = %s，指纹 = %s", created.CertificateID, pki.FingerprintOf(cert))
	}
	if !strings.Contains(created.KeyPair.PrivateKey, "PRIVATE KEY") {
		t.Fatal("签发响应应带回私钥")
	}
	got, err := r.s.ControlPlane().GetCertificate(created.CertificateID)
	if err != nil {
		t.Fatal(err)
	}
	if got.PEM == "" || strings.Contains(got.PEM, "PRIVATE KEY") {
		t.Fatalf("登记的证书正文 = %q", got.PEM)
	}
}

// TestAWSDataPlaneRoundTrip 验证数据面接到了本节点的 broker 和影子。
//
// 只把路由挂上、却去读另一份保留表，发布成功后这里读不到正文。
// 影子没接上时更新会是 503。签成 iot 必须被拒，否则数据面和控制面的服务名就混了。
func TestAWSDataPlaneRoundTrip(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	cred := r.newAWSKey("admin")
	base := "http://" + r.s.AdminAddr() + awsapi.Prefix
	call := func(method, path string, payload []byte, service string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, base+path, strings.NewReader(string(payload)))
		if err != nil {
			t.Fatal(err)
		}
		signAWS(t, req, payload, service, cred)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, raw
	}

	code, raw := call(http.MethodPost, "/topics/sensors/temp?qos=1&retain=true", []byte("hello"), sigv4.ServiceIoTData)
	if code != http.StatusOK || len(raw) != 0 {
		t.Fatalf("发布 = %d %q", code, raw)
	}
	code, raw = call(http.MethodPost, "/topics/sensors/temp?qos=1&retain=true", []byte("hello"), sigv4.ServiceIoT)
	if code != http.StatusUnauthorized {
		t.Fatalf("签成 iot = %d %s", code, raw)
	}

	code, raw = call(http.MethodGet, "/retainedMessage/sensors/temp", nil, sigv4.ServiceIoTData)
	if code != http.StatusOK {
		t.Fatalf("读取保留 = %d %s", code, raw)
	}
	var kept struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
		QoS     int    `json:"qos"`
		When    int64  `json:"lastModifiedTime"`
	}
	if err := json.Unmarshal(raw, &kept); err != nil {
		t.Fatal(err)
	}
	plain, err := base64.StdEncoding.DecodeString(kept.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if kept.Topic != "sensors/temp" || string(plain) != "hello" || kept.QoS != 1 || kept.When <= 0 {
		t.Fatalf("保留消息 = %+v 正文 %q", kept, plain)
	}

	code, raw = call(http.MethodGet, "/retainedMessage", nil, sigv4.ServiceIoTData)
	if code != http.StatusOK || !strings.Contains(string(raw), "sensors/temp") {
		t.Fatalf("列举 = %d %s", code, raw)
	}

	code, raw = call(http.MethodPost, "/topics/a/+/b", []byte("x"), sigv4.ServiceIoTData)
	if code != http.StatusBadRequest {
		t.Fatalf("通配主题 = %d %s", code, raw)
	}

	// 同一主题的保留发布每秒只有一次。立刻再写会被限流，清不掉刚才那条。
	time.Sleep(1100 * time.Millisecond)
	code, raw = call(http.MethodPost, "/topics/sensors/temp?retain=true", nil, sigv4.ServiceIoTData)
	if code != http.StatusOK {
		t.Fatalf("清除 = %d %s", code, raw)
	}
	code, raw = call(http.MethodGet, "/retainedMessage/sensors/temp", nil, sigv4.ServiceIoTData)
	if code != http.StatusNotFound {
		t.Fatalf("清除后 = %d %s", code, raw)
	}

	shadowBody := []byte(`{"state":{"desired":{"a":1}}}`)
	code, raw = call(http.MethodPost, "/things/lamp/shadow", shadowBody, sigv4.ServiceIoTData)
	if code != http.StatusOK || !strings.Contains(string(raw), `"desired"`) {
		t.Fatalf("更新影子 = %d %s", code, raw)
	}
	code, raw = call(http.MethodGet, "/things/lamp/shadow", nil, sigv4.ServiceIoTData)
	if code != http.StatusOK || !strings.Contains(string(raw), `"a":1`) && !strings.Contains(string(raw), `"a": 1`) {
		t.Fatalf("读取影子 = %d %s", code, raw)
	}
}

// TestAWSAPIEnforcesKeyRole 验证密钥文件里的角色真的接到了兼容面上
// （AUTH-081）。
//
// awsapi 包自己的用例装的是它自己的 Role 函数，证明不了 server 这边
// 把线接上了：把 `Role: a.s.awsRole` 那一行删掉，那些用例照样全绿，
// 而真实节点会退回"验过签就放行"。所以这条必须在真节点上跑。
func TestAWSAPIEnforcesKeyRole(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	cred := r.newAWSKey("reader")

	call := func(method, path string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, "http://"+r.s.AdminAddr()+awsapi.Prefix+path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		signControlPlane(t, req, cred)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}

	if code, raw := call(http.MethodGet, "/things"); code != http.StatusOK {
		t.Fatalf("reader 读设备列表 = %d %s，应当放行", code, raw)
	}
	code, raw := call(http.MethodPost, "/things/lamp")
	if code != http.StatusForbidden {
		t.Fatalf("reader 建设备 = %d %s，应当是 403", code, raw)
	}
	// 必须是 AccessDenied：403 配上 Unauthorized 会把人支去查密钥和时钟。
	if !strings.Contains(string(raw), "AccessDeniedException") {
		t.Fatalf("拒绝的错误体 = %s", raw)
	}
	// 被拒的请求不能留下痕迹：真建出来了才是最糟的一种"拒绝"。
	if _, err := r.s.ControlPlane().GetThing("lamp"); err == nil {
		t.Fatal("被拒的请求不该建出设备")
	}
}

// awsCred 是一把现发的 access key。
//
// 测试不再用固定的 AKIDEXAMPLE：钥匙现在由节点发放，id 与 secret 都是
// 随机的（AUTH-086）。固定值留在这里的话，测的就不是真实路径了。
type awsCred struct {
	id     string
	secret string
}

// newAWSKey 在这个节点上发一把钥匙。
func (r *testRig) newAWSKey(role string) awsCred {
	r.t.Helper()
	id, secret, err := r.s.CreateAWSKey(context.Background(), role, "test", 0)
	if err != nil {
		r.t.Fatalf("发 %s 钥匙失败: %v", role, err)
	}
	return awsCred{id: id, secret: secret}
}

func signControlPlane(t *testing.T, req *http.Request, c awsCred) {
	t.Helper()
	signAWS(t, req, nil, sigv4.ServiceIoT, c)
}

func signAWS(t *testing.T, req *http.Request, body []byte, service string, c awsCred) {
	t.Helper()
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	amz := time.Now().UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amz)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	headers := map[string][]string{
		"Host":                 {req.Host},
		"X-Amz-Date":           {amz},
		"X-Amz-Content-Sha256": {payload},
	}
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canon, err := sigv4.CanonicalRequestRaw(req.Method, req.URL.Path, req.URL.RawPath, req.URL.RawQuery, headers, signed, payload)
	if err != nil {
		t.Fatal(err)
	}
	sig := sigv4.Sign(c.secret, sigv4.Scope{
		AccessKeyID: c.id,
		Date:        amz[:8],
		Region:      "us-east-1",
		Service:     service,
	}, amz, canon)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+c.id+"/"+amz[:8]+"/us-east-1/"+service+"/aws4_request, "+
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+sig)
}
