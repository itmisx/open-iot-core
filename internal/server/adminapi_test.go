package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/registry"
)

const testAdminToken = "test-admin-token-aaaaaaaaaaaaaaaaaaaaaaaa"

// adminRig 在 testRig 之上配好管理面令牌。
func newAdminRig(t *testing.T, mut func(*config.Config)) *testRig {
	t.Helper()
	r := newTestRig(t, mut)
	// 令牌经 data dir 下的默认文件提供，与 `openiotd init` 的行为一致
	if err := WriteAdminTokenFile(
		r.dd.Path(AdminTokenFileName), testAdminToken); err != nil {
		t.Fatal(err)
	}
	return r
}

// adminReq 发起一次带令牌的管理面请求。
func (r *testRig) adminReq(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	return r.adminReqToken(t, method, path, body, testAdminToken)
}

func (r *testRig) adminReqToken(t *testing.T, method, path string, body any,
	token string,
) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		switch v := body.(type) {
		case string:
			rdr = strings.NewReader(v)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			rdr = bytes.NewReader(b)
		}
	}
	req, err := http.NewRequest(method, "http://"+r.s.AdminAddr()+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// ---------- 鉴权 ----------

// TestAdminAPIRequiresToken 验证管理面一律要求令牌。
//
// 这个端点能吊销证书、踢掉连接、改写策略。它的默认状态必须是关闭。
func TestAdminAPIRequiresToken(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()

	for _, c := range []struct {
		name  string
		token string
	}{
		{"无令牌", ""},
		{"令牌错误", "wrong-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"令牌为旧值的前缀", testAdminToken[:20]},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, _ := r.adminReqToken(t, "GET", adminAPIPrefix+"/node", nil, c.token)
			if code != http.StatusUnauthorized {
				t.Errorf("状态码 = %d，期望 401", code)
			}
		})
	}
	// 正确令牌放行
	code, _ := r.adminReq(t, "GET", adminAPIPrefix+"/node", nil)
	if code != http.StatusOK {
		t.Errorf("正确令牌的状态码 = %d", code)
	}
}

// TestAdminAPINotMountedWithoutToken 验证无令牌时整个管理面不挂载。
//
// 未挂载时请求得到 404，攻击者无法从响应里确认这里有一个管理面；
// 挂载但拒绝则暴露了它的存在。
func TestAdminAPINotMountedWithoutToken(t *testing.T) {
	r := newTestRig(t, nil) // 刻意不写令牌文件
	r.start()

	code, _ := r.adminReqToken(t, "GET", adminAPIPrefix+"/node", nil, testAdminToken)
	if code != http.StatusNotFound {
		t.Errorf("无令牌配置时状态码 = %d，期望 404（整个管理面不挂载）", code)
	}
	// 探针仍须可用：它们不泄露任何东西，而编排系统需要它们
	resp, err := http.Get("http://" + r.s.AdminAddr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz 状态码 = %d，探针应始终可用", resp.StatusCode)
	}
}

func TestAdminTokenResolution(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()

	// 环境变量优先
	got, err := ResolveAdminToken(&cfg, dir, func(k string) (string, bool) {
		if k == EnvAdminToken {
			return testAdminToken, true
		}
		return "", false
	})
	if err != nil || got != testAdminToken {
		t.Errorf("= %q, %v", got, err)
	}

	// data dir 下的默认文件
	if err := WriteAdminTokenFile(filepath.Join(dir, AdminTokenFileName),
		testAdminToken); err != nil {
		t.Fatal(err)
	}
	got, err = ResolveAdminToken(&cfg, dir, nil)
	if err != nil || got != testAdminToken {
		t.Errorf("= %q, %v", got, err)
	}
	// 权限须为 600
	fi, err := os.Stat(filepath.Join(dir, AdminTokenFileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("令牌文件权限 = %o，期望 600", fi.Mode().Perm())
	}

	// 显式配置的文件
	custom := filepath.Join(t.TempDir(), "tok")
	if err := WriteAdminTokenFile(custom, testAdminToken); err != nil {
		t.Fatal(err)
	}
	cfg.Listeners.Admin.AuthTokenFile = custom
	got, err = ResolveAdminToken(&cfg, dir, nil)
	if err != nil || got != testAdminToken {
		t.Errorf("= %q, %v", got, err)
	}

	// 都没有时返回 ErrNoAdminToken，而不是退化为无鉴权
	cfg2 := config.Default()
	if _, err := ResolveAdminToken(&cfg2, t.TempDir(), nil); !errors.Is(err, ErrNoAdminToken) {
		t.Errorf("= %v，期望 ErrNoAdminToken", err)
	}
	// 过短的令牌被拒：挡住 admin、changeme 这类
	short := filepath.Join(t.TempDir(), "tok")
	if err := WriteAdminTokenFile(short, "admin"); err != nil {
		t.Fatal(err)
	}
	cfg2.Listeners.Admin.AuthTokenFile = short
	if _, err := ResolveAdminToken(&cfg2, "", nil); err == nil {
		t.Error("过短的令牌应被拒")
	}
}

func TestBearerTokenParsing(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":   "abc",
		"bearer abc":   "abc", // 方案名大小写不敏感（RFC 7235）
		"BEARER  abc ": "abc",
		"Basic abc":    "",
		"Bearer":       "",
		"":             "",
		"abc":          "",
	}
	for in, want := range cases {
		if got := bearerToken(in); got != want {
			t.Errorf("bearerToken(%q) = %q，期望 %q", in, got, want)
		}
	}
}

func TestCheckAdminToken(t *testing.T) {
	if checkAdminToken("", "x") || checkAdminToken("x", "") {
		t.Error("空令牌不应通过")
	}
	if !checkAdminToken("abc", "abc") {
		t.Error("相同令牌应通过")
	}
	if checkAdminToken("abc", "abd") || checkAdminToken("abc", "ab") {
		t.Error("不同令牌不应通过")
	}
}

// TestAdminPublicExposureNeedsAcknowledgement 验证绑非回环地址须显式确认。
func TestAdminPublicExposureNeedsAcknowledgement(t *testing.T) {
	cfg := config.Default()
	cfg.Listeners.Admin.Addr = "0.0.0.0:9090"
	err := cfg.Validate()
	var fe *config.FieldError
	if !errors.As(err, &fe) ||
		fe.Field != "listeners.admin.acknowledge_public_exposure" {
		t.Fatalf("= %v，期望指向确认字段", err)
	}
	cfg.Listeners.Admin.AcknowledgePublicExposure = true
	if err := cfg.Validate(); err != nil {
		t.Errorf("确认后应允许: %v", err)
	}
}

// ---------- 端点 ----------

func TestAdminNodeEndpoint(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/node", nil)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d: %s", code, body)
	}
	var out nodeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.NodeID != r.mf.NodeID || out.ClusterID != r.mf.ClusterID {
		t.Errorf("= %+v", out)
	}
	if !out.Ready {
		t.Error("应报告就绪")
	}
}

func TestAdminPolicyEndpoints(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()

	// 创建
	code, body := r.adminReq(t, "PUT", adminAPIPrefix+"/policies/apitest",
		policyFullAccess)
	if code != http.StatusNoContent {
		t.Fatalf("创建状态码 = %d: %s", code, body)
	}
	// 读取：文档须原样回传为 JSON 对象
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/policies/apitest", nil)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d: %s", code, body)
	}
	var got struct {
		Name     string          `json:"name"`
		Document json.RawMessage `json:"document"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "apitest" {
		t.Errorf("= %+v", got)
	}
	var doc map[string]any
	if err := json.Unmarshal(got.Document, &doc); err != nil {
		t.Errorf("文档应是 JSON 对象而非字符串: %v", err)
	}

	// 列举
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/policies", nil)
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d", code)
	}
	if !strings.Contains(string(body), "apitest") {
		t.Errorf("列举结果不含新建的策略: %s", body)
	}

	// 编译不过的策略被拒
	code, body = r.adminReq(t, "PUT", adminAPIPrefix+"/policies/bad",
		`{"Statement":[{"Effect":"Maybe","Action":"iot:*","Resource":"*"}]}`)
	if code != http.StatusBadRequest {
		t.Errorf("状态码 = %d，期望 400: %s", code, body)
	}

	// 删除
	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/policies/apitest", nil)
	if code != http.StatusNoContent {
		t.Errorf("删除状态码 = %d", code)
	}
	code, _ = r.adminReq(t, "GET", adminAPIPrefix+"/policies/apitest", nil)
	if code != http.StatusNotFound {
		t.Errorf("删除后状态码 = %d，期望 404", code)
	}
}

func TestAdminThingEndpoints(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	if code, body := r.adminReq(t, "PUT", adminAPIPrefix+"/policies/p",
		policyFullAccess); code != http.StatusNoContent {
		t.Fatalf("= %d: %s", code, body)
	}

	code, body := r.adminReq(t, "PUT", adminAPIPrefix+"/things/dev-api",
		map[string]any{"policyName": "p", "attributes": map[string]string{"site": "sh"}})
	if code != http.StatusNoContent {
		t.Fatalf("= %d: %s", code, body)
	}
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/things/dev-api", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	if !strings.Contains(string(body), `"site"`) ||
		!strings.Contains(string(body), `"sh"`) {
		t.Errorf("属性未回传: %s", body)
	}

	// 分页列举
	for i := 0; i < 5; i++ {
		r.adminReq(t, "PUT", fmt.Sprintf("%s/things/bulk-%d", adminAPIPrefix, i),
			map[string]any{"policyName": "p"})
	}
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/things?limit=2&prefix=bulk-", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d", code)
	}
	var page registry.ThingPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Things) != 2 {
		t.Errorf("一页条数 = %d，期望 2（分页须在服务端完成）", len(page.Things))
	}
	if page.NextAfter == "" {
		t.Error("应给出下一页游标")
	}

	// 未知字段被拒，而不是忽略
	code, body = r.adminReq(t, "PUT", adminAPIPrefix+"/things/dev-x",
		`{"policy":"p"}`)
	if code != http.StatusBadRequest {
		t.Errorf("未知字段应被拒，状态码 = %d: %s", code, body)
	}

	// 绑定不存在的策略被拒
	code, _ = r.adminReq(t, "PUT", adminAPIPrefix+"/things/dev-y",
		map[string]any{"policyName": "nope"})
	if code != http.StatusBadRequest {
		t.Errorf("= %d，期望 400", code)
	}

	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/things/dev-api", nil)
	if code != http.StatusNoContent {
		t.Errorf("= %d", code)
	}
}

// TestAdminDeleteInUseReturns409 验证"状态不允许"与"请求非法"被区分。
func TestAdminDeleteInUseReturns409(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	r.adminReq(t, "PUT", adminAPIPrefix+"/policies/p", policyFullAccess)
	r.adminReq(t, "PUT", adminAPIPrefix+"/things/d1", map[string]any{"policyName": "p"})

	code, body := r.adminReq(t, "DELETE", adminAPIPrefix+"/policies/p", nil)
	if code != http.StatusConflict {
		t.Fatalf("状态码 = %d，期望 409: %s", code, body)
	}
	// 错误须指出是谁在引用
	if !strings.Contains(string(body), "d1") {
		t.Errorf("错误应指出引用者: %s", body)
	}
}

// TestAdminRevocationKicksConnection 是 PKI-030 的 HTTP 路径验收。
func TestAdminRevocationKicksConnection(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-http-rev")
	r.start()

	certs, err := reg.LoadCerts()
	if err != nil || len(certs) == 0 {
		t.Fatal(err)
	}
	fingerprint := certs[0].Fingerprint

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-http-rev"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	// 连接应出现在列举里
	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/connections", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	if !strings.Contains(string(body), "dev-http-rev") {
		t.Errorf("连接列举不含该客户端: %s", body)
	}

	// 经 HTTP 吊销
	code, body = r.adminReq(t, "PUT",
		adminAPIPrefix+"/certificates/"+fingerprint+"/status",
		map[string]any{"status": registry.StatusRevoked, "reason": "经 API 吊销"})
	if code != http.StatusNoContent {
		t.Fatalf("吊销状态码 = %d: %s", code, body)
	}

	// 在用连接被踢
	if rc := c.expectClosed(5 * time.Second); rc != reason.NotAuthorized {
		t.Errorf("断开原因码 = %s，期望 Not authorized", rc)
	}
}

func TestAdminKickConnection(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-kick")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-kick"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	// 详情
	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/connections/dev-kick", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	if !strings.Contains(string(body), "dev-kick") {
		t.Errorf("= %s", body)
	}
	// 踢出
	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/connections/dev-kick", nil)
	if code != http.StatusNoContent {
		t.Fatalf("踢出状态码 = %d", code)
	}
	if rc := c.expectClosed(5 * time.Second); rc != reason.AdministrativeAction {
		t.Errorf("断开原因码 = %s，期望 Administrative action", rc)
	}
	// 不在线的 clientId
	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/connections/nobody", nil)
	if code != http.StatusNotFound {
		t.Errorf("= %d，期望 404", code)
	}
}

// TestAdminConnDetailShowsSubscriptionsAndInflight 验证连接详情带订阅和在途，
// 并且响应里没有消息负载。
//
// handleGetConn 若退回不含这两项的快照，过滤器不会出现，本测试失败。
func TestAdminConnDetailShowsSubscriptionsAndInflight(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	subCert := r.issue("dev-detail")
	pubCert := r.issue("dev-detail-pub")
	r.start()

	sub, err := r.dial(&subCert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := sub.connect("dev-detail"); ack.Code != reason.Success {
		t.Fatal(ack.Code)
	}
	if rc := sub.subscribe("floor/#"); rc.IsError() {
		t.Fatal(rc)
	}
	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/connections/dev-detail", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	if !strings.Contains(string(body), `"filter": "floor/#"`) {
		t.Fatalf("详情没有订阅: %s", body)
	}
	if !strings.Contains(string(body), `"inflight": []`) {
		t.Fatalf("空在途应是空数组: %s", body)
	}

	pub, err := r.dial(&pubCert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := pub.connect("dev-detail-pub"); ack.Code != reason.Success {
		t.Fatal(ack.Code)
	}
	if rc := pub.publish("floor/temp", "secret-payload", packet.QoS1); rc.IsError() {
		t.Fatal(rc)
	}
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/connections/dev-detail", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	if !strings.Contains(string(body), `"topic": "floor/temp"`) || !strings.Contains(string(body), `"state": "puback"`) {
		t.Fatalf("详情没有在途: %s", body)
	}
	if strings.Contains(string(body), "secret-payload") {
		t.Fatalf("详情带了负载: %s", body)
	}
}

func TestAdminCertificateEndpoints(t *testing.T) {
	r := newAdminRig(t, nil)
	r.issue("dev-cert-api")
	r.start()

	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/certificates", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	var page registry.CertPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Certificates) != 1 {
		t.Fatalf("证书数 = %d", len(page.Certificates))
	}
	fp := page.Certificates[0].Fingerprint

	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/certificates/"+fp, nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	// 非法状态被拒
	code, _ = r.adminReq(t, "PUT", adminAPIPrefix+"/certificates/"+fp+"/status",
		map[string]any{"status": "ENABLED"})
	if code != http.StatusBadRequest {
		t.Errorf("= %d，期望 400", code)
	}
	// 未登记的指纹
	code, _ = r.adminReq(t, "GET",
		adminAPIPrefix+"/certificates/"+strings.Repeat("a", 64), nil)
	if code != http.StatusNotFound {
		t.Errorf("= %d，期望 404", code)
	}
	// 按状态筛选
	code, body = r.adminReq(t, "GET",
		adminAPIPrefix+"/certificates?status=REVOKED", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d", code)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Certificates) != 0 {
		t.Errorf("筛选 REVOKED 应为空，实际 %d", len(page.Certificates))
	}
}

func TestAdminShadowEndpoint(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-sh-api")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-sh-api"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-sh-api/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	if rc := c.publish("$aws/things/dev-sh-api/shadow/update",
		`{"state":{"reported":{"t":1}}}`, packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	collectByTopic(t, c, 3)

	code, body := r.adminReq(t, "GET",
		adminAPIPrefix+"/things/dev-sh-api/shadow", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	// 响应形状须与 MQTT 的 /get/accepted 一致：小写的 state、metadata、
	// version，而不是内部结构体的 Go 字段名
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["version"] != float64(1) {
		t.Errorf("version = %v，期望 1；响应形状须与 /get/accepted 一致: %s",
			doc["version"], body)
	}
	st, _ := doc["state"].(map[string]any)
	rep, _ := st["reported"].(map[string]any)
	if rep["t"] != float64(1) {
		t.Errorf("state.reported.t = %v: %s", rep["t"], body)
	}
	if _, bad := doc["Desired"]; bad {
		t.Error("不得暴露内部结构体的 Go 字段名")
	}
	// 不存在的影子
	code, _ = r.adminReq(t, "GET", adminAPIPrefix+"/things/nobody/shadow", nil)
	if code != http.StatusNotFound {
		t.Errorf("= %d，期望 404", code)
	}
}

func TestAdminBodyLimits(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	// 空请求体
	code, _ := r.adminReq(t, "PUT", adminAPIPrefix+"/policies/empty", "")
	if code != http.StatusBadRequest {
		t.Errorf("空请求体状态码 = %d，期望 400", code)
	}
	// 过大的策略
	big := `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"` +
		strings.Repeat("a", maxPolicyBytes) + `"}]}`
	code, _ = r.adminReq(t, "PUT", adminAPIPrefix+"/policies/big", big)
	if code != http.StatusRequestEntityTooLarge && code != http.StatusBadRequest {
		t.Errorf("过大的请求体状态码 = %d", code)
	}
}
