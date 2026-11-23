package server

import (
	"bufio"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/registry"
)

// ---------- 控制台 ----------

// TestConsoleServed 验证前端产物经 go:embed 随二进制提供（PRD UI-001）。
func TestConsoleServed(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	base := "http://" + r.s.AdminAddr()

	// 外壳不需要令牌：它不含任何数据，只是一段通用 JS。
	// 浏览器导航无法携带 Authorization 头，这是唯一可行的做法。
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / 状态码 = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q", ct)
	}

	for _, p := range []string{"/assets/app.js", "/assets/index.css"} {
		r2, err := http.Get(base + p)
		if err != nil {
			t.Fatal(err)
		}
		_ = r2.Body.Close()
		if r2.StatusCode != http.StatusOK {
			t.Errorf("GET %s 状态码 = %d", p, r2.StatusCode)
		}
	}

	// 资源路径不回落到 HTML：回落会让一个拼错的 .js 路径返回 HTML，
	// 浏览器报出的错误与真实原因完全无关
	r3, err := http.Get(base + "/assets/nonexistent.js")
	if err != nil {
		t.Fatal(err)
	}
	_ = r3.Body.Close()
	if r3.StatusCode != http.StatusNotFound {
		t.Errorf("不存在的资源状态码 = %d，期望 404", r3.StatusCode)
	}

	// 探针的路由优先级高于控制台的通配
	r4, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = r4.Body.Close()
	if r4.StatusCode != http.StatusOK {
		t.Errorf("/healthz 状态码 = %d；控制台的通配路由不应盖住探针", r4.StatusCode)
	}
}

func TestConsoleCanBeDisabled(t *testing.T) {
	r := newAdminRig(t, func(c *config.Config) { c.Console.Enabled = false })
	r.start()
	resp, err := http.Get("http://" + r.s.AdminAddr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("关闭后状态码 = %d，期望 404", resp.StatusCode)
	}
}

// TestConsoleBasePath 验证反向代理子路径（PRD UI-001）。
func TestConsoleBasePath(t *testing.T) {
	r := newAdminRig(t, func(c *config.Config) { c.Console.BasePath = "/openiotd" })
	r.start()
	base := "http://" + r.s.AdminAddr()

	resp, err := http.Get(base + "/openiotd/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("子路径状态码 = %d", resp.StatusCode)
	}
	// 资源也要能经子路径取到
	r2, err := http.Get(base + "/openiotd/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	_ = r2.Body.Close()
	if r2.StatusCode != http.StatusOK {
		t.Errorf("子路径下的资源状态码 = %d", r2.StatusCode)
	}
	// 根路径不再提供控制台
	r3, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = r3.Body.Close()
	if r3.StatusCode == http.StatusOK {
		t.Error("配置了子路径后根路径不应提供控制台")
	}
}

func TestNormalizeBasePath(t *testing.T) {
	cases := map[string]string{
		"":         "/",
		"/":        "/",
		"ui":       "/ui",
		"/ui":      "/ui",
		"/ui/":     "/ui",
		"  /ui/  ": "/ui",
		"/a/b/":    "/a/b",
	}
	for in, want := range cases {
		if got := normalizeBasePath(in); got != want {
			t.Errorf("normalizeBasePath(%q) = %q，期望 %q", in, got, want)
		}
	}
}

// ---------- 策略干跑 ----------

// TestSimulateAllowAndDeny 是 PRD UI-010 的验收。
//
// 变量求值结果是这个端点的核心价值：它在判定时捕获，事后无法重建
// （AUTH-035），而"策略写对了却仍被拒"几乎总是某个变量没求出值。
func TestSimulateAllowAndDeny(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	r.issue("dev-sim")
	r.start()

	// 自己名下的主题：放行
	code, body := r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate",
		map[string]any{
			"thing":    "dev-sim",
			"action":   "iot:Publish",
			"resource": "$aws/things/dev-sim/shadow/update",
		})
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	var ok struct {
		Allowed        bool   `json:"allowed"`
		Decision       string `json:"decision"`
		PolicyName     string `json:"policyName"`
		StatementIndex int    `json:"statementIndex"`
		Variables      []struct {
			Name     string `json:"name"`
			Value    string `json:"value"`
			Resolved bool   `json:"resolved"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(body, &ok); err != nil {
		t.Fatal(err)
	}
	if !ok.Allowed || ok.Decision != "Allow" {
		t.Errorf("应放行: %s", body)
	}
	if ok.StatementIndex < 0 {
		t.Error("放行时应指出命中的语句下标")
	}
	// 变量求值结果必须回传——这是整个端点的存在理由
	found := false
	for _, v := range ok.Variables {
		if v.Name == "iot:Connection.Thing.ThingName" {
			found = true
			if v.Value != "dev-sim" || !v.Resolved {
				t.Errorf("变量求值 = %+v", v)
			}
		}
	}
	if !found {
		t.Errorf("未回传策略变量的求值结果: %s", body)
	}

	// 别人名下的主题：拒绝，且给出可操作建议
	code, body = r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate",
		map[string]any{
			"thing":    "dev-sim",
			"action":   "iot:Publish",
			"resource": "$aws/things/other/shadow/update",
		})
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	var denied struct {
		Allowed  bool     `json:"allowed"`
		Decision string   `json:"decision"`
		Advice   []string `json:"advice"`
	}
	if err := json.Unmarshal(body, &denied); err != nil {
		t.Fatal(err)
	}
	if denied.Allowed {
		t.Error("越权主题应被拒")
	}
	if denied.Decision != "ImplicitDeny" {
		t.Errorf("判定 = %q，期望 ImplicitDeny", denied.Decision)
	}
	// 建议是调试器区别于"打印判定结果"的地方
	if len(denied.Advice) == 0 {
		t.Error("被拒时应给出如何放行的建议")
	}
	if !strings.Contains(strings.Join(denied.Advice, " "), "iot:Publish") {
		t.Errorf("建议应指出需要哪个动作: %v", denied.Advice)
	}
}

// TestSimulateByCertificate 验证按证书指纹判定。
//
// 这是最接近真实情形的入口：策略、设备绑定、证书字段都由指纹推出，
// 不需要调用方自己拼。
func TestSimulateByCertificate(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	r.issue("dev-bycert")
	r.start()

	certs, err := reg.LoadCerts()
	if err != nil || len(certs) == 0 {
		t.Fatal(err)
	}
	code, body := r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate",
		map[string]any{
			"certFingerprint": certs[0].Fingerprint,
			"action":          "iot:Subscribe",
			"resource":        "$aws/things/dev-bycert/shadow/#",
		})
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	// 解析后判断而不是字符串匹配：响应是带缩进的，
	// `"allowed":true` 永远匹配不上 `"allowed": true`
	var byCert struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.Unmarshal(body, &byCert); err != nil {
		t.Fatal(err)
	}
	if !byCert.Allowed {
		t.Errorf("应放行: %s", body)
	}
}

// TestSimulateExplicitDenyAdvice 验证显式拒绝给出的建议不同。
//
// ExplicitDeny 与 ImplicitDeny 的区别对排障很要紧：前者加 Allow 无用，
// 必须改那条 Deny；后者加一条就行。建议必须说清这一点。
func TestSimulateExplicitDenyAdvice(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	const denyPolicy = `{"Statement":[
	  {"Effect":"Allow","Action":"iot:*","Resource":"*"},
	  {"Effect":"Deny","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/secret/*"}
	]}`
	if code, body := r.adminReq(t, "PUT", adminAPIPrefix+"/policies/withdeny",
		denyPolicy); code != http.StatusNoContent {
		t.Fatalf("= %d: %s", code, body)
	}

	code, body := r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate",
		map[string]any{
			"policyNames": []string{"withdeny"},
			"clientId":    "c1",
			"action":      "iot:Publish",
			"resource":    "secret/data",
		})
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	var out struct {
		Allowed  bool     `json:"allowed"`
		Decision string   `json:"decision"`
		Advice   []string `json:"advice"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Allowed || out.Decision != "ExplicitDeny" {
		t.Fatalf("应为显式拒绝: %s", body)
	}
	joined := strings.Join(out.Advice, " ")
	if !strings.Contains(joined, "Deny") {
		t.Errorf("建议应说明命中了显式 Deny: %v", out.Advice)
	}
}

func TestSimulateValidation(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	cases := []struct {
		name string
		body map[string]any
	}{
		{"未知动作", map[string]any{"thing": "x", "action": "s3:GetObject", "resource": "a"}},
		{"缺少资源", map[string]any{"thing": "x", "action": "iot:Publish"}},
		{"未指定主体", map[string]any{"action": "iot:Publish", "resource": "a"}},
		{"设备不存在", map[string]any{"thing": "nobody", "action": "iot:Publish", "resource": "a"}},
		{"策略不存在", map[string]any{
			"policyNames": []string{"nope"}, "action": "iot:Publish", "resource": "a",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, body := r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate", c.body)
			if code != http.StatusBadRequest {
				t.Errorf("状态码 = %d，期望 400: %s", code, body)
			}
		})
	}
}

// ---------- 事件流 ----------

// TestEventStreamDeliversEvents 验证 SSE 真的推送事件（PRD API-051、UI-003）。
func TestEventStreamDeliversEvents(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-sse")
	r.start()

	// 令牌走查询参数：EventSource 不支持自定义请求头
	url := "http://" + r.s.AdminAddr() + adminAPIPrefix + "/events?token=" + testAdminToken
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	// 响应缓冲必须关掉，否则事件会被代理攒住
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Error("应声明 X-Accel-Buffering: no")
	}

	lines := make(chan string, 64)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	// 触发一次连接，它应当出现在流里
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-sse"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	deadline := time.After(10 * time.Second)
	var got string
	for got == "" {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("事件流提前结束")
			}
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, "dev-sse") {
				got = strings.TrimPrefix(line, "data: ")
			}
		case <-deadline:
			t.Fatal("未在 10 秒内收到连接事件")
		}
	}

	var ev map[string]any
	if err := json.Unmarshal([]byte(got), &ev); err != nil {
		t.Fatalf("事件不是合法 JSON: %v\n%s", err, got)
	}
	if ev["kind"] != "Conn" {
		t.Errorf("事件类型 = %v", ev["kind"])
	}
	if ev["clientId"] != "dev-sse" {
		t.Errorf("clientId = %v", ev["clientId"])
	}
	// 不得暴露内部结构体的 Go 字段名
	if _, bad := ev["Conn"]; bad {
		t.Error("事件形状不得使用内部结构体的字段名")
	}
	conn, _ := ev["conn"].(map[string]any)
	if conn["result"] != "Accepted" {
		t.Errorf("conn.result = %v", conn["result"])
	}
}

func TestEventStreamRequiresToken(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	resp, err := http.Get("http://" + r.s.AdminAddr() + adminAPIPrefix + "/events")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("状态码 = %d，期望 401", resp.StatusCode)
	}
}

// ---------- 指标摘要 ----------

func TestOverviewEndpoint(t *testing.T) {
	r := newAdminRig(t, nil)
	r.issue("dev-ov")
	r.start()

	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/overview", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	var out struct {
		Node struct {
			NodeID string `json:"nodeId"`
			Ready  bool   `json:"ready"`
		} `json:"node"`
		Quotas []struct {
			Name     string `json:"name"`
			Allowed  uint64 `json:"allowed"`
			Rejected uint64 `json:"rejected"`
			Current  int64  `json:"current"`
		} `json:"quotas"`
		Events struct {
			Emitted map[string]uint64 `json:"emitted"`
			Dropped uint64            `json:"dropped"`
		} `json:"events"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Node.NodeID != r.mf.NodeID || !out.Node.Ready {
		t.Errorf("= %+v", out.Node)
	}
	// 配额为空也须是数组而不是 null：前端对 null 做 .map 会炸。
	// 用解析判断而不是字符串匹配——响应是带缩进的，
	// 字符串匹配会被空格绊倒。
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(body, &shape); err != nil {
		t.Fatal(err)
	}
	if string(shape["quotas"]) == "null" {
		t.Error("配额为空时应序列化为 []，而不是 null")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(shape["quotas"], &arr); err != nil {
		t.Errorf("配额应是数组: %v", err)
	}
	if len(out.Quotas) != quota.QuotaCount() {
		t.Fatalf("配额数 = %d，期望全部 %d 项定义", len(out.Quotas), quota.QuotaCount())
	}
	if out.Quotas[0].Allowed != 0 || out.Quotas[0].Rejected != 0 || out.Quotas[0].Current != 0 {
		t.Fatalf("尚未使用的配额应保留零统计: %+v", out.Quotas[0])
	}
}

// TestOverviewOnlineCurveFollowsConnections 验证在线率曲线跟着连接数走。
//
// 删掉 noteOnline，总览的 online 就不再随连接增减，这条失败。
func TestOverviewOnlineCurveFollowsConnections(t *testing.T) {
	r := newAdminRig(t, nil)
	cert := r.issue("dev-curve")
	r.start()

	read := func() []struct {
		N int64 `json:"n"`
	} {
		t.Helper()
		code, body := r.adminReq(t, "GET", adminAPIPrefix+"/overview", nil)
		if code != http.StatusOK {
			t.Fatalf("= %d: %s", code, body)
		}
		var out struct {
			Online []struct {
				N int64 `json:"n"`
			} `json:"online"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		return out.Online
	}

	first := read()
	if len(first) != 1 || first[0].N != 0 {
		t.Fatalf("尚无连接时采样 = %+v，期望一个 0", first)
	}
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	ack := c.connect("dev-curve")
	if ack.Code != reason.Success {
		t.Fatalf("连接 = %s", ack.Code)
	}
	second := read()
	if len(second) != 2 || second[0].N != 0 || second[1].N < 1 {
		t.Fatalf("连上之后采样 = %+v，期望 [0, >=1]", second)
	}
}

// ---------- 影子写入 ----------

// TestAdminShadowUpdatePublishesDelta 验证经控制台改 desired 会通知设备。
//
// 这是 SHDW-045 的要点：REST 与 MQTT 共用状态机，因此经控制台
// 下发配置不是"只改了一行数据库"，设备会真的收到 delta。
func TestAdminShadowUpdatePublishesDelta(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-push")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-push"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-push/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	// 设备先上报一个状态
	if rc := c.publish("$aws/things/dev-push/shadow/update",
		`{"state":{"reported":{"mode":"idle"}}}`, packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	collectByTopic(t, c, 3)

	// 控制台下发一个不同的 desired
	code, body := r.adminReq(t, "PUT", adminAPIPrefix+"/things/dev-push/shadow",
		map[string]any{"state": map[string]any{"desired": map[string]any{"mode": "run"}}})
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}

	// 设备须收到 delta
	got := collectByTopic(t, c, 2)
	raw, ok := got["$aws/things/dev-push/shadow/update/delta"]
	if !ok {
		t.Fatalf("经控制台下发 desired 后设备未收到 delta（收到: %v）；"+
			"那会让控制台的\"下发配置\"变成只改了数据库的假动作", topicsOf(got))
	}
	var delta map[string]any
	if err := json.Unmarshal(raw, &delta); err != nil {
		t.Fatal(err)
	}
	st, _ := delta["state"].(map[string]any)
	if st["mode"] != "run" {
		t.Errorf("delta = %s", raw)
	}

	// 影子的读写都经同一服务，形状一致
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/things/dev-push/shadow", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d", code)
	}
	if !strings.Contains(string(body), `"delta"`) {
		t.Errorf("读取响应应含 delta: %s", body)
	}

	// 删除影子
	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/things/dev-push/shadow", nil)
	if code != http.StatusOK {
		t.Errorf("删除影子状态码 = %d", code)
	}
	code, _ = r.adminReq(t, "GET", adminAPIPrefix+"/things/dev-push/shadow", nil)
	if code != http.StatusNotFound {
		t.Errorf("删除后状态码 = %d，期望 404", code)
	}
}

func TestAdminListShadows(t *testing.T) {
	r := newAdminRig(t, nil)
	r.issue("dev-ls")
	r.start()
	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/things/dev-ls/shadows", nil)
	if code != http.StatusOK {
		t.Fatalf("= %d: %s", code, body)
	}
	// 空列表须是数组而不是 null：前端对 null 做 .map 会炸
	var out struct {
		Results []string `json:"results"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Results == nil {
		t.Errorf("空列表应序列化为 []，而不是 null: %s", body)
	}
}

// TestRegistryStatusConstantsMatchAPI 固定住状态字符串。
//
// 它们出现在控制台的下拉框与 API 契约里，改动会同时破坏两边。
func TestRegistryStatusConstantsMatchAPI(t *testing.T) {
	for _, s := range []string{
		registry.StatusActive, registry.StatusInactive,
		registry.StatusRevoked, registry.StatusPendingActivation,
	} {
		if err := registry.ValidateStatus(s); err != nil {
			t.Errorf("%q 应为合法状态: %v", s, err)
		}
	}
	if registry.StatusActive != "ACTIVE" || registry.StatusRevoked != "REVOKED" {
		t.Error("状态字符串是 API 契约，不得改动")
	}
}

// TestConnectionRejectionIsLogged 验证被拒的连接会写进日志。
//
// 这条测试来自一次真实的排障：设备返回 NotAuthorized，而服务端日志
// 一行都没有——原因只在事件总线上。运维不会先去订阅事件流，他会先看日志。
//
// broker 的输出面只有事件总线（契约 11），因此日志由 server 订阅事件后产出，
// 协议层仍然不知道日志的存在。
func TestConnectionRejectionIsLogged(t *testing.T) {
	var buf safeBuffer
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	// 只允许 clientId 等于设备名，复现最常见的那类拒绝
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(`{"Statement":[{"Effect":"Allow","Action":"iot:Connect",`+
			`"Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-log")
	r.logTo(&buf)
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	// clientId 与设备名不一致：策略不放行
	if ack := c.connect("some-random-client"); ack.Code != reason.NotAuthorized {
		t.Fatalf("应被拒，得到 %s", ack.Code)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "连接被拒") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "连接被拒") {
		t.Fatalf("被拒的连接未写入日志:\n%s", out)
	}
	// 排障要用的字段必须齐：谁、为什么、从哪来、用什么证书
	for _, want := range []string{
		"some-random-client", "dev-log", "策略不允许该 clientId 连接", "mTLS",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("日志缺少 %q:\n%s", want, out)
		}
	}
	// 成功的连接不写日志：量级太大，它属于指标
	before := len(buf.String())
	c2, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c2.connect("dev-log"); ack.Code != reason.Success {
		t.Fatalf("应成功: %s", ack.Code)
	}
	time.Sleep(300 * time.Millisecond)
	if added := buf.String()[before:]; strings.Contains(added, "连接被拒") {
		t.Errorf("成功的连接不应写入被拒日志: %s", added)
	}
}

// safeBuffer 是并发安全的日志缓冲。
//
// slog 的处理器会在事件订阅协程里写入，而测试在主协程里读。
type safeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// 授权被拒也必须写日志。
//
// 由来同 TestConnectionRejectionIsLogged：用户连上之后订阅被拒，
// 拿到 SUBACK 0x87，而服务端日志依旧一行都没有——同一个可诊断性
// 缺口只是下移了一层（连接 → 授权）。
func TestAuthzRejectionIsLogged(t *testing.T) {
	var buf safeBuffer
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	// 只放行设备自己的影子主题，复现"订阅业务主题被拒"
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(`{"Statement":[{"Effect":"Allow","Action":["iot:Connect"],`+
			`"Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"},`+
			`{"Effect":"Allow","Action":["iot:Subscribe"],`+
			`"Resource":"arn:aws:iot:*:*:topicfilter/$aws/things/`+
			`${iot:Connection.Thing.ThingName}/*"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-az")
	r.logTo(&buf)
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-az"); ack.Code != reason.Success {
		t.Fatalf("连接应成功: %s", ack.Code)
	}
	if code := c.subscribe("sensors/temp"); code != reason.NotAuthorized {
		t.Fatalf("订阅应被拒，得到 %s", code)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "授权被拒") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "授权被拒") {
		t.Fatalf("被拒的订阅未写入日志:\n%s", out)
	}
	// 排障要用的字段必须齐：谁、做什么、什么资源、怎么判的
	for _, want := range []string{
		"dev-az", "iot:Subscribe", "sensors/temp", "ImplicitDeny",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("日志缺少 %q:\n%s", want, out)
		}
	}
	// 变量求值结果是关键线索：多数"策略看起来写对了却被拒"
	// 都是某个变量求出的值与预期不同
	if !strings.Contains(out, "iot:Connection.Thing.ThingName") {
		t.Errorf("日志应带上策略变量的求值结果:\n%s", out)
	}
	// 放行的订阅不写日志
	before := len(buf.String())
	if code := c.subscribe("$aws/things/dev-az/shadow/update/accepted"); code != reason.GrantedQoS1 {
		t.Fatalf("订阅应放行，得到 %s", code)
	}
	time.Sleep(300 * time.Millisecond)
	if added := buf.String()[before:]; strings.Contains(added, "授权被拒") {
		t.Errorf("放行的订阅不应写入被拒日志: %s", added)
	}
}

// 控制台必须为每一种事件类型挂上 SSE 监听。
//
// 由来：服务端给每条事件都写了 event 字段（event: Conn 等），而按 SSE
// 规范 onmessage 只接收类型为 "message" 的事件。控制台原先只挂了
// onmessage，于是一条事件都收不到——而连接本身是成功的，状态灯还显示
// "实时"，所以这个故障看起来像"功能没做"而不是"坏了"。
//
// Go 端的测试断言 SSE 原文，那部分一直是对的；漏掉的是浏览器侧的接线。
// 这个测试把两边钉在一起：以后服务端加一种事件类型而控制台忘了加，
// 门禁会拦住，不会再变成静默丢弃。
func TestConsoleListensForEveryEventKind(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "web", "console", "src", "views", "events.tsx"))
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// 控制台用 KINDS 数组逐个 addEventListener，所以每个类型名都该出现在那里。
	for _, name := range events.KindNames() {
		if !strings.Contains(code, "'"+name+"'") {
			t.Errorf("事件类型 %q 不在控制台的 KINDS 里，该类型的事件会被静默丢弃", name)
		}
	}
	// 回填结束标记也要有人听，否则分隔线与空状态都不会出现。
	if !strings.Contains(code, "'sync'") {
		t.Error("控制台未监听 sync 标记，回填与实时的分界无法呈现")
	}
	// 必须是 addEventListener 而不是只有 onmessage。
	if !strings.Contains(code, "addEventListener") {
		t.Error("控制台只用 onmessage 会收不到任何命名事件")
	}
}

// 空状态必须说清三件事（UI-030 至 UI-032）。
//
// 由来：事件流曾经只显示"等待事件"，而空面板与坏掉的面板在界面上
// 完全一样——用户的第一反应一定是后者。立了标准之后，其余五个页面
// 却仍是一行"没有数据"，于是同一个项目里两套做法。
//
// 这个门禁把标准钉在代码上：每个列表页都要有自己的空状态文案（通道
// 是通的、此刻确实没有数据、怎么造出一条来），带筛选的页面还要能
// 区分"被筛选滤空"——后者的处置办法是去掉筛选，与排查故障相反。
// 文案是两种语言的，少一边等于英文界面上开个天窗，所以一并检查。
func TestConsoleEmptyStatesAreActionable(t *testing.T) {
	base := filepath.Join("..", "..", "web", "console", "src")
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(base, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// 每个页面要用的空状态键。带 filtered 的页面有筛选控件，
	// 必须另给一套说法。
	pages := []struct {
		file     string
		keys     []string
		filtered bool
	}{
		{"views/things.tsx", []string{"things.emptyTitle", "things.emptyBody", "things.emptyCmd"}, true},
		{"views/certificates.tsx", []string{"certs.emptyTitle", "certs.emptyBody", "certs.emptyCmd"}, true},
		{"views/connections.tsx", []string{"conns.emptyTitle", "conns.emptyBody", "common.connectCmd", "conns.subsEmptyTitle", "conns.subsEmptyBody"}, true},
		{"views/policies.tsx", []string{"policies.emptyTitle", "policies.emptyBody"}, false},
		{"views/overview.tsx", []string{"overview.quotaNone", "overview.quotaNoneBody"}, false},
		{"views/events.tsx", []string{"events.emptyTitle", "events.emptyHint", "common.connectCmd"}, true},
		// 测试客户端一页两档（UI-070）：订阅模式与旁观模式的空状态
		// 说的不是一回事——后者要说清"旁观看的是经过本节点的发布，
		// 包括没有订阅者的那些"，共用一句就把这个区别抹掉了。
		{"views/mqtt.tsx", []string{
			"mqtt.emptyTitle", "mqtt.emptyBody", "mqtt.emptyCmd",
			"capture.emptyBody", "capture.emptyCmd",
		}, true},
		// 访问控制页给的是按钮不是命令：命令行没有建控制台用户的子命令。
		// UI-030 要的第三件事是"怎么造出一条来"，按钮与命令都算数。
		{"views/access.tsx", []string{"access.emptyTitle", "access.emptyBody"}, false},
		{"views/thing-detail.tsx", []string{"timeline.emptyTitle", "timeline.emptyBody"}, false},
	}

	i18n := read("i18n.ts")
	zh, en, ok := strings.Cut(i18n, "const en: Dict = {")
	if !ok {
		t.Fatal("i18n.ts 的结构变了，找不到英文词典的起点")
	}

	for _, p := range pages {
		code := read(p.file)
		// 一行"没有数据"是被这条标准淘汰的做法，不得再出现。
		if strings.Contains(code, "common.empty") {
			t.Errorf("%s 仍在用一行通用空文案，UI-030 要求说清为什么空、怎么不空", p.file)
		}
		if !strings.Contains(code, "<Empty") {
			t.Errorf("%s 未使用共用的 Empty 组件，空状态会各写各的", p.file)
		}
		// UI-030 的第三件事：怎么造出一条来。一条可粘贴的命令（cmd）
		// 或一个就在眼前的按钮（action），有一个就行，一个都没有就是
		// 把问题原样推回给用户。
		if !strings.Contains(code, "cmd=") && !strings.Contains(code, "action=") {
			t.Errorf("%s 的空状态只说了为什么空，没说怎么不空（UI-030）", p.file)
		}
		if p.filtered && !strings.Contains(code, "Filtered") {
			t.Errorf("%s 有筛选控件却不区分「被筛选滤空」，UI-032 要求分开说", p.file)
		}
		for _, k := range p.keys {
			if !strings.Contains(code, "'"+k+"'") {
				t.Errorf("%s 没有引用空状态文案 %s", p.file, k)
			}
			if !strings.Contains(zh, "'"+k+"':") {
				t.Errorf("i18n 中文缺少 %s", k)
			}
			if !strings.Contains(en, "'"+k+"':") {
				t.Errorf("i18n 英文缺少 %s", k)
			}
		}
	}

	// 滤空的统一说法与"清除筛选"的出口。
	for _, k := range []string{"common.filteredTitle", "common.filteredBody", "common.clearFilter"} {
		if !strings.Contains(zh, "'"+k+"':") || !strings.Contains(en, "'"+k+"':") {
			t.Errorf("i18n 缺少 %s（中英须齐备）", k)
		}
	}

	// 回填未完成时显示加载中而不是空状态（UI-033）：否则首屏会闪一下
	// "没有活动"再被历史事件填满。
	if ev := read("views/events.tsx"); !strings.Contains(ev, "if (!synced)") {
		t.Error("事件流未在回填结束前显示加载中，UI-033 要求如此")
	}
}

// TestConsoleTablesAreWindowed 验证长表只渲染窗口内的行。
//
// 把 win.slice 改回整页 map，百万行就会进 DOM。空状态仍然留在原处。
func TestConsoleTablesAreWindowed(t *testing.T) {
	base := filepath.Join("..", "..", "web", "console", "src")
	for _, rel := range []string{
		"views/things.tsx", "views/certificates.tsx", "views/connections.tsx",
	} {
		b, err := os.ReadFile(filepath.Join(base, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		if !strings.Contains(text, "useWindow") || !strings.Contains(text, "win.slice.map") {
			t.Errorf("%s 没有按窗口渲染", rel)
		}
		if strings.Contains(text, "page.things.map") ||
			strings.Contains(text, "page.certificates.map") ||
			strings.Contains(text, "page.connections.map") {
			t.Errorf("%s 仍在映射整页", rel)
		}
	}
	curve, err := os.ReadFile(filepath.Join(base, "views", "connections.tsx"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(curve), "overview.curve") {
		t.Error("连接页没有在线率曲线")
	}
}

// TestConsoleMQTTStaysOnAdminAPI 钉住测试客户端不走设备的 /mqtt（UI-002）。
//
// 设备那条要 SigV4。控制台拿着的是管理面令牌，接上去会被拒，
// 而且会把两个面焊在一起。
func TestConsoleMQTTStaysOnAdminAPI(t *testing.T) {
	base := filepath.Join("..", "..", "web", "console", "src")
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(base, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	api := read("api.ts")
	view := read("views/mqtt.tsx")
	if !strings.Contains(api, "${API_BASE}/${path}") || !strings.Contains(api, "'mqtt'") {
		t.Fatal("测试客户端的地址应从管理面前缀拼出")
	}
	if !strings.Contains(view, "mqttSocketURL") {
		t.Fatal("页面应使用 mqttSocketURL，而不是自己拼一条套接字")
	}
	if strings.Contains(view, "/api/aws") || strings.Contains(view, "iotdevicegateway") {
		t.Fatal("测试客户端碰到了设备兼容面")
	}
}

// Prometheus 指标端点（OBS-020）。
func TestMetricsEndpoint(t *testing.T) {
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(`{"Statement":[{"Effect":"Allow","Action":["iot:Connect"],`+
			`"Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-m")
	r.start()

	// 制造一些可观测的事实：一次成功连接、一次被拒订阅
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-m"); ack.Code != reason.Success {
		t.Fatalf("连接应成功: %s", ack.Code)
	}
	if code := c.subscribe("x/y"); !code.IsError() {
		t.Fatalf("订阅应被拒，得到 %s", code)
	}

	code, raw := r.adminReq(t, http.MethodGet, "/metrics", nil)
	if code != http.StatusOK {
		t.Fatalf("GET /metrics = %d: %s", code, raw)
	}
	body := string(raw)

	// 必须含 HELP 与 TYPE：缺了 Prometheus 仍能抓，但指标没有说明，
	// 而这些指标的含义不写清楚就没人用对。
	for _, want := range []string{
		"# HELP openiot_connections ",
		"# TYPE openiot_connections gauge",
		"openiot_build_info{",
		"openiot_connections_total{",
		`result="Accepted"`,
		`auth_method="mTLS"`,
		"openiot_handshake_duration_ms_bucket{",
		`le="+Inf"`,
		"openiot_handshake_duration_ms_count{",
		"openiot_subscriptions_total{",
		`result="denied"`,
		"openiot_certificates{",
		"openiot_goroutines{",
		"openiot_stack_inuse_bytes{",
		"openiot_runtime_sys_bytes{",
		"openiot_runtime_memory_limit_bytes{",
		"openiot_meta_revision{",
		"openiot_quota_global_rate_members{",
		"openiot_events_dropped_total{",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("指标输出缺少 %q", want)
		}
	}

	// 每个指标名的 HELP 只能出现一次，且必须在该名字的样本之前。
	// 顺序错了 Prometheus 会拒绝整个响应，而那是静默失败。
	assertExpositionWellFormed(t, body)
}

// 指标端点必须要令牌（OBS-020、API-080 同一理由）。
//
// API-082 豁免 /healthz 与 /readyz 的前提是"不泄露任何信息"，
// 而指标泄露设备数、证书数、拒绝原因分布与运行时细节。
func TestMetricsRequiresToken(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	code, _ := r.adminReqToken(t, http.MethodGet, "/metrics", nil, "")
	if code != http.StatusUnauthorized {
		t.Errorf("无令牌访问 /metrics = %d，想要 401", code)
	}
}

// 未配置令牌时 /metrics 不挂载（与 API-081 同一处理）。
func TestMetricsNotMountedWithoutToken(t *testing.T) {
	// newTestRig 不写令牌文件，因此管理面整体不挂载
	r := newTestRig(t, nil)
	r.start()
	code, _ := r.adminReqToken(t, http.MethodGet, "/metrics", nil, "")
	if code != http.StatusNotFound {
		t.Errorf("未配置令牌时 /metrics = %d，想要 404（不挂载，不是 401）", code)
	}
}

// assertExpositionWellFormed 校验文本暴露格式的结构约束。
func assertExpositionWellFormed(t *testing.T, body string) {
	t.Helper()
	helpSeen := map[string]int{}
	typeSeen := map[string]int{}
	sampleBefore := map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		switch {
		case line == "":
		case strings.HasPrefix(line, "# HELP "):
			name := field(line, 2)
			helpSeen[name]++
			if sampleBefore[name] {
				t.Errorf("%s 的 HELP 出现在样本之后", name)
			}
		case strings.HasPrefix(line, "# TYPE "):
			typeSeen[field(line, 2)]++
		case strings.HasPrefix(line, "#"):
		default:
			name, _, _ := strings.Cut(line, "{")
			name, _, _ = strings.Cut(name, " ")
			// 直方图的 _bucket/_sum/_count 共用基名
			base := name
			for _, suf := range []string{"_bucket", "_sum", "_count"} {
				base = strings.TrimSuffix(base, suf)
			}
			sampleBefore[base] = true
			if helpSeen[base] == 0 {
				t.Errorf("样本 %s 没有对应的 HELP", name)
			}
		}
	}
	for name, n := range helpSeen {
		if n > 1 {
			t.Errorf("%s 的 HELP 出现了 %d 次，格式要求只能一次", name, n)
		}
		if typeSeen[name] != 1 {
			t.Errorf("%s 的 TYPE 出现了 %d 次，想要 1", name, typeSeen[name])
		}
	}
}

func field(s string, i int) string {
	f := strings.Fields(s)
	if i >= len(f) {
		return ""
	}
	return f[i]
}
