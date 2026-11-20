package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRolesGateWritesAndCapture 验证三档角色。
//
// 只读用户若能删除设备，或者运维能打开抓包，角色就没有落在请求上。
// 引导令牌的审计操作者仍是 admin，额外用户记自己的名字，不记令牌。
func TestRolesGateWritesAndCapture(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()

	code, raw := r.adminReq(t, "PUT", adminAPIPrefix+"/users/ada", map[string]string{"role": "reader"})
	if code != 200 {
		t.Fatalf("创建只读用户 = %d %s", code, raw)
	}
	var reader struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &reader); err != nil || reader.Token == "" {
		t.Fatal(raw)
	}
	code, raw = r.adminReq(t, "PUT", adminAPIPrefix+"/users/bea", map[string]string{"role": "operator"})
	if code != 200 {
		t.Fatalf("创建运维用户 = %d %s", code, raw)
	}
	var op struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &op); err != nil || op.Token == "" {
		t.Fatal(raw)
	}
	st, err := os.Stat(filepath.Join(r.dd.Root(), "users.json"))
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("users.json 权限 = %v %v", st, err)
	}

	code, _ = r.adminReq(t, "PUT", adminAPIPrefix+"/things/lamp", map[string]any{})
	if code != 200 && code != 204 {
		t.Fatalf("管理员创建设备 = %d", code)
	}
	code, raw = r.adminReqToken(t, "DELETE", adminAPIPrefix+"/things/lamp", nil, reader.Token)
	if code != 403 || !strings.Contains(string(raw), "当前角色不能做这件事") {
		t.Fatalf("只读删除 = %d %s", code, raw)
	}
	code, raw = r.adminReqToken(t, "GET", adminAPIPrefix+"/capture", nil, reader.Token)
	if code != 403 {
		t.Fatalf("只读抓包 = %d %s", code, raw)
	}
	code, raw = r.adminReqToken(t, "GET", adminAPIPrefix+"/capture", nil, op.Token)
	if code != 403 {
		t.Fatalf("运维抓包 = %d %s", code, raw)
	}
	code, raw = r.adminReq(t, "GET", adminAPIPrefix+"/capture", nil)
	if code != 400 {
		t.Fatalf("管理员抓包在升级之前应是 400，得到 %d %s", code, raw)
	}

	code, raw = r.adminReqToken(t, "PUT", adminAPIPrefix+"/things/meter", map[string]any{}, op.Token)
	if code != 200 && code != 204 {
		t.Fatalf("运维创建设备 = %d %s", code, raw)
	}
	audit := waitAudit(t, filepath.Join(r.dd.Root(), auditLogName), `"actor":"bea"`)
	if !strings.Contains(audit, `"actor":"admin"`) {
		t.Fatal("引导令牌的操作者应当仍是 admin")
	}
	if strings.Contains(audit, reader.Token) || strings.Contains(audit, op.Token) {
		t.Fatal("审计文件里出现了令牌")
	}
	if !strings.Contains(audit, `"actor":"bea"`) {
		t.Fatalf("运维的写操作没有记到本人名下: %s", audit)
	}
}

// TestTimelineReadsEventLog 验证时间线读的是事件日志文件。
//
// 只查内存环的话，这行只存在于磁盘上的 clientId 不会出现在响应里。
func TestTimelineReadsEventLog(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	path := filepath.Join(r.dd.Root(), eventLogName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	line := `{"kind":"conn","node":"n1","seq":9,"at":1,"clientId":"from-disk","thing":"lamp"}` + "\n"
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	code, raw := r.adminReq(t, "GET", adminAPIPrefix+"/things/lamp/timeline", nil)
	if code != 200 || !strings.Contains(string(raw), "from-disk") {
		t.Fatalf("时间线 = %d %s", code, raw)
	}
	code, raw = r.adminReq(t, "GET", adminAPIPrefix+"/things/missing/timeline", nil)
	var empty struct {
		Events []sseEvent `json:"events"`
	}
	if code != 200 || json.Unmarshal(raw, &empty) != nil || empty.Events == nil {
		t.Fatalf("空时间线 = %d %s", code, raw)
	}
}

// TestCaptureWebSocketSeesPublish 验证抓包 WebSocket 在没有设备订阅者时仍能看到发布。
func TestCaptureWebSocketSeesPublish(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	ws := dialAdminWS(t, r.s.AdminAddr(), adminAPIPrefix+"/capture", testAdminToken)
	defer ws.Close()
	if ws.readJSON(t)["op"] != "ready" {
		t.Fatal("没有 ready")
	}
	ws.sendJSON(t, map[string]any{"op": "watch", "filter": "floor/#"})
	if ws.readJSON(t)["op"] != "watching" {
		t.Fatal("没有 watching")
	}
	if err := r.s.brk.PublishData(context.Background(), "floor/1", []byte("hi"), 0, false); err != nil {
		t.Fatal(err)
	}
	msg := ws.readJSON(t)
	raw, err := base64.StdEncoding.DecodeString(msg["payload"].(string))
	if err != nil || msg["topic"] != "floor/1" || string(raw) != "hi" {
		t.Fatalf("抓包帧 = %v %q", msg, raw)
	}
}
