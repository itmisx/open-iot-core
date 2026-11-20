package server

import (
	"encoding/json"
	"testing"
)

// TestConsoleCriticalFlows 把控制台的四条关键流程串在同一次启动里：
// 登录、设备列表、策略调试、MQTT 测试客户端。
//
// 拆开测时，每一条都可能还在，整页走不通却全绿。拿掉其中任一处理函数，
// 本测试在对应的一步失败。页面本身没有在浏览器里点过。
func TestConsoleCriticalFlows(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()

	code, body := r.adminReq(t, "GET", adminAPIPrefix+"/session", nil)
	if code != 200 {
		t.Fatalf("登录 = %d: %s", code, body)
	}
	var who struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	if err := json.Unmarshal(body, &who); err != nil {
		t.Fatal(err)
	}
	if who.Name != "admin" || who.Role != "admin" {
		t.Fatalf("会话 = %+v", who)
	}

	code, body = r.adminReq(t, "PUT", adminAPIPrefix+"/things/lamp", map[string]any{})
	if code != 204 && code != 200 {
		t.Fatalf("登记设备 = %d: %s", code, body)
	}
	code, body = r.adminReq(t, "GET", adminAPIPrefix+"/things", nil)
	if code != 200 {
		t.Fatalf("设备列表 = %d: %s", code, body)
	}
	var page struct {
		Things []struct {
			Name string `json:"name"`
		} `json:"things"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, th := range page.Things {
		if th.Name == "lamp" {
			found = true
		}
	}
	if !found {
		t.Fatalf("设备列表没有 lamp: %s", body)
	}

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
	code, body = r.adminReq(t, "PUT", adminAPIPrefix+"/policies/flow", doc)
	if code != 204 {
		t.Fatalf("写策略 = %d: %s", code, body)
	}
	code, body = r.adminReq(t, "POST", adminAPIPrefix+"/authz/simulate", map[string]any{
		"policyNames": []string{"flow"},
		"action":      "iot:Publish",
		"resource":    "floor/temp",
	})
	if code != 200 {
		t.Fatalf("策略调试 = %d: %s", code, body)
	}
	var sim struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.Unmarshal(body, &sim); err != nil {
		t.Fatal(err)
	}
	if !sim.Allowed {
		t.Fatalf("应放行: %s", body)
	}

	ws := dialConsoleMQTT(t, r.s.AdminAddr(), testAdminToken)
	defer ws.Close()
	ready := ws.readJSON(t)
	if ready["op"] != "ready" {
		t.Fatalf("MQTT 客户端首帧 = %v", ready)
	}
	ws.sendJSON(t, map[string]any{"op": "subscribe", "filter": "floor/#", "qos": 0})
	if ack := ws.readJSON(t); ack["op"] != "suback" {
		t.Fatalf("订阅 = %v", ack)
	}
	ws.sendJSON(t, map[string]any{
		"op": "publish", "topic": "floor/temp", "qos": 0,
		"payload": "aGk=",
	})
	var published, echoed bool
	for i := 0; i < 6 && !(published && echoed); i++ {
		frame := ws.readJSON(t)
		switch frame["op"] {
		case "published":
			published = true
		case "message":
			echoed = frame["topic"] == "floor/temp"
		default:
			t.Fatalf("意外的帧: %v", frame)
		}
	}
	if !published || !echoed {
		t.Fatal("MQTT 测试客户端没有把消息送回")
	}
}
