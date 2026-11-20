package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/registry"
)

// principalRig 建好一份按设备收敛的策略与两台设备。
func principalRig(t *testing.T) *testRig {
	t.Helper()
	r := newAdminRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(`{"Statement":[
          {"Effect":"Allow","Action":["iot:Connect"],
           "Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"},
          {"Effect":"Allow","Action":["iot:Subscribe"],
           "Resource":"arn:aws:iot:*:*:topicfilter/d/${iot:Connection.Thing.ThingName}/*"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	return r
}

func firstFP(t *testing.T, r *testRig) string {
	t.Helper()
	page, err := r.s.reg2.ListCertificates(registry.CertQuery{})
	if err != nil || len(page.Certificates) == 0 {
		t.Fatalf("取证书失败: %v", err)
	}
	return page.Certificates[0].Fingerprint
}

// 改绑证书必须断开使用它的活连接。
//
// 这是 PKI-030（吊销即时断连）的同一类：连接的
// ${iot:Connection.Thing.ThingName} 在握手时解析并固定。把证书改绑到
// 另一台设备之后，旧连接仍以旧设备的身份收发——凭据已经移走，权限
// 却留在原处。运维做的是"把这张证书给另一台设备"，不会预期旧设备
// 还能继续用它。
func TestRebindDisconnectsLiveConnection(t *testing.T) {
	r := principalRig(t)
	cert := r.issue("thing-a")
	r.start()
	if code, b := r.adminReq(t, http.MethodPut, "/api/admin/v1/things/thing-b",
		map[string]any{"policyName": "device"}); code >= 300 {
		t.Fatalf("建 thing-b 失败 %d: %s", code, b)
	}

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("thing-a"); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	if code := c.subscribe("d/thing-a/x"); code.IsError() {
		t.Fatalf("应能订阅自己设备的主题: %s", code)
	}

	fp := firstFP(t, r)
	if code, b := r.adminReq(t, http.MethodPut,
		"/api/admin/v1/things/thing-b/principals/"+fp, nil); code != http.StatusNoContent {
		t.Fatalf("改绑失败 %d: %s", code, b)
	}

	// 连接须被断开。
	//
	// 断言"连接不在了"而不是"订阅被拒"：前者才是要保证的性质，
	// 后者还取决于夹具如何呈现一个已被服务端关闭的连接。
	waitConnGone(t, r, "thing-a",
		"证书已改绑到 thing-b，旧连接仍在 —— 凭据移走了而权限没有跟着走")
}

// waitConnGone 等到某个 clientId 的连接从活连接列表里消失。
func waitConnGone(t *testing.T, r *testRig, clientID, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		code, body := r.adminReq(t, http.MethodGet,
			"/api/admin/v1/connections/"+clientID, nil)
		if code == http.StatusNotFound {
			return
		}
		if code != http.StatusOK {
			t.Fatalf("查连接失败 %d: %s", code, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// 解绑同样须断连：解绑后连接仍持有一个已不属于它的设备身份。
func TestDetachDisconnectsLiveConnection(t *testing.T) {
	r := principalRig(t)
	cert := r.issue("thing-a")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("thing-a"); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	if code := c.subscribe("d/thing-a/x"); code.IsError() {
		t.Fatalf("应能订阅: %s", code)
	}

	fp := firstFP(t, r)
	if code, b := r.adminReq(t, http.MethodDelete,
		"/api/admin/v1/things/thing-a/principals/"+fp, nil); code != http.StatusNoContent {
		t.Fatalf("解绑失败 %d: %s", code, b)
	}
	waitConnGone(t, r, "thing-a", "解绑后旧连接仍在")
}

// 幂等改绑不得断连。
//
// 把证书绑到它已经绑着的那台设备上是空操作，不该把设备踢下线——
// 否则一个重复执行的配置脚本会周期性地断开全网设备。
func TestAttachToSameThingIsNoop(t *testing.T) {
	r := principalRig(t)
	cert := r.issue("thing-a")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("thing-a"); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	fp := firstFP(t, r)
	for i := 0; i < 3; i++ {
		if code, b := r.adminReq(t, http.MethodPut,
			"/api/admin/v1/things/thing-a/principals/"+fp, nil); code != http.StatusNoContent {
			t.Fatalf("重复绑定应成功 %d: %s", code, b)
		}
	}
	time.Sleep(400 * time.Millisecond)
	if code, _ := r.adminReq(t, http.MethodGet,
		"/api/admin/v1/connections/thing-a", nil); code != http.StatusOK {
		t.Errorf("绑到同一台设备是空操作，不该断连，连接查询返回 %d", code)
	}
	if code := c.subscribe("d/thing-a/x"); code.IsError() {
		t.Errorf("空操作后订阅应仍然放行，得到 %s", code)
	}
}

// 解绑时设备名不符须报错，不得静默成功。
//
// 调用方多半拿的是过期信息；让它以为解绑了某个其实没动的关联，
// 比报错更危险。
func TestDetachRejectsWrongThing(t *testing.T) {
	r := principalRig(t)
	r.issue("thing-a")
	r.start()
	if code, b := r.adminReq(t, http.MethodPut, "/api/admin/v1/things/thing-b",
		map[string]any{"policyName": "device"}); code >= 300 {
		t.Fatalf("建 thing-b 失败 %d: %s", code, b)
	}
	fp := firstFP(t, r)
	code, body := r.adminReq(t, http.MethodDelete,
		"/api/admin/v1/things/thing-b/principals/"+fp, nil)
	if code == http.StatusNoContent {
		t.Error("设备名不符时解绑不应成功")
	}
	t.Logf("拒绝响应 %d: %s", code, body)
}

// 两个方向的列举。
func TestListPrincipalsBothDirections(t *testing.T) {
	r := principalRig(t)
	r.issue("thing-a")
	r.start()
	fp := firstFP(t, r)

	code, body := r.adminReq(t, http.MethodGet,
		"/api/admin/v1/things/thing-a/principals", nil)
	if code != http.StatusOK {
		t.Fatalf("列举设备的证书失败 %d: %s", code, body)
	}
	var fwd struct {
		Principals []string `json:"principals"`
	}
	if err := json.Unmarshal(body, &fwd); err != nil {
		t.Fatal(err)
	}
	if len(fwd.Principals) != 1 || fwd.Principals[0] != fp {
		t.Errorf("设备的证书 = %v，想要 [%s]", fwd.Principals, fp)
	}

	code, body = r.adminReq(t, http.MethodGet,
		"/api/admin/v1/certificates/"+fp+"/things", nil)
	if code != http.StatusOK {
		t.Fatalf("列举证书的设备失败 %d: %s", code, body)
	}
	var rev struct {
		Things []string `json:"things"`
	}
	if err := json.Unmarshal(body, &rev); err != nil {
		t.Fatal(err)
	}
	if len(rev.Things) != 1 || rev.Things[0] != "thing-a" {
		t.Errorf("证书的设备 = %v，想要 [thing-a]", rev.Things)
	}

	// 解绑后反向列举须是空数组而不是 null
	if code, b := r.adminReq(t, http.MethodDelete,
		"/api/admin/v1/things/thing-a/principals/"+fp, nil); code != http.StatusNoContent {
		t.Fatalf("解绑失败 %d: %s", code, b)
	}
	_, body = r.adminReq(t, http.MethodGet,
		"/api/admin/v1/certificates/"+fp+"/things", nil)
	if string(body) == "" || !jsonHasEmptyArray(t, body, "things") {
		t.Errorf("空结果应是 []，不是 null：%s", body)
	}
}

func jsonHasEmptyArray(t *testing.T, body []byte, key string) bool {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	return string(m[key]) == "[]"
}

// 关联操作须要令牌。
func TestPrincipalOpsRequireToken(t *testing.T) {
	r := principalRig(t)
	r.issue("thing-a")
	r.start()
	fp := firstFP(t, r)
	for _, c := range []struct{ method, path string }{
		{http.MethodPut, "/api/admin/v1/things/thing-a/principals/" + fp},
		{http.MethodDelete, "/api/admin/v1/things/thing-a/principals/" + fp},
		{http.MethodGet, "/api/admin/v1/things/thing-a/principals"},
		{http.MethodGet, "/api/admin/v1/certificates/" + fp + "/things"},
	} {
		if code, _ := r.adminReqToken(t, c.method, c.path, nil, ""); code != http.StatusUnauthorized {
			t.Errorf("%s %s 无令牌 = %d，想要 401", c.method, c.path, code)
		}
	}
}
