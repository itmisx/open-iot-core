package broker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

// TestConnDetailShowsSubscriptionsAndInflight 验证连接详情带上订阅和出站在途，
// 并且不带消息负载。
//
// 拿掉 ConnDetail 里对订阅快照或在途快照的读取，过滤器和主题就消失，本测试失败。
// 把负载字段加进运维视图，负载会进 JSON，本测试同样失败。
func TestConnDetailShowsSubscriptionsAndInflight(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", false)
	sub.subscribe("floor/#", packet.QoS1)

	d, ok := r.b.ConnDetail("sub")
	if !ok {
		t.Fatal("连接不在线")
	}
	if len(d.Subscriptions) != 1 || d.Subscriptions[0].Filter != "floor/#" || d.Subscriptions[0].QoS != 1 {
		t.Fatalf("订阅 = %+v", d.Subscriptions)
	}
	if len(d.Inflight) != 0 {
		t.Fatalf("还没投递就有在途 %+v", d.Inflight)
	}

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	if rc := pub.publish("floor/temp", "secret-payload", packet.QoS1, false); rc.IsError() {
		t.Fatal(rc)
	}
	// 不确认，在途才留得住。
	if got := recvPublish(sub); string(got.Payload) != "secret-payload" {
		t.Fatalf("负载 = %q", got.Payload)
	}
	d, ok = r.b.ConnDetail("sub")
	if !ok || len(d.Inflight) != 1 {
		t.Fatalf("在途 = %+v ok=%v", d.Inflight, ok)
	}
	if d.Inflight[0].Topic != "floor/temp" || d.Inflight[0].State != "puback" || d.Inflight[0].QoS != 1 {
		t.Fatalf("在途 = %+v", d.Inflight[0])
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret-payload") {
		t.Fatalf("在途详情带了负载: %s", raw)
	}
}
