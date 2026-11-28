package broker

import (
	"context"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/session"
)

// recvPublish 读取一条投递，不回确认。
//
// expectPublish 会立刻 ACK。重传与在途额度要看的正是「还没确认」这段，
// 用它就把被测的状态清掉了。
func recvPublish(c *testClient) *packet.Publish {
	c.t.Helper()
	return c.expect(packet.PUBLISH).(*packet.Publish)
}

func assertInflight(t *testing.T, r *rig, clientID string, want int64) {
	t.Helper()
	if cur := r.lim.Current(quota.OutboundInflight, clientID); cur != want {
		t.Fatalf("%s 的出站在途额度 = %d，应为 %d", clientID, cur, want)
	}
}

// waitInflight 等到额度变成 want。
//
// 确认是客户端写出去的，服务端在自己的读循环里处理，调用方返回时
// 额度往往还没变。
func waitInflight(t *testing.T, r *rig, clientID string, want int64) {
	t.Helper()
	waitFor(t, time.Second, func() bool {
		return r.lim.Current(quota.OutboundInflight, clientID) == want
	})
	assertInflight(t, r, clientID, want)
}

// syncConn 用一次往返把已经写出去的确认挡在它前面处理完。
func syncConn(c *testClient) {
	c.t.Helper()
	c.send(&packet.Pingreq{})
	c.expect(packet.PINGRESP)
}

func dropConn(r *rig, c *testClient, remain int64) {
	c.t.Helper()
	_ = c.nc.Close()
	r.waitConns(remain)
}

// TestQoS0DoesNotTakeInflightQuota 验证 QoS 0 不占出站在途窗口。
func TestQoS0DoesNotTakeInflightQuota(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/q0", packet.QoS0)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/q0", "x", packet.QoS0, false)
	if got := sub.expectPublish(); string(got.Payload) != "x" {
		t.Fatalf("负载 = %q", got.Payload)
	}
	assertInflight(t, r, "sub", 0)
}

// TestPersistentResumeRedeliversSamePacket 验证续上会话时按原标识、带 DUP 重投，
// 并且额度继续占着，不会再占一份。
func TestPersistentResumeRedeliversSamePacket(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", false)
	sub.subscribe("t/keep", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	if rc := pub.publish("t/keep", "keep", packet.QoS1, false); rc.IsError() {
		t.Fatalf("发布失败: %s", rc)
	}
	first := recvPublish(sub)
	assertInflight(t, r, "sub", 1)

	dropConn(r, sub, 1)
	// 断开之后、重连之前额度必须还在。在 persist 里顺手归还的话，
	// 这里就是 0，而消息仍在会话里——下次重投会再占一份，或者永远占不回来。
	assertInflight(t, r, "sub", 1)

	r.clk.advance(2 * time.Second)
	sub2 := r.dial(ConnInfo{})
	if ack := sub2.connect("sub", false); !ack.SessionPresent {
		t.Fatal("续上会话应报告会话已存在")
	}
	again := recvPublish(sub2)
	if again.PacketID != first.PacketID || !again.Dup || string(again.Payload) != "keep" {
		t.Fatalf("重投 = id %d dup %v 负载 %q，原来是 id %d",
			again.PacketID, again.Dup, again.Payload, first.PacketID)
	}
	assertInflight(t, r, "sub", 1)

	sub2.send(&packet.Ack{Kind: packet.PUBACK, PacketID: again.PacketID})
	waitInflight(t, r, "sub", 0)
}

// TestCleanStartReleasesInflightQuota 验证清洁会话丢掉未确认消息时把额度一起放掉。
func TestCleanStartReleasesInflightQuota(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", false)
	sub.subscribe("t/clean", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/clean", "x", packet.QoS1, false)
	recvPublish(sub)
	assertInflight(t, r, "sub", 1)

	dropConn(r, sub, 1)
	r.clk.advance(2 * time.Second)
	sub2 := r.dial(ConnInfo{})
	if ack := sub2.connect("sub", true); ack.SessionPresent {
		t.Fatal("clean start 不应报告会话已存在")
	}
	assertInflight(t, r, "sub", 0)
	sub2.expectNothing(300 * time.Millisecond)
}

// TestExpiredSessionReleasesInflightQuota 验证会话过期后再连，在途额度被放掉。
func TestExpiredSessionReleasesInflightQuota(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	cp := &packet.Connect{Version: packet.V5, ClientID: []byte("sub")}
	cp.Props.SessionExpiry = 10
	cp.Props.Set(packet.PropSessionExpiry)
	sub.send(cp)
	sub.expect(packet.CONNACK)
	sub.subscribe("t/exp", packet.QoS1)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/exp", "x", packet.QoS1, false)
	recvPublish(sub)
	assertInflight(t, r, "sub", 1)

	dropConn(r, sub, 1)
	assertInflight(t, r, "sub", 1)

	r.clk.advance(2 * time.Minute)
	sub2 := r.dial(ConnInfo{})
	if ack := sub2.connect("sub", false); ack.SessionPresent {
		t.Fatal("会话已过期，不应报告会话已存在")
	}
	assertInflight(t, r, "sub", 0)
	sub2.expectNothing(300 * time.Millisecond)
}

// TestOnlineRetrySetsDupUntilAck 验证在线重试带 DUP、沿用标识，确认之后不再发。
func TestOnlineRetrySetsDupUntilAck(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/retry", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/retry", "x", packet.QoS1, false)
	first := recvPublish(sub)
	if first.Dup {
		t.Fatal("首次发出不应带 DUP")
	}

	r.advance(time.Second)
	again := recvPublish(sub)
	if again.PacketID != first.PacketID || !again.Dup || string(again.Payload) != "x" {
		t.Fatalf("重试 = id %d dup %v 负载 %q", again.PacketID, again.Dup, again.Payload)
	}
	assertInflight(t, r, "sub", 1)

	sub.send(&packet.Ack{Kind: packet.PUBACK, PacketID: again.PacketID})
	waitInflight(t, r, "sub", 0)
	r.advance(2 * time.Second)
	sub.expectNothing(300 * time.Millisecond)
}

// TestRetryBudgetIsSixteenMinutesOnMQTT5 验证 MQTT 5 到 16 分钟就放弃并归还额度。
//
// 一次推进跨过整个预算。定时器是按 1 秒登记的，回调必须拿当前时刻和
// 放弃时刻比，不能假设自己是准点醒来的。
func TestRetryBudgetIsSixteenMinutesOnMQTT5(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/budget", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/budget", "x", packet.QoS1, false)
	recvPublish(sub)
	assertInflight(t, r, "sub", 1)

	r.advance(16 * time.Minute)
	assertInflight(t, r, "sub", 0)
	sub.expectNothing(300 * time.Millisecond)
}

// TestRetryBudgetIsOneHourOnMQTT3 验证 MQTT 3 的放弃时限是 1 小时，不是 16 分钟。
func TestRetryBudgetIsOneHourOnMQTT3(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.ver = packet.V311
	sub.connect("sub311", false)
	sub.subscribe("t/budget3", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/budget3", "x", packet.QoS1, false)
	first := recvPublish(sub)

	r.advance(16 * time.Minute)
	again := recvPublish(sub)
	if again.PacketID != first.PacketID || !again.Dup {
		t.Fatalf("16 分钟时仍应重试，得到 id %d dup %v", again.PacketID, again.Dup)
	}
	assertInflight(t, r, "sub311", 1)

	r.advance(time.Hour)
	assertInflight(t, r, "sub311", 0)
	sub.expectNothing(300 * time.Millisecond)
}

// TestRestartAcquiresInflightDeficit 验证重启后计数器是空的，重投前先补占额度。
func TestRestartAcquiresInflightDeficit(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	if err := r.sess.Save(context.Background(), &session.State{
		ClientID:         "rebooted",
		Owner:            "node-1",
		ProtoVer:         5,
		ExpiryInterval:   3600,
		DisconnectUnixMs: r.clk.now(),
		Subscriptions: []session.Subscription{
			{Filter: "t/restart", QoS: 1},
		},
		Inflight: []session.InflightMsg{{
			PacketID:     7,
			QoS:          1,
			State:        session.AwaitPuback,
			Topic:        "t/restart",
			Payload:      []byte("again"),
			SentUnixMs:   r.clk.now(),
			ExpiryUnixMs: r.clk.now() + int64(time.Hour/time.Millisecond),
		}},
	}); err != nil {
		t.Fatalf("预置会话失败: %v", err)
	}
	assertInflight(t, r, "rebooted", 0)

	c := r.dial(ConnInfo{})
	if ack := c.connect("rebooted", false); !ack.SessionPresent {
		t.Fatal("存储里的持久会话应被恢复")
	}
	got := recvPublish(c)
	if got.PacketID != 7 || !got.Dup || string(got.Payload) != "again" {
		t.Fatalf("重投 = id %d dup %v 负载 %q", got.PacketID, got.Dup, got.Payload)
	}
	assertInflight(t, r, "rebooted", 1)
}

// TestTakeoverAdoptsLiveInflight 验证接管重投的是运行时里的在途消息，不是存储里那份。
//
// 会话在 CONNECT 时落盘，此后不再落盘。未确认的消息只在内存里。
// 被抢占的连接 persist 会提前返回，存储里始终没有它。不接运行时的话，
// 这条消息就丢了，额度也没人还。
func TestTakeoverAdoptsLiveInflight(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	first := r.dial(ConnInfo{})
	first.connect("sub", false)
	first.subscribe("t/take", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/take", "live", packet.QoS1, false)
	sent := recvPublish(first)
	assertInflight(t, r, "sub", 1)

	r.clk.advance(2 * time.Second)
	second := r.dial(ConnInfo{})
	if ack := second.connect("sub", false); !ack.SessionPresent {
		t.Fatal("接管活会话应报告会话已存在")
	}
	got := recvPublish(second)
	if got.PacketID != sent.PacketID || !got.Dup || string(got.Payload) != "live" {
		t.Fatalf("接管重投 = id %d dup %v 负载 %q，原来是 id %d",
			got.PacketID, got.Dup, got.Payload, sent.PacketID)
	}
	assertInflight(t, r, "sub", 1)
	first.expectClosed(testPatience)
}

// TestOutboundWindowQueuesInsteadOfDropping 验证窗口满时先排队，确认后再发出。
func TestOutboundWindowQueuesInsteadOfDropping(t *testing.T) {
	r := newPolicyRig(t, policyFull, map[quota.ID]int{quota.OutboundInflight: 1})
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/win", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/win", "one", packet.QoS1, false)
	first := recvPublish(sub)
	pub.publish("t/win", "two", packet.QoS1, false)
	sub.expectNothing(300 * time.Millisecond)
	assertInflight(t, r, "sub", 1)

	sub.send(&packet.Ack{Kind: packet.PUBACK, PacketID: first.PacketID})
	second := sub.expectPublish()
	if string(second.Payload) != "two" {
		t.Fatalf("窗口腾出后应发出第二条，得到 %q", second.Payload)
	}
	waitInflight(t, r, "sub", 0)
}

// TestDuplicatePubackDoesNotReleaseTwice 验证重复确认不会把额度多还一份。
func TestDuplicatePubackDoesNotReleaseTwice(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/dupack", packet.QoS1)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/dupack", "a", packet.QoS1, false)
	pub.publish("t/dupack", "b", packet.QoS1, false)
	a := recvPublish(sub)
	b := recvPublish(sub)
	assertInflight(t, r, "sub", 2)

	sub.send(&packet.Ack{Kind: packet.PUBACK, PacketID: a.PacketID})
	waitInflight(t, r, "sub", 1)
	sub.send(&packet.Ack{Kind: packet.PUBACK, PacketID: a.PacketID})
	syncConn(sub)
	assertInflight(t, r, "sub", 1)
	sub.send(&packet.Ack{Kind: packet.PUBACK, PacketID: b.PacketID})
	waitInflight(t, r, "sub", 0)
}

// TestQoS2RetryResendsPubrel 验证 QoS 2 收到 PUBREC 之后，重试发的是 PUBREL。
func TestQoS2RetryResendsPubrel(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("t/qos2", packet.QoS2)
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	if rc := pub.publish("t/qos2", "x", packet.QoS2, false); rc.IsError() {
		t.Fatalf("发布失败: %s", rc)
	}
	pp := recvPublish(sub)
	if pp.QoS != packet.QoS2 {
		t.Fatalf("QoS = %d", pp.QoS)
	}
	sub.send(&packet.Ack{Kind: packet.PUBREC, PacketID: pp.PacketID})
	rel := sub.expect(packet.PUBREL).(*packet.Ack)
	if rel.PacketID != pp.PacketID {
		t.Fatalf("PUBREL 标识 = %d，应为 %d", rel.PacketID, pp.PacketID)
	}
	assertInflight(t, r, "sub", 1)

	r.advance(time.Second)
	rel2 := sub.expect(packet.PUBREL).(*packet.Ack)
	if rel2.PacketID != pp.PacketID {
		t.Fatalf("重试的 PUBREL 标识 = %d", rel2.PacketID)
	}
	sub.send(&packet.Ack{Kind: packet.PUBCOMP, PacketID: rel2.PacketID})
	waitInflight(t, r, "sub", 0)
	r.advance(2 * time.Second)
	sub.expectNothing(300 * time.Millisecond)
}
