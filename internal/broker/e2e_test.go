package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/session"
)

// TestEndToEndPubSub 是最基本的端到端链路：连得上、订阅得了、收发得到。
func TestEndToEndPubSub(t *testing.T) {
	for _, qos := range []packet.QoS{packet.QoS0, packet.QoS1, packet.QoS2} {
		t.Run("QoS"+string(rune('0'+qos)), func(t *testing.T) {
			r := newPolicyRig(t, policyFull, nil)

			sub := r.dial(ConnInfo{})
			if ack := sub.connect("subscriber", true); ack.Code != reason.Success {
				t.Fatalf("订阅端连接失败: %s", ack.Code)
			}
			if rc := sub.subscribe("sensors/temp", qos); rc.IsError() {
				t.Fatalf("订阅失败: %s", rc)
			}

			pub := r.dial(ConnInfo{})
			if ack := pub.connect("publisher", true); ack.Code != reason.Success {
				t.Fatalf("发布端连接失败: %s", ack.Code)
			}
			if rc := pub.publish("sensors/temp", "21.5", qos, false); rc.IsError() {
				t.Fatalf("发布失败: %s", rc)
			}

			got := sub.expectPublish()
			if string(got.Topic) != "sensors/temp" || string(got.Payload) != "21.5" {
				t.Errorf("收到 %q / %q", got.Topic, got.Payload)
			}
			// 投递 QoS 是发布 QoS 与订阅 QoS 的较小者
			if got.QoS != qos {
				t.Errorf("投递 QoS = %d，期望 %d", got.QoS, qos)
			}
		})
	}
}

// TestQoSDowngradedToSubscription 验证投递 QoS 取两者较小值（MQTT-3.8.4）。
func TestQoSDowngradedToSubscription(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("a/b", packet.QoS0); rc != reason.GrantedQoS0 {
		t.Fatalf("SUBACK = %s", rc)
	}
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("a/b", "x", packet.QoS2, false)

	got := sub.expectPublish()
	if got.QoS != packet.QoS0 {
		t.Errorf("投递 QoS = %d，期望降级为 0", got.QoS)
	}
}

// TestPolicyDeniesPublish 验证策略拒绝发布，且 PUBACK 携带准确原因码。
func TestPolicyDeniesPublish(t *testing.T) {
	r := newPolicyRig(t, policyDeviceScoped, nil)

	c := r.dial(ConnInfo{})
	if ack := c.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	// 自己名下的主题放行
	if rc := c.publish("$aws/things/dev-1/shadow/update", "{}", packet.QoS1, false); rc.IsError() {
		t.Errorf("自己名下的主题应放行，得到 %s", rc)
	}
	// 别人名下的主题拒绝
	if rc := c.publish("$aws/things/dev-2/shadow/update", "{}", packet.QoS1, false); rc != reason.NotAuthorized {
		t.Errorf("别人名下的主题应被拒，得到 %s", rc)
	}
	// 完全无关的主题拒绝
	if rc := c.publish("random/topic", "x", packet.QoS1, false); rc != reason.NotAuthorized {
		t.Errorf("无关主题应被拒，得到 %s", rc)
	}
}

// TestPolicyDeniesSubscribe 验证策略拒绝订阅，且 SUBACK 逐项返回原因码。
func TestPolicyDeniesSubscribe(t *testing.T) {
	r := newPolicyRig(t, policyDeviceScoped, nil)

	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)

	if rc := c.subscribe("$aws/things/dev-1/shadow/update/delta", packet.QoS1); rc != reason.GrantedQoS1 {
		t.Errorf("自己名下的过滤器应放行，得到 %s", rc)
	}
	if rc := c.subscribe("$aws/things/dev-2/shadow/update/delta", packet.QoS1); rc != reason.NotAuthorized {
		t.Errorf("别人名下的过滤器应被拒，得到 %s", rc)
	}

	// 部分失败：一包里混合放行与拒绝，须逐项返回而不是整包失败
	c.send(&packet.Subscribe{
		PacketID: c.nextPID(),
		Filters: []packet.Subscription{
			{Filter: []byte("$aws/things/dev-1/jobs/notify"), QoS: packet.QoS1},
			{Filter: []byte("forbidden/#"), QoS: packet.QoS1},
			{Filter: []byte("$aws/things/dev-1/shadow/get/accepted"), QoS: packet.QoS0},
		},
	})
	ack := c.expect(packet.SUBACK).(*packet.Suback)
	want := []reason.Code{reason.GrantedQoS1, reason.NotAuthorized, reason.GrantedQoS0}
	if len(ack.Codes) != 3 {
		t.Fatalf("原因码数 = %d，期望 3", len(ack.Codes))
	}
	for i, w := range want {
		if ack.Codes[i] != w {
			t.Errorf("第 %d 项原因码 = %s，期望 %s", i, ack.Codes[i], w)
		}
	}
}

// TestSubscribeAllowedButReceiveDenied 验证 PRD AUTH-023 在端到端链路上成立：
// 允许订阅不等于允许接收。
func TestSubscribeAllowedButReceiveDenied(t *testing.T) {
	// 只给 Subscribe 与 Publish，不给 Receive
	r := newPolicyRig(t, `{"Statement":[
	  {"Effect":"Allow","Action":["iot:Connect","iot:Publish","iot:Subscribe"],
	   "Resource":"*"}]}`, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("a/b", packet.QoS0); rc.IsError() {
		t.Fatalf("订阅应放行，得到 %s", rc)
	}
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("a/b", "x", packet.QoS0, false)

	// 订阅成功但没有 iot:Receive，消息不应被投递
	sub.expectNothing(300 * time.Millisecond)
}

// TestRetainedDelivery 验证 retained 消息在订阅时下发，且 RETAIN 标志置位。
func TestRetainedDelivery(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	if rc := pub.publish("state/dev-1", "online", packet.QoS1, true); rc.IsError() {
		t.Fatalf("保留发布失败: %s", rc)
	}

	// 后订阅者应立即收到
	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("state/+", packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	got := sub.expectPublish()
	if string(got.Payload) != "online" {
		t.Errorf("负载 = %q", got.Payload)
	}
	// RETAIN 必须置位，使客户端能区分历史状态与刚发生的事件
	if !got.Retain {
		t.Error("retained 投递的 RETAIN 标志应置位")
	}

	// 空负载清除。
	//
	// 每主题的保留发布速率固定 1/秒（PRD QTA-042，对齐 AWS），
	// 因此同一主题的第二次保留发布必须等时钟推进——否则会被配额拒绝。
	// 返回码必须断言：忽略它会让「清除其实没发生」被测试悄悄放过，
	// 本测试最初就是这样误报通过的。
	r.clk.advance(2 * time.Second)
	if rc := pub.publish("state/dev-1", "", packet.QoS1, true); rc.IsError() {
		t.Fatalf("清除保留消息失败: %s", rc)
	}
	if n := r.rt.Retained().Len(); n != 0 {
		t.Fatalf("清除后 retained 条数 = %d，期望 0", n)
	}

	sub2 := r.dial(ConnInfo{})
	sub2.connect("sub2", true)
	sub2.subscribe("state/+", packet.QoS1)
	sub2.expectNothing(300 * time.Millisecond)
}

// TestRetainHandlingTwoSkipsRetained 验证 RetainHandling=2 时订阅不下发 retained。
func TestRetainHandlingTwoSkipsRetained(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("s/x", "v", packet.QoS1, true)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.send(&packet.Subscribe{
		PacketID: sub.nextPID(),
		Filters: []packet.Subscription{
			{Filter: []byte("s/x"), QoS: packet.QoS1, RetainHandling: 2},
		},
	})
	sub.expect(packet.SUBACK)
	sub.expectNothing(300 * time.Millisecond)
}

// TestNoLocalSuppressesEcho 验证 No Local 不把消息投回发布者自己。
func TestNoLocalSuppressesEcho(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	c := r.dial(ConnInfo{})
	c.connect("self", true)
	c.send(&packet.Subscribe{
		PacketID: c.nextPID(),
		Filters: []packet.Subscription{
			{Filter: []byte("echo/test"), QoS: packet.QoS0, NoLocal: true},
		},
	})
	c.expect(packet.SUBACK)

	c.publish("echo/test", "x", packet.QoS0, false)
	c.expectNothing(300 * time.Millisecond)

	// 别人发的仍能收到
	other := r.dial(ConnInfo{})
	other.connect("other", true)
	other.publish("echo/test", "y", packet.QoS0, false)
	if got := c.expectPublish(); string(got.Payload) != "y" {
		t.Errorf("负载 = %q", got.Payload)
	}
}

// TestSessionTakeoverFencesOldConnection 验证契约 01 的 epoch 围栏在
// 端到端链路上生效：同 clientId 的新连接抢占后，旧连接被断开。
func TestSessionTakeoverFencesOldConnection(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	first := r.dial(ConnInfo{})
	if ack := first.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("首个连接失败: %s", ack.Code)
	}

	// 每 clientId 的接入速率固定 1/秒，是防重连风暴的关键设计
	// （PRD QTA-028）。同 clientId 的立即重连本应被限速，
	// 因此这里推进时钟而不是放宽配额——放宽会把被测行为也改掉。
	r.clk.advance(2 * time.Second)

	second := r.dial(ConnInfo{})
	if ack := second.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("抢占连接失败: %s", ack.Code)
	}

	// 旧连接必须被断开，且原因码是「会话被接管」
	if rc := first.expectClosed(testPatience); rc != reason.SessionTakenOver {
		t.Errorf("旧连接的断开原因码 = %s，期望 Session taken over", rc)
	}
	// 新连接仍可用
	if rc := second.subscribe("a/b", packet.QoS1); rc.IsError() {
		t.Errorf("抢占者应可正常使用，订阅得到 %s", rc)
	}
}

// TestRevocationDisconnectsImmediately 是本轮最关键的一条端到端测试。
//
// 它串起了：元数据复制日志 → 热视图更新 → 变更发布 → broker 消费变更
// → 按证书索引找到活连接 → 立即断开。
//
// 吊销必须立即断开所有在用该证书的活连接——仅改状态不断连视为未完成吊销
// （PRD PKI-030）。广播由日志复制完成，不需要额外的跨节点失效通知机制
// （PRD CLU-019）。
func TestRevocationDisconnectsImmediately(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	ps := r.addPolicy(policyDeviceScoped)
	certA, certB := fp(1), fp(2)
	r.addCert(certA, "dev-1", ps, meta.CertActive)
	r.addCert(certB, "dev-2", ps, meta.CertActive)

	getEvents, stop := r.collect(events.Filter{Kinds: events.KindMeta.Mask()})
	defer stop()

	a := r.dial(certInfo(certA, "dev-1"))
	if ack := a.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("dev-1 连接失败: %s", ack.Code)
	}
	b := r.dial(certInfo(certB, "dev-2"))
	if ack := b.connect("dev-2", true); ack.Code != reason.Success {
		t.Fatalf("dev-2 连接失败: %s", ack.Code)
	}

	// 吊销 dev-1 的证书
	r.revoke(certA, "dev-1", ps, "密钥泄露")

	// dev-1 必须被断开，原因码为未授权
	if rc := a.expectClosed(testPatience); rc != reason.NotAuthorized {
		t.Errorf("被吊销连接的原因码 = %s，期望 Not authorized", rc)
	}
	// dev-2 不受影响——吊销只影响在用该证书的连接
	if rc := b.subscribe("$aws/things/dev-2/shadow/get/accepted", packet.QoS1); rc.IsError() {
		t.Errorf("未被吊销的连接应不受影响，订阅得到 %s", rc)
	}

	// 事件总线上应有一条带吊销原因的变更事件，使控制台能直接展示
	// 「这次吊销断开了哪些连接」（PRD PKI-033）
	deadline := time.Now().Add(time.Second)
	var found *events.Event
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Meta.RequiresDisconnect && e.ClientID == "dev-1" {
				ev := e
				found = &ev
				break
			}
		}
		if found != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if found == nil {
		t.Fatal("未收到吊销导致断连的事件")
	}
	if found.Meta.Reason != "密钥泄露" {
		t.Errorf("事件中的吊销原因 = %q", found.Meta.Reason)
	}
	if found.Meta.CertFP != certA.String() {
		t.Errorf("事件中的证书指纹 = %q", found.Meta.CertFP)
	}

	// 吊销后不能再连上
	r.clk.advance(2 * time.Second)
	again := r.dial(certInfo(certA, "dev-1"))
	if ack := again.connect("dev-1", true); ack.Code == reason.Success {
		t.Error("已吊销的证书不应能建立新连接")
	}
}

// TestCertAuthRejectsUnknownAndInactive 验证证书状态与原因码的对应关系。
func TestCertAuthRejectsUnknownAndInactive(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	ps := r.addPolicy(policyFull)

	t.Run("未注册证书", func(t *testing.T) {
		c := r.dial(certInfo(fp(9), "nope"))
		if ack := c.connect("dev-9", true); ack.Code == reason.Success {
			t.Error("未注册证书不应放行")
		}
	})
	t.Run("未激活证书", func(t *testing.T) {
		r.addCert(fp(3), "dev-3", ps, meta.CertInactive)
		c := r.dial(certInfo(fp(3), "dev-3"))
		if ack := c.connect("dev-3", true); ack.Code != reason.NotAuthorized {
			t.Errorf("未激活证书的原因码 = %s", ack.Code)
		}
	})
	t.Run("待激活证书", func(t *testing.T) {
		r.addCert(fp(4), "dev-4", ps, meta.CertPendingActivation)
		c := r.dial(certInfo(fp(4), "dev-4"))
		if ack := c.connect("dev-4", true); ack.Code != reason.NotAuthorized {
			t.Errorf("待激活证书的原因码 = %s", ack.Code)
		}
	})
	t.Run("无证书", func(t *testing.T) {
		c := r.dial(ConnInfo{})
		if ack := c.connect("dev-x", true); ack.Code == reason.Success {
			t.Error("无证书不应放行")
		}
	})
	t.Run("无策略绑定", func(t *testing.T) {
		// 没有任何策略等价于隐式拒绝一切，应在连接时就明确拒绝，
		// 而不是让它在首次发布时才暴露
		r.addCert(fp(5), "dev-5", meta.NoPolicySet, meta.CertActive)
		c := r.dial(certInfo(fp(5), "dev-5"))
		if ack := c.connect("dev-5", true); ack.Code != reason.NotAuthorized {
			t.Errorf("无策略绑定的原因码 = %s", ack.Code)
		}
	})
}

// TestInsecureNoAuthRequiresAcknowledgement 验证「放行一切」的认证器
// 不会被误用。
func TestInsecureNoAuthRequiresAcknowledgement(t *testing.T) {
	r := newRig(t, InsecureNoAuth{}, nil) // 未设置 Acknowledged
	c := r.dial(ConnInfo{})
	if ack := c.connect("dev-1", true); ack.Code != reason.NotAuthorized {
		t.Errorf("未确认的 InsecureNoAuth 应拒绝一切，得到 %s", ack.Code)
	}
}

// TestPersistentSessionRestoresSubscriptions 验证持久会话重连后订阅仍在。
func TestPersistentSessionRestoresSubscriptions(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	first := r.dial(ConnInfo{})
	if ack := first.connect("dev-1", false); ack.SessionPresent {
		t.Error("首次连接不应报告会话已存在")
	}
	first.subscribe("a/b", packet.QoS1)
	first.send(&packet.Disconnect{})
	first.expectClosed(time.Second)
	r.clk.advance(2 * time.Second)

	second := r.dial(ConnInfo{})
	ack := second.connect("dev-1", false)
	if !ack.SessionPresent {
		t.Error("重连应报告会话已存在")
	}

	// 订阅应仍然生效，无需重新订阅
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("a/b", "restored", packet.QoS1, false)
	if got := second.expectPublish(); string(got.Payload) != "restored" {
		t.Errorf("负载 = %q", got.Payload)
	}
}

// TestCleanStartDiscardsSession 验证 clean start 清除旧会话与订阅。
func TestCleanStartDiscardsSession(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	first := r.dial(ConnInfo{})
	first.connect("dev-1", false)
	first.subscribe("a/b", packet.QoS1)
	first.send(&packet.Disconnect{})
	first.expectClosed(time.Second)
	r.clk.advance(2 * time.Second)

	second := r.dial(ConnInfo{})
	if ack := second.connect("dev-1", true); ack.SessionPresent {
		t.Error("clean start 不应报告会话已存在")
	}
	if got := r.sess.Len(); got != 0 {
		t.Fatalf("clean session 不应写入会话存储，当前记录数 = %d", got)
	}
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("a/b", "x", packet.QoS0, false)
	second.expectNothing(300 * time.Millisecond)
}

// TestQuotaRejectsAndEmitsEvent 验证配额拒绝带准确原因码并产出事件。
func TestQuotaRejectsAndEmitsEvent(t *testing.T) {
	r := newPolicyRig(t, policyFull, map[quota.ID]int{quota.PublishRatePerConn: 2})
	getEvents, stop := r.collect(events.Filter{Kinds: events.KindQuota.Mask()})
	defer stop()

	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	for i := 0; i < 2; i++ {
		if rc := c.publish("a/b", "x", packet.QoS1, false); rc.IsError() {
			t.Fatalf("第 %d 条应通过，得到 %s", i+1, rc)
		}
	}
	if rc := c.publish("a/b", "x", packet.QoS1, false); rc != reason.QuotaExceeded {
		t.Errorf("超限应返回 Quota exceeded，得到 %s", rc)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Quota.QuotaName == "publish_rate_per_conn" {
				if e.Quota.Limit != 2 {
					t.Errorf("事件中的限制值 = %d，期望 2", e.Quota.Limit)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("未收到配额拒绝事件")
}

// TestAuthzDenialEmitsTraceWithVariables 验证授权拒绝事件携带变量求值结果。
//
// 这是控制台策略调试器的数据前提：变量求值的中间结果如果不在判定时捕获
// 就无法事后重建（PRD AUTH-035）。
func TestAuthzDenialEmitsTraceWithVariables(t *testing.T) {
	r := newPolicyRig(t, policyDeviceScoped, nil)
	getEvents, stop := r.collect(events.Filter{
		Kinds: events.KindAuthz.Mask(), OnlyDenied: true,
	})
	defer stop()

	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.publish("$aws/things/dev-2/shadow/update", "{}", packet.QoS1, false)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Authz.Decision == events.Allow {
				continue
			}
			if e.Authz.Resource != "$aws/things/dev-2/shadow/update" {
				continue
			}
			vars := e.Authz.VarList()
			if len(vars) == 0 {
				t.Fatal("拒绝事件应携带变量求值结果")
			}
			for _, v := range vars {
				if v.Name == "iot:Connection.Thing.ThingName" {
					if v.Value != "dev-1" {
						t.Errorf("变量求值 = %q，期望 dev-1", v.Value)
					}
					return
				}
			}
			t.Fatalf("变量列表中缺少 ThingName: %+v", vars)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("未收到授权拒绝事件")
}

// TestConnEventCarriesTransportFacts 验证连接事件携带传输层事实。
//
// AWS 只能去日志服务翻晦涩字符串；这里让控制台一屏看完
// （PRD UI-011、ARCH-036）。
func TestConnEventCarriesTransportFacts(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	ps := r.addPolicy(policyFull)
	f := fp(7)
	r.addCert(f, "dev-7", ps, meta.CertActive)

	getEvents, stop := r.collect(events.Filter{Kinds: events.KindConn.Mask()})
	defer stop()

	c := r.dial(certInfo(f, "dev-7.example"))
	if ack := c.connect("dev-7", true); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Conn.Result != events.ConnAccepted {
				continue
			}
			if e.ClientID != "dev-7" || e.Thing != "dev-7" {
				t.Errorf("事件主体 = %q / %q", e.ClientID, e.Thing)
			}
			if e.Conn.CertFP != f.String() {
				t.Errorf("证书指纹 = %q", e.Conn.CertFP)
			}
			if e.Conn.CertCN != "dev-7.example" {
				t.Errorf("证书主题 CN = %q", e.Conn.CertCN)
			}
			if e.Conn.AuthMethod != events.AuthMTLS {
				t.Errorf("认证方式 = %s", e.Conn.AuthMethod)
			}
			if e.Conn.TLSVersion != 0x0304 || e.Conn.ALPN != "x-amzn-mqtt-ca" {
				t.Errorf("TLS 事实 = %#x / %q", e.Conn.TLSVersion, e.Conn.ALPN)
			}
			if e.Conn.HandshakeMS != 1.5 {
				t.Errorf("握手耗时 = %v", e.Conn.HandshakeMS)
			}
			if e.Conn.ProtoVer != 5 {
				t.Errorf("协议版本 = %d", e.Conn.ProtoVer)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("未收到连接接受事件")
}

// TestViewStalenessFailsClosed 验证视图陈旧时对新连接 fail-closed，
// 但不断开已有连接（PRD CLU-023 至 CLU-025）。
func TestViewStalenessFailsClosed(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	r.b.opt.MaxViewStalenessMs = 1000

	// 先建立一条连接，此时视图是新鲜的
	alive := r.dial(ConnInfo{})
	if ack := alive.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}

	// 推进时钟使视图陈旧
	r.clk.advance(5 * time.Second)

	// 新连接被拒
	fresh := r.dial(ConnInfo{})
	if ack := fresh.connect("dev-2", true); ack.Code != reason.ServerUnavailable {
		t.Errorf("视图陈旧时新连接应被拒，得到 %s", ack.Code)
	}
	// 已有连接不受影响——控制面故障不应放大为数据面全网掉线
	if rc := alive.publish("a/b", "x", packet.QoS1, false); rc.IsError() {
		t.Errorf("已有连接应不受影响，发布得到 %s", rc)
	}
}

// TestKickClient 验证运维踢出。
func TestKickClient(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	if !r.b.KickClient("dev-1", reason.AdministrativeAction) {
		t.Fatal("踢出应成功")
	}
	if rc := c.expectClosed(testPatience); rc != reason.AdministrativeAction {
		t.Errorf("断开原因码 = %s", rc)
	}
	if r.b.KickClient("nope", reason.AdministrativeAction) {
		t.Error("踢出不存在的客户端应返回 false")
	}
}

// TestFirstPacketMustBeConnect 验证协议状态机拒绝非 CONNECT 的首个报文。
func TestFirstPacketMustBeConnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.send(&packet.Pingreq{})
	c.expectClosed(testPatience)
}

// TestSecondConnectIsProtocolError 验证一个连接上只能有一个 CONNECT。
func TestSecondConnectIsProtocolError(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.send(&packet.Connect{Version: packet.V5, ClientID: []byte("dev-1"), CleanStart: true})
	if rc := c.expectClosed(testPatience); rc != reason.ProtocolError {
		t.Errorf("断开原因码 = %s，期望 Protocol Error", rc)
	}
}

// TestPingPong 验证保活往返。
func TestPingPong(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	for i := 0; i < 3; i++ {
		c.send(&packet.Pingreq{})
		c.expect(packet.PINGRESP)
	}
}

// TestV311Interop 验证 3.1.1 客户端同样可用。
func TestV311Interop(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.ver = packet.V311
	if ack := sub.connect("sub-311", true); ack.Code != reason.Success {
		t.Fatalf("3.1.1 连接失败: %s", ack.Code)
	}
	if rc := sub.subscribe("a/b", packet.QoS1); rc != reason.GrantedQoS1 {
		t.Fatalf("3.1.1 订阅失败: %s", rc)
	}

	// 跨版本：MQTT 5 发布，3.1.1 订阅
	pub := r.dial(ConnInfo{})
	pub.connect("pub-5", true)
	pub.publish("a/b", "cross", packet.QoS1, false)
	if got := sub.expectPublish(); string(got.Payload) != "cross" {
		t.Errorf("负载 = %q", got.Payload)
	}
}

// TestAssignedClientID 验证 MQTT 5 下空 clientId 由服务端分配。
func TestAssignedClientID(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	ack := c.connect("", true)
	if ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	if !ack.Props.Has(packet.PropAssignedClientID) || len(ack.Props.AssignedClientID) == 0 {
		t.Error("服务端应在 CONNACK 中回传分配的 clientId")
	}
}

// TestConnackAdvertisesCapabilities 验证 CONNACK 声明服务端能力。
func TestConnackAdvertisesCapabilities(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	ack := c.connect("dev-1", true)

	for _, id := range []packet.PropID{
		packet.PropReceiveMaximum, packet.PropMaximumPacketSize,
		packet.PropTopicAliasMaximum, packet.PropRetainAvailable,
		packet.PropWildcardSubAvail, packet.PropSubIDAvail, packet.PropSharedSubAvail,
	} {
		if !ack.Props.Has(id) {
			t.Errorf("CONNACK 缺少属性 0x%02x", byte(id))
		}
	}
	// 支持 QoS 2 时按规范省略 Maximum QoS 属性
	if ack.Props.Has(packet.PropMaximumQoS) {
		t.Error("支持 QoS 2 时不应声明 Maximum QoS")
	}
}

// TestSubscriptionIDRoundTrip 验证订阅标识符随投递回传。
func TestSubscriptionIDRoundTrip(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sp := &packet.Subscribe{
		PacketID: sub.nextPID(),
		Filters:  []packet.Subscription{{Filter: []byte("a/b"), QoS: packet.QoS1}},
	}
	sp.Props.SubscriptionIDs = []uint32{42}
	sp.Props.Set(packet.PropSubscriptionID)
	sub.send(sp)
	sub.expect(packet.SUBACK)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("a/b", "x", packet.QoS1, false)

	got := sub.expectPublish()
	if len(got.Props.SubscriptionIDs) != 1 || got.Props.SubscriptionIDs[0] != 42 {
		t.Errorf("订阅标识符 = %v，期望 [42]", got.Props.SubscriptionIDs)
	}
}

// TestMessagePropsForwarded 验证消息相关属性随投递透传。
func TestMessagePropsForwarded(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("a/b", packet.QoS1)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pp := &packet.Publish{
		Topic: []byte("a/b"), Payload: []byte("{}"),
		QoS: packet.QoS1, PacketID: pub.nextPID(),
	}
	pp.Props.ContentType = []byte("application/json")
	pp.Props.Set(packet.PropContentType)
	pp.Props.ResponseTopic = []byte("a/resp")
	pp.Props.Set(packet.PropResponseTopic)
	pp.Props.CorrelationData = []byte{0xAB}
	pp.Props.Set(packet.PropCorrelationData)
	pp.Props.PayloadFormat = 1
	pp.Props.Set(packet.PropPayloadFormat)
	pp.Props.UserProps = []packet.UserProperty{{Key: []byte("k"), Value: []byte("v")}}
	pp.Props.Set(packet.PropUserProperty)
	pub.send(pp)
	pub.expect(packet.PUBACK)

	got := sub.expectPublish()
	if string(got.Props.ContentType) != "application/json" {
		t.Errorf("内容类型 = %q", got.Props.ContentType)
	}
	if string(got.Props.ResponseTopic) != "a/resp" {
		t.Errorf("响应主题 = %q", got.Props.ResponseTopic)
	}
	if len(got.Props.CorrelationData) != 1 || got.Props.CorrelationData[0] != 0xAB {
		t.Errorf("关联数据 = %v", got.Props.CorrelationData)
	}
	if got.Props.PayloadFormat != 1 {
		t.Errorf("负载格式 = %d", got.Props.PayloadFormat)
	}
	if len(got.Props.UserProps) != 1 || string(got.Props.UserProps[0].Key) != "k" {
		t.Errorf("用户属性 = %+v", got.Props.UserProps)
	}
}

// TestWillPublishedOnDisconnectWithWill 验证 reason 0x04 触发遗嘱。
func TestWillPublishedOnDisconnectWithWill(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	watcher := r.dial(ConnInfo{})
	watcher.connect("watcher", true)
	watcher.subscribe("dev/offline", packet.QoS1)

	c := r.dial(ConnInfo{})
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-1"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("bye"), QoS: packet.QoS1,
		},
	})
	c.expect(packet.CONNACK)

	// 正常断开不触发遗嘱
	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(time.Second)
	watcher.expectNothing(300 * time.Millisecond)

	// reason 0x04 触发遗嘱
	c2 := r.dial(ConnInfo{})
	c2.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-2"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("crash"), QoS: packet.QoS1,
		},
	})
	c2.expect(packet.CONNACK)
	c2.send(&packet.Disconnect{Code: reason.DisconnectWithWill})
	c2.expectClosed(time.Second)

	if got := watcher.expectPublish(); string(got.Payload) != "crash" {
		t.Errorf("遗嘱负载 = %q", got.Payload)
	}
}

// TestOfflineQueueDeliversAcrossSessions 验证离线队列的补投不止生效一次。
//
// 由来：存储层有离线队列的测试，会话层有，但没有一条端到端跑通
// "离线期间发布 → 重连收到"。真机对跑时暴露出来：同一个节点上第一个
// 离线会话能收到补投，之后的全都收不到——clientId 各不相同，所以不是
// 会话自身的状态，是某处的一次性副作用。
//
// 因此这个测试跑两轮而不是一轮：只跑一轮的话它是绿的，什么都抓不到。
func TestOfflineQueueDeliversAcrossSessions(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	for round := 0; round < 2; round++ {
		cid := fmt.Sprintf("persist-%d", round)
		name := fmt.Sprintf("offline/%d", round)

		// 每一步都推进时钟：连接速率按时间窗口计，冻住的时钟会让
		// 同一 clientId 的重连撞上 connect_rate_per_client，
		// 得到一个与本测试无关的失败。
		r.clk.advance(time.Minute)

		// 建立持久会话并订阅，然后断开。
		c := r.dial(ConnInfo{})
		c.send(&packet.Connect{
			Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
			Props: func() packet.Properties {
				var p packet.Properties
				p.SessionExpiry = 3600
				p.Set(packet.PropSessionExpiry)
				return p
			}(),
		})
		c.expect(packet.CONNACK)
		if rc := c.subscribe(name, packet.QoS1); rc.IsError() {
			t.Fatalf("第 %d 轮订阅失败: %s", round, rc)
		}
		c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
		c.expectClosed(testPatience)
		// 等服务端把断开处理完：清理跑在连接的读协程上、在关套接字之后。
		// 没走完时 lookup(clientId) 还返回那条将死的连接，于是下面那条
		// 发布走的是「投给在线连接」而不是「入离线队列」——消息就此丢掉，
		// 而现象是重连之后收不到，等满整个耐心值（2026-11-28 门禁实证）。
		//
		// 按 clientId 等而不是数总数：上一轮的发布者按设计还连着。
		r.waitClientGone(cid)

		// 离线期间发布一条 QoS 1。
		pub := r.dial(ConnInfo{})
		pub.connect(fmt.Sprintf("pub-%d", round), true)
		if rc := pub.publish(name, "while-offline", packet.QoS1, false); rc.IsError() {
			t.Fatalf("第 %d 轮发布失败: %s", round, rc)
		}

		// 重连，必须拿到会话与那条消息。
		r.clk.advance(time.Minute)
		again := r.dial(ConnInfo{})
		again.send(&packet.Connect{
			Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
			Props: func() packet.Properties {
				var p packet.Properties
				p.SessionExpiry = 3600
				p.Set(packet.PropSessionExpiry)
				return p
			}(),
		})
		ack, ok := again.expect(packet.CONNACK).(*packet.Connack)
		if !ok {
			t.Fatalf("第 %d 轮没有收到 CONNACK", round)
		}
		if ack.Code.IsError() {
			t.Fatalf("第 %d 轮重连被拒: %s", round, ack.Code)
		}
		if !ack.SessionPresent {
			t.Fatalf("第 %d 轮 SessionPresent=0，持久会话没有被保留", round)
		}
		got := again.expectPublish()
		if string(got.Payload) != "while-offline" {
			t.Errorf("第 %d 轮补投负载 = %q", round, got.Payload)
		}
		again.send(&packet.Disconnect{Code: reason.NormalDisconnection})
		again.expectClosed(time.Second)
	}
}

// TestWillPublishedOnAbruptDisconnect 验证连接异常终止时补发遗嘱（MQTT-060）。
//
// 由来：遗嘱原先只在 MQTT 5 的 DISCONNECT reason 0x04 时发出，也就是只在
// 客户端**主动请求**发遗嘱时才发；设备掉电、断网、保活超时一律静悄悄。
// 这恰好把遗嘱的用途反过来了，而上一个测试（reason 0x04 那条）是绿的，
// 所以门禁看不出问题——它只覆盖了唯一能工作的那条路径。
//
// 真机验证时才暴露：3.1.1 掐断 socket 不发、保活超时不发、0x04 发。
func TestWillPublishedOnAbruptDisconnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	watcher := r.dial(ConnInfo{})
	watcher.connect("watcher", true)
	watcher.subscribe("dev/offline", packet.QoS1)

	// 3.1.1 客户端直接关套接字，不发 DISCONNECT——设备掉电的真实情形。
	c := r.dial(ConnInfo{})
	c.ver = packet.V311
	c.send(&packet.Connect{
		Version: packet.V311, ClientID: []byte("dev-abrupt"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("abrupt"), QoS: packet.QoS1,
		},
	})
	c.expect(packet.CONNACK)
	_ = c.nc.Close()

	if got := watcher.expectPublish(); string(got.Payload) != "abrupt" {
		t.Errorf("遗嘱负载 = %q，想要 abrupt", got.Payload)
	}

	// 服务端主动踢出同样算"没说再见"：客户端没有机会发 DISCONNECT。
	c2 := r.dial(ConnInfo{})
	c2.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-kicked"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("kicked"), QoS: packet.QoS1,
		},
	})
	c2.expect(packet.CONNACK)
	if !r.b.KickClient("dev-kicked", reason.AdministrativeAction) {
		t.Fatal("踢出失败")
	}
	if got := watcher.expectPublish(); string(got.Payload) != "kicked" {
		t.Errorf("遗嘱负载 = %q，想要 kicked", got.Payload)
	}

	// 正常 DISCONNECT 仍然不发，且 0x04 只发一条（不与补发叠加）。
	c3 := r.dial(ConnInfo{})
	c3.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-polite"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("polite"), QoS: packet.QoS1,
		},
	})
	c3.expect(packet.CONNACK)
	c3.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c3.expectClosed(time.Second)
	watcher.expectNothing(300 * time.Millisecond)

	c4 := r.dial(ConnInfo{})
	c4.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-0x04"), CleanStart: true,
		Will: &packet.Will{
			Topic: []byte("dev/offline"), Payload: []byte("asked"), QoS: packet.QoS1,
		},
	})
	c4.expect(packet.CONNACK)
	c4.send(&packet.Disconnect{Code: reason.DisconnectWithWill})
	c4.expectClosed(time.Second)
	if got := watcher.expectPublish(); string(got.Payload) != "asked" {
		t.Errorf("遗嘱负载 = %q，想要 asked", got.Payload)
	}
	// 关键：0x04 发过之后，随后的连接终止不得再补一条。
	watcher.expectNothing(300 * time.Millisecond)
}

// willConnect 建立一条带遗嘱的 MQTT 5 连接。
func willConnect(r *rig, clientID, topic, payload string,
	delaySecs, expirySecs uint32,
) *testClient {
	r.t.Helper()
	c := r.dial(ConnInfo{})
	cp := &packet.Connect{
		Version: packet.V5, ClientID: []byte(clientID), CleanStart: false,
		Will: &packet.Will{
			Topic: []byte(topic), Payload: []byte(payload), QoS: packet.QoS1,
		},
	}
	cp.Props.SessionExpiry = expirySecs
	cp.Props.Set(packet.PropSessionExpiry)
	if delaySecs > 0 {
		cp.Will.Props.WillDelay = delaySecs
		cp.Will.Props.Set(packet.PropWillDelay)
	}
	c.send(cp)
	if ack, ok := c.expect(packet.CONNACK).(*packet.Connack); !ok || ack.Code.IsError() {
		r.t.Fatalf("%s 连接失败: %+v", clientID, ack)
	}
	return c
}

// TestWillDelayDefersPublish 验证 Will Delay Interval（MQTT-061）。
//
// 延迟的用意是"短暂掉线不要惊动别人"：设备网络抖一下就广播一条下线，
// 比不发还糟——告警会被噪声淹没。此前 Will.DelaySeconds 解析后无人读取，
// 带延迟的遗嘱与不带延迟的一样立刻发出。
func TestWillDelayDefersPublish(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	watcher := r.dial(ConnInfo{})
	watcher.connect("watcher", true)
	watcher.subscribe("dev/offline", packet.QoS1)

	c := willConnect(r, "dev-delay", "dev/offline", "delayed", 10, 3600)
	_ = c.nc.Close()
	r.waitConns(1)

	// 延迟没走完之前不得发出。
	r.advance(9 * time.Second)
	watcher.expectNothing(300 * time.Millisecond)

	// 走完就该发。
	r.advance(2 * time.Second)
	if got := watcher.expectPublish(); string(got.Payload) != "delayed" {
		t.Errorf("遗嘱负载 = %q", got.Payload)
	}
}

// TestWillDelayCancelledByReconnect 验证重连取消待发的遗嘱（MQTT-3.1.2.5）。
//
// 这是延迟的全部意义所在：设备在窗口内回来了，就当什么都没发生。
// 不取消的话，延迟只是把那条噪声推迟了十秒。
func TestWillDelayCancelledByReconnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	watcher := r.dial(ConnInfo{})
	watcher.connect("watcher", true)
	watcher.subscribe("dev/offline", packet.QoS1)

	c := willConnect(r, "dev-back", "dev/offline", "should-not-fire", 10, 3600)
	_ = c.nc.Close()
	r.waitConns(1)

	r.advance(3 * time.Second)
	again := willConnect(r, "dev-back", "dev/offline", "should-not-fire", 10, 3600)

	// 推过原定的触发点：取消了就不该有任何东西。
	r.advance(30 * time.Second)
	watcher.expectNothing(300 * time.Millisecond)

	// 这一次正常断开，同样不发。
	again.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	again.expectClosed(time.Second)
	r.advance(30 * time.Second)
	watcher.expectNothing(300 * time.Millisecond)
}

// TestWillDelayCappedBySessionExpiry 验证会话先结束时提前发出。
//
// 规范是"延迟走完或会话结束，以先到者为准"（MQTT-3.1.3.2.2）。
// 只认延迟的话，一个延迟一小时、会话只留五秒的设备，会在会话早已消失
// 之后才发出遗嘱——而那时它的订阅方多半已经自己超时判定下线了。
func TestWillDelayCappedBySessionExpiry(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	watcher := r.dial(ConnInfo{})
	watcher.connect("watcher", true)
	watcher.subscribe("dev/offline", packet.QoS1)

	c := willConnect(r, "dev-short", "dev/offline", "capped", 3600, 5)
	_ = c.nc.Close()
	r.waitConns(1)

	r.advance(4 * time.Second)
	watcher.expectNothing(300 * time.Millisecond)

	r.advance(2 * time.Second)
	if got := watcher.expectPublish(); string(got.Payload) != "capped" {
		t.Errorf("遗嘱负载 = %q", got.Payload)
	}
}

// TestWillTopicRequiresPublishPermission 验证遗嘱主题要过授权。
//
// 遗嘱是设备在 CONNECT 里指定的任意主题，服务端替它发。不判授权的话，
// 一台只被允许发 `$aws/things/<自己>/*` 的设备，可以把遗嘱设到别的设备的
// 命令主题上——之后只要断一次线，服务端就替它把那条消息发出去了。
//
// 这个缺口此前够不着：遗嘱只在客户端主动要求（DISCONNECT 0x04）时才发。
// 补上 MQTT-064 之后任何掉线都会发，缺口才真正打开——所以两件事必须一起做。
//
// 按 AWS 的行为，越权的遗嘱是**丢弃**而不是拒绝连接；但不能静默丢弃，
// 否则运维查不出设备为什么没有下线消息。因此同时断言拒绝事件产出。
func TestWillTopicRequiresPublishPermission(t *testing.T) {
	r := newPolicyRig(t, policyDeviceScoped, nil)
	getEvents, stop := r.collect(events.Filter{
		Kinds: events.KindAuthz.Mask(), OnlyDenied: true,
	})
	defer stop()

	victim := r.dial(ConnInfo{})
	victim.connect("victim", true)
	if rc := victim.subscribe("$aws/things/victim/cmd", packet.QoS1); rc.IsError() {
		t.Fatalf("受害者订阅失败: %s", rc)
	}

	// 攻击者的策略只覆盖它自己名下的主题，却把遗嘱设到受害者的主题上。
	bad := r.dial(ConnInfo{})
	bad.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("attacker"), CleanStart: true,
		Will: &packet.Will{
			Topic:   []byte("$aws/things/victim/cmd"),
			Payload: []byte("forged"), QoS: packet.QoS1,
		},
	})
	ack, ok := bad.expect(packet.CONNACK).(*packet.Connack)
	if !ok || ack.Code.IsError() {
		t.Fatalf("遗嘱越权不该影响连接本身（与 AWS 一致）: %+v", ack)
	}
	_ = bad.nc.Close()
	r.waitConns(1)

	victim.expectNothing(500 * time.Millisecond)

	// 丢弃要留下痕迹：事件流与策略调试器是运维查"为什么没有下线消息"
	// 的唯一入口。
	deadline := time.Now().Add(time.Second)
	found := false
	for !found && time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Authz.Resource == "$aws/things/victim/cmd" &&
				e.Authz.Decision != events.Allow {
				found = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Error("越权的遗嘱被丢弃了，但没有产出授权拒绝事件——运维无从查起")
	}
}

// TestDuplicateSubscribeDoesNotLeakQuota 验证重复订阅不重复占配额。
//
// MQTT 允许客户端重复订阅同一过滤器以修改 QoS，路由表与会话都是就地替换。
// 配额此前每次都加一、永不对消：一个按常规在重连后重订阅的客户端，会一路
// 吃掉全局订阅配额（上限 50 万），到顶之后**整个 broker** 都不能再新增
// 订阅，且只能重启恢复——而它自始至终只持有一条订阅。
func TestDuplicateSubscribeDoesNotLeakQuota(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dup", true)

	for i := 0; i < 5; i++ {
		if rc := c.subscribe("dup/topic", packet.QoS1); rc.IsError() {
			t.Fatalf("第 %d 次订阅失败: %s", i, rc)
		}
	}

	if cur := r.lim.Current(quota.Subscriptions, ""); cur != 1 {
		t.Errorf("重复订阅 5 次后全局订阅配额 = %d，应为 1", cur)
	}
	if cur := r.lim.Current(quota.SubsPerConn, "dup"); cur != 1 {
		t.Errorf("重复订阅 5 次后每连接订阅配额 = %d，应为 1", cur)
	}
	if st := r.rt.Stats(); st.Subscriptions != 1 {
		t.Errorf("路由表订阅数 = %d，应为 1", st.Subscriptions)
	}
}

// TestSubscriptionQuotaReleasedWhenSessionEnds 验证会话结束时归还订阅配额。
//
// 全局订阅配额此前只在显式 UNSUBSCRIBE 时归还。而最常见的那条路径是
// "设备订阅完直接断开"——清洁会话的订阅会被移出路由表，额度却留在那里，
// 每一次都永久少掉几份。
//
// 持久会话相反：订阅仍留在路由表等重连，继续占着资源，额度也该继续占着。
// 两种情形必须分别断言，只测一种会把另一种的回归放过去。
func TestSubscriptionQuotaReleasedWhenSessionEnds(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	// 清洁会话：断开即归还。
	c := r.dial(ConnInfo{})
	c.connect("clean-one", true)
	c.subscribe("q/a", packet.QoS1)
	c.subscribe("q/b", packet.QoS1)
	if cur := r.lim.Current(quota.Subscriptions, ""); cur != 2 {
		t.Fatalf("订阅两条后配额 = %d，应为 2", cur)
	}
	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(time.Second)
	waitFor(t, time.Second, func() bool {
		return r.lim.Current(quota.Subscriptions, "") == 0
	})
	if cur := r.lim.Current(quota.Subscriptions, ""); cur != 0 {
		t.Errorf("清洁会话断开后订阅配额 = %d，应为 0", cur)
	}

	// 持久会话：订阅留在路由表，额度也留着。
	r.clk.advance(2 * time.Second) // 避开每 clientId 连接速率配额
	p := r.dial(ConnInfo{})
	p.connect("persist-one", false)
	p.subscribe("q/c", packet.QoS1)
	p.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	p.expectClosed(time.Second)
	waitFor(t, time.Second, func() bool { return r.b.Conns() == 0 })
	if cur := r.lim.Current(quota.Subscriptions, ""); cur != 1 {
		t.Errorf("持久会话断开后订阅配额 = %d，应为 1（订阅仍在路由表里）", cur)
	}
	if st := r.rt.Stats(); st.Subscriptions != 1 {
		t.Errorf("持久会话断开后路由表订阅数 = %d，应为 1", st.Subscriptions)
	}
}

// TestTakeoverKeepsLiveSubscriptionState 验证接管不会把订阅状态回退到上次落盘时。
//
// 订阅与退订只改内存里的会话，落盘发生在断开时。同 clientId 的新连接
// 若从存储恢复，拿到的是上一次断开时的那份快照，于是本次连接期间的
// 订阅与退订全部被悄悄撤销（见 conn.adoptLive）。
//
// 两个方向都要测，而且顺序相反地错：退订丢失会让设备重新收到它已经
// 明确不要的主题（下半段），订阅丢失则让它收不到刚订上的主题
// （上半段，表现为记账分叉）。只测一个方向会把另一个方向的回归放过去。
func TestTakeoverKeepsLiveSubscriptionState(t *testing.T) {
	t.Run("接管保留本次连接新增的订阅", func(t *testing.T) {
		r := newPolicyRig(t, policyFull, nil)

		first := r.dial(ConnInfo{})
		first.connect("keep-sub", false)
		// 这一条只存在于内存：会话是在 CONNECT 时落盘的，此后到断开之前
		// 不再落盘。
		first.subscribe("t/keep", packet.QoS1)

		r.clk.advance(2 * time.Second) // 避开每 clientId 连接速率配额
		second := r.dial(ConnInfo{})
		if ack := second.connect("keep-sub", false); !ack.SessionPresent {
			t.Error("接管活会话应报告会话已存在")
		}
		first.expectClosed(testPatience)

		// 重订同一条过滤器不该再占一份额度——会话必须知道自己已经持有它。
		// 回退过的会话会以为这是条新订阅，于是额度加一而路由表就地替换，
		// 全局额度就此多出一份（上限全局 50 万，耗尽则整个 broker 不能
		// 再新增订阅）。
		if rc := second.subscribe("t/keep", packet.QoS1); rc.IsError() {
			t.Fatalf("重订失败: %s", rc)
		}
		assertSubAccounting(t, r, 1)
	})

	t.Run("接管保留本次连接的退订", func(t *testing.T) {
		r := newPolicyRig(t, policyFull, nil)

		first := r.dial(ConnInfo{})
		first.connect("drop-sub", false)
		first.subscribe("t/drop", packet.QoS1)
		first.send(&packet.Disconnect{Code: reason.NormalDisconnection})
		first.expectClosed(time.Second)
		waitFor(t, time.Second, func() bool { return r.b.Conns() == 0 })

		// 存储里现在是 {t/drop}。恢复、退订，但不断开——存储仍是 {t/drop}。
		r.clk.advance(2 * time.Second)
		second := r.dial(ConnInfo{})
		if ack := second.connect("drop-sub", false); !ack.SessionPresent {
			t.Fatal("重连应报告会话已存在")
		}
		second.send(&packet.Unsubscribe{
			PacketID: second.nextPID(), Filters: [][]byte{[]byte("t/drop")},
		})
		second.expect(packet.UNSUBACK)

		r.clk.advance(2 * time.Second)
		third := r.dial(ConnInfo{})
		third.connect("drop-sub", false)
		// 这里**不能**等连接数降到 0：接管上来的 third 按设计还活着（这条用例
		// 要的正是「被接管的那条断了、新的那条在」）。
		second.expectClosed(testPatience)
		r.waitConns(1)

		// 设备明明退订了。接管之后又收到，就是订阅状态被回退——
		// 这是记账分叉背后真正的问题。
		pub := r.dial(ConnInfo{})
		pub.connect("pub", true)
		pub.publish("t/drop", "x", packet.QoS1, false)
		third.expectNothing(300 * time.Millisecond)
		assertSubAccounting(t, r, 0)
	})
}

// TestExpiredSessionLeavesNoRouterOrphans 验证过期会话不会在路由表里留下孤儿订阅。
//
// 会话过期只是让恢复落空：restoreSession 发现它过期就当新会话处理，
// 而没有人去摘掉路由表里那几条订阅。后果有两层——
//
// 其一是订阅连同全局额度一起永久留下：新会话订阅同一个主题时额度加一
// 而路由表就地替换，额度于此多出一份（随机种子深扫报的「配额比路由表多」
// 就是它）；其二更严重，路由仍按 clientId 投递，于是**新会话会收到它
// 从未订阅过的主题**。两层都断言。
func TestExpiredSessionLeavesNoRouterOrphans(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	// 过期间隔取 10 秒，这样推进时钟就能让它过期。测试装置的 connect
	// 助手固定写 3600 秒，这里自己构造 CONNECT。
	first := r.dial(ConnInfo{})
	cp := &packet.Connect{Version: packet.V5, ClientID: []byte("expiring")}
	cp.Props.SessionExpiry = 10
	cp.Props.Set(packet.PropSessionExpiry)
	first.send(cp)
	first.expect(packet.CONNACK)
	first.subscribe("t/gone", packet.QoS1)
	first.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	first.expectClosed(time.Second)
	waitFor(t, time.Second, func() bool { return r.b.Conns() == 0 })

	// 持久会话断开后订阅留在路由表等重连，额度也该继续占着。
	assertSubAccounting(t, r, 1)

	r.clk.advance(2 * time.Minute) // 会话已过期

	second := r.dial(ConnInfo{})
	if ack := second.connect("expiring", false); ack.SessionPresent {
		t.Error("会话已过期，不应报告会话已存在")
	}
	// 新会话没有订阅任何东西，路由表与额度都该是空的。
	assertSubAccounting(t, r, 0)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/gone", "x", packet.QoS1, false)
	second.expectNothing(300 * time.Millisecond)
}

// TestReplayAfterRestartTakesSubscriptionQuota 验证重启后重放订阅要占全局额度。
//
// 路由表是纯内存的，而持久会话落在 bbolt 里。重启之后会话还在、订阅还在，
// 路由表却是空的——此时重放是**真的**往路由表里新加几条订阅，必须占额度。
// 此前这里一概不占（理由是"持久会话断开期间订阅一直留在路由表里"，
// 那只对同一进程内的重连成立），于是重启后每个恢复出来的订阅都在白拿
// 一条路由表条目。账歪了不会立刻出事，要等设备重订同一条过滤器时才被
// 当成新订阅补记上，而那时多记少记已经分不清了。
//
// 用预置会话存储来模拟重启：装置是新起的（路由表空），而存储里已经有
// 一个带订阅的持久会话，这正是重启后的状态。
func TestReplayAfterRestartTakesSubscriptionQuota(t *testing.T) {
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
	}); err != nil {
		t.Fatalf("预置会话失败: %v", err)
	}
	// 重启后的路由表是空的。
	assertSubAccounting(t, r, 0)

	c := r.dial(ConnInfo{})
	if ack := c.connect("rebooted", false); !ack.SessionPresent {
		t.Fatal("存储里的持久会话应被恢复")
	}
	// 订阅重新装进了路由表，额度必须跟着占上。
	assertSubAccounting(t, r, 1)

	// 并且订阅是真的生效的——只对账不验行为，会把"额度占了但没装进路由表"
	// 这种反向错误放过去。
	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pub.publish("t/restart", "back", packet.QoS1, false)
	if got := c.expectPublish(); string(got.Payload) != "back" {
		t.Errorf("负载 = %q", got.Payload)
	}
}

// assertSubAccounting 断言全局订阅额度与路由表订阅数都等于 want。
//
// 两者必须一起断言：只看额度，路由泄漏会被放过；只看路由表，额度泄漏
// 会被放过。而额度到顶的后果是整个 broker 不能再新增订阅。
func assertSubAccounting(t *testing.T, r *rig, want int) {
	t.Helper()
	// 订阅是在 SUBACK **之后**才写进路由表的，刚收到 SUBACK 的那一刻
	// 可能还差这一步，所以先等收敛再断言。
	waitFor(t, time.Second, func() bool {
		return r.rt.Stats().Subscriptions == want &&
			r.lim.Current(quota.Subscriptions, "") == int64(want)
	})
	if n := r.rt.Stats().Subscriptions; n != want {
		t.Errorf("路由表订阅数 = %d，应为 %d", n, want)
	}
	if n := r.lim.Current(quota.Subscriptions, ""); n != int64(want) {
		t.Errorf("全局订阅配额 = %d，应为 %d", n, want)
	}
}

// waitFor 轮询等待条件成立。清理是异步的，直接断言会撞上中间态。
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d) // contract:allow-time-now 测试超时
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPipelinedPacketsAreNotCorrupted 验证一次读取里的多个报文互不污染。
//
// packet.Decode 是零拷贝的：解出的报文直接引用连接的读缓冲。读循环原先
// 在返回报文**之前**就把缓冲压实，于是一次 TCP 读里收到多个报文时，
// 后一个报文的字节会盖在前一个报文的主题、过滤器或负载上——而前一个
// 报文还没被处理。
//
// 症状极其隐蔽：客户端订阅 pipe/x，服务端却给它订上了别的主题，
// 并且回 Granted。两边都不报错，只是从此收不到该收的消息、收到不该收的。
// 鉴权也是按被污染后的那个主题做的。
//
// 触发条件要凑齐：后一个报文必须足够长，长到搬到偏移 0 之后能盖住前一个
// 报文的字符串字段（过滤器大约在偏移 7）。尾包只有几字节时盖不到，
// 所以它在单条用例里几乎碰不到——是确定性模拟跑出来的。
func TestPipelinedPacketsAreNotCorrupted(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	getSubs, stop := r.collect(events.Filter{Kinds: events.KindSubscription.Mask()})
	defer stop()

	c := r.dial(ConnInfo{})
	c.connect("pipe", true)

	sub, err := (&packet.Subscribe{
		PacketID: 1,
		Filters:  []packet.Subscription{{Filter: []byte("pipe/x"), QoS: packet.QoS1}},
	}).EncodeTo(nil, packet.V5)
	if err != nil {
		t.Fatal(err)
	}
	// 跟在后面的长报文：它的字节正是会被搬上去的那些。
	pub, err := (&packet.Publish{
		Topic:   []byte("pipe/other"),
		Payload: []byte("0123456789abcdef"),
		QoS:     packet.QoS0,
	}).EncodeTo(nil, packet.V5)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.nc.SetWriteDeadline(time.Now().Add(testPatience)); err != nil {
		t.Fatal(err)
	}
	// 一次写出，使两个报文尽可能落在服务端的同一次读取里。
	if _, err := c.nc.Write(append(append([]byte(nil), sub...), pub...)); err != nil {
		t.Fatal(err)
	}

	ack, ok := c.expect(packet.SUBACK).(*packet.Suback)
	if !ok || len(ack.Codes) != 1 || ack.Codes[0].IsError() {
		t.Fatalf("SUBACK 异常: %+v", ack)
	}

	// 断言服务端**实际看到的**过滤器，而不只是"有没有报错"。
	// 只看错误码是抓不到这个缺陷的：被污染的过滤器照样能通过校验与鉴权。
	waitFor(t, time.Second, func() bool { return len(getSubs()) > 0 })
	evs := getSubs()
	if len(evs) == 0 {
		t.Fatal("没有收到订阅事件")
	}
	if got := evs[0].Sub.Filter; got != "pipe/x" {
		t.Errorf("服务端看到的过滤器 = %q，客户端发的是 \"pipe/x\"（读缓冲被后一个报文污染）", got)
	}
}

// TestSharedSubscriptionLoadBalances 验证共享订阅组内负载均衡。
func TestSharedSubscriptionLoadBalances(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	w1 := r.dial(ConnInfo{})
	w1.connect("w1", true)
	if rc := w1.subscribe("$share/workers/jobs/#", packet.QoS1); rc.IsError() {
		t.Fatalf("共享订阅失败: %s", rc)
	}
	w2 := r.dial(ConnInfo{})
	w2.connect("w2", true)
	w2.subscribe("$share/workers/jobs/#", packet.QoS1)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)

	// 两条消息应分别落到两个工作者身上
	pub.publish("jobs/a", "1", packet.QoS1, false)
	pub.publish("jobs/b", "2", packet.QoS1, false)

	got := map[string]bool{}
	for _, w := range []*testClient{w1, w2} {
		if err := w.nc.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if p, err := w.recv(); err == nil {
			if pp, ok := p.(*packet.Publish); ok {
				got[string(pp.Payload)] = true
				w.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pp.PacketID})
			}
		}
	}
	if len(got) != 2 {
		t.Errorf("两条消息应分别投给两个成员，实际收到 %v", got)
	}
}

// TestOfflineQueueIsBounded 钉住离线队列的上限（QTA-111、MQTT-127）。
//
// 没有上限时这条用例会一直通过——那正是 2026-11-19 之前的状态：一台持久
// 会话的设备下线后，发给它的每条 QoS 1 都往队列里堆，不报错、只是内存与
// 磁盘慢慢涨，而且每入队一条都把整块会话状态重写一遍。
//
// 判据有两条，缺一条都证明不了：**补投的条数等于上限**（证明上限生效），
// 且**补投的是最早那几条**（证明满了之后丢的是新的，设备收到的是已发布
// 序列的一个前缀，而不是带空洞的一段）。
func TestOfflineQueueIsBounded(t *testing.T) {
	const limit = 3
	r := newPolicyRig(t, policyFull, map[quota.ID]int{
		quota.QueuedPerSession: limit,
	})

	const cid = "bounded-1"
	const name = "offline/bounded"

	r.clk.advance(time.Minute)
	c := r.dial(ConnInfo{})
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
		Props: func() packet.Properties {
			var p packet.Properties
			p.SessionExpiry = 3600
			p.Set(packet.PropSessionExpiry)
			return p
		}(),
	})
	c.expect(packet.CONNACK)
	if rc := c.subscribe(name, packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(testPatience)
	r.waitClientGone(cid)

	// 离线期间发布 limit+2 条，发布本身都应成功——配额拦的是入队，
	// 不是发布：发布者没有理由因为某个订阅者的队列满了而被拒。
	pub := r.dial(ConnInfo{})
	pub.connect("pub-bounded", true)
	for i := 0; i < limit+2; i++ {
		if rc := pub.publish(name, fmt.Sprintf("m%d", i), packet.QoS1, false); rc.IsError() {
			t.Fatalf("第 %d 条发布失败: %s", i, rc)
		}
	}

	r.clk.advance(time.Minute)
	again := r.dial(ConnInfo{})
	again.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
		Props: func() packet.Properties {
			var p packet.Properties
			p.SessionExpiry = 3600
			p.Set(packet.PropSessionExpiry)
			return p
		}(),
	})
	ack, ok := again.expect(packet.CONNACK).(*packet.Connack)
	if !ok || ack.Code.IsError() || !ack.SessionPresent {
		t.Fatalf("重连没有拿到持久会话: %+v", ack)
	}

	var got []string
	for i := 0; i < limit; i++ {
		got = append(got, string(again.expectPublish().Payload))
	}
	want := []string{"m0", "m1", "m2"}
	if len(got) != len(want) {
		t.Fatalf("补投 %d 条，期望 %d 条", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("补投序列 = %v，期望 %v；满了之后应当丢新的而不是挤掉最旧的",
				got, want)
		}
	}
	// 再没有第 limit+1 条：上限之外的那两条确实没有入队。
	again.expectNothing(200 * time.Millisecond)

	// 丢弃要能被看到：这是运维侧唯一的信号（MQTT-126 同一个理由）。
	var rejected uint64
	for _, st := range r.lim.Stats() {
		if st.Name == "queued_messages_per_session" {
			rejected = st.Rejected
		}
	}
	if rejected != 2 {
		t.Errorf("配额拒绝计数 = %d，期望 2；没有计数的话"+
			"「部分消息被丢了」这件事没有任何地方能看出来", rejected)
	}
}

// TestOfflineQueueIsNotRedelivered 钉住「补发过的离线消息不再重投」。
//
// 队列拆分之后（独立记录 PrefixQueued）这条路上有两道清理：补发时
// `flushQueued` 从盘上取走并删除，断开时 `persist` 又按内存里那份对账。
// 两道都生效，所以**这条用例分不出少了哪一道**——实测过：把补发改回
// 只从内存取，它照样绿。原因是 `State.Clone()` 把 `Queued` 做成非 nil
// 的空切片，而断开时 Save 用的正是克隆，于是那次对账把盘上记录删了。
//
// 真正只有「盘上是权威」才覆盖的是另一种：**进程在补发之后、断开落盘
// 之前死掉**。那时没有第二道清理，只从内存取就会在下次重连重投一遍。
// 这条用例覆盖不到它（鼓不动进程崩溃），所以留着它是作为"正常流程不
// 重投"的回归，而不是那条设计判据的证明。
func TestOfflineQueueIsNotRedelivered(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	const cid = "redeliver-1"
	const name = "offline/redeliver"

	connect := func() *testClient {
		r.clk.advance(time.Minute)
		c := r.dial(ConnInfo{})
		c.send(&packet.Connect{
			Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
			Props: func() packet.Properties {
				var p packet.Properties
				p.SessionExpiry = 3600
				p.Set(packet.PropSessionExpiry)
				return p
			}(),
		})
		c.expect(packet.CONNACK)
		return c
	}

	c := connect()
	if rc := c.subscribe(name, packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(testPatience)
	r.waitClientGone(cid)

	pub := r.dial(ConnInfo{})
	pub.connect("pub-redeliver", true)
	if rc := pub.publish(name, "once", packet.QoS1, false); rc.IsError() {
		t.Fatalf("发布失败: %s", rc)
	}

	// 第一次重连：收到那一条。
	first := connect()
	if got := string(first.expectPublish().Payload); got != "once" {
		t.Fatalf("第一次重连收到 %q，期望 once", got)
	}
	first.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	first.expectClosed(time.Second)

	// 第二次重连：不该再收到。
	second := connect()
	second.expectNothing(300 * time.Millisecond)
}

// TestRemoteFrameReachesOfflineQueue 钉住跨节点投递能落进离线队列。
//
// 这条是三进程验收里找出来的缺口（2026-11-20）：会话复制接上之后，
// "设备在 a 上建持久会话、从 b 发一条、到 c 上重连收到它"这条路走不通，
// 而会话本身明明已经复制过去了。排查方向被"离线队列没跨节点"带偏了一轮
// ——盘上根本没有入队记录，也就是说那一帧从没走到入队这一步。
//
// 判据是这里这条：**远端来的帧**落到一个当前离线的持久会话上时，要入队。
// 本地发布那条路已有用例（TestOfflineQueueIsBounded），而 DeliverRemote
// 是另一段代码——它自己算本地订阅者再逐个 deliverTo，少一步就全无声息。
func TestRemoteFrameReachesOfflineQueue(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	const cid = "remote-queue-1"
	const name = "offline/remote"

	r.clk.advance(time.Minute)
	c := r.dial(ConnInfo{})
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
		Props: func() packet.Properties {
			var p packet.Properties
			p.SessionExpiry = 3600
			p.Set(packet.PropSessionExpiry)
			return p
		}(),
	})
	c.expect(packet.CONNACK)
	// 通配符订阅：兴趣通告只带通配符（精确订阅走设备归属，CLU-061），
	// 而跨节点转发正是按兴趣表算的。
	if rc := c.subscribe("offline/#", packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(testPatience)
	// 等服务端把断开处理完，别只等客户端看见套接字关闭。
	//
	// 清理跑在连接自己的读协程上、在关套接字**之后**：解绑 clientId 索引、
	// 持久化会话都在那一步。不等的话 `b.lookup(cid)` 还返回这条将死的连接，
	// 于是下面那一帧走的是「投给在线连接」而不是「入离线队列」，断言当场
	// 看到队列里 0 条——而现象看上去像「远端帧没有入队」这个功能坏了。
	// 2026-11-28 的门禁就红在这里（0.00 秒失败，覆盖率插桩把这个窗口拉宽了）。
	r.waitConns(0)

	// 断开之后订阅必须还在路由表里：兴趣通告读的就是它，掉了的话
	// 别的节点根本不会把帧转过来——那时失败现象是"收不到"，而原因
	// 在通告这一侧。
	found := false
	for _, f := range r.rt.LocalWildcards() {
		if f == "offline/#" {
			found = true
		}
	}
	if !found {
		t.Fatalf("持久会话断开后通配符订阅从路由表消失了，本节点的兴趣通告里"+
			"不再有它: %v", r.rt.LocalWildcards())
	}

	// 一帧远端消息，就像 mesh 送过来的那样。
	r.b.DeliverRemote("peer-b", []cluster.Frame{{
		Version: cluster.FrameVersion,
		Topic:   name, Payload: []byte("from-peer"), QoS: 1,
	}})

	queued, err := r.sess.TakeQueued(cid)
	if err != nil {
		t.Fatalf("TakeQueued: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("远端来的帧没有进离线队列，队列里有 %d 条", len(queued))
	}
	if got := string(queued[0].Payload); got != "from-peer" {
		t.Fatalf("入队的载荷是 %q", got)
	}
}

// TestQueuedSurvivesDisconnectPersist 钉住断开那一刻盘上的队列不被覆盖
// （MQTT-135、TST-240）。
//
// 这一条是确定性的 A/B：不需要压中任何时序窗口。断开时的 persist 以前把
// 内存里那份队列整份写回盘上（`State.Clone` 的 Queued 永远非 nil，而 Save
// 对非 nil 是「先清空再写回」），于是**这期间落在盘上的任何一条都会被抹掉**
// ——而「投给将死连接的消息」现在正好走这条路（见 deliverTo 的 MQTT-135）。
//
// 用例直接往存储里追加一条来代表那个并发追加：它在断开之前就在盘上，断开
// 之后必须还在。退回旧实现时它当场失败。
func TestQueuedSurvivesDisconnectPersist(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	const cid = "queue-survivor"

	r.clk.advance(time.Minute)
	c := r.dial(ConnInfo{})
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
		Props: func() packet.Properties {
			var p packet.Properties
			p.SessionExpiry = 3600
			p.Set(packet.PropSessionExpiry)
			return p
		}(),
	})
	c.expect(packet.CONNACK)
	if rc := c.subscribe("q/#", packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}

	// 代表「发布者在断开窗口里把一条消息追加到了盘上」。
	if err := r.sess.AppendQueued(cid, session.QueuedMsg{
		QoS: 1, Topic: "q/x", Payload: []byte("raced-in"),
	}); err != nil {
		t.Fatalf("预置队列记录失败: %v", err)
	}

	c.send(&packet.Disconnect{Code: reason.NormalDisconnection})
	c.expectClosed(testPatience)
	r.waitClientGone(cid)

	queued, err := r.sess.TakeQueued(cid)
	if err != nil {
		t.Fatalf("TakeQueued: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("断开时的 persist 把盘上的队列覆盖了，现在有 %d 条", len(queued))
	}
	if got := string(queued[0].Payload); got != "raced-in" {
		t.Fatalf("留下来的是 %q", got)
	}
}

// TestPublishDuringTeardownIsNotLost 钉住断开窗口里到达的消息不丢
// （MQTT-135、TST-240）。
//
// 它刻意**不等**服务端清理完：DISCONNECT 之后立刻从另一条连接发布。三种
// 交错都必须不丢，而三种的保全机制并不相同：
//
//   - 还完全在线：投给连接、QoS 1 登记在途，而在途表在 persist 里落盘，
//     重连按原标识带 DUP 重投；
//   - closedRC 已置位但索引还没摘：按 MQTT-135 当成离线，入队；
//   - 已经摘掉索引：本来就走离线队列。
//
// 修复之前第二种会丢（投给将死的连接，而在途登记可能发生在 persist 之后，
// 那条登记永远不落盘），而它正是 CI 上那三条用例红的原因。这条用例是概率
// 性的——窗口很窄，所以跑多轮；确定性的那一半由
// TestQueuedSurvivesDisconnectPersist 承担。
func TestPublishDuringTeardownIsNotLost(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	for round := 0; round < 10; round++ {
		cid := fmt.Sprintf("teardown-%d", round)
		name := fmt.Sprintf("teardown/%d", round)
		r.clk.advance(time.Minute)

		connectPersistent := func() *testClient {
			c := r.dial(ConnInfo{})
			c.send(&packet.Connect{
				Version: packet.V5, ClientID: []byte(cid), CleanStart: false,
				Props: func() packet.Properties {
					var p packet.Properties
					p.SessionExpiry = 3600
					p.Set(packet.PropSessionExpiry)
					return p
				}(),
			})
			c.expect(packet.CONNACK)
			return c
		}

		c := connectPersistent()
		if rc := c.subscribe(name, packet.QoS1); rc.IsError() {
			t.Fatalf("第 %d 轮订阅失败: %s", round, rc)
		}
		// 不等 waitClientGone：这一轮要压的正是那个窗口。
		c.send(&packet.Disconnect{Code: reason.NormalDisconnection})

		pub := r.dial(ConnInfo{})
		pub.connect(fmt.Sprintf("tpub-%d", round), true)
		if rc := pub.publish(name, "in-window", packet.QoS1, false); rc.IsError() {
			t.Fatalf("第 %d 轮发布失败: %s", round, rc)
		}
		_ = pub.nc.Close()

		r.clk.advance(time.Minute)
		r.waitClientGone(cid)
		again := connectPersistent()
		got := again.expectPublish()
		if string(got.Payload) != "in-window" {
			t.Fatalf("第 %d 轮收到 %q", round, got.Payload)
		}
		_ = again.nc.Close()
		r.waitClientGone(cid)
	}
}
