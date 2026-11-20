package broker

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// TestNewValidatesDependencies 固定住一条决策：缺失依赖必须在构造时报错，
// 而不是在第一条连接进来时 panic。
func TestNewValidatesDependencies(t *testing.T) {
	clk := newClock()
	full := func() Options {
		st := meta.NewLocalStore(meta.LocalOptions{Now: clk.now})
		t.Cleanup(func() { _ = st.Close() })
		ow := cluster.NewLocalOwnership("n", 8)
		t.Cleanup(func() { _ = ow.Close() })
		return Options{
			Node: "n", Meta: st,
			Router:   router.New(router.Options{Self: "n", Now: clk.now}),
			Sessions: session.NewMemStore(),
			Owners:   ow,
			Limiter:  quota.NewLocal(quota.Options{Now: clk.now}),
			Bus:      events.New(events.Options{Node: "n", Now: clk.now}),
			Auth:     InsecureNoAuth{Acknowledged: true},
			Now:      clk.now,
		}
	}
	cases := map[string]func(*Options){
		"Meta":     func(o *Options) { o.Meta = nil },
		"Router":   func(o *Options) { o.Router = nil },
		"Sessions": func(o *Options) { o.Sessions = nil },
		"Owners":   func(o *Options) { o.Owners = nil },
		"Limiter":  func(o *Options) { o.Limiter = nil },
		"Bus":      func(o *Options) { o.Bus = nil },
		"Auth":     func(o *Options) { o.Auth = nil },
		"Now":      func(o *Options) { o.Now = nil },
	}
	for name, mangle := range cases {
		t.Run("缺少"+name, func(t *testing.T) {
			o := full()
			mangle(&o)
			b, err := New(o)
			if err == nil {
				_ = b.Close()
				t.Fatalf("缺少 %s 应报错", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("错误信息 %q 应提到 %s", err.Error(), name)
			}
		})
	}
	// 完整依赖应成功，且默认参数被填充
	b, err := New(full())
	if err != nil {
		t.Fatalf("完整依赖应成功: %v", err)
	}
	defer func() { _ = b.Close() }()
	if b.opt.WriterWorkers == 0 || b.opt.OutboundBuffer == 0 ||
		b.opt.MaxWriteBatch == 0 || b.opt.ReadBufferSize == 0 ||
		b.opt.KeepAliveGraceFactor == 0 {
		t.Errorf("默认参数未填充: %+v", b.opt)
	}
	if b.opt.Limits.MaxNameLen == 0 {
		t.Error("主题配额默认值未填充")
	}
	if b.opt.Env == (authz.Env{}) {
		t.Error("ARN 环境默认值未填充")
	}
}

// TestKeepAliveTimeoutDisconnects 验证保活超时断开。
//
// MQTT 要求服务端在 1.5 倍保活间隔内未收到任何报文时断开
// （MQTT-3.1.2.10）。这里用很短的保活与宽限倍数把窗口压到可测范围。
func TestKeepAliveTimeoutDisconnects(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	// 保活下限是 30 秒，用极小的宽限倍数把窗口压到约 150 毫秒
	r.b.opt.KeepAliveGraceFactor = 0.005

	c := r.dial(ConnInfo{})
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-1"),
		CleanStart: true, KeepAlive: 30,
	})
	c.expect(packet.CONNACK)
	// 不再发送任何报文，连接应因保活超时被断开
	c.expectClosed(3 * time.Second)
}

// TestOversizePacketRejected 验证超出最大包长的报文被拒绝。
//
// 读缓冲按需扩张，但上限由配额约束——否则一个声明了巨大剩余长度的
// 报文就能让服务端为它分配任意内存。
func TestOversizePacketRejected(t *testing.T) {
	r := newPolicyRig(t, policyFull, map[quota.ID]int{quota.PacketSize: 256})
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)

	// 构造一个远超上限的 PUBLISH
	big := strings.Repeat("x", 4096)
	c.send(&packet.Publish{Topic: []byte("a/b"), Payload: []byte(big)})
	c.expectClosed(2 * time.Second)
}

// TestOversizePayloadRejectedByQuota 验证负载超限以准确原因码断开。
func TestOversizePayloadRejectedByQuota(t *testing.T) {
	r := newPolicyRig(t, policyFull, map[quota.ID]int{quota.PayloadSize: 8})
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.send(&packet.Publish{Topic: []byte("a/b"), Payload: []byte("0123456789")})
	if rc := c.expectClosed(2 * time.Second); rc != reason.PacketTooLarge {
		t.Errorf("断开原因码 = %s，期望 Packet too large", rc)
	}
}

// TestMalformedPacketDisconnects 验证畸形报文导致断开。
func TestMalformedPacketDisconnects(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	// 保留类型 0 是畸形的
	if _, err := c.nc.Write([]byte{0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	c.expectClosed(2 * time.Second)
}

// TestInvalidTopicNameDisconnects 验证非法主题名导致断开。
func TestInvalidTopicNameDisconnects(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	// 主题名超出斜杠配额
	deep := strings.Repeat("a/", 20) + "b"
	c.send(&packet.Publish{Topic: []byte(deep), Payload: []byte("x")})
	if rc := c.expectClosed(2 * time.Second); rc != reason.TopicNameInvalid {
		t.Errorf("断开原因码 = %s，期望 Topic Name invalid", rc)
	}
}

// TestInvalidFilterReturnsCodeNotDisconnect 验证非法过滤器只拒该项，
// 不断开连接——订阅的部分失败是正常情形。
func TestInvalidFilterReturnsCodeNotDisconnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)

	// "#/a" 的多层通配符不在最后一级，非法
	c.send(&packet.Subscribe{
		PacketID: c.nextPID(),
		Filters: []packet.Subscription{
			{Filter: []byte("#/a"), QoS: packet.QoS0},
		},
	})
	ack := c.expect(packet.SUBACK).(*packet.Suback)
	if ack.Codes[0] != reason.TopicFilterInvalid {
		t.Errorf("原因码 = %s，期望 Topic Filter invalid", ack.Codes[0])
	}
	// 连接仍可用
	c.send(&packet.Pingreq{})
	c.expect(packet.PINGRESP)
}

// TestQoS0DenialHasNoAck 固定住 MQTT 的一个固有限制：
// QoS 0 没有确认报文，服务端无法告知发布者它被拒绝了。
//
// 这不是缺陷而是协议事实，因此只记录事件。测试它是为了防止
// 有人"修"成发一个无效的确认。
func TestQoS0DenialHasNoAck(t *testing.T) {
	r := newPolicyRig(t, policyDeviceScoped, nil)
	getEvents, stop := r.collect(events.Filter{
		Kinds: events.KindAuthz.Mask(), OnlyDenied: true,
	})
	defer stop()

	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.send(&packet.Publish{Topic: []byte("forbidden/x"), Payload: []byte("x")})
	// 没有任何确认报文
	c.expectNothing(300 * time.Millisecond)
	// 但事件总线上有拒绝记录
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.Authz.Resource == "forbidden/x" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("QoS 0 的拒绝应产出授权事件")
}

// TestCloseDisconnectsAll 验证关闭 broker 时断开全部连接。
func TestCloseDisconnectsAll(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	var cs []*testClient
	for _, id := range []string{"a", "b", "c"} {
		c := r.dial(ConnInfo{})
		if ack := c.connect(id, true); ack.Code != reason.Success {
			t.Fatalf("%s 连接失败: %s", id, ack.Code)
		}
		cs = append(cs, c)
	}
	if n := r.b.Conns(); n != 3 {
		t.Errorf("连接数 = %d，期望 3", n)
	}
	if err := r.b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.b.Close(); err != nil {
		t.Errorf("重复关闭应幂等: %v", err)
	}
	for i, c := range cs {
		if rc := c.expectClosed(2 * time.Second); rc != reason.ServerShuttingDown {
			t.Errorf("第 %d 个连接的原因码 = %s，期望 Server shutting down", i, rc)
		}
	}
	// 关闭后不再接受新连接。
	// 这条路径不涉及服务端写出，因此可以用同步管道。
	srv, cli := net.Pipe()
	defer func() { _ = cli.Close() }()
	if err := r.b.Serve(context.Background(), srv, ConnInfo{}); err == nil {
		t.Error("关闭后 Serve 应报错")
	}
}

// TestStats 验证运行指标。
func TestStats(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.subscribe("a/b", packet.QoS1)

	st := r.b.Stats()
	if st.Conns != 1 {
		t.Errorf("连接数 = %d，期望 1", st.Conns)
	}
	if st.Router.Subscriptions != 1 {
		t.Errorf("订阅数 = %d，期望 1", st.Router.Subscriptions)
	}
	if st.Writer.Flushed == 0 {
		t.Error("写出池应有刷写计数")
	}
}

// TestServeContextCancellation 验证上下文取消使连接退出。
//
// 走真实 TCP 而非同步管道：取消后服务端会写出一个带原因码的 DISCONNECT，
// 而同步管道在客户端不读时会阻塞住那次写出。
func TestServeContextCancellation(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ctx, cancel := context.WithCancel(context.Background())
	c, done := r.dialCtx(ctx, ConnInfo{})
	c.connect("dev-1", true)

	cancel()
	// 取消后须有报文触发读循环检查上下文
	c.send(&packet.Pingreq{})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("上下文取消后 Serve 应返回")
	}
}

// TestUnsubscribeUnknownFilter 验证退订不存在的过滤器返回对应原因码。
func TestUnsubscribeUnknownFilter(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)

	c.send(&packet.Unsubscribe{
		PacketID: c.nextPID(),
		Filters:  [][]byte{[]byte("never/subscribed")},
	})
	ack := c.expect(packet.UNSUBACK).(*packet.Unsuback)
	if len(ack.Codes) != 1 || ack.Codes[0] != reason.NoSubscriptionExisted {
		t.Errorf("原因码 = %+v，期望 No subscription existed", ack.Codes)
	}
}

// TestSubscriptionQuotaReleasedOnUnsubscribe 验证退订归还配额。
//
// 不归还会让长期运行的客户端在反复订阅退订后耗尽配额——
// 这类泄漏只在运行很久之后才显现，必须有测试固定住。
func TestSubscriptionQuotaReleasedOnUnsubscribe(t *testing.T) {
	r := newPolicyRig(t, policyFull, map[quota.ID]int{quota.SubsPerConn: 2})
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)

	for i := 0; i < 5; i++ {
		if rc := c.subscribe("a/b", packet.QoS0); rc.IsError() {
			t.Fatalf("第 %d 轮订阅失败: %s", i, rc)
		}
		c.send(&packet.Unsubscribe{
			PacketID: c.nextPID(), Filters: [][]byte{[]byte("a/b")},
		})
		c.expect(packet.UNSUBACK)
	}
	// 配额应已完全归还
	if n := r.lim.Current(quota.SubsPerConn, "dev-1"); n != 0 {
		t.Errorf("订阅配额占用 = %d，期望 0（退订未归还配额）", n)
	}
	if n := r.lim.Current(quota.Subscriptions, ""); n != 0 {
		t.Errorf("全局订阅配额占用 = %d，期望 0", n)
	}
}

// TestConnectionQuotaReleasedOnDisconnect 验证断开归还连接数配额。
func TestConnectionQuotaReleasedOnDisconnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	for i := 0; i < 3; i++ {
		c := r.dial(ConnInfo{})
		c.connect("dev-"+string(rune('a'+i)), true)
		c.send(&packet.Disconnect{})
		c.expectClosed(time.Second)
	}
	// 等待清理完成
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.lim.Current(quota.Connections, "") == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("连接数配额占用 = %d，期望 0", r.lim.Current(quota.Connections, ""))
}

// TestAuthFailureReleasesConnectionQuota 验证认证失败也归还连接数配额。
//
// 这条路径最容易漏：失败分支上的资源归还往往是事后补的，
// 而漏掉它会让反复失败的连接尝试把配额耗尽。
func TestAuthFailureReleasesConnectionQuota(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	for i := 0; i < 5; i++ {
		c := r.dial(certInfo(fp(byte(100+i)), "nope"))
		if ack := c.connect("dev-"+string(rune('a'+i)), true); ack.Code == reason.Success {
			t.Fatal("未注册证书不应放行")
		}
		c.expectClosed(time.Second)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.lim.Current(quota.Connections, "") == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("认证失败后连接数配额占用 = %d，期望 0", r.lim.Current(quota.Connections, ""))
}

// TestV311HasNoProps 验证 3.1.1 的 CONNACK 不携带属性。
func TestV311HasNoProps(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.ver = packet.V311
	ack := c.connect("dev-311", true)
	if !ack.Props.Empty() {
		t.Error("3.1.1 的 CONNACK 不应携带属性")
	}
}

// TestAuthPacketRejected 验证增强认证尚未实现时以准确原因码拒绝。
func TestAuthPacketRejected(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	c.send(&packet.Auth{Code: reason.ContinueAuthentication})
	if rc := c.expectClosed(2 * time.Second); rc != reason.BadAuthenticationMethod {
		t.Errorf("断开原因码 = %s，期望 Bad authentication method", rc)
	}
}

// TestServerSideOnlyPacketRejected 验证服务端不应接收的报文导致断开。
func TestServerSideOnlyPacketRejected(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{})
	c.connect("dev-1", true)
	// CONNACK 是服务端发往客户端的，不应由客户端发来
	c.send(&packet.Connack{Code: reason.Success})
	if rc := c.expectClosed(2 * time.Second); rc != reason.ProtocolError {
		t.Errorf("断开原因码 = %s，期望 Protocol Error", rc)
	}
}

// TestMessageExpiryDecrementsOnDelivery 验证出站消息过期时间递减。
func TestMessageExpiryDecrementsOnDelivery(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	sub.subscribe("a/b", packet.QoS1)

	pub := r.dial(ConnInfo{})
	pub.connect("pub", true)
	pp := &packet.Publish{
		Topic: []byte("a/b"), Payload: []byte("x"),
		QoS: packet.QoS1, PacketID: pub.nextPID(),
	}
	pp.Props.MessageExpiry = 60
	pp.Props.Set(packet.PropMessageExpiry)
	pub.send(pp)
	pub.expect(packet.PUBACK)

	got := sub.expectPublish()
	if !got.Props.Has(packet.PropMessageExpiry) {
		t.Fatal("投递应携带消息过期间隔")
	}
	// 时钟未推进，剩余时间应仍为 60 秒左右
	if got.Props.MessageExpiry > 60 {
		t.Errorf("剩余过期时间 = %d，不应大于原值 60", got.Props.MessageExpiry)
	}
}

// TestListConns 验证连接列举的筛选与分页（PRD API-050）。
//
// 筛选与分页在服务端完成：十万连接下"拉全量到前端过滤"是一次
// 几十 MB 的响应，而调用方只要一页。
func TestListConns(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	for i := 0; i < 7; i++ {
		c := r.dial(ConnInfo{})
		if ack := c.connect(fmt.Sprintf("dev-%d", i), true); ack.Code != reason.Success {
			t.Fatalf("连接被拒: %s", ack.Code)
		}
	}
	// 等索引建立
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conns, _ := r.b.ListConns(ConnQuery{Limit: 100}); len(conns) == 7 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	all, next := r.b.ListConns(ConnQuery{Limit: 100})
	if len(all) != 7 {
		t.Fatalf("连接数 = %d，期望 7", len(all))
	}
	if next != "" {
		t.Errorf("一页装得下时不应给出游标，得到 %q", next)
	}
	// 升序，使分页游标有意义
	for i := 1; i < len(all); i++ {
		if all[i-1].ClientID >= all[i].ClientID {
			t.Errorf("列举顺序非升序: %v", all)
			break
		}
	}

	// 分页不重不漏
	seen := map[string]bool{}
	after := ""
	for i := 0; i < 10; i++ {
		page, nx := r.b.ListConns(ConnQuery{Limit: 3, After: after})
		for _, c := range page {
			if seen[c.ClientID] {
				t.Errorf("%s 出现在多页里", c.ClientID)
			}
			seen[c.ClientID] = true
		}
		if nx == "" {
			break
		}
		after = nx
	}
	if len(seen) != 7 {
		t.Errorf("分页共见到 %d 条，期望 7", len(seen))
	}

	// 前缀筛选
	page, _ := r.b.ListConns(ConnQuery{ClientIDPrefix: "dev-1", Limit: 100})
	if len(page) != 1 || page[0].ClientID != "dev-1" {
		t.Errorf("前缀筛选 = %v", page)
	}
	// 无匹配
	if page, _ := r.b.ListConns(ConnQuery{ClientIDPrefix: "zzz", Limit: 100}); len(page) != 0 {
		t.Errorf("无匹配应为空，得到 %v", page)
	}
	// 单页上限
	if page, _ := r.b.ListConns(ConnQuery{Limit: 99999}); len(page) != 7 {
		t.Errorf("= %d", len(page))
	}
}

// TestGetConnSnapshot 验证连接详情携带运维所需的字段。
func TestGetConnSnapshot(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	c := r.dial(ConnInfo{
		TLSVersion: 0x0304, ALPN: "x-amzn-mqtt-ca",
	})
	if ack := c.connect("dev-snap", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	deadline := time.Now().Add(3 * time.Second)
	var snap ConnSnapshot
	for time.Now().Before(deadline) {
		if s, ok := r.b.GetConn("dev-snap"); ok {
			snap = s
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snap.ClientID != "dev-snap" {
		t.Fatalf("未取到连接详情: %+v", snap)
	}
	if snap.ProtoVer != 5 {
		t.Errorf("协议版本 = %d", snap.ProtoVer)
	}
	if snap.TLSVersion != 0x0304 || snap.ALPN != "x-amzn-mqtt-ca" {
		t.Errorf("传输层字段未回传: %+v", snap)
	}
	if snap.ConnectedAtUnixMs == 0 {
		t.Error("应记录连接建立时间")
	}
	if snap.Epoch == 0 {
		t.Error("应记录所有权围栏世代")
	}
	if snap.AuthMethod == "" {
		t.Error("应记录认证方式")
	}

	// 不在线的 clientId
	if _, ok := r.b.GetConn("nobody"); ok {
		t.Error("不在线的 clientId 不应返回详情")
	}
}

// TestListConnsByThing 验证按设备名筛选。
func TestListConnsByThing(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	ps := r.addPolicy(policyFull)
	fp := meta.CertFingerprint{1, 2, 3}
	r.addCert(fp, "thing-a", ps, meta.CertActive)
	c := r.dial(ConnInfo{HasCert: true, CertFP: fp})
	if ack := c.connect("client-x", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if page, _ := r.b.ListConns(ConnQuery{Thing: "thing-a", Limit: 10}); len(page) == 1 {
			if page[0].Thing != "thing-a" {
				t.Errorf("= %+v", page[0])
			}
			if page[0].CertFP == "" {
				t.Error("证书认证的连接应回传指纹")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("按设备名筛选未命中")
}

func TestSortStringsHelper(t *testing.T) {
	s := []string{"c", "a", "b", "a"}
	sortStrings(s)
	for i := 1; i < len(s); i++ {
		if s[i-1] > s[i] {
			t.Fatalf("未排序: %v", s)
		}
	}
	sortStrings(nil)
	sortStrings([]string{"x"})
}

func TestHasPrefixHelper(t *testing.T) {
	if !hasPrefix("abc", "") || !hasPrefix("abc", "ab") {
		t.Error("应匹配")
	}
	if hasPrefix("ab", "abc") || hasPrefix("abc", "b") {
		t.Error("不应匹配")
	}
}
