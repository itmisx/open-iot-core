package router

import (
	"sort"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

func newTestRouter(strict bool) *Router {
	l := topic.DefaultLimits()
	l.AWSStrict = strict
	return New(Options{Self: "node-1", Limits: l, Now: func() int64 { return 1_000 }})
}

func clientsOf(local []SubRef) []string {
	out := make([]string, 0, len(local))
	for _, s := range local {
		out = append(out, s.ClientID)
	}
	sort.Strings(out)
	return out
}

func mustSub(t *testing.T, r *Router, clientID, filter string) {
	t.Helper()
	if err := r.Subscribe(Subscription{ClientID: clientID, Filter: filter, QoS: 1}); err != nil {
		t.Fatalf("%s 订阅 %q 失败: %v", clientID, filter, err)
	}
}

// TestRouteReturnsBothLocalAndRemote 固定住契约 02 最关键的性质：
// Route 从 v0.1 起就返回两份结果，单机下 remote 为空但调用形态不变。
func TestRouteReturnsBothLocalAndRemote(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "app-1", "sensors/#")
	mustSub(t, r, "dev-1", "sensors/temp")

	local, remote := r.Route([]byte("sensors/temp"))
	if got := clientsOf(local); len(got) != 2 || got[0] != "app-1" || got[1] != "dev-1" {
		t.Errorf("本地订阅者 = %v，期望 [app-1 dev-1]", got)
	}
	// 单机下 remote 恒为空——但返回值的形态与集群一致
	if len(remote) != 0 {
		t.Errorf("单节点下远端目标应为空，得到 %v", remote)
	}
}

func TestRouteCarriesSubscriptionOptions(t *testing.T) {
	r := newTestRouter(false)
	if err := r.Subscribe(Subscription{
		ClientID: "c1", Filter: "a/b", QoS: 2,
		NoLocal: true, RetainAsPub: true, RetainHandling: 2, ID: 77,
	}); err != nil {
		t.Fatal(err)
	}
	local, _ := r.Route([]byte("a/b"))
	if len(local) != 1 {
		t.Fatalf("命中 %d 条", len(local))
	}
	s := local[0]
	if s.QoS != 2 || !s.NoLocal || !s.RetainAsPub || s.RetainHandling != 2 || s.ID != 77 {
		t.Errorf("订阅选项未透传: %+v", s)
	}
}

func TestRouteDeviceScopedUsesLocator(t *testing.T) {
	// 设备域主题经归属查询定位，不进入全局订阅表（契约 03）
	r := New(Options{
		Self:    "node-1",
		Limits:  topic.DefaultLimits(),
		Locator: fixedLocator{node: "node-2", ok: true},
	})
	_, remote := r.Route([]byte("$aws/things/dev-001/shadow/update/delta"))
	if len(remote) != 1 || remote[0] != "node-2" {
		t.Errorf("远端目标 = %v，期望 [node-2]", remote)
	}

	// 会话在本节点时不转发
	r2 := New(Options{
		Self: "node-1", Limits: topic.DefaultLimits(),
		Locator: fixedLocator{node: "node-1", ok: true},
	})
	if _, remote := r2.Route([]byte("$aws/things/dev-001/shadow/get")); len(remote) != 0 {
		t.Errorf("本节点会话不应转发，得到 %v", remote)
	}

	// 设备无会话、且没有任何节点通告过兴趣时不转发
	r3 := New(Options{
		Self: "node-1", Limits: topic.DefaultLimits(),
		Locator: fixedLocator{ok: false},
	})
	if _, remote := r3.Route([]byte("$aws/things/dev-001/shadow/get")); len(remote) != 0 {
		t.Errorf("无会话时不应转发，得到 %v", remote)
	}
}

// TestDeviceScopedFallsBackToInterest 钉住 MQTT-128。
//
// 设备离线时没有任何节点归属它，但它的持久会话连着订阅仍留在上次接入的
// 那个节点上，而那个节点通告过 `$aws/things/<设备>/#`。归属查不到就此
// 返回的后果是：发给离线设备的 QoS 1 只在它上次接入的那个节点上入队，
// 在别的节点上发布就直接消失——而没有任何位置会报错。
//
// 这个缺口是会话复制接上之后在三进程验收里压出来的（2026-11-20）。
func TestDeviceScopedFallsBackToInterest(t *testing.T) {
	r := New(Options{
		Self: "node-1", Limits: topic.DefaultLimits(),
		// 设备不在线：谁都不归属它。
		Locator: fixedLocator{ok: false},
	})
	// node-2 上有它的离线持久会话，于是 node-2 通告了这个通配符。
	r.Interest().Advertise("node-2", []string{"$aws/things/dev-001/#"})

	_, remote := r.Route([]byte("$aws/things/dev-001/cmd"))
	if len(remote) != 1 || remote[0] != "node-2" {
		t.Fatalf("远端目标 = %v，期望 [node-2]：设备域主题在归属查不到时"+
			"必须回落到兴趣表，否则离线队列只在上次接入的那个节点上存在",
			remote)
	}

	// 归属查得到时仍以归属为准，不要把兴趣表的那一份也加上——
	// 同一帧转两次会在那个节点上被投两遍。
	r2 := New(Options{
		Self: "node-1", Limits: topic.DefaultLimits(),
		Locator: fixedLocator{node: "node-3", ok: true},
	})
	r2.Interest().Advertise("node-2", []string{"$aws/things/dev-001/#"})
	if _, remote := r2.Route([]byte("$aws/things/dev-001/cmd")); len(remote) != 1 ||
		remote[0] != "node-3" {
		t.Fatalf("远端目标 = %v，期望只有归属节点 [node-3]", remote)
	}
}

type fixedLocator struct {
	node cluster.NodeID
	ok   bool
}

func (f fixedLocator) LocateThing([]byte) (cluster.NodeID, bool) { return f.node, f.ok }

// TestExactFilterIsNotPropagated 验证精确订阅不会进入跨节点兴趣。
//
// 删掉 Advertise 里的通配符判断，floor/temp 会让节点留在兴趣表里。
func TestExactFilterIsNotPropagated(t *testing.T) {
	s := NewInterestSet(topic.Limits{})
	s.Advertise("node-2", []string{"floor/temp", "floor/#"})
	if s.Nodes() != 1 {
		t.Fatalf("节点数 = %d，通配符应当留下", s.Nodes())
	}
	s.Advertise("node-2", []string{"floor/temp"})
	if s.Nodes() != 0 {
		t.Fatal("只剩精确订阅时，兴趣表里不该还有这个节点")
	}
}

// TestRouteGeneralTopicUsesInterest 验证只有通配符兴趣参与跨节点传播。
func TestRouteGeneralTopicUsesInterest(t *testing.T) {
	r := newTestRouter(false)
	r.Interest().Advertise("node-2", []string{"telemetry/#"})
	r.Interest().Advertise("node-3", []string{"other/+"})

	_, remote := r.Route([]byte("telemetry/site-1/temp"))
	if len(remote) != 1 || remote[0] != "node-2" {
		t.Errorf("远端目标 = %v，期望 [node-2]", remote)
	}
	if _, remote := r.Route([]byte("unrelated/topic")); len(remote) != 0 {
		t.Errorf("无兴趣匹配时不应转发，得到 %v", remote)
	}
	// 同一节点有多条匹配过滤器时只出现一次
	r.Interest().Advertise("node-2", []string{"telemetry/#", "telemetry/+/temp"})
	if _, remote := r.Route([]byte("telemetry/site-1/temp")); len(remote) != 1 {
		t.Errorf("同节点应去重，得到 %v", remote)
	}
	// 撤销兴趣
	r.Interest().Forget("node-2")
	if _, remote := r.Route([]byte("telemetry/site-1/temp")); len(remote) != 0 {
		t.Errorf("撤销兴趣后不应转发，得到 %v", remote)
	}
	// 空通告即移除
	r.Interest().Advertise("node-3", nil)
	if n := r.Interest().Nodes(); n != 0 {
		t.Errorf("节点数 = %d，期望 0", n)
	}
}

func TestSubscribeRejectsInvalidFilter(t *testing.T) {
	r := newTestRouter(false)
	for _, f := range []string{"", "a/#/b", "a+b", "$share/g"} {
		if err := r.Subscribe(Subscription{ClientID: "c", Filter: f}); err == nil {
			t.Errorf("非法过滤器 %q 应被拒绝", f)
		}
	}
}

func TestUnsubscribeAndRemoveClient(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "c1", "a/b")
	mustSub(t, r, "c1", "a/#")
	mustSub(t, r, "c2", "a/b")

	if !r.Unsubscribe("c1", "a/b") {
		t.Fatal("退订应成功")
	}
	if r.Unsubscribe("c1", "a/b") {
		t.Error("重复退订应返回 false")
	}
	if r.Unsubscribe("unknown", "a/b") {
		t.Error("未知客户端退订应返回 false")
	}
	local, _ := r.Route([]byte("a/b"))
	if got := clientsOf(local); len(got) != 2 || got[0] != "c1" || got[1] != "c2" {
		// c1 仍通过 a/# 命中
		t.Errorf("命中 %v，期望 [c1 c2]", got)
	}

	if n := r.RemoveClient("c1"); n != 1 {
		t.Errorf("移除 %d 条，期望 1", n)
	}
	local, _ = r.Route([]byte("a/b"))
	if got := clientsOf(local); len(got) != 1 || got[0] != "c2" {
		t.Errorf("命中 %v，期望 [c2]", got)
	}
	if n := r.RemoveClient("c1"); n != 0 {
		t.Errorf("重复移除应返回 0，得到 %d", n)
	}
}

// TestHandleReuseDoesNotLeakSubscriptions 是句柄回收的回归保护：
// 句柄复用若未正确清理，新客户端会继承旧客户端的订阅。
func TestHandleReuseDoesNotLeakSubscriptions(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "c1", "a/b")
	r.RemoveClient("c1")
	// c2 会复用 c1 释放的句柄
	mustSub(t, r, "c2", "x/y")

	local, _ := r.Route([]byte("a/b"))
	if len(local) != 0 {
		t.Errorf("已移除客户端的订阅不应被继承，命中 %v", clientsOf(local))
	}
	local, _ = r.Route([]byte("x/y"))
	if got := clientsOf(local); len(got) != 1 || got[0] != "c2" {
		t.Errorf("命中 %v，期望 [c2]", got)
	}
}

func TestSubscribeReplacesOptions(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "c1", "a/b")
	if err := r.Subscribe(Subscription{ClientID: "c1", Filter: "a/b", QoS: 2, ID: 9}); err != nil {
		t.Fatal(err)
	}
	local, _ := r.Route([]byte("a/b"))
	if len(local) != 1 {
		t.Fatalf("重复订阅应替换而非新增，命中 %d 条", len(local))
	}
	if local[0].QoS != 2 || local[0].ID != 9 {
		t.Errorf("选项未被替换: %+v", local[0])
	}
}

func TestRouteIntoReusesBuffers(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "c1", "a/b")
	r.Interest().Advertise("node-2", []string{"a/#"})

	lb := make([]SubRef, 0, 8)
	rb := make([]cluster.NodeID, 0, 4)
	lb, rb = r.RouteInto(lb[:0], rb[:0], []byte("a/b"))
	if len(lb) != 1 || len(rb) != 1 {
		t.Fatalf("命中 %d 本地 %d 远端", len(lb), len(rb))
	}
	lb, rb = r.RouteInto(lb[:0], rb[:0], []byte("zzz"))
	if len(lb) != 0 || len(rb) != 0 {
		t.Errorf("无命中时长度应为 0，得到 %d 与 %d", len(lb), len(rb))
	}
}

func TestStats(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "c1", "a/b")
	mustSub(t, r, "c2", "a/#")
	if err := r.Subscribe(Subscription{ClientID: "c3", Filter: "$share/g/x/y"}); err != nil {
		t.Fatal(err)
	}
	r.Retained().Set("a/b", &Message{Payload: []byte("x")}, r.NextVersion())
	r.Interest().Advertise("node-2", []string{"#"})

	st := r.Stats()
	if st.Subscriptions != 2 {
		t.Errorf("订阅数 = %d，期望 2（共享订阅不入订阅树）", st.Subscriptions)
	}
	if st.SharedGroups != 1 {
		t.Errorf("共享组数 = %d，期望 1", st.SharedGroups)
	}
	if st.Retained != 1 {
		t.Errorf("retained 数 = %d，期望 1", st.Retained)
	}
	if st.InterestNodes != 1 {
		t.Errorf("兴趣节点数 = %d，期望 1", st.InterestNodes)
	}
	if st.Clients != 2 {
		t.Errorf("客户端数 = %d，期望 2", st.Clients)
	}
	if st.TrieNodes <= 1 {
		t.Errorf("订阅树节点数 = %d", st.TrieNodes)
	}
}

func TestNextVersionIsMonotonic(t *testing.T) {
	r := newTestRouter(false)
	prev := Version{}
	for i := 0; i < 100; i++ {
		v := r.NextVersion()
		if !v.After(prev) {
			t.Fatalf("版本号不单调: %v 未大于 %v", v, prev)
		}
		// 节点 ID 必须跟着带出来：少了它，两个节点的版本在同一个计数器上
		// 平手，覆盖判定就退回"谁后到谁赢"（MQTT-123）。
		if v.Node != string(r.self) {
			t.Fatalf("版本里的节点 ID = %q，期望 %q", v.Node, r.self)
		}
		prev = v
	}
}

func BenchmarkRouteExact(b *testing.B) {
	r := newTestRouter(false)
	for i := 0; i < 1000; i++ {
		_ = r.Subscribe(Subscription{
			ClientID: "dev-" + itoa(i),
			Filter:   "$aws/things/dev-" + itoa(i) + "/shadow/update/delta",
		})
	}
	_ = r.Subscribe(Subscription{ClientID: "app", Filter: "$aws/things/+/shadow/update/delta"})
	name := []byte("$aws/things/dev-500/shadow/update/delta")
	lb := make([]SubRef, 0, 8)
	rb := make([]cluster.NodeID, 0, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb, rb = r.RouteInto(lb[:0], rb[:0], name)
	}
	if len(lb) != 2 {
		b.Fatalf("命中 %d 条，期望 2", len(lb))
	}
}

func BenchmarkRouteNoHit(b *testing.B) {
	r := newTestRouter(false)
	for i := 0; i < 1000; i++ {
		_ = r.Subscribe(Subscription{ClientID: "dev-" + itoa(i), Filter: "a/" + itoa(i)})
	}
	// 主题提到循环外：字面量转换会在每次迭代分配，掩盖被测代码的真实开销
	name := []byte("no/such/topic")
	lb := make([]SubRef, 0, 8)
	rb := make([]cluster.NodeID, 0, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lb, rb = r.RouteInto(lb[:0], rb[:0], name)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[p:])
}

// TestRouterHas 验证路由表能回答"这个客户端是否已持有该订阅"。
//
// 它是全局订阅额度的权威（见 Router.Has）：普通订阅问订阅树，
// 共享订阅问组成员表，两条路都要覆盖——漏掉共享那条会让共享订阅
// 每次重订都多占一份全局额度。
func TestRouterHas(t *testing.T) {
	r := newTestRouter(false)
	mustSub(t, r, "dev-1", "a/b")
	mustSub(t, r, "dev-2", "$share/g/s/t")

	if !r.Has("dev-1", "a/b") {
		t.Error("普通订阅应报告存在")
	}
	if r.Has("dev-2", "a/b") {
		t.Error("别人的订阅不应算在自己头上")
	}
	if r.Has("dev-1", "a/c") {
		t.Error("未订阅的过滤器不应报告存在")
	}
	// 从未订阅过任何东西的客户端连句柄都没有，这条分支要单独走到。
	if r.Has("dev-never", "a/b") {
		t.Error("无句柄的客户端不应报告存在")
	}
	if !r.Has("dev-2", "$share/g/s/t") {
		t.Error("共享订阅应报告存在")
	}
	if r.Has("dev-2", "$share/other/s/t") {
		t.Error("另一个组不应报告存在")
	}
	// 退订与整体移除之后都必须翻回 false。
	if !r.Unsubscribe("dev-1", "a/b") {
		t.Fatal("退订应成功")
	}
	if r.Has("dev-1", "a/b") {
		t.Error("退订后仍报告存在")
	}
	if n := r.RemoveClient("dev-2"); n != 1 {
		t.Fatalf("移除客户端返回 %d，期望 1", n)
	}
	if r.Has("dev-2", "$share/g/s/t") {
		t.Error("移除客户端后仍报告存在")
	}
}
