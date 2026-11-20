package router

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

func newRetained(strict bool, now *int64) *RetainedStore {
	l := topic.DefaultLimits()
	l.AWSStrict = strict
	return NewRetainedStore(l, func() int64 { return *now })
}

// ver 造一个只带计数器的版本。
//
// 单机用例里平手判据（节点 ID）用不上：只有一个版本源，Counter 自己就是
// 全序。跨节点的那一半由 TestRetainedConvergence 与集群验收覆盖。
func ver(n uint64) Version { return Version{Counter: n} }

func TestRetainedSetGet(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)

	if !s.Set("a/b", &Message{Payload: []byte("v1")}, ver(1)) {
		t.Fatal("首次写入应成功")
	}
	m, ok := s.Get([]byte("a/b"))
	if !ok || string(m.Payload) != "v1" || m.Topic != "a/b" || m.Version != ver(1) {
		t.Fatalf("取出 %+v ok=%v", m, ok)
	}
	if _, ok := s.Get([]byte("nope")); ok {
		t.Error("不存在的主题不应被找到")
	}
}

// TestRetainedOverwriteByVersionNotTimestamp 固定住契约 10 的 ARCH-033：
// 覆盖判定依据 Version 而非时间戳。
//
// 集群下各节点的墙上时钟不可比，用时间戳会出现「旧消息覆盖新消息」的
// 偶发故障，而这类问题极难排查（契约 08）。
func TestRetainedOverwriteByVersionNotTimestamp(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	s.Set("a/b", &Message{Payload: []byte("v2")}, ver(2))

	// 更小的版本号不得覆盖，即便它后到达
	if s.Set("a/b", &Message{Payload: []byte("v1")}, ver(1)) {
		t.Error("更小版本号的写入应被拒绝")
	}
	m, _ := s.Get([]byte("a/b"))
	if string(m.Payload) != "v2" {
		t.Errorf("负载 = %q，期望 v2（旧版本不应覆盖）", m.Payload)
	}
	// 相同版本号也不覆盖，保持幂等
	if s.Set("a/b", &Message{Payload: []byte("same")}, ver(2)) {
		t.Error("相同版本号的写入应被拒绝")
	}
	// 更大版本号覆盖
	if !s.Set("a/b", &Message{Payload: []byte("v3")}, ver(3)) {
		t.Error("更大版本号应覆盖")
	}
	m, _ = s.Get([]byte("a/b"))
	if string(m.Payload) != "v3" {
		t.Errorf("负载 = %q，期望 v3", m.Payload)
	}
}

// TestRetainedEmptyPayloadClears 固定住 MQTT-3.3.1.3：
// 空负载表示清除该主题的 retained 消息。
func TestRetainedEmptyPayloadClears(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	s.Set("a/b", &Message{Payload: []byte("v1")}, ver(1))

	if !s.Set("a/b", &Message{Payload: nil}, ver(2)) {
		t.Error("清除应返回已变更")
	}
	if _, ok := s.Get([]byte("a/b")); ok {
		t.Error("清除后不应被找到")
	}
	// 对不存在的主题清除是幂等的，返回未变更
	if s.Set("nope", &Message{}, ver(3)) {
		t.Error("清除不存在的主题应返回未变更")
	}
	if s.Len() != 0 {
		t.Errorf("条数 = %d，期望 0", s.Len())
	}
}

func TestRetainedCopiesPayload(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	payload := []byte("original")
	props := []byte{0x01}
	s.Set("a/b", &Message{Payload: payload, Props: props}, ver(1))
	// 调用方复用缓冲区不应影响已保存的消息
	payload[0] = 'X'
	props[0] = 0xFF
	m, _ := s.Get([]byte("a/b"))
	if string(m.Payload) != "original" || m.Props[0] != 0x01 {
		t.Errorf("存储未复制负载: payload=%q props=%v", m.Payload, m.Props)
	}
}

func TestRetainedDelete(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	s.Set("a/b", &Message{Payload: []byte("v")}, ver(5))

	if s.Delete("a/b", ver(3)) {
		t.Error("更小版本号的删除应被拒绝")
	}
	if _, ok := s.Get([]byte("a/b")); !ok {
		t.Error("删除被拒后消息应仍在")
	}
	if !s.Delete("a/b", ver(6)) {
		t.Error("更大版本号的删除应成功")
	}
	if s.Delete("a/b", ver(7)) {
		t.Error("删除不存在的主题应返回 false")
	}
}

func TestRetainedMatch(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	for i, name := range []string{"a/b", "a/c", "a/b/c", "x/y"} {
		s.Set(name, &Message{Payload: []byte(name)}, ver(uint64(i+1)))
	}

	// 精确过滤器走一次 map 查找
	if got := s.MatchInto(nil, []byte("a/b")); len(got) != 1 || got[0].Topic != "a/b" {
		t.Errorf("精确匹配 = %+v", got)
	}
	if got := s.MatchInto(nil, []byte("nope")); len(got) != 0 {
		t.Errorf("精确不匹配应为空，得到 %+v", got)
	}
	// 单层通配符
	got := s.MatchInto(nil, []byte("a/+"))
	if len(got) != 2 {
		t.Errorf("a/+ 匹配 %d 条，期望 2", len(got))
	}
	// 多层通配符
	got = s.MatchInto(nil, []byte("a/#"))
	if len(got) != 3 {
		t.Errorf("a/# 匹配 %d 条，期望 3", len(got))
	}
	got = s.MatchInto(nil, []byte("#"))
	if len(got) != 4 {
		t.Errorf("# 匹配 %d 条，期望 4", len(got))
	}
}

func TestRetainedExpiry(t *testing.T) {
	now := int64(1000)
	s := newRetained(false, &now)
	s.Set("live", &Message{Payload: []byte("x")}, ver(1))
	s.Set("dies", &Message{Payload: []byte("x"), ExpiryUnixMs: 2000}, ver(2))

	if _, ok := s.Get([]byte("dies")); !ok {
		t.Error("未到期时应能取出")
	}
	now = 2000
	// 过期的 retained 消息在新订阅时不应被投递（PRD MQTT-055）
	if _, ok := s.Get([]byte("dies")); ok {
		t.Error("过期后不应被取出")
	}
	if got := s.MatchInto(nil, []byte("#")); len(got) != 1 || got[0].Topic != "live" {
		t.Errorf("过期消息不应参与匹配，得到 %+v", got)
	}
	// Get 与 Match 只做即时过滤，Sweep 才回收内存
	if s.Len() != 2 {
		t.Errorf("清理前条数 = %d，期望 2", s.Len())
	}
	if n := s.Sweep(); n != 1 {
		t.Errorf("清理 %d 条，期望 1", n)
	}
	if s.Len() != 1 {
		t.Errorf("清理后条数 = %d，期望 1", s.Len())
	}
}

func TestRetainedNoClock(t *testing.T) {
	// 未注入时钟时不做过期判定
	s := NewRetainedStore(topic.DefaultLimits(), nil)
	s.Set("a", &Message{Payload: []byte("x"), ExpiryUnixMs: 1}, ver(1))
	if _, ok := s.Get([]byte("a")); !ok {
		t.Error("无时钟时不应判为过期")
	}
}

// TestRouterRetainedForRespectsAWSStrict 固定住 PRD CMP-012：
// AWS 的通配符订阅收不到 retained，而规范要求能收到。
func TestRouterRetainedForRespectsAWSStrict(t *testing.T) {
	for _, strict := range []bool{false, true} {
		r := newTestRouter(strict)
		r.Retained().Set("a/b", &Message{Payload: []byte("v")}, r.NextVersion())

		// 精确订阅在两种模式下都能收到
		if got := r.RetainedFor([]byte("a/b")); len(got) != 1 {
			t.Errorf("strict=%v 精确订阅应收到 retained，得到 %d 条", strict, len(got))
		}
		// 通配符订阅：规范模式收到，AWS 模式收不到
		got := r.RetainedFor([]byte("a/#"))
		if strict && len(got) != 0 {
			t.Errorf("AWS 模式下通配符订阅不应收到 retained，得到 %d 条", len(got))
		}
		if !strict && len(got) != 1 {
			t.Errorf("规范模式下通配符订阅应收到 retained，得到 %d 条", len(got))
		}
		// 共享订阅在两种模式下都收不到（PRD MQTT-053）
		if got := r.RetainedFor([]byte("$share/g/a/b")); len(got) != 0 {
			t.Errorf("strict=%v 共享订阅不应收到 retained，得到 %d 条", strict, len(got))
		}
	}
}

func BenchmarkRetainedMatchExact(b *testing.B) {
	now := int64(0)
	s := newRetained(false, &now)
	for i := 0; i < 10000; i++ {
		s.Set("$aws/things/dev-"+itoa(i)+"/shadow", &Message{Payload: []byte("x")}, ver(uint64(i+1)))
	}
	name := []byte("$aws/things/dev-5000/shadow")
	buf := make([]*Message, 0, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = s.MatchInto(buf[:0], name)
	}
	if len(buf) != 1 {
		b.Fatalf("命中 %d 条", len(buf))
	}
}

// TestRetainedTombstonePreventsResurrection 钉住墓碑（MQTT-124、TST-157）。
//
// 清除之后，一条**更旧版本**的负载到达——它必须被拒。直接删除条目的实现
// 在这里会通过"本地没有记录所以不比版本"那一支，把它当成新消息接受，
// 于是已清除的 retained 自己回来了。
//
// 单机下碰不到：本地版本单调，而且没有迟到的更新。集群下必现，因为
// 乱序是常态（事件广播与全量 dump 会撞在一起）。
//
// 注意这条**不能**由 server 包的收敛不变量代替：缺墓碑时两个节点的反应
// 是对称的（都复活），集合仍然相同，收敛那条用例照样绿。
func TestRetainedTombstonePreventsResurrection(t *testing.T) {
	s := NewRetainedStore(topic.DefaultLimits(), func() int64 { return 1000 })

	s.Set("a/b", &Message{Payload: []byte("v5")}, ver(5))
	if !s.Set("a/b", &Message{}, ver(6)) {
		t.Fatal("清除应返回已变更")
	}
	// 迟到的旧负载。
	if s.Apply("a/b", &Message{Payload: []byte("v4")}, ver(4)) {
		t.Error("版本 4 不应能盖掉版本 6 的清除")
	}
	if m, ok := s.Get([]byte("a/b")); ok {
		t.Fatalf("已清除的保留消息复活了: %q", m.Payload)
	}
	// 更新的负载仍然可以覆盖墓碑——墓碑不是终态。
	if !s.Apply("a/b", &Message{Payload: []byte("v7")}, ver(7)) {
		t.Fatal("版本 7 应能覆盖版本 6 的墓碑")
	}
	if m, ok := s.Get([]byte("a/b")); !ok || string(m.Payload) != "v7" {
		t.Fatalf("= %v/%q，期望 v7", ok, payloadOf(m))
	}
}

// TestRetainedTombstoneIsInvisibleButDumped 墓碑对读侧不可见、对 dump 可见。
//
// 两头都要钉：对读侧可见的后果是订阅者收到一条空负载的 retained；
// 对 dump 不可见的后果是漏掉清除通知的那个节点永远撤不掉它本地那条
// （CLU-108 要求 dump 带上墓碑）。
func TestRetainedTombstoneIsInvisibleButDumped(t *testing.T) {
	s := NewRetainedStore(topic.DefaultLimits(), func() int64 { return 1000 })
	s.Set("a/b", &Message{Payload: []byte("v")}, ver(1))
	s.Set("a/b", &Message{}, ver(2))

	if _, ok := s.Get([]byte("a/b")); ok {
		t.Error("墓碑不该被 Get 看到")
	}
	if n := len(s.MatchInto(nil, []byte("a/+"))); n != 0 {
		t.Errorf("通配符匹配到 %d 条，期望 0", n)
	}
	if page, _ := s.List("", 10); len(page) != 0 {
		t.Errorf("列举到 %d 条，期望 0", len(page))
	}
	if s.Len() != 0 {
		t.Errorf("Len = %d，期望 0（墓碑不计数，否则界面上像是保留消息泄漏）", s.Len())
	}
	all := s.All()
	if len(all) != 1 || !all[0].Tombstone() {
		t.Fatalf("All 应带上墓碑，实际 %d 条", len(all))
	}
}

// TestRetainedTombstoneReclaimedAfterTTL 墓碑过了 TTL 才回收。
func TestRetainedTombstoneReclaimedAfterTTL(t *testing.T) {
	now := int64(1000)
	s := NewRetainedStore(topic.DefaultLimits(), func() int64 { return now })
	s.Set("a/b", &Message{Payload: []byte("v")}, ver(1))
	s.Set("a/b", &Message{}, ver(2))

	if n := s.Sweep(); n != 0 {
		t.Fatalf("回收了 %d 条；TTL 未到不该回收——回收太早会让一条更旧的"+
			"负载把清除撤销", n)
	}
	now += tombstoneTTLMs + 1
	if n := s.Sweep(); n != 1 {
		t.Fatalf("回收了 %d 条，期望 1", n)
	}
	if len(s.All()) != 0 {
		t.Error("TTL 过后墓碑应被回收")
	}
}

// TestVersionTieBreakIsDeterministic 平手判据必须是确定性全序（MQTT-123）。
//
// 两个节点在同一个计数器上撞车时，两边必须选出**同一个**胜者。少了这一半，
// 判据退化成"谁后到谁赢"，而那在两个节点上的结果恰好相反。
func TestVersionTieBreakIsDeterministic(t *testing.T) {
	a := Version{Counter: 7, Node: "node-a"}
	b := Version{Counter: 7, Node: "node-b"}
	if a.After(b) == b.After(a) {
		t.Fatalf("平手判据不是反对称的: a.After(b)=%v b.After(a)=%v",
			a.After(b), b.After(a))
	}
	if !b.After(a) {
		t.Error("期望节点 ID 大的那个赢（规则本身任选，但必须固定）")
	}
	// 计数器优先于节点 ID：否则一个节点 ID 靠后的节点会永远赢。
	older := Version{Counter: 6, Node: "node-z"}
	newer := Version{Counter: 7, Node: "node-a"}
	if !newer.After(older) || older.After(newer) {
		t.Error("计数器必须优先于节点 ID 比较")
	}
}

// TestNextVersionMonotonicWithFrozenClock 同一毫秒内连发两条，后一条必须赢。
//
// 这条是用逻辑时钟而不是墙钟戳的直接理由：墙钟方案在同一毫秒内两次发布
// 会拿到相同的戳，于是后一条被当成重复而丢弃——现象是"改了 retained 但
// 读出来还是旧的"。时钟在这条用例里是冻结的。
func TestNextVersionMonotonicWithFrozenClock(t *testing.T) {
	r := New(Options{Self: "node-a", Limits: topic.DefaultLimits(),
		Now: func() int64 { return 1000 }})
	v1 := r.NextVersion()
	v2 := r.NextVersion()
	if !v2.After(v1) {
		t.Fatalf("%v 未大于 %v", v2, v1)
	}
	s := r.Retained()
	s.Set("a/b", &Message{Payload: []byte("first")}, v1)
	s.Set("a/b", &Message{Payload: []byte("second")}, v2)
	if m, ok := s.Get([]byte("a/b")); !ok || string(m.Payload) != "second" {
		t.Fatalf("= %q，期望 second", payloadOf(m))
	}
}

// payloadOf 在 m 可能为 nil 时安全取负载，供错误信息使用。
func payloadOf(m *Message) []byte {
	if m == nil {
		return nil
	}
	return m.Payload
}
