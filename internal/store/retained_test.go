package store

import (
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/router"
)

func newRetainedMem(t *testing.T) *router.RetainedStore {
	t.Helper()
	return router.NewRetainedStore(topic.DefaultLimits(),
		func() int64 { return nowSec * 1000 })
}

// ver 造一个只带计数器的版本（单机用例用不上平手判据，见 router 包的同名函数）。
func ver(n uint64) router.Version { return router.Version{Counter: n} }

func TestRetainedWriteThrough(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	ps := NewRetainedStore(kv)
	mem := newRetainedMem(t)
	mem.SetSink(ps)

	mem.Set("a/b", &router.Message{Topic: "a/b", Payload: []byte("v1"), QoS: 1}, ver(1))
	if ps.Count() != 1 {
		t.Fatalf("持久化条数 = %d，期望 1", ps.Count())
	}
	// 覆盖
	mem.Set("a/b", &router.Message{Topic: "a/b", Payload: []byte("v2"), QoS: 1}, ver(2))
	if ps.Count() != 1 {
		t.Errorf("覆盖后条数 = %d，期望 1", ps.Count())
	}
	// 空负载清除须同步删除持久记录
	mem.Set("a/b", &router.Message{Topic: "a/b"}, ver(3))
	if ps.Count() != 0 {
		t.Errorf("清除后条数 = %d，期望 0", ps.Count())
	}
	// 被版本判定拒绝的写入不应落盘
	mem.Set("c/d", &router.Message{Topic: "c/d", Payload: []byte("x")}, ver(10))
	mem.Set("c/d", &router.Message{Topic: "c/d", Payload: []byte("stale")}, ver(5))
	loaded := newRetainedMem(t)
	if _, _, err := ps.LoadInto(loaded); err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Get([]byte("c/d"))
	if !ok || string(got.Payload) != "x" {
		t.Errorf("= %v, %v；被拒的写入不应落盘", got, ok)
	}
}

func TestRetainedPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	kv1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mem1 := newRetainedMem(t)
	mem1.SetSink(NewRetainedStore(kv1))
	for i, topicName := range []string{"a/1", "a/2", "b/1"} {
		mem1.Set(topicName, &router.Message{
			Topic: topicName, Payload: []byte(topicName), QoS: uint8(i % 3),
			Props: []byte{0x01, 0x02},
		}, ver(uint64(i+1)))
	}
	if err := kv1.Close(); err != nil {
		t.Fatal(err)
	}

	kv2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv2.Close() }()
	mem2 := newRetainedMem(t)
	n, maxVer, err := NewRetainedStore(kv2).LoadInto(mem2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("恢复条数 = %d，期望 3", n)
	}
	if maxVer.Counter != 3 {
		t.Errorf("最大版本 = %d，期望 3", maxVer.Counter)
	}
	for _, topicName := range []string{"a/1", "a/2", "b/1"} {
		m, ok := mem2.Get([]byte(topicName))
		if !ok {
			t.Errorf("%s 未恢复", topicName)
			continue
		}
		if string(m.Payload) != topicName {
			t.Errorf("%s 的负载 = %q", topicName, m.Payload)
		}
		if len(m.Props) != 2 {
			t.Errorf("%s 的属性未恢复: %v", topicName, m.Props)
		}
	}
	// 恢复后通配符匹配须可用——这要求内部的主题字节缓存被重建
	msgs := mem2.MatchInto(nil, []byte("a/+"))
	if len(msgs) != 2 {
		t.Errorf("通配符匹配到 %d 条，期望 2；恢复时未经 Set 重建内部缓存"+
			"会让匹配失效", len(msgs))
	}
}

// TestVersionHighWaterMarkPreventsLostWrites 是本轮修掉的真缺陷的回归测试。
//
// 版本计数器从 0 重新开始时，新发布的保留消息拿到版本 1，而覆盖判定是
// "版本更大才覆盖"——它会被恢复出来的版本 5000 的旧消息挡住并静默丢失。
// 这与 Shadow 的版本回退（SHDW-071）是同一类错误。
func TestVersionHighWaterMarkPreventsLostWrites(t *testing.T) {
	dir := t.TempDir()
	kv1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mem1 := newRetainedMem(t)
	mem1.SetSink(NewRetainedStore(kv1))
	// 模拟长期运行后的高版本号
	mem1.Set("t", &router.Message{Topic: "t", Payload: []byte("old")}, ver(5000))
	if err := kv1.Close(); err != nil {
		t.Fatal(err)
	}

	kv2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv2.Close() }()

	// 重启：一个全新的 Router，其计数器从 0 开始
	rt := router.New(router.Options{
		Self: "node-1", Limits: topic.DefaultLimits(),
		Now: func() int64 { return nowSec * 1000 },
	})
	ps := NewRetainedStore(kv2)
	_, maxVer, err := ps.LoadInto(rt.Retained())
	if err != nil {
		t.Fatal(err)
	}

	// 不调 SeedVersion 时，下一个版本是 1，新消息会被挡住
	if v := rt.NextVersion(); v.Counter != 1 {
		t.Fatalf("未 seed 时的下一个版本 = %d，期望 1", v.Counter)
	}
	if rt.Retained().Set("t", &router.Message{
		Topic: "t", Payload: []byte("new"),
	}, ver(1)) {
		t.Error("版本 1 不应能覆盖版本 5000")
	}
	got, _ := rt.Retained().Get([]byte("t"))
	if string(got.Payload) != "old" {
		t.Error("前置条件不成立")
	}

	// 抬高水位之后，新发布才能正常覆盖
	rt.SeedVersion(maxVer)
	v := rt.NextVersion()
	if v.Counter <= 5000 {
		t.Fatalf("seed 后的下一个版本 = %d，应大于 5000", v.Counter)
	}
	if !rt.Retained().Set("t", &router.Message{
		Topic: "t", Payload: []byte("new"),
	}, v) {
		t.Fatal("seed 后新发布应能覆盖")
	}
	got, _ = rt.Retained().Get([]byte("t"))
	if string(got.Payload) != "new" {
		t.Errorf("= %q，期望 new", got.Payload)
	}
}

func TestSeedVersionNeverLowers(t *testing.T) {
	rt := router.New(router.Options{
		Self: "n", Limits: topic.DefaultLimits(),
		Now: func() int64 { return nowSec * 1000 },
	})
	for i := 0; i < 10; i++ {
		rt.NextVersion()
	}
	// 更低的水位不得把计数器拉回去
	rt.SeedVersion(ver(3))
	if v := rt.NextVersion(); v.Counter != 11 {
		t.Errorf("= %d，期望 11；SeedVersion 不应降低计数器", v.Counter)
	}
}

// TestRetainedSweepWritesThrough 验证过期清理同步删除持久记录。
//
// 不写穿透的后果是过期的保留消息在重启后复活。
func TestRetainedSweepWritesThrough(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	ps := NewRetainedStore(kv)
	now := int64(nowSec * 1000)
	mem := router.NewRetainedStore(topic.DefaultLimits(), func() int64 { return now })
	mem.SetSink(ps)

	mem.Set("gone", &router.Message{
		Topic: "gone", Payload: []byte("x"), ExpiryUnixMs: now + 1000,
	}, ver(1))
	mem.Set("stays", &router.Message{Topic: "stays", Payload: []byte("y")}, ver(2))
	if ps.Count() != 2 {
		t.Fatalf("持久化条数 = %d", ps.Count())
	}

	now += 2000
	if n := mem.Sweep(); n != 1 {
		t.Fatalf("清理条数 = %d，期望 1", n)
	}
	if ps.Count() != 1 {
		t.Errorf("清理后持久化条数 = %d，期望 1；过期消息会在重启后复活",
			ps.Count())
	}
	loaded := newRetainedMem(t)
	if _, _, err := ps.LoadInto(loaded); err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.Get([]byte("gone")); ok {
		t.Error("过期消息不应被恢复")
	}
	if _, ok := loaded.Get([]byte("stays")); !ok {
		t.Error("未过期消息应被恢复")
	}
}

// TestRetainedPersistFailuresCounted 验证失败不被静默。
func TestRetainedPersistFailuresCounted(t *testing.T) {
	kv := NewMemKV()
	ps := NewRetainedStore(kv)
	mem := newRetainedMem(t)
	mem.SetSink(ps)
	// 关掉 KV 让写入必然失败
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	mem.Set("t", &router.Message{Topic: "t", Payload: []byte("x")}, ver(1))
	if n := mem.PersistFailures(); n == 0 {
		t.Error("持久化失败应被计数，而不是静默丢弃")
	}
	// 内存状态仍然改变：消息已可投递，此时让发布失败是更糟的选择
	if _, ok := mem.Get([]byte("t")); !ok {
		t.Error("持久化失败不应阻止内存写入")
	}
}

func TestRetainedNoSinkIsFine(t *testing.T) {
	mem := newRetainedMem(t)
	// 未设置 sink 时不应 panic，也不应计数失败
	mem.Set("t", &router.Message{Topic: "t", Payload: []byte("x")}, ver(1))
	mem.Set("t", &router.Message{Topic: "t"}, ver(2))
	if n := mem.PersistFailures(); n != 0 {
		t.Errorf("无 sink 时失败计数 = %d，期望 0", n)
	}
}

func TestRetainedCorruptRecord(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	if err := kv.Set(retainedKey("t"), []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewRetainedStore(kv).LoadInto(newRetainedMem(t)); err == nil {
		t.Error("损坏记录应使恢复失败，而不是静默跳过")
	}
	// 缺少主题的记录
	if err := kv.Set(retainedKey("t"), []byte(`{"v":1,"payload":"eA=="}`), true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewRetainedStore(kv).LoadInto(newRetainedMem(t)); err == nil {
		t.Error("缺少主题的记录应报错")
	}
}

func TestRetainedClosedKV(t *testing.T) {
	kv := NewMemKV()
	ps := NewRetainedStore(kv)
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ps.PutRetained("t", &router.Message{Topic: "t"}, ver(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("= %v，期望 ErrClosed", err)
	}
	if err := ps.DeleteRetained("t"); !errors.Is(err, ErrClosed) {
		t.Errorf("= %v，期望 ErrClosed", err)
	}
	if _, _, err := ps.LoadInto(newRetainedMem(t)); !errors.Is(err, ErrClosed) {
		t.Errorf("= %v，期望 ErrClosed", err)
	}
}
