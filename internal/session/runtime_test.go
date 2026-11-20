package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

func newTestRuntime(t *testing.T, outBuf int) (*Runtime, net.Conn) {
	t.Helper()
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	st := &State{
		ClientID: "dev-001",
		Epoch:    cluster.MakeEpoch(1, 1),
		Owner:    "node-1",
		ProtoVer: 5,
	}
	return NewRuntime(st, srv, outBuf), cli
}

func TestRuntimeEnqueueChecksEpoch(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	cur := r.Epoch()

	if err := r.Enqueue(cur, Outbound{Bytes: []byte{0xD0, 0x00}}); err != nil {
		t.Fatalf("当前 epoch 入队应成功，得到 %v", err)
	}
	// 被接管的旧会话不得继续写出
	stale := cluster.MakeEpoch(1, 0)
	if err := r.Enqueue(stale, Outbound{Bytes: []byte{0x00}}); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 入队应返回 ErrStaleEpoch，得到 %v", err)
	}
	if got := <-r.Outbound(); len(got.Bytes) != 2 {
		t.Errorf("出站队列内容不正确: %+v", got)
	}
}

func TestRuntimeEnqueueBackpressure(t *testing.T) {
	r, _ := newTestRuntime(t, 2)
	cur := r.Epoch()
	for i := 0; i < 2; i++ {
		if err := r.Enqueue(cur, Outbound{Bytes: []byte{byte(i)}}); err != nil {
			t.Fatalf("第 %d 条入队失败: %v", i, err)
		}
	}
	// 队列满时必须立即返回而非阻塞——投递路径不能被单个慢客户端拖住
	if err := r.Enqueue(cur, Outbound{Bytes: []byte{9}}); !errors.Is(err, ErrOutboundFull) {
		t.Errorf("队列满时应返回 ErrOutboundFull，得到 %v", err)
	}
	if NewRuntime(&State{}, nil, 0).cap() != 64 {
		t.Error("非法队列深度应回落到默认值")
	}
}

// cap 便于测试检查默认队列深度。
func (r *Runtime) cap() int { return cap(r.out) }

func TestRuntimeSubscribeReplacesAndUnsubscribe(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	cur := r.Epoch()

	if added, err := r.Subscribe(cur, Subscription{Filter: "a/b", QoS: 0}); err != nil || !added {
		t.Fatal(err)
	}
	// 同一过滤器重复订阅应替换选项而非追加（MQTT-3.8.4）
	if added, err := r.Subscribe(cur, Subscription{Filter: "a/b", QoS: 2, NoLocal: true}); err != nil || added {
		t.Fatal(err)
	}
	if n := len(r.State().Subscriptions); n != 1 {
		t.Fatalf("订阅数 = %d，期望 1", n)
	}
	if s := r.State().Subscriptions[0]; s.QoS != 2 || !s.NoLocal {
		t.Errorf("选项未被替换: %+v", s)
	}
	// 共享订阅与同名非共享订阅是不同的订阅
	if added, err := r.Subscribe(cur, Subscription{Filter: "a/b", Share: "grp", QoS: 1}); err != nil || !added {
		t.Fatal(err)
	}
	if n := len(r.State().Subscriptions); n != 2 {
		t.Fatalf("共享与非共享应各占一条，订阅数 = %d", n)
	}

	ok, err := r.Unsubscribe(cur, "a/b", "")
	if err != nil || !ok {
		t.Fatalf("退订应成功: ok=%v err=%v", ok, err)
	}
	if ok, _ := r.Unsubscribe(cur, "a/b", ""); ok {
		t.Error("重复退订应返回 false")
	}
	if ok, _ := r.Unsubscribe(cur, "zzz", ""); ok {
		t.Error("退订不存在的过滤器应返回 false")
	}
	if n := len(r.State().Subscriptions); n != 1 {
		t.Errorf("退订后订阅数 = %d，期望 1", n)
	}

	// epoch 校验同样作用于订阅变更
	stale := cluster.MakeEpoch(1, 0)
	if _, err := r.Subscribe(stale, Subscription{Filter: "x"}); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 订阅应被拒绝，得到 %v", err)
	}
	if _, err := r.Unsubscribe(stale, "x", ""); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 退订应被拒绝，得到 %v", err)
	}
}

// TestRuntimeSubscriptionsHandover 覆盖接管用的那对方法。
//
// 快照必须是副本：接管流程在新连接的协程里读它，而原会话仍在被对方的
// 读协程改动，共享底层数组就是竞态加别名陷阱（TST-107）。
func TestRuntimeSubscriptionsHandover(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	cur := r.Epoch()

	// 空订阅集取出来必须是 nil 而不是空切片：调用方会把它直接赋给
	// State.Subscriptions，而会话状态要能序列化后往返相等（TST-058）。
	if snap := r.SubscriptionsSnapshot(); snap != nil {
		t.Errorf("空会话的快照 = %#v，应为 nil", snap)
	}

	if _, err := r.Subscribe(cur, Subscription{Filter: "a/b", QoS: 1}); err != nil {
		t.Fatalf("订阅失败: %v", err)
	}
	snap := r.SubscriptionsSnapshot()
	if len(snap) != 1 || snap[0].Filter != "a/b" {
		t.Fatalf("快照 = %+v", snap)
	}
	// 改快照不得影响原会话。
	snap[0].Filter = "tampered"
	if got := r.State().Subscriptions[0].Filter; got != "a/b" {
		t.Errorf("快照与会话共享底层数组：会话里变成了 %q", got)
	}

	// 整体替换：接管时用被抢占连接的订阅集覆盖恢复出来的那份。
	if err := r.ReplaceSubscriptions(cur, []Subscription{{Filter: "c/d", QoS: 2}}); err != nil {
		t.Fatalf("替换失败: %v", err)
	}
	if subs := r.State().Subscriptions; len(subs) != 1 || subs[0].Filter != "c/d" {
		t.Errorf("替换后订阅集 = %+v", subs)
	}
	// 替换完旧的那条就该当不存在，否则重订会被当成重复而不占配额。
	if r.HasSubscription("a/b", "") {
		t.Error("替换后仍报告持有旧订阅")
	}

	// 被接管的旧会话不得改动订阅集。单机下这条分支也必须被覆盖
	// （PRD ARCH-011、ARCH-057）。
	stale := cluster.MakeEpoch(1, 0)
	if err := r.ReplaceSubscriptions(stale, nil); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 替换应返回 ErrStaleEpoch，得到 %v", err)
	}
	if subs := r.State().Subscriptions; len(subs) != 1 {
		t.Errorf("被拒的替换改动了订阅集: %+v", subs)
	}
}

func TestRuntimeCloseIsIdempotent(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	var calls int
	var mu sync.Mutex
	r.SetOnClose(func(cluster.EvictReason) {
		mu.Lock()
		calls++
		mu.Unlock()
	})

	const now = 1_700_000_000_000
	if err := r.Close(now, cluster.EvictTakenOver); err != nil {
		t.Fatal(err)
	}
	// 重复断开不得重复触发遗嘱与生命周期事件
	for i := 0; i < 5; i++ {
		if err := r.Close(now, cluster.EvictAdministrative); err != nil {
			t.Fatalf("重复关闭应幂等，得到 %v", err)
		}
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("断开回调被调用 %d 次，期望 1 次", got)
	}
	if !r.Detached() {
		t.Error("关闭后应为已解绑")
	}
	if r.Conn() != nil {
		t.Error("关闭后连接应为 nil")
	}
	if r.State().DisconnectUnixMs != now {
		t.Errorf("断开时间戳 = %d，期望 %d", r.State().DisconnectUnixMs, now)
	}
	if err := r.Enqueue(r.Epoch(), Outbound{Bytes: []byte{0}}); !errors.Is(err, ErrDetached) {
		t.Errorf("解绑后入队应返回 ErrDetached，得到 %v", err)
	}
}

func TestRuntimeCloseConcurrent(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	var mu sync.Mutex
	calls := 0
	r.SetOnClose(func(cluster.EvictReason) {
		mu.Lock()
		calls++
		mu.Unlock()
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Close(1, cluster.EvictTakenOver)
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("并发断开下回调被调用 %d 次，期望 1 次", calls)
	}
}

func TestRuntimeAccessors(t *testing.T) {
	r, _ := newTestRuntime(t, 4)
	if r.ClientID() != "dev-001" {
		t.Errorf("ClientID = %q", r.ClientID())
	}
	if r.Conn() == nil {
		t.Error("未关闭时连接不应为 nil")
	}
	if r.Detached() {
		t.Error("未关闭时不应为已解绑")
	}
}

func TestMemStore(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if _, ok, err := m.Load(ctx, "dev-1"); ok || err != nil {
		t.Errorf("空存储应返回未找到: ok=%v err=%v", ok, err)
	}

	st := sampleState()
	if err := m.Save(ctx, st); err != nil {
		t.Fatal(err)
	}
	if m.Len() != 1 {
		t.Errorf("会话数 = %d，期望 1", m.Len())
	}

	// 取出的必须是拷贝，否则调用方可绕过 epoch 校验直接改动存储
	got, ok, err := m.Load(ctx, "dev-001")
	if err != nil || !ok {
		t.Fatalf("应能取出: ok=%v err=%v", ok, err)
	}
	got.Subscriptions[0].QoS = 9
	again, _, _ := m.Load(ctx, "dev-001")
	if again.Subscriptions[0].QoS == 9 {
		t.Error("Load 返回的不是拷贝，存储被外部改动")
	}
}

func TestMemStoreEpochFencing(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	newer := sampleState()
	newer.Epoch = cluster.MakeEpoch(2, 1)
	if err := m.Save(ctx, newer); err != nil {
		t.Fatal(err)
	}

	// 被接管的旧会话不得覆盖新会话的状态
	older := sampleState()
	older.Epoch = cluster.MakeEpoch(1, 9)
	if err := m.Save(ctx, older); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 保存应返回 ErrStaleEpoch，得到 %v", err)
	}
	cur, _, _ := m.Load(ctx, "dev-001")
	if cur.Epoch != newer.Epoch {
		t.Errorf("存储中的 epoch = %d，期望 %d", cur.Epoch, newer.Epoch)
	}

	// 删除同样要校验 epoch
	if err := m.Delete(ctx, "dev-001", older.Epoch); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("旧 epoch 删除应被拒绝，得到 %v", err)
	}
	if m.Len() != 1 {
		t.Error("会话不应被旧 epoch 删除")
	}
	if err := m.Delete(ctx, "dev-001", newer.Epoch); err != nil {
		t.Errorf("当前 epoch 删除应成功，得到 %v", err)
	}
	if m.Len() != 0 {
		t.Errorf("删除后会话数 = %d，期望 0", m.Len())
	}
	// 删除不存在的会话不是错误
	if err := m.Delete(ctx, "dev-001", newer.Epoch); err != nil {
		t.Errorf("删除不存在的会话应无错误，得到 %v", err)
	}
	// 同 epoch 覆盖保存是允许的
	if err := m.Save(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(ctx, newer); err != nil {
		t.Errorf("同 epoch 覆盖应允许，得到 %v", err)
	}
}

func TestMemStoreExpire(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	const now = 1_700_000_000_000

	// 在线会话
	online := &State{ClientID: "online", ExpiryInterval: 10}
	// 已过期
	gone := &State{ClientID: "gone", ExpiryInterval: 10, DisconnectUnixMs: now - 20_000}
	// 未到期
	fresh := &State{ClientID: "fresh", ExpiryInterval: 10, DisconnectUnixMs: now - 1_000}
	for _, st := range []*State{online, gone, fresh} {
		if err := m.Save(ctx, st); err != nil {
			t.Fatal(err)
		}
	}

	n, err := m.Expire(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("清理 %d 条，期望 1", n)
	}
	if _, ok, _ := m.Load(ctx, "gone"); ok {
		t.Error("过期会话应被清理")
	}
	if _, ok, _ := m.Load(ctx, "online"); !ok {
		t.Error("在线会话不应被清理")
	}
	if _, ok, _ := m.Load(ctx, "fresh"); !ok {
		t.Error("未到期会话不应被清理")
	}
}

// 确认 MemStore 满足 Store 接口。
var _ Store = (*MemStore)(nil)
