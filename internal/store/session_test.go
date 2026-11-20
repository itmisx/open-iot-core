package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

func ep(term uint32, seq uint32) cluster.Epoch {
	return cluster.MakeEpoch(term, seq)
}

func st(clientID string, e cluster.Epoch) *session.State {
	return &session.State{
		ClientID: clientID, Epoch: e, Owner: "node-1", ProtoVer: 5,
		ExpiryInterval: 3600,
		Subscriptions: []session.Subscription{
			{Filter: "a/b", QoS: 1, ID: 7},
		},
		CreatedUnixMs: nowSec * 1000,
	}
}

func TestSessionRoundTrip(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	if _, ok, err := s.Load(ctx, "c1"); err != nil || ok {
		t.Errorf("空存储 = %v, %v", ok, err)
	}
	in := st("c1", ep(1, 1))
	in.Queued = []session.QueuedMsg{
		{QoS: 1, Topic: "t", Payload: []byte("p"), SubIDs: []uint32{7}},
	}
	in.Inflight = []session.InflightMsg{
		{PacketID: 3, QoS: 1, Topic: "t2", Payload: []byte("q")},
	}
	in.InboundUnacked = []uint16{9, 10}
	in.HasWill = true
	in.Will = session.WillMsg{Topic: "w", Payload: []byte("bye"), QoS: 1}
	if err := s.Save(ctx, in); err != nil {
		t.Fatal(err)
	}

	out, ok, err := s.Load(ctx, "c1")
	if err != nil || !ok {
		t.Fatalf("= %v, %v", ok, err)
	}
	// 整体比较：逐字段断言会在 State 新增字段时漏测，
	// 而漏测的表现是"重启后某个属性没了"
	a, _ := json.Marshal(in)
	b, _ := json.Marshal(out)
	if string(a) != string(b) {
		t.Errorf("往返后不一致\n写入: %s\n读出: %s", a, b)
	}
}

// TestSessionEpochFencing 是契约 01 在持久化层的落点。
//
// 已被接管的旧会话不得覆盖新会话的状态。若放过它，一个正在断开的
// 旧连接会用它的旧订阅列表覆盖掉新连接刚建立的订阅。
func TestSessionEpochFencing(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	newer := st("c1", ep(1, 5))
	newer.Subscriptions = []session.Subscription{{Filter: "new", QoS: 1}}
	if err := s.Save(ctx, newer); err != nil {
		t.Fatal(err)
	}

	older := st("c1", ep(1, 2))
	older.Subscriptions = []session.Subscription{{Filter: "old", QoS: 0}}
	if err := s.Save(ctx, older); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("旧 epoch 的写入应被拒，得到 %v", err)
	}
	// 状态不得被改动
	out, _, _ := s.Load(ctx, "c1")
	if len(out.Subscriptions) != 1 || out.Subscriptions[0].Filter != "new" {
		t.Errorf("被拒的写入改动了状态: %+v", out.Subscriptions)
	}

	// 相同 epoch 的写入允许（同一连接的多次保存）
	same := st("c1", ep(1, 5))
	same.Subscriptions = []session.Subscription{{Filter: "same", QoS: 2}}
	if err := s.Save(ctx, same); err != nil {
		t.Errorf("相同 epoch 应允许: %v", err)
	}
	// 更高 epoch 的写入允许（接管）
	if err := s.Save(ctx, st("c1", ep(2, 1))); err != nil {
		t.Errorf("更高 epoch 应允许: %v", err)
	}
}

func TestSessionDeleteFencing(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	if err := s.Save(ctx, st("c1", ep(1, 5))); err != nil {
		t.Fatal(err)
	}
	// 删除是破坏性操作，围栏用相等而非小于：
	// 只有持有当前 epoch 的那个连接才有权删
	if err := s.Delete(ctx, "c1", ep(1, 4)); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("epoch 不符的删除应被拒，得到 %v", err)
	}
	if err := s.Delete(ctx, "c1", ep(2, 1)); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Errorf("更高 epoch 的删除也应被拒，得到 %v", err)
	}
	if _, ok, _ := s.Load(ctx, "c1"); !ok {
		t.Fatal("被拒的删除不应真的删掉会话")
	}
	if err := s.Delete(ctx, "c1", ep(1, 5)); err != nil {
		t.Errorf("持有当前 epoch 的删除应成功: %v", err)
	}
	if _, ok, _ := s.Load(ctx, "c1"); ok {
		t.Error("会话应已被删除")
	}
	// 删除不存在的会话不是错误
	if err := s.Delete(ctx, "nobody", ep(1, 1)); err != nil {
		t.Errorf("删除不存在的会话应成功: %v", err)
	}
}

func TestSessionPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	kv1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	s1 := NewSessionStore(kv1)
	want := st("c1", ep(3, 7))
	want.Queued = []session.QueuedMsg{
		{QoS: 1, Topic: "offline/1", Payload: []byte("m1")},
		{QoS: 2, Topic: "offline/2", Payload: []byte("m2")},
	}
	if err := s1.Save(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := kv1.Close(); err != nil {
		t.Fatal(err)
	}

	kv2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv2.Close() }()
	s2 := NewSessionStore(kv2)

	got, ok, err := s2.Load(ctx, "c1")
	if err != nil || !ok {
		t.Fatalf("重启后应能读回会话: %v, %v", ok, err)
	}
	if got.Epoch != want.Epoch {
		t.Errorf("epoch = %v，期望 %v；epoch 丢失会让围栏失效", got.Epoch, want.Epoch)
	}
	// 离线队列必须一同恢复：客户端以 CleanStart=false 重连时期待它仍在
	if len(got.Queued) != 2 || got.Queued[0].Topic != "offline/1" {
		t.Errorf("离线队列 = %+v", got.Queued)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0].ID != 7 {
		t.Errorf("订阅 = %+v", got.Subscriptions)
	}
}

func TestSessionExpire(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	// 一个已断开且过期的会话
	gone := st("expired", ep(1, 1))
	gone.ExpiryInterval = 10
	gone.DisconnectUnixMs = 1000
	if err := s.Save(ctx, gone); err != nil {
		t.Fatal(err)
	}
	// 一个仍在线的会话（未断开）
	live := st("live", ep(1, 1))
	live.ExpiryInterval = 10
	if err := s.Save(ctx, live); err != nil {
		t.Fatal(err)
	}

	n, err := s.Expire(ctx, 1000+11_000)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("清理条数 = %d，期望 1", n)
	}
	if _, ok, _ := s.Load(ctx, "expired"); ok {
		t.Error("过期会话应已被清理")
	}
	if _, ok, _ := s.Load(ctx, "live"); !ok {
		t.Error("在线会话不应被清理")
	}
	// 幂等
	if n, err := s.Expire(ctx, 1000+11_000); err != nil || n != 0 {
		t.Errorf("= %d, %v", n, err)
	}
}

// TestSessionExpireCleansCorruptRecords 验证坏记录被一并清理。
//
// 留着它只会让对应 clientId 每次连接都读到一条坏记录。
func TestSessionExpireCleansCorruptRecords(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	if err := kv.Set(sessionKey("broken"), []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	n, err := s.Expire(context.Background(), nowSec*1000)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("清理条数 = %d，期望 1", n)
	}
	if s.Count() != 0 {
		t.Error("坏记录应已被清理")
	}
}

// TestSessionCorruptRecordCanBeOverwritten 验证坏记录不会让 clientId 永久失联。
//
// 与影子不同：会话状态可以从新连接重建，而影子是设备数据——
// 覆盖影子的坏记录等于丢数据，所以那边是报错，这边是允许覆盖。
func TestSessionCorruptRecordCanBeOverwritten(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()
	if err := kv.Set(sessionKey("c1"), []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Load(ctx, "c1"); err == nil {
		t.Error("读取坏记录应报错")
	}
	if err := s.Save(ctx, st("c1", ep(1, 1))); err != nil {
		t.Errorf("应允许覆盖坏记录: %v", err)
	}
	if _, ok, err := s.Load(ctx, "c1"); err != nil || !ok {
		t.Errorf("覆盖后应可读: %v, %v", ok, err)
	}
}

func TestSessionValidation(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()
	if err := s.Save(ctx, nil); err == nil {
		t.Error("nil 会话应报错")
	}
	if err := s.Save(ctx, &session.State{}); err == nil {
		t.Error("缺少 clientId 应报错")
	}
	raw, _ := json.Marshal(&sessionRecord{V: sessionRecordVersion + 1})
	if _, err := decodeSession(raw); err == nil {
		t.Error("更高的记录版本应报错")
	}
	raw, _ = json.Marshal(&sessionRecord{V: 1})
	if _, err := decodeSession(raw); err == nil {
		t.Error("缺少状态的记录应报错")
	}
}

// TestSessionConcurrentFencing 验证并发写入下围栏不被绕过。
//
// 围栏是「读当前 epoch、比较、写入」三步。若不原子，两个并发的 Save
// 可能交错，旧 epoch 的那个在新 epoch 之后落地——被接管的旧会话
// 就覆盖掉了新会话。
func TestSessionConcurrentFencing(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cur := st("c1", ep(1, uint32(i)))
			cur.KeepAlive = uint16(i)
			_ = s.Save(ctx, cur)
		}(i)
	}
	wg.Wait()

	out, ok, err := s.Load(ctx, "c1")
	if err != nil || !ok {
		t.Fatal(err)
	}
	// 最终留下的必须是某个 epoch，且其 KeepAlive 与 epoch 一致——
	// 不一致意味着两次 Save 的字段被交错写入了
	if out.KeepAlive != uint16(out.Epoch.Seq()) {
		t.Errorf("状态被交错写入：epoch seq = %d 但 KeepAlive = %d",
			out.Epoch.Seq(), out.KeepAlive)
	}
}

// TestSessionConcurrentSavesAllLand 钉住合并提交不丢写入（MQTT-118）。
//
// 会话落盘走的是 bbolt 的 Batch：多个并发调用被合进同一个事务，fsync 被
// 摊薄。这条用例压的是那条路径上唯一的新约束——**事务体会被重试，因此
// 必须幂等**。写错的表现不是报错，而是并发高时偶尔少一条会话记录，而少
// 一条的现象是「那台设备重连后订阅没了」，几乎无法溯源。
//
// 用真 bbolt 而不是 MemKV：合并是 bbolt 的行为，MemKV 压不到它。
func TestSessionConcurrentSavesAllLand(t *testing.T) {
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)
	ctx := context.Background()

	const workers, each = 32, 8
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				id := clientIDFor(w, i)
				if err := s.Save(ctx, st(id, ep(1, uint32(i+1)))); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发落盘失败: %v", err)
	}

	for w := 0; w < workers; w++ {
		for i := 0; i < each; i++ {
			id := clientIDFor(w, i)
			got, ok, err := s.Load(ctx, id)
			if err != nil || !ok {
				t.Fatalf("%s 没落下来: %v, %v", id, ok, err)
			}
			if len(got.Subscriptions) != 1 || got.Subscriptions[0].ID != 7 {
				t.Fatalf("%s 的订阅是 %+v", id, got.Subscriptions)
			}
		}
	}
}

func clientIDFor(w, i int) string {
	return "c-" + string(rune('a'+w%26)) + string(rune('0'+i%10)) +
		string(rune('A'+w/26))
}

// TestQueuedIsStoredSeparately 钉住离线队列与会话状态分开存（分区组的前置）。
//
// 三条判据：
//
//   - 追加一条不重写整份会话：入队之后状态记录本身没变（否则 N 条消息
//     累计写入是 O(N²)，而这发生在发布路径上；跨节点复制时还要把上百 KB
//     的会话提进日志）；
//   - 顺序按序号而不是字典序：十进制字符串会让第 10 条排在第 9 条前面，
//     而 QoS 1 的使用者通常按顺序处理；
//   - `Save` 的 `Queued == nil` 表示"这次不碰队列"。少了这个区别，任何
//     一个只想改订阅的 Save 都会把离线队列静默清掉——而"Load 改字段再
//     Save"这条路径在 broker 里到处都是。
func TestQueuedIsStoredSeparately(t *testing.T) {
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	ps := NewSessionStore(kv)
	ctx := context.Background()

	st := &session.State{
		ClientID: "dev-1", Epoch: 1, ExpiryInterval: 3600,
		Subscriptions: []session.Subscription{{Filter: "a/#", QoS: 1}},
	}
	if err := ps.Save(ctx, st); err != nil {
		t.Fatal(err)
	}
	stateRaw, err := kv.Get(Key(PrefixSession, "dev-1"))
	if err != nil {
		t.Fatal(err)
	}

	// 追加 12 条：够跨过 9 到 10 这个字典序会出错的边界。
	for i := 0; i < 12; i++ {
		if err := ps.AppendQueued("dev-1", session.QueuedMsg{
			QoS: 1, Topic: "a/b", Payload: []byte(itoa(i)),
		}); err != nil {
			t.Fatalf("第 %d 条入队失败: %v", i, err)
		}
	}
	after, err := kv.Get(Key(PrefixSession, "dev-1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(stateRaw) {
		t.Error("入队改动了会话状态记录：队列没有真的分开存")
	}

	got, ok, err := ps.Load(ctx, "dev-1")
	if err != nil || !ok {
		t.Fatalf("读回失败: %v / %v", err, ok)
	}
	if len(got.Queued) != 12 {
		t.Fatalf("读回 %d 条队列，期望 12", len(got.Queued))
	}
	for i := range got.Queued {
		if string(got.Queued[i].Payload) != itoa(i) {
			t.Fatalf("第 %d 条是 %q，期望 %q；顺序按序号而不是字典序",
				i, got.Queued[i].Payload, itoa(i))
		}
	}

	// Queued 为 nil 的 Save 不得碰队列。
	got.Queued = nil
	got.Subscriptions = append(got.Subscriptions, session.Subscription{Filter: "c/#", QoS: 1})
	if err := ps.Save(ctx, got); err != nil {
		t.Fatal(err)
	}
	again, _, err := ps.Load(ctx, "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Queued) != 12 {
		t.Fatalf("Queued 为 nil 的 Save 之后队列剩 %d 条，期望 12 条不变", len(again.Queued))
	}

	// 取走之后队列清空，记录也不该留着。
	taken, err := ps.TakeQueued("dev-1")
	if err != nil || len(taken) != 12 {
		t.Fatalf("取走 %d 条（err=%v），期望 12", len(taken), err)
	}
	n := 0
	_ = kv.Scan([]byte{byte(PrefixQueued)}, func([]byte, []byte) bool { n++; return true })
	if n != 0 {
		t.Errorf("取走之后还剩 %d 条队列记录", n)
	}

	// 删会话要把队列一起删，否则同一个 clientId 的下一个会话会把它
	// 当成自己的离线消息补发——那是跨会话串消息。
	if err := ps.AppendQueued("dev-1", session.QueuedMsg{QoS: 1, Topic: "a/b"}); err != nil {
		t.Fatal(err)
	}
	if err := ps.Delete(ctx, "dev-1", 1); err != nil {
		t.Fatal(err)
	}
	n = 0
	_ = kv.Scan([]byte{byte(PrefixQueued)}, func([]byte, []byte) bool { n++; return true })
	if n != 0 {
		t.Errorf("删掉会话后还剩 %d 条队列记录（孤儿）", n)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
