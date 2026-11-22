package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/session"
)

// TestPruneMarkedIsIdempotentAndAtomic 钉住「一次性裁剪」的三条语义。
//
// 它用在 RF=3 首次启用时裁掉本节点不该再持有的影子（CLU-162 那一类）：
//
//   - 标记已存在就什么都不做并报告 already。少了这一条，每次重启都会重扫
//     一遍全库，而那是 20 万键的 cursor 遍历。
//   - 回调报错要整事务回滚，连标记一起不写。写了标记而删到一半的后果是
//     下次启动跳过裁剪，于是那些键永久留着——而它们属于别的副本。
//   - 删除与写标记在同一个事务里。分成两步的话，两步之间崩溃等于前一条。
func TestPruneMarkedIsIdempotentAndAtomic(t *testing.T) {
	// NewMemKV 返回接口，而 PruneMarked 是 *MemKV 的方法：这一组语义只有
	// 实现自己能提供（持久实现有同名方法），所以这里取具体类型。
	kv := NewMemKV().(*MemKV)
	marker := []byte("marker/pruned")
	for _, k := range []string{"s/keep-1", "s/drop-1", "s/keep-2", "s/drop-2"} {
		if err := kv.Set([]byte(k), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	dropHalf := func(key []byte) (bool, error) {
		return bytes.Contains(key, []byte("drop")), nil
	}

	n, already, err := kv.PruneMarked([]byte("s/"), marker, dropHalf)
	if err != nil || already || n != 2 {
		t.Fatalf("首次裁剪 = %d %v %v，期望 2 false nil", n, already, err)
	}
	for _, k := range []string{"s/drop-1", "s/drop-2"} {
		if _, err := kv.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s 没被删掉", k)
		}
	}
	for _, k := range []string{"s/keep-1", "s/keep-2"} {
		if _, err := kv.Get([]byte(k)); err != nil {
			t.Fatalf("%s 被误删: %v", k, err)
		}
	}

	// 第二次必须直接认标记：不再扫描，也不再删。
	n, already, err = kv.PruneMarked([]byte("s/"), marker, func([]byte) (bool, error) {
		t.Fatal("标记已存在时不该再调回调")
		return false, nil
	})
	if err != nil || !already || n != 0 {
		t.Fatalf("二次裁剪 = %d %v %v，期望 0 true nil", n, already, err)
	}
}

// TestPruneMarkedRollsBackOnCallbackError 钉住回调报错时的整事务回滚。
func TestPruneMarkedRollsBackOnCallbackError(t *testing.T) {
	kv := NewMemKV().(*MemKV)
	marker := []byte("marker/pruned")
	for _, k := range []string{"s/a", "s/b", "s/c"} {
		if err := kv.Set([]byte(k), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("放置表还没加载好")
	seen := 0
	n, already, err := kv.PruneMarked([]byte("s/"), marker, func([]byte) (bool, error) {
		seen++
		if seen == 2 {
			return false, boom
		}
		return true, nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("回调的错误必须原样返回，得到 %v", err)
	}
	if n != 0 || already {
		t.Fatalf("失败时 = %d %v，期望 0 false（不得报告删了几条）", n, already)
	}
	// 回滚之后三条都要还在，标记也不能写下去——否则下次启动会跳过裁剪。
	for _, k := range []string{"s/a", "s/b", "s/c"} {
		if _, err := kv.Get([]byte(k)); err != nil {
			t.Fatalf("回滚后 %s 丢了: %v", k, err)
		}
	}
	if _, err := kv.Get(marker); !errors.Is(err, ErrNotFound) {
		t.Fatal("失败的那一轮不得留下标记")
	}
}

// TestNextCounterIsMonotonicAndRejectsCorruption 钉住计数器的两条语义。
//
// 计数器用在策略版本号一类「不得重用」的地方，所以它必须严格递增，而且
// 记录损坏时要报错而不是从 0 重新开始——后者会把已经发出去的号再发一遍。
func TestNextCounterIsMonotonicAndRejectsCorruption(t *testing.T) {
	kv := NewMemKV()
	for want := uint64(1); want <= 3; want++ {
		got, err := NextCounter(kv, "policy-version")
		if err != nil || got != want {
			t.Fatalf("第 %d 次 = %d %v", want, got, err)
		}
	}
	// 另一个名字是独立的序列。
	if got, err := NextCounter(kv, "other"); err != nil || got != 1 {
		t.Fatalf("另一个计数器 = %d %v，期望 1", got, err)
	}

	if err := kv.Set(Key(PrefixCounter, "broken"), []byte("xx"), false); err != nil {
		t.Fatal(err)
	}
	if _, err := NextCounter(kv, "broken"); err == nil ||
		!strings.Contains(err.Error(), "损坏") {
		t.Fatalf("长度不对的记录必须报错，得到 %v", err)
	}
}

// TestReadReplicaSnapshotNeedsStateOrQueue 钉住远程会话快照的「有没有」判定。
//
// 它是非副本节点读会话的唯一入口（CLU-147 的批量读最终落在它上面）。
// 三种情形都要分清：什么都没有要回 false（对端据此判定「这个设备没会话」）；
// 只有离线队列也要回 true，否则那几条消息会被当成不存在而丢掉。
func TestReadReplicaSnapshotNeedsStateOrQueue(t *testing.T) {
	kv := NewMemKV()
	t.Cleanup(func() { _ = kv.Close() })
	s := NewSessionStore(kv)
	ctx := context.Background()

	raw, ok, err := s.ReadReplicaSnapshot("ghost")
	if err != nil || ok || raw != nil {
		t.Fatalf("无会话无队列 = %v %v %v，期望 nil false nil", raw, ok, err)
	}

	// 有会话记录：快照要带上状态，且订阅要原样过来——对端拿它重建路由表。
	in := st("c1", ep(1, 1))
	in.Subscriptions = []session.Subscription{{Filter: "$aws/things/c1/x", QoS: 1}}
	if err := s.Save(ctx, in); err != nil {
		t.Fatal(err)
	}
	raw, ok, err = s.ReadReplicaSnapshot("c1")
	if err != nil || !ok {
		t.Fatalf("有会话 = %v %v", ok, err)
	}
	var snap sessionReplicaSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatalf("快照不是合法 JSON: %v", err)
	}
	if snap.State == nil || len(snap.State.Subscriptions) != 1 ||
		snap.State.Subscriptions[0].Filter != "$aws/things/c1/x" {
		t.Fatalf("快照里的订阅 = %+v", snap.State)
	}
	if snap.QueueUpTo != 0 || len(snap.Queue) != 0 {
		t.Fatalf("没有离线消息时队列水位应为 0，得到 %d/%d",
			snap.QueueUpTo, len(snap.Queue))
	}

	// 只有离线队列、没有会话记录：也必须回 true。
	//
	// 回 false 的后果是对端把它当成「这个设备没有会话」，于是那几条已经
	// 入队的消息被当成不存在而丢掉——而入队本身是成功的，不会有任何错误。
	if err := s.AppendQueued("c2", session.QueuedMsg{
		QoS: 1, Topic: "t", Payload: []byte("p"),
	}); err != nil {
		t.Fatal(err)
	}
	raw, ok, err = s.ReadReplicaSnapshot("c2")
	if err != nil || !ok {
		t.Fatalf("只有队列 = %v %v", ok, err)
	}
	snap = sessionReplicaSnapshot{}
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Queue) != 1 || snap.QueueUpTo == 0 {
		t.Fatalf("队列快照 = %d 条，水位 %d（水位为 0 时补发命令会删空整个队列）",
			len(snap.Queue), snap.QueueUpTo)
	}
}
