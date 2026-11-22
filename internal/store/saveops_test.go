package store

import (
	"encoding/json"
	"testing"

	"github.com/itmisx/open-iot-core/internal/session"
)

// applyOne 把一条会话命令按 apply 路径走一遍，返回状态机的返回值。
//
// 走 ApplySessionCmd 而不是直接调 saveOps：围栏判定与队列对账都发生在
// apply 里（CLU-116），而「在提案之前判」正是这条路径反复修过的缺陷。
func applyOne(t *testing.T, s *SessionStore, index uint64, cmd *sessionCmd) uint64 {
	t.Helper()
	raw, err := encodeSessionCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.ApplySessionCmd(16, index, raw, func(string) bool { return true })
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return res
}

// TestSaveOpsQueueReconciliation 钉住 save 命令对离线队列的三种处理。
//
// `Queued` 的 nil / 空切片 / 非空三态是这条路径最容易写错的地方（见
// SessionStore.Save 的约定）：
//
//   - nil 表示「这次不碰队列」。少了这个区别，任何一个只想改订阅的 save
//     都会把离线队列静默清掉——而那条路径（读出来改字段再写回）在 broker
//     里到处都是。
//   - 空切片表示「队列应当是空的」，要真的删干净。
//   - 非空表示「队列就是这一份」，序号从 1 起重排。
func TestSaveOpsQueueReconciliation(t *testing.T) {
	kv := NewMemKV()
	t.Cleanup(func() { _ = kv.Close() })
	s := NewSessionStore(kv)

	// 先写两条离线消息。
	for i := 0; i < 2; i++ {
		if res := applyOne(t, s, uint64(i+1), &sessionCmd{
			Op: sessionOpEnqueue, ClientID: "c1",
			Msg: &session.QueuedMsg{QoS: 1, Topic: "t", Payload: []byte{byte(i)}},
		}); res != sessionResOK {
			t.Fatalf("入队第 %d 条返回 %d", i, res)
		}
	}
	msgs, upTo, err := s.PeekQueued("c1")
	if err != nil || len(msgs) != 2 || upTo != 2 {
		t.Fatalf("入队后 = %d 条，水位 %d，%v", len(msgs), upTo, err)
	}

	// Queued 为 nil：队列必须原样留着。
	base := st("c1", ep(1, 1))
	base.Queued = nil
	base.Subscriptions = []session.Subscription{{Filter: "$aws/things/c1/a", QoS: 1}}
	if res := applyOne(t, s, 3, &sessionCmd{
		Op: sessionOpSave, ClientID: "c1", State: base,
	}); res != sessionResOK {
		t.Fatalf("nil 队列的 save 返回 %d", res)
	}
	if msgs, _, err = s.PeekQueued("c1"); err != nil || len(msgs) != 2 {
		t.Fatalf("nil 队列的 save 把队列改成了 %d 条（%v）", len(msgs), err)
	}

	// Queued 非空：队列替换成这一份，序号从 1 起重排。
	withQueue := st("c1", ep(1, 1))
	withQueue.Queued = []session.QueuedMsg{
		{QoS: 1, Topic: "t", Payload: []byte("only")},
	}
	if res := applyOne(t, s, 4, &sessionCmd{
		Op: sessionOpSave, ClientID: "c1", State: withQueue,
	}); res != sessionResOK {
		t.Fatalf("带队列的 save 返回 %d", res)
	}
	msgs, upTo, err = s.PeekQueued("c1")
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "only" {
		t.Fatalf("替换后 = %+v 水位 %d %v", msgs, upTo, err)
	}
	if upTo != 1 {
		t.Fatalf("替换后的水位 = %d，期望从 1 起重排", upTo)
	}

	// Queued 为空切片：队列必须被删干净。
	empty := st("c1", ep(1, 1))
	empty.Queued = []session.QueuedMsg{}
	if res := applyOne(t, s, 5, &sessionCmd{
		Op: sessionOpSave, ClientID: "c1", State: empty,
	}); res != sessionResOK {
		t.Fatalf("空切片的 save 返回 %d", res)
	}
	if msgs, _, err = s.PeekQueued("c1"); err != nil || len(msgs) != 0 {
		t.Fatalf("空切片的 save 之后仍有 %d 条（%v）", len(msgs), err)
	}
}

// TestSaveOpsRejectsStaleAndMalformed 钉住围栏与输入校验。
//
// 围栏判定必须发生在 apply 里：提交之前判等于在提案方那个节点上判，而它
// 看到的当前 epoch 可能比别的节点旧——两个节点并发接管同一个 clientId 时，
// 旧 epoch 的那条写入就可能后落地（CLU-116）。
func TestSaveOpsRejectsStaleAndMalformed(t *testing.T) {
	kv := NewMemKV()
	t.Cleanup(func() { _ = kv.Close() })
	s := NewSessionStore(kv)

	cur := st("c1", ep(1, 5))
	if res := applyOne(t, s, 1, &sessionCmd{
		Op: sessionOpSave, ClientID: "c1", State: cur,
	}); res != sessionResOK {
		t.Fatalf("首次 save 返回 %d", res)
	}

	// 更旧的 epoch 必须被判为陈旧，而且不得改动已有记录。
	old := st("c1", ep(1, 4))
	old.Subscriptions = []session.Subscription{{Filter: "should-not-land", QoS: 0}}
	if res := applyOne(t, s, 2, &sessionCmd{
		Op: sessionOpSave, ClientID: "c1", State: old,
	}); res != sessionResStale {
		t.Fatalf("旧 epoch 返回 %d，期望 stale", res)
	}
	got, ok, err := s.Load(t.Context(), "c1")
	if err != nil || !ok {
		t.Fatal(err)
	}
	for _, sub := range got.Subscriptions {
		if sub.Filter == "should-not-land" {
			t.Fatal("被判陈旧的那条写入落地了")
		}
	}

	// 缺状态或缺 clientId 是致命的编码错误，不是 stale：返回错误而不是
	// 悄悄跳过——跳过会让这条已被多数派提交的日志在本节点上没有效果，
	// 那就是状态机分叉。
	for _, c := range []*sessionCmd{
		{Op: sessionOpSave, ClientID: "c1"},
		{Op: sessionOpSave, ClientID: "c1", State: &session.State{}},
	} {
		raw, err := encodeSessionCmd(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplySessionCmd(16, 9, raw, func(string) bool { return true }); err == nil {
			t.Fatalf("缺状态的 save 必须报错：%s", raw)
		}
	}

	// 解不开的命令同样必须报错。
	if _, err := s.ApplySessionCmd(16, 10, []byte("{"), func(string) bool { return true }); err == nil {
		t.Fatal("解不开的命令必须报错，否则本节点跳过就是分叉")
	}
}

// TestExpireOnlyTouchesOwnedShard 钉住「一个分片的日志只许改它自己拥有的
// 键」（CLU-114）。
//
// 过期清理是**扫描式**的：它扫整个会话前缀，所以必须按归属过滤。跨分片
// 删的后果不是报错而是**复活**——另一个分片的快照含着那些键，而它那条
// 日志从没删过它们。
//
// 注意只有 expire 需要这道过滤：save / delete / enqueue 都是按 clientId
// 寻址的，提案本来就进了对的那条日志，所以它们不走 owns。
func TestExpireOnlyTouchesOwnedShard(t *testing.T) {
	kv := NewMemKV()
	t.Cleanup(func() { _ = kv.Close() })
	s := NewSessionStore(kv)

	// 两个会话都已过期：mine 属于本分片，yours 不属于。
	for _, id := range []string{"mine", "yours"} {
		expired := st(id, ep(1, 1))
		expired.ExpiryInterval = 1
		expired.DisconnectUnixMs = 1_000
		if res := applyOne(t, s, 1, &sessionCmd{
			Op: sessionOpSave, ClientID: id, State: expired,
		}); res != sessionResOK {
			t.Fatalf("写入 %s 返回 %d", id, res)
		}
	}

	raw, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpExpire, NowMs: 9_999_999,
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.ApplySessionCmd(16, 5, raw, func(id string) bool { return id == "mine" })
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("清理了 %d 条，期望只清本分片那一条", n)
	}
	if _, ok, err := s.Load(t.Context(), "mine"); err != nil || ok {
		t.Fatalf("本分片的过期会话没被清掉（ok=%v err=%v）", ok, err)
	}
	if _, ok, err := s.Load(t.Context(), "yours"); err != nil || !ok {
		t.Fatalf("别的分片的会话被清掉了（ok=%v err=%v）——那会在对端复活", ok, err)
	}

	// 缺归属判定是编码错误：宁可报错也不能当成「全都属于我」。
	if _, err := s.ApplySessionCmd(16, 6, raw, nil); err == nil {
		t.Fatal("expire 缺 owns 必须报错")
	}
}

// TestQueuedMsgEncodingRoundTrips 钉住离线消息的编码往返。
//
// 它进复制日志，所以字段丢失是跨节点的数据丢失而不是本地 bug：订阅标识
// 丢了之后，补发时客户端收不到它赖以分流的 subscription identifier。
func TestQueuedMsgEncodingRoundTrips(t *testing.T) {
	in := session.QueuedMsg{
		QoS: 1, Topic: "$aws/things/c1/x", Payload: []byte("p"),
		SubIDs: []uint32{7, 9}, Retain: true,
	}
	raw, err := json.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	var out session.QueuedMsg
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Topic != in.Topic || out.QoS != in.QoS || !out.Retain ||
		len(out.SubIDs) != 2 || out.SubIDs[1] != 9 || string(out.Payload) != "p" {
		t.Fatalf("往返丢字段：%+v", out)
	}
}
