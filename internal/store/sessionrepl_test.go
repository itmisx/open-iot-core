package store

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

// loopProposer 是一条"立刻应用"的复制日志：提案即应用。
//
// 用它而不是起真的 dragonboat 分片，是为了把**命令语义**单独验证——
// 序号怎么分配、围栏在哪判、快照含什么，这些与共识层无关。共识层那半
// 在 internal/cluster/shards_test.go 与 tests/cluster/run.sh。
//
// 可以挂多个 SessionStore：同一串命令喂给两个独立的库，用来验证
// "各节点算出同一个结果"这件事（那是复制日志的硬要求）。
type loopProposer struct {
	stores []*SessionStore
	count  uint64
	index  map[uint64]uint64
}

func newLoopProposer(count uint64, stores ...*SessionStore) *loopProposer {
	return &loopProposer{stores: stores, count: count, index: map[uint64]uint64{}}
}

func (p *loopProposer) ShardOf(clientID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))
	return 16 + h.Sum64()%p.count
}

func (p *loopProposer) ShardIDs() []uint64 {
	out := make([]uint64, 0, p.count)
	for i := uint64(0); i < p.count; i++ {
		out = append(out, 16+i)
	}
	return out
}

func (p *loopProposer) Propose(clientID string, data []byte) (uint64, error) {
	return p.ProposeShard(p.ShardOf(clientID), data)
}

func (p *loopProposer) ProposeShard(shard uint64, data []byte) (uint64, error) {
	p.index[shard]++
	idx := p.index[shard]
	owns := func(id string) bool { return p.ShardOf(id) == shard }
	var res uint64
	for _, st := range p.stores {
		v, err := st.ApplySessionCmd(shard, idx, data, owns)
		if err != nil {
			return 0, err
		}
		res = v
	}
	return res, nil
}

func newReplSession(t *testing.T, extra int) (*ReplicatedSessionStore, []*SessionStore) {
	t.Helper()
	stores := make([]*SessionStore, 0, extra+1)
	for i := 0; i <= extra; i++ {
		stores = append(stores, NewSessionStore(NewMemKV()))
	}
	prop := newLoopProposer(4, stores...)
	return NewReplicatedSessionStore(stores[0], prop), stores
}

func replState(clientID string, epoch cluster.Epoch) *session.State {
	return &session.State{
		ClientID: clientID, Epoch: epoch, ExpiryInterval: 3600,
	}
}

func TestReplicatedSessionRoundTrip(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-1", 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	for i := 0; i < 3; i++ {
		m := session.QueuedMsg{Topic: "t", Payload: []byte{byte('a' + i)}, QoS: 1}
		if err := r.AppendQueued("dev-1", m); err != nil {
			t.Fatalf("AppendQueued: %v", err)
		}
	}
	st, ok, err := r.Load(ctx, "dev-1")
	if err != nil || !ok {
		t.Fatalf("Load: %v ok=%v", err, ok)
	}
	if len(st.Queued) != 3 {
		t.Fatalf("队列应有 3 条，实为 %d", len(st.Queued))
	}
	// 顺序必须是入队顺序：序号按大端字节序排，而 QoS 1 的使用者按序处理。
	for i := range st.Queued {
		if got := st.Queued[i].Payload[0]; got != byte('a'+i) {
			t.Fatalf("第 %d 条是 %q，顺序错了", i, got)
		}
	}
	msgs, err := r.TakeQueued("dev-1")
	if err != nil || len(msgs) != 3 {
		t.Fatalf("TakeQueued: %v 条数 %d", err, len(msgs))
	}
	st, _, _ = r.Load(ctx, "dev-1")
	if len(st.Queued) != 0 {
		t.Fatalf("取走之后队列应为空，实为 %d", len(st.Queued))
	}
}

// TestReplicatedSaveFencesStaleEpoch 钉住契约 01：围栏在 apply 时判。
func TestReplicatedSaveFencesStaleEpoch(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-1", 5)); err != nil {
		t.Fatalf("Save epoch 5: %v", err)
	}
	err := r.Save(ctx, replState("dev-1", 4))
	if !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("旧 epoch 的写入应被拒，得到 %v", err)
	}
	// 相等的 epoch 要放行：同一个连接会多次 Save（订阅变化、属性变化）。
	if err := r.Save(ctx, replState("dev-1", 5)); err != nil {
		t.Fatalf("同 epoch 的写入应放行，得到 %v", err)
	}
}

func TestReplicatedDeleteFencesOnExactEpoch(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-1", 7)); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, "dev-1", 6); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("非当前 epoch 的删除应被拒，得到 %v", err)
	}
	if err := r.Delete(ctx, "dev-1", 7); err != nil {
		t.Fatalf("当前 epoch 的删除应放行，得到 %v", err)
	}
	if _, ok, _ := r.Load(ctx, "dev-1"); ok {
		t.Fatal("删除之后仍读到会话")
	}
}

// TestEveryReplicaComputesTheSameQueue 是复制日志的硬要求：同一串命令在
// 不同节点上必须算出同一个结果。
//
// 入队序号正是最容易在这里出错的地方——由提案方分配的话，两个节点并发
// 入队就会撞号，而撞号的表现是一条消息静默覆盖另一条（键相同），没有
// 任何位置会报错。序号在 apply 时由当前状态推出，才有这条性质。
func TestEveryReplicaComputesTheSameQueue(t *testing.T) {
	r, stores := newReplSession(t, 2) // 本地 + 两个"跟随者"
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		m := session.QueuedMsg{Topic: "t", Payload: []byte{byte(i)}, QoS: 1}
		if err := r.AppendQueued("dev-1", m); err != nil {
			t.Fatal(err)
		}
	}
	var want []byte
	for i, st := range stores {
		got := dumpQueue(t, st, "dev-1")
		if i == 0 {
			want = got
			if len(want) != 5 {
				t.Fatalf("第一个副本只有 %d 条", len(want))
			}
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("副本 %d 的队列是 %v，与第一个的 %v 不同：状态机分叉",
				i, got, want)
		}
	}
	// applied index 也要跟着数据落盘（CLU-109）。
	shard := newLoopProposer(4).ShardOf("dev-1")
	for i, st := range stores {
		if st.SessionAppliedIndex(shard) == 0 {
			t.Fatalf("副本 %d 的分片 %d 没有记下 applied index", i, shard)
		}
	}
}

func dumpQueue(t *testing.T, s *SessionStore, clientID string) []byte {
	t.Helper()
	msgs, err := s.loadQueued(clientID)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Payload...)
	}
	return out
}

// TestTakeOnlyDeletesWhatItRead 钉住 take 带 UpTo 而不是"清空"。
//
// 命令在途期间还会有新消息入队。写成清空会把它们一起删掉——设备收不到，
// 而且没有任何位置会报错。
func TestTakeOnlyDeletesWhatItRead(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := r.AppendQueued("dev-1", session.QueuedMsg{
			Topic: "t", Payload: []byte{byte('a' + i)}, QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 读出当前两条与它们的最大序号，然后在提案之前再入一条：
	// 这正是真实时序里 take 在途时发生的事。
	msgs, upTo, err := r.local.PeekQueued("dev-1")
	if err != nil || len(msgs) != 2 {
		t.Fatalf("PeekQueued: %v 条数 %d", err, len(msgs))
	}
	if err := r.AppendQueued("dev-1", session.QueuedMsg{
		Topic: "t", Payload: []byte("z"), QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	data, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpTake, ClientID: "dev-1", UpTo: upTo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.prop.Propose("dev-1", data); err != nil {
		t.Fatal(err)
	}
	left, _, err := r.local.PeekQueued("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || string(left[0].Payload) != "z" {
		t.Fatalf("在途期间入队的那条应当留下，剩余 %d 条 %v", len(left), left)
	}
	_ = ctx
}

// TestExpireOnlyTouchesItsOwnShard 钉住 CLU-114。
//
// 过期清理删出来的键必须落在本分片的日志里：另一个分片的快照含着别的
// 会话，而它那条日志从没删过它们——跨分片删就会在快照追平之后复活。
func TestExpireOnlyTouchesItsOwnShard(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	prop := r.prop.(*loopProposer)

	// 找两个落在不同分片上的名字。
	var a, b string
	for i := 0; i < 200 && (a == "" || b == ""); i++ {
		id := string(rune('A'+i%26)) + string(rune('a'+i/26))
		if prop.ShardOf(id) == 16 && a == "" {
			a = id
		}
		if prop.ShardOf(id) == 17 && b == "" {
			b = id
		}
	}
	if a == "" || b == "" {
		t.Skip("没找到落在两个不同分片上的名字")
	}
	for _, id := range []string{a, b} {
		st := replState(id, 1)
		st.ExpiryInterval = 0
		st.DisconnectUnixMs = 1
		if err := r.Save(ctx, st); err != nil {
			t.Fatal(err)
		}
	}
	// 只在 a 所属的那个分片上清理。
	data, err := encodeSessionCmd(&sessionCmd{Op: sessionOpExpire, NowMs: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	n, err := prop.ProposeShard(16, data)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("分片 16 应只清掉 1 条，报 %d", n)
	}
	if _, ok, _ := r.Load(ctx, a); ok {
		t.Fatalf("%s 属于分片 16，应被清掉", a)
	}
	if _, ok, _ := r.Load(ctx, b); !ok {
		t.Fatalf("%s 属于分片 17，不该被分片 16 的清理删掉", b)
	}
}

// TestSessionShardSnapshotCarriesOnlyItsOwnShard 钉住快照按分片切。
func TestSessionShardSnapshotCarriesOnlyItsOwnShard(t *testing.T) {
	src := NewSessionStore(NewMemKV())
	dst := NewSessionStore(NewMemKV())
	prop := newLoopProposer(4, src)
	r := NewReplicatedSessionStore(src, prop)
	ctx := context.Background()

	ids := []string{"dev-1", "dev-2", "dev-3", "dev-4", "dev-5", "dev-6"}
	for _, id := range ids {
		if err := r.Save(ctx, replState(id, 1)); err != nil {
			t.Fatal(err)
		}
		if err := r.AppendQueued(id, session.QueuedMsg{
			Topic: "t", Payload: []byte(id), QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	shard := prop.ShardOf("dev-1")
	owns := func(id string) bool { return prop.ShardOf(id) == shard }

	var buf bytes.Buffer
	if err := src.SnapshotSessionShard(shard, owns, &buf); err != nil {
		t.Fatalf("SnapshotSessionShard: %v", err)
	}
	// 目标库先放一条属于本分片但快照里没有的会话：恢复必须把它删掉，
	// 否则"已经删掉的会话在落后过的节点上又活了"。
	ghost := ""
	for i := 0; i < 500; i++ {
		id := "ghost-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if owns(id) {
			ghost = id
			break
		}
	}
	if ghost == "" {
		t.Skip("没找到落在该分片上的幽灵名字")
	}
	if err := dst.Save(ctx, replState(ghost, 1)); err != nil {
		t.Fatal(err)
	}
	if err := dst.RestoreSessionShard(shard, owns, &buf); err != nil {
		t.Fatalf("RestoreSessionShard: %v", err)
	}
	if _, ok, _ := dst.Load(ctx, ghost); ok {
		t.Fatalf("%s 不在快照里，恢复后不该还在", ghost)
	}
	for _, id := range ids {
		_, ok, err := dst.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if owns(id) != ok {
			t.Fatalf("%s 归属=%v 而恢复后存在=%v：快照没有按分片切",
				id, owns(id), ok)
		}
		if !ok {
			continue
		}
		q, _, qerr := dst.PeekQueued(id)
		if qerr != nil {
			t.Fatal(qerr)
		}
		if len(q) != 1 || string(q[0].Payload) != id {
			t.Fatalf("%s 的队列没跟着快照过来: %v", id, q)
		}
	}
	// applied index 也在快照里：恢复之后状态机报的位置必须是快照那一刻的，
	// 报 0 会让 dragonboat 从头重放——而那些日志已被压掉。
	if dst.SessionAppliedIndex(shard) != src.SessionAppliedIndex(shard) {
		t.Fatalf("恢复后 applied index 是 %d，源是 %d",
			dst.SessionAppliedIndex(shard), src.SessionAppliedIndex(shard))
	}
}

func TestUnknownSessionOpIsFatal(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	data, err := encodeSessionCmd(&sessionCmd{Op: 99, ClientID: "dev-1"})
	if err != nil {
		t.Fatal(err)
	}
	owns := func(string) bool { return true }
	if _, aerr := s.ApplySessionCmd(16, 1, data, owns); aerr == nil {
		// 不认识的操作码必须报错，不能静默跳过：它已被多数派提交，
		// 别的节点（新版本）会应用它，跳过就是状态机分叉。
		t.Fatal("未知操作码应当报错")
	}
}

// TestReplicatedExpireSweepsEveryShard 钉住过期清理每个分片各提一条。
//
// 少提一条的后果不是报错而是"某些 clientId 的过期会话永远不被清理"
// ——它们按哈希落在没被扫到的那个分片上，而哪些设备受影响取决于名字。
func TestReplicatedExpireSweepsEveryShard(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	const n = 24
	for i := 0; i < n; i++ {
		st := replState("exp-"+string(rune('a'+i)), 1)
		st.ExpiryInterval = 0
		st.DisconnectUnixMs = 1
		if err := r.Save(ctx, st); err != nil {
			t.Fatal(err)
		}
		if err := r.AppendQueued(st.ClientID, session.QueuedMsg{
			Topic: "t", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.Count(); got != n {
		t.Fatalf("清理前有 %d 个会话，期望 %d", got, n)
	}
	cleaned, err := r.Expire(ctx, 1<<40)
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if cleaned != n {
		t.Fatalf("只清掉 %d 个，期望 %d：有分片没被扫到", cleaned, n)
	}
	if got := r.Count(); got != 0 {
		t.Fatalf("清理后仍有 %d 个会话", got)
	}
	// 队列记录也要一起走，不然是孤儿——同一个 clientId 下次建新会话时
	// 会把它们当成自己的离线消息补发出去（跨会话串消息）。
	left := 0
	_ = r.Local().kv.Scan([]byte{byte(PrefixQueued)}, func([]byte, []byte) bool {
		left++
		return true
	})
	if left != 0 {
		t.Fatalf("清理后还剩 %d 条队列记录（孤儿）", left)
	}
}

// TestSessionSnapshotRejectsTruncatedStream 钉住截断的快照必须报错。
//
// 快照来自对端。悄悄按已读到的部分恢复，会让本分片少掉一批会话，而
// dragonboat 认为恢复成功——于是状态机分叉，且没有任何位置报错。
func TestSessionSnapshotRejectsTruncatedStream(t *testing.T) {
	src := NewSessionStore(NewMemKV())
	prop := newLoopProposer(4, src)
	r := NewReplicatedSessionStore(src, prop)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if err := r.Save(ctx, replState("dev-"+string(rune('a'+i)), 1)); err != nil {
			t.Fatal(err)
		}
	}
	shard := prop.ShardOf("dev-a")
	owns := func(id string) bool { return prop.ShardOf(id) == shard }
	var buf bytes.Buffer
	if err := src.SnapshotSessionShard(shard, owns, &buf); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	if len(full) < 20 {
		t.Fatalf("快照只有 %d 字节，截不出有意义的残缺流", len(full))
	}
	dst := NewSessionStore(NewMemKV())
	for _, cut := range []int{4, len(full) - 3} {
		err := dst.RestoreSessionShard(shard, owns, bytes.NewReader(full[:cut]))
		if err == nil {
			t.Fatalf("截到 %d 字节的快照应当报错", cut)
		}
	}
	// 完整的那份要能恢复，否则上面两条只是"全都报错"。
	if err := dst.RestoreSessionShard(shard, owns, bytes.NewReader(full)); err != nil {
		t.Fatalf("完整快照恢复失败: %v", err)
	}
}

func TestCorruptSessionCmdIsFatal(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	owns := func(string) bool { return true }
	for _, raw := range [][]byte{
		[]byte("not json"),
		[]byte(`{"v":99,"op":1,"c":"dev-1"}`),
	} {
		if _, err := s.ApplySessionCmd(16, 1, raw, owns); err == nil {
			t.Fatalf("损坏的命令 %q 应当报错", raw)
		}
	}
}
