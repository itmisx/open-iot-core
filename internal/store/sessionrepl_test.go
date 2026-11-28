package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

type gatedSessionProposer struct {
	entered chan string
	release chan struct{}
}

func (*gatedSessionProposer) ShardOf(string) uint64 { return 16 }
func (*gatedSessionProposer) ShardIDs() []uint64    { return []uint64{16} }
func (p *gatedSessionProposer) Propose(_ string, data []byte) (uint64, error) {
	return p.ProposeShard(16, data)
}
func (p *gatedSessionProposer) ProposeShard(_ uint64, data []byte) (uint64, error) {
	p.entered <- string(data)
	<-p.release
	return 0, nil
}

// loopProposer 是一条"立刻应用"的复制日志：提案即应用。
//
// 用它而不是起真的 dragonboat 分片，是为了把**命令语义**单独验证——
// 序号怎么分配、围栏在哪判、快照含什么，这些与共识层无关。共识层那半
// 在 internal/cluster/shards_test.go 与 tests/cluster/run.sh。
//
// 可以挂多个 SessionStore：同一串命令喂给两个独立的库，用来验证
// "各节点算出同一个结果"这件事（那是复制日志的硬要求）。
type loopProposer struct {
	mu     sync.Mutex
	stores []*SessionStore
	count  uint64
	index  map[uint64]uint64
}

type remoteReadProposer struct {
	*loopProposer
	local        bool
	raw          []byte
	found        bool
	err          error
	reads        int
	claimedRaw   []byte
	claimedFound bool
	claimedOK    bool
	claimedEpoch cluster.Epoch
}

func (p *remoteReadProposer) HasLocalReplica(string) bool { return p.local }
func (p *remoteReadProposer) ReadSession(string) ([]byte, bool, error) {
	p.reads++
	return append([]byte(nil), p.raw...), p.found, p.err
}
func (p *remoteReadProposer) TakeClaimedSession(_ string, epoch cluster.Epoch) ([]byte, bool, bool, error) {
	if !p.claimedOK || epoch != p.claimedEpoch {
		return nil, false, false, nil
	}
	p.claimedOK = false
	return append([]byte(nil), p.claimedRaw...), p.claimedFound, true, nil
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
	// Dragonboat 允许并发提交，但同一 shard 的状态机 apply 仍然串行。
	// 这个内存替身也保持该语义，避免测试把 map 并发写误当成复制层行为。
	p.mu.Lock()
	defer p.mu.Unlock()
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

func TestReplicatedSessionLoadUsesRemoteOnlyWithoutLocalReplica(t *testing.T) {
	local := NewSessionStore(NewMemKV())
	want := replState("remote-device", 7)
	want.Queued = []session.QueuedMsg{{Topic: "cmd", Payload: []byte("on"), QoS: 1}}
	raw, err := json.Marshal(sessionReplicaSnapshot{State: want, Queue: want.Queued, QueueUpTo: 1})
	if err != nil {
		t.Fatal(err)
	}
	prop := &remoteReadProposer{
		loopProposer: newLoopProposer(1, local), raw: raw, found: true,
	}
	r := NewReplicatedSessionStore(local, prop)
	got, found, err := r.Load(context.Background(), want.ClientID)
	if err != nil || !found {
		t.Fatalf("远程 Load = found %v err %v", found, err)
	}
	if got.ClientID != want.ClientID || got.Epoch != want.Epoch || len(got.Queued) != 1 ||
		string(got.Queued[0].Payload) != "on" {
		t.Fatalf("远程会话 = %+v", got)
	}
	if prop.reads != 1 {
		t.Fatalf("远程读取次数 = %d，期望 1", prop.reads)
	}

	prop.local = true
	if got, found, err := r.Load(context.Background(), "missing-local"); err != nil || found || got != nil {
		t.Fatalf("本地缺失 Load = %+v, %v, %v", got, found, err)
	}
	if prop.reads != 1 {
		t.Fatal("有本地副本时仍发出了远程读取")
	}
}

func TestReplicatedSessionLoadClaimedConsumesPiggybackWithoutReadRPC(t *testing.T) {
	local := NewSessionStore(NewMemKV())
	want := replState("remote-device", 7)
	raw, err := json.Marshal(sessionReplicaSnapshot{State: want})
	if err != nil {
		t.Fatal(err)
	}
	prop := &remoteReadProposer{
		loopProposer: newLoopProposer(1, local),
		claimedRaw:   raw, claimedFound: true, claimedOK: true, claimedEpoch: 99,
	}
	r := NewReplicatedSessionStore(local, prop)
	got, found, err := r.LoadClaimed(context.Background(), want.ClientID, 99)
	if err != nil || !found || got.ClientID != want.ClientID {
		t.Fatalf("LoadClaimed = %+v found=%v err=%v", got, found, err)
	}
	if prop.reads != 0 {
		t.Fatalf("命中 Claim 预读后仍发出 %d 次 SessionRead RPC", prop.reads)
	}
	// 一次性结果已经消费；下一次必须回到普通远程读取。
	prop.raw, prop.found = raw, true
	if _, found, err := r.LoadClaimed(context.Background(), want.ClientID, 99); err != nil || !found {
		t.Fatalf("预读消费后的回退读取 = found %v err %v", found, err)
	}
	if prop.reads != 1 {
		t.Fatalf("回退远程读取次数 = %d，期望 1", prop.reads)
	}
}

func TestReplicatedSessionTakeQueuedUsesRemoteWithoutLocalReplica(t *testing.T) {
	local := NewSessionStore(NewMemKV())
	queue := []session.QueuedMsg{
		{Topic: "cmd/1", Payload: []byte("one"), QoS: 1},
		{Topic: "cmd/2", Payload: []byte("two"), QoS: 1},
	}
	raw, err := json.Marshal(sessionReplicaSnapshot{Queue: queue, QueueUpTo: 42})
	if err != nil {
		t.Fatal(err)
	}
	prop := &remoteReadProposer{
		loopProposer: newLoopProposer(1, local), raw: raw, found: true,
	}
	r := NewReplicatedSessionStore(local, prop)
	got, err := r.TakeQueued("remote-device")
	if err != nil {
		t.Fatalf("远程 TakeQueued: %v", err)
	}
	if len(got) != 2 || string(got[0].Payload) != "one" || string(got[1].Payload) != "two" {
		t.Fatalf("远程队列 = %+v", got)
	}
	if prop.reads != 1 {
		t.Fatalf("远程读取次数 = %d，期望 1", prop.reads)
	}
}

func TestMergeSessionProposalsFlattensAccessBatches(t *testing.T) {
	makeSave := func(id string, epoch cluster.Epoch) []byte {
		raw, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: id, State: replState(id, epoch),
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a, b, c := makeSave("a", 1), makeSave("b", 2), makeSave("c", 3)
	left, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpBatch, Batch: []json.RawMessage{a, b},
	})
	if err != nil {
		t.Fatal(err)
	}
	merged, widths, consumed, err := MergeSessionProposals([][]byte{left, c})
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 2 || !slices.Equal(widths, []uint8{2, 1}) {
		t.Fatalf("consumed=%d widths=%v", consumed, widths)
	}
	cmd, err := decodeSessionCmd(merged)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Op != sessionOpBatch || len(cmd.Batch) != 3 {
		t.Fatalf("合并命令 op=%d 子命令=%d", cmd.Op, len(cmd.Batch))
	}
}

func TestMergeSessionProposalsKeepsSameClientInLogOrder(t *testing.T) {
	makeSave := func(epoch cluster.Epoch) []byte {
		raw, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: "same", State: replState("same", epoch),
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	old, fresh := makeSave(4), makeSave(5)
	merged, widths, consumed, err := MergeSessionProposals([][]byte{old, fresh})
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 2 || !slices.Equal(widths, []uint8{1, 1}) {
		t.Fatalf("same-client merge consumed=%d widths=%v", consumed, widths)
	}
	cmd, err := decodeSessionCmd(merged)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Op != sessionOpBatch || len(cmd.Batch) != 2 {
		t.Fatalf("same-client merged op=%d parts=%d", cmd.Op, len(cmd.Batch))
	}
	first, err := decodeSessionCmd(cmd.Batch[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := decodeSessionCmd(cmd.Batch[1])
	if err != nil {
		t.Fatal(err)
	}
	if first.State.Epoch != 4 || second.State.Epoch != 5 {
		t.Fatalf("same-client order=%d,%d, want 4,5", first.State.Epoch, second.State.Epoch)
	}
}

func TestSessionBatchFastEncoderMatchesWireFormatAndRejectsInvalidJSON(t *testing.T) {
	parts := []json.RawMessage{
		json.RawMessage(`{"v":1,"op":1,"c":"a"}`),
		json.RawMessage(`{"v":1,"op":2,"c":"b","e":7}`),
	}
	want, err := encodeSessionCmd(&sessionCmd{Op: sessionOpBatch, Batch: parts})
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeSessionRawBatch(parts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fast batch wire differs:\n got %s\nwant %s", got, want)
	}
	decoded, err := decodeSessionCmd(got)
	if err != nil || len(decoded.Batch) != 2 {
		t.Fatalf("decode fast batch = %#v, %v", decoded, err)
	}
	if _, err := encodeSessionRawBatch([]json.RawMessage{json.RawMessage(`{"v":`)}); err == nil {
		t.Fatal("invalid raw command accepted")
	}
	if _, err := encodeSessionRequestBatch([]*sessionProposalRequest{{data: []byte(`{`)}}); err == nil {
		t.Fatal("invalid proposal command accepted")
	}
}

func TestSessionMergeZeroCopySplitterHandlesEscapesAndNestedState(t *testing.T) {
	makeCmd := func(id, thing string) []byte {
		raw, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: id,
			State: &session.State{ClientID: id, Thing: thing, Epoch: 1,
				Subscriptions: []session.Subscription{{Filter: `a/"],[{}/b`, QoS: 1}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a := makeCmd(`dev"\a`, `thing"],[{\x`)
	b := makeCmd("normal", "normal")
	batch, err := encodeSessionRawBatch([]json.RawMessage{a, b})
	if err != nil {
		t.Fatal(err)
	}
	view, err := decodeSessionMergeView(batch, true, make([]json.RawMessage, 0, 64))
	if err != nil {
		t.Fatal(err)
	}
	if view.op != sessionOpBatch || len(view.batch) != 2 ||
		!bytes.Equal(view.batch[0], a) || !bytes.Equal(view.batch[1], b) {
		t.Fatalf("split batch = op %d parts %d", view.op, len(view.batch))
	}
	merged, widths, consumed, err := MergeSessionProposals([][]byte{batch})
	if err != nil || consumed != 1 || !slices.Equal(widths, []uint8{2}) || !bytes.Equal(merged, batch) {
		t.Fatalf("escaped merge consumed=%d widths=%v err=%v", consumed, widths, err)
	}
}

func TestMergeSessionProposalsKeepsExpireSeparate(t *testing.T) {
	expire, err := encodeSessionCmd(&sessionCmd{Op: sessionOpExpire, NowMs: 123})
	if err != nil {
		t.Fatal(err)
	}
	save, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpSave, ClientID: "a", State: replState("a", 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	merged, widths, consumed, err := MergeSessionProposals([][]byte{expire, save})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := decodeSessionCmd(merged)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != 1 || !slices.Equal(widths, []uint8{1}) || cmd.Op != sessionOpExpire {
		t.Fatalf("expire 被错误合并：consumed=%d widths=%v op=%d", consumed, widths, cmd.Op)
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

func TestReplicatedProposalBatchKeepsPerRequestResults(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-stale", 2)); err != nil {
		t.Fatal(err)
	}
	encode := func(cmd *sessionCmd) []byte {
		data, err := encodeSessionCmd(cmd)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	batch := []*sessionProposalRequest{
		{clientID: "dev-stale", data: encode(&sessionCmd{
			Op: sessionOpSave, ClientID: "dev-stale", State: replState("dev-stale", 1),
		})},
		{clientID: "dev-fresh", data: encode(&sessionCmd{
			Op: sessionOpSave, ClientID: "dev-fresh", State: replState("dev-fresh", 1),
		})},
	}
	shard := r.prop.ShardOf("dev-stale")
	// 手工选一个同分片 id：合并器只会把同分片命令装一起。
	for i := 0; r.prop.ShardOf(batch[1].clientID) != shard; i++ {
		batch[1].clientID = fmt.Sprintf("dev-fresh-%d", i)
		batch[1].data = encode(&sessionCmd{
			Op: sessionOpSave, ClientID: batch[1].clientID,
			State: replState(batch[1].clientID, 1),
		})
	}
	res, err := r.proposeBatch(shard, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Fatalf("批提案结果位图 = %b，期望只有第 0 条 stale", res)
	}
	if _, ok, err := r.Load(ctx, batch[1].clientID); err != nil || !ok {
		t.Fatalf("非 stale 子命令未落盘: ok=%v err=%v", ok, err)
	}
}

func TestReplicatedProposalBatchOrdersSameClientAndKeepsResults(t *testing.T) {
	r, _ := newReplSession(t, 0)
	ctx := context.Background()
	if err := r.Save(ctx, replState("dev-same", 5)); err != nil {
		t.Fatal(err)
	}
	encode := func(epoch cluster.Epoch) []byte {
		data, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: "dev-same", State: replState("dev-same", epoch),
		})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	batch := []*sessionProposalRequest{
		{clientID: "dev-same", data: encode(4)}, // stale，不能挡住后面的新连接
		{clientID: "dev-same", data: encode(6)},
	}
	res, err := r.proposeBatch(r.prop.ShardOf("dev-same"), batch)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Fatalf("同 clientId 批结果位图=%b，期望第 0 条 stale", res)
	}
	st, found, err := r.Load(ctx, "dev-same")
	if err != nil || !found || st.Epoch != 6 {
		t.Fatalf("批后会话 found=%v epoch=%v err=%v，期望 epoch 6", found, st, err)
	}
	// 旧连接的断开写也可能在线程调度下晚于新 CONNECT 入队；围栏应让
	// 后到的旧写 stale，而不是因为同批执行就覆盖新 owner。
	batch = []*sessionProposalRequest{
		{clientID: "dev-same", data: encode(7)},
		{clientID: "dev-same", data: encode(6)},
	}
	res, err = r.proposeBatch(r.prop.ShardOf("dev-same"), batch)
	if err != nil || res != 2 {
		t.Fatalf("新后旧的结果位图=%b err=%v，期望第 1 条 stale", res, err)
	}
	st, found, err = r.Load(ctx, "dev-same")
	if err != nil || !found || st.Epoch != 7 {
		t.Fatalf("新后旧批后会话 found=%v epoch=%v err=%v，期望 epoch 7", found, st, err)
	}
}

func TestReplicatedProposalBatchCoalescesConcurrentWrites(t *testing.T) {
	r, _ := newReplSession(t, 0)
	p := r.prop.(*loopProposer)
	shard := p.ShardOf("seed")
	// 故意超过单批 64 条，强制走「选一批、余下留队列」的
	// 分支。这条曾因就地复用 pending 底层数组而丢请求。
	ids := make([]string, 0, 256)
	for i := 0; len(ids) < cap(ids); i++ {
		id := fmt.Sprintf("batch-%d", i)
		if p.ShardOf(id) == shard {
			ids = append(ids, id)
		}
	}
	start := make(chan struct{})
	errCh := make(chan error, len(ids))
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			errCh <- r.Save(context.Background(), replState(id, 1))
		}(id)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if proposals := p.index[shard]; proposals >= uint64(len(ids)) {
		t.Fatalf("%d 条并发写入仍发出 %d 次提案，未合并", len(ids), proposals)
	}
	for _, id := range ids {
		if _, ok, err := r.Load(context.Background(), id); err != nil || !ok {
			t.Fatalf("会话 %s 未落盘: ok=%v err=%v", id, ok, err)
		}
	}
}

func TestReplicatedProposalPipelineIsBounded(t *testing.T) {
	const logical = sessionProposalBatchMax * (sessionProposalConcurrentMax + 1)
	prop := &gatedSessionProposer{
		// release 关闭后剩余批次会快速进入，channel 要能容纳它们；
		// 测试只在 release 前断言在途上限。
		entered: make(chan string, logical),
		release: make(chan struct{}),
	}
	r := NewReplicatedSessionStore(NewSessionStore(NewMemKV()), prop)
	start := make(chan struct{})
	errCh := make(chan error, logical)
	var wg sync.WaitGroup
	for i := 0; i < logical; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errCh <- r.propose(fmt.Sprintf("pipeline-%04d", i),
				[]byte(`{"v":1,"op":1,"c":"pipeline"}`))
		}(i)
	}
	close(start)
	for i := 0; i < sessionProposalConcurrentMax; i++ {
		select {
		case raw := <-prop.entered:
			cmd, err := decodeSessionCmd([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			if cmd.Op == sessionOpBatch && len(cmd.Batch) > sessionAccessProposalBatchMax {
				t.Fatalf("Access 批=%d，超过上限 %d", len(cmd.Batch),
					sessionAccessProposalBatchMax)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("只有 %d 个提案进入 pipeline，期望 %d", i, sessionProposalConcurrentMax)
		}
	}
	st := r.ProposalQueueStats()
	if st.InFlightBatches != sessionProposalConcurrentMax {
		t.Fatalf("in-flight batches=%d，期望 %d", st.InFlightBatches,
			sessionProposalConcurrentMax)
	}
	if st.PeakInFlightBatches != sessionProposalConcurrentMax {
		t.Fatalf("peak in-flight batches=%d，期望 %d", st.PeakInFlightBatches,
			sessionProposalConcurrentMax)
	}
	if st.InFlight == 0 || st.InFlight > sessionAccessProposalBatchMax*sessionProposalConcurrentMax {
		t.Fatalf("in-flight logical=%d，超出有界范围", st.InFlight)
	}
	if st.CachedBatches > sessionProposalConcurrentMax {
		t.Fatalf("cached batches=%d，超过硬上限", st.CachedBatches)
	}
	select {
	case <-prop.entered:
		t.Fatalf("单 shard 超过 %d 个物理提案在途", sessionProposalConcurrentMax)
	case <-time.After(5 * time.Millisecond):
	}
	close(prop.release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReplicatedProposalPipelineSerializesSameClient(t *testing.T) {
	prop := &gatedSessionProposer{
		entered: make(chan string, 2),
		release: make(chan struct{}),
	}
	r := NewReplicatedSessionStore(NewSessionStore(NewMemKV()), prop)
	done := make(chan error, 2)
	go func() { done <- r.propose("same-client", []byte("first")) }()
	select {
	case got := <-prop.entered:
		if got != "first" {
			t.Fatalf("首个提案=%q", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("首个提案没有进入 pipeline")
	}
	go func() { done <- r.propose("same-client", []byte("second")) }()
	select {
	case got := <-prop.entered:
		t.Fatalf("前一提案未完成时同 clientId 的 %q 已进入 pipeline", got)
	case <-time.After(5 * time.Millisecond):
	}
	close(prop.release)
	select {
	case got := <-prop.entered:
		if got != "second" {
			t.Fatalf("第二个提案=%q", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("首个提案完成后第二个仍未进入 pipeline")
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
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

type failSnapshotWriter struct{ remaining int }

func (w *failSnapshotWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("snapshot sink failed")
	}
	if len(p) > w.remaining {
		p = p[:w.remaining]
	}
	w.remaining -= len(p)
	if w.remaining == 0 {
		return len(p), errors.New("snapshot sink failed")
	}
	return len(p), nil
}

func TestSessionShardSnapshotPropagatesWriterFailure(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	st := replState("dev-1", 1)
	if err := s.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	err := s.SnapshotSessionShard(16, func(string) bool { return true },
		&failSnapshotWriter{remaining: 12})
	if err == nil {
		t.Fatal("snapshot writer failure was silently accepted")
	}
}

// TestApplySessionBatchBitsBeyond64 钉 CLU-193 的宽度判据：一条日志里
// 装超过 64 条命令时，**第 64 条之后的围栏判定不得丢**。
//
// 丢位的后果不是报错而是一次静默覆盖：提案方读到"这条没被拦"，于是把
// 一个已被接管的旧会话的状态当成写成功了，而那正是契约 01 要拦的那件事。
// 判据取在第 64 位之后（旧的 uint64 位图恰好在这里溢出成 0）。
func TestApplySessionBatchBitsBeyond64(t *testing.T) {
	st := NewSessionStore(NewMemKV())
	const total = 100
	// 先让每隔一条的那些 clientId 在盘上持有更高的 epoch：它们随后那条
	// 低 epoch 的写入必须被围栏判成 stale。
	stale := map[int]bool{}
	subs := make([]json.RawMessage, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("dev-%03d", i)
		epoch := cluster.Epoch(1)
		if i%3 == 0 {
			stale[i] = true
			if _, err := st.ApplySessionCmd(1, uint64(i+1),
				mustEncodeSessionCmd(t, &sessionCmd{
					Op: sessionOpSave, ClientID: id,
					State: replState(id, cluster.Epoch(9)),
				}), nil); err != nil {
				t.Fatal(err)
			}
		}
		subs = append(subs, mustEncodeSessionCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: id, State: replState(id, epoch),
		}))
	}
	data := mustEncodeSessionCmd(t, &sessionCmd{Op: sessionOpBatch, Batch: subs})
	ents := []SessionEntry{{Index: 100, Data: data}}
	if err := st.ApplySessionBatch(1, ents, nil); err != nil {
		t.Fatalf("整批 apply: %v", err)
	}
	bits := ents[0].ResultBits
	if want := (total + 7) / 8; len(bits) != want {
		t.Fatalf("判定位图 %d 字节，期望 %d（一位一条）", len(bits), want)
	}
	beyond := 0
	for i := 0; i < total; i++ {
		got := bits[i/8]&(1<<uint(i%8)) != 0
		if got != stale[i] {
			t.Fatalf("第 %d 条的 stale 位 = %v，期望 %v", i, got, stale[i])
		}
		if got && i >= 64 {
			beyond++
		}
	}
	if beyond == 0 {
		t.Fatal("用例没有覆盖到第 64 位之后，证明不了宽位图")
	}
}

// TestApplySessionBatchFillsBothResultForms 钉 CLU-193 的另一半：批命令必须
// **同时**填 Result 与 ResultBits。
//
// 一条 Raft 日志只装了一个 Access 批时（最常见的那种），cluster 层交回提案方
// 的是那个 uint64 而不是位图——那条路径还要给 Expire 回条数，所以不能改成
// 按位切。只填位图的后果是围栏判定在最常见的路径上静默变成“没被拦”，
// 也就是一次覆盖。
//
// 这条用例必须走 ApplySessionBatch：用例里的假提案方走的是逐条的
// ApplySessionCmd，而生产走整批那条，所以缺口在逐条路径上看不见。
func TestApplySessionBatchFillsBothResultForms(t *testing.T) {
	st := NewSessionStore(NewMemKV())
	// dev-b 先在盘上持有更高的 epoch，它随后那条低 epoch 的写必须被拦。
	if _, err := st.ApplySessionCmd(1, 1, mustEncodeSessionCmd(t, &sessionCmd{
		Op: sessionOpSave, ClientID: "dev-b",
		State: replState("dev-b", cluster.Epoch(9)),
	}), nil); err != nil {
		t.Fatal(err)
	}
	subs := []json.RawMessage{
		mustEncodeSessionCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-a", State: replState("dev-a", 1),
		}),
		mustEncodeSessionCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-b", State: replState("dev-b", 1),
		}),
	}
	ents := []SessionEntry{{Index: 7, Data: mustEncodeSessionCmd(t,
		&sessionCmd{Op: sessionOpBatch, Batch: subs})}}
	if err := st.ApplySessionBatch(1, ents, nil); err != nil {
		t.Fatalf("整批 apply: %v", err)
	}
	if ents[0].Result != 0b10 {
		t.Fatalf("Result = %02b，期望 10（只有第 1 条被围栏拦）", ents[0].Result)
	}
	if len(ents[0].ResultBits) != 1 || ents[0].ResultBits[0] != 0b10 {
		t.Fatalf("ResultBits = %v，期望 [2]", ents[0].ResultBits)
	}
}

func mustEncodeSessionCmd(t *testing.T, cmd *sessionCmd) []byte {
	t.Helper()
	data, err := encodeSessionCmd(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
