package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

var ownershipBenchmarkSink *ownershipShardState
var ownershipBatchBenchmarkSink *ownershipClaimBatch

func BenchmarkOwnershipState200K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		st := &ownershipShardState{owners: make(map[string]shardOwnerRec, 200_000)}
		for j := 0; j < 200_000; j++ {
			clientID := fmt.Sprintf("device-%08d", j)
			// 模拟每条 wire claim 独立解码出来的同值字符串。
			node := NodeID(strings.Clone("access-node-0001"))
			st.put(clientID, node, Epoch(j+1))
		}
		ownershipBenchmarkSink = st
	}
}

func BenchmarkSelectOwnershipClaimBatch256(b *testing.B) {
	now := time.Unix(300, 0)
	requests := make([]*ownershipClaimRequest, ownershipClaimBatchMax)
	for i := range requests {
		requests[i] = ownershipTestRequest(fmt.Sprintf("device-%03d", i), now)
	}
	lane := &ownershipClaimLane{
		pending:  append([]*ownershipClaimRequest(nil), requests...),
		inFlight: make(map[string]struct{}, ownershipClaimBatchMax),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, batch, ok := selectOwnershipClaimBatch(lane, now)
		if !ok || len(batch.requests) != ownershipClaimBatchMax {
			b.Fatalf("选批失败: ok=%v len=%d", ok, len(batch.requests))
		}
		for _, req := range batch.requests {
			delete(lane.inFlight, req.clientID)
		}
		lane.active--
		lane.pending = append(lane.pending[:0], requests...)
		ownershipBatchBenchmarkSink = batch
		lane.putBatch(batch)
	}
}

func TestOwnershipStateInternsNodesAndCompactsAfterChurn(t *testing.T) {
	st := &ownershipShardState{owners: make(map[string]shardOwnerRec)}
	const total = ownershipCompactThreshold * 2
	for i := 0; i < total; i++ {
		st.put(fmt.Sprintf("device-%d", i), NodeID(strings.Clone("access-a")), Epoch(i+1))
	}
	if len(st.nodes) != 1 {
		t.Fatalf("相同节点名驻留了 %d 份，期望 1", len(st.nodes))
	}
	prev, had := st.put("device-0", "access-b", 999)
	if !had || prev.node != "access-a" || prev.epoch != 1 {
		t.Fatalf("接管前 owner = %+v had=%v", prev, had)
	}
	if got := st.expand(st.owners["device-0"]); got.node != "access-b" || got.epoch != 999 {
		t.Fatalf("接管后 owner = %+v", got)
	}
	old := st.owners
	for i := 0; i < ownershipCompactThreshold+1; i++ {
		st.remove(fmt.Sprintf("device-%d", i))
	}
	old["old-map-probe"] = shardOwnerRec{}
	if _, ok := st.owners["old-map-probe"]; ok {
		t.Fatal("大量 Release 后仍复用旧 owners map，哈希桶不能回收")
	}
	if got := len(st.owners); got != total-ownershipCompactThreshold-1 {
		t.Fatalf("缩表后 owner 数 = %d", got)
	}
}

func TestShardOwnershipUsesLeaderTermAndEvictsPreviousNode(t *testing.T) {
	groups, addrs := newTLSPair(t)
	_ = waitLeader(t, []*TLSGroup{groups["a"], groups["b"]}, 10*time.Second)
	replicas := replicasOf(addrs)

	shards := map[NodeID]*ShardGroup{}
	owners := map[NodeID]*ShardOwnership{}
	for _, id := range []NodeID{"a", "b"} {
		fsm := newResultShardFSM()
		sg, err := groups[id].StartSessionShards(ShardOptions{
			Count: 1, Replicas: replicas, Peers: addrs, Bootstrap: id == "a",
			Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
			SnapshotTo:   func(uint64, io.Writer) error { return nil },
			RestoreFrom:  func(uint64, io.Reader) error { return nil },
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("节点 %s 起分片: %v", id, err)
		}
		shards[id] = sg
		t.Cleanup(func() { _ = sg.Close() })
		o, err := NewShardOwnership(groups[id], sg, 8)
		if err != nil {
			t.Fatalf("节点 %s 起所有权服务: %v", id, err)
		}
		owners[id] = o
		t.Cleanup(func() { _ = o.Close() })
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!shards["a"].Ready() || !shards["b"].Ready()) {
		time.Sleep(20 * time.Millisecond)
	}
	if !shards["a"].Ready() || !shards["b"].Ready() {
		t.Fatal("会话分片没有就绪")
	}

	leaderReplica, term, ok, err := shards["a"].nh.GetLeaderID(shardSessionBase)
	if err != nil || !ok {
		t.Fatalf("读分片 leader: id=%d term=%d ok=%v err=%v", leaderReplica, term, ok, err)
	}
	leaderID := shards["a"].replicaNode[leaderReplica]
	followerID := NodeID("a")
	if followerID == leaderID {
		followerID = "b"
	}

	// 从 follower 发起，必须一跳到 leader，epoch 高位用真实 Raft term。
	before := groups[leaderID].ownAccepted.Load()
	first, err := owners[followerID].Claim(context.Background(), "dev-1", followerID)
	if err != nil {
		t.Fatalf("远程 Claim: %v", err)
	}
	if uint64(first.Term()) != term || first.Seq() != 1 {
		t.Fatalf("首个 epoch=%d (term=%d seq=%d)，期望 term=%d seq=1",
			first, first.Term(), first.Seq(), term)
	}
	if got := groups[leaderID].ownAccepted.Load(); got != before+1 {
		t.Fatalf("Claim 没有经长连接直达 leader：入站 %d -> %d", before, got)
	}

	second, err := owners[leaderID].Claim(context.Background(), "dev-1", leaderID)
	if err != nil {
		t.Fatalf("二次 Claim: %v", err)
	}
	if second <= first || second.Term() != first.Term() || second.Seq() != first.Seq()+1 {
		t.Fatalf("二次 epoch=%d，首次=%d；同 term 必须严格递增", second, first)
	}
	select {
	case ev := <-owners[followerID].Evictions():
		if ev.ClientID != "dev-1" || ev.PrevEpoch != first || ev.NewEpoch != second || ev.By != leaderID {
			t.Fatalf("驱逐通知 = %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("前一个 owner 没有收到跨节点驱逐通知")
	}

	if err := owners[followerID].Release(context.Background(), "dev-1", first); err != ErrStaleEpoch {
		t.Fatalf("旧 epoch Release = %v，期望 ErrStaleEpoch", err)
	}
	if err := owners[followerID].Release(context.Background(), "dev-1", second); err != nil {
		t.Fatalf("当前 epoch Release: %v", err)
	}
	if _, _, ok := owners[leaderID].Lookup(context.Background(), "dev-1"); ok {
		t.Fatal("Release 之后仍能查到 owner")
	}

	// 并发 Claim 必须真正合批，不能只是协议上有个 slice。
	const claims = 256
	start := make(chan struct{})
	errCh := make(chan error, claims)
	beforeBatches := owners[followerID].remoteBatches.Load()
	var wg sync.WaitGroup
	for i := 0; i < claims; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := owners[followerID].Claim(context.Background(), fmt.Sprintf("batch-%d", i), followerID)
			errCh <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	used := owners[followerID].remoteBatches.Load() - beforeBatches
	if used >= claims/2 {
		t.Fatalf("%d 条并发 Claim 发了 %d 次 RPC，合批已退化", claims, used)
	}
}

func TestSwitchableOwnershipKeepsEvictionChannelStable(t *testing.T) {
	first := NewLocalOwnership("a", 4)
	s := NewSwitchableOwnership(first, 4)
	ch := s.Evictions()
	second := NewLocalOwnership("b", 4)
	if err := s.Use(second); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.Claim(context.Background(), "dev", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(context.Background(), "dev", "b"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.ClientID != "dev" || ev.By != "b" {
			t.Fatalf("切换后的驱逐通知 = %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("切换实现后原 Evictions 通道没有收到通知")
	}
}

func TestShardOwnershipCloseUnblocksPendingClaims(t *testing.T) {
	g := &TLSGroup{}
	req := &ownershipClaimRequest{done: make(chan ownershipClaimResult, 1)}
	lane := &ownershipClaimLane{
		running:  true,
		pending:  []*ownershipClaimRequest{req},
		inFlight: make(map[string]struct{}),
	}
	o := &ShardOwnership{
		group:  g,
		state:  make(map[uint64]*ownershipShardState),
		events: make(chan Eviction, 1),
		batch:  map[uint64]*ownershipClaimLane{shardSessionBase: lane},
	}
	g.ownership = o

	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-req.done:
		if res.err != ErrClosed {
			t.Fatalf("关闭后的待处理 Claim = %v，期望 ErrClosed", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close 没有唤醒合批窗口中的 Claim")
	}

	o.batchMu.Lock()
	closed := o.batchClosed
	left := len(lane.pending)
	o.batchMu.Unlock()
	if !closed || left != 0 {
		t.Fatalf("关闭后 batchClosed=%v pending=%d", closed, left)
	}
	if g.ownership != nil {
		t.Fatal("Close 后 TLSGroup 仍暴露所有权服务")
	}
}

func ownershipTestRequest(id string, queued time.Time) *ownershipClaimRequest {
	return &ownershipClaimRequest{clientID: id, node: "access-a", leader: "leader-a", queued: queued}
}

func TestSelectOwnershipClaimBatchBoundsAndPreservesRequests(t *testing.T) {
	now := time.Unix(100, 0)
	lane := &ownershipClaimLane{inFlight: make(map[string]struct{}), active: ownershipClaimConcurrentMax}
	for i := 0; i < ownershipClaimBatchMax+44; i++ {
		lane.pending = append(lane.pending,
			ownershipTestRequest(fmt.Sprintf("device-%03d", i), now.Add(-2*ownershipClaimMaxWait)))
	}
	original := append([]*ownershipClaimRequest(nil), lane.pending...)
	if _, batch, ok := selectOwnershipClaimBatch(lane, now); ok || batch != nil {
		t.Fatal("已达在途 RPC 上限时仍派发了批次")
	}
	if len(lane.pending) != len(original) {
		t.Fatalf("背压后 pending=%d，期望 %d", len(lane.pending), len(original))
	}

	lane.active = 0
	_, first, ok := selectOwnershipClaimBatch(lane, now)
	if !ok || len(first.requests) != ownershipClaimBatchMax || len(lane.pending) != 44 {
		t.Fatalf("首批 ok=%v batch=%d remain=%d", ok, len(first.requests), len(lane.pending))
	}
	for _, req := range first.requests {
		delete(lane.inFlight, req.clientID)
	}
	lane.active--
	_, second, ok := selectOwnershipClaimBatch(lane, now)
	if !ok || len(second.requests) != 44 || len(lane.pending) != 0 {
		t.Fatalf("第二批 ok=%v batch=%d remain=%d", ok, len(second.requests), len(lane.pending))
	}
	seen := make(map[*ownershipClaimRequest]bool, len(original))
	for _, req := range append(first.requests, second.requests...) {
		seen[req] = true
	}
	for _, req := range original {
		if !seen[req] {
			t.Fatalf("请求 %s 在 scratch 交换后丢失", req.clientID)
		}
	}
}

func TestSelectOwnershipClaimBatchSerializesClientAndClearsDeferredScratch(t *testing.T) {
	now := time.Unix(200, 0)
	first := ownershipTestRequest("same-client", now)
	second := ownershipTestRequest("same-client", now)
	lane := &ownershipClaimLane{
		pending:  []*ownershipClaimRequest{first, second},
		inFlight: make(map[string]struct{}),
	}
	_, batch, ok := selectOwnershipClaimBatch(lane, now)
	if !ok || len(batch.requests) != 1 || batch.requests[0] != first || len(lane.pending) != 1 {
		t.Fatalf("同 client 首批 ok=%v batch=%v pending=%d", ok, batch, len(lane.pending))
	}
	if _, _, ok := selectOwnershipClaimBatch(lane, now.Add(2*ownershipClaimMaxWait)); ok {
		t.Fatal("前一个同 client Claim 在途时又派发了一批")
	}
	delete(lane.inFlight, first.clientID)
	lane.active--
	_, batch, ok = selectOwnershipClaimBatch(lane, now.Add(2*ownershipClaimMaxWait))
	if !ok || len(batch.requests) != 1 || batch.requests[0] != second {
		t.Fatalf("同 client 串行的第二批 ok=%v batch=%v", ok, batch)
	}

	// 已有 RPC 在途时，小批先等到 1ms；这条分支会使用 scratch/seen
	// 做一次候选选择，但不得移除 pending 或残留 seen。
	lane = &ownershipClaimLane{active: 1, inFlight: make(map[string]struct{})}
	for i := 0; i < ownershipClaimConcurrentMin-1; i++ {
		lane.pending = append(lane.pending, ownershipTestRequest(fmt.Sprintf("small-%d", i), now))
	}
	if _, _, ok := selectOwnershipClaimBatch(lane, now); ok {
		t.Fatal("未到最大等待时间的小批被提前派发")
	}
	if len(lane.pending) != ownershipClaimConcurrentMin-1 || len(lane.seen) != 0 {
		t.Fatalf("延后分支破坏复用状态: pending=%d seen=%d", len(lane.pending), len(lane.seen))
	}
	_, batch, ok = selectOwnershipClaimBatch(lane, now.Add(ownershipClaimMaxWait))
	if !ok || len(batch.requests) != ownershipClaimConcurrentMin-1 {
		t.Fatalf("超时后小批未派发: ok=%v batch=%d", ok, len(batch.requests))
	}
}

func TestOwnershipClaimRequestCacheIsBounded(t *testing.T) {
	o := &ShardOwnership{}
	for i := 0; i < ownershipClaimRequestCacheMax+100; i++ {
		o.recycleClaimRequest(&ownershipClaimRequest{
			clientID: "device", node: "access", leader: "leader", queued: time.Now(),
			done: make(chan ownershipClaimResult, 1),
		})
	}
	if got := len(o.reqFree); got != ownershipClaimRequestCacheMax {
		t.Fatalf("请求缓存=%d，期望硬上限 %d", got, ownershipClaimRequestCacheMax)
	}
	for _, req := range o.reqFree {
		if req.clientID != "" || req.node != "" || req.leader != "" || !req.queued.IsZero() {
			t.Fatal("回收的 Claim 请求仍持有调用方数据")
		}
	}
}
