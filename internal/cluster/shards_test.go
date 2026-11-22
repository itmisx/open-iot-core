package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestShardLeaderTargetsAreDeterministicAndBalanced(t *testing.T) {
	replicas := map[NodeID]uint64{
		"node-c": 30,
		"node-a": 10,
		"node-b": 20,
	}
	got := shardLeaderTargets(replicas, 8)
	want := []uint64{10, 20, 30, 10, 20, 30, 10, 20}
	if len(got) != len(want) {
		t.Fatalf("目标数 = %d，期望 %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 个分片的目标 = %d，期望 %d；map 顺序不得影响布局",
				i, got[i], want[i])
		}
	}
	if got := shardLeaderTargets(map[NodeID]uint64{"bad": 0}, 8); got != nil {
		t.Fatalf("无有效副本时应返回 nil，得到 %v", got)
	}
}

// 会话分片组的验收（分区组的第一块，CLU-006）。
//
// 两条判据：
//
//   - **路由是纯函数**：同一个 clientId 在任何节点上算出同一个分片。
//     用带随机种子的哈希（Go 的 map 哈希就是）会让不同节点把同一个会话
//     算到不同分片上，表现是"会话时有时无"，而那种 bug 只在多节点下显形。
//   - **分片是独立的日志**：往不同 clientId 提交，命令落在不同分片上，
//     各自的 applied index 互不影响。不独立就等于没分片——吞吐仍然是
//     单条日志那个数。

// shardFSM 是分片状态机的用例实现：按分片分开记。
type shardFSM struct {
	mu      sync.Mutex
	byShard map[uint64][]string
	applied map[uint64]uint64
}

func newShardFSM() *shardFSM {
	return &shardFSM{byShard: map[uint64][]string{}, applied: map[uint64]uint64{}}
}

func (f *shardFSM) apply(shard, index uint64, data []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byShard[shard] = append(f.byShard[shard], string(data))
	f.applied[shard] = index
	return 0, nil
}

func (f *shardFSM) appliedIndex(shard uint64) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied[shard]
}

func (f *shardFSM) count(shard uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.byShard[shard])
}

func (f *shardFSM) shardsUsed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.byShard {
		if len(v) > 0 {
			n++
		}
	}
	return n
}

func TestShardRoutingIsDeterministic(t *testing.T) {
	s := &ShardGroup{count: 8}
	// 同一个 clientId 反复算必须一样；换一个实例也必须一样
	// （纯函数，不带状态与种子）。
	other := &ShardGroup{count: 8}
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("dev-%d", i)
		a, b := s.ShardOf(id), s.ShardOf(id)
		if a != b {
			t.Fatalf("%s 两次算出 %d 与 %d", id, a, b)
		}
		if c := other.ShardOf(id); c != a {
			t.Fatalf("%s 在另一个实例上算出 %d，期望 %d；"+
				"路由必须是纯函数，否则不同节点会把同一个会话算到不同分片", id, c, a)
		}
		if a < shardSessionBase || a >= shardSessionBase+8 {
			t.Fatalf("%s 算出的分片 %d 超出范围", id, a)
		}
	}
	// 200 个 clientId 应当摊到多个分片上，而不是全挤在一个里。
	seen := map[uint64]bool{}
	for i := 0; i < 200; i++ {
		seen[s.ShardOf(fmt.Sprintf("dev-%d", i))] = true
	}
	if len(seen) < 6 {
		t.Errorf("200 个 clientId 只用到 %d 个分片（共 8），分布太偏", len(seen))
	}
}

func TestSessionShardsAreIndependentLogs(t *testing.T) {
	ca, caKey, caPEM := mintCA(t)
	addrs := map[NodeID]string{"node-a": freeLocalAddr(t)}
	cfg := mintNode(t, ca, caKey, caPEM, "node-a", 201)
	g := startTLSGroup(t, "node-a", addrs, cfg, true, newTestFSM())

	// 等元数据组选出 leader：分片建在它的 NodeHost 上。
	deadline := time.Now().Add(20 * time.Second)
	for g.LeaderID() == "" && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if g.LeaderID() == "" {
		t.Fatal("元数据组没有选出 leader")
	}

	fsm := newShardFSM()
	shards, err := g.StartSessionShards(ShardOptions{
		Count:     4,
		Replicas:  replicasOf(addrs),
		Peers:     addrs,
		Bootstrap: true,
		Apply:     fsm.apply, AppliedIndex: fsm.appliedIndex,
		SnapshotTo:   func(uint64, io.Writer) error { return nil },
		RestoreFrom:  func(uint64, io.Reader) error { return nil },
		ApplyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("起分片失败: %v", err)
	}
	t.Cleanup(func() { _ = shards.Close() })

	for deadline := time.Now().Add(20 * time.Second); !shards.Ready() && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
	}
	if !shards.Ready() {
		t.Fatal("20 秒内分片没有全部就绪")
	}

	// 往一批 clientId 提交，命令要落到各自的分片上。
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("dev-%d", i)
		if _, err := shards.Propose(id, []byte(id)); err != nil {
			t.Fatalf("提交 %s 失败: %v", id, err)
		}
	}
	// 分片号与条数也是接口的一部分：过期清理按 ShardIDs 逐个提命令，
	// 少报一个就有一批会话永远不被清理（CLU-114）。
	if got := shards.ShardCount(); got != 4 {
		t.Errorf("ShardCount = %d，期望 4", got)
	}
	if ids := shards.ShardIDs(); len(ids) != 4 ||
		ids[0] != shardSessionBase || ids[3] != shardSessionBase+3 {
		t.Errorf("ShardIDs = %v", ids)
	}
	// Barrier 要能等到追平：启动期与读取前都靠它。
	if err := shards.Barrier("dev-0", 10*time.Second); err != nil {
		t.Errorf("Barrier: %v", err)
	}
	if n := fsm.shardsUsed(); n < 3 {
		t.Errorf("40 条命令只落到 %d 个分片（共 4）：分片没有真的并行", n)
	}
	// 各分片的 applied index 互相独立：每个都是自己那条日志的序号，
	// 不是全局递增的。
	idx := map[uint64]uint64{}
	for i := uint64(0); i < 4; i++ {
		shard := shardSessionBase + i
		if fsm.count(shard) > 0 {
			idx[shard] = fsm.appliedIndex(shard)
		}
	}
	if len(idx) < 3 {
		t.Fatalf("只有 %d 个分片有 applied index", len(idx))
	}
	total := 0
	for _, n := range idx {
		total += int(n)
	}
	if total >= 40*len(idx) {
		t.Errorf("各分片的 applied index 之和 = %d，看起来是全局递增的；"+
			"分片应当各有自己的日志序号", total)
	}
}

// TestShardProposalRoutesDirectlyToLeader 用两个真 TLS 节点验证会话
// 提案的快路径。不只验「最后写进去了」：Dragonboat 原本就能
// 从 follower 转发，那样的功能用例会假绿。这里同时数 markOwn
// 入站连接，证明合并好的命令先直达 leader。
func TestShardProposalRoutesDirectlyToLeader(t *testing.T) {
	groups, addrs := newTLSPair(t)
	_ = waitLeader(t, []*TLSGroup{groups["a"], groups["b"]}, 10*time.Second)

	replicas := replicasOf(addrs)
	fsms := map[NodeID]*resultShardFSM{"a": newResultShardFSM(), "b": newResultShardFSM()}
	shards := map[NodeID]*ShardGroup{}
	for _, id := range []NodeID{"a", "b"} {
		id := id
		sg, err := groups[id].StartSessionShards(ShardOptions{
			Count: 1, Replicas: replicas, Peers: addrs, Bootstrap: id == "a",
			Apply: fsms[id].apply, AppliedIndex: fsms[id].appliedIndex,
			SnapshotTo:   func(uint64, io.Writer) error { return nil },
			RestoreFrom:  func(uint64, io.Reader) error { return nil },
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("节点 %s 起分片失败: %v", id, err)
		}
		shards[id] = sg
		t.Cleanup(func() { _ = sg.Close() })
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!shards["a"].Ready() || !shards["b"].Ready()) {
		time.Sleep(20 * time.Millisecond)
	}
	if !shards["a"].Ready() || !shards["b"].Ready() {
		t.Fatal("10 秒内会话分片没有选出 leader")
	}

	leaderReplica, _, ok, err := shards["a"].nh.GetLeaderID(shardSessionBase)
	if err != nil || !ok {
		t.Fatalf("读 leader: id=%d ok=%v err=%v", leaderReplica, ok, err)
	}
	leaderID := shards["a"].replicaNode[leaderReplica]
	followerID := NodeID("a")
	if leaderID == followerID {
		followerID = "b"
	}

	before := groups[leaderID].ownAccepted.Load()
	value, err := shards[followerID].ProposeShard(shardSessionBase, []byte("remote"))
	if err != nil {
		t.Fatalf("跨节点提案: %v", err)
	}
	if value != resultShardValue {
		t.Fatalf("跨节点返回值 = %#x，期望 %#x", value, uint64(resultShardValue))
	}
	if got := groups[leaderID].ownAccepted.Load(); got != before+1 {
		t.Fatalf("leader 的 markOwn 入站连接从 %d 变为 %d，期望 +1；提案没有直接路由", before, got)
	}
	if st := shards[followerID].Stats(); st.ProposeRemote != 1 || st.ProposeErrors != 0 {
		t.Fatalf("follower 提案统计 = %+v", st)
	}

	// 本地 leader 路径不应绕进 markOwn。
	before = groups[followerID].ownAccepted.Load()
	if _, err := shards[leaderID].ProposeShard(shardSessionBase, []byte("local")); err != nil {
		t.Fatalf("leader 本地提案: %v", err)
	}
	if got := groups[followerID].ownAccepted.Load(); got != before {
		t.Fatalf("本地 leader 提案却连了 follower：入站 %d -> %d", before, got)
	}

	// 故意把请求送给 follower；接收端必须就地拒绝，不能
	// 又转回 leader 形成第二跳。
	leaderAccepted := groups[leaderID].ownAccepted.Load()
	if _, err := groups[leaderID].forwardShard(followerID, shardSessionBase, []byte("wrong-target")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("非 leader 接收端返回 %v，期望 ErrNotLeader", err)
	}
	if got := groups[leaderID].ownAccepted.Load(); got != leaderAccepted {
		t.Fatalf("非 leader 把提案又转回 leader：入站 %d -> %d", leaderAccepted, got)
	}
}

func TestSessionReadRoutesToPlacedReplica(t *testing.T) {
	groups, addrs := newTLSPair(t)
	_ = waitLeader(t, []*TLSGroup{groups["a"], groups["b"]}, 10*time.Second)
	replicas := replicasOf(addrs)
	placement, err := newSessionPlacement(replicas, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	placedReplica, _ := placement.leader(shardSessionBase)
	placedNode := NodeID("")
	for id, replica := range replicas {
		if replica == placedReplica {
			placedNode = id
		}
	}
	if placedNode == "" {
		t.Fatal("RF=1 放置没有节点")
	}
	nonMember := NodeID("a")
	if nonMember == placedNode {
		nonMember = "b"
	}

	shards := map[NodeID]*ShardGroup{}
	fsms := map[NodeID]*shardFSM{}
	for _, id := range []NodeID{"a", "b"} {
		id := id
		fsm := newShardFSM()
		sg, serr := groups[id].StartSessionShards(ShardOptions{
			Count: 1, Replicas: replicas, Peers: addrs, Bootstrap: id == "a",
			Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
			SnapshotTo:  func(uint64, io.Writer) error { return nil },
			RestoreFrom: func(uint64, io.Reader) error { return nil },
			ReadSession: func(clientID string) ([]byte, bool, error) {
				if clientID == "missing" {
					return nil, false, nil
				}
				return []byte("state:" + clientID), true, nil
			},
		})
		if serr != nil {
			t.Fatalf("节点 %s 起分片失败: %v", id, serr)
		}
		// 启动仍保持 RF=all；本用例把目录切成 RF=1，验证 Access 节点
		// 在真正停掉非成员副本之前已经具备正确的远程读路径。
		sg.placement = placement
		shards[id] = sg
		fsms[id] = fsm
		t.Cleanup(func() { _ = sg.Close() })
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!shards["a"].Ready() || !shards["b"].Ready()) {
		time.Sleep(20 * time.Millisecond)
	}
	if !shards["a"].Ready() || !shards["b"].Ready() {
		t.Fatal("会话分片没有选出 leader")
	}

	if shards[nonMember].HasLocalReplica("device-1") {
		t.Fatalf("非成员节点 %s 被误判为本地副本", nonMember)
	}
	raw, found, err := shards[nonMember].ReadSession("device-1")
	if err != nil || !found || string(raw) != "state:device-1" {
		t.Fatalf("远程读 = %q, %v, %v", raw, found, err)
	}
	if raw, found, err := shards[nonMember].ReadSession("missing"); err != nil || found || raw != nil {
		t.Fatalf("远程缺失读 = %q, %v, %v", raw, found, err)
	}

	// 把真实 leader 转到 RF=1 的放置节点，再从“非副本”Access 节点
	// 提交。虽然本用例底层仍启动了 RF=all，placement 分支会强制它走
	// 远程 session-cell 路由，钉住选择性启动后的行为。
	leaderReplica, _, ok, err := shards[placedNode].nh.GetLeaderID(shardSessionBase)
	if err != nil || !ok {
		t.Fatalf("读取分片 leader: %v ok=%v", err, ok)
	}
	if leaderReplica != placedReplica {
		leaderNode := NodeID("")
		for id, replica := range replicas {
			if replica == leaderReplica {
				leaderNode = id
			}
		}
		if leaderNode == "" {
			t.Fatalf("未知 leader 副本 %d", leaderReplica)
		}
		if err := shards[leaderNode].nh.RequestLeaderTransfer(shardSessionBase, placedReplica); err != nil {
			t.Fatalf("转移分片 leader: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			leader, _, ready, _ := shards[placedNode].nh.GetLeaderID(shardSessionBase)
			if ready && leader == placedReplica {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if _, err := shards[nonMember].ProposeShard(shardSessionBase, []byte("from-access")); err != nil {
		t.Fatalf("非副本节点提案: %v", err)
	}
	if got := fsms[placedNode].count(shardSessionBase); got == 0 {
		t.Fatal("放置副本没有应用非副本节点的提案")
	}
}

func TestSelectiveSessionReplicasServeNonMemberAccessNode(t *testing.T) {
	ids := []NodeID{"a", "b", "c", "d"}
	groups, addrs := newTLSCluster(t, ids)
	allGroups := make([]*TLSGroup, 0, len(ids))
	for _, id := range ids {
		allGroups = append(allGroups, groups[id])
	}
	_ = waitLeader(t, allGroups, 15*time.Second)
	replicas := replicasOf(addrs)

	shards := make(map[NodeID]*ShardGroup, len(ids))
	fsms := make(map[NodeID]*shardFSM, len(ids))
	owners := make(map[NodeID]*ShardOwnership, len(ids))
	for _, id := range ids {
		fsm := newShardFSM()
		sg, err := groups[id].StartSessionShards(ShardOptions{
			Count: 4, ReplicationFactor: 3,
			Replicas: replicas, Peers: addrs, Bootstrap: id == "a",
			Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
			SnapshotTo:  func(uint64, io.Writer) error { return nil },
			RestoreFrom: func(uint64, io.Reader) error { return nil },
			ReadSession: func(clientID string) ([]byte, bool, error) {
				return []byte("cell:" + clientID), true, nil
			},
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("节点 %s 起 RF=3 分片: %v", id, err)
		}
		shards[id], fsms[id] = sg, fsm
		t.Cleanup(func() { _ = sg.Close() })
		o, err := NewShardOwnership(groups[id], sg, 8)
		if err != nil {
			t.Fatalf("节点 %s 起 ownership: %v", id, err)
		}
		owners[id] = o
		t.Cleanup(func() { _ = o.Close() })
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, id := range ids {
			ready = ready && shards[id].Ready()
		}
		if ready {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	for _, id := range ids {
		if !shards[id].Ready() {
			t.Fatalf("节点 %s 看不到全部 session cell 就绪", id)
		}
	}

	// 每个节点恰好缺一个分片；挑出那个分片对应的 clientId，从非副本
	// Access 节点验证读、写、线性 Barrier 和 ownership。
	access := NodeID("d")
	clientID := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("rf3-device-%d", i)
		if !shards[access].HasLocalReplica(candidate) {
			clientID = candidate
			break
		}
	}
	if clientID == "" {
		t.Fatal("没有找到 d 不承载的分片")
	}
	shard := shards[access].ShardOf(clientID)
	if _, err := shards[access].Propose(clientID, []byte("from-access")); err != nil {
		t.Fatalf("非副本写入: %v", err)
	}
	if err := shards[access].Barrier(clientID, 5*time.Second); err != nil {
		t.Fatalf("非副本 Barrier: %v", err)
	}
	raw, found, err := shards[access].ReadSession(clientID)
	if err != nil || !found || string(raw) != "cell:"+clientID {
		t.Fatalf("非副本读取 = %q found=%v err=%v", raw, found, err)
	}
	applied := 0
	for _, id := range ids {
		if shards[id].placement.contains(shard, replicas[id]) && fsms[id].count(shard) > 0 {
			applied++
		}
	}
	if applied < 2 {
		t.Fatalf("RF=3 写入只在 %d 个副本应用，未形成多数派", applied)
	}

	// 故意把 leader 从放置表首项转走。非副本节点会先收到首项明确的
	// ErrNotLeader，再安全尝试其余副本；这不能依赖后台 leader 平衡。
	members := shards[access].placement.members(shard)
	preferred := members[0]
	target := members[1]
	current, ready, err := groups[access].forwardShardStatus(
		shards[access].replicaNode[preferred], shard)
	if err != nil || !ready {
		t.Fatalf("读取 RF=3 leader: %d ready=%v err=%v", current, ready, err)
	}
	if current == target {
		target = members[2]
	}
	leaderNode := shards[access].replicaNode[current]
	if err := shards[leaderNode].nh.RequestLeaderTransfer(shard, target); err != nil {
		t.Fatalf("转走首选 leader: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader, ready, err := groups[access].forwardShardStatus(
			shards[access].replicaNode[preferred], shard)
		if err == nil && ready && leader == target {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	epoch, err := owners[access].Claim(context.Background(), clientID, access)
	if err != nil || !epoch.Valid() {
		t.Fatalf("非副本 ownership Claim = %d, %v", epoch, err)
	}
}

const resultShardValue uint64 = 0xa55a5aa55aa55aa5

type resultShardFSM struct {
	mu      sync.Mutex
	applied map[uint64]uint64
}

func newResultShardFSM() *resultShardFSM {
	return &resultShardFSM{applied: make(map[uint64]uint64)}
}

func (f *resultShardFSM) apply(shard, index uint64, _ []byte) (uint64, error) {
	f.mu.Lock()
	f.applied[shard] = index
	f.mu.Unlock()
	return resultShardValue, nil
}

func (f *resultShardFSM) appliedIndex(shard uint64) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied[shard]
}

func TestSessionReadLaneBatchesRequests(t *testing.T) {
	const count = 128
	var mu sync.Mutex
	calls := 0
	s := &ShardGroup{
		readLane: make(map[uint64]*sessionReadLane),
		readRemote: func(_ uint64, ids []string) ([]sessionReadResult, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			out := make([]sessionReadResult, len(ids))
			for i := range ids {
				out[i] = sessionReadResult{data: []byte(ids[i]), found: true}
			}
			return out, nil
		},
	}
	lane := &sessionReadLane{running: true}
	requests := make([]*sessionReadRequest, count)
	for i := range requests {
		id := fmt.Sprintf("device-%d", i)
		requests[i] = &sessionReadRequest{
			clientID: id, done: make(chan sessionReadResult, 1),
		}
		lane.pending = append(lane.pending, requests[i])
	}
	s.readLane[7] = lane
	s.readWG.Add(1)
	go s.runSessionReadLane(7, lane)
	for i, req := range requests {
		res := <-req.done
		if res.err != nil || !res.found || string(res.data) != req.clientID {
			t.Fatalf("第 %d 条应答 = %+v", i, res)
		}
	}
	s.readWG.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("%d 条并发读取产生 %d 次 RPC，期望 1", count, calls)
	}
	st := s.Stats()
	// 本用例直接预装 lane，只验证 worker；逻辑请求计数由 queue 入口负责，
	// 批次数仍必须精确反映物理 RPC。
	if st.SessionReadBatches != 1 || st.SessionReadErrors != 0 {
		t.Fatalf("读取指标 = %+v", st)
	}
}

func TestSessionReadQueueRejectsAfterClose(t *testing.T) {
	s := &ShardGroup{readLane: make(map[uint64]*sessionReadLane), readStop: true}
	if res := s.queueRemoteSessionRead(1, "device"); !errors.Is(res.err, ErrClosed) {
		t.Fatalf("关闭后读取错误 = %v", res.err)
	}
}

func TestSessionReadLaneCapsBatch(t *testing.T) {
	const count = sessionReadBatchMax + 44
	var mu sync.Mutex
	var sizes []int
	s := &ShardGroup{
		readLane: make(map[uint64]*sessionReadLane),
		readRemote: func(_ uint64, ids []string) ([]sessionReadResult, error) {
			mu.Lock()
			sizes = append(sizes, len(ids))
			mu.Unlock()
			return make([]sessionReadResult, len(ids)), nil
		},
	}
	lane := &sessionReadLane{running: true}
	requests := make([]*sessionReadRequest, count)
	for i := range requests {
		requests[i] = &sessionReadRequest{
			clientID: fmt.Sprintf("device-%d", i), done: make(chan sessionReadResult, 1),
		}
		lane.pending = append(lane.pending, requests[i])
	}
	s.readLane[9] = lane
	s.readWG.Add(1)
	go s.runSessionReadLane(9, lane)
	for _, req := range requests {
		<-req.done
	}
	s.readWG.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 2 || sizes[0] != sessionReadBatchMax || sizes[1] != 44 {
		t.Fatalf("批大小 = %v", sizes)
	}
}

func TestClaimedSessionCacheIsEpochScopedAndOneShot(t *testing.T) {
	s := &ShardGroup{}
	s.cacheClaimedSession("device", 7, sessionReadResult{data: []byte("snapshot"), found: true})
	if _, _, cached, _ := s.TakeClaimedSession("device", 8); cached {
		t.Fatal("不同 epoch 误取了 Claim 会话快照")
	}
	raw, found, cached, err := s.TakeClaimedSession("device", 7)
	if err != nil || !cached || !found || string(raw) != "snapshot" {
		t.Fatalf("Claim 快照 = %q found=%v cached=%v err=%v", raw, found, cached, err)
	}
	if _, _, cached, _ := s.TakeClaimedSession("device", 7); cached {
		t.Fatal("Claim 会话快照被重复消费")
	}

	// “明确不存在”也必须缓存；它正是新设备建连时省掉远程读的常见值。
	s.cacheClaimedSession("new-device", 9, sessionReadResult{})
	if _, found, cached, err := s.TakeClaimedSession("new-device", 9); err != nil || !cached || found {
		t.Fatalf("缺失快照 = found=%v cached=%v err=%v", found, cached, err)
	}
	stats := s.Stats()
	if stats.SessionReadLogical != 2 || stats.SessionReadClaimed != 2 || stats.SessionReadBatches != 0 {
		t.Fatalf("Claim 预读指标 = %+v", stats)
	}
}
