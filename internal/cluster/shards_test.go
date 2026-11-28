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

func (f *shardFSM) entries(shard uint64) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.byShard[shard]...)
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

// TestShardLeaderFailureKeepsCommittedOrder 是 SHDW-062 的共识层故障门禁：
// 真正停掉当前 leader 的副本（不是礼貌的 leader transfer），等剩余多数派
// 自动选出新 leader，再写第二个版本。两个存活状态机必须保留故障前已确认的
// 前缀并严格追加，不能从空状态重新开始或把版本顺序倒回去。
func TestShardLeaderFailureKeepsCommittedOrder(t *testing.T) {
	ids := []NodeID{"a", "b", "c"}
	groups, addrs := newTLSCluster(t, ids)
	allGroups := make([]*TLSGroup, 0, len(ids))
	for _, id := range ids {
		allGroups = append(allGroups, groups[id])
	}
	_ = waitLeader(t, allGroups, 10*time.Second)

	replicas := replicasOf(addrs)
	fsms := make(map[NodeID]*shardFSM, len(ids))
	shards := make(map[NodeID]*ShardGroup, len(ids))
	const base = uint64(50)
	for _, id := range ids {
		id := id
		fsm := newShardFSM()
		sg, err := groups[id].StartShardGroup(ShardOptions{
			Base: base, Count: 1, LayoutName: "failover-shadow-layout.json",
			Label: "影子故障门禁", Replicas: replicas, Peers: addrs,
			Bootstrap: id == "a", Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
			SnapshotTo:   func(uint64, io.Writer) error { return nil },
			RestoreFrom:  func(uint64, io.Reader) error { return nil },
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("节点 %s 起影子故障门禁分片: %v", id, err)
		}
		fsms[id], shards[id] = fsm, sg
		t.Cleanup(func() { _ = sg.Close() })
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, id := range ids {
			ready = ready && shards[id].Ready()
		}
		if ready {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range ids {
		if !shards[id].Ready() {
			t.Fatalf("节点 %s 的影子故障门禁分片未就绪", id)
		}
	}
	leaders := 0
	for _, id := range ids {
		view := shards[id].LocalLeadership()
		if len(view) != 1 || !view[0].Ready || view[0].Leader == "" {
			t.Fatalf("节点 %s 的本地leader视图=%+v", id, view)
		}
		if view[0].IsLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("故障前报告leader的副本数=%d，期望1", leaders)
	}

	if _, err := shards["a"].ProposeShard(base, []byte("version-1")); err != nil {
		t.Fatalf("故障前写入: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fsms["a"].count(base) == 1 && fsms["b"].count(base) == 1 &&
			fsms["c"].count(base) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range ids {
		if got := fsms[id].entries(base); len(got) != 1 || got[0] != "version-1" {
			t.Fatalf("节点 %s 故障前状态=%v", id, got)
		}
		view := shards[id].LocalLeadership()
		if len(view) != 1 || view[0].AppliedIndex == 0 {
			t.Fatalf("节点 %s 没有暴露已持久化applied index: %+v", id, view)
		}
	}

	leaderReplica, _, ok, err := shards["a"].nh.GetLeaderID(base)
	if err != nil || !ok {
		t.Fatalf("读取待故障 leader: replica=%d ready=%v err=%v", leaderReplica, ok, err)
	}
	leaderID := shards["a"].replicaNode[leaderReplica]
	if leaderID == "" {
		t.Fatalf("leader replica %d 没有节点映射", leaderReplica)
	}
	failureAt := time.Now()
	if err := shards[leaderID].nh.StopReplica(base, leaderReplica); err != nil {
		t.Fatalf("停止 leader %s/%d: %v", leaderID, leaderReplica, err)
	}

	survivors := make([]NodeID, 0, 2)
	for _, id := range ids {
		if id != leaderID {
			survivors = append(survivors, id)
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	newLeader := uint64(0)
	for time.Now().Before(deadline) {
		leader, _, ready, leaderErr := shards[survivors[0]].nh.GetLeaderID(base)
		if leaderErr == nil && ready && leader != 0 && leader != leaderReplica {
			newLeader = leader
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == 0 {
		t.Fatalf("停止 leader %s 后剩余多数派未在5秒内选出新 leader", leaderID)
	}
	failoverDuration := time.Since(failureAt)
	leaders = 0
	for _, id := range survivors {
		view := shards[id].LocalLeadership()
		if len(view) != 1 || !view[0].Ready || view[0].Leader == "" {
			t.Fatalf("存活节点 %s 的故障后leader视图=%+v", id, view)
		}
		if view[0].IsLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("故障后存活副本报告leader的数量=%d，期望1", leaders)
	}

	if _, err := shards[survivors[0]].ProposeShard(base, []byte("version-2")); err != nil {
		t.Fatalf("切换 leader 后写入: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fsms[survivors[0]].count(base) == 2 && fsms[survivors[1]].count(base) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range survivors {
		got := fsms[id].entries(base)
		if len(got) != 2 || got[0] != "version-1" || got[1] != "version-2" {
			t.Fatalf("存活节点 %s 在 leader 故障后的提交序列=%v", id, got)
		}
		if fsms[id].appliedIndex(base) < 2 {
			t.Fatalf("存活节点 %s applied index 回退到 %d", id, fsms[id].appliedIndex(base))
		}
	}
	t.Logf("停止 leader %s 后由 replica %d 接任，恢复写入耗时 %s",
		leaderID, newLeader, failoverDuration)
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

	// leader 合批队列取请求时若任期已经切走，它尚未调用 Dragonboat，
	// 必须返回“确定未发送”。上层只有凭这个错误才可安全等新 leader
	// 后重路由；若压成普通 NotLeader，就只能把一整批 CONNECT 拒掉。
	if _, _, err := shards[followerID].syncProposeIfLeader(shardSessionBase,
		[]byte("known-unsent")); !errors.Is(err, ErrRequestUnsent) {
		t.Fatalf("follower提交前复查返回 %v，期望 ErrRequestUnsent", err)
	}

	// 故意把请求送给 follower；接收端必须就地拒绝，不能
	// 又转回 leader 形成第二跳。
	leaderAccepted := groups[leaderID].ownAccepted.Load()
	// 接收端的入口复查同样发生在 SyncPropose 之前，所以它回的也必须是
	// “确定未发送”——这是换届期最常见的一种，压成 ErrNotLeader 就与
	// “提交了但应答丢了”混在一起，发起方只能整批拒掉。
	if _, err := groups[leaderID].forwardShard(followerID, shardSessionBase, []byte("wrong-target")); !errors.Is(err, ErrRequestUnsent) {
		t.Fatalf("非 leader 接收端返回 %v，期望 ErrRequestUnsent", err)
	}
	if got := groups[leaderID].ownAccepted.Load(); got != leaderAccepted {
		t.Fatalf("非 leader 把提案又转回 leader：入站 %d -> %d", leaderAccepted, got)
	}
}

// TestShardStatsExposeSafeReroutes 只验读数搬运，不验触发路径；
// 触发侧的判据在 TestSafeRerouteAcceptsReelectedSelfReplica。
func TestShardStatsExposeSafeReroutes(t *testing.T) {
	var g ShardGroup
	g.stats.safeRerouteLocal.Add(2)
	g.stats.safeRerouteRemote.Add(3)
	st := g.Stats()
	if st.SafeRerouteLocal != 2 || st.SafeRerouteRemote != 3 {
		t.Fatalf("安全重路由统计 = local:%d remote:%d，期望 2/3",
			st.SafeRerouteLocal, st.SafeRerouteRemote)
	}
}

// TestSafeRerouteAcceptsReelectedSelfReplica 钉 CLU-191 等待侧的两条判据：
//
//   - 一条“可证明未发送”的命令重路由后必须**恰好提交一次**；
//   - **换届的结果可能是本副本再次当选**，这时必须立刻收下。提交前复查
//     失败最常见的形态是“此刻无 leader”而不是“已经换人”，只认“leader
//     换人”就会在这一支上等一个不会发生的事件，挂满 applyTimeout
//     （生产默认 10 秒）才失败——比不重路由的立刻失败更糟，而这条
//     CONNECT 本来是可以成功的。
//
// 这个形状三进程验收与单机都碰不到：它要求换届恰好落在合批入队与出队
// 之间，所以判据只能写在这一层。
func TestSafeRerouteAcceptsReelectedSelfReplica(t *testing.T) {
	groups, addrs := newTLSCluster(t, []NodeID{"a"})
	_ = waitLeader(t, []*TLSGroup{groups["a"]}, 10*time.Second)

	fsm := newShardFSM()
	sg, err := groups["a"].StartSessionShards(ShardOptions{
		Count: 1, Replicas: replicasOf(addrs), Peers: addrs, Bootstrap: true,
		Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
		SnapshotTo:  func(uint64, io.Writer) error { return nil },
		RestoreFrom: func(uint64, io.Reader) error { return nil },
		// 取小值：判据之一是“没有白等”，而默认的 10 秒会让这条用例
		// 在回归时慢到被当成卡死。
		ApplyTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("起分片失败: %v", err)
	}
	t.Cleanup(func() { _ = sg.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !sg.Ready() {
		time.Sleep(20 * time.Millisecond)
	}
	if !sg.Ready() {
		t.Fatal("10 秒内会话分片没有选出 leader")
	}

	// oldLeader 取本副本号，模拟“入队时是 leader、出队前复查失败、
	// 随后又是自己当选”。
	start := time.Now()
	if _, err := sg.proposeAfterLeaderChange(shardSessionBase, sg.replica,
		[]byte("reelected-self")); err != nil {
		t.Fatalf("本副本再次当选后重路由: %v", err)
	}
	if waited := time.Since(start); waited > sg.applyTO/2 {
		t.Fatalf("等了 %v 才提交；对“本副本再次当选”必须立刻收下，"+
			"否则一条可证明未发送的 CONNECT 会白挂满 applyTimeout", waited)
	}
	if n := fsm.count(shardSessionBase); n != 1 {
		t.Fatalf("提交条数 = %d，期望恰好 1；重路由只许补发未发送的那一条", n)
	}

	// 确定未发送的错误仍必须严格区分于歧义错误：提交前复查给出的是
	// ErrRequestUnsent，调用方据此才敢重路由。
	if _, _, err := sg.syncProposeIfLeader(shardSessionBase+1,
		[]byte("not-a-local-shard")); !errors.Is(err, ErrRequestUnsent) {
		t.Fatalf("非本地分片的提交前复查返回 %v，期望 ErrRequestUnsent", err)
	}
	if n := fsm.count(shardSessionBase); n != 1 {
		t.Fatalf("复查失败后仍提交了日志：条数 = %d", n)
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

	// 本地副本的leader判断必须越过旧缓存读取NodeHost当前任期。否则本节点
	// 曾任leader后换届，影子路由会误走本地Apply并等待follower追平。
	local := shards[NodeID("a")]
	localThing := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("local-leader-cache-%d", i)
		if local.HasLocalReplica(candidate) {
			localThing = candidate
			break
		}
	}
	if localThing == "" {
		t.Fatal("没有找到a承载的测试thing")
	}
	localShard := local.ShardOf(localThing)
	actualReplica, _, ready, err := local.nh.GetLeaderID(localShard)
	if err != nil || !ready {
		t.Fatalf("读取本地分片leader: replica=%d ready=%v err=%v", actualReplica, ready, err)
	}
	for _, stale := range local.placement.members(localShard) {
		if stale != actualReplica {
			local.rememberLeader(localShard, stale)
			break
		}
	}
	if got, ok := local.LeaderNode(localThing); !ok || got != local.replicaNode[actualReplica] {
		t.Fatalf("本地副本使用了陈旧leader缓存: got=%q ok=%v want=%q", got, ok,
			local.replicaNode[actualReplica])
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
	// 非副本 Access 的冷路径必须通过状态探针解析真实 leader；第二次调用
	// 应命中缓存。生产影子 RF=3 的正常写路由依赖的正是这条路径。
	resolvedLeader, ok := shards[access].LeaderNode(clientID)
	if !ok || resolvedLeader == "" {
		t.Fatalf("非副本Access无法解析shard=%d leader: node=%q ok=%v", shard, resolvedLeader, ok)
	}
	if cached, ok := shards[access].LeaderNode(clientID); !ok || cached != resolvedLeader {
		t.Fatalf("leader缓存=%q ok=%v，期望%q", cached, ok, resolvedLeader)
	}
	shards[access].ForgetLeader(clientID, resolvedLeader)
	if refreshed, ok := shards[access].LeaderNode(clientID); !ok || refreshed == "" {
		t.Fatalf("leader缓存失效后无法重新解析: node=%q ok=%v", refreshed, ok)
	}
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

// TestSliceResultBitsCoversBeyond64 钉 CLU-193 的切位逻辑。
//
// 两条判据：位图非空时第 64 位之后仍切得出来（旧实现是一个 uint64，
// 那些位根本不存在），以及位图为空时退回状态机那个 uint64——不填
// ResultBits 的状态机（元数据组、影子）行为必须不变。
func TestSliceResultBitsCoversBeyond64(t *testing.T) {
	bits := make([]byte, 16) // 128 位
	for _, at := range []uint{0, 63, 64, 70, 127} {
		bits[at/8] |= 1 << (at % 8)
	}
	if got := sliceResultBits(0, bits, 64, 8); got != 0b0100_0001 {
		t.Fatalf("第 64 位起 8 位 = %08b，期望 01000001", got)
	}
	if got := sliceResultBits(0, bits, 120, 8); got != 0b1000_0000 {
		t.Fatalf("第 120 位起 8 位 = %08b，期望 10000000", got)
	}
	if got := sliceResultBits(0, bits, 1, 8); got != 0 {
		t.Fatalf("第 1 位起 8 位 = %08b，期望全 0", got)
	}
	// 位图越界时按 0 读，不得 panic：状态机回少了位的那条路径由调用方
	// 单独报错，切位本身不能先崩。
	if got := sliceResultBits(0, bits, 1000, 8); got != 0 {
		t.Fatalf("越界读 = %d，期望 0", got)
	}
	// 位图为空：退回 uint64。
	if got := sliceResultBits(0b1011, nil, 1, 3); got != 0b101 {
		t.Fatalf("回退到 uint64 的第 1 位起 3 位 = %03b，期望 101", got)
	}
	if got := sliceResultBits(42, nil, 0, 64); got != 42 {
		t.Fatalf("整宽回退 = %d，期望 42", got)
	}
	if !widthsFitUint64([]uint8{16, 16, 64}) || widthsFitUint64([]uint8{65}) {
		t.Fatal("单请求位宽判定错误：每个请求自己那一段必须装得进 uint64")
	}
}
