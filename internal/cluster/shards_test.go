package cluster

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

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
