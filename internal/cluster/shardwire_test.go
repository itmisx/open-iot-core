package cluster

import (
	"io"
	"strings"
	"testing"
	"time"
)

// startTwoNodeShards 起一个进程内两节点集群并在上面开一组会话分片。
//
// 为什么要进程内而不是靠 tests/cluster 的三进程验收：这里要走的是**拒绝与
// 转发**那几条分支——不是 leader 时提案往哪走、非副本节点怎么读、布局不一致
// 时启动要不要失败。三进程验收只回答「通不通」，这几条它碰不到。
func startTwoNodeShards(t *testing.T, count uint64, rf int) (map[NodeID]*ShardGroup, map[NodeID]string) {
	t.Helper()
	groups, addrs := newTLSPair(t)
	_ = waitLeader(t, []*TLSGroup{groups["a"], groups["b"]}, 10*time.Second)
	replicas := replicasOf(addrs)

	shards := map[NodeID]*ShardGroup{}
	for _, id := range []NodeID{"a", "b"} {
		fsm := newResultShardFSM()
		sg, err := groups[id].StartSessionShards(ShardOptions{
			Count: count, Replicas: replicas, Peers: addrs, Bootstrap: id == "a",
			ReplicationFactor: rf,
			Apply:             fsm.apply, AppliedIndex: fsm.appliedIndex,
			SnapshotTo:   func(uint64, io.Writer) error { return nil },
			RestoreFrom:  func(uint64, io.Reader) error { return nil },
			ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("节点 %s 起分片: %v", id, err)
		}
		shards[id] = sg
		t.Cleanup(func() { _ = sg.Close() })
	}
	return shards, addrs
}

// waitShardsReady 等分片选出 leader，并把 leader 推到放置表指定的副本上。
//
// 两步都要做：Raft 自己选谁当 leader 是不确定的，而放置表要的是确定性的
// 目标副本——那一步靠 BalanceLeaders 主动转移（生产里由 internal/server 的
// 后台循环调）。不调它而直接断言 LeadersBalanced 会一直为假。
func waitShardsReady(t *testing.T, groups ...*ShardGroup) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, sg := range groups {
			if _, err := sg.BalanceLeaders(); err != nil {
				all = false
			}
			if !sg.LeadersBalanced() {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("30 秒内 leader 仍未落到放置表指定的副本上")
}

// TestShardGroupWire 在**同一个**两节点集群上验完提案、就绪与拒绝三组语义。
//
// 合成一个用例而不是三个：每个两节点集群都带两套 dragonboat NodeHost 与各自
// 的 LogDB，而同一个测试二进制里多起几套就足以把别的时序敏感用例压垮——
// tls_test.go 的 TestMultiProcessRaft 在 -coverpkg 全量插桩下就这样红过一次。
// 子用例之间有先后依赖（先等 leader 落位才谈得上提案），所以不并行。
func TestShardGroupWire(t *testing.T) {
	shards, _ := startTwoNodeShards(t, 2, 0)
	waitShardsReady(t, shards["a"], shards["b"])

	// 哪个节点都能提案：本节点是 leader 时就地提交，不是时经传输层送到
	// leader。两条路径都要走到，而且 Stats 里的 local/remote 分类必须真的
	// 分开计数——那两个计数器是归因「时间花在本地还是共识」的唯一依据
	// （OBS-148），分类错了归因就反了。
	t.Run("两个节点都能提案", func(t *testing.T) {
		for _, id := range []NodeID{"a", "b"} {
			sg := shards[id]
			shard := sg.ShardOf("dev-" + string(id))
			v, err := sg.ProposeShard(shard, []byte(`{"v":1}`))
			if err != nil {
				t.Fatalf("节点 %s 提案: %v", id, err)
			}
			if v != resultShardValue {
				t.Fatalf("节点 %s 提案返回 %#x，期望状态机的 %#x", id, v, resultShardValue)
			}
		}
		var local, remote, unknown, raft uint64
		for _, id := range []NodeID{"a", "b"} {
			st := shards[id].Stats()
			local += st.ProposeLocal
			remote += st.ProposeRemote
			unknown += st.ProposeUnknown
			raft += st.RaftProposals
		}
		if local+remote+unknown < 2 {
			t.Fatalf("提案分类合计 = %d（local=%d remote=%d unknown=%d）",
				local+remote+unknown, local, remote, unknown)
		}
		if raft == 0 {
			t.Fatal("物理 Raft 提案数为 0：二次合批没有把任何日志提交上去")
		}
	})

	// Barrier 是跨节点读之前的追平点：它必须对本节点承载的分片成立，
	// 否则「写完立刻读」会读到旧值。
	t.Run("就绪与追平", func(t *testing.T) {
		for _, id := range []NodeID{"a", "b"} {
			if err := shards[id].Barrier("dev-1", 5*time.Second); err != nil {
				t.Fatalf("节点 %s Barrier: %v", id, err)
			}
		}
		// 副本因子等于节点数时，每个键都属于本副本。
		if !shards["a"].HasLocalReplicaBytes([]byte("dev-1")) {
			t.Fatal("RF 等于节点数时，HasLocalReplicaBytes 必须为真")
		}
	})

	// 分片号来自对端的线上消息，所以它是不可信输入。越界时必须拒绝而不是
	// 按取模落到某个真实分片上——后者会把一条属于别人的命令写进本节点的
	// 日志。
	t.Run("越界分片被拒而正常分片仍可用", func(t *testing.T) {
		sg := shards["a"]
		if _, err := sg.ProposeShard(sg.shardBase()+99, []byte(`{"v":1}`)); err == nil {
			t.Fatal("越界的分片号必须被拒绝")
		}
		if _, err := sg.ProposeShard(sg.ShardOf("dev-1"), []byte(`{"v":1}`)); err != nil {
			t.Fatalf("越界拒绝之后正常分片也提不进去: %v", err)
		}
	})
}

// TestShardLayoutIsImmutable 钉住布局一经落盘不可更改。
//
// 分片号参与键的归属判定，所以改了分片数等于把一部分键换到别的日志上，而
// 那些键的历史还留在原来的日志里。必须在启动时就失败——放过去之后的现象是
// 「会话明明写过却读不到」，而那时已经没有线索指向配置。
func TestShardLayoutIsImmutable(t *testing.T) {
	groups, addrs := newTLSPair(t)
	_ = waitLeader(t, []*TLSGroup{groups["a"], groups["b"]}, 10*time.Second)
	replicas := replicasOf(addrs)
	fsm := newResultShardFSM()
	opt := ShardOptions{
		Count: 2, Replicas: replicas, Peers: addrs, Bootstrap: true,
		Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
		SnapshotTo:   func(uint64, io.Writer) error { return nil },
		RestoreFrom:  func(uint64, io.Reader) error { return nil },
		ApplyTimeout: 5 * time.Second,
	}
	sg, err := groups["a"].StartSessionShards(opt)
	if err != nil {
		t.Fatalf("首次启动: %v", err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("关闭: %v", err)
	}

	changed := opt
	changed.Count = 4
	if _, err := groups["a"].StartSessionShards(changed); err == nil {
		t.Fatal("分片数变了却启动成功：布局必须不可更改")
	} else if !strings.Contains(err.Error(), "布局不可变") {
		t.Fatalf("错误信息没有指出布局不可变：%v", err)
	}
}

// TestShardGroupRejectsUnknownShard 钉住分片号越界的拒绝。
//
// 分片号来自对端的线上消息，所以它是不可信输入。越界时必须拒绝而不是按
// 取模落到某个真实分片上——后者会把一条属于别人的命令写进本节点的日志。
func TestShardGroupRejectsUnknownShard(t *testing.T) {
	shards, _ := startTwoNodeShards(t, 2, 0)
	sg := shards["a"]
	if _, err := sg.ProposeShard(sg.shardBase()+99, []byte(`{"v":1}`)); err == nil {
		t.Fatal("越界的分片号必须被拒绝")
	}
	// 正常分片必须仍然可提案：上一条拒绝不能是「全都拒绝」。先等 leader
	// 落位，否则这里会把 ErrNotLeader 误读成越界拒绝扩大化了。
	waitShardsReady(t, shards["a"], shards["b"])
	if _, err := sg.ProposeShard(sg.ShardOf("dev-1"), []byte(`{"v":1}`)); err != nil {
		t.Fatalf("越界拒绝之后正常分片也提不进去: %v", err)
	}
}
