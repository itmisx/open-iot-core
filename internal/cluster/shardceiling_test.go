package cluster

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShardProposeCeiling 量「apply 侧不落盘时」会话分片的提交上限。
//
// 它存在的理由是一个具体的决定：CLU-125 要把 apply 侧改成按批提交，而
// 在动手之前必须知道**修完之后的天花板是多少**。修 apply 侧只能把瓶颈
// 让给下一环——dragonboat 自己的 raft 日志 fsync。如果那一环本身也只有
// 一百多条每秒，那 CLU-125 一分钱都不值。
//
// 做法是用同一条路径、同一个分片数、同一个并发度，只把状态机换成内存
// 的那个（shardFSM.apply 不落盘）。于是测出来的数就是「raft 提交 + 库
// 的批量机制」的上限，也就是 apply 侧改完之后能达到的那个数。
//
// 单副本：对照组（tests/bench/connect-attribution.sh 的 1 节点集群那两行）
// 也是单副本，而这里要比的是 apply 侧的差别，不是多数派的代价。
//
// 不进门禁：它要起真的 NodeHost、落真的 raft 日志，而且结果随机器变化
// （TST-127 的同一条理由：不把耗时写进门禁）。设环境变量才跑。
func TestShardProposeCeiling(t *testing.T) {
	if os.Getenv("OPENIOT_SHARD_CEILING") == "" {
		t.Skip("性能探针，不进门禁：设 OPENIOT_SHARD_CEILING=1 再跑")
	}
	const (
		shardCount = 8
		workers    = 256
		window     = 5 * time.Second
	)

	ca, caKey, caPEM := mintCA(t)
	addrs := map[NodeID]string{"node-a": freeLocalAddr(t)}
	cfg := mintNode(t, ca, caKey, caPEM, "node-a", 301)
	g := startTLSGroup(t, "node-a", addrs, cfg, true, newTestFSM())

	deadline := time.Now().Add(20 * time.Second)
	for g.LeaderID() == "" && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if g.LeaderID() == "" {
		t.Fatal("元数据组没有选出 leader")
	}

	// 顺带统计 dragonboat 交给 Update 的**批大小**。
	//
	// 它是 CLU-125 收益的上限：整批一次事务对逐条一次事务，省下的 fsync
	// 次数就是 (批大小 - 1)/批大小。批大小是 1 的话，那条改动一分钱都不值
	// ——而这件事只能量，不能推。
	var batches, entries, maxBatch atomic.Int64
	fsm := newShardFSM()
	shards, err := g.StartSessionShards(ShardOptions{
		Count:     shardCount,
		Replicas:  replicasOf(addrs),
		Peers:     addrs,
		Bootstrap: true,
		ApplyBatch: func(_ uint64, ents []ShardEntry) error {
			batches.Add(1)
			entries.Add(int64(len(ents)))
			for {
				cur := maxBatch.Load()
				if int64(len(ents)) <= cur ||
					maxBatch.CompareAndSwap(cur, int64(len(ents))) {
					break
				}
			}
			for i := range ents {
				if _, err := fsm.apply(0, ents[i].Index, ents[i].Data); err != nil {
					return err
				}
			}
			return nil
		},
		AppliedIndex: fsm.appliedIndex,
		SnapshotTo:   func(uint64, io.Writer) error { return nil },
		RestoreFrom:  func(uint64, io.Reader) error { return nil },
		ApplyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("起分片失败: %v", err)
	}
	t.Cleanup(func() { _ = shards.Close() })
	for deadline := time.Now().Add(20 * time.Second); !shards.Ready() &&
		time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
	}
	if !shards.Ready() {
		t.Fatal("20 秒内分片没有全部就绪")
	}

	// 预热：第一条提交要等选举稳定与日志文件创建，算进窗口会压低结果。
	for i := 0; i < workers; i++ {
		if _, err := shards.Propose(fmt.Sprintf("warm-%d", i), []byte("warm")); err != nil {
			t.Fatalf("预热提交失败: %v", err)
		}
	}

	var ok, failed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 会话记录大约几百字节，用 256 字节的负载而不是空命令：命令大小会
	// 影响批量提交能攒多少条，空命令量出来的数偏乐观。
	payload := make([]byte, 256)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// clientId 要散开：同一个 clientId 会被路由到同一个分片，
				// 全打在一个分片上量的就不是分片组的上限了。
				id := fmt.Sprintf("probe-%d-%d", w, i)
				if _, err := shards.Propose(id, payload); err != nil {
					failed.Add(1)
					return
				}
				ok.Add(1)
			}
		}(w)
	}
	t0 := time.Now()
	time.Sleep(window)
	close(stop)
	wg.Wait()
	el := time.Since(t0).Seconds()

	rate := float64(ok.Load()) / el
	t.Logf("apply 不落盘：%d 分片 × %d 并发，提交 %.0f 条/秒（%d 条 / %.2f 秒，失败 %d）",
		shardCount, workers, rate, ok.Load(), el, failed.Load())
	t.Logf("对照：同样单副本 8 分片、apply 走 bbolt 逐条提交是 187 条/秒" +
		"（tests/bench/connect-attribution.sh）")
	if b := batches.Load(); b > 0 {
		t.Logf("Update 的批大小：平均 %.2f 条，最大 %d 条（%d 批 / %d 条）",
			float64(entries.Load())/float64(b), maxBatch.Load(), b, entries.Load())
	}
	if failed.Load() > 0 {
		t.Errorf("有 %d 个提交失败", failed.Load())
	}
	if ok.Load() == 0 {
		t.Fatal("窗口内没有成功提交")
	}
}
