package meta

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/store"
)

// TestLocalApplyRate 量单机元数据写入这一层的上限，并与整批提交对照。
//
// 为什么要有它：影子写入在单节点上的端到端吞吐**不随并发上升**（实测并发
// 1 到 100 都是 111 到 119 条/秒，延迟线性增长），而同一份代码在三节点下会
// 随并发涨到好几百。两边差的不是共识，是单机这条路径——`LocalStore.Apply`
// 全程持 s.mu，而那把锁里包着一次同步落盘事务，于是并发请求之间一条也合
// 不了批。
//
// 这个探针把 MQTT 接入、授权与共识全部摘掉，只留 bbolt：
//
//   - 「并发 Apply」是现状：N 个协程各自调 Apply。
//   - 「整批 ApplyCommittedBatch」是上限：同样 N 条命令一次事务提交，与
//     集群路径（CLU-127）走的是同一个入口。
//
// 两个数之比就是这一层还能给的全部收益。不进门禁：它要落真盘，结果随机器
// 变化（与 TST-127、TST-170 同一条理由）。
//
//	OPENIOT_META_APPLY_RATE=1 go test ./internal/meta/ -run TestLocalApplyRate -v
func TestLocalApplyRate(t *testing.T) {
	if os.Getenv("OPENIOT_META_APPLY_RATE") == "" {
		t.Skip("性能探针，不进门禁：设 OPENIOT_META_APPLY_RATE=1 再跑")
	}
	const (
		conc    = 16 // 并发协程数，模拟并发的影子更新
		batchSz = 16 // 与集群下 dragonboat 交给状态机的批大小同量级
		window  = 3 * time.Second
	)

	// newStore 造一个落真盘的元数据存储：冷层与事务都接到 bbolt 上，
	// 这样 Apply 里那次提交是真的 fsync，而不是内存写。
	newStore := func(t *testing.T) *LocalStore {
		t.Helper()
		kv, err := store.Open(store.Options{Dir: t.TempDir()})
		if err != nil {
			t.Fatalf("打开存储: %v", err)
		}
		t.Cleanup(func() { _ = kv.Close() })
		s := NewLocalStore(LocalOptions{
			Now:         func() int64 { return 1_700_000_000_000 },
			Compile:     stubCompile,
			Cold:        kv,
			RevisionKey: store.Key(store.PrefixSystem, "probe-revision"),
			InTransaction: func(fn func(Transaction) error) error {
				return kv.InTx(true, func(tx store.Tx) error { return fn(tx) })
			},
		})
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	// 每条命令都带一条冷层记录：影子、建设备、签证书都是这个形状，而
	// 那条记录正是必须跟着同一条日志落盘的东西（CLU-065）。
	cmd := func(lane, seq int) Command {
		key := store.Key(store.PrefixSystem, "probe-"+string(rune('a'+lane%26)))
		return Command{
			Kind:    CmdNop,
			Records: []RecordOp{{Key: key, Value: []byte{byte(seq)}}},
		}
	}

	measure := func(t *testing.T, name string, run func(s *LocalStore, lane, seq int) int) float64 {
		t.Helper()
		s := newStore(t)
		var done atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for lane := 0; lane < conc; lane++ {
			wg.Add(1)
			go func(lane int) {
				defer wg.Done()
				for seq := 1; ; seq++ {
					select {
					case <-stop:
						return
					default:
					}
					done.Add(int64(run(s, lane, seq)))
				}
			}(lane)
		}
		t0 := time.Now()
		time.Sleep(window)
		close(stop)
		wg.Wait()
		el := time.Since(t0).Seconds()
		rate := float64(done.Load()) / el
		t.Logf("%s：%.0f 条/秒（%d 条 / %.2f 秒）", name, rate, done.Load(), el)
		return rate
	}

	ctx := context.Background()
	single := measure(t, "并发 Apply（现状）", func(s *LocalStore, lane, seq int) int {
		if _, err := s.Apply(ctx, cmd(lane, seq)); err != nil {
			return 0
		}
		return 1
	})

	// 整批那一臂只在一个协程上跑：批内已经没有并发可言，多协程只会在
	// s.mu 上重新排队，量出来的就不是「整批能跑多快」了。
	var rev uint64
	batched := measure(t, "整批 ApplyCommittedBatch（上限）", func(s *LocalStore, lane, seq int) int {
		if lane != 0 {
			time.Sleep(time.Millisecond)
			return 0
		}
		ents := make([]CommittedEntry, 0, batchSz)
		for i := 0; i < batchSz; i++ {
			rev++
			ents = append(ents, CommittedEntry{
				Revision: Revision(rev), Command: cmd(i, seq),
			})
		}
		if err := s.ApplyCommittedBatch(ents); err != nil {
			t.Errorf("整批应用: %v", err)
			return 0
		}
		return len(ents)
	})

	if single <= 0 || batched <= 0 {
		t.Fatalf("两臂都要有数：%f / %f", single, batched)
	}
	t.Logf("整批 / 并发 = %.1f 倍——这是单机这一层还能给的收益上限", batched/single)
}
