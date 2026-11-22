package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/session"
)

// TestSessionApplyRate 量**存储层**的 apply 吞吐：逐条对整批。
//
// 它要回答的是 CLU-125 改完之后冒出来的那个问题：dragonboat 交给 Update
// 的批平均 16.8 条，整批一次事务本该把 fsync 砍掉十几倍，而端到端的建连
// 只涨了两成。那么瓶颈到底在不在这一层？
//
// 这个用例把 MQTT 接入与共识全部摘掉，只留 bbolt：N 个协程模拟 N 个分片
// 的 apply 循环，一边逐条调 ApplySessionCmd，一边整批调 ApplySessionBatch。
// 两个数之比就是这一层能给的全部收益。
//
// 不进门禁：它要落真盘，结果随机器变化（与 TST-127 同一条理由）。
func TestSessionApplyRate(t *testing.T) {
	if os.Getenv("OPENIOT_APPLY_RATE") == "" {
		t.Skip("性能探针，不进门禁：设 OPENIOT_APPLY_RATE=1 再跑")
	}
	const (
		shards  = 8  // 模拟分片数，也就是并发 apply 的协程数
		batchSz = 17 // 实测 dragonboat 给的批平均 16.8 条
		window  = 3 * time.Second
	)

	measure := func(t *testing.T, name string, run func(s *SessionStore, lane, seq int) int) {
		t.Helper()
		s := NewSessionStore(openTestKV(t))
		var done atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for lane := 0; lane < shards; lane++ {
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
		t.Logf("%s：%.0f 条/秒（%d 条 / %.2f 秒）", name, float64(done.Load())/el,
			done.Load(), el)
	}

	// 两边写的是同一种命令：建一个会话。会话记录是 CONNECT 真正要落的
	// 那条，用别的命令量出来的数没有可比性。
	cmdFor := func(lane, seq int) []byte {
		id := fmt.Sprintf("lane-%d-dev-%d", lane, seq)
		data, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: id,
			State: &session.State{ClientID: id, Epoch: 1, ExpiryInterval: 3600},
		})
		if err != nil {
			t.Fatalf("编码失败: %v", err)
		}
		return data
	}
	owns := func(string) bool { return true }

	t.Run("逐条", func(t *testing.T) {
		measure(t, "逐条 ApplySessionCmd", func(s *SessionStore, lane, seq int) int {
			if _, err := s.ApplySessionCmd(uint64(16+lane), uint64(seq),
				cmdFor(lane, seq), owns); err != nil {
				t.Errorf("apply 失败: %v", err)
				return 0
			}
			return 1
		})
	})

	t.Run("整批", func(t *testing.T) {
		measure(t, "整批 ApplySessionBatch", func(s *SessionStore, lane, seq int) int {
			ents := make([]SessionEntry, batchSz)
			for i := range ents {
				ents[i] = SessionEntry{
					Index: uint64(seq*batchSz + i),
					Data:  cmdFor(lane, seq*batchSz+i),
				}
			}
			if err := s.ApplySessionBatch(uint64(16+lane), ents, owns); err != nil {
				t.Errorf("整批 apply 失败: %v", err)
				return 0
			}
			return len(ents)
		})
	})

	// 另外量一次「同样的写入量，但只有一个事务」：它是这一层的上限，
	// 用来判断整批那一档离上限还有多远。
	t.Run("单事务裸写", func(t *testing.T) {
		kv := openTestKV(t)
		var done atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for lane := 0; lane < shards; lane++ {
			wg.Add(1)
			go func(lane int) {
				defer wg.Done()
				for seq := 1; ; seq++ {
					select {
					case <-stop:
						return
					default:
					}
					err := kv.InTx(false, func(tx Tx) error {
						for i := 0; i < batchSz; i++ {
							key := []byte(fmt.Sprintf("x-%d-%d-%d", lane, seq, i))
							if serr := tx.Set(key, make([]byte, 128)); serr != nil {
								return serr
							}
						}
						return nil
					})
					if err != nil {
						t.Errorf("裸写失败: %v", err)
						return
					}
					done.Add(batchSz)
				}
			}(lane)
		}
		t0 := time.Now()
		time.Sleep(window)
		close(stop)
		wg.Wait()
		el := time.Since(t0).Seconds()
		t.Logf("单事务裸写（不读、不编解码）：%.0f 条/秒", float64(done.Load())/el)
	})
}
