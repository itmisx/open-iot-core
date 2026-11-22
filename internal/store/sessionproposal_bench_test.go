package store

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

// proposalBenchSink 阻止编译器把等待结果的整条路径消掉。
var proposalBenchSink atomic.Uint64

type immediateSessionProposer struct {
	physical atomic.Uint64
}

func BenchmarkMergeSessionProposals64(b *testing.B) {
	items := make([][]byte, 64)
	for i := range items {
		id := fmt.Sprintf("merge-%04d", i)
		subs := make([]session.Subscription, 8)
		for j := range subs {
			subs[j] = session.Subscription{Filter: fmt.Sprintf("devices/%s/%d", id, j), QoS: 1}
		}
		raw, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpSave, ClientID: id,
			State: &session.State{ClientID: id, Epoch: cluster.Epoch(i + 1), Subscriptions: subs},
		})
		if err != nil {
			b.Fatal(err)
		}
		items[i] = raw
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged, widths, consumed, err := MergeSessionProposals(items)
		if err != nil || consumed != 64 || len(widths) != 64 {
			b.Fatalf("merged=%d widths=%d consumed=%d err=%v", len(merged), len(widths), consumed, err)
		}
		proposalBenchSink.Store(uint64(len(merged)))
	}
}

func (*immediateSessionProposer) ShardOf(string) uint64 { return 16 }
func (*immediateSessionProposer) ShardIDs() []uint64    { return []uint64{16} }
func (p *immediateSessionProposer) Propose(string, []byte) (uint64, error) {
	p.physical.Add(1)
	return 0, nil
}
func (p *immediateSessionProposer) ProposeShard(uint64, []byte) (uint64, error) {
	p.physical.Add(1)
	return 0, nil
}

// BenchmarkSessionProposalQueue 只量 Access 侧合批编排：不含 JSON 编码、
// bbolt 与 Raft。它用来发现重连风暴下的 request/channel/slice 分配，
// 不能当作 CONNECT/s 成绩。
func BenchmarkSessionProposalQueue(b *testing.B) {
	ids := make([]string, 4096)
	for i := range ids {
		ids[i] = fmt.Sprintf("bench-%04d", i)
	}
	prop := &immediateSessionProposer{}
	r := NewReplicatedSessionStore(NewSessionStore(NewMemKV()), prop)
	data := []byte(`{"v":1,"o":1,"c":"bench"}`)
	var next atomic.Uint64
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := next.Add(1)
			if err := r.propose(ids[i&4095], data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	proposalBenchSink.Store(prop.physical.Load())
	b.ReportMetric(float64(prop.physical.Load())/float64(b.N), "physical/logical")
}
