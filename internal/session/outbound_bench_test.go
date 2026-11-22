package session

import (
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

type legacyOutboundQueue struct {
	mu  sync.Mutex
	out chan Outbound
}

func (q *legacyOutboundQueue) cycle(o Outbound) {
	q.mu.Lock()
	q.out <- o
	q.mu.Unlock()
	<-q.out
}

func BenchmarkOutboundSingleLegacyChannel(b *testing.B) {
	q := legacyOutboundQueue{out: make(chan Outbound, 32)}
	o := Outbound{Bytes: []byte{1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.cycle(o)
	}
}

func BenchmarkOutboundSingleInlineQueue(b *testing.B) {
	e := cluster.MakeEpoch(1, 1)
	r := NewRuntime(&State{Epoch: e}, nil, 32)
	o := Outbound{Bytes: []byte{1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Enqueue(e, o); err != nil {
			b.Fatal(err)
		}
		if _, ok := r.Dequeue(); !ok {
			b.Fatal("missing outbound")
		}
	}
}
