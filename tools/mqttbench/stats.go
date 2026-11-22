// 测量窗口内的计数与延迟分布。

package main

import (
	"math"
	"sync"
	"sync/atomic"
)

// ---------- 统计 ----------

// stats 用对数桶直方图而不是存下每个样本：一轮 20 秒的测量有几百万个样本，
// 存下来要占几十 MB，而那部分内存与 GC 压力都会算到压测器自己头上。
type stats struct {
	published atomic.Int64
	delivered atomic.Int64
	acked     atomic.Int64
	errs      atomic.Int64

	mu      sync.Mutex
	buckets [nbuckets]uint64
	maxMs   float64
}

const (
	nbuckets = 600
	// 1.05 的对数桶：相邻桶相差 5%，对 p99 这个用途足够，而桶数仍然很小。
	growth = 1.05
)

func newStats() *stats { return &stats{} }

func (s *stats) observe(ms float64) {
	if ms < 0 {
		return
	}
	i := 0
	if ms > 0 {
		i = int(math.Log(ms*1000+1) / math.Log(growth))
		if i < 0 {
			i = 0
		}
		if i >= nbuckets {
			i = nbuckets - 1
		}
	}
	s.mu.Lock()
	s.buckets[i]++
	if ms > s.maxMs {
		s.maxMs = ms
	}
	s.mu.Unlock()
}

func (s *stats) reset() {
	s.published.Store(0)
	s.delivered.Store(0)
	s.acked.Store(0)
	s.errs.Store(0)
	s.mu.Lock()
	s.buckets = [nbuckets]uint64{}
	s.maxMs = 0
	s.mu.Unlock()
}

func (s *stats) snapshot() (pub, del, ack, errs int64) {
	return s.published.Load(), s.delivered.Load(), s.acked.Load(), s.errs.Load()
}

func (s *stats) percentiles() (p50, p99, pmax float64) {
	s.mu.Lock()
	b := s.buckets
	pmax = s.maxMs
	s.mu.Unlock()
	var total uint64
	for _, c := range b {
		total += c
	}
	if total == 0 {
		return 0, 0, pmax
	}
	at := func(q float64) float64 {
		want := uint64(float64(total) * q)
		var seen uint64
		for i, c := range b {
			seen += c
			if seen >= want {
				return (math.Pow(growth, float64(i)) - 1) / 1000
			}
		}
		return pmax
	}
	return at(0.50), at(0.99), pmax
}
