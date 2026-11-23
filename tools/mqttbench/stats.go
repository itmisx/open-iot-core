// 测量窗口内的计数与延迟分布。

package main

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- 统计 ----------

// stats 用对数桶直方图而不是存下每个样本：一轮 20 秒的测量有几百万个样本，
// 存下来要占几十 MB，而那部分内存与 GC 压力都会算到压测器自己头上。
type stats struct {
	published atomic.Int64
	delivered atomic.Int64
	acked     atomic.Int64
	errs      atomic.Int64
	// 影子故障轮把明确rejected与5秒无响应分开。两者都计入errs，但不能
	// 混成同一种失败：前者证明服务端明确拒绝，后者可能是模糊提交结果。
	shadowRejected     atomic.Int64
	shadowTimeouts     atomic.Int64
	shadowStale        atomic.Int64
	shadowTimeoutMinNs atomic.Int64
	shadowTimeoutMaxNs atomic.Int64
	shadowLastOKNs     atomic.Int64
	shadowMaxGapNs     atomic.Int64

	// shadowGen separates a warm-up request that may still be waiting for its
	// accepted reply when the measurement window starts. Atomic counters alone
	// cannot do that: reset can land between send and accepted and make the reply
	// look like a measured completion without a matching measured publish.
	shadowMu  sync.RWMutex
	shadowGen uint64

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
	s.shadowMu.Lock()
	defer s.shadowMu.Unlock()
	s.published.Store(0)
	s.delivered.Store(0)
	s.acked.Store(0)
	s.errs.Store(0)
	s.shadowRejected.Store(0)
	s.shadowTimeouts.Store(0)
	s.shadowStale.Store(0)
	s.shadowTimeoutMinNs.Store(0)
	s.shadowTimeoutMaxNs.Store(0)
	s.shadowLastOKNs.Store(0)
	s.shadowMaxGapNs.Store(0)
	s.mu.Lock()
	s.buckets = [nbuckets]uint64{}
	s.maxMs = 0
	s.mu.Unlock()
	s.shadowGen++
}

// shadowGeneration returns the measurement generation for a new request.
// Generation zero is warm-up and is deliberately never counted.
func (s *stats) shadowGeneration() uint64 {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	return s.shadowGen
}

func (s *stats) shadowPublished(gen uint64) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.published.Add(1)
	}
}

func (s *stats) shadowDelivered(gen uint64, ms float64) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.observe(ms)
		s.delivered.Add(1)
		now := time.Now().UnixNano()
		if previous := s.shadowLastOKNs.Swap(now); previous > 0 && now > previous {
			raiseInt64(&s.shadowMaxGapNs, now-previous)
		}
	}
}

func (s *stats) shadowFailed(gen uint64) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.errs.Add(1)
	}
}

func (s *stats) shadowRejectedReply(gen uint64) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.shadowRejected.Add(1)
		s.errs.Add(1)
	}
}

func (s *stats) shadowTimedOut(gen uint64, elapsed ...time.Duration) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.shadowTimeouts.Add(1)
		s.errs.Add(1)
		if len(elapsed) != 0 {
			ns := elapsed[0].Nanoseconds()
			raiseInt64(&s.shadowTimeoutMaxNs, ns)
			for old := s.shadowTimeoutMinNs.Load(); old == 0 || ns < old; old = s.shadowTimeoutMinNs.Load() {
				if s.shadowTimeoutMinNs.CompareAndSwap(old, ns) {
					break
				}
			}
		}
	}
}

func (s *stats) shadowStaleReply(gen uint64) {
	s.shadowMu.RLock()
	defer s.shadowMu.RUnlock()
	if gen != 0 && gen == s.shadowGen {
		s.shadowStale.Add(1)
	}
}

func (s *stats) shadowFailures() (rejected, timeouts, lastOKNs, maxGapNs int64) {
	return s.shadowRejected.Load(), s.shadowTimeouts.Load(),
		s.shadowLastOKNs.Load(), s.shadowMaxGapNs.Load()
}

func (s *stats) shadowStaleReplies() int64 { return s.shadowStale.Load() }

func (s *stats) shadowTimeoutRange() (minNs, maxNs int64) {
	return s.shadowTimeoutMinNs.Load(), s.shadowTimeoutMaxNs.Load()
}

func raiseInt64(dst *atomic.Int64, value int64) {
	for old := dst.Load(); value > old; old = dst.Load() {
		if dst.CompareAndSwap(old, value) {
			return
		}
	}
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
