package metrics

import (
	"math"
	"sync/atomic"
)

// Histogram 是固定分桶的直方图。
//
// 为什么是固定桶而不是可配置：桶边界一旦随配置变化，不同节点的
// 直方图就无法相加，而 Prometheus 侧的聚合恰恰依赖边界一致。
// 边界定义在代码里，跨节点必然相同。
//
// 为什么不用互斥锁：观测发生在热路径上（每次握手、每次路由），
// 而桶数固定，用原子加即可。代价是 Snapshot 读到的各桶可能来自
// 略不同的瞬间——对分位数估计无影响，它本来就是估计。
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64
	// sum 以整数微秒累加而非浮点累加。
	//
	// 浮点原子加需要 CAS 循环，而且长时间累加会丢精度：观测值达到
	// 千万次之后，一个几毫秒的新样本加到一个很大的 float64 上会被
	// 直接吃掉。整数累加没有这个问题。
	sumMicros atomic.Int64
	count     atomic.Uint64
}

// Snapshot 是直方图在某一瞬间的读数。
type Snapshot struct {
	// Bounds 是各桶上界，与 Counts 等长。
	Bounds []float64
	// Counts 是落入各桶的数量，**非累积**。编码时才累加成 le 语义。
	Counts []uint64
	// Sum 是全部观测值之和，单位与观测值一致。
	Sum float64
	// Count 是观测总数，等于 Counts 之和加上超出最大桶的那些。
	Count uint64
}

// NewHistogram 按给定上界创建直方图。bounds 须升序且不含 +Inf。
func NewHistogram(bounds ...float64) *Histogram {
	return &Histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds))}
}

// Observe 记录一个观测值，单位毫秒。
func (h *Histogram) Observe(ms float64) {
	if math.IsNaN(ms) {
		return
	}
	h.count.Add(1)
	h.sumMicros.Add(int64(ms * 1000))
	// 桶数很少（约十个），线性查找比二分更快且无分支预测失败。
	for i, b := range h.bounds {
		if ms <= b {
			h.counts[i].Add(1)
			return
		}
	}
	// 超出最大桶：只计入总数，由 +Inf 桶体现。
}

// Snapshot 读取当前状态。
func (h *Histogram) Snapshot() Snapshot {
	s := Snapshot{
		Bounds: h.bounds,
		Counts: make([]uint64, len(h.counts)),
		Count:  h.count.Load(),
		Sum:    float64(h.sumMicros.Load()) / 1000,
	}
	for i := range h.counts {
		s.Counts[i] = h.counts[i].Load()
	}
	return s
}

// LatencyBuckets 是耗时类指标的默认桶，单位毫秒。
//
// 覆盖从亚毫秒到十秒：TLS 握手在局域网内是个位毫秒，而一次卡住的
// 握手可以到秒级，两端都要能看见。
func LatencyBuckets() []float64 {
	return []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000}
}
