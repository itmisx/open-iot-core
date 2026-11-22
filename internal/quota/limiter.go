package quota

import (
	"context"
	"sync"
)

// Limiter 是配额判定接口（契约 09）。
//
// 作用域在方法层面分开，而不是作为参数：这样调用点一眼能看出它判的是
// 本地还是全局配额，也让「某个限流点语义上本该全局却只在本地判」
// 这类错误在代码评审中可见。
type Limiter interface {
	// AllowLocal 判定一项本地作用域配额。
	// subject 是作用对象（如 clientId），节点级配额传空串。
	// 零分配。
	AllowLocal(id ID, subject string, n int) bool

	// AllowGlobal 判定一项全局作用域配额。
	//
	// 集群下它基于本地分摊额度，是近似的，必要时才与协调者协商，
	// 因此带 ctx。单机下等同精确判定。
	AllowGlobal(ctx context.Context, id ID, subject string, n int) bool

	// Acquire 占用 n 个计数型配额。失败时不占用任何额度。
	Acquire(id ID, subject string, n int) bool
	// Release 归还计数型配额。
	Release(id ID, subject string, n int)

	// Within 判定一项大小型配额。
	Within(id ID, v int) bool

	// Limit 返回某项配额当前生效的限制值。
	//
	// 它在接口上而不是只在实现上，是因为调用方需要把生效值写入
	// CONNACK 的能力声明与配额拒绝事件——若只取默认值，
	// 运维在告警里看到的数字会和实际生效的不一致。
	Limit(id ID) int

	// Stats 返回各配额的命中与拒绝计数（PRD OBS-029）。
	Stats() []Stat
}

// Stat 是一项配额的统计。
type Stat struct {
	Def
	Allowed  uint64
	Rejected uint64
	// Current 仅对计数型配额有意义。
	Current int64
}

// milliScale 把令牌按千分之一计量，使补充与消耗都用整数运算，
// 避免热路径上的浮点。
const milliScale = 1000

const bucketShards = 64

type bucket struct {
	// tokens 以千分之一令牌计。
	tokens int64
	lastMs int64
}

type bucketShard struct {
	mu sync.Mutex
	// bucket 按值内联，避免每个唯一 clientId 再分配一个独立小对象。
	// 更新时取值、修改、写回；锁已覆盖整个过程。
	m map[bucketKey]bucket
}

type bucketKey struct {
	id      ID
	subject string
}

type counterShard struct {
	mu sync.Mutex
	m  map[bucketKey]int64
}

// Local 是 Limiter 的单节点实现。
//
// 全局配额在此处等同精确判定：节点数为一，分摊即全额。集群实现会替换
// AllowGlobal 的内部逻辑为「本地额度加周期再均衡」，而调用方不变
// （PRD ARCH-032）。
type Local struct {
	defs [numQuotas]Def
	// nowUnixMs 是注入的时钟（契约 08）。令牌桶只用它做补充计算，
	// 不用于比较事件先后。
	nowUnixMs func() int64

	buckets  [bucketShards]bucketShard
	counters [bucketShards]counterShard

	stats [numQuotas]struct {
		allowed  atomicU64
		rejected atomicU64
		current  atomicI64
	}
	// rateBucketCount/swept 使容量与回收效果可观测；不在指标采集时遍历 map。
	rateBucketCount  atomicI64
	rateBucketsSwept atomicU64
	sweepCursor      atomicU64
}

// Options 是 Local 的构造参数。
type Options struct {
	// Now 提供当前时间（Unix 毫秒）。必填。
	Now func() int64
	// Overrides 覆盖默认配额。键为配额编号，值为新的 Limit；
	// Limit 为 0 表示取消该项限制。
	Overrides map[ID]int
}

// NewLocal 创建单节点配额判定器。
func NewLocal(opt Options) *Local {
	l := &Local{nowUnixMs: opt.Now}
	if l.nowUnixMs == nil {
		// 没有时钟时速率配额无法判定，退化为只做计数与大小判定。
		l.nowUnixMs = func() int64 { return 0 }
	}
	l.defs = defaults
	for id, limit := range opt.Overrides {
		if id < numQuotas {
			l.defs[id].Limit = limit
			if l.defs[id].Kind == Rate {
				l.defs[id].Burst = limit
			}
		}
	}
	for i := range l.buckets {
		l.buckets[i].m = make(map[bucketKey]bucket)
		l.counters[i].m = make(map[bucketKey]int64)
	}
	return l
}

func shardOf(id ID, subject string) int {
	h := uint32(id) * 2654435761
	for i := 0; i < len(subject); i++ {
		h = h*31 + uint32(subject[i])
	}
	return int(h % bucketShards)
}

func (l *Local) AllowLocal(id ID, subject string, n int) bool {
	return l.allow(id, subject, n, ScopeLocal)
}

func (l *Local) AllowGlobal(_ context.Context, id ID, subject string, n int) bool {
	// 单节点：分摊即全额，与本地判定等价。
	return l.allow(id, subject, n, ScopeGlobal)
}

// allow 判定一项速率配额。wantScope 用于在开发期捕获作用域用错的调用。
func (l *Local) allow(id ID, subject string, n int, wantScope Scope) bool {
	if id >= numQuotas {
		return false
	}
	d := l.defs[id]
	if d.Scope != wantScope {
		// 作用域用错是编码错误，不是运行时情况：直接拒绝并计数，
		// 使它在压测与指标中立刻可见，而不是悄悄放宽 N 倍。
		l.stats[id].rejected.add(1)
		return false
	}
	if d.Limit <= 0 {
		l.stats[id].allowed.add(1)
		return true
	}
	if d.Kind != Rate {
		// 非速率配额走 Acquire 或 Within。
		l.stats[id].rejected.add(1)
		return false
	}

	burst := d.Burst
	if burst <= 0 {
		burst = d.Limit
	}
	capTokens := int64(burst) * milliScale
	want := int64(n) * milliScale
	now := l.nowUnixMs()

	sh := &l.buckets[shardOf(id, subject)]
	key := bucketKey{id: id, subject: subject}
	sh.mu.Lock()
	b, ok := sh.m[key]
	if !ok {
		b = bucket{tokens: capTokens, lastMs: now}
		l.rateBucketCount.add(1)
	}
	// 懒补充：经过的毫秒数乘以每秒速率，正好是千分之一令牌的增量。
	if elapsed := now - b.lastMs; elapsed > 0 {
		b.tokens += elapsed * int64(d.Limit)
		if b.tokens > capTokens {
			b.tokens = capTokens
		}
		b.lastMs = now
	}
	ok = b.tokens >= want
	if ok {
		b.tokens -= want
	}
	sh.m[key] = b
	sh.mu.Unlock()

	if ok {
		l.stats[id].allowed.add(1)
	} else {
		l.stats[id].rejected.add(1)
	}
	return ok
}

// SweepRateBuckets 增量回收已经恢复到满额的令牌桶。
//
// 满桶被删除与保留的下一次判定完全等价：两者都会以完整 burst 接受请求。
// 每次只扫 maxShards 个分片，避免百万设备时一次长时间持有所有 shard 锁。
func (l *Local) SweepRateBuckets(maxShards int) int {
	if maxShards <= 0 {
		return 0
	}
	if maxShards > bucketShards {
		maxShards = bucketShards
	}
	now := l.nowUnixMs()
	start := int(l.sweepCursor.load() % bucketShards)
	l.sweepCursor.add(uint64(maxShards))
	removed := 0
	for off := 0; off < maxShards; off++ {
		sh := &l.buckets[(start+off)%bucketShards]
		sh.mu.Lock()
		removedInShard := 0
		for key, b := range sh.m {
			d := l.defs[key.id]
			if d.Kind != Rate || d.Limit <= 0 {
				continue
			}
			burst := d.Burst
			if burst <= 0 {
				burst = d.Limit
			}
			capTokens := int64(burst) * milliScale
			if b.tokens >= capTokens ||
				(now > b.lastMs && b.tokens+(now-b.lastMs)*int64(d.Limit) >= capTokens) {
				delete(sh.m, key)
				removed++
				removedInShard++
			}
		}
		// Go map 删除键不会缩小 backing buckets。若本轮删掉的比剩下的
		// 还多，重建小 map 才能真的把百万设备峰值归还给 GC；只 delete
		// 会让对象数下降而 RSS/HeapInuse 仍长期停在峰值。
		if removedInShard > len(sh.m) {
			compact := make(map[bucketKey]bucket, len(sh.m))
			for key, b := range sh.m {
				compact[key] = b
			}
			sh.m = compact
		}
		sh.mu.Unlock()
	}
	if removed > 0 {
		l.rateBucketCount.add(-int64(removed))
		l.rateBucketsSwept.add(uint64(removed))
	}
	return removed
}

// RateBucketCount 返回当前常驻的速率桶数。
func (l *Local) RateBucketCount() int64 { return l.rateBucketCount.load() }

// RateBucketsSwept 返回启动以来回收的速率桶数。
func (l *Local) RateBucketsSwept() uint64 { return l.rateBucketsSwept.load() }

func (l *Local) Acquire(id ID, subject string, n int) bool {
	if id >= numQuotas || l.defs[id].Kind != Count {
		return false
	}
	d := l.defs[id]
	if d.Limit <= 0 {
		l.stats[id].allowed.add(1)
		l.stats[id].current.add(int64(n))
		return true
	}
	sh := &l.counters[shardOf(id, subject)]
	key := bucketKey{id: id, subject: subject}
	sh.mu.Lock()
	cur := sh.m[key]
	ok := cur+int64(n) <= int64(d.Limit)
	if ok {
		sh.m[key] = cur + int64(n)
	}
	sh.mu.Unlock()

	if ok {
		l.stats[id].allowed.add(1)
		l.stats[id].current.add(int64(n))
	} else {
		l.stats[id].rejected.add(1)
	}
	return ok
}

func (l *Local) Release(id ID, subject string, n int) {
	if id >= numQuotas || l.defs[id].Kind != Count {
		return
	}
	sh := &l.counters[shardOf(id, subject)]
	key := bucketKey{id: id, subject: subject}
	sh.mu.Lock()
	cur := sh.m[key]
	cur -= int64(n)
	if cur <= 0 {
		// 计数归零时删除条目，避免长期运行后 map 无界增长。
		delete(sh.m, key)
		if cur < 0 {
			// 归还多于占用是调用方错误；钳到 0 以免计数漂移。
			n += int(cur)
		}
	} else {
		sh.m[key] = cur
	}
	sh.mu.Unlock()
	if n > 0 {
		l.stats[id].current.add(-int64(n))
	}
}

// Current 返回某个作用对象当前占用的计数型配额。
func (l *Local) Current(id ID, subject string) int64 {
	if id >= numQuotas {
		return 0
	}
	sh := &l.counters[shardOf(id, subject)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[bucketKey{id: id, subject: subject}]
}

func (l *Local) Within(id ID, v int) bool {
	if id >= numQuotas || l.defs[id].Kind != Size {
		return false
	}
	d := l.defs[id]
	ok := d.Limit <= 0 || v <= d.Limit
	if ok {
		l.stats[id].allowed.add(1)
	} else {
		l.stats[id].rejected.add(1)
	}
	return ok
}

// Limit 返回某项配额当前生效的限制值。
func (l *Local) Limit(id ID) int {
	if id >= numQuotas {
		return 0
	}
	return l.defs[id].Limit
}

func (l *Local) Stats() []Stat {
	out := make([]Stat, 0, numQuotas)
	for i := ID(0); i < numQuotas; i++ {
		out = append(out, Stat{
			Def:      l.defs[i],
			Allowed:  l.stats[i].allowed.load(),
			Rejected: l.stats[i].rejected.load(),
			Current:  l.stats[i].current.load(),
		})
	}
	return out
}

var _ Limiter = (*Local)(nil)
