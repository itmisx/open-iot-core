package events

import (
	"sync"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// Filter 选择订阅者关心的事件。零值匹配全部类型。
type Filter struct {
	// Kinds 是类型位掩码。0 等同 AllKinds。
	Kinds uint32
	// ClientID 非空时只匹配该客户端。
	ClientID string
	// Thing 非空时只匹配该设备。
	Thing string
	// OnlyDenied 为 true 时，授权事件只匹配拒绝。
	OnlyDenied bool
}

func (f *Filter) match(e *Event) bool {
	kinds := f.Kinds
	if kinds == 0 {
		kinds = AllKinds
	}
	if kinds&e.Kind.Mask() == 0 {
		return false
	}
	if f.ClientID != "" && f.ClientID != e.ClientID {
		return false
	}
	if f.Thing != "" && f.Thing != e.Thing {
		return false
	}
	if f.OnlyDenied && e.Kind == KindAuthz && e.Authz.Decision == Allow {
		return false
	}
	return true
}

// Bus 是进程内的事件总线。
//
// 单节点实现即完整实现：集群下控制台订阅跨节点事件时，由 Admin 层
// 扇出到各节点的本地总线并合并（契约 12、PRD OBS-004），
// 本总线本身不需要知道集群的存在。
type Bus struct {
	node      cluster.NodeID
	nowUnixMs func() int64

	seq     atomic.Uint64
	dropped atomic.Uint64
	emitted [numKinds]atomic.Uint64

	// sampling 是每类事件的采样分母：n 表示每 n 条取 1 条。
	// 0 与 1 都表示全量。
	sampling [numKinds]atomic.Uint32
	counter  [numKinds]atomic.Uint64

	mu   sync.RWMutex
	subs map[uint64]*sink
	next uint64

	// ring 是最近事件的环形缓冲，用于订阅时回填。
	//
	// 为什么需要它：事件流最有价值的时刻恰恰是"出问题之后"，而运维
	// 是在出问题之后才去打开控制台的。没有回填的话，他看到的是一个
	// 空面板——而空面板与坏掉的面板无法区分。
	//
	// 用独立的锁而不是复用 mu：mu 在 Emit 里是读锁，多个发布者可以
	// 并发分发；若改成写锁，Emit 就被完全串行化了，而它在热路径上。
	// 环形写入本身必然串行，但代价只是一次 728 字节拷贝。
	ringMu  sync.Mutex
	ring    []Event
	ringPos int
	ringLen int

	// c 是指标计数器。见 metrics.go 关于"为什么指标不能从事件流
	// 推导"的说明。
	c *counters
}

type sink struct {
	filter Filter
	ch     chan Event
}

// Options 是 Bus 的构造参数。
type Options struct {
	// Node 是本节点 ID，写入每条事件。
	Node cluster.NodeID
	// Now 提供当前时间（Unix 毫秒），仅用于展示与保留期判定。
	Now func() int64
	// RingSize 是回填用环形缓冲的容量，0 取默认值，负数表示不保留。
	//
	// 每条事件 728 字节，默认 512 条约 0.4 MiB。事件结构里不含设备
	// 载荷（NFR-041），因此内存里留一份不会把业务数据留在进程里。
	RingSize int
}

// DefaultRingSize 是回填缓冲的默认容量。
//
// 512 条足够覆盖"一台设备反复重连被拒"的一整段现场，又只占 0.4 MiB。
const DefaultRingSize = 512

// New 创建事件总线。
func New(opt Options) *Bus {
	b := &Bus{node: opt.Node, nowUnixMs: opt.Now, subs: make(map[uint64]*sink), c: newCounters()}
	switch {
	case opt.RingSize == 0:
		b.ring = make([]Event, DefaultRingSize)
	case opt.RingSize > 0:
		b.ring = make([]Event, opt.RingSize)
	}
	if b.nowUnixMs == nil {
		b.nowUnixMs = func() int64 { return 0 }
	}
	// 连接事件与授权拒绝默认全量（PRD OBS-003）。
	for k := Kind(0); k < numKinds; k++ {
		b.sampling[k].Store(1)
	}
	return b
}

// SetSampling 设定某类事件的采样分母。n 小于 2 表示全量。
//
// 授权放行事件在高吞吐下量极大，通常只采样；拒绝事件不受采样影响，
// 因为它们是排障的关键输入。
func (b *Bus) SetSampling(k Kind, n uint32) {
	if k >= numKinds {
		return
	}
	if n == 0 {
		n = 1
	}
	b.sampling[k].Store(n)
}

// Emit 发布一条事件。
//
// 它会填充 Node、Seq 与时间戳。调用方传入指针以避免在热路径上复制，
// 但总线按值分发给订阅者，因此调用方可以复用同一个结构体。
//
// Emit 永不阻塞：订阅者通道满时丢弃并计数（PRD OBS-005）。
// 事件是观测数据，不能让它反压业务路径。
func (b *Bus) Emit(e *Event) {
	if e.Kind >= numKinds {
		return
	}
	// 指标在采样之前累加。放到之后会让指标按采样倍数偏小，
	// 而指标不允许失真（见 metrics.go）。
	b.c.record(e)

	// 采样：授权放行这类高频事件按分母抽取；拒绝与错误不受采样影响。
	if n := b.sampling[e.Kind].Load(); n > 1 && !mustKeep(e) {
		if b.counter[e.Kind].Add(1)%uint64(n) != 0 {
			return
		}
	}
	e.Node = b.node
	e.Seq = b.seq.Add(1)
	e.AtUnixMs = b.nowUnixMs()
	b.emitted[e.Kind].Add(1)

	b.mu.RLock()
	for _, s := range b.subs {
		if !s.filter.match(e) {
			continue
		}
		select {
		case s.ch <- *e:
		default:
			b.dropped.Add(1)
		}
	}
	b.mu.RUnlock()

	// 写环形缓冲放在分发之后：实时订阅者优先，回填是次要用途。
	if len(b.ring) > 0 {
		b.ringMu.Lock()
		b.ring[b.ringPos] = *e
		b.ringPos = (b.ringPos + 1) % len(b.ring)
		if b.ringLen < len(b.ring) {
			b.ringLen++
		}
		b.ringMu.Unlock()
	}
}

// mustKeep 报告事件是否不受采样影响。
//
// 拒绝、失败与须断连的变更是排障与安全审计的关键输入，
// 任何采样配置都不得丢弃它们。
func mustKeep(e *Event) bool {
	switch e.Kind {
	case KindAuthz:
		return e.Authz.Decision != Allow
	case KindConn:
		return e.Conn.Result != ConnAccepted
	case KindMeta:
		return e.Meta.RequiresDisconnect
	case KindQuota, KindCluster, KindAudit:
		return true
	default:
		return false
	}
}

// Subscribe 注册一个订阅者，返回事件通道与取消函数。
//
// buf 是通道缓冲深度，必须有界。取消函数幂等。
func (b *Bus) Subscribe(f Filter, buf int) (<-chan Event, func()) {
	if buf <= 0 {
		buf = 256
	}
	s := &sink{filter: f, ch: make(chan Event, buf)}
	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = s
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
			close(s.ch)
		})
	}
	return s.ch, cancel
}

// Recent 返回环形缓冲中匹配 f 的事件，按时间升序，最多 limit 条。
//
// limit 小于 1 时取全部。
func (b *Bus) Recent(f Filter, limit int) []Event {
	if len(b.ring) == 0 {
		return nil
	}
	b.ringMu.Lock()
	defer b.ringMu.Unlock()

	// 从最旧的一条开始遍历。缓冲未满时最旧的在下标 0。
	start := 0
	if b.ringLen == len(b.ring) {
		start = b.ringPos
	}
	out := make([]Event, 0, b.ringLen)
	for i := 0; i < b.ringLen; i++ {
		e := &b.ring[(start+i)%len(b.ring)]
		if f.match(e) {
			out = append(out, *e)
		}
	}
	// 超出 limit 时保留**最新**的那些：排障关心的是最近发生了什么。
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// SubscribeWithRecent 注册订阅者并同时取回最近事件，用于控制台这类
// "打开即需要看到现场"的消费者。
//
// 注册在快照之前，顺序是刻意的：
//   - 先注册后快照，可能让一条事件既进快照又进通道（重复）；
//   - 先快照后注册，会让这期间的事件两边都没有（丢失）。
//
// 重复可以靠 Seq 过滤掉，丢失无法补回，所以选前者。调用方应记下
// 回填里最大的 Seq，并跳过通道里 Seq 不大于它的事件。
//
// 普通的 Subscribe 不回填，这一点很重要：被拒连接的日志协程若拿到
// 历史事件，每次重启都会把旧的拒绝重新打一遍日志。
func (b *Bus) SubscribeWithRecent(f Filter, buf, limit int) (<-chan Event, []Event, func()) {
	ch, cancel := b.Subscribe(f, buf)
	return ch, b.Recent(f, limit), cancel
}

// Stats 是总线的运行指标。
type Stats struct {
	// Emitted 按类型统计已发布数。
	Emitted [numKinds]uint64
	// Dropped 是因订阅者通道满而丢弃的投递次数。
	Dropped uint64
	// Subscribers 是当前订阅者数。
	Subscribers int
	// Seq 是已分配的最大序号。
	Seq uint64
}

// Stats 返回总线指标。
func (b *Bus) Stats() Stats {
	st := Stats{Dropped: b.dropped.Load(), Seq: b.seq.Load()}
	for k := Kind(0); k < numKinds; k++ {
		st.Emitted[k] = b.emitted[k].Load()
	}
	b.mu.RLock()
	st.Subscribers = len(b.subs)
	b.mu.RUnlock()
	return st
}
