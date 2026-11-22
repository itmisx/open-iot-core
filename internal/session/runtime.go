package session

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// ErrDetached 表示运行时已与连接解绑，不能再写出。
var ErrDetached = errors.New("session: 运行时已解绑")

// ErrOutboundFull 表示出站队列已满。调用方应据此施加背压，
// 而不是让服务端无界积压（PRD MQTT-035）。
var ErrOutboundFull = errors.New("session: 出站队列已满")

// 常见队列深度的环形缓冲跨连接复用。缓冲一旦排空就从 Runtime 解除引用，
// 因而一次历史突发不会让百万空闲连接永久恢复到完整队列的内存水位。
// sync.Pool 在 GC 时可自行丢弃缓存，不形成新的全局常驻上限。
var outboundRingPools = map[int]*sync.Pool{
	32:  {},
	64:  {},
	128: {},
	256: {},
}

type outboundQueue struct {
	one  Outbound
	ring []Outbound
	head int
	len  int
}

var outboundQueuePool sync.Pool

func acquireOutboundQueue() *outboundQueue {
	if v := outboundQueuePool.Get(); v != nil {
		return v.(*outboundQueue)
	}
	return new(outboundQueue)
}

func releaseOutboundQueue(q *outboundQueue) {
	*q = outboundQueue{}
	outboundQueuePool.Put(q)
}

func acquireOutboundRing(n int) []Outbound {
	if p := outboundRingPools[n]; p != nil {
		if v := p.Get(); v != nil {
			return v.([]Outbound)
		}
	}
	return make([]Outbound, n)
}

func releaseOutboundRing(ring []Outbound) {
	if p := outboundRingPools[len(ring)]; p != nil {
		p.Put(ring)
	}
}

// Outbound 是一条待写出的报文。
//
// 它只携带已编码的字节与少量元信息：编码在投递决策时一次完成，
// 写出路径不再接触报文结构，这使共享 writer 池可以纯粹地做 IO。
type Outbound struct {
	// Bytes 是完整的 MQTT 报文。其底层数组可能来自池，
	// 写出完成后由 Release 归还。
	Bytes []byte
	// Release 在写出完成后被调用，可为 nil。
	Release func()
	// PacketID 非零时表示这是一条需要确认的报文。
	PacketID uint16
	QoS      uint8
}

// Runtime 是一个活跃会话的运行时侧。
//
// 它持有连接、出站队列与锁，不可序列化，生命周期短于 State：
// 连接断开后 Runtime 被销毁，而 State 依其过期间隔继续存在。
//
// 所有改变状态的方法都要求调用方提供 epoch，不匹配即返回
// cluster.ErrStaleEpoch。单机下 epoch 也必须校验——这条分支在 v0.1
// 就被持续覆盖，才不会在集群化时才第一次运行（PRD ARCH-011、ARCH-057）。
type Runtime struct {
	// state 在 Runtime 存续期间由其独占修改，外部只能经本类型的方法访问。
	state *State

	mu       sync.Mutex
	conn     net.Conn
	detached bool

	// 出站队列把第一条消息内联；第二条并发积压时才分配完整环形缓冲。
	// 绝大多数空闲 IoT 连接只发一次 CONNACK，旧的 buffered channel 却会
	// 永久常驻 outCap 个槽。inline-first 保留相同有界背压而不预付突发内存。
	out    *outboundQueue
	outCap int
	// closeOnce 保证 Close 幂等，重复断开不会重复触发遗嘱与事件。
	closeOnce sync.Once
	onClose   func(reason cluster.EvictReason)
}

// NewRuntime 把一个会话状态绑定到连接上。
// outBuf 是出站队列深度，必须有界。
func NewRuntime(st *State, conn net.Conn, outBuf int) *Runtime {
	if outBuf <= 0 {
		outBuf = 64
	}
	return &Runtime{
		state:  st,
		conn:   conn,
		outCap: outBuf,
	}
}

// State 返回底层状态的只读视图。
//
// 调用方不得修改返回值；需要修改请使用 Runtime 的带 epoch 校验的方法。
// 返回指针而非拷贝是为了避免热路径上的复制，这一约定由代码评审保证。
func (r *Runtime) State() *State { return r.state }

// ClientID 返回会话的 clientId。
func (r *Runtime) ClientID() string { return r.state.ClientID }

// Epoch 返回会话当前持有的 epoch。
func (r *Runtime) Epoch() cluster.Epoch { return r.state.Epoch }

// Conn 返回底层连接，可能为 nil（已解绑）。
func (r *Runtime) Conn() net.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn
}

// Dequeue 取出最早的一条出站报文。共享 writer pool 是唯一消费者。
func (r *Runtime) Dequeue() (Outbound, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.out
	if q == nil || q.len == 0 {
		return Outbound{}, false
	}
	if q.ring == nil {
		out := q.one
		r.out = nil
		releaseOutboundQueue(q)
		return out, true
	}
	out := q.ring[q.head]
	q.ring[q.head] = Outbound{}
	q.head = (q.head + 1) % r.outCap
	q.len--
	if q.len == 0 {
		ring := q.ring
		r.out = nil
		releaseOutboundRing(ring)
		releaseOutboundQueue(q)
	}
	return out, true
}

// OutboundLen 返回当前积压，供 writer 清除 queued 标记后复查唤醒竞态。
func (r *Runtime) OutboundLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.out == nil {
		return 0
	}
	return r.out.len
}

// Enqueue 把一条报文放入出站队列。队列满时立即返回 ErrOutboundFull，
// 不阻塞调用方——投递路径不能被单个慢客户端拖住。
//
// 解绑判定与入队必须在**同一个临界区**里。此前是「取锁读 detached、
// 放锁、再入队」，中间那道缝是会让整个进程崩掉的：投递跑在发布者的协程
// 上，而 Close 跑在订阅者自己的协程上；发布者刚看到 detached 为假，
// 订阅者就把通道关了，接着那次 `r.out <- o` 就是**向已关闭的通道发送**
// ——panic，而全仓库没有任何 recover，于是一条正常的"客户端在别人正给
// 它投消息时断开"把 broker 整个带走。
//
// 现在解绑判定与环形队列写入仍在同一个临界区，且 Close 不再关闭 channel
// （队列已经不是 channel），因此这类 send/close 竞态从结构上消失。锁的
// 次数没有增加——原来那次读 detached 本来也要取锁。
func (r *Runtime) Enqueue(e cluster.Epoch, o Outbound) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.detached {
		return ErrDetached
	}
	q := r.out
	if q != nil && q.len >= r.outCap {
		return ErrOutboundFull
	}
	if q == nil {
		q = acquireOutboundQueue()
		q.one = o
		q.len = 1
		r.out = q
		return nil
	}
	if q.ring == nil {
		q.ring = acquireOutboundRing(r.outCap)
		q.ring[0] = q.one
		q.one = Outbound{}
		q.head = 0
	}
	idx := (q.head + q.len) % r.outCap
	q.ring[idx] = o
	q.len++
	return nil
}

// Subscribe 记录一条订阅。同一过滤器重复订阅时替换其选项（MQTT-3.8.4）。
// 返回 added 表示这是一条新订阅；重复订阅同一过滤器只是就地替换
// （MQTT 允许客户端重复订阅以修改 QoS），调用方据此决定要不要占配额——
// 不区分的话，一个反复订阅同一主题的客户端会把配额一路耗光，
// 而它实际只持有一条订阅。
func (r *Runtime) Subscribe(e cluster.Epoch, s Subscription) (added bool, err error) {
	if err := r.state.CheckEpoch(e); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Subscriptions {
		if r.state.Subscriptions[i].Filter == s.Filter &&
			r.state.Subscriptions[i].Share == s.Share {
			r.state.Subscriptions[i] = s
			return false, nil
		}
	}
	r.state.Subscriptions = append(r.state.Subscriptions, s)
	return true, nil
}

// HasSubscription 报告会话是否已持有该订阅。
//
// 给配额判定用：要在占用配额之前知道这是不是一条新订阅，而 Subscribe
// 的时机太晚——那时 SUBACK 的原因码已经定了。
func (r *Runtime) HasSubscription(filter, share string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Subscriptions {
		if r.state.Subscriptions[i].Filter == filter &&
			r.state.Subscriptions[i].Share == share {
			return true
		}
	}
	return false
}

// Unsubscribe 移除一条订阅，返回是否确实移除了。
func (r *Runtime) Unsubscribe(e cluster.Epoch, filter, share string) (bool, error) {
	if err := r.state.CheckEpoch(e); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	subs := r.state.Subscriptions
	for i := range subs {
		if subs[i].Filter == filter && subs[i].Share == share {
			r.state.Subscriptions = append(subs[:i], subs[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

// SubscriptionsSnapshot 返回订阅集的副本。
//
// 订阅集与在途表、待发队列的改动都在本类型的锁下。投递跑在发布者的
// 协程上，确认跑在订阅者自己的读协程上，两边都要改在途表——不进这把锁
// 就是竞态。接管从新连接的协程读被抢占连接的状态，走的也是这些快照。
// 别名表仍由持有连接的读协程独占，不在此列。
func (r *Runtime) SubscriptionsSnapshot() []Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.state.Subscriptions) == 0 {
		// 返回 nil 而不是空切片：调用方会把它直接赋给 State.Subscriptions，
		// 而会话状态要能序列化后往返相等（TST-058），空切片与 nil
		// 在 JSON 里不是同一个值。
		return nil
	}
	return append([]Subscription(nil), r.state.Subscriptions...)
}

// ReplaceSubscriptions 整体替换订阅集。
//
// 只有接管会用到：新连接要用被抢占连接的内存订阅集覆盖掉从存储恢复出来
// 的那份（理由见 broker 的 conn.adoptLive）。普通的订阅变更一律走
// Subscribe 与 Unsubscribe——它们逐条判定是否新增，调用方据此占配额。
//
// subs 的底层数组由本会话接管，调用方不得继续持有它：
// SubscriptionsSnapshot 返回的本来就是一份新副本，再拷一次是白花。
func (r *Runtime) ReplaceSubscriptions(e cluster.Epoch, subs []Subscription) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Subscriptions = subs
	return nil
}

// AddInflight 追加一条出站未确认消息。负载与属性会被复制，
// 调用方可以继续使用原来的缓冲。
func (r *Runtime) AddInflight(e cluster.Epoch, m InflightMsg) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	m.Payload = append([]byte(nil), m.Payload...)
	m.Props = append([]byte(nil), m.Props...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Inflight = append(r.state.Inflight, m)
	return nil
}

// RemoveInflight 按报文标识移除一条在途消息。没找到返回 false。
func (r *Runtime) RemoveInflight(e cluster.Epoch, id uint16) bool {
	if err := r.state.CheckEpoch(e); err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Inflight {
		if r.state.Inflight[i].PacketID == id {
			r.state.Inflight = append(r.state.Inflight[:i], r.state.Inflight[i+1:]...)
			return true
		}
	}
	return false
}

// InflightHas 报告报文标识是否已在在途表里。分配新标识时用来避开它。
func (r *Runtime) InflightHas(id uint16) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Inflight {
		if r.state.Inflight[i].PacketID == id {
			return true
		}
	}
	return false
}

// InflightByID 返回一条在途消息的副本。
func (r *Runtime) InflightByID(id uint16) (InflightMsg, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Inflight {
		if r.state.Inflight[i].PacketID == id {
			return cloneInflight(r.state.Inflight[i]), true
		}
	}
	return InflightMsg{}, false
}

// InflightSnapshot 返回在途表的深拷贝。
func (r *Runtime) InflightSnapshot() []InflightMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneInflightSlice(r.state.Inflight)
}

// ReplaceInflight 用快照替换在途表。只有接管会用到。
//
// msgs 的底层数组由本会话接管，调用方不得继续持有它。
func (r *Runtime) ReplaceInflight(e cluster.Epoch, msgs []InflightMsg) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Inflight = msgs
	return nil
}

// NoteRetry 记录一次重投：重试次数加一，发送时刻改到 nowUnixMs。
func (r *Runtime) NoteRetry(e cluster.Epoch, id uint16, nowUnixMs int64) bool {
	if err := r.state.CheckEpoch(e); err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Inflight {
		if r.state.Inflight[i].PacketID == id {
			r.state.Inflight[i].Retries++
			r.state.Inflight[i].SentUnixMs = nowUnixMs
			r.state.Inflight[i].Dup = true
			return true
		}
	}
	return false
}

// MarkInflight 改一条在途消息的阶段（QoS 2 收到 PUBREC 之后）。
func (r *Runtime) MarkInflight(e cluster.Epoch, id uint16, st InflightState) bool {
	if err := r.state.CheckEpoch(e); err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.state.Inflight {
		if r.state.Inflight[i].PacketID == id {
			r.state.Inflight[i].State = st
			return true
		}
	}
	return false
}

// PushQueued 把一条消息放进待发队列。负载、属性与订阅标识会被复制。
func (r *Runtime) PushQueued(e cluster.Epoch, m QueuedMsg) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	m.Payload = append([]byte(nil), m.Payload...)
	m.Props = append([]byte(nil), m.Props...)
	m.SubIDs = append([]uint32(nil), m.SubIDs...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Queued = append(r.state.Queued, m)
	return nil
}

// PopQueued 取出待发队列的第一条。
func (r *Runtime) PopQueued(e cluster.Epoch) (QueuedMsg, bool) {
	if err := r.state.CheckEpoch(e); err != nil {
		return QueuedMsg{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.state.Queued) == 0 {
		return QueuedMsg{}, false
	}
	m := r.state.Queued[0]
	r.state.Queued = r.state.Queued[1:]
	return m, true
}

// TakeQueued 取走全部待发消息，会话里的队列清空。
func (r *Runtime) TakeQueued(e cluster.Epoch) []QueuedMsg {
	if err := r.state.CheckEpoch(e); err != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.state.Queued
	r.state.Queued = nil
	return q
}

// QueuedSnapshot 返回待发队列的深拷贝。
func (r *Runtime) QueuedSnapshot() []QueuedMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.state.Queued) == 0 {
		return nil
	}
	out := make([]QueuedMsg, len(r.state.Queued))
	for i, m := range r.state.Queued {
		m.Payload = append([]byte(nil), m.Payload...)
		m.Props = append([]byte(nil), m.Props...)
		m.SubIDs = append([]uint32(nil), m.SubIDs...)
		out[i] = m
	}
	return out
}

// ReplaceQueued 用快照替换待发队列。只有接管会用到。
func (r *Runtime) ReplaceQueued(e cluster.Epoch, msgs []QueuedMsg) error {
	if err := r.state.CheckEpoch(e); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Queued = msgs
	return nil
}

// CloneState 返回会话状态的深拷贝，拷贝期间与在途表、队列的改动互斥。
//
// 落盘发生在连接自己的读协程上，而在途表会从发布者的协程追加。
// 直接把 State 指针交给存储会让编码与追加撞在同一片切片上。
func (r *Runtime) CloneState() *State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state.Clone()
}

func cloneInflight(m InflightMsg) InflightMsg {
	m.Payload = append([]byte(nil), m.Payload...)
	m.Props = append([]byte(nil), m.Props...)
	return m
}

func cloneInflightSlice(in []InflightMsg) []InflightMsg {
	if len(in) == 0 {
		return nil
	}
	out := make([]InflightMsg, len(in))
	for i := range in {
		out[i] = cloneInflight(in[i])
	}
	return out
}

// SetOnClose 注册断开回调，用于触发遗嘱与生命周期事件。
func (r *Runtime) SetOnClose(fn func(cluster.EvictReason)) {
	r.mu.Lock()
	r.onClose = fn
	r.mu.Unlock()
}

// Close 断开连接并解绑。幂等：重复调用不会重复触发回调。
//
// 状态本身不在此处销毁——它依过期间隔由会话存储决定去留。
func (r *Runtime) Close(nowUnixMs int64, why cluster.EvictReason) error {
	var err error
	r.closeOnce.Do(func() {
		r.mu.Lock()
		conn, fn := r.conn, r.onClose
		r.conn = nil
		r.detached = true
		r.state.DisconnectUnixMs = nowUnixMs
		r.mu.Unlock()

		if conn != nil {
			err = conn.Close()
		}
		if fn != nil {
			fn(why)
		}
	})
	return err
}

// Detached 报告运行时是否已解绑。
func (r *Runtime) Detached() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.detached
}

// Store 是会话状态的持久化接口。
//
// 单机默认实现把状态留在内存（节点故障即丢失，该行为须在文档首屏声明，
// 见 PRD ARCH-005）；可选的复制后端在 v0.4 提供。
// 接口在 v0.1 就要存在，因为"从哪里恢复会话"会影响接管流程的形状。
type Store interface {
	// Load 取出会话状态。
	Load(ctx context.Context, clientID string) (*State, bool, error)
	// Save 保存会话状态。实现须校验 epoch，拒绝来自已被接管会话的写入。
	Save(ctx context.Context, st *State) error
	// Replace 原子地删掉既有记录并写入 st。与 Save 一样，
	// 实现须校验 epoch，拒绝来自已被接管会话的替换。
	//
	// 持久会话的 clean start 用它，而不是 Delete 再 Save：后者在集群下
	// 是**两条**复制日志，而且中间那一瞬存储里没有这个会话——此时另一个
	// 节点的读会判定"没有会话"。一条命令就没有那个中间态。
	Replace(ctx context.Context, st *State) error
	// Delete 删除会话状态。
	Delete(ctx context.Context, clientID string, e cluster.Epoch) error
	// Expire 清理已过期的会话，返回清理条数。
	Expire(ctx context.Context, nowUnixMs int64) (int, error)
	// AppendQueued 往离线队列尾部追加一条消息。
	//
	// 单独一个方法而不是"改 State.Queued 再 Save"：后者在发布路径上
	// 要把整份会话重新编码落盘（N 条累计 O(N²)），集群下还要把上百 KB
	// 的会话提进复制日志。
	AppendQueued(clientID string, m QueuedMsg) error
	// TakeQueued 取出并清空离线队列，重连时补发用。
	TakeQueued(clientID string) ([]QueuedMsg, error)
}
