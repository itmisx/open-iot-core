package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// 节点间的消息面（契约 07、阶段 D）。
//
// 复用复制组那条 TLS 连接，不另开端口、不另发一套证书：节点之间只有
// 一条互信通道，上面跑多种消息（选举、日志、消息帧、兴趣通告、设备归属）。
// 另开一条的代价是运维要配两个端口两套证书，而安全边界完全一样。

// MeshPayload 是 mesh 通道上的一条消息。
//
// 三类负载共用一个消息类型，按非空字段区分：
//   - Frames 是要投递给对端本地订阅者的消息帧；
//   - Filters 是发送方的通配符订阅**全量快照**（InterestSet 要的就是快照，
//     丢一次通告或乱序都不会让兴趣集永久偏离）；
//   - Things 是发送方设备归属的按需全量快照；正常心跳只带 ThingChanges。
type MeshPayload struct {
	Frames  []Frame
	Filters []string
	Things  []string
	// ThingEpoch 标识发送进程的本次生命周期，ThingFrom/ThingVersion 是
	// 增量覆盖的版本区间。接收方只有在本地版本恰好等于 ThingFrom 时才
	// 应用增量；进程重启或版本断档时用 ThingSync 请求一份快照。
	ThingEpoch   uint64
	ThingFrom    uint64
	ThingVersion uint64
	ThingChanges []ThingChange
	// ThingSnapshot 明确区分「空快照」与「没带快照」。不能只看 Things
	// 是否为 nil：gob 对空切片的往返表示不应成为协议正确性条件。
	ThingSnapshot bool
	// ThingSync 表示发送方请求接收方在下一轮回一份设备归属快照。
	ThingSync bool
	// Shares 是发送方各共享订阅组的在线成员数**全量快照**（MQTT-116）。
	//
	// 与 Filters 同样是快照而不是增量：丢一次通告或乱序都不会让组成员
	// 视图永久偏离，而增量丢一条就永久少一个节点——现象是"那一组里有
	// 一台机器再也收不到消息"。
	Shares []ShareCount
	// Retained 是保留消息的跨节点更新（MQTT-122）。
	//
	// 与上面几个字段不同，它**不是**周期性全量快照而是事件驱动的增量：
	// retained 集合可能上万条、每条 KB 级，每秒全量通告一遍是带宽灾难。
	// 代价是增量会丢（队列满、发送失败），所以另有两条补路——新对端
	// 出现时发一次全量 dump（CLU-108），以及下一阶段的 digest 对账
	// （MQTT-125 写明了在那之前的边界）。
	//
	// 乱序与重复都不要紧：收端按 Version 定序（MQTT-123），同一条更新
	// 重复到达是幂等的。这正是把判据做成确定性全序的理由。
	Retained []RetainedUpdate
	// Evict 是发送方刚刚接管的 clientId。收到的节点若本地还有同名连接，
	// 就把它断掉（CLU-081）。
	//
	// 这是「最终一致的互斥」：窗口是一次 mesh 往返，而不是零。严格的
	// 零窗口要求连接时等一次共识（epoch 由 leader 统一发放），而设备
	// 上下线是高频事件——把它放进复制日志会让日志被连接流量撑爆，
	// 也会让连接路径上多一次 RPC（契约 05 刚好禁止这件事）。
	Evict []string
}

// Empty 报告这条消息是否什么都没带。
func (p MeshPayload) Empty() bool {
	return len(p.Frames) == 0 && p.Filters == nil &&
		p.Things == nil && !p.ThingSnapshot && !p.ThingSync &&
		len(p.ThingChanges) == 0 && p.ThingEpoch == 0 && p.Shares == nil &&
		len(p.Evict) == 0 && len(p.Retained) == 0
}

// ThingChange 是一台设备在发送节点上的最新存在状态。重复应用是幂等的。
type ThingChange struct {
	Name    string
	Present bool
}

// RetainedUpdate 是一条保留消息的跨节点更新。
//
// 逐字段列出而不是传 router.Message：cluster 包不依赖 router（那会让
// 传输层知道路由层的内部结构，而 Message 里有未导出的缓存字段，gob
// 编码会把它们丢掉且不报错）。
type RetainedUpdate struct {
	Topic string
	// Payload 为空表示**墓碑**，即这条 retained 已被清除（MQTT-124）。
	// 墓碑必须和活条目一样传播：漏掉清除的那个节点会一直留着旧负载。
	Payload []byte
	Props   []byte
	QoS     uint8
	// Counter 与 Node 是 Lamport 戳的两半（MQTT-123）。不要换成时间戳：
	// 各节点时钟不可比，用时钟的后果是偶发的"旧消息覆盖新消息"（ARCH-033）。
	Counter        uint64
	Node           string
	ExpiryUnixMs   int64
	StoredAtUnixMs int64
}

// SetMeshHandler 注册 mesh 消息的处理函数。
//
// 处理函数在接收协程上被调用，可能并发于不同的对端。实现侧要自己同步。
func (g *TLSGroup) SetMeshHandler(fn func(from NodeID, p MeshPayload)) {
	g.meshMu.Lock()
	g.meshFn = fn
	g.meshMu.Unlock()
}

// SendMesh 把一条 mesh 消息发给对端。
//
// 失败即返回错误，不在这里重试：消息面的背压决策属于调用方
// （CLU-062：对端跟不上时要拒绝，不是把帧堆进内存）。
func (g *TLSGroup) SendMesh(to NodeID, p MeshPayload) error {
	if p.Empty() {
		return nil
	}
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireMesh, From: string(g.id), Mesh: &p,
	})
	if !ok {
		return fmt.Errorf("cluster: 发往 %s 的 mesh 消息没有送达", to)
	}
	if resp.Err != "" {
		return fmt.Errorf("cluster: %s 拒绝了 mesh 消息: %s", to, resp.Err)
	}
	return nil
}

// Peers 返回除本节点之外的成员。
func (g *TLSGroup) Peers() []NodeID {
	return g.peerIDs()
}

// deliverMesh 把收到的 mesh 消息交给处理函数。
func (g *TLSGroup) deliverMesh(from NodeID, p MeshPayload) wireResp {
	g.meshMu.RLock()
	fn := g.meshFn
	g.meshMu.RUnlock()
	if fn == nil {
		// 没人处理就明确说出来，而不是回 OK：静默丢弃会让发送方以为
		// 对端收到了，而那条消息从此不存在。
		return wireResp{Err: "mesh handler not set"}
	}
	fn(from, p)
	return wireResp{OK: true}
}

// MeshTransport 用一个 TLSGroup 实现 Transport（契约 07）。
//
// 它只做「把帧送到对端」这一件事：匹配、去重、背压判定都在上层
// （router 算出远端节点集合，broker 决定拒不拒绝）。
type MeshTransport struct {
	group *TLSGroup
	self  NodeID
	send  func(NodeID, MeshPayload) error

	mu   sync.RWMutex
	recv func(NodeID, []Frame)

	peersMu sync.Mutex
	peers   map[NodeID]*meshPeerQueue
	closed  bool
	done    chan struct{}
	wg      sync.WaitGroup

	queueFrames int
	queueBytes  int
	batchFrames int
	batchBytes  int
	coalesce    time.Duration
}

type meshPeerQueue struct {
	mu     sync.Mutex
	frames []Frame
	bytes  int
	wake   chan struct{}
}

const (
	defaultMeshQueueFrames = 8192
	defaultMeshQueueBytes  = 16 << 20
	defaultMeshBatchFrames = 128
	defaultMeshBatchBytes  = 1 << 20
	defaultMeshCoalesce    = 200 * time.Microsecond
	meshRetryMin           = 10 * time.Millisecond
	meshRetryMax           = time.Second
)

// NewMeshTransport 包住一个复制组。
func NewMeshTransport(g *TLSGroup) *MeshTransport {
	return newMeshTransport(g, g.id, g.SendMesh)
}

func newMeshTransport(g *TLSGroup, self NodeID, send func(NodeID, MeshPayload) error) *MeshTransport {
	return &MeshTransport{
		group:       g,
		self:        self,
		send:        send,
		peers:       make(map[NodeID]*meshPeerQueue),
		done:        make(chan struct{}),
		queueFrames: defaultMeshQueueFrames,
		queueBytes:  defaultMeshQueueBytes,
		batchFrames: defaultMeshBatchFrames,
		batchBytes:  defaultMeshBatchBytes,
		coalesce:    defaultMeshCoalesce,
	}
}

// Forward 把一批帧转发给若干节点。
//
// Forward 只把帧交给每个对端的有界队列，不等待网络往返。每个对端有独立
// worker，慢节点不会阻塞健康节点；队列满时立即返回 ErrBackpressure。
// 已经入队的节点不回滚——消息面是至少一次语义，回滚没有意义。
func (t *MeshTransport) Forward(_ context.Context, to []NodeID, batch []Frame) error {
	if len(batch) == 0 || len(to) == 0 {
		return nil
	}
	frames, bytes := cloneMeshFrames(batch)
	var firstErr error
	for _, node := range to {
		if node == t.self {
			continue
		}
		if err := t.enqueue(node, frames, bytes); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func cloneMeshFrames(batch []Frame) ([]Frame, int) {
	out := make([]Frame, len(batch))
	bytes := 0
	for i := range batch {
		out[i] = batch[i]
		out[i].Payload = append([]byte(nil), batch[i].Payload...)
		out[i].Props = append([]byte(nil), batch[i].Props...)
		bytes += meshFrameBytes(out[i])
	}
	return out, bytes
}

func meshFrameBytes(f Frame) int {
	// 64 字节覆盖固定字段与 gob 编码开销。限制的目的是给内存
	// 一个保守上界，不是预测精确线上字节数。
	return 64 + len(f.Topic) + len(f.Payload) + len(f.Props) + len(f.Share) + len(f.Filter)
}

func (t *MeshTransport) enqueue(node NodeID, frames []Frame, bytes int) error {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	if t.closed {
		return ErrClosed
	}
	q := t.peers[node]
	if q == nil {
		q = &meshPeerQueue{wake: make(chan struct{}, 1)}
		t.peers[node] = q
		t.wg.Add(1)
		go t.runPeer(node, q)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.frames)+len(frames) > t.queueFrames || q.bytes+bytes > t.queueBytes {
		return ErrBackpressure
	}
	q.frames = append(q.frames, frames...)
	q.bytes += bytes
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

func (t *MeshTransport) runPeer(node NodeID, q *meshPeerQueue) {
	defer t.wg.Done()
	retry := meshRetryMin
	for {
		select {
		case <-t.done:
			return
		case <-q.wake:
		}
		if t.coalesce > 0 {
			timer := time.NewTimer(t.coalesce)
			select {
			case <-t.done:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		for {
			batch, bytes := t.peekBatch(q)
			if len(batch) == 0 {
				break
			}
			if err := t.send(node, MeshPayload{Frames: batch}); err != nil {
				timer := time.NewTimer(retry)
				select {
				case <-t.done:
					timer.Stop()
					return
				case <-timer.C:
				}
				if retry < meshRetryMax {
					retry *= 2
					if retry > meshRetryMax {
						retry = meshRetryMax
					}
				}
				continue
			}
			t.commitBatch(q, len(batch), bytes)
			retry = meshRetryMin
		}
	}
}

func (t *MeshTransport) peekBatch(q *meshPeerQueue) ([]Frame, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, bytes := 0, 0
	for n < len(q.frames) && n < t.batchFrames {
		fb := meshFrameBytes(q.frames[n])
		if n > 0 && bytes+fb > t.batchBytes {
			break
		}
		bytes += fb
		n++
	}
	if n == 0 {
		return nil, 0
	}
	return append([]Frame(nil), q.frames[:n]...), bytes
}

func (t *MeshTransport) commitBatch(q *meshPeerQueue, n, bytes int) {
	q.mu.Lock()
	clear(q.frames[:n])
	q.frames = q.frames[n:]
	q.bytes -= bytes
	if len(q.frames) == 0 {
		q.frames = nil
		q.bytes = 0
	}
	q.mu.Unlock()
}

// OnReceive 注册帧的接收回调。
func (t *MeshTransport) OnReceive(fn func(from NodeID, batch []Frame)) {
	t.mu.Lock()
	t.recv = fn
	t.mu.Unlock()
}

// Deliver 把收到的帧交给回调。由装配层在 mesh 处理函数里调用。
func (t *MeshTransport) Deliver(from NodeID, batch []Frame) {
	t.mu.RLock()
	fn := t.recv
	t.mu.RUnlock()
	if fn != nil && len(batch) > 0 {
		fn(from, batch)
	}
}

func (t *MeshTransport) Self() NodeID { return t.self }

// Close 停掉发送 worker，但不关复制组：组的生命周期由装配层管。
func (t *MeshTransport) Close() error {
	t.peersMu.Lock()
	if !t.closed {
		t.closed = true
		close(t.done)
	}
	t.peersMu.Unlock()
	t.wg.Wait()
	return nil
}

func (t *MeshTransport) isClosed() bool {
	t.peersMu.Lock()
	closed := t.closed
	t.peersMu.Unlock()
	return closed
}

var _ Transport = (*MeshTransport)(nil)

// 成员变更（CLU-090 至 CLU-093）。
//
// 成员列表是 raft 自己的状态，不是配置里的静态表：它决定多数派的分母，
// 而按一份过期的成员算多数派正是脑裂最直接的来路。变更作为日志条目
// 复制，提交之后才生效。

// Members 返回当前成员列表。
func (g *TLSGroup) Members() []Member { return g.members() }

// AddMember 把一个节点加入复制组。
//
// 只有 leader 能发起：变更要先被**当前**多数派接受。
//
// 不再需要那条占位命令（原先的 nop 参数）：上层的 revision 判据已经从
// 「恰好加一」放宽成「严格递增」，成员变更占掉的日志序号不再需要用一条
// 空命令补上（LANG-014 修订）。
func (g *TLSGroup) AddMember(id NodeID, addr string, replicaID uint64) error {
	if id == "" || addr == "" {
		return errors.New("cluster: 加入成员要给出节点 ID 与地址")
	}
	grp := g.g()
	if grp == nil {
		return ErrNotLeader
	}
	return grp.AddMember(id, addr, replicaID)
}

// RemoveMember 把一个节点移出复制组。
//
// 移除自己是允许的（运维下线本节点），但移除最后一个成员不是：
// 那会让这个组再也选不出 leader，日志从此无法提交。
func (g *TLSGroup) RemoveMember(id NodeID) error {
	grp := g.g()
	if grp == nil {
		return ErrNotLeader
	}
	if err := grp.RemoveMember(id); err != nil {
		return err
	}
	// 顺手丢掉这个对端的空闲连接（CLU-038 的连接池）。
	//
	// 不丢也不会错——地址比对与空闲淘汰最终都会清掉它——但一个已经被
	// 移出成员的节点不该还占着本节点的文件描述符与它那侧的一个协程。
	g.pool.drop(id)
	return nil
}
