package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
//   - Things 是发送方此刻承载的设备**全量快照**，用于设备域主题的归属查询。
type MeshPayload struct {
	Frames  []Frame
	Filters []string
	Things  []string
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
		p.Things == nil && p.Shares == nil &&
		len(p.Evict) == 0 && len(p.Retained) == 0
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

	mu   sync.RWMutex
	recv func(NodeID, []Frame)
}

// NewMeshTransport 包住一个复制组。
func NewMeshTransport(g *TLSGroup) *MeshTransport {
	t := &MeshTransport{group: g, self: g.id}
	return t
}

// Forward 把一批帧转发给若干节点。
//
// 任一节点失败即返回错误（调用方据此记一次投递失败并施加背压）；
// 已经成功的那几个不回滚——消息面是至少一次语义，回滚没有意义。
func (t *MeshTransport) Forward(_ context.Context, to []NodeID, batch []Frame) error {
	if len(batch) == 0 || len(to) == 0 {
		return nil
	}
	var firstErr error
	for _, node := range to {
		if node == t.self {
			continue
		}
		if err := t.group.SendMesh(node, MeshPayload{Frames: batch}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

// Close 不关复制组：组的生命周期由装配层管（它还承载选举与日志）。
func (t *MeshTransport) Close() error { return nil }

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
	return grp.RemoveMember(id)
}
