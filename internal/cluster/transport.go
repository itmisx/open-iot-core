package cluster

import (
	"context"
	"sync"
)

// Frame 是节点间转发的一条消息（PRD CLU-038）。
//
// 它携带接收侧路由所需的全部信息：协议版本、源节点、消息序号、主题、QoS、
// 负载，以及可选的目标集合（当发送侧已解析出具体订阅者时可省去接收侧的匹配）。
//
// Seq 与 Origin 共同构成消息的顺序来源：契约 08 禁止用墙上时钟排序，
// 跨节点的先后由 (Origin, Seq) 二元组确定。
type Frame struct {
	Version uint8
	Origin  NodeID
	Seq     uint64
	Topic   string
	Payload []byte
	// Props 是已编码的 MQTT 5 属性，原样透传，避免中间节点反复解析。
	Props  []byte
	QoS    uint8
	Retain bool
	// Targets 可选：发送侧已确定的目标 clientId。为空时由接收侧按本地订阅匹配。
	Targets []string
	// Share 与 Filter 非空时，这一帧是投给某个**共享订阅组**的
	// （MQTT-116）：接收侧只在自己的本地成员里选一个投，不再做跨节点
	// 选取。否则两个各自有成员的节点会把同一条消息来回推——而共享订阅
	// 的语义恰好是"组内只投一个"，弹一次就破了。
	Share  string
	Filter string
}

// ShareCount 是一个节点在某个共享订阅组里的在线成员数（MQTT-116）。
//
// 只传计数而不传 clientId：发布侧要回答的问题是"这一跳该发给谁"，
// 而具体投给组里哪一个成员由那个节点自己决定（它才知道谁此刻在线）。
// 传 clientId 会让每次成员上下线都要跨节点同步一次，而这是高频事件。
type ShareCount struct {
	Share  string
	Filter string
	Online int
}

// FrameVersion 是当前的 mesh 帧格式版本，用于滚动升级期间的兼容判定。
const FrameVersion uint8 = 1

// QueryScope 决定一次 Admin 查询的扇出范围（契约 12）。
type QueryScope uint8

const (
	// ScopeLocalOnly 只查本节点。
	ScopeLocalOnly QueryScope = iota
	// ScopeAllNodes 扇出到全部成员并合并结果。
	ScopeAllNodes
)

// AdminQuery 是一次 Admin 面的扇出查询。
//
// 控制台的查询天然是全集群的——「列出所有在线连接」必须跨节点聚合。
// 这个扇出抽象在 v0.1 就要存在，否则控制台会写成只认本地（契约 12）。
type AdminQuery struct {
	Kind  string
	Scope QueryScope
	// Params 由具体查询解释。
	Params map[string]string
	// Cursor 是跨节点分页游标，编码每个节点各自读到的位置。
	// 它不能是单一 offset，否则合并分页无法正确推进（PRD ARCH-040）。
	Cursor map[NodeID]string
	Limit  int
}

// AdminResult 是单个节点对一次扇出查询的应答。
type AdminResult struct {
	Node NodeID
	// Items 是该节点返回的条目，由具体查询解释其元素类型。
	Items []any
	// NextCursor 是该节点的续读位置，为空表示该节点已读完。
	NextCursor string
	// Err 非空表示该节点失败。扇出层须把它表达为部分失败而非整体失败
	// （PRD ARCH-041）：运维工具在故障时必须仍然可用，那正是最需要它的时刻。
	Err error
}

// Transport 是节点间的消息面与 Admin 扇出通道（契约 07）。
//
// Forward 从 v0.1 起就是批量接口并返回 error，以支持背压与重试决策——
// 事后把单条接口改成批量会牵动所有调用点与流控逻辑。
type Transport interface {
	// Forward 把一批消息转发给若干节点。返回错误表示发送侧应当重试或施加背压。
	Forward(ctx context.Context, to []NodeID, batch []Frame) error
	// OnReceive 注册接收回调。实现须保证回调不被并发调用于同一 from。
	OnReceive(fn func(from NodeID, batch []Frame))
	// Query 执行 Admin 扇出。单个节点失败体现在对应 AdminResult.Err 上，
	// 整体仍返回 nil error。
	Query(ctx context.Context, to []NodeID, q AdminQuery) ([]AdminResult, error)
	// Self 返回本节点 ID。
	Self() NodeID
	Close() error
}

// LoopbackTransport 是 Transport 的单节点实现。
//
// Forward 对本节点以外的目标是空操作——单机下 Route 返回的 remote 恒为空，
// 因此该分支不会进入；但接口与调用点必须存在（契约 02、07）。
// 投向本节点的帧会同步回灌给 OnReceive 回调，使接收路径同样被覆盖。
type LoopbackTransport struct {
	self NodeID

	mu      sync.RWMutex
	recv    func(NodeID, []Frame)
	closed  bool
	queried func(AdminQuery) ([]any, string, error)
}

// NewLoopbackTransport 创建单节点传输。
func NewLoopbackTransport(self NodeID) *LoopbackTransport {
	return &LoopbackTransport{self: self}
}

// SetLocalQuery 注册本节点的 Admin 查询处理器。
func (t *LoopbackTransport) SetLocalQuery(fn func(AdminQuery) ([]any, string, error)) {
	t.mu.Lock()
	t.queried = fn
	t.mu.Unlock()
}

func (t *LoopbackTransport) Forward(_ context.Context, to []NodeID, batch []Frame) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return ErrClosed
	}
	if t.recv == nil || len(batch) == 0 {
		return nil
	}
	for _, n := range to {
		if n == t.self {
			t.recv(t.self, batch)
		}
		// 其余目标在单节点模式下不存在，静默丢弃。
	}
	return nil
}

func (t *LoopbackTransport) OnReceive(fn func(from NodeID, batch []Frame)) {
	t.mu.Lock()
	t.recv = fn
	t.mu.Unlock()
}

func (t *LoopbackTransport) Query(_ context.Context, to []NodeID, q AdminQuery) ([]AdminResult, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return nil, ErrClosed
	}
	// 单节点模式下扇出目标只有自己；游标仍是 map 形式，只是只有一个条目。
	want := false
	if q.Scope == ScopeAllNodes || len(to) == 0 {
		want = true
	} else {
		for _, n := range to {
			if n == t.self {
				want = true
				break
			}
		}
	}
	if !want {
		return nil, nil
	}
	res := AdminResult{Node: t.self}
	if t.queried != nil {
		res.Items, res.NextCursor, res.Err = t.queried(q)
	}
	return []AdminResult{res}, nil
}

func (t *LoopbackTransport) Self() NodeID { return t.self }

func (t *LoopbackTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}
