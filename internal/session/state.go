// Package session 实现会话状态与会话运行时的分离（契约 04）。
//
// 这是契约清单里事后代价最高的一条。绝大多数单机 broker 把 net.Conn、
// chan、sync.Mutex 直接塞进 session 结构体；一旦如此，持久会话落盘、
// 会话迁移、跨节点接管、控制台的会话快照全部无从下手。而会话结构体是
// 全系统引用最密集的类型，拆分它意味着协议状态机、投递、保活、流控、
// 持久化、指标采集全改一遍，且极易引入并发竞态——等于重写 broker。
//
// 因此本包把两者彻底分开：
//   - SessionState 是纯数据，可序列化、可复制、可跨节点搬运；
//   - SessionRuntime 持有连接与 goroutine，不可序列化，生命周期短于 State。
package session

import "github.com/itmisx/open-iot-core/internal/cluster"

// InflightState 是一条出站未确认消息所处的阶段。
type InflightState uint8

const (
	// AwaitPuback 是 QoS 1 已发出、等待 PUBACK。
	AwaitPuback InflightState = iota
	// AwaitPubrec 是 QoS 2 已发出、等待 PUBREC。
	AwaitPubrec
	// AwaitPubcomp 是 QoS 2 已发 PUBREL、等待 PUBCOMP。
	AwaitPubcomp
)

// Subscription 是会话持有的一条订阅。
type Subscription struct {
	Filter string
	// Share 非空表示这是共享订阅的成员，Filter 为其内层过滤器。
	// 共享订阅的组成员由契约 10 的 SharedGroups 管理，此处仅记录以便重放。
	Share          string
	QoS            uint8
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling uint8
	ID             uint32
}

// InflightMsg 是一条出站未确认消息。
type InflightMsg struct {
	PacketID uint16
	QoS      uint8
	Dup      bool
	State    InflightState
	Topic    string
	Payload  []byte
	// Props 是已编码的 MQTT 5 属性，原样保存以便重传时逐字节一致。
	Props []byte
	// Retries 是已重传次数，用于退避与放弃判定。
	Retries uint16
	// SentUnixMs 是最近一次发出的时刻。ExpiryUnixMs 是放弃重试的时刻：
	// 首次发出后的重试预算与消息过期时刻取较早者，0 表示不放弃。
	// 两者都只用于超时与过期判定，禁止用于比较消息先后（契约 08）。
	// 消息顺序由 (Origin, Seq) 确定。
	SentUnixMs   int64
	ExpiryUnixMs int64
	// Origin 与 Seq 是消息的顺序来源。
	Origin cluster.NodeID
	Seq    uint64
}

// QueuedMsg 是一条离线队列中的消息。
type QueuedMsg struct {
	QoS     uint8
	Retain  bool
	Topic   string
	Payload []byte
	Props   []byte
	// SubIDs 是命中的订阅标识符，投递时须随 PUBLISH 回传。
	SubIDs       []uint32
	ExpiryUnixMs int64
	Origin       cluster.NodeID
	Seq          uint64
}

// TopicAlias 是一条主题别名映射。
type TopicAlias struct {
	Alias uint16
	Topic string
}

// WillMsg 是遗嘱消息。
type WillMsg struct {
	Topic         string
	Payload       []byte
	Props         []byte
	QoS           uint8
	Retain        bool
	DelaySeconds  uint32
	ExpirySeconds uint32
}

// Props 是 MQTT 5 协商出的会话级参数。
type Props struct {
	ReceiveMaximum    uint16
	MaximumPacketSize uint32
	TopicAliasMaximum uint16
	RequestProblem    bool
	RequestResponse   bool
	// InboundAliases 是客户端建立的别名表（客户端发往服务端方向）。
	InboundAliases []TopicAlias
	// OutboundAliases 是服务端建立的别名表。
	OutboundAliases []TopicAlias
}

// State 是一个 MQTT 会话的全部持久状态。
//
// 本结构体必须保持为纯数据：禁止出现 net.Conn、chan、func、sync.*、
// unsafe.Pointer，也不使用指针字段（遗嘱以 HasWill 加值字段表达，
// 而非 *WillMsg），以便 CI 的字段类型白名单检查足够简单可靠
// （PRD ARCH-018、ARCH-053、TST-058）。
//
// 一切涉及会话状态变更的操作都必须携带并校验 Epoch，单机下的本地操作
// 也不例外（PRD ARCH-011）。
type State struct {
	ClientID string
	// Epoch 是本会话持有的所有权围栏令牌（契约 01）。
	Epoch cluster.Epoch
	// Owner 是当前承载本会话的节点。
	Owner cluster.NodeID
	// ProtoVer 是协议级别：4 为 3.1.1，5 为 5.0。
	ProtoVer uint8

	CleanStart bool
	// ExpiryInterval 是会话过期间隔（秒）。0 表示连接断开即删除。
	ExpiryInterval uint32
	KeepAlive      uint16

	// Thing 是认证时解析出的设备名，为空表示该连接未绑定 Thing。
	// 策略变量求值会用到它（PRD AUTH-031）。
	Thing string

	Subscriptions []Subscription
	// Inflight 是出站未确认消息，按发送顺序排列。
	Inflight []InflightMsg
	// Queued 是离线队列。
	Queued []QueuedMsg
	// InboundUnacked 是入站未确认的 QoS 2 报文标识符，用于去重。
	InboundUnacked []uint16

	HasWill bool
	Will    WillMsg

	Props Props

	// 时间字段仅用于 TTL、超时与展示，禁止用于比较先后（契约 08）。
	CreatedUnixMs    int64
	DisconnectUnixMs int64
}

// IsV5 报告会话是否为 MQTT 5。
func (s *State) IsV5() bool { return s.ProtoVer == 5 }

// Expired 报告会话是否已超过过期间隔。nowUnixMs 由注入的时钟提供。
func (s *State) Expired(nowUnixMs int64) bool {
	if s.DisconnectUnixMs == 0 {
		return false // 仍在线
	}
	if s.ExpiryInterval == 0 {
		return true
	}
	return nowUnixMs-s.DisconnectUnixMs >= int64(s.ExpiryInterval)*1000
}

// CheckEpoch 校验操作携带的 epoch 是否仍然有效。
//
// 小于当前 epoch 表示本会话已被接管，操作必须被拒绝；
// 大于当前 epoch 表示调用方持有更新的授权，属调用方错误。
func (s *State) CheckEpoch(e cluster.Epoch) error {
	if e != s.Epoch {
		return cluster.ErrStaleEpoch
	}
	return nil
}

// Clone 返回一份深拷贝，其切片与原状态不共享底层数组。
// 会话迁移与控制台快照需要它。
func (s *State) Clone() *State {
	c := *s
	c.Subscriptions = append([]Subscription(nil), s.Subscriptions...)
	c.InboundUnacked = append([]uint16(nil), s.InboundUnacked...)
	c.Inflight = make([]InflightMsg, len(s.Inflight))
	for i, m := range s.Inflight {
		m.Payload = cloneBytes(m.Payload)
		m.Props = cloneBytes(m.Props)
		c.Inflight[i] = m
	}
	c.Queued = make([]QueuedMsg, len(s.Queued))
	for i, m := range s.Queued {
		m.Payload = cloneBytes(m.Payload)
		m.Props = cloneBytes(m.Props)
		m.SubIDs = append([]uint32(nil), m.SubIDs...)
		c.Queued[i] = m
	}
	c.Will.Payload = cloneBytes(s.Will.Payload)
	c.Will.Props = cloneBytes(s.Will.Props)
	c.Props.InboundAliases = append([]TopicAlias(nil), s.Props.InboundAliases...)
	c.Props.OutboundAliases = append([]TopicAlias(nil), s.Props.OutboundAliases...)
	return &c
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
