package packet

import "github.com/itmisx/open-iot-core/internal/mqtt/reason"

// Will 是 CONNECT 中携带的遗嘱消息。
type Will struct {
	Topic   []byte
	Payload []byte
	QoS     QoS
	Retain  bool
	Props   Properties // 仅 V5；WillDelay 存放于此
}

// Connect 是 CONNECT 报文（MQTT-3.1）。
type Connect struct {
	Version     Version
	ClientID    []byte
	KeepAlive   uint16
	CleanStart  bool // 在 3.1.1 中即 CleanSession
	Username    []byte
	Password    []byte
	HasUsername bool
	HasPassword bool
	Will        *Will
	Props       Properties
}

func (*Connect) Type() Type { return CONNECT }

// Connack 是 CONNACK 报文（MQTT-3.2）。
type Connack struct {
	SessionPresent bool
	Code           reason.Code
	Props          Properties
}

func (*Connack) Type() Type { return CONNACK }

// Publish 是 PUBLISH 报文（MQTT-3.3）。
type Publish struct {
	Topic    []byte
	Payload  []byte
	PacketID uint16
	QoS      QoS
	Retain   bool
	Dup      bool
	Props    Properties
}

func (*Publish) Type() Type { return PUBLISH }

// Ack 覆盖 PUBACK、PUBREC、PUBREL、PUBCOMP 四种共享同一线路形状的报文。
// Kind 选择具体类型，编码前必须设置。
type Ack struct {
	Kind     Type
	PacketID uint16
	Code     reason.Code
	Props    Properties
}

func (a *Ack) Type() Type { return a.Kind }

// Subscription 是 SUBSCRIBE 负载中的一个条目。
type Subscription struct {
	Filter            []byte
	QoS               QoS
	NoLocal           bool // 仅 V5
	RetainAsPublished bool // 仅 V5
	RetainHandling    byte // 仅 V5：0 发送，1 仅新订阅时发送，2 不发送
	// ID 是作用于该过滤器的订阅标识符。它不在此处编码（实际位于报文属性中），
	// 但服务端解析出来后随过滤器一起携带会很方便。
	ID uint32
}

// Subscribe 是 SUBSCRIBE 报文（MQTT-3.8）。
type Subscribe struct {
	PacketID uint16
	Filters  []Subscription
	Props    Properties
}

func (*Subscribe) Type() Type { return SUBSCRIBE }

// Suback 是 SUBACK 报文（MQTT-3.9）。
type Suback struct {
	PacketID uint16
	Codes    []reason.Code
	Props    Properties
}

func (*Suback) Type() Type { return SUBACK }

// Unsubscribe 是 UNSUBSCRIBE 报文（MQTT-3.10）。
type Unsubscribe struct {
	PacketID uint16
	Filters  [][]byte
	Props    Properties
}

func (*Unsubscribe) Type() Type { return UNSUBSCRIBE }

// Unsuback 是 UNSUBACK 报文（MQTT-3.11）。Codes 仅 V5 有。
type Unsuback struct {
	PacketID uint16
	Codes    []reason.Code
	Props    Properties
}

func (*Unsuback) Type() Type { return UNSUBACK }

// Pingreq 是 PINGREQ 报文（MQTT-3.12）。
type Pingreq struct{}

func (*Pingreq) Type() Type { return PINGREQ }

// Pingresp 是 PINGRESP 报文（MQTT-3.13）。
type Pingresp struct{}

func (*Pingresp) Type() Type { return PINGRESP }

// Disconnect 是 DISCONNECT 报文（MQTT-3.14）。Code 与 Props 仅 V5 有。
type Disconnect struct {
	Code  reason.Code
	Props Properties
}

func (*Disconnect) Type() Type { return DISCONNECT }

// Auth 是 AUTH 报文（MQTT-3.15），仅 V5。
type Auth struct {
	Code  reason.Code
	Props Properties
}

func (*Auth) Type() Type { return AUTH }

// --- 深拷贝 ---

// Clone 返回 p 的副本，其字节切片不再引用解码缓冲区。
// 若服务端需要让一条发布消息存活到读循环之外，必须先克隆。
func (p *Publish) Clone() *Publish {
	c := *p
	c.Topic = cloneBytes(p.Topic)
	c.Payload = cloneBytes(p.Payload)
	c.Props = p.Props.Clone()
	return &c
}

// Clone 返回 c 的副本，其字节切片不再引用解码缓冲区。
func (c *Connect) Clone() *Connect {
	n := *c
	n.ClientID = cloneBytes(c.ClientID)
	n.Username = cloneBytes(c.Username)
	n.Password = cloneBytes(c.Password)
	n.Props = c.Props.Clone()
	if c.Will != nil {
		w := *c.Will
		w.Topic = cloneBytes(c.Will.Topic)
		w.Payload = cloneBytes(c.Will.Payload)
		w.Props = c.Will.Props.Clone()
		n.Will = &w
	}
	return &n
}

// Clone 返回 s 的副本，其过滤器不再引用解码缓冲区。
func (s *Subscribe) Clone() *Subscribe {
	n := *s
	n.Props = s.Props.Clone()
	if s.Filters != nil {
		n.Filters = make([]Subscription, len(s.Filters))
		for i, f := range s.Filters {
			n.Filters[i] = f
			n.Filters[i].Filter = cloneBytes(f.Filter)
		}
	}
	return &n
}

// Clone 返回 u 的副本，其过滤器不再引用解码缓冲区。
func (u *Unsubscribe) Clone() *Unsubscribe {
	n := *u
	n.Props = u.Props.Clone()
	if u.Filters != nil {
		n.Filters = make([][]byte, len(u.Filters))
		for i, f := range u.Filters {
			n.Filters[i] = cloneBytes(f)
		}
	}
	return &n
}

// --- 订阅选项字节编码 ---

const (
	subOptNoLocal    = 1 << 2
	subOptRAP        = 1 << 3
	subOptRHShift    = 4
	subOptRHMask     = 0x30
	subOptReserved   = 0xC0
	retainHandlingNo = 2
)

func (s Subscription) optionByte(v Version) byte {
	b := byte(s.QoS)
	if v != V5 {
		return b
	}
	if s.NoLocal {
		b |= subOptNoLocal
	}
	if s.RetainAsPublished {
		b |= subOptRAP
	}
	b |= s.RetainHandling << subOptRHShift
	return b
}
