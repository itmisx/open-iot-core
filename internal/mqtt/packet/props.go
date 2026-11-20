package packet

import (
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// PropID 是 MQTT 5 的属性标识符（MQTT-2.2.2.2）。
type PropID byte

const (
	PropPayloadFormat     PropID = 0x01
	PropMessageExpiry     PropID = 0x02
	PropContentType       PropID = 0x03
	PropResponseTopic     PropID = 0x08
	PropCorrelationData   PropID = 0x09
	PropSubscriptionID    PropID = 0x0B
	PropSessionExpiry     PropID = 0x11
	PropAssignedClientID  PropID = 0x12
	PropServerKeepAlive   PropID = 0x13
	PropAuthMethod        PropID = 0x15
	PropAuthData          PropID = 0x16
	PropRequestProblem    PropID = 0x17
	PropWillDelay         PropID = 0x18
	PropRequestResponse   PropID = 0x19
	PropResponseInfo      PropID = 0x1A
	PropServerReference   PropID = 0x1C
	PropReasonString      PropID = 0x1F
	PropReceiveMaximum    PropID = 0x21
	PropTopicAliasMaximum PropID = 0x22
	PropTopicAlias        PropID = 0x23
	PropMaximumQoS        PropID = 0x24
	PropRetainAvailable   PropID = 0x25
	PropUserProperty      PropID = 0x26
	PropMaximumPacketSize PropID = 0x27
	PropWildcardSubAvail  PropID = 0x28
	PropSubIDAvail        PropID = 0x29
	PropSharedSubAvail    PropID = 0x2A
)

// propCtx 标识属性列表出现的位置。属性只在特定报文中合法（MQTT-2.2.2.2），
// 解码时据此强制校验。
type propCtx uint16

const (
	ctxConnect propCtx = 1 << iota
	ctxConnack
	ctxPublish
	ctxWill
	ctxPubAck // 覆盖 PUBACK、PUBREC、PUBREL、PUBCOMP
	ctxSubscribe
	ctxSubAck // 覆盖 SUBACK、UNSUBACK
	ctxUnsubscribe
	ctxDisconnect
	ctxAuth
)

// propAllowed 把每个属性映射到它允许出现的上下文。取值为 0 表示该标识符未分配。
var propAllowed = [0x2B]propCtx{
	PropPayloadFormat:     ctxPublish | ctxWill,
	PropMessageExpiry:     ctxPublish | ctxWill,
	PropContentType:       ctxPublish | ctxWill,
	PropResponseTopic:     ctxPublish | ctxWill,
	PropCorrelationData:   ctxPublish | ctxWill,
	PropSubscriptionID:    ctxPublish | ctxSubscribe,
	PropSessionExpiry:     ctxConnect | ctxConnack | ctxDisconnect,
	PropAssignedClientID:  ctxConnack,
	PropServerKeepAlive:   ctxConnack,
	PropAuthMethod:        ctxConnect | ctxConnack | ctxAuth,
	PropAuthData:          ctxConnect | ctxConnack | ctxAuth,
	PropRequestProblem:    ctxConnect,
	PropWillDelay:         ctxWill,
	PropRequestResponse:   ctxConnect,
	PropResponseInfo:      ctxConnack,
	PropServerReference:   ctxConnack | ctxDisconnect,
	PropReasonString:      ctxConnack | ctxPubAck | ctxSubAck | ctxDisconnect | ctxAuth,
	PropReceiveMaximum:    ctxConnect | ctxConnack,
	PropTopicAliasMaximum: ctxConnect | ctxConnack,
	PropTopicAlias:        ctxPublish,
	PropMaximumQoS:        ctxConnack,
	PropRetainAvailable:   ctxConnack,
	PropUserProperty: ctxConnect | ctxConnack | ctxPublish | ctxWill | ctxPubAck |
		ctxSubscribe | ctxSubAck | ctxUnsubscribe | ctxDisconnect | ctxAuth,
	PropMaximumPacketSize: ctxConnect | ctxConnack,
	PropWildcardSubAvail:  ctxConnack,
	PropSubIDAvail:        ctxConnack,
	PropSharedSubAvail:    ctxConnack,
}

// UserProperty 是一对名值。两个切片都引用解码缓冲区。
type UserProperty struct {
	Key   []byte
	Value []byte
}

// Properties 保存一个 MQTT 5 属性列表。标量属性的存在性用位掩码记录，
// 以保证结构体本身零分配；字节切片字段以 nil 表示缺失。
//
// Properties 仅在版本为 V5 时编码，V311 下被忽略。
type Properties struct {
	present uint64

	PayloadFormat     byte
	MessageExpiry     uint32
	ContentType       []byte
	ResponseTopic     []byte
	CorrelationData   []byte
	SubscriptionIDs   []uint32
	SessionExpiry     uint32
	AssignedClientID  []byte
	ServerKeepAlive   uint16
	AuthMethod        []byte
	AuthData          []byte
	RequestProblem    byte
	WillDelay         uint32
	RequestResponse   byte
	ResponseInfo      []byte
	ServerReference   []byte
	ReasonString      []byte
	ReceiveMaximum    uint16
	TopicAliasMaximum uint16
	TopicAlias        uint16
	MaximumQoS        byte
	RetainAvailable   byte
	UserProps         []UserProperty
	MaximumPacketSize uint32
	WildcardSubAvail  byte
	SubIDAvail        byte
	SharedSubAvail    byte
}

// Has 报告该属性在线路上是否存在。对字节切片类属性，本方法可区分
// 「缺失」与「存在但为空」——这对 Correlation Data 这类字段很重要。
func (p *Properties) Has(id PropID) bool {
	if id >= 64 {
		return false
	}
	return p.present&(1<<uint(id)) != 0
}

func (p *Properties) set(id PropID) { p.present |= 1 << uint(id) }

// Set 把一个属性标记为存在。手工填充 Properties 用于编码的调用方，
// 必须为每个填写的字段调用 Set。
func (p *Properties) Set(id PropID) { p.set(id) }

// Empty 报告是否没有任何属性存在。
func (p *Properties) Empty() bool { return p.present == 0 }

// Reset 清空 p 以便复用，同时保留切片容量。
func (p *Properties) Reset() {
	subs := p.SubscriptionIDs[:0]
	ups := p.UserProps[:0]
	*p = Properties{}
	p.SubscriptionIDs = subs
	p.UserProps = ups
}

// decodeProps 在给定上下文下从 r 读取一个属性列表。
func decodeProps(r *reader, ctx propCtx, p *Properties) error {
	n, err := r.varint()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	pr, err := r.sub(n)
	if err != nil {
		return err
	}
	for !pr.done() {
		raw, err := pr.byte()
		if err != nil {
			return err
		}
		id := PropID(raw)
		if int(id) >= len(propAllowed) || propAllowed[id] == 0 {
			return malformed("unknown property 0x%02x", raw)
		}
		if propAllowed[id]&ctx == 0 {
			return malformed("property 0x%02x not valid in this packet", raw)
		}
		// 只有 User Property 与 Subscription Identifier 允许重复出现。
		if id != PropUserProperty && id != PropSubscriptionID && p.Has(id) {
			return protoErr("duplicate property 0x%02x", raw)
		}
		if err := decodeOneProp(pr, id, p); err != nil {
			return err
		}
		p.set(id)
	}
	return nil
}

func decodeOneProp(r *reader, id PropID, p *Properties) error {
	switch id {
	case PropPayloadFormat:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("payload format indicator must be 0 or 1, got %d", v)
		}
		p.PayloadFormat = v
	case PropMessageExpiry:
		v, err := r.uint32()
		if err != nil {
			return err
		}
		p.MessageExpiry = v
	case PropContentType:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.ContentType = v
	case PropResponseTopic:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.ResponseTopic = v
	case PropCorrelationData:
		v, err := r.bin()
		if err != nil {
			return err
		}
		p.CorrelationData = v
	case PropSubscriptionID:
		v, err := r.varint()
		if err != nil {
			return err
		}
		if v == 0 {
			return protoErr("subscription identifier must not be 0")
		}
		p.SubscriptionIDs = append(p.SubscriptionIDs, uint32(v))
	case PropSessionExpiry:
		v, err := r.uint32()
		if err != nil {
			return err
		}
		p.SessionExpiry = v
	case PropAssignedClientID:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.AssignedClientID = v
	case PropServerKeepAlive:
		v, err := r.uint16()
		if err != nil {
			return err
		}
		p.ServerKeepAlive = v
	case PropAuthMethod:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.AuthMethod = v
	case PropAuthData:
		v, err := r.bin()
		if err != nil {
			return err
		}
		p.AuthData = v
	case PropRequestProblem:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("request problem information must be 0 or 1, got %d", v)
		}
		p.RequestProblem = v
	case PropWillDelay:
		v, err := r.uint32()
		if err != nil {
			return err
		}
		p.WillDelay = v
	case PropRequestResponse:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("request response information must be 0 or 1, got %d", v)
		}
		p.RequestResponse = v
	case PropResponseInfo:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.ResponseInfo = v
	case PropServerReference:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.ServerReference = v
	case PropReasonString:
		v, err := r.str()
		if err != nil {
			return err
		}
		p.ReasonString = v
	case PropReceiveMaximum:
		v, err := r.uint16()
		if err != nil {
			return err
		}
		if v == 0 {
			return protoErr("receive maximum must not be 0")
		}
		p.ReceiveMaximum = v
	case PropTopicAliasMaximum:
		v, err := r.uint16()
		if err != nil {
			return err
		}
		p.TopicAliasMaximum = v
	case PropTopicAlias:
		v, err := r.uint16()
		if err != nil {
			return err
		}
		if v == 0 {
			return codeErr(reason.TopicAliasInvalid, "topic alias must not be 0")
		}
		p.TopicAlias = v
	case PropMaximumQoS:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("maximum QoS must be 0 or 1, got %d", v)
		}
		p.MaximumQoS = v
	case PropRetainAvailable:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("retain available must be 0 or 1, got %d", v)
		}
		p.RetainAvailable = v
	case PropUserProperty:
		k, err := r.str()
		if err != nil {
			return err
		}
		val, err := r.str()
		if err != nil {
			return err
		}
		p.UserProps = append(p.UserProps, UserProperty{Key: k, Value: val})
	case PropMaximumPacketSize:
		v, err := r.uint32()
		if err != nil {
			return err
		}
		if v == 0 {
			return protoErr("maximum packet size must not be 0")
		}
		p.MaximumPacketSize = v
	case PropWildcardSubAvail:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("wildcard subscription available must be 0 or 1, got %d", v)
		}
		p.WildcardSubAvail = v
	case PropSubIDAvail:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("subscription identifier available must be 0 or 1, got %d", v)
		}
		p.SubIDAvail = v
	case PropSharedSubAvail:
		v, err := r.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return protoErr("shared subscription available must be 0 or 1, got %d", v)
		}
		p.SharedSubAvail = v
	default:
		return malformed("unhandled property 0x%02x", byte(id))
	}
	return nil
}

// propsLen 返回属性列表自身编码后的字节长度，不含其变长整数长度前缀。
func (p *Properties) propsLen() int {
	if p == nil || p.present == 0 {
		return 0
	}
	n := 0
	if p.Has(PropPayloadFormat) {
		n += 2
	}
	if p.Has(PropMessageExpiry) {
		n += 5
	}
	if p.Has(PropContentType) {
		n += 3 + len(p.ContentType)
	}
	if p.Has(PropResponseTopic) {
		n += 3 + len(p.ResponseTopic)
	}
	if p.Has(PropCorrelationData) {
		n += 3 + len(p.CorrelationData)
	}
	if p.Has(PropSubscriptionID) {
		for _, id := range p.SubscriptionIDs {
			n += 1 + varintLen(int(id))
		}
	}
	if p.Has(PropSessionExpiry) {
		n += 5
	}
	if p.Has(PropAssignedClientID) {
		n += 3 + len(p.AssignedClientID)
	}
	if p.Has(PropServerKeepAlive) {
		n += 3
	}
	if p.Has(PropAuthMethod) {
		n += 3 + len(p.AuthMethod)
	}
	if p.Has(PropAuthData) {
		n += 3 + len(p.AuthData)
	}
	if p.Has(PropRequestProblem) {
		n += 2
	}
	if p.Has(PropWillDelay) {
		n += 5
	}
	if p.Has(PropRequestResponse) {
		n += 2
	}
	if p.Has(PropResponseInfo) {
		n += 3 + len(p.ResponseInfo)
	}
	if p.Has(PropServerReference) {
		n += 3 + len(p.ServerReference)
	}
	if p.Has(PropReasonString) {
		n += 3 + len(p.ReasonString)
	}
	if p.Has(PropReceiveMaximum) {
		n += 3
	}
	if p.Has(PropTopicAliasMaximum) {
		n += 3
	}
	if p.Has(PropTopicAlias) {
		n += 3
	}
	if p.Has(PropMaximumQoS) {
		n += 2
	}
	if p.Has(PropRetainAvailable) {
		n += 2
	}
	if p.Has(PropUserProperty) {
		for _, up := range p.UserProps {
			n += 5 + len(up.Key) + len(up.Value)
		}
	}
	if p.Has(PropMaximumPacketSize) {
		n += 5
	}
	if p.Has(PropWildcardSubAvail) {
		n += 2
	}
	if p.Has(PropSubIDAvail) {
		n += 2
	}
	if p.Has(PropSharedSubAvail) {
		n += 2
	}
	return n
}

// encodedLen 返回属性列表含长度前缀的总字节数。
// 即使列表为空也仍占一个字节（长度 0）。
func (p *Properties) encodedLen() int {
	n := p.propsLen()
	return varintLen(n) + n
}

// appendTo 写出属性列表及其长度前缀。
func (p *Properties) appendTo(dst []byte) []byte {
	n := p.propsLen()
	dst = appendVarint(dst, n)
	if n == 0 {
		return dst
	}
	if p.Has(PropPayloadFormat) {
		dst = append(dst, byte(PropPayloadFormat), p.PayloadFormat)
	}
	if p.Has(PropMessageExpiry) {
		dst = appendUint32(append(dst, byte(PropMessageExpiry)), p.MessageExpiry)
	}
	if p.Has(PropContentType) {
		dst = appendBin(append(dst, byte(PropContentType)), p.ContentType)
	}
	if p.Has(PropResponseTopic) {
		dst = appendBin(append(dst, byte(PropResponseTopic)), p.ResponseTopic)
	}
	if p.Has(PropCorrelationData) {
		dst = appendBin(append(dst, byte(PropCorrelationData)), p.CorrelationData)
	}
	if p.Has(PropSubscriptionID) {
		for _, id := range p.SubscriptionIDs {
			dst = appendVarint(append(dst, byte(PropSubscriptionID)), int(id))
		}
	}
	if p.Has(PropSessionExpiry) {
		dst = appendUint32(append(dst, byte(PropSessionExpiry)), p.SessionExpiry)
	}
	if p.Has(PropAssignedClientID) {
		dst = appendBin(append(dst, byte(PropAssignedClientID)), p.AssignedClientID)
	}
	if p.Has(PropServerKeepAlive) {
		dst = appendUint16(append(dst, byte(PropServerKeepAlive)), p.ServerKeepAlive)
	}
	if p.Has(PropAuthMethod) {
		dst = appendBin(append(dst, byte(PropAuthMethod)), p.AuthMethod)
	}
	if p.Has(PropAuthData) {
		dst = appendBin(append(dst, byte(PropAuthData)), p.AuthData)
	}
	if p.Has(PropRequestProblem) {
		dst = append(dst, byte(PropRequestProblem), p.RequestProblem)
	}
	if p.Has(PropWillDelay) {
		dst = appendUint32(append(dst, byte(PropWillDelay)), p.WillDelay)
	}
	if p.Has(PropRequestResponse) {
		dst = append(dst, byte(PropRequestResponse), p.RequestResponse)
	}
	if p.Has(PropResponseInfo) {
		dst = appendBin(append(dst, byte(PropResponseInfo)), p.ResponseInfo)
	}
	if p.Has(PropServerReference) {
		dst = appendBin(append(dst, byte(PropServerReference)), p.ServerReference)
	}
	if p.Has(PropReasonString) {
		dst = appendBin(append(dst, byte(PropReasonString)), p.ReasonString)
	}
	if p.Has(PropReceiveMaximum) {
		dst = appendUint16(append(dst, byte(PropReceiveMaximum)), p.ReceiveMaximum)
	}
	if p.Has(PropTopicAliasMaximum) {
		dst = appendUint16(append(dst, byte(PropTopicAliasMaximum)), p.TopicAliasMaximum)
	}
	if p.Has(PropTopicAlias) {
		dst = appendUint16(append(dst, byte(PropTopicAlias)), p.TopicAlias)
	}
	if p.Has(PropMaximumQoS) {
		dst = append(dst, byte(PropMaximumQoS), p.MaximumQoS)
	}
	if p.Has(PropRetainAvailable) {
		dst = append(dst, byte(PropRetainAvailable), p.RetainAvailable)
	}
	if p.Has(PropUserProperty) {
		for _, up := range p.UserProps {
			dst = append(dst, byte(PropUserProperty))
			dst = appendBin(dst, up.Key)
			dst = appendBin(dst, up.Value)
		}
	}
	if p.Has(PropMaximumPacketSize) {
		dst = appendUint32(append(dst, byte(PropMaximumPacketSize)), p.MaximumPacketSize)
	}
	if p.Has(PropWildcardSubAvail) {
		dst = append(dst, byte(PropWildcardSubAvail), p.WildcardSubAvail)
	}
	if p.Has(PropSubIDAvail) {
		dst = append(dst, byte(PropSubIDAvail), p.SubIDAvail)
	}
	if p.Has(PropSharedSubAvail) {
		dst = append(dst, byte(PropSharedSubAvail), p.SharedSubAvail)
	}
	return dst
}

// Clone 返回一个深拷贝，其字节切片不再引用任何解码缓冲区。
func (p *Properties) Clone() Properties {
	if p == nil {
		return Properties{}
	}
	c := *p
	c.ContentType = cloneBytes(p.ContentType)
	c.ResponseTopic = cloneBytes(p.ResponseTopic)
	c.CorrelationData = cloneBytes(p.CorrelationData)
	c.AssignedClientID = cloneBytes(p.AssignedClientID)
	c.AuthMethod = cloneBytes(p.AuthMethod)
	c.AuthData = cloneBytes(p.AuthData)
	c.ResponseInfo = cloneBytes(p.ResponseInfo)
	c.ServerReference = cloneBytes(p.ServerReference)
	c.ReasonString = cloneBytes(p.ReasonString)
	if p.SubscriptionIDs != nil {
		c.SubscriptionIDs = append([]uint32(nil), p.SubscriptionIDs...)
	}
	if p.UserProps != nil {
		c.UserProps = make([]UserProperty, len(p.UserProps))
		for i, up := range p.UserProps {
			c.UserProps[i] = UserProperty{Key: cloneBytes(up.Key), Value: cloneBytes(up.Value)}
		}
	}
	return c
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// AppendTo 把属性列表及其长度前缀写入 dst，是 appendTo 的导出形式。
//
// 它用于节点间与离线队列中透传属性：属性列表自带长度前缀，因此
// 单独编码出来的字节串是自描述的，可以原样保存与还原，
// 不必在每一跳重新解析（见 cluster.Frame.Props）。
func (p *Properties) AppendTo(dst []byte) []byte { return p.appendTo(dst) }

// EncodedLen 返回属性列表含长度前缀的总字节数。
func (p *Properties) EncodedLen() int { return p.encodedLen() }

// DecodeProps 把 AppendTo 产出的字节串还原为属性列表。
//
// ctx 决定哪些属性合法；跨节点透传与离线队列用 PublishContext。
func DecodeProps(raw []byte, ctx PropContext, dst *Properties) error {
	r := &reader{b: raw}
	if err := decodeProps(r, propCtx(ctx), dst); err != nil {
		return err
	}
	if !r.done() {
		return malformed("属性列表后有 %d 字节多余内容", r.remaining())
	}
	return nil
}

// PropContext 是属性列表的合法上下文，是 propCtx 的导出形式。
type PropContext uint16

// 可供外部使用的属性上下文。
const (
	// PublishContext 是 PUBLISH 报文的属性上下文。
	PublishContext = PropContext(ctxPublish)
	// WillContext 是遗嘱的属性上下文。
	WillContext = PropContext(ctxWill)
)
