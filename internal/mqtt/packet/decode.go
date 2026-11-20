package packet

import (
	"bytes"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

var protocolName = []byte("MQTT")

// Decode 从 b 的开头读取一个完整报文，返回该报文与消费的总字节数，
// 以便调用方推进流式缓冲区。当 b 尚不足一个完整报文时返回 ErrIncomplete。
//
// v 是 CONNECT 协商出的协议版本。解码 CONNECT 时该参数被忽略，
// 因为 CONNECT 自身携带协议级别。
//
// 返回报文中的字节切片字段引用 b，详见包注释。
func Decode(b []byte, v Version) (Packet, int, error) {
	fh, n, err := ReadFixedHeader(b)
	if err != nil {
		return nil, 0, err
	}
	total := n + fh.Length
	if len(b) < total {
		return nil, 0, ErrIncomplete
	}
	p, err := DecodeBody(fh, b[n:total], v)
	if err != nil {
		return nil, 0, err
	}
	return p, total, nil
}

// ErrIncomplete 表示还需要更多字节才能解出一个报文，它不是协议违规。
var ErrIncomplete = &Error{Code: reason.Success, Msg: "incomplete packet"}

// DecodeBody 解码一个固定报头已读取完毕的报文的可变报头与负载。
// body 的长度必须恰好等于 fh.Length。
func DecodeBody(fh FixedHeader, body []byte, v Version) (Packet, error) {
	if len(body) != fh.Length {
		return nil, malformed("body length %d does not match remaining length %d",
			len(body), fh.Length)
	}
	r := &reader{b: body}
	var (
		p   Packet
		err error
	)
	switch fh.Type {
	case CONNECT:
		p, err = decodeConnect(fh, r)
	case CONNACK:
		p, err = decodeConnack(fh, r, v)
	case PUBLISH:
		p, err = decodePublish(fh, r, v)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		p, err = decodeAck(fh, r, v)
	case SUBSCRIBE:
		p, err = decodeSubscribe(fh, r, v)
	case SUBACK:
		p, err = decodeSuback(fh, r, v)
	case UNSUBSCRIBE:
		p, err = decodeUnsubscribe(fh, r, v)
	case UNSUBACK:
		p, err = decodeUnsuback(fh, r, v)
	case PINGREQ:
		p, err = decodeEmpty(fh, r, &Pingreq{})
	case PINGRESP:
		p, err = decodeEmpty(fh, r, &Pingresp{})
	case DISCONNECT:
		p, err = decodeDisconnect(fh, r, v)
	case AUTH:
		p, err = decodeAuth(fh, r, v)
	default:
		return nil, malformed("unhandled packet type %d", fh.Type)
	}
	if err != nil {
		return nil, err
	}
	// PUBLISH 把剩余部分整体作为负载，因此只有其他类型才可能出现未消费的尾部字节。
	if fh.Type != PUBLISH && !r.done() {
		return nil, malformed("%s has %d trailing bytes", fh.Type, r.remaining())
	}
	return p, nil
}

// requireFlags 拒绝固定报头标志位与 want 不符的报文。
func requireFlags(fh FixedHeader, want byte) error {
	if fh.Flags != want {
		return malformed("%s reserved flags must be 0x%x, got 0x%x", fh.Type, want, fh.Flags)
	}
	return nil
}

func decodeConnect(fh FixedHeader, r *reader) (*Connect, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	name, err := r.str()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(name, protocolName) {
		// MQTT 3.1 使用的是 "MQIsdp"；除此之外的名称都不是本服务支持的协议。
		return nil, codeErr(reason.UnsupportedProtocolVersion,
			"protocol name %q is not MQTT", name)
	}
	lvl, err := r.byte()
	if err != nil {
		return nil, err
	}
	ver := Version(lvl)
	if !ver.Supported() {
		return nil, codeErr(reason.UnsupportedProtocolVersion,
			"protocol level %d is not supported", lvl)
	}
	flags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if flags&0x01 != 0 {
		return nil, malformed("CONNECT reserved flag bit 0 must be 0")
	}
	c := &Connect{
		Version:     ver,
		CleanStart:  flags&0x02 != 0,
		HasPassword: flags&0x40 != 0,
		HasUsername: flags&0x80 != 0,
	}
	willFlag := flags&0x04 != 0
	willQoS := QoS(flags >> 3 & 0x03)
	willRetain := flags&0x20 != 0
	if !willFlag {
		if willQoS != QoS0 {
			return nil, malformed("will QoS must be 0 when will flag is clear")
		}
		if willRetain {
			return nil, malformed("will retain must be 0 when will flag is clear")
		}
	}
	if !willQoS.Valid() {
		return nil, malformed("invalid will QoS %d", willQoS)
	}
	// MQTT 3.1.1 禁止只有密码而没有用户名；MQTT 5 允许。
	if ver == V311 && c.HasPassword && !c.HasUsername {
		return nil, malformed("password flag set without username flag")
	}
	if c.KeepAlive, err = r.uint16(); err != nil {
		return nil, err
	}
	if ver == V5 {
		if err := decodeProps(r, ctxConnect, &c.Props); err != nil {
			return nil, err
		}
		// AUTH 交换要求提供认证方法；只有认证数据而没有方法属于协议错误
		// （MQTT-3.1.2.32）。
		if c.Props.Has(PropAuthData) && !c.Props.Has(PropAuthMethod) {
			return nil, protoErr("authentication data without authentication method")
		}
	}
	if c.ClientID, err = r.str(); err != nil {
		return nil, err
	}
	// 在 3.1.1 中，零长度客户端标识符要求 clean session；
	// 在 MQTT 5 中由服务端分配一个。
	if len(c.ClientID) == 0 && ver == V311 && !c.CleanStart {
		return nil, codeErr(reason.ClientIdentifierNotValid,
			"empty client identifier requires clean session")
	}
	if willFlag {
		w := &Will{QoS: willQoS, Retain: willRetain}
		if ver == V5 {
			if err := decodeProps(r, ctxWill, &w.Props); err != nil {
				return nil, err
			}
		}
		if w.Topic, err = r.str(); err != nil {
			return nil, err
		}
		if len(w.Topic) == 0 {
			return nil, malformed("will topic must not be empty")
		}
		if hasWildcard(w.Topic) {
			return nil, codeErr(reason.TopicNameInvalid, "will topic contains a wildcard")
		}
		if w.Payload, err = r.bin(); err != nil {
			return nil, err
		}
		c.Will = w
	}
	if c.HasUsername {
		if c.Username, err = r.str(); err != nil {
			return nil, err
		}
	}
	if c.HasPassword {
		if c.Password, err = r.bin(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func decodeConnack(fh FixedHeader, r *reader, v Version) (*Connack, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	ackFlags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if ackFlags&0xFE != 0 {
		return nil, malformed("CONNACK reserved acknowledge flags must be 0")
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	c := &Connack{SessionPresent: ackFlags&0x01 != 0}
	if v == V5 {
		c.Code = reason.Code(code)
		if err := decodeProps(r, ctxConnack, &c.Props); err != nil {
			return nil, err
		}
		return c, nil
	}
	// 3.1.1 的 CONNACK 携带的是返回码而非原因码，两者数值空间不同，
	// 必须提升到原因码空间，否则上层会读到无关语义（见 reason.MapFrom311）。
	c.Code = reason.MapFrom311(reason.Return311(code))
	return c, nil
}

func decodePublish(fh FixedHeader, r *reader, v Version) (*Publish, error) {
	q := QoS(fh.Flags >> 1 & 0x03)
	if !q.Valid() {
		return nil, malformed("invalid PUBLISH QoS 3")
	}
	p := &Publish{
		QoS:    q,
		Dup:    fh.Flags&0x08 != 0,
		Retain: fh.Flags&0x01 != 0,
	}
	if q == QoS0 && p.Dup {
		return nil, malformed("DUP must be 0 for QoS 0 PUBLISH")
	}
	var err error
	if p.Topic, err = r.str(); err != nil {
		return nil, err
	}
	if hasWildcard(p.Topic) {
		return nil, codeErr(reason.TopicNameInvalid, "PUBLISH topic contains a wildcard")
	}
	if q > QoS0 {
		if p.PacketID, err = r.uint16(); err != nil {
			return nil, err
		}
		if p.PacketID == 0 {
			return nil, protoErr("packet identifier must not be 0")
		}
	}
	if v == V5 {
		if err := decodeProps(r, ctxPublish, &p.Props); err != nil {
			return nil, err
		}
		// 零长度主题只有在由主题别名提供时才合法。
		if len(p.Topic) == 0 && !p.Props.Has(PropTopicAlias) {
			return nil, codeErr(reason.TopicNameInvalid,
				"empty topic requires a topic alias")
		}
	} else if len(p.Topic) == 0 {
		return nil, codeErr(reason.TopicNameInvalid, "PUBLISH topic must not be empty")
	}
	p.Payload = r.rest()
	return p, nil
}

func decodeAck(fh FixedHeader, r *reader, v Version) (*Ack, error) {
	want := byte(0)
	if fh.Type == PUBREL {
		want = 0x02
	}
	if err := requireFlags(fh, want); err != nil {
		return nil, err
	}
	a := &Ack{Kind: fh.Type, Code: reason.Success}
	var err error
	if a.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if a.PacketID == 0 {
		return nil, protoErr("packet identifier must not be 0")
	}
	if v != V5 || r.done() {
		// 当原因码为 Success 且没有属性时，MQTT 5 允许同时省略原因码与属性
		// （MQTT-3.4.2.1）。
		return a, nil
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	a.Code = reason.Code(code)
	if r.done() {
		return a, nil
	}
	if err := decodeProps(r, ctxPubAck, &a.Props); err != nil {
		return nil, err
	}
	return a, nil
}

func decodeSubscribe(fh FixedHeader, r *reader, v Version) (*Subscribe, error) {
	if err := requireFlags(fh, 0x02); err != nil {
		return nil, err
	}
	s := &Subscribe{}
	var err error
	if s.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if s.PacketID == 0 {
		return nil, protoErr("packet identifier must not be 0")
	}
	if v == V5 {
		if err := decodeProps(r, ctxSubscribe, &s.Props); err != nil {
			return nil, err
		}
		if len(s.Props.SubscriptionIDs) > 1 {
			return nil, protoErr("SUBSCRIBE carries more than one subscription identifier")
		}
	}
	for !r.done() {
		filter, err := r.str()
		if err != nil {
			return nil, err
		}
		if len(filter) == 0 {
			return nil, codeErr(reason.TopicFilterInvalid, "topic filter must not be empty")
		}
		opt, err := r.byte()
		if err != nil {
			return nil, err
		}
		sub := Subscription{Filter: filter, QoS: QoS(opt & 0x03)}
		if !sub.QoS.Valid() {
			return nil, malformed("invalid subscription QoS 3")
		}
		if v == V5 {
			if opt&subOptReserved != 0 {
				return nil, malformed("subscription options reserved bits must be 0")
			}
			sub.NoLocal = opt&subOptNoLocal != 0
			sub.RetainAsPublished = opt&subOptRAP != 0
			sub.RetainHandling = opt & subOptRHMask >> subOptRHShift
			if sub.RetainHandling > retainHandlingNo {
				return nil, protoErr("retain handling must be 0, 1 or 2")
			}
			if len(s.Props.SubscriptionIDs) == 1 {
				sub.ID = s.Props.SubscriptionIDs[0]
			}
		} else if opt&0xFC != 0 {
			return nil, malformed("subscription options reserved bits must be 0 in 3.1.1")
		}
		s.Filters = append(s.Filters, sub)
	}
	if len(s.Filters) == 0 {
		return nil, protoErr("SUBSCRIBE must contain at least one topic filter")
	}
	return s, nil
}

func decodeSuback(fh FixedHeader, r *reader, v Version) (*Suback, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	s := &Suback{}
	var err error
	if s.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if v == V5 {
		if err := decodeProps(r, ctxSubAck, &s.Props); err != nil {
			return nil, err
		}
	}
	for !r.done() {
		c, err := r.byte()
		if err != nil {
			return nil, err
		}
		s.Codes = append(s.Codes, reason.Code(c))
	}
	if len(s.Codes) == 0 {
		return nil, protoErr("SUBACK must contain at least one reason code")
	}
	return s, nil
}

func decodeUnsubscribe(fh FixedHeader, r *reader, v Version) (*Unsubscribe, error) {
	if err := requireFlags(fh, 0x02); err != nil {
		return nil, err
	}
	u := &Unsubscribe{}
	var err error
	if u.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if u.PacketID == 0 {
		return nil, protoErr("packet identifier must not be 0")
	}
	if v == V5 {
		if err := decodeProps(r, ctxUnsubscribe, &u.Props); err != nil {
			return nil, err
		}
	}
	for !r.done() {
		filter, err := r.str()
		if err != nil {
			return nil, err
		}
		if len(filter) == 0 {
			return nil, codeErr(reason.TopicFilterInvalid, "topic filter must not be empty")
		}
		u.Filters = append(u.Filters, filter)
	}
	if len(u.Filters) == 0 {
		return nil, protoErr("UNSUBSCRIBE must contain at least one topic filter")
	}
	return u, nil
}

func decodeUnsuback(fh FixedHeader, r *reader, v Version) (*Unsuback, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	u := &Unsuback{}
	var err error
	if u.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if v != V5 {
		// 3.1.1 的 UNSUBACK 没有负载。
		return u, nil
	}
	if err := decodeProps(r, ctxSubAck, &u.Props); err != nil {
		return nil, err
	}
	for !r.done() {
		c, err := r.byte()
		if err != nil {
			return nil, err
		}
		u.Codes = append(u.Codes, reason.Code(c))
	}
	if len(u.Codes) == 0 {
		return nil, protoErr("UNSUBACK must contain at least one reason code")
	}
	return u, nil
}

func decodeEmpty(fh FixedHeader, r *reader, p Packet) (Packet, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	if r.remaining() != 0 {
		return nil, malformed("%s must have zero remaining length", fh.Type)
	}
	return p, nil
}

func decodeDisconnect(fh FixedHeader, r *reader, v Version) (*Disconnect, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	d := &Disconnect{Code: reason.NormalDisconnection}
	if v != V5 {
		if r.remaining() != 0 {
			return nil, malformed("3.1.1 DISCONNECT must have zero remaining length")
		}
		return d, nil
	}
	if r.done() {
		return d, nil
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	d.Code = reason.Code(code)
	if r.done() {
		return d, nil
	}
	if err := decodeProps(r, ctxDisconnect, &d.Props); err != nil {
		return nil, err
	}
	return d, nil
}

func decodeAuth(fh FixedHeader, r *reader, v Version) (*Auth, error) {
	if err := requireFlags(fh, 0); err != nil {
		return nil, err
	}
	if v != V5 {
		return nil, malformed("AUTH is not valid in MQTT 3.1.1")
	}
	a := &Auth{Code: reason.Success}
	if r.done() {
		return a, nil
	}
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	a.Code = reason.Code(code)
	if r.done() {
		return a, nil
	}
	if err := decodeProps(r, ctxAuth, &a.Props); err != nil {
		return nil, err
	}
	return a, nil
}

// hasWildcard 报告主题名中是否含有订阅通配符。
// PUBLISH 主题与遗嘱主题中出现通配符是非法的（MQTT-3.3.2.1）。
func hasWildcard(topic []byte) bool {
	return bytes.IndexByte(topic, '+') >= 0 || bytes.IndexByte(topic, '#') >= 0
}
