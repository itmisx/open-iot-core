package packet

import "github.com/itmisx/open-iot-core/internal/mqtt/reason"

// EncodeTo 把 CONNECT 的线路形式追加到 dst。
func (c *Connect) EncodeTo(dst []byte, v Version) ([]byte, error) {
	// 线路上的版本来自报文自身而非参数，因为版本正是由 CONNECT 确立的。
	ver := c.Version
	if ver == 0 {
		ver = v
	}
	if !ver.Supported() {
		return nil, codeErr(reason.UnsupportedProtocolVersion,
			"cannot encode protocol level %d", byte(ver))
	}

	n := 2 + len(protocolName) + 1 + 1 + 2 // 协议名、级别、标志位、保活时间
	if ver == V5 {
		n += c.Props.encodedLen()
	}
	n += 2 + len(c.ClientID)
	if c.Will != nil {
		if ver == V5 {
			n += c.Will.Props.encodedLen()
		}
		n += 2 + len(c.Will.Topic) + 2 + len(c.Will.Payload)
	}
	if c.HasUsername {
		n += 2 + len(c.Username)
	}
	if c.HasPassword {
		n += 2 + len(c.Password)
	}

	var err error
	if dst, err = appendHeader(dst, CONNECT, 0, n); err != nil {
		return nil, err
	}
	dst = appendBin(dst, protocolName)
	dst = append(dst, byte(ver))

	var flags byte
	if c.CleanStart {
		flags |= 0x02
	}
	if c.Will != nil {
		flags |= 0x04
		flags |= byte(c.Will.QoS) << 3
		if c.Will.Retain {
			flags |= 0x20
		}
	}
	if c.HasPassword {
		flags |= 0x40
	}
	if c.HasUsername {
		flags |= 0x80
	}
	dst = append(dst, flags)
	dst = appendUint16(dst, c.KeepAlive)
	if ver == V5 {
		dst = c.Props.appendTo(dst)
	}
	dst = appendBin(dst, c.ClientID)
	if c.Will != nil {
		if ver == V5 {
			dst = c.Will.Props.appendTo(dst)
		}
		dst = appendBin(dst, c.Will.Topic)
		dst = appendBin(dst, c.Will.Payload)
	}
	if c.HasUsername {
		dst = appendBin(dst, c.Username)
	}
	if c.HasPassword {
		dst = appendBin(dst, c.Password)
	}
	return dst, nil
}

// EncodeTo 把 CONNACK 的线路形式追加到 dst。
// 在 V311 下，原因码会被映射到更窄的 3.1.1 返回码集合。
func (c *Connack) EncodeTo(dst []byte, v Version) ([]byte, error) {
	n := 2
	if v == V5 {
		n += c.Props.encodedLen()
	}
	var err error
	if dst, err = appendHeader(dst, CONNACK, 0, n); err != nil {
		return nil, err
	}
	var ackFlags byte
	if c.SessionPresent {
		ackFlags = 0x01
	}
	dst = append(dst, ackFlags)
	if v == V5 {
		dst = append(dst, byte(c.Code))
		return c.Props.appendTo(dst), nil
	}
	return append(dst, byte(reason.MapTo311(c.Code))), nil
}

// EncodeTo 把 PUBLISH 的线路形式追加到 dst。
func (p *Publish) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if !p.QoS.Valid() {
		return nil, malformed("cannot encode PUBLISH with QoS %d", p.QoS)
	}
	if p.QoS > QoS0 && p.PacketID == 0 {
		return nil, protoErr("PUBLISH with QoS %d requires a packet identifier", p.QoS)
	}
	n := 2 + len(p.Topic) + len(p.Payload)
	if p.QoS > QoS0 {
		n += 2
	}
	if v == V5 {
		n += p.Props.encodedLen()
	}

	var flags byte
	if p.Dup {
		if p.QoS == QoS0 {
			return nil, malformed("DUP must be 0 for QoS 0 PUBLISH")
		}
		flags |= 0x08
	}
	flags |= byte(p.QoS) << 1
	if p.Retain {
		flags |= 0x01
	}

	var err error
	if dst, err = appendHeader(dst, PUBLISH, flags, n); err != nil {
		return nil, err
	}
	dst = appendBin(dst, p.Topic)
	if p.QoS > QoS0 {
		dst = appendUint16(dst, p.PacketID)
	}
	if v == V5 {
		dst = p.Props.appendTo(dst)
	}
	return append(dst, p.Payload...), nil
}

// EncodeTo 把 PUBACK、PUBREC、PUBREL 或 PUBCOMP 追加到 dst。
//
// 在 MQTT 5 中，当原因码为 Success 且未设置任何属性时，省略原因码与属性长度。
// 这是最常见的情形，每条确认报文可省两个字节。
func (a *Ack) EncodeTo(dst []byte, v Version) ([]byte, error) {
	switch a.Kind {
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
	default:
		return nil, malformed("Ack.Kind %s is not an acknowledgement type", a.Kind)
	}
	if a.PacketID == 0 {
		return nil, protoErr("%s requires a packet identifier", a.Kind)
	}

	n := 2
	writeCode := false
	writeProps := false
	if v == V5 {
		propLen := a.Props.propsLen()
		if propLen > 0 {
			writeCode, writeProps = true, true
			n += 1 + a.Props.encodedLen()
		} else if a.Code != reason.Success {
			writeCode = true
			n += 1
		}
	}

	flags := byte(0)
	if a.Kind == PUBREL {
		flags = 0x02
	}
	var err error
	if dst, err = appendHeader(dst, a.Kind, flags, n); err != nil {
		return nil, err
	}
	dst = appendUint16(dst, a.PacketID)
	if writeCode {
		dst = append(dst, byte(a.Code))
	}
	if writeProps {
		dst = a.Props.appendTo(dst)
	}
	return dst, nil
}

// EncodeTo 把 SUBSCRIBE 的线路形式追加到 dst。
func (s *Subscribe) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if len(s.Filters) == 0 {
		return nil, protoErr("SUBSCRIBE requires at least one topic filter")
	}
	if s.PacketID == 0 {
		return nil, protoErr("SUBSCRIBE requires a packet identifier")
	}
	n := 2
	if v == V5 {
		n += s.Props.encodedLen()
	}
	for _, f := range s.Filters {
		n += 2 + len(f.Filter) + 1
	}
	var err error
	if dst, err = appendHeader(dst, SUBSCRIBE, 0x02, n); err != nil {
		return nil, err
	}
	dst = appendUint16(dst, s.PacketID)
	if v == V5 {
		dst = s.Props.appendTo(dst)
	}
	for _, f := range s.Filters {
		dst = appendBin(dst, f.Filter)
		dst = append(dst, f.optionByte(v))
	}
	return dst, nil
}

// EncodeTo 把 SUBACK 的线路形式追加到 dst。
// 在 V311 下，除已授予的 QoS 之外的任何码都归并为唯一的失败字节 0x80。
func (s *Suback) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if len(s.Codes) == 0 {
		return nil, protoErr("SUBACK requires at least one reason code")
	}
	n := 2 + len(s.Codes)
	if v == V5 {
		n += s.Props.encodedLen()
	}
	var err error
	if dst, err = appendHeader(dst, SUBACK, 0, n); err != nil {
		return nil, err
	}
	dst = appendUint16(dst, s.PacketID)
	if v == V5 {
		dst = s.Props.appendTo(dst)
		for _, c := range s.Codes {
			dst = append(dst, byte(c))
		}
		return dst, nil
	}
	for _, c := range s.Codes {
		if c > reason.GrantedQoS2 {
			c = reason.SubackFailure311
		}
		dst = append(dst, byte(c))
	}
	return dst, nil
}

// EncodeTo 把 UNSUBSCRIBE 的线路形式追加到 dst。
func (u *Unsubscribe) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if len(u.Filters) == 0 {
		return nil, protoErr("UNSUBSCRIBE requires at least one topic filter")
	}
	if u.PacketID == 0 {
		return nil, protoErr("UNSUBSCRIBE requires a packet identifier")
	}
	n := 2
	if v == V5 {
		n += u.Props.encodedLen()
	}
	for _, f := range u.Filters {
		n += 2 + len(f)
	}
	var err error
	if dst, err = appendHeader(dst, UNSUBSCRIBE, 0x02, n); err != nil {
		return nil, err
	}
	dst = appendUint16(dst, u.PacketID)
	if v == V5 {
		dst = u.Props.appendTo(dst)
	}
	for _, f := range u.Filters {
		dst = appendBin(dst, f)
	}
	return dst, nil
}

// EncodeTo 把 UNSUBACK 的线路形式追加到 dst。原因码仅 V5 有；
// 3.1.1 的 UNSUBACK 在报文标识符之后不再携带任何内容。
func (u *Unsuback) EncodeTo(dst []byte, v Version) ([]byte, error) {
	n := 2
	if v == V5 {
		if len(u.Codes) == 0 {
			return nil, protoErr("MQTT 5 UNSUBACK requires at least one reason code")
		}
		n += u.Props.encodedLen() + len(u.Codes)
	}
	var err error
	if dst, err = appendHeader(dst, UNSUBACK, 0, n); err != nil {
		return nil, err
	}
	dst = appendUint16(dst, u.PacketID)
	if v != V5 {
		return dst, nil
	}
	dst = u.Props.appendTo(dst)
	for _, c := range u.Codes {
		dst = append(dst, byte(c))
	}
	return dst, nil
}

// EncodeTo 把 PINGREQ 追加到 dst。
func (*Pingreq) EncodeTo(dst []byte, _ Version) ([]byte, error) {
	return append(dst, byte(PINGREQ)<<4, 0), nil
}

// EncodeTo 把 PINGRESP 追加到 dst。
func (*Pingresp) EncodeTo(dst []byte, _ Version) ([]byte, error) {
	return append(dst, byte(PINGRESP)<<4, 0), nil
}

// EncodeTo 把 DISCONNECT 的线路形式追加到 dst。
// 3.1.1 的 DISCONNECT 没有原因码与属性。
func (d *Disconnect) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if v != V5 {
		return append(dst, byte(DISCONNECT)<<4, 0), nil
	}
	n := 0
	writeCode := false
	writeProps := false
	if propLen := d.Props.propsLen(); propLen > 0 {
		writeCode, writeProps = true, true
		n = 1 + d.Props.encodedLen()
	} else if d.Code != reason.NormalDisconnection {
		writeCode = true
		n = 1
	}
	var err error
	if dst, err = appendHeader(dst, DISCONNECT, 0, n); err != nil {
		return nil, err
	}
	if writeCode {
		dst = append(dst, byte(d.Code))
	}
	if writeProps {
		dst = d.Props.appendTo(dst)
	}
	return dst, nil
}

// EncodeTo 把 AUTH 的线路形式追加到 dst。AUTH 仅存在于 MQTT 5。
func (a *Auth) EncodeTo(dst []byte, v Version) ([]byte, error) {
	if v != V5 {
		return nil, malformed("AUTH is not valid in MQTT 3.1.1")
	}
	// AUTH 总是携带原因码；唯一可省略的形式是空报文，而这对服务端没有用处。
	n := 1 + a.Props.encodedLen()
	var err error
	if dst, err = appendHeader(dst, AUTH, 0, n); err != nil {
		return nil, err
	}
	dst = append(dst, byte(a.Code))
	return a.Props.appendTo(dst), nil
}

// Encode 是一个便捷包装，每次分配一个新缓冲区。
func Encode(p Packet, v Version) ([]byte, error) {
	return p.EncodeTo(nil, v)
}
