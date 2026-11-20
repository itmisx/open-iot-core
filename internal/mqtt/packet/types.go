// Package packet 实现 MQTT 3.1.1 与 5.0 控制报文的编解码。
//
// 本包有意只依赖标准库与 internal/mqtt/reason，以保证可独立 fuzz 并可作为
// 独立库发布（PRD ARCH-052、MQTT-005）。
//
// 解码在热路径上零分配：解出的报文中的字节切片字段直接引用调用方的缓冲区，
// 不做复制（PRD MQTT-012）。因此缓冲区的生命周期由调用方负责，在解出的报文
// 仍被引用期间不得复用该缓冲区。若报文的存活期需要长于缓冲区，请使用 Clone。
package packet

import (
	"fmt"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// Type 是 MQTT 控制报文类型（MQTT-2.1.2）。
type Type byte

const (
	CONNECT     Type = 1
	CONNACK     Type = 2
	PUBLISH     Type = 3
	PUBACK      Type = 4
	PUBREC      Type = 5
	PUBREL      Type = 6
	PUBCOMP     Type = 7
	SUBSCRIBE   Type = 8
	SUBACK      Type = 9
	UNSUBSCRIBE Type = 10
	UNSUBACK    Type = 11
	PINGREQ     Type = 12
	PINGRESP    Type = 13
	DISCONNECT  Type = 14
	AUTH        Type = 15
)

var typeNames = [16]string{
	0: "RESERVED", 1: "CONNECT", 2: "CONNACK", 3: "PUBLISH", 4: "PUBACK",
	5: "PUBREC", 6: "PUBREL", 7: "PUBCOMP", 8: "SUBSCRIBE", 9: "SUBACK",
	10: "UNSUBSCRIBE", 11: "UNSUBACK", 12: "PINGREQ", 13: "PINGRESP",
	14: "DISCONNECT", 15: "AUTH",
}

func (t Type) String() string {
	if t > 15 {
		return "INVALID"
	}
	return typeNames[t]
}

// Valid 报告 t 是否为已分配的报文类型。类型 0 为保留值。
func (t Type) Valid() bool { return t >= CONNECT && t <= AUTH }

// Version 是 CONNECT 中携带的协议级别（MQTT-3.1.2.2）。
type Version byte

const (
	// V311 为 MQTT 3.1.1，协议级别 4。
	V311 Version = 4
	// V5 为 MQTT 5.0，协议级别 5。
	V5 Version = 5
)

func (v Version) String() string {
	switch v {
	case V311:
		return "3.1.1"
	case V5:
		return "5.0"
	default:
		return fmt.Sprintf("unsupported(%d)", byte(v))
	}
}

// Supported 报告该协议级别是否为本实现支持的版本。
// 按 PRD MQTT-004，级别 3（MQTT 3.1）被拒绝。
func (v Version) Supported() bool { return v == V311 || v == V5 }

// QoS 是服务质量等级。
type QoS byte

const (
	QoS0 QoS = 0
	QoS1 QoS = 1
	QoS2 QoS = 2
)

// Valid 报告 q 是否为三个已定义等级之一。QoS 3 在线路上属于畸形报文。
func (q QoS) Valid() bool { return q <= QoS2 }

// MaxRemainingLength 是 4 字节变长整数能表示的最大值（MQTT-2.2.3）。
const MaxRemainingLength = 268435455

// Error 是协议层错误，定义在 reason 包，此处以别名导出。
// 一次解码或编码失败携带服务端应当上报的原因码：结构性畸形映射为
// MalformedPacket；结构合法但语义非法的内容映射为 ProtocolError 或更具体的码。
type Error = reason.Error

func malformed(format string, args ...any) *Error {
	return reason.Errorf(reason.MalformedPacket, format, args...)
}

func protoErr(format string, args ...any) *Error {
	return reason.Errorf(reason.ProtocolError, format, args...)
}

func codeErr(c reason.Code, format string, args ...any) *Error {
	return reason.Errorf(c, format, args...)
}

// ErrMalformed 与 ErrProtocol 是供 errors.Is 使用的比较目标。
var (
	ErrMalformed = &Error{Code: reason.MalformedPacket}
	ErrProtocol  = &Error{Code: reason.ProtocolError}
)

// Packet 由每一种控制报文类型实现。
type Packet interface {
	// Type 返回控制报文类型。
	Type() Type
	// EncodeTo 把线路表示追加到 dst 并返回。v 选择协议版本：
	// 仅在 V5 下写出属性。
	EncodeTo(dst []byte, v Version) ([]byte, error)
}

// FixedHeader 是任意报文开头已解析出的固定报头。
type FixedHeader struct {
	Type   Type
	Flags  byte
	Length int // 剩余长度
}
