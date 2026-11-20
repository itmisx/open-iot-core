// Package reason 定义 MQTT 原因码与 3.1.1 的 CONNACK 返回码。
//
// 本包除标准库外无任何依赖，以保证协议层可独立 fuzz（见 PRD ARCH-052、MQTT-005）。
package reason

import "fmt"

// Code 是 MQTT 5 的原因码。MQTT 3.1.1 只在 CONNACK 与 SUBACK 中携带一组更窄的
// 返回码；存在映射关系的部分由 MapTo311 转换。
type Code byte

const (
	Success                     Code = 0x00 // 同时也是 NormalDisconnection 与 GrantedQoS0
	GrantedQoS1                 Code = 0x01
	GrantedQoS2                 Code = 0x02
	DisconnectWithWill          Code = 0x04
	NoMatchingSubscribers       Code = 0x10
	NoSubscriptionExisted       Code = 0x11
	ContinueAuthentication      Code = 0x18
	ReAuthenticate              Code = 0x19
	UnspecifiedError            Code = 0x80
	MalformedPacket             Code = 0x81
	ProtocolError               Code = 0x82
	ImplementationSpecificError Code = 0x83
	UnsupportedProtocolVersion  Code = 0x84
	ClientIdentifierNotValid    Code = 0x85
	BadUserNameOrPassword       Code = 0x86
	NotAuthorized               Code = 0x87
	ServerUnavailable           Code = 0x88
	ServerBusy                  Code = 0x89
	Banned                      Code = 0x8A
	ServerShuttingDown          Code = 0x8B
	BadAuthenticationMethod     Code = 0x8C
	KeepAliveTimeout            Code = 0x8D
	SessionTakenOver            Code = 0x8E
	TopicFilterInvalid          Code = 0x8F
	TopicNameInvalid            Code = 0x90
	PacketIdentifierInUse       Code = 0x91
	PacketIdentifierNotFound    Code = 0x92
	ReceiveMaximumExceeded      Code = 0x93
	TopicAliasInvalid           Code = 0x94
	PacketTooLarge              Code = 0x95
	MessageRateTooHigh          Code = 0x96
	QuotaExceeded               Code = 0x97
	AdministrativeAction        Code = 0x98
	PayloadFormatInvalid        Code = 0x99
	RetainNotSupported          Code = 0x9A
	QoSNotSupported             Code = 0x9B
	UseAnotherServer            Code = 0x9C
	ServerMoved                 Code = 0x9D
	SharedSubsNotSupported      Code = 0x9E
	ConnectionRateExceeded      Code = 0x9F
	MaximumConnectTime          Code = 0xA0
	SubscriptionIDsNotSupported Code = 0xA1
	WildcardSubsNotSupported    Code = 0xA2
)

// 以下别名与上面的码值相同，只是在不同报文中的语义读法不同。
const (
	NormalDisconnection = Success
	GrantedQoS0         = Success
)

// SubackFailure311 是 3.1.1 的 SUBACK 失败字节。MQTT 5 使用 UnspecifiedError。
const SubackFailure311 Code = 0x80

// IsError 报告该码是否表示失败。按 MQTT-2.4，小于 0x80 的码为成功或信息性。
func (c Code) IsError() bool { return c >= 0x80 }

// String 返回该码在规范中的名称。
func (c Code) String() string {
	if s, ok := names[c]; ok {
		return s
	}
	return "Unknown"
}

var names = map[Code]string{
	Success:                     "Success",
	GrantedQoS1:                 "Granted QoS 1",
	GrantedQoS2:                 "Granted QoS 2",
	DisconnectWithWill:          "Disconnect with Will Message",
	NoMatchingSubscribers:       "No matching subscribers",
	NoSubscriptionExisted:       "No subscription existed",
	ContinueAuthentication:      "Continue authentication",
	ReAuthenticate:              "Re-authenticate",
	UnspecifiedError:            "Unspecified error",
	MalformedPacket:             "Malformed Packet",
	ProtocolError:               "Protocol Error",
	ImplementationSpecificError: "Implementation specific error",
	UnsupportedProtocolVersion:  "Unsupported Protocol Version",
	ClientIdentifierNotValid:    "Client Identifier not valid",
	BadUserNameOrPassword:       "Bad User Name or Password",
	NotAuthorized:               "Not authorized",
	ServerUnavailable:           "Server unavailable",
	ServerBusy:                  "Server busy",
	Banned:                      "Banned",
	ServerShuttingDown:          "Server shutting down",
	BadAuthenticationMethod:     "Bad authentication method",
	KeepAliveTimeout:            "Keep Alive timeout",
	SessionTakenOver:            "Session taken over",
	TopicFilterInvalid:          "Topic Filter invalid",
	TopicNameInvalid:            "Topic Name invalid",
	PacketIdentifierInUse:       "Packet Identifier in use",
	PacketIdentifierNotFound:    "Packet Identifier not found",
	ReceiveMaximumExceeded:      "Receive Maximum exceeded",
	TopicAliasInvalid:           "Topic Alias invalid",
	PacketTooLarge:              "Packet too large",
	MessageRateTooHigh:          "Message rate too high",
	QuotaExceeded:               "Quota exceeded",
	AdministrativeAction:        "Administrative action",
	PayloadFormatInvalid:        "Payload format invalid",
	RetainNotSupported:          "Retain not supported",
	QoSNotSupported:             "QoS not supported",
	UseAnotherServer:            "Use another server",
	ServerMoved:                 "Server moved",
	SharedSubsNotSupported:      "Shared Subscriptions not supported",
	ConnectionRateExceeded:      "Connection rate exceeded",
	MaximumConnectTime:          "Maximum connect time",
	SubscriptionIDsNotSupported: "Subscription Identifiers not supported",
	WildcardSubsNotSupported:    "Wildcard Subscriptions not supported",
}

// Return311 是 MQTT 3.1.1 第 3.2.2.3 节定义的 CONNACK 返回码。
type Return311 byte

const (
	Accepted311                 Return311 = 0
	UnacceptableProtocolVersion Return311 = 1
	IdentifierRejected          Return311 = 2
	Server311Unavailable        Return311 = 3
	BadUserNameOrPassword311    Return311 = 4
	NotAuthorized311            Return311 = 5
)

// MapFrom311 把 3.1.1 的 CONNACK 返回码提升到 MQTT 5 的原因码空间。
//
// 这个方向的转换是必需的：Return311 与 Code 是两个不同的数值空间
// （例如返回码 2 表示标识符被拒，而原因码 0x02 表示已授予 QoS 2），
// 若把返回码原样存入 Code 字段，服务端上层就会读到完全无关的语义。
// 解码 3.1.1 的 CONNACK 时必须经过本函数，使整个服务端只在一个码空间上工作。
func MapFrom311(r Return311) Code {
	switch r {
	case Accepted311:
		return Success
	case UnacceptableProtocolVersion:
		return UnsupportedProtocolVersion
	case IdentifierRejected:
		return ClientIdentifierNotValid
	case Server311Unavailable:
		return ServerUnavailable
	case BadUserNameOrPassword311:
		return BadUserNameOrPassword
	case NotAuthorized311:
		return NotAuthorized
	default:
		// 3.1.1 没有定义 6 及以上的返回码。
		return UnspecifiedError
	}
}

// MapTo311 把原因码转换为最接近的 3.1.1 CONNACK 返回码。
// 在 3.1.1 中没有对应项的码统一归到 Server311Unavailable——这是旧版 CONNACK
// 唯一能表达的通用失败。
func MapTo311(c Code) Return311 {
	switch c {
	case Success:
		return Accepted311
	case UnsupportedProtocolVersion:
		return UnacceptableProtocolVersion
	case ClientIdentifierNotValid:
		return IdentifierRejected
	case BadUserNameOrPassword:
		return BadUserNameOrPassword311
	case NotAuthorized, Banned, QuotaExceeded:
		return NotAuthorized311
	default:
		return Server311Unavailable
	}
}

// Error 表示一次协议层失败，并携带服务端应当上报的原因码。
//
// 它定义在本包而非 packet 包，是为了让 packet、topic 以及后续的会话层
// 共用同一个错误类型——上层只需要一条路径就能从任意协议错误取出原因码。
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string {
	if e.Msg == "" {
		return "mqtt: " + e.Code.String()
	}
	return "mqtt: " + e.Msg + " (" + e.Code.String() + ")"
}

// Is 使调用方可以用 errors.Is 按原因码匹配：
// 以只设了 Code 的 Error 作为比较目标即可。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code && t.Msg == ""
}

// Errorf 构造一个携带原因码的错误。
func Errorf(c Code, format string, args ...any) *Error {
	if len(args) == 0 {
		return &Error{Code: c, Msg: format}
	}
	return &Error{Code: c, Msg: fmt.Sprintf(format, args...)}
}
