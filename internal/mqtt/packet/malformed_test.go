package packet

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// build 组装一个报文：固定报头加给定的可变报头与负载。
func build(t Type, flags byte, body ...byte) []byte {
	b := []byte{byte(t)<<4 | flags&0x0f}
	b = appendVarint(b, len(body))
	return append(b, body...)
}

// bs 生成带两字节长度前缀的字符串字段。
func bs(s string) []byte {
	return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...)
}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// connectBody 组装一个 CONNECT 的可变报头，便于逐项构造非法输入。
func connectBody(lvl byte, flags byte, keepalive uint16, tail ...byte) []byte {
	b := cat(bs("MQTT"), []byte{lvl, flags, byte(keepalive >> 8), byte(keepalive)})
	return append(b, tail...)
}

func TestMalformedRejected(t *testing.T) {
	cases := []struct {
		name string
		ver  Version
		buf  []byte
		code reason.Code // 0 表示不检查具体码
	}{
		// --- 固定报头 ---
		{"保留类型 0", V5, []byte{0x00, 0x00}, reason.MalformedPacket},
		{"剩余长度非最小编码", V5, []byte{0xC0, 0x80, 0x00}, reason.MalformedPacket},

		// --- CONNECT ---
		{
			"CONNECT 标志位非 0", V5,
			build(CONNECT, 0x01, connectBody(5, 0x02, 60, bs("c")...)...), reason.MalformedPacket,
		},
		{
			"协议名不是 MQTT", V5,
			build(CONNECT, 0, cat(bs("MQIsdp"), []byte{3, 0x02, 0, 60}, bs("c"))...),
			reason.UnsupportedProtocolVersion,
		},
		{
			"协议级别 3 不支持", V5,
			build(CONNECT, 0, connectBody(3, 0x02, 60, bs("c")...)...),
			reason.UnsupportedProtocolVersion,
		},
		{
			"协议级别 6 不支持", V5,
			build(CONNECT, 0, connectBody(6, 0x02, 60, bs("c")...)...),
			reason.UnsupportedProtocolVersion,
		},
		{
			"CONNECT 保留标志位 0 置位", V5,
			build(CONNECT, 0, connectBody(5, 0x03, 60, bs("c")...)...), reason.MalformedPacket,
		},
		{
			"无遗嘱却设了遗嘱 QoS", V5,
			build(CONNECT, 0, connectBody(5, 0x0A, 60, bs("c")...)...), reason.MalformedPacket,
		},
		{
			"无遗嘱却设了遗嘱 retain", V5,
			build(CONNECT, 0, connectBody(5, 0x22, 60, bs("c")...)...), reason.MalformedPacket,
		},
		{
			"遗嘱 QoS 3", V5,
			build(CONNECT, 0, connectBody(5, 0x1E, 60, cat([]byte{0}, bs("c"), bs("t"), bs("w"))...)...),
			reason.MalformedPacket,
		},
		{
			"3.1.1 有密码无用户名", V311,
			build(CONNECT, 0, connectBody(4, 0x42, 60, cat(bs("c"), bs("p"))...)...),
			reason.MalformedPacket,
		},
		{
			"3.1.1 空 clientId 且非 clean", V311,
			build(CONNECT, 0, connectBody(4, 0x00, 60, bs("")...)...),
			reason.ClientIdentifierNotValid,
		},
		{
			"遗嘱主题为空", V5,
			build(CONNECT, 0, connectBody(5, 0x06, 60,
				cat([]byte{0}, bs("c"), []byte{0}, bs(""), bs("w"))...)...),
			reason.MalformedPacket,
		},
		{
			"遗嘱主题含通配符", V5,
			build(CONNECT, 0, connectBody(5, 0x06, 60,
				cat([]byte{0}, bs("c"), []byte{0}, bs("a/+"), bs("w"))...)...),
			reason.TopicNameInvalid,
		},
		{
			"CONNECT 截断于 clientId", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60, 0x00)...), reason.MalformedPacket,
		},
		{
			"clientId 含空字符", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60, cat([]byte{0}, []byte{0, 1, 0})...)...),
			reason.MalformedPacket,
		},
		{
			"认证数据缺认证方法", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{4, byte(PropAuthData), 0, 1, 0xAA}, bs("c"))...)...),
			reason.ProtocolError,
		},
		{
			"属性重复", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{
					10,
					byte(PropSessionExpiry), 0, 0, 0, 1,
					byte(PropSessionExpiry), 0, 0, 0, 2,
				}, bs("c"))...)...),
			reason.ProtocolError,
		},
		{
			"未知属性", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{2, 0x7F, 0x00}, bs("c"))...)...),
			reason.MalformedPacket,
		},
		{
			"属性不属于本报文", V5, // TopicAlias 只能出现在 PUBLISH
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{3, byte(PropTopicAlias), 0, 1}, bs("c"))...)...),
			reason.MalformedPacket,
		},
		{
			"ReceiveMaximum 为 0", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{3, byte(PropReceiveMaximum), 0, 0}, bs("c"))...)...),
			reason.ProtocolError,
		},
		{
			"MaximumPacketSize 为 0", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{5, byte(PropMaximumPacketSize), 0, 0, 0, 0}, bs("c"))...)...),
			reason.ProtocolError,
		},
		{
			"遗嘱 PayloadFormat 取值非法", V5,
			build(CONNECT, 0, connectBody(5, 0x06, 60,
				cat([]byte{0}, bs("c"), []byte{2, byte(PropPayloadFormat), 9}, bs("t"), bs("w"))...)...),
			reason.ProtocolError,
		},
		{
			"CONNECT 属性段不接受 PayloadFormat", V5,
			build(CONNECT, 0, connectBody(5, 0x02, 60,
				cat([]byte{2, byte(PropPayloadFormat), 1}, bs("c"))...)...),
			reason.MalformedPacket,
		},

		// --- CONNACK ---
		{"CONNACK 保留确认标志位", V5, build(CONNACK, 0, 0x02, 0x00, 0x00), reason.MalformedPacket},
		{"CONNACK 标志位非 0", V5, build(CONNACK, 0x01, 0x00, 0x00, 0x00), reason.MalformedPacket},

		// --- PUBLISH ---
		{"PUBLISH QoS 3", V5, build(PUBLISH, 0x06, cat(bs("a"), []byte{0, 1, 0})...), reason.MalformedPacket},
		{"QoS 0 置 DUP", V5, build(PUBLISH, 0x08, cat(bs("a"), []byte{0})...), reason.MalformedPacket},
		{"PUBLISH 主题含 +", V5, build(PUBLISH, 0x00, cat(bs("a/+"), []byte{0})...), reason.TopicNameInvalid},
		{"PUBLISH 主题含 #", V5, build(PUBLISH, 0x00, cat(bs("a/#"), []byte{0})...), reason.TopicNameInvalid},
		{
			"QoS1 报文标识符为 0", V5,
			build(PUBLISH, 0x02, cat(bs("a"), []byte{0, 0, 0})...), reason.ProtocolError,
		},
		{"3.1.1 主题为空", V311, build(PUBLISH, 0x00, bs("")...), reason.TopicNameInvalid},
		{"5.0 主题为空且无别名", V5, build(PUBLISH, 0x00, cat(bs(""), []byte{0})...), reason.TopicNameInvalid},
		{
			"主题别名为 0", V5,
			build(PUBLISH, 0x00, cat(bs("a"), []byte{3, byte(PropTopicAlias), 0, 0})...),
			reason.TopicAliasInvalid,
		},
		{
			"订阅标识符为 0", V5,
			build(PUBLISH, 0x00, cat(bs("a"), []byte{2, byte(PropSubscriptionID), 0})...),
			reason.ProtocolError,
		},

		// --- 确认类 ---
		{"PUBACK 标识符为 0", V5, build(PUBACK, 0, 0, 0), reason.ProtocolError},
		{"PUBREL 标志位错误", V5, build(PUBREL, 0, 0, 1), reason.MalformedPacket},
		{"PUBACK 标志位错误", V5, build(PUBACK, 0x02, 0, 1), reason.MalformedPacket},
		{"PUBACK 截断", V5, build(PUBACK, 0, 0), reason.MalformedPacket},

		// --- SUBSCRIBE ---
		{
			"SUBSCRIBE 标志位非 2", V5,
			build(SUBSCRIBE, 0x00, cat([]byte{0, 1, 0}, bs("a"), []byte{0})...), reason.MalformedPacket,
		},
		{
			"SUBSCRIBE 标识符为 0", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 0, 0}, bs("a"), []byte{0})...), reason.ProtocolError,
		},
		{"SUBSCRIBE 无过滤器", V5, build(SUBSCRIBE, 0x02, 0, 1, 0), reason.ProtocolError},
		{
			"过滤器为空", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 1, 0}, bs(""), []byte{0})...), reason.TopicFilterInvalid,
		},
		{
			"订阅 QoS 3", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 1, 0}, bs("a"), []byte{3})...), reason.MalformedPacket,
		},
		{
			"5.0 订阅选项保留位", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 1, 0}, bs("a"), []byte{0x40})...), reason.MalformedPacket,
		},
		{
			"retain handling 为 3", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 1, 0}, bs("a"), []byte{0x30})...), reason.ProtocolError,
		},
		{
			"3.1.1 订阅选项保留位", V311,
			build(SUBSCRIBE, 0x02, cat([]byte{0, 1}, bs("a"), []byte{0x04})...), reason.MalformedPacket,
		},
		{
			"多个订阅标识符", V5,
			build(SUBSCRIBE, 0x02, cat([]byte{
				0, 1,
				4, byte(PropSubscriptionID), 1, byte(PropSubscriptionID), 2,
			}, bs("a"), []byte{0})...),
			reason.ProtocolError,
		},

		// --- SUBACK / UNSUBACK ---
		{"SUBACK 无原因码", V5, build(SUBACK, 0, 0, 1, 0), reason.ProtocolError},
		{"UNSUBACK 5.0 无原因码", V5, build(UNSUBACK, 0, 0, 1, 0), reason.ProtocolError},

		// --- UNSUBSCRIBE ---
		{
			"UNSUBSCRIBE 标志位非 2", V5,
			build(UNSUBSCRIBE, 0x00, cat([]byte{0, 1, 0}, bs("a"))...), reason.MalformedPacket,
		},
		{"UNSUBSCRIBE 无过滤器", V5, build(UNSUBSCRIBE, 0x02, 0, 1, 0), reason.ProtocolError},
		{
			"UNSUBSCRIBE 标识符为 0", V5,
			build(UNSUBSCRIBE, 0x02, cat([]byte{0, 0, 0}, bs("a"))...), reason.ProtocolError,
		},
		{
			"UNSUBSCRIBE 过滤器为空", V5,
			build(UNSUBSCRIBE, 0x02, cat([]byte{0, 1, 0}, bs(""))...), reason.TopicFilterInvalid,
		},

		// --- PING / DISCONNECT / AUTH ---
		{"PINGREQ 带负载", V5, build(PINGREQ, 0, 0x00), reason.MalformedPacket},
		{"PINGRESP 标志位非 0", V5, build(PINGRESP, 0x01), reason.MalformedPacket},
		{"3.1.1 DISCONNECT 带负载", V311, build(DISCONNECT, 0, 0x00), reason.MalformedPacket},
		{"DISCONNECT 标志位非 0", V5, build(DISCONNECT, 0x01), reason.MalformedPacket},
		{"3.1.1 下出现 AUTH", V311, build(AUTH, 0, 0x18, 0), reason.MalformedPacket},
		{"AUTH 标志位非 0", V5, build(AUTH, 0x01, 0x18, 0), reason.MalformedPacket},

		// --- 尾部多余字节 ---
		{"CONNACK 尾部多余字节", V311, build(CONNACK, 0, 0, 0, 0xFF), reason.MalformedPacket},
		{"PINGREQ 之后多余字节属长度不符", V5, build(PINGREQ, 0, 0xFF), reason.MalformedPacket},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _, err := Decode(tc.buf, tc.ver)
			if err == nil {
				t.Fatalf("应被拒绝，却解出 %T %+v", p, p)
			}
			pe, ok := err.(*Error)
			if !ok {
				t.Fatalf("期望 *packet.Error，得到 %T: %v", err, err)
			}
			if tc.code != 0 && pe.Code != tc.code {
				t.Errorf("原因码 = %s（0x%02x），期望 %s（0x%02x）",
					pe.Code, byte(pe.Code), tc.code, byte(tc.code))
			}
		})
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		p    Packet
		ver  Version
	}{
		{"QoS 3 的 PUBLISH", &Publish{Topic: []byte("a"), QoS: QoS(3)}, V5},
		{"QoS1 缺报文标识符", &Publish{Topic: []byte("a"), QoS: QoS1}, V5},
		{"QoS0 置 DUP", &Publish{Topic: []byte("a"), Dup: true}, V5},
		{"Ack.Kind 非确认类", &Ack{Kind: CONNECT, PacketID: 1}, V5},
		{"Ack 缺报文标识符", &Ack{Kind: PUBACK}, V5},
		{"SUBSCRIBE 无过滤器", &Subscribe{PacketID: 1}, V5},
		{"SUBSCRIBE 缺标识符", &Subscribe{Filters: []Subscription{{Filter: []byte("a")}}}, V5},
		{"SUBACK 无原因码", &Suback{PacketID: 1}, V5},
		{"UNSUBSCRIBE 无过滤器", &Unsubscribe{PacketID: 1}, V5},
		{"UNSUBSCRIBE 缺标识符", &Unsubscribe{Filters: [][]byte{[]byte("a")}}, V5},
		{"5.0 UNSUBACK 无原因码", &Unsuback{PacketID: 1}, V5},
		{"3.1.1 下 AUTH", &Auth{}, V311},
		{"不支持的协议级别", &Connect{Version: Version(3), ClientID: []byte("c")}, V311},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(tc.p, tc.ver); err == nil {
				t.Fatal("应被拒绝编码")
			}
		})
	}
}

func TestDecodeBodyLengthMismatch(t *testing.T) {
	fh := FixedHeader{Type: PINGREQ, Length: 5}
	if _, err := DecodeBody(fh, nil, V5); err == nil {
		t.Fatal("body 长度与剩余长度不符应报错")
	}
}

func TestDecodeTruncatedAtEveryOffset(t *testing.T) {
	// 对每种报文在每个字节位置截断，断言既不 panic 也不误判为成功。
	// 对应 PRD TST-059。
	pkts := []struct {
		p Packet
		v Version
	}{
		{&Connect{
			Version: V5, ClientID: []byte("dev"), HasUsername: true, Username: []byte("u"),
			Will: &Will{Topic: []byte("t"), Payload: []byte("w")},
		}, V5},
		{&Connack{Code: reason.Success}, V5},
		{&Publish{Topic: []byte("a/b"), Payload: []byte("xy"), QoS: QoS2, PacketID: 3}, V5},
		{&Ack{Kind: PUBREC, PacketID: 3, Code: reason.NoMatchingSubscribers}, V5},
		{&Subscribe{PacketID: 1, Filters: []Subscription{{Filter: []byte("a/#")}}}, V5},
		{&Suback{PacketID: 1, Codes: []reason.Code{reason.GrantedQoS1}}, V5},
		{&Unsubscribe{PacketID: 1, Filters: [][]byte{[]byte("a")}}, V5},
		{&Unsuback{PacketID: 1, Codes: []reason.Code{reason.Success}}, V5},
		{&Disconnect{Code: reason.ServerMoved}, V5},
		{&Auth{Code: reason.ContinueAuthentication}, V5},
	}
	for _, tc := range pkts {
		full, err := Encode(tc.p, tc.v)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < len(full); i++ {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%s 截断到 %d 字节时 panic: %v", tc.p.Type(), i, r)
					}
				}()
				if _, _, err := Decode(full[:i], tc.v); err == nil {
					t.Fatalf("%s 截断到 %d 字节仍解码成功", tc.p.Type(), i)
				}
			}()
		}
	}
}
