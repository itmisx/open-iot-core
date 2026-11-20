package packet

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// TestAllPropertiesRoundTrip 在每个属性各自合法的上下文中逐一行使它，
// 确保每个属性的编码分支与解码分支都被走到，且往返稳定。
func TestAllPropertiesRoundTrip(t *testing.T) {
	up := []UserProperty{
		{Key: []byte("tenant"), Value: []byte("acme")},
		{Key: []byte("区域"), Value: []byte("华东")},
	}

	t.Run("CONNECT 全属性", func(t *testing.T) {
		c := &Connect{Version: V5, ClientID: []byte("dev"), CleanStart: true}
		p := &c.Props
		p.SessionExpiry = 7200
		p.Set(PropSessionExpiry)
		p.AuthMethod = []byte("SCRAM-SHA-256")
		p.Set(PropAuthMethod)
		p.AuthData = []byte{0x11, 0x22}
		p.Set(PropAuthData)
		p.RequestProblem = 1
		p.Set(PropRequestProblem)
		p.RequestResponse = 1
		p.Set(PropRequestResponse)
		p.ReceiveMaximum = 50
		p.Set(PropReceiveMaximum)
		p.TopicAliasMaximum = 16
		p.Set(PropTopicAliasMaximum)
		p.MaximumPacketSize = 65536
		p.Set(PropMaximumPacketSize)
		p.UserProps = up
		p.Set(PropUserProperty)

		got := roundTrip(t, c, V5).(*Connect)
		g := &got.Props
		if g.SessionExpiry != 7200 || g.RequestProblem != 1 || g.RequestResponse != 1 ||
			g.ReceiveMaximum != 50 || g.TopicAliasMaximum != 16 || g.MaximumPacketSize != 65536 {
			t.Errorf("标量属性错误: %+v", g)
		}
		if string(g.AuthMethod) != "SCRAM-SHA-256" || len(g.AuthData) != 2 {
			t.Errorf("认证属性错误: %+v", g)
		}
		if len(g.UserProps) != 2 || string(g.UserProps[1].Value) != "华东" {
			t.Errorf("用户属性错误: %+v", g.UserProps)
		}
	})

	t.Run("遗嘱全属性", func(t *testing.T) {
		c := &Connect{
			Version: V5, ClientID: []byte("dev"), CleanStart: true,
			Will: &Will{Topic: []byte("dev/offline"), Payload: []byte("bye"), QoS: QoS1},
		}
		p := &c.Will.Props
		p.WillDelay = 60
		p.Set(PropWillDelay)
		p.PayloadFormat = 1
		p.Set(PropPayloadFormat)
		p.MessageExpiry = 120
		p.Set(PropMessageExpiry)
		p.ContentType = []byte("text/plain")
		p.Set(PropContentType)
		p.ResponseTopic = []byte("dev/resp")
		p.Set(PropResponseTopic)
		p.CorrelationData = []byte{0xAB}
		p.Set(PropCorrelationData)
		p.UserProps = up
		p.Set(PropUserProperty)

		got := roundTrip(t, c, V5).(*Connect)
		g := &got.Will.Props
		if g.WillDelay != 60 || g.PayloadFormat != 1 || g.MessageExpiry != 120 {
			t.Errorf("遗嘱标量属性错误: %+v", g)
		}
		if string(g.ContentType) != "text/plain" || string(g.ResponseTopic) != "dev/resp" {
			t.Errorf("遗嘱字符串属性错误: %+v", g)
		}
	})

	t.Run("CONNACK 全属性", func(t *testing.T) {
		c := &Connack{SessionPresent: true, Code: reason.Success}
		p := &c.Props
		p.SessionExpiry = 3600
		p.Set(PropSessionExpiry)
		p.AssignedClientID = []byte("auto-7")
		p.Set(PropAssignedClientID)
		p.ServerKeepAlive = 300
		p.Set(PropServerKeepAlive)
		p.AuthMethod = []byte("SCRAM-SHA-256")
		p.Set(PropAuthMethod)
		p.AuthData = []byte{0x33}
		p.Set(PropAuthData)
		p.ResponseInfo = []byte("resp/prefix")
		p.Set(PropResponseInfo)
		p.ServerReference = []byte("node-2:8883")
		p.Set(PropServerReference)
		p.ReasonString = []byte("已接受")
		p.Set(PropReasonString)
		p.ReceiveMaximum = 100
		p.Set(PropReceiveMaximum)
		p.TopicAliasMaximum = 16
		p.Set(PropTopicAliasMaximum)
		p.MaximumQoS = 1
		p.Set(PropMaximumQoS)
		p.RetainAvailable = 1
		p.Set(PropRetainAvailable)
		p.MaximumPacketSize = 146 * 1024
		p.Set(PropMaximumPacketSize)
		p.WildcardSubAvail = 1
		p.Set(PropWildcardSubAvail)
		p.SubIDAvail = 1
		p.Set(PropSubIDAvail)
		p.SharedSubAvail = 1
		p.Set(PropSharedSubAvail)
		p.UserProps = up
		p.Set(PropUserProperty)

		got := roundTrip(t, c, V5).(*Connack)
		g := &got.Props
		if g.ServerKeepAlive != 300 || g.MaximumQoS != 1 || g.RetainAvailable != 1 ||
			g.WildcardSubAvail != 1 || g.SubIDAvail != 1 || g.SharedSubAvail != 1 {
			t.Errorf("CONNACK 能力属性错误: %+v", g)
		}
		if string(g.AssignedClientID) != "auto-7" || string(g.ResponseInfo) != "resp/prefix" ||
			string(g.ServerReference) != "node-2:8883" || string(g.ReasonString) != "已接受" {
			t.Errorf("CONNACK 字符串属性错误: %+v", g)
		}
	})

	t.Run("PUBLISH 全属性", func(t *testing.T) {
		pub := &Publish{Topic: []byte("a/b"), Payload: []byte("x"), QoS: QoS1, PacketID: 1}
		p := &pub.Props
		p.PayloadFormat = 1
		p.Set(PropPayloadFormat)
		p.MessageExpiry = 60
		p.Set(PropMessageExpiry)
		p.ContentType = []byte("application/cbor")
		p.Set(PropContentType)
		p.ResponseTopic = []byte("a/resp")
		p.Set(PropResponseTopic)
		p.CorrelationData = []byte{0x01, 0x02, 0x03}
		p.Set(PropCorrelationData)
		p.SubscriptionIDs = []uint32{1, 200, 70000} // 覆盖 1、2、3 字节变长整数
		p.Set(PropSubscriptionID)
		p.TopicAlias = 3
		p.Set(PropTopicAlias)
		p.UserProps = up
		p.Set(PropUserProperty)

		got := roundTrip(t, pub, V5).(*Publish)
		if len(got.Props.SubscriptionIDs) != 3 || got.Props.SubscriptionIDs[2] != 70000 {
			t.Errorf("订阅标识符错误: %+v", got.Props.SubscriptionIDs)
		}
	})

	t.Run("确认类属性", func(t *testing.T) {
		a := &Ack{Kind: PUBREC, PacketID: 5, Code: reason.QuotaExceeded}
		a.Props.ReasonString = []byte("超出配额")
		a.Props.Set(PropReasonString)
		a.Props.UserProps = up
		a.Props.Set(PropUserProperty)
		roundTrip(t, a, V5)
	})

	t.Run("SUBACK 属性", func(t *testing.T) {
		s := &Suback{PacketID: 5, Codes: []reason.Code{reason.GrantedQoS2}}
		s.Props.ReasonString = []byte("ok")
		s.Props.Set(PropReasonString)
		s.Props.UserProps = up
		s.Props.Set(PropUserProperty)
		roundTrip(t, s, V5)
	})

	t.Run("DISCONNECT 属性", func(t *testing.T) {
		d := &Disconnect{Code: reason.UseAnotherServer}
		d.Props.SessionExpiry = 0
		d.Props.Set(PropSessionExpiry)
		d.Props.ServerReference = []byte("node-3:8883")
		d.Props.Set(PropServerReference)
		d.Props.ReasonString = []byte("再均衡")
		d.Props.Set(PropReasonString)
		d.Props.UserProps = up
		d.Props.Set(PropUserProperty)
		got := roundTrip(t, d, V5).(*Disconnect)
		if string(got.Props.ServerReference) != "node-3:8883" {
			t.Errorf("服务端引用错误: %q", got.Props.ServerReference)
		}
	})

	t.Run("AUTH 属性", func(t *testing.T) {
		a := &Auth{Code: reason.ReAuthenticate}
		a.Props.AuthMethod = []byte("SCRAM-SHA-256")
		a.Props.Set(PropAuthMethod)
		a.Props.AuthData = []byte{0x44, 0x55}
		a.Props.Set(PropAuthData)
		a.Props.ReasonString = []byte("重新认证")
		a.Props.Set(PropReasonString)
		a.Props.UserProps = up
		a.Props.Set(PropUserProperty)
		roundTrip(t, a, V5)
	})

	t.Run("UNSUBSCRIBE 与 UNSUBACK 属性", func(t *testing.T) {
		u := &Unsubscribe{PacketID: 5, Filters: [][]byte{[]byte("a")}}
		u.Props.UserProps = up
		u.Props.Set(PropUserProperty)
		roundTrip(t, u, V5)

		ua := &Unsuback{PacketID: 5, Codes: []reason.Code{reason.Success}}
		ua.Props.ReasonString = []byte("ok")
		ua.Props.Set(PropReasonString)
		ua.Props.UserProps = up
		ua.Props.Set(PropUserProperty)
		roundTrip(t, ua, V5)
	})

	t.Run("SUBSCRIBE 属性", func(t *testing.T) {
		s := &Subscribe{PacketID: 5, Filters: []Subscription{{Filter: []byte("a"), QoS: QoS1}}}
		s.Props.SubscriptionIDs = []uint32{99}
		s.Props.Set(PropSubscriptionID)
		s.Props.UserProps = up
		s.Props.Set(PropUserProperty)
		roundTrip(t, s, V5)
	})
}

func TestPropertiesHelpers(t *testing.T) {
	var p Properties
	if !p.Empty() {
		t.Error("零值 Properties 应为空")
	}
	if p.Has(PropTopicAlias) {
		t.Error("未设置的属性 Has 应为 false")
	}
	// 超出位掩码范围的标识符不应 panic，也不应误报存在
	if p.Has(PropID(200)) {
		t.Error("超范围标识符 Has 应为 false")
	}

	p.TopicAlias = 5
	p.Set(PropTopicAlias)
	p.SubscriptionIDs = append(p.SubscriptionIDs, 1)
	p.UserProps = append(p.UserProps, UserProperty{Key: []byte("a")})
	if !p.Has(PropTopicAlias) || p.Empty() {
		t.Error("设置后应报告存在")
	}

	p.Reset()
	if !p.Empty() || p.Has(PropTopicAlias) || p.TopicAlias != 0 {
		t.Errorf("Reset 后应为零值: %+v", p)
	}
	// Reset 保留切片容量以便复用
	if cap(p.SubscriptionIDs) == 0 || cap(p.UserProps) == 0 {
		t.Error("Reset 应保留切片容量")
	}
	if len(p.SubscriptionIDs) != 0 || len(p.UserProps) != 0 {
		t.Error("Reset 应清空切片长度")
	}
}

func TestPropertiesEmptyListEncodesAsZero(t *testing.T) {
	// 无属性时仍须写出一个长度为 0 的前缀
	var p Properties
	if got := p.encodedLen(); got != 1 {
		t.Errorf("空属性列表编码长度 = %d，期望 1", got)
	}
	buf := p.appendTo(nil)
	if len(buf) != 1 || buf[0] != 0 {
		t.Errorf("空属性列表编码为 % x，期望 00", buf)
	}
	var nilp *Properties
	if got := nilp.propsLen(); got != 0 {
		t.Errorf("nil Properties 的 propsLen = %d，期望 0", got)
	}
}

func TestAppendVarintNegative(t *testing.T) {
	// 防御性分支：负值按 0 处理，不得产生无效编码
	if buf := appendVarint(nil, -1); len(buf) != 1 || buf[0] != 0 {
		t.Errorf("负值编码为 % x，期望 00", buf)
	}
}

func TestVersionStringUnsupported(t *testing.T) {
	if got := Version(9).String(); got != "unsupported(9)" {
		t.Errorf("Version(9).String() = %q", got)
	}
}

// TestBodyTruncationExhaustive 对带完整属性集的报文，在报文体的每个字节位置截断，
// 覆盖各解码函数内部逐字段的截断分支。
func TestBodyTruncationExhaustive(t *testing.T) {
	up := []UserProperty{{Key: []byte("k"), Value: []byte("v")}}
	mk := func() []struct {
		p Packet
		v Version
	} {
		conn := &Connect{
			Version: V5, ClientID: []byte("dev"), CleanStart: true,
			HasUsername: true, Username: []byte("u"), HasPassword: true, Password: []byte("p"),
			Will: &Will{Topic: []byte("t"), Payload: []byte("w"), QoS: QoS2},
		}
		conn.Props.SessionExpiry = 1
		conn.Props.Set(PropSessionExpiry)
		conn.Props.UserProps = up
		conn.Props.Set(PropUserProperty)
		conn.Will.Props.WillDelay = 1
		conn.Will.Props.Set(PropWillDelay)
		conn.Will.Props.ContentType = []byte("c")
		conn.Will.Props.Set(PropContentType)

		ca := &Connack{Code: reason.Success}
		ca.Props.AssignedClientID = []byte("x")
		ca.Props.Set(PropAssignedClientID)
		ca.Props.ServerKeepAlive = 1
		ca.Props.Set(PropServerKeepAlive)
		ca.Props.MaximumPacketSize = 2
		ca.Props.Set(PropMaximumPacketSize)
		ca.Props.MaximumQoS = 1
		ca.Props.Set(PropMaximumQoS)
		ca.Props.UserProps = up
		ca.Props.Set(PropUserProperty)

		pub := &Publish{Topic: []byte("a/b"), Payload: []byte("xy"), QoS: QoS2, PacketID: 3}
		pub.Props.ContentType = []byte("c")
		pub.Props.Set(PropContentType)
		pub.Props.CorrelationData = []byte{1}
		pub.Props.Set(PropCorrelationData)
		pub.Props.SubscriptionIDs = []uint32{300}
		pub.Props.Set(PropSubscriptionID)
		pub.Props.TopicAlias = 2
		pub.Props.Set(PropTopicAlias)
		pub.Props.MessageExpiry = 9
		pub.Props.Set(PropMessageExpiry)
		pub.Props.PayloadFormat = 1
		pub.Props.Set(PropPayloadFormat)
		pub.Props.UserProps = up
		pub.Props.Set(PropUserProperty)

		ack := &Ack{Kind: PUBREC, PacketID: 3, Code: reason.NotAuthorized}
		ack.Props.ReasonString = []byte("r")
		ack.Props.Set(PropReasonString)

		sub := &Subscribe{PacketID: 1, Filters: []Subscription{
			{Filter: []byte("a/#"), QoS: QoS1}, {Filter: []byte("b/+")},
		}}
		sub.Props.SubscriptionIDs = []uint32{7}
		sub.Props.Set(PropSubscriptionID)
		sub.Props.UserProps = up
		sub.Props.Set(PropUserProperty)

		sa := &Suback{PacketID: 1, Codes: []reason.Code{reason.GrantedQoS1, reason.NotAuthorized}}
		sa.Props.ReasonString = []byte("r")
		sa.Props.Set(PropReasonString)

		un := &Unsubscribe{PacketID: 1, Filters: [][]byte{[]byte("a"), []byte("b")}}
		un.Props.UserProps = up
		un.Props.Set(PropUserProperty)

		ua := &Unsuback{PacketID: 1, Codes: []reason.Code{reason.Success, reason.NoSubscriptionExisted}}
		ua.Props.ReasonString = []byte("r")
		ua.Props.Set(PropReasonString)

		dis := &Disconnect{Code: reason.ServerMoved}
		dis.Props.ServerReference = []byte("n")
		dis.Props.Set(PropServerReference)
		dis.Props.SessionExpiry = 1
		dis.Props.Set(PropSessionExpiry)

		au := &Auth{Code: reason.ContinueAuthentication}
		au.Props.AuthMethod = []byte("m")
		au.Props.Set(PropAuthMethod)
		au.Props.AuthData = []byte{1}
		au.Props.Set(PropAuthData)

		return []struct {
			p Packet
			v Version
		}{
			{conn, V5},
			{ca, V5},
			{pub, V5},
			{ack, V5},
			{sub, V5},
			{sa, V5},
			{un, V5},
			{ua, V5},
			{dis, V5},
			{au, V5},
			{&Connect{
				Version: V311, ClientID: []byte("d"), CleanStart: true,
				HasUsername: true, Username: []byte("u"), HasPassword: true, Password: []byte("p"),
				Will: &Will{Topic: []byte("t"), Payload: []byte("w")},
			}, V311},
			{&Connack{Code: reason.Success}, V311},
			{&Publish{Topic: []byte("a"), Payload: []byte("y"), QoS: QoS1, PacketID: 1}, V311},
			{&Subscribe{PacketID: 1, Filters: []Subscription{{Filter: []byte("a")}}}, V311},
			{&Suback{PacketID: 1, Codes: []reason.Code{reason.GrantedQoS0}}, V311},
			{&Unsubscribe{PacketID: 1, Filters: [][]byte{[]byte("a")}}, V311},
			{&Unsuback{PacketID: 1}, V311},
		}
	}

	for _, tc := range mk() {
		full, err := Encode(tc.p, tc.v)
		if err != nil {
			t.Fatalf("%s 编码失败: %v", tc.p.Type(), err)
		}
		hdrLen := 1 + varintLen(len(full)-1-varintLen(len(full)))
		fh, n, err := ReadFixedHeader(full)
		if err != nil {
			t.Fatal(err)
		}
		_ = hdrLen
		body := full[n:]
		for i := 0; i < len(body); i++ {
			short := fh
			short.Length = i
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%s/%s 体截断到 %d 字节时 panic: %v",
							tc.p.Type(), tc.v, i, r)
					}
				}()
				_, err := DecodeBody(short, body[:i], tc.v)
				// 本测试的硬性断言是「不 panic、不越界」（PRD TST-059）。
				//
				// 「必须失败」只对定长结构的报文成立。以下两类报文从前缀解码出
				// 合法结果是正确行为，不是缺陷：
				//   - 负载为重复列表者（SUBSCRIBE/UNSUBSCRIBE 的过滤器、
				//     SUBACK/UNSUBACK 的原因码），在列表元素边界截断即为更短的合法报文；
				//   - 尾部可选者（5.0 精简确认、无原因码的 DISCONNECT 与 AUTH），
				//     以及以剩余部分为负载的 PUBLISH。
				if err == nil {
					switch tc.p.Type() {
					case SUBSCRIBE, UNSUBSCRIBE, SUBACK, UNSUBACK,
						PUBACK, PUBREC, PUBREL, PUBCOMP, DISCONNECT, AUTH, PUBLISH:
						return
					}
					t.Fatalf("%s/%s 体截断到 %d 字节仍解码成功", tc.p.Type(), tc.v, i)
				}
			}()
		}
	}
}

// propList 把属性内容包成带长度前缀的属性列表。
func propList(content ...byte) []byte {
	return append(appendVarint(nil, len(content)), content...)
}

// TestPropertyValueTruncation 覆盖单个属性值在属性列表内部被截断的情形。
//
// 外层报文体的截断测试碰不到这些分支：属性列表有自己的长度前缀，
// sub(n) 会先因整段缺失而失败。只有当列表长度自洽、而末个属性的值不足时，
// 才会进入 decodeOneProp 内部的逐字段错误分支。
func TestPropertyValueTruncation(t *testing.T) {
	// 各类值的最小截断形式
	oneByte := func(id PropID) []byte { return []byte{byte(id)} }               // 缺 1 字节
	u16 := func(id PropID) []byte { return []byte{byte(id), 0x00} }             // 缺 1 字节
	u32 := func(id PropID) []byte { return []byte{byte(id), 0x00, 0x00, 0x00} } // 缺 1 字节
	strp := func(id PropID) []byte { return []byte{byte(id), 0x00, 0x05, 'a'} } // 声明 5 字节只给 1
	vint := func(id PropID) []byte { return []byte{byte(id), 0x80} }            // 续位后无字节
	userp := []byte{byte(PropUserProperty), 0x00, 0x01, 'k', 0x00, 0x05, 'a'}   // 值被截断
	userpKey := []byte{byte(PropUserProperty), 0x00, 0x05, 'k'}                 // 键被截断

	// 以 PUBLISH 承载：topic + 属性列表
	inPublish := func(content []byte) []byte {
		return build(PUBLISH, 0, cat(bs("a"), propList(content...))...)
	}
	// 以 CONNACK 承载：确认标志 + 原因码 + 属性列表
	inConnack := func(content []byte) []byte {
		return build(CONNACK, 0, cat([]byte{0x00, 0x00}, propList(content...))...)
	}
	// 以 CONNECT 承载：固定前缀 + 属性列表 + clientId
	inConnect := func(content []byte) []byte {
		return build(CONNECT, 0, connectBody(5, 0x02, 60,
			cat(propList(content...), bs("c"))...)...)
	}
	// 以遗嘱属性承载：CONNECT 属性为空 + clientId + 遗嘱属性列表 + 遗嘱主题与负载
	inWill := func(content []byte) []byte {
		return build(CONNECT, 0, connectBody(5, 0x06, 60,
			cat([]byte{0}, bs("c"), propList(content...), bs("t"), bs("w"))...)...)
	}

	cases := []struct {
		name string
		buf  []byte
	}{
		// PUBLISH 上下文
		{"PayloadFormat", inPublish(oneByte(PropPayloadFormat))},
		{"MessageExpiry", inPublish(u32(PropMessageExpiry))},
		{"ContentType", inPublish(strp(PropContentType))},
		{"ResponseTopic", inPublish(strp(PropResponseTopic))},
		{"CorrelationData", inPublish(strp(PropCorrelationData))},
		{"SubscriptionID", inPublish(vint(PropSubscriptionID))},
		{"TopicAlias", inPublish(u16(PropTopicAlias))},
		{"UserProperty 值截断", inPublish(userp)},
		{"UserProperty 键截断", inPublish(userpKey)},
		// CONNACK 上下文
		{"SessionExpiry", inConnack(u32(PropSessionExpiry))},
		{"AssignedClientID", inConnack(strp(PropAssignedClientID))},
		{"ServerKeepAlive", inConnack(u16(PropServerKeepAlive))},
		{"AuthMethod", inConnack(strp(PropAuthMethod))},
		{"AuthData", inConnack(strp(PropAuthData))},
		{"ResponseInfo", inConnack(strp(PropResponseInfo))},
		{"ServerReference", inConnack(strp(PropServerReference))},
		{"ReasonString", inConnack(strp(PropReasonString))},
		{"ReceiveMaximum", inConnack(u16(PropReceiveMaximum))},
		{"TopicAliasMaximum", inConnack(u16(PropTopicAliasMaximum))},
		{"MaximumQoS", inConnack(oneByte(PropMaximumQoS))},
		{"RetainAvailable", inConnack(oneByte(PropRetainAvailable))},
		{"MaximumPacketSize", inConnack(u32(PropMaximumPacketSize))},
		{"WildcardSubAvail", inConnack(oneByte(PropWildcardSubAvail))},
		{"SubIDAvail", inConnack(oneByte(PropSubIDAvail))},
		{"SharedSubAvail", inConnack(oneByte(PropSharedSubAvail))},
		// CONNECT 上下文
		{"RequestProblem", inConnect(oneByte(PropRequestProblem))},
		{"RequestResponse", inConnect(oneByte(PropRequestResponse))},
		// 遗嘱上下文
		{"WillDelay", inWill(u32(PropWillDelay))},
		// 属性列表长度前缀本身非法
		{"属性列表长度超出报文体", build(PUBLISH, 0, cat(bs("a"), []byte{0x7F})...)},
		{"属性列表长度前缀截断", build(PUBLISH, 0, cat(bs("a"), []byte{0x80})...)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if p, _, err := Decode(tc.buf, V5); err == nil {
				t.Fatalf("应被拒绝，却解出 %T %+v", p, p)
			}
		})
	}
}

// TestOtherPropertyValueConstraints 覆盖剩余的取值校验分支。
func TestOtherPropertyValueConstraints(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"RequestProblem 取值非法", build(CONNECT, 0, connectBody(5, 0x02, 60,
			cat(propList(byte(PropRequestProblem), 2), bs("c"))...)...)},
		{"RequestResponse 取值非法", build(CONNECT, 0, connectBody(5, 0x02, 60,
			cat(propList(byte(PropRequestResponse), 2), bs("c"))...)...)},
		{"MaximumQoS 取值非法", build(CONNACK, 0,
			cat([]byte{0, 0}, propList(byte(PropMaximumQoS), 2))...)},
		{"RetainAvailable 取值非法", build(CONNACK, 0,
			cat([]byte{0, 0}, propList(byte(PropRetainAvailable), 2))...)},
		{"WildcardSubAvail 取值非法", build(CONNACK, 0,
			cat([]byte{0, 0}, propList(byte(PropWildcardSubAvail), 2))...)},
		{"SubIDAvail 取值非法", build(CONNACK, 0,
			cat([]byte{0, 0}, propList(byte(PropSubIDAvail), 2))...)},
		{"SharedSubAvail 取值非法", build(CONNACK, 0,
			cat([]byte{0, 0}, propList(byte(PropSharedSubAvail), 2))...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Decode(tc.buf, V5); err == nil {
				t.Fatal("应被拒绝")
			}
		})
	}
}

// TestShortFormsAccepted 明确固定住 MQTT 5 允许的各种精简形式。
func TestShortFormsAccepted(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
		ver  Version
	}{
		{"确认仅含标识符", build(PUBACK, 0, 0, 1), V5},
		{"确认含原因码不含属性", build(PUBACK, 0, 0, 1, 0x10), V5},
		{"DISCONNECT 空", build(DISCONNECT, 0), V5},
		{"DISCONNECT 仅原因码", build(DISCONNECT, 0, 0x04), V5},
		{"AUTH 空", build(AUTH, 0), V5},
		{"AUTH 仅原因码", build(AUTH, 0, 0x18), V5},
		{"3.1.1 UNSUBACK 仅标识符", build(UNSUBACK, 0, 0, 1), V311},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Decode(tc.buf, tc.ver); err != nil {
				t.Fatalf("精简形式应被接受: %v", err)
			}
		})
	}
}

// TestPropsStandaloneCodec 覆盖属性列表的独立编解码。
//
// 这组 API 用于节点间与离线队列中透传属性：属性列表自带长度前缀，
// 因此单独编码出来的字节串是自描述的，可以原样保存与还原，
// 不必在每一跳重新解析。
func TestPropsStandaloneCodec(t *testing.T) {
	var p Properties
	p.ContentType = []byte("application/json")
	p.Set(PropContentType)
	p.ResponseTopic = []byte("a/resp")
	p.Set(PropResponseTopic)
	p.CorrelationData = []byte{0xAB, 0xCD}
	p.Set(PropCorrelationData)
	p.PayloadFormat = 1
	p.Set(PropPayloadFormat)
	p.UserProps = []UserProperty{{Key: []byte("k"), Value: []byte("v")}}
	p.Set(PropUserProperty)

	raw := p.AppendTo(nil)
	if len(raw) != p.EncodedLen() {
		t.Fatalf("编码长度 = %d，EncodedLen 报告 %d", len(raw), p.EncodedLen())
	}

	var got Properties
	if err := DecodeProps(raw, PublishContext, &got); err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if string(got.ContentType) != "application/json" ||
		string(got.ResponseTopic) != "a/resp" ||
		got.PayloadFormat != 1 ||
		len(got.CorrelationData) != 2 ||
		len(got.UserProps) != 1 {
		t.Errorf("往返不一致: %+v", got)
	}
	// 再次编码应得到完全相同的字节
	if string(got.AppendTo(nil)) != string(raw) {
		t.Error("二次编码与首次不一致")
	}

	// 空属性列表
	var empty Properties
	eraw := empty.AppendTo(nil)
	if len(eraw) != 1 || eraw[0] != 0 {
		t.Errorf("空列表编码为 % x", eraw)
	}
	var back Properties
	if err := DecodeProps(eraw, PublishContext, &back); err != nil {
		t.Errorf("空列表解码失败: %v", err)
	}
	if !back.Empty() {
		t.Error("空列表往返后应仍为空")
	}

	// 尾部多余内容须被拒绝：透传的字节串必须正好是一个完整的属性列表
	if err := DecodeProps(append(raw, 0xFF), PublishContext, &back); err == nil {
		t.Error("尾部多余内容应被拒绝")
	}
	// 上下文不符的属性须被拒绝：WillContext 不接受 TopicAlias
	var ta Properties
	ta.TopicAlias = 3
	ta.Set(PropTopicAlias)
	if err := DecodeProps(ta.AppendTo(nil), WillContext, &back); err == nil {
		t.Error("遗嘱上下文不应接受主题别名")
	}
}
