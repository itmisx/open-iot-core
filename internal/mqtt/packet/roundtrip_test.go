package packet

import (
	"bytes"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// roundTrip 断言核心不变量：encode(decode(encode(p))) == encode(p)。
// 以字节流而非结构体比较，可以避开 nil 与空切片的差异，
// 同时验证的正是线路上真正重要的性质。
func roundTrip(t *testing.T, p Packet, v Version) Packet {
	t.Helper()
	first, err := Encode(p, v)
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	got, n, err := Decode(first, v)
	if err != nil {
		t.Fatalf("解码失败: %v (% x)", err, first)
	}
	if n != len(first) {
		t.Fatalf("消费 %d 字节，报文长 %d", n, len(first))
	}
	if got.Type() != p.Type() {
		t.Fatalf("类型 = %s，期望 %s", got.Type(), p.Type())
	}
	second, err := Encode(got, v)
	if err != nil {
		t.Fatalf("重新编码失败: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("往返不一致\n首次 % x\n再次 % x", first, second)
	}
	return got
}

func TestConnectRoundTrip(t *testing.T) {
	for _, v := range []Version{V311, V5} {
		t.Run(v.String(), func(t *testing.T) {
			c := &Connect{
				Version:     v,
				ClientID:    []byte("dev-001"),
				KeepAlive:   60,
				CleanStart:  true,
				HasUsername: true,
				Username:    []byte("user"),
				HasPassword: true,
				Password:    []byte{0x01, 0x02, 0x00, 0x03}, // 密码是二进制，可含 0x00
				Will: &Will{
					Topic:   []byte("dev/001/offline"),
					Payload: []byte("bye"),
					QoS:     QoS1,
					Retain:  true,
				},
			}
			if v == V5 {
				c.Props.SessionExpiry = 3600
				c.Props.Set(PropSessionExpiry)
				c.Props.ReceiveMaximum = 100
				c.Props.Set(PropReceiveMaximum)
				c.Props.MaximumPacketSize = 146 * 1024
				c.Props.Set(PropMaximumPacketSize)
				c.Props.TopicAliasMaximum = 16
				c.Props.Set(PropTopicAliasMaximum)
				c.Props.UserProps = []UserProperty{{Key: []byte("k"), Value: []byte("v")}}
				c.Props.Set(PropUserProperty)
				c.Will.Props.WillDelay = 30
				c.Will.Props.Set(PropWillDelay)
				c.Will.Props.PayloadFormat = 1
				c.Will.Props.Set(PropPayloadFormat)
			}
			got := roundTrip(t, c, v).(*Connect)
			if string(got.ClientID) != "dev-001" || got.KeepAlive != 60 || !got.CleanStart {
				t.Errorf("基本字段错误: %+v", got)
			}
			if got.Version != v {
				t.Errorf("版本 = %s，期望 %s", got.Version, v)
			}
			if got.Will == nil || got.Will.QoS != QoS1 || !got.Will.Retain {
				t.Errorf("遗嘱字段错误: %+v", got.Will)
			}
			if !bytes.Equal(got.Password, c.Password) {
				t.Errorf("密码 = % x", got.Password)
			}
			if v == V5 {
				if got.Props.SessionExpiry != 3600 || got.Props.ReceiveMaximum != 100 {
					t.Errorf("属性错误: %+v", got.Props)
				}
				if got.Will.Props.WillDelay != 30 {
					t.Errorf("遗嘱延迟 = %d", got.Will.Props.WillDelay)
				}
			}
		})
	}
}

func TestConnectMinimal(t *testing.T) {
	// 空 clientId 加 clean session 在两个版本下都合法
	for _, v := range []Version{V311, V5} {
		roundTrip(t, &Connect{Version: v, ClientID: nil, CleanStart: true}, v)
	}
}

func TestConnackRoundTrip(t *testing.T) {
	t.Run("5.0", func(t *testing.T) {
		c := &Connack{SessionPresent: true, Code: reason.Success}
		c.Props.AssignedClientID = []byte("auto-123")
		c.Props.Set(PropAssignedClientID)
		c.Props.MaximumQoS = 1
		c.Props.Set(PropMaximumQoS)
		c.Props.RetainAvailable = 1
		c.Props.Set(PropRetainAvailable)
		c.Props.ServerKeepAlive = 120
		c.Props.Set(PropServerKeepAlive)
		got := roundTrip(t, c, V5).(*Connack)
		if !got.SessionPresent || string(got.Props.AssignedClientID) != "auto-123" {
			t.Errorf("得到 %+v", got)
		}
	})
	t.Run("3.1.1 原因码映射", func(t *testing.T) {
		// 3.1.1 没有 NotAuthorized 之外的细分码，须映射到 5
		buf, err := Encode(&Connack{Code: reason.Banned}, V311)
		if err != nil {
			t.Fatal(err)
		}
		if buf[len(buf)-1] != byte(reason.NotAuthorized311) {
			t.Errorf("返回码 = %d，期望 %d", buf[len(buf)-1], reason.NotAuthorized311)
		}
	})
}

func TestPublishRoundTrip(t *testing.T) {
	for _, v := range []Version{V311, V5} {
		for _, q := range []QoS{QoS0, QoS1, QoS2} {
			t.Run(v.String()+"/QoS"+string(rune('0'+q)), func(t *testing.T) {
				p := &Publish{
					Topic:   []byte("$aws/things/dev-001/shadow/update"),
					Payload: []byte(`{"state":{"reported":{"t":21}}}`),
					QoS:     q,
					Retain:  true,
				}
				if q > QoS0 {
					p.PacketID = 42
					p.Dup = true
				}
				if v == V5 {
					p.Props.PayloadFormat = 1
					p.Props.Set(PropPayloadFormat)
					p.Props.ContentType = []byte("application/json")
					p.Props.Set(PropContentType)
					p.Props.MessageExpiry = 300
					p.Props.Set(PropMessageExpiry)
					p.Props.ResponseTopic = []byte("dev/001/resp")
					p.Props.Set(PropResponseTopic)
					p.Props.CorrelationData = []byte{0xDE, 0xAD}
					p.Props.Set(PropCorrelationData)
					p.Props.TopicAlias = 7
					p.Props.Set(PropTopicAlias)
					p.Props.SubscriptionIDs = []uint32{1, 9}
					p.Props.Set(PropSubscriptionID)
				}
				got := roundTrip(t, p, v).(*Publish)
				if got.QoS != q || !got.Retain || !bytes.Equal(got.Payload, p.Payload) {
					t.Errorf("得到 %+v", got)
				}
				if q > QoS0 && (got.PacketID != 42 || !got.Dup) {
					t.Errorf("QoS>0 字段错误: id=%d dup=%v", got.PacketID, got.Dup)
				}
				if v == V5 {
					if string(got.Props.ContentType) != "application/json" ||
						got.Props.TopicAlias != 7 ||
						len(got.Props.SubscriptionIDs) != 2 {
						t.Errorf("属性错误: %+v", got.Props)
					}
				}
			})
		}
	}
}

func TestPublishEmptyPayload(t *testing.T) {
	// 空负载是清除 retained 消息的手段，必须支持
	for _, v := range []Version{V311, V5} {
		got := roundTrip(t, &Publish{Topic: []byte("a/b"), Retain: true}, v).(*Publish)
		if len(got.Payload) != 0 {
			t.Errorf("负载应为空，得到 % x", got.Payload)
		}
	}
}

func TestAckRoundTrip(t *testing.T) {
	for _, kind := range []Type{PUBACK, PUBREC, PUBREL, PUBCOMP} {
		t.Run(kind.String(), func(t *testing.T) {
			// 3.1.1：仅报文标识符
			got := roundTrip(t, &Ack{Kind: kind, PacketID: 7}, V311).(*Ack)
			if got.PacketID != 7 || got.Code != reason.Success {
				t.Errorf("得到 %+v", got)
			}
			// 5.0 精简形式：Success 且无属性时省略原因码
			buf, err := Encode(&Ack{Kind: kind, PacketID: 7}, V5)
			if err != nil {
				t.Fatal(err)
			}
			if len(buf) != 4 {
				t.Errorf("精简形式应为 4 字节，得到 %d: % x", len(buf), buf)
			}
			// 5.0 带原因码
			got = roundTrip(t, &Ack{Kind: kind, PacketID: 7, Code: reason.NoMatchingSubscribers}, V5).(*Ack)
			if got.Code != reason.NoMatchingSubscribers {
				t.Errorf("原因码 = %s", got.Code)
			}
			// 5.0 带原因码与属性
			a := &Ack{Kind: kind, PacketID: 7, Code: reason.NotAuthorized}
			a.Props.ReasonString = []byte("策略拒绝")
			a.Props.Set(PropReasonString)
			got = roundTrip(t, a, V5).(*Ack)
			if string(got.Props.ReasonString) != "策略拒绝" {
				t.Errorf("原因串 = %q", got.Props.ReasonString)
			}
		})
	}
}

func TestSubscribeRoundTrip(t *testing.T) {
	for _, v := range []Version{V311, V5} {
		t.Run(v.String(), func(t *testing.T) {
			s := &Subscribe{
				PacketID: 100,
				Filters: []Subscription{
					{Filter: []byte("a/b/c"), QoS: QoS1},
					{Filter: []byte("$aws/things/+/shadow/update/delta"), QoS: QoS0},
					{Filter: []byte("$share/grp/sensors/#"), QoS: QoS2},
				},
			}
			if v == V5 {
				s.Filters[0].NoLocal = true
				s.Filters[0].RetainAsPublished = true
				s.Filters[0].RetainHandling = 2
				s.Props.SubscriptionIDs = []uint32{55}
				s.Props.Set(PropSubscriptionID)
			}
			got := roundTrip(t, s, v).(*Subscribe)
			if len(got.Filters) != 3 || got.PacketID != 100 {
				t.Fatalf("得到 %+v", got)
			}
			if string(got.Filters[2].Filter) != "$share/grp/sensors/#" || got.Filters[2].QoS != QoS2 {
				t.Errorf("第三个过滤器错误: %+v", got.Filters[2])
			}
			if v == V5 {
				f := got.Filters[0]
				if !f.NoLocal || !f.RetainAsPublished || f.RetainHandling != 2 {
					t.Errorf("订阅选项错误: %+v", f)
				}
				// 订阅标识符应回填到每个过滤器上
				for i, f := range got.Filters {
					if f.ID != 55 {
						t.Errorf("过滤器 %d 的 ID = %d，期望 55", i, f.ID)
					}
				}
			} else if got.Filters[0].NoLocal {
				t.Error("3.1.1 不应解出 NoLocal")
			}
		})
	}
}

func TestSubackRoundTrip(t *testing.T) {
	t.Run("5.0", func(t *testing.T) {
		s := &Suback{
			PacketID: 100,
			Codes:    []reason.Code{reason.GrantedQoS0, reason.GrantedQoS1, reason.NotAuthorized},
		}
		s.Props.ReasonString = []byte("部分失败")
		s.Props.Set(PropReasonString)
		got := roundTrip(t, s, V5).(*Suback)
		if len(got.Codes) != 3 || got.Codes[2] != reason.NotAuthorized {
			t.Errorf("得到 %+v", got.Codes)
		}
	})
	t.Run("3.1.1 失败码归并", func(t *testing.T) {
		buf, err := Encode(&Suback{
			PacketID: 1,
			Codes:    []reason.Code{reason.GrantedQoS2, reason.NotAuthorized},
		}, V311)
		if err != nil {
			t.Fatal(err)
		}
		body := buf[len(buf)-2:]
		if body[0] != 0x02 || body[1] != byte(reason.SubackFailure311) {
			t.Errorf("3.1.1 SUBACK 载荷 = % x，期望 02 80", body)
		}
	})
}

func TestUnsubscribeRoundTrip(t *testing.T) {
	for _, v := range []Version{V311, V5} {
		t.Run(v.String(), func(t *testing.T) {
			u := &Unsubscribe{
				PacketID: 9,
				Filters:  [][]byte{[]byte("a/b"), []byte("c/#")},
			}
			if v == V5 {
				u.Props.UserProps = []UserProperty{{Key: []byte("why"), Value: []byte("cleanup")}}
				u.Props.Set(PropUserProperty)
			}
			got := roundTrip(t, u, v).(*Unsubscribe)
			if len(got.Filters) != 2 || string(got.Filters[1]) != "c/#" {
				t.Errorf("得到 %+v", got)
			}
		})
	}
}

func TestUnsubackRoundTrip(t *testing.T) {
	t.Run("5.0", func(t *testing.T) {
		got := roundTrip(t, &Unsuback{
			PacketID: 9,
			Codes:    []reason.Code{reason.Success, reason.NoSubscriptionExisted},
		}, V5).(*Unsuback)
		if len(got.Codes) != 2 || got.Codes[1] != reason.NoSubscriptionExisted {
			t.Errorf("得到 %+v", got.Codes)
		}
	})
	t.Run("3.1.1 无载荷", func(t *testing.T) {
		buf, err := Encode(&Unsuback{PacketID: 9}, V311)
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) != 4 {
			t.Errorf("3.1.1 UNSUBACK 应为 4 字节，得到 % x", buf)
		}
		got := roundTrip(t, &Unsuback{PacketID: 9}, V311).(*Unsuback)
		if len(got.Codes) != 0 {
			t.Errorf("3.1.1 不应有原因码，得到 %+v", got.Codes)
		}
	})
	t.Run("5.0 缺原因码应报错", func(t *testing.T) {
		if _, err := Encode(&Unsuback{PacketID: 9}, V5); err == nil {
			t.Fatal("MQTT 5 的 UNSUBACK 缺原因码应被拒绝")
		}
	})
}

func TestPingRoundTrip(t *testing.T) {
	for _, v := range []Version{V311, V5} {
		roundTrip(t, &Pingreq{}, v)
		roundTrip(t, &Pingresp{}, v)
	}
}

func TestDisconnectRoundTrip(t *testing.T) {
	t.Run("3.1.1", func(t *testing.T) {
		buf, err := Encode(&Disconnect{}, V311)
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) != 2 {
			t.Errorf("3.1.1 DISCONNECT 应为 2 字节，得到 % x", buf)
		}
		roundTrip(t, &Disconnect{}, V311)
	})
	t.Run("5.0 精简", func(t *testing.T) {
		buf, err := Encode(&Disconnect{Code: reason.NormalDisconnection}, V5)
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) != 2 {
			t.Errorf("正常断开应省略原因码，得到 % x", buf)
		}
	})
	t.Run("5.0 带原因码与属性", func(t *testing.T) {
		d := &Disconnect{Code: reason.ServerMoved}
		d.Props.ServerReference = []byte("node-2.example:8883")
		d.Props.Set(PropServerReference)
		d.Props.ReasonString = []byte("节点腾空")
		d.Props.Set(PropReasonString)
		got := roundTrip(t, d, V5).(*Disconnect)
		if got.Code != reason.ServerMoved ||
			string(got.Props.ServerReference) != "node-2.example:8883" {
			t.Errorf("得到 %+v", got)
		}
	})
	t.Run("5.0 遗嘱断开", func(t *testing.T) {
		got := roundTrip(t, &Disconnect{Code: reason.DisconnectWithWill}, V5).(*Disconnect)
		if got.Code != reason.DisconnectWithWill {
			t.Errorf("原因码 = %s", got.Code)
		}
	})
}

func TestAuthRoundTrip(t *testing.T) {
	a := &Auth{Code: reason.ContinueAuthentication}
	a.Props.AuthMethod = []byte("SCRAM-SHA-256")
	a.Props.Set(PropAuthMethod)
	a.Props.AuthData = []byte{0x01, 0x02}
	a.Props.Set(PropAuthData)
	got := roundTrip(t, a, V5).(*Auth)
	if string(got.Props.AuthMethod) != "SCRAM-SHA-256" {
		t.Errorf("得到 %+v", got.Props)
	}
	if _, err := Encode(a, V311); err == nil {
		t.Fatal("3.1.1 下 AUTH 应被拒绝编码")
	}
}

func TestCloneDetachesFromBuffer(t *testing.T) {
	buf, err := Encode(&Publish{
		Topic:    []byte("a/b"),
		Payload:  []byte("hello"),
		QoS:      QoS1,
		PacketID: 1,
	}, V5)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := Decode(buf, V5)
	if err != nil {
		t.Fatal(err)
	}
	pub := p.(*Publish)
	clone := pub.Clone()
	// 擦掉原缓冲区，克隆体不应受影响
	for i := range buf {
		buf[i] = 0xFF
	}
	if string(clone.Topic) != "a/b" || string(clone.Payload) != "hello" {
		t.Errorf("克隆体仍引用原缓冲区: topic=%q payload=%q", clone.Topic, clone.Payload)
	}
}

func TestCloneAllTypes(t *testing.T) {
	c := &Connect{
		Version: V5, ClientID: []byte("c"), HasUsername: true, Username: []byte("u"),
		HasPassword: true, Password: []byte("p"),
		Will: &Will{Topic: []byte("t"), Payload: []byte("w")},
	}
	c.Props.ContentType = []byte("x")
	c.Props.Set(PropContentType)
	c.Props.UserProps = []UserProperty{{Key: []byte("a"), Value: []byte("b")}}
	c.Props.Set(PropUserProperty)
	c.Props.SubscriptionIDs = []uint32{1}
	if cc := c.Clone(); string(cc.ClientID) != "c" || cc.Will == nil ||
		string(cc.Props.UserProps[0].Key) != "a" {
		t.Error("Connect.Clone 不正确")
	}
	s := &Subscribe{PacketID: 1, Filters: []Subscription{{Filter: []byte("f")}}}
	if ss := s.Clone(); string(ss.Filters[0].Filter) != "f" {
		t.Error("Subscribe.Clone 不正确")
	}
	u := &Unsubscribe{PacketID: 1, Filters: [][]byte{[]byte("f")}}
	if uu := u.Clone(); string(uu.Filters[0]) != "f" {
		t.Error("Unsubscribe.Clone 不正确")
	}
	var nilProps *Properties
	if p := nilProps.Clone(); !p.Empty() {
		t.Error("nil Properties 的克隆应为空")
	}
}
