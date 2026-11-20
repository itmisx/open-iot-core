package packet

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// FuzzDecode 断言解码器对任意输入都不 panic、不越界读、不无界分配。
// 畸形报文处理是此类项目的 CVE 高发区，而 broker 暴露在公网（PRD NFR-043、TST-059）。
func FuzzDecode(f *testing.F) {
	// 种子语料：每种报文的合法编码，加若干已知的畸形构造
	for _, v := range []Version{V311, V5} {
		seeds := []Packet{
			&Connect{
				Version: v, ClientID: []byte("dev"), CleanStart: true,
				Will: &Will{Topic: []byte("t"), Payload: []byte("w"), QoS: QoS1},
			},
			&Connack{SessionPresent: true, Code: reason.Success},
			&Publish{Topic: []byte("a/b/c"), Payload: []byte("hello"), QoS: QoS1, PacketID: 1},
			&Publish{Topic: []byte("a"), QoS: QoS2, PacketID: 2, Retain: true},
			&Ack{Kind: PUBACK, PacketID: 1},
			&Ack{Kind: PUBREL, PacketID: 1},
			&Subscribe{PacketID: 1, Filters: []Subscription{
				{Filter: []byte("a/#"), QoS: QoS2}, {Filter: []byte("$share/g/b/+")},
			}},
			&Suback{PacketID: 1, Codes: []reason.Code{reason.GrantedQoS1}},
			&Unsubscribe{PacketID: 1, Filters: [][]byte{[]byte("a/#")}},
			&Pingreq{}, &Pingresp{},
			&Disconnect{},
		}
		if v == V5 {
			seeds = append(seeds,
				&Unsuback{PacketID: 1, Codes: []reason.Code{reason.Success}},
				&Auth{Code: reason.ContinueAuthentication},
				&Disconnect{Code: reason.ServerMoved})
		} else {
			seeds = append(seeds, &Unsuback{PacketID: 1})
		}
		for _, p := range seeds {
			if b, err := Encode(p, v); err == nil {
				f.Add(b, byte(v))
			}
		}
	}
	// 人工构造的边界输入
	f.Add([]byte{0x00, 0x00}, byte(V5))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, byte(V5))
	f.Add([]byte{0x10, 0x7F}, byte(V5))
	f.Add([]byte{0x30, 0x00}, byte(V5))

	f.Fuzz(func(t *testing.T, data []byte, rawVer byte) {
		v := V5
		if rawVer%2 == 0 {
			v = V311
		}
		p, n, err := Decode(data, v)
		if err != nil {
			return
		}
		if n <= 0 || n > len(data) {
			t.Fatalf("消费字节数 %d 超出输入长度 %d", n, len(data))
		}
		// 解码成功的报文必须可以重新编码，且再次解码得到稳定结果。
		enc, err := Encode(p, v)
		if err != nil {
			// CONNECT 会携带自身版本，用另一版本重新编码可能合法失败。
			if p.Type() == CONNECT {
				return
			}
			t.Fatalf("%s 解码成功但无法重新编码: %v", p.Type(), err)
		}
		p2, _, err := Decode(enc, v)
		if err != nil {
			t.Fatalf("%s 重新编码后无法解码: %v (% x)", p.Type(), err, enc)
		}
		enc2, err := Encode(p2, v)
		if err != nil {
			t.Fatalf("%s 二次编码失败: %v", p.Type(), err)
		}
		if string(enc) != string(enc2) {
			t.Fatalf("%s 编码不稳定\n一次 % x\n二次 % x", p.Type(), enc, enc2)
		}
	})
}

// FuzzReadFixedHeader 单独 fuzz 固定报头解析，因为它是所有输入的第一道门。
func FuzzReadFixedHeader(f *testing.F) {
	f.Add([]byte{0x10, 0x00})
	f.Add([]byte{0x30, 0xFF, 0xFF, 0xFF, 0x7F})
	f.Add([]byte{0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		fh, n, err := ReadFixedHeader(data)
		if err != nil {
			return
		}
		if n < 2 || n > 5 {
			t.Fatalf("固定报头长度 %d 不合理", n)
		}
		if fh.Length < 0 || fh.Length > MaxRemainingLength {
			t.Fatalf("剩余长度 %d 超出范围", fh.Length)
		}
		if !fh.Type.Valid() {
			t.Fatalf("解出无效类型 %d", fh.Type)
		}
	})
}

// FuzzValidateUTF8 保证字符串校验不会 panic。
func FuzzValidateUTF8(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte("中文"))
	f.Add([]byte{0xED, 0xA0, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = ValidateUTF8(data)
	})
}
