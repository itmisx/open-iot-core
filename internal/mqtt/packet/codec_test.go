package packet

import (
	"bytes"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func TestVarintRoundTrip(t *testing.T) {
	// 四个长度分界点与若干边界值
	cases := []int{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, MaxRemainingLength}
	for _, want := range cases {
		buf := appendVarint(nil, want)
		if got := varintLen(want); got != len(buf) {
			t.Errorf("varintLen(%d) = %d，实际编码 %d 字节", want, got, len(buf))
		}
		r := &reader{b: buf}
		got, err := r.varint()
		if err != nil {
			t.Fatalf("解码 %d 失败: %v", want, err)
		}
		if got != want {
			t.Errorf("往返 %d 得到 %d", want, got)
		}
		if !r.done() {
			t.Errorf("解码 %d 后仍有 %d 字节未消费", want, r.remaining())
		}
	}
}

func TestVarintRejectsNonMinimal(t *testing.T) {
	// 0x80 0x00 把 0 写成了两字节，属非最小编码
	r := &reader{b: []byte{0x80, 0x00}}
	if _, err := r.varint(); err == nil {
		t.Fatal("非最小编码应被拒绝")
	}
}

func TestVarintRejectsTooLong(t *testing.T) {
	r := &reader{b: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F}}
	if _, err := r.varint(); err == nil {
		t.Fatal("超过 4 字节的变长整数应被拒绝")
	}
}

func TestVarintTruncated(t *testing.T) {
	r := &reader{b: []byte{0x80}}
	if _, err := r.varint(); err == nil {
		t.Fatal("截断的变长整数应被拒绝")
	}
}

func TestReaderPrimitives(t *testing.T) {
	buf := []byte{0x12, 0x34, 0x00, 0x00, 0x00, 0x05, 0x00, 0x02, 'h', 'i'}
	r := &reader{b: buf}
	if v, err := r.uint16(); err != nil || v != 0x1234 {
		t.Fatalf("uint16 = %#x, %v", v, err)
	}
	if v, err := r.uint32(); err != nil || v != 5 {
		t.Fatalf("uint32 = %d, %v", v, err)
	}
	s, err := r.str()
	if err != nil || string(s) != "hi" {
		t.Fatalf("str = %q, %v", s, err)
	}
	if !r.done() {
		t.Fatalf("应已读完，剩余 %d", r.remaining())
	}
	if _, err := r.byte(); err == nil {
		t.Fatal("读空应报错")
	}
}

func TestReaderTruncation(t *testing.T) {
	for _, tc := range []struct {
		name string
		buf  []byte
		fn   func(*reader) error
	}{
		{"uint16", []byte{1}, func(r *reader) error { _, err := r.uint16(); return err }},
		{"uint32", []byte{1, 2, 3}, func(r *reader) error { _, err := r.uint32(); return err }},
		{"bin 长度不足", []byte{0, 5, 1, 2}, func(r *reader) error { _, err := r.bin(); return err }},
		{"sub 长度不足", []byte{1}, func(r *reader) error { _, err := r.sub(4); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(&reader{b: tc.buf}); err == nil {
				t.Fatal("应报截断错误")
			}
		})
	}
}

func TestValidateUTF8(t *testing.T) {
	ok := []string{"", "hello", "中文主题", "a/b/c", "é", "\U0001F600"}
	for _, s := range ok {
		if err := ValidateUTF8([]byte(s)); err != nil {
			t.Errorf("%q 应合法: %v", s, err)
		}
	}
	bad := [][]byte{
		{0x00},                   // U+0000
		{'a', 0x00, 'b'},         // 内嵌空字符
		{0xC3},                   // 截断的两字节序列
		{0x80},                   // 孤立的续字节
		{0xED, 0xA0, 0x80},       // UTF-8 编码的代理项 U+D800
		{0xF5, 0x80, 0x80, 0x80}, // 超出 U+10FFFF
	}
	for _, b := range bad {
		if err := ValidateUTF8(b); err == nil {
			t.Errorf("% x 应被拒绝", b)
		}
	}
}

func TestReadFixedHeader(t *testing.T) {
	// PUBLISH，QoS 1，剩余长度 300
	buf := appendVarint([]byte{0x32}, 300)
	fh, n, err := ReadFixedHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if fh.Type != PUBLISH || fh.Flags != 0x02 || fh.Length != 300 {
		t.Fatalf("得到 %+v", fh)
	}
	if n != 3 {
		t.Fatalf("消费字节数 = %d，期望 3", n)
	}
}

func TestReadFixedHeaderRejectsReservedType(t *testing.T) {
	if _, _, err := ReadFixedHeader([]byte{0x00, 0x00}); err == nil {
		t.Fatal("类型 0 应被拒绝")
	}
	if _, _, err := ReadFixedHeader(nil); err == nil {
		t.Fatal("空输入应被拒绝")
	}
}

func TestDecodeIncomplete(t *testing.T) {
	full, err := Encode(&Publish{Topic: []byte("a/b"), Payload: []byte("xyz")}, V5)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(full); i++ {
		if _, _, err := Decode(full[:i], V5); err == nil {
			t.Fatalf("前 %d 字节应判定为不完整", i)
		} else if !errors.Is(err, ErrIncomplete) && i > 2 {
			// 前两字节可能在固定报头阶段就失败，这也是可接受的
			t.Fatalf("前 %d 字节期望 ErrIncomplete，得到 %v", i, err)
		}
	}
	if _, n, err := Decode(full, V5); err != nil || n != len(full) {
		t.Fatalf("完整报文应解码成功: n=%d err=%v", n, err)
	}
}

func TestTypeAndVersionStrings(t *testing.T) {
	if CONNECT.String() != "CONNECT" || Type(0).String() != "RESERVED" || Type(99).String() != "INVALID" {
		t.Error("Type.String 不正确")
	}
	if V311.String() != "3.1.1" || V5.String() != "5.0" {
		t.Error("Version.String 不正确")
	}
	if Version(3).Supported() || !V311.Supported() || !V5.Supported() {
		t.Error("Version.Supported 不正确")
	}
	if QoS(3).Valid() || !QoS2.Valid() {
		t.Error("QoS.Valid 不正确")
	}
}

func TestErrorIs(t *testing.T) {
	err := malformed("测试")
	if !errors.Is(err, ErrMalformed) {
		t.Error("应匹配 ErrMalformed")
	}
	if errors.Is(err, ErrProtocol) {
		t.Error("不应匹配 ErrProtocol")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("Malformed Packet")) {
		t.Errorf("错误信息应含原因码名称: %s", err.Error())
	}
}

func TestReasonCodes(t *testing.T) {
	if reason.Success.IsError() || !reason.NotAuthorized.IsError() {
		t.Error("IsError 不正确")
	}
	if reason.MalformedPacket.String() != "Malformed Packet" {
		t.Error("Code.String 不正确")
	}
	if reason.Code(0x77).String() != "Unknown" {
		t.Error("未知码应返回 Unknown")
	}
	for in, want := range map[reason.Code]reason.Return311{
		reason.Success:                    reason.Accepted311,
		reason.UnsupportedProtocolVersion: reason.UnacceptableProtocolVersion,
		reason.ClientIdentifierNotValid:   reason.IdentifierRejected,
		reason.BadUserNameOrPassword:      reason.BadUserNameOrPassword311,
		reason.NotAuthorized:              reason.NotAuthorized311,
		reason.Banned:                     reason.NotAuthorized311,
		reason.QuotaExceeded:              reason.NotAuthorized311,
		reason.ServerBusy:                 reason.Server311Unavailable,
	} {
		if got := reason.MapTo311(in); got != want {
			t.Errorf("MapTo311(%s) = %d，期望 %d", in, got, want)
		}
	}
}

// TestConnack311CodeSpace 是对 fuzz 发现的缺陷的回归测试：
// 3.1.1 的返回码与 MQTT 5 的原因码是不同的数值空间，
// 曾经被原样存入 Code 字段，导致 0x02（标识符被拒）被当作 GrantedQoS2，
// 重新编码后变成 0x03。
func TestConnack311CodeSpace(t *testing.T) {
	for raw, want := range map[byte]reason.Code{
		0: reason.Success,
		1: reason.UnsupportedProtocolVersion,
		2: reason.ClientIdentifierNotValid,
		3: reason.ServerUnavailable,
		4: reason.BadUserNameOrPassword,
		5: reason.NotAuthorized,
		6: reason.UnspecifiedError, // 3.1.1 未定义，归入未指定错误
	} {
		p, _, err := Decode([]byte{0x20, 0x02, 0x00, raw}, V311)
		if err != nil {
			t.Fatalf("返回码 %d 解码失败: %v", raw, err)
		}
		got := p.(*Connack).Code
		if got != want {
			t.Errorf("返回码 %d 映射为 %s，期望 %s", raw, got, want)
		}
		// 已定义的返回码必须可以无损回到线路上
		if raw <= 5 {
			buf, err := Encode(&Connack{Code: want}, V311)
			if err != nil {
				t.Fatal(err)
			}
			if buf[3] != raw {
				t.Errorf("%s 回编码为 %d，期望 %d", want, buf[3], raw)
			}
		}
	}
}

func TestAppendHeaderRejectsOversize(t *testing.T) {
	if _, err := appendHeader(nil, PUBLISH, 0, MaxRemainingLength+1); err == nil {
		t.Fatal("超长剩余长度应被拒绝")
	}
}
