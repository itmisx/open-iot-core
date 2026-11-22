package broker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

// newFrameReader 造一个只用来解帧的服务端 wsConn。
//
// 不走真的网络：这里要验的是**拒绝**，而每一条拒绝都对应 RFC 6455 的一句
// 硬要求。一条都不验的后果是对端可以用一个畸形帧让服务端读出错长度，而
// 那一步之后读到的全是错位的字节。
func newFrameReader(raw []byte) *wsConn {
	return &wsConn{br: bufio.NewReader(bytes.NewReader(raw)), client: false}
}

// clientFrame 按客户端方向组一帧（带掩码），服务端读它应当成功。
func clientFrame(op byte, fin bool, payload []byte) []byte {
	b0 := op
	if fin {
		b0 |= 0x80
	}
	out := []byte{b0}
	mask := []byte{0x01, 0x02, 0x03, 0x04}
	switch n := len(payload); {
	case n < 126:
		out = append(out, byte(n)|0x80)
	case n <= 65535:
		out = append(out, 126|0x80, 0, 0)
		binary.BigEndian.PutUint16(out[len(out)-2:], uint16(n))
	default:
		out = append(out, 127|0x80)
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], uint64(n))
		out = append(out, l[:]...)
	}
	out = append(out, mask...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	return append(out, masked...)
}

// TestWSReadFrameAcceptsWellFormed 钉住三种长度编码都能解。
func TestWSReadFrameAcceptsWellFormed(t *testing.T) {
	for _, n := range []int{0, 5, 125, 126, 1000, 65535, 65536} {
		payload := bytes.Repeat([]byte{0xa5}, n)
		c := newFrameReader(clientFrame(0x2, true, payload))
		fin, op, got, err := c.readFrame()
		if err != nil {
			t.Fatalf("长度 %d: %v", n, err)
		}
		if !fin || op != 0x2 || !bytes.Equal(got, payload) {
			t.Fatalf("长度 %d 解出 fin=%v op=%#x %d 字节", n, fin, op, len(got))
		}
	}
}

// TestWSReadFrameRejectsMalformed 钉住每一条拒绝。
//
// 这些拒绝都不是「宁严勿宽」的洁癖：非最短长度编码与掩码方向错误是
// 走私畸形帧的常用手法，而 RSV 位非零意味着对端以为协商过某个扩展。
func TestWSReadFrameRejectsMalformed(t *testing.T) {
	long := func(u uint64) []byte {
		out := []byte{0x82, 127 | 0x80}
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], u)
		return append(append(out, l[:]...), 1, 2, 3, 4)
	}
	cases := []struct {
		name string
		raw  []byte
	}{
		{"RSV 位非零", []byte{0x82 | 0x40, 0x80, 1, 2, 3, 4}},
		{"16 位长度不是最短形式", []byte{0x82, 126 | 0x80, 0x00, 0x7d, 1, 2, 3, 4}},
		{"64 位长度不是最短形式", long(100)},
		{"64 位长度超过 1 MiB", long(maxWSPayload + 1)},
		{"64 位长度设了最高位", long(1 << 63)},
		{"控制帧未置 FIN", []byte{0x08, 0x80, 1, 2, 3, 4}},
		{"控制帧负载超过 125", append([]byte{0x88, 126 | 0x80, 0x00, 0x7e, 1, 2, 3, 4},
			bytes.Repeat([]byte{0}, 126)...)},
		{"服务端读到未掩码的帧", []byte{0x82, 0x01, 0x00}},
		{"头部被截断", []byte{0x82}},
		{"掩码键被截断", []byte{0x82, 0x85, 1, 2}},
		{"负载被截断", []byte{0x82, 0x85, 1, 2, 3, 4, 0x00}},
	}
	for _, c := range cases {
		if _, _, _, err := newFrameReader(c.raw).readFrame(); err == nil {
			t.Fatalf("%s：必须被拒绝", c.name)
		}
	}
}

// TestWSReadFrameClientDirectionWantsUnmasked 钉住掩码方向的另一半。
//
// 客户端侧读到带掩码的帧同样是对端违规。两个方向都要验，否则只改一边的
// 判定不会让任何用例失败。
func TestWSReadFrameClientDirectionWantsUnmasked(t *testing.T) {
	// 服务端发给客户端的帧不带掩码：客户端侧应当接受。
	raw := []byte{0x82, 0x02, 0xaa, 0xbb}
	c := &wsConn{br: bufio.NewReader(bytes.NewReader(raw)), client: true}
	fin, op, payload, err := c.readFrame()
	if err != nil || !fin || op != 0x2 || !bytes.Equal(payload, []byte{0xaa, 0xbb}) {
		t.Fatalf("客户端读未掩码帧 = %v %v %#x %v", err, fin, op, payload)
	}
	// 而带掩码的要拒绝。
	c = &wsConn{br: bufio.NewReader(bytes.NewReader(clientFrame(0x2, true, []byte{1}))), client: true}
	if _, _, _, err := c.readFrame(); err == nil {
		t.Fatal("客户端读到带掩码的帧必须拒绝")
	}
}
