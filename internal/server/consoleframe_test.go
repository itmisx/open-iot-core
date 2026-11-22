package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

// 控制台 WebSocket 的帧解析。
//
// 这条路径的输入来自浏览器，所以它是不可信输入。每一条拒绝都对应 RFC 6455
// 的一句硬要求；一条都不验的后果是一个畸形帧让服务端读出错长度，而那之后
// 读到的全是错位的字节——而现象只是「控制台的测试客户端偶尔不响应」。

func newConsoleFrameReader(raw []byte) *consoleWS {
	br := bufio.NewReader(bytes.NewReader(raw))
	return &consoleWS{rw: bufio.NewReadWriter(br, bufio.NewWriter(&bytes.Buffer{}))}
}

// consoleClientFrame 按浏览器方向组一帧：置 FIN、带掩码。
func consoleClientFrame(op byte, payload []byte) []byte {
	out := []byte{0x80 | op}
	mask := []byte{0x11, 0x22, 0x33, 0x44}
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

func TestConsoleReadFrameAcceptsWellFormed(t *testing.T) {
	for _, n := range []int{0, 7, 125, 126, 2000, 65536} {
		payload := bytes.Repeat([]byte{0x5a}, n)
		op, got, err := newConsoleFrameReader(consoleClientFrame(0x1, payload)).readFrame()
		if err != nil {
			t.Fatalf("长度 %d: %v", n, err)
		}
		if op != 0x1 || !bytes.Equal(got, payload) {
			t.Fatalf("长度 %d 解出 op=%#x %d 字节", n, op, len(got))
		}
	}
}

func TestConsoleReadFrameRejectsMalformed(t *testing.T) {
	long := func(u uint64) []byte {
		out := []byte{0x81, 127 | 0x80}
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], u)
		return append(append(out, l[:]...), 1, 2, 3, 4)
	}
	cases := []struct {
		name string
		raw  []byte
	}{
		// 分片帧不支持：控制台的命令都是单帧 JSON，接受分片等于要维护一个
		// 重组缓冲，而那是一条没人用的攻击面。
		{"未置 FIN", []byte{0x01, 0x80, 1, 2, 3, 4}},
		{"RSV 位非零", []byte{0x81 | 0x40, 0x80, 1, 2, 3, 4}},
		{"客户端帧未掩码", []byte{0x81, 0x01, 0x00}},
		{"64 位长度超过上限", long(maxConsoleFrame + 1)},
		{"16 位长度超过上限", func() []byte {
			out := []byte{0x81, 126 | 0x80, 0, 0}
			binary.BigEndian.PutUint16(out[2:4], uint16(65535))
			return append(out, 1, 2, 3, 4)
		}()},
		{"头部被截断", []byte{0x81}},
		{"扩展长度被截断", []byte{0x81, 126 | 0x80, 0x01}},
		{"掩码键被截断", []byte{0x81, 0x85, 1, 2}},
		{"负载被截断", []byte{0x81, 0x85, 1, 2, 3, 4, 0x00}},
	}
	for _, c := range cases {
		if _, _, err := newConsoleFrameReader(c.raw).readFrame(); err == nil {
			t.Fatalf("%s：必须被拒绝", c.name)
		}
	}
}
