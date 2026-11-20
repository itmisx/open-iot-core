package packet

import (
	"encoding/binary"
	"unicode/utf8"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// reader 是对报文字节的带边界检查的游标。所有返回的切片都引用底层缓冲区，
// 缓冲区归属参见包注释。
type reader struct {
	b []byte
	i int
}

func (r *reader) remaining() int { return len(r.b) - r.i }

func (r *reader) done() bool { return r.i >= len(r.b) }

func (r *reader) byte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, malformed("truncated: want 1 byte, have 0")
	}
	c := r.b[r.i]
	r.i++
	return c, nil
}

func (r *reader) uint16() (uint16, error) {
	if r.remaining() < 2 {
		return 0, malformed("truncated: want 2 bytes, have %d", r.remaining())
	}
	v := binary.BigEndian.Uint16(r.b[r.i:])
	r.i += 2
	return v, nil
}

func (r *reader) uint32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, malformed("truncated: want 4 bytes, have %d", r.remaining())
	}
	v := binary.BigEndian.Uint32(r.b[r.i:])
	r.i += 4
	return v, nil
}

// varint 解码变长整数（MQTT-1.5.5）。非最小编码以及超过 4 字节的值均为畸形。
func (r *reader) varint() (int, error) {
	var value int
	var multiplier int = 1
	for n := 0; n < 4; n++ {
		c, err := r.byte()
		if err != nil {
			return 0, malformed("truncated variable byte integer")
		}
		value += int(c&0x7f) * multiplier
		if c&0x80 == 0 {
			// 拒绝非最小编码：一个没有贡献数值的后续字节，说明该值本可以写得更短。
			if n > 0 && c == 0 {
				return 0, malformed("non-minimal variable byte integer")
			}
			return value, nil
		}
		multiplier *= 128
	}
	return 0, malformed("variable byte integer exceeds 4 bytes")
}

// bin 读取带两字节长度前缀的二进制字段（MQTT-1.5.6）。
func (r *reader) bin() ([]byte, error) {
	n, err := r.uint16()
	if err != nil {
		return nil, err
	}
	if r.remaining() < int(n) {
		return nil, malformed("truncated binary data: want %d bytes, have %d", n, r.remaining())
	}
	v := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return v, nil
}

// str 读取带两字节长度前缀的 UTF-8 字符串，并按 MQTT-1.5.4 校验。
func (r *reader) str() ([]byte, error) {
	v, err := r.bin()
	if err != nil {
		return nil, err
	}
	if err := validateUTF8(v); err != nil {
		return nil, err
	}
	return v, nil
}

// rest 返回尚未消费的剩余部分，并把游标推到末尾。
func (r *reader) rest() []byte {
	v := r.b[r.i:]
	r.i = len(r.b)
	return v
}

// sub 返回覆盖接下来 n 个字节的 reader，并把游标推过这些字节。
func (r *reader) sub(n int) (*reader, error) {
	if r.remaining() < n {
		return nil, malformed("truncated section: want %d bytes, have %d", n, r.remaining())
	}
	s := &reader{b: r.b[r.i : r.i+n]}
	r.i += n
	return s, nil
}

// validateUTF8 执行 MQTT 的 UTF-8 字符串规则：编码必须良构、不含 U+0000、
// 不含代理项码点。代理项本身已被良构性检查排除（其 UTF-8 编码非法），
// 而空字符检查是 MQTT 特有的要求。
func validateUTF8(b []byte) error {
	for i := 0; i < len(b); {
		c := b[i]
		if c == 0x00 {
			return malformed("string contains U+0000")
		}
		if c < utf8.RuneSelf {
			i++
			continue
		}
		rn, size := utf8.DecodeRune(b[i:])
		if rn == utf8.RuneError && size <= 1 {
			return malformed("string is not well-formed UTF-8 at byte %d", i)
		}
		i += size
	}
	return nil
}

// ValidateUTF8 报告 b 是否满足 MQTT 的 UTF-8 字符串规则。
// 导出以供 topic 包与测试使用。
func ValidateUTF8(b []byte) error { return validateUTF8(b) }

// --- 编码辅助函数 ---

func appendUint16(dst []byte, v uint16) []byte {
	return append(dst, byte(v>>8), byte(v))
}

func appendUint32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// appendVarint 写出一个变长整数。超过 MaxRemainingLength 的值无法表示。
func appendVarint(dst []byte, v int) []byte {
	if v < 0 {
		v = 0
	}
	for {
		c := byte(v % 128)
		v /= 128
		if v > 0 {
			c |= 0x80
		}
		dst = append(dst, c)
		if v == 0 {
			return dst
		}
	}
}

// varintLen 返回 v 作为变长整数编码后的字节数。
func varintLen(v int) int {
	switch {
	case v < 128:
		return 1
	case v < 16384:
		return 2
	case v < 2097152:
		return 3
	default:
		return 4
	}
}

func appendBin(dst []byte, v []byte) []byte {
	dst = appendUint16(dst, uint16(len(v)))
	return append(dst, v...)
}

// appendHeader 为可变报头与负载合计 bodyLen 字节的报文写出固定报头。
func appendHeader(dst []byte, t Type, flags byte, bodyLen int) ([]byte, error) {
	if bodyLen > MaxRemainingLength {
		return nil, codeErr(reason.PacketTooLarge,
			"remaining length %d exceeds %d", bodyLen, MaxRemainingLength)
	}
	dst = append(dst, byte(t)<<4|flags&0x0f)
	return appendVarint(dst, bodyLen), nil
}

// ReadFixedHeader 从 b 的开头解码固定报头，返回报头与消费的字节数。
// 它不要求 b 中已有完整报文，因此可用于驱动流式读取循环。
func ReadFixedHeader(b []byte) (FixedHeader, int, error) {
	if len(b) == 0 {
		return FixedHeader{}, 0, malformed("empty input")
	}
	r := &reader{b: b}
	first, _ := r.byte()
	t := Type(first >> 4)
	if !t.Valid() {
		return FixedHeader{}, 0, malformed("reserved packet type 0")
	}
	n, err := r.varint()
	if err != nil {
		return FixedHeader{}, 0, err
	}
	return FixedHeader{Type: t, Flags: first & 0x0f, Length: n}, r.i, nil
}
