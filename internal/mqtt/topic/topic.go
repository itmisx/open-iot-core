// Package topic 实现 MQTT 主题名与主题过滤器的校验、匹配、分类，
// 以及一棵无指针的订阅树。
//
// 本包只依赖标准库与 internal/mqtt/reason，以保证协议层可独立 fuzz
// （PRD ARCH-052、MQTT-005）。
//
// 本包的所有函数都以 []byte 为输入并且不复制输入，调用方可以直接传入
// 解码缓冲区中的切片。
package topic

import (
	"bytes"
	"unicode/utf8"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// 共享订阅的固定前缀（MQTT-4.8.2、PRD MQTT-090）。
var sharePrefix = []byte("$share/")

// Limits 是主题相关的配额与行为开关。零值不可用，请从 DefaultLimits 出发修改。
type Limits struct {
	// MaxNameLen 是主题名的最大字节数（PRD QTA-013）。
	MaxNameLen int
	// MaxFilterLen 是主题过滤器的最大字节数。
	MaxFilterLen int
	// MaxSlashes 是斜杠数上限（PRD QTA-014）。
	// $share/{ShareName}/ 的两个斜杠与 Basic Ingest 的前三段不计入。
	MaxSlashes int
	// MaxShareName 是共享订阅组名的最大字节数（PRD MQTT-090）。
	MaxShareName int
	// AWSStrict 为 true 时退化为 AWS IoT Core 的行为：
	// `#` 不匹配父层级（PRD CMP-011），通配符订阅不接收保留消息（CMP-012）。
	// 默认 false，即遵循 OASIS 规范。
	AWSStrict bool
}

// DefaultLimits 返回与 AWS IoT Core 对齐的默认配额，但行为遵循 OASIS 规范。
func DefaultLimits() Limits {
	return Limits{
		MaxNameLen:   256,
		MaxFilterLen: 256,
		MaxSlashes:   7,
		MaxShareName: 128,
		AWSStrict:    false,
	}
}

// ValidateName 校验一个用于 PUBLISH 的主题名。
//
// 主题名不得含通配符（MQTT-3.3.2.1），不得为空，须为合法 UTF-8 且不含 U+0000。
// 允许为空的唯一场合是 MQTT 5 中由主题别名提供主题，该判断由 packet 包完成。
func ValidateName(name []byte, l Limits) error {
	if len(name) == 0 {
		return reason.Errorf(reason.TopicNameInvalid, "主题名不得为空")
	}
	if l.MaxNameLen > 0 && len(name) > l.MaxNameLen {
		return reason.Errorf(reason.TopicNameInvalid,
			"主题名长度 %d 超出上限 %d", len(name), l.MaxNameLen)
	}
	if err := validateUTF8(name, reason.TopicNameInvalid); err != nil {
		return err
	}
	slashes := 0
	for _, c := range name {
		switch c {
		case '+', '#':
			return reason.Errorf(reason.TopicNameInvalid, "主题名不得含通配符 %q", c)
		case '/':
			slashes++
		}
	}
	if l.MaxSlashes > 0 && slashes > l.MaxSlashes {
		return reason.Errorf(reason.TopicNameInvalid,
			"主题名的斜杠数 %d 超出上限 %d", slashes, l.MaxSlashes)
	}
	return nil
}

// ValidateFilter 校验一个用于 SUBSCRIBE 的主题过滤器，含共享订阅形式。
//
// 规则（MQTT-4.7）：
//   - `+` 必须独占一个层级；
//   - `#` 必须独占一个层级且是最后一级；
//   - 共享订阅形式为 $share/{ShareName}/{TopicFilter}，
//     ShareName 不得含 /、+、#，且前缀的两个斜杠不计入斜杠配额。
func ValidateFilter(filter []byte, l Limits) error {
	if len(filter) == 0 {
		return reason.Errorf(reason.TopicFilterInvalid, "主题过滤器不得为空")
	}
	if l.MaxFilterLen > 0 && len(filter) > l.MaxFilterLen {
		return reason.Errorf(reason.TopicFilterInvalid,
			"主题过滤器长度 %d 超出上限 %d", len(filter), l.MaxFilterLen)
	}
	if err := validateUTF8(filter, reason.TopicFilterInvalid); err != nil {
		return err
	}
	body := filter
	if share, rest, ok := ParseShared(filter); ok {
		if len(share) == 0 {
			return reason.Errorf(reason.TopicFilterInvalid, "共享订阅组名不得为空")
		}
		if l.MaxShareName > 0 && len(share) > l.MaxShareName {
			return reason.Errorf(reason.TopicFilterInvalid,
				"共享订阅组名长度 %d 超出上限 %d", len(share), l.MaxShareName)
		}
		if i := bytes.IndexAny(share, "/+#"); i >= 0 {
			return reason.Errorf(reason.TopicFilterInvalid,
				"共享订阅组名不得含 %q", share[i])
		}
		if len(rest) == 0 {
			return reason.Errorf(reason.TopicFilterInvalid, "共享订阅的过滤器部分不得为空")
		}
		body = rest
	} else if bytes.HasPrefix(filter, []byte("$share")) {
		// $share 开头但不符合 $share/{name}/{filter} 形式
		return reason.Errorf(reason.TopicFilterInvalid,
			"共享订阅形式须为 $share/{ShareName}/{TopicFilter}")
	}

	slashes := 0
	it := newIter(body)
	for {
		lvl, ok := it.next()
		if !ok {
			break
		}
		if i := bytes.IndexByte(lvl, '#'); i >= 0 {
			if len(lvl) != 1 {
				return reason.Errorf(reason.TopicFilterInvalid,
					"多层通配符 # 必须独占一个层级")
			}
			if !it.exhausted() {
				return reason.Errorf(reason.TopicFilterInvalid,
					"多层通配符 # 必须是最后一个层级")
			}
		}
		if i := bytes.IndexByte(lvl, '+'); i >= 0 && len(lvl) != 1 {
			return reason.Errorf(reason.TopicFilterInvalid,
				"单层通配符 + 必须独占一个层级")
		}
		if !it.exhausted() {
			slashes++
		}
	}
	if l.MaxSlashes > 0 && slashes > l.MaxSlashes {
		return reason.Errorf(reason.TopicFilterInvalid,
			"主题过滤器的斜杠数 %d 超出上限 %d", slashes, l.MaxSlashes)
	}
	return nil
}

// ParseShared 解析共享订阅过滤器 $share/{ShareName}/{TopicFilter}。
// ok 为 false 时 filter 不是共享订阅形式，share 与 rest 无意义。
//
// 返回的切片引用 filter，不做复制。
func ParseShared(filter []byte) (share, rest []byte, ok bool) {
	if !bytes.HasPrefix(filter, sharePrefix) {
		return nil, nil, false
	}
	tail := filter[len(sharePrefix):]
	i := bytes.IndexByte(tail, '/')
	if i < 0 {
		return nil, nil, false
	}
	return tail[:i], tail[i+1:], true
}

// IsShared 报告过滤器是否为共享订阅形式。
func IsShared(filter []byte) bool {
	_, _, ok := ParseShared(filter)
	return ok
}

// HasWildcard 报告过滤器是否含通配符。
// 用于 CMP-012：AWS 行为下通配符订阅不接收保留消息。
func HasWildcard(filter []byte) bool {
	return bytes.IndexByte(filter, '+') >= 0 || bytes.IndexByte(filter, '#') >= 0
}

// Match 报告主题过滤器是否匹配主题名。
//
// filter 可以是共享订阅形式，匹配时只看其过滤器部分。
// 调用方应保证二者已通过校验；本函数对未校验的输入不保证有意义的结果。
//
// 与规范的两处差异由 l.AWSStrict 控制：
//   - 规范下 `sport/#` 匹配 `sport` 本身；AWS 下不匹配（PRD CMP-011）。
//   - 以通配符开头的过滤器不匹配以 $ 开头的主题（MQTT-4.7.2），两种模式一致。
func Match(filter, name []byte, l Limits) bool {
	if _, rest, ok := ParseShared(filter); ok {
		filter = rest
	}
	fi, ti := newIter(filter), newIter(name)
	first := true
	for {
		fl, fok := fi.next()
		tl, tok := ti.next()
		switch {
		case fok && len(fl) == 1 && fl[0] == '#':
			// 以通配符开头的过滤器不得匹配 $ 开头的主题。
			if first && tok && len(tl) > 0 && tl[0] == '$' {
				return false
			}
			if l.AWSStrict {
				// AWS 行为：# 不匹配父层级，要求至少还有一个层级。
				return tok
			}
			// 规范行为：# 匹配剩余任意层级，含零层级。
			return true
		case !fok && !tok:
			return true
		case !fok || !tok:
			return false
		case len(fl) == 1 && fl[0] == '+':
			if first && len(tl) > 0 && tl[0] == '$' {
				return false
			}
		default:
			if !bytes.Equal(fl, tl) {
				return false
			}
		}
		first = false
	}
}

// --- 层级迭代 ---

// iter 按 '/' 切分主题的层级，不分配内存。
type iter struct {
	b    []byte
	done bool
}

func newIter(b []byte) iter { return iter{b: b} }

// next 取出下一个层级。ok 为 false 表示层级已耗尽。
// 注意空串会产出一个空层级，这与 MQTT 对尾部斜杠的处理一致：
// "a/" 的层级序列为 ["a", ""]。
func (it *iter) next() (lvl []byte, ok bool) {
	if it.done {
		return nil, false
	}
	i := bytes.IndexByte(it.b, '/')
	if i < 0 {
		lvl, it.b, it.done = it.b, nil, true
		return lvl, true
	}
	lvl, it.b = it.b[:i], it.b[i+1:]
	return lvl, true
}

// exhausted 报告是否已无更多层级。
func (it *iter) exhausted() bool { return it.done }

// validateUTF8 执行 MQTT 的 UTF-8 字符串规则，失败时返回指定的原因码。
func validateUTF8(b []byte, code reason.Code) error {
	for i := 0; i < len(b); {
		c := b[i]
		if c == 0x00 {
			return reason.Errorf(code, "含 U+0000")
		}
		if c < utf8.RuneSelf {
			i++
			continue
		}
		rn, size := utf8.DecodeRune(b[i:])
		if rn == utf8.RuneError && size <= 1 {
			return reason.Errorf(code, "第 %d 字节处不是良构的 UTF-8", i)
		}
		i += size
	}
	return nil
}
