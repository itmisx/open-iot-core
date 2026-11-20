// Package shadow 实现 Device Shadow 的文档语义与状态机。
//
// 状态机是纯的：它不碰网络、不读时钟、不做配额判定，全部由调用方注入。
// MQTT 与 REST 两条路径共用这同一份实现（PRD SHDW-044）——各自实现一遍
// 合并逻辑，等于保证两条路径的语义会慢慢分叉。
package shadow

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// 文档中的 JSON 值用 map[string]any 表示，数字用 json.Number。
//
// 不自建值类型树，有两个理由：
//   - Shadow 不在热路径上（每 thing 20 次/秒，QTA-063），可读性比
//     零分配更重要；
//   - json.Number 保留字面量。若解成 float64，设备上报的 1.50 会变成
//     1.5、大整数会丢精度，而影子文档是设备与云之间逐字段比对的契约，
//     这种"看起来等价"的改写会让对照测试出现无法解释的差异。
type Object = map[string]any

// MaxDepth 是 JSON 嵌套深度上限（PRD SHDW-031、QTA-061）。
//
// 固定而非可配：它的作用是防御递归爆栈与恶意构造，
// 放开它只会把攻击面交给配置。
const MaxDepth = 8

// Decode 解析影子状态 JSON。
//
// 用 Decoder 加 UseNumber 而不是 json.Unmarshal：后者把所有数字变成
// float64（见 Object 的说明）。DisallowUnknownFields 不适用——
// 影子状态本身就是任意用户数据。
func Decode(b []byte) (Object, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("无法解析 JSON: %w", err)
	}
	// 多余的尾随内容须报错：`{"a":1} {"b":2}` 这类输入若只取第一段，
	// 调用方会以为整条请求都被接受了。
	if dec.More() {
		return nil, fmt.Errorf("JSON 之后有多余内容")
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("顶层须为 JSON 对象")
	}
	return obj, nil
}

// Encode 序列化为 JSON。
func Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// 不转义 HTML：影子里的字符串是设备数据，把 < 变成 <
	// 会让文档与设备发出的内容不一致。
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder 会追加换行
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Depth 返回值的嵌套深度。标量为 0，`{"a":1}` 为 1。
func Depth(v any) int {
	switch t := v.(type) {
	case map[string]any:
		max := 0
		for _, child := range t {
			if d := Depth(child); d > max {
				max = d
			}
		}
		return max + 1
	case []any:
		max := 0
		for _, child := range t {
			if d := Depth(child); d > max {
				max = d
			}
		}
		return max + 1
	default:
		return 0
	}
}

// Clone 深拷贝一个值。
//
// 状态机的输入输出必须互不共享底层 map：调用方持有的请求对象若与
// 存储中的文档共享子树，之后对请求的任何改动都会静默改掉已存储的影子。
func Clone(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, child := range t {
			out[k] = Clone(child)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, child := range t {
			out[i] = Clone(child)
		}
		return out
	default:
		// 标量（含 json.Number、string、bool、nil）均为不可变值
		return v
	}
}

// CloneObject 深拷贝一个对象，nil 返回 nil。
func CloneObject(o Object) Object {
	if o == nil {
		return nil
	}
	return Clone(o).(map[string]any)
}

// Equal 判断两个 JSON 值是否相等。
//
// 自己实现而不用 reflect.DeepEqual：数字是 json.Number（字符串），
// DeepEqual 会把 1 与 1.0 判为不等。影子的语义是 JSON 语义，
// 这里按字面量比较并在注释中说明这一取舍。
func Equal(a, b any) bool {
	switch ta := a.(type) {
	case map[string]any:
		tb, ok := b.(map[string]any)
		if !ok || len(ta) != len(tb) {
			return false
		}
		for k, va := range ta {
			vb, ok := tb[k]
			if !ok || !Equal(va, vb) {
				return false
			}
		}
		return true
	case []any:
		tb, ok := b.([]any)
		if !ok || len(ta) != len(tb) {
			return false
		}
		for i := range ta {
			if !Equal(ta[i], tb[i]) {
				return false
			}
		}
		return true
	case json.Number:
		tb, ok := b.(json.Number)
		// 按字面量比较：1 与 1.0 视为不同。这与 AWS 一致——
		// 影子回传的是设备写入的字面量，而非规范化后的数值。
		return ok && string(ta) == string(tb)
	case nil:
		return b == nil
	default:
		return a == b
	}
}

// isObject 是一个便利判断。
func isObject(v any) bool {
	_, ok := v.(map[string]any)
	return ok
}
