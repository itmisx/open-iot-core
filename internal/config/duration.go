package config

import (
	"fmt"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Duration 是带 YAML 解析的时长。
//
// 不直接用 time.Duration：YAML 里 `30s` 是字符串，而 time.Duration 的
// 零值解析会把它当成整数纳秒。自己实现解析能在出错时指出具体字段与
// 可接受的写法，而不是给一个"无法把 string 转成 int64"的泛泛错误。
type Duration time.Duration

// D 返回标准时长。
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML 实现 yaml.Unmarshaler。
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("须为时长字符串（如 30s、5m、1h），得到 %s", n.Tag)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("无法解析时长 %q：可接受的写法如 500ms、30s、5m、1h", s)
	}
	if v < 0 {
		return fmt.Errorf("时长不得为负：%s", s)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML 使 `config print` 输出人类可读的时长。
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }
