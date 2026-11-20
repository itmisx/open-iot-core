// Package authz 实现 AWS IoT 风格的策略求值。
//
// 这是本项目的核心差异点：策略在进程内求值，零回调、零网络跳。
// 契约 05 的内存物化视图保证这条路径不触网，契约 11 的 AuthzFields
// 保证判定过程可被控制台回放。
//
// 求值器是纯函数：输入为编译后的策略集与请求上下文，无外部依赖，
// 便于全覆盖单测与模糊测试（PRD AUTH-026）。
// 策略在 apply 阶段一次性编译为匹配器，消息路径上不解析 JSON
// （PRD ARCH-024、AUTH-025）。
package authz

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Action 是被判定的 MQTT 动作。
type Action uint8

const (
	ActConnect Action = iota
	ActPublish
	ActSubscribe
	ActReceive
	ActRetainPublish
	numActions
)

var actionNames = [...]string{
	"iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive", "iot:RetainPublish",
}

func (a Action) String() string {
	if int(a) >= len(actionNames) {
		return "iot:Unknown"
	}
	return actionNames[a]
}

// mask 返回该动作在语句动作位掩码中的位。
func (a Action) mask() uint32 {
	if a >= numActions {
		return 0
	}
	return 1 << uint(a)
}

// ResourceType 返回该动作判定的资源类型。
//
// 这个映射是 PRD AUTH-023 的落地，也是最容易做错的一处：
// iot:Subscribe 校验 topicfilter/ 资源，而 iot:Receive 校验 topic/ 资源。
// 二者必须分别判定——把它们合并会让「允许订阅通配符」意外等价于
// 「允许接收该通配符下的全部主题」，或者反过来让正常订阅被拒。
func (a Action) ResourceType() string {
	switch a {
	case ActConnect:
		return "client"
	case ActSubscribe:
		return "topicfilter"
	case ActPublish, ActReceive, ActRetainPublish:
		return "topic"
	default:
		return ""
	}
}

// ParseAction 解析策略文档中的动作名。
func ParseAction(s string) (Action, bool) {
	for i, n := range actionNames {
		if strings.EqualFold(s, n) {
			return Action(i), true
		}
	}
	return 0, false
}

// Effect 是语句的效果。
type Effect uint8

const (
	// EffectDeny 是显式拒绝。它优先于任何放行（PRD AUTH-021）。
	EffectDeny Effect = iota
	EffectAllow
)

func (e Effect) String() string {
	if e == EffectAllow {
		return "Allow"
	}
	return "Deny"
}

// Document 是一份 IoT 策略文档。
type Document struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement 是策略文档中的一条语句。
type Statement struct {
	Sid      string     `json:"Sid,omitempty"`
	Effect   string     `json:"Effect"`
	Action   StringList `json:"Action"`
	Resource StringList `json:"Resource"`
	// Condition 不被支持。
	//
	// 它必须被显式拒绝而非静默忽略：忽略一个本应收紧权限的 Condition
	// 会把受限策略变成放行策略，这是安全缺陷而非兼容性缺口。
	Condition json.RawMessage `json:"Condition,omitempty"`
	// NotAction 与 NotResource 同样不被支持，理由相同。
	NotAction   json.RawMessage `json:"NotAction,omitempty"`
	NotResource json.RawMessage `json:"NotResource,omitempty"`
	// Principal 在身份策略中无意义，出现即为误用。
	Principal json.RawMessage `json:"Principal,omitempty"`
}

// StringList 接受字符串或字符串数组。
//
// AWS 的策略文档对 Action 与 Resource 两种写法都接受，
// 设备 SDK 示例与控制台生成的策略混用，必须都能解析。
type StringList []string

// UnmarshalJSON 实现 json.Unmarshaler。
func (l *StringList) UnmarshalJSON(b []byte) error {
	b = trimSpace(b)
	if len(b) == 0 {
		return fmt.Errorf("authz: 空值")
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*l = StringList{s}
		return nil
	}
	var ss []string
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	*l = ss
	return nil
}

// MarshalJSON 单元素时写成字符串，与 AWS 的习惯一致。
func (l StringList) MarshalJSON() ([]byte, error) {
	if len(l) == 1 {
		return json.Marshal(l[0])
	}
	return json.Marshal([]string(l))
}

func trimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && isSpace(b[i]) {
		i++
	}
	for j > i && isSpace(b[j-1]) {
		j--
	}
	return b[i:j]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// ParseDocument 解析一份策略文档。
//
// 解析只做语法与支持范围检查；语义编译在 Compile 中完成。
func ParseDocument(b []byte) (*Document, error) {
	var d Document
	dec := json.NewDecoder(strings.NewReader(string(b)))
	// 拒绝未知字段：策略文档里出现拼错的字段名（如 "Resouce"）会让该语句
	// 失去约束作用，静默接受是危险的。
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("authz: 策略文档解析失败: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("authz: 策略文档含多余内容")
	}
	if len(d.Statement) == 0 {
		return nil, fmt.Errorf("authz: 策略文档没有语句")
	}
	for i := range d.Statement {
		if err := d.Statement[i].validate(i); err != nil {
			return nil, err
		}
	}
	return &d, nil
}

func (s *Statement) validate(idx int) error {
	switch {
	case len(s.Condition) > 0:
		return fmt.Errorf("authz: 语句 %d 含 Condition，本实现不支持；"+
			"静默忽略它会把受限策略变成放行策略，故显式拒绝", idx)
	case len(s.NotAction) > 0:
		return fmt.Errorf("authz: 语句 %d 含 NotAction，本实现不支持", idx)
	case len(s.NotResource) > 0:
		return fmt.Errorf("authz: 语句 %d 含 NotResource，本实现不支持", idx)
	case len(s.Principal) > 0:
		return fmt.Errorf("authz: 语句 %d 含 Principal，身份策略中无此字段", idx)
	}
	switch strings.ToLower(s.Effect) {
	case "allow", "deny":
	default:
		return fmt.Errorf("authz: 语句 %d 的 Effect 为 %q，须为 Allow 或 Deny", idx, s.Effect)
	}
	if len(s.Action) == 0 {
		return fmt.Errorf("authz: 语句 %d 没有 Action", idx)
	}
	if len(s.Resource) == 0 {
		return fmt.Errorf("authz: 语句 %d 没有 Resource", idx)
	}
	return nil
}

// effect 返回语句的效果。validate 已保证取值合法。
func (s *Statement) effect() Effect {
	if strings.EqualFold(s.Effect, "allow") {
		return EffectAllow
	}
	return EffectDeny
}
