package shadow

import (
	"strings"
)

// Op 是影子操作。
type Op uint8

const (
	// OpNone 表示主题不是影子请求。
	OpNone Op = iota
	OpGet
	OpUpdate
	OpDelete
)

var opNames = [...]string{"none", "get", "update", "delete"}

func (o Op) String() string {
	if int(o) >= len(opNames) {
		return "unknown"
	}
	return opNames[o]
}

// 主题常量（PRD SHDW-001、SHDW-002）。
const (
	topicPrefix = "$aws/things/"
	shadowSeg   = "shadow"
	nameSeg     = "name"
)

// MaxShadowNameLen 是命名影子名长度上限（PRD SHDW-032、QTA-062）。
const MaxShadowNameLen = 64

// MaxClientTokenLen 是 clientToken 长度上限。
//
// 与 AWS 一致为 64 字节。它会被原样回传，不限长等于给了一个
// 把任意长度数据经影子响应放大扇出的手段。
const MaxClientTokenLen = 64

// Request 是从主题解析出的影子请求。
type Request struct {
	Thing string
	// Shadow 为空表示经典影子（PRD SHDW-001）。
	Shadow string
	Op     Op
}

// Key 返回分区键。
//
// 按 thingName 而非 thingName 加 shadowName 分区：同一 thing 的全部影子
// 必须落在同一分区，否则 ListNamedShadowsForThing 要跨分区扫描，
// 而它是一个常用接口（PRD SHDW-043）。
func (r Request) Key() string { return r.Thing }

// ParseRequest 解析影子请求主题。
//
// 只识别请求主题（/get、/update、/delete）。响应主题（/accepted 等）
// 由平台自己发布，设备向它们发布消息不是影子请求——若也识别，
// 一个订阅了 /update/accepted 又误发消息的设备会触发一次真实更新。
func ParseRequest(topic string) (Request, bool) {
	rest, ok := cutPrefix(topic, topicPrefix)
	if !ok {
		return Request{}, false
	}
	thing, rest, ok := cutSeg(rest)
	if !ok || thing == "" {
		return Request{}, false
	}
	seg, rest, ok := cutSeg(rest)
	if !ok || seg != shadowSeg {
		return Request{}, false
	}

	var name string
	// 命名影子：shadow/name/{shadowName}/{op}
	if next, after, ok := cutSeg(rest); ok && next == nameSeg {
		name, rest, ok = cutSeg(after)
		if !ok || name == "" {
			return Request{}, false
		}
	}

	var op Op
	switch rest {
	case "get":
		op = OpGet
	case "update":
		op = OpUpdate
	case "delete":
		op = OpDelete
	default:
		return Request{}, false
	}
	return Request{Thing: thing, Shadow: name, Op: op}, true
}

// Base 返回请求主题的前缀，响应主题由它拼出。
func (r Request) Base() string {
	var sb strings.Builder
	sb.Grow(len(topicPrefix) + len(r.Thing) + len(shadowSeg) +
		len(nameSeg) + len(r.Shadow) + 4)
	sb.WriteString(topicPrefix)
	sb.WriteString(r.Thing)
	sb.WriteByte('/')
	sb.WriteString(shadowSeg)
	if r.Shadow != "" {
		sb.WriteByte('/')
		sb.WriteString(nameSeg)
		sb.WriteByte('/')
		sb.WriteString(r.Shadow)
	}
	return sb.String()
}

// AcceptedTopic、RejectedTopic、DeltaTopic、DocumentsTopic 返回响应主题。
func (r Request) AcceptedTopic() string { return r.Base() + "/" + r.Op.String() + "/accepted" }
func (r Request) RejectedTopic() string { return r.Base() + "/" + r.Op.String() + "/rejected" }
func (r Request) DeltaTopic() string    { return r.Base() + "/update/delta" }
func (r Request) DocumentsTopic() string {
	return r.Base() + "/update/documents"
}

// ValidateShadowName 校验命名影子的名字。
//
// 字符集与 AWS 一致：字母、数字、冒号、下划线、连字符。
// 不允许斜杠——它会把一个影子名变成多个主题层级，使
// `shadow/name/a/b/update` 既像名为 "a/b" 的影子，又像名为 "a" 的
// 影子的某个未知操作。
func ValidateShadowName(name string) error {
	if name == "" {
		return nil // 经典影子
	}
	if len(name) > MaxShadowNameLen {
		return ErrInvalidShadowName
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == ':' || c == '_' || c == '-':
		default:
			return ErrInvalidShadowName
		}
	}
	return nil
}

// ValidateThingName 校验设备名。
func ValidateThingName(name string) error {
	if name == "" || len(name) > 128 {
		return ErrInvalidThingName
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == ':' || c == '_' || c == '-' || c == '.':
		default:
			return ErrInvalidThingName
		}
	}
	return nil
}

// validateClientToken 校验 clientToken。
func validateClientToken(t string) error {
	if len(t) > MaxClientTokenLen {
		return ErrInvalidClientToken
	}
	return nil
}

// cutPrefix 与 cutSeg 是主题切分的小工具。
func cutPrefix(s, prefix string) (string, bool) {
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	return s[len(prefix):], true
}

// cutSeg 切出第一个主题层级。
func cutSeg(s string) (seg, rest string, ok bool) {
	if s == "" {
		return "", "", false
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", true
}
