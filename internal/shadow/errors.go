package shadow

import "fmt"

// Error 是影子操作的错误，带 AWS 兼容的状态码与文案。
//
// 文案与 AWS 对齐是刻意的：SDK 与既有运维脚本会匹配这些字符串。
// 自创文案会让"把 endpoint 换成本平台"这件事在错误处理分支上崩掉。
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%d %s", e.Code, e.Message) }

// Response 构造带 clientToken 的错误响应。
func (e *Error) Response(token string) *ErrorResponse {
	return &ErrorResponse{Code: e.Code, Message: e.Message, ClientToken: token}
}

// 预定义错误。状态码取值与 AWS Device Shadow 一致。
var (
	ErrInvalidJSON        = &Error{400, "Invalid JSON"}
	ErrMissingState       = &Error{400, "Missing required node: state"}
	ErrStateNotObject     = &Error{400, "State node must be an object"}
	ErrEmptyState         = &Error{400, "State node must not be empty"}
	ErrInvalidClientToken = &Error{400, "Invalid clientToken"}
	ErrInvalidShadowName  = &Error{400, "Invalid shadow name"}
	ErrInvalidThingName   = &Error{400, "Invalid thing name"}
	ErrUnsupportedSection = &Error{400,
		"State contains an unsupported node; only desired and reported are allowed"}
	ErrNotFound        = &Error{404, "No shadow exists with name: "}
	ErrVersionConflict = &Error{409, "Version conflict"}
	ErrPayloadTooLarge = &Error{413, "The payload exceeds the maximum size allowed"}
	ErrTooManyRequests = &Error{429, "Too many requests"}
	ErrInternal        = &Error{500, "Internal service failure"}
)

// errDepthExceeded 构造深度超限错误。
func errDepthExceeded() *Error {
	return &Error{400, fmt.Sprintf(
		"JSON contains too many levels of nesting; maximum is %d", MaxDepth)}
}

// errNotFound 构造带影子名的 404。
//
// 带上名字是必要的：一个 thing 可以有多个命名影子，
// 不说明是哪一个的 404 无法排查。
func errNotFound(name string) *Error {
	n := name
	if n == "" {
		n = "classic"
	}
	return &Error{404, "No shadow exists with name: " + n}
}

// errForwardFailed 构造转发失败错误（集群路径）。
func errForwardFailed(owner string) *Error {
	return &Error{500, "Shadow partition is owned by another node (" + owner +
		") and forwarding failed"}
}
