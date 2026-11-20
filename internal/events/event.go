// Package events 实现结构化事件总线（契约 11）。
//
// 一个事件源，四个消费者：$aws/events 保留主题、控制台实时流、审计日志、
// 指标聚合。绝不为各消费者分别埋点（PRD ARCH-035、OBS-001）。
//
// 这条契约是「不可事后加」的典型：埋点位置分布在协议状态机、TLS 握手
// 回调、策略求值器内部，都是热路径与深层函数；而策略变量求值的中间结果
// 在未捕获时无法事后重建，补做等于重写策略求值器。
// AuthzFields.Vars 这一个字段，就是控制台的策略调试器能够存在的唯一前提。
package events

import (
	"net/netip"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// Kind 是事件类型。
type Kind uint8

const (
	// KindConn 是连接建立与断开。
	KindConn Kind = iota
	// KindSubscription 是订阅与退订。
	KindSubscription
	// KindAuthz 是授权判定。默认只记录拒绝，可配置采样率。
	KindAuthz
	// KindMeta 是元数据变更，吊销后踢连接走这条。
	KindMeta
	// KindQuota 是配额拒绝。
	KindQuota
	// KindCluster 是成员与角色变更。
	KindCluster
	// KindAudit 是管理面与兼容面的写操作。命令行签发不在这个进程里，
	// 它把同样的一行直接追加进审计文件，不另起一种类型。
	//
	// 它和运行日志分开：审计要留下操作者与请求摘要，而运行日志会按级别被丢掉。
	KindAudit
	// numKinds 必须是最后一项。
	numKinds
)

var kindName = [...]string{"Conn", "Subscription", "Authz", "Meta", "Quota", "Cluster", "Audit"}

// KindNames 返回全部事件类型名，顺序与 Kind 的取值一致。
//
// 供需要穷举类型的消费者使用（控制台的 SSE 监听、门禁测试），
// 避免它们各自维护一份会漂移的副本。
func KindNames() []string { return kindName[:] }

func (k Kind) String() string {
	if int(k) >= len(kindName) {
		return "Unknown"
	}
	return kindName[k]
}

// Mask 返回该类型在过滤器位掩码中的位。
func (k Kind) Mask() uint32 {
	if k >= numKinds {
		return 0
	}
	return 1 << uint(k)
}

// AllKinds 是匹配全部类型的掩码。
const AllKinds uint32 = (1 << uint(numKinds)) - 1

// ConnResult 是一次连接尝试的结果。
type ConnResult uint8

const (
	ConnAccepted ConnResult = iota
	ConnAuthFailed
	ConnThrottled
	ConnEvicted
	ConnClosed
	ConnViewStale
	ConnProtocolError
)

var connResultName = [...]string{
	"Accepted", "AuthFailed", "Throttled", "Evicted", "Closed", "ViewStale", "ProtocolError",
}

// NumConnResults 是 ConnResult 的取值个数。
//
// 由名字数组的长度推出，而不是手写一个数字：指标按该枚举定长
// 分维（见 metrics.go），若两者脱钩，新增的枚举值会静默落进
// 0 号桶，而那是个看起来正常的错误。
const NumConnResults ConnResult = ConnResult(len(connResultName))

func (r ConnResult) String() string {
	if int(r) >= len(connResultName) {
		return "Unknown"
	}
	return connResultName[r]
}

// AuthMethod 是认证方式。
type AuthMethod uint8

const (
	AuthNone AuthMethod = iota
	AuthMTLS
	AuthSigV4
	AuthWASM
	AuthEnhanced
)

var authMethodName = [...]string{"None", "mTLS", "SigV4", "WASM", "Enhanced"}

// NumAuthMethods 是 AuthMethod 的取值个数。
//
// 由名字数组的长度推出，而不是手写一个数字：指标按该枚举定长
// 分维（见 metrics.go），若两者脱钩，新增的枚举值会静默落进
// 0 号桶，而那是个看起来正常的错误。
const NumAuthMethods AuthMethod = AuthMethod(len(authMethodName))

func (m AuthMethod) String() string {
	if int(m) >= len(authMethodName) {
		return "Unknown"
	}
	return authMethodName[m]
}

// Decision 是授权判定结果。
type Decision uint8

const (
	// Allow 放行。
	Allow Decision = iota
	// ExplicitDeny 是策略中的显式拒绝，优先于任何放行。
	ExplicitDeny
	// ImplicitDeny 是无匹配语句导致的拒绝。
	ImplicitDeny
)

var decisionName = [...]string{"Allow", "ExplicitDeny", "ImplicitDeny"}

// NumDecisions 是 Decision 的取值个数。
//
// 由名字数组的长度推出，而不是手写一个数字：指标按该枚举定长
// 分维（见 metrics.go），若两者脱钩，新增的枚举值会静默落进
// 0 号桶，而那是个看起来正常的错误。
const NumDecisions Decision = Decision(len(decisionName))

func (d Decision) String() string {
	if int(d) >= len(decisionName) {
		return "Unknown"
	}
	return decisionName[d]
}

// Action 是被判定的 MQTT 动作。
type Action uint8

const (
	ActConnect Action = iota
	ActPublish
	ActSubscribe
	ActReceive
	ActRetainPublish
)

var actionName = [...]string{"Connect", "Publish", "Subscribe", "Receive", "RetainPublish"}

// NumActions 是 Action 的取值个数。
//
// 由名字数组的长度推出，而不是手写一个数字：指标按该枚举定长
// 分维（见 metrics.go），若两者脱钩，新增的枚举值会静默落进
// 0 号桶，而那是个看起来正常的错误。
const NumActions Action = Action(len(actionName))

func (a Action) String() string {
	if int(a) >= len(actionName) {
		return "Unknown"
	}
	return actionName[a]
}

// maxVars 是单次授权判定记录的策略变量上限。
//
// 策略变量的种类有限（clientId、thingName、证书主题与签发者的若干字段），
// 实际引用通常不超过三四个，因此用定长数组而非 map——后者会让每条
// 授权事件都分配一次，而授权判定在热路径上。
const maxVars = 8

// Var 是一条策略变量的求值结果。
type Var struct {
	Name  string
	Value string
}

// ConnFields 是连接事件的字段（PRD ARCH-036）。
type ConnFields struct {
	Result     ConnResult
	ReasonCode reason.Code
	// ReasonText 是人类可读原因，控制台直接展示。
	// AWS 只能去日志服务翻晦涩字符串，这里是明确的改进点。
	ReasonText string
	Epoch      cluster.Epoch
	CertFP     string
	CertCN     string
	AuthMethod AuthMethod
	ProtoVer   uint8
	Remote     netip.AddrPort
	TLSVersion uint16
	ALPN       string
	// HandshakeMS 是 TLS 握手耗时，用于定位握手 CPU 瓶颈。
	HandshakeMS float64
	// Duration 是连接存续毫秒数，仅断开事件有意义。
	DurationMS int64
}

// SubFields 是订阅事件的字段。
type SubFields struct {
	Filter string
	Share  string
	QoS    uint8
	// Unsubscribe 为 true 表示退订。
	Unsubscribe bool
	Granted     reason.Code
}

// AuthzFields 是授权判定事件的字段（PRD ARCH-037、AUTH-035）。
type AuthzFields struct {
	Action   Action
	Resource string
	Decision Decision
	// PolicyName 与 StatementIndex 指出命中的语句，
	// 使控制台能告诉用户「为什么被拒」而不只是「被拒了」。
	PolicyName     string
	StatementIndex int
	// Vars 是全部策略变量的求值结果。NVars 为有效条数。
	//
	// 这个字段无法事后重建：变量求值的中间结果如果不在判定时捕获，
	// 策略调试器就不可能存在（PRD AUTH-035）。
	Vars  [maxVars]Var
	NVars uint8
	// VarsTruncated 表示变量数超过 maxVars 被截断。
	VarsTruncated bool
}

// SetVar 记录一条策略变量求值结果。超出上限时置截断标记。
func (a *AuthzFields) SetVar(name, value string) {
	if int(a.NVars) >= maxVars {
		a.VarsTruncated = true
		return
	}
	a.Vars[a.NVars] = Var{Name: name, Value: value}
	a.NVars++
}

// VarList 返回已记录的变量切片。
func (a *AuthzFields) VarList() []Var { return a.Vars[:a.NVars] }

// MetaFields 是元数据变更事件的字段。
type MetaFields struct {
	Revision uint64
	// CertFP 非空表示证书类变更。
	CertFP string
	// RequiresDisconnect 为 true 时，消费者须断开使用该证书的连接。
	// 吊销后立即踢连接走这条链路（PRD PKI-030、CLU-019）。
	RequiresDisconnect bool
	Thing              string
	Reason             string
}

// QuotaFields 是配额拒绝事件的字段。
type QuotaFields struct {
	QuotaName string
	Subject   string
	Limit     int
}

// ClusterFields 是集群变更事件的字段。
type ClusterFields struct {
	Member cluster.NodeID
	Role   string
	Detail string
}

// AuditFields 是一次写操作的审计字段。
//
// Target 只放名字或指纹。策略正文、私钥、令牌、签名和设备负载都不进这里：
// 审计文件会长期留在磁盘上，这些内容进去就下不来了。
type AuditFields struct {
	Actor  string
	Remote string
	Method string
	Path   string
	Action string
	Target string
}

// Event 是事件总线上的一条记录。
//
// 用单一结构体加类型判别位，而不是接口：接口会让每条事件装箱分配，
// 而总线是热路径（每连接一条、每次授权拒绝一条）。
// 按类型分组的字段未使用时留零值。
type Event struct {
	Kind Kind
	// Node 与 Seq 共同构成事件的顺序来源。
	// 契约 08 禁止用墙上时钟排序，AtUnixMs 只用于展示与保留期判定。
	Node     cluster.NodeID
	Seq      uint64
	AtUnixMs int64

	ClientID string
	Thing    string

	Conn    ConnFields
	Sub     SubFields
	Authz   AuthzFields
	Meta    MetaFields
	Quota   QuotaFields
	Cluster ClusterFields
	Audit   AuditFields
}

// ParseKind 解析事件类型名。
//
// 供 Admin 面的事件流按类型筛选使用。名字取 Kind.String() 的输出，
// 比较时忽略大小写——它来自 URL 查询参数，要求用户记住大小写
// 只会制造无谓的失败。
func ParseKind(s string) (Kind, bool) {
	for i, n := range kindName {
		if equalFold(s, n) {
			return Kind(i), true
		}
	}
	return 0, false
}

// NumKinds 返回事件类型的数量，供调用方遍历。
func NumKinds() Kind { return numKinds }

// equalFold 是 ASCII 的大小写无关比较。
//
// 不用 strings.EqualFold 是因为本包在契约 02 的 import 约束下保持精简，
// 且事件类型名全为 ASCII 字母。
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
