// Package quota 实现配额判定与作用域划分（契约 09）。
//
// 核心是把每项配额的作用域在类型层面固定下来：
//   - 本地作用域（每连接、每节点）精确判定，不触网；
//   - 全局作用域（每账户、全集群）在集群下只能是近似的——本地额度分摊
//     加周期再均衡，允许短时超限。
//
// 这个分类本身就是设计决策，不能含糊（PRD ARCH-031、QTA-002）：
// 事后才区分会发现有些限流点语义上本该全局却只在本地判，
// 集群下配额直接放大 N 倍，而这类问题只在压测或线上才暴露。
package quota

import (
	"fmt"
	"sort"
)

// Scope 是配额的作用域。
type Scope uint8

const (
	// ScopeLocal 是每连接或每节点的配额，判定精确且无网络开销。
	ScopeLocal Scope = iota
	// ScopeGlobal 是每账户或全集群的配额。单机下等同精确；
	// 集群下按节点分摊额度并周期再均衡，是近似的。
	ScopeGlobal
)

func (s Scope) String() string {
	if s == ScopeGlobal {
		return "全局"
	}
	return "本地"
}

// Kind 区分配额的判定方式。
type Kind uint8

const (
	// Rate 是速率配额，按令牌桶判定。
	Rate Kind = iota
	// Count 是计数配额，须成对 Acquire 与 Release。
	Count
	// Size 是单次大小上限，直接比较。
	Size
)

// ID 是配额编号。
//
// 用编号而非字符串作为配额标识，是为了让热路径上的配额判定零分配：
// 若以 "配额名/作用对象" 的复合字符串为键，每次判定都要拼接字符串
// （违反 PRD LANG-007）。
type ID uint16

// 配额编号。与 docs/prd/17-quotas.md 的 QTA 条目一一对应。
const (
	// --- 本地：每连接 ---
	PayloadSize        ID = iota // QTA-010 MQTT 负载大小
	PacketSize                   // QTA-011 MQTT 5 最大包长
	TopicLen                     // QTA-013 主题长度
	TopicSlashes                 // QTA-014 主题斜杠数
	ClientIDLen                  // QTA-015 clientId 长度
	PublishRatePerConn           // QTA-016 每连接发布速率
	ThroughputPerConn            // QTA-017 每连接吞吐
	SubsPerConn                  // QTA-018 每连接订阅数
	SubsPerRequest               // QTA-019 单次 SUBSCRIBE 订阅数
	InboundInflight              // QTA-020 入站未确认 QoS 1
	OutboundInflight             // QTA-021 出站未确认 QoS 1
	QueuedPerSession             // QTA-111 每会话离线队列长度
	TopicAliasMax                // QTA-050 topic alias 上限
	UserPropsSize                // QTA-052 user properties 总大小
	CorrelationSize              // QTA-053 correlation data
	ContentTypeLen               // QTA-054 content type
	ShadowDocSize                // QTA-060 Shadow 文档大小
	ShadowDepth                  // QTA-061 Shadow JSON 深度
	ShadowRatePerThing           // QTA-063 每 thing Shadow 速率
	ShadowInflight               // QTA-064 每 thing 未确认

	// --- 全局：每账户 / 全集群 ---
	Connections          // QTA-026 并发连接数
	ConnectRate          // QTA-027 连接请求速率
	ConnectRatePerClient // QTA-028 每 clientId 连接速率
	InboundPublishRate   // QTA-029 入站发布速率
	OutboundPublishRate  // QTA-030 出站发布速率
	Subscriptions        // QTA-031 订阅总数
	SubscribeRate        // QTA-032 订阅请求速率
	RetainedCount        // QTA-040 retained 总数
	RetainedRate         // QTA-041 retained 发布速率
	RetainedRatePerTopic // QTA-042 每主题 retained 速率
	ShadowAPIRate        // QTA-065 Shadow 全局 API 速率
	CertRegisterRate     // QTA-080 证书注册速率
	HandshakeRate        // NET-042 TLS 握手速率

	// numQuotas 必须是最后一项，用作数组长度。
	numQuotas
)

// Def 是一项配额的定义。
type Def struct {
	ID    ID
	Name  string
	Scope Scope
	Kind  Kind
	// Limit 是配额值：Rate 为每秒数量，Count 为上限，Size 为字节或层级上限。
	// 0 表示不限制。
	Limit int
	// Burst 是令牌桶容量，仅对 Rate 有效。0 时取 Limit。
	Burst int
	// PRD 是对应的需求编号，便于从告警反查规格。
	PRD string
}

// defaults 是与 AWS IoT Core 对齐的默认配额。
//
// 默认值对齐是为了行为可预期，但全部可配——自建场景没有理由继承
// 云厂商的商业分层（PRD QTA-001）。放宽之处在 Note 中注明。
var defaults = [numQuotas]Def{
	PayloadSize:        {PayloadSize, "payload_size", ScopeLocal, Size, 128 * 1024, 0, "QTA-010"},
	PacketSize:         {PacketSize, "packet_size", ScopeLocal, Size, 146 * 1024, 0, "QTA-011"},
	TopicLen:           {TopicLen, "topic_len", ScopeLocal, Size, 256, 0, "QTA-013"},
	TopicSlashes:       {TopicSlashes, "topic_slashes", ScopeLocal, Size, 7, 0, "QTA-014"},
	ClientIDLen:        {ClientIDLen, "client_id_len", ScopeLocal, Size, 128, 0, "QTA-015"},
	PublishRatePerConn: {PublishRatePerConn, "publish_rate_per_conn", ScopeLocal, Rate, 100, 100, "QTA-016"},
	ThroughputPerConn:  {ThroughputPerConn, "throughput_per_conn", ScopeLocal, Rate, 512 * 1024, 512 * 1024, "QTA-017"},
	// 默认放宽于 AWS 的 50
	SubsPerConn: {SubsPerConn, "subs_per_conn", ScopeLocal, Count, 200, 0, "QTA-018"},
	// 默认放宽于 AWS 的 8
	SubsPerRequest:   {SubsPerRequest, "subs_per_request", ScopeLocal, Size, 64, 0, "QTA-019"},
	InboundInflight:  {InboundInflight, "inbound_inflight", ScopeLocal, Count, 100, 0, "QTA-020"},
	OutboundInflight: {OutboundInflight, "outbound_inflight", ScopeLocal, Count, 100, 0, "QTA-021"},
	// 与 AWS 的「每持久会话排队消息数」对齐。
	//
	// Kind 取 Size 而不是 Count：队列长度本来就在会话状态里，用 Within 做
	// 一次纯粹的上限判定即可。做成 Count 型就要 Acquire/Release 配对，而
	// 这种计数在本仓库已经漏过一次（订阅额度只在显式 UNSUBSCRIBE 时归还，
	// 于是"订阅完直接断开"每次永久吃掉额度）——没有状态就没有漏的可能。
	QueuedPerSession: {QueuedPerSession, "queued_messages_per_session", ScopeLocal, Size, 100, 0, "QTA-111"},
	// 默认放宽于 AWS 的 8
	TopicAliasMax:      {TopicAliasMax, "topic_alias_max", ScopeLocal, Size, 16, 0, "QTA-050"},
	UserPropsSize:      {UserPropsSize, "user_props_size", ScopeLocal, Size, 8 * 1024, 0, "QTA-052"},
	CorrelationSize:    {CorrelationSize, "correlation_size", ScopeLocal, Size, 8 * 1024, 0, "QTA-053"},
	ContentTypeLen:     {ContentTypeLen, "content_type_len", ScopeLocal, Size, 256, 0, "QTA-054"},
	ShadowDocSize:      {ShadowDocSize, "shadow_doc_size", ScopeLocal, Size, 8 * 1024, 0, "QTA-060"},
	ShadowDepth:        {ShadowDepth, "shadow_depth", ScopeLocal, Size, 8, 0, "QTA-061"},
	ShadowRatePerThing: {ShadowRatePerThing, "shadow_rate_per_thing", ScopeLocal, Rate, 20, 20, "QTA-063"},
	ShadowInflight:     {ShadowInflight, "shadow_inflight", ScopeLocal, Count, 10, 0, "QTA-064"},

	Connections: {Connections, "connections", ScopeGlobal, Count, 500_000, 0, "QTA-026"},
	ConnectRate: {ConnectRate, "connect_rate", ScopeGlobal, Rate, 3_000, 3_000, "QTA-027"},
	// 固定为 1：这是防重连风暴的关键，不建议放宽
	ConnectRatePerClient: {ConnectRatePerClient, "connect_rate_per_client", ScopeGlobal, Rate, 1, 1, "QTA-028"},
	InboundPublishRate:   {InboundPublishRate, "inbound_publish_rate", ScopeGlobal, Rate, 20_000, 20_000, "QTA-029"},
	OutboundPublishRate:  {OutboundPublishRate, "outbound_publish_rate", ScopeGlobal, Rate, 20_000, 20_000, "QTA-030"},
	Subscriptions:        {Subscriptions, "subscriptions", ScopeGlobal, Count, 500_000, 0, "QTA-031"},
	SubscribeRate:        {SubscribeRate, "subscribe_rate", ScopeGlobal, Rate, 3_000, 3_000, "QTA-032"},
	RetainedCount:        {RetainedCount, "retained_count", ScopeGlobal, Count, 500_000, 0, "QTA-040"},
	RetainedRate:         {RetainedRate, "retained_rate", ScopeGlobal, Rate, 500, 500, "QTA-041"},
	RetainedRatePerTopic: {RetainedRatePerTopic, "retained_rate_per_topic", ScopeGlobal, Rate, 1, 1, "QTA-042"},
	ShadowAPIRate:        {ShadowAPIRate, "shadow_api_rate", ScopeGlobal, Rate, 4_000, 4_000, "QTA-065"},
	CertRegisterRate:     {CertRegisterRate, "cert_register_rate", ScopeGlobal, Rate, 15, 15, "QTA-080"},
	HandshakeRate:        {HandshakeRate, "handshake_rate", ScopeGlobal, Rate, 3_000, 3_000, "NET-042"},
}

// Defaults 返回默认配额表的副本。
func Defaults() []Def {
	out := make([]Def, 0, numQuotas)
	for i := ID(0); i < numQuotas; i++ {
		out = append(out, defaults[i])
	}
	return out
}

// LookupName 按配额名取出编号。
//
// 存在的理由是配置覆盖（QTA-109）：配置文件里写的是名字（`connections`），
// 而判定器用的是编号。名字表的权威只能是本包的 defaults——若在 config 包
// 里另抄一份名字到编号的映射，加一项配额就要改两处，而漏改的表现是
// 「配置写了却没生效」，没有任何报错。
//
// 线性扫描足够：调用点只有启动时解析配置一处，配额总数是常量级。
func LookupName(name string) (ID, bool) {
	for i := ID(0); i < numQuotas; i++ {
		if defaults[i].Name == name {
			return i, true
		}
	}
	return 0, false
}

// Names 返回全部配额名，已按字典序排列。
//
// 供配置校验在报「未知配额名」时把可用名字列出来：NFR-060 要求配置错误
// 指出具体字段，而一个只说「未知」的错误会让人去翻 PRD 猜拼写。
func Names() []string {
	out := make([]string, 0, numQuotas)
	for i := ID(0); i < numQuotas; i++ {
		out = append(out, defaults[i].Name)
	}
	sort.Strings(out)
	return out
}

// Lookup 按编号取出配额定义。
func Lookup(id ID) (Def, bool) {
	if id >= numQuotas {
		return Def{}, false
	}
	return defaults[id], true
}

// Count 返回配额项总数。
func QuotaCount() int { return int(numQuotas) }

func (d Def) String() string {
	return fmt.Sprintf("%s(%s/%s, limit=%d, %s)", d.Name, d.Scope, d.kindName(), d.Limit, d.PRD)
}

func (d Def) kindName() string {
	switch d.Kind {
	case Rate:
		return "速率"
	case Count:
		return "计数"
	default:
		return "大小"
	}
}
