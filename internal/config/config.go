// Package config 承载 openiotd 的配置。
//
// 本包是整个项目中唯一允许 import 第三方 YAML 库的地方（PRD ARCH-062，
// 与 LANG-015 把共识库关在 MetaStore 实现体内是同一模式）。上层只看见
// 本包定义的类型，换库或迁到 yaml v4 只改本包。
package config

import "time"

// Mode 是节点启动模式（PRD CLU-030）。
type Mode string

const (
	// ModeStandalone 单机：初始化一副本 meta group 与固定 shard layout。
	ModeStandalone Mode = "standalone"
	// ModeBootstrap 创建新集群，仅可在空 data dir 执行。
	ModeBootstrap Mode = "bootstrap"
	// ModeJoin 加入已有集群，须提供 seeds。
	ModeJoin Mode = "join"
)

// Role 是节点角色。
type Role string

const (
	// RoleBroker 承载设备流量。
	RoleBroker Role = "broker"
	// RoleSigner 持有 CA 私钥并执行签发（PRD CLU-029）。
	//
	// 单独成一个角色是因为 CA 私钥永不进入复制状态：只有 signer 节点
	// 持有它，其余节点把 CSR 转发过来（PRD CLU-028）。
	RoleSigner Role = "signer"
)

// Config 是 openiotd 的完整配置。
//
// 字段顺序与 03-cluster.md 第 7 节的配置模型一致，便于对照。
type Config struct {
	Node      NodeConfig      `yaml:"node"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Meta      MetaConfig      `yaml:"meta"`
	View      ViewConfig      `yaml:"view"`
	Listeners ListenersConfig `yaml:"listeners"`
	// Broker 是接入层的运行参数。全部留零即取 broker 包的默认值。
	Broker BrokerConfig `yaml:"broker"`
	// Quota 按配额名覆盖默认配额（QTA-001、QTA-109）。
	//
	// 用 map 而不是逐项字段：配额表有三十余项且会随需求增加，逐项字段
	// 意味着每加一项配额都要改动本包，而漏改的表现是「配置写了却配不上」。
	// 名字的权威是 quota 包的配额表（见 quota.LookupName），未知名字在启动
	// 时被拒绝（QTA-110），不静默忽略。
	//
	// 值的语义与配额定义一致：速率为每秒数量，计数为上限，大小为字节或
	// 层级；0 表示取消该项限制。
	Quota   map[string]int `yaml:"quota"`
	PKI     PKIConfig      `yaml:"pki"`
	IoT     IoTConfig      `yaml:"iot"`
	Console ConsoleConfig  `yaml:"console"`
	Auth    AuthConfig     `yaml:"auth"`
	Log     LogConfig      `yaml:"log"`
	// AWSAPI 是 /api/aws 上的 AWS IoT 控制面。默认关闭。
	AWSAPI AWSAPIConfig `yaml:"aws_api"`
	// OTel 把控制面追踪导出到收集器。默认关闭。
	OTel OTelConfig `yaml:"otel"`
}

// OTelConfig 是追踪导出。
type OTelConfig struct {
	Enabled bool `yaml:"enabled"`
	// Endpoint 是收集器基址，不含 /v1/traces。
	Endpoint string `yaml:"endpoint"`
	// ServiceName 写入 service.name。空则用 open-iot-core。
	ServiceName string `yaml:"service_name"`
}

// NodeConfig 是节点身份与拓扑。
type NodeConfig struct {
	// ID 为空时首次启动生成并持久化到 manifest（PRD CLU-033）。
	ID string `yaml:"id"`
	// Mode 见 Mode 常量。
	Mode Mode `yaml:"mode"`
	// Role 可同时含 broker 与 signer。
	Role []Role `yaml:"role"`
	// DataDir 是持久化根目录。manifest、PKI、冷层都在其下。
	DataDir string `yaml:"data_dir"`
	// AdvertiseAddr 与 MeshAddr 供集群使用，单机留空。
	AdvertiseAddr string `yaml:"advertise_addr"`
	MeshAddr      string `yaml:"mesh_addr"`
	// Seeds 在 join 模式下必填。
	Seeds []string `yaml:"seeds"`
}

// ClusterConfig 是集群参数。
type ClusterConfig struct {
	// ShardCount 集群创建后不可变（PRD CLU-033）。
	ShardCount int `yaml:"shard_count"`
	// Voters 是**初始**投票者数，须与 replication.peers 的条数一致。
	//
	// 成员变更之后实际投票者数由日志决定，这个值不再跟着变——
	// 当前成员与多数派看 `GET /cluster/members`（OBS-147）。
	Voters int `yaml:"voters"`
	// MaxViewStaleness 超过此值时拒绝新连接但不断开已有连接
	// （PRD CLU-023 至 CLU-025）。0 表示不启用陈旧度判定。
	MaxViewStaleness Duration `yaml:"max_view_staleness"`
	// Replication 是元数据复制组的监听与互信材料，
	// 只在 bootstrap 与 join 模式下使用。
	Replication ReplicationConfig `yaml:"replication"`
}

// ReplicationConfig 是元数据复制组（契约 05 的日志）的传输参数。
//
// 它与设备监听器刻意分开：设备用的是业务 CA 签的证书，而集群内部互信
// 是另一条信任链——拿设备证书就能冒充节点，等于把元数据写入权发给了
// 任何一台设备（CLU-068）。
type ReplicationConfig struct {
	// Addr 是本节点的复制监听地址。
	Addr string `yaml:"addr"`
	// Peers 是**初始**成员：节点 ID 到复制地址的映射，须含本节点自己。
	//
	// 只在第一次启动（日志为空）时用。之后成员以复制日志为准
	// （CLU-090）——加减节点走 Admin API，不改这份配置；重启时落盘的
	// 成员列表会覆盖它。所以变更过成员之后，这张表与实际成员不一致
	// 是正常的，不要照着它去"修"。
	//
	// 新加入的节点这里只需要写自己：它的地址随成员变更复制给别人，
	// 别人的地址随日志复制给它。
	//
	// 不从 seeds 推导：seeds 是「去哪里打听集群」，而这张表是初始
	// 投票者名单，两者在成员变更期间不是一回事。
	Peers map[string]string `yaml:"peers"`
	// Replicas 是节点 ID 到副本号的映射，键集合须与 Peers 完全相同。
	//
	// 共识库按 uint64 的副本号认成员，而本项目的身份是字符串节点 ID
	// （并与证书 CommonName 绑死，CLU-069）。两者之间要有一张映射。
	//
	// **显式配置，不从节点 ID 哈希推导**：哈希撞号之后两个节点会抢同一个
	// 副本号，而那是共识层的身份——症状是日志复制错乱或选举异常，日志里
	// 看不出跟节点 ID 有关系。显式写下来则是启动时一次校验就能拦住。
	//
	// 与 Peers 同理，它也是**初始**映射：成员变更之后以复制日志为准。
	Replicas map[string]uint64 `yaml:"replicas"`
	// CertFile 与 KeyFile 是本节点的复制证书。
	//
	// 证书的 CommonName 必须等于节点 ID（CLU-060 在握手时校验它），
	// 且 SAN 须含 localhost——拨号侧以 localhost 做主机名校验，
	// 节点身份由 CommonName 单独认（CLU-069）。
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// CAFile 是签发各节点复制证书的 CA，用于双向校验。
	CAFile string `yaml:"ca_file"`
	// PumpInterval 是追平已提交日志的间隔。0 表示默认 200 毫秒。
	//
	// 跟随者全靠它：那边没有写入调用，视图却必须跟上 leader。
	PumpInterval Duration `yaml:"pump_interval"`
	// SessionShards 是会话分区组的 raft 日志条数。0 表示默认 8（CLU-111）。
	//
	// **它不是 cluster.shard_count。** 后者是路由用的虚拟分区数（默认
	// 64，影子的分区键按它取模），而这个是真实的 raft 日志条数——64 条
	// 日志意味着 64 套心跳与 fsync，那是把节点压死而不是提高吞吐。
	// 两个名字相近的数不是一回事，这一条值得写下来：合并它们的直觉很
	// 自然，而后果要到压测时才显形。
	//
	// 会话与离线队列不走元数据那条日志，而是按 clientId 哈希分到若干条
	// 独立日志上——元数据的写入是人工操作的频率，会话的写入跟着消息走，
	// 合用一条日志时后者会把前者挤死。
	//
	// **它一经初始化不得更改**：分片号参与键的归属判定（哪个分片拥有
	// 哪些会话），改了之后既有会话会被算到别的分片上，而那条日志里没有
	// 它们。症状是"集群里一部分设备的持久会话忽然不见了"。要改只能把
	// 三个节点的数据目录一起清掉重建。
	//
	// 调大的收益是写入吞吐线性增长，代价是心跳与 fsync 的放大也线性
	// 增长（dragonboat 按对端合批，但不是零成本）。
	SessionShards uint64 `yaml:"session_shards"`
}

// MetaConfig 是元数据后端。
type MetaConfig struct {
	// Backend 目前只支持 embedded。外部 SQL 后端属 v0.4。
	Backend string `yaml:"backend"`
}

// ViewConfig 是物化视图参数。
type ViewConfig struct {
	// ResyncInterval 是周期性全量校验的间隔（PRD CLU-017）。
	ResyncInterval Duration `yaml:"resync_interval"`
}

// ListenersConfig 是各监听端点。
type ListenersConfig struct {
	// MQTTTLS 是 8883 上的 mTLS 端点（PRD NET-001）。
	MQTTTLS TLSListener `yaml:"mqtt_tls"`
	// MQTTALPN 是 443 上以 ALPN x-amzn-mqtt-ca 协商的端点（PRD NET-002）。
	//
	// 单列一项而不是复用 MQTTTLS：受限网络里 443 常是唯一出口，
	// 两个端点的 ALPN 要求不同，合并会让配置含义变模糊。
	MQTTALPN TLSListener `yaml:"mqtt_alpn"`
	// MQTTPlain 仅用于前置代理已终结 TLS 的部署（PRD NET-017）。
	MQTTPlain PlainListener `yaml:"mqtt_plain"`
	// MQTTWSS 是 MQTT over WebSocket。
	//
	// 它和 mqtt_alpn 都常写在 443 上，但是两条监听器：后者要客户端证书
	// 和 ALPN，前者验查询串里的 SigV4、不要客户端证书。同一地址不能同时启用。
	MQTTWSS WSSListener `yaml:"mqtt_wss"`
	// Admin 是管理与健康检查端点。
	Admin AdminListener `yaml:"admin"`
}

// TLSListener 是一个 TLS 监听端点。
type TLSListener struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	// RequireClientCert 要求客户端出示并通过校验的证书。
	RequireClientCert bool `yaml:"require_client_cert"`
	// ALPN 为空表示不限制协议名。
	ALPN []string `yaml:"alpn"`
	// MinTLSVersion 取 "1.2" 或 "1.3"（PRD NET-010）。
	MinTLSVersion string `yaml:"min_tls_version"`
	// RequireSNI 在缺少 SNI 时拒绝连接（PRD NET-013）。
	RequireSNI       bool     `yaml:"require_sni"`
	HandshakeTimeout Duration `yaml:"handshake_timeout"`
	// CertFile 与 KeyFile 为空时使用 data dir 下由 init 生成的服务端证书。
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// ClientCAFile 为空时使用内嵌 CA 的信任池。
	ClientCAFile string `yaml:"client_ca_file"`
}

// WSSListener 是 MQTT over WebSocket（NET-003、NET-020 至 NET-023）。
type WSSListener struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	// Region 是 SigV4 凭证范围里的区域。启用时必填。
	Region string `yaml:"region"`
	// KeysFile 是 access key 文件。启用时必填。
	// 内容是 JSON 数组：[{"id","secret","thing"}]——设备钥匙，权限来自
	// 这台设备已附加的策略（AUTH-076），不写 role。密钥不进元数据日志。
	KeysFile string `yaml:"keys_file"`
	// MaxConnection 是单条连接的时长上限，零值表示 24 小时。
	MaxConnection Duration `yaml:"max_connection"`
	// RedirectTo 到期时发给 MQTT 5 客户端的 Server Reference。为空则只关连接。
	RedirectTo       string   `yaml:"redirect_to"`
	MinTLSVersion    string   `yaml:"min_tls_version"`
	RequireSNI       bool     `yaml:"require_sni"`
	HandshakeTimeout Duration `yaml:"handshake_timeout"`
	CertFile         string   `yaml:"cert_file"`
	KeyFile          string   `yaml:"key_file"`
}

// PlainListener 是一个明文监听端点。
type PlainListener struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

// AdminListener 是管理端点。
//
// 它与普通明文端点分开，因为它多一个鉴权维度：这个端点能吊销证书、
// 踢掉连接、改写策略，无鉴权暴露它等同于交出整个平台的控制权。
type AdminListener struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	// AuthTokenFile 指向鉴权令牌文件。
	//
	// 令牌也可经环境变量 OPENIOTD_ADMIN_TOKEN 提供。
	// 两者都没有时，`/api/admin/v1` 整体不提供服务——只保留
	// `/healthz` 与 `/readyz`，因为探针需要它们且它们不泄露任何东西。
	//
	// 不提供"无鉴权也能用"的选项：一个能吊销证书的接口，
	// 其默认状态必须是关闭而不是敞开。
	AuthTokenFile string `yaml:"auth_token_file"`
	// AcknowledgePublicExposure 是把管理端点绑到非回环地址的显式确认。
	//
	// 即便配了令牌，把管理面暴露到网络上仍是一个需要有意识做出的决定，
	// 因此单独一道闸——与 auth.acknowledge_insecure 同一模式。
	AcknowledgePublicExposure bool `yaml:"acknowledge_public_exposure"`
	// AcknowledgePlaintextKeyExport 允许经明文 HTTP 回传设备私钥。
	//
	// 管理监听器没有 TLS 选项。绑在回环地址时私钥不出本机，与 CLI
	// 把它写到本地文件没有区别；绑在非回环地址时，私钥会以明文穿过
	// 网络，而令牌保护的是访问权限，不是传输机密性。
	//
	// 与 AcknowledgePublicExposure 分成两道闸：前者确认"愿意把管理面
	// 暴露出去"，这里确认"该链路的传输机密性由外部保证"（典型是前置
	// 一个 TLS 终止的反向代理）。合并会让一个只想开管理面的人顺带
	// 获得明文导出私钥的能力。
	//
	// 不需要它的正确做法是 CSR 签发（PKI-011）：设备自持私钥，
	// 私钥根本不经过网络。
	AcknowledgePlaintextKeyExport bool `yaml:"acknowledge_plaintext_key_export"`
}

// BrokerConfig 是接入层的运行参数（PRD MQTT-119 至 MQTT-121）。
//
// 这几项此前是 broker 包里的常量，调用方从不设置，于是「每个部署都跑
// 同一组数字」。它们之中至少 WriterWorkers 与机器规格强相关：写出池的
// 每个工作协程要做 TLS 加密与一次写系统调用，固定 4 个与 CPU 核数无关。
//
// 零值一律表示「取 broker 包的默认值」，默认值本身不在这里重复一遍：
// 抄过来就是第二个真相，而两处数字不一致时没有任何测试会发现。
// 生效值由 `openiotd config print` 之外的指标暴露（写出池的 Pending 与
// 工作协程数进 /metrics）。
type BrokerConfig struct {
	// WriterWorkers 是共享写出池的工作协程数。
	//
	// 0 表示按 GOMAXPROCS 自适应。调高它的理由是慢客户端：一次刷写最长
	// 占住一个工作协程 WriteTimeout 那么久，池子越小，少数几个停止读取的
	// 客户端就能让全部投递停摆（LANG-005 不允许每连接一个写协程，所以
	// 这个权衡无法回避，只能把池子开大）。
	WriterWorkers int `yaml:"writer_workers"`
	// OutboundBuffer 是每连接出站队列的深度，必须有界。
	//
	// 队列满等于判定这个客户端跟不上，连接会被断开（MQTT-035）。调大它
	// 换来对短时突发的容忍，代价是每连接内存（NFR-023 的 40 KB 预算里
	// 已经含了默认深度那一份）。
	OutboundBuffer int `yaml:"outbound_buffer"`
	// ReadBuffer 是每连接读缓冲的初始大小（字节）。
	//
	// 它会按需增长到最大包长为止，所以这个数只影响「小报文的连接要不要
	// 为大缓冲付内存」。百万连接下它乘以连接数就是百 MB 量级。
	ReadBuffer int `yaml:"read_buffer"`
	// MaxWriteBatch 是单次写出合并的字节上限。
	MaxWriteBatch int `yaml:"max_write_batch"`
	// WriteTimeout 是单次刷写的上限。
	//
	// 停止读取的客户端不能永久占住工作协程，超时即断开它——这是对慢
	// 客户端唯一正确的处置，服务端不能为它无界等待。
	WriteTimeout Duration `yaml:"write_timeout"`
}

// PKIConfig 是内嵌 CA 的配置。
type PKIConfig struct {
	Enabled bool `yaml:"enabled"`
	// RootCertFile 等为空时取 data dir 下的默认路径。
	RootCertFile    string `yaml:"root_cert_file"`
	IssuingCertFile string `yaml:"issuing_cert_file"`
	IssuingKeyFile  string `yaml:"issuing_key_file"`
	// PassphraseFile 已废弃（AUTH-092）。
	//
	// CA 私钥的静态加密整条去掉了：私钥以 PEM 落盘，保护只有文件权限。
	// 字段留着只为在配了的时候报出来——删掉它的话，旧配置里的这一行
	// 会被 YAML 的严格模式报成"未知字段"，那句话指向拼写错误，而真实
	// 成因是这个选项不存在了。
	PassphraseFile string `yaml:"passphrase_file"`
	// DefaultValidity 与 MaxValidity 见 PRD PKI-013。
	DefaultValidity Duration `yaml:"default_validity"`
	MaxValidity     Duration `yaml:"max_validity"`
	// ServerHosts 是服务端证书的主机名与 IP。
	//
	// 设备校验服务端证书时按主机名匹配 SAN，这里漏掉实际连接用的名字，
	// 症状是设备报"证书名称不匹配"而服务端日志里什么都看不到。
	ServerHosts []string `yaml:"server_hosts"`
}

// IoTConfig 是 ARN 的命名空间。
//
// IoT Policy 的 Resource 是完整 ARN，其分区、区域、账号必须与本平台
// 自报的一致，否则所有策略都匹配不上。单机默认用占位值，
// 但必须可配——迁移自 AWS 的用户会直接搬运既有策略文档。
type IoTConfig struct {
	Partition string `yaml:"partition"`
	Region    string `yaml:"region"`
	Account   string `yaml:"account"`
}

// AWSAPIConfig 是兼容面（PRD API-001、API-010）。
//
// 它挂在管理端口的 /api/aws，用 SigV4（服务名 iot）而不是管理令牌。
// Region 是签名里的区域，可以和 iot.region 不同：后者进 ARN，前者核对凭证。
type AWSAPIConfig struct {
	Enabled bool `yaml:"enabled"`
	// Region 是 SigV4 凭证范围里的区域。启用时必填。
	Region string `yaml:"region"`
	// KeysFile 已废弃（AUTH-086）。
	//
	// 兼容面的 access key 现在存在复制的元数据里、secret 加密落盘，
	// 经管理面发放与吊销。字段留着只为在配了的时候报出来——删掉它
	// 的话，旧配置会因为"未知字段"报一个指不出改法的错。
	KeysFile string `yaml:"keys_file"`
	// EndpointAddress 是 DescribeEndpoint 返回的地址。启用时必填。
	EndpointAddress string `yaml:"endpoint_address"`
}

// ConsoleConfig 是控制台配置（PRD UI-001）。
type ConsoleConfig struct {
	// Enabled 为假时完全不挂载控制台。
	//
	// 边缘部署不需要界面，而少挂一个 HTTP 处理器就少一个暴露面。
	Enabled bool `yaml:"enabled"`
	// BasePath 是反向代理下的子路径，如 /openiotd。
	//
	// 前端产物用相对路径引用资源，因此只需服务端去掉前缀即可。
	BasePath string `yaml:"base_path"`
}

// AuthMode 是设备认证方式。
type AuthMode string

const (
	// AuthCert 以 X.509 客户端证书认证，是唯一的生产方式。
	AuthCert AuthMode = "cert"
	// AuthInsecureNone 不认证，仅用于本地开发。
	//
	// 必须同时把 AcknowledgeInsecure 置为真才生效：
	// 一个能被单个配置项意外打开的"不认证"模式，迟早会被打开。
	AuthInsecureNone AuthMode = "insecure-none"
)

// AuthConfig 是认证配置。
type AuthConfig struct {
	Mode AuthMode `yaml:"mode"`
	// AcknowledgeInsecure 是 insecure-none 模式的显式确认。
	AcknowledgeInsecure bool `yaml:"acknowledge_insecure"`
}

// LogConfig 是日志配置。
type LogConfig struct {
	// Level 取 debug | info | warn | error。
	Level string `yaml:"level"`
	// Format 取 json | text。
	Format string `yaml:"format"`
}

// Default 返回默认配置。
//
// 默认值的取法：安全相关的默认取严（要求客户端证书、认证方式为 cert、
// 明文端点默认关闭、Admin 只听回环），容量相关的默认取保守。
// 一个"开箱即不安全"的默认配置，比没有默认配置更糟。
func Default() Config {
	return Config{
		Node: NodeConfig{
			Mode:    ModeStandalone,
			Role:    []Role{RoleBroker, RoleSigner},
			DataDir: "./data",
		},
		Cluster: ClusterConfig{
			ShardCount: 64,
			Voters:     3,
			// 单机默认不启用陈旧度判定：只有一个副本，视图永不落后。
			MaxViewStaleness: 0,
		},
		Meta: MetaConfig{Backend: "embedded"},
		View: ViewConfig{ResyncInterval: Duration(30 * time.Second)},
		Listeners: ListenersConfig{
			MQTTTLS: TLSListener{
				Enabled:           true,
				Addr:              "0.0.0.0:8883",
				RequireClientCert: true,
				MinTLSVersion:     "1.2",
				HandshakeTimeout:  Duration(10 * time.Second),
			},
			MQTTALPN: TLSListener{
				Enabled:           false,
				Addr:              "0.0.0.0:443",
				RequireClientCert: true,
				ALPN:              []string{"x-amzn-mqtt-ca"},
				MinTLSVersion:     "1.2",
				HandshakeTimeout:  Duration(10 * time.Second),
			},
			MQTTPlain: PlainListener{Enabled: false, Addr: "127.0.0.1:1883"},
			MQTTWSS: WSSListener{
				Enabled:          false,
				Addr:             "0.0.0.0:443",
				MaxConnection:    Duration(24 * time.Hour),
				MinTLSVersion:    "1.2",
				HandshakeTimeout: Duration(10 * time.Second),
			},
			Admin: AdminListener{Enabled: true, Addr: "127.0.0.1:9090"},
		},
		PKI: PKIConfig{
			Enabled:         true,
			DefaultValidity: Duration(90 * 24 * time.Hour),
			MaxValidity:     Duration(397 * 24 * time.Hour),
			ServerHosts:     []string{"localhost", "127.0.0.1"},
		},
		IoT:     IoTConfig{Partition: "aws", Region: "local", Account: "000000000000"},
		Console: ConsoleConfig{Enabled: true, BasePath: "/"},
		Auth:    AuthConfig{Mode: AuthCert},
		Log:     LogConfig{Level: "info", Format: "json"},
		OTel:    OTelConfig{ServiceName: "open-iot-core"},
	}
}

// HasRole 判断节点是否具备某角色。
func (c *Config) HasRole(r Role) bool {
	for _, got := range c.Node.Role {
		if got == r {
			return true
		}
	}
	return false
}
