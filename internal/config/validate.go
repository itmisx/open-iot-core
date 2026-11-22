package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"time"
)

// FieldError 是指向具体配置字段的错误。
//
// NFR-060 要求配置错误在启动时拒绝并指出具体字段。一个只说
// "配置无效"的错误会让运维去逐行猜，而配置文件往往有上百行。
type FieldError struct {
	// Field 是 YAML 路径，如 listeners.mqtt_tls.addr。
	Field string
	// Problem 说明问题。
	Problem string
	// Hint 说明怎么改。NFR-065 要求关键故障给出可操作信息。
	Hint string
}

func (e *FieldError) Error() string {
	s := fmt.Sprintf("配置项 %s %s", e.Field, e.Problem)
	if e.Hint != "" {
		s += "；" + e.Hint
	}
	return s
}

func fieldErr(field, problem, hint string) *FieldError {
	return &FieldError{Field: field, Problem: problem, Hint: hint}
}

// Validate 校验配置。返回的错误指向第一个有问题的字段。
func (c *Config) Validate() error {
	for _, check := range []func() error{
		c.validateNode,
		c.validateCluster,
		c.validateMeta,
		c.validateListeners,
		c.validateBroker,
		c.validateQuota,
		c.validatePKI,
		c.validateIoT,
		c.validateAuth,
		c.validateLog,
		c.validateAWSAPI,
		c.validateOTel,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateAWSAPI() error {
	a := &c.AWSAPI
	if !a.Enabled {
		return nil
	}
	if a.Region == "" {
		return fieldErr("aws_api.region", "启用了兼容面但没有区域",
			"SigV4 凭证范围里的区域必须与这里一致")
	}
	// 密钥文件已不再是兼容面的 access key 来源（AUTH-086）。
	//
	// 配了也拒绝，而不是忽略：静默忽略的后果是节点照样起来，而那份
	// 文件里的钥匙一把都不生效——现象是全部 401，没有一个字指向
	// "这个配置项已经没用了"。
	if a.KeysFile != "" {
		return fieldErr("aws_api.keys_file", "兼容面的 access key 已改为存在复制的元数据里",
			"删掉这一行，改用管理面 POST /api/admin/v1/awskeys 发钥匙"+
				"（控制台的「访问控制」页也可以）。设备的 WebSocket 密钥文件"+
				"listeners.mqtt_wss.keys_file 不受影响")
	}
	if a.EndpointAddress == "" {
		return fieldErr("aws_api.endpoint_address", "启用了兼容面但没有端点地址",
			"DescribeEndpoint 返回这个地址，而不是 ATS 域名")
	}
	return nil
}

func (c *Config) validateOTel() error {
	o := &c.OTel
	if !o.Enabled {
		return nil
	}
	if o.Endpoint == "" {
		return fieldErr("otel.endpoint", "启用了追踪但没有导出地址",
			"填写收集器的基址，例如 http://127.0.0.1:4318")
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fieldErr("otel.endpoint", "不是 http 或 https 地址",
			"例如 http://127.0.0.1:4318")
	}
	if o.ServiceName == "" {
		o.ServiceName = "open-iot-core"
	}
	return nil
}

func (c *Config) validateNode() error {
	switch c.Node.Mode {
	case ModeStandalone, ModeBootstrap, ModeJoin:
	default:
		return fieldErr("node.mode", fmt.Sprintf("取值 %q 无效", c.Node.Mode),
			"可取 standalone、bootstrap、join")
	}
	if c.Node.DataDir == "" {
		return fieldErr("node.data_dir", "不得为空",
			"它存放 manifest、CA 与冷层数据；节点身份依赖它持久化")
	}
	if len(c.Node.Role) == 0 {
		return fieldErr("node.role", "不得为空", "至少须含 broker 或 signer")
	}
	for i, r := range c.Node.Role {
		switch r {
		case RoleBroker, RoleSigner:
		default:
			return fieldErr(fmt.Sprintf("node.role[%d]", i),
				fmt.Sprintf("取值 %q 无效", r), "可取 broker、signer")
		}
	}
	// PRD CLU-031：join 模式无 seed peer 须拒绝启动
	if c.Node.Mode == ModeJoin && len(c.Node.Seeds) == 0 {
		return fieldErr("node.seeds", "在 join 模式下不得为空",
			"须给出至少一个已在集群中的节点地址")
	}
	if c.Node.Mode == ModeStandalone && len(c.Node.Seeds) > 0 {
		return fieldErr("node.seeds", "在 standalone 模式下须为空",
			"配了 seeds 却以单机启动，通常是把 mode 写错了；要加入集群请设 mode: join")
	}
	for i, s := range c.Node.Seeds {
		if err := checkHostPort(s); err != nil {
			return fieldErr(fmt.Sprintf("node.seeds[%d]", i), err.Error(),
				"须为 host:port")
		}
	}
	for field, addr := range map[string]string{
		"node.advertise_addr": c.Node.AdvertiseAddr,
		"node.mesh_addr":      c.Node.MeshAddr,
	} {
		if addr == "" {
			continue
		}
		if err := checkHostPort(addr); err != nil {
			return fieldErr(field, err.Error(), "须为 host:port")
		}
	}
	return nil
}

func (c *Config) validateCluster() error {
	if c.Cluster.ShardCount <= 0 || c.Cluster.ShardCount > 65536 {
		return fieldErr("cluster.shard_count",
			fmt.Sprintf("取值 %d 超出范围", c.Cluster.ShardCount),
			"须在 1 到 65536 之间；集群创建后不可变，请一次定好")
	}
	if c.Cluster.Voters <= 0 {
		return fieldErr("cluster.voters",
			fmt.Sprintf("取值 %d 无效", c.Cluster.Voters), "须为正数")
	}
	if c.Cluster.Voters%2 == 0 {
		return fieldErr("cluster.voters",
			fmt.Sprintf("取值 %d 为偶数", c.Cluster.Voters),
			"须为奇数：偶数副本数不提高可用性，只增加写入延迟")
	}
	return c.validateReplication()
}

// validateReplication 校验复制组的参数。
//
// 单机写了复制配置要报错，不是忽略：那份配置看起来像是在跑集群，
// 而进程其实是单机——「不能把单机进程说成高可用」这条（RMAP-004）
// 只有在配置层面也拦住才守得住（CLU-070）。
func (c *Config) validateReplication() error {
	rep := &c.Cluster.Replication
	standalone := c.Node.Mode == ModeStandalone
	written := rep.Addr != "" || len(rep.Peers) > 0 ||
		rep.CertFile != "" || rep.KeyFile != "" || rep.CAFile != "" ||
		rep.SessionShards != 0 || rep.SessionReplicationFactor != 0 || rep.ShadowShards != 0 ||
		rep.ShadowReplicationFactor != 0 || rep.RTT != 0
	if standalone {
		if written {
			return fieldErr("cluster.replication",
				"node.mode 是 standalone，但写了复制组配置",
				"单机不组复制组。要跑集群请把 node.mode 改成 bootstrap 或 join")
		}
		return nil
	}

	if rep.Addr == "" {
		return fieldErr("cluster.replication.addr", "不得为空",
			"集群模式下它是本节点接受其他节点连接的地址")
	}
	switch {
	case len(rep.Peers) == 0:
		return fieldErr("cluster.replication.peers", "不得为空",
			"它是投票者名单，须含本节点自己")
	case c.Node.ID == "":
		return fieldErr("node.id", "集群模式下不得为空",
			"复制组按节点 ID 互认，且证书的 CommonName 必须等于它")
	}
	if _, ok := rep.Peers[c.Node.ID]; !ok {
		// 少了自己的那条，这个节点会向所有人要票却从不被计入多数派，
		// 而症状是「选不出 leader」，看不出是名单写漏了。
		return fieldErr("cluster.replication.peers",
			fmt.Sprintf("没有本节点 %q 的条目", c.Node.ID),
			"投票者名单须含本节点自己")
	}
	if len(rep.Peers) != c.Cluster.Voters {
		return fieldErr("cluster.replication.peers",
			fmt.Sprintf("有 %d 个节点，而 cluster.voters 是 %d", len(rep.Peers), c.Cluster.Voters),
			"两者须一致。它们是**初始**成员与初始投票者数——"+
				"成员变更之后以复制日志为准（CLU-090），不要照着配置去修")
	}
	if err := validateReplicas(rep); err != nil {
		return err
	}
	// 分片数的上限是防御性的：每个分片是一条独立的 raft 日志，各有心跳
	// 与 fsync。配到几百条时节点会忙于心跳而不是转发消息，而那不像是
	// 配错了的样子——所以在这里拦住，而不是让它慢慢显形。
	if rep.SessionShards > 64 {
		return fieldErr("cluster.replication.session_shards",
			fmt.Sprintf("%d 超出上限 64", rep.SessionShards),
			"每个分片是一条独立的 raft 日志，分片数要与节点的核数相称")
	}
	if rep.ShadowShards > 14 {
		return fieldErr("cluster.replication.shadow_shards",
			fmt.Sprintf("%d 超出上限 14", rep.ShadowShards),
			"影子分片使用 metadata 与 session 之间预留的 shard ID 区间")
	}
	if rf := rep.ShadowReplicationFactor; rf != 0 {
		if rep.ShadowShards == 0 {
			return fieldErr("cluster.replication.shadow_replication_factor",
				"shadow_shards 为 0", "先启用影子分片")
		}
		if rf < 3 || rf%2 == 0 || rf > uint64(len(rep.Replicas)) {
			return fieldErr("cluster.replication.shadow_replication_factor",
				fmt.Sprintf("%d 不是可用的奇数副本因子", rf),
				"设为 0 保持 RF=all，或设为不超过投票节点数的奇数（生产建议 3）")
		}
	}
	if rf := rep.SessionReplicationFactor; rf != 0 {
		if rf < 3 {
			return fieldErr("cluster.replication.session_replication_factor",
				fmt.Sprintf("%d 小于高可用下限 3", rf),
				"设为 0 保持 RF=all，或在至少 4 个投票节点时设为 3")
		}
		if rf%2 == 0 {
			return fieldErr("cluster.replication.session_replication_factor",
				fmt.Sprintf("%d 为偶数", rf),
				"Raft 副本因子须为奇数；偶数不增加可容忍故障数，只增加写放大")
		}
		if rf > uint64(len(rep.Replicas)) {
			return fieldErr("cluster.replication.session_replication_factor",
				fmt.Sprintf("%d 超出副本节点数 %d", rf, len(rep.Replicas)),
				"副本因子不得大于 cluster.replication.replicas 的节点数")
		}
	}
	// RTT 的两端都要拦。
	//
	// 下限 1 毫秒是库的要求（它的 tick 单位是毫秒，0 会被当成"没配"）；
	// 上限 1 秒是运维上的：心跳与选举按它的倍数算，10 秒的 RTT 意味着
	// 选举超时一百秒，一次 leader 故障期间所有设备都会重连完毕，而那
	// 不像是配错了的样子——所以在这里拦住。
	if d := time.Duration(rep.RTT); rep.RTT != 0 && (d < time.Millisecond || d > time.Second) {
		return fieldErr("cluster.replication.rtt",
			fmt.Sprintf("%s 超出 1ms 到 1s 的范围", d),
			"它是共识库的 tick 间隔，心跳与选举超时按它的倍数计")
	}
	for _, f := range []struct {
		name, val string
	}{
		{"cluster.replication.cert_file", rep.CertFile},
		{"cluster.replication.key_file", rep.KeyFile},
		{"cluster.replication.ca_file", rep.CAFile},
	} {
		if f.val == "" {
			return fieldErr(f.name, "不得为空",
				"集群内部互信是独立的信任链：拿设备证书就能冒充节点，"+
					"等于把元数据写入权发给了任何一台设备")
		}
	}
	return nil
}

func (c *Config) validateMeta() error {
	if c.Meta.Backend != "embedded" {
		return fieldErr("meta.backend",
			fmt.Sprintf("取值 %q 暂不支持", c.Meta.Backend),
			"当前只支持 embedded；外部 SQL 后端属 v0.4")
	}
	return nil
}

func (c *Config) validateListeners() error {
	ls := &c.Listeners
	if !ls.MQTTTLS.Enabled && !ls.MQTTALPN.Enabled && !ls.MQTTPlain.Enabled && !ls.MQTTWSS.Enabled {
		return fieldErr("listeners", "没有启用任何 MQTT 端点",
			"至少启用 mqtt_tls、mqtt_alpn、mqtt_plain 或 mqtt_wss 之一，否则设备无法接入")
	}
	if err := c.validateWSS(); err != nil {
		return err
	}
	for name, l := range map[string]*TLSListener{
		"mqtt_tls": &ls.MQTTTLS, "mqtt_alpn": &ls.MQTTALPN,
	} {
		if err := c.validateTLSListener("listeners."+name, l); err != nil {
			return err
		}
	}
	if ls.MQTTPlain.Enabled {
		if err := checkHostPort(ls.MQTTPlain.Addr); err != nil {
			return fieldErr("listeners.mqtt_plain.addr", err.Error(), "须为 host:port")
		}
		switch ls.MQTTPlain.ProxyProtocol {
		case "":
			if len(ls.MQTTPlain.TrustedProxies) != 0 {
				return fieldErr("listeners.mqtt_plain.trusted_proxies",
					"只在 proxy_protocol 为 v2 时生效",
					"删除 trusted_proxies，或显式设置 proxy_protocol: v2")
			}
		case "v2":
			if len(ls.MQTTPlain.TrustedProxies) == 0 {
				return fieldErr("listeners.mqtt_plain.trusted_proxies", "不得为空",
					"列出允许连接此端点的 TLS 终结器 CIDR，例如 127.0.0.1/32")
			}
			for i, raw := range ls.MQTTPlain.TrustedProxies {
				if _, err := netip.ParsePrefix(raw); err != nil {
					return fieldErr(fmt.Sprintf("listeners.mqtt_plain.trusted_proxies[%d]", i),
						fmt.Sprintf("取值 %q 不是合法 CIDR", raw),
						"须包含前缀长度，例如 10.0.0.0/24 或 ::1/128")
				}
			}
			if d := ls.MQTTPlain.ProxyHeaderTimeout.D(); d <= 0 {
				return fieldErr("listeners.mqtt_plain.proxy_header_timeout", "须为正数",
					"建议 3s；它限制慢代理头占用连接资源的时间")
			}
		default:
			return fieldErr("listeners.mqtt_plain.proxy_protocol",
				fmt.Sprintf("取值 %q 无效", ls.MQTTPlain.ProxyProtocol),
				"留空表示普通明文 MQTT，或取 v2 启用受信代理身份入口")
		}
	}
	return c.validateAdmin()
}

// validateWSS 校验 WebSocket 端点。未启用时不看其余字段。
func (c *Config) validateWSS() error {
	l := &c.Listeners.MQTTWSS
	if !l.Enabled {
		return nil
	}
	if err := checkHostPort(l.Addr); err != nil {
		return fieldErr("listeners.mqtt_wss.addr", err.Error(), "须为 host:port")
	}
	switch l.MinTLSVersion {
	case "1.2", "1.3":
	default:
		return fieldErr("listeners.mqtt_wss.min_tls_version",
			fmt.Sprintf("取值 %q 无效", l.MinTLSVersion),
			`可取 "1.2" 或 "1.3"；更低版本已不安全，不提供选项`)
	}
	if (l.CertFile == "") != (l.KeyFile == "") {
		return fieldErr("listeners.mqtt_wss.cert_file",
			"须与 key_file 同时给出或同时留空",
			"留空则使用 data dir 下由 `openiotd init` 生成的服务端证书")
	}
	if l.Region == "" {
		return fieldErr("listeners.mqtt_wss.region", "启用了 WebSocket 但没有区域",
			"SigV4 凭证范围里的区域必须与这里一致")
	}
	if l.KeysFile == "" {
		return fieldErr("listeners.mqtt_wss.keys_file", "启用了 WebSocket 但没有密钥文件",
			"access key 与密钥由本节点的密钥文件提供，不进入元数据日志")
	}
	if c.Listeners.MQTTALPN.Enabled && l.Addr == c.Listeners.MQTTALPN.Addr {
		return fieldErr("listeners.mqtt_wss.addr",
			"与 mqtt_alpn 监听同一地址",
			"443 上的 ALPN 客户端证书接入与 WebSocket 不能共用一个套接字，请把其中一个改到别的端口")
	}
	return nil
}

// validateAdmin 校验管理端点。
//
// 管理端点能吊销证书、踢连接、改写策略。它的暴露面须由两道独立的闸
// 把住：一道是鉴权令牌，一道是"绑到网络上"这个决定本身。
func (c *Config) validateAdmin() error {
	l := &c.Listeners.Admin
	if !l.Enabled {
		return nil
	}
	if err := checkHostPort(l.Addr); err != nil {
		return fieldErr("listeners.admin.addr", err.Error(), "须为 host:port")
	}
	if !isLoopbackAddr(l.Addr) && !l.AcknowledgePublicExposure {
		return fieldErr("listeners.admin.acknowledge_public_exposure",
			fmt.Sprintf("须显式设为 true 才能把管理端点绑到非回环地址 %s", l.Addr),
			"该端点能吊销证书、踢掉连接、改写策略；"+
				"把它暴露到网络上须是一个有意识的决定。"+
				"同时请确认已配置 auth_token_file 或 OPENIOTD_ADMIN_TOKEN")
	}
	return nil
}

func (c *Config) validateTLSListener(path string, l *TLSListener) error {
	if !l.Enabled {
		return nil
	}
	if err := checkHostPort(l.Addr); err != nil {
		return fieldErr(path+".addr", err.Error(), "须为 host:port")
	}
	switch l.MinTLSVersion {
	case "1.2", "1.3":
	default:
		return fieldErr(path+".min_tls_version",
			fmt.Sprintf("取值 %q 无效", l.MinTLSVersion),
			`可取 "1.2" 或 "1.3"；更低版本已不安全，不提供选项`)
	}
	// 证书与私钥必须同时给出或同时留空。只给一个是最常见的配置错误，
	// 而它的症状是启动时才发现找不到私钥。
	if (l.CertFile == "") != (l.KeyFile == "") {
		return fieldErr(path+".cert_file",
			"须与 key_file 同时给出或同时留空",
			"留空则使用 data dir 下由 `openiotd init` 生成的服务端证书")
	}
	// 要求客户端证书就必须有信任来源
	if l.RequireClientCert && l.ClientCAFile == "" && !c.PKI.Enabled {
		return fieldErr(path+".require_client_cert",
			"为真但没有客户端证书的信任来源",
			"请设置 client_ca_file，或启用 pki 以使用内嵌 CA 的信任池")
	}
	return nil
}

// validateBroker 校验接入层运行参数。
//
// 全部允许为零（取默认值），但不允许为负，也不允许小到会让节点自毁的值：
// 一个 1ms 的 write_timeout 会把每个客户端都判成慢客户端并断开，而现象
// 是「所有设备都连不上」，不指向这一行配置。
func (c *Config) validateBroker() error {
	b := &c.Broker
	for _, f := range []struct {
		name string
		v    int
	}{
		{"tls_handshake_concurrency", b.TLSHandshakeConcurrency},
		{"writer_workers", b.WriterWorkers},
		{"outbound_buffer", b.OutboundBuffer},
		{"read_buffer", b.ReadBuffer},
		{"max_write_batch", b.MaxWriteBatch},
	} {
		if f.v < 0 {
			return fieldErr("broker."+f.name, "不能为负", "留空或填 0 表示取默认值")
		}
	}
	if b.WriterWorkers > maxWriterWorkers {
		return fieldErr("broker.writer_workers",
			fmt.Sprintf("为 %d，超出上限 %d", b.WriterWorkers, maxWriterWorkers),
			"写出池的协程数远超 CPU 核数不会提高吞吐，只是多占栈内存；"+
				"留空按核数自适应")
	}
	if b.TLSHandshakeConcurrency > maxTLSHandshakeConcurrency {
		return fieldErr("broker.tls_handshake_concurrency",
			fmt.Sprintf("为 %d，超出上限 %d", b.TLSHandshakeConcurrency, maxTLSHandshakeConcurrency),
			"过多并发全握手会同时制造大量密码学对象和 GC 压力；留空按核数自适应")
	}
	if b.ReadBuffer > 0 && b.ReadBuffer < minReadBuffer {
		return fieldErr("broker.read_buffer",
			fmt.Sprintf("为 %d，小于下限 %d 字节", b.ReadBuffer, minReadBuffer),
			"读缓冲小于一个固定头加变长长度就无法解出任何报文")
	}
	if b.MaxWriteBatch > 0 && b.MaxWriteBatch < minWriteBatch {
		return fieldErr("broker.max_write_batch",
			fmt.Sprintf("为 %d，小于下限 %d 字节", b.MaxWriteBatch, minWriteBatch),
			"合并上限小于单个报文时每条消息都是一次写系统调用，批量写就失效了")
	}
	if d := b.WriteTimeout.D(); d != 0 && d < minWriteTimeout {
		return fieldErr("broker.write_timeout",
			fmt.Sprintf("为 %s，小于下限 %s", d, minWriteTimeout),
			"过短的刷写超时会把正常客户端判成慢客户端并断开；留空取默认值")
	}
	if d := b.TCPKeepAlive.D(); d != 0 && d < minTCPKeepAlive {
		return fieldErr("broker.tcp_keepalive",
			fmt.Sprintf("为 %s，小于容量安全下限 %s", d, minTCPKeepAlive),
			"过短的内核保活会按连接数放大探测包和定时器；留空取默认值")
	}
	return nil
}

// 接入层参数的边界。它们不是配额，只是「填了会让节点自毁」的下限与上限。
const (
	maxWriterWorkers           = 4096
	maxTLSHandshakeConcurrency = 4096
	minReadBuffer              = 64
	minWriteBatch              = 512
	minWriteTimeout            = 100 * time.Millisecond
	minTCPKeepAlive            = 30 * time.Second
)

// validateQuota 校验配额覆盖。
//
// 这里只判值，不判名字：名字的权威是 quota 包的配额表，而本包刻意不
// import 它（config 只依赖标准库与 YAML 库，ARCH-062）。未知名字由装配
// 期拒绝，错误形状与这里一致（见 server.quotaOverrides）。
//
// 键按字典序遍历而不是直接 range：map 的遍历顺序是随机的，同一份坏配置
// 两次启动报不同的字段，会让人以为改了一处又冒出一处。
func (c *Config) validateQuota() error {
	if len(c.Quota) == 0 {
		return nil
	}
	names := make([]string, 0, len(c.Quota))
	for name := range c.Quota {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if name == "" {
			return fieldErr("quota", "有一项的名字为空", "配额名见 docs/prd/17-quotas.md")
		}
		if c.Quota[name] < 0 {
			return fieldErr("quota."+name,
				fmt.Sprintf("为 %d，不能为负", c.Quota[name]),
				"0 表示取消该项限制，删掉这一行表示取默认值")
		}
	}
	return nil
}

func (c *Config) validatePKI() error {
	// 口令文件已不再使用（AUTH-092）。
	//
	// 配了也拒绝，而不是忽略：静默忽略之后，运维以为私钥是加密的，
	// 而磁盘上那份是明文——这个误解比报错严重得多。这一条在
	// PKI.Enabled 之前判，因为配了它的人更可能是从旧版本升上来的，
	// 而那种配置多半同时关掉了 pki。
	if c.PKI.PassphraseFile != "" {
		return fieldErr("pki.passphrase_file", "CA 私钥不再做静态加密",
			"删掉这一行，并一并去掉环境变量 OPENIOTD_CA_PASSPHRASE。"+
				"私钥以 PEM 落在数据目录里，靠 0600 的文件权限保护；"+
				"需要静态加密请在卷一层解决")
	}
	if !c.PKI.Enabled {
		return nil
	}
	if len(c.PKI.ServerHosts) == 0 {
		return fieldErr("pki.server_hosts", "不得为空",
			"须列出设备实际用于连接的主机名或 IP，否则证书名称校验会失败")
	}
	if c.PKI.DefaultValidity <= 0 {
		return fieldErr("pki.default_validity", "须为正时长", "默认 2160h（90 天）")
	}
	if c.PKI.MaxValidity <= 0 {
		return fieldErr("pki.max_validity", "须为正时长", "默认 9528h（397 天）")
	}
	if c.PKI.DefaultValidity > c.PKI.MaxValidity {
		return fieldErr("pki.default_validity",
			fmt.Sprintf("（%s）大于 max_validity（%s）",
				c.PKI.DefaultValidity, c.PKI.MaxValidity),
			"默认有效期不能超过上限，否则每次签发都会被静默截断")
	}
	// 文件路径须成组给出：三者缺一就无法构成可用的签发 CA
	set := 0
	for _, f := range []string{c.PKI.RootCertFile, c.PKI.IssuingCertFile, c.PKI.IssuingKeyFile} {
		if f != "" {
			set++
		}
	}
	if set != 0 && set != 3 {
		return fieldErr("pki.issuing_cert_file",
			"root_cert_file、issuing_cert_file、issuing_key_file 须同时给出或同时留空",
			"留空则使用 data dir 下由 `openiotd init` 生成的 CA")
	}
	return nil
}

func (c *Config) validateIoT() error {
	for field, v := range map[string]string{
		"iot.partition": c.IoT.Partition,
		"iot.region":    c.IoT.Region,
		"iot.account":   c.IoT.Account,
	} {
		if v == "" {
			return fieldErr(field, "不得为空",
				"它构成策略 Resource 中的 ARN；留空会让所有策略匹配不上")
		}
	}
	return nil
}

func (c *Config) validateAuth() error {
	switch c.Auth.Mode {
	case AuthCert:
		if !c.PKI.Enabled && c.Listeners.MQTTTLS.ClientCAFile == "" &&
			c.Listeners.MQTTALPN.ClientCAFile == "" &&
			c.Listeners.MQTTPlain.ProxyProtocol != "v2" {
			return fieldErr("auth.mode",
				"为 cert 但既未启用 pki 也未配置 client_ca_file",
				"证书认证需要一个信任来源")
		}
	case AuthInsecureNone:
		// 两道确认对应两种不同的风险，不合并：
		// 一是"根本不认证"，二是"不认证且对外暴露"。
		if !c.Auth.AcknowledgeInsecure {
			return fieldErr("auth.acknowledge_insecure",
				"须显式设为 true 才能使用 insecure-none",
				"该模式接受任何客户端，仅可用于本地开发；生产请用 mode: cert")
		}
		if addr := c.firstPublicMQTTAddr(); addr != "" {
			return fieldErr("auth.mode",
				fmt.Sprintf("为 insecure-none，但 MQTT 端点监听在非回环地址 %s", addr),
				"这会让任何能访问该地址的人接入并收发任意主题。"+
					"请改为 mode: cert，或把端点改绑回环地址")
		}
	default:
		return fieldErr("auth.mode", fmt.Sprintf("取值 %q 无效", c.Auth.Mode),
			"可取 cert、insecure-none")
	}
	return nil
}

// firstPublicMQTTAddr 返回第一个绑定在非回环地址的 MQTT 端点地址。
func (c *Config) firstPublicMQTTAddr() string {
	addrs := []struct {
		on   bool
		addr string
	}{
		{c.Listeners.MQTTTLS.Enabled, c.Listeners.MQTTTLS.Addr},
		{c.Listeners.MQTTALPN.Enabled, c.Listeners.MQTTALPN.Addr},
		{c.Listeners.MQTTPlain.Enabled, c.Listeners.MQTTPlain.Addr},
		{c.Listeners.MQTTWSS.Enabled, c.Listeners.MQTTWSS.Addr},
	}
	for _, a := range addrs {
		if !a.on {
			continue
		}
		if !isLoopbackAddr(a.addr) {
			return a.addr
		}
	}
	return ""
}

// isLoopbackAddr 判断监听地址是否只接受回环流量。
//
// 空主机与 0.0.0.0、:: 都表示监听全部网卡，均视为对外暴露。
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// 主机名无法在配置校验阶段解析，保守地视为对外暴露
		return false
	}
	return ip.IsLoopback()
}

func (c *Config) validateLog() error {
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fieldErr("log.level", fmt.Sprintf("取值 %q 无效", c.Log.Level),
			"可取 debug、info、warn、error")
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fieldErr("log.format", fmt.Sprintf("取值 %q 无效", c.Log.Format),
			"可取 json、text")
	}
	return nil
}

func checkHostPort(addr string) error {
	if addr == "" {
		return fmt.Errorf("不得为空")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("取值 %q 不是合法的 host:port", addr)
	}
	if port == "" {
		return fmt.Errorf("取值 %q 缺少端口", addr)
	}
	// 端口须为数字：服务名（如 "https"）在不同系统上解析结果不同，
	// 不接受它可以避免跨平台的行为差异。
	for _, r := range port {
		if r < '0' || r > '9' {
			return fmt.Errorf("端口 %q 须为数字", port)
		}
	}
	if host != "" && net.ParseIP(host) == nil && strings.ContainsAny(host, " \t") {
		return fmt.Errorf("主机名 %q 含空白字符", host)
	}
	return nil
}

// validateReplicas 校验节点 ID 到副本号的映射。
//
// 三条都在启动时拦，因为它们在运行时的症状都不指向这张表：
// 缺一个节点 → 那个节点起不来或被当成未知成员；号重复 → 两个节点抢
// 同一个共识身份，表现是日志复制错乱；号为 0 → 共识库把它当成"未指定"。
func validateReplicas(rep *ReplicationConfig) error {
	if len(rep.Replicas) == 0 {
		return fieldErr("cluster.replication.replicas", "不得为空",
			"共识库按 uint64 副本号认成员，而本项目的身份是字符串节点 ID，"+
				"两者之间要有一张显式映射（不从节点 ID 哈希推导：撞号之后"+
				"两个节点会抢同一个共识身份，而症状不指向这张表）")
	}
	if len(rep.Replicas) != len(rep.Peers) {
		return fieldErr("cluster.replication.replicas",
			fmt.Sprintf("有 %d 条，而 peers 有 %d 条", len(rep.Replicas), len(rep.Peers)),
			"两张表的键集合须完全相同：peers 给地址，replicas 给副本号，"+
				"少一条就是那个节点没有共识身份")
	}
	seen := make(map[uint64]string, len(rep.Replicas))
	for id := range rep.Peers {
		n, ok := rep.Replicas[id]
		if !ok {
			return fieldErr("cluster.replication.replicas",
				fmt.Sprintf("没有节点 %q 的条目", id),
				"键集合须与 peers 完全相同")
		}
		if n == 0 {
			return fieldErr("cluster.replication.replicas",
				fmt.Sprintf("节点 %q 的副本号是 0", id),
				"副本号须为正数：共识库把 0 当成未指定")
		}
		if other, dup := seen[n]; dup {
			return fieldErr("cluster.replication.replicas",
				fmt.Sprintf("节点 %q 与 %q 的副本号都是 %d", other, id, n),
				"副本号须互不相同：两个节点抢同一个共识身份时，"+
					"表现是日志复制错乱，而日志里看不出跟节点 ID 有关系")
		}
		seen[n] = id
	}
	return nil
}
