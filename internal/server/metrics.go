package server

import (
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/metrics"
	"github.com/itmisx/open-iot-core/internal/registry"
)

// Prometheus 指标端点（PRD OBS-020 至 OBS-029）。
//
// 本文件是唯一的采集点，不是第二套埋点：计数器由事件总线在 Emit 里
// 维护（见 events/metrics.go），其余是各包已有 Stats() 的读取。
// OBS-001「单一事件源，禁止为各消费者分别埋点」因此成立。
//
// 鉴权：须带管理面令牌。PRD API-082 把 /healthz 与 /readyz 豁免的
// 理由是"它们不泄露任何信息"，而指标泄露设备数、证书数、拒绝原因
// 分布与运行时细节，不满足那个前提。Prometheus 原生支持
// bearer_token_file，所以要求令牌不影响可用性。
//
// 未配置令牌时 /metrics 与 /api/admin/v1 一样不挂载（API-081 同理）。

// metricPrefix 是全部指标名的前缀。
//
// 固定前缀使 Prometheus 侧可以用 openiot_* 一次性选中本项目的全部
// 指标，也避免与其他 exporter 的名字相撞。
const metricPrefix = "openiot_"

// handleMetrics 以 Prometheus 文本格式暴露指标。
func (a *adminServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	// version=0.0.4 是文本暴露格式的版本号，不是本项目的版本。
	// 不带它的话部分采集端会按 OpenMetrics 解析，而二者对 _total
	// 后缀与 EOF 标记的要求不同。
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	e := metrics.NewEncoder(w)

	node := metrics.Label{Name: "node", Value: string(a.s.mf.NodeID)}
	a.collectBuild(e, node)
	a.collectConn(e, node)
	a.collectMessages(e, node)
	a.collectRouting(e, node)
	a.collectAuthz(e, node)
	a.collectMeta(e, node)
	a.collectCluster(e, node)
	a.collectPKI(e, node)
	a.collectQuota(e, node)
	a.collectEvents(e, node)
	collectRuntime(e, node)

	_ = e.Flush()
}

// collectBuild 暴露一条恒为 1 的构建信息指标。
//
// 这是 Prometheus 的惯例做法：版本这类字符串不能做指标值，只能做
// 标签，于是用一个值恒为 1 的 gauge 承载它。
func (a *adminServer) collectBuild(e *metrics.Encoder, node metrics.Label) {
	e.Help(metricPrefix+"build_info", "构建与节点信息，值恒为 1", metrics.TypeGauge)
	e.Gauge(metricPrefix+"build_info", 1, node,
		metrics.Label{Name: "cluster", Value: string(a.s.mf.ClusterID)},
		metrics.Label{Name: "go_version", Value: runtime.Version()})
}

// collectConn 暴露连接指标（OBS-021）。
func (a *adminServer) collectConn(e *metrics.Encoder, node metrics.Label) {
	st := a.s.brk.Stats()

	e.Help(metricPrefix+"connections", "当前连接数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"connections", st.Conns, node)

	// 按 结果 × 认证方式 × 协议版本 的累计数。
	//
	// 只导出非零组合：全维度展开是 7×5×6=210 行，而一个真实部署里
	// 非零的通常不到十个。恒为零的样本会把响应体撑大二十倍，而
	// Prometheus 对"指标消失"与"指标为零"的处理是一样的。
	e.Help(metricPrefix+"connections_total", "累计连接尝试数，按结果与认证方式",
		metrics.TypeCounter)
	for r := events.ConnResult(0); r < events.NumConnResults; r++ {
		for m := events.AuthMethod(0); m < events.NumAuthMethods; m++ {
			for v := uint8(0); v <= events.MaxProtoVer; v++ {
				n := a.s.bus.ConnCount(r, m, v)
				if n == 0 {
					continue
				}
				e.CounterUint(metricPrefix+"connections_total", n, node,
					metrics.Label{Name: "result", Value: r.String()},
					metrics.Label{Name: "auth_method", Value: m.String()},
					metrics.Label{Name: "proto_ver", Value: protoVerLabel(v)})
			}
		}
	}

	rejects, disconnects := a.s.bus.ReasonCodesSeen()

	e.Help(metricPrefix+"connection_rejections_total", "累计被拒连接数，按原因码",
		metrics.TypeCounter)
	for _, c := range rejects {
		e.CounterUint(metricPrefix+"connection_rejections_total",
			a.s.bus.ConnRejectCount(c), node,
			metrics.Label{Name: "reason", Value: c.String()},
			metrics.Label{Name: "code", Value: strconv.Itoa(int(c))})
	}

	e.Help(metricPrefix+"disconnections_total", "累计断开数，按原因码", metrics.TypeCounter)
	for _, c := range disconnects {
		e.CounterUint(metricPrefix+"disconnections_total",
			a.s.bus.DisconnectCount(c), node,
			metrics.Label{Name: "reason", Value: c.String()},
			metrics.Label{Name: "code", Value: strconv.Itoa(int(c))})
	}

	e.Help(metricPrefix+"handshake_duration_ms", "TLS 握手耗时，仅成功的连接",
		metrics.TypeHistogram)
	e.Histogram(metricPrefix+"handshake_duration_ms", a.s.bus.Handshake(), node)
	var tlsOK, tlsResumed, tlsFailed, tlsThrottled uint64
	var tlsInFlight, tlsPeak, tlsCapacity int64
	for _, l := range a.s.listeners {
		st := l.TLSHandshakeStats()
		tlsOK += st.Succeeded
		tlsResumed += st.Resumed
		tlsFailed += st.Failed
		tlsThrottled += st.Throttled
		tlsInFlight += st.InFlight
		tlsPeak += st.Peak
		tlsCapacity += int64(st.Capacity)
	}
	e.Help(metricPrefix+"tls_handshakes_total",
		"TLS 握手数，不等 MQTT CONNECT；按全握手、恢复和失败分类", metrics.TypeCounter)
	for _, m := range []struct {
		result string
		v      uint64
	}{
		{"full", tlsOK - tlsResumed}, {"resumed", tlsResumed},
		{"failed", tlsFailed}, {"throttled", tlsThrottled},
	} {
		e.CounterUint(metricPrefix+"tls_handshakes_total", m.v, node,
			metrics.Label{Name: "result", Value: m.result})
	}
	for _, m := range []struct {
		name, help string
		v          int64
	}{
		{"tls_handshakes_in_flight", "当前正在执行的 TLS 握手数", tlsInFlight},
		{"tls_handshake_peak", "各 TLS listener 历史在途峰值之和", tlsPeak},
		{"tls_handshake_capacity", "全部 TLS listener 的握手并发硬上限之和", tlsCapacity},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, m.v, node)
	}

	// CONNECT 分段耗时（OBS-148）。
	//
	// 读法：auth 是本地热视图；RF=3 下 session_load 可由 Claim 带回，
	// owner_claim 是可合批的 leader 内存 RPC，session_write 才是一轮共识。
	// 建连慢时先看这四段谁占住了
	// 端到端时间——此前只有一个端到端数字，于是 CLU-126 那 25 至 48 倍
	// 无从归因。
	e.Help(metricPrefix+"connect_stage_duration_ms",
		"CONNECT 各段耗时，按阶段；仅走完全部四段的连接", metrics.TypeHistogram)
	for st := events.ConnStage(0); st < events.NumConnStages; st++ {
		e.Histogram(metricPrefix+"connect_stage_duration_ms", a.s.bus.ConnStage(st), node,
			metrics.Label{Name: "stage", Value: st.String()})
	}

	// 一次建连发出几次逻辑会话写：本计数器除以
	// connections_total{result="accepted"}。
	//
	// 不做成直方图：取值只有 0、1、2，而这个商是 CLU-126 第一条假设的
	// 直接判据——非持久会话应当是 0，不是 0 就说明 persistSession 那层
	// 判定有漏网的路径，而那条路径在功能上完全看不出来。
	//
	// 只统计 CONNECT 路径，不含断开时的那次落盘：分母是被接受的连接数，
	// 混进断开侧的写入就不再是"一次建连写几次"了。物理
	// Raft 提案数看 session_proposals_total；CLU-134 之后多个逻辑
	// 写可以合成一条日志，两个计数器不应再相等。
	e.Help(metricPrefix+"session_writes_total",
		"CONNECT 路径上的累计逻辑会话写入次数；多次可合成一条 Raft 提案",
		metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_writes_total", a.s.bus.SessionWrites(), node)

	// epoch 撞号的次数（CLU-131）。
	//
	// 非零意味着两个节点真的为同一个 clientId 发出了相同的 epoch，而
	// 不是理论上会。它是这条路径唯一的外部信号：撞号本身既不报错也不
	// 丢数据（平手判据保住了还连着的那一份），但它说明所有权还在用
	// 每节点各自递增的序号，该排 CLU-009 了。
	e.Help(metricPrefix+"session_epoch_ties_total",
		"epoch 相等但归属节点不同的会话写入次数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_epoch_ties_total",
		a.s.sessionLocal.EpochTies(), node)

	// 被围栏判为陈旧的会话写入（CLU-131）。
	//
	// 集群下非零是正常的，而且正是要暴露的那件事：epoch 由各节点的本地
	// 所有权表发放，设备换节点重连时新节点的序号可能比旧节点小，于是一次
	// 正常的重连被判成陈旧。**单节点下它应当恒为 0。**
	e.Help(metricPrefix+"session_write_stale_total",
		"被 epoch 围栏判为陈旧的会话写入次数；集群下非零属正常，单节点下不应出现",
		metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_write_stale_total",
		a.s.bus.SessionWriteStale(), node)

	e.Help(metricPrefix+"session_write_failures_total",
		"会话存储写入失败数，仅已接受的连接；被拒的见 connections_total{result=\"StoreFailed\"}",
		metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_write_failures_total",
		a.s.bus.SessionWriteFailures(), node)
}

// collectMessages 暴露消息指标（OBS-022）。
func (a *adminServer) collectMessages(e *metrics.Encoder, node metrics.Label) {
	st := a.s.brk.Stats()

	e.Help(metricPrefix+"writer_flushed_total", "累计写出批次数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"writer_flushed_total", st.Writer.Flushed, node)

	e.Help(metricPrefix+"writer_errors_total", "累计写失败数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"writer_errors_total", st.Writer.WriteErrs, node)

	e.Help(metricPrefix+"writer_timeouts_total", "累计写超时数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"writer_timeouts_total", st.Writer.Timeouts, node)

	e.Help(metricPrefix+"writer_pending", "当前待写出的连接数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"writer_pending", int64(st.Writer.Pending), node)

	for _, m := range []struct {
		name, help string
		v          uint64
	}{
		{"reserved_responses_total", "平台保留主题累计响应数", st.ReservedResponses},
		{"reserved_response_no_subscriber_total", "平台响应没有匹配本地订阅的次数", st.ReservedNoSubscriber},
		{"reserved_service_over_1s_total", "平台保留主题服务处理超过1秒的次数", st.ReservedServiceOver1s},
		{"outbound_inflight_queued_total", "出站QoS消息因在途窗口满而进入会话队列的次数", st.OutboundInflightQueued},
		{"outbound_inflight_failed_total", "出站QoS消息在途登记失败而未发送的次数", st.OutboundInflightFailed},
		{"outbound_enqueue_failed_total", "出站QoS消息编码后进入连接队列失败的次数", st.OutboundEnqueueFailed},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeCounter)
		e.CounterUint(metricPrefix+m.name, m.v, node)
	}
	e.Help(metricPrefix+"reserved_service_max_ms", "进程启动以来平台保留主题服务最大处理耗时", metrics.TypeGauge)
	e.Gauge(metricPrefix+"reserved_service_max_ms", float64(st.ReservedServiceMaxNs)/float64(time.Millisecond), node)

	e.Help(metricPrefix+"subscriptions_total", "累计订阅与退订数", metrics.TypeCounter)
	for _, op := range []struct {
		unsub bool
		name  string
	}{{false, "subscribe"}, {true, "unsubscribe"}} {
		for _, ok := range []bool{true, false} {
			n := a.s.bus.SubCount(op.unsub, ok)
			if n == 0 {
				continue
			}
			e.CounterUint(metricPrefix+"subscriptions_total", n, node,
				metrics.Label{Name: "op", Value: op.name},
				metrics.Label{Name: "result", Value: grantedLabel(ok)})
		}
	}
}

// collectRouting 暴露路由指标（OBS-023）。
func (a *adminServer) collectRouting(e *metrics.Encoder, node metrics.Label) {
	st := a.s.rt.Stats()

	for _, m := range []struct {
		name, help string
		v          int
	}{
		{"router_subscriptions", "当前订阅总数", st.Subscriptions},
		{"router_trie_nodes", "订阅树节点数", st.TrieNodes},
		{"router_shared_groups", "共享订阅组数", st.SharedGroups},
		{"router_retained", "保留消息条数（不含墓碑）", st.Retained},
		{"router_interest_nodes", "兴趣表节点数", st.InterestNodes},
		{"router_clients", "有订阅的客户端数", st.Clients},
		// 丢弃数非零意味着部分节点可能缺某几条保留消息，直到下一次 dump
		// 或下一次发布（MQTT-125、MQTT-126）。它是这条路径唯一的外部信号，
		// 所以必须暴露出来——下一阶段的 digest 对账才会让它自愈。
		{"retained_broadcast_dropped", "保留消息广播丢弃数", int(a.s.retainedDrops.Load())},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, int64(m.v), node)
	}

	e.Help(metricPrefix+"retained_persist_failures_total",
		"保留消息落盘失败数。非零意味着重启后会丢保留消息", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"retained_persist_failures_total",
		a.s.rt.Retained().PersistFailures(), node)
}

// collectAuthz 暴露授权指标（OBS-024）。
func (a *adminServer) collectAuthz(e *metrics.Encoder, node metrics.Label) {
	e.Help(metricPrefix+"authz_decisions_total", "累计授权判定数，按动作与判定",
		metrics.TypeCounter)
	for act := events.Action(0); act < events.NumActions; act++ {
		for d := events.Decision(0); d < events.NumDecisions; d++ {
			n := a.s.bus.AuthzCount(act, d)
			if n == 0 {
				continue
			}
			// 动作用策略文档的拼写（iot:Subscribe），与日志一致：
			// 运维要能拿同一个串去 grep 策略文件（OBS-105）。
			e.CounterUint(metricPrefix+"authz_decisions_total", n, node,
				metrics.Label{Name: "action", Value: "iot:" + act.String()},
				metrics.Label{Name: "decision", Value: d.String()})
		}
	}
}

// collectMeta 暴露元数据视图指标（OBS-025）。
func (a *adminServer) collectMeta(e *metrics.Encoder, node metrics.Label) {
	v := a.s.store.View()
	if v == nil {
		return
	}
	e.Help(metricPrefix+"meta_revision", "元数据视图 revision", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"meta_revision", int64(v.Revision()), node)

	e.Help(metricPrefix+"meta_last_confirmed_timestamp_ms",
		"最后一次确认追平的时刻", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"meta_last_confirmed_timestamp_ms",
		v.LastConfirmedUnixMs(), node)

	// 陈旧视图对新连接失败闭合，但不断开已有连接（CLU-023 至 025）。
	// 它是告警的直接依据，所以要有一个布尔 gauge，而不是只能从
	// 时间戳去推。
	e.Help(metricPrefix+"meta_view_stale",
		"视图是否陈旧。1 表示新连接会被拒，已有连接不受影响", metrics.TypeGauge)
	stale := 0.0
	if v.Stale(a.s.now(), a.s.cfg.Cluster.MaxViewStaleness.D().Milliseconds()) {
		stale = 1
	}
	e.Gauge(metricPrefix+"meta_view_stale", stale, node)

	ms := v.Stats()
	for _, m := range []struct {
		name, help string
		v          int
	}{
		{"meta_certs", "视图中的证书数", ms.Certs},
		{"meta_things", "视图中的设备数", ms.Things},
		{"meta_policy_sets", "视图中的策略集数", ms.PolicySets},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, int64(m.v), node)
	}
}

// collectCluster 暴露集群指标（OBS-026）。
//
// 单机下仍然暴露，且值是真实的：本节点即 leader，term 为 0，
// 成员数为 1。不暴露会让"从单机扩到集群"变成一次仪表盘重做；
// 暴露成假值则更糟。epoch 拒绝数是真实计数，单机下也会发生
// （同一 clientId 的会话接管）。
func (a *adminServer) collectCluster(e *metrics.Encoder, node metrics.Label) {
	e.Help(metricPrefix+"cluster_members", "集群成员数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"cluster_members", 1, node)

	e.Help(metricPrefix+"cluster_is_leader", "本节点是否为 leader，值恒为 1 于单机",
		metrics.TypeGauge)
	e.Gauge(metricPrefix+"cluster_is_leader", 1, node)

	e.Help(metricPrefix+"cluster_shards", "分片数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"cluster_shards", int64(a.s.cfg.Cluster.ShardCount), node)

	a.collectSessionShards(e, node)
	a.collectShadowShards(e, node)
}

func (a *adminServer) collectShadowShards(e *metrics.Encoder, node metrics.Label) {
	g := a.s.shadowShards.Load()
	if g == nil {
		return
	}
	st := g.Stats()
	e.Help(metricPrefix+"shadow_shard_count", "影子分片数（真实 raft 日志条数）",
		metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"shadow_shard_count", int64(g.ShardCount()), node)
	e.Help(metricPrefix+"shadow_shard_ready",
		"本节点承载的影子分片是否已经识别 leader", metrics.TypeGauge)
	e.Help(metricPrefix+"shadow_shard_is_leader",
		"本节点是否为该影子分片的当前 leader", metrics.TypeGauge)
	e.Help(metricPrefix+"shadow_shard_applied_index",
		"本节点影子分片已持久化应用的Raft日志序号", metrics.TypeGauge)
	for _, lead := range g.LocalLeadership() {
		shard := metrics.Label{Name: "shard", Value: strconv.FormatUint(lead.Shard, 10)}
		ready, isLeader := int64(0), int64(0)
		if lead.Ready {
			ready = 1
		}
		if lead.IsLeader {
			isLeader = 1
		}
		e.GaugeInt(metricPrefix+"shadow_shard_ready", ready, node, shard)
		e.GaugeInt(metricPrefix+"shadow_shard_is_leader", isLeader, node, shard)
		e.Gauge(metricPrefix+"shadow_shard_applied_index", float64(lead.AppliedIndex),
			node, shard)
	}
	e.Help(metricPrefix+"shadow_proposals_total",
		"累计影子提案数，按发起时本节点是否为该分片 leader", metrics.TypeCounter)
	for _, m := range []struct {
		where string
		v     uint64
	}{{"local", st.ProposeLocal}, {"remote", st.ProposeRemote}, {"unknown", st.ProposeUnknown}} {
		e.CounterUint(metricPrefix+"shadow_proposals_total", m.v, node,
			metrics.Label{Name: "leader", Value: m.where})
	}
	e.Help(metricPrefix+"shadow_proposal_errors_total",
		"影子提案失败数（没有 leader、失去多数派或超时）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_proposal_errors_total", st.ProposeErrors, node)
	e.Help(metricPrefix+"shadow_raft_proposals_total",
		"实际提交给 Raft 的影子日志条数（二级合批后的物理提案数）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_raft_proposals_total", st.RaftProposals, node)
	for _, m := range []struct {
		name, help string
		v          uint64
	}{
		{"shadow_leader_merge_access_batches_total", "leader 二次合批已消费的 Access 侧影子批次数", st.LeaderMergeAccessBatches},
		{"shadow_leader_merge_logical_total", "leader 二次合批已装入 Raft 日志的逻辑影子命令数", st.LeaderMergeLogical},
		{"shadow_leader_merge_physical_total", "leader 二次合批实际提交的物理影子 Raft 日志数", st.LeaderMergePhysical},
		{"shadow_leader_merge_full_total", "leader 二次合批达到当前配置上限的影子日志数", st.LeaderMergeFull},
		{"shadow_leader_merge_drained_total", "leader 二次合批未满且已耗尽当前影子队列快照的日志数", st.LeaderMergeDrained},
		{"shadow_leader_merge_blocked_total", "leader 二次合批未满且被字节上限或无效命令截断的影子日志数", st.LeaderMergeBlocked},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeCounter)
		e.CounterUint(metricPrefix+m.name, m.v, node)
	}
	for _, m := range []struct {
		name, help string
		v          int64
	}{
		{"shadow_leader_merge_queued", "当前在 shard leader 等待二次合并的影子提案数", st.LeaderMergeQueued},
		{"shadow_leader_merge_in_flight", "当前由 shard leader 提交给 Raft 的二次合并影子日志数", st.LeaderMergeInFlight},
		{"shadow_leader_merge_peak_queued", "进程启动以来影子 leader 二次合并排队峰值", st.LeaderMergePeakQueued},
		{"shadow_leader_merge_peak_in_flight", "进程启动以来影子 leader 提交 Raft 的在途日志峰值", st.LeaderMergePeakInFlight},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, m.v, node)
	}
	e.Help(metricPrefix+"shadow_proposal_duration_ms",
		"影子提案自发起到本地副本应用返回的耗时", metrics.TypeHistogram)
	e.Histogram(metricPrefix+"shadow_proposal_duration_ms", st.Duration, node)
	if a.s.shadowRepl != nil {
		waits, over1s, maxNs := a.s.shadowRepl.CommitWaitStats()
		e.Help(metricPrefix+"shadow_commit_waits_total", "leader提案返回后仍等待本地影子apply结果的次数", metrics.TypeCounter)
		e.CounterUint(metricPrefix+"shadow_commit_waits_total", waits, node)
		e.Help(metricPrefix+"shadow_commit_wait_over_1s_total", "等待本地影子apply结果超过1秒的次数", metrics.TypeCounter)
		e.CounterUint(metricPrefix+"shadow_commit_wait_over_1s_total", over1s, node)
		e.Help(metricPrefix+"shadow_commit_wait_max_ms", "进程启动以来本地影子apply结果最大等待耗时", metrics.TypeGauge)
		e.Gauge(metricPrefix+"shadow_commit_wait_max_ms", float64(maxNs)/float64(time.Millisecond), node)
	}
	e.Help(metricPrefix+"shadow_remote_reads_total",
		"由非副本节点发起的逻辑影子读数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_reads_total", a.s.shadowRoute.remoteReads.Load(), node)
	e.Help(metricPrefix+"shadow_remote_read_rpcs_total",
		"影子远程读物理 RPC 数（含副本回退）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_read_rpcs_total", a.s.shadowRoute.remoteReadRPCs.Load(), node)
	e.Help(metricPrefix+"shadow_remote_read_fallbacks_total",
		"首选副本失败后尝试其他副本的次数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_read_fallbacks_total", a.s.shadowRoute.remoteReadFallbacks.Load(), node)
	e.Help(metricPrefix+"shadow_remote_read_errors_total",
		"所有放置副本均读失败的逻辑请求数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_read_errors_total", a.s.shadowRoute.remoteReadErrors.Load(), node)
	e.Help(metricPrefix+"shadow_remote_writes_total",
		"由非副本节点发出的影子写 RPC 数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_writes_total", a.s.shadowRoute.remoteWrites.Load(), node)
	e.Help(metricPrefix+"shadow_remote_write_errors_total",
		"影子远程写失败数；写失败不会换副本重试", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"shadow_remote_write_errors_total", a.s.shadowRoute.remoteWriteErrors.Load(), node)
}

// collectSessionShards 暴露会话分区组的提案侧指标（OBS-148）。
//
// 单机下 s.shards 为 nil：那条路径不经复制组，没有提案可言。
func (a *adminServer) collectSessionShards(e *metrics.Encoder, node metrics.Label) {
	if a.s.shards == nil {
		return
	}
	st := a.s.shards.Stats()

	e.Help(metricPrefix+"session_shard_count", "会话分片数（真实 raft 日志条数）",
		metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"session_shard_count", int64(a.s.shards.ShardCount()), node)
	e.Help(metricPrefix+"session_shard_ready",
		"本节点承载的会话分片是否已经识别 leader", metrics.TypeGauge)
	e.Help(metricPrefix+"session_shard_is_leader",
		"本节点是否为该会话分片的当前 leader", metrics.TypeGauge)
	e.Help(metricPrefix+"session_shard_applied_index",
		"本节点会话分片已持久化应用的 Raft 日志序号", metrics.TypeGauge)
	for _, lead := range a.s.shards.LocalLeadership() {
		shard := metrics.Label{Name: "shard", Value: strconv.FormatUint(lead.Shard, 10)}
		ready, isLeader := int64(0), int64(0)
		if lead.Ready {
			ready = 1
		}
		if lead.IsLeader {
			isLeader = 1
		}
		e.GaugeInt(metricPrefix+"session_shard_ready", ready, node, shard)
		e.GaugeInt(metricPrefix+"session_shard_is_leader", isLeader, node, shard)
		e.Gauge(metricPrefix+"session_shard_applied_index", float64(lead.AppliedIndex),
			node, shard)
	}

	// 按 leader 位置分类的提案数。
	//
	// 读法：remote 占比是接入节点不是该分片 leader 的比例。
	// CLU-136 之后这些提案会经 markOwn 长连接直达 leader，不再
	// 先交给本地 follower 由 Dragonboat 内部转发。副本因子等于
	// 节点数时，这个比例的期望仍是 (N-1)/N。
	e.Help(metricPrefix+"session_proposals_total",
		"累计会话提案数，按发起时本节点是否为该分片 leader", metrics.TypeCounter)
	for _, m := range []struct {
		where string
		v     uint64
	}{{"local", st.ProposeLocal}, {"remote", st.ProposeRemote}, {"unknown", st.ProposeUnknown}} {
		e.CounterUint(metricPrefix+"session_proposals_total", m.v, node,
			metrics.Label{Name: "leader", Value: m.where})
	}

	e.Help(metricPrefix+"session_proposal_errors_total",
		"会话提案失败数（没有 leader、失去多数派或超时）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_proposal_errors_total", st.ProposeErrors, node)
	e.Help(metricPrefix+"session_proposal_safe_reroutes_total",
		"leader 切换时可证明尚未发送并安全重路由的会话提案数", metrics.TypeCounter)
	for _, m := range []struct {
		source string
		v      uint64
	}{{"local", st.SafeRerouteLocal}, {"remote", st.SafeRerouteRemote}} {
		e.CounterUint(metricPrefix+"session_proposal_safe_reroutes_total", m.v, node,
			metrics.Label{Name: "source", Value: m.source})
	}
	e.Help(metricPrefix+"session_raft_proposals_total",
		"实际提交给 Raft 的会话日志条数（二级合批后的物理提案数）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_raft_proposals_total", st.RaftProposals, node)
	for _, m := range []struct {
		name, help string
		v          uint64
	}{
		{"session_leader_merge_access_batches_total", "leader 二次合批已消费的 Access 侧批次数", st.LeaderMergeAccessBatches},
		{"session_leader_merge_logical_total", "leader 二次合批已装入 Raft 日志的逻辑会话命令数", st.LeaderMergeLogical},
		{"session_leader_merge_physical_total", "leader 二次合批实际提交的物理 Raft 日志数", st.LeaderMergePhysical},
		{"session_leader_merge_full_total", "leader 二次合批装满 64 条逻辑命令的日志数", st.LeaderMergeFull},
		{"session_leader_merge_drained_total", "leader 二次合批未满且已耗尽当前队列快照的日志数", st.LeaderMergeDrained},
		{"session_leader_merge_blocked_total", "leader 二次合批未满且被重复 ID、字节上限或维护命令截断的日志数", st.LeaderMergeBlocked},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeCounter)
		e.CounterUint(metricPrefix+m.name, m.v, node)
	}
	for _, m := range []struct {
		name, help string
		v          int64
	}{
		{"session_leader_merge_queued", "当前在 shard leader 等待二次合并的 Access 批次数", st.LeaderMergeQueued},
		{"session_leader_merge_in_flight", "当前由 shard leader 提交给 Raft 的二次合并日志数", st.LeaderMergeInFlight},
		{"session_leader_merge_peak_queued", "进程启动以来 shard leader 二次合并排队峰值", st.LeaderMergePeakQueued},
		{"session_leader_merge_peak_in_flight", "进程启动以来 shard leader 提交 Raft 的在途日志峰值", st.LeaderMergePeakInFlight},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, m.v, node)
	}

	e.Help(metricPrefix+"session_remote_reads_total",
		"非本地 session cell 的逻辑读取数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_remote_reads_total", st.SessionReadLogical, node)
	e.Help(metricPrefix+"session_remote_read_batches_total",
		"非本地 session cell 的批量 RPC 数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_remote_read_batches_total", st.SessionReadBatches, node)
	e.Help(metricPrefix+"session_remote_read_claim_piggyback_total",
		"由 ownership Claim 应答直接携带、因而未另发读取 RPC 的会话读取数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_remote_read_claim_piggyback_total", st.SessionReadClaimed, node)
	e.Help(metricPrefix+"session_remote_read_errors_total",
		"非本地 session cell 读取失败的逻辑请求数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_remote_read_errors_total", st.SessionReadErrors, node)

	// Access 侧提案 pipeline 的队列压力。in_flight_batches 长时间顶到 8、
	// queued 同时增长才说明共识已形成背压；只看 CONNECT p99 无法区分
	// 是 TLS、ownership 还是 session 写在排队。
	qst := a.s.sessions.proposalQueueStats()
	e.Help(metricPrefix+"session_proposal_backpressured_total",
		"入队时所在分片已用满在途提案上限的会话写数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"session_proposal_backpressured_total", qst.Backpressured, node)
	for _, m := range []struct {
		name, help string
		v          int
	}{
		{"session_proposal_queued", "当前等待合批或 pipeline 背压的会话写数", qst.Queued},
		{"session_proposal_in_flight", "当前已交给共识层、等待结果的逻辑会话写数", qst.InFlight},
		{"session_proposal_in_flight_batches", "当前已交给共识层的 Access 侧物理批次数", qst.InFlightBatches},
		{"session_proposal_peak_queued", "进程启动以来 Access 侧会话写排队峰值", qst.PeakQueued},
		{"session_proposal_peak_in_flight_batches", "进程启动以来 Access 侧会话提案在途批次峰值", qst.PeakInFlightBatches},
		{"session_proposal_cached_requests", "当前缓存的会话提案请求对象数", qst.CachedRequests},
		{"session_proposal_cached_batches", "当前缓存的会话提案批工作区数", qst.CachedBatches},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, int64(m.v), node)
	}

	// Ownership Claim 是每次集群 CONNECT 必经的一轮写语义 RPC。
	// logical/rpc 的比值是合批系数；queued 和 in_flight_rpcs 同时顶高
	// 说明对端或网络已饱和，不应再用增加 CONNECT 协程掩盖。
	ost := a.s.owners.Stats()
	e.Help(metricPrefix+"ownership_remote_claims_total",
		"非 leader Access 节点发起的逻辑 Ownership Claim 数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"ownership_remote_claims_total", ost.RemoteClaims, node)
	e.Help(metricPrefix+"ownership_claim_rpcs_total",
		"Ownership Claim 物理 RPC 数（包含明确 NotLeader 后的副本回退）", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"ownership_claim_rpcs_total", ost.RemoteRPCs, node)
	e.Help(metricPrefix+"ownership_claim_errors_total",
		"Ownership Claim 最终失败的逻辑请求数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"ownership_claim_errors_total", ost.RemoteErrors, node)
	e.Help(metricPrefix+"ownership_claim_backpressured_total",
		"入队时所在分片已用满在途 RPC 上限的 Claim 数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"ownership_claim_backpressured_total", ost.Backpressured, node)
	for _, m := range []struct {
		name, help string
		v          int
	}{
		{"ownership_claim_queued", "当前等待合批或背压的 Claim 数", ost.QueuedClaims},
		{"ownership_claim_in_flight", "当前已发出、等待应答的逻辑 Claim 数", ost.InFlightClaims},
		{"ownership_claim_in_flight_rpcs", "当前已发出的 Claim 物理 RPC 数", ost.InFlightRPCs},
		{"ownership_claim_cached_requests", "当前缓存的 Claim 请求对象数", ost.CachedRequests},
	} {
		e.Help(metricPrefix+m.name, m.help, metrics.TypeGauge)
		e.GaugeInt(metricPrefix+m.name, int64(m.v), node)
	}

	// 与 connect_stage_duration_ms{stage="session_write"} 对照着看：
	// 两者的差额是存储层在共识之外花的时间（编解码与本地写），
	// 差额很小就说明时间都在共识那一轮里。
	e.Help(metricPrefix+"session_proposal_duration_ms",
		"会话提案自发起到应用返回的耗时", metrics.TypeHistogram)
	e.Histogram(metricPrefix+"session_proposal_duration_ms", st.Duration, node)
}

// collectPKI 暴露 PKI 指标（OBS-027）。
func (a *adminServer) collectPKI(e *metrics.Encoder, node metrics.Label) {
	certs, err := a.s.reg2.ListCertificates(registry.CertQuery{Limit: 0})
	if err != nil {
		// 取不到就整族不导出，而不是导出零值。零值会被读成"确实
		// 没有证书"，从而把一次读失败伪装成一个正常读数。
		return
	}
	// 按状态分桶。状态取值是 API 契约里的固定集合，不会有高基数问题。
	//
	// 未知状态单独一桶而不是丢掉：如果存储里混进了拼错的状态值，
	// 指标上要能看见——证书状态决定能不能连接，一个数不上来的
	// 状态值是安全问题，不是展示问题。
	counts := map[string]int64{
		registry.StatusActive:            0,
		registry.StatusInactive:          0,
		registry.StatusRevoked:           0,
		registry.StatusPendingActivation: 0,
	}
	for _, c := range certs.Certificates {
		if _, ok := counts[c.Status]; ok {
			counts[c.Status]++
		} else {
			counts["unknown"]++
		}
	}
	e.Help(metricPrefix+"certificates", "证书数，按状态", metrics.TypeGauge)
	// 固定顺序输出，使两次抓取之间的样本顺序稳定，便于 diff。
	for _, st := range []string{
		registry.StatusActive, registry.StatusInactive,
		registry.StatusRevoked, registry.StatusPendingActivation, "unknown",
	} {
		n, ok := counts[st]
		if !ok && st != "unknown" {
			continue
		}
		e.GaugeInt(metricPrefix+"certificates", n, node,
			metrics.Label{Name: "status", Value: st})
	}

	e.Help(metricPrefix+"certificates_expiring_soon",
		"30 天内到期的有效证书数。到期会让设备在无人干预下掉线",
		metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"certificates_expiring_soon",
		countExpiringSoon(certs.Certificates, a.s.now()), node)
}

// expirySoonMs 是"即将到期"的阈值（OBS-027）。
//
// 30 天：证书轮换要走签发、下发、设备重连三步，留一个月才来得及。
const expirySoonMs int64 = 30 * 24 * 60 * 60 * 1000

// countExpiringSoon 数出窗口内到期的有效证书。
//
// 只数有效的：已吊销或未激活的证书到期与否不影响任何设备。
func countExpiringSoon(certs []registry.Certificate, nowMs int64) int64 {
	var n int64
	for i := range certs {
		c := &certs[i]
		if c.Status != registry.StatusActive {
			continue
		}
		if c.NotAfterUnixMs > 0 && c.NotAfterUnixMs-nowMs < expirySoonMs {
			n++
		}
	}
	return n
}

// collectQuota 暴露配额指标（OBS-029）。
func (a *adminServer) collectQuota(e *metrics.Encoder, node metrics.Label) {
	stats := a.s.lim.Stats()
	e.Help(metricPrefix+"quota_global_rate_members",
		"聚合型全局速率配额当前等分的Raft成员数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"quota_global_rate_members", a.s.quotaMembers.Load(), node)
	e.Help(metricPrefix+"quota_rate_buckets", "当前常驻的速率令牌桶数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"quota_rate_buckets", a.s.lim.RateBucketCount(), node)
	e.Help(metricPrefix+"quota_rate_buckets_swept_total",
		"恢复满额后被回收的速率令牌桶数", metrics.TypeCounter)
	e.CounterUint(metricPrefix+"quota_rate_buckets_swept_total",
		a.s.lim.RateBucketsSwept(), node)

	e.Help(metricPrefix+"quota_allowed_total", "配额放行数", metrics.TypeCounter)
	for _, s := range stats {
		if s.Allowed == 0 {
			continue
		}
		e.CounterUint(metricPrefix+"quota_allowed_total", s.Allowed, node,
			metrics.Label{Name: "quota", Value: s.Name})
	}

	e.Help(metricPrefix+"quota_rejected_total",
		"配额拒绝数。这一族是容量问题的第一现场", metrics.TypeCounter)
	for _, s := range stats {
		if s.Rejected == 0 {
			continue
		}
		e.CounterUint(metricPrefix+"quota_rejected_total", s.Rejected, node,
			metrics.Label{Name: "quota", Value: s.Name})
	}

	e.Help(metricPrefix+"quota_current", "计数型配额的当前值", metrics.TypeGauge)
	for _, s := range stats {
		if s.Current == 0 {
			continue
		}
		e.GaugeInt(metricPrefix+"quota_current", s.Current, node,
			metrics.Label{Name: "quota", Value: s.Name})
	}
}

// collectEvents 暴露事件总线自身的指标。
func (a *adminServer) collectEvents(e *metrics.Encoder, node metrics.Label) {
	st := a.s.bus.Stats()

	e.Help(metricPrefix+"events_emitted_total", "已发布事件数，按类型", metrics.TypeCounter)
	for i, name := range events.KindNames() {
		e.CounterUint(metricPrefix+"events_emitted_total", st.Emitted[i], node,
			metrics.Label{Name: "kind", Value: name})
	}

	e.Help(metricPrefix+"events_dropped_total",
		"因订阅者跟不上而丢弃的投递数。非零意味着控制台看到的事件不完整",
		metrics.TypeCounter)
	e.CounterUint(metricPrefix+"events_dropped_total", st.Dropped, node)

	e.Help(metricPrefix+"event_subscribers", "当前事件订阅者数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"event_subscribers", int64(st.Subscribers), node)
}

// collectRuntime 暴露 Go 运行时指标（OBS-028）。
func collectRuntime(e *metrics.Encoder, node metrics.Label) {
	e.Help(metricPrefix+"goroutines", "goroutine 数", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"goroutines", int64(runtime.NumGoroutine()), node)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	for _, g := range []struct {
		name, help string
		v          uint64
	}{
		{"heap_alloc_bytes", "当前堆上活跃对象字节数", m.HeapAlloc},
		{"heap_sys_bytes", "向系统申请的堆字节数", m.HeapSys},
		{"heap_inuse_bytes", "堆中已映射并正在使用的 span 字节数", m.HeapInuse},
		{"heap_released_bytes", "已归还操作系统的空闲堆字节数", m.HeapReleased},
		{"heap_objects", "堆上对象数", m.HeapObjects},
		{"stack_inuse_bytes", "goroutine 栈正在使用的字节数", m.StackInuse},
		{"stack_sys_bytes", "goroutine 栈占用字节数", m.StackSys},
		{"runtime_sys_bytes", "Go 运行时从操作系统取得的总字节数", m.Sys},
		{"gc_cycles_total", "GC 轮次", uint64(m.NumGC)},
	} {
		e.Help(metricPrefix+g.name, g.help, metrics.TypeGauge)
		e.CounterUint(metricPrefix+g.name, g.v, node)
	}
	memoryLimit := debug.SetMemoryLimit(-1)
	e.Help(metricPrefix+"runtime_memory_limit_bytes",
		"Go runtime 当前 soft memory limit；未限制时为 MaxInt64", metrics.TypeGauge)
	e.GaugeInt(metricPrefix+"runtime_memory_limit_bytes", memoryLimit, node)
	if rss, ok := processResidentBytes(); ok {
		e.Help(metricPrefix+"process_resident_memory_bytes",
			"进程当前驻留物理内存字节数（Linux /proc）", metrics.TypeGauge)
		e.CounterUint(metricPrefix+"process_resident_memory_bytes", rss, node)
	}

	// GC 暂停分位（OBS-028）。
	//
	// 用 debug.GCStats 的 PauseQuantiles 而不是自己攒直方图：运行时
	// 已经在记了，再记一份既浪费又会与它不一致。
	var gc debug.GCStats
	gc.PauseQuantiles = make([]time.Duration, 5) // min, p25, p50, p75, max
	debug.ReadGCStats(&gc)
	e.Help(metricPrefix+"gc_pause_seconds", "GC 暂停时长分位", metrics.TypeGauge)
	for i, q := range []string{"0", "0.25", "0.5", "0.75", "1"} {
		e.Gauge(metricPrefix+"gc_pause_seconds", gc.PauseQuantiles[i].Seconds(), node,
			metrics.Label{Name: "quantile", Value: q})
	}
}

// protoVerLabel 把协议版本号转成可读标签。
func protoVerLabel(v uint8) string {
	switch v {
	case 4:
		return "3.1.1"
	case 5:
		return "5.0"
	case 3:
		return "3.1"
	default:
		return "unknown"
	}
}

func grantedLabel(ok bool) string {
	if ok {
		return "granted"
	}
	return "denied"
}
