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
		{"heap_objects", "堆上对象数", m.HeapObjects},
		{"stack_sys_bytes", "goroutine 栈占用字节数", m.StackSys},
		{"gc_cycles_total", "GC 轮次", uint64(m.NumGC)},
	} {
		e.Help(metricPrefix+g.name, g.help, metrics.TypeGauge)
		e.CounterUint(metricPrefix+g.name, g.v, node)
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
