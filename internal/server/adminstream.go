package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// 事件流与指标摘要（PRD API-051、API-059、UI-003、UI-020）。

// sseEvent 是推给控制台的事件。
//
// 字段名用小写，与 Admin 面其余响应一致；不直接序列化 events.Event，
// 因为那会把内部结构（含定长数组与未使用的字段族）暴露成 API 契约。
type sseEvent struct {
	Kind     string `json:"kind"`
	Node     string `json:"node"`
	Seq      uint64 `json:"seq"`
	At       int64  `json:"at"`
	ClientID string `json:"clientId,omitempty"`
	Thing    string `json:"thing,omitempty"`
	// Backlog 为真表示这条是订阅时回填的历史事件，不是刚发生的。
	//
	// 必须区分：一段历史上的连接拒绝若不加标记，看起来就像正在发生的
	// 一场风暴，会把人引向错误的紧急判断。
	Backlog bool `json:"backlog,omitempty"`

	// 以下按事件类型填充其中一组。
	Conn  *sseConn  `json:"conn,omitempty"`
	Sub   *sseSub   `json:"sub,omitempty"`
	Authz *sseAuthz `json:"authz,omitempty"`
	Meta  *sseMeta  `json:"meta,omitempty"`
	Quota *sseQuota `json:"quota,omitempty"`
	Audit *sseAudit `json:"audit,omitempty"`
}

type sseAudit struct {
	Action string `json:"action"`
	Target string `json:"target,omitempty"`
	Actor  string `json:"actor,omitempty"`
	Remote string `json:"remote,omitempty"`
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`
}

type sseConn struct {
	Result      string  `json:"result"`
	ReasonCode  uint8   `json:"reasonCode"`
	ReasonText  string  `json:"reasonText,omitempty"`
	CertFP      string  `json:"certFingerprint,omitempty"`
	CertCN      string  `json:"certCommonName,omitempty"`
	AuthMethod  string  `json:"authMethod,omitempty"`
	ProtoVer    uint8   `json:"protoVer,omitempty"`
	Remote      string  `json:"remote,omitempty"`
	TLSVersion  uint16  `json:"tlsVersion,omitempty"`
	ALPN        string  `json:"alpn,omitempty"`
	HandshakeMS float64 `json:"handshakeMs,omitempty"`
	DurationMS  int64   `json:"durationMs,omitempty"`
}

type sseSub struct {
	Filter      string `json:"filter"`
	Share       string `json:"share,omitempty"`
	QoS         uint8  `json:"qos"`
	Unsubscribe bool   `json:"unsubscribe,omitempty"`
	Granted     uint8  `json:"granted"`
}

type sseAuthz struct {
	Action         string   `json:"action"`
	Resource       string   `json:"resource"`
	Decision       string   `json:"decision"`
	PolicyName     string   `json:"policyName,omitempty"`
	StatementIndex int      `json:"statementIndex"`
	Variables      []sseVar `json:"variables,omitempty"`
}

type sseVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type sseMeta struct {
	Revision uint64 `json:"revision"`
	CertFP   string `json:"certFingerprint,omitempty"`
	Thing    string `json:"thing,omitempty"`
	Reason   string `json:"reason,omitempty"`
	// RequiresDisconnect 为真表示该变更导致了连接被踢。
	// 控制台据此把这条事件标为"吊销生效"而不只是"元数据变更"。
	RequiresDisconnect bool `json:"requiresDisconnect,omitempty"`
}

type sseQuota struct {
	Name    string `json:"name"`
	Subject string `json:"subject,omitempty"`
	Limit   int    `json:"limit"`
}

// handleEvents 以 SSE 推送事件流（PRD API-051）。
//
// 用 SSE 而非 WebSocket：事件是单向的，SSE 走普通 HTTP，
// 自带断线重连，且能被反向代理透明转发。WebSocket 留给
// 双向的主题抓包（API-052）。
func (a *adminServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError,
			"当前服务端不支持流式响应", "")
		return
	}

	filter := events.Filter{Kinds: parseKinds(r.URL.Query().Get("kinds"))}
	if v := r.URL.Query().Get("clientId"); v != "" {
		filter.ClientID = v
	}
	if v := r.URL.Query().Get("thing"); v != "" {
		filter.Thing = v
	}

	// 回填条数。0 表示不回填，用于只想看实时的消费者。
	backlog := defaultBacklog
	if v := r.URL.Query().Get("backlog"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeAPIError(w, http.StatusBadRequest,
				"backlog 须为非负整数", "回填条数，0 表示只看实时")
			return
		}
		backlog = n
	}

	// 缓冲 256 条：订阅者跟不上时事件总线会丢弃并计数（契约 11），
	// 而不是阻塞发布者。控制台少看到几条事件远好于让它拖慢 broker。
	//
	// 带回填订阅：事件流最有用的时刻是"出问题之后"，而运维是在出问题
	// 之后才打开控制台的。不回填的话他看到一个空面板，而空面板与坏掉的
	// 面板无法区分。
	ch, recent, cancel := a.s.bus.SubscribeWithRecent(filter, 256, backlog)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	// 关掉 nginx 的响应缓冲，否则事件会被攒住直到缓冲满
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// 心跳：代理与负载均衡常在 60 秒无数据时断开连接。
	// 注释行（以冒号开头）是 SSE 规范里的空操作，客户端会忽略。
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	enc := json.NewEncoder(w)

	// 先推回填，并记下其中最大的 Seq。
	//
	// SubscribeWithRecent 为了不丢事件而允许重复（见其注释），
	// 这里用 Seq 把重复的那几条滤掉。
	var lastSeq uint64
	for i := range recent {
		if recent[i].Seq > lastSeq {
			lastSeq = recent[i].Seq
		}
		if !writeSSE(w, flusher, enc, &recent[i], true) {
			return
		}
	}
	// 回填结束的标记。控制台据此画一条"以上为历史"的分隔线——
	// 否则一段历史上的拒绝看起来像正在发生的风暴。
	if _, err := fmt.Fprintf(w, "event: sync\ndata: {\"backlog\":%d}\n\n",
		len(recent)); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// 已在回填里推过的跳过。
			if ev.Seq <= lastSeq {
				continue
			}
			if !writeSSE(w, flusher, enc, &ev, false) {
				return
			}
		}
	}
}

// defaultBacklog 是订阅事件流时默认回填的条数。
//
// 比总线环形缓冲（512）小：控制台一次渲染两百条已经够看一整段现场，
// 而回填是在建立连接时同步写出的，太多会让首屏变慢。
const defaultBacklog = 200

// writeSSE 写一条 SSE 事件。返回 false 表示连接已不可写，调用方应退出。
func writeSSE(w io.Writer, flusher http.Flusher, enc *json.Encoder,
	e *events.Event, isBacklog bool) bool {
	if _, err := fmt.Fprintf(w, "event: %s\ndata: ", e.Kind.String()); err != nil {
		return false
	}
	se := toSSE(e)
	se.Backlog = isBacklog
	if err := enc.Encode(se); err != nil {
		return false
	}
	if _, err := fmt.Fprint(w, "\n"); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// parseKinds 解析逗号分隔的事件类型筛选。
func parseKinds(s string) uint32 {
	if strings.TrimSpace(s) == "" {
		return events.AllKinds
	}
	var mask uint32
	for _, name := range strings.Split(s, ",") {
		k, ok := events.ParseKind(strings.TrimSpace(name))
		if !ok {
			continue
		}
		mask |= k.Mask()
	}
	if mask == 0 {
		return events.AllKinds
	}
	return mask
}

// toSSE 把内部事件转成对外形状。
func toSSE(e *events.Event) *sseEvent {
	out := &sseEvent{
		Kind: e.Kind.String(), Node: string(e.Node), Seq: e.Seq,
		At: e.AtUnixMs, ClientID: e.ClientID, Thing: e.Thing,
	}
	switch e.Kind {
	case events.KindConn:
		out.Conn = &sseConn{
			Result: e.Conn.Result.String(), ReasonCode: uint8(e.Conn.ReasonCode),
			ReasonText: e.Conn.ReasonText, CertFP: e.Conn.CertFP,
			CertCN: e.Conn.CertCN, AuthMethod: e.Conn.AuthMethod.String(),
			ProtoVer: e.Conn.ProtoVer, TLSVersion: e.Conn.TLSVersion,
			ALPN: e.Conn.ALPN, HandshakeMS: e.Conn.HandshakeMS,
			DurationMS: e.Conn.DurationMS,
		}
		if e.Conn.Remote.IsValid() {
			out.Conn.Remote = e.Conn.Remote.String()
		}
	case events.KindSubscription:
		out.Sub = &sseSub{
			Filter: e.Sub.Filter, Share: e.Sub.Share, QoS: e.Sub.QoS,
			Unsubscribe: e.Sub.Unsubscribe, Granted: uint8(e.Sub.Granted),
		}
	case events.KindAuthz:
		az := &sseAuthz{
			Action: e.Authz.Action.String(), Resource: e.Authz.Resource,
			Decision: e.Authz.Decision.String(), PolicyName: e.Authz.PolicyName,
			StatementIndex: e.Authz.StatementIndex,
		}
		// 变量求值结果是策略调试器"一键回放"的数据来源（UI-010）：
		// 它在判定时捕获，事后无法重建。
		for _, v := range e.Authz.VarList() {
			az.Variables = append(az.Variables, sseVar{Name: v.Name, Value: v.Value})
		}
		out.Authz = az
	case events.KindMeta:
		out.Meta = &sseMeta{
			Revision: e.Meta.Revision, CertFP: e.Meta.CertFP,
			Thing: e.Meta.Thing, Reason: e.Meta.Reason,
			RequiresDisconnect: e.Meta.RequiresDisconnect,
		}
	case events.KindQuota:
		out.Quota = &sseQuota{
			Name: e.Quota.QuotaName, Subject: e.Quota.Subject,
			Limit: e.Quota.Limit,
		}
	case events.KindAudit:
		out.Audit = &sseAudit{
			Action: e.Audit.Action, Target: e.Audit.Target,
			Actor: e.Audit.Actor, Remote: e.Audit.Remote,
			Method: e.Audit.Method, Path: e.Audit.Path,
		}
	}
	return out
}

// ---------- 指标摘要（PRD API-059、UI-020） ----------

type overviewResponse struct {
	Node nodeResponse `json:"node"`
	// Quotas 是各配额的命中与拒绝计数。
	//
	// 异常排行比平均值有用（UI-020）：按拒绝数降序排，
	// 运维一眼能看到"哪一项正在被打满"。
	Quotas []quotaStat `json:"quotas"`
	// Events 是事件总线的发布与丢弃计数。
	//
	// 丢弃数非零意味着有订阅者跟不上，控制台看到的事件不完整——
	// 不暴露它会让"少了几条事件"成为无法解释的现象。
	Events eventStats `json:"events"`
	// Online 是在线连接的采样点，按时间从旧到新。
	// 控制台用它画曲线；只有一个点时还画不成线。
	Online []onlinePoint `json:"online"`
}

type onlinePoint struct {
	T int64 `json:"t"`
	N int64 `json:"n"`
}

type quotaStat struct {
	Name     string `json:"name"`
	Scope    string `json:"scope"`
	Kind     string `json:"kind"`
	Limit    int    `json:"limit"`
	Allowed  uint64 `json:"allowed"`
	Rejected uint64 `json:"rejected"`
	Current  int64  `json:"current"`
	PRDRef   string `json:"prdRef,omitempty"`
}

type eventStats struct {
	Emitted map[string]uint64 `json:"emitted"`
	// Dropped 非零意味着有订阅者跟不上，控制台看到的事件不完整——
	// 不暴露它会让"少了几条事件"成为无法解释的现象。
	Dropped     uint64 `json:"dropped"`
	Subscribers int    `json:"subscribers"`
	Seq         uint64 `json:"seq"`
}

func (a *adminServer) handleOverview(w http.ResponseWriter, _ *http.Request) {
	s := a.s
	out := overviewResponse{
		Node: nodeResponse{
			NodeID: s.mf.NodeID, ClusterID: s.mf.ClusterID,
			ShardCount: s.mf.ShardCount, Mode: string(s.cfg.Node.Mode),
			Ready: s.ready.Ready(), Connections: s.brk.Conns(),
			Shadows: s.shadows.CountShadows(), Sessions: s.sessions.Count(),
			Retained: s.retained.Count(),
		},
		Quotas: []quotaStat{},
		Events: eventStats{Emitted: map[string]uint64{}},
	}
	for _, st := range s.lim.Stats() {
		// 只回传被用过的配额：三十多项里多数在一个部署里从不触发，
		// 全列出来会把真正在动的那几项埋掉。
		if st.Allowed == 0 && st.Rejected == 0 && st.Current == 0 {
			continue
		}
		out.Quotas = append(out.Quotas, quotaStat{
			Name: st.Name, Scope: scopeName(st.Scope), Kind: kindName(st.Kind),
			Limit: s.lim.Limit(st.ID), Allowed: st.Allowed,
			Rejected: st.Rejected, Current: st.Current, PRDRef: st.PRD,
		})
	}
	bs := s.bus.Stats()
	for k := events.Kind(0); k < events.NumKinds(); k++ {
		if n := bs.Emitted[k]; n > 0 {
			out.Events.Emitted[k.String()] = n
		}
	}
	// 丢弃是全局计数而非按类型：丢弃发生在投递给订阅者时，
	// 那一刻已经不区分类型了。非零意味着控制台看到的事件不完整。
	out.Events.Dropped = bs.Dropped
	out.Events.Subscribers = bs.Subscribers
	out.Events.Seq = bs.Seq
	out.Online = s.noteOnline()
	writeJSON(w, http.StatusOK, out)
}

// noteOnline 记下当前在线连接数并返回全部采样的副本。
func (s *Server) noteOnline() []onlinePoint {
	s.onlineMu.Lock()
	defer s.onlineMu.Unlock()
	s.online = append(s.online, onlinePoint{T: s.now(), N: s.brk.Conns()})
	if len(s.online) > 120 {
		s.online = append([]onlinePoint(nil), s.online[len(s.online)-120:]...)
	}
	if len(s.online) == 0 {
		return []onlinePoint{}
	}
	return append([]onlinePoint(nil), s.online...)
}

func scopeName(s quota.Scope) string {
	if s == quota.ScopeGlobal {
		return "global"
	}
	return "local"
}

func kindName(k quota.Kind) string {
	switch k {
	case quota.Rate:
		return "rate"
	case quota.Count:
		return "count"
	case quota.Size:
		return "size"
	}
	return "unknown"
}
