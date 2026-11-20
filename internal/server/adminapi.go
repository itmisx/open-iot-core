package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// adminAPIPrefix 是管理面的路径前缀（PRD API-002）。
const adminAPIPrefix = "/api/admin/v1"

// mountAWS 把 /api/aws 挂到管理端口上。
//
// 与管理面共用监听器，是因为控制面是低频的 HTTPS/HTTP API，
// 不该再占一个端口。鉴权是 SigV4（服务名 iot），不是管理令牌。
func (a *adminServer) mountAWS(mux *http.ServeMux) {
	c := a.s.cfg.AWSAPI
	if !c.Enabled {
		return
	}
	var issuer awsapi.CertIssuer
	if a.s.ca != nil {
		issuer = caIssuer{ca: a.s.ca}
	}
	h := awsapi.Handler{
		Things: a.s.reg2,
		Checker: sigv4.Checker{
			Region: c.Region,
			Services: map[string]bool{
				sigv4.ServiceIoT: true, sigv4.ServiceIoTData: true,
			},
			Secret: a.s.awsSecret,
		},
		Role:      a.s.awsRole,
		Partition: a.s.cfg.IoT.Partition,
		Region:    a.s.cfg.IoT.Region,
		Account:   a.s.cfg.IoT.Account,
		Endpoint:  c.EndpointAddress,
		Issuer:    issuer,
		Shadows:   a.s.shadowSvc,
		Publish:   a.publishData,
		Retained:  retainedView{a.s.brk.RetainedStore()},
		Now:       a.s.now,
		Audit:     a.s.emitAudit,
		Limiter:   a.s.lim,
		Online: func(thing string) bool {
			conns, _ := a.s.brk.ListConns(broker.ConnQuery{Thing: thing, Limit: 1})
			return len(conns) > 0
		},
		Shadow: func(thing string) (map[string]any, map[string]any, bool) {
			doc, ok := a.s.shadows.Get(thing, "")
			if !ok || doc == nil {
				return nil, nil, false
			}
			return doc.Reported, doc.Desired, true
		},
	}
	h.Mount(mux)
}

// apiError 是管理面的错误响应。
//
// 形状自有且可自由演进（API-002），与 AWS 兼容面的冻结契约无关。
type apiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// Hint 说明怎么改。运维面的错误若只说"失败了"，
	// 调用方只能去翻服务端日志。
	Hint string `json:"hint,omitempty"`
}

// mount 把管理面挂到 mux 上。
//
// 没有令牌时整个 /api/admin/v1 不挂载——而不是挂载一个无鉴权的版本。
// 这个区别很要紧：未挂载时请求得到 404，攻击者无法从响应里确认
// 这里有一个管理面；挂载但拒绝则暴露了它的存在。
func (a *adminServer) mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /readyz", a.handleReadyz)

	a.mountConsole(mux)
	// 兼容面不看管理令牌：它用 SigV4。未启用时不挂载，请求得到 404。
	a.mountAWS(mux)

	if a.token == "" {
		a.s.log.Warn("管理面未挂载：缺少鉴权令牌",
			"如何启用", "设置 "+EnvAdminToken+" 或 listeners.admin.auth_token_file",
			"仍可用", "/healthz 与 /readyz",
			"同时不可用", "/metrics")
		return
	}

	p := adminAPIPrefix
	// 节点
	mux.Handle("GET "+p+"/node", a.auth(a.handleNode))
	// 策略
	mux.Handle("GET "+p+"/policies", a.auth(a.handleListPolicies))
	mux.Handle("GET "+p+"/policies/{name}", a.auth(a.handleGetPolicy))
	mux.Handle("PUT "+p+"/policies/{name}", a.auth(a.handlePutPolicy))
	mux.Handle("DELETE "+p+"/policies/{name}", a.auth(a.handleDeletePolicy))
	// 设备
	mux.Handle("GET "+p+"/things", a.auth(a.handleListThings))
	mux.Handle("GET "+p+"/things/{name}", a.auth(a.handleGetThing))
	mux.Handle("PUT "+p+"/things/{name}", a.auth(a.handlePutThing))
	mux.Handle("DELETE "+p+"/things/{name}", a.auth(a.handleDeleteThing))
	// 证书
	mux.Handle("GET "+p+"/certificates", a.auth(a.handleListCerts))
	mux.Handle("GET "+p+"/certificates/{fp}", a.auth(a.handleGetCert))
	mux.Handle("PUT "+p+"/certificates/{fp}/status", a.auth(a.handleSetCertStatus))
	// 签发。分两个端点而不是一个按字段分支：两者的安全性质不同，
	// 而"少传一个字段就从不传私钥变成传私钥"是最容易出事的隐式行为。
	mux.Handle("POST "+p+"/certificates/issue", a.auth(a.handleIssueCert))
	mux.Handle("POST "+p+"/certificates/sign-csr", a.auth(a.handleSignCSR))
	// 证书与设备的关联。
	//
	// 路径以设备为主语（things/{name}/principals/{fp}），与 AWS 的
	// AttachThingPrincipal 同一视角；反向查询另给一个入口。
	mux.Handle("PUT "+p+"/things/{name}/principals/{fp}",
		a.auth(a.handleAttachPrincipal))
	mux.Handle("DELETE "+p+"/things/{name}/principals/{fp}",
		a.auth(a.handleDetachPrincipal))
	mux.Handle("GET "+p+"/things/{name}/principals",
		a.auth(a.handleListThingPrincipals))
	mux.Handle("GET "+p+"/certificates/{fp}/things",
		a.auth(a.handleListPrincipalThings))
	mux.Handle("DELETE "+p+"/certificates/{fp}", a.auth(a.handleDeleteCert))
	// 连接
	mux.Handle("GET "+p+"/connections", a.auth(a.handleListConns))
	mux.Handle("GET "+p+"/connections/{clientId}", a.auth(a.handleGetConn))
	mux.Handle("DELETE "+p+"/connections/{clientId}", a.auth(a.handleKickConn))
	// 影子。读写都经 shadow.Service，与 MQTT 路径共用同一份语义，
	// 因此经控制台改 desired 同样会触发 delta 与 documents 的发布
	// （PRD SHDW-045）。
	mux.Handle("GET "+p+"/things/{name}/shadow", a.auth(a.handleGetShadow))
	mux.Handle("PUT "+p+"/things/{name}/shadow", a.auth(a.handleUpdateShadow))
	mux.Handle("DELETE "+p+"/things/{name}/shadow", a.auth(a.handleDeleteShadow))
	mux.Handle("GET "+p+"/things/{name}/shadows", a.auth(a.handleListShadows))
	// 策略干跑（★ UI-010）
	mux.Handle("POST "+p+"/authz/simulate", a.auth(a.handleSimulate))
	// 事件流与指标摘要
	mux.Handle("GET "+p+"/events", a.auth(a.handleEvents))
	mux.Handle("GET "+p+"/overview", a.auth(a.handleOverview))
	// 测试客户端。浏览器的 WebSocket 同样不能带 Authorization 头，
	// 令牌走查询参数，与事件流相同。
	// 集群成员（CLU-090 至 CLU-094）。改成员是把元数据写入权发给
	// 一个新节点，所以和其他写操作一样要管理面令牌，并写审计。
	mux.Handle("GET "+p+"/cluster/members", a.auth(a.handleListMembers))
	mux.Handle("POST "+p+"/cluster/members", a.auth(a.handleAddMember))
	mux.Handle("DELETE "+p+"/cluster/members/{id}", a.auth(a.handleRemoveMember))

	mux.Handle("GET "+p+"/mqtt", a.auth(a.handleMQTTConsole))
	mux.Handle("GET "+p+"/capture", a.auth(a.handleCapture))
	mux.Handle("GET "+p+"/things/{name}/timeline", a.auth(a.handleTimeline))
	mux.Handle("GET "+p+"/session", a.auth(a.handleSession))
	mux.Handle("GET "+p+"/users", a.auth(a.handleListUsers))
	mux.Handle("PUT "+p+"/users/{name}", a.auth(a.handlePutUser))
	mux.Handle("DELETE "+p+"/users/{name}", a.auth(a.handleDeleteUser))
	// 兼容面的 access key（API-125 至 API-127）。只给 admin，见 permit。
	mux.Handle("GET "+p+"/awskeys", a.auth(a.handleListAWSKeys))
	mux.Handle("POST "+p+"/awskeys", a.auth(a.handleCreateAWSKey))
	mux.Handle("PATCH "+p+"/awskeys/{id}", a.auth(a.handlePatchAWSKey))
	mux.Handle("DELETE "+p+"/awskeys/{id}", a.auth(a.handleDeleteAWSKey))

	// Prometheus 指标（OBS-020）。
	//
	// 路径是惯例的 /metrics 而不是挂在 p 下面：采集端的默认配置找的
	// 就是这个路径，换一个会让每个用户都得改 scrape 配置。
	//
	// 但鉴权与 p 下的端点一致。API-082 豁免 /healthz 与 /readyz 的
	// 理由是"它们不泄露任何信息"，而指标泄露设备数、证书数、拒绝
	// 原因分布与运行时细节，不满足那个前提。Prometheus 原生支持
	// bearer_token_file，因此要求令牌不影响可用性。
	mux.Handle("GET /metrics", a.auth(a.handleMetrics))

	a.s.log.Info("管理面已挂载", "前缀", p, "指标", "/metrics")
}

// auth 是鉴权中间件。
func (a *adminServer) auth(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := a.identify(r)
		if !ok {
			// 不区分"没带令牌"与"令牌不对"：区分它们等于告诉攻击者
			// 哪一步走对了。
			w.Header().Set("WWW-Authenticate", `Bearer realm="openiotd admin"`)
			writeAPIError(w, http.StatusUnauthorized, "鉴权失败",
				"请在 Authorization 头中携带 Bearer 令牌")
			return
		}
		if !permit(p, r.Method, r.URL.Path) {
			writeAPIError(w, http.StatusForbidden, "当前角色不能做这件事", "")
			return
		}
		h(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

// requestToken 取出请求携带的令牌。
//
// 优先 Authorization 头；其次查询参数。浏览器的 EventSource 和
// WebSocket 都不能设置请求头，事件流和测试客户端因此用查询参数。
//
// 这是一个有意识的权衡，而不是疏忽：查询参数会出现在访问日志里。
// 管理端点默认只绑回环地址，且令牌只在控制台与本机之间传递；
// 要彻底避免它需要一套基于 Cookie 的会话层，那随 RBAC（API-058）
// 一并落地。其余端点走请求头。
func requestToken(r *http.Request) string {
	if t := bearerToken(r.Header.Get("Authorization")); t != "" {
		return t
	}
	return r.URL.Query().Get("token")
}

// ---------- 节点 ----------

type nodeResponse struct {
	NodeID      string `json:"nodeId"`
	ClusterID   string `json:"clusterId"`
	ShardCount  int    `json:"shardCount"`
	Mode        string `json:"mode"`
	Ready       bool   `json:"ready"`
	Connections int64  `json:"connections"`
	Shadows     int    `json:"shadows"`
	Sessions    int    `json:"sessions"`
	Retained    int    `json:"retained"`
	// MetaRevision 是本节点热视图的 revision。
	//
	// 集群运维要靠它看出哪个节点落后了：三个节点的这个数字不一致，
	// 说明有人没追上已提交的日志（OBS-146）。单机下它就是一副本组的
	// 提交序号。
	MetaRevision uint64 `json:"metaRevision"`
}

func (a *adminServer) handleNode(w http.ResponseWriter, _ *http.Request) {
	s := a.s
	writeJSON(w, http.StatusOK, nodeResponse{
		NodeID: s.mf.NodeID, ClusterID: s.mf.ClusterID,
		ShardCount: s.mf.ShardCount, Mode: string(s.cfg.Node.Mode),
		Ready: s.ready.Ready(), Connections: s.brk.Conns(),
		Shadows: s.shadows.CountShadows(), Sessions: s.sessions.Count(),
		Retained:     s.retained.Count(),
		MetaRevision: uint64(s.store.View().Revision()),
	})
}

// ---------- 策略 ----------

func (a *adminServer) handleListPolicies(w http.ResponseWriter, _ *http.Request) {
	names, err := a.s.reg2.ListPolicies()
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": names})
}

func (a *adminServer) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	p, err := a.s.reg2.GetPolicy(r.PathValue("name"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	// 文档原样回传为 JSON 对象而不是 base64 字符串：
	// 控制台要在编辑器里展示它，而运维要能直接 curl 出来看。
	var doc json.RawMessage = p.Document
	writeJSON(w, http.StatusOK, map[string]any{
		"name": p.Name, "document": doc,
		"createdAt": p.CreatedAtUnixMs, "updatedAt": p.UpdatedAtUnixMs,
	})
}

func (a *adminServer) handlePutPolicy(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, maxPolicyBytes)
	if !ok {
		return
	}
	if err := a.s.reg2.PutPolicy(r.Context(), r.PathValue("name"), body); err != nil {
		writeRegistryError(w, err)
		return
	}
	// 策略可能引入新的属性引用，须同步属性槽位
	if err := a.s.reg2.SyncAttrSlots(r.Context()); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "PutPolicy", r.PathValue("name"))
	w.WriteHeader(http.StatusNoContent)
}

func (a *adminServer) handleDeletePolicy(w http.ResponseWriter, r *http.Request) {
	if err := a.s.reg2.DeletePolicy(r.Context(), r.PathValue("name")); err != nil {
		writeRegistryError(w, err)
		return
	}
	if err := a.s.reg2.SyncAttrSlots(r.Context()); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "DeletePolicy", r.PathValue("name"))
	w.WriteHeader(http.StatusNoContent)
}

// ---------- 设备 ----------

func (a *adminServer) handleListThings(w http.ResponseWriter, r *http.Request) {
	page, err := a.s.reg2.ListThings(registry.Query{
		Prefix: r.URL.Query().Get("prefix"),
		Policy: r.URL.Query().Get("policy"),
		After:  r.URL.Query().Get("after"),
		Limit:  intParam(r, "limit"),
	})
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	if page.Things == nil {
		page.Things = []registry.Thing{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *adminServer) handleGetThing(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	t, err := a.s.reg2.GetThing(name)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	certs, err := a.s.reg2.CertificatesOfThing(name)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	if certs == nil {
		certs = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"thing": t, "certificates": certs,
	})
}

func (a *adminServer) handlePutThing(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Attributes map[string]string `json:"attributes"`
		PolicyName string            `json:"policyName"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	err := a.s.reg2.PutThing(r.Context(), registry.Thing{
		Name: r.PathValue("name"), Attributes: in.Attributes,
		PolicyName: in.PolicyName,
	})
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "PutThing", r.PathValue("name"))
	w.WriteHeader(http.StatusNoContent)
}

func (a *adminServer) handleDeleteThing(w http.ResponseWriter, r *http.Request) {
	if err := a.s.reg2.DeleteThing(r.Context(), r.PathValue("name")); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "DeleteThing", r.PathValue("name"))
	w.WriteHeader(http.StatusNoContent)
}

// handleGetShadow 返回影子文档。
//
// 经 shadow.Service 而不是直接序列化存储中的结构体，有两个理由：
//   - 响应形状须与 MQTT 的 /get/accepted 及 REST 的 GetThingShadow
//     完全一致（PRD SHDW-044）。直接序列化内部结构会把 Go 的字段名
//     （Desired、ReportedMeta）暴露成 API 契约，那与两条既有路径不一致，
//     控制台也就无法复用同一套解析。
//   - delta 的计算逻辑只有一份。
func (a *adminServer) handleGetShadow(w http.ResponseWriter, r *http.Request) {
	req := shadow.Request{
		Thing:  r.PathValue("name"),
		Shadow: r.URL.Query().Get("name"),
		Op:     shadow.OpGet,
	}
	out := a.s.shadowSvc.Get(r.Context(), req, nil)
	if out.Err != nil {
		writeAPIError(w, out.Err.Code, out.Err.Message, "")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Response)
}

// handleUpdateShadow 更新影子。
//
// 经 shadow.Service 而不是直接写存储：合并语义、版本递增、delta 与
// documents 的发布都在那里。控制台改 desired 后设备应当收到 delta，
// 这正是 SHDW-045 要求 REST 与 MQTT 共用状态机的原因。
func (a *adminServer) handleUpdateShadow(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, maxShadowBytes)
	if !ok {
		return
	}
	req := shadow.Request{
		Thing:  r.PathValue("name"),
		Shadow: r.URL.Query().Get("name"),
		Op:     shadow.OpUpdate,
	}
	out := a.s.shadowSvc.Update(r.Context(), req, body)
	if out.Err != nil {
		writeAPIError(w, out.Err.Code, out.Err.Message, "")
		return
	}
	// 服务层产出的 delta 与 documents 须真的发出去，否则经控制台
	// 改 desired 的设备永远收不到通知——那会让控制台的"下发配置"
	// 变成一个只改了数据库的假动作。
	a.publishShadow(out.Publishes)
	a.audit(r, "UpdateThingShadow", req.Thing)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Response)
}

func (a *adminServer) handleDeleteShadow(w http.ResponseWriter, r *http.Request) {
	req := shadow.Request{
		Thing:  r.PathValue("name"),
		Shadow: r.URL.Query().Get("name"),
		Op:     shadow.OpDelete,
	}
	out := a.s.shadowSvc.Delete(r.Context(), req, nil)
	if out.Err != nil {
		writeAPIError(w, out.Err.Code, out.Err.Message, "")
		return
	}
	a.publishShadow(out.Publishes)
	a.audit(r, "DeleteThingShadow", req.Thing)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Response)
}

func (a *adminServer) handleListShadows(w http.ResponseWriter, r *http.Request) {
	names, e := a.s.shadowSvc.ListNamedShadows(r.Context(), r.PathValue("name"))
	if e != nil {
		writeAPIError(w, e.Code, e.Message, "")
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": names})
}

// publishShadow 把服务层产出的消息经 broker 发出。
func (a *adminServer) publishShadow(pubs []shadow.Publish) {
	for i := range pubs {
		a.s.brk.PublishInternal(pubs[i].Topic, pubs[i].Payload, pubs[i].QoS1)
	}
}

// ---------- 证书 ----------

func (a *adminServer) handleListCerts(w http.ResponseWriter, r *http.Request) {
	page, err := a.s.reg2.ListCertificates(registry.CertQuery{
		Status: r.URL.Query().Get("status"),
		Thing:  r.URL.Query().Get("thing"),
		After:  r.URL.Query().Get("after"),
		Limit:  intParam(r, "limit"),
	})
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	if page.Certificates == nil {
		page.Certificates = []registry.Certificate{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *adminServer) handleGetCert(w http.ResponseWriter, r *http.Request) {
	c, err := a.s.reg2.GetCertificate(r.PathValue("fp"))
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

// handleSetCertStatus 是 PRD PKI-030 的 HTTP 入口。
//
// 吊销在此处生效，不需要重启节点：控制面写入热视图后
// broker 据变更踢掉在用连接。
func (a *adminServer) handleSetCertStatus(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	fp := r.PathValue("fp")
	if err := a.s.reg2.SetCertificateStatus(r.Context(), fp, in.Status, in.Reason); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "SetCertificateStatus", fp+" -> "+in.Status)
	w.WriteHeader(http.StatusNoContent)
}

func (a *adminServer) handleDeleteCert(w http.ResponseWriter, r *http.Request) {
	fp := r.PathValue("fp")
	if err := a.s.reg2.DeleteCertificate(r.Context(), fp); err != nil {
		writeRegistryError(w, err)
		return
	}
	a.audit(r, "DeleteCertificate", fp)
	w.WriteHeader(http.StatusNoContent)
}

// ---------- 连接 ----------

func (a *adminServer) handleListConns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// 集群下必须跨节点聚合（契约 12）：只看本节点会给出一份
	// 看起来完整而其实少了大半的列表，而用户无从知道自己漏看了什么。
	if a.s.mesh != nil {
		conns, next, partial, err := a.s.fanoutConns(r.Context(),
			map[string]string{"prefix": q.Get("prefix"), "thing": q.Get("thing")},
			q.Get("after"), intParam(r, "limit"))
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, err.Error(), "")
			return
		}
		if conns == nil {
			conns = []broker.ConnSnapshot{}
		}
		body := map[string]any{"connections": conns, "nextAfter": next}
		if len(partial) > 0 {
			// 部分失败要说出来（ARCH-041）。少了哪个节点的数据，
			// 调用方必须能看见——否则它会把一份不完整的列表当成全部。
			body["partial"] = true
			body["unavailableNodes"] = partial
		}
		writeJSON(w, http.StatusOK, body)
		return
	}

	conns, next := a.s.brk.ListConns(broker.ConnQuery{
		ClientIDPrefix: q.Get("prefix"),
		Thing:          q.Get("thing"),
		After:          q.Get("after"),
		Limit:          intParam(r, "limit"),
	})
	if conns == nil {
		conns = []broker.ConnSnapshot{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"connections": conns, "nextAfter": next,
	})
}

// memberResponse 是一个成员的对外形状。
type memberResponse struct {
	ID string `json:"id"`
	// Addr 是复制监听地址。单机下为空。
	Addr string `json:"addr,omitempty"`
	// Self 标出哪一条是本节点。
	Self bool `json:"self,omitempty"`
}

func (a *adminServer) handleListMembers(w http.ResponseWriter, _ *http.Request) {
	members := a.s.ClusterMembers()
	out := make([]memberResponse, 0, len(members))
	for _, m := range members {
		out = append(out, memberResponse{
			ID: string(m.ID), Addr: m.Addr,
			Self: string(m.ID) == a.s.mf.NodeID,
		})
	}
	body := map[string]any{
		"members": out,
		// 多数派所需票数一并给出：运维要据此判断还能掉几个节点。
		"quorum": len(members)/2 + 1,
	}
	// 当前 leader 也给出（API-128）。运维在故障时第一个要知道的就是
	// 「现在谁在接写入」，而原先这张表看不出来——它只说有哪些成员。
	// 选举是库按超时自己跑的，所以这个字段会变，不是配置。
	if a.s.repl != nil {
		body["leader"] = string(a.s.repl.LeaderID())
	} else {
		body["leader"] = a.s.mf.NodeID
	}
	writeJSON(w, http.StatusOK, body)
}

type addMemberRequest struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
	// ReplicaID 是新节点的副本号，必须与它自己配置里的那一个相同
	// （CLU-002 修订）。
	//
	// 必须由调用方给：共识库按 uint64 认成员，而这个号只写在新节点
	// 自己的 `cluster.replication.replicas` 里——leader 无从知道。
	// 让 leader 自己挑（比如取最大值加一）更糟：挑出来的号与新节点
	// 配置里的对不上，而那时新节点会以另一个身份加入，症状是它永远
	// 追不平，日志里看不出是号不一致。
	ReplicaID uint64 `json:"replicaId"`
}

func (a *adminServer) handleAddMember(w http.ResponseWriter, r *http.Request) {
	var in addMemberRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if err := a.s.AddClusterMember(in.ID, in.Addr, in.ReplicaID); err != nil {
		writeMemberError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"added": in.ID})
}

func (a *adminServer) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.s.RemoveClusterMember(id); err != nil {
		writeMemberError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": id})
}

// writeMemberError 把成员变更的失败映射成状态码。
//
// 「不是 leader」「上一次还没提交」「没有多数派」都是 503：请求本身
// 合法，是此刻做不了（CLU-073 的同一条道理）。
func writeMemberError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cluster.ErrAlreadyMember), errors.Is(err, cluster.ErrNotMember):
		writeAPIError(w, http.StatusConflict, err.Error(), "")
	case errors.Is(err, cluster.ErrLastMember):
		writeAPIError(w, http.StatusConflict, err.Error(),
			"移除最后一个成员会让这个组再也选不出 leader")
	case errors.Is(err, cluster.ErrNotLeader), errors.Is(err, cluster.ErrNoQuorum),
		errors.Is(err, cluster.ErrConfInFlight), errors.Is(err, cluster.ErrDraining):
		writeAPIError(w, http.StatusServiceUnavailable, err.Error(),
			"成员变更只能在 leader 上发起，且要等上一次变更提交")
	default:
		writeAPIError(w, http.StatusBadRequest, err.Error(), "")
	}
}

func (a *adminServer) handleGetConn(w http.ResponseWriter, r *http.Request) {
	d, ok := a.s.brk.ConnDetail(r.PathValue("clientId"))
	if !ok {
		writeAPIError(w, http.StatusNotFound, "连接不存在", "该 clientId 当前不在线")
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (a *adminServer) handleKickConn(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientId")
	if !a.s.brk.KickClient(id, reason.AdministrativeAction) {
		writeAPIError(w, http.StatusNotFound, "连接不存在", "该 clientId 当前不在线")
		return
	}
	a.audit(r, "KickConnection", id)
	w.WriteHeader(http.StatusNoContent)
}

// ---------- 共用 ----------

const (
	// maxPolicyBytes 是策略文档的大小上限。
	maxPolicyBytes = 64 << 10
	// maxBodyBytes 是一般请求体的大小上限。
	maxBodyBytes = 1 << 20
	// maxShadowBytes 是影子更新请求的上限。
	//
	// 比影子文档配额（8 KB）留出余量：请求里还有 clientToken 与
	// version 等字段，卡在正好 8 KB 会让一个合法的满额更新被 HTTP 层拒掉，
	// 而真正的配额判定应当由 shadow 服务给出带 413 与明确文案的响应。
	maxShadowBytes = 64 << 10
)

// audit 记录一次已经成功的管理面写操作（PRD API-061）。
//
// 操作者记成 admin，不记令牌本身。令牌进了审计文件就等于把管理面钥匙留下了。
func (a *adminServer) audit(r *http.Request, action, target string) {
	actor := "admin"
	if p, ok := principalFrom(r.Context()); ok && p.Name != "" {
		actor = p.Name
	}
	a.s.emitAudit(actor, r.RemoteAddr, r.Method, r.URL.Path, action, target)
}

func writeAPIError(w http.ResponseWriter, code int, msg, hint string) {
	writeJSON(w, code, apiError{Code: code, Message: msg, Hint: hint})
}

// writeRegistryError 把控制面错误映射到 HTTP 状态码。
func writeRegistryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "记录不存在", "")
	case errors.Is(err, registry.ErrInUse):
		// 409 而不是 400：请求本身合法，是当前状态不允许这个操作。
		// 错误原文带着"谁在引用"，原样回传给调用方。
		writeAPIError(w, http.StatusConflict, err.Error(), "请先解除引用")
	case errors.Is(err, meta.ErrReadOnly), errors.Is(err, cluster.ErrNotLeader),
		errors.Is(err, cluster.ErrNoQuorum), errors.Is(err, cluster.ErrDraining):
		// 503 而不是 400：请求完全合法，是这个节点此刻提交不了日志
		// （丢了多数派、正在排空、或还没选出 leader）。按 API-089，
		// 「状态不允许」与「请求非法」必须用不同的码——400 会让调用方
		// 去改请求，而它该做的是重试或换个节点。
		writeAPIError(w, http.StatusServiceUnavailable, err.Error(),
			"元数据此刻无法写入；读取不受影响，稍后重试")
	default:
		// 其余多为校验失败。不把内部错误原文直接外泄的那部分
		// 由各校验函数自己保证——它们的信息都是面向调用方的。
		writeAPIError(w, http.StatusBadRequest, err.Error(), "")
	}
}

// readBody 读取并限制请求体大小。
func readBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, max)
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			writeAPIError(w, http.StatusRequestEntityTooLarge,
				"请求体过大或读取失败", "")
			return nil, false
		}
	}
	if len(buf) == 0 {
		writeAPIError(w, http.StatusBadRequest, "请求体为空", "")
		return nil, false
	}
	return buf, true
}

// decodeBody 解析 JSON 请求体。
//
// 未知字段一律报错：与配置解析同一条原则（NFR-060a）。
// 一个把 policyName 拼成 policy 的请求若被静默接受，
// 调用方会以为绑定成功了，而设备连不上。
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeAPIError(w, http.StatusBadRequest,
			"请求体不是合法 JSON 或含未知字段: "+err.Error(),
			"未知字段会被拒绝而非忽略，以免拼错的字段名被静默接受")
		return false
	}
	if dec.More() {
		writeAPIError(w, http.StatusBadRequest, "请求体含多余内容", "")
		return false
	}
	return true
}

func intParam(r *http.Request, name string) int {
	v := strings.TrimSpace(r.URL.Query().Get(name))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
