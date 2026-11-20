// Package awsapi 是 AWS IoT 控制面与数据面的兼容入口（PRD API-001）。
//
// 路径挂在 /api/aws 下。boto3 的 endpoint_url 要写成这个前缀，
// 例如 http://127.0.0.1:9090/api/aws。控制面签 iot，数据面签 iotdata。
// 响应形状按 AWS 的冻结契约，不随控制台的需要改。
package awsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// Prefix 是兼容面的路径前缀（PRD API-001）。
const Prefix = "/api/aws"

// 单页上限与 AWS IoT ListThings 的 maxResults 一致。
const maxPage = 250

// Handler 处理已验签的控制面请求。
type Handler struct {
	Things  *registry.Registry
	Checker sigv4.Checker
	// Partition、Region、Account 组成 thing ARN，取自本节点的 IoT 命名空间。
	// 它和 SigV4 的区域不是一回事：策略文档里的区域可以是 local。
	Partition string
	Region    string
	Account   string
	// Endpoint 是 DescribeEndpoint 返回的地址（API-026）。
	Endpoint string
	// Issuer 签发设备证书。为空表示没有内嵌 CA。
	Issuer CertIssuer
	// Shadows 是 MQTT 与 REST 共用的影子状态机。为空时影子路由返回 503。
	Shadows *shadow.Service
	// Publish 把数据面的 Publish 送进 broker。为空时该路由返回 503。
	Publish func(ctx context.Context, topic string, payload []byte, qos uint8, retain bool) error
	// Retained 读取保留消息。为空时相关路由返回 503。
	Retained RetainedView
	// Now 是 Unix 毫秒。列举命名影子时写入响应里的 timestamp。
	Now func() int64
	// Role 按 access key id 取这把钥匙的权限级别（AUTH-081）。
	//
	// 为空时一切请求都被拒。漏接这根线必须失败，而不是退化成
	// 「不判权限」——那正是这个字段要堵的缺口，静默放行等于白加。
	Role func(accessKeyID string) (Role, bool)
	// Audit 记录一次已经成功的写操作。为空时不记。
	//
	// 参数是操作者、来源地址、方法、路径、动作、目标。
	// 目标只能是名字或指纹。
	Audit func(actor, remote, method, path, action, target string)
	// Limiter 判定证书注册速率。为空时不限速。
	Limiter quota.Limiter
	// Online 报告一台设备当前是否有连接。检索在连接索引打开时使用。
	Online func(thing string) bool
	// Shadow 读取经典影子的 reported 与 desired。影子索引打开时使用。
	Shadow func(thing string) (reported, desired map[string]any, ok bool)
}

// Mount 注册本切片实现的 action。
//
// 每条路由都要写出它要求的最低角色（AUTH-081）。**不能按 HTTP 方法推**，
// 两个方向都会错：
//
//   - POST 不等于写。ListAttachedPolicies、ListTargetsForPolicy、
//     SearchIndex 在 AWS 的 REST 映射里都是 POST，按方法判会把只读的
//     调用方挡在外面，而他们会以为是自己签名签错了。
//   - GET 不等于读。`/role-aliases/{n}/credentials` 的语义是发凭证，
//     按方法判就把它放给了 reader。
//
// 所以是一条一条定的，级别按**语义**而不是按当前实现：上面那条今天
// 只回 503（本节点不签发云临时凭证），级别照样给 admin——哪天真签了，
// 没人会回头想起来还要抬一级。
func (h Handler) Mount(mux *http.ServeMux) {
	p := Prefix
	mux.HandleFunc("POST "+p+"/things/{thingName}", h.authed(RoleOperator, h.createThing))
	mux.HandleFunc("GET "+p+"/things/{thingName}", h.authed(RoleReader, h.describeThing))
	mux.HandleFunc("PATCH "+p+"/things/{thingName}", h.authed(RoleOperator, h.updateThing))
	mux.HandleFunc("DELETE "+p+"/things/{thingName}", h.authed(RoleOperator, h.deleteThing))
	mux.HandleFunc("GET "+p+"/things", h.authed(RoleReader, h.listThings))
	mux.HandleFunc("GET "+p+"/endpoint", h.authed(RoleReader, h.describeEndpoint))

	mux.HandleFunc("POST "+p+"/policies/{policyName}", h.authed(RoleOperator, h.createPolicy))
	mux.HandleFunc("GET "+p+"/policies/{policyName}", h.authed(RoleReader, h.getPolicy))
	mux.HandleFunc("DELETE "+p+"/policies/{policyName}", h.authed(RoleOperator, h.deletePolicy))
	mux.HandleFunc("GET "+p+"/policies", h.authed(RoleReader, h.listPolicies))
	mux.HandleFunc("POST "+p+"/policies/{policyName}/version", h.authed(RoleOperator, h.createPolicyVersion))
	mux.HandleFunc("GET "+p+"/policies/{policyName}/version/{policyVersionId}", h.authed(RoleReader, h.getPolicyVersion))
	mux.HandleFunc("GET "+p+"/policies/{policyName}/version", h.authed(RoleReader, h.listPolicyVersions))
	mux.HandleFunc("DELETE "+p+"/policies/{policyName}/version/{policyVersionId}", h.authed(RoleOperator, h.deletePolicyVersion))
	mux.HandleFunc("PATCH "+p+"/policies/{policyName}/version/{policyVersionId}", h.authed(RoleOperator, h.setDefaultPolicyVersion))

	mux.HandleFunc("PUT "+p+"/target-policies/{policyName}", h.authed(RoleOperator, h.attachPolicy))
	mux.HandleFunc("POST "+p+"/target-policies/{policyName}", h.authed(RoleOperator, h.detachPolicy))
	mux.HandleFunc("POST "+p+"/attached-policies/{target}", h.authed(RoleReader, h.listAttachedPolicies))
	mux.HandleFunc("POST "+p+"/policy-targets/{policyName}", h.authed(RoleReader, h.listTargetsForPolicy))

	mux.HandleFunc("PUT "+p+"/things/{thingName}/principals", h.authed(RoleOperator, h.attachThingPrincipal))
	mux.HandleFunc("DELETE "+p+"/things/{thingName}/principals", h.authed(RoleOperator, h.detachThingPrincipal))
	mux.HandleFunc("GET "+p+"/things/{thingName}/principals", h.authed(RoleReader, h.listThingPrincipals))
	mux.HandleFunc("GET "+p+"/principals/things", h.authed(RoleReader, h.listPrincipalThings))

	mux.HandleFunc("POST "+p+"/keys-and-certificate", h.authed(RoleOperator, h.createKeys))
	mux.HandleFunc("POST "+p+"/certificates", h.authed(RoleOperator, h.createFromCSR))
	mux.HandleFunc("GET "+p+"/certificates/{certificateId}", h.authed(RoleReader, h.describeCertificate))
	mux.HandleFunc("PUT "+p+"/certificates/{certificateId}", h.authed(RoleOperator, h.updateCertificate))
	mux.HandleFunc("DELETE "+p+"/certificates/{certificateId}", h.authed(RoleOperator, h.deleteCertificate))
	mux.HandleFunc("GET "+p+"/certificates", h.authed(RoleReader, h.listCertificates))

	mux.HandleFunc("POST "+p+"/topics/{topic...}", h.data(RoleOperator, h.publish))
	mux.HandleFunc("GET "+p+"/things/{thingName}/shadow", h.data(RoleReader, h.getShadow))
	mux.HandleFunc("POST "+p+"/things/{thingName}/shadow", h.data(RoleOperator, h.updateShadow))
	mux.HandleFunc("DELETE "+p+"/things/{thingName}/shadow", h.data(RoleOperator, h.deleteShadow))
	mux.HandleFunc("GET "+p+"/api/things/shadow/ListNamedShadowsForThing/{thingName}", h.data(RoleReader, h.listShadows))
	mux.HandleFunc("GET "+p+"/retainedMessage/{topic...}", h.data(RoleReader, h.getRetained))
	mux.HandleFunc("GET "+p+"/retainedMessage", h.data(RoleReader, h.listRetained))
	h.mountFleet(mux)
}

// authed 是控制面（服务名 iot）的门。need 是这条路由要求的最低级别。
//
// 级别写在挂载处而不是另一张按路径查的表：表会和路由漂移，而漂移的那条
// 路由不会报错，只会悄悄变成谁都能走。写成参数之后，新增路由不填级别
// 根本编译不过。
func (h Handler) authed(need Role, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return h.gate(sigv4.ServiceIoT, need, next)
}

// data 是数据面（服务名 iotdata）的同一道门。
func (h Handler) data(need Role, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return h.gate(sigv4.ServiceIoTData, need, next)
}

// roleOf 取这把钥匙的级别。没装配 Role 时取不到，于是一律拒绝。
func (h Handler) roleOf(id string) (Role, bool) {
	if h.Role == nil {
		return "", false
	}
	return h.Role(id)
}

func (h Handler) gate(service string, need Role, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			writeAWSError(w, "InvalidRequestException", "请求体无法读取")
			return
		}
		hdr := r.Header.Clone()
		if r.Host != "" {
			hdr.Set("Host", r.Host)
		}
		scope, err := h.Checker.VerifyRaw(r.Method, r.URL.Path, r.URL.RawPath, r.URL.RawQuery, hdr, body)
		// 控制面要 iot，数据面要 iotdata。签错服务名与签名不对是同一种拒绝。
		if err != nil || scope.Service != service || scope.AccessKeyID == "" {
			writeAWSError(w, "UnauthorizedException", "请求未通过签名校验")
			return
		}
		// 签名对了但权限不够，与签名不对是两件事，必须给两种错误：
		// 前者要改的是这把钥匙的级别，后者要改的是密钥或时钟。合成一种
		// 之后，调用方只能靠猜。密钥是否存在仍然不泄露——到这一步
		// 签名已经验过，告诉持有者自己是 reader 不多透露任何东西。
		role, ok := h.roleOf(scope.AccessKeyID)
		if !ok || !allows(role, need) {
			writeAWSError(w, "AccessDeniedException", "这把 access key 的权限不足，本操作至少需要 "+string(need))
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next(w, r.WithContext(withAccessKey(r.Context(), scope.AccessKeyID)))
	}
}

type accessKeyCtx struct{}

func withAccessKey(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, accessKeyCtx{}, id)
}

func accessKeyActor(ctx context.Context) string {
	id, _ := ctx.Value(accessKeyCtx{}).(string)
	if id == "" {
		return "sigv4"
	}
	return "ak:" + id
}

// note 在写入已经成功之后记一笔审计。
//
// 失败的请求不记：审计里出现的动作应当是已经生效的。
func (h Handler) note(r *http.Request, action, target string) {
	if h.Audit == nil {
		return
	}
	h.Audit(accessKeyActor(r.Context()), r.RemoteAddr, r.Method, r.URL.Path, action, target)
}

type attrPayload struct {
	Attributes map[string]string `json:"attributes"`
	Merge      bool              `json:"merge"`
}

type createBody struct {
	AttributePayload *attrPayload `json:"attributePayload"`
	ThingTypeName    string       `json:"thingTypeName"`
	BillingGroupName string       `json:"billingGroupName"`
}

type updateBody struct {
	AttributePayload *attrPayload `json:"attributePayload"`
	ExpectedVersion  *int64       `json:"expectedVersion"`
	ThingTypeName    string       `json:"thingTypeName"`
	RemoveThingType  bool         `json:"removeThingType"`
	BillingGroupName string       `json:"billingGroupName"`
}

type thingJSON struct {
	ThingName  string            `json:"thingName"`
	ThingArn   string            `json:"thingArn"`
	Attributes map[string]string `json:"attributes"`
	Version    int64             `json:"version"`
}

func (h Handler) createThing(w http.ResponseWriter, r *http.Request) {
	var in createBody
	if !decodeBody(w, r, &in) {
		return
	}
	if msg := rejectUnsupported(&in.ThingTypeName, &in.BillingGroupName); msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	name := r.PathValue("thingName")
	var attrs map[string]string
	if in.AttributePayload != nil {
		attrs = in.AttributePayload.Attributes
	}
	err := h.Things.CreateThing(r.Context(), registry.Thing{Name: name, Attributes: attrs})
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "CreateThing", name)
	th, err := h.Things.GetThing(name)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.thingJSON(th))
}

func (h Handler) describeThing(w http.ResponseWriter, r *http.Request) {
	th, err := h.Things.GetThing(r.PathValue("thingName"))
	if err != nil {
		writeRegistry(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.thingJSON(th))
}

func (h Handler) updateThing(w http.ResponseWriter, r *http.Request) {
	var in updateBody
	if !decodeBody(w, r, &in) {
		return
	}
	if msg := rejectUnsupported(&in.ThingTypeName, &in.BillingGroupName); msg != "" {
		writeAWSError(w, "InvalidRequestException", msg)
		return
	}
	if in.RemoveThingType {
		writeAWSError(w, "InvalidRequestException", "不支持 Thing Type")
		return
	}
	var expected int64
	if in.ExpectedVersion != nil {
		if *in.ExpectedVersion < 1 {
			writeAWSError(w, "InvalidRequestException", "expectedVersion 须为正整数")
			return
		}
		expected = *in.ExpectedVersion
	}
	var attrs map[string]string
	replace := false
	if in.AttributePayload != nil {
		attrs = in.AttributePayload.Attributes
		replace = !in.AttributePayload.Merge
	}
	err := h.Things.UpdateThing(r.Context(), r.PathValue("thingName"), attrs, replace, expected)
	if err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "UpdateThing", r.PathValue("thingName"))
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) deleteThing(w http.ResponseWriter, r *http.Request) {
	var expected int64
	if r.URL.Query().Has("expectedVersion") {
		n, err := strconv.ParseInt(r.URL.Query().Get("expectedVersion"), 10, 64)
		if err != nil || n < 1 {
			writeAWSError(w, "InvalidRequestException", "expectedVersion 须为正整数")
			return
		}
		expected = n
	}
	name := r.PathValue("thingName")
	if expected != 0 {
		th, err := h.Things.GetThing(name)
		if err != nil {
			writeRegistry(w, err)
			return
		}
		if th.Version != expected {
			writeAWSError(w, "VersionConflictException", "版本不一致")
			return
		}
	}
	if err := h.Things.DeleteThing(r.Context(), name); err != nil {
		writeRegistry(w, err)
		return
	}
	h.note(r, "DeleteThing", name)
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h Handler) listThings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("thingTypeName") != "" {
		writeAWSError(w, "InvalidRequestException", "不支持 thingTypeName")
		return
	}
	hasName, hasValue := q.Has("attributeName"), q.Has("attributeValue")
	if hasName != hasValue {
		writeAWSError(w, "InvalidRequestException", "attributeName 与 attributeValue 须同时给出")
		return
	}
	if q.Has("usePrefixAttributeValue") && !hasName {
		writeAWSError(w, "InvalidRequestException", "usePrefixAttributeValue 须与属性筛选一起使用")
		return
	}
	prefix := false
	if q.Has("usePrefixAttributeValue") {
		switch q.Get("usePrefixAttributeValue") {
		case "true":
			prefix = true
		case "false":
		default:
			writeAWSError(w, "InvalidRequestException", "usePrefixAttributeValue 只能是 true 或 false")
			return
		}
	}
	limit := registry.DefaultPageLimit
	if q.Has("maxResults") {
		n, err := strconv.Atoi(q.Get("maxResults"))
		if err != nil || n < 1 || n > maxPage {
			writeAWSError(w, "InvalidRequestException", "maxResults 须在 1 到 250 之间")
			return
		}
		limit = n
	}
	page, err := h.Things.ListThings(registry.Query{
		After:      q.Get("nextToken"),
		Limit:      limit,
		AttrName:   q.Get("attributeName"),
		AttrValue:  q.Get("attributeValue"),
		AttrPrefix: prefix,
	})
	if err != nil {
		writeRegistry(w, err)
		return
	}
	out := make([]thingJSON, 0, len(page.Things))
	for i := range page.Things {
		out = append(out, h.thingJSON(&page.Things[i]))
	}
	resp := struct {
		Things    []thingJSON `json:"things"`
		NextToken string      `json:"nextToken,omitempty"`
	}{Things: out, NextToken: page.NextAfter}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) describeEndpoint(w http.ResponseWriter, r *http.Request) {
	// endpointType 接受，但各种类型都返回本节点配置的地址（API-026）。
	if h.Endpoint == "" {
		writeAWSError(w, "InternalFailureException", "未配置端点地址")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"endpointAddress": h.Endpoint})
}

func (h Handler) thingJSON(t *registry.Thing) thingJSON {
	attrs := t.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}
	return thingJSON{
		ThingName:  t.Name,
		ThingArn:   h.arn("thing", t.Name),
		Attributes: attrs,
		Version:    t.Version,
	}
}

func rejectUnsupported(thingType, billing *string) string {
	if thingType != nil && *thingType != "" {
		return "不支持 Thing Type"
	}
	if billing != nil && *billing != "" {
		return "不支持 Billing Group"
	}
	return ""
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		writeAWSError(w, "InvalidRequestException", "请求体无法解析")
		return false
	}
	return true
}

func writeRegistry(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		writeAWSError(w, "ResourceNotFoundException", "资源不存在")
	case errors.Is(err, registry.ErrAlreadyExists):
		writeAWSError(w, "ResourceAlreadyExistsException", "资源已存在")
	case errors.Is(err, registry.ErrVersionConflict):
		writeAWSError(w, "VersionConflictException", "版本不一致")
	case errors.Is(err, registry.ErrInUse):
		writeAWSError(w, "InvalidRequestException", err.Error())
	case errors.Is(err, registry.ErrLimitExceeded):
		writeAWSError(w, "LimitExceededException", err.Error())
	case errors.Is(err, registry.ErrBadPolicy), errors.Is(err, registry.ErrDefaultVersion):
		writeAWSError(w, "InvalidRequestException", err.Error())
	case errors.Is(err, meta.ErrReadOnly), errors.Is(err, cluster.ErrNotLeader),
		errors.Is(err, cluster.ErrNoQuorum), errors.Is(err, cluster.ErrDraining):
		// 请求合法，是这个节点此刻提交不了日志。SDK 按状态码决定是否
		// 重试（API-012），而 InvalidRequestException 是 400——它会让
		// SDK 放弃并把错误抛给调用方，而正确的反应是重试。
		writeAWSError(w, "ServiceUnavailableException", err.Error())
	default:
		msg := err.Error()
		if strings.Contains(msg, "失败") || strings.Contains(msg, "损坏") {
			writeAWSError(w, "InternalFailureException", "内部错误")
			return
		}
		writeAWSError(w, "InvalidRequestException", msg)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// awsStatus 是 API-011 列出的异常与 HTTP 状态码。
// SDK 按状态码决定是否重试，按 x-amzn-ErrorType 决定异常类型（API-012）。
var awsStatus = map[string]int{
	"ResourceNotFoundException":      http.StatusNotFound,
	"ResourceAlreadyExistsException": http.StatusConflict,
	"InvalidRequestException":        http.StatusBadRequest,
	"ThrottlingException":            http.StatusTooManyRequests,
	"UnauthorizedException":          http.StatusUnauthorized,
	"AccessDeniedException":          http.StatusForbidden,
	"ServiceUnavailableException":    http.StatusServiceUnavailable,
	"VersionConflictException":       http.StatusConflict,
	"LimitExceededException":         http.StatusGone,
	"InternalFailureException":       http.StatusInternalServerError,
}

func writeAWSError(w http.ResponseWriter, code, msg string) {
	status, ok := awsStatus[code]
	if !ok {
		status = http.StatusInternalServerError
		code = "InternalFailureException"
		msg = "内部错误"
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header()["x-amzn-ErrorType"] = []string{code}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Message string `json:"message"`
		Type    string `json:"__type"`
	}{Message: msg, Type: code})
}
