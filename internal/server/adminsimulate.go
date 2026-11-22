package server

import (
	"fmt"
	"net/http"

	"github.com/itmisx/open-iot-core/internal/authz"
)

// 策略干跑（PRD API-053、UI-010）。
//
// 这是本项目相对 AWS 的头号差异化能力：AWS 的策略调试几乎不可能，
// 因为变量求值的中间结果不可见。此处把 authz.Trace 完整回传——
// 它在判定时捕获，事后无法重建（AUTH-035）。

// simulateRequest 是干跑请求。
type simulateRequest struct {
	// 主体可用三种方式指定，优先级从上到下。
	//
	// 按证书指纹指定最接近真实情形：策略、设备绑定、属性都由它推出，
	// 不需要调用方自己拼。
	CertFingerprint string `json:"certFingerprint,omitempty"`
	// Thing 按设备名指定，取该设备绑定的策略。
	Thing string `json:"thing,omitempty"`
	// PolicyNames 直接指定策略集，用于"这份策略能不能放行"这类假设验证。
	PolicyNames []string `json:"policyNames,omitempty"`

	// ClientID 是 MQTT 的客户端标识符。为空时取 Thing。
	ClientID string `json:"clientId,omitempty"`
	// Action 取 iot:Connect、iot:Publish、iot:Subscribe、iot:Receive、
	// iot:RetainPublish。
	Action string `json:"action"`
	// Resource 是被判定的对象：Connect 为 clientId，Subscribe 为主题过滤器，
	// 其余为主题名。
	Resource string `json:"resource"`

	// CertSubjectCN 等用于覆盖证书类策略变量，便于假设验证。
	CertSubjectCN    string `json:"certSubjectCommonName,omitempty"`
	CertIssuerCN     string `json:"certIssuerCommonName,omitempty"`
	CertSerialNumber string `json:"certSerialNumber,omitempty"`
}

// simulateResponse 是干跑结果。
type simulateResponse struct {
	// Allowed 是最终判定。
	Allowed bool `json:"allowed"`
	// Decision 取 Allow、ExplicitDeny、ImplicitDeny。
	//
	// 三者的区别对排障很要紧：ExplicitDeny 说明有一条 Deny 语句命中，
	// 加放行语句无用；ImplicitDeny 说明没有任何语句匹配，加一条就行。
	Decision string `json:"decision"`
	// PolicyName 与 StatementIndex 指出命中的语句。
	PolicyName     string `json:"policyName,omitempty"`
	StatementIndex int    `json:"statementIndex"`
	// MatchedResource 是命中的资源模式原文。
	MatchedResource string `json:"matchedResource,omitempty"`
	// Variables 是每个策略变量的求值结果。
	//
	// 这是整个端点的核心价值：Resolved 为假的变量说明它无法求值，
	// 该条资源因此不匹配——而那是策略写对了却仍被拒的最常见原因。
	Variables []simulateVar `json:"variables"`
	// VariablesTruncated 表示变量数超过记录上限。
	VariablesTruncated bool `json:"variablesTruncated,omitempty"`
	// EvaluatedPolicies 是参与本次判定的策略名。
	EvaluatedPolicies []string `json:"evaluatedPolicies"`
	// Advice 是让判定变为放行所需的修改建议。
	Advice []string `json:"advice,omitempty"`
}

type simulateVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	// Resolved 为假表示无法求值，该条资源因此不匹配。
	Resolved bool `json:"resolved"`
}

func (a *adminServer) handleSimulate(w http.ResponseWriter, r *http.Request) {
	var in simulateRequest
	if !decodeBody(w, r, &in) {
		return
	}
	act, ok := authz.ParseAction(in.Action)
	if !ok {
		writeAPIError(w, http.StatusBadRequest,
			fmt.Sprintf("未知的动作 %q", in.Action),
			"可取 iot:Connect、iot:Publish、iot:Subscribe、iot:Receive、iot:RetainPublish")
		return
	}
	if in.Resource == "" {
		writeAPIError(w, http.StatusBadRequest, "须给出 resource",
			"Connect 填 clientId，Subscribe 填主题过滤器，其余填主题名")
		return
	}

	sets, names, ctx, err := a.resolveSubject(&in)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error(), "")
		return
	}

	var trace authz.Trace
	decision := authz.Eval(sets, ctx, authz.Request{Action: act, Resource: in.Resource},
		&trace)

	resp := simulateResponse{
		Allowed:            decision == authz.Allow,
		Decision:           decision.String(),
		PolicyName:         trace.PolicyName,
		StatementIndex:     trace.StatementIndex,
		MatchedResource:    trace.MatchedResource,
		VariablesTruncated: trace.VarsTruncated,
		EvaluatedPolicies:  names,
		Variables:          []simulateVar{},
	}
	for _, v := range trace.VarList() {
		resp.Variables = append(resp.Variables, simulateVar{
			Name: v.Name, Value: v.Value, Resolved: v.Resolved,
		})
	}
	resp.Advice = simulateAdvice(decision, act, &in, &trace, sets)
	writeJSON(w, http.StatusOK, resp)
}

// resolveSubject 依请求推出参与判定的策略集与求值上下文。
func (a *adminServer) resolveSubject(in *simulateRequest) (
	[]*authz.CompiledSet, []string, *authz.Context, error,
) {
	clientID := in.ClientID
	thing := in.Thing
	var policyNames []string
	cert := authz.CertContext{
		SerialNumber: in.CertSerialNumber,
		Subject:      authz.DN{CommonName: in.CertSubjectCN},
		Issuer:       authz.DN{CommonName: in.CertIssuerCN},
	}

	switch {
	case in.CertFingerprint != "":
		// 按证书指纹：最接近真实情形，策略与设备绑定都由它推出
		c, err := a.s.reg2.GetCertificate(in.CertFingerprint)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("证书 %s 未登记: %w",
				shortFP(in.CertFingerprint), err)
		}
		policyNames = []string{c.PolicyName}
		if thing == "" {
			thing = c.Thing
		}
		// 冷层里的 Subject/Issuer 是 DN 的可读串，取其通用名部分。
		// 干跑的目的是复现真实判定，因此优先用登记里的真值，
		// 请求中的覆盖值只在登记里没有时才用。
		if cn := commonNameOf(c.Subject); cn != "" {
			cert.Subject.CommonName = cn
		}
		if cn := commonNameOf(c.Issuer); cn != "" {
			cert.Issuer.CommonName = cn
		}
	case len(in.PolicyNames) > 0:
		policyNames = in.PolicyNames
	case thing != "":
		t, err := a.s.reg2.GetThing(thing)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("设备 %s 不存在: %w", thing, err)
		}
		if t.PolicyName == "" {
			return nil, nil, nil, fmt.Errorf("设备 %s 未绑定策略", thing)
		}
		policyNames = []string{t.PolicyName}
	default:
		return nil, nil, nil, fmt.Errorf(
			"须给出 certFingerprint、thing 或 policyNames 之一")
	}

	if clientID == "" {
		clientID = thing
	}

	sets := make([]*authz.CompiledSet, 0, len(policyNames))
	used := make([]string, 0, len(policyNames))
	view := a.s.store.View()
	for _, name := range policyNames {
		if name == "" {
			continue
		}
		id, ok := a.s.reg2.PolicySetID(name)
		if !ok {
			return nil, nil, nil, fmt.Errorf("策略 %q 不存在", name)
		}
		ps, ok := view.PolicySet(id)
		if !ok {
			return nil, nil, nil, fmt.Errorf("策略 %q 未载入热视图", name)
		}
		cs, ok := ps.Compiled.(*authz.CompiledSet)
		if !ok {
			return nil, nil, nil, fmt.Errorf("策略 %q 的编译结果不可用", name)
		}
		sets = append(sets, cs)
		used = append(used, name)
	}
	if len(sets) == 0 {
		return nil, nil, nil, fmt.Errorf("没有可用于判定的策略")
	}

	ctx := &authz.Context{
		ClientID: clientID, Thing: thing,
		// ThingAttached 为真：干跑的前提是"这台设备用它绑定的证书连进来"。
		// 置假会让全部 Thing 类变量解析失败，那不是用户想问的问题。
		ThingAttached: thing != "",
		Cert:          &cert,
	}
	if thing != "" {
		ctx.ThingAttrs = view
	}
	return sets, used, ctx, nil
}

// simulateAdvice 给出让判定变为放行所需的修改建议。
//
// 这是策略调试器区别于"打印判定结果"的地方：告诉用户下一步做什么，
// 而不是让他对着一个 Deny 自己猜。
func simulateAdvice(d authz.Decision, act authz.Action, in *simulateRequest,
	tr *authz.Trace, sets []*authz.CompiledSet,
) []string {
	if d == authz.Allow {
		return nil
	}
	var out []string

	// 无法求值的变量是"策略写对了却仍被拒"的最常见原因
	for _, v := range tr.VarList() {
		if v.Resolved {
			continue
		}
		switch {
		case in.Thing == "" && isThingVar(v.Name):
			out = append(out, fmt.Sprintf(
				"变量 ${%s} 无法求值：本次干跑未指定设备。"+
					"请填入 thing，或改用 certFingerprint 让它从证书绑定推出", v.Name))
		case isAttrVar(v.Name):
			out = append(out, fmt.Sprintf(
				"变量 ${%s} 无法求值：该属性不在热视图中。"+
					"热视图只物化策略引用过的属性，请确认设备上确实设了这个属性", v.Name))
		default:
			out = append(out, fmt.Sprintf(
				"变量 ${%s} 无法求值，引用它的资源模式因此不匹配", v.Name))
		}
	}

	if d == authz.ExplicitDeny {
		out = append(out, fmt.Sprintf(
			"命中显式 Deny：策略 %q 的第 %d 条语句。"+
				"显式拒绝优先于任何放行，加 Allow 语句无效——须改或删这条 Deny",
			tr.PolicyName, tr.StatementIndex))
		return out
	}

	// 隐式拒绝：没有任何语句匹配
	// ResourceType 不带尾部斜杠，拼 ARN 时须自己补上，
	// 否则建议里会出现 "topictelemetry/x" 这种拼不出来的东西
	resType := act.ResourceType()
	out = append(out, fmt.Sprintf(
		"没有任何语句匹配。需要一条 Allow，其 Action 含 %s，"+
			"且 Resource 匹配 arn:aws:iot:<区域>:<账号>:%s/%s",
		act.String(), resType, in.Resource))
	if act == authz.ActSubscribe {
		out = append(out, "注意 iot:Subscribe 判定的是主题过滤器本身，"+
			"资源类型为 topicfilter/；收消息另需 iot:Receive 对 topic/ 放行")
	}
	if len(sets) == 1 {
		out = append(out, "本次只有一份策略参与判定；"+
			"真实连接可能还有其他来源的策略，可用 policyNames 指定多份一并验证")
	}
	return out
}

func isThingVar(name string) bool {
	return len(name) > 15 && name[:15] == "iot:Connection."
}

func isAttrVar(name string) bool {
	const marker = "Attributes["
	for i := 0; i+len(marker) <= len(name); i++ {
		if name[i:i+len(marker)] == marker {
			return true
		}
	}
	return false
}

// commonNameOf 从 DN 可读串中取出通用名。
//
// 冷层把证书主题存成 "CN=dev-1,O=Acme" 这样的串（便于展示与审计），
// 而策略变量要的是单个字段。这里只取 CN——它是唯一会被
// ${iot:Certificate.Subject.CommonName} 引用的字段。
func commonNameOf(dn string) string {
	const prefix = "CN="
	for i := 0; i < len(dn); i++ {
		if i == 0 || dn[i-1] == ',' {
			if len(dn)-i >= len(prefix) && dn[i:i+len(prefix)] == prefix {
				rest := dn[i+len(prefix):]
				for j := 0; j < len(rest); j++ {
					if rest[j] == ',' {
						return rest[:j]
					}
				}
				return rest
			}
		}
	}
	return ""
}
