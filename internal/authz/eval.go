package authz

import "fmt"

// Decision 是授权判定结果。
type Decision uint8

const (
	// Allow 放行。
	Allow Decision = iota
	// ExplicitDeny 是策略中的显式拒绝。它优先于任何放行。
	ExplicitDeny
	// ImplicitDeny 是无匹配语句导致的拒绝，也是默认结果。
	ImplicitDeny
)

var decisionNames = [...]string{"Allow", "ExplicitDeny", "ImplicitDeny"}

func (d Decision) String() string {
	if int(d) >= len(decisionNames) {
		return "Unknown"
	}
	return decisionNames[d]
}

// Allowed 报告判定是否放行。
func (d Decision) Allowed() bool { return d == Allow }

// Request 是一次授权判定请求。
type Request struct {
	Action Action
	// Resource 是被判定的对象：Connect 为 clientId，
	// Publish 与 Receive 与 RetainPublish 为主题名，Subscribe 为主题过滤器。
	Resource string
}

// maxTraceVars 是判定轨迹记录的变量上限。
const maxTraceVars = 8

// TraceVar 是一条变量求值结果。
type TraceVar struct {
	Name  string
	Value string
	// Resolved 为 false 表示该变量无法求值，该条资源因此不匹配。
	Resolved bool
}

// Trace 是一次判定的完整过程记录。
//
// 它是控制台策略调试器的数据来源。变量求值的中间结果如果不在判定时捕获
// 就无法事后重建——这是 PRD AUTH-035 要求捕获它的原因，也是
// 「策略调试几乎不可能」这个 AWS 头号痛点的解法。
//
// Trace 是值类型且变量用定长数组，因此可以在栈上使用，
// 热路径传 nil 即完全不付代价。
type Trace struct {
	Decision Decision
	// PolicyName 与 StatementIndex 指出命中的语句。
	// Decision 为 ImplicitDeny 时它们无意义，StatementIndex 为 -1。
	PolicyName     string
	StatementIndex int
	// MatchedResource 是命中的资源模式原文。
	MatchedResource string
	// Vars 是全部被求值过的变量，NVars 为有效条数。
	Vars  [maxTraceVars]TraceVar
	NVars uint8
	// VarsTruncated 表示变量数超过上限被截断。
	VarsTruncated bool
}

// VarList 返回已记录的变量。
func (t *Trace) VarList() []TraceVar { return t.Vars[:t.NVars] }

func (t *Trace) addVar(name, value string, resolved bool) {
	// 同一变量在多条资源上重复出现时只记一次，保持轨迹可读。
	for i := 0; i < int(t.NVars); i++ {
		if t.Vars[i].Name == name {
			return
		}
	}
	if int(t.NVars) >= maxTraceVars {
		t.VarsTruncated = true
		return
	}
	t.Vars[t.NVars] = TraceVar{Name: name, Value: value, Resolved: resolved}
	t.NVars++
}

// Reset 清空轨迹以便复用。
func (t *Trace) Reset() {
	*t = Trace{StatementIndex: -1}
}

// Eval 对一组策略集求值。
//
// 判定规则（PRD AUTH-021）：
//  1. 任何一条匹配的 Deny 语句立即拒绝，优先于全部放行；
//  2. 否则有匹配的 Allow 语句则放行；
//  3. 否则隐式拒绝。
//
// 因此必须扫完全部语句才能确认放行——不能在遇到第一条 Allow 时就返回，
// 否则后面的 Deny 会被忽略，这是权限放大。
//
// trace 为 nil 时不记录过程，用于热路径；非 nil 时填充完整轨迹，
// 用于拒绝事件与策略调试器。
func Eval(sets []*CompiledSet, ctx *Context, req Request, trace *Trace) Decision {
	if trace != nil {
		trace.Reset()
	}
	resType := req.Action.ResourceType()
	if resType == "" {
		return setTrace(trace, ImplicitDeny, "", -1, "")
	}
	mask := req.Action.mask()

	var (
		allowed     bool
		allowPolicy string
		allowIdx    = -1
		allowRes    string
	)
	for _, cs := range sets {
		if cs == nil {
			continue
		}
		for i := range cs.Statements {
			st := &cs.Statements[i]
			if st.actions&mask == 0 {
				continue
			}
			var onVar func(string, string, bool)
			if trace != nil {
				onVar = trace.addVar
			}
			raw, ok := st.matchResource(cs.env, resType, req.Resource, ctx, onVar)
			if !ok {
				continue
			}
			if st.Effect == EffectDeny {
				// 显式拒绝立即生效，无需继续扫描。
				return setTrace(trace, ExplicitDeny, cs.Name, i, raw)
			}
			if !allowed {
				allowed, allowPolicy, allowIdx, allowRes = true, cs.Name, i, raw
			}
		}
	}
	if allowed {
		return setTrace(trace, Allow, allowPolicy, allowIdx, allowRes)
	}
	return setTrace(trace, ImplicitDeny, "", -1, "")
}

// matchResource 返回首个匹配的资源模式原文。
func (st *CompiledStatement) matchResource(env Env, resType, resID string,
	ctx *Context, onVar func(string, string, bool),
) (string, bool) {
	for i := range st.resources {
		if st.resources[i].matches(env, resType, resID, ctx, onVar) {
			return st.resources[i].raw, true
		}
	}
	return "", false
}

func setTrace(t *Trace, d Decision, policy string, idx int, res string) Decision {
	if t != nil {
		t.Decision = d
		t.PolicyName = policy
		t.StatementIndex = idx
		t.MatchedResource = res
	}
	return d
}

// Allowed 是热路径上的便捷判定，不记录轨迹。
func Allowed(sets []*CompiledSet, ctx *Context, req Request) bool {
	return Eval(sets, ctx, req, nil) == Allow
}

// Explain 是策略调试器的入口（PRD AUTH-040）。
//
// 它返回判定结果与完整轨迹，并在拒绝时给出修改建议——
// AWS 没有等价能力，而策略调试是 IoT Core 用户的头号痛点。
func Explain(sets []*CompiledSet, ctx *Context, req Request) (Trace, string) {
	var tr Trace
	Eval(sets, ctx, req, &tr)
	return tr, advise(sets, ctx, req, &tr)
}

// advise 对拒绝结果给出可操作的建议（PRD AUTH-042）。
func advise(sets []*CompiledSet, ctx *Context, req Request, tr *Trace) string {
	switch tr.Decision {
	case Allow:
		return fmt.Sprintf("放行：策略 %q 的第 %d 条语句，命中资源 %s",
			tr.PolicyName, tr.StatementIndex, tr.MatchedResource)

	case ExplicitDeny:
		return fmt.Sprintf("被显式拒绝：策略 %q 的第 %d 条语句（资源 %s）。"+
			"显式 Deny 优先于任何 Allow，须先移除或收窄该语句",
			tr.PolicyName, tr.StatementIndex, tr.MatchedResource)
	}

	// 隐式拒绝：先看是不是变量求值失败导致的，这是最常见也最难自查的原因。
	for _, v := range tr.VarList() {
		if v.Resolved {
			continue
		}
		return fmt.Sprintf("隐式拒绝：策略变量 %s 无法求值。"+
			"常见原因是连接未绑定设备，或使用 Thing 类变量时 clientId 与 thingName 不一致"+
			"（后者是 AWS 的隐含要求）。当前 clientId=%q thing=%q",
			v.Name, ctx.ClientID, ctx.Thing)
	}
	// 再看是否有语句覆盖了该动作。
	mask := req.Action.mask()
	hasAction := false
	for _, cs := range sets {
		if cs == nil {
			continue
		}
		for i := range cs.Statements {
			if cs.Statements[i].actions&mask != 0 {
				hasAction = true
				break
			}
		}
	}
	if !hasAction {
		return fmt.Sprintf("隐式拒绝：没有任何语句包含动作 %s。"+
			"须新增一条 Allow 语句并在 Action 中列出该动作", req.Action)
	}
	return fmt.Sprintf("隐式拒绝：有语句覆盖动作 %s，但没有资源模式匹配 %s/%s。"+
		"须扩展相应语句的 Resource，使其包含该 ARN",
		req.Action, req.Action.ResourceType(), req.Resource)
}
