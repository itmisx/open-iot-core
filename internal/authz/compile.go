package authz

import (
	"fmt"
	"sort"
	"strings"
)

// ARN 的固定段数：arn:partition:service:region:account:resource
const arnParts = 6

// Env 是本地的 ARN 环境，用于匹配策略资源中的区域与账户段。
//
// 自建场景没有真正的 AWS 区域与账户，但策略文档里会带着它们
// （设备 SDK 示例与控制台生成的策略都是完整 ARN），因此必须能匹配。
// 本地值可配，策略里写 * 则一律匹配。
type Env struct {
	Partition string
	Region    string
	Account   string
}

// DefaultEnv 返回自建场景的默认 ARN 环境。
func DefaultEnv() Env {
	return Env{Partition: "aws", Region: "local", Account: "000000000000"}
}

// tokenKind 是资源 id 模式中一个片段的种类。
type tokenKind uint8

const (
	tokLiteral tokenKind = iota
	tokStar
	tokQuestion
	tokVar
)

type token struct {
	kind tokenKind
	text string
	v    varSpec
}

// resourcePattern 是一条编译后的资源模式。
type resourcePattern struct {
	// anyARN 表示整个 Resource 就是 "*"。
	anyARN bool
	// partition、service、region、account 的通配模式。
	partition string
	service   string
	region    string
	account   string
	// anyType 表示资源部分为 "*"，匹配任意类型与 id。
	anyType bool
	resType string
	id      []token
	// raw 保留原始文本，供判定轨迹与控制台展示。
	raw string
}

// CompiledStatement 是编译后的一条语句。
type CompiledStatement struct {
	Sid    string
	Effect Effect
	// actions 是动作位掩码，判定时一次按位测试即可。
	actions   uint32
	resources []resourcePattern
}

// CompiledSet 是编译后的策略集。
//
// 编译在 apply 阶段完成，消息路径上不解析 JSON（PRD ARCH-024、AUTH-025）。
// 多个主体共享同一策略集时只占一份，因此策略集的数量是数十到数千，
// 而非与设备数同阶。
type CompiledSet struct {
	// Name 是策略名，写入判定轨迹。
	Name       string
	Statements []CompiledStatement
	env        Env
	// attrRefs 是本策略引用的设备属性名，去重且已排序。
	attrRefs []string
	// certVars 表示资源模板引用了任一证书变量。broker 据此只让确实需要
	// 证书字段的长连接保留 CertContext。
	certVars bool
}

// AttrRefs 返回本策略引用的设备属性名。
//
// 它存在是为了让热视图只物化策略真正读取的属性（PRD CLU-053、REG-006）：
// 没有它，控制面只能把全部属性塞进热视图，而百万设备下那是几十 MB
// 的无用内存——绝大多数属性从不被策略引用。
func (c *CompiledSet) AttrRefs() []string {
	if len(c.attrRefs) == 0 {
		return nil
	}
	out := make([]string, len(c.attrRefs))
	copy(out, c.attrRefs)
	return out
}

// UsesCertificateVariables 报告策略是否引用证书序列号、Subject 或 Issuer。
func (c *CompiledSet) UsesCertificateVariables() bool { return c != nil && c.certVars }

// Compile 把一份策略文档编译为匹配器。
func Compile(name string, d *Document, env Env) (*CompiledSet, error) {
	cs := &CompiledSet{Name: name, env: env}
	for i, st := range d.Statement {
		c := CompiledStatement{Sid: st.Sid, Effect: st.effect()}
		for _, a := range st.Action {
			if a == "*" || strings.EqualFold(a, "iot:*") {
				c.actions = (1 << uint(numActions)) - 1
				continue
			}
			act, ok := ParseAction(a)
			if !ok {
				return nil, fmt.Errorf("authz: 语句 %d 的动作 %q 不被支持", i, a)
			}
			c.actions |= act.mask()
		}
		if c.actions == 0 {
			return nil, fmt.Errorf("authz: 语句 %d 没有可识别的动作", i)
		}
		for _, r := range st.Resource {
			p, err := compileResource(r)
			if err != nil {
				return nil, fmt.Errorf("authz: 语句 %d: %w", i, err)
			}
			c.resources = append(c.resources, p)
		}
		cs.Statements = append(cs.Statements, c)
	}
	cs.attrRefs = collectAttrRefs(cs.Statements)
	cs.certVars = usesCertificateVariables(cs.Statements)
	return cs, nil
}

func usesCertificateVariables(sts []CompiledStatement) bool {
	for i := range sts {
		for _, res := range sts[i].resources {
			for _, tok := range res.id {
				if tok.kind == tokVar && (tok.v.kind == varCertSerial ||
					tok.v.kind == varSubject || tok.v.kind == varIssuer) {
					return true
				}
			}
		}
	}
	return false
}

// collectAttrRefs 收集策略引用的设备属性名。
//
// 在编译期一次算出，而不是在鉴权路径上反复扫描：鉴权是热路径，
// 而属性引用是策略的静态性质，不随请求变化。
func collectAttrRefs(sts []CompiledStatement) []string {
	var out []string
	seen := make(map[string]bool)
	for i := range sts {
		for _, res := range sts[i].resources {
			for _, tok := range res.id {
				if tok.kind != tokVar || tok.v.kind != varThingAttr {
					continue
				}
				if tok.v.attr == "" || seen[tok.v.attr] {
					continue
				}
				seen[tok.v.attr] = true
				out = append(out, tok.v.attr)
			}
		}
	}
	sort.Strings(out)
	return out
}

// CompileJSON 是解析加编译的便捷组合。
func CompileJSON(name string, b []byte, env Env) (*CompiledSet, error) {
	d, err := ParseDocument(b)
	if err != nil {
		return nil, err
	}
	return Compile(name, d, env)
}

func compileResource(r string) (resourcePattern, error) {
	p := resourcePattern{raw: r}
	if r == "*" {
		p.anyARN = true
		return p, nil
	}
	parts := strings.SplitN(r, ":", arnParts)
	if len(parts) != arnParts || parts[0] != "arn" {
		return p, fmt.Errorf("资源 %q 不是合法 ARN，也不是 *", r)
	}
	p.partition, p.service, p.region, p.account = parts[1], parts[2], parts[3], parts[4]
	if p.service != "iot" && p.service != "*" {
		return p, fmt.Errorf("资源 %q 的服务段为 %q，只支持 iot", r, p.service)
	}
	res := parts[5]
	if res == "*" {
		p.anyType = true
		return p, nil
	}
	slash := strings.IndexByte(res, '/')
	if slash < 0 {
		return p, fmt.Errorf("资源 %q 缺少 <类型>/<标识> 形式的资源段", r)
	}
	p.resType = res[:slash]
	toks, err := tokenize(res[slash+1:])
	if err != nil {
		return p, fmt.Errorf("资源 %q: %w", r, err)
	}
	if len(toks) > maxTokens {
		return p, fmt.Errorf("资源 %q 的标识被切分为 %d 个片段，超出上限 %d",
			r, len(toks), maxTokens)
	}
	p.id = toks
	return p, nil
}

// tokenize 把资源标识切分为字面量、通配符与变量引用。
func tokenize(s string) ([]token, error) {
	var out []token
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			out = append(out, token{kind: tokLiteral, text: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(s); {
		switch {
		case s[i] == '*':
			flush()
			// 连续的 * 等价于一个
			if n := len(out); n == 0 || out[n-1].kind != tokStar {
				out = append(out, token{kind: tokStar})
			}
			i++
		case s[i] == '?':
			flush()
			out = append(out, token{kind: tokQuestion})
			i++
		case s[i] == '$' && i+1 < len(s) && s[i+1] == '{':
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				return nil, fmt.Errorf("变量引用缺少右花括号")
			}
			name := s[i+2 : i+2+end]
			v, err := parseVar(name)
			if err != nil {
				return nil, err
			}
			flush()
			out = append(out, token{kind: tokVar, text: name, v: v})
			i += 2 + end + 1
		default:
			lit.WriteByte(s[i])
			i++
		}
	}
	flush()
	return out, nil
}

// globMatch 是 IAM 风格的通配匹配：* 匹配任意字符序列，? 匹配单个字符。
//
// 注意它与 MQTT 的通配符语义不同：MQTT 的 + 不跨越 /，而策略资源中的 *
// 跨越 /。把两者混同是常见错误——它会让 topic/a/* 意外只匹配一层，
// 或者反过来让 topicfilter 的层级约束失效。
func globMatch(pattern, s string) bool {
	pi, si := 0, 0
	starPi, starSi := -1, -1
	for si < len(s) {
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				starPi, starSi = pi, si
				pi++
				continue
			case '?':
				pi++
				si++
				continue
			default:
				if pattern[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}
		if starPi < 0 {
			return false
		}
		starSi++
		si = starSi
		pi = starPi + 1
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// maxTokens 是单条资源标识允许的 token 数上限。
//
// 有了上限，匹配器就能用栈上定长数组保存预求值结果，整条判定路径零分配。
// 超过上限的模式在编译期被拒绝，而不是在匹配时静默退化——
// 一条无法正确求值的策略必须让人立刻知道。
const maxTokens = 32

// matchTokens 用带变量的 token 序列匹配目标串。
//
// 变量在匹配前一次性预求值，而不是在匹配中按需求值：回溯会反复经过同一个
// 变量 token，按需求值会让 onVar 对同一变量重复上报，污染判定轨迹。
// 解析失败则整条资源不匹配——不得退化为空串（PRD AUTH-036）。
// onVar 用于记录求值结果，可为 nil。
func matchTokens(toks []token, s string, ctx *Context, onVar func(name, value string, ok bool)) bool {
	if len(toks) > maxTokens {
		return false
	}
	var resolved [maxTokens]string
	for i := range toks {
		if toks[i].kind != tokVar {
			continue
		}
		val, ok := ctx.resolve(toks[i].v)
		if onVar != nil {
			onVar(toks[i].v.name, val, ok)
		}
		if !ok {
			return false
		}
		resolved[i] = val
	}

	ti, si := 0, 0
	starTi, starSi := -1, -1
	for {
		if ti < len(toks) {
			t := toks[ti]
			switch t.kind {
			case tokStar:
				starTi, starSi = ti, si
				ti++
				continue
			case tokQuestion:
				if si < len(s) {
					ti++
					si++
					continue
				}
			case tokLiteral:
				if strings.HasPrefix(s[si:], t.text) {
					ti++
					si += len(t.text)
					continue
				}
			case tokVar:
				if val := resolved[ti]; strings.HasPrefix(s[si:], val) {
					ti++
					si += len(val)
					continue
				}
			}
		} else if si == len(s) {
			return true
		}
		if starTi < 0 {
			return false
		}
		starSi++
		if starSi > len(s) {
			return false
		}
		si = starSi
		ti = starTi + 1
	}
}

// matches 判定一条资源模式是否匹配请求。
func (p *resourcePattern) matches(env Env, resType, resID string, ctx *Context,
	onVar func(name, value string, ok bool),
) bool {
	if p.anyARN {
		return true
	}
	if !globMatch(p.partition, env.Partition) ||
		!globMatch(p.service, "iot") ||
		!globMatch(p.region, env.Region) ||
		!globMatch(p.account, env.Account) {
		return false
	}
	if p.anyType {
		return true
	}
	// 资源类型必须精确匹配，不接受通配。
	//
	// 这是 PRD AUTH-023 的保证：若允许 topic* 这类模式，
	// 一条意在放行 topic/ 的语句会连带放行 topicfilter/，
	// 使「允许订阅」与「允许接收」的区分失效。
	if p.resType != resType {
		return false
	}
	return matchTokens(p.id, resID, ctx, onVar)
}
