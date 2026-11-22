package authz

import (
	"strings"
	"testing"
)

func mustCompile(t *testing.T, name, src string) *CompiledSet {
	t.Helper()
	cs, err := CompileJSON(name, []byte(src), DefaultEnv())
	if err != nil {
		t.Fatalf("编译 %s 失败: %v", name, err)
	}
	return cs
}

func devCtx(clientID, thing string) *Context {
	return &Context{
		ClientID:      clientID,
		Thing:         thing,
		ThingAttached: true,
		Cert: &CertContext{
			SerialNumber: "0A1B2C",
			Subject:      DN{CommonName: clientID + ".example", Organization: "Acme"},
			Issuer:       DN{CommonName: "Acme Device CA", Country: "CN"},
		},
	}
}

// TestExplicitDenyBeatsAllow 固定住 PRD AUTH-021 的核心规则。
//
// 必须扫完全部语句才能确认放行——若在遇到第一条 Allow 时就返回，
// 后面的 Deny 会被忽略，这是权限放大。本测试把 Deny 放在 Allow 之后，
// 正是为了捕获这种实现错误。
func TestExplicitDenyBeatsAllow(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/*"},
	  {"Effect":"Deny","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/secret/*"}
	]}`)
	ctx := devCtx("dev-1", "dev-1")

	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "public/x"}, nil); d != Allow {
		t.Errorf("普通主题应放行，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "secret/x"}, nil); d != ExplicitDeny {
		t.Errorf("显式拒绝应优先于放行，得到 %s", d)
	}
	// 跨策略集同样成立
	allow := mustCompile(t, "allow", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish","Resource":"*"}]}`)
	deny := mustCompile(t, "deny", `{"Statement":[
	  {"Effect":"Deny","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/x"}]}`)
	if d := Eval([]*CompiledSet{allow, deny}, ctx, Request{ActPublish, "x"}, nil); d != ExplicitDeny {
		t.Errorf("跨策略集的显式拒绝也应生效，得到 %s", d)
	}
	// 顺序颠倒结果不变
	if d := Eval([]*CompiledSet{deny, allow}, ctx, Request{ActPublish, "x"}, nil); d != ExplicitDeny {
		t.Errorf("策略集顺序不应影响结果，得到 %s", d)
	}
}

func TestImplicitDenyIsDefault(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Connect","Resource":"arn:aws:iot:*:*:client/dev-1"}]}`)
	ctx := devCtx("dev-1", "dev-1")

	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "x"}, nil); d != ImplicitDeny {
		t.Errorf("无匹配语句应隐式拒绝，得到 %s", d)
	}
	// 空策略集、nil 元素、nil 切片都应隐式拒绝
	for _, sets := range [][]*CompiledSet{nil, {}, {nil}} {
		if d := Eval(sets, ctx, Request{ActConnect, "dev-1"}, nil); d != ImplicitDeny {
			t.Errorf("策略集 %v 应隐式拒绝，得到 %s", sets, d)
		}
	}
	// 越界动作隐式拒绝
	if d := Eval([]*CompiledSet{cs}, ctx, Request{Action(99), "x"}, nil); d != ImplicitDeny {
		t.Errorf("未知动作应隐式拒绝，得到 %s", d)
	}
}

// TestSubscribeAndReceiveAreJudgedSeparately 固定住 PRD AUTH-023。
func TestSubscribeAndReceiveAreJudgedSeparately(t *testing.T) {
	// 只允许订阅过滤器，不允许接收主题
	subOnly := mustCompile(t, "subOnly", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/sensors/*"}]}`)
	ctx := devCtx("dev-1", "dev-1")

	if d := Eval([]*CompiledSet{subOnly}, ctx, Request{ActSubscribe, "sensors/#"}, nil); d != Allow {
		t.Errorf("订阅应放行，得到 %s", d)
	}
	// 允许订阅不等于允许接收
	if d := Eval([]*CompiledSet{subOnly}, ctx, Request{ActReceive, "sensors/temp"}, nil); d != ImplicitDeny {
		t.Errorf("只允许订阅时接收应被拒绝，得到 %s", d)
	}

	// 反过来：只允许接收主题，不允许订阅
	recvOnly := mustCompile(t, "recvOnly", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Receive",
	   "Resource":"arn:aws:iot:*:*:topic/sensors/*"}]}`)
	if d := Eval([]*CompiledSet{recvOnly}, ctx, Request{ActReceive, "sensors/temp"}, nil); d != Allow {
		t.Errorf("接收应放行，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{recvOnly}, ctx, Request{ActSubscribe, "sensors/#"}, nil); d != ImplicitDeny {
		t.Errorf("只允许接收时订阅应被拒绝，得到 %s", d)
	}

	// topicfilter 资源不能满足 topic 动作，即便标识串相同
	mixed := mustCompile(t, "mixed", `{"Statement":[
	  {"Effect":"Allow","Action":["iot:Publish","iot:Subscribe"],
	   "Resource":"arn:aws:iot:*:*:topicfilter/a/b"}]}`)
	if d := Eval([]*CompiledSet{mixed}, ctx, Request{ActPublish, "a/b"}, nil); d != ImplicitDeny {
		t.Errorf("topicfilter 资源不应满足 Publish，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{mixed}, ctx, Request{ActSubscribe, "a/b"}, nil); d != Allow {
		t.Errorf("topicfilter 资源应满足 Subscribe，得到 %s", d)
	}
}

func TestPolicyVariables(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Connect",
	   "Resource":"arn:aws:iot:*:*:client/${iot:ClientId}"},
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"},
	  {"Effect":"Allow","Action":"iot:Receive",
	   "Resource":"arn:aws:iot:*:*:topic/certs/${iot:Certificate.Subject.CommonName}"},
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/issuer/${iot:Certificate.Issuer.CommonName}/#"},
	  {"Effect":"Allow","Action":"iot:RetainPublish",
	   "Resource":"arn:aws:iot:*:*:topic/serial/${iot:Certificate.SerialNumber}"}
	]}`)
	ctx := devCtx("dev-1", "dev-1")

	cases := []struct {
		req  Request
		want Decision
	}{
		{Request{ActConnect, "dev-1"}, Allow},
		{Request{ActConnect, "dev-2"}, ImplicitDeny},
		{Request{ActPublish, "$aws/things/dev-1/shadow/update"}, Allow},
		{Request{ActPublish, "$aws/things/dev-2/shadow/update"}, ImplicitDeny},
		{Request{ActReceive, "certs/dev-1.example"}, Allow},
		{Request{ActReceive, "certs/other"}, ImplicitDeny},
		{Request{ActSubscribe, "issuer/Acme Device CA/#"}, Allow},
		{Request{ActSubscribe, "issuer/Acme Device CA/x"}, ImplicitDeny},
		{Request{ActRetainPublish, "serial/0A1B2C"}, Allow},
		{Request{ActRetainPublish, "serial/other"}, ImplicitDeny},
	}
	for _, tc := range cases {
		if d := Eval([]*CompiledSet{cs}, ctx, tc.req, nil); d != tc.want {
			t.Errorf("%s %q = %s，期望 %s", tc.req.Action, tc.req.Resource, d, tc.want)
		}
	}
}

// TestMQTTWildcardsAreLiteralInPolicies 固定住一处容易做错的语义。
//
// 在策略资源中，# 与 + 是普通字面字符，只有 * 与 ? 是通配符。
// 而 iot:Subscribe 判定的资源就是客户端发来的过滤器串本身，
// 因此策略要么写出完全一致的过滤器，要么用 * 覆盖。
//
// 把 MQTT 通配符当策略通配符处理会让 topicfilter/a/# 这样的策略
// 意外放行 a/b、a/b/c 等任意订阅，这是权限放大。
func TestMQTTWildcardsAreLiteralInPolicies(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/a/#"}]}`)
	ctx := devCtx("dev-1", "dev-1")

	// 字面一致才匹配
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActSubscribe, "a/#"}, nil); d != Allow {
		t.Errorf("字面一致的过滤器应放行，得到 %s", d)
	}
	// # 不作为通配符展开
	for _, f := range []string{"a/b", "a/b/c", "a/+", "a"} {
		if d := Eval([]*CompiledSet{cs}, ctx, Request{ActSubscribe, f}, nil); d != ImplicitDeny {
			t.Errorf("过滤器 %q 不应被 topicfilter/a/# 放行，得到 %s", f, d)
		}
	}
	// 要覆盖子树须显式用 *
	star := mustCompile(t, "star", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/a/*"}]}`)
	for _, f := range []string{"a/#", "a/b", "a/b/c", "a/+"} {
		if d := Eval([]*CompiledSet{star}, ctx, Request{ActSubscribe, f}, nil); d != Allow {
			t.Errorf("topicfilter/a/* 应放行 %q，得到 %s", f, d)
		}
	}
	// + 同样是字面字符
	plus := mustCompile(t, "plus", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/a/+"}]}`)
	if d := Eval([]*CompiledSet{plus}, ctx, Request{ActSubscribe, "a/+"}, nil); d != Allow {
		t.Errorf("字面 + 应匹配，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{plus}, ctx, Request{ActSubscribe, "a/b"}, nil); d != ImplicitDeny {
		t.Errorf("+ 不应作为通配符，得到 %s", d)
	}
}

// TestThingVariablesRequireClientIDMatch 固定住 PRD AUTH-034 的隐含约束。
//
// AWS 要求使用 Thing 类策略变量时 clientId 必须等于 thingName。
// 漏掉它意味着设备 A 可以用引用了 ThingName 的策略访问设备 B 的主题。
func TestThingVariablesRequireClientIDMatch(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/update"}]}`)

	// clientId 与 thingName 一致：放行
	ok := devCtx("dev-1", "dev-1")
	if d := Eval([]*CompiledSet{cs}, ok, Request{ActPublish, "$aws/things/dev-1/shadow/update"}, nil); d != Allow {
		t.Errorf("一致时应放行，得到 %s", d)
	}

	// clientId 与 thingName 不一致：拒绝，即便目标主题正是该 thing 的
	bad := devCtx("attacker", "dev-1")
	if d := Eval([]*CompiledSet{cs}, bad, Request{ActPublish, "$aws/things/dev-1/shadow/update"}, nil); d != ImplicitDeny {
		t.Errorf("clientId 与 thingName 不一致时应拒绝，得到 %s", d)
	}
	// 也不能借此访问自己名下的主题
	if d := Eval([]*CompiledSet{cs}, bad, Request{ActPublish, "$aws/things/attacker/shadow/update"}, nil); d != ImplicitDeny {
		t.Errorf("不一致时任何主题都应拒绝，得到 %s", d)
	}

	// 未绑定设备：拒绝
	none := devCtx("dev-1", "")
	if d := Eval([]*CompiledSet{cs}, none, Request{ActPublish, "$aws/things/dev-1/shadow/update"}, nil); d != ImplicitDeny {
		t.Errorf("未绑定设备时应拒绝，得到 %s", d)
	}
}

// TestUnresolvedVariableDenies 固定住 PRD AUTH-036。
//
// 变量求值失败必须拒绝，不得退化为空串匹配——空串匹配会让
// topic/${iot:Connection.Thing.ThingName}/x 在设备未绑定时意外匹配
// topic//x，这是权限放大。
func TestUnresolvedVariableDenies(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.ThingName}/x"}]}`)
	ctx := devCtx("dev-1", "") // 未绑定设备

	// 退化为空串时会匹配的那个主题，必须被拒绝
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "/x"}, nil); d != ImplicitDeny {
		t.Errorf("变量无法求值时应拒绝，得到 %s", d)
	}

	// 各类变量的求值失败都应拒绝
	for _, tc := range []struct {
		name, res string
		ctx       *Context
	}{
		{"clientId 为空", "topic/${iot:ClientId}", &Context{}},
		{"证书序列号为空", "topic/${iot:Certificate.SerialNumber}", &Context{ClientID: "c"}},
		{"证书主题为空", "topic/${iot:Certificate.Subject.CommonName}", &Context{ClientID: "c"}},
		{"签发者为空", "topic/${iot:Certificate.Issuer.CommonName}", &Context{ClientID: "c"}},
		{"属性查询器为 nil", "topic/${iot:Connection.Thing.Attributes[site]}",
			&Context{ClientID: "c", Thing: "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
			  "Action":"iot:Publish","Resource":"arn:aws:iot:*:*:`+tc.res+`"}]}`)
			if d := Eval([]*CompiledSet{cs}, tc.ctx, Request{ActPublish, ""}, nil); d != ImplicitDeny {
				t.Errorf("应拒绝，得到 %s", d)
			}
		})
	}
}

func TestThingAttributesAndIsAttached(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/site/${iot:Connection.Thing.Attributes[site]}/data"},
	  {"Effect":"Allow","Action":"iot:Receive",
	   "Resource":"arn:aws:iot:*:*:topic/attached/${iot:Connection.Thing.IsAttached}"}
	]}`)
	ctx := devCtx("dev-1", "dev-1")
	ctx.ThingAttrs = testThingAttrs{"dev-1/site": "shanghai"}

	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "site/shanghai/data"}, nil); d != Allow {
		t.Errorf("属性匹配应放行，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "site/beijing/data"}, nil); d != ImplicitDeny {
		t.Errorf("属性不匹配应拒绝，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActReceive, "attached/true"}, nil); d != Allow {
		t.Errorf("IsAttached 为 true 应放行，得到 %s", d)
	}
	ctx.ThingAttached = false
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActReceive, "attached/false"}, nil); d != Allow {
		t.Errorf("IsAttached 为 false 应匹配 false，得到 %s", d)
	}
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActReceive, "attached/true"}, nil); d != ImplicitDeny {
		t.Errorf("IsAttached 为 false 不应匹配 true，得到 %s", d)
	}
}

type testThingAttrs map[string]string

func (a testThingAttrs) ThingAttr(thing, name string) (string, bool) {
	v, ok := a[thing+"/"+name]
	return v, ok
}

// TestRealWorldSDKPolicy 用 AWS 设备 SDK 示例里典型的策略形态做端到端验证。
func TestRealWorldSDKPolicy(t *testing.T) {
	cs := mustCompile(t, "device", `{
	  "Version": "2012-10-17",
	  "Statement": [
	    {
	      "Effect": "Allow",
	      "Action": "iot:Connect",
	      "Resource": "arn:aws:iot:us-east-1:123456789012:client/${iot:Connection.Thing.ThingName}"
	    },
	    {
	      "Effect": "Allow",
	      "Action": ["iot:Publish", "iot:Receive"],
	      "Resource": [
	        "arn:aws:iot:us-east-1:123456789012:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*",
	        "arn:aws:iot:us-east-1:123456789012:topic/$aws/things/${iot:Connection.Thing.ThingName}/jobs/*"
	      ]
	    },
	    {
	      "Effect": "Allow",
	      "Action": "iot:Subscribe",
	      "Resource": "arn:aws:iot:us-east-1:123456789012:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"
	    }
	  ]
	}`)
	// 策略写的是具体区域与账户，环境必须对得上
	cs.env = Env{Partition: "aws", Region: "us-east-1", Account: "123456789012"}
	ctx := devCtx("dev-001", "dev-001")

	cases := []struct {
		req  Request
		want Decision
	}{
		{Request{ActConnect, "dev-001"}, Allow},
		{Request{ActPublish, "$aws/things/dev-001/shadow/update"}, Allow},
		{Request{ActPublish, "$aws/things/dev-001/shadow/name/cfg/update"}, Allow},
		{Request{ActReceive, "$aws/things/dev-001/shadow/update/accepted"}, Allow},
		{Request{ActPublish, "$aws/things/dev-001/jobs/start-next"}, Allow},
		{Request{ActSubscribe, "$aws/things/dev-001/shadow/update/delta"}, Allow},
		// 别的设备的主题
		{Request{ActPublish, "$aws/things/dev-002/shadow/update"}, ImplicitDeny},
		// 未授权的动作
		{Request{ActRetainPublish, "$aws/things/dev-001/shadow/update"}, ImplicitDeny},
		// jobs 只授权了 topic 而非 topicfilter，订阅应被拒
		{Request{ActSubscribe, "$aws/things/dev-001/jobs/notify"}, ImplicitDeny},
	}
	for _, tc := range cases {
		if d := Eval([]*CompiledSet{cs}, ctx, tc.req, nil); d != tc.want {
			t.Errorf("%s %q = %s，期望 %s", tc.req.Action, tc.req.Resource, d, tc.want)
		}
	}
}

func TestTraceCapturesVariables(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Sid":"allow-shadow","Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"}]}`)
	ctx := devCtx("dev-1", "dev-1")

	var tr Trace
	d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "$aws/things/dev-1/shadow/update"}, &tr)
	if d != Allow || tr.Decision != Allow {
		t.Fatalf("判定 = %s / %s", d, tr.Decision)
	}
	if tr.PolicyName != "p" || tr.StatementIndex != 0 {
		t.Errorf("命中位置 = %q/%d", tr.PolicyName, tr.StatementIndex)
	}
	if !strings.Contains(tr.MatchedResource, "shadow") {
		t.Errorf("命中资源 = %q", tr.MatchedResource)
	}
	vars := tr.VarList()
	if len(vars) != 1 {
		t.Fatalf("变量数 = %d，期望 1: %+v", len(vars), vars)
	}
	if vars[0].Name != "iot:Connection.Thing.ThingName" ||
		vars[0].Value != "dev-1" || !vars[0].Resolved {
		t.Errorf("变量记录 = %+v", vars[0])
	}
}

// TestTraceDeduplicatesVariables 验证同一变量在多条资源上只记一次，
// 且回溯不会造成重复上报。
func TestTraceDeduplicatesVariables(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish","Resource":[
	    "arn:aws:iot:*:*:topic/a/${iot:ClientId}/x",
	    "arn:aws:iot:*:*:topic/b/${iot:ClientId}/y",
	    "arn:aws:iot:*:*:topic/*/${iot:ClientId}/*/${iot:ClientId}/*"]}]}`)
	ctx := devCtx("dev-1", "dev-1")
	var tr Trace
	Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "zzz/no/match"}, &tr)
	if n := len(tr.VarList()); n != 1 {
		t.Errorf("同一变量应只记一次，得到 %d 条: %+v", n, tr.VarList())
	}
}

func TestTraceRecordsUnresolvedVariable(t *testing.T) {
	cs := mustCompile(t, "p", `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.ThingName}/x"}]}`)
	ctx := devCtx("dev-1", "") // 未绑定设备
	var tr Trace
	if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "dev-1/x"}, &tr); d != ImplicitDeny {
		t.Fatalf("判定 = %s", d)
	}
	vars := tr.VarList()
	if len(vars) != 1 || vars[0].Resolved {
		t.Fatalf("应记录一条求值失败的变量，得到 %+v", vars)
	}
	if tr.StatementIndex != -1 {
		t.Errorf("隐式拒绝时语句下标应为 -1，得到 %d", tr.StatementIndex)
	}
}

func TestTraceVarTruncation(t *testing.T) {
	var tr Trace
	tr.Reset()
	for i := 0; i < maxTraceVars+3; i++ {
		tr.addVar(string(rune('a'+i)), "v", true)
	}
	if int(tr.NVars) != maxTraceVars {
		t.Errorf("变量数 = %d，期望钳到 %d", tr.NVars, maxTraceVars)
	}
	if !tr.VarsTruncated {
		t.Error("超出上限应置截断标记")
	}
}

// TestExplainGivesActionableAdvice 验证策略调试器的建议质量。
// AWS 没有等价能力，这是 IoT Core 用户头号痛点的解法。
func TestExplainGivesActionableAdvice(t *testing.T) {
	ctx := devCtx("dev-1", "dev-1")

	t.Run("放行", func(t *testing.T) {
		cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
		  "Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/a"}]}`)
		tr, adv := Explain([]*CompiledSet{cs}, ctx, Request{ActPublish, "a"})
		if tr.Decision != Allow || !strings.Contains(adv, "放行") {
			t.Errorf("建议 = %q", adv)
		}
	})

	t.Run("显式拒绝指出须移除的语句", func(t *testing.T) {
		cs := mustCompile(t, "p", `{"Statement":[
		  {"Effect":"Allow","Action":"iot:Publish","Resource":"*"},
		  {"Effect":"Deny","Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/a"}]}`)
		_, adv := Explain([]*CompiledSet{cs}, ctx, Request{ActPublish, "a"})
		for _, want := range []string{"显式拒绝", "优先", "第 1 条"} {
			if !strings.Contains(adv, want) {
				t.Errorf("建议 %q 应含 %q", adv, want)
			}
		}
	})

	t.Run("变量求值失败给出最可能的原因", func(t *testing.T) {
		cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
		  "Action":"iot:Publish",
		  "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.ThingName}/x"}]}`)
		bad := devCtx("attacker", "dev-1")
		_, adv := Explain([]*CompiledSet{cs}, bad, Request{ActPublish, "dev-1/x"})
		for _, want := range []string{"无法求值", "clientId", "thingName"} {
			if !strings.Contains(adv, want) {
				t.Errorf("建议 %q 应含 %q", adv, want)
			}
		}
	})

	t.Run("没有语句覆盖该动作", func(t *testing.T) {
		cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
		  "Action":"iot:Connect","Resource":"*"}]}`)
		_, adv := Explain([]*CompiledSet{cs}, ctx, Request{ActPublish, "a"})
		if !strings.Contains(adv, "没有任何语句包含动作") {
			t.Errorf("建议 = %q", adv)
		}
	})

	t.Run("有动作但资源不匹配", func(t *testing.T) {
		cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
		  "Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/other"}]}`)
		_, adv := Explain([]*CompiledSet{cs}, ctx, Request{ActPublish, "a"})
		for _, want := range []string{"没有资源模式匹配", "topic/a"} {
			if !strings.Contains(adv, want) {
				t.Errorf("建议 %q 应含 %q", adv, want)
			}
		}
	})
}

func TestDecisionHelpers(t *testing.T) {
	if !Allow.Allowed() || ExplicitDeny.Allowed() || ImplicitDeny.Allowed() {
		t.Error("Allowed 不正确")
	}
	if Allow.String() != "Allow" || Decision(99).String() != "Unknown" {
		t.Error("Decision.String 不正确")
	}
	cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
	  "Action":"iot:Connect","Resource":"*"}]}`)
	ctx := devCtx("c", "c")
	if !Allowed([]*CompiledSet{cs}, ctx, Request{ActConnect, "c"}) {
		t.Error("Allowed 便捷入口不正确")
	}
}

func TestSupportedVariables(t *testing.T) {
	vs := SupportedVariables()
	if len(vs) < 5+2*int(numDNFields) {
		t.Errorf("支持的变量数 = %d，少于预期", len(vs))
	}
	// 清单里的每个具体变量名都必须真的能解析
	for _, v := range vs {
		if strings.Contains(v, "<name>") {
			continue
		}
		if _, err := parseVar(v); err != nil {
			t.Errorf("清单声明支持 %q 但解析失败: %v", v, err)
		}
	}
}

// BenchmarkAllowedHotPath 验证热路径判定零分配——
// 每条 PUBLISH 都要走一次，这是把策略求值放在消息路径上的前提。
func BenchmarkAllowedHotPath(b *testing.B) {
	cs, err := CompileJSON("p", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":["iot:Publish","iot:Receive"],
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"},
	  {"Effect":"Deny","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/+/shadow/delete"}]}`), DefaultEnv())
	if err != nil {
		b.Fatal(err)
	}
	ctx := &Context{ClientID: "dev-001", Thing: "dev-001", ThingAttached: true}
	sets := []*CompiledSet{cs}
	req := Request{ActPublish, "$aws/things/dev-001/shadow/update"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !Allowed(sets, ctx, req) {
			b.Fatal("应放行")
		}
	}
}

func BenchmarkAllowedDenied(b *testing.B) {
	cs, _ := CompileJSON("p", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/allowed/*"}]}`), DefaultEnv())
	ctx := &Context{ClientID: "dev-001"}
	sets := []*CompiledSet{cs}
	req := Request{ActPublish, "denied/topic"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if Allowed(sets, ctx, req) {
			b.Fatal("应拒绝")
		}
	}
}

func BenchmarkEvalWithTrace(b *testing.B) {
	cs, _ := CompileJSON("p", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:ClientId}/shadow/*"}]}`), DefaultEnv())
	ctx := &Context{ClientID: "dev-001", Thing: "dev-001", ThingAttached: true}
	sets := []*CompiledSet{cs}
	req := Request{ActPublish, "$aws/things/dev-001/shadow/update"}
	var tr Trace
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Eval(sets, ctx, req, &tr)
	}
}

func BenchmarkCompile(b *testing.B) {
	src := []byte(`{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Action":"iot:Connect",
	   "Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"},
	  {"Effect":"Allow","Action":["iot:Publish","iot:Receive"],
	   "Resource":["arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*",
	               "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/jobs/*"]}]}`)
	env := DefaultEnv()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := CompileJSON("p", src, env); err != nil {
			b.Fatal(err)
		}
	}
}
