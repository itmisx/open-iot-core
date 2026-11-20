package authz

import (
	"strings"
	"testing"
)

// TestAllDNFieldsAreReferenceable 验证每个声明支持的 DN 字段都真的可用：
// 能被解析、能被求值、能参与匹配。
//
// 清单与实现不一致是很隐蔽的缺陷——策略里写了一个"支持"的字段，
// 求值时却恒为空，结果是该语句永远不匹配，表现为设备莫名被拒。
func TestAllDNFieldsAreReferenceable(t *testing.T) {
	full := DN{
		CommonName:         "cn-value",
		Country:            "CN",
		Organization:       "org-value",
		OrganizationalUnit: "ou-value",
		State:              "state-value",
		Locality:           "locality-value",
		SerialNumber:       "dn-serial",
		Qualifier:          "qualifier-value",
	}
	for i := dnField(0); i < numDNFields; i++ {
		name := dnFieldNames[i]
		want := full.field(i)
		if want == "" {
			t.Fatalf("测试数据未覆盖字段 %s", name)
		}
		for _, which := range []string{"Subject", "Issuer"} {
			t.Run(which+"."+name, func(t *testing.T) {
				res := "arn:aws:iot:*:*:topic/x/${iot:Certificate." + which + "." + name + "}"
				cs := mustCompile(t, "p", `{"Statement":[{"Effect":"Allow",
				  "Action":"iot:Publish","Resource":"`+res+`"}]}`)
				ctx := &Context{ClientID: "c"}
				if which == "Subject" {
					ctx.Cert.Subject = full
				} else {
					ctx.Cert.Issuer = full
				}
				if d := Eval([]*CompiledSet{cs}, ctx, Request{ActPublish, "x/" + want}, nil); d != Allow {
					t.Errorf("字段 %s.%s 求值为 %q 时应放行，得到 %s", which, name, want, d)
				}
				// 另一侧未填充，应求值失败而拒绝
				other := &Context{ClientID: "c"}
				if which == "Subject" {
					other.Cert.Issuer = full
				} else {
					other.Cert.Subject = full
				}
				if d := Eval([]*CompiledSet{cs}, other, Request{ActPublish, "x/" + want}, nil); d != ImplicitDeny {
					t.Errorf("字段 %s.%s 未填充时应拒绝，得到 %s", which, name, d)
				}
			})
		}
	}
	// 越界字段返回空串
	if full.field(numDNFields) != "" {
		t.Error("越界 DN 字段应返回空串")
	}
	if _, ok := parseDNField("Nope"); ok {
		t.Error("未知 DN 字段应被拒绝")
	}
}

func TestResolveUnknownVarKind(t *testing.T) {
	ctx := &Context{ClientID: "c"}
	if _, ok := ctx.resolve(varSpec{kind: 99}); ok {
		t.Error("未知变量种类应求值失败")
	}
}

func TestCutPrefixSuffix(t *testing.T) {
	for _, tc := range []struct {
		s, pre, suf, want string
		ok                bool
	}{
		{"abc", "a", "", "bc", true},
		{"abc", "x", "", "", false},
		{"a[b]", "a[", "]", "b", true},
		{"a[b", "a[", "]", "", false},
	} {
		got, ok := cutPrefixSuffix(tc.s, tc.pre, tc.suf)
		if ok != tc.ok || got != tc.want {
			t.Errorf("cutPrefixSuffix(%q,%q,%q) = (%q,%v)", tc.s, tc.pre, tc.suf, got, ok)
		}
	}
}

func TestTokenizeEdgeCases(t *testing.T) {
	// 只有通配符
	if toks, err := tokenize("*"); err != nil || len(toks) != 1 || toks[0].kind != tokStar {
		t.Errorf("得到 %+v, %v", toks, err)
	}
	// 空标识
	if toks, err := tokenize(""); err != nil || len(toks) != 0 {
		t.Errorf("空标识应产出空 token 序列，得到 %+v, %v", toks, err)
	}
	// 孤立的 $ 是字面量
	toks, err := tokenize("a$b")
	if err != nil || len(toks) != 1 || toks[0].text != "a$b" {
		t.Errorf("孤立的 $ 应作为字面量，得到 %+v, %v", toks, err)
	}
	// 结尾的 $ 是字面量
	if toks, err := tokenize("a$"); err != nil || len(toks) != 1 || toks[0].text != "a$" {
		t.Errorf("结尾的 $ 应作为字面量，得到 %+v, %v", toks, err)
	}
	// 变量后紧跟字面量
	toks, err = tokenize("${iot:ClientId}/x")
	if err != nil || len(toks) != 2 || toks[0].kind != tokVar || toks[1].text != "/x" {
		t.Errorf("得到 %+v, %v", toks, err)
	}
}

func TestMatchTokensEdgeCases(t *testing.T) {
	ctx := &Context{ClientID: "c1"}
	// 空模式只匹配空串
	if !matchTokens(nil, "", ctx, nil) {
		t.Error("空模式应匹配空串")
	}
	if matchTokens(nil, "x", ctx, nil) {
		t.Error("空模式不应匹配非空串")
	}
	// 超出 token 上限的模式不匹配（编译期已拒绝，此处是防御）
	long := make([]token, maxTokens+1)
	for i := range long {
		long[i] = token{kind: tokStar}
	}
	if matchTokens(long, "x", ctx, nil) {
		t.Error("超出 token 上限应不匹配")
	}
	// 以 * 结尾可匹配任意剩余
	toks, _ := tokenize("a/*")
	if !matchTokens(toks, "a/b/c", ctx, nil) {
		t.Error("a/* 应匹配 a/b/c")
	}
	// ? 不能匹配空
	toks, _ = tokenize("a?")
	if matchTokens(toks, "a", ctx, nil) {
		t.Error("a? 不应匹配 a")
	}
}

func TestCompileRejectsStatementWithoutUsableAction(t *testing.T) {
	// Action 列表里全是无法识别的项
	d, err := ParseDocument([]byte(`{"Statement":[{"Effect":"Allow",
	  "Action":["iot:Nope"],"Resource":"*"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compile("p", d, DefaultEnv()); err == nil {
		t.Error("无可识别动作的语句应被拒绝")
	}
}

// FuzzParseDocument 保证策略解析对任意输入都不 panic。
//
// 策略文档来自控制面 API，即来自网络；解析器崩溃等于拒绝服务。
func FuzzParseDocument(f *testing.F) {
	f.Add([]byte(`{"Statement":[{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}]}`))
	f.Add([]byte(`{"Statement":[{"Effect":"Deny","Action":["iot:Publish"],
	  "Resource":["arn:aws:iot:*:*:topic/${iot:ClientId}/*"]}]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"Statement":[]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := ParseDocument(data)
		if err != nil {
			return
		}
		// 解析成功的文档必须能被编译，或给出明确错误——不得 panic
		cs, err := Compile("fuzz", d, DefaultEnv())
		if err != nil {
			return
		}
		// 编译成功的策略集必须能被求值
		ctx := &Context{ClientID: "c", Thing: "c", ThingAttached: true}
		for a := Action(0); a < numActions; a++ {
			var tr Trace
			Eval([]*CompiledSet{cs}, ctx, Request{a, "some/topic"}, &tr)
			if tr.Decision == Allow && tr.StatementIndex < 0 {
				t.Fatalf("放行判定应指出命中语句，轨迹 = %+v", tr)
			}
		}
	})
}

// FuzzGlobMatch 保证通配匹配不 panic 且不死循环。
func FuzzGlobMatch(f *testing.F) {
	f.Add("a*b", "axb")
	f.Add("*", "")
	f.Add("a?c", "abc")
	f.Add("**", "x")
	f.Fuzz(func(t *testing.T, pattern, s string) {
		if len(pattern) > 1024 || len(s) > 1024 {
			return
		}
		got := globMatch(pattern, s)
		// 无通配符时等价于相等比较
		if !strings.ContainsAny(pattern, "*?") && got != (pattern == s) {
			t.Fatalf("无通配符时 globMatch(%q,%q) = %v，应等于相等比较", pattern, s, got)
		}
		// 纯 * 匹配一切
		if pattern == "*" && !got {
			t.Fatalf("* 应匹配 %q", s)
		}
	})
}

// FuzzEvalNeverPanics 以 fuzz 生成的资源与主题交叉求值。
func FuzzEvalNeverPanics(f *testing.F) {
	f.Add("arn:aws:iot:*:*:topic/${iot:ClientId}/*", "dev/x", uint8(1))
	f.Add("*", "anything", uint8(0))
	f.Add("arn:aws:iot:*:*:topicfilter/a/#", "a/#", uint8(2))
	f.Fuzz(func(t *testing.T, resource, target string, rawAct uint8) {
		if len(resource) > 512 || len(target) > 512 {
			return
		}
		p, err := compileResource(resource)
		if err != nil {
			return
		}
		ctx := &Context{
			ClientID: "dev", Thing: "dev", ThingAttached: true,
			ThingAttr: func(string) (string, bool) { return "v", true },
			Cert: CertContext{
				SerialNumber: "SN",
				Subject:      DN{CommonName: "cn"},
				Issuer:       DN{CommonName: "ca"},
			},
		}
		act := Action(rawAct % uint8(numActions))
		rt := act.ResourceType()
		// 匹配必须是确定的：同样的输入两次结果一致
		a := p.matches(DefaultEnv(), rt, target, ctx, nil)
		b := p.matches(DefaultEnv(), rt, target, ctx, nil)
		if a != b {
			t.Fatalf("匹配不确定: %v 与 %v", a, b)
		}
	})
}
