package authz

import (
	"strings"
	"testing"
)

func TestCompileRejectsBadResources(t *testing.T) {
	cases := []struct {
		name, res, want string
	}{
		{"不是 ARN", "topic/a", "合法 ARN"},
		{"段数不足", "arn:aws:iot:r:a", "合法 ARN"},
		{"服务段不是 iot", "arn:aws:s3:r:a:bucket/x", "只支持 iot"},
		{"缺少类型段", "arn:aws:iot:r:a:topic", "资源段"},
		{"变量缺右花括号", "arn:aws:iot:r:a:topic/${iot:ClientId", "右花括号"},
		{"未知变量", "arn:aws:iot:r:a:topic/${iot:Nope}", "不支持的策略变量"},
		{"变量缺前缀", "arn:aws:iot:r:a:topic/${ClientId}", "iot: 前缀"},
		{"属性名为空", "arn:aws:iot:r:a:topic/${iot:Connection.Thing.Attributes[]}", "属性名为空"},
		{"未知主题字段", "arn:aws:iot:r:a:topic/${iot:Certificate.Subject.Nope}", "证书主题字段"},
		{"未知签发者字段", "arn:aws:iot:r:a:topic/${iot:Certificate.Issuer.Nope}", "签发者字段"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileResource(tc.res)
			if err == nil {
				t.Fatal("应被拒绝")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息 %q 应含 %q", err.Error(), tc.want)
			}
		})
	}
}

func TestCompileRejectsTooManyTokens(t *testing.T) {
	// 超出 token 上限的模式在编译期被拒绝，而不是匹配时静默退化
	id := strings.Repeat("a*", maxTokens)
	_, err := compileResource("arn:aws:iot:r:a:topic/" + id)
	if err == nil || !strings.Contains(err.Error(), "超出上限") {
		t.Fatalf("应报出超出上限，得到 %v", err)
	}
}

func TestCompileActions(t *testing.T) {
	d, err := ParseDocument([]byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	cs, err := Compile("p", d, DefaultEnv())
	if err != nil {
		t.Fatal(err)
	}
	// iot:* 覆盖全部动作
	for a := Action(0); a < numActions; a++ {
		if cs.Statements[0].actions&a.mask() == 0 {
			t.Errorf("iot:* 应覆盖 %s", a)
		}
	}

	// 裸 * 同样覆盖全部
	d, _ = ParseDocument([]byte(`{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`))
	cs, _ = Compile("p", d, DefaultEnv())
	if cs.Statements[0].actions != (1<<uint(numActions))-1 {
		t.Error("裸 * 应覆盖全部动作")
	}

	// 未知动作被拒绝
	d, _ = ParseDocument([]byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`))
	if _, err := Compile("p", d, DefaultEnv()); err == nil {
		t.Error("未知动作应被拒绝")
	}
}

func TestCompiledSetTracksCertificateVariables(t *testing.T) {
	plain := mustCompile(t, "plain", `{"Statement":[{"Effect":"Allow",
	  "Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/${iot:ClientId}"}]}`)
	if plain.UsesCertificateVariables() {
		t.Fatal("只引用 ClientId 的策略被误标为证书变量策略")
	}
	for _, variable := range []string{
		"iot:Certificate.SerialNumber",
		"iot:Certificate.Subject.CommonName",
		"iot:Certificate.Issuer.Organization",
	} {
		cs := mustCompile(t, variable, `{"Statement":[{"Effect":"Allow",
		  "Action":"iot:Publish","Resource":"arn:aws:iot:*:*:topic/${`+variable+`}"}]}`)
		if !cs.UsesCertificateVariables() {
			t.Errorf("引用 %s 的策略未标记证书变量", variable)
		}
	}
}

// TestGlobMatchCrossesSlash 固定住一处语义陷阱：
// 策略资源中的 * 跨越 /，而 MQTT 的 + 不跨越。
// 混同两者会让 topic/a/* 意外只匹配一层。
func TestGlobMatchCrossesSlash(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything/with/slashes", true},
		{"a/*", "a/b", true},
		{"a/*", "a/b/c/d", true}, // 跨越 /
		{"a/*", "a/", true},
		{"a/*", "a", false},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "abcd", false},
		{"a*b*c", "axbyc", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "acb", false},
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"a??c", "abxc", true},
		{"", "", true},
		{"", "a", false},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"**", "anything", true},
		{"a**b", "ab", true},
	}
	for _, tc := range cases {
		if got := globMatch(tc.pattern, tc.s); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v，期望 %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

func TestTokenizeCollapsesStars(t *testing.T) {
	toks, err := tokenize("a**b")
	if err != nil {
		t.Fatal(err)
	}
	stars := 0
	for _, tk := range toks {
		if tk.kind == tokStar {
			stars++
		}
	}
	if stars != 1 {
		t.Errorf("连续的 * 应合并为一个，得到 %d 个", stars)
	}
}

func TestARNEnvMatching(t *testing.T) {
	env := Env{Partition: "aws", Region: "cn-east-1", Account: "123456789012"}
	ctx := &Context{ClientID: "c1"}

	cases := []struct {
		res  string
		want bool
	}{
		{"arn:aws:iot:cn-east-1:123456789012:client/c1", true},
		{"arn:aws:iot:*:*:client/c1", true},
		{"arn:*:*:*:*:client/c1", true},
		{"arn:aws:iot:cn-east-1:999999999999:client/c1", false},
		{"arn:aws:iot:us-west-2:123456789012:client/c1", false},
		{"arn:partition-x:iot:*:*:client/c1", false},
		{"*", true},
		{"arn:aws:iot:*:*:*", true},
		{"arn:aws:iot:cn-*:1234*:client/c1", true},
	}
	for _, tc := range cases {
		p, err := compileResource(tc.res)
		if err != nil {
			t.Fatalf("%q 编译失败: %v", tc.res, err)
		}
		if got := p.matches(env, "client", "c1", ctx, nil); got != tc.want {
			t.Errorf("%q 匹配 client/c1 = %v，期望 %v", tc.res, got, tc.want)
		}
	}
}

// TestResourceTypeIsExactMatch 固定住 PRD AUTH-023 的实现保证：
// 资源类型必须精确匹配，不接受通配。
//
// 若允许 topic* 这类模式，一条意在放行 topic/ 的语句会连带放行
// topicfilter/，使「允许订阅」与「允许接收」的区分失效。
func TestResourceTypeIsExactMatch(t *testing.T) {
	env := DefaultEnv()
	ctx := &Context{ClientID: "c1"}
	p, err := compileResource("arn:aws:iot:*:*:topic*/a")
	if err != nil {
		t.Fatal(err)
	}
	if p.matches(env, "topic", "a", ctx, nil) {
		t.Error("资源类型不应按通配匹配")
	}
	if p.matches(env, "topicfilter", "a", ctx, nil) {
		t.Error("资源类型不应按通配匹配")
	}
	// 精确类型则匹配
	p2, _ := compileResource("arn:aws:iot:*:*:topic/a")
	if !p2.matches(env, "topic", "a", ctx, nil) {
		t.Error("精确类型应匹配")
	}
	if p2.matches(env, "topicfilter", "a", ctx, nil) {
		t.Error("类型不同不应匹配")
	}
}

func TestCompileJSON(t *testing.T) {
	cs, err := CompileJSON("p", []byte(`{"Statement":[
	  {"Sid":"s1","Effect":"Allow","Action":"iot:Connect","Resource":"*"}]}`), DefaultEnv())
	if err != nil {
		t.Fatal(err)
	}
	if cs.Name != "p" || len(cs.Statements) != 1 || cs.Statements[0].Sid != "s1" {
		t.Errorf("得到 %+v", cs)
	}
	if _, err := CompileJSON("p", []byte(`{`), DefaultEnv()); err == nil {
		t.Error("非法文档应被拒绝")
	}
}

// TestAttrRefsCollectedAtCompileTime 验证策略引用的设备属性名在编译期被收集。
//
// 它服务于热视图只物化策略真正读取的属性（PRD CLU-053、REG-006）：
// 没有这份清单，控制面只能把全部属性塞进热视图，而百万设备下
// 那是几十 MB 的无用内存——绝大多数属性从不被策略引用。
func TestAttrRefsCollectedAtCompileTime(t *testing.T) {
	cs, err := CompileJSON("p", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.Attributes[site]}/*"},
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/${iot:Connection.Thing.Attributes[zone]}/${iot:Connection.Thing.Attributes[site]}/#"}
	]}`), DefaultEnv())
	if err != nil {
		t.Fatal(err)
	}
	got := cs.AttrRefs()
	// 去重且排序
	if len(got) != 2 || got[0] != "site" || got[1] != "zone" {
		t.Errorf("= %v，期望 [site zone]", got)
	}
	// 返回副本：外部修改不得影响已编译的策略
	got[0] = "tampered"
	if cs.AttrRefs()[0] == "tampered" {
		t.Error("AttrRefs 应返回副本")
	}

	// 不引用属性的策略返回空
	plain, err := CompileJSON("p2", []byte(
		`{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`), DefaultEnv())
	if err != nil {
		t.Fatal(err)
	}
	if n := len(plain.AttrRefs()); n != 0 {
		t.Errorf("不引用属性的策略应返回空，得到 %d 项", n)
	}
}
