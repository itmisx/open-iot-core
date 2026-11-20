package authz

import (
	"strings"
	"testing"
)

func TestParseDocumentAcceptsBothForms(t *testing.T) {
	// AWS 的策略文档对 Action 与 Resource 两种写法都接受，
	// SDK 示例与控制台生成的策略混用，必须都能解析。
	for _, src := range []string{
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		  "Action":"iot:Connect","Resource":"arn:aws:iot:r:a:client/c1"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		  "Action":["iot:Connect","iot:Publish"],
		  "Resource":["arn:aws:iot:r:a:client/c1","arn:aws:iot:r:a:topic/t"]}]}`,
	} {
		d, err := ParseDocument([]byte(src))
		if err != nil {
			t.Fatalf("解析失败: %v\n%s", err, src)
		}
		if len(d.Statement) != 1 {
			t.Errorf("语句数 = %d", len(d.Statement))
		}
	}
}

// TestParseDocumentRejectsUnsupported 固定住一条安全决策：
// 不支持的字段必须被显式拒绝，而不是静默忽略。
//
// 忽略一个本应收紧权限的 Condition 会把受限策略变成放行策略——
// 这是安全缺陷而非兼容性缺口。
func TestParseDocumentRejectsUnsupported(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"Condition", `{"Statement":[{"Effect":"Allow","Action":"iot:Connect",
		  "Resource":"*","Condition":{"Bool":{"aws:SecureTransport":"true"}}}]}`, "Condition"},
		{"NotAction", `{"Statement":[{"Effect":"Deny","NotAction":"iot:Connect",
		  "Action":"iot:Publish","Resource":"*"}]}`, "NotAction"},
		{"NotResource", `{"Statement":[{"Effect":"Deny","Action":"iot:Publish",
		  "Resource":"*","NotResource":"arn:aws:iot:r:a:topic/x"}]}`, "NotResource"},
		{"Principal", `{"Statement":[{"Effect":"Allow","Action":"iot:Connect",
		  "Resource":"*","Principal":{"AWS":"*"}}]}`, "Principal"},
		{"字段名拼错", `{"Statement":[{"Effect":"Allow","Action":"iot:Connect",
		  "Resouce":"*"}]}`, ""},
		{"Effect 非法", `{"Statement":[{"Effect":"Permit","Action":"iot:Connect",
		  "Resource":"*"}]}`, "Effect"},
		{"没有语句", `{"Version":"2012-10-17","Statement":[]}`, "没有语句"},
		{"缺少 Action", `{"Statement":[{"Effect":"Allow","Resource":"*"}]}`, ""},
		{"缺少 Resource", `{"Statement":[{"Effect":"Allow","Action":"iot:Connect"}]}`, ""},
		{"非法 JSON", `{`, ""},
		{"多余内容", `{"Statement":[{"Effect":"Allow","Action":"iot:Connect","Resource":"*"}]} {}`, ""},
		{"Action 为空值", `{"Statement":[{"Effect":"Allow","Action":,"Resource":"*"}]}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDocument([]byte(tc.src))
			if err == nil {
				t.Fatal("应被拒绝")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息 %q 应含 %q", err.Error(), tc.want)
			}
		})
	}
}

func TestStringListRoundTrip(t *testing.T) {
	var l StringList
	if err := l.UnmarshalJSON([]byte(`  "a"  `)); err != nil {
		t.Fatal(err)
	}
	if len(l) != 1 || l[0] != "a" {
		t.Errorf("得到 %v", l)
	}
	b, err := l.MarshalJSON()
	if err != nil || string(b) != `"a"` {
		t.Errorf("单元素应序列化为字符串，得到 %s, %v", b, err)
	}
	if err := l.UnmarshalJSON([]byte(`["a","b"]`)); err != nil {
		t.Fatal(err)
	}
	b, _ = l.MarshalJSON()
	if string(b) != `["a","b"]` {
		t.Errorf("多元素应序列化为数组，得到 %s", b)
	}
	for _, bad := range []string{``, `   `, `123`, `{`} {
		if err := l.UnmarshalJSON([]byte(bad)); err == nil {
			t.Errorf("%q 应被拒绝", bad)
		}
	}
}

func TestActionParsing(t *testing.T) {
	for _, n := range actionNames {
		a, ok := ParseAction(n)
		if !ok || a.String() != n {
			t.Errorf("%q 往返失败: %v %q", n, ok, a.String())
		}
		// 大小写不敏感
		if _, ok := ParseAction(strings.ToUpper(n)); !ok {
			t.Errorf("%q 的大写形式应被接受", n)
		}
	}
	if _, ok := ParseAction("iot:Nope"); ok {
		t.Error("未知动作应被拒绝")
	}
	if Action(99).String() != "iot:Unknown" || Action(99).mask() != 0 {
		t.Error("越界动作处理不正确")
	}
}

// TestActionResourceTypeMapping 固定住 PRD AUTH-023。
//
// iot:Subscribe 校验 topicfilter/ 资源，iot:Receive 校验 topic/ 资源。
// 把它们合并会让「允许订阅通配符」意外等价于「允许接收其下全部主题」，
// 或反过来让正常订阅被拒。
func TestActionResourceTypeMapping(t *testing.T) {
	for a, want := range map[Action]string{
		ActConnect:       "client",
		ActSubscribe:     "topicfilter",
		ActPublish:       "topic",
		ActReceive:       "topic",
		ActRetainPublish: "topic",
	} {
		if got := a.ResourceType(); got != want {
			t.Errorf("%s 的资源类型 = %q，期望 %q", a, got, want)
		}
	}
	if Action(99).ResourceType() != "" {
		t.Error("越界动作的资源类型应为空")
	}
}

func TestEffectString(t *testing.T) {
	if EffectAllow.String() != "Allow" || EffectDeny.String() != "Deny" {
		t.Error("Effect.String 不正确")
	}
}
