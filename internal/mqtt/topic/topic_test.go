package topic

import (
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func TestValidateName(t *testing.T) {
	l := DefaultLimits()
	ok := []string{
		"a", "a/b/c", "sport/tennis/player1", "中文/主题",
		"$aws/things/dev-001/shadow/update", "/", "a/", "/a", "a//b",
	}
	for _, s := range ok {
		if err := ValidateName([]byte(s), l); err != nil {
			t.Errorf("%q 应合法: %v", s, err)
		}
	}
	bad := []struct {
		name string
		code reason.Code
	}{
		{"", reason.TopicNameInvalid},
		{"a/+", reason.TopicNameInvalid},
		{"a/#", reason.TopicNameInvalid},
		{"+", reason.TopicNameInvalid},
		{"#", reason.TopicNameInvalid},
		{"a/b/c/d/e/f/g/h/i", reason.TopicNameInvalid}, // 8 个斜杠，超出 7
	}
	for _, tc := range bad {
		err := ValidateName([]byte(tc.name), l)
		if err == nil {
			t.Errorf("%q 应被拒绝", tc.name)
			continue
		}
		if !errors.Is(err, &reason.Error{Code: tc.code}) {
			t.Errorf("%q 的原因码 = %v，期望 %s", tc.name, err, tc.code)
		}
	}
	// 含 U+0000 与非法 UTF-8
	for _, b := range [][]byte{{'a', 0x00}, {0xC3}, {0xED, 0xA0, 0x80}} {
		if err := ValidateName(b, l); err == nil {
			t.Errorf("% x 应被拒绝", b)
		}
	}
	// 超长
	long := make([]byte, 257)
	for i := range long {
		long[i] = 'a'
	}
	if err := ValidateName(long, l); err == nil {
		t.Error("超长主题名应被拒绝")
	}
}

func TestValidateFilter(t *testing.T) {
	l := DefaultLimits()
	ok := []string{
		"a", "a/b", "#", "+", "+/+", "a/#", "a/+/b", "+/tennis/#",
		"$aws/things/+/shadow/update/delta", "/", "a//b", "sport/+",
		"$share/grp/a/b", "$share/grp/#", "$share/g/$aws/things/+/shadow/get",
	}
	for _, s := range ok {
		if err := ValidateFilter([]byte(s), l); err != nil {
			t.Errorf("%q 应合法: %v", s, err)
		}
	}
	bad := []string{
		"",
		"a#",        // # 未独占层级
		"#/a",       // # 不是最后一级
		"a/#/b",     // 同上
		"a+",        // + 未独占层级
		"+a",        //
		"a/b+/c",    //
		"$share",    // 不符合共享订阅形式
		"$share/g",  // 缺过滤器部分
		"$share//a", // 组名为空
		"$share/g/", // 过滤器部分为空仅在长度为 0 时非法；"/" 是合法过滤器
		"$share/a+b/x",
		"$share/a#b/x",
		"$share/a/b/x/y/z/w/v/u/t/s", // 过滤器部分 8 个斜杠，超出上限 7
	}
	for _, s := range bad {
		if err := ValidateFilter([]byte(s), l); err == nil {
			t.Errorf("%q 应被拒绝", s)
		}
	}
	// 组名含斜杠会被解析成更短的组名，故单独验证组名长度上限
	longShare := "$share/" + string(make([]byte, 129)) + "/a"
	if err := ValidateFilter([]byte(longShare), l); err == nil {
		t.Error("超长组名应被拒绝")
	}
}

func TestSharedFilterSlashesNotCounted(t *testing.T) {
	// $share/{name}/ 的两个斜杠不计入配额（PRD MQTT-090）
	l := DefaultLimits()
	f := []byte("$share/grp/a/b/c/d/e/f/g/h") // 过滤器部分恰好 7 个斜杠
	if err := ValidateFilter(f, l); err != nil {
		t.Errorf("共享订阅前缀的斜杠不应计入配额: %v", err)
	}
}

func TestParseShared(t *testing.T) {
	cases := []struct {
		in          string
		share, rest string
		ok          bool
	}{
		{"$share/grp/a/b", "grp", "a/b", true},
		{"$share/g/#", "g", "#", true},
		{"$share/g/", "g", "", true},
		{"$share/g", "", "", false},
		{"$share/", "", "", false},
		{"a/b", "", "", false},
		{"$shared/g/a", "", "", false},
	}
	for _, tc := range cases {
		share, rest, ok := ParseShared([]byte(tc.in))
		if ok != tc.ok {
			t.Errorf("%q 的 ok = %v，期望 %v", tc.in, ok, tc.ok)
			continue
		}
		if ok && (string(share) != tc.share || string(rest) != tc.rest) {
			t.Errorf("%q 解析为 (%q, %q)，期望 (%q, %q)",
				tc.in, share, rest, tc.share, tc.rest)
		}
	}
	if !IsShared([]byte("$share/g/a")) || IsShared([]byte("a")) {
		t.Error("IsShared 不正确")
	}
}

func TestHasWildcard(t *testing.T) {
	if !HasWildcard([]byte("a/+")) || !HasWildcard([]byte("#")) || HasWildcard([]byte("a/b")) {
		t.Error("HasWildcard 不正确")
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		filter, topic string
		spec, aws     bool
	}{
		// 精确匹配
		{"sport/tennis/player1", "sport/tennis/player1", true, true},
		{"sport/tennis/player1", "sport/tennis/player2", false, false},
		{"a", "a", true, true},
		{"a", "a/b", false, false},
		{"a/b", "a", false, false},

		// 多层通配符：# 是否匹配父层级，是本项目与 AWS 的分歧点（CMP-011）
		{"sport/#", "sport", true, false},
		{"sport/#", "sport/", true, true},
		{"sport/#", "sport/tennis", true, true},
		{"sport/#", "sport/tennis/player1", true, true},
		{"#", "a", true, true},
		{"#", "a/b/c", true, true},
		{"a/#", "a/b", true, true},

		// 单层通配符
		{"sport/+", "sport/tennis", true, true},
		{"sport/+", "sport/", true, true},
		{"sport/+", "sport", false, false},
		{"sport/+", "sport/tennis/player1", false, false},
		{"+", "a", true, true},
		{"+/+", "/finance", true, true},
		{"/+", "/finance", true, true},
		{"+", "/finance", false, false},
		{"+/tennis/#", "sport/tennis/player1", true, true},
		{"sport/+/player1", "sport/tennis/player1", true, true},

		// $ 开头的主题不被以通配符开头的过滤器匹配（MQTT-4.7.2）
		{"#", "$aws/things/dev/shadow/get", false, false},
		{"+/things/dev/shadow/get", "$aws/things/dev/shadow/get", false, false},
		{"$aws/#", "$aws/things/dev/shadow/get", true, true},
		{"$aws/things/+/shadow/get", "$aws/things/dev/shadow/get", true, true},
		{"$aws/+", "$aws/x", true, true},
		// $ 规则只作用于第一层
		{"$aws/things/#", "$aws/things/dev", true, true},

		// 空层级
		{"a//b", "a//b", true, true},
		{"a/+/b", "a//b", true, true},

		// 共享订阅按其内层过滤器匹配
		{"$share/grp/sport/#", "sport/tennis", true, true},
		{"$share/grp/sport/tennis", "sport/tennis", true, true},
		{"$share/grp/sport/tennis", "sport/x", false, false},
	}

	spec := DefaultLimits()
	aws := DefaultLimits()
	aws.AWSStrict = true

	for _, tc := range cases {
		if got := Match([]byte(tc.filter), []byte(tc.topic), spec); got != tc.spec {
			t.Errorf("规范模式 Match(%q, %q) = %v，期望 %v", tc.filter, tc.topic, got, tc.spec)
		}
		if got := Match([]byte(tc.filter), []byte(tc.topic), aws); got != tc.aws {
			t.Errorf("AWS 模式 Match(%q, %q) = %v，期望 %v", tc.filter, tc.topic, got, tc.aws)
		}
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		topic string
		class Class
		scope string
	}{
		{"$aws/things/dev-001/shadow/update", ClassDeviceScoped, "dev-001"},
		{"$aws/things/dev-001/jobs/get", ClassDeviceScoped, "dev-001"},
		{"$aws/things/dev-001/streams/s1/data/json", ClassDeviceScoped, "dev-001"},
		{"$aws/things/dev-001/defender/metrics/json", ClassDeviceScoped, "dev-001"},
		{"$aws/things/dev-001/tunnels/notify", ClassDeviceScoped, "dev-001"},
		{"$aws/certificates/create/json", ClassReservedExact, ""},
		{"$aws/provisioning-templates/t/provision/json", ClassReservedExact, ""},
		{"$aws/rules/myrule", ClassReservedExact, ""},
		{"$aws/events/presence/connected/c1", ClassReservedExact, ""},
		{"$aws/things", ClassReservedExact, ""},
		{"$aws/things/", ClassReservedExact, ""},
		{"$aws/things//shadow", ClassReservedExact, ""}, // thingName 为空
		{"$other/x", ClassReservedExact, ""},
		{"sensors/temp", ClassGeneral, ""},
		{"", ClassGeneral, ""},
		// 过滤器形式的通配 thingName 不是可路由的设备域主题
		{"$aws/things/+/shadow/update", ClassReservedExact, ""},
		{"$aws/things/#", ClassReservedExact, ""},
	}
	for _, tc := range cases {
		c, scope := Classify([]byte(tc.topic))
		if c != tc.class || string(scope) != tc.scope {
			t.Errorf("Classify(%q) = (%s, %q)，期望 (%s, %q)",
				tc.topic, c, scope, tc.class, tc.scope)
		}
	}
	if Class(99).String() != "Unknown" {
		t.Error("Class.String 对越界值应返回 Unknown")
	}
}

func TestScopeOfFilter(t *testing.T) {
	cases := []struct {
		filter string
		scope  string
		ok     bool
	}{
		{"$aws/things/dev-001/shadow/update/delta", "dev-001", true},
		{"$share/g/$aws/things/dev-001/jobs/notify", "dev-001", true},
		{"$aws/things/+/shadow/update", "", false},
		{"sensors/#", "", false},
		{"$aws/events/presence/connected/+", "", false},
	}
	for _, tc := range cases {
		scope, ok := ScopeOfFilter([]byte(tc.filter))
		if ok != tc.ok || string(scope) != tc.scope {
			t.Errorf("ScopeOfFilter(%q) = (%q, %v)，期望 (%q, %v)",
				tc.filter, scope, ok, tc.scope, tc.ok)
		}
	}
}
