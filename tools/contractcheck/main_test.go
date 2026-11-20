package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 本测试回答一个关键问题：这些契约检查真的会在违反时报错吗？
// 一个永远通过的检查器比没有检查更糟——它给人虚假的安全感。
// 每条规则都用一份故意违反的样例代码验证其确实触发。

// writePkg 在临时目录里写出一个包，返回模块根目录。
func writePkg(t *testing.T, rel, src string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, rel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "x.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPureDataRuleCatchesViolations(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"net.Conn 字段", `package p
import "net"
type State struct { Conn net.Conn }`, "外部类型 net.Conn"},
		{"通道字段", `package p
type State struct { Out chan int }`, "通道"},
		{"互斥锁字段", `package p
import "sync"
type State struct { Mu sync.Mutex }`, "外部类型 sync.Mutex"},
		{"函数字段", `package p
type State struct { Fn func() }`, "函数值"},
		{"指针字段", `package p
type State struct { Will *Will }
type Will struct { Topic string }`, "指针"},
		{"映射字段", `package p
type State struct { Attrs map[string]string }`, "映射"},
		{"接口字段", `package p
type State struct { Any any }`, "接口"},
		{"嵌套结构体中的指针", `package p
type State struct { Props Props }
type Props struct { Inner *int }`, "指针"},
		{"切片元素含指针", `package p
type State struct { Items []Item }
type Item struct { P *int }`, "指针"},
		{"未列入白名单的外部类型", `package p
import "time"
type State struct { At time.Time }`, "未列入白名单"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writePkg(t, "internal/session", tc.src)
			got := checkTypes(root, typeRule{
				pkgDir: "internal/session", types: []string{"State"},
				level: pureData, allowed: []string{"cluster.Epoch", "cluster.NodeID"},
				prdRef: "test",
			})
			if len(got) == 0 {
				t.Fatalf("应报出违反，源码:\n%s", tc.src)
			}
			if !strings.Contains(got[0].msg, tc.want) {
				t.Errorf("消息 %q 未包含 %q", got[0].msg, tc.want)
			}
		})
	}
}

func TestPureDataRuleAcceptsValidTypes(t *testing.T) {
	src := `package p
type Epoch = uint64
type State struct {
	ID      string
	Ver     uint8
	On      bool
	Payload []byte
	Subs    []Sub
	Nested  Inner
	Arr     [4]uint16
	E       cluster.Epoch
	N       cluster.NodeID
}
type Sub struct { Filter string; QoS uint8 }
type Inner struct { X int64 }`
	root := writePkg(t, "internal/session", src)
	got := checkTypes(root, typeRule{
		pkgDir: "internal/session", types: []string{"State"},
		level: pureData, allowed: []string{"cluster.Epoch", "cluster.NodeID"},
		prdRef: "test",
	})
	if len(got) != 0 {
		t.Errorf("合法的纯数据结构不应报错，得到 %+v", got)
	}
}

func TestPointerFreeRuleIsStricter(t *testing.T) {
	// pointerFree 在 pureData 之上还禁止切片与字符串
	cases := []struct {
		name, src, want string
	}{
		{"切片", `package p
type node struct { Kids []int32 }`, "切片"},
		{"字符串", `package p
type node struct { Seg string }`, "字符串"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writePkg(t, "internal/mqtt/topic", tc.src)
			got := checkTypes(root, typeRule{
				pkgDir: "internal/mqtt/topic", types: []string{"node"},
				level: pointerFree, prdRef: "test",
			})
			if len(got) == 0 || !strings.Contains(got[0].msg, tc.want) {
				t.Fatalf("应报出 %q，得到 %+v", tc.want, got)
			}
		})
	}
	// 同样的切片在 pureData 下是允许的，证明两档严格程度确有区别
	root := writePkg(t, "internal/session", `package p
type State struct { Kids []int32 }`)
	if got := checkTypes(root, typeRule{
		pkgDir: "internal/session", types: []string{"State"},
		level: pureData, prdRef: "test",
	}); len(got) != 0 {
		t.Errorf("pureData 下切片应被允许，得到 %+v", got)
	}
}

func TestPointerFreeRuleAcceptsArenaNode(t *testing.T) {
	src := `package p
type nodeIdx = int32
type segIdx = int32
type node struct {
	parent nodeIdx
	seg    segIdx
	subs   int32
	nchild int32
}
type sub struct {
	nodePrev int32
	handle   int32
	id       uint32
	qos      uint8
}`
	root := writePkg(t, "internal/mqtt/topic", src)
	if got := checkTypes(root, typeRule{
		pkgDir: "internal/mqtt/topic", types: []string{"node", "sub"},
		level: pointerFree, prdRef: "test",
	}); len(got) != 0 {
		t.Errorf("合法的 arena 节点不应报错，得到 %+v", got)
	}
}

func TestTypeRuleDetectsStaleRuleTable(t *testing.T) {
	// 规则表引用了不存在的类型时必须报错，否则契约会随重命名静默失效
	root := writePkg(t, "internal/session", "package p\ntype Other struct{}")
	got := checkTypes(root, typeRule{
		pkgDir: "internal/session", types: []string{"State"},
		level: pureData, prdRef: "test",
	})
	if len(got) == 0 || !strings.Contains(got[0].msg, "规则表已过期") {
		t.Fatalf("应报出规则表过期，得到 %+v", got)
	}
}

func TestSignatureRuleCatchesViolations(t *testing.T) {
	cases := []struct {
		name, src, want string
	}{
		{"参数含 context", `package p
import "context"
type View struct{}
func (v *View) CertStatus(ctx context.Context, fp string) int { return 0 }`,
			"context.Context"},
		{"返回值含 error", `package p
type View struct{}
func (v *View) Thing(name string) (int, error) { return 0, nil }`,
			"error"},
		{"值接收者同样受约束", `package p
import "context"
type View struct{}
func (v View) Revision(ctx context.Context) int { return 0 }`,
			"context.Context"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writePkg(t, "internal/meta", tc.src)
			got := checkSignatures(root, sigRule{
				pkgDir: "internal/meta", receiver: "View", prdRef: "test",
			})
			if len(got) == 0 || !strings.Contains(got[0].msg, tc.want) {
				t.Fatalf("应报出 %q，得到 %+v", tc.want, got)
			}
		})
	}
}

func TestSignatureRuleAcceptsValidView(t *testing.T) {
	src := `package p
type View struct{}
type Cert struct{}
func (v *View) Revision() uint64 { return 0 }
func (v *View) CertStatus(fp [32]byte) uint8 { return 0 }
func (v *View) Cert(fp [32]byte) (Cert, bool) { return Cert{}, false }
func (v *View) unexported(err error) {}`
	root := writePkg(t, "internal/meta", src)
	if got := checkSignatures(root, sigRule{
		pkgDir: "internal/meta", receiver: "View", prdRef: "test",
	}); len(got) != 0 {
		t.Errorf("合法签名不应报错，得到 %+v", got)
	}
}

func TestSignatureRuleDetectsStaleRuleTable(t *testing.T) {
	root := writePkg(t, "internal/meta", "package p\ntype Other struct{}")
	got := checkSignatures(root, sigRule{
		pkgDir: "internal/meta", receiver: "View", prdRef: "test",
	})
	if len(got) == 0 || !strings.Contains(got[0].msg, "规则表已过期") {
		t.Fatalf("应报出规则表过期，得到 %+v", got)
	}
}

func TestImportRuleCatchesViolations(t *testing.T) {
	root := writePkg(t, "internal/mqtt/packet", `package packet
import _ "`+modulePath+`/internal/meta"`)
	got := checkImports(root, importRule{
		dir:    "internal/mqtt",
		deny:   modulePath + "/internal/",
		allow:  []string{modulePath + "/internal/mqtt/"},
		prdRef: "test",
	})
	if len(got) == 0 || !strings.Contains(got[0].msg, "不得导入") {
		t.Fatalf("应报出非法导入，得到 %+v", got)
	}
}

func TestImportRuleAllowsSubpackages(t *testing.T) {
	root := writePkg(t, "internal/mqtt/packet", `package packet
import (
	_ "bytes"
	_ "`+modulePath+`/internal/mqtt/reason"
)`)
	if got := checkImports(root, importRule{
		dir:    "internal/mqtt",
		deny:   modulePath + "/internal/",
		allow:  []string{modulePath + "/internal/mqtt/"},
		prdRef: "test",
	}); len(got) != 0 {
		t.Errorf("mqtt 子包之间的导入与标准库应被允许，得到 %+v", got)
	}
}

func TestTimeNowRuleCatchesViolations(t *testing.T) {
	root := writePkg(t, "internal/session", `package p
import "time"
func f() int64 { return time.Now().UnixMilli() }`)
	got := checkTimeNow(root, timeRule{dir: "internal/session", prdRef: "test"})
	if len(got) == 0 || !strings.Contains(got[0].msg, "time.Now") {
		t.Fatalf("应报出 time.Now 调用，得到 %+v", got)
	}
}

func TestTimeNowRuleHonoursExemption(t *testing.T) {
	// 按 ARCH-029，例外须显式注释并说明用途。
	//
	// 多行豁免注释必须被识别：豁免总要写明理由，因而往往不止一行。
	// 分析器最初只回看紧邻上一行，导致仓库里合法的多行豁免被误报——
	// 这条用例就是为固定住那次修复而加的。
	for _, src := range []string{
		`package p
import "time"
func f() int64 {
	// contract:allow-time-now 用于保活超时判定
	return time.Now().UnixMilli()
}`,
		`package p
import "time"
func f() int64 {
	return time.Now().UnixMilli() // contract:allow-time-now 仅用于展示
}`,
		`package p
import "time"
func f() int64 {
	// contract:allow-time-now 读超时是 net.Conn 的绝对时间接口要求，
	// 仅用于超时判定，不参与任何排序或版本比较。
	return time.Now().UnixMilli()
}`,
		`package p
import "time"
func f() int64 {
	// 三行的豁免说明。
	// contract:allow-time-now 标记不在首行也应生效。
	// 第三行继续解释。
	return time.Now().UnixMilli()
}`,
	} {
		root := writePkg(t, "internal/session", src)
		if got := checkTimeNow(root, timeRule{
			dir: "internal/session", prdRef: "test",
		}); len(got) != 0 {
			t.Errorf("带豁免注释的调用不应报错，得到 %+v", got)
		}
	}
}

func TestTimeNowRuleIgnoresTestsAndMissingDirs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal/session")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 测试文件可以自由使用时钟
	if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte(`package p
import "time"
func f() { _ = time.Now() }`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := checkTimeNow(root, timeRule{dir: "internal/session", prdRef: "test"}); len(got) != 0 {
		t.Errorf("测试文件不应受约束，得到 %+v", got)
	}
	// 尚未创建的包不报错，便于规则表先行登记
	if got := checkTimeNow(root, timeRule{dir: "internal/authz", prdRef: "test"}); len(got) != 0 {
		t.Errorf("不存在的目录不应报错，得到 %+v", got)
	}
}

// TestRealCodebasePasses 保证规则表与真实代码保持一致：
// 若某条规则引用的类型被重命名或删除，上面的「规则表已过期」分支会触发。
func TestRealCodebasePasses(t *testing.T) {
	root := "../.."
	var all []finding
	for _, r := range typeRules {
		all = append(all, checkTypes(root, r)...)
	}
	for _, r := range sigRules {
		all = append(all, checkSignatures(root, r)...)
	}
	for _, r := range importRules {
		all = append(all, checkImports(root, r)...)
	}
	for _, r := range timeRules {
		all = append(all, checkTimeNow(root, r)...)
	}
	for _, f := range all {
		t.Errorf("%s:%d: %s（%s）", f.pos.Filename, f.pos.Line, f.msg, f.prdRef)
	}
}

func TestStrictnessString(t *testing.T) {
	if pureData.String() != "纯数据" || pointerFree.String() != "无指针" {
		t.Error("strictness.String 不正确")
	}
}

// 每个直接第三方依赖都必须有人管：要么被某条 import 规则隔离，
// 要么在豁免名单里显式说明为何不必隔离。
//
// 由来：冷层由 Pebble 改成 bbolt 之后，隔离规则仍只拦
// github.com/cockroachdb/，于是 ARCH-065 守着一个项目不再使用的库，
// 而真正在用的 bbolt 可以被任意包直接 import。门禁照常通过。
//
// 这是"规则表过期"的一类，与 TestTypeRuleDetectsStaleRuleTable 同一
// 用意：机制本身有测试不等于规则表写对了。新增依赖时这个测试会迫使
// 作者做一次决定——隔离它，还是写下为什么不用。
func TestEveryDirectDependencyIsAccountedFor(t *testing.T) {
	// 豁免名单。写进来须给出理由，不接受"暂时不管"。
	exempt := map[string]string{}

	deps := directDeps(t, "../..")
	if len(deps) == 0 {
		t.Fatal("没有解析出任何直接依赖，本测试无效")
	}
	for _, dep := range deps {
		if why, ok := exempt[dep]; ok {
			if why == "" {
				t.Errorf("%s 在豁免名单里但没写理由", dep)
			}
			continue
		}
		covered := false
		for _, r := range importRules {
			// deny 是前缀匹配，与 checkImports 的判定保持一致
			if strings.HasPrefix(dep, r.deny) {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("直接依赖 %s 既未被任何 import 规则隔离，也不在豁免名单里。\n"+
				"若它应当被关在某个包内，给 importRules 加一条；\n"+
				"若它可以在任何地方使用，加进本测试的 exempt 并写明理由。", dep)
		}
	}
}

// 每条 import 规则的 deny 前缀都必须真的能拦住东西。
//
// 一个拼错的前缀（少一个斜杠、域名写错）会让规则静默失效——它仍然
// 出现在规则表里，门禁仍然通过，而约束已经不存在了。这正是 bbolt
// 那次的形态。
func TestImportRuleDenyPrefixesAreEffective(t *testing.T) {
	root := t.TempDir()
	for _, r := range importRules {
		if strings.HasPrefix(r.deny, modulePath) {
			// 内部包的规则，前缀由 modulePath 拼出，不会拼错
			continue
		}
		// 造一个 import 该前缀的文件，规则必须报违反
		dir := filepath.Join(root, "probe")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		src := "package probe\n\nimport _ \"" + r.deny + "x/y\"\n"
		if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		got := checkImports(root, importRule{
			dir: "probe", deny: r.deny, prdRef: r.prdRef,
		})
		if len(got) == 0 {
			t.Errorf("规则 %q 拦不住 import %q —— 前缀可能写错了，规则已静默失效",
				r.prdRef, r.deny+"x/y")
		}
		_ = os.RemoveAll(dir)
	}
}

// directDeps 读 go.mod 的直接依赖。
func directDeps(t *testing.T, root string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	inBlock := false
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		}
		if !inBlock && !strings.HasPrefix(line, "require ") {
			continue
		}
		line = strings.TrimPrefix(line, "require ")
		// 间接依赖不受此约束：它们不出现在本项目的 import 里
		if strings.Contains(line, "// indirect") {
			continue
		}
		if f := strings.Fields(line); len(f) >= 2 && strings.Contains(f[0], ".") {
			out = append(out, f[0])
		}
	}
	return out
}
