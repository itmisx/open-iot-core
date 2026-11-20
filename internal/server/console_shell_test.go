package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// consoleSrc 读一份控制台源码。
//
// 这些测试读的是 TSX/TS 原文而不是构建产物：要钉的是跨语言的约定，
// 而产物里这些约定已经被编译没了。
func consoleSrc(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "web", "console", "src", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var i18nKeyRe = regexp.MustCompile(`(?m)^\s{2}'([^']+)':`)

// TestConsoleI18nDictionariesMatch 中英两份词典的键集合必须完全一致。
//
// 由来：这条规矩写在 CLAUDE.md 里，却没有任何东西检查它——此前只有
// 空状态那三十来个键被逐个点名。漏一个键的表现是英文界面上冒出一句
// 中文（makeT 缺键时回退到 zh），而写代码的人多半只看中文界面，
// 于是这种天窗只有母语不同的人才会发现。
//
// 逐键比对而不是只比个数：补一个键的同时删掉另一个，个数一样而两边
// 都错了。
func TestConsoleI18nDictionariesMatch(t *testing.T) {
	src := consoleSrc(t, "i18n.ts")
	zhPart, enPart, ok := strings.Cut(src, "const en: Dict = {")
	if !ok {
		t.Fatal("i18n.ts 的结构变了，找不到英文词典的起点")
	}
	keys := func(s string) map[string]bool {
		out := map[string]bool{}
		for _, m := range i18nKeyRe.FindAllStringSubmatch(s, -1) {
			out[m[1]] = true
		}
		return out
	}
	zh, en := keys(zhPart), keys(enPart)
	if len(zh) == 0 || len(en) == 0 {
		t.Fatalf("没抽到键（zh=%d en=%d），正则与文件格式对不上了", len(zh), len(en))
	}
	missing := func(a, b map[string]bool) []string {
		var out []string
		for k := range a {
			if !b[k] {
				out = append(out, k)
			}
		}
		sort.Strings(out)
		return out
	}
	if m := missing(zh, en); len(m) > 0 {
		t.Errorf("英文词典缺 %d 个键：%v\n（缺键会在英文界面上回退成中文）", len(m), m)
	}
	if m := missing(en, zh); len(m) > 0 {
		t.Errorf("中文词典缺 %d 个键：%v", len(m), m)
	}

	// 文案里不得有 markdown 标记。
	//
	// 由来：界面上这些字符串是直接塞进 <p> 的纯文本，没有任何 markdown
	// 渲染。写了 **强调** 的后果是用户看到两对星号——而写的人在编辑器里
	// 看着是对的。一轮里连犯三次之后加上这条。
	//
	// 只拦成对的 ** 与反引号：单个 * 是合法的（策略资源写成
	// $aws/things/<设备名>/*）。
	for _, m := range regexp.MustCompile(`(?m)^\s{2}'[^']+':\s*\n?\s*'([^']*)'`).FindAllStringSubmatch(src, -1) {
		if strings.Contains(m[1], "**") || strings.Contains(m[1], "`") {
			t.Errorf("文案里有 markdown 标记，界面上会原样显示：%s", m[1])
		}
	}

	// 词典里定义了却没人用的键，和用了却没定义的键，都是维护负担。
	// 这里只查后者——前者可能是别的分支正在用。
	var code strings.Builder
	err := filepath.Walk(filepath.Join("..", "..", "web", "console", "src"),
		func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".tsx") {
				return err
			}
			b, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			code.Write(b)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	// t('某个键') 的字面量调用。模板拼出来的键（t(`theme.${x}`)）抓不到，
	// 那种本来也没法静态检查。
	used := regexp.MustCompile(`\bt\('([a-z][\w.]+)'`).FindAllStringSubmatch(code.String(), -1)
	var undef []string
	for _, m := range used {
		if !zh[m[1]] {
			undef = append(undef, m[1])
		}
	}
	sort.Strings(undef)
	if len(undef) > 0 {
		t.Errorf("界面上用了未定义的文案键：%v\n（makeT 会把键名本身显示出来）", uniq(undef))
	}
}

func uniq(in []string) []string {
	out := in[:0:0]
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// TestConsoleShellConventions 钉住外壳的几条约定（UI-061 至 UI-064）。
//
// 它们都属于"Go 侧完全看不见、浏览器里也不报错，但用户会卡住"的一类：
// 登录页少一个语言开关不会让任何测试变红，只会让读不懂中文的人卡在
// 第一屏。
func TestConsoleShellConventions(t *testing.T) {
	app := consoleSrc(t, "app.tsx")

	// 1) 登录页必须有语言（与主题）开关。
	//
	// 它曾经只在登录之后的侧栏里：浏览器语言判错时，用户得先用一种
	// 读不懂的语言完成登录，才能找到切换的地方。
	gate := app[strings.Index(app, "function Gate("):]
	if strings.Index(app, "function Gate(") < 0 {
		t.Fatal("app.tsx 里找不到 Gate 组件，这个测试的切片假设失效了")
	}
	if !strings.Contains(gate, "ShellControls") || !strings.Contains(gate, "onLang") {
		t.Error("登录页没有语言/主题开关（UI-062）：第一屏切不了语言，读不懂的人只能卡在这里")
	}

	// 2) 节点就绪不得只由颜色表达。
	//
	// 原先是一个 9px 的圆点加 aria-hidden，颜色是它携带的全部信息——
	// 色觉差异的用户、黑白截图、以及任何一张贴进工单的图里都读不出来。
	// 而这恰恰是出事时第一个要看的信号。
	for _, k := range []string{"topbar.ready", "topbar.notReady"} {
		if !strings.Contains(app, "'"+k+"'") {
			t.Errorf("标题栏没有引用 %s（UI-061）：就绪状态仍然只靠颜色", k)
		}
	}

	// 3) 退出在标题栏右侧，不在侧栏。
	//
	// 侧栏在窄屏下会变成横向滚动条，底部那组控件会被滚出可视区——
	// "怎么退出登录"于是要靠横向拖动才找得到。
	topbar := slice(t, app, "function Topbar(", "function ShellControls(")
	sidebar := slice(t, app, "function Sidebar(", "function Gate(")
	if !strings.Contains(topbar, "'common.signout'") {
		t.Error("退出不在标题栏里（UI-063）")
	}
	if strings.Contains(sidebar, "'common.signout'") {
		t.Error("侧栏里还留着退出：两处都能退出会让人不确定哪个才算数")
	}

	// 4) 导航必须分组，且每一项都有图标。
	//
	// 少一个图标的表现是那一行文字左边空一块、整列对不齐，而 Go 侧
	// 看不见；加了页面忘了配图标正是最容易漏的一步。
	nav := slice(t, app, "const NAV_GROUPS", "/**")
	views := strings.Count(nav, "view: '")
	icons := strings.Count(nav, "icon: '")
	if views == 0 {
		t.Fatal("NAV_GROUPS 里没有导航项，切片假设失效了")
	}
	if views != icons {
		t.Errorf("导航有 %d 项却只有 %d 个图标（UI-064）", views, icons)
	}
	groups := strings.Count(nav, "key: 'navgroup.")
	if groups < 3 {
		t.Errorf("导航只有 %d 个分组：十项平铺时「策略」与「策略调试器」这类相邻项读起来一样", groups)
	}
	// 组标题也是文案，必须中英齐备——否则英文界面上冒出中文组名。
	i18n := consoleSrc(t, "i18n.ts")
	zh, en, _ := strings.Cut(i18n, "const en: Dict = {")
	for _, m := range regexp.MustCompile(`key: '(navgroup\.[\w.]+)'`).FindAllStringSubmatch(nav, -1) {
		if !strings.Contains(zh, "'"+m[1]+"':") || !strings.Contains(en, "'"+m[1]+"':") {
			t.Errorf("分组文案 %s 中英没有齐备", m[1])
		}
	}

	// 5) 图标是自己画的，不引图标库（LANG-019）。
	if strings.Contains(app, "lucide") || strings.Contains(app, "react-icons") ||
		strings.Contains(app, "@heroicons") {
		t.Error("引了图标库：控制台的依赖纪律是只有 react 与 react-dom")
	}
}

// slice 取 app.tsx 里两个锚点之间的一段，锚点不在就直接失败。
//
// 找不到就 Fatal 而不是返回空串：空串会让后面的 Contains 断言全部
// "通过"，于是组件改名之后这些检查静默失效（TST-116 记过这个反模式）。
func slice(t *testing.T, src, from, to string) string {
	t.Helper()
	i := strings.Index(src, from)
	if i < 0 {
		t.Fatalf("找不到锚点 %q，切片假设失效了", from)
	}
	rest := src[i:]
	j := strings.Index(rest, to)
	if j < 0 {
		t.Fatalf("在 %q 之后找不到锚点 %q", from, to)
	}
	return rest[:j]
}

// TestConsoleAPIBindingsAreAllUsed api.ts 导出的每个接口方法都要有人调用。
//
// 由来：服务端早就支持命名影子（SHDW-002、SHDW-043），`api.ts` 里
// `listShadows` 也写好了，而**没有任何视图调用它**——一台用命名影子的
// 设备在控制台上只看得到经典那一个。后端做完、前端忘了接，是这个项目
// 的典型缺口形状（后端先行），而它不会让任何测试变红：那段代码语法正确、
// 类型正确、只是没人用。
//
// 这条与 i18n 那条方向相反：那条查"用了没定义"，这条查"定义了没人用"。
// 两头都堵上，前后端之间那层接线才算有人看着。
//
// 判定留了余地：调用写成 `api\n  .foo(...)` 的链式形式也算（视图里到处
// 都是这种写法）。真的不再需要某个方法时，正确做法是删掉它而不是放着——
// 一个没人调用的接口绑定，读代码的人分不清是"忘了接"还是"留着备用"。
func TestConsoleAPIBindingsAreAllUsed(t *testing.T) {
	apiSrc := consoleSrc(t, "api.ts")
	i := strings.Index(apiSrc, "export const api = {")
	if i < 0 {
		t.Fatal("api.ts 里找不到 api 对象，这个测试的切片假设失效了")
	}
	keys := regexp.MustCompile(`(?m)^  (\w+):`).FindAllStringSubmatch(apiSrc[i:], -1)
	if len(keys) == 0 {
		t.Fatal("没抽到任何接口方法，正则与文件格式对不上了")
	}

	// 调用方：src 下除 api.ts 以外的全部 ts/tsx
	var callers strings.Builder
	base := filepath.Join("..", "..", "web", "console", "src")
	err := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if filepath.Base(path) == "api.ts" {
			return nil
		}
		if !strings.HasSuffix(path, ".ts") && !strings.HasSuffix(path, ".tsx") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		callers.Write(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	code := callers.String()

	var unused []string
	for _, m := range keys {
		name := m[1]
		if !regexp.MustCompile(`\bapi\s*\.\s*` + regexp.QuoteMeta(name) + `\s*\(`).MatchString(code) {
			unused = append(unused, name)
		}
	}
	sort.Strings(unused)
	if len(unused) > 0 {
		t.Errorf("api.ts 里这些方法没有任何视图调用：%v\n"+
			"要么是后端做完了而界面漏接，要么是已经不需要了——后者请直接删掉。",
			unused)
	}
}

// TestConsoleStringsGoThroughI18n 界面文案不得写死在 TSX 里。
//
// 由来：策略调试器上有三句 hint 直接写成了中文字面量，于是英文界面上
// 那三行是中文。这类遗漏逐键比对查不出来——它压根没进词典，而写代码
// 的人只看中文界面，一切正常。
//
// 只查 JSX 属性里的中日韩字符：正文里的中文注释是本仓库要求的风格，
// 不能一起拦下。这不覆盖 {'中文'} 这种写法，但属性是实测出问题的地方。
func TestConsoleStringsGoThroughI18n(t *testing.T) {
	base := filepath.Join("..", "..", "web", "console", "src")
	attr := regexp.MustCompile(`(?:hint|label|placeholder|title|aria-label)="[^"]*\p{Han}[^"]*"`)
	err := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".tsx") {
			return err
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, m := range attr.FindAllString(string(b), -1) {
			rel, _ := filepath.Rel(base, path)
			t.Errorf("%s 里的文案没走 i18n：%s\n（英文界面上它会原样显示中文）", rel, m)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestConsoleRuntimeDepsStayTwo 控制台的运行时依赖只有 react 与 react-dom。
//
// 这条纪律（LANG-019）此前只写在文档里。它要拦的不是"引了个库"，
// 而是这个产物**提交进仓库**（UI-039）：一次失手把几百 KB 写进 git
// 历史，之后谁都删不掉。
func TestConsoleRuntimeDepsStayTwo(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "web", "console", "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pkg struct {
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(b, &pkg); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"react": true, "react-dom": true}
	var extra []string
	for name := range pkg.Dependencies {
		if !allowed[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("控制台多了运行时依赖 %v。\nLANG-019 要求依赖为自己的体积换来相称的东西；"+
			"路由与数据获取各约 40/80 行手写就够了", extra)
	}
	if len(pkg.Dependencies) != 2 {
		t.Errorf("运行时依赖 = %d 个，期望 react 与 react-dom 两个", len(pkg.Dependencies))
	}
}
