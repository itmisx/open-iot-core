package server

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

/*
使用教程的门禁（TST-142）。

教程落在「源文件在仓库里、产物也在仓库里」这个形状上：正文是
docs/tutorial/{zh,en}/ 下的 markdown，由 vite 以 ?raw 打进控制台产物，
而产物提交进仓库（UI-039）。本仓库已经为同一个形状付过代价——只改源码
不重建，跑起来的是旧界面（CLAUDE.md 第 1 条），而教程更容易这样：改一份
markdown 不像改 TSX，不会让人想起还有一步构建。

这里查六件事，全部只要 Go 工具链，所以进 make gate：

 1. include 的锚点都解析得到（UI-080）
 2. 两棵树的相对路径集合一致（UI-079）
 3. 同一页在两棵树里的代码块逐字相同（UI-079：代码不翻译）
 4. 页内的 #/guide/... 链接都指向真实存在的页（两棵树都要有）
 5. markdown 用的语法在子集之内（UI-078）
 6. 每一页的标题都出现在**已提交的**控制台产物里——这一条抓的是
    「改了教程没跑 make console」

第 6 条是其中最不像检查的一条，也是唯一抓得住产物过期的一条：它只认
标题，所以改正文而不重建抓不住，但加页、改标题、删页这些结构性改动一定
会被它拦下，而那正是「忘了重建」最常发生的时刻。
*/

const guideRel = "docs/tutorial"

// guideFile 是一份教程源文件解析出来的东西。
type guideFile struct {
	rel      string // zh/01-sdk/01-overview.md
	title    string
	includes []string // examples/sdk/golang/main.go#client
	codes    []string // 围栏代码块的正文，按出现顺序
	links    []string // #/guide/sdk/golang
}

var (
	guideIncludeRe = regexp.MustCompile(`<!--\s*include:\s*([^\s]+)\s*-->`)
	guideLinkRe    = regexp.MustCompile(`\]\((#/guide/[^)]*)\)`)
)

// readGuide 读整棵教程树。一棵树是空的就直接失败：那说明目录改过名字，
// 而后面每一条断言都会在空集合上"通过"（TST-134 的同一个反模式）。
func readGuide(t *testing.T) map[string]*guideFile {
	t.Helper()
	root := filepath.Join("..", "..", filepath.FromSlash(guideRel))
	out := map[string]*guideFile{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		out[filepath.ToSlash(rel)] = parseGuideFile(t, filepath.ToSlash(rel), string(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var zh, en int
	for rel := range out {
		switch {
		case strings.HasPrefix(rel, "zh/"):
			zh++
		case strings.HasPrefix(rel, "en/"):
			en++
		}
	}
	if zh == 0 || en == 0 {
		t.Fatalf("教程树读出来是空的（zh=%d en=%d），目录假设失效了", zh, en)
	}
	return out
}

func parseGuideFile(t *testing.T, rel, src string) *guideFile {
	t.Helper()
	f := &guideFile{rel: rel}
	lines := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	inFence := false
	var fence []string
	for n, raw := range lines {
		line := strings.TrimRight(raw, " \t")
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if inFence {
				f.codes = append(f.codes, strings.Join(fence, "\n"))
				fence = nil
			}
			inFence = !inFence
			continue
		}
		if inFence {
			fence = append(fence, line)
			continue
		}
		if m := guideIncludeRe.FindStringSubmatch(line); m != nil {
			f.includes = append(f.includes, m[1])
			continue
		}
		for _, m := range guideLinkRe.FindAllStringSubmatch(line, -1) {
			f.links = append(f.links, m[1])
		}
		if strings.HasPrefix(line, "# ") && f.title == "" {
			f.title = strings.TrimSpace(line[2:])
		}
		checkGuideSubset(t, rel, n+1, line)
	}
	if inFence {
		t.Errorf("%s: 代码块没有收尾的三个反引号", rel)
	}
	if f.title == "" {
		t.Errorf("%s: 缺一级标题，页标题取自它（UI-078）", rel)
	}
	return f
}

// checkGuideSubset 把子集之外的语法拦在合并之前（UI-078）。
//
// 渲染器遇到这些写法会给一条显眼的错误而不是静默丢弃，但那只在有人打开
// 那一页时才看得到——而最可能打开它的人正是写它的那个，他的浏览器里
// 刚好是对的。所以这条检查在门禁里。
func checkGuideSubset(t *testing.T, rel string, line int, s string) {
	t.Helper()
	bad := func(why string) {
		t.Errorf("%s:%d %s：%s", rel, line, why, s)
	}
	trimmed := strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(trimmed, "!["):
		bad("子集不支持图片（产物提交进仓库，图片会直接进 git 历史）")
	case strings.HasPrefix(trimmed, "* "), strings.HasPrefix(trimmed, "+ "):
		bad("无序列表统一用 \"- \"")
	case strings.HasPrefix(trimmed, "#### "):
		bad("子集只到三级标题；再深一层说明这一页该拆开")
	case trimmed == "---", strings.HasPrefix(trimmed, "***"):
		bad("子集不支持分隔线与 frontmatter；分段用二级标题")
	}
	// 成对标记必须闭合。渲染器把未闭合的标记当普通文字留着，于是界面上
	// 出现一对裸星号或一个反引号——而写的人在编辑器里看着是对的
	// （TST-137 是同一个毛病，只是那次在 i18n 词典里）。
	if strings.Count(s, "`")%2 != 0 {
		bad("行内代码的反引号没有闭合")
	}
	if strings.Count(s, "**")%2 != 0 {
		bad("强调的 ** 没有闭合")
	}
	// 内嵌 HTML 只放过 include 指令。<设备名> 这类占位符不受影响：
	// 这里拦的是 < 紧跟 ASCII 字母或斜杠的写法。
	if m := regexp.MustCompile(`<[a-zA-Z/]`).FindString(s); m != "" &&
		!strings.Contains(s, "<!-- include:") && !strings.Contains(s, "`") {
		bad("子集不支持内嵌 HTML")
	}
}

// TestGuideIncludesResolve include 的锚点都要解析得到（UI-080）。
//
// 示例代码不写在 markdown 里，而是来自 examples/ 下真实可编译的文件：
// 文档里的代码会腐坏而没有任何东西会发现——改了接口形状，markdown 里
// 那段照样好好地印在页面上。锚点解析不到时渲染器给的是错误而不是空串，
// 而这条检查让它在合并前就红。
func TestGuideIncludesResolve(t *testing.T) {
	files := readGuide(t)
	total := 0
	for _, f := range files {
		for _, spec := range f.includes {
			total++
			path, anchor, ok := strings.Cut(spec, "#")
			if !ok {
				t.Errorf("%s: include 要写成 路径#锚点，这里是 %q", f.rel, spec)
				continue
			}
			if !strings.HasPrefix(path, "examples/") {
				t.Errorf("%s: include 只能指向 examples/ 下的文件，这里是 %q", f.rel, path)
				continue
			}
			b, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(path)))
			if err != nil {
				t.Errorf("%s: include 的文件读不到：%v", f.rel, err)
				continue
			}
			body, err := guideRegion(string(b), anchor)
			if err != nil {
				t.Errorf("%s: %s 里 %v", f.rel, path, err)
				continue
			}
			if strings.TrimSpace(body) == "" {
				t.Errorf("%s: %s 的锚点 %s 之间是空的", f.rel, path, anchor)
			}
		}
	}
	if total == 0 {
		t.Fatal("一条 include 都没抽到，正则与 markdown 的写法对不上了")
	}
}

func guideRegion(src, anchor string) (string, error) {
	begin, end := "tutorial:begin "+anchor, "tutorial:end "+anchor
	i := strings.Index(src, begin)
	if i < 0 {
		return "", errString("找不到锚点 " + anchor + " 的 begin 标记")
	}
	rest := src[i+len(begin):]
	j := strings.Index(rest, end)
	if j < 0 {
		return "", errString("锚点 " + anchor + " 没有 end 标记")
	}
	return rest[:j], nil
}

type errString string

func (e errString) Error() string { return string(e) }

// TestGuideTreesMatch 两棵语言树的结构必须一致（UI-079）。
//
// 缺一边的表现是切到那种语言之后整章空白——而写的人多半只看自己读的
// 那一棵。同时比对代码块：代码与协议字面量在两棵树里必须逐字相同，
// 翻译它们就是写错。
func TestGuideTreesMatch(t *testing.T) {
	files := readGuide(t)
	paths := func(lang string) map[string]*guideFile {
		out := map[string]*guideFile{}
		for rel, f := range files {
			if strings.HasPrefix(rel, lang+"/") {
				out[strings.TrimPrefix(rel, lang+"/")] = f
			}
		}
		return out
	}
	zh, en := paths("zh"), paths("en")

	missing := func(a, b map[string]*guideFile) []string {
		var out []string
		for k := range a {
			if _, ok := b[k]; !ok {
				out = append(out, k)
			}
		}
		sort.Strings(out)
		return out
	}
	if m := missing(zh, en); len(m) > 0 {
		t.Errorf("英文树缺 %d 个文件：%v\n（切到英文之后这些章是空的）", len(m), m)
	}
	if m := missing(en, zh); len(m) > 0 {
		t.Errorf("中文树缺 %d 个文件：%v", len(m), m)
	}

	for rel, a := range zh {
		b, ok := en[rel]
		if !ok {
			continue
		}
		if len(a.codes) != len(b.codes) {
			t.Errorf("%s 的代码块数量两棵树不一致（zh=%d en=%d）", rel, len(a.codes), len(b.codes))
			continue
		}
		for i := range a.codes {
			if a.codes[i] != b.codes[i] {
				t.Errorf("%s 的第 %d 个代码块两棵树不一致。\nzh:\n%s\nen:\n%s\n"+
					"（代码与协议字面量不翻译——翻译它们就是写错）", rel, i+1, a.codes[i], b.codes[i])
			}
		}
		if len(a.includes) != len(b.includes) {
			t.Errorf("%s 的 include 条数两棵树不一致（zh=%d en=%d）", rel, len(a.includes), len(b.includes))
			continue
		}
		for i := range a.includes {
			if a.includes[i] != b.includes[i] {
				t.Errorf("%s 的第 %d 条 include 两棵树指向不同：zh=%s en=%s",
					rel, i+1, a.includes[i], b.includes[i])
			}
		}
	}
}

// TestGuideLinksResolve 页内的 #/guide/... 链接都要指向真实存在的页。
//
// 死链在教程里的代价比别处高：读者正处在"还不知道该找什么"的状态，点一下
// 落到一个空页之后，他没有第二个线索。两棵树都要有那一页——中文页链到一节
// 英文树里没有的内容时，切语言就断了。
func TestGuideLinksResolve(t *testing.T) {
	files := readGuide(t)
	exists := map[string]bool{} // lang/chapter/page
	for rel := range files {
		parts := strings.Split(strings.TrimSuffix(rel, ".md"), "/")
		if len(parts) != 3 || parts[2] == "_chapter" {
			continue
		}
		lang := parts[0]
		chapter := regexp.MustCompile(`^\d+-`).ReplaceAllString(parts[1], "")
		page := regexp.MustCompile(`^\d+-`).ReplaceAllString(parts[2], "")
		exists[lang+"/"+chapter+"/"+page] = true
	}
	if len(exists) == 0 {
		t.Fatal("没抽到任何页，路径假设失效了")
	}

	total := 0
	for rel, f := range files {
		for _, link := range f.links {
			total++
			target := strings.TrimPrefix(link, "#/guide/")
			if target == "" {
				continue // #/guide 是目录页，总是存在
			}
			seg := strings.Split(target, "/")
			if len(seg) != 2 {
				t.Errorf("%s: 教程链接要写成 #/guide/章/页，这里是 %q", rel, link)
				continue
			}
			for _, lang := range []string{"zh", "en"} {
				if !exists[lang+"/"+seg[0]+"/"+seg[1]] {
					t.Errorf("%s: 链接 %s 在 %s 树里没有对应的页", rel, link, lang)
				}
			}
		}
	}
	if total == 0 {
		t.Fatal("一条教程内链都没抽到，正则与 markdown 的写法对不上了")
	}

	// 控制台源码里的教程链接也要查。
	//
	// 两个「使用说明」抽屉链到教程的具体一页（UI-081），而那是 TSX 里的
	// 字面量：教程改一个文件名之后它照样编译、照样渲染，点下去才是空页。
	// 这与 TST-136 查的是同一种缺口——接线断了而语法和类型都对。
	tsxLink := regexp.MustCompile(`view: 'guide', chapter: '([\w-]+)', page: '([\w-]+)'`)
	hits := 0
	for _, view := range []string{"views/shadow.tsx", "views/policies.tsx"} {
		for _, m := range tsxLink.FindAllStringSubmatch(consoleSrc(t, view), -1) {
			hits++
			for _, lang := range []string{"zh", "en"} {
				if !exists[lang+"/"+m[1]+"/"+m[2]] {
					t.Errorf("%s 链到 #/guide/%s/%s，但 %s 树里没有这一页", view, m[1], m[2], lang)
				}
			}
		}
	}
	if hits == 0 {
		t.Fatal("TSX 里一条教程链接都没抽到，正则与写法对不上了（UI-081 要求抽屉链到教程）")
	}
}

// TestGuideEntryIsInTopbar 教程入口必须在标题栏里，且只有那一处
// （UI-076 修订）。
//
// 它属于「Go 侧看不见、浏览器里也不报错，但用户会卡住」的一类：入口从
// 标题栏掉回侧栏末尾之后，第一次来的人照样找不到它，而没有任何测试会
// 变红。
//
// 「只有那一处」与「在标题栏」同等重要：两个入口就是两份当前态
// （标题栏点亮、侧栏不点亮，或反过来），而它们不会一起改。
func TestGuideEntryIsInTopbar(t *testing.T) {
	app := consoleSrc(t, "app.tsx")
	topbar := slice(t, app, "function Topbar(", "function ShellControls(")
	nav := slice(t, app, "const NAV_GROUPS", "/**")

	if !strings.Contains(topbar, "view: 'guide'") {
		t.Error("标题栏里没有教程入口（UI-076 修订：它和语言、主题、退出同组）")
	}
	if !strings.Contains(topbar, "t('nav.guide')") {
		t.Error("教程入口没有文案（UI-076 修订）：图标按钮不给名字，读屏软件念到的是\"链接\"")
	}
	if !strings.Contains(topbar, `name="guide"`) {
		t.Error("教程入口没配图标（UI-064：同一个入口全站同一个图标）")
	}
	// aria-current 要给：没有它，人已经在教程页时这个入口看起来和别处
	// 一样，而侧栏的当前项是点亮的——同一个信号两处不一致。
	if !strings.Contains(topbar, "aria-current") {
		t.Error("教程入口没有 aria-current（UI-061：当前位置不能只靠别处的高亮推断）")
	}
	if strings.Contains(nav, "view: 'guide'") {
		t.Error("侧栏里还留着教程项：两处入口就是两份当前态，而它们不会一起改")
	}

	// 功能名是「使用教程」（UI-082）。这里钉的是命名决定而不是具体措辞：
	// 须含「教程」、且不得退回「系统教程」——「系统」在本仓库已指节点
	// 自身，系统教程读起来像"节点自带的教程"而不是"怎么用这套系统"。
	if m := regexp.MustCompile(`'nav\.guide': '([^']*)'`).FindStringSubmatch(consoleSrc(t, "i18n.ts")); m == nil {
		t.Error("i18n.ts 里抽不到 nav.guide 的中文文案")
	} else if !strings.Contains(m[1], "教程") || strings.Contains(m[1], "系统教程") {
		t.Errorf("教程入口的文案是 %q（UI-082：功能名为「使用教程」）", m[1])
	}

	// 项标题要中英齐备，否则英文界面上冒出中文。
	i18n := consoleSrc(t, "i18n.ts")
	zh, en, ok := strings.Cut(i18n, "const en: Dict = {")
	if !ok {
		t.Fatal("i18n.ts 的结构变了，找不到英文词典的起点")
	}
	for _, key := range []string{"nav.guide", "guide.title", "guide.onThisPage", "guide.link"} {
		if !strings.Contains(zh, "'"+key+"':") || !strings.Contains(en, "'"+key+"':") {
			t.Errorf("文案 %s 中英没有齐备", key)
		}
	}

	// 两个「使用说明」抽屉要链到教程（UI-081）：抽屉是当前上下文的清单，
	// 教程是完整路径，重叠的事实权威在教程。少了这条链接，两份说明就是
	// 两个看起来都像权威的来源。
	for _, view := range []string{"views/shadow.tsx", "views/policies.tsx"} {
		src := consoleSrc(t, view)
		if !strings.Contains(src, "view: 'guide'") {
			t.Errorf("%s 的使用说明没有链到教程（UI-081）", view)
		}
	}
}

// TestGuideBuiltIntoConsoleAssets 已提交的控制台产物里要有每一页的标题。
//
// 这是唯一抓得住「改了教程没跑 make console」的检查。产物提交进仓库
// （UI-039），而重建要 Node——门禁跑不了 Node，所以只能从另一头查：
// 产物里有没有这些字符串。
//
// 它抓不住"只改了正文"，这是刻意的取舍：抓得住的是加页、改标题、删页，
// 而那几种恰恰是忘了重建时最常发生的改动。
func TestGuideBuiltIntoConsoleAssets(t *testing.T) {
	bundle, err := os.ReadFile(filepath.Join("consoleassets", "dist", "assets", "app.js"))
	if err != nil {
		t.Fatalf("读控制台产物失败：%v\n（产物随仓库提交，UI-039）", err)
	}
	src := string(bundle)
	files := readGuide(t)
	for rel, f := range files {
		if !strings.Contains(src, f.title) {
			t.Errorf("产物里没有 %s 的标题 %q —— 跑一次 make console（教程正文由 vite 以 ?raw 打进产物）",
				rel, f.title)
		}
	}
}

// 口语化表述的拦截名单（UI-082、TST-146）。
//
// 名单是**穷举**而不是启发式：逐条都是 2026-10-31 那轮改写里真实去掉的
// 写法，不做"像不像口语"的猜测。理由是这条规则的判据本身是人读出来的，
// 而一个会猜的检查在这件事上只会制造两类噪音——误报让人加例外，漏报让
// 人以为规则有人看着。
//
// 它拦的是**回归**：教程会被反复修订，而口语化是改着改着回去的那一类，
// 单份 diff 里又看不出语体已经跑偏（同一页里混进一句口语，读起来只是
// "这句松一点"）。
var guideColloquial = []string{
	"拿一把钥匙", "一把钥匙", // 凭据就叫凭据：钥匙是比喻，而本仓库另有真的密钥文件
	"跑一遍",   // → 运行示例
	"靠猜",    // → 不应依靠推测
	"好像",    // 判定结果不能用"好像"陈述
	"看一眼",   // → 核对 / 查看
	"照抄",    // → 按相同形状追加
	"先读这一页", // 标题须是名词性短语
	"什么也不会发生",
}

// 章页标题同时是侧边章节树与页内目录的菜单项（UI-082）。菜单项用来扫视
// 定位，祈使句与疑问句在那个位置读起来像提示而不像索引。
var guideTitleBad = []string{"先读", "一遍", "在哪", "怎么", "吗"}

// TestGuideProseRegister 教程的菜单文案与正文须是书面技术语体（UI-082）。
//
// 这一条与本文件其余五项不同：它检查的不是接线而是语体，而语体错了不会
// 让任何东西坏掉——页面照样渲染，链接照样能点。代价出现在别处：教程是
// 本项目面向使用者的唯一正式文档，而"这是规范还是建议"在口语句里读不
// 出来；两棵树各自口语化的方式不同，切到英文之后又放大成语气不一致。
func TestGuideProseRegister(t *testing.T) {
	root := filepath.Join("..", "..", filepath.FromSlash(guideRel), "zh")
	seen := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		seen++
		rel := filepath.ToSlash(path)
		for _, bad := range guideColloquial {
			if strings.Contains(string(b), bad) {
				t.Errorf("%s 里有口语化表述 %q（UI-082：教程正文须为书面技术语体）", rel, bad)
			}
		}
		for _, line := range strings.Split(string(b), "\n") {
			h, ok := strings.CutPrefix(line, "# ")
			if !ok {
				continue
			}
			for _, bad := range guideTitleBad {
				if strings.Contains(h, bad) {
					t.Errorf("%s 的标题 %q 含 %q（UI-082：章页标题须是名词性短语，它同时是菜单项）",
						rel, h, bad)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("中文教程树读出来是空的，目录假设失效了")
	}

	// 控制台里教程相关的文案同受这条约束：入口名、副标题、空页与解析
	// 失败提示和正文是同一份文档的两头，只改一头就是语体分叉。
	i18n := consoleSrc(t, "i18n.ts")
	zh, _, ok := strings.Cut(i18n, "const en: Dict = {")
	if !ok {
		t.Fatal("i18n.ts 的结构变了，找不到英文词典的起点")
	}
	for _, line := range strings.Split(zh, "\n") {
		if !strings.Contains(line, "'guide.") && !strings.Contains(line, "'nav.guide'") {
			continue
		}
		for _, bad := range guideColloquial {
			if strings.Contains(line, bad) {
				t.Errorf("i18n.ts 的教程文案里有口语化表述 %q：%s", bad, strings.TrimSpace(line))
			}
		}
	}
}
