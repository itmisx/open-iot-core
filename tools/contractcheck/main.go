// Command contractcheck 机械检查 docs/prd/02-architecture.md 定义的架构契约。
//
// 契约写下来不等于守得住。真正让契约不被侵蚀的是 CI 里的静态检查——
// 人会在赶进度时在 CONNECT 路径上加一发数据库查询，lint 不会。
// 本工具是 ARCH-052 至 ARCH-058 中可静态判定的那几条的实现。
//
// 它只依赖标准库：所有规则都在语法层面可判定，不需要类型信息，
// 因此不引入 golang.org/x/tools（见 PRD LANG-019 的依赖纪律）。
//
// 用法：
//
//	go run ./tools/contractcheck
//	go run ./tools/contractcheck -root .
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/itmisx/open-iot-core"

// strictness 决定类型白名单的严格程度。
type strictness int

const (
	// pureData 要求可序列化：禁止指针、通道、函数、映射、接口，
	// 以及 sync、net、unsafe 等包的类型。切片与字符串允许。
	pureData strictness = iota
	// pointerFree 在 pureData 之上进一步禁止一切含指针的表示，
	// 包括切片、字符串、映射——GC 扫描成本必须与元素数无关。
	pointerFree
)

func (s strictness) String() string {
	if s == pointerFree {
		return "无指针"
	}
	return "纯数据"
}

// typeRule 要求某个包里的某些命名类型满足给定的严格程度。
type typeRule struct {
	pkgDir  string
	types   []string
	level   strictness
	allowed []string // 允许的外部限定类型，如 cluster.Epoch
	prdRef  string
}

// sigRule 要求某个接收者上的导出方法签名不含 context.Context 与 error。
type sigRule struct {
	pkgDir   string
	receiver string
	prdRef   string
}

// importRule 禁止某个目录子树导入匹配 deny 前缀的包。
type importRule struct {
	dir   string
	deny  string
	allow []string
	// exceptDirs 是豁免的目录子树（相对仓库根）。
	//
	// 用于「全仓库禁止，仅某一处允许」的规则：把第三方库关在一个
	// 实现体里，上层只见本项目的类型（LANG-015 对共识库、
	// ARCH-062 对 YAML 库都是这个形态）。
	exceptDirs []string
	prdRef     string
}

// timeRule 禁止某个目录子树直接调用 time.Now。
type timeRule struct {
	dir    string
	prdRef string
}

// 契约规则表。新增契约时在此处登记，而不是靠评审记忆。
var (
	typeRules = []typeRule{{
		pkgDir:  "internal/session",
		types:   []string{"State"},
		level:   pureData,
		allowed: []string{"cluster.Epoch", "cluster.NodeID"},
		prdRef:  "ARCH-018/ARCH-053（契约 04：会话状态必须是纯数据）",
	}, {
		pkgDir: "internal/mqtt/topic",
		types:  []string{"node", "sub"},
		level:  pointerFree,
		prdRef: "ARCH-042/ARCH-058（契约 13：订阅树节点不得含指针）",
	}}

	sigRules = []sigRule{{
		pkgDir:   "internal/meta",
		receiver: "View",
		prdRef:   "ARCH-021/ARCH-054（契约 05：视图读取不触网、不失败）",
	}}

	importRules = []importRule{{
		dir:    "internal/mqtt",
		deny:   modulePath + "/internal/",
		allow:  []string{modulePath + "/internal/mqtt/"},
		prdRef: "ARCH-052（协议层须可独立 fuzz 与开源）",
	}, {
		// YAML 库只允许出现在 internal/config 内。
		//
		// 它是本项目唯一的第三方依赖，而 v3 分支已是"仅安全修复"状态，
		// 迁到 v4 或换库是可预见的事。把它关在一个包里，使那次迁移
		// 只改一个文件，而不是遍布全仓库的结构体 tag。
		dir:        ".",
		deny:       "go.yaml.in/",
		exceptDirs: []string{"internal/config"},
		prdRef:     "ARCH-062/LANG-020（第三方 YAML 库须关在 internal/config 内）",
	}, {
		// 冷层存储引擎只允许出现在 internal/store 内。
		//
		// 关在一个包里有两重作用：让"换存储引擎"保持为模块级改动
		// （与 LANG-015 对共识库的约束同一模式），以及防止引擎自带的
		// 类型（Tx、Bucket、Batch、Iterator、WriteOptions）渗进业务
		// 代码的函数签名——一旦渗进去，替换就不再是模块级的了。
		//
		// 这条规则的价值在 Pebble 改 bbolt 时兑现过一次：实际代价是
		// 一个文件（见 LANG-021 的修订）。
		dir:        ".",
		deny:       "go.etcd.io/bbolt",
		exceptDirs: []string{"internal/store"},
		prdRef:     "ARCH-065/LANG-021（冷层存储引擎须关在 internal/store 内）",
	}, {
		// 共识库只允许出现在 internal/cluster 与 internal/raftstore 内
		// （LANG-015：上层对共识实现零感知，可模块级替换）。
		//
		// 两个包而不是一个，是一条既存依赖方向逼出来的：internal/store
		// 自己 import 了 internal/cluster（会话围栏的类型），所以
		// LogStore/StableStore 的实现不能放在 cluster 里——那会成环。
		// internal/raftstore 是这两个接口与 store.KV 之间的粘合层。
		//
		// 规则按 github.com/hashicorp/ 整个前缀拦，而且**没有豁免目录**：
		// 2026-11-19 换到 dragonboat 之后，这套库已经整条退场
		// （LANG-014 第二次修订）。规则留着是为了让它别再回来——
		// 一个号称只有三个直接依赖的项目，不该在某个包里悄悄冒出一个
		// 第三方日志库或编码库，而那些正是跟着 raft 一起进来的。
		dir:    ".",
		deny:   "github.com/hashicorp/",
		prdRef: "LANG-014（hashicorp/raft 已退场，不得再引入）",
	}, {
		// dragonboat 只允许出现在 internal/cluster 内
		// （LANG-014 第二次修订、LANG-015）。
		//
		// `internal/raftstore` 不在豁免里，因为它已经随换库一起退场：
		// dragonboat 自带 LogDB，本项目不再实现日志存储（LANG-023 修订）。
		//
		// 按 github.com/lni/ 整个前缀拦：它的 goutils 与 vfs 是同一次
		// 引入带进来的，散到别处的代价与共识库本身一样。
		//
		// 这条规则是补上的——加完依赖之后 contractcheck 照样通过，因为
		// 规则表里没有它。这正是 ARCH-070 写下的那个形状：机制有测试，
		// 不等于规则表写对了。
		dir:        ".",
		deny:       "github.com/lni/",
		exceptDirs: []string{"internal/cluster"},
		prdRef:     "LANG-014/LANG-015（共识库须关在复制组实现内）",
	}, {
		// Pebble 同样只允许出现在 internal/store 内。
		//
		// 当前冷层是 bbolt。Pebble 随 dragonboat 进来做 Raft 日志后端
		// （LANG-014 第二次修订），但它是**传递依赖**——dragonboat 内部
		// 用它，本项目自己不 import。这条规则因此拦的是"有人直接拿它
		// 当第二个本地存储引擎用"，而不是它在不在构建图里。
		//
		// 两条分开写而不是合并成一个前缀列表：它们的理由不同，
		// 合并之后违反报告说不清到底违反了哪一条约束。
		dir:        ".",
		deny:       "github.com/cockroachdb/",
		exceptDirs: []string{"internal/store"},
		prdRef:     "ARCH-065/LANG-014（Pebble 须关在 internal/store 内）",
	}}

	timeRules = []timeRule{
		{dir: "internal/mqtt", prdRef: "ARCH-029/ARCH-055（契约 08：禁用墙上时钟排序）"},
		{dir: "internal/session", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/cluster", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/meta", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/router", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/broker", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/events", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/quota", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/authz", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/pki", prdRef: "ARCH-029/ARCH-055"},
		{dir: "internal/shadow", prdRef: "ARCH-029/ARCH-055"},
	}
)

// allowTimeNow 是豁免注释。按 ARCH-029，例外须显式注释并说明用途。
const allowTimeNow = "contract:allow-time-now"

// 允许出现在纯数据结构里的内建类型。
var builtins = map[string]bool{
	"bool": true, "string": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"byte": true, "rune": true, "float32": true, "float64": true,
	"complex64": true, "complex128": true,
}

// 预声明的接口类型。它们是标识符而非 InterfaceType 节点，
// 因此须单独识别，否则会落进「无法解析的类型」分支，
// 结果虽然仍是拒绝，但报出的原因不准确。
var predeclaredIfaces = map[string]bool{
	"any": true, "error": true, "comparable": true,
}

// 预声明但不可作为纯数据的类型。
var predeclaredUnsafe = map[string]string{
	"uintptr": "指针数值",
}

type finding struct {
	pos    token.Position
	msg    string
	prdRef string
}

func main() {
	root := flag.String("root", ".", "模块根目录")
	flag.Parse()

	var found []finding
	for _, r := range typeRules {
		found = append(found, checkTypes(*root, r)...)
	}
	for _, r := range sigRules {
		found = append(found, checkSignatures(*root, r)...)
	}
	for _, r := range importRules {
		found = append(found, checkImports(*root, r)...)
	}
	for _, r := range timeRules {
		found = append(found, checkTimeNow(*root, r)...)
	}

	if len(found) == 0 {
		fmt.Println("contractcheck: 通过")
		return
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].pos.Filename != found[j].pos.Filename {
			return found[i].pos.Filename < found[j].pos.Filename
		}
		return found[i].pos.Line < found[j].pos.Line
	})
	for _, f := range found {
		fmt.Fprintf(os.Stderr, "%s:%d:%d: %s\n    违反 %s\n",
			f.pos.Filename, f.pos.Line, f.pos.Column, f.msg, f.prdRef)
	}
	fmt.Fprintf(os.Stderr, "\ncontractcheck: 发现 %d 处契约违反\n", len(found))
	os.Exit(1)
}

// parsePkg 解析一个目录下的非测试 Go 文件。
func parsePkg(fset *token.FileSet, dir string) (map[string]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make(map[string]*ast.File)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		files[path] = f
	}
	return files, nil
}

// collectTypes 收集包内所有命名类型的定义表达式。
func collectTypes(files map[string]*ast.File) map[string]ast.Expr {
	out := make(map[string]ast.Expr)
	for _, f := range files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, sp := range gd.Specs {
				ts, ok := sp.(*ast.TypeSpec)
				if !ok {
					continue
				}
				out[ts.Name.Name] = ts.Type
			}
		}
	}
	return out
}

func checkTypes(root string, r typeRule) []finding {
	fset := token.NewFileSet()
	dir := filepath.Join(root, r.pkgDir)
	files, err := parsePkg(fset, dir)
	if err != nil {
		return []finding{{msg: fmt.Sprintf("无法解析 %s: %v", dir, err), prdRef: r.prdRef}}
	}
	defs := collectTypes(files)
	allowed := make(map[string]bool, len(r.allowed))
	for _, a := range r.allowed {
		allowed[a] = true
	}

	var out []finding
	for _, name := range r.types {
		def, ok := defs[name]
		if !ok {
			out = append(out, finding{
				msg:    fmt.Sprintf("契约要求检查的类型 %s 不存在，规则表已过期", name),
				prdRef: r.prdRef,
			})
			continue
		}
		c := &typeChecker{
			fset: fset, defs: defs, allowed: allowed,
			level: r.level, prdRef: r.prdRef, seen: map[string]bool{},
		}
		c.walk(def, name)
		out = append(out, c.found...)
	}
	return out
}

type typeChecker struct {
	fset    *token.FileSet
	defs    map[string]ast.Expr
	allowed map[string]bool
	level   strictness
	prdRef  string
	seen    map[string]bool
	found   []finding
}

func (c *typeChecker) reject(pos token.Pos, path, what string) {
	c.found = append(c.found, finding{
		pos:    c.fset.Position(pos),
		msg:    fmt.Sprintf("%s 含 %s，不满足%s要求", path, what, c.level),
		prdRef: c.prdRef,
	})
}

func (c *typeChecker) walk(e ast.Expr, path string) {
	switch t := e.(type) {
	case *ast.Ident:
		if builtins[t.Name] {
			if c.level == pointerFree && t.Name == "string" {
				c.reject(t.Pos(), path, "字符串")
			}
			return
		}
		if predeclaredIfaces[t.Name] {
			c.reject(t.Pos(), path, "接口")
			return
		}
		if what, ok := predeclaredUnsafe[t.Name]; ok {
			c.reject(t.Pos(), path, what)
			return
		}
		if c.seen[t.Name] {
			return // 递归类型：已检查过
		}
		def, ok := c.defs[t.Name]
		if !ok {
			// 同包内找不到定义，通常是类型别名指向外部类型；保守地拒绝。
			c.reject(t.Pos(), path, fmt.Sprintf("无法解析的类型 %s", t.Name))
			return
		}
		c.seen[t.Name] = true
		c.walk(def, path)

	case *ast.SelectorExpr:
		q := exprString(t)
		if c.allowed[q] {
			return
		}
		c.reject(t.Pos(), path, fmt.Sprintf("外部类型 %s（未列入白名单）", q))

	case *ast.ArrayType:
		if t.Len == nil && c.level == pointerFree {
			c.reject(t.Pos(), path, "切片")
			return
		}
		c.walk(t.Elt, path+"[]")

	case *ast.StructType:
		for _, f := range t.Fields.List {
			sub := path
			if len(f.Names) > 0 {
				sub = path + "." + f.Names[0].Name
			}
			c.walk(f.Type, sub)
		}

	case *ast.StarExpr:
		c.reject(t.Pos(), path, "指针")
	case *ast.MapType:
		c.reject(t.Pos(), path, "映射")
	case *ast.ChanType:
		c.reject(t.Pos(), path, "通道")
	case *ast.FuncType:
		c.reject(t.Pos(), path, "函数值")
	case *ast.InterfaceType:
		c.reject(t.Pos(), path, "接口")
	default:
		c.reject(e.Pos(), path, fmt.Sprintf("无法判定的类型 %T", e))
	}
}

func checkSignatures(root string, r sigRule) []finding {
	fset := token.NewFileSet()
	dir := filepath.Join(root, r.pkgDir)
	files, err := parsePkg(fset, dir)
	if err != nil {
		return []finding{{msg: fmt.Sprintf("无法解析 %s: %v", dir, err), prdRef: r.prdRef}}
	}
	var out []finding
	checked := 0
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) == 0 {
				continue
			}
			if recvTypeName(fd.Recv.List[0].Type) != r.receiver {
				continue
			}
			if !fd.Name.IsExported() {
				continue
			}
			checked++
			if fd.Type.Params != nil {
				for _, p := range fd.Type.Params.List {
					if exprString(p.Type) == "context.Context" {
						out = append(out, finding{
							pos: fset.Position(p.Pos()),
							msg: fmt.Sprintf("%s.%s 的参数含 context.Context；该签名承诺本地内存读取，不得触网",
								r.receiver, fd.Name.Name),
							prdRef: r.prdRef,
						})
					}
				}
			}
			if fd.Type.Results != nil {
				for _, res := range fd.Type.Results.List {
					if exprString(res.Type) == "error" {
						out = append(out, finding{
							pos: fset.Position(res.Pos()),
							msg: fmt.Sprintf("%s.%s 的返回值含 error；该签名承诺读取不会失败",
								r.receiver, fd.Name.Name),
							prdRef: r.prdRef,
						})
					}
				}
			}
		}
	}
	if checked == 0 {
		out = append(out, finding{
			msg:    fmt.Sprintf("未找到接收者为 %s 的导出方法，规则表已过期", r.receiver),
			prdRef: r.prdRef,
		})
	}
	return out
}

// inExceptDir 判断文件是否位于豁免目录子树内。
func inExceptDir(root, path string, except []string) bool {
	if len(except) == 0 {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	rel = filepath.ToSlash(rel)
	for _, e := range except {
		e = filepath.ToSlash(e)
		if rel == e || strings.HasPrefix(rel, e+"/") {
			return true
		}
	}
	return false
}

func checkImports(root string, r importRule) []finding {
	var out []finding
	base := filepath.Join(root, r.dir)
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if !strings.HasPrefix(p, r.deny) {
				continue
			}
			if inExceptDir(root, path, r.exceptDirs) {
				continue
			}
			ok := false
			for _, a := range r.allow {
				if strings.HasPrefix(p, a) {
					ok = true
					break
				}
			}
			if !ok {
				out = append(out, finding{
					pos:    fset.Position(imp.Pos()),
					msg:    fmt.Sprintf("%s 不得导入 %s", r.dir, p),
					prdRef: r.prdRef,
				})
			}
		}
		return nil
	})
	if err != nil {
		out = append(out, finding{msg: fmt.Sprintf("遍历 %s 失败: %v", base, err), prdRef: r.prdRef})
	}
	return out
}

func checkTimeNow(root string, r timeRule) []finding {
	var out []finding
	base := filepath.Join(root, r.dir)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return nil // 尚未创建的包不报错，便于规则表先行登记
	}
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil // 测试可以自由使用时钟
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return perr
		}
		exempt := exemptLines(fset, f, allowTimeNow)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if exprString(call.Fun) != "time.Now" {
				return true
			}
			if exempt[fset.Position(call.Pos()).Line] {
				return true
			}
			out = append(out, finding{
				pos: fset.Position(call.Pos()),
				msg: "核心包禁止直接调用 time.Now；请使用注入的时钟。" +
					"确有必要（超时、TTL、展示）时用 //" + allowTimeNow + " 注释豁免并说明用途",
				prdRef: r.prdRef,
			})
			return true
		})
		return nil
	})
	if err != nil {
		out = append(out, finding{msg: fmt.Sprintf("遍历 %s 失败: %v", base, err), prdRef: r.prdRef})
	}
	return out
}

// exemptLines 返回被豁免注释覆盖的行号集合。
//
// 按注释「组」而非单行判断：豁免注释通常要写明理由，因而是多行的，
// 只看紧邻上一行会让多行注释失效。本函数把整个注释组所在的行，
// 以及紧随其后的那一行，都标记为豁免——这同时覆盖了
// 「注释块在上」与「行尾注释」两种写法。
//
// 这个缺陷是真实代码抓出来的：分析器最初只回看一行，
// 于是仓库里合法的多行豁免被误报为违反。
func exemptLines(fset *token.FileSet, f *ast.File, marker string) map[int]bool {
	lines := make(map[int]bool)
	for _, cg := range f.Comments {
		hit := false
		for _, c := range cg.List {
			if strings.Contains(c.Text, marker) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		start := fset.Position(cg.Pos()).Line
		end := fset.Position(cg.End()).Line
		for l := start; l <= end+1; l++ {
			lines[l] = true
		}
	}
	return lines
}

func recvTypeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvTypeName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr: // 泛型接收者
		return recvTypeName(t.X)
	default:
		return ""
	}
}

// exprString 把类型表达式还原为形如 pkg.Type 的字符串。
func exprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return exprString(t.X) + "." + t.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	default:
		return ""
	}
}
