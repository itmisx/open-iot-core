package server

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/itmisx/open-iot-core/internal/server/consoleassets"
)

// 控制台（PRD UI-001）。
//
// 前端产物经 go:embed 打进同一二进制：零额外进程、零额外依赖。
// 这是"单二进制"这个卖点的一部分——控制台要是得单独部署，
// 自建场景的门槛就回到了和其他方案一样的位置。

// mountConsole 把控制台挂到 mux 上。
//
// 静态外壳本身不鉴权，因为浏览器导航无法携带 Authorization 头。
// 这是安全的：外壳里没有任何数据，只是一段通用 JS；全部数据都要
// 经 /api/admin/v1 取，而那里一律要求令牌（API-080）。
// 界面会向用户索取令牌并只存在 sessionStorage 里。
func (a *adminServer) mountConsole(mux *http.ServeMux) {
	if !a.s.cfg.Console.Enabled {
		a.s.log.Info("控制台未启用", "开关", "console.enabled")
		return
	}
	if !consoleassets.Present() {
		// 没有产物时给出明确提示而不是白屏：这通常意味着
		// 构建时没跑 `make console`。
		a.s.log.Warn("控制台产物缺失，未挂载",
			"如何构建", "make console（需要 Node）",
			"说明", "产物通常随仓库提交，无需本地构建")
		return
	}
	sub, err := consoleassets.FS()
	if err != nil {
		a.s.log.Error("控制台产物无法读取", "错误", err)
		return
	}

	base := normalizeBasePath(a.s.cfg.Console.BasePath)
	handler := consoleHandler(sub)
	if base == "/" {
		mux.Handle("GET /", handler)
	} else {
		// 反向代理子路径（UI-001）：去掉前缀后交给静态处理器。
		mux.Handle("GET "+base, http.RedirectHandler(base+"/", http.StatusMovedPermanently))
		mux.Handle("GET "+base+"/", http.StripPrefix(strings.TrimSuffix(base, "/"), handler))
	}
	a.s.log.Info("控制台已挂载", "路径", base)
}

// consoleHandler 提供静态文件，找不到时回落到 index.html。
//
// 回落是单页应用的常规需要。本控制台用哈希路由，理论上所有路径
// 都落在 "/"，但回落仍然保留：它让"用户手打了一个带路径的地址"
// 这件事得到一个可用的页面而不是 404。
func consoleHandler(files fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(files))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(files, p); err != nil {
			// 三类路径不回落到 index.html：
			//
			//  - 资源文件：回落会让一个拼错的 .js 路径返回 HTML，
			//    浏览器报出的错误与真实原因完全无关。
			//  - /api/ 下的路径：回落会让"管理面未挂载"变成 200 HTML，
			//    从而破坏 API-081——攻击者能据响应确认这里有一个管理面。
			//    控制台的通配路由不得盖住这条安全属性。
			//  - /metrics：回落会让采集端拿到 200 加一段 HTML，于是
			//    Prometheus 报的是"解析失败"而不是"这里没有指标"，
			//    把一个配置问题伪装成格式问题。
			if strings.Contains(p, "/assets/") || strings.HasPrefix(p, "assets/") ||
				strings.HasPrefix(p, "api/") || p == "metrics" {
				http.NotFound(w, r)
				return
			}
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		// 带内容哈希的资源可以长缓存，但本项目刻意不给文件名加哈希
		// （见 vite.config.ts），因此一律不缓存——嵌入式产物随二进制
		// 更新，一个过期的缓存会让升级后的控制台行为诡异。
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}

// normalizeBasePath 规范化控制台挂载路径。
func normalizeBasePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimSuffix(p, "/")
}
