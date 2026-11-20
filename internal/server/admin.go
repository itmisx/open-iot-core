package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/itmisx/open-iot-core/internal/node"
)

// adminServer 承载健康检查与管理接口。
//
// v0.1 内核阶段只实现 /healthz 与 /readyz（PRD NFR-061）。
// 完整的 Admin API 属下一步。
type adminServer struct {
	s   *Server
	srv *http.Server
	ln  net.Listener
	// token 为空表示未配置鉴权令牌，此时 /api/admin/v1 不挂载。
	token string

	mu      sync.Mutex
	started bool
}

func newAdminServer(s *Server, token string) *adminServer {
	return &adminServer{s: s, token: token}
}

func (a *adminServer) start(ctx context.Context) error {
	mux := http.NewServeMux()
	a.mount(mux)

	ln, err := net.Listen("tcp", a.s.cfg.Listeners.Admin.Addr)
	if err != nil {
		return fmt.Errorf("server: 监听管理端点 %s 失败: %w",
			a.s.cfg.Listeners.Admin.Addr, err)
	}
	a.ln = ln
	a.srv = &http.Server{
		Handler: a.s.tracer.Wrap(mux),
		// 管理端点面向运维工具，超时取紧一些：一个卡住的管理请求
		// 不应占用连接直到进程退出。
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	a.mu.Lock()
	a.started = true
	a.mu.Unlock()

	a.s.wg.Add(1)
	go func() {
		defer a.s.wg.Done()
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.s.log.Error("管理端点退出", "错误", err)
		}
	}()
	return nil
}

func (a *adminServer) stop() error {
	a.mu.Lock()
	started := a.started
	a.mu.Unlock()
	if !started || a.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.srv.Shutdown(ctx)
}

func (a *adminServer) addr() string {
	if a.ln == nil {
		return ""
	}
	return a.ln.Addr().String()
}

// healthzResponse 是 /healthz 的响应。
//
// healthz 回答的是"进程还活着吗"，readyz 回答"能接流量吗"。
// 把两者合并会导致一个常见的运维事故：节点因视图落后而未就绪时，
// 编排系统误以为它已死并重启它——而重启只会让它重新落后一次。
type healthzResponse struct {
	Status    string `json:"status"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
}

type readyzResponse struct {
	Ready      bool                   `json:"ready"`
	NodeID     string                 `json:"node_id"`
	Components []node.ComponentStatus `json:"components"`
}

func (a *adminServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthzResponse{
		Status: "ok", NodeID: a.s.mf.NodeID, ClusterID: a.s.mf.ClusterID,
	})
}

func (a *adminServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready := a.s.ready.Ready()
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	// 未就绪时也返回各组件明细：只回 503 的探针无法回答"卡在哪"，
	// 而那是运维唯一真正需要的信息。
	writeJSON(w, code, readyzResponse{
		Ready: ready, NodeID: a.s.mf.NodeID,
		Components: a.s.ready.Report(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// 管理端点的响应不应被缓存：探针拿到缓存的就绪状态比拿不到更糟。
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
