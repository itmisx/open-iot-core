package node

import (
	"sort"
	"sync"
)

// ComponentStatus 是一个组件的就绪状态。
type ComponentStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	// Detail 在未就绪时说明还差什么，就绪后可为空。
	Detail string `json:"detail,omitempty"`
}

// Readiness 汇总各组件的就绪状态。
//
// 它的存在直接服务于两条要求：
//   - PRD CLU-032：节点在 ready 之前不得监听设备端口。半空的视图会导致
//     误拒（证书查不到）或误放（吊销状态还没同步到）。
//   - PRD NFR-061：/readyz 须包含视图追平判定。
//
// 设计上刻意不把"就绪"做成一个布尔量：运维需要知道**卡在哪个组件**，
// 一个只回 503 的健康检查无法回答这个问题。
type Readiness struct {
	mu    sync.RWMutex
	state map[string]ComponentStatus
}

// NewReadiness 创建就绪状态汇总。
//
// components 是必须全部就绪才算整体就绪的组件名。预先登记而非动态添加：
// 若允许动态添加，一个尚未注册的组件会让节点在它还没启动时就被判为就绪。
func NewReadiness(components ...string) *Readiness {
	r := &Readiness{state: make(map[string]ComponentStatus, len(components))}
	for _, c := range components {
		r.state[c] = ComponentStatus{Name: c, Detail: "尚未启动"}
	}
	return r
}

// Set 更新一个组件的状态。未登记的组件名会被忽略。
func (r *Readiness) Set(name string, ready bool, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.state[name]; !ok {
		return
	}
	r.state[name] = ComponentStatus{Name: name, Ready: ready, Detail: detail}
}

// Ready 判断是否全部组件就绪。
func (r *Readiness) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.state {
		if !s.Ready {
			return false
		}
	}
	return true
}

// Report 返回按名字排序的全部组件状态。
//
// 排序是为了让 /readyz 的输出稳定：输出顺序随 map 迭代变化会让
// 两次响应的 diff 充满噪声，而运维就是在 diff 这个输出。
func (r *Readiness) Report() []ComponentStatus {
	r.mu.RLock()
	out := make([]ComponentStatus, 0, len(r.state))
	for _, s := range r.state {
		out = append(out, s)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// NotReady 返回未就绪的组件名，供日志与错误信息使用。
func (r *Readiness) NotReady() []string {
	var out []string
	for _, s := range r.Report() {
		if !s.Ready {
			out = append(out, s.Name)
		}
	}
	return out
}
