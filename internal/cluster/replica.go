package cluster

import "sync"

// SingleReplica 是一副本 Raft 组。
//
// 投票者只有自己，term 从 1 开始，它就是 leader。没有别的副本要复制，
// 追加即提交。单机的元数据写入走这里，而不是跳过日志直接改视图。
// 多节点的选举、复制和再均衡不在这个类型里。
type SingleReplica struct {
	mu     sync.Mutex
	term   uint64
	commit uint64
}

// NewSingleReplica 创建一副本组。
func NewSingleReplica() *SingleReplica {
	return &SingleReplica{term: 1}
}

// Voters 返回投票者数量。一副本组永远是 1。
func (g *SingleReplica) Voters() int { return 1 }

// Term 返回当前任期。
func (g *SingleReplica) Term() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.term
}

// CommitIndex 返回已提交的最后一条日志序号。还没有命令时是 0。
func (g *SingleReplica) CommitIndex() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.commit
}

// Append 追加一条已经通过校验的命令并立即提交。
//
// 调用方必须先拒绝非法命令。日志提交之后不能靠返回错误来撤销。
func (g *SingleReplica) Append() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commit++
	return g.commit
}
