package shadow

import (
	"context"
	"sort"
	"sync"
)

// CmdKind 是命令类型。
type CmdKind uint8

const (
	// CmdUpdate 深度合并一个补丁，影子不存在时创建。
	CmdUpdate CmdKind = iota
	// CmdDelete 删除影子。
	CmdDelete
)

// Command 是一条影子变更命令。
//
// 它被设计成**纯数据**而不是回调，因为它要能被复制：Raft 的模型是
// 「提案命令 → 各副本按同一顺序确定地 apply」，而回调无法复制。
//
// NowSec 由提案方填入、在 apply 时使用，不在 apply 里读时钟。
// 这是契约 08 在复制状态机下的具体含义：若各副本 apply 时各读一次
// 墙上时钟，它们会算出不同的 metadata 时间戳，状态机就不再确定，
// 快照与日志重放的结果也不再一致。
type Command struct {
	Kind   CmdKind
	Thing  string
	Shadow string
	// Patch 是 desired 与 reported 的补丁，仅 CmdUpdate 使用。
	Patch Object
	// ExpectVersion 为 0 表示不做乐观并发检查（PRD SHDW-025）。
	ExpectVersion int64
	// NowSec 是本次变更的时间戳，Unix 秒。
	NowSec int64
	// ClientToken 原样回传。
	ClientToken string
}

// Result 是一次变更的结果，供调用方构造响应与待发布的消息。
type Result struct {
	// Previous 是变更前的文档，nil 表示本次是创建。
	Previous *Document
	// Current 是变更后的文档，nil 表示已被删除。
	Current *Document
	// Version 是变更后的版本；删除时为被删除文档的版本。
	Version int64
	// Changed 表示状态是否真的发生了变化。
	//
	// 与"更新被接受"不是一回事：一个把字段设为相同值的更新会被接受、
	// 版本会加一、元数据时间戳会刷新，但 Changed 为假，因此不发 delta。
	Changed bool
	// Delta 是变更后的 desired 相对 reported 的差异，无差异时为 nil。
	Delta Object
	// DeltaMeta 是 Delta 中各字段对应的元数据。
	DeltaMeta Object
}

// Store 是影子的存储面。
//
// 实现须保证同一 thing 的变更串行化——这正是契约 06 的分区单写者：
// 单机下用一把 per-thing 锁，集群下由分区 Raft 组提供。
type Store interface {
	// Get 读取一份影子。
	Get(thing, shadow string) (*Document, bool)
	// List 返回一个 thing 的全部命名影子名，按字典序。
	List(thing string) []string
	// Apply 应用一条命令。
	Apply(ctx context.Context, cmd Command) (*Result, error)
	Close() error
}

// ApplyCommand 是纯状态机：给定当前文档与命令，算出新文档与结果。
//
// 它不碰锁、不碰存储、不读时钟，因此可以被穷举测试，也能直接作为
// Raft 的 apply 函数体。把它与存储分开是为了让"语义正确"与
// "并发正确"这两件事可以分别验证。
//
// 导出它是为了让持久化实现（internal/store）复用同一份语义：
// 存储层只负责"读出、应用、写回"的原子性与持久性，语义仍在此处。
// 若让存储层自己实现一遍合并，两个 Store 的语义会慢慢分叉。
func ApplyCommand(cur *Document, cmd Command) (*Document, *Result, error) {
	switch cmd.Kind {
	case CmdUpdate:
		return applyUpdate(cur, cmd)
	case CmdDelete:
		return applyDelete(cur, cmd)
	default:
		return nil, nil, ErrInternal
	}
}

func applyUpdate(cur *Document, cmd Command) (*Document, *Result, error) {
	curVersion := int64(0)
	if cur != nil {
		curVersion = cur.Version
	}
	// PRD SHDW-025：乐观并发控制
	if cmd.ExpectVersion != 0 && cmd.ExpectVersion != curVersion {
		return nil, nil, ErrVersionConflict
	}

	var prev *Document
	next := &Document{}
	if cur != nil {
		prev = cur.Clone()
		next = cur.Clone()
	}
	if next.Desired == nil {
		next.Desired = Object{}
	}
	if next.Reported == nil {
		next.Reported = Object{}
	}
	if next.DesiredMeta == nil {
		next.DesiredMeta = Object{}
	}
	if next.ReportedMeta == nil {
		next.ReportedMeta = Object{}
	}

	changed := false
	if d, ok := sectionOf(cmd.Patch, "desired"); ok {
		if mergeState(next.Desired, next.DesiredMeta, d, cmd.NowSec) {
			changed = true
		}
	}
	if r, ok := sectionOf(cmd.Patch, "reported"); ok {
		if mergeState(next.Reported, next.ReportedMeta, r, cmd.NowSec) {
			changed = true
		}
	}

	// 规则 4（见 merge.go）：顶层段为空则不保留
	next.Desired = pruneEmptySection(next.Desired)
	next.Reported = pruneEmptySection(next.Reported)
	next.DesiredMeta = pruneEmptySection(next.DesiredMeta)
	next.ReportedMeta = pruneEmptySection(next.ReportedMeta)

	// 版本每次被接受的更新都加一，即使状态没有实际变化。
	// 这与 AWS 一致：版本计的是"第几次更新"，不是"第几次变化"。
	next.Version = curVersion + 1
	next.UpdatedAtSec = cmd.NowSec

	d := delta(next.Desired, next.Reported)
	res := &Result{
		Previous:  prev,
		Current:   next.Clone(),
		Version:   next.Version,
		Changed:   changed,
		Delta:     d,
		DeltaMeta: deltaMetadata(d, next.DesiredMeta),
	}
	return next, res, nil
}

func applyDelete(cur *Document, cmd Command) (*Document, *Result, error) {
	if cur == nil {
		return nil, nil, errNotFound(cmd.Shadow)
	}
	if cmd.ExpectVersion != 0 && cmd.ExpectVersion != cur.Version {
		return nil, nil, ErrVersionConflict
	}
	// PRD SHDW-028：删除后版本重置，重新创建从 1 开始。
	// 实现上即"把文档整个丢掉"，下一次 applyUpdate 的 curVersion 为 0。
	return nil, &Result{
		Previous: cur.Clone(),
		Current:  nil,
		Version:  cur.Version,
		Changed:  true,
	}, nil
}

// sectionOf 取出 patch 中的一个顶层段。
//
// 段存在但为 null 时视为不存在：`{"state":{"desired":null}}` 的含义是
// "删除整个 desired 段"，但 AWS 不支持这种写法，因此当作未提供处理。
func sectionOf(patch Object, name string) (Object, bool) {
	v, ok := patch[name]
	if !ok || v == nil {
		return nil, false
	}
	obj, ok := v.(map[string]any)
	return obj, ok
}

// MemStore 是单节点实现，每 thing 一把锁。
//
// 锁的粒度是 thing 而不是全局：影子写入是串行化的，但不同设备之间
// 没有任何共享状态，用一把全局锁会让影子吞吐与设备数无关。
type MemStore struct {
	mu     sync.RWMutex
	things map[string]*thingShadows
	closed bool
}

type thingShadows struct {
	mu      sync.Mutex
	shadows map[string]*Document
}

// NewMemStore 创建单节点影子存储。
func NewMemStore() *MemStore {
	return &MemStore{things: make(map[string]*thingShadows)}
}

func (s *MemStore) lane(thing string, create bool) *thingShadows {
	s.mu.RLock()
	t := s.things[thing]
	s.mu.RUnlock()
	if t != nil || !create {
		return t
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t = s.things[thing]; t == nil {
		t = &thingShadows{shadows: make(map[string]*Document)}
		s.things[thing] = t
	}
	return t
}

func (s *MemStore) Get(thing, shadow string) (*Document, bool) {
	t := s.lane(thing, false)
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	d, ok := t.shadows[shadow]
	if !ok {
		return nil, false
	}
	// 返回副本：调用方拿到的文档不得与存储共享子树，
	// 否则它对响应对象的任何改动都会静默改掉已存储的影子。
	return d.Clone(), true
}

func (s *MemStore) List(thing string) []string {
	t := s.lane(thing, false)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	names := make([]string, 0, len(t.shadows))
	for n := range t.shadows {
		if n != "" { // 经典影子不计入命名影子列表
			names = append(names, n)
		}
	}
	t.mu.Unlock()
	sort.Strings(names)
	return names
}

func (s *MemStore) Apply(_ context.Context, cmd Command) (*Result, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrInternal
	}
	t := s.lane(cmd.Thing, true)

	// 单写者：同一 thing 的全部变更在这把锁下串行化（契约 06）
	t.mu.Lock()
	defer t.mu.Unlock()

	next, res, err := ApplyCommand(t.shadows[cmd.Shadow], cmd)
	if err != nil {
		return nil, err
	}
	if next == nil {
		delete(t.shadows, cmd.Shadow)
	} else {
		t.shadows[cmd.Shadow] = next
	}
	return res, nil
}

func (s *MemStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.things = nil
	s.mu.Unlock()
	return nil
}
