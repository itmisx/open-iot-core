package cluster

import "sync"

// RemoteThings 记录「哪个设备此刻连在哪个节点上」。
//
// 设备域主题（$aws/things/<名字>/…）不进全局订阅表：它们的归属由名字
// 直接查出来，而不是靠订阅匹配。这是本项目相对通用 broker 的主要性能
// 杠杆（ARCH-017）——百万条精确订阅不跨节点传播，只有这张归属表要。
//
// 归属在正常路径上按 epoch + 连续版本传播增量；只有新对端、进程重启、
// 版本断档或发送失败后才安装一次精确全量快照。这样百万空闲连接不会每秒
// 复制一遍全部设备名，而漏掉一批增量也会被版本缺口发现，不会永久偏离。
type RemoteThings struct {
	self NodeID

	mu sync.RWMutex
	// where 是唯一的权威索引：设备名到承载它的节点。
	//
	// 旧实现还为每个节点永久维护一份 map[string]bool，百万连接时会把每个
	// 设备名再放进一套哈希桶。节点离开和快照替换是低频控制面操作，可以
	// 扫描这张表；Locate 是逐消息热路径，仍然保持一次 O(1) 查表。
	where map[string]NodeID
	// counts 只保存每个节点当前拥有的设备数量，用来 O(1) 回答 Nodes，
	// 也让首次快照不必创建临时集合。
	counts map[NodeID]int
	// cursors 使增量具备可验证的连续性；needSync 记录发现断档后要向谁
	// 请求全量快照。两者都与归属表共用一把锁，快照替换是原子的。
	cursors  map[NodeID]thingCursor
	needSync map[NodeID]bool
}

type thingCursor struct {
	epoch   uint64
	version uint64
}

// NewRemoteThings 创建归属表。
func NewRemoteThings(self NodeID) *RemoteThings {
	return &RemoteThings{
		self:     self,
		where:    make(map[string]NodeID),
		counts:   make(map[NodeID]int),
		cursors:  make(map[NodeID]thingCursor),
		needSync: make(map[NodeID]bool),
	}
}

// Advertise 用一份全量快照替换某节点承载的设备集合。
//
// 整体替换而非增量：快照是幂等的，通告丢失或乱序都不会让归属表永久
// 偏离（ARCH-012 的最终一致）。
func (t *RemoteThings) Advertise(node NodeID, things []string) {
	if node == t.self {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.replaceLocked(node, things)
}

// ApplySnapshot 原子地安装一份带进程 epoch 与版本的精确快照。
func (t *RemoteThings) ApplySnapshot(node NodeID, epoch, version uint64, things []string) {
	if node == t.self {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.replaceLocked(node, things)
	t.cursors[node] = thingCursor{epoch: epoch, version: version}
	delete(t.needSync, node)
}

// ApplyDelta 在版本连续时应用增量。返回 false 表示发现重启或断档，调用方
// 应在下一次发往该节点的心跳里设置 ThingSync。
func (t *RemoteThings) ApplyDelta(node NodeID, epoch, from, version uint64, changes []ThingChange) bool {
	if node == t.self {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, ok := t.cursors[node]
	if !ok || cur.epoch != epoch || cur.version != from || version < from {
		t.needSync[node] = true
		return false
	}
	for _, change := range changes {
		if change.Name == "" {
			continue
		}
		if change.Present {
			t.assignLocked(change.Name, node)
			continue
		}
		t.removeLocked(change.Name, node)
	}
	t.cursors[node] = thingCursor{epoch: epoch, version: version}
	delete(t.needSync, node)
	return true
}

// NeedsSync 报告本节点是否需要对端发一份全量快照。
func (t *RemoteThings) NeedsSync(node NodeID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.needSync[node]
}

func (t *RemoteThings) replaceLocked(node NodeID, things []string) {
	// 首次快照是集群扩容和进程启动的常见路径。没有旧归属时直接安装，
	// 避免为了判定“哪些旧项消失了”再分配一张同样大的临时 map。
	if t.counts[node] == 0 {
		if len(t.where) == 0 && len(things) > 0 {
			// 全表首份快照可以精确预分配；让 map 从零逐级扩容会同时
			// 保留新旧桶，10 万设备基准里这部分分配接近表本身大小。
			t.where = make(map[string]NodeID, len(things))
		}
		for _, name := range things {
			if name != "" {
				t.assignLocked(name, node)
			}
		}
		return
	}

	next := make(map[string]struct{}, len(things))
	for _, name := range things {
		if name != "" {
			next[name] = struct{}{}
		}
	}
	removed := 0
	for name, owner := range t.where {
		if owner == node {
			if _, ok := next[name]; !ok {
				t.removeLocked(name, node)
				removed++
			}
		}
	}
	for name := range next {
		t.assignLocked(name, node)
	}
	t.compactWhereLocked(removed)
}

// assignLocked 实现同一设备“后到者胜”，并同步节点计数。
func (t *RemoteThings) assignLocked(name string, node NodeID) {
	old, ok := t.where[name]
	if ok && old == node {
		return
	}
	if ok {
		t.decNodeLocked(old)
	}
	t.where[name] = node
	t.counts[node]++
}

func (t *RemoteThings) removeLocked(name string, node NodeID) bool {
	if owner, ok := t.where[name]; !ok || owner != node {
		return false
	}
	delete(t.where, name)
	t.decNodeLocked(node)
	return true
}

func (t *RemoteThings) decNodeLocked(node NodeID) {
	if t.counts[node] <= 1 {
		delete(t.counts, node)
		return
	}
	t.counts[node]--
}

// compactWhereLocked 在大量删除后重建 map。Go map 删除键不会缩小哈希桶，
// 若节点离开后保留原桶，百万连接的一大块内存仍无法归还。
func (t *RemoteThings) compactWhereLocked(removed int) {
	if removed <= len(t.where) {
		return
	}
	next := make(map[string]NodeID, len(t.where))
	for name, node := range t.where {
		next[name] = node
	}
	t.where = next
}

// Forget 移除某节点的全部归属，用于成员离开。
func (t *RemoteThings) Forget(node NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	removed := 0
	for name, owner := range t.where {
		if owner == node {
			delete(t.where, name)
			removed++
		}
	}
	delete(t.counts, node)
	t.compactWhereLocked(removed)
	delete(t.cursors, node)
	delete(t.needSync, node)
}

// Locate 返回承载该设备的远端节点。
//
// 参数是 []byte 而非 string：它直接来自主题分类的结果，后者引用的是
// 解码缓冲区。转成 string 会在每条消息的路由路径上分配一次
// （LANG-007）。m[string(key)] 这个形式 Go 有免分配优化。
func (t *RemoteThings) Locate(thing []byte) (NodeID, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node, ok := t.where[string(thing)]
	return node, ok
}

// Nodes 返回已通告归属的节点数。
func (t *RemoteThings) Nodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.counts)
}

// Len 返回已知的远端设备数。
func (t *RemoteThings) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.where)
}
