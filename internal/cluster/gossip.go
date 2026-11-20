package cluster

import "sync"

// RemoteThings 记录「哪个设备此刻连在哪个节点上」。
//
// 设备域主题（$aws/things/<名字>/…）不进全局订阅表：它们的归属由名字
// 直接查出来，而不是靠订阅匹配。这是本项目相对通用 broker 的主要性能
// 杠杆（ARCH-017）——百万条精确订阅不跨节点传播，只有这张归属表要。
//
// 目前的编码是**全量名字快照**（CLU-075）：每个节点周期性把自己承载的
// 设备名列表发给别人，整体替换。节点数与设备数都不大时够用；到百万
// 设备要换成摘要（bloom 或 cuckoo），而 ARCH-047 把编码留作可延后的
// 实现细节，正是为了这次替换不牵动调用方。
//
// 换摘要时要注意一件事：摘要会有假阳性，于是消息会被转发给一个其实
// 没有这台设备的节点。那是可接受的（对端找不到订阅者就丢掉），
// 但假阴性不可接受——它会让消息静默丢失。
type RemoteThings struct {
	self NodeID

	mu     sync.RWMutex
	byNode map[NodeID]map[string]bool
	// where 是反向索引：设备名到承载它的节点。
	//
	// 单独维护而不是每次遍历 byNode：这是消息路由的热路径，
	// 而节点数虽小，每条消息都扫一遍所有节点的设备集合是 O(节点×设备)。
	where map[string]NodeID
}

// NewRemoteThings 创建归属表。
func NewRemoteThings(self NodeID) *RemoteThings {
	return &RemoteThings{
		self:   self,
		byNode: make(map[NodeID]map[string]bool),
		where:  make(map[string]NodeID),
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
	next := make(map[string]bool, len(things))
	for _, name := range things {
		next[name] = true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// 先撤掉这个节点上次通告的、这次不在里面的
	for name := range t.byNode[node] {
		if !next[name] && t.where[name] == node {
			delete(t.where, name)
		}
	}
	if len(next) == 0 {
		delete(t.byNode, node)
	} else {
		t.byNode[node] = next
	}
	for name := range next {
		// 同一台设备被两个节点同时声明：以后到的那条为准。
		//
		// 这是设备正在从一个节点迁到另一个节点的样子（重连到了别处），
		// 而两边的通告有先后。按后到者更新会短暂指错，但指错的后果是
		// 一条消息转给了旧节点并在那里被丢掉；反过来固守旧值则会让
		// 新连上的设备一直收不到消息，直到旧节点的通告过期。
		t.where[name] = node
	}
}

// Forget 移除某节点的全部归属，用于成员离开。
func (t *RemoteThings) Forget(node NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name := range t.byNode[node] {
		if t.where[name] == node {
			delete(t.where, name)
		}
	}
	delete(t.byNode, node)
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
	return len(t.byNode)
}

// Len 返回已知的远端设备数。
func (t *RemoteThings) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.where)
}
