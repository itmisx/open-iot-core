package router

import (
	"sort"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// Member 是共享订阅组的一个成员。
type Member struct {
	ClientID string
	Node     cluster.NodeID
	QoS      uint8
	// Persistent 为 true 表示该成员是持久会话。
	// 组内只要有一个持久会话成员，整组即启用消息排队（PRD MQTT-095）。
	Persistent bool
	// Online 为 false 表示持久会话成员当前离线。
	Online bool
}

// groupKey 唯一标识一个共享订阅组。
//
// 同一 {ShareName}/{TopicFilter} 属同一组（MQTT-4.8.2）：
// 组名相同但过滤器不同是两个组。
type groupKey struct {
	share  string
	filter string
}

type group struct {
	// filterB 是过滤器的字节形式，预存以避免每次匹配都转换。
	filterB []byte
	members []Member
	// rr 是轮转下标。
	//
	// 规范与 AWS 都描述为「随机选一个」，而轮转是等价的负载均衡实现且
	// 可确定性测试。具体算法属可延后的实现细节（PRD ARCH-048）。
	rr int
}

// Target 是一次共享订阅投递的目标。
type Target struct {
	Share    string
	Filter   string
	ClientID string
	Node     cluster.NodeID
	QoS      uint8
	// Queue 为 true 表示组内无在线成员且启用了排队，
	// 调用方应把消息放入该组的积压队列而非丢弃（PRD MQTT-095）。
	Queue bool
	// Remote 为 true 表示选中的成员在**别的节点**上（MQTT-096、MQTT-116）：
	// ClientID 此时为空，调用方应把这一帧转给 Node 并带上 Share/Filter，
	// 由那个节点在自己的本地成员里选一个投。
	//
	// 单独一个字段而不是靠"ClientID 为空"判断：那个条件原先的含义是
	// 「组内没有可投的成员」，两件事共用一个判据会让"转发给远端"被当成
	// "丢弃"——而现象是跨节点的那一组永远收不到消息。
	Remote bool
}

// SharedGroups 管理共享订阅组（契约 10）。
//
// 共享订阅不入订阅树：「从一组订阅者中选一个」是全局逻辑，
// 集群下需要跨节点协调，因此它与普通订阅分开管理，
// 由 Route 与 RouteShared 两个入口分别服务（契约 02 的 ARCH-015）。
type SharedGroups struct {
	mu     sync.RWMutex
	groups map[groupKey]*group
	limits topic.Limits
	self   cluster.NodeID

	// remote 是别的节点通告来的组成员（MQTT-116）。
	//
	// 只存计数：发布侧要回答的是"这一跳发给谁"，而具体投给那个节点上
	// 的哪一个成员由它自己决定（它才知道谁此刻在线）。存 clientId 会让
	// 每次成员上下线都要跨节点同步一次，而那是高频事件。
	remote map[groupKey]*remoteGroup
}

// remoteGroup 是一个组在**别的节点**上的成员情况。
//
// filterB 与 nodes 都是在**通告时**算好的（约每秒一次），不是在投递时。
//
// 真正要紧的是 nodes：第一版在投递时重建并排序一遍节点列表，实测每条
// 消息三次堆分配，而它随"有多少个只在远端有成员的组"线性放大——
// 正是 LANG-007 禁止的那类热路径分配（TestSharedRemotePickDoesNotAllocate
// 盯着它，把预计算摘掉立刻变红）。
//
// filterB 则是顺带：`[]byte(k.filter)` 当场转换目前会被逃逸分析落在栈上，
// 所以它不计入分配；预存它是因为那个结论依赖 topic.Match 不持有这个切片，
// 而那是别人的实现细节，不该由热路径来赌。
type remoteGroup struct {
	filterB []byte
	// byNode 是各节点的在线成员数，通告按节点全量替换。
	byNode map[cluster.NodeID]int
	// nodes 是 byNode 里在线数大于零的节点，已排序。
	//
	// 排序是必需的：map 的遍历顺序随机，而轮转下标要对着一个稳定的
	// 序列才有意义，否则同一个节点可能被连续选中多次。
	nodes []cluster.NodeID
	rr    int
}

// refresh 重算排序后的节点列表。在通告时调用，不在投递时。
func (g *remoteGroup) refresh(self cluster.NodeID) {
	g.nodes = g.nodes[:0]
	for node, online := range g.byNode {
		if online > 0 && node != self {
			g.nodes = append(g.nodes, node)
		}
	}
	sort.Slice(g.nodes, func(i, j int) bool { return g.nodes[i] < g.nodes[j] })
	if len(g.nodes) == 0 {
		g.rr = 0
	} else if g.rr >= len(g.nodes) {
		g.rr = 0
	}
}

// NewSharedGroups 创建共享订阅组管理器。
func NewSharedGroups(self cluster.NodeID, l topic.Limits) *SharedGroups {
	return &SharedGroups{
		groups: make(map[groupKey]*group),
		limits: l,
		self:   self,
		remote: make(map[groupKey]*remoteGroup),
	}
}

// Join 把一个成员加入共享订阅组。重复加入时更新其属性。
func (s *SharedGroups) Join(share, filter string, m Member) {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[k]
	if !ok {
		g = &group{filterB: []byte(filter)}
		s.groups[k] = g
	}
	for i := range g.members {
		if g.members[i].ClientID == m.ClientID {
			g.members[i] = m
			return
		}
	}
	g.members = append(g.members, m)
}

// Leave 把一个成员移出共享订阅组。返回是否确实移除了。
//
// 清洁会话成员断开即离组；持久会话成员断开只是转为离线，
// 须显式退订或会话过期才离组（PRD MQTT-095 的会话生命周期）。
func (s *SharedGroups) Leave(share, filter, clientID string) bool {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[k]
	if !ok {
		return false
	}
	for i := range g.members {
		if g.members[i].ClientID == clientID {
			g.members = append(g.members[:i], g.members[i+1:]...)
			if len(g.members) == 0 {
				delete(s.groups, k)
			} else if g.rr >= len(g.members) {
				g.rr = 0
			}
			return true
		}
	}
	return false
}

// Has 报告某客户端是否已是该组的成员。
//
// 与 Trie.Has 同一个用途：全局订阅额度的预判（见那里的说明）。
// 不用 Members 来判断是因为它要分配一个切片，而这是订阅热路径。
func (s *SharedGroups) Has(share, filter, clientID string) bool {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[k]
	if !ok {
		return false
	}
	for i := range g.members {
		if g.members[i].ClientID == clientID {
			return true
		}
	}
	return false
}

// SetOnline 更新某成员的在线状态。持久会话断开时调用。
func (s *SharedGroups) SetOnline(share, filter, clientID string, online bool) bool {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[k]
	if !ok {
		return false
	}
	for i := range g.members {
		if g.members[i].ClientID == clientID {
			g.members[i].Online = online
			return true
		}
	}
	return false
}

// LeaveAll 把某个客户端移出其所有共享订阅组，返回移除条数。
// 会话结束时调用。
func (s *SharedGroups) LeaveAll(clientID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, g := range s.groups {
		for i := range g.members {
			if g.members[i].ClientID == clientID {
				g.members = append(g.members[:i], g.members[i+1:]...)
				if len(g.members) == 0 {
					delete(s.groups, k)
				} else if g.rr >= len(g.members) {
					g.rr = 0
				}
				n++
				break
			}
		}
	}
	return n
}

// MatchInto 为一个主题选出所有匹配组各自的投递目标。
//
// 多个含通配符的共享订阅同时命中时，每个匹配组各投递一份副本
// （PRD MQTT-093）——组之间是广播，组内才是负载均衡。
func (s *SharedGroups) MatchInto(dst []Target, name []byte) []Target {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, g := range s.groups {
		if !topic.Match(g.filterB, name, s.limits) {
			continue
		}
		if t, ok := s.pickLocked(k, g); ok {
			dst = append(dst, t)
		}
	}
	// 只有远端成员的组也要算进来：本节点一个成员都没有时 groups 里
	// 没有这个键，而那正是"订阅者全在别的节点上"的常见形状——
	// 漏掉它的现象是跨节点的共享订阅只在有本地成员时才工作。
	for k, rg := range s.remote {
		if _, local := s.groups[k]; local {
			continue
		}
		if !topic.Match(rg.filterB, name, s.limits) {
			continue
		}
		if t, ok := s.pickRemoteLocked(k); ok {
			dst = append(dst, t)
		}
	}
	return dst
}

// AdvertiseRemote 用一个节点的**全量快照**替换它的组成员计数（MQTT-116）。
//
// 全量替换而不是增量：丢一次通告或乱序都不会让视图永久偏离。
// counts 为空表示那个节点此刻一个共享订阅成员都没有，于是它的条目被清掉。
func (s *SharedGroups) AdvertiseRemote(node cluster.NodeID, counts []cluster.ShareCount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, g := range s.remote {
		if _, had := g.byNode[node]; !had {
			continue
		}
		delete(g.byNode, node)
		if len(g.byNode) == 0 {
			delete(s.remote, k)
			continue
		}
		g.refresh(s.self)
	}
	for _, c := range counts {
		if c.Online <= 0 {
			continue
		}
		k := groupKey{share: c.Share, filter: c.Filter}
		g := s.remote[k]
		if g == nil {
			g = &remoteGroup{filterB: []byte(c.Filter), byNode: make(map[cluster.NodeID]int)}
			s.remote[k] = g
		}
		g.byNode[node] = c.Online
		g.refresh(s.self)
	}
}

// ForgetRemote 丢掉一个节点的组成员计数。节点离开成员列表时调用。
func (s *SharedGroups) ForgetRemote(node cluster.NodeID) {
	s.AdvertiseRemote(node, nil)
}

// LocalCounts 是本节点各组的在线成员数，用于周期性通告。
func (s *SharedGroups) LocalCounts() []cluster.ShareCount {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]cluster.ShareCount, 0, len(s.groups))
	for k, g := range s.groups {
		online := 0
		for _, m := range g.members {
			if m.Online && m.Node == s.self {
				online++
			}
		}
		if online > 0 {
			out = append(out, cluster.ShareCount{Share: k.share, Filter: k.filter, Online: online})
		}
	}
	// 顺序确定：通告是全量快照，而两次内容相同、顺序不同的快照
	// 不该被下游当成变化。
	sort.Slice(out, func(i, j int) bool {
		if out[i].Share != out[j].Share {
			return out[i].Share < out[j].Share
		}
		return out[i].Filter < out[j].Filter
	})
	return out
}

// PickLocal 只在本节点的成员里选一个（MQTT-116）。
//
// 收到别的节点转来的共享订阅帧时走这里：那一帧已经由发布侧选定了
// 本节点，再做一次跨节点选取会把它又转出去——而共享订阅的语义是
// 组内只投一个，弹一次就破了。
func (s *SharedGroups) PickLocal(share, filter string) (Target, bool) {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pickLocalLocked(k, s.groups[k])
}

// Pick 为某个组选出一个投递目标。
//
// 本节点没有这个组的成员时不能直接返回 false：组可能整个在别的节点上
// （MQTT-116），而那正是"订阅者全在另一台机器"的常见形状。
func (s *SharedGroups) Pick(share, filter string) (Target, bool) {
	k := groupKey{share: share, filter: filter}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pickLocked(k, s.groups[k])
}

// pickLocked 在组内选一个在线成员。须在持锁状态下调用。
//
// 顺序是「本地在线成员 → 有在线成员的远端节点 → 排队或丢弃」。
// 本地优先是为了省一跳（PRD MQTT-096）：发布在哪个节点发生，就尽量在
// 那个节点投出去。负载仍然是均衡的——发布本身分散在各节点上。
func (s *SharedGroups) pickLocked(k groupKey, g *group) (Target, bool) {
	if t, ok := s.pickLocalLocked(k, g); ok {
		return t, true
	}
	// 本地没有在线成员：看有没有哪个远端节点有（MQTT-116）。
	if t, ok := s.pickRemoteLocked(k); ok {
		return t, true
	}
	if g == nil {
		return Target{}, false
	}
	// 无处可投：组内若有持久会话成员则排队，否则丢弃。
	// QoS 0 消息在任何失败下都丢弃（PRD MQTT-095）。
	for _, m := range g.members {
		if m.Persistent {
			return Target{Share: k.share, Filter: k.filter, Queue: true}, true
		}
	}
	return Target{}, false
}

// pickRemoteLocked 在有在线成员的远端节点之间轮转选一个。
func (s *SharedGroups) pickRemoteLocked(k groupKey) (Target, bool) {
	g := s.remote[k]
	if g == nil || len(g.nodes) == 0 {
		return Target{}, false
	}
	idx := g.rr % len(g.nodes)
	g.rr = (idx + 1) % len(g.nodes)
	return Target{
		Share: k.share, Filter: k.filter,
		Node: g.nodes[idx], Remote: true,
	}, true
}

// pickLocalLocked 只在本节点的成员里选。须在持锁状态下调用。
func (s *SharedGroups) pickLocalLocked(k groupKey, g *group) (Target, bool) {
	if g == nil {
		return Target{}, false
	}
	n := len(g.members)
	if n == 0 {
		return Target{}, false
	}
	// 从轮转位置起找本节点的在线成员。
	//
	// members 里只会有本节点的成员：别的节点的成员以计数形式存在
	// remote 里（MQTT-116）。保留 Node 字段的判断是为了容错——
	// 装配层若误把远端成员塞进来，这里不会把它当成本地可投的目标。
	start := g.rr % n
	chosen := -1
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		m := g.members[idx]
		if m.Online && m.Node == s.self {
			chosen = idx
			break
		}
	}
	if chosen < 0 {
		return Target{}, false
	}
	g.rr = (chosen + 1) % n
	m := g.members[chosen]
	return Target{
		Share: k.share, Filter: k.filter,
		ClientID: m.ClientID, Node: m.Node, QoS: m.QoS,
	}, true
}

// Members 返回某组的成员快照。
func (s *SharedGroups) Members(share, filter string) []Member {
	k := groupKey{share: share, filter: filter}
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[k]
	if !ok {
		return nil
	}
	return append([]Member(nil), g.members...)
}

// Groups 返回当前组数。
func (s *SharedGroups) Groups() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.groups)
}
