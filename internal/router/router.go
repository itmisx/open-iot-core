package router

import (
	"sort"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// SubRef 是一次匹配命中的本地订阅。
//
// 值类型且不含会话指针：契约 02 要求路由返回的目标与投递解耦，
// 由上层按 ClientID 查到运行时。这也使同一条路由结果可以被序列化后
// 跨节点转发（契约 07 的 Frame.Targets）。
type SubRef struct {
	ClientID       string
	ID             uint32
	QoS            uint8
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling uint8
}

// SessionLocator 把设备名解析为承载其会话的节点。
//
// 契约 03 的设备域主题优化依赖它，而不是把主题直接哈希到目标节点——
// 实现时发现这两件事不同：thingName 的哈希给出的是「所有权记录」所在的
// 分区 leader，而会话本身在设备实际连上的那个节点，二者通常不是同一个。
//
// 因此设备域主题的路由是「一次哈希分区的归属查询」，而非「零查询」。
// 这仍然是本项目的主要性能杠杆：数百万条精确订阅不进入全局订阅表，
// 只有后端应用的通配符订阅需要跨节点传播，而那是数十条而非数百万条。
// 归属查询本身在 CONNECT 时就要做，远比全局复制订阅树便宜。
type SessionLocator interface {
	// LocateThing 返回承载该设备会话的节点。
	// 第二个返回值为 false 表示该设备当前无会话。
	//
	// 参数是 []byte 而非 string：它直接来自主题分类的结果，
	// 后者引用的是解码缓冲区。转成 string 会在每条消息的路由路径上
	// 分配一次（违反 LANG-007）。实现侧若需要以它查 map，
	// 可写 m[string(key)]——Go 对这个形式有免分配优化。
	LocateThing(thing []byte) (cluster.NodeID, bool)
}

// LocalLocator 是 SessionLocator 的单节点实现：会话一律在本节点。
type LocalLocator struct{ Self cluster.NodeID }

func (l LocalLocator) LocateThing([]byte) (cluster.NodeID, bool) { return l.Self, true }

// GossipLocator 按通告来的归属表定位设备（阶段 D）。
//
// 本地有这台设备就判本地（不转发）；只在远端有才给出远端节点。
// 两者都没有时返回 false——那台设备此刻没连着任何节点，消息无处可投，
// 而转给一个随便挑的节点只会让它在那边被丢掉。
type GossipLocator struct {
	Self cluster.NodeID
	// Local 报告本节点是否承载该设备。
	Local func(thing []byte) bool
	// Remote 是通告来的归属表。
	Remote *cluster.RemoteThings
}

func (l GossipLocator) LocateThing(thing []byte) (cluster.NodeID, bool) {
	if l.Local != nil && l.Local(thing) {
		return l.Self, true
	}
	if l.Remote != nil {
		if node, ok := l.Remote.Locate(thing); ok {
			return node, true
		}
	}
	return "", false
}

// Subscription 是一次订阅请求。
type Subscription struct {
	ClientID       string
	Filter         string
	QoS            uint8
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling uint8
	ID             uint32
	// Persistent 与 Online 仅对共享订阅有意义，用于组内排队判定。
	Persistent bool
}

// Options 是 Router 的构造参数。
type Options struct {
	Self    cluster.NodeID
	Limits  topic.Limits
	Part    cluster.Partitioner
	Locator SessionLocator
	// Now 提供当前时间（Unix 毫秒），仅用于 retained 过期判定。
	Now func() int64
}

// Router 把主题解析为投递目标。
type Router struct {
	self    cluster.NodeID
	limits  topic.Limits
	part    cluster.Partitioner
	locator SessionLocator

	shared   *SharedGroups
	retained *RetainedStore
	interest *InterestSet

	mu   sync.RWMutex
	trie *topic.Trie
	// handles 把 clientID 映射为订阅树句柄；clients 是其反向索引。
	// 句柄是密集的小整数，因此反向索引用切片而非 map。
	handles map[string]topic.Handle
	clients []string
	free    []topic.Handle
	// version 是 retained 的 Lamport 计数器（MQTT-123）。
	// 它只是计数器，节点 ID 的那一半在 NextVersion 里补上。
	version uint64
	// wildcards 是本节点持有的通配符过滤器及其引用计数，
	// byClient 是它的反向索引（会话结束时要按它逐条减）。
	//
	// 只记通配符，有两重理由。其一是正确性：精确订阅不跨节点传播
	// （ARCH-017——设备域主题的归属由 thingName 算出）。其二是内存：
	// 通配符订阅是后端应用的那数十条，而精确订阅是设备的百万条——
	// 给后者留一份 per-client 的字符串集合会让每订阅的内存翻倍，
	// 而 TST-127 的基线正钉着那个数字（185 字节/订阅）。
	//
	// 引用计数而不是集合：两个客户端订阅同一个过滤器时，
	// 一个退订不该把这条兴趣从集群里撤掉。
	wildcards map[string]int
	byClient  map[string]map[string]bool
}

// New 创建一个路由器。
func New(opt Options) *Router {
	if opt.Part == nil {
		opt.Part = cluster.NewLocalPartitioner(opt.Self, 0)
	}
	if opt.Locator == nil {
		opt.Locator = LocalLocator{Self: opt.Self}
	}
	return &Router{
		self:      opt.Self,
		limits:    opt.Limits,
		part:      opt.Part,
		locator:   opt.Locator,
		shared:    NewSharedGroups(opt.Self, opt.Limits),
		retained:  NewRetainedStore(opt.Limits, opt.Now),
		interest:  NewInterestSet(opt.Limits),
		trie:      topic.New(opt.Limits),
		handles:   make(map[string]topic.Handle),
		wildcards: make(map[string]int),
		byClient:  make(map[string]map[string]bool),
	}
}

// Shared 返回共享订阅组管理器。
func (r *Router) Shared() *SharedGroups { return r.shared }

// Retained 返回 retained 存储。
func (r *Router) Retained() *RetainedStore { return r.retained }

// Interest 返回远端兴趣集。
func (r *Router) Interest() *InterestSet { return r.interest }

// NextVersion 分配一个 retained 版本号（Lamport 戳，MQTT-123）。
//
// 计数器加一，再带上本节点 ID 作为平手判据。节点 ID 来自 Options.Self，
// 单机下它是什么都无所谓——那时只有一个版本源，平手判据用不上。
func (r *Router) NextVersion() Version {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.version++
	return Version{Counter: r.version, Node: string(r.self)}
}

// SeedVersion 把版本计数器抬到不低于 v 的位置。
//
// 两个调用方，理由不同但机制相同：
//
//   - 重启后，参数取持久化的保留消息中的最大版本。不这么做的后果是实打实
//     的数据错误：计数器从 0 重新开始，一条新发布的保留消息会拿到版本 1，
//     而覆盖判定是"版本更大才覆盖"——于是它被恢复出来的版本 5000 的旧消息
//     挡住，新消息静默丢失。这与 Shadow 的版本回退（SHDW-071）是同一类
//     错误（MQTT-109）。
//   - 收到远端更新时，参数取对端的版本。这是 Lamport 的接收规则
//     （MQTT-123）：不抬的话，一个刚重启、计数器落后的节点发出的新 retained
//     会被对端更老的那一条盖掉，而且它自己不知道。
//
// 只看 Counter，不看 Node：Node 是平手判据，不是水位。
func (r *Router) SeedVersion(v Version) {
	r.mu.Lock()
	if v.Counter > r.version {
		r.version = v.Counter
	}
	r.mu.Unlock()
}

// Subscribe 登记一条订阅。
//
// 共享订阅交给 SharedGroups，普通订阅进订阅树——「从一组订阅者中选一个」
// 是全局逻辑，与逐个投递的普通订阅走不同入口（契约 10、ARCH-015）。
func (r *Router) Subscribe(s Subscription) error {
	if err := topic.ValidateFilter([]byte(s.Filter), r.limits); err != nil {
		return err
	}
	if share, inner, ok := topic.ParseShared([]byte(s.Filter)); ok {
		r.shared.Join(string(share), string(inner), Member{
			ClientID:   s.ClientID,
			Node:       r.self,
			QoS:        s.QoS,
			Persistent: s.Persistent,
			Online:     true,
		})
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.handleForLocked(s.ClientID)
	// 已经有这条订阅时不重复计数：重复订阅在订阅树里是就地替换
	// （见 TestDuplicateSubscribeDoesNotLeakQuota 那条教训），
	// 兴趣的引用计数也必须跟着就地替换，否则退订一次就少算一次。
	had := r.trie.Has([]byte(s.Filter), h)
	if err := r.trie.Subscribe([]byte(s.Filter), topic.Subscription{
		Handle:         h,
		QoS:            s.QoS,
		NoLocal:        s.NoLocal,
		RetainAsPub:    s.RetainAsPub,
		RetainHandling: s.RetainHandling,
		ID:             s.ID,
	}); err != nil {
		return err
	}
	if !had && topic.HasWildcard([]byte(s.Filter)) {
		r.wildcards[s.Filter]++
		if r.byClient[s.ClientID] == nil {
			r.byClient[s.ClientID] = make(map[string]bool, 1)
		}
		r.byClient[s.ClientID][s.Filter] = true
	}
	return nil
}

// Unsubscribe 移除一条订阅，返回是否确实移除了。
func (r *Router) Unsubscribe(clientID, filter string) bool {
	if share, inner, ok := topic.ParseShared([]byte(filter)); ok {
		return r.shared.Leave(string(share), string(inner), clientID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.handles[clientID]
	if !ok {
		return false
	}
	removed := r.trie.Unsubscribe([]byte(filter), h)
	if removed {
		r.dropWildcardLocked(clientID, filter)
	}
	return removed
}

// dropWildcardLocked 把一条通配符过滤器的引用计数减一。
//
// 按 byClient 判定该不该减，而不是只看过滤器带不带通配符：
// 退订一条从来没订上的过滤器会返回 removed=false，但更隐蔽的是
// 「订阅时已经有了所以没计数、退订时却减了一次」——那会让兴趣
// 提前从集群里消失，而本节点仍有订阅者在等消息。
func (r *Router) dropWildcardLocked(clientID, filter string) {
	own := r.byClient[clientID]
	if !own[filter] {
		return
	}
	delete(own, filter)
	if len(own) == 0 {
		delete(r.byClient, clientID)
	}
	if n := r.wildcards[filter] - 1; n > 0 {
		r.wildcards[filter] = n
	} else {
		delete(r.wildcards, filter)
	}
}

// LocalWildcards 返回本节点持有的通配符过滤器，供兴趣通告使用。
//
// 返回全量快照：InterestSet.Advertise 要的就是快照，通告丢一次或乱序
// 都不会让兴趣集永久偏离（ARCH-012 的最终一致）。
func (r *Router) LocalWildcards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.wildcards))
	for f := range r.wildcards {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// Has 报告路由表是否已持有某客户端对该过滤器的订阅。
//
// 全局订阅额度的权威是路由表而不是会话（见 broker 的 grantSubscription）：
// 额度数的是"路由表里有多少条订阅"，判定也必须问路由表。拿会话去判会错，
// 因为两者会不一致——接管、会话过期都能让路由表里留着当前会话不知道的
// 订阅，那时按会话判会多占一份额度而路由表就地替换，额度于此多出一份。
func (r *Router) Has(clientID, filter string) bool {
	if share, inner, ok := topic.ParseShared([]byte(filter)); ok {
		return r.shared.Has(string(share), string(inner), clientID)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handles[clientID]
	if !ok {
		return false
	}
	return r.trie.Has([]byte(filter), h)
}

// RemoveClient 移除某客户端的全部订阅，返回移除条数。会话结束时调用。
func (r *Router) RemoveClient(clientID string) int {
	n := r.shared.LeaveAll(clientID)
	r.mu.Lock()
	defer r.mu.Unlock()
	for f := range r.byClient[clientID] {
		r.dropWildcardLocked(clientID, f)
	}
	if h, ok := r.handles[clientID]; ok {
		n += r.trie.RemoveHandle(h)
		r.releaseHandleLocked(clientID, h)
	}
	return n
}

// Route 返回主题的本地订阅者与须转发的远端节点。
//
// 契约 02 的核心：从 v0.1 起就返回两份结果。单机下 remote 恒为空，
// 但调用方必须写出两条分支——事后插入转发步骤时，整条投递路径已经
// 假设目标是本地指针，届时 retained 下发、遗嘱、共享订阅、Shadow 回包
// 全部要改（契约 02 的延后代价）。
func (r *Router) Route(name []byte) (local []SubRef, remote []cluster.NodeID) {
	return r.RouteInto(nil, nil, name)
}

// RouteInto 是 Route 的复用缓冲版本，供热路径使用。
func (r *Router) RouteInto(local []SubRef, remote []cluster.NodeID, name []byte) ([]SubRef, []cluster.NodeID) {
	r.mu.RLock()
	r.trie.Match(name, func(s topic.SubRef) bool {
		if cid, ok := r.clientOfLocked(s.Handle); ok {
			local = append(local, SubRef{
				ClientID:       cid,
				ID:             s.ID,
				QoS:            s.QoS,
				NoLocal:        s.NoLocal,
				RetainAsPub:    s.RetainAsPub,
				RetainHandling: s.RetainHandling,
			})
		}
		return true
	})
	r.mu.RUnlock()

	remote = r.remoteInto(remote, name)
	return local, remote
}

// remoteInto 计算须转发的远端节点。
func (r *Router) remoteInto(dst []cluster.NodeID, name []byte) []cluster.NodeID {
	class, scope := topic.Classify(name)
	if class == topic.ClassDeviceScoped {
		// 设备域主题：不进入全局订阅表，经归属查询定位（见 SessionLocator）。
		if node, ok := r.locator.LocateThing(scope); ok {
			if node != r.self {
				return append(dst, node)
			}
			return dst
		}
		// 归属查不到时**继续查兴趣表**，不要就此返回（MQTT-128）。
		//
		// 归属只记在线的连接。设备离线时没有任何节点归属它，而它的
		// 持久会话连着订阅仍留在它上次接入的那个节点的路由表里——那个
		// 节点通告过 `$aws/things/<设备>/#` 这类通配符兴趣。就此返回的
		// 后果是那一帧永远不被转过去，于是**发给离线设备的 QoS 1 只在
		// 它上次接入的那个节点上入队**，在别的节点上发布就直接消失。
		//
		// 这个缺口是会话复制接上之后在三进程验收里压出来的（2026-11-20）：
		// 会话与队列明明已经复制到三个节点，而"在 b 上发、到 c 上收"
		// 仍然收不到——盘上根本没有入队记录，那一帧在 b 上就没出去。
		// 单机碰不到它（没有远端），而集群的用例此前只验在线设备。
		//
		// 不会重复投递：兴趣只让帧到达那个节点，而 enqueueOffline 要求
		// 本地路由表里真有这个 clientId 的订阅，那只有它上次接入的那个
		// 节点才成立。
	}
	// 其余主题：只有通告过通配符兴趣的节点需要转发。
	return r.interest.MatchInto(dst, name)
}

// RouteShared 返回主题命中的各共享订阅组的投递目标。
//
// 多个含通配符的共享订阅同时命中时，每组各投一份副本（PRD MQTT-093）：
// 组之间是广播，组内才是负载均衡。
func (r *Router) RouteShared(name []byte) []Target {
	return r.shared.MatchInto(nil, name)
}

// RouteSharedInto 是 RouteShared 的复用缓冲版本。
func (r *Router) RouteSharedInto(dst []Target, name []byte) []Target {
	return r.shared.MatchInto(dst, name)
}

// AdvertiseSharedRemote 吃下一个节点通告来的共享订阅组成员数（MQTT-116）。
func (r *Router) AdvertiseSharedRemote(node cluster.NodeID, counts []cluster.ShareCount) {
	r.shared.AdvertiseRemote(node, counts)
}

// ForgetSharedRemote 丢掉一个节点的共享订阅组成员数。
func (r *Router) ForgetSharedRemote(node cluster.NodeID) { r.shared.ForgetRemote(node) }

// LocalSharedCounts 是本节点各共享订阅组的在线成员数，供周期性通告使用。
func (r *Router) LocalSharedCounts() []cluster.ShareCount { return r.shared.LocalCounts() }

// PickLocalShared 只在本节点的成员里为某个组选一个目标（MQTT-116）。
func (r *Router) PickLocalShared(share, filter string) (Target, bool) {
	return r.shared.PickLocal(share, filter)
}

// RetainedFor 返回新订阅应收到的 retained 消息。
//
// AWS 的通配符订阅收不到 retained 消息，而规范要求能收到；
// 此处依 limits.AWSStrict 决定行为（PRD CMP-012）。
// 共享订阅在两种模式下都不接收 retained（PRD MQTT-053）。
func (r *Router) RetainedFor(filter []byte) []*Message {
	if topic.IsShared(filter) {
		return nil
	}
	if r.limits.AWSStrict && topic.HasWildcard(filter) {
		return nil
	}
	return r.retained.MatchInto(nil, filter)
}

// Stats 是路由器的规模指标。
type Stats struct {
	Subscriptions int
	TrieNodes     int
	SharedGroups  int
	Retained      int
	InterestNodes int
	Clients       int
}

// Stats 返回规模指标。
func (r *Router) Stats() Stats {
	r.mu.RLock()
	st := Stats{
		Subscriptions: r.trie.Len(),
		TrieNodes:     r.trie.NodeCount(),
		Clients:       len(r.handles),
	}
	r.mu.RUnlock()
	st.SharedGroups = r.shared.Groups()
	st.Retained = r.retained.Len()
	st.InterestNodes = r.interest.Nodes()
	return st
}

// --- 句柄管理 ---

// handleForLocked 取得或分配某客户端的订阅树句柄。
func (r *Router) handleForLocked(clientID string) topic.Handle {
	if h, ok := r.handles[clientID]; ok {
		return h
	}
	var h topic.Handle
	if n := len(r.free); n > 0 {
		h = r.free[n-1]
		r.free = r.free[:n-1]
		r.clients[h] = clientID
	} else {
		h = topic.Handle(len(r.clients))
		r.clients = append(r.clients, clientID)
	}
	r.handles[clientID] = h
	return h
}

func (r *Router) clientOfLocked(h topic.Handle) (string, bool) {
	if h < 0 || int(h) >= len(r.clients) {
		return "", false
	}
	cid := r.clients[h]
	return cid, cid != ""
}

// releaseHandleLocked 回收客户端的句柄。
//
// 只在 RemoveClient 时回收，而不在每次退订后探测该客户端是否已无订阅：
// 订阅树不提供「按句柄计数」，探测的代价高于保留一个切片槽位。
func (r *Router) releaseHandleLocked(clientID string, h topic.Handle) {
	delete(r.handles, clientID)
	if int(h) < len(r.clients) {
		r.clients[h] = ""
		r.free = append(r.free, h)
	}
}
