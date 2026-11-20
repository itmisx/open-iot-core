package router

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// Version 是 retained 消息的覆盖判定依据：Lamport 逻辑时钟（MQTT-123）。
//
// 契约 10 明确要求按 Version 而非时间戳判定覆盖（ARCH-033）：集群下
// 各节点的墙上时钟不可比，用时间戳会出现「旧消息覆盖新消息」的偶发
// 故障，而这类问题极难排查（契约 08）。
//
// Counter 是本节点的单调计数器，Node 是节点 ID。两者按字典序构成
// **确定性全序**，这是三件事同时成立的唯一形状：
//
//   - 同一节点连发两条（哪怕同一毫秒），后一条必须赢——靠 Counter 单调；
//   - 两节点并发写同一主题，各节点必须收敛到**同一个**胜者——靠 Node
//     平手判据。换成"谁后到谁赢"，两边会各自留下不同的那一条，而且
//     不报错；收敛不变量（TST-155）正是钉这一条的；
//   - 重启后不能回退——靠 SeedVersion 从持久化的最大值抬起（MQTT-109）。
//
// 这里刻意不是 Raft 日志序号：那要求 retained 进共识，而它在消息热路径
// 上，单组写入吞吐是每秒数千（MQTT-122 已排除这条路）。此前本类型的
// 注释写的是「集群下来自分区的 Raft log index」，那是分区组那一步的设计，
// 分区组没有实现而 retained 先接上了跨节点。
type Version struct {
	Counter uint64
	Node    string
}

// After 报告 v 是否应当覆盖 old。
//
// 严格大于：相等即不覆盖，于是同一条更新重复到达是幂等的（广播会重复，
// dump 与事件广播也会撞在一起）。
func (v Version) After(old Version) bool {
	if v.Counter != old.Counter {
		return v.Counter > old.Counter
	}
	return v.Node > old.Node
}

// IsZero 报告这是不是零值版本（升级前的记录、或从未写入）。
func (v Version) IsZero() bool { return v.Counter == 0 && v.Node == "" }

// Message 是一条待路由或已保留的消息。
type Message struct {
	Topic   string
	Payload []byte
	// Props 是已编码的 MQTT 5 属性，原样透传。
	Props  []byte
	QoS    uint8
	Retain bool
	// ExpiryUnixMs 为 0 表示不过期。
	ExpiryUnixMs int64
	// StoredAtUnixMs 是写入时刻。0 表示这条记录没有时间（升级前的数据）。
	//
	// 对**墓碑**（Payload 为空的条目，MQTT-124）它另有一个用处：TTL 回收
	// 的判据。那是垃圾回收，不是覆盖判定——覆盖判定只看 Version，ARCH-033
	// 禁止的是后者用时钟，不是前者。
	StoredAtUnixMs int64
	Version        Version
	// topicB 是 Topic 的字节形式，仅由 RetainedStore 填充，
	// 使通配符匹配不必在每条消息上做一次 string 到 []byte 的转换。
	topicB []byte
}

// RetainedStore 保存 retained 消息。
//
// 它是全局状态而非某个连接的状态，因此必须在接口之后——集群下由复制
// 存储承载（量小，适合走共识），单机下是本地表，接口不变（契约 10）。
type RetainedStore struct {
	mu      sync.RWMutex
	byTopic map[string]*Message
	limits  topic.Limits
	// nowUnixMs 是注入的时钟（契约 08），仅用于过期判定。
	nowUnixMs func() int64
	// sink 是持久化出口，可为 nil。
	sink RetainedSink
	// replicator 是跨节点复制出口，可为 nil（单机）。见 SetReplicator。
	replicator func(topic string, m *Message)
	// persistFailures 计数持久化失败，见 PersistFailures 的说明。
	persistFailures atomic.Uint64
}

// NewRetainedStore 创建 retained 存储。
// RetainedSink 把保留消息的变更写到持久存储。
//
// 定义在本包而不是让存储层注入回调，是为了让"写穿透"覆盖全部变更点：
// 保留消息的写入、清除与过期清理三条路径都在本包内，
// 由调用方在各处手工持久化必然会漏掉一条。
//
// 实现须是幂等的：同一条变更可能因重试被写入多次。
type RetainedSink interface {
	// PutRetained 持久化一条保留消息。
	PutRetained(topic string, m *Message, v Version) error
	// DeleteRetained 删除一条保留消息。删除不存在的主题不是错误。
	DeleteRetained(topic string) error
}

// SetSink 设置持久化出口。为 nil 表示不持久化。
//
// 在接受流量之前调用。运行中替换没有意义——已有的保留消息不会被补写。
func (s *RetainedStore) SetSink(sink RetainedSink) {
	s.mu.Lock()
	s.sink = sink
	s.mu.Unlock()
}

// PersistFailures 返回持久化失败的次数。
//
// 不把错误从 Set 里返回出去，是因为内存状态已经改变、消息已经可以投递，
// 此时让发布失败是更糟的选择。但失败也不能静默：计数非零意味着
// 保留消息可能无法跨重启存活，须由指标与就绪报告暴露出来。
func (s *RetainedStore) PersistFailures() uint64 {
	return s.persistFailures.Load()
}

func NewRetainedStore(l topic.Limits, now func() int64) *RetainedStore {
	if now == nil {
		now = func() int64 { return 0 }
	}
	return &RetainedStore{
		byTopic:   make(map[string]*Message),
		limits:    l,
		nowUnixMs: now,
	}
}

// Set 写入或覆盖一条 retained 消息，并把这次变更广播给其他节点。
//
// 空负载表示清除该主题的 retained 消息（MQTT-3.3.1.3）。仅当 v 按
// Version.After 胜过已有版本时才覆盖，使乱序到达的旧消息不会盖掉新消息。
// 返回是否实际发生了变更。
//
// **发布路径一律用这个方法，不要用 Apply。** 两者的区别只有一处：Set
// 会触发复制（MQTT-122），Apply 不会。方向是刻意选的——漏用 Set 的后果
// 是那条 retained 不出本节点（静默分叉），而误用 Set 的后果只是多播一次
// 幂等的更新。所以默认值放在"会复制"这一边。
func (s *RetainedStore) Set(name string, m *Message, v Version) bool {
	changed, repl, cp := s.apply(name, m, v)
	// 钩子在锁外调用：它是入队（可能还要序列化），不该压在这把锁下面，
	// 而出队顺序无所谓——收端按 Version 定序，这正是有 stamp 的理由。
	//
	// 判据是 cp != nil（这次确实写了状态）而不是 changed（订阅者能否
	// 看出区别）：清除一个本地没有的主题属于"写了但不可观察"，而它
	// 恰恰必须广播出去，见 apply 里的说明。
	if cp != nil && repl != nil {
		repl(name, cp)
	}
	return changed
}

// Apply 写入或覆盖一条 retained 消息，但**不**广播。
//
// 两个调用方：从持久层恢复（那是本节点自己的数据，广播出去没有意义），
// 以及应用远端送来的更新（再广播回去会让两个节点之间无限弹，与 CLU-077
// 同理）。
func (s *RetainedStore) Apply(name string, m *Message, v Version) bool {
	changed, _, _ := s.apply(name, m, v)
	return changed
}

// apply 是 Set 与 Apply 共用的本体，返回是否变更、复制钩子与变更后的条目。
func (s *RetainedStore) apply(name string, m *Message, v Version) (bool, func(string, *Message), *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.byTopic[name]; ok && !v.After(old.Version) {
		return false, nil, nil
	}
	if len(m.Payload) == 0 {
		// 空负载即清除，但**留一块墓碑**而不是删掉条目（MQTT-124）。
		//
		// 直接删掉的后果是：一条迟到的旧负载到达时，上面那句"本地有记录
		// 才比版本"不成立，于是它被当成新消息接受——已清除的 retained
		// 自己回来了。单机下碰不到（本地版本单调且不会迟到），集群下必现。
		old, existed := s.byTopic[name]
		tomb := &Message{
			Topic: name, topicB: []byte(name), Retain: true,
			Version: v, StoredAtUnixMs: s.nowUnixMs(),
		}
		s.byTopic[name] = tomb
		s.persistPut(name, tomb, v)
		// 返回值与复制在这里**分开**判断，这是刻意的：
		//
		//   - 返回值是"订阅者能不能看出区别"，所以清除一个本来就没有的
		//     主题返回未变更（MQTT 的幂等语义，调用方与用例都按这个来）；
		//   - 复制看的是"本节点的状态有没有写"，墓碑一律写、一律广播。
		//     本地没有那条而别的节点有（它漏掉了原来那次发布，或者这条
		//     retained 本来就是在别处发的）时，不广播的后果是那个节点
		//     继续对外提供一条已经被清除的 retained，两边都不报错。
		return existed && !old.Tombstone(), s.replicator, tomb
	}
	cp := *m
	cp.Topic = name
	cp.topicB = []byte(name)
	cp.Version = v
	cp.Payload = append([]byte(nil), m.Payload...)
	if m.Props != nil {
		cp.Props = append([]byte(nil), m.Props...)
	}
	s.byTopic[name] = &cp
	s.persistPut(name, &cp, v)
	return true, s.replicator, &cp
}

// SetReplicator 注册跨节点复制的出口（MQTT-122）。为 nil 表示单机。
//
// 在接受流量之前调用。实现必须**不阻塞**：它在发布路径上被调用，而
// 契约 05 禁止发布路径等网络——所以那头是一个有界队列，满了就丢并计数
// （丢一条的后果见 MQTT-125，不是正确性问题，但要有指标）。
func (s *RetainedStore) SetReplicator(fn func(topic string, m *Message)) {
	s.mu.Lock()
	s.replicator = fn
	s.mu.Unlock()
}

// Tombstone 报告一条条目是不是墓碑（已清除，仅为定序而保留）。
func (m *Message) Tombstone() bool { return len(m.Payload) == 0 }

// persistPut 与 persistDelete 在持有锁时调用 sink。
//
// 在锁内调用是有意的：若放到锁外，两个并发的 Set 可能以与内存状态
// 相反的顺序落盘，于是重启后恢复出来的是被覆盖掉的那一条。
// 代价是 sink 必须快——它是一次本地写入，不是网络调用。
func (s *RetainedStore) persistPut(name string, m *Message, v Version) {
	if s.sink == nil {
		return
	}
	if err := s.sink.PutRetained(name, m, v); err != nil {
		s.persistFailures.Add(1)
	}
}

func (s *RetainedStore) persistDelete(name string) {
	if s.sink == nil {
		return
	}
	if err := s.sink.DeleteRetained(name); err != nil {
		s.persistFailures.Add(1)
	}
}

// Delete 显式删除一条 retained 消息。
//
// 走 Set 的空负载路径，于是它同样留墓碑并同样广播——两条清除路径
// （发布空负载、调这个方法）必须产生同一个结果，分开实现就会有一条漏掉
// 墓碑或漏掉广播。
func (s *RetainedStore) Delete(name string, v Version) bool {
	s.mu.RLock()
	_, ok := s.byTopic[name]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	return s.Set(name, &Message{Topic: name}, v)
}

// Get 取出某个主题的 retained 消息。
func (s *RetainedStore) Get(name []byte) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// m[string(b)] 是 Go 的免分配 map 查找形式。
	m, ok := s.byTopic[string(name)]
	// 墓碑不是消息：它只为定序而存在（MQTT-124），对读侧必须完全不可见，
	// 否则订阅者会收到一条空负载的 retained。
	if !ok || m.Tombstone() || s.expired(m) {
		return nil, false
	}
	return m, true
}

// RetainedMeta 是列举用的摘要。正文留在 Get 里，避免一页把全部负载读出来。
type RetainedMeta struct {
	Topic          string
	PayloadSize    int
	QoS            uint8
	StoredAtUnixMs int64
}

// List 按主题名升序分页。after 是上一页的末尾，空表示从头。
func (s *RetainedStore) List(after string, limit int) ([]RetainedMeta, string) {
	if limit <= 0 {
		limit = 25
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.byTopic))
	for name, m := range s.byTopic {
		if m.Tombstone() || s.expired(m) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var page []RetainedMeta
	next := ""
	for _, name := range names {
		if name <= after {
			continue
		}
		if len(page) == limit {
			next = page[len(page)-1].Topic
			break
		}
		m := s.byTopic[name]
		page = append(page, RetainedMeta{
			Topic: name, PayloadSize: len(m.Payload), QoS: m.QoS,
			StoredAtUnixMs: m.StoredAtUnixMs,
		})
	}
	return page, next
}

// MatchInto 把匹配过滤器的 retained 消息追加到 dst。
//
// 精确过滤器走一次 map 查找；含通配符的过滤器需要遍历全表。
//
// 这个取舍是有意的：通配符订阅来自后端应用，数量是数十而非数百万，
// 且 retained 匹配只发生在订阅时而非每条消息，因此 O(n) 可以接受。
// 若 retained 规模增长到使订阅延迟可感，优化手段是建一棵按主题分段的
// 索引树（与订阅树相反的遍历方向）；这属于后续优化，不在 v0.1 范围。
//
// 另注：AWS 的通配符订阅收不到 retained 消息，而规范要求能收到。
// 本方法按规范实现，AWS 行为由调用方依 limits.AWSStrict 决定是否调用
// （PRD CMP-012）。
func (s *RetainedStore) MatchInto(dst []*Message, filter []byte) []*Message {
	if !topic.HasWildcard(filter) {
		if m, ok := s.Get(filter); ok {
			return append(dst, m)
		}
		return dst
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.byTopic {
		if m.Tombstone() || s.expired(m) {
			continue
		}
		// 已保存消息的 Topic 字段即其主题，无需从 map 键再转换。
		if topic.Match(filter, m.topicB, s.limits) {
			dst = append(dst, m)
		}
	}
	return dst
}

// expired 判定消息是否已过期。须在持锁状态下调用。
func (s *RetainedStore) expired(m *Message) bool {
	if m.ExpiryUnixMs == 0 {
		return false
	}
	return s.nowUnixMs() >= m.ExpiryUnixMs
}

// Sweep 清理已过期的 retained 消息，返回清理条数。
//
// 过期的 retained 消息在新订阅时不应被投递（PRD MQTT-055）。
// Get 与 MatchInto 已做即时过滤，本方法用于回收内存。
func (s *RetainedStore) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for name, m := range s.byTopic {
		switch {
		case m.Tombstone():
			// 墓碑过了 TTL 才回收。TTL 必须远大于一次全量 dump 的周期
			// （MQTT-124）：回收太早，一个刚追上来的节点可能先收到那条
			// 更旧的负载、而墓碑已经没了，于是清除被撤销。
			if m.StoredAtUnixMs == 0 || s.nowUnixMs()-m.StoredAtUnixMs < tombstoneTTLMs {
				continue
			}
		case s.expired(m):
		default:
			continue
		}
		delete(s.byTopic, name)
		// 清理同样要写穿透，否则过期的保留消息会在重启后复活
		s.persistDelete(name)
		n++
	}
	return n
}

// tombstoneTTLMs 是墓碑的保留时长。
//
// 一小时，而 dump 周期是秒级：这中间留了三个数量级，因为回收太早的后果
// （清除被一条更旧的负载撤销）是静默的，而留久一点的代价只是一条空记录。
const tombstoneTTLMs int64 = 60 * 60 * 1000

// All 返回全部条目，**含墓碑**，用于跨节点全量 dump（CLU-108）。
//
// 墓碑必须一起带上：一个离线期间漏掉了清除通知的节点，只有看到墓碑才会
// 把它本地那条撤掉；只传活条目的话，它那条会一直留着，而两边都不报错。
//
// 返回的是内部指针。条目一旦存入就不再被修改（apply 是整条替换），所以
// 调用方只读是安全的。
func (s *RetainedStore) All() []*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Message, 0, len(s.byTopic))
	for _, m := range s.byTopic {
		out = append(out, m)
	}
	return out
}

// Replace 用给定的全集整体替换内存里的保留消息，不写穿透。
//
// 两个调用方都是"磁盘才是真相"的场合：启动时装载，以及 Raft 快照安装
// 之后重载（CLU-107）。必须是一次加锁完成的整体替换——先清空再逐条装入
// 的话，中间那一瞬的订阅者会看到一张空表。
//
// 不写穿透：数据正是从持久层来的，再写一遍只是把同样的字节写回去。
func (s *RetainedStore) Replace(entries []*Message) {
	byTopic := make(map[string]*Message, len(entries))
	for _, m := range entries {
		cp := *m
		cp.topicB = []byte(cp.Topic)
		byTopic[cp.Topic] = &cp
	}
	s.mu.Lock()
	s.byTopic = byTopic
	s.mu.Unlock()
}

// Len 返回 retained 消息条数，含尚未清理的过期项，**不含墓碑**。
//
// 不含墓碑是有意的：这个数字进指标（router_retained）与控制台概览，
// 而墓碑是已经被清除的主题——把它算进去，界面上会显示一个只增不减的
// 数字，看起来像保留消息泄漏。
func (s *RetainedStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, m := range s.byTopic {
		if !m.Tombstone() {
			n++
		}
	}
	return n
}
