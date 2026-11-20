package meta

import (
	"sync"
	"sync/atomic"
)

// shardCount 是热视图的分片数。
//
// 读取是 RLock 单片加一次 map 查找，随核心数线性扩展；写入稀疏
// （注册、吊销、策略变更），分片锁的代价可忽略。接口允许后续替换为
// 不可变 HAMT 以实现无锁读（PRD CLU-022）。
const shardCount = 256

// certHot 是鉴权热路径需要的证书字段，紧凑且无指针。
//
// 只保留热路径真正读取的三项。两个字段被有意移出：
//   - 到期时间：TLS 握手本身就会校验 notAfter，broker 不需要在热视图里
//     重复判定，它只是控制台的到期预警所需，属冷层；
//   - 签发 CA：对已注册证书而言，CA 只是审计与控制台关心的信息，属冷层。
//
// 这两项各占 4 到 8 字节，在百万证书规模下合计十几 MB，
// 而它们在热路径上一次都不会被读。
type certHot struct {
	status    CertStatus
	policySet PolicySetID
	thing     nameRef // 绑定的设备名；noRef 表示未绑定或绑定了多个
}

// thingHot 是热路径需要的 Thing 字段子集。
//
// 完整的 Thing 记录留在冷层。热层只保留鉴权真正会用到的东西：
// 绑定的策略集，以及被策略变量实际引用的那几个属性。
// 若把全部属性都物化进热视图，百万设备的内存会远超预算（PRD CLU-021）。
type thingHot struct {
	policySet PolicySetID
	// attrsRef 指向属性侧表；noRef 表示没有被策略引用的属性。
	//
	// 这里用一个 int32 间接引用而不是直接内嵌 []string：切片头即使为 nil
	// 也占 24 字节，在百万设备规模下仅此一项就是数十 MB，
	// 而「策略引用了设备属性」是少数情形，多数部署的策略只用
	// ${iot:ClientId} 或设备名。
	attrsRef int32
}

type certShard struct {
	mu sync.RWMutex
	m  map[CertFingerprint]certHot
}

type thingShard struct {
	mu sync.RWMutex
	m  map[nameRef]thingHot
}

// Cert 是对外暴露的证书热信息，名字已解析。
//
// 证书的到期时间与签发 CA 不在热视图中，须查冷层——见 certHot 的说明。
type Cert struct {
	Status    CertStatus
	PolicySet PolicySetID
	// Thing 是绑定的设备名；为空表示未绑定或绑定了多个，须查冷层。
	Thing string
}

// Thing 是对外暴露的设备热信息。
type Thing struct {
	Name      string
	PolicySet PolicySetID
	// Attrs 按 View.AttrSlots 的槽位顺序排列，只含被策略引用的属性。
	Attrs []string
}

// PolicySet 是一组已编译的策略。
//
// 具体的匹配器结构由 authz 包定义并在 apply 阶段填充；本包只按 ID 存取，
// 不解释其内容。这个间接层使策略编译器可以独立演进，而 View 的形状不变。
type PolicySet struct {
	ID       PolicySetID
	Name     string
	Revision Revision
	// Compiled 是 authz 编译出的匹配器。本包视其为不透明值。
	Compiled any
}

// View 是元数据的本节点全量热视图。
//
// 契约 05 的全部方法都没有 context.Context、没有 error：这个签名本身
// 就是「本地内存、不阻塞、不会失败」的承诺，使任何人都不可能在连接路径上
// 偷偷加一发 RPC。CI 中有自定义分析器强制该签名（PRD ARCH-054）。
type View struct {
	rev               atomic.Uint64
	confirmedAtUnixMs atomic.Int64

	names *nameTable
	attrs *attrStore

	certs  [shardCount]certShard
	things [shardCount]thingShard

	// policies 与 attrSlots 的写入发生在 apply 循环，读取受 policyMu 保护。
	// 它们的规模远小于证书与设备表，不需要分片。
	policyMu  sync.RWMutex
	policies  map[PolicySetID]PolicySet
	attrSlots []string
}

// NewView 创建一个空的热视图。
func NewView() *View {
	v := &View{
		names:    newNameTable(),
		attrs:    newAttrStore(),
		policies: make(map[PolicySetID]PolicySet),
	}
	for i := range v.certs {
		v.certs[i].m = make(map[CertFingerprint]certHot)
	}
	for i := range v.things {
		v.things[i].m = make(map[nameRef]thingHot)
	}
	return v
}

func certShardOf(fp CertFingerprint) int {
	// 指纹本身是密码学哈希，直接取低位即可获得均匀分布。
	return int(fp[31]) % shardCount
}

func thingShardOf(r nameRef) int {
	return int(uint32(r) % shardCount)
}

// Revision 返回视图已应用到的复制日志索引。
func (v *View) Revision() Revision { return Revision(v.rev.Load()) }

// LastConfirmedUnixMs 返回最后一次确认视图已追平 leader 的时间。
//
// 它只用于陈旧度判定与展示，不得用于比较元数据的先后——
// 那由 Revision 确定（契约 08）。
func (v *View) LastConfirmedUnixMs() int64 { return v.confirmedAtUnixMs.Load() }

// Stale 报告视图是否已陈旧到不应再接受新连接。
//
// 这是自建场景独有而托管服务不存在的问题：网络分区中的少数派节点，
// 其视图冻结在最后提交的 revision 上，可能仍然接受一张在多数派侧
// 已被吊销的证书。因此超过阈值后对新连接 fail-closed，
// 但不得断开已有连接——控制面故障不应放大为数据面全网掉线
// （PRD CLU-023 至 CLU-025）。
//
// 吊销的传播上界因此等于该阈值，而非「立即」，这一点须在用户文档声明。
func (v *View) Stale(nowUnixMs, maxStalenessMs int64) bool {
	if maxStalenessMs <= 0 {
		return false
	}
	last := v.confirmedAtUnixMs.Load()
	if last == 0 {
		// 从未确认过追平：视为陈旧，节点在 ready 之前不应接受设备连接
		// （PRD CLU-032）。
		return true
	}
	return nowUnixMs-last > maxStalenessMs
}

// CertStatus 返回证书状态，未注册时返回 CertUnknown。
//
// 这是最热的一次查询：每次 CONNECT 一次，每条 PUBLISH 与 SUBSCRIBE
// 的授权判定也会用到。零分配。
func (v *View) CertStatus(fp CertFingerprint) CertStatus {
	s := &v.certs[certShardOf(fp)]
	s.mu.RLock()
	h, ok := s.m[fp]
	s.mu.RUnlock()
	if !ok {
		return CertUnknown
	}
	return h.status
}

// CertPolicySet 返回证书绑定的策略集，未注册时返回 NoPolicySet。零分配。
func (v *View) CertPolicySet(fp CertFingerprint) PolicySetID {
	s := &v.certs[certShardOf(fp)]
	s.mu.RLock()
	h, ok := s.m[fp]
	s.mu.RUnlock()
	if !ok {
		return NoPolicySet
	}
	return h.policySet
}

// Cert 返回证书的完整热信息，名字已解析。
//
// 相比 CertStatus 它会物化字符串，因此只应在连接路径上调用一次，
// 不要放进逐消息的授权判定。
func (v *View) Cert(fp CertFingerprint) (Cert, bool) {
	s := &v.certs[certShardOf(fp)]
	s.mu.RLock()
	h, ok := s.m[fp]
	s.mu.RUnlock()
	if !ok {
		return Cert{}, false
	}
	return Cert{
		Status:    h.status,
		PolicySet: h.policySet,
		Thing:     v.names.at(h.thing),
	}, true
}

// Thing 返回设备的热信息。
func (v *View) Thing(name string) (Thing, bool) {
	ref, ok := v.names.find(name)
	if !ok {
		return Thing{}, false
	}
	s := &v.things[thingShardOf(ref)]
	s.mu.RLock()
	h, ok := s.m[ref]
	s.mu.RUnlock()
	if !ok {
		return Thing{}, false
	}
	return Thing{Name: name, PolicySet: h.policySet, Attrs: v.attrs.get(h.attrsRef)}, true
}

// ThingPolicySet 返回设备绑定的策略集，不存在时返回 NoPolicySet。
func (v *View) ThingPolicySet(name string) PolicySetID {
	ref, ok := v.names.find(name)
	if !ok {
		return NoPolicySet
	}
	s := &v.things[thingShardOf(ref)]
	s.mu.RLock()
	h, ok := s.m[ref]
	s.mu.RUnlock()
	if !ok {
		return NoPolicySet
	}
	return h.policySet
}

// PolicySet 按 ID 取出已编译的策略集。
func (v *View) PolicySet(id PolicySetID) (PolicySet, bool) {
	if id == NoPolicySet {
		return PolicySet{}, false
	}
	v.policyMu.RLock()
	defer v.policyMu.RUnlock()
	p, ok := v.policies[id]
	return p, ok
}

// AttrSlots 返回被策略引用的属性名及其槽位顺序。
// Thing.Attrs 按该顺序排列。
func (v *View) AttrSlots() []string {
	v.policyMu.RLock()
	defer v.policyMu.RUnlock()
	return v.attrSlots
}

// ThingAttr 取出设备的某个属性。只有被策略引用的属性在热视图中可见；
// 其余属性须查冷层。
func (v *View) ThingAttr(name, attr string) (string, bool) {
	v.policyMu.RLock()
	slot := -1
	for i, a := range v.attrSlots {
		if a == attr {
			slot = i
			break
		}
	}
	v.policyMu.RUnlock()
	if slot < 0 {
		return "", false
	}
	t, ok := v.Thing(name)
	if !ok || slot >= len(t.Attrs) {
		return "", false
	}
	return t.Attrs[slot], true
}

// Stats 返回视图的规模指标，供 Prometheus 与容量测试使用。
func (v *View) Stats() Stats {
	st := Stats{Revision: v.Revision(), Names: v.names.Len()}
	for i := range v.certs {
		v.certs[i].mu.RLock()
		st.Certs += len(v.certs[i].m)
		v.certs[i].mu.RUnlock()
	}
	for i := range v.things {
		v.things[i].mu.RLock()
		st.Things += len(v.things[i].m)
		v.things[i].mu.RUnlock()
	}
	v.policyMu.RLock()
	st.PolicySets = len(v.policies)
	v.policyMu.RUnlock()
	return st
}

// Stats 是热视图的规模指标。
type Stats struct {
	Revision   Revision
	Certs      int
	Things     int
	PolicySets int
	Names      int
}

// reset 把热视图清空。
//
// 它只在恢复快照时用（CLU-098）：快照是状态机的全量状态，装它之前
// 必须先把旧内容丢掉，否则快照里**已删除**的记录会以旧值留在视图里——
// 而那种残留在鉴权路径上是放行一张本该已吊销的证书。
//
// 原地清空而不是换一个新 View：View 的指针已经发给了 broker、authz 与
// 各个视图读取方（契约 05 的签名承诺"本地内存、不阻塞、不会失败"），
// 换指针等于让他们继续读一份再也不更新的旧视图。
func (v *View) reset() {
	for i := range v.certs {
		v.certs[i].mu.Lock()
		v.certs[i].m = make(map[CertFingerprint]certHot)
		v.certs[i].mu.Unlock()
	}
	for i := range v.things {
		v.things[i].mu.Lock()
		v.things[i].m = make(map[nameRef]thingHot)
		v.things[i].mu.Unlock()
	}
	v.policyMu.Lock()
	v.policies = make(map[PolicySetID]PolicySet)
	v.attrSlots = nil
	v.policyMu.Unlock()
	// 名字表与属性侧表一起换：它们是上面两张表的间接引用来源，
	// 留着旧的只会让条目永远不被回收（百万设备下是几十 MB）。
	v.names = newNameTable()
	v.attrs = newAttrStore()
	v.rev.Store(0)
}
