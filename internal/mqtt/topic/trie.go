package topic

import (
	"bytes"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// Handle 是调用方赋予订阅者的句柄。
//
// 订阅树不持有任何指向会话的指针（契约 13），由上层维护 Handle 到会话的映射。
// 这同时满足契约 02：Route 返回的是值类型而非会话指针，使投递路径天然分为
// 本地与远端两条分支。
type Handle int32

// 索引类型。全部为 int32，使节点与订阅记录中不出现任何指针。
type (
	nodeIdx = int32
	subIdx  = int32
	segIdx  = int32
)

const nilIdx int32 = -1

// 预置的段 id：两个通配符。实际段从 2 开始分配。
const (
	segPlus segIdx = 0
	segHash segIdx = 1
)

// 订阅选项位。
const (
	optNoLocal uint8 = 1 << 0
	optRAP     uint8 = 1 << 1
)

// node 是订阅树的一个节点。
//
// 全部字段为 int32，不含指针、不含字符串、不含切片。这使得 GC 对 nodes
// 切片的扫描成本与节点数无关（契约 13、PRD ARCH-043）。
// 段文本存放在 segTable 中，子节点查找经 Trie.children 这一无指针的 map。
type node struct {
	parent nodeIdx
	seg    segIdx
	subs   subIdx // 本节点订阅链表头
	nchild int32  // 子节点数，用于空节点回收
}

// sub 是一条订阅记录。同样不含指针。
//
// 节点内链表为双向，使按 (filter, handle) 退订时可以 O(1) 摘除；
// 按 handle 的链表为单向，因为断连时总是整条遍历（每连接订阅数有上限，
// 见 PRD QTA-018）。
type sub struct {
	nodePrev   subIdx
	nodeNext   subIdx
	handleNext subIdx
	node       nodeIdx
	handle     Handle
	id         uint32 // 订阅标识符，0 表示无
	qos        uint8
	opts       uint8
	retainH    uint8
	_          uint8
}

// Subscription 是一次订阅请求的参数。
type Subscription struct {
	Handle         Handle
	QoS            uint8
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling uint8
	ID             uint32 // 订阅标识符，0 表示无
}

// SubRef 是一次匹配命中的订阅。值类型，不含指针。
type SubRef struct {
	Handle         Handle
	ID             uint32
	QoS            uint8
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling uint8
}

// Trie 是一棵主题过滤器树。
//
// 不是并发安全的；调用方负责加锁。本项目中它由路由层持有，
// 读多写少，适合读写锁。
//
// 共享订阅不由本结构管理：$share/ 形式的过滤器须交给契约 10 的
// SharedGroups，因为"从一组订阅者中选一个"是全局逻辑。Subscribe 会拒绝它们。
type Trie struct {
	nodes    []node
	subs     []sub
	nodeFree []nodeIdx
	subFree  []subIdx

	// children 把 (parent, seg) 映射到子节点。
	// 键与值都不含指针，Go 的 GC 完全不扫描这类 map——
	// 这既避免了兄弟链表 O(子节点数) 的查找，又不引入可扫描的指针。
	children map[uint64]nodeIdx

	// handleHead 是每个 Handle 的订阅链表头。键值同样无指针。
	handleHead map[Handle]subIdx

	segs   segTable
	limits Limits
	nsub   int
}

// New 创建一棵空的订阅树。
func New(l Limits) *Trie {
	t := &Trie{
		children:   make(map[uint64]nodeIdx),
		handleHead: make(map[Handle]subIdx),
		limits:     l,
	}
	t.segs.init()
	// 根节点。
	t.nodes = append(t.nodes, node{parent: nilIdx, seg: nilIdx, subs: nilIdx})
	return t
}

// Len 返回订阅总数。
func (t *Trie) Len() int { return t.nsub }

// NodeCount 返回在用节点数，含根节点。用于指标与容量测试。
func (t *Trie) NodeCount() int { return len(t.nodes) - len(t.nodeFree) }

func childKey(parent nodeIdx, seg segIdx) uint64 {
	return uint64(uint32(parent))<<32 | uint64(uint32(seg))
}

// Subscribe 加入或更新一条订阅。
//
// 同一 (filter, Handle) 重复订阅时替换其选项，与 MQTT-3.8.4 一致。
func (t *Trie) Subscribe(filter []byte, s Subscription) error {
	if IsShared(filter) {
		return reason.Errorf(reason.TopicFilterInvalid,
			"共享订阅须交给 SharedGroups 管理，不入订阅树")
	}
	if err := ValidateFilter(filter, t.limits); err != nil {
		return err
	}
	n := nodeIdx(0)
	it := newIter(filter)
	for {
		lvl, ok := it.next()
		if !ok {
			break
		}
		seg := t.segs.intern(lvl)
		n = t.childOrCreate(n, seg)
	}
	// 已存在同 handle 的订阅则更新选项。
	for i := t.nodes[n].subs; i != nilIdx; i = t.subs[i].nodeNext {
		if t.subs[i].handle == s.Handle {
			t.fillSub(i, s)
			return nil
		}
	}
	si := t.allocSub()
	t.fillSub(si, s)
	t.subs[si].node = n
	// 挂到节点链表头。
	t.subs[si].nodePrev = nilIdx
	t.subs[si].nodeNext = t.nodes[n].subs
	if t.nodes[n].subs != nilIdx {
		t.subs[t.nodes[n].subs].nodePrev = si
	}
	t.nodes[n].subs = si
	// 挂到 handle 链表头。
	//
	// 必须用双值取值：map 的零值是 0，而 0 是一个合法的订阅索引，
	// 直接取值会把"该 handle 尚无订阅"误当成"下一个是订阅 0"，
	// 从而让链表串进别人的订阅。
	head, ok := t.handleHead[s.Handle]
	if !ok {
		head = nilIdx
	}
	t.subs[si].handleNext = head
	t.handleHead[s.Handle] = si
	t.nsub++
	return nil
}

func (t *Trie) fillSub(i subIdx, s Subscription) {
	sb := &t.subs[i]
	sb.handle = s.Handle
	sb.qos = s.QoS
	sb.retainH = s.RetainHandling
	sb.id = s.ID
	sb.opts = 0
	if s.NoLocal {
		sb.opts |= optNoLocal
	}
	if s.RetainAsPub {
		sb.opts |= optRAP
	}
}

// Unsubscribe 移除一条订阅，返回是否确实移除了。
func (t *Trie) Unsubscribe(filter []byte, h Handle) bool {
	if IsShared(filter) {
		return false
	}
	n, ok := t.find(filter)
	if !ok {
		return false
	}
	for i := t.nodes[n].subs; i != nilIdx; i = t.subs[i].nodeNext {
		if t.subs[i].handle == h {
			t.unlinkFromHandle(i, h)
			t.unlinkFromNode(i)
			t.freeSub(i)
			t.nsub--
			t.prune(n)
			return true
		}
	}
	return false
}

// Has 报告某个 Handle 是否已持有该过滤器的订阅。
//
// 给配额判定用：全局订阅额度跟着「订阅是否在树里」走，而占额度必须发生在
// 插入之前（SUBACK 的原因码要先定下来），所以需要一次只读的预判。
// 结构与 Unsubscribe 一致，只是不动链表。
func (t *Trie) Has(filter []byte, h Handle) bool {
	if IsShared(filter) {
		return false // 共享订阅不入树，由 SharedGroups 回答
	}
	n, ok := t.find(filter)
	if !ok {
		return false
	}
	for i := t.nodes[n].subs; i != nilIdx; i = t.subs[i].nodeNext {
		if t.subs[i].handle == h {
			return true
		}
	}
	return false
}

// RemoveHandle 移除某个 Handle 的全部订阅，返回移除条数。
// 会话断开时调用。
func (t *Trie) RemoveHandle(h Handle) int {
	i, ok := t.handleHead[h]
	if !ok {
		return 0
	}
	removed := 0
	for i != nilIdx {
		next := t.subs[i].handleNext
		n := t.subs[i].node
		t.unlinkFromNode(i)
		t.freeSub(i)
		t.nsub--
		t.prune(n)
		removed++
		i = next
	}
	delete(t.handleHead, h)
	return removed
}

// Match 对每个命中 name 的订阅调用 fn。fn 返回 false 时提前停止。
//
// 本方法不分配内存。契约 02 要求路由返回本地与远端两份目标，
// 远端部分由上层依兴趣集与分区归属计算，本方法只负责本地订阅。
func (t *Trie) Match(name []byte, fn func(SubRef) bool) {
	t.match(0, newIter(name), true, fn)
}

// MatchAppend 把命中的订阅追加到 dst 并返回。传入复用的切片可避免分配。
func (t *Trie) MatchAppend(dst []SubRef, name []byte) []SubRef {
	t.Match(name, func(r SubRef) bool {
		dst = append(dst, r)
		return true
	})
	return dst
}

func (t *Trie) match(n nodeIdx, it iter, first bool, fn func(SubRef) bool) bool {
	lvl, ok := it.next()
	if !ok {
		// 主题层级耗尽：命中本节点上的订阅。
		if !t.emit(n, fn) {
			return false
		}
		// 规范行为：sport/# 也匹配 sport 本身（MQTT-4.7.1.2）。
		// AWS 偏离此规则，见 PRD CMP-011。
		if !t.limits.AWSStrict {
			if h, found := t.child(n, segHash); found {
				if !t.emit(h, fn) {
					return false
				}
			}
		}
		return true
	}
	// 以通配符开头的过滤器不得匹配以 $ 开头的主题（MQTT-4.7.2）。
	dollar := first && len(lvl) > 0 && lvl[0] == '$'
	if !dollar {
		if h, found := t.child(n, segHash); found {
			if !t.emit(h, fn) {
				return false
			}
		}
		if p, found := t.child(n, segPlus); found {
			if !t.match(p, it, false, fn) {
				return false
			}
		}
	}
	// 精确层级分支。
	//
	// 必须排除两个保留的通配符段 id：主题名中字面为 "+" 或 "#" 的层级
	// 会被段表解析为 segPlus / segHash，若不排除就会让精确分支与上面的
	// 通配符分支指向同一个节点，造成同一订阅被投递两次。
	//
	// 这同时是语义上正确的做法：MQTT 的过滤器中 "+" 与 "#" 永远是通配符，
	// 不存在"字面为通配符字符的精确订阅"，因此精确分支本就不该命中它们。
	// 合法的主题名不含通配符（MQTT-3.3.2.1，由 ValidateName 与解码器保证），
	// 此处的排除属于纵深防御。
	if seg, found := t.segs.find(lvl); found && seg != segPlus && seg != segHash {
		if c, ok := t.child(n, seg); ok {
			if !t.match(c, it, false, fn) {
				return false
			}
		}
	}
	return true
}

func (t *Trie) emit(n nodeIdx, fn func(SubRef) bool) bool {
	for i := t.nodes[n].subs; i != nilIdx; i = t.subs[i].nodeNext {
		sb := &t.subs[i]
		if !fn(SubRef{
			Handle:         sb.handle,
			ID:             sb.id,
			QoS:            sb.qos,
			NoLocal:        sb.opts&optNoLocal != 0,
			RetainAsPub:    sb.opts&optRAP != 0,
			RetainHandling: sb.retainH,
		}) {
			return false
		}
	}
	return true
}

// --- 节点与订阅的分配与回收 ---

func (t *Trie) child(n nodeIdx, seg segIdx) (nodeIdx, bool) {
	c, ok := t.children[childKey(n, seg)]
	return c, ok
}

func (t *Trie) childOrCreate(n nodeIdx, seg segIdx) nodeIdx {
	if c, ok := t.child(n, seg); ok {
		return c
	}
	c := t.allocNode()
	t.nodes[c] = node{parent: n, seg: seg, subs: nilIdx}
	t.children[childKey(n, seg)] = c
	t.nodes[n].nchild++
	return c
}

// find 定位过滤器对应的节点，不创建。
func (t *Trie) find(filter []byte) (nodeIdx, bool) {
	n := nodeIdx(0)
	it := newIter(filter)
	for {
		lvl, ok := it.next()
		if !ok {
			return n, true
		}
		seg, found := t.segs.find(lvl)
		if !found {
			return nilIdx, false
		}
		n, found = t.child(n, seg)
		if !found {
			return nilIdx, false
		}
	}
}

// prune 自下而上回收既无订阅又无子节点的节点。
func (t *Trie) prune(n nodeIdx) {
	for n > 0 && t.nodes[n].subs == nilIdx && t.nodes[n].nchild == 0 {
		p, seg := t.nodes[n].parent, t.nodes[n].seg
		delete(t.children, childKey(p, seg))
		t.nodes[p].nchild--
		t.freeNode(n)
		n = p
	}
}

func (t *Trie) allocNode() nodeIdx {
	if k := len(t.nodeFree); k > 0 {
		i := t.nodeFree[k-1]
		t.nodeFree = t.nodeFree[:k-1]
		return i
	}
	t.nodes = append(t.nodes, node{})
	return nodeIdx(len(t.nodes) - 1)
}

func (t *Trie) freeNode(i nodeIdx) {
	t.nodes[i] = node{parent: nilIdx, seg: nilIdx, subs: nilIdx}
	t.nodeFree = append(t.nodeFree, i)
}

func (t *Trie) allocSub() subIdx {
	if k := len(t.subFree); k > 0 {
		i := t.subFree[k-1]
		t.subFree = t.subFree[:k-1]
		return i
	}
	t.subs = append(t.subs, sub{})
	return subIdx(len(t.subs) - 1)
}

func (t *Trie) freeSub(i subIdx) {
	t.subs[i] = sub{nodePrev: nilIdx, nodeNext: nilIdx, handleNext: nilIdx, node: nilIdx}
	t.subFree = append(t.subFree, i)
}

func (t *Trie) unlinkFromNode(i subIdx) {
	sb := &t.subs[i]
	if sb.nodePrev != nilIdx {
		t.subs[sb.nodePrev].nodeNext = sb.nodeNext
	} else {
		t.nodes[sb.node].subs = sb.nodeNext
	}
	if sb.nodeNext != nilIdx {
		t.subs[sb.nodeNext].nodePrev = sb.nodePrev
	}
}

func (t *Trie) unlinkFromHandle(i subIdx, h Handle) {
	head, ok := t.handleHead[h]
	if !ok {
		return
	}
	if head == i {
		if next := t.subs[i].handleNext; next != nilIdx {
			t.handleHead[h] = next
		} else {
			delete(t.handleHead, h)
		}
		return
	}
	for j := head; j != nilIdx; j = t.subs[j].handleNext {
		if t.subs[j].handleNext == i {
			t.subs[j].handleNext = t.subs[i].handleNext
			return
		}
	}
}

// --- 段表 ---

// segTable 把层级文本驻留为整型 id。
//
// 文本连续存放于 buf，offs 记录边界，因此节点里只需要一个 int32。
// 去重索引以 64 位哈希为键、以 next 数组做冲突链，键与值都不含指针，
// 故 GC 不扫描该 map；若以 map[string]segIdx 实现，百万级不同段
// 就会带来百万个被扫描的字符串头。
type segTable struct {
	buf   []byte
	offs  []int32
	next  []segIdx
	index map[uint64]segIdx
}

func (s *segTable) init() {
	s.index = make(map[uint64]segIdx)
	s.offs = append(s.offs, 0)
	// 预置两个通配符，使其 id 固定为 segPlus 与 segHash。
	s.intern([]byte("+"))
	s.intern([]byte("#"))
}

func (s *segTable) at(i segIdx) []byte {
	return s.buf[s.offs[i]:s.offs[i+1]]
}

func (s *segTable) find(b []byte) (segIdx, bool) {
	h := hash64(b)
	head, ok := s.index[h]
	if !ok {
		return nilIdx, false
	}
	for i := head; i != nilIdx; i = s.next[i] {
		if bytes.Equal(s.at(i), b) {
			return i, true
		}
	}
	return nilIdx, false
}

func (s *segTable) intern(b []byte) segIdx {
	if i, ok := s.find(b); ok {
		return i
	}
	i := segIdx(len(s.offs) - 1)
	s.buf = append(s.buf, b...)
	s.offs = append(s.offs, int32(len(s.buf)))
	h := hash64(b)
	head, ok := s.index[h]
	if !ok {
		head = nilIdx
	}
	s.next = append(s.next, head)
	s.index[h] = i
	return i
}

// hash64 是 FNV-1a，内联且不分配。
func hash64(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}
