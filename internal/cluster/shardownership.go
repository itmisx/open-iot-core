package cluster

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// SwitchableOwnership 是 broker 装配期的后置切换点。单机模式一直
// 使用 LocalOwnership；集群模式要等会话分片选举能力建好后，
// 才能换成 ShardOwnership。Evictions 通道保持不变，因为 broker
// 在切换之前就已经开始监听它。
type SwitchableOwnership struct {
	mu     sync.RWMutex
	target SessionOwnership
	local  SessionOwnership
	events chan Eviction
	wg     sync.WaitGroup
	closed bool
}

func NewSwitchableOwnership(local SessionOwnership, evictBuf int) *SwitchableOwnership {
	if evictBuf <= 0 {
		evictBuf = 64
	}
	s := &SwitchableOwnership{target: local, local: local, events: make(chan Eviction, evictBuf)}
	if local != nil {
		s.pump(local.Evictions())
	}
	return s
}

// Use 切到集群所有权实现。只在设备监听器启动前调用一次。
func (s *SwitchableOwnership) Use(next SessionOwnership) error {
	if next == nil {
		return errors.New("cluster: 所有权实现为 nil")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	old := s.target
	s.target = next
	s.pump(next.Evictions())
	s.mu.Unlock()
	// 装配期还没有 MQTT 连接，旧本地表不含有效归属。
	if old != nil && old != next {
		return old.Close()
	}
	return nil
}

func (s *SwitchableOwnership) pump(src <-chan Eviction) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for ev := range src {
			for {
				select {
				case s.events <- ev:
					break
				default:
					select {
					case <-s.events:
					default:
					}
					continue
				}
				break
			}
		}
	}()
}

func (s *SwitchableOwnership) current() SessionOwnership {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.target
}

func (s *SwitchableOwnership) Claim(ctx context.Context, id string, node NodeID) (Epoch, error) {
	if t := s.current(); t != nil {
		return t.Claim(ctx, id, node)
	}
	return ZeroEpoch, ErrClosed
}
func (s *SwitchableOwnership) Release(ctx context.Context, id string, e Epoch) error {
	if t := s.current(); t != nil {
		return t.Release(ctx, id, e)
	}
	return ErrClosed
}
func (s *SwitchableOwnership) Lookup(ctx context.Context, id string) (NodeID, Epoch, bool) {
	if t := s.current(); t != nil {
		return t.Lookup(ctx, id)
	}
	return "", ZeroEpoch, false
}
func (s *SwitchableOwnership) Evictions() <-chan Eviction { return s.events }

// Stats 在单机所有权实现下返回零值；切到分片实现后返回
// 真实的远程 Claim 队列读数。
func (s *SwitchableOwnership) Stats() OwnershipStats {
	if o, ok := s.current().(*ShardOwnership); ok {
		return o.Stats()
	}
	return OwnershipStats{}
}

func (s *SwitchableOwnership) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	target, local := s.target, s.local
	s.target = nil
	s.mu.Unlock()
	var err error
	if target != nil {
		err = target.Close()
	}
	if local != nil && local != target {
		if e := local.Close(); err == nil {
			err = e
		}
	}
	s.wg.Wait()
	close(s.events)
	return err
}

type ownershipShardState struct {
	term      uint32
	seq       uint32
	owners    map[string]shardOwnerRec
	nodes     map[NodeID]uint32
	nodeNames []NodeID
	deleted   int
}

type shardOwnerRec struct {
	node  uint32
	epoch Epoch
}

const ownershipCompactThreshold = 1024

func (s *ownershipShardState) put(clientID string, node NodeID, epoch Epoch) (ownerRec, bool) {
	nodeIndex, ok := s.nodes[node]
	if !ok {
		if s.nodes == nil {
			s.nodes = make(map[NodeID]uint32)
		}
		s.nodeNames = append(s.nodeNames, node)
		nodeIndex = uint32(len(s.nodeNames))
		s.nodes[node] = nodeIndex
	}
	prev, had := s.owners[clientID]
	s.owners[clientID] = shardOwnerRec{node: nodeIndex, epoch: epoch}
	return s.expand(prev), had
}

func (s *ownershipShardState) expand(rec shardOwnerRec) ownerRec {
	if rec.node == 0 || int(rec.node) > len(s.nodeNames) {
		return ownerRec{epoch: rec.epoch}
	}
	return ownerRec{node: s.nodeNames[rec.node-1], epoch: rec.epoch}
}

func (s *ownershipShardState) remove(clientID string) {
	delete(s.owners, clientID)
	s.deleted++
	if s.deleted < ownershipCompactThreshold || s.deleted <= len(s.owners) {
		return
	}
	next := make(map[string]shardOwnerRec, len(s.owners))
	for id, rec := range s.owners {
		next[id] = rec
	}
	s.owners = next
	s.deleted = 0
}

// ShardOwnership 由会话分片 leader 在内存中发放 epoch（CLU-008/009）。
// 它不写 Raft 日志：term 是围栏的高位，leader 换届后新 term
// 天然淘汰旧 leader 发出的全部 epoch。
type ShardOwnership struct {
	self   NodeID
	shards *ShardGroup
	group  *TLSGroup

	mu     sync.Mutex
	state  map[uint64]*ownershipShardState
	events chan Eviction
	closed bool

	batchMu sync.Mutex
	batch   map[uint64]*ownershipClaimLane
	reqFree []*ownershipClaimRequest
	// batchClosed 与 batch/pending 共用一把锁。Close 用它阻止新的远程
	// Claim 入队，并把尚未发出的请求全部唤醒；否则关闭恰好撞上 200us
	// 合批窗口时，CONNECT 协程会永远等在 req.done 上。
	batchClosed bool
	// 累计值用原子读取，不让管理面抓取干扰 CONNECT 合批锁。
	remoteClaims       atomic.Uint64
	remoteBatches      atomic.Uint64
	remoteErrors       atomic.Uint64
	remoteBackpressure atomic.Uint64
}

type ownershipClaimRequest struct {
	clientID string
	node     NodeID
	leader   NodeID
	queued   time.Time
	done     chan ownershipClaimResult
}

type ownershipClaimResult struct {
	epoch Epoch
	prev  ownerRec
	had   bool
	err   error
}

type ownershipClaimLane struct {
	running   bool
	pending   []*ownershipClaimRequest
	scratch   []*ownershipClaimRequest
	seen      map[string]struct{}
	active    int
	inFlight  map[string]struct{}
	batchFree []*ownershipClaimBatch
}

type ownershipClaimBatch struct {
	requests []*ownershipClaimRequest
	claims   []wireOwnershipClaimItem
}

// OwnershipStats 是远程 Claim 路径的有界快照。累计值用于计算
// 合批系数和失败率，瞬时值用于判断 CONNECT 风暴是否已把每分片
// 8 个在途 RPC 用满。
type OwnershipStats struct {
	RemoteClaims   uint64
	RemoteRPCs     uint64
	RemoteErrors   uint64
	Backpressured  uint64
	QueuedClaims   int
	InFlightClaims int
	InFlightRPCs   int
	CachedRequests int
}

// Stats 返回 Claim 合批器的快照。锁内只扫描分片数，不扫描请求。
func (o *ShardOwnership) Stats() OwnershipStats {
	st := OwnershipStats{
		RemoteClaims:  o.remoteClaims.Load(),
		RemoteRPCs:    o.remoteBatches.Load(),
		RemoteErrors:  o.remoteErrors.Load(),
		Backpressured: o.remoteBackpressure.Load(),
	}
	o.batchMu.Lock()
	for _, lane := range o.batch {
		st.QueuedClaims += len(lane.pending)
		st.InFlightClaims += len(lane.inFlight)
		st.InFlightRPCs += lane.active
	}
	st.CachedRequests = len(o.reqFree)
	o.batchMu.Unlock()
	return st
}

const (
	ownershipClaimBatchMax      = 256
	ownershipClaimBatchDelay    = 200 * time.Microsecond
	ownershipClaimConcurrentMin = 8
	// 每 shard 最多 8 个在途 Claim RPC；全批请求也必须受界。
	// 否则对端变慢时每 200us 派生一个 goroutine/RPC，重连风暴
	// 会把局部背压扩大成集群雪崩。
	ownershipClaimConcurrentMax   = 8
	ownershipClaimMaxWait         = time.Millisecond
	ownershipClaimRequestCacheMax = 4096
	ownershipClaimSliceCacheMax   = 4096
)

func NewShardOwnership(group *TLSGroup, shards *ShardGroup, evictBuf int) (*ShardOwnership, error) {
	if group == nil || shards == nil {
		return nil, errors.New("cluster: 分片所有权缺少传输或分片组")
	}
	if evictBuf <= 0 {
		evictBuf = 64
	}
	o := &ShardOwnership{
		self: group.id, shards: shards, group: group,
		state: make(map[uint64]*ownershipShardState), events: make(chan Eviction, evictBuf),
		batch: make(map[uint64]*ownershipClaimLane),
	}
	group.ownershipMu.Lock()
	if group.ownership != nil {
		group.ownershipMu.Unlock()
		return nil, errors.New("cluster: 分片所有权服务已经启动")
	}
	group.ownership = o
	group.ownershipMu.Unlock()
	return o, nil
}

func (o *ShardOwnership) route(clientID string) (uint64, NodeID, bool, error) {
	shard := o.shards.ShardOf(clientID)
	if !o.shards.placement.contains(shard, o.shards.replica) {
		// 非副本 Access 节点的 NodeHost 不认识这个 shard。先把请求发给
		// 放置表的首选 leader；接收端仍会用当前 Raft term 做权威判定，
		// 所以这里绝不能自行制造 epoch。
		replica, ok := o.shards.placement.leader(shard)
		if !ok {
			return shard, "", false, ErrNotLeader
		}
		node := o.shards.replicaNode[replica]
		if node == "" {
			return shard, "", false, ErrNotLeader
		}
		return shard, node, false, nil
	}
	leader, term, ok, err := o.shards.nh.GetLeaderID(shard)
	if err != nil || !ok {
		return shard, "", false, ErrNotLeader
	}
	node := o.shards.replicaNode[leader]
	if node == "" || term == 0 || term > math.MaxUint32 {
		return shard, "", false, ErrNotLeader
	}
	return shard, node, leader == o.shards.replica, nil
}

func (o *ShardOwnership) Claim(ctx context.Context, clientID string, node NodeID) (Epoch, error) {
	if clientID == "" || node == "" {
		return ZeroEpoch, errors.New("cluster: 所有权缺少 clientId 或节点")
	}
	shard, leader, local, err := o.route(clientID)
	if err != nil {
		return ZeroEpoch, err
	}
	var ep Epoch
	var prev ownerRec
	var had bool
	if local {
		ep, prev, had, err = o.claimLocal(shard, clientID, node)
	} else {
		ep, prev, had, err = o.claimRemote(ctx, shard, leader, clientID, node)
	}
	if err != nil {
		return ZeroEpoch, err
	}
	if had {
		o.evictPrevious(clientID, prev, ep, node)
	}
	return ep, nil
}

func (o *ShardOwnership) claimRemote(ctx context.Context, shard uint64, leader NodeID, clientID string, node NodeID) (Epoch, ownerRec, bool, error) {
	o.batchMu.Lock()
	if o.batchClosed {
		o.batchMu.Unlock()
		return ZeroEpoch, ownerRec{}, false, ErrClosed
	}
	var req *ownershipClaimRequest
	if n := len(o.reqFree); n > 0 {
		req = o.reqFree[n-1]
		o.reqFree = o.reqFree[:n-1]
	} else {
		req = &ownershipClaimRequest{done: make(chan ownershipClaimResult, 1)}
	}
	req.clientID, req.node, req.leader = clientID, node, leader
	// contract:allow-time-now 只限制组批等待，不参与 epoch 定序。
	req.queued = time.Now()
	lane := o.batch[shard]
	if lane == nil {
		lane = &ownershipClaimLane{inFlight: make(map[string]struct{})}
		o.batch[shard] = lane
	}
	o.remoteClaims.Add(1)
	if lane.active >= ownershipClaimConcurrentMax {
		o.remoteBackpressure.Add(1)
	}
	lane.pending = append(lane.pending, req)
	if !lane.running {
		lane.running = true
		go o.runClaimLane(shard, lane)
	}
	o.batchMu.Unlock()
	select {
	case res := <-req.done:
		o.recycleClaimRequest(req)
		return res.epoch, res.prev, res.had, res.err
	case <-ctx.Done():
		// done 有一个缓冲位，已经发出的批次仍可安全收尾；所有权可能已在
		// leader 生效，之后的同 clientId Claim 会用更大的 epoch 覆盖它。
		return ZeroEpoch, ownerRec{}, false, ctx.Err()
	}
}

func (o *ShardOwnership) recycleClaimRequest(req *ownershipClaimRequest) {
	req.clientID, req.node, req.leader, req.queued = "", "", "", time.Time{}
	o.batchMu.Lock()
	if len(o.reqFree) < ownershipClaimRequestCacheMax {
		o.reqFree = append(o.reqFree, req)
	}
	o.batchMu.Unlock()
}

func (o *ShardOwnership) runClaimLane(shard uint64, lane *ownershipClaimLane) {
	for {
		time.Sleep(ownershipClaimBatchDelay)
		o.batchMu.Lock()
		if len(lane.pending) == 0 {
			if lane.active == 0 {
				lane.running = false
				if cap(lane.pending) > ownershipClaimSliceCacheMax {
					lane.pending = nil
				}
				if cap(lane.scratch) > ownershipClaimSliceCacheMax {
					lane.scratch = nil
				}
				o.batchMu.Unlock()
				return
			}
			o.batchMu.Unlock()
			continue
		}
		// contract:allow-time-now 只判断本地组批等待是否到期，不参与 epoch
		// 定序、所有权选择或任何可复制状态。
		leader, batch, dispatch := selectOwnershipClaimBatch(lane, time.Now())
		if !dispatch {
			o.batchMu.Unlock()
			continue
		}
		o.batchMu.Unlock()

		go o.finishClaimBatch(lane, leader, batch)
	}
}

// selectOwnershipClaimBatch 在 batchMu 下选出一批可并行的请求。同一
// clientId 在前一批完成前不会再发，否则网络乱序可能让较早
// Claim 反而获得更大 epoch。now 可注入是为了稳定验证小批等待边界。
func selectOwnershipClaimBatch(lane *ownershipClaimLane, now time.Time) (NodeID, *ownershipClaimBatch, bool) {
	if len(lane.pending) == 0 || lane.active >= ownershipClaimConcurrentMax {
		return "", nil, false
	}
	leader := lane.pending[0].leader
	batch := lane.takeBatch()
	pending := lane.pending
	remain := lane.scratch[:0]
	seen := lane.seen
	if seen == nil {
		seen = make(map[string]struct{}, ownershipClaimBatchMax)
		lane.seen = seen
	}
	for _, req := range pending {
		_, duplicate := seen[req.clientID]
		_, blocked := lane.inFlight[req.clientID]
		if len(batch.requests) < ownershipClaimBatchMax && req.leader == leader && !duplicate && !blocked {
			batch.requests = append(batch.requests, req)
			seen[req.clientID] = struct{}{}
		} else {
			remain = append(remain, req)
		}
	}
	if len(batch.requests) == 0 || (lane.active > 0 && len(batch.requests) < ownershipClaimConcurrentMin &&
		now.Sub(batch.requests[0].queued) < ownershipClaimMaxWait) {
		clear(remain)
		clear(seen)
		lane.putBatch(batch)
		return "", nil, false
	}
	lane.pending = remain
	for id := range seen {
		lane.inFlight[id] = struct{}{}
	}
	clear(pending)
	lane.scratch = pending[:0]
	clear(seen)
	lane.active++
	return leader, batch, true
}

func (lane *ownershipClaimLane) takeBatch() *ownershipClaimBatch {
	if n := len(lane.batchFree); n > 0 {
		batch := lane.batchFree[n-1]
		lane.batchFree = lane.batchFree[:n-1]
		return batch
	}
	return &ownershipClaimBatch{
		requests: make([]*ownershipClaimRequest, 0, ownershipClaimBatchMax),
		claims:   make([]wireOwnershipClaimItem, 0, ownershipClaimBatchMax),
	}
}

func (lane *ownershipClaimLane) putBatch(batch *ownershipClaimBatch) {
	clear(batch.requests)
	clear(batch.claims)
	batch.requests = batch.requests[:0]
	batch.claims = batch.claims[:0]
	// active 上限就是同时需要的工作区上限。这个硬界防止曾经的
	// 重连峰值被永久保留。
	if len(lane.batchFree) < ownershipClaimConcurrentMax {
		lane.batchFree = append(lane.batchFree, batch)
	}
}

func (o *ShardOwnership) finishClaimBatch(lane *ownershipClaimLane, leader NodeID,
	batch *ownershipClaimBatch,
) {
	for _, req := range batch.requests {
		batch.claims = append(batch.claims, wireOwnershipClaimItem{ClientID: req.clientID, Node: string(req.node)})
	}
	var results []wireOwnershipResult
	var err error
	for _, target := range o.remoteTargets(batch.requests[0].clientID, leader) {
		results, err = o.group.forwardOwnershipClaimBatch(target, batch.claims)
		o.remoteBatches.Add(1)
		if err != nil {
			// 连接失败可能是应答丢失，绝不能换目标重放 Claim。
			break
		}
		if !ownershipBatchNotLeader(results) {
			break
		}
		// 全批明确在 claimLocalBatch 的 leader 检查处被拒，尚未发放
		// 任何 epoch，才可以安全尝试下一个放置副本。
		err = ErrNotLeader
	}
	o.batchMu.Lock()
	for _, req := range batch.requests {
		delete(lane.inFlight, req.clientID)
	}
	lane.active--
	o.batchMu.Unlock()
	for i, req := range batch.requests {
		res := ownershipClaimResult{err: err}
		if err == nil {
			item := results[i]
			res.epoch = Epoch(item.Epoch)
			res.prev = ownerRec{node: NodeID(item.PrevNode), epoch: Epoch(item.PrevEpoch)}
			res.had = item.Had
			if item.Err != "" {
				res.err = errFromWire(wireResp{Err: item.Err})
			} else if item.SessionRead {
				o.shards.cacheClaimedSession(req.clientID, res.epoch, sessionReadResult{
					data: item.SessionData, found: item.SessionFound,
				})
			}
		}
		if res.err != nil {
			o.remoteErrors.Add(1)
		}
		req.done <- res
	}
	o.batchMu.Lock()
	lane.putBatch(batch)
	o.batchMu.Unlock()
}

func ownershipBatchNotLeader(results []wireOwnershipResult) bool {
	if len(results) == 0 {
		return false
	}
	for _, item := range results {
		if item.Err != ErrNotLeader.Error() {
			return false
		}
	}
	return true
}

func (o *ShardOwnership) remoteTargets(clientID string, first NodeID) []NodeID {
	nodes := o.shards.ReplicaNodes(clientID)
	out := make([]NodeID, 0, len(nodes))
	if first != "" {
		out = append(out, first)
	}
	for _, node := range nodes {
		if node != "" && node != first {
			out = append(out, node)
		}
	}
	return out
}

func (o *ShardOwnership) claimLocal(shard uint64, clientID string, node NodeID) (Epoch, ownerRec, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ZeroEpoch, ownerRec{}, false, ErrClosed
	}
	leader, term64, ok, err := o.shards.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != o.shards.replica || term64 == 0 || term64 > math.MaxUint32 {
		return ZeroEpoch, ownerRec{}, false, ErrNotLeader
	}
	term := uint32(term64)
	st := o.state[shard]
	if st == nil || st.term != term {
		st = &ownershipShardState{term: term, owners: make(map[string]shardOwnerRec)}
		o.state[shard] = st
	}
	if st.seq == math.MaxUint32 {
		return ZeroEpoch, ownerRec{}, false, errors.New("cluster: 分片 epoch 序号耗尽")
	}
	st.seq++
	ep := MakeEpoch(term, st.seq)
	prev, had := st.put(clientID, node, ep)
	return ep, prev, had, nil
}

func (o *ShardOwnership) claimLocalBatch(shard uint64, claims []wireOwnershipClaimItem) []wireOwnershipResult {
	out := make([]wireOwnershipResult, len(claims))
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		for i := range out {
			out[i].Err = ErrClosed.Error()
		}
		return out
	}
	leader, term64, ok, err := o.shards.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != o.shards.replica || term64 == 0 || term64 > math.MaxUint32 {
		for i := range out {
			out[i].Err = ErrNotLeader.Error()
		}
		return out
	}
	term := uint32(term64)
	st := o.state[shard]
	if st == nil || st.term != term {
		st = &ownershipShardState{term: term, owners: make(map[string]shardOwnerRec)}
		o.state[shard] = st
	}
	for i, claim := range claims {
		if claim.ClientID == "" || claim.Node == "" {
			out[i].Err = "cluster: 所有权缺少 clientId 或节点"
			continue
		}
		if st.seq == math.MaxUint32 {
			out[i].Err = "cluster: 分片 epoch 序号耗尽"
			continue
		}
		st.seq++
		ep := MakeEpoch(term, st.seq)
		prev, had := st.put(claim.ClientID, NodeID(claim.Node), ep)
		out[i] = wireOwnershipResult{
			Epoch: uint64(ep), PrevNode: string(prev.node), PrevEpoch: uint64(prev.epoch), Had: had,
		}
	}
	return out
}

func (o *ShardOwnership) evictPrevious(clientID string, prev ownerRec, next Epoch, by NodeID) {
	ev := Eviction{ClientID: clientID, PrevEpoch: prev.epoch, NewEpoch: next, By: by, Reason: EvictTakenOver}
	if prev.node == o.self {
		o.notify(ev)
		return
	}
	// 驱逐是加速资源回收，epoch 围栏才是正确性边界；对端暂时
	// 不可达不得让已经成功的 Claim 反过来失败。
	_ = o.group.forwardOwnershipEviction(prev.node, ev)
}

func (o *ShardOwnership) Release(_ context.Context, clientID string, e Epoch) error {
	shard, leader, local, err := o.route(clientID)
	if err != nil {
		return err
	}
	if !local {
		var lastErr error
		for _, target := range o.remoteTargets(clientID, leader) {
			err := o.group.forwardOwnershipRelease(target, clientID, e)
			if err == nil {
				return nil
			}
			lastErr = err
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
		}
		if lastErr == nil {
			lastErr = ErrNotLeader
		}
		return lastErr
	}
	return o.releaseLocal(shard, clientID, e)
}

func (o *ShardOwnership) releaseLocal(shard uint64, clientID string, e Epoch) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	leader, term64, ok, err := o.shards.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != o.shards.replica {
		return ErrNotLeader
	}
	if term64 != uint64(e.Term()) {
		// leader 换届后旧 term 的所有权整代失效，新 leader 的
		// 内存表本来就不含它；Release 可以幂等成功。
		return nil
	}
	st := o.state[shard]
	if st == nil || st.term != e.Term() {
		return nil
	}
	cur, ok := st.owners[clientID]
	if !ok {
		return nil
	}
	if cur.epoch != e {
		return ErrStaleEpoch
	}
	st.remove(clientID)
	return nil
}

func (o *ShardOwnership) Lookup(_ context.Context, clientID string) (NodeID, Epoch, bool) {
	shard, leader, local, err := o.route(clientID)
	if err != nil {
		return "", ZeroEpoch, false
	}
	if !local {
		for _, target := range o.remoteTargets(clientID, leader) {
			node, ep, found, rerr := o.group.forwardOwnershipLookup(target, clientID)
			if rerr == nil {
				return node, ep, found
			}
			// Lookup 只读，任何失败都可以安全尝试下一个副本。
		}
		return "", ZeroEpoch, false
	}
	node, ep, found, _ := o.lookupLocal(shard, clientID)
	return node, ep, found
}

func (o *ShardOwnership) lookupLocal(shard uint64, clientID string) (NodeID, Epoch, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	leader, term64, ok, err := o.shards.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != o.shards.replica {
		return "", ZeroEpoch, false, ErrNotLeader
	}
	st := o.state[shard]
	if st == nil || uint64(st.term) != term64 {
		return "", ZeroEpoch, false, nil
	}
	rec, ok := st.owners[clientID]
	expanded := st.expand(rec)
	return expanded.node, expanded.epoch, ok, nil
}

func (o *ShardOwnership) deliverEviction(ev Eviction) { o.notify(ev) }

func (o *ShardOwnership) dispatchWire(req wireOwnership) wireResp {
	switch req.Op {
	// 单项 Claim 的**发送端已经删掉**（合批之后没有调用方，CLU-147）。
	// 接收端必须留着：滚动升级时旧版本的节点仍会发它，而 op 码一经分配
	// 不得重用——删掉接收端的后果是升级过程中旧节点的 Claim 被当成未知
	// 操作拒绝，表现为那些设备连不上，且原因只能从对端日志看出来。
	case wireOwnershipClaim:
		shard := o.shards.ShardOf(req.ClientID)
		ep, prev, had, err := o.claimLocal(shard, req.ClientID, NodeID(req.Node))
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{
			OK: true, Value: uint64(ep), Owner: string(prev.node),
			PrevValue: uint64(prev.epoch), Found: had,
		}
	case wireOwnershipRelease:
		shard := o.shards.ShardOf(req.ClientID)
		if err := o.releaseLocal(shard, req.ClientID, Epoch(req.Epoch)); err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true}
	case wireOwnershipLookup:
		shard := o.shards.ShardOf(req.ClientID)
		node, ep, ok, err := o.lookupLocal(shard, req.ClientID)
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true, Owner: string(node), Value: uint64(ep), Found: ok}
	case wireOwnershipEvict:
		if req.Eviction == nil {
			return wireResp{Err: "empty eviction"}
		}
		o.deliverEviction(*req.Eviction)
		return wireResp{OK: true}
	case wireOwnershipClaimBatch:
		if len(req.Claims) == 0 || len(req.Claims) > ownershipClaimBatchMax {
			return wireResp{Err: "invalid ownership claim batch"}
		}
		shard := o.shards.ShardOf(req.Claims[0].ClientID)
		for _, claim := range req.Claims[1:] {
			if o.shards.ShardOf(claim.ClientID) != shard {
				return wireResp{Err: "mixed ownership shards"}
			}
		}
		results := o.claimLocalBatch(shard, req.Claims)
		// 所有权与 session cell 按同一个 clientId 哈希分片。Claim 已经到达
		// leader 后顺手读取本地副本，把 RF=3 Access 节点原本串行的第二轮
		// SessionRead RPC 折叠进本应答。读取失败不反悔已经成功的 Claim；
		// SessionRead=false 会让调用方沿原路径换副本重试。
		for i, claim := range req.Claims {
			if results[i].Err != "" {
				continue
			}
			data, found, err := o.shards.readLocalSession(shard, claim.ClientID)
			if err == nil {
				results[i].SessionData = data
				results[i].SessionFound = found
				results[i].SessionRead = true
			}
		}
		return wireResp{OK: true, OwnershipResults: results}
	default:
		return wireResp{Err: "unknown ownership operation"}
	}
}

func (o *ShardOwnership) notify(ev Eviction) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	for {
		select {
		case o.events <- ev:
			return
		default:
			select {
			case <-o.events:
			default:
				return
			}
		}
	}
}

func (o *ShardOwnership) Evictions() <-chan Eviction { return o.events }

func (o *ShardOwnership) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	close(o.events)
	o.mu.Unlock()

	// 先禁止入队，再摘走所有尚未发出的请求。已经发到网络上的批次受
	// roundTrip 超时约束，并通过容量为 1 的 done 通道完成，不会因调用方
	// 或所有权服务关闭而泄漏协程。
	o.batchMu.Lock()
	o.batchClosed = true
	var pending []*ownershipClaimRequest
	for _, lane := range o.batch {
		pending = append(pending, lane.pending...)
		lane.pending = nil
	}
	o.batchMu.Unlock()
	for _, req := range pending {
		req.done <- ownershipClaimResult{err: ErrClosed}
	}

	o.group.ownershipMu.Lock()
	if o.group.ownership == o {
		o.group.ownership = nil
	}
	o.group.ownershipMu.Unlock()
	return nil
}
