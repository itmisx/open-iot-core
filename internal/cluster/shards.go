package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/metrics"

	"github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// 分区组（CLU-006）。会话与离线队列按 clientId 哈希分到若干个分片，
// 每个分片是一条独立的 raft 日志。
//
// 为什么要分片：会话的写入频率跟着消息走，而单条 raft 日志的写入是
// 每秒数千（见 03-cluster.md）。分片把它摊到 N 条独立日志上，吞吐随
// N 线性增长——这正是换到 dragonboat 的理由，它的多分片共享同一套
// 传输与批量落盘，起 N 个 NodeHost 就把这个收益白扔了。
//
// 为什么**不**分放置：每个分片都由全部节点承载（副本因子等于节点数）。
// 于是 CONNECT 时读会话仍然是本地读——契约 05 要求连接路径不发 RPC，
// 而"先问哪个节点有这个会话"恰好是一次 RPC。分片在这里只为并行写入。
//
// 分片号从 shardSessionBase 起，与元数据组（1）错开。

const (
	// shardSessionBase 是会话分片的起始号。
	//
	// 从 16 起而不是 2：留出低位号给后续可能的其他用途（比如把影子
	// 也挪出元数据组），免得那时要改已经落盘的分片号——分片号是写进
	// 数据目录的，改它等于让既有部署认不出自己的日志。
	shardSessionBase uint64 = 16
	// defaultSessionShards 是默认分片数。
	//
	// 8 是个折中：心跳与 fsync 的放大随分片数线性增长（dragonboat 会
	// 按对端合批，但不是零成本），而 8 条日志已经把单组的每秒数千抬到
	// 万级。真要更高再加，分片数是配置项。
	defaultSessionShards uint64 = 8
)

// ShardOptions 是会话分片组的构造参数。
type ShardOptions struct {
	// Base 是本组第一个 Dragonboat shard ID。0 保持会话历史值 16。
	Base uint64
	// LayoutName 是独立的持久化布局文件名。空值保持 session-layout.json。
	LayoutName string
	// Label 只用于启动错误。空值为“会话”。
	Label string
	// Count 是分片数。0 取默认值。
	Count uint64
	// ReplicationFactor 是每个 session cell 的副本数。0 保持历史行为：
	// 所有节点承载全部分片。非零值目前供选择性副本启动与集群测试使用；
	// 生产配置须等放置元数据持久化后再开放。
	ReplicationFactor int
	// Replicas 与 Peers 同元数据组：共识层按副本号认成员。
	Replicas map[NodeID]uint64
	Peers    map[NodeID]string
	// Bootstrap 为真时用 Peers 建组，否则以 join 方式启动。
	Bootstrap bool

	// ApplyBatch 整批应用已提交日志，非空时**取代** Apply（CLU-125）。
	//
	// 分批的全部意义是 fsync 的次数：逐条交给状态机时每条一次事务提交，
	// 而每个分片只有一个 apply 协程、循环里是串行的，于是可合并的并发度
	// 等于分片数。实测单副本 8 分片 187 条/秒，而同一条路径上换成不落盘
	// 的状态机是 13772 条/秒——余量全在这里（见 docs/baseline/throughput.md）。
	//
	// 实现方要保证的两件事与 Apply 相同，只是作用域变成整批：批内后一条
	// 要看得见前一条的效果（CLU-115），每条的判定值要逐条回填（CLU-116）。
	// 出错即整批不生效，并由本组按致命错误处理。
	ApplyBatch func(shard uint64, ents []ShardEntry) error
	// Apply 应用一条已提交日志。shard 是分片号。
	//
	// 与元数据组同一条规矩（CLU-109）：返回前必须把状态与 index 一起
	// 落盘，否则重启后 AppliedIndex 报的位置与实际不符。
	//
	// 第一个返回值回给 Propose 的调用方，用来回报围栏判定（见
	// metaMachine.apply 的说明）。
	Apply func(shard, index uint64, data []byte) (uint64, error)
	// AppliedIndex 回报某个分片已持久化到哪一条。
	AppliedIndex func(shard uint64) uint64
	SnapshotTo   func(shard uint64, w io.Writer) error
	RestoreFrom  func(shard uint64, r io.Reader) error
	// ReadSession 为未承载该分片的 Access 节点提供只读会话快照。
	// 返回的是上层自描述编码，cluster 不解释内容，避免反向依赖 store/session。
	ReadSession func(clientID string) ([]byte, bool, error)
	// MergeProposals 允许当前 shard leader 把来自多个 Access 节点的物理
	// 提案再次合并。返回值 widths 描述合并结果位图中每个输入占几位；
	// consumed 是实际消费的输入前缀。nil 表示关闭 leader 侧二级合批。
	MergeProposals func(items [][]byte) (merged []byte, widths []uint8, consumed int, err error)
	// MergeMaxItems 是一次二级合批最多查看的输入数。0保持会话历史值64。
	// 上限256用于不依赖uint64返回位图的状态机（例如影子）；会话必须保持64。
	MergeMaxItems int
	// MergeIgnoresResult 表示合并提案的调用方不读取状态机uint64返回值。
	// 只有这类状态机才允许widths总和超过64；每个请求仍分别等待同一条
	// Raft日志完成，错误也逐请求返回。
	MergeIgnoresResult bool

	ApplyTimeout time.Duration
}

// ShardGroup 是一组会话分片。
type ShardGroup struct {
	nh      *dragonboat.NodeHost
	base    uint64
	count   uint64
	replica uint64
	applyTO time.Duration
	applied func(shard uint64) uint64
	// transport 复用集群已有的 TLS 长连接池；replicaNode 把
	// Dragonboat 的数字副本号还原为对端节点 ID。
	transport   *TLSGroup
	replicaNode map[uint64]NodeID
	// leaderCache 按本组offset缓存最近一次权威leader副本号。非副本
	// Access节点靠Ready的状态探针填充它，使影子RPC稳态直达leader，
	// 不必每次写先敲放置表首项或额外做一次状态查询。
	leaderCache []atomic.Uint64
	// leaderProbe 把非副本 Access 的冷路径解析限制为每分片单飞。
	// 并发请求共享一个有界完成信号，不各自建链，也不产生拒绝风暴。
	leaderProbe []atomic.Pointer[leaderProbeState]
	// placement 把当前 RF=all 也表达成显式放置表。后续切 RF=3 时，
	// 启动哪些本地副本、读请求发给谁和 leader 均衡都必须读同一张表，
	// 不能各自再算一遍。
	placement    sessionPlacement
	read         func(clientID string) ([]byte, bool, error)
	readRemote   func(shard uint64, clientIDs []string) ([]sessionReadResult, error)
	readMu       sync.Mutex
	readWG       sync.WaitGroup
	readLane     map[uint64]*sessionReadLane
	readStop     bool
	claimReadMu  sync.Mutex
	claimReads   map[claimedSessionKey]sessionReadResult
	merge        func(items [][]byte) ([]byte, []uint8, int, error)
	mergeMax     int
	mergeNoValue bool
	mergeMu      sync.Mutex
	mergeWG      sync.WaitGroup
	mergeLane    map[uint64]*leaderProposalLane
	mergeStop    bool
	mergeReqFree []*leaderProposalRequest

	stats shardStats
}

type leaderProposalRequest struct {
	data []byte
	done chan leaderProposalResult
}

type leaderProbeState struct {
	done chan struct{}
}

const leaderResolveTimeout = 3 * time.Second

type leaderProposalResult struct {
	value uint64
	err   error
}

type leaderProposalLane struct {
	running bool
	pending []*leaderProposalRequest
	// items/batch 由本 lane 唯一worker复用，避免每个物理Raft提案分配
	// 临时切片。会话长度64；不使用返回位图的影子可配置到256。
	items [][]byte
	batch []*leaderProposalRequest
}

type sessionReadRequest struct {
	clientID string
	done     chan sessionReadResult
}

type sessionReadResult struct {
	data  []byte
	found bool
	err   error
}

type claimedSessionKey struct {
	clientID string
	epoch    Epoch
}

type sessionReadLane struct {
	running bool
	pending []*sessionReadRequest
}

const (
	sessionReadBatchMax   = 256
	sessionReadBatchDelay = 200 * time.Microsecond
	// Claim 成功到 CONNECT 消费预读结果之间通常只有几个微秒。上限只为
	// 防御调用方 Claim 后不再 Load；达到上限时退回原有远程读，不影响正确性。
	claimedSessionCacheMax = 8192
)

const (
	leaderProposalMergeDelay      = time.Millisecond
	leaderProposalRequestCacheMax = 4096
	leaderProposalMergeDefaultMax = 64
	leaderProposalMergeHardMax    = 256
)

// shardStats 是提案侧的观测（OBS-148）。
//
// 为什么按 leader 的位置分：dragonboat 对跟随者上发起的提案是**转发**
// 给 leader（raft.handleFollowerPropose 把消息改投 leader），不是就地
// 拒绝——所以提案在哪个节点发起都能成功，但代价不同：本节点是 leader
// 时只有一轮复制，不是时要先过一次自定义传输层再加一轮复制。
// CLU-126 要排除的假设之一正是这一跳，而它此前没有任何信号。
type shardStats struct {
	// local/remote/unknown 按发起提案那一刻本节点是否为该分片 leader 分类。
	// unknown 是还没选出 leader（选举中或刚起）。
	local             atomic.Uint64
	remote            atomic.Uint64
	unknown           atomic.Uint64
	fail              atomic.Uint64
	raft              atomic.Uint64
	readLogical       atomic.Uint64
	readBatches       atomic.Uint64
	readErrors        atomic.Uint64
	readClaimed       atomic.Uint64
	mergeQueued       atomic.Int64
	mergeInFlight     atomic.Int64
	mergePeakQueued   atomic.Int64
	mergePeakInFlight atomic.Int64
	// mergeAccessBatches/mergeLogical/mergePhysical 把 leader 二次合批的
	// 实际宽度暴露出来。仅有 raft 总数无法判断日志偏小究竟发生在 Access
	// 合批、leader 合批，还是来自不能合并的维护命令。
	mergeAccessBatches atomic.Uint64
	mergeLogical       atomic.Uint64
	mergePhysical      atomic.Uint64
	mergeFull          atomic.Uint64
	mergeDrained       atomic.Uint64
	mergeBlocked       atomic.Uint64
	// dur 可能为 nil：包内有几处只为 ShardOf 构造空的 ShardGroup。
	// 两个用到它的地方各自判一次，免得那种构造法变成一个 nil 解引用。
	dur *metrics.Histogram
}

// observe 记一次提案耗时。
func (st *shardStats) observe(ms float64) {
	if st.dur != nil {
		st.dur.Observe(ms)
	}
}

// snapshot 读出耗时分布；没有直方图时给一个空读数。
func (st *shardStats) snapshot() metrics.Snapshot {
	if st.dur == nil {
		return metrics.Snapshot{}
	}
	return st.dur.Snapshot()
}

func raiseAtomicPeak(peak *atomic.Int64, value int64) {
	for old := peak.Load(); value > old; old = peak.Load() {
		if peak.CompareAndSwap(old, value) {
			return
		}
	}
}

// ShardStats 是分片组提案侧的读数。
type ShardStats struct {
	ProposeLocal             uint64
	ProposeRemote            uint64
	ProposeUnknown           uint64
	ProposeErrors            uint64
	RaftProposals            uint64
	SessionReadLogical       uint64
	SessionReadBatches       uint64
	SessionReadErrors        uint64
	SessionReadClaimed       uint64
	LeaderMergeQueued        int64
	LeaderMergeInFlight      int64
	LeaderMergePeakQueued    int64
	LeaderMergePeakInFlight  int64
	LeaderMergeAccessBatches uint64
	LeaderMergeLogical       uint64
	LeaderMergePhysical      uint64
	LeaderMergeFull          uint64
	LeaderMergeDrained       uint64
	LeaderMergeBlocked       uint64
	// Duration 是提案到应用返回的耗时分布，单位毫秒。
	Duration metrics.Snapshot
}

// ShardLeadership 是本节点承载的一个分片当前看到的 leader 状态。
// 非本地副本不返回：调用方抓齐所有节点即可得到完整布局，同时不会让
// 每个 Access 节点为不承载的分片额外发状态 RPC。
type ShardLeadership struct {
	Shard        uint64
	Leader       NodeID
	Ready        bool
	IsLeader     bool
	AppliedIndex uint64
}

// LocalLeadership 返回本节点各本地副本的 leader 视图，供运维指标和
// 故障注入脚本精确选择目标。该方法只读 Dragonboat 本地状态，不走网络。
func (s *ShardGroup) LocalLeadership() []ShardLeadership {
	out := make([]ShardLeadership, 0, s.count)
	for i := uint64(0); i < s.count; i++ {
		shard := s.shardBase() + i
		if !s.placement.contains(shard, s.replica) {
			continue
		}
		leaderReplica, _, ready, err := s.nh.GetLeaderID(shard)
		applied := uint64(0)
		if s.applied != nil {
			applied = s.applied(shard)
		}
		if err != nil || !ready {
			out = append(out, ShardLeadership{Shard: shard, AppliedIndex: applied})
			continue
		}
		out = append(out, ShardLeadership{
			Shard: shard, Leader: s.replicaNode[leaderReplica], Ready: true,
			IsLeader: leaderReplica == s.replica, AppliedIndex: applied,
		})
	}
	return out
}

// Stats 返回提案侧读数（OBS-148）。
func (s *ShardGroup) Stats() ShardStats {
	return ShardStats{
		ProposeLocal:             s.stats.local.Load(),
		ProposeRemote:            s.stats.remote.Load(),
		ProposeUnknown:           s.stats.unknown.Load(),
		ProposeErrors:            s.stats.fail.Load(),
		RaftProposals:            s.stats.raft.Load(),
		SessionReadLogical:       s.stats.readLogical.Load(),
		SessionReadBatches:       s.stats.readBatches.Load(),
		SessionReadErrors:        s.stats.readErrors.Load(),
		SessionReadClaimed:       s.stats.readClaimed.Load(),
		LeaderMergeQueued:        s.stats.mergeQueued.Load(),
		LeaderMergeInFlight:      s.stats.mergeInFlight.Load(),
		LeaderMergePeakQueued:    s.stats.mergePeakQueued.Load(),
		LeaderMergePeakInFlight:  s.stats.mergePeakInFlight.Load(),
		LeaderMergeAccessBatches: s.stats.mergeAccessBatches.Load(),
		LeaderMergeLogical:       s.stats.mergeLogical.Load(),
		LeaderMergePhysical:      s.stats.mergePhysical.Load(),
		LeaderMergeFull:          s.stats.mergeFull.Load(),
		LeaderMergeDrained:       s.stats.mergeDrained.Load(),
		LeaderMergeBlocked:       s.stats.mergeBlocked.Load(),
		Duration:                 s.stats.snapshot(),
	}
}

// PlacementFingerprint 返回本节点当前持久化分片布局的稳定标识。
// 它用于一次性本地数据迁移标记；包含 replica ID，因此同一布局
// 不同节点不会共用一个逻辑标识。
func (s *ShardGroup) PlacementFingerprint() string {
	h := sha256.New()
	var word [8]byte
	put := func(v uint64) {
		binary.BigEndian.PutUint64(word[:], v)
		_, _ = h.Write(word[:])
	}
	put(s.base)
	put(s.count)
	put(s.replica)
	for i := uint64(0); i < s.count; i++ {
		shard := s.base + i
		put(shard)
		members := s.placement.members(shard)
		put(uint64(len(members)))
		for _, member := range members {
			put(member)
		}
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// nowMonotonic 返回一个只用于测量时长的时间点。
//
// contract:allow-time-now 只做提案耗时的差值计算，不参与任何排序或
// 版本比较（契约 08 禁的是用墙上时钟定序）。调用方传入的 Duration
// 仍然是超时的唯一来源。
func nowMonotonic() time.Time { return time.Now() }

// ShardOf 返回某个 clientId 所属的分片号。
//
// FNV-1a：纯函数、无状态，所以每个节点算出的是同一个分片——这一点是
// 必须的，而不是优化。换成带随机种子的哈希（Go 的 map 哈希就是）会让
// 不同节点把同一个会话算到不同分片上，表现是"会话时有时无"。
func (s *ShardGroup) ShardOf(clientID string) uint64 {
	return ShardFor(s.shardBase(), s.count, clientID)
}

// ShardFor 是持久化分片布局使用的稳定哈希。装配层可在组启动返回前用它
// 过滤快照，避免恢复回调捕获一个尚未赋值的 *ShardGroup。
func ShardFor(base, count uint64, key string) uint64 {
	return shardForBytes(base, count, []byte(key))
}

func shardForBytes(base, count uint64, key []byte) uint64 {
	if count == 0 {
		return base
	}
	var h uint64 = 14695981039346656037
	for _, b := range key {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return base + h%count
}

func (s *ShardGroup) shardBase() uint64 {
	if s.base != 0 {
		return s.base
	}
	return shardSessionBase
}

// ShardCount 返回分片数。
func (s *ShardGroup) ShardCount() uint64 { return s.count }

// ShardIDs 返回全部分片号。
func (s *ShardGroup) ShardIDs() []uint64 {
	out := make([]uint64, 0, s.count)
	for i := uint64(0); i < s.count; i++ {
		out = append(out, s.shardBase()+i)
	}
	return out
}

// PreferredNode 返回 clientId 所属分片的首选节点。它是确定性放置目标，
// 不是瞬时 Raft leader；后续 Access/Core 路由先用它定位 session cell，
// cell 内部再按当前任期决定真正 leader。
func (s *ShardGroup) PreferredNode(clientID string) (NodeID, bool) {
	replica, ok := s.placement.leader(s.ShardOf(clientID))
	if !ok {
		return "", false
	}
	node := s.replicaNode[replica]
	return node, node != ""
}

// Owner/IsLocal/Shard/Count 使独立状态机组可直接作为按 key 的 Partitioner。
// Owner 是持久布局的首选副本，不是瞬时 leader；IsLocal 表示本节点
// 是否承载任一副本，因此 RF=3 的二级副本也可直接读和提案。
func (s *ShardGroup) Owner(key string) NodeID {
	node, _ := s.PreferredNode(key)
	return node
}

func (s *ShardGroup) IsLocal(key string) bool {
	return s.HasLocalReplica(key)
}

func (s *ShardGroup) Shard(key string) uint32 {
	return uint32(s.ShardOf(key) - s.shardBase())
}

func (s *ShardGroup) Count() int { return int(s.count) }

// ReplicaNodes 返回 clientId 所属分片的放置节点，首项是首选节点。
func (s *ShardGroup) ReplicaNodes(clientID string) []NodeID {
	members := s.placement.members(s.ShardOf(clientID))
	out := make([]NodeID, 0, len(members))
	for _, replica := range members {
		if node := s.replicaNode[replica]; node != "" {
			out = append(out, node)
		}
	}
	return out
}

func (s *ShardGroup) rememberLeader(shard, leader uint64) {
	if leader == 0 || shard < s.shardBase() {
		return
	}
	off := shard - s.shardBase()
	if off >= uint64(len(s.leaderCache)) || !s.placement.contains(shard, leader) {
		return
	}
	s.leaderCache[off].Store(leader)
}

// LeaderNode 返回当前已知的权威leader。缓存为空时只做幂等状态查询，
// 不发送写；选举窗口返回false，让上层快速拒绝而不是把写挂进旧任期。
func (s *ShardGroup) LeaderNode(clientID string) (NodeID, bool) {
	shard := s.ShardOf(clientID)
	off := shard - s.shardBase()
	// 本节点承载该分片时，NodeHost 的当前任期视图是权威来源，必须先于
	// 缓存。旧顺序会在本节点曾是 leader、换届后仍缓存自己时把 follower
	// 误判成 leader；影子层随后走本地 Apply，leader 已提交后又同步等待这个
	// follower 追平，最坏正好卡满10秒 apply预算。非副本才需要跨节点探针
	// 缓存，因为它的 NodeHost 根本不认识这个 shard。
	if s.placement.contains(shard, s.replica) && s.nh != nil {
		leader, _, ready, err := s.nh.GetLeaderID(shard)
		if err == nil && ready {
			s.rememberLeader(shard, leader)
			return s.replicaNode[leader], s.replicaNode[leader] != ""
		}
		return "", false
	}
	if off < uint64(len(s.leaderCache)) {
		if leader := s.leaderCache[off].Load(); leader != 0 {
			if node := s.replicaNode[leader]; node != "" {
				return node, true
			}
		}
	}
	if s.transport == nil {
		return "", false
	}
	if off >= uint64(len(s.leaderProbe)) {
		return "", false
	}
	probe := &leaderProbeState{done: make(chan struct{})}
	if !s.leaderProbe[off].CompareAndSwap(nil, probe) {
		probe = s.leaderProbe[off].Load()
		if probe != nil {
			timer := time.NewTimer(leaderResolveTimeout)
			select {
			case <-probe.done:
				timer.Stop()
			case <-timer.C:
			}
		}
		// 单飞发起者若找到 leader 已写入缓存；否则本次快速失败，
		// 下一条独立请求再开启新一轮，避免同一批等待者同时重试。
		if leader := s.leaderCache[off].Load(); leader != 0 {
			if node := s.replicaNode[leader]; node != "" {
				return node, true
			}
		}
		return "", false
	}
	defer func() {
		s.leaderProbe[off].CompareAndSwap(probe, nil)
		close(probe.done)
	}()
	members := s.placement.members(shard)
	type statusResult struct {
		replica uint64
		leader  uint64
		ready   bool
		err     error
	}
	// 状态查询是幂等的：并行问全部放置副本，用第一个有效的权威视图。
	// 这是故障窗口冷路径，稳态直接命中 leaderCache，不增加写入扇出。
	quorum := len(members)/2 + 1
	deadline := time.NewTimer(leaderResolveTimeout)
	defer deadline.Stop()
	for {
		results := make(chan statusResult, len(members))
		probes := 0
		for _, replica := range members {
			node := s.replicaNode[replica]
			if node == "" {
				continue
			}
			probes++
			go func() {
				leader, ready, err := s.transport.forwardShardStatus(node, shard)
				results <- statusResult{replica: replica, leader: leader, ready: ready, err: err}
			}()
		}
		responses := 0
		round := time.NewTimer(250 * time.Millisecond)
	roundLoop:
		for responses < probes {
			var result statusResult
			select {
			case result = <-results:
			case <-round.C:
				break roundLoop
			case <-deadline.C:
				return "", false
			}
			responses++
			if result.err == nil && result.ready && s.placement.contains(shard, result.leader) &&
				result.leader == result.replica {
				// follower 在换届窗口可能仍暂存旧 leader；只接受
				// 新 leader 对自己身份的自证。
				round.Stop()
				s.rememberLeader(shard, result.leader)
				leaderNode := s.replicaNode[result.leader]
				return leaderNode, leaderNode != ""
			}
			if responses >= quorum {
				break roundLoop
			}
		}
		round.Stop()
		// 多数副本尚未看到可自证的 leader，留在同一次单飞中低频
		// 重探，不唤醒所有设备形成同步重试波。
		retry := time.NewTimer(100 * time.Millisecond)
		select {
		case <-retry.C:
		case <-deadline.C:
			retry.Stop()
			return "", false
		}
	}
}

// ForgetLeader 只在目标仍是缓存值时失效它，避免并发请求把刚学到的新
// leader清掉。模糊写失败不能重试当前请求，但必须让下一条独立请求重查。
func (s *ShardGroup) ForgetLeader(clientID string, target NodeID) {
	shard := s.ShardOf(clientID)
	off := shard - s.shardBase()
	if off >= uint64(len(s.leaderCache)) {
		return
	}
	for replica, node := range s.replicaNode {
		if node == target {
			s.leaderCache[off].CompareAndSwap(replica, 0)
			return
		}
	}
}

// HasLocalReplica 报告本节点是否承载 clientId 所属分片。
func (s *ShardGroup) HasLocalReplica(clientID string) bool {
	return s.placement.contains(s.ShardOf(clientID), s.replica)
}

// HasLocalReplicaBytes 是大规模存储迁移的零分配版本；key 只在调用期间有效。
func (s *ShardGroup) HasLocalReplicaBytes(key []byte) bool {
	return s.placement.contains(shardForBytes(s.shardBase(), s.count, key), s.replica)
}

// ReadSession 从放置副本读取一份会话快照。读是幂等的，首选副本不可达时
// 可以依次尝试其余副本；写入仍只走当前 Raft leader。
func (s *ShardGroup) ReadSession(clientID string) ([]byte, bool, error) {
	shard := s.ShardOf(clientID)
	if s.placement.contains(shard, s.replica) {
		return s.readLocalSession(shard, clientID)
	}
	if s.transport == nil || s.readRemote == nil {
		return nil, false, ErrNotMember
	}
	res := s.queueRemoteSessionRead(shard, clientID)
	return res.data, res.found, res.err
}

// cacheClaimedSession 暂存远程 Claim 应答顺带返回的 session cell 快照。
// epoch 是键的一部分，避免同 clientId 并发重连互相拿走对方的快照。
func (s *ShardGroup) cacheClaimedSession(clientID string, epoch Epoch, res sessionReadResult) {
	if clientID == "" || !epoch.Valid() || res.err != nil {
		return
	}
	s.claimReadMu.Lock()
	defer s.claimReadMu.Unlock()
	if s.claimReads == nil {
		s.claimReads = make(map[claimedSessionKey]sessionReadResult)
	}
	if len(s.claimReads) >= claimedSessionCacheMax {
		return
	}
	s.claimReads[claimedSessionKey{clientID: clientID, epoch: epoch}] = res
}

// TakeClaimedSession 取走 Claim RPC 携带的一次性会话快照。
// cached=false 时调用方必须走正常 ReadSession；缺失会话本身是 cached=true、
// found=false，正是新设备风暴里省掉第二次 RPC 的主要情形。
func (s *ShardGroup) TakeClaimedSession(clientID string, epoch Epoch) (
	data []byte, found, cached bool, err error,
) {
	key := claimedSessionKey{clientID: clientID, epoch: epoch}
	s.claimReadMu.Lock()
	res, ok := s.claimReads[key]
	if ok {
		delete(s.claimReads, key)
	}
	s.claimReadMu.Unlock()
	if !ok {
		return nil, false, false, nil
	}
	s.stats.readLogical.Add(1)
	s.stats.readClaimed.Add(1)
	return res.data, res.found, true, res.err
}

func (s *ShardGroup) readRemoteSessions(shard uint64, clientIDs []string) ([]sessionReadResult, error) {
	out := make([]sessionReadResult, len(clientIDs))
	pending := make([]int, len(clientIDs))
	lastErr := make([]error, len(clientIDs))
	for i := range pending {
		pending[i] = i
	}
	for _, replica := range s.placement.members(shard) {
		if len(pending) == 0 {
			return out, nil
		}
		node := s.replicaNode[replica]
		if node == "" {
			continue
		}
		ids := make([]string, len(pending))
		for i, original := range pending {
			ids[i] = clientIDs[original]
		}
		results, err := s.transport.forwardSessionReadBatch(node, shard, ids)
		if err != nil {
			for _, original := range pending {
				lastErr[original] = err
			}
			continue
		}
		next := make([]int, 0, len(pending))
		for i, original := range pending {
			if results[i].err != nil {
				lastErr[original] = results[i].err
				next = append(next, original)
				continue
			}
			out[original] = results[i]
		}
		pending = next
	}
	for _, original := range pending {
		if lastErr[original] == nil {
			lastErr[original] = ErrNotLeader
		}
		out[original].err = lastErr[original]
	}
	return out, nil
}

func (s *ShardGroup) queueRemoteSessionRead(shard uint64, clientID string) sessionReadResult {
	s.stats.readLogical.Add(1)
	req := &sessionReadRequest{clientID: clientID, done: make(chan sessionReadResult, 1)}
	s.readMu.Lock()
	if s.readStop {
		s.readMu.Unlock()
		return sessionReadResult{err: ErrClosed}
	}
	lane := s.readLane[shard]
	if lane == nil {
		lane = &sessionReadLane{}
		s.readLane[shard] = lane
	}
	lane.pending = append(lane.pending, req)
	if !lane.running {
		lane.running = true
		s.readWG.Add(1)
		go s.runSessionReadLane(shard, lane)
	}
	s.readMu.Unlock()
	return <-req.done
}

func (s *ShardGroup) runSessionReadLane(shard uint64, lane *sessionReadLane) {
	defer s.readWG.Done()
	for {
		time.Sleep(sessionReadBatchDelay)
		s.readMu.Lock()
		if len(lane.pending) == 0 {
			lane.running = false
			s.readMu.Unlock()
			return
		}
		n := len(lane.pending)
		if n > sessionReadBatchMax {
			n = sessionReadBatchMax
		}
		batch := append([]*sessionReadRequest(nil), lane.pending[:n]...)
		lane.pending = lane.pending[n:]
		s.readMu.Unlock()

		ids := make([]string, len(batch))
		for i := range batch {
			ids[i] = batch[i].clientID
		}
		results, err := s.readRemote(shard, ids)
		s.stats.readBatches.Add(1)
		if err != nil || len(results) != len(batch) {
			s.stats.readErrors.Add(uint64(len(batch)))
			if err == nil {
				err = errors.New("cluster: 会话批量读取应答数量不符")
			}
			for _, req := range batch {
				req.done <- sessionReadResult{err: err}
			}
			continue
		}
		for i, req := range batch {
			if results[i].err != nil {
				s.stats.readErrors.Add(1)
			}
			req.done <- results[i]
		}
	}
}

func (s *ShardGroup) readLocalSession(shard uint64, clientID string) ([]byte, bool, error) {
	if s.ShardOf(clientID) != shard || !s.placement.contains(shard, s.replica) {
		return nil, false, ErrNotMember
	}
	if s.read == nil {
		return nil, false, errors.New("cluster: 会话读取处理器未配置")
	}
	return s.read(clientID)
}

// Propose 把一条命令提交到 clientId 所属的分片，返回状态机的判定值。
//
// 用 NoOP session 而不是 dragonboat 的客户端会话（它提供恰好一次语义）：
// 本项目的会话命令本来就是幂等的——save 是整块覆盖，delete 与 take 是
// 删一批键，enqueue 的序号在 apply 时由当前状态推出。客户端会话要额外
// 一次注册往返，换的是本项目不需要的那个保证。
func (s *ShardGroup) Propose(clientID string, data []byte) (uint64, error) {
	return s.ProposeShard(s.ShardOf(clientID), data)
}

// ProposeShard 把一条命令提交到指定分片。
//
// 直接按分片号提交的用处是那些**没有 clientId 可言**的命令：会话过期
// 清理是全库扫描，它的归属不是某个设备，而是"这个分片负责的那一批"。
func (s *ShardGroup) ProposeShard(shard uint64, data []byte) (uint64, error) {
	start := nowMonotonic()
	// Access 节点可能不承载这个 session cell。此时本地 NodeHost 根本
	// 不认识 shard，不能用 GetLeaderID；按持久化放置表逐个问副本。
	// 只有明确的 ErrNotLeader 证明命令未被接收，才允许换下一个目标。
	// ErrNoQuorum/超时可能是“已提交但应答丢失”，重发会造成重复写。
	if !s.placement.contains(shard, s.replica) {
		s.stats.remote.Add(1)
		value, err := s.proposePlaced(shard, data)
		s.stats.observe(float64(nowMonotonic().Sub(start).Microseconds()) / 1000)
		if err != nil {
			s.stats.fail.Add(1)
			return 0, err
		}
		return value, nil
	}

	// leader 的位置在提案**之前**取：之后取到的是这一轮
	// 复制结束时的状态，而要归因的是这条提案走了哪条路。
	leader, _, ok, leaderErr := s.nh.GetLeaderID(shard)
	if leaderErr == nil && ok {
		s.rememberLeader(shard, leader)
	}
	var value uint64
	var err error
	switch {
	case leaderErr != nil || !ok:
		s.stats.unknown.Add(1)
		// 选举窗口里的这条新请求还没有发送。不能直接 SyncPropose：
		// Dragonboat 会把它挂在“暂无leader”的旧视图上，即使约1秒后
		// 完成换届也要等满 apply timeout。等权威视图出现再提交，既不
		// 重发也不会把每个客户端各挂住一条请求。
		value, err = s.proposeAfterLeaderChange(shard, 0, data)
	case leader == s.replica:
		s.stats.local.Add(1)
		value, err = s.proposeLocalLeader(shard, data)
	default:
		s.stats.remote.Add(1)
		to := s.replicaNode[leader]
		if to == "" || s.transport == nil {
			err = ErrNotLeader
		} else {
			value, err = s.transport.forwardShard(to, shard, data)
			if errors.Is(err, ErrRequestUnsent) || errors.Is(err, ErrDraining) {
				// 旧 leader 已经不可拨达且这条请求确定没有发出去。
				// 等本地视图真正进入新一届再提交；立刻 SyncPropose 会把
				// 请求挂在旧任期上，实测即使约1秒后选出新 leader，也要
				// 等满10秒 apply timeout 才释放，造成整条 shard 断流。
				// 不能在模糊的 ErrNoQuorum 上走这里，否则应答丢失会把
				// 同一条命令应用两遍。
				value, err = s.proposeAfterLeaderChange(shard, leader, data)
			}
		}
	}
	s.stats.observe(float64(nowMonotonic().Sub(start).Microseconds()) / 1000)
	if err != nil {
		s.stats.fail.Add(1)
		return 0, err
	}
	return value, nil
}

func (s *ShardGroup) proposePlaced(shard uint64, data []byte) (uint64, error) {
	if s.transport == nil {
		return 0, ErrNotLeader
	}
	var lastErr error
	for _, replica := range s.placement.members(shard) {
		to := s.replicaNode[replica]
		if to == "" {
			continue
		}
		value, err := s.transport.forwardShard(to, shard, data)
		if err == nil {
			s.rememberLeader(shard, replica)
			return value, nil
		}
		lastErr = err
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrRequestUnsent) &&
			!errors.Is(err, ErrDraining) {
			return 0, err
		}
	}
	if lastErr == nil {
		lastErr = ErrNotLeader
	}
	return 0, lastErr
}

// proposeAfterLeaderChange 等本地 NodeHost 的权威 leader 视图离开 oldLeader，
// 再把一条“已证明尚未发送”的命令交给新 leader。故障窗口才走这条冷路径，
// 10ms 轮询不会进入正常写入热路径，也不会为每条成功更新增加 timer。
func (s *ShardGroup) proposeAfterLeaderChange(shard, oldLeader uint64, data []byte) (uint64, error) {
	deadline := time.NewTimer(s.applyTO)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		leader, _, ready, err := s.nh.GetLeaderID(shard)
		if err == nil && ready && leader != 0 && leader != oldLeader {
			s.rememberLeader(shard, leader)
			if leader == s.replica {
				return s.proposeLocalLeader(shard, data)
			}
			to := s.replicaNode[leader]
			if to != "" && s.transport != nil {
				value, forwardErr := s.transport.forwardShard(to, shard, data)
				switch {
				case forwardErr == nil:
					return value, nil
				case errors.Is(forwardErr, ErrNotLeader),
					errors.Is(forwardErr, ErrRequestUnsent),
					errors.Is(forwardErr, ErrDraining):
					// 明确未接收，可以继续等下一次权威视图；若这个
					// 新 leader 也失效，必须等 leader ID 再次变化。
					oldLeader = leader
				default:
					return 0, forwardErr
				}
			}
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return 0, ErrNotLeader
		}
	}
}

// syncPropose 在本 NodeHost 上提交并统一 Dragonboat 的就绪/超时错误。
func (s *ShardGroup) syncPropose(shard uint64, data []byte) (uint64, error) {
	s.stats.raft.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), s.applyTO)
	defer cancel()
	res, err := s.nh.SyncPropose(ctx, s.nh.GetNoOPSession(shard), data)
	if errors.Is(err, dragonboat.ErrShardNotReady) || errors.Is(err, dragonboat.ErrTimeout) {
		return 0, ErrNotLeader
	}
	if err != nil {
		return 0, err
	}
	return res.Value, nil
}

// proposeLocalLeader 是跨节点路由的接收端。它不再路由，且只在
// 本副本仍是 leader 时才提交，防止两个过期 leader 视图互相弹回。
func (s *ShardGroup) proposeLocalLeader(shard uint64, data []byte) (uint64, error) {
	leader, _, ok, err := s.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != s.replica {
		return 0, ErrNotLeader
	}
	if s.merge == nil {
		return s.syncPropose(shard, data)
	}
	s.mergeMu.Lock()
	if s.mergeStop {
		s.mergeMu.Unlock()
		return 0, ErrClosed
	}
	var req *leaderProposalRequest
	if n := len(s.mergeReqFree); n > 0 {
		req = s.mergeReqFree[n-1]
		s.mergeReqFree = s.mergeReqFree[:n-1]
	} else {
		req = &leaderProposalRequest{done: make(chan leaderProposalResult, 1)}
	}
	// 调用方同步等待提案结果，合并器可在此期间直接引用 data。
	req.data = data
	lane := s.mergeLane[shard]
	if lane == nil {
		lane = &leaderProposalLane{
			items: make([][]byte, s.mergeMax),
			batch: make([]*leaderProposalRequest, s.mergeMax),
		}
		s.mergeLane[shard] = lane
	}
	lane.pending = append(lane.pending, req)
	queued := s.stats.mergeQueued.Add(1)
	raiseAtomicPeak(&s.stats.mergePeakQueued, queued)
	if !lane.running {
		lane.running = true
		s.mergeWG.Add(1)
		go s.runLeaderProposalLane(shard, lane)
	}
	s.mergeMu.Unlock()
	result := <-req.done
	req.data = nil
	s.mergeMu.Lock()
	if len(s.mergeReqFree) < leaderProposalRequestCacheMax {
		s.mergeReqFree = append(s.mergeReqFree, req)
	}
	s.mergeMu.Unlock()
	return result.value, result.err
}

func (s *ShardGroup) runLeaderProposalLane(shard uint64, lane *leaderProposalLane) {
	defer s.mergeWG.Done()
	for {
		time.Sleep(leaderProposalMergeDelay)
		s.mergeMu.Lock()
		if len(lane.pending) == 0 {
			lane.running = false
			s.mergeMu.Unlock()
			return
		}
		limit := len(lane.pending)
		if limit > s.mergeMax {
			limit = s.mergeMax
		}
		items := lane.items[:limit]
		for i := 0; i < limit; i++ {
			items[i] = lane.pending[i].data
		}
		s.mergeMu.Unlock()

		merged, widths, consumed, mergeErr := s.merge(items)
		if consumed < 1 || consumed > len(items) || len(widths) != consumed {
			merged, widths, consumed = items[0], nil, 1
			if mergeErr == nil {
				mergeErr = errors.New("cluster: 提案合并器返回了无效结果")
			}
		}
		s.mergeMu.Lock()
		batch := lane.batch[:consumed]
		copy(batch, lane.pending[:consumed])
		lane.pending = lane.pending[consumed:]
		s.stats.mergeQueued.Add(-int64(consumed))
		s.mergeMu.Unlock()

		if mergeErr != nil {
			for _, req := range batch {
				req.done <- leaderProposalResult{err: mergeErr}
			}
			clear(items)
			clear(batch)
			continue
		}
		logical := uint64(0)
		for _, width := range widths {
			logical += uint64(width)
		}
		// 合法的单项 merger 也必须给出宽度；防御自定义 merger 没给时，
		// 至少仍把它记作一个逻辑命令，不能让观测值悄悄少算。
		if logical == 0 {
			logical = 1
		}
		if !s.mergeNoValue && logical > 64 {
			err := errors.New("cluster: 需要返回位图的合并提案超过64条")
			for _, req := range batch {
				req.done <- leaderProposalResult{err: err}
			}
			clear(items)
			clear(batch)
			continue
		}
		s.stats.mergeAccessBatches.Add(uint64(consumed))
		s.stats.mergeLogical.Add(logical)
		s.stats.mergePhysical.Add(1)
		switch {
		case logical >= uint64(s.mergeMax):
			s.stats.mergeFull.Add(1)
		case consumed == len(items):
			s.stats.mergeDrained.Add(1)
		default:
			s.stats.mergeBlocked.Add(1)
		}
		// 单项不改写，保留状态机完整的uint64返回值；多项会话提案按
		// widths拆位图。影子通过pending ID交付结果，不读这个位图，因而可
		// 安全超过64条；显式配置mergeNoValue后统一回0。
		if consumed == 1 {
			inFlight := s.stats.mergeInFlight.Add(1)
			raiseAtomicPeak(&s.stats.mergePeakInFlight, inFlight)
			value, err := s.syncProposeIfLeader(shard, batch[0].data)
			s.stats.mergeInFlight.Add(-1)
			batch[0].done <- leaderProposalResult{value: value, err: err}
			clear(items)
			clear(batch)
			continue
		}
		inFlight := s.stats.mergeInFlight.Add(1)
		raiseAtomicPeak(&s.stats.mergePeakInFlight, inFlight)
		value, err := s.syncProposeIfLeader(shard, merged)
		s.stats.mergeInFlight.Add(-1)
		if s.mergeNoValue {
			for _, req := range batch {
				req.done <- leaderProposalResult{err: err}
			}
			clear(items)
			clear(batch)
			continue
		}
		offset := uint(0)
		for i, req := range batch {
			width := uint(widths[i])
			var part uint64
			if width == 64 {
				part = value
			} else if width > 0 {
				part = (value >> offset) & ((uint64(1) << width) - 1)
			}
			req.done <- leaderProposalResult{value: part, err: err}
			offset += width
		}
		clear(items)
		clear(batch)
	}
}

func (s *ShardGroup) syncProposeIfLeader(shard uint64, data []byte) (uint64, error) {
	leader, _, ok, err := s.nh.GetLeaderID(shard)
	if err != nil || !ok || leader != s.replica {
		return 0, ErrNotLeader
	}
	return s.syncPropose(shard, data)
}

// Barrier 等某个分片追平到当前已提交的位置。
func (s *ShardGroup) Barrier(clientID string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = s.applyTO
	}
	shard := s.ShardOf(clientID)
	if !s.placement.contains(shard, s.replica) {
		if s.transport == nil {
			return ErrNotMember
		}
		var lastErr error
		for _, replica := range s.placement.members(shard) {
			node := s.replicaNode[replica]
			if node == "" {
				continue
			}
			if err := s.transport.forwardShardBarrier(node, shard, timeout); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
		if lastErr == nil {
			lastErr = ErrNotMember
		}
		return lastErr
	}
	return s.barrierLocal(shard, timeout)
}

func (s *ShardGroup) barrierLocal(shard uint64, timeout time.Duration) error {
	if !s.placement.contains(shard, s.replica) {
		return ErrNotMember
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := s.nh.SyncRead(ctx, shard, barrierQuery{})
	return err
}

// Ready 报告所有分片是否都已选出 leader。
//
// 启动时要等它：分片没就绪时 Propose 返回 ErrNotLeader，而那条错误
// 在连接路径上会变成"设备连不上"。
func (s *ShardGroup) Ready() bool {
	for i := uint64(0); i < s.count; i++ {
		shard := s.shardBase() + i
		if s.placement.contains(shard, s.replica) {
			leader, _, ok, err := s.nh.GetLeaderID(shard)
			if err != nil || !ok {
				return false
			}
			s.rememberLeader(shard, leader)
			continue
		}
		if !s.remoteShardReady(shard) {
			return false
		}
	}
	return true
}

func (s *ShardGroup) remoteShardReady(shard uint64) bool {
	if s.transport == nil {
		return false
	}
	for _, replica := range s.placement.members(shard) {
		node := s.replicaNode[replica]
		if node == "" {
			continue
		}
		if leader, ready, err := s.transport.forwardShardStatus(node, shard); err == nil && ready {
			s.rememberLeader(shard, leader)
			return true
		}
	}
	return false
}

func (s *ShardGroup) localShardStatus(shard uint64) (uint64, bool, error) {
	if !s.placement.contains(shard, s.replica) {
		return 0, false, ErrNotMember
	}
	leader, _, ok, err := s.nh.GetLeaderID(shard)
	if err == nil && ok {
		s.rememberLeader(shard, leader)
	}
	return leader, ok, err
}

// BalanceLeaders 请求当前 leader 把各分片转移到确定性的
// 目标副本。返回本节点发起的转移数。
//
// 只有「本节点当前是 leader」时才发请求：所有节点都可以在
// 就绪后调这个方法，但一个分片只会有一个发起者。转移是
// Dragonboat 自身的 Raft leader transfer，不改副本集、不停服。
func (s *ShardGroup) BalanceLeaders() (int, error) {
	requested := 0
	var firstErr error
	for i := uint64(0); i < s.count; i++ {
		shard := s.shardBase() + i
		if !s.placement.contains(shard, s.replica) {
			continue
		}
		leader, _, ok, err := s.nh.GetLeaderID(shard)
		target, placed := s.placement.leader(shard)
		if err != nil || !ok || !placed || leader != s.replica || leader == target {
			continue
		}
		if err := s.nh.RequestLeaderTransfer(shard, target); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		requested++
	}
	return requested, firstErr
}

// LeadersBalanced 报告全部分片是否已落在目标 leader 上。
func (s *ShardGroup) LeadersBalanced() bool {
	for i := uint64(0); i < s.count; i++ {
		shard := s.shardBase() + i
		if !s.placement.contains(shard, s.replica) {
			continue
		}
		leader, _, ok, err := s.nh.GetLeaderID(shard)
		target, placed := s.placement.leader(shard)
		if err != nil || !ok || !placed || leader != target {
			return false
		}
	}
	return true
}

// Close 停掉所有分片。
//
// 不关 NodeHost：它是元数据组的，由那边关（两者共享同一个实例，
// 这正是分片的意义所在）。
func (s *ShardGroup) Close() error {
	s.readMu.Lock()
	s.readStop = true
	s.readMu.Unlock()
	s.readWG.Wait()
	s.mergeMu.Lock()
	s.mergeStop = true
	s.mergeMu.Unlock()
	// 已入队的写不能丢：它可能已被 worker 交给 Raft。等它们拿到明确
	// 结果后再停副本；mergeStop 只阻止新的请求进入。
	s.mergeWG.Wait()
	if s.transport != nil {
		s.transport.unregisterShardGroup(s)
	}
	var firstErr error
	for i := uint64(0); i < s.count; i++ {
		shard := s.shardBase() + i
		if !s.placement.contains(shard, s.replica) {
			continue
		}
		// 副本号要传本节点真实的那个。传 0 会得到 "shard not found"
		// ——库不会据此去查，0 就是一个不存在的副本号。那个错误在停机
		// 路径上无害，但它会盖掉停机过程里真正的那个。
		if err := s.nh.StopReplica(shard, s.replica); err != nil &&
			firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// StartSessionShards 在元数据组的 NodeHost 上起会话分片。
//
// 必须在元数据组之后调：NodeHost 是它建的。共享一个 NodeHost 的好处
// 正是 dragonboat 的多分片模型——一套传输、一次批量落盘。
func (g *TLSGroup) StartSessionShards(opt ShardOptions) (*ShardGroup, error) {
	if opt.Base != 0 && opt.Base != shardSessionBase {
		return nil, errors.New("cluster: 会话分片起始号不可改变")
	}
	opt.Base = shardSessionBase
	if opt.LayoutName == "" {
		opt.LayoutName = sessionLayoutName
	}
	if opt.Label == "" {
		opt.Label = "会话"
	}
	return g.StartShardGroup(opt)
}

// StartShardGroup 在共享 NodeHost 上启动一个独立的分片状态机组。
// 不同组必须使用互不重叠的 shard ID 区间和不同的布局文件。
func (g *TLSGroup) StartShardGroup(opt ShardOptions) (*ShardGroup, error) {
	grp := g.g()
	if grp == nil {
		return nil, errors.New("cluster: 元数据组还没装配完成")
	}
	dg, ok := grp.(*dragonGroup)
	if !ok {
		return nil, errors.New("cluster: 当前复制组不支持分片")
	}
	if (opt.Apply == nil && opt.ApplyBatch == nil) || opt.AppliedIndex == nil ||
		opt.SnapshotTo == nil || opt.RestoreFrom == nil {
		return nil, errors.New("cluster: 分片组缺少状态机回调")
	}
	count := opt.Count
	if count == 0 {
		count = defaultSessionShards
	}
	base := opt.Base
	if base == 0 {
		base = shardSessionBase
	}
	if base <= metaShardID || count > ^uint64(0)-base {
		return nil, fmt.Errorf("cluster: 无效的分片区间 [%d,%d)", base, base+count)
	}
	if err := g.ensureShardRangeAvailable(base, count); err != nil {
		return nil, err
	}
	label := opt.Label
	if label == "" {
		label = "分片"
	}
	layoutName := opt.LayoutName
	if layoutName == "" {
		layoutName = sessionLayoutName
	}
	replica, ok := opt.Replicas[g.id]
	if !ok || replica == 0 {
		return nil, fmt.Errorf("cluster: 副本号映射里没有本节点 %q", g.id)
	}

	placement, err := newShardPlacement(base, opt.Replicas, count, opt.ReplicationFactor)
	if err != nil {
		return nil, err
	}
	hasExisting := false
	for i := uint64(0); i < count; i++ {
		if dg.nh.HasNodeInfo(base+i, replica) {
			hasExisting = true
			break
		}
	}
	if err := ensureShardLayout(g.dir, layoutName, label,
		makeSessionLayout(count, opt.ReplicationFactor, opt.Replicas),
		hasExisting); err != nil {
		return nil, err
	}
	s := &ShardGroup{
		nh: dg.nh, base: base, count: count, replica: replica, applyTO: opt.ApplyTimeout,
		applied:      opt.AppliedIndex,
		placement:    placement,
		leaderCache:  make([]atomic.Uint64, count),
		leaderProbe:  make([]atomic.Pointer[leaderProbeState], count),
		read:         opt.ReadSession,
		readLane:     make(map[uint64]*sessionReadLane),
		merge:        opt.MergeProposals,
		mergeMax:     opt.MergeMaxItems,
		mergeNoValue: opt.MergeIgnoresResult,
		mergeLane:    make(map[uint64]*leaderProposalLane),
		transport:    g, replicaNode: make(map[uint64]NodeID, len(opt.Replicas)),
	}
	if s.mergeMax == 0 {
		s.mergeMax = leaderProposalMergeDefaultMax
	}
	if s.mergeMax < 1 || s.mergeMax > leaderProposalMergeHardMax {
		return nil, fmt.Errorf("cluster: leader提案合批上限%d超出1到%d",
			s.mergeMax, leaderProposalMergeHardMax)
	}
	s.readRemote = s.readRemoteSessions
	for id, n := range opt.Replicas {
		if n != 0 {
			s.replicaNode[n] = id
		}
	}
	s.stats.dur = metrics.NewHistogram(metrics.LatencyBuckets()...)
	if s.applyTO <= 0 {
		s.applyTO = 10 * time.Second
	}

	for i := uint64(0); i < count; i++ {
		shard := base + i
		if !placement.contains(shard, replica) {
			continue
		}
		// RF=all 保持历史冷启动方式。选择性放置时，每个分片的首选
		// 副本是唯一冷启动发起者；否则 node.mode=bootstrap 恰好不在
		// 某个分片里时，没有任何节点会创建那条 Raft 日志。
		starter, _ := placement.leader(shard)
		bootstrap := opt.Bootstrap
		if opt.ReplicationFactor != 0 {
			bootstrap = replica == starter
		}
		initial := map[uint64]dragonboat.Target{}
		if bootstrap {
			for _, member := range placement.members(shard) {
				id := s.replicaNode[member]
				addr, has := opt.Peers[id]
				if id == "" || !has || addr == "" {
					_ = s.Close()
					return nil, fmt.Errorf("cluster: %s分片 %d 的副本 %d 缺少节点地址", label, shard, member)
				}
				initial[member] = addr
			}
		}
		// 每个分片一套回调：状态机那头按分片号分开存，于是分片之间的
		// applied index 互不影响（它们是独立的日志）。
		machine := &metaMachine{
			appliedIndex: func() uint64 { return opt.AppliedIndex(shard) },
			snapshotTo:   func(w io.Writer) error { return opt.SnapshotTo(shard, w) },
			restoreFrom:  func(r io.Reader) error { return opt.RestoreFrom(shard, r) },
		}
		if opt.Apply != nil {
			machine.apply = func(index uint64, data []byte) (uint64, error) {
				return opt.Apply(shard, index, data)
			}
		}
		if opt.ApplyBatch != nil {
			machine.applyBatch = func(ents []ShardEntry) error {
				return opt.ApplyBatch(shard, ents)
			}
		}
		cfg := dbconfig.Config{
			ReplicaID: replica, ShardID: shard,
			ElectionRTT: 10, HeartbeatRTT: 1, CheckQuorum: true,
		}
		create := func(uint64, uint64) sm.IOnDiskStateMachine { return machine }
		if err := dg.nh.StartOnDiskReplica(initial, !bootstrap, create, cfg); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("cluster: 启动%s分片 %d 失败: %w", label, shard, err)
		}
	}
	if err := g.registerShardGroup(s); err != nil {
		_ = s.Close()
		return nil, err
	}
	if base == shardSessionBase {
		g.shardMu.Lock()
		if g.shards != nil && g.shards != s {
			g.shardMu.Unlock()
			_ = s.Close()
			return nil, errors.New("cluster: 会话分片组已经启动")
		}
		g.shards = s
		g.shardMu.Unlock()
	}
	return s, nil
}

// shardLeaderTargets 按 replica ID 排序后轮转分配分片 leader。
// 不用 map 迭代顺序：各节点必须为同一分片算出同一目标，
// 否则它们会反复把 leader 来回转移。
func shardLeaderTargets(replicas map[NodeID]uint64, count uint64) []uint64 {
	p, err := newSessionPlacement(replicas, count, 0)
	if err != nil {
		return nil
	}
	return p.leaders()
}
