package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"time"

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
	// Count 是分片数。0 取默认值。
	Count uint64
	// Replicas 与 Peers 同元数据组：共识层按副本号认成员。
	Replicas map[NodeID]uint64
	Peers    map[NodeID]string
	// Bootstrap 为真时用 Peers 建组，否则以 join 方式启动。
	Bootstrap bool

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

	ApplyTimeout time.Duration
}

// ShardGroup 是一组会话分片。
type ShardGroup struct {
	nh      *dragonboat.NodeHost
	count   uint64
	replica uint64
	applyTO time.Duration
}

// ShardOf 返回某个 clientId 所属的分片号。
//
// FNV-1a：纯函数、无状态，所以每个节点算出的是同一个分片——这一点是
// 必须的，而不是优化。换成带随机种子的哈希（Go 的 map 哈希就是）会让
// 不同节点把同一个会话算到不同分片上，表现是"会话时有时无"。
func (s *ShardGroup) ShardOf(clientID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))
	return shardSessionBase + h.Sum64()%s.count
}

// ShardCount 返回分片数。
func (s *ShardGroup) ShardCount() uint64 { return s.count }

// ShardIDs 返回全部分片号。
func (s *ShardGroup) ShardIDs() []uint64 {
	out := make([]uint64, 0, s.count)
	for i := uint64(0); i < s.count; i++ {
		out = append(out, shardSessionBase+i)
	}
	return out
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
	ctx, cancel := context.WithTimeout(context.Background(), s.applyTO)
	defer cancel()
	res, err := s.nh.SyncPropose(ctx, s.nh.GetNoOPSession(shard), data)
	if err != nil {
		if errors.Is(err, dragonboat.ErrShardNotReady) ||
			errors.Is(err, dragonboat.ErrTimeout) {
			return 0, ErrNotLeader
		}
		return 0, err
	}
	return res.Value, nil
}

// Barrier 等某个分片追平到当前已提交的位置。
func (s *ShardGroup) Barrier(clientID string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = s.applyTO
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := s.nh.SyncRead(ctx, s.ShardOf(clientID), barrierQuery{})
	return err
}

// Ready 报告所有分片是否都已选出 leader。
//
// 启动时要等它：分片没就绪时 Propose 返回 ErrNotLeader，而那条错误
// 在连接路径上会变成"设备连不上"。
func (s *ShardGroup) Ready() bool {
	for i := uint64(0); i < s.count; i++ {
		if _, _, ok, err := s.nh.GetLeaderID(shardSessionBase + i); err != nil || !ok {
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
	var firstErr error
	for i := uint64(0); i < s.count; i++ {
		// 副本号要传本节点真实的那个。传 0 会得到 "shard not found"
		// ——库不会据此去查，0 就是一个不存在的副本号。那个错误在停机
		// 路径上无害，但它会盖掉停机过程里真正的那个。
		if err := s.nh.StopReplica(shardSessionBase+i, s.replica); err != nil &&
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
	grp := g.g()
	if grp == nil {
		return nil, errors.New("cluster: 元数据组还没装配完成")
	}
	dg, ok := grp.(*dragonGroup)
	if !ok {
		return nil, errors.New("cluster: 当前复制组不支持分片")
	}
	if opt.Apply == nil || opt.AppliedIndex == nil ||
		opt.SnapshotTo == nil || opt.RestoreFrom == nil {
		return nil, errors.New("cluster: 分片组缺少状态机回调")
	}
	count := opt.Count
	if count == 0 {
		count = defaultSessionShards
	}
	replica, ok := opt.Replicas[g.id]
	if !ok || replica == 0 {
		return nil, fmt.Errorf("cluster: 副本号映射里没有本节点 %q", g.id)
	}

	s := &ShardGroup{nh: dg.nh, count: count, replica: replica, applyTO: opt.ApplyTimeout}
	if s.applyTO <= 0 {
		s.applyTO = 10 * time.Second
	}

	initial := map[uint64]dragonboat.Target{}
	if opt.Bootstrap {
		for id, addr := range opt.Peers {
			n, has := opt.Replicas[id]
			if !has {
				return nil, fmt.Errorf("cluster: 成员 %q 没有副本号", id)
			}
			initial[n] = addr
		}
	}

	for i := uint64(0); i < count; i++ {
		shard := shardSessionBase + i
		// 每个分片一套回调：状态机那头按分片号分开存，于是分片之间的
		// applied index 互不影响（它们是独立的日志）。
		machine := &metaMachine{
			apply: func(index uint64, data []byte) (uint64, error) {
				return opt.Apply(shard, index, data)
			},
			appliedIndex: func() uint64 { return opt.AppliedIndex(shard) },
			snapshotTo:   func(w io.Writer) error { return opt.SnapshotTo(shard, w) },
			restoreFrom:  func(r io.Reader) error { return opt.RestoreFrom(shard, r) },
		}
		cfg := dbconfig.Config{
			ReplicaID: replica, ShardID: shard,
			ElectionRTT: 10, HeartbeatRTT: 1, CheckQuorum: true,
		}
		create := func(uint64, uint64) sm.IOnDiskStateMachine { return machine }
		if err := dg.nh.StartOnDiskReplica(initial, !opt.Bootstrap, create, cfg); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("cluster: 启动会话分片 %d 失败: %w", shard, err)
		}
	}
	return s, nil
}
