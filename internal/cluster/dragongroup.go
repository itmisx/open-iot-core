package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// metaShardID 是元数据复制组的分片号。
//
// 固定为 1：当前只有一个组（分区组是后面那一步，CLU-006）。写成常量而
// 不是配置项——它是协议标识不是旋钮，配错了的表现是"节点之间看不见
// 对方的日志"，而那在日志里像网络问题。
const metaShardID uint64 = 1

// barrierQuery 是 Barrier 用的只读查询标记，见 metaMachine.Lookup。
type barrierQuery struct{}

// DragonOptions 是 dragonGroup 的构造参数。
//
// 字段与 RaftOptions 刻意保持对应，使换库在装配层只是换一个构造函数。
type DragonOptions struct {
	ID  NodeID
	Dir string
	// Replicas 是节点 ID 到副本号的映射（cluster.replication.replicas）。
	// 须含本节点自己，且与 Peers 的键集合相同——校验在配置层做。
	Replicas map[NodeID]uint64
	Peers    map[NodeID]string

	// Apply 由状态机调用，index 是 raft 日志序号。
	Apply func(index uint64, data []byte) error
	// AppliedIndex 回报已持久化到哪一条（CLU-109）。
	AppliedIndex func() uint64
	SnapshotTo   func(w io.Writer) error
	RestoreFrom  func(r io.Reader) error

	// Transport 是接在本项目 TLS 监听上的传输层工厂。
	Transport dbconfig.TransportFactory

	Bootstrap    bool
	RTT          time.Duration
	ElectionRTT  uint64
	HeartbeatRTT uint64
	ApplyTimeout time.Duration
}

// dragonGroup 是建在 dragonboat 上的元数据复制组。
//
// 方法面与 RaftGroup 一一对应（Propose/IsLeader/LeaderID/AppliedIndex/
// Barrier/Members/AddMember/RemoveMember/Snapshot/Close），所以上层
// （TLSGroup 与装配层）不感知换了哪一个实现——这正是 LANG-015 要的
// "可模块级替换"。
type dragonGroup struct {
	nh      *dragonboat.NodeHost
	self    NodeID
	replica uint64
	applyTO time.Duration

	// byID 与 byReplica 是两个方向的身份映射。
	//
	// 共识层按 uint64 认成员，本项目按字符串节点 ID（并与证书
	// CommonName 绑死，CLU-069）。两张表都留着，是因为两个方向都要用：
	// 加成员时要把 ID 换成号，报 leader 时要把号换回 ID。
	mu        sync.RWMutex
	byID      map[NodeID]uint64
	byReplica map[uint64]NodeID

	appliedIndex func() uint64
}

// newDragonGroup 建起复制组。
func newDragonGroup(opt DragonOptions) (*dragonGroup, error) {
	replica, ok := opt.Replicas[opt.ID]
	if !ok || replica == 0 {
		return nil, fmt.Errorf("cluster: 副本号映射里没有本节点 %q", opt.ID)
	}
	g := &dragonGroup{
		self: opt.ID, replica: replica,
		applyTO:      opt.ApplyTimeout,
		byID:         make(map[NodeID]uint64, len(opt.Replicas)),
		byReplica:    make(map[uint64]NodeID, len(opt.Replicas)),
		appliedIndex: opt.AppliedIndex,
	}
	if g.applyTO <= 0 {
		g.applyTO = 10 * time.Second
	}
	for id, n := range opt.Replicas {
		g.byID[id] = n
		g.byReplica[n] = id
	}

	rtt := opt.RTT
	if rtt <= 0 {
		rtt = 100 * time.Millisecond
	}
	nhc := dbconfig.NodeHostConfig{
		NodeHostDir:    opt.Dir,
		RTTMillisecond: uint64(rtt / time.Millisecond),
		// 地址即本节点的复制地址。它必须跨重启稳定：dragonboat 把它记进
		// 数据目录，换了地址再打开会直接报"不是这个目录的主人"。
		RaftAddress: opt.Peers[opt.ID],
	}
	nhc.Expert.TransportFactory = opt.Transport

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("cluster: 建 NodeHost 失败: %w", err)
	}
	g.nh = nh

	machine := &metaMachine{
		// 元数据组不用返回值：提案方从本地视图读 revision。
		apply: func(index uint64, data []byte) (uint64, error) {
			return 0, opt.Apply(index, data)
		},
		appliedIndex: opt.AppliedIndex,
		snapshotTo:   opt.SnapshotTo,
		restoreFrom:  opt.RestoreFrom,
	}
	cfg := dbconfig.Config{
		ReplicaID: replica, ShardID: metaShardID,
		ElectionRTT: opt.ElectionRTT, HeartbeatRTT: opt.HeartbeatRTT,
		CheckQuorum: true,
	}
	if cfg.ElectionRTT == 0 {
		cfg.ElectionRTT = 10
	}
	if cfg.HeartbeatRTT == 0 {
		cfg.HeartbeatRTT = 1
	}

	// bootstrap 给初始成员，join 给空表加 join=true。
	//
	// 两者不能混：带着初始成员去 join，这个节点会以为自己本来就在组里，
	// 于是不等别人同意就开始竞选——而它的日志是空的。
	initial := map[uint64]dragonboat.Target{}
	if opt.Bootstrap {
		for id, addr := range opt.Peers {
			n, ok := g.byID[id]
			if !ok {
				nh.Close()
				return nil, fmt.Errorf("cluster: 成员 %q 没有副本号", id)
			}
			initial[n] = addr
		}
	}
	create := func(uint64, uint64) sm.IOnDiskStateMachine { return machine }
	if err := nh.StartOnDiskReplica(initial, !opt.Bootstrap, create, cfg); err != nil {
		nh.Close()
		return nil, fmt.Errorf("cluster: 启动复制组失败: %w", err)
	}
	return g, nil
}

func (g *dragonGroup) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), g.applyTO)
}

// Propose 提一条命令并等它被提交与应用。
func (g *dragonGroup) Propose(data []byte) error {
	ctx, cancel := g.ctx()
	defer cancel()
	_, err := g.nh.SyncPropose(ctx, g.nh.GetNoOPSession(metaShardID), data)
	if err != nil {
		if errors.Is(err, dragonboat.ErrShardNotReady) ||
			errors.Is(err, dragonboat.ErrTimeout) {
			// 没有 leader 或还没追平时按"不是 leader"回报：上层据此
			// 转发或回 503，与换库之前的行为一致。
			return ErrNotLeader
		}
		return err
	}
	return nil
}

// IsLeader 报告本节点是不是 leader。
func (g *dragonGroup) IsLeader() bool {
	leader, _, valid, err := g.nh.GetLeaderID(metaShardID)
	return err == nil && valid && leader == g.replica
}

// LeaderID 返回当前 leader 的节点 ID，未知时为空。
func (g *dragonGroup) LeaderID() NodeID {
	leader, _, valid, err := g.nh.GetLeaderID(metaShardID)
	if err != nil || !valid {
		return ""
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.byReplica[leader]
}

// AppliedIndex 返回本节点已应用到的日志序号。
//
// 取自状态机而不是库：这个数字要与视图 revision 一致（CLU-059 比的就是
// 它），而库那边的 applied 是它自己的记账。
func (g *dragonGroup) AppliedIndex() uint64 {
	if g.appliedIndex == nil {
		return 0
	}
	return g.appliedIndex()
}

// Barrier 等本节点追平到当前已提交的位置。
//
// 走一次 linearizable read（read index），而不是提一条空日志：后者会
// 把屏障本身写进日志，于是"确认追平"这件事自己撑大了日志。
func (g *dragonGroup) Barrier(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = g.applyTO
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := g.nh.SyncRead(ctx, metaShardID, barrierQuery{})
	return err
}

// Members 返回当前成员列表。
func (g *dragonGroup) Members() []Member {
	ctx, cancel := g.ctx()
	defer cancel()
	ms, err := g.nh.SyncGetShardMembership(ctx, metaShardID)
	if err != nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Member, 0, len(ms.Nodes))
	for replica, addr := range ms.Nodes {
		id := g.byReplica[replica]
		if id == "" {
			// 映射里没有这个副本号：它是本节点启动之后加进来的成员。
			// 用号当名字而不是丢掉——丢掉会让成员数对不上，而那正是
			// 多数派算术的依据。
			id = NodeID(fmt.Sprintf("replica-%d", replica))
		}
		out = append(out, Member{ID: id, Addr: addr})
	}
	return out
}

// AddMember 把一个节点加进成员列表（CLU-090）。只有 leader 能做。
func (g *dragonGroup) AddMember(id NodeID, addr string, replicaID uint64) error {
	if !g.IsLeader() {
		return ErrNotLeader
	}
	replica := replicaID
	if replica == 0 {
		// 没给就查本节点的配置：初始成员都在那里。
		g.mu.RLock()
		replica = g.byID[id]
		g.mu.RUnlock()
	}
	if replica == 0 {
		return fmt.Errorf("cluster: 节点 %q 没有副本号，加入时必须给出 replicaId", id)
	}
	for _, m := range g.Members() {
		if m.ID == id {
			return ErrAlreadyMember
		}
	}
	ctx, cancel := g.ctx()
	defer cancel()
	// configChangeIndex 传 0 表示"不做乐观并发检查"。
	// 成员变更一次只允许一个在途（CLU-091），由上层串行化。
	if err := g.nh.SyncRequestAddReplica(ctx, metaShardID, replica, addr, 0); err != nil {
		return err
	}
	// 记住新成员的号：之后报 leader、列成员都要用它反查节点 ID。
	g.mu.Lock()
	g.byID[id] = replica
	g.byReplica[replica] = id
	g.mu.Unlock()
	return nil
}

// RemoveMember 把一个节点移出成员列表。只有 leader 能做。
func (g *dragonGroup) RemoveMember(id NodeID) error {
	if !g.IsLeader() {
		return ErrNotLeader
	}
	g.mu.RLock()
	replica, ok := g.byID[id]
	g.mu.RUnlock()
	if !ok {
		return fmt.Errorf("cluster: 节点 %q 没有副本号，无法移出", id)
	}
	ctx, cancel := g.ctx()
	defer cancel()
	return g.nh.SyncRequestDeleteReplica(ctx, metaShardID, replica, 0)
}

// Snapshot 主动做一次快照。
func (g *dragonGroup) Snapshot() error {
	ctx, cancel := g.ctx()
	defer cancel()
	_, err := g.nh.SyncRequestSnapshot(ctx, metaShardID, dragonboat.SnapshotOption{})
	if err != nil && errors.Is(err, dragonboat.ErrRejected) {
		// 两次快照之间没有新日志时库会拒绝。那不是错误：调用方要的是
		// "现在有一份快照"，而它已经有了。
		return nil
	}
	return err
}

// Close 停掉复制组。
func (g *dragonGroup) Close() error {
	g.nh.Close()
	return nil
}
