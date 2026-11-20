package cluster

import "time"

// replicationGroup 是复制组的接口面。
//
// 当前只有一个实现（dragonGroup，建在 dragonboat 上）。接口留着是
// LANG-015 要的那件事：上层对共识实现零感知、可模块级替换——2026-11-19
// 从 hashicorp/raft 换过来时，上层确实一行没改。
//
// 十个方法，与换库之前的导出面完全相同。
type replicationGroup interface {
	// Propose 提一条命令并等它被提交与应用。
	Propose(data []byte) error
	// IsLeader 报告本节点是不是 leader。
	IsLeader() bool
	// LeaderID 返回当前 leader 的节点 ID，未知时为空。
	LeaderID() NodeID
	// AppliedIndex 返回本节点已应用到的日志序号。
	AppliedIndex() uint64
	// Barrier 等本节点追平到当前已提交的位置。
	Barrier(timeout time.Duration) error
	// Members 返回当前成员列表。
	Members() []Member
	// AddMember 把一个节点加进成员列表（CLU-090）。只有 leader 能做。
	//
	// replicaID 为 0 表示"用本节点配置里记着的那个"——初始成员都在
	// 配置里，加的是新节点时它不在，必须由调用方给（见 adminapi 的说明）。
	AddMember(id NodeID, addr string, replicaID uint64) error
	// RemoveMember 把一个节点移出成员列表。只有 leader 能做。
	RemoveMember(id NodeID) error
	// Snapshot 主动做一次快照。
	Snapshot() error
	// Close 停掉复制组。
	Close() error
}

// groupRef 把接口值包一层，好放进 atomic.Pointer。
//
// 直接用 atomic.Pointer[replicationGroup] 存的是"指向接口值的指针"，
// 读写两侧都要多一次解引用，而装配期那次并发（见 TLSGroup.group 的说明）
// 本来就容易看错。包一层让它只是一个普通的原子指针。
type groupRef struct{ g replicationGroup }

var _ replicationGroup = (*dragonGroup)(nil)
