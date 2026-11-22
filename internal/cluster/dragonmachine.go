package cluster

import (
	"errors"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// metaMachine 把本项目的元数据状态机装成 dragonboat 的 IOnDiskStateMachine。
//
// 选 on-disk 而不是 IStateMachine（CLU-003）：本项目的状态本来就在盘上
// （冷层是 bbolt），而 IStateMachine 的模型是"状态在内存、重启时由库把
// 快照整个灌回来"——百万设备下那是每次重启都把整个库复制一遍。
//
// 一条跟着 dragonboat 来的部署约束：它把 NodeHost 的 RaftAddress **记进
// 数据目录**，换了地址再打开会直接报 "not the owner of the data directory"。
// 所以复制地址必须跨重启稳定——k8s 那份清单用的是 Pod 的稳定 DNS 名、
// compose 用的是服务名，两者都满足；但"改一下 cluster.replication.addr
// 再重启"从此是起不来的操作，而不是改了就生效。这条是实测出来的（用例
// 第一版用了随机端口，重启那一步直接失败）。
//
// 这个适配器能成立，靠的是 CLU-109 那一步：applied index 已经随冷层
// 一起落盘了。没有它，Open 只能报 0，而 dragonboat **不会重放早于该序号
// 的日志**（那些已被快照压掉），后果是丢数据而不是慢。
// ShardEntry 是一批已提交日志中的一条。
//
// Result 由 ApplyBatch 回填并逐条交回提案方：epoch 围栏的判定是按条
// 返回的（CLU-116），整批一个值会把"我这条被旧 epoch 拦了"变成"这批
// 里有人被拦了"，而提案方要的是前者。
type ShardEntry struct {
	Index  uint64
	Data   []byte
	Result uint64
}

type metaMachine struct {
	// apply 应用一条已提交日志，index 即 raft 日志序号。
	// 它必须在返回前把状态与 index 一起落盘（见 Update 的说明）。
	//
	// 第一个返回值回给提案方（`SyncPropose` 的 Result.Value）。元数据组
	// 不用它（提案方从本地视图读 revision），会话分片用它回报围栏判定：
	// 「这条写入来自已被接管的旧会话」这件事只有在 apply 时才判得准，
	// 提交之前判等于在两个节点各判一次，而它们看到的当前 epoch 可能
	// 不同——那正是契约 01 要拦的那种覆盖。
	apply func(index uint64, data []byte) (uint64, error)
	// applyBatch 整批应用，非空时取代 apply（CLU-125）。
	//
	// 两条路径并存而不是统一掉：元数据组的一条命令可能带很多条冷层
	// 记录，而它的写入是人工操作的结果（每秒几十到上百），没有合批的
	// 需要；会话分片在 CONNECT 路径上，有。
	applyBatch func(ents []ShardEntry) error
	// appliedIndex 回报已经持久化到哪一条，供 Open 使用。
	appliedIndex func() uint64
	snapshotTo   func(io.Writer) error
	restoreFrom  func(io.Reader) error
}

var _ sm.IOnDiskStateMachine = (*metaMachine)(nil)

// Open 回报已持久化的 applied index。
//
// 这个数字是整个适配器的支点：dragonboat 据它决定从哪一条开始重放。
// 报大了会跳过没应用的日志（丢数据），报小了只是重做几条（幂等，无害）。
// 所以它取的是**冷层里那个**，不是内存里的——两者的差别正是崩溃窗口。
func (m *metaMachine) Open(stopc <-chan struct{}) (uint64, error) {
	select {
	case <-stopc:
		return 0, sm.ErrOpenStopped
	default:
	}
	if m.appliedIndex == nil {
		return 0, errors.New("cluster: 状态机没有提供 applied index")
	}
	return m.appliedIndex(), nil
}

// Update 应用一批已提交日志。
//
// 本实现选择 dragonboat 契约里的**第一种**做法：在 Update 内部就把状态
// 与 index 同步落盘（控制面的写入本来就是 sync 的，见 meta 的说明），
// 而不是推迟到 Sync。代价是吞吐，换到的是重启后需要重放的日志最少。
// 对一个"写入是人工操作的结果"的控制面，这个取舍是对的。
//
// 返回错误是**致命**的：这一批已经被多数派提交，别的节点会应用它们，
// 本节点应用不了就意味着状态机分叉。dragonboat 会据此停掉本节点，
// 这与原先 cluster.ErrApplyFailed 的行为一致——不跳过、不继续。
func (m *metaMachine) Update(ents []sm.Entry) ([]sm.Entry, error) {
	// 整批那一条路径（CLU-125）：会话分片走它，元数据组仍走逐条。
	//
	// 两者的差别只在 fsync 次数，不在语义——applyBatch 也要在返回前把
	// 状态与 index 一起落盘，只是"一起"的范围变成整批。
	if m.applyBatch != nil {
		batch := make([]ShardEntry, len(ents))
		for i := range ents {
			batch[i] = ShardEntry{Index: ents[i].Index, Data: ents[i].Cmd}
		}
		if err := m.applyBatch(batch); err != nil {
			return nil, err
		}
		for i := range ents {
			ents[i].Result = sm.Result{Value: batch[i].Result}
		}
		return ents, nil
	}
	for i := range ents {
		v, err := m.apply(ents[i].Index, ents[i].Cmd)
		if err != nil {
			return nil, err
		}
		// Result 是给提案方的返回值，含义由 apply 决定（见上面的说明）。
		ents[i].Result = sm.Result{Value: v}
	}
	return ents, nil
}

// Lookup 是只读查询入口。
//
// 只认一种查询：Barrier 用的那个空标记。它要的不是数据，是"这次 read
// index 走通了"这件事本身——也就是本节点已经追平到当前已提交的位置。
//
// 其余一概拒绝。本项目的读取一律从本地物化视图来（契约 05 要求读不经过
// 写入路径，`MetaView` 的方法签名里连 error 都没有）；返回明确的错误而
// 不是 nil，免得将来有人以为这条路通着、把读取挪过来。
func (m *metaMachine) Lookup(q interface{}) (interface{}, error) {
	if _, ok := q.(barrierQuery); ok {
		return struct{}{}, nil
	}
	return nil, errors.New("cluster: 元数据读取走本地物化视图，不经状态机")
}

// Sync 是空操作：Update 里已经同步落盘了。
func (m *metaMachine) Sync() error { return nil }

// PrepareSnapshot 不需要捕获任何东西。
//
// 冷层是 bbolt，它的只读事务本身就是一致快照（`Tx.WriteTo`），
// 所以这里不必先把状态复制出来再慢慢写——这正是 CLU-003 要的那种
// "流式产出、内存占用与数据量无关"。
func (m *metaMachine) PrepareSnapshot() (interface{}, error) { return nil, nil }

// SaveSnapshot 把快照写进 w，形如「revision 头加冷层整库流」。
func (m *metaMachine) SaveSnapshot(_ interface{}, w io.Writer,
	stopc <-chan struct{},
) error {
	select {
	case <-stopc:
		return sm.ErrSnapshotStopped
	default:
	}
	return m.snapshotTo(w)
}

// RecoverFromSnapshot 用 leader 送来的快照替换本地状态。
//
// 它只在"落后太多、日志已被压掉"时被调用。恢复侧换掉冷层之后要重建
// 热视图（registry.HydrateHot），那一步在 meta.ReplicatedStore.RestoreFrom
// 里，与启动时的重建（CLU-110）共用同一份扫描。
func (m *metaMachine) RecoverFromSnapshot(r io.Reader,
	stopc <-chan struct{},
) error {
	select {
	case <-stopc:
		return sm.ErrSnapshotStopped
	default:
	}
	return m.restoreFrom(r)
}

// Close 不关闭冷层：那个库的生命周期归装配层，状态机只是用它。
//
// 在这里关掉的后果是节点停机时冷层被关两次，而第二次的错误会盖掉
// 停机过程里真正的那个。
func (m *metaMachine) Close() error { return nil }
