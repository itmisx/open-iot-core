package meta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
)

// Group 是 ReplicatedStore 依赖的共识组。
//
// 只要一个方法：提一条日志。选举、复制、快照、落盘都在组里面，本包不碰——
// 那条分界就是「状态机不认识共识实现」。
//
// 已提交的条目**由组推过来**（组调 ReplicatedStore.ApplyCommitted），
// 不再由本包去拉。原先这里还有一个 Applied() [][]byte，拉模型的代价是
// 实打实的：实现侧必须把全部历史命令一直留在内存里，而每次追平都要把
// 这份历史整体深拷贝一遍——pump 每 200 毫秒一次，百万条命令时就是
// 每 200 毫秒复制上百 MB。推模型下状态机只看到增量。
type Group interface {
	// Propose 提一条日志并等它被提交与应用。非 leader 时返回
	// cluster.ErrNotLeader（由上层转发给 leader）；丢失多数派、
	// 正在排空或落盘失败时返回对应的错误。
	Propose(data []byte) error
}

// ReplicatedOptions 是 ReplicatedStore 的装配参数。
type ReplicatedOptions struct {
	// Group 是共识组，必填。
	Group Group
	// Node 是本节点 ID，用于生成命令的幂等键。
	Node string
	// Local 是承载视图的状态机。必填，且它的 Cold 与 Compile 要已注入。
	Local *LocalStore
	// Settle 等一会儿再重试追平，返回错误即放弃。
	//
	// 跟随者上必须有它：Propose 转发给 leader 并提交之后，这条日志还要
	// 由 leader 的下一轮 appendEntries 推回来，本节点才看得到。提交后
	// 立刻查本地必然查不到——第一版就是这样，症状是「从跟随者写入返回
	// 500，而那条数据其实写成功了」（CLU-071）。
	//
	// 为 nil 时只追平一次：进程内的组是同步的，提交即可见。
	Settle func(context.Context) error
	// SettleAttempts 是放弃前的重试次数。0 表示默认 40 次。
	SettleAttempts int

	// ColdSnapshot 把冷层整库流式写出（store.Snapshotter，CLU-003）。
	//
	// 由装配层传进来而不是本包自己持有 KV：冷层的生命周期属于节点装配，
	// 而本包只在"做快照"这一刻需要它。
	ColdSnapshot func(w io.Writer) error
	// ColdRestore 用一份整库快照替换冷层。返回后冷层就是新内容。
	ColdRestore func(r io.Reader) error
	// Hydrate 扫冷层、把热视图重建出来。
	//
	// 实现在 internal/registry：冷层记录的解码器在那边，而"热视图需要
	// 哪几个字段"是本包的事——两者在这个回调上碰头。
	Hydrate func(apply func(Command) error) error
}

// ReplicatedStore 是 Store 的多副本实现。
//
// 它与 LocalStore 的关系不是「另一种实现」，而是「在同一个状态机前面
// 加一道日志」：命令先编码提给共识组，等它被多数派提交之后，再由
// ApplyCommitted 作用到同一个 LocalStore 上。单机那条路径（LocalStore
// 直接 Apply 一副本组）因此不是旁路——两者应用命令的代码完全相同
// （ARCH-057、CLU-067）。
type ReplicatedStore struct {
	group    Group
	local    *LocalStore
	node     string
	seq      atomic.Uint64
	settle   func(context.Context) error
	attempts int

	coldSnapshot func(w io.Writer) error
	coldRestore  func(r io.Reader) error
	hydrate      func(apply func(Command) error) error

	// mu 串行化提交与追平。
	//
	// Apply 要「提一条、追平到它、读回它的 revision」，这三步之间若有
	// 别人插进来追平，revision 仍然正确（按命令 ID 查），但两个 Apply
	// 同时推进 nextIdx 会让同一条日志被应用两次——ApplyCommitted 会以
	// 序号不连续报错，而那不是真分叉，只是本地没串行化。
	mu      sync.Mutex
	nextIdx int
}

// NewReplicatedStore 创建多副本元数据存储。
func NewReplicatedStore(opt ReplicatedOptions) (*ReplicatedStore, error) {
	switch {
	case opt.Group == nil:
		return nil, errors.New("meta: 缺少共识组")
	case opt.Local == nil:
		return nil, errors.New("meta: 缺少状态机")
	case opt.Node == "":
		return nil, errors.New("meta: 缺少节点 ID（命令的幂等键要用它）")
	}
	attempts := opt.SettleAttempts
	if attempts <= 0 {
		attempts = 40
	}
	return &ReplicatedStore{
		group: opt.Group, local: opt.Local, node: opt.Node,
		settle: opt.Settle, attempts: attempts,
		coldSnapshot: opt.ColdSnapshot, coldRestore: opt.ColdRestore,
		hydrate: opt.Hydrate,
	}, nil
}

func (s *ReplicatedStore) View() *View            { return s.local.View() }
func (s *ReplicatedStore) Changes() <-chan Change { return s.local.Changes() }
func (s *ReplicatedStore) Close() error           { return s.local.Close() }

// Apply 提交一条命令，等它被应用，然后返回它的 revision。
//
// 命令 ID 为空时自动补一个（节点 ID 加本地序号）：集群下必须靠 ID 才能
// 在追平之后认出「刚才提的是第几号」，而控制面的多数命令本来不带 ID。
// 它同时给了重试的幂等性（CLU-036）。
func (s *ReplicatedStore) Apply(ctx context.Context, cmd Command) (Revision, error) {
	if err := cmd.Validate(); err != nil {
		return 0, err
	}
	if cmd.ID == "" {
		cmd.ID = s.node + "-" + strconv.FormatUint(s.seq.Add(1), 10)
	}

	// 已经应用过的命令不再提日志：重试与至少一次投递都安全。
	if rev, ok := s.local.AppliedRevision(cmd.ID); ok {
		return rev, nil
	}

	raw, err := EncodeCommand(cmd)
	if err != nil {
		return 0, err
	}
	if err := s.group.Propose(raw); err != nil {
		// 丢失多数派、正在排空、或落盘失败：写入失败而读取照常
		// （CLU-042——设备连接、订阅、收发都只读本地视图）。
		return 0, fmt.Errorf("meta: 提交日志失败: %w", err)
	}
	// 等到本节点真的应用了它再返回 revision。
	//
	// 推模型下，leader 上 Propose 返回时状态机已经应用过它（库的
	// Apply future 要等 FSM 走完），所以第一圈就能查到。跟随者上
	// 这条要等 leader 把它复制回来——那是一次网络往返，不是错误。
	for attempt := 0; ; attempt++ {
		if rev, ok := s.local.AppliedRevision(cmd.ID); ok {
			return rev, nil
		}
		if s.settle == nil || attempt+1 >= s.attempts {
			// 已经提交却迟迟没在本地出现：报出来而不是返回一个编出来的
			// revision。调用方拿到的是「不知道它生效了没有」，这比
			// 「告诉你成功了」诚实——那条命令其实已经在日志里。
			return 0, fmt.Errorf("meta: 命令 %s 已提交，但本节点在等了 %d 次之后仍没有应用它",
				cmd.ID, attempt+1)
		}
		if err := s.settle(ctx); err != nil {
			return 0, fmt.Errorf("meta: 等待命令 %s 被本节点应用时中断: %w", cmd.ID, err)
		}
	}
}

// ApplyCommitted 应用一条由共识组推过来的已提交日志（CLU-097）。
//
// 它是装给 cluster.RaftOptions.Apply 的那个回调：共识库按序、恰好一次
// 地调它，index 是 raft 的日志序号，直接当 revision 用。
//
// 返回错误就是**致命**的：那一条已经被多数派提交，别的节点会应用它，
// 本节点应用不了意味着状态机分叉。组会据此停掉本节点的写入
// （cluster.ErrApplyFailed），而不是跳过这一条继续跑。
func (s *ReplicatedStore) ApplyCommitted(index uint64, data []byte) error {
	return s.ApplyCommittedBatch([]CommittedData{{Index: index, Data: data}})
}

// CommittedData 是共识层交付的一条原始已提交日志。
type CommittedData struct {
	Index uint64
	Data  []byte
}

// ApplyCommittedBatch 先解码整批，再以一个持久化事务交给 LocalStore。
// 预先解码保证坏条目不会在前半批已经改变热视图之后才被发现。
func (s *ReplicatedStore) ApplyCommittedBatch(batch []CommittedData) error {
	entries := make([]CommittedEntry, len(batch))
	for i := range batch {
		cmd, err := DecodeCommand(batch[i].Data)
		if err != nil {
			return fmt.Errorf("meta: 第 %d 条已提交日志无法解析: %w", batch[i].Index, err)
		}
		entries[i] = CommittedEntry{Revision: Revision(batch[i].Index), Command: cmd}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.local.ApplyCommittedBatch(entries); err != nil {
		if len(batch) == 0 {
			return err
		}
		return fmt.Errorf("meta: 应用第 %d..%d 条已提交日志失败: %w",
			batch[0].Index, batch[len(batch)-1].Index, err)
	}
	for i := range batch {
		if int(batch[i].Index) > s.nextIdx {
			s.nextIdx = int(batch[i].Index)
		}
	}
	return nil
}

// Hydrate 用冷层的内容重建热视图，不写日志。
//
// 与 RestoreFrom 的区别只在来路：那边是刚装上来的快照，这边是节点自己
// 盘上已有的冷层（启动时，P2）。两者都必须把 revision 设进视图，否则
// 就绪度会判它陈旧（CLU-059 比的就是 revision）。
//
// emit 由调用方给（registry 扫冷层），与装快照那条路径共用同一份扫描
// ——两处各写一遍的结局是只改一份，而"重建时漏了一类记录"的表现是
// 那一类对象在重建后的节点上静默消失。
func (s *ReplicatedStore) Hydrate(rev Revision,
	emit func(apply func(Command) error) error,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.local.Hydrate(rev, emit); err != nil {
		return err
	}
	if int(rev) > s.nextIdx {
		s.nextIdx = int(rev)
	}
	return nil
}

// MarkConfirmed 记录视图已确认追平（CLU-023）。
//
// 由装配层在 Pump 成功之后调用：没人调它，视图会被判为陈旧并对新连接
// fail-closed——那是正确的保守行为，但一个其实追平了的节点不该拒连。
func (s *ReplicatedStore) MarkConfirmed(nowUnixMs int64) { s.local.MarkConfirmed(nowUnixMs) }

// AppliedIndex 返回本节点已经应用到第几条日志，供就绪度与指标使用。
func (s *ReplicatedStore) AppliedIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextIdx
}

var _ Store = (*ReplicatedStore)(nil)

/*
状态机快照（CLU-098、CLU-099）。

形状是「8 字节 revision + 冷层整库流」。冷层是元数据的权威记录
（CLU-065），热视图是从它派生出来的，所以快照只需要带冷层——
热视图在恢复侧重建。

为什么要把 revision 放进去:raft 自己记的快照序号是它调 Snapshot() 那一刻
的 lastApplied,而真正把库写出去是在 Persist 里（流式导出的意义就在于
不先读进内存）。两者之间本节点可能又应用了几条命令，于是流里的冷层
**可能比 raft 记的序号新**。把实际的 revision 带在流里，恢复侧就知道
视图该报哪个数；而 raft 从它自己那个较旧的序号之后重放日志时，早于
视图 revision 的那些条目会被 ApplyCommitted 当重复投递忽略——这正是
把判据放宽成"严格递增"之后自然得到的收敛行为。
*/

// snapshotVersion 是状态机快照的格式版本。
const snapshotVersion byte = 1

// SnapshotTo 把状态机的全量状态流式写出。
func (s *ReplicatedStore) SnapshotTo(w io.Writer) error {
	if s.coldSnapshot == nil {
		return errors.New("meta: 未注入冷层快照函数，无法产出状态机快照")
	}
	head := make([]byte, 0, 1+8)
	head = append(head, snapshotVersion)
	head = binary.BigEndian.AppendUint64(head, uint64(s.local.Revision()))
	if _, err := w.Write(head); err != nil {
		return fmt.Errorf("meta: 写快照头失败: %w", err)
	}
	if err := s.coldSnapshot(w); err != nil {
		return fmt.Errorf("meta: 导出冷层失败: %w", err)
	}
	return nil
}

// RestoreFrom 用一份快照替换状态机的全量状态。
//
// 三步：换冷层、重建热视图、把 revision 设成快照里的那个。中途失败时
// 热视图停在半份状态而 revision 仍是 0,于是就绪度判它陈旧并对新连接
// fail-closed（CLU-023）——那是正确的保守行为：一个只重建了一半的节点
// 不知道哪张证书已经被吊销。
func (s *ReplicatedStore) RestoreFrom(r io.Reader) error {
	if s.coldRestore == nil || s.hydrate == nil {
		return errors.New("meta: 未注入冷层恢复或热视图重建函数")
	}
	head := make([]byte, 1+8)
	if _, err := io.ReadFull(r, head); err != nil {
		return fmt.Errorf("meta: 读快照头失败: %w", err)
	}
	if head[0] != snapshotVersion {
		return fmt.Errorf("meta: 快照格式版本是 %d，本程序只认 %d", head[0], snapshotVersion)
	}
	rev := Revision(binary.BigEndian.Uint64(head[1:]))

	if err := s.coldRestore(r); err != nil {
		return fmt.Errorf("meta: 用快照替换冷层失败: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.local.Hydrate(rev, s.hydrate); err != nil {
		return err
	}
	s.nextIdx = int(rev)
	return nil
}
