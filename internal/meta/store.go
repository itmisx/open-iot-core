package meta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// ErrRevisionConflict 表示命令的 RequestedRevision 与当前 revision 不符。
// 对外映射为 AWS 的 VersionConflictException（PRD API-011）。
var ErrRevisionConflict = errors.New("meta: revision 冲突")

// ErrStoreClosed 表示存储已关闭。
var ErrStoreClosed = errors.New("meta: 存储已关闭")

// ErrReadOnly 表示当前无法写入。
//
// 集群下丢失 quorum 时返回它：设备连接、订阅、收发仍然正常，
// 因为认证鉴权只读本地视图；只有注册、吊销、策略变更这类写操作失败。
// 这是内嵌共识的固有取舍，须在用户文档明确声明（PRD CLU-042）。
var ErrReadOnly = errors.New("meta: 元数据当前只读，无法提交变更")

// Store 是元数据的复制日志（契约 05）。
//
// 读写路径严格分离：
//   - 写入经 Apply 走复制日志，可能失败、可能阻塞；
//   - 读取经 View 查本节点内存，不触网、不会失败。
//
// View 的方法签名里没有 ctx 与 error，这个承诺由 CI 的自定义分析器强制
// （PRD ARCH-054）。
type Store interface {
	// Apply 提交一条命令并返回其 revision。
	// 对相同 Command.ID 幂等，重复提交返回首次应用的 revision。
	Apply(ctx context.Context, cmd Command) (Revision, error)

	// View 返回本节点的热视图。返回值在 Store 存续期间始终有效，
	// 调用方可以长期持有。
	View() *View

	// Changes 返回已应用变更的流，供事件总线扇出。
	// 通道由实现关闭。
	Changes() <-chan Change

	Close() error
}

// LocalStore 是 Store 的单副本实现。
//
// 它不是旁路实现：命令校验、幂等去重、revision 推进、变更发布都走与集群
// 一致的语义，只是复制因子为一。按 PRD ARCH-057，单机测试跑的就是这条
// 代码路径，因此集群化时填充的是共识实现而非重构调用方。
type LocalStore struct {
	view *View

	cold     RecordWriter
	compile  Compiler
	applyShd func(string, []byte) error
	// revKey 是 applied index 在冷层里的键。为空表示不持久化它
	// （单机的内存装配与大量用例都是这种）。见 ApplyCommitted 的说明。
	revKey []byte

	mu      sync.Mutex
	rev     Revision
	applied map[string]Revision // 幂等窗口：命令 ID 到首次应用的 revision
	order   []string            // 幂等窗口的淘汰顺序
	window  int
	changes chan Change
	closed  bool
	// readOnly 供测试与演练模拟丢失 quorum 的情形。
	readOnly bool
	// nowUnixMs 是注入的时钟（契约 08）。核心包不直接调用 time.Now。
	nowUnixMs func() int64
	// dropped 是因变更通道满而被丢弃的通知数。
	dropped uint64
	// group 是一副本 Raft 组。校验通过之后先追加日志再改视图。
	// 删掉这一步，提交序号就不再等于视图 revision。
	group *cluster.SingleReplica
}

// RecordWriter 是冷层的写入口。
//
// 只要求两个方法，store.KV 天然满足。本包不 import internal/store：
// 冷层引擎按 ARCH-065 关在那个包里，这里连它的类型都不该见到。
type RecordWriter interface {
	Set(key, value []byte, sync bool) error
	Delete(key []byte, sync bool) error
}

// Compiler 把策略文档编成匹配器。返回值对本包不透明。
//
// 注入而非直接调用 authz：协议层与策略层的依赖方向是单向的，
// meta 认识 authz 就会把「编译器怎么演进」和「状态机怎么应用」绑在一起。
type Compiler func(name string, doc []byte) (any, error)

// LocalOptions 是 LocalStore 的可选参数。
type LocalOptions struct {
	// DedupWindow 是幂等窗口容纳的命令 ID 数量。
	//
	// 真实的 Raft 状态机同样只能保留有界的去重窗口；超出窗口的重复命令
	// 会被再次应用，因此命令本身应尽量设计为幂等（如 SetCert 是覆盖写）。
	DedupWindow int
	// ChangeBuffer 是变更通道的缓冲深度。
	ChangeBuffer int
	// Now 提供当前时间（Unix 毫秒）。为 nil 时视图的确认时间不推进，
	// 调用方须自行调用 MarkConfirmed。
	Now func() int64
	// Cold 是冷层写入口。为 nil 时带冷层记录的命令被拒绝——
	// 静默丢掉它们会让控制面以为写成功了，而记录只在热视图里。
	Cold RecordWriter
	// Compile 编译策略文档。为 nil 时 CmdSetPolicySet 被拒绝。
	Compile Compiler
	// ApplyShadow 把一条影子命令作用到本地影子存储。
	//
	// 为 nil 时带影子载荷的命令被拒绝：静默跳过会让这个节点的影子比
	// 别人少一次更新，而没有任何报错（CLU-082）。
	ApplyShadow func(id string, raw []byte) error
	// RevisionKey 是 applied index 写进冷层时用的键，由装配层给。
	//
	// 本包不认识冷层的前缀方案（ARCH-065：冷层引擎与它的键空间都关在
	// internal/store 内），所以键从外面注入而不是在这里拼。
	// 为空表示不持久化 applied index——那是单机内存装配与用例的形态。
	RevisionKey []byte
	// InitialRevision 是启动时从冷层读回的 applied index。
	//
	// 不给它的后果见 ApplyCommitted：状态机会把已经应用过的日志再应用
	// 一遍。在"库把整条日志推回来"的模型下那只是白做功（写入是幂等的），
	// 而 dragonboat 的 IOnDiskStateMachine 根本不会重放那一段——它按这个
	// 数字决定从哪里接着应用，报 0 就是丢数据。
	InitialRevision Revision
}

// NewLocalStore 创建单副本元数据存储。
func NewLocalStore(opt LocalOptions) *LocalStore {
	if opt.DedupWindow <= 0 {
		opt.DedupWindow = 4096
	}
	if opt.ChangeBuffer <= 0 {
		opt.ChangeBuffer = 256
	}
	s := &LocalStore{
		view:      NewView(),
		applied:   make(map[string]Revision),
		window:    opt.DedupWindow,
		changes:   make(chan Change, opt.ChangeBuffer),
		nowUnixMs: opt.Now,
		group:     cluster.NewSingleReplica(),
		cold:      opt.Cold,
		compile:   opt.Compile,
		applyShd:  opt.ApplyShadow,
		revKey:    opt.RevisionKey,
		rev:       opt.InitialRevision,
	}
	// 视图的 revision 要跟着一起起步：就绪度按它判断本节点是否追平，
	// 而一个刚从冷层恢复完、revision 却报 0 的节点看起来像永远陈旧。
	s.view.rev.Store(uint64(opt.InitialRevision))
	// 单副本永远是追平状态，初始即标记已确认，使节点可以立即接受连接。
	if s.nowUnixMs != nil {
		s.view.confirmedAtUnixMs.Store(s.nowUnixMs())
	}
	return s
}

func (s *LocalStore) View() *View { return s.view }

// Group 返回单机使用的那一副本 Raft 组。
func (s *LocalStore) Group() *cluster.SingleReplica { return s.group }

func (s *LocalStore) Changes() <-chan Change { return s.changes }

// SetReadOnly 模拟丢失写入能力，用于演练与测试 CLU-042 的行为。
func (s *LocalStore) SetReadOnly(ro bool) {
	s.mu.Lock()
	s.readOnly = ro
	s.mu.Unlock()
}

// AppliedRevision 返回某条命令首次应用得到的 revision。
//
// 复制层用它在追平之后认出「刚才提的那条是第几号」。
func (s *LocalStore) AppliedRevision(id string) (Revision, bool) {
	if id == "" {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rev, ok := s.applied[id]
	return rev, ok
}

// MarkConfirmed 记录视图已确认追平。
//
// 单副本下每次 Apply 自动调用；集群下由复制层在确认跟上 leader 的
// commit index 后调用。未调用则视图会被判为陈旧并对新连接 fail-closed
// （PRD CLU-023）。
func (s *LocalStore) MarkConfirmed(nowUnixMs int64) {
	s.view.confirmedAtUnixMs.Store(nowUnixMs)
}

func (s *LocalStore) Apply(_ context.Context, cmd Command) (Revision, error) {
	if err := cmd.Validate(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}
	if s.readOnly {
		return 0, ErrReadOnly
	}
	// 幂等：相同命令 ID 返回首次应用的 revision，不重复改动状态。
	if cmd.ID != "" {
		if rev, ok := s.applied[cmd.ID]; ok {
			return rev, nil
		}
	}
	if cmd.RequestedRevision != 0 && cmd.RequestedRevision != s.rev {
		return 0, ErrRevisionConflict
	}

	// 先写一副本组的日志。只有一个投票者，追加即提交。
	// 视图 revision 用这次提交的序号，两者必须相同。
	idx := s.group.Append()
	s.rev = Revision(idx)
	rev := s.rev
	ch, err := s.apply(rev, cmd)
	s.view.rev.Store(uint64(rev))
	if s.nowUnixMs != nil {
		s.view.confirmedAtUnixMs.Store(s.nowUnixMs())
	}
	s.remember(cmd.ID, rev)
	s.publish(ch)
	if err != nil {
		// 日志已经提交，不能靠返回错误把它撤销（CLU-046）。调用方拿到的是
		// 「这条命令没有完整应用」，补偿要靠新命令。把错误吞掉才是更坏的
		// 选择：那会让视图与日志不一致而无人知道。
		return rev, err
	}
	return rev, nil
}

// errCommittedGap 表示复制层送来的序号不是递增的。
var errCommittedGap = errors.New("meta: 已提交日志的序号没有递增")

// ApplyCommitted 应用一条已经由多数派提交的命令。
//
// rev 是共识层的日志序号。序号不比当前视图新时直接返回：快照重放和
// 重复投递都安全。
//
// 判据是**严格递增**而不是「恰好加一」（LANG-014 修订）。原先要求连续，
// 依据是自写那套实现里每条日志都是一条上层命令；换成 hashicorp/raft 之后
// 不再成立：成员变更与当选 no-op 也占日志序号，而它们不进状态机，于是
// 上层看到的序号天然有空洞。
//
// 放宽之后「少应用了一条命令」由谁来挡？由共识库：它按序、恰好一次地
// 把每条 LogCommand 交给状态机，而本函数返回的错误会被当作致命——
// 应用失败的节点停止接受写入而不是跳过那一条（cluster.ErrApplyFailed）。
// 这与原先那条检查防的是同一件事（CLU-066：解不开的条目不能跳过），
// 只是责任从「调用方自己数序号」挪到了「库保证顺序 + 失败即停」。
func (s *LocalStore) ApplyCommitted(cmd Command, rev Revision) error {
	if err := cmd.Validate(); err != nil {
		return err
	}
	if rev == 0 {
		return errCommittedGap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if rev <= s.rev {
		return nil
	}
	// 这里只能是「比当前新」。相等与更旧的情况上面已经返回了。
	s.rev = rev
	ch, err := s.apply(rev, cmd)
	if err == nil {
		// applied index 落盘，**在记录之后**。
		//
		// 顺序是判据，不是风格（P1）：先记录后 revision 时，崩在中间
		// 留下的是「revision 偏旧」——那一条会被重放一遍，而冷层写入
		// 是覆盖语义，重放一遍的结果与应用一遍相同。反过来写留下的是
		// 「revision 偏新、记录没落盘」，那一条**永远不会被重放**，
		// 表现是重启之后控制面少一条记录，不报错。
		//
		// 每条命令都写，哪怕它没有冷层记录：不写的话 applied index 会
		// 落在后面，日志就压不掉——而压不掉的后果要到快照那一步才显形。
		if werr := s.persistRevision(rev); werr != nil {
			return werr
		}
	}
	s.view.rev.Store(uint64(rev))
	if s.nowUnixMs != nil {
		s.view.confirmedAtUnixMs.Store(s.nowUnixMs())
	}
	s.remember(cmd.ID, rev)
	s.publish(ch)
	return err
}

// persistRevision 把 applied index 写进冷层。
//
// 写失败是致命的：这一条已经被多数派提交并且已经改了本节点的状态，
// 而 applied index 没跟上意味着重启后它会被再应用一遍——在重放模型下
// 无害，在 dragonboat 的 on-disk 模型下则是"状态机比它自己以为的新"，
// 两者对不上。所以这里返回错误，由组按 ErrApplyFailed 停掉本节点的写入。
func (s *LocalStore) persistRevision(rev Revision) error {
	if len(s.revKey) == 0 || s.cold == nil {
		return nil
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(rev))
	if err := s.cold.Set(s.revKey, buf[:], true); err != nil {
		return fmt.Errorf("meta: applied index 落盘失败: %w", err)
	}
	return nil
}

// DecodeRevision 解出持久化的 applied index，供装配层启动时读回。
//
// 放在本包而不是让装配层自己解：编码（8 字节大端）是本包写的，
// 两处各写一遍的结局是只改一份。
func DecodeRevision(raw []byte) Revision {
	if len(raw) != 8 {
		return 0
	}
	return Revision(binary.BigEndian.Uint64(raw))
}

// errNoCompiler 与 errNoColdWriter 表示装配时漏了依赖。
//
// 显式报错而不是跳过：跳过的后果是策略集进了视图却没有匹配器
// （什么都不放行），或者记录只落在热视图里而冷层永远缺一条——
// 两者都没有任何报错。
var (
	errNoCompiler   = errors.New("meta: 未注入策略编译器，无法应用 SetPolicySet")
	errNoColdWriter = errors.New("meta: 未注入冷层写入口，无法应用带记录的命令")
	errNoShadow     = errors.New("meta: 未注入影子应用函数，无法应用影子命令")
)

// apply 把命令作用到热视图与冷层上，并返回对应的变更记录。
// 必须在持锁状态下调用。
//
// 顺序是「先热视图、后冷层」（REG-111）：反过来的那个窗口是不安全的——
// 冷层已经写着「已吊销」而热视图还在放行。先改热视图，最坏只是冷层落后，
// 而安全判定已经生效。
func (s *LocalStore) apply(rev Revision, cmd Command) (Change, error) {
	ch := Change{Revision: rev, Kind: cmd.Kind}
	switch cmd.Kind {
	case CmdNop:
		// 只推进 revision 与确认时间。

	case CmdSetCert:
		c := cmd.Cert
		ch.Fingerprint = c.Fingerprint
		ch.Status = c.Status
		ch.Reason = c.Reason
		ch.Thing = c.Thing
		ch.PrevStatus, ch.PrevThing = s.view.putCert(c)

	case CmdDeleteCert:
		ch.Fingerprint = cmd.Cert.Fingerprint
		ch.PrevStatus = s.view.deleteCert(cmd.Cert.Fingerprint)
		ch.Status = CertUnknown

	case CmdSetThing:
		ch.Thing = cmd.Thing.Name
		ch.PolicySet = cmd.Thing.PolicySet
		s.view.putThing(cmd.Thing)

	case CmdDeleteThing:
		ch.Thing = cmd.Thing.Name
		s.view.deleteThing(cmd.Thing.Name)

	case CmdSetPolicySet:
		if s.compile == nil {
			return ch, errNoCompiler
		}
		compiled, err := s.compile(cmd.Policy.Name, cmd.Policy.Doc)
		if err != nil {
			// 到这一步才编译不过，说明这条命令在别的节点上编译得过
			// （控制面写入前已经校验过一次）——那是真分叉，不是输入错误。
			return ch, fmt.Errorf("meta: 策略 %s 在本节点编译失败: %w", cmd.Policy.Name, err)
		}
		ch.PolicySet = cmd.Policy.ID
		s.view.putPolicySet(rev, cmd.Policy, compiled)

	case CmdDeletePolicySet:
		ch.PolicySet = cmd.Policy.ID
		s.view.deletePolicySet(cmd.Policy.ID)

	case CmdSetAttrSlots:
		s.view.setAttrSlots(cmd.AttrSlots)
	}

	if len(cmd.Shadow) > 0 {
		if s.applyShd == nil {
			return ch, errNoShadow
		}
		if err := s.applyShd(cmd.ID, cmd.Shadow); err != nil {
			return ch, fmt.Errorf("meta: 应用影子命令失败: %w", err)
		}
	}

	if len(cmd.Records) > 0 {
		if s.cold == nil {
			return ch, errNoColdWriter
		}
		for _, op := range cmd.Records {
			// 控制面的写入用 sync：它们是人工操作的结果，
			// 丢失一次吊销的后果远大于一次 fsync 的开销。
			var err error
			if op.Delete {
				err = s.cold.Delete(op.Key, true)
			} else {
				err = s.cold.Set(op.Key, op.Value, true)
			}
			if err != nil {
				return ch, fmt.Errorf("meta: 冷层写入失败: %w", err)
			}
		}
	}
	return ch, nil
}

// remember 把命令 ID 记入幂等窗口，并按 FIFO 淘汰。
func (s *LocalStore) remember(id string, rev Revision) {
	if id == "" {
		return
	}
	s.applied[id] = rev
	s.order = append(s.order, id)
	for len(s.order) > s.window {
		delete(s.applied, s.order[0])
		s.order = s.order[1:]
	}
}

// publish 发布变更。通道满时丢弃最旧的一条并计数，不阻塞 apply 循环——
// 变更已经落到视图上，通知只是用于加速连接清理（PRD OBS-005）。
func (s *LocalStore) publish(ch Change) {
	for {
		select {
		case s.changes <- ch:
			return
		default:
			select {
			case <-s.changes:
				s.dropped++
			default:
				return
			}
		}
	}
}

// Dropped 返回因通道满而被丢弃的变更数，供指标使用。
func (s *LocalStore) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

func (s *LocalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.changes)
	return nil
}

var _ Store = (*LocalStore)(nil)

// Hydrate 用冷层的内容重建热视图（CLU-098）。
//
// 它存在的理由是本仓库此前缺一条路：热视图原先**只能**靠重放复制日志
// 的全部历史来重建（重启时从序号 1 开始 Pump 一遍），于是日志无法截断、
// 重启时间随历史线性增长。换成库管快照之后这条路是必需的——快照装上来
// 的是冷层整库，热视图得从它派生出来。
//
// emit 由调用方驱动（internal/registry 扫冷层，它持有记录的解码器），
// 每条命令只改热视图：命令里**不带 Records**，所以不会把刚装上的那个库
// 原样重写一遍——百万设备下那是几百 MB 的无用写入。
//
// rev 是快照对应的日志序号。它必须被设进视图：CLU-059 判断三个节点是否
// 一致的依据就是 revision 相同，而一个刚装完快照却报 0 的节点看起来
// 像是什么都没追上。
func (s *LocalStore) Hydrate(rev Revision, emit func(apply func(Command) error) error) error {
	if emit == nil {
		return errors.New("meta: 缺少冷层扫描函数")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if s.readOnly {
		return ErrReadOnly
	}

	s.view.reset()
	// 幂等窗口也要清：它记的是"命令 ID 到 revision"，而那些 revision
	// 属于被快照替换掉的那段历史。留着的话一条重试的命令会拿到一个
	// 与当前视图无关的 revision。
	s.applied = make(map[string]Revision, s.window)
	s.order = s.order[:0]

	n := 0
	err := emit(func(cmd Command) error {
		if err := cmd.Validate(); err != nil {
			return err
		}
		if len(cmd.Records) > 0 {
			// 防呆：重建时冷层已经是权威，再写一遍是把整个库重写一遍。
			return fmt.Errorf("meta: 重建热视图的命令不得带冷层记录（%s）", cmd.Kind)
		}
		if _, err := s.apply(rev, cmd); err != nil {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		// 中途失败时视图是半份的，不能让它继续服务：revision 留在 0,
		// 于是就绪度判它陈旧并对新连接 fail-closed（CLU-023）。
		return fmt.Errorf("meta: 重建热视图失败（已处理 %d 条）: %w", n, err)
	}

	s.rev = rev
	s.view.rev.Store(uint64(rev))
	if s.nowUnixMs != nil {
		s.view.confirmedAtUnixMs.Store(s.nowUnixMs())
	}
	return nil
}

// Revision 返回状态机已应用到的日志序号。
func (s *LocalStore) Revision() Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev
}
