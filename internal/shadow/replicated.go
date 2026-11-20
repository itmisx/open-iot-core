package shadow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

// 影子的跨节点一致（契约 06、SHDW-029、CLU-082）。
//
// 选的是「命令进复制日志」而不是「按 thingName 分区 + 转发」。两条路都
// 在 PRD 里有依据，差别在故障语义：
//
// 分区方案要有副本就得**每分区一个 raft 组**（契约 06 说的「分区 leader
// 同时就是 Raft leader」正是这个意思）。做成单副本分区的话，归属节点
// 永久故障就直接丢掉那一部分影子——而 SHDW-070 要求影子持久化、重启后
// 完整恢复，丢数据不在选项里。
//
// 命令进日志则每个节点都有全量影子：读在本地（与契约 05 的 View 同一
// 精神），任一节点挂掉不影响别人。代价是写入吞吐受单个 raft 组限制；
// 真要更高吞吐再叠加分区（那时每个分区各有一个组，本实现仍然是组内的
// 那一层）。
//
// SHDW-029 本来就是为复制状态机写的：命令自带时间戳、apply 时不读时钟，
// 否则各副本会算出不同的 metadata 时间戳，状态机不再确定。

// CommandLog 把一条影子命令提交到复制日志。
//
// 由装配层用 meta.Store 实现。本包不认识 meta：影子的语义与元数据的
// 复制机制是两件事，绑在一起会让「换共识实现」牵动影子。
type CommandLog interface {
	// AppendShadow 提交命令并等它在本节点被应用。
	//
	// id 是本次命令的幂等键，实现须原样交给 apply 回调。
	AppendShadow(ctx context.Context, id string, raw []byte) error
}

// ReplicatedStore 是 Store 的复制实现。
//
// 它不自己算影子：命令走日志，各节点在 apply 阶段用**同一个**本地
// Store（也就是同一份 ApplyCommand 状态机，SHDW-074 要求的那个）
// 算出结果。发起方从 pending 里取回本节点算出的那一份。
type ReplicatedStore struct {
	local Store
	log   CommandLog
	node  string
	seq   atomic.Uint64

	mu sync.Mutex
	// pending 是「谁在等哪条命令的结果」。
	//
	// 只有发起命令的那个节点会在这里登记；别的节点同样应用命令，
	// 但没人等结果，deliver 直接丢掉——那不是错误，是正常情形。
	pending map[string]*outcome
}

// outcome 是一条命令在本节点应用后的产物。
type outcome struct {
	res *Result
	err error
}

// NewReplicatedStore 包住一个本地 Store。
func NewReplicatedStore(local Store, log CommandLog, node string) (*ReplicatedStore, error) {
	switch {
	case local == nil:
		return nil, errors.New("shadow: 缺少本地存储")
	case log == nil:
		return nil, errors.New("shadow: 缺少命令日志")
	case node == "":
		return nil, errors.New("shadow: 缺少节点 ID（命令的幂等键要用它）")
	}
	return &ReplicatedStore{
		local: local, log: log, node: node,
		pending: make(map[string]*outcome),
	}, nil
}

// Get 读本地：每个节点都有全量影子，不必转发。
func (s *ReplicatedStore) Get(thing, shadow string) (*Document, bool) {
	return s.local.Get(thing, shadow)
}

// List 读本地。
func (s *ReplicatedStore) List(thing string) []string { return s.local.List(thing) }

// Apply 把命令提交到复制日志，等它在本节点应用，然后返回结果。
//
// 每次用一个新的幂等键，不复用调用方可能重试的那个：影子的幂等由
// ExpectVersion 提供（乐观并发，SHDW-025），而复用键会让「重试」拿不到
// 结果——状态机对同 ID 的第二次提交不再应用，于是没有结果可取。
func (s *ReplicatedStore) Apply(ctx context.Context, cmd Command) (*Result, error) {
	raw, err := EncodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	id := s.node + "-sh-" + strconv.FormatUint(s.seq.Add(1), 10)

	s.mu.Lock()
	s.pending[id] = nil
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()

	if err := s.log.AppendShadow(ctx, id, raw); err != nil {
		return nil, err
	}

	s.mu.Lock()
	got := s.pending[id]
	s.mu.Unlock()
	if got == nil {
		// 日志说提交并应用了，而本节点没有算出结果：那是真不一致，
		// 报出来而不是返回一个空结果——后者会让设备收到一条
		// 「成功但什么都没变」的响应。
		return nil, fmt.Errorf("shadow: 命令 %s 已提交但本节点没有应用它", id)
	}
	return got.res, got.err
}

// ApplyCommitted 在 apply 阶段把一条已提交的影子命令作用到本地存储。
//
// 由装配层挂到复制层的 apply 回调上，**每个节点都会调用**。
//
// 返回的 error 只表示存储故障。影子的 409 冲突不是故障：给定同样的
// 日志前缀，所有节点都会算出同样的冲突，它是确定性的状态机结果，
// 要交回发起方而不是让 apply 失败——apply 失败会让日志与状态不一致。
func (s *ReplicatedStore) ApplyCommitted(id string, raw []byte) error {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return err
	}
	res, applyErr := s.local.Apply(context.Background(), cmd)

	s.mu.Lock()
	if _, waiting := s.pending[id]; waiting {
		s.pending[id] = &outcome{res: res, err: applyErr}
	}
	s.mu.Unlock()

	// 冲突与校验失败交回发起方，不算 apply 失败
	var e *Error
	if applyErr != nil && !errors.As(applyErr, &e) {
		return applyErr
	}
	return nil
}

// Close 关本地存储。
func (s *ReplicatedStore) Close() error { return s.local.Close() }

// wireCommand 是影子命令的线上表示。
//
// 单独一个类型而不给 Command 加 tag：Patch 是 map[string]any，
// 而影子的数字必须按字面量保留（SHDW-026a——解析为浮点会让 1.50 变成
// 1.5、大整数丢精度，而影子是设备与云之间逐字段比对的契约）。
// 这里用 json.RawMessage 原样搬运那段字节，不经过 any。
type wireCommand struct {
	V             int             `json:"v"`
	Kind          CmdKind         `json:"kind"`
	Thing         string          `json:"thing"`
	Shadow        string          `json:"shadow,omitempty"`
	Patch         json.RawMessage `json:"patch,omitempty"`
	ExpectVersion int64           `json:"expectVersion,omitempty"`
	NowSec        int64           `json:"nowSec"`
	ClientToken   string          `json:"clientToken,omitempty"`
}

// commandWireVersion 是当前线格式版本。
const commandWireVersion = 1

// EncodeCommand 把影子命令编成日志条目。
func EncodeCommand(cmd Command) ([]byte, error) {
	w := wireCommand{
		V: commandWireVersion, Kind: cmd.Kind, Thing: cmd.Thing,
		Shadow: cmd.Shadow, ExpectVersion: cmd.ExpectVersion,
		NowSec: cmd.NowSec, ClientToken: cmd.ClientToken,
	}
	if cmd.Patch != nil {
		// 用本包的 Encode 而不是 json.Marshal：后者会把数字经过 float64，
		// 而影子要求字面量保真（SHDW-026a、SHDW-073）。
		raw, err := Encode(cmd.Patch)
		if err != nil {
			return nil, fmt.Errorf("shadow: 编码补丁失败: %w", err)
		}
		w.Patch = raw
	}
	return json.Marshal(w)
}

// DecodeCommand 把日志条目解回影子命令。
func DecodeCommand(raw []byte) (Command, error) {
	var w wireCommand
	if err := json.Unmarshal(raw, &w); err != nil {
		return Command{}, fmt.Errorf("shadow: 日志条目无法解析: %w", err)
	}
	if w.V != commandWireVersion {
		return Command{}, fmt.Errorf("shadow: 日志条目的格式版本是 %d，本节点只认 %d",
			w.V, commandWireVersion)
	}
	cmd := Command{
		Kind: w.Kind, Thing: w.Thing, Shadow: w.Shadow,
		ExpectVersion: w.ExpectVersion, NowSec: w.NowSec,
		ClientToken: w.ClientToken,
	}
	if len(w.Patch) > 0 {
		obj, err := Decode(w.Patch)
		if err != nil {
			return Command{}, fmt.Errorf("shadow: 补丁无法解析: %w", err)
		}
		cmd.Patch = obj
	}
	return cmd, nil
}

var _ Store = (*ReplicatedStore)(nil)
