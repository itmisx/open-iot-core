package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

// 会话的组内复制（CLU-111 至 CLU-117）。
//
// 这一层解决的是两件在单节点上不存在的事：设备连到另一个节点时会话还
// 在不在，以及节点死掉之后它的离线队列还在不在。v0.3 之前两者的答案都
// 是「不在」——会话与队列只落在接入它的那个节点的盘上。
//
// 做法是把会话的四种写入（save / delete / enqueue / take）提进会话分片
// 的复制日志，由每个节点的状态机各自应用一遍。副本因子等于节点数，
// 所以每个节点都有全量——**读仍然是本地读**，这是契约 05 的硬要求
// （连接路径不得发 RPC），也是选"不分放置"的全部理由。
//
// 刻意不复制的是**在途消息**（Inflight）。理由是写入频率：在途随每条
// QoS 1 消息变化，复制它等于把复制日志的写入频率抬到消息速率；而离线
// 队列只在设备**离线**时增长，那是个小得多的量。代价是节点死掉时该节点
// 上在途的那几条会重传（QoS 1 本来就允许重复，MQTT-033），而不是丢。

const sessionCmdVersion = 1

// 会话命令的操作码。
//
// 数字直接进复制日志，而日志是**跨版本持久**的（快照压不掉旧日志里
// 已有的编码）。所以这些值一经分配不得重用，加新操作只能往后排。
const (
	sessionOpSave    uint8 = 1
	sessionOpDelete  uint8 = 2
	sessionOpEnqueue uint8 = 3
	sessionOpTake    uint8 = 4
	sessionOpExpire  uint8 = 5
)

// 状态机的返回值，由 Propose 交回提案方。
const (
	sessionResOK    uint64 = 0
	sessionResStale uint64 = 1
)

// sessionCmd 是一条会话命令。
//
// 用 JSON 而不是手写二进制：会话状态本身就是 JSON 落盘的
// （sessionRecord），再给命令发明一套编码等于同一个结构有两份编解码，
// 而它们会各自演进。体积上的差别由「这条日志的频率是设备离线时的消息
// 速率，不是全部消息速率」兜住。
type sessionCmd struct {
	V        int                `json:"v"`
	Op       uint8              `json:"op"`
	ClientID string             `json:"c"`
	State    *session.State     `json:"s,omitempty"`
	Msg      *session.QueuedMsg `json:"m,omitempty"`
	Epoch    cluster.Epoch      `json:"e,omitempty"`
	// UpTo 是 take 要删到的序号（含）。
	//
	// take 不是"清空队列"而是"删到第 N 条"：提案方读到的那一批就是
	// 它要补发的那一批，而命令在途期间可能又入队了新的消息。写成清空
	// 会把那几条一起删掉——设备收不到，而且没有任何位置会报错。
	UpTo uint64 `json:"u,omitempty"`
	// NowMs 是 expire 判定用的时刻。
	//
	// 必须进命令：状态机不许看自己的钟（ARCH-033），否则各节点按各自
	// 的时刻判定过期，同一条日志在不同节点上产生不同结果——那是分叉，
	// 而表现是"某个节点上会话莫名消失"。
	NowMs int64 `json:"n,omitempty"`
}

func encodeSessionCmd(c *sessionCmd) ([]byte, error) {
	c.V = sessionCmdVersion
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("store: 编码会话命令失败: %w", err)
	}
	return b, nil
}

func decodeSessionCmd(raw []byte) (*sessionCmd, error) {
	var c sessionCmd
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("store: 会话命令损坏: %w", err)
	}
	if c.V != sessionCmdVersion {
		return nil, fmt.Errorf("store: 会话命令版本 %d 不支持", c.V)
	}
	return &c, nil
}

// SessionAppliedKey 是某个会话分片的 applied index 键。
//
// 一个分片一个键：分片是独立的日志，序号互不相干。合用一个键会让任一
// 分片的进度覆盖掉其余分片的（CLU-109 的那条约束对每个分片各成立一次）。
func SessionAppliedKey(shard uint64) []byte {
	return Key(PrefixSystem, fmt.Sprintf("session-applied-%d", shard))
}

// SessionAppliedIndex 读出某个分片已持久化到哪一条。
func (s *SessionStore) SessionAppliedIndex(shard uint64) uint64 {
	raw, err := s.kv.Get(SessionAppliedKey(shard))
	if err != nil || len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func appliedIndexOp(shard, index uint64) Op {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], index)
	return Op{Kind: OpSet, Key: SessionAppliedKey(shard), Value: b[:]}
}

// ApplySessionCmd 应用一条已提交的会话命令。
//
// 状态与 applied index 进**同一个 Batch**（CLU-109）：分两次写会留出一个
// 窗口，在那里崩溃之后 Open 报的位置与盘上实际的状态不符——报大了就是
// 跳过日志，而 dragonboat 不会重放已被快照压掉的那些，于是丢数据。
//
// sync 传 false 不是放松持久性：bbolt 的 Batch 仍然在返回前提交并 fsync，
// 只是把多个并发调用合进同一个事务（见 boltKV.write）。会话写入落在
// CONNECT 与发布路径上，逐条 fsync 时它就是建连速率的天花板（MQTT-118）。
func (s *SessionStore) ApplySessionCmd(shard, index uint64, data []byte,
	owns func(string) bool,
) (uint64, error) {
	cmd, err := decodeSessionCmd(data)
	if err != nil {
		// 解不开的命令是致命的：它已被多数派提交，别的节点会应用它，
		// 本节点跳过就是状态机分叉。返回错误让 dragonboat 停掉本节点。
		return 0, err
	}
	lane := s.lane(cmd.ClientID)
	lane.Lock()
	defer lane.Unlock()

	ops, res, err := s.sessionCmdOps(cmd, owns)
	if err != nil {
		return 0, err
	}
	ops = append(ops, appliedIndexOp(shard, index))
	if err := s.kv.Batch(ops, false); err != nil {
		return 0, err
	}
	return res, nil
}

// sessionCmdOps 把一条命令翻成一组写入，不自己落盘。
//
// 拆出来是为了让 applied index 能和它们进同一个 Batch。所有分支都只
// **读**当前状态再算出写入——于是同一条命令在任何节点上算出的是同一组
// 写入，这正是复制日志要求的确定性。
func (s *SessionStore) sessionCmdOps(cmd *sessionCmd, owns func(string) bool,
) ([]Op, uint64, error) {
	switch cmd.Op {
	case sessionOpSave:
		return s.saveOps(cmd)
	case sessionOpDelete:
		return s.deleteOps(cmd)
	case sessionOpEnqueue:
		return s.enqueueOps(cmd)
	case sessionOpTake:
		return s.takeOps(cmd)
	case sessionOpExpire:
		return s.expireOps(cmd, owns)
	default:
		return nil, 0, fmt.Errorf("store: 未知的会话操作码 %d", cmd.Op)
	}
}

func (s *SessionStore) saveOps(cmd *sessionCmd) ([]Op, uint64, error) {
	st := cmd.State
	if st == nil || st.ClientID == "" {
		return nil, 0, errors.New("store: save 命令缺少会话状态")
	}
	cur, found, err := s.peek(st.ClientID)
	if err != nil {
		return nil, 0, err
	}
	// 契约 01 的围栏判定就在这里，而不在提案之前。
	//
	// 提交之前判等于在提案方那个节点上判，而它看到的当前 epoch 可能比
	// 别的节点旧——两个节点并发接管同一个 clientId 时，旧 epoch 的那条
	// 写入就可能后落地。放在 apply 里判，看到的是日志序，判定因此唯一。
	if found && st.Epoch < cur.Epoch {
		return nil, sessionResStale, nil
	}
	bare := *st
	bare.Queued = nil
	enc, eerr := encodeSession(&bare)
	if eerr != nil {
		return nil, 0, eerr
	}
	ops := []Op{{Kind: OpSet, Key: sessionKey(st.ClientID), Value: enc}}
	// 队列只在传进来的那一份非 nil 时对账（与单机的 Save 同一条约定）。
	if st.Queued != nil {
		clear, cerr := s.queuedKeys(st.ClientID)
		if cerr != nil {
			return nil, 0, cerr
		}
		for _, k := range clear {
			ops = append(ops, Op{Kind: OpDelete, Key: k})
		}
		for i, m := range st.Queued {
			// 序号从 1 起重排：这一份就是队列的全部内容，没有"接着
			// 原来的序号往后排"可言。
			enc, merr := json.Marshal(&m)
			if merr != nil {
				return nil, 0, fmt.Errorf("store: 编码离线消息失败: %w", merr)
			}
			ops = append(ops, Op{
				Kind: OpSet, Key: queuedKey(st.ClientID, uint64(i)+1), Value: enc,
			})
		}
	}
	return ops, sessionResOK, nil
}

func (s *SessionStore) deleteOps(cmd *sessionCmd) ([]Op, uint64, error) {
	cur, found, err := s.peek(cmd.ClientID)
	if err != nil {
		return nil, 0, err
	}
	if !found {
		return nil, sessionResOK, nil
	}
	if cur != nil && cur.Epoch != cmd.Epoch {
		// 相等而非小于：删除是破坏性操作，只有持有当前 epoch 的那个
		// 连接有权删（与单机的 Delete 同一条）。
		return nil, sessionResStale, nil
	}
	keys, kerr := s.queuedKeys(cmd.ClientID)
	if kerr != nil {
		return nil, 0, kerr
	}
	ops := make([]Op, 0, len(keys)+1)
	for _, k := range keys {
		ops = append(ops, Op{Kind: OpDelete, Key: k})
	}
	return append(ops, Op{Kind: OpDelete, Key: sessionKey(cmd.ClientID)}), sessionResOK, nil
}

func (s *SessionStore) enqueueOps(cmd *sessionCmd) ([]Op, uint64, error) {
	if cmd.Msg == nil {
		return nil, 0, errors.New("store: enqueue 命令缺少消息")
	}
	// 序号在**应用时**由当前状态推出，不由提案方带过来。
	//
	// 这一步是确定性的来源：各节点按同一个日志序应用，于是算出同一个
	// 序号。换成提案方分配，两个节点并发入队就会撞号——而撞号的表现是
	// 一条消息静默覆盖另一条（键相同），不是报错。
	last, err := s.lastQueuedSeq(cmd.ClientID)
	if err != nil {
		return nil, 0, err
	}
	enc, merr := json.Marshal(cmd.Msg)
	if merr != nil {
		return nil, 0, fmt.Errorf("store: 编码离线消息失败: %w", merr)
	}
	return []Op{{
		Kind: OpSet, Key: queuedKey(cmd.ClientID, last+1), Value: enc,
	}}, sessionResOK, nil
}

func (s *SessionStore) takeOps(cmd *sessionCmd) ([]Op, uint64, error) {
	prefix := queuedPrefix(cmd.ClientID)
	var ops []Op
	err := s.kv.Scan(prefix, func(key, _ []byte) bool {
		if len(key) != len(prefix)+8 {
			return true
		}
		if binary.BigEndian.Uint64(key[len(prefix):]) > cmd.UpTo {
			// 升序扫描，后面的序号只会更大，可以直接停。
			return false
		}
		ops = append(ops, Op{Kind: OpDelete, Key: append([]byte(nil), key...)})
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	return ops, sessionResOK, nil
}

// expireOps 只清理**本分片拥有**的会话。
//
// 过期清理是全库扫描，它没有 clientId 可言——但它删出来的键必须落在
// 本分片的日志里，否则快照会把它们复活：另一个分片的快照含着这些键，
// 而它那条日志从没删过它们。所以同一轮清理在每个分片上各提一条命令，
// 各自只删自己那一批（CLU-114）。
func (s *SessionStore) expireOps(cmd *sessionCmd, owns func(string) bool,
) ([]Op, uint64, error) {
	if owns == nil {
		return nil, 0, errors.New("store: expire 命令缺少分片归属判定")
	}
	var ops []Op
	var ids []string
	err := s.kv.Scan([]byte{byte(PrefixSession)}, func(key, value []byte) bool {
		st, derr := decodeSession(value)
		if derr != nil {
			if parts, ok := SplitKey(key, PrefixSession, 1); ok && owns(parts[0]) {
				ids = append(ids, parts[0])
			}
			return true
		}
		if st.Expired(cmd.NowMs) && owns(st.ClientID) {
			ids = append(ids, st.ClientID)
		}
		return true
	})
	if err != nil {
		return nil, 0, err
	}
	for _, id := range ids {
		ops = append(ops, Op{Kind: OpDelete, Key: sessionKey(id)})
		keys, kerr := s.queuedKeys(id)
		if kerr != nil {
			return nil, 0, kerr
		}
		for _, k := range keys {
			ops = append(ops, Op{Kind: OpDelete, Key: k})
		}
	}
	return ops, uint64(len(ids)), nil
}

// peek 读出会话当前状态（不含队列），供围栏判定使用。
func (s *SessionStore) peek(clientID string) (*session.State, bool, error) {
	raw, err := s.kv.Get(sessionKey(clientID))
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	st, derr := decodeSession(raw)
	if derr != nil {
		// 记录损坏时按"不存在"处理，于是允许覆盖——与单机的 Save
		// 同一条理由：一个解不开的记录会让该 clientId 永久无法连接。
		return nil, false, nil
	}
	return st, true, nil
}

func (s *SessionStore) queuedKeys(clientID string) ([][]byte, error) {
	var keys [][]byte
	err := s.kv.Scan(queuedPrefix(clientID), func(key, _ []byte) bool {
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	return keys, err
}

func (s *SessionStore) lastQueuedSeq(clientID string) (uint64, error) {
	var last uint64
	prefix := queuedPrefix(clientID)
	err := s.kv.Scan(prefix, func(key, _ []byte) bool {
		if len(key) == len(prefix)+8 {
			last = binary.BigEndian.Uint64(key[len(prefix):])
		}
		return true
	})
	return last, err
}

// PeekQueued 读出离线队列与其中最大的序号，不删除。
//
// TakeQueued 的复制版要先读再提案（见 ReplicatedSessionStore.TakeQueued），
// 这个方法就是那一步。返回的序号用来限定命令删到哪里。
func (s *SessionStore) PeekQueued(clientID string) ([]session.QueuedMsg, uint64, error) {
	lane := s.lane(clientID)
	lane.Lock()
	defer lane.Unlock()
	msgs, err := s.loadQueued(clientID)
	if err != nil {
		return nil, 0, err
	}
	if len(msgs) == 0 {
		return nil, 0, nil
	}
	last, lerr := s.lastQueuedSeq(clientID)
	if lerr != nil {
		return nil, 0, lerr
	}
	return msgs, last, nil
}

// --- 快照 ---

// SnapshotSessionShard 把属于某个分片的全部记录写进 w。
//
// 形如「applied index 头 + 键值流」。按分片过滤是必须的：一个分片的
// 快照被用来追平**那个分片**的落后副本，带上别的分片的数据会让接收侧
// 把那些键按本分片的日志覆盖一遍——而它们的权威在另一条日志上。
func (s *SessionStore) SnapshotSessionShard(shard uint64, owns func(string) bool,
	w io.Writer,
) error {
	var head [8]byte
	binary.BigEndian.PutUint64(head[:], s.SessionAppliedIndex(shard))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	for _, prefix := range []Prefix{PrefixSession, PrefixQueued} {
		err := s.kv.Scan([]byte{byte(prefix)}, func(key, value []byte) bool {
			if !owns(clientOfKey(key, prefix)) {
				return true
			}
			return writeKV(w, key, value) == nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// RestoreSessionShard 用快照替换本分片的记录。
//
// 先删后写，而不是直接覆盖：快照是那个分片的**全部**内容，本地多出来
// 的键（在快照之后被删掉的会话）必须跟着消失。只覆盖的话它们会留下，
// 表现是"已经删掉的会话在落后过的节点上又活了"。
func (s *SessionStore) RestoreSessionShard(shard uint64, owns func(string) bool,
	r io.Reader,
) error {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return fmt.Errorf("store: 会话快照缺少头部: %w", err)
	}
	var ops []Op
	for _, prefix := range []Prefix{PrefixSession, PrefixQueued} {
		err := s.kv.Scan([]byte{byte(prefix)}, func(key, _ []byte) bool {
			if owns(clientOfKey(key, prefix)) {
				ops = append(ops, Op{Kind: OpDelete, Key: append([]byte(nil), key...)})
			}
			return true
		})
		if err != nil {
			return err
		}
	}
	for {
		key, value, err := readKV(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		ops = append(ops, Op{Kind: OpSet, Key: key, Value: value})
	}
	// applied index 与数据同一个 Batch，同 ApplySessionCmd 的理由。
	ops = append(ops, Op{
		Kind: OpSet, Key: SessionAppliedKey(shard), Value: head[:],
	})
	return s.kv.Batch(ops, true)
}

// clientOfKey 从一条记录的键里取出 clientId。
//
// 两种前缀的键形状不同：会话是 `c<clientID>`，队列是
// `Q<clientID>\x00<序号>`。取不出来时回空串——调用方的 owns 对空串
// 返回假，于是这条记录既不进快照也不被删，留在原地。那比"猜一个分片"
// 安全：猜错会让它在两个分片的快照里各出现一次。
func clientOfKey(key []byte, prefix Prefix) string {
	if len(key) < 1 || key[0] != byte(prefix) {
		return ""
	}
	body := key[1:]
	if prefix == PrefixQueued {
		i := bytes.IndexByte(body, KeySep)
		if i < 0 {
			return ""
		}
		return string(body[:i])
	}
	return string(body)
}

func writeKV(w io.Writer, key, value []byte) error {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(value)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func readKV(r io.Reader) ([]byte, []byte, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, io.EOF
		}
		return nil, nil, fmt.Errorf("store: 会话快照截断: %w", err)
	}
	kl := binary.BigEndian.Uint32(hdr[0:4])
	vl := binary.BigEndian.Uint32(hdr[4:8])
	// 上限是防御性的：快照来自对端，一个坏掉的长度会让这里申请几 GB。
	if kl > 1<<20 || vl > 1<<26 {
		return nil, nil, fmt.Errorf("store: 会话快照长度异常 %d/%d", kl, vl)
	}
	key := make([]byte, kl)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, nil, fmt.Errorf("store: 会话快照截断: %w", err)
	}
	value := make([]byte, vl)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, nil, fmt.Errorf("store: 会话快照截断: %w", err)
	}
	return key, value, nil
}

// --- 复制包装 ---

// SessionProposer 是会话分片组在本包这一侧的样子。
//
// 只声明用得到的两个方法，而不是直接依赖 cluster.ShardGroup：本包是
// 存储层，它不该知道共识库的存在（LANG-015 把共识库关在 cluster 与
// raftstore 两个包里）。
type SessionProposer interface {
	// Propose 提交一条命令到 clientId 所属的分片，返回状态机的判定值。
	Propose(clientID string, data []byte) (uint64, error)
	// ProposeShard 提交一条命令到指定分片。
	ProposeShard(shard uint64, data []byte) (uint64, error)
	// ShardOf 回报某个 clientId 所属的分片号。
	ShardOf(clientID string) uint64
	// ShardIDs 返回全部分片号。
	ShardIDs() []uint64
}

// ReplicatedSessionStore 是会话的集群实现：写入走复制日志，读取走本地。
//
// 读不经过共识是刻意的（契约 05）：CONNECT 时要读会话，而那条路径上
// 不许发 RPC。能这么做是因为副本因子等于节点数——每个节点都有全量。
// 代价是跨节点的生效窗口等于一次复制往返，不是零：设备从 A 断开立刻
// 连到 B，B 有可能还没应用 A 的最后一次 Save。契约 01 的 epoch 围栏
// 保证的是"旧的不覆盖新的"，不是"新的立刻看得见旧的"。
type ReplicatedSessionStore struct {
	local *SessionStore
	prop  SessionProposer
}

// NewReplicatedSessionStore 包装一个本地会话存储。
func NewReplicatedSessionStore(local *SessionStore, prop SessionProposer,
) *ReplicatedSessionStore {
	return &ReplicatedSessionStore{local: local, prop: prop}
}

var _ session.Store = (*ReplicatedSessionStore)(nil)

// Local 返回底层的本地存储，供状态机与启动期统计使用。
func (r *ReplicatedSessionStore) Local() *SessionStore { return r.local }

// Load 从本地读。
func (r *ReplicatedSessionStore) Load(ctx context.Context, clientID string,
) (*session.State, bool, error) {
	return r.local.Load(ctx, clientID)
}

// Save 把会话状态提进复制日志。
func (r *ReplicatedSessionStore) Save(_ context.Context, st *session.State) error {
	if st == nil || st.ClientID == "" {
		return errors.New("store: 会话缺少 clientId")
	}
	data, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpSave, ClientID: st.ClientID, State: st,
	})
	if err != nil {
		return err
	}
	return r.propose(st.ClientID, data)
}

// Delete 把删除提进复制日志。
func (r *ReplicatedSessionStore) Delete(_ context.Context, clientID string,
	e cluster.Epoch,
) error {
	data, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpDelete, ClientID: clientID, Epoch: e,
	})
	if err != nil {
		return err
	}
	return r.propose(clientID, data)
}

// Expire 把过期清理按分片各提一条进复制日志。
//
// 每个分片各一条，而不是一条扫全库（CLU-114，理由见 expireOps）。
//
// 每个节点的清理协程都会提这一轮，于是同一轮在集群里有「节点数乘分片
// 数」条命令。没有去重是刻意的：去重要么要选一个节点来做（那就得知道
// 谁是 leader，而分片的 leader 各不相同），要么要一把分布式锁。重复的
// 代价很小——后到的命令扫到的已经是清理后的状态，算出零个写入。
//
// 返回的条数是本节点这一轮各分片清掉的总数。中途失败时返回已清掉的
// 条数与那个错误：清理是幂等的，下一轮会把剩下的补上。
func (r *ReplicatedSessionStore) Expire(_ context.Context, nowUnixMs int64) (int, error) {
	total := 0
	for _, shard := range r.prop.ShardIDs() {
		data, err := encodeSessionCmd(&sessionCmd{
			Op: sessionOpExpire, NowMs: nowUnixMs,
		})
		if err != nil {
			return total, err
		}
		n, perr := r.prop.ProposeShard(shard, data)
		if perr != nil {
			return total, perr
		}
		total += int(n)
	}
	return total, nil
}

// AppendQueued 把入队提进复制日志。
func (r *ReplicatedSessionStore) AppendQueued(clientID string, m session.QueuedMsg) error {
	data, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpEnqueue, ClientID: clientID, Msg: &m,
	})
	if err != nil {
		return err
	}
	return r.propose(clientID, data)
}

// TakeQueued 读出离线队列，再把"删到第 N 条"提进复制日志。
//
// 先读后提案，而不是让状态机把取出的消息回给提案方：状态机的返回值
// 只有一个 uint64，而且把整批消息塞进去等于让它们再过一遍复制层。
// 本地已经有全量（副本因子等于节点数），直接读即可。
//
// 顺序是"读、提案、返回"。提案失败时不返回任何消息——队列仍在盘上，
// 下次重连再补发。反过来（先返回再提案）会在提案失败时把消息投出去
// 却没删，下次重连重投一遍。
func (r *ReplicatedSessionStore) TakeQueued(clientID string) ([]session.QueuedMsg, error) {
	msgs, upTo, err := r.local.PeekQueued(clientID)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	data, eerr := encodeSessionCmd(&sessionCmd{
		Op: sessionOpTake, ClientID: clientID, UpTo: upTo,
	})
	if eerr != nil {
		return nil, eerr
	}
	if perr := r.propose(clientID, data); perr != nil {
		return nil, perr
	}
	return msgs, nil
}

func (r *ReplicatedSessionStore) propose(clientID string, data []byte) error {
	res, err := r.prop.Propose(clientID, data)
	if err != nil {
		return err
	}
	if res == sessionResStale {
		return cluster.ErrStaleEpoch
	}
	return nil
}

// Count 返回本地已持久化的会话数。
func (r *ReplicatedSessionStore) Count() int { return r.local.Count() }
