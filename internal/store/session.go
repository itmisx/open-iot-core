package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

const sessionRecordVersion = 1

// sessionRecord 是会话的持久化编码。
//
// 直接内嵌 session.State 而不是逐字段抄一遍：State 是契约 04 规定的
// 纯数据结构（没有指针、没有接口，由 contractcheck 保证），因此它
// 可以被整体序列化。逐字段抄的版本会在 State 新增字段时静默丢数据——
// 那种 bug 的表现是"重启后某个属性没了"，极难追。
type sessionRecord struct {
	V     int            `json:"v"`
	State *session.State `json:"state"`
}

// SessionStore 是 session.Store 的持久化实现。
type SessionStore struct {
	kv KV

	// epochTies 是「epoch 相等但归属节点不同」的写入次数（CLU-131）。
	//
	// 它是这条路径唯一的外部信号：epoch 由各节点的本地所有权表发放，
	// term 恒为 1、序号各自递增，所以两个节点可以为同一个 clientId 发出
	// **相同**的 epoch——而相等的 epoch 之间没有先后，围栏那条
	// `st.Epoch < cur.Epoch` 判不出来。非零意味着真的撞上了，而不是
	// 理论上会撞。
	epochTies atomic.Uint64

	// 同 clientId 的操作落到同一条带锁。
	//
	// 围栏判定是「读当前 epoch、比较、写入」三步，必须原子：
	// 两个并发的 Save 若交错，旧 epoch 的那个可能在新 epoch 之后落地，
	// 于是被接管的旧会话覆盖掉了新会话的状态。
	// 不能永久保存 map[clientId]*Mutex：百万持久会话会留下百万个 GC
	// 对象。1024 条带足以覆盖本地并行 apply，常驻成本与设备数无关。
	lanes [1024]sync.Mutex
}

// staleAtEqualEpoch 判断一条写入是否该在 epoch 相等时被拒（CLU-131）。
//
// 相等只可能来自两个节点各自发放的 epoch 撞号（同一个节点的序号严格
// 递增，不会重复）。两者之间没有可比的先后，所以不能按 epoch 定序；
// 但有一条判据是确定的：**已经断开的那一份不得覆盖还连着的那一份**。
// 断开时的落盘一定带 DisconnectUnixMs，而 CONNECT 时落的那份是 0。
//
// 为什么不是"一律拒绝"或"一律接受"：撞号时哪一份更新无从得知，两种
// 一刀切都有一半概率错；而"活的压过死的"这条与哪一份更新无关，它在
// 两种情形下都是对的——设备此刻确实连在写 0 那一份的节点上。
//
// 为什么不加节点名做字典序平手判据（retained 那条路的做法，MQTT-123）：
// retained 要的是**收敛**，谁赢不重要；会话要的是"别把活着的那份盖掉"，
// 按节点名定序会让某个节点永久输——那个节点上的设备从此存不住会话。
//
// 计数但不报警：撞号本身是所有权实现的缺陷（LocalOwnership 的 term 恒为
// 1），真正的修法是分片 ownership 服务，见 CLU-131。
func (s *SessionStore) staleAtEqualEpoch(cur, in *session.State) bool {
	if cur.Epoch != in.Epoch || cur.Owner == in.Owner {
		return false
	}
	s.epochTies.Add(1)
	return in.DisconnectUnixMs != 0 && cur.DisconnectUnixMs == 0
}

// EpochTies 返回「epoch 相等但归属节点不同」的写入次数（CLU-131）。
func (s *SessionStore) EpochTies() uint64 { return s.epochTies.Load() }

// NewSessionStore 创建持久化会话存储。
func NewSessionStore(kv KV) *SessionStore {
	return &SessionStore{kv: kv}
}

func (s *SessionStore) lane(clientID string) *sync.Mutex {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(clientID); i++ {
		h ^= uint64(clientID[i])
		h *= 1099511628211
	}
	return &s.lanes[h&uint64(len(s.lanes)-1)]
}

func sessionKey(clientID string) []byte {
	return Key(PrefixSession, clientID)
}

// queuedPrefix 是某个会话的离线队列前缀。
func queuedPrefix(clientID string) []byte {
	return append(Key(PrefixQueued, clientID), KeySep)
}

// queuedKey 是一条离线消息的键：前缀加 8 字节大端序号。
//
// 序号定序，而且**按字节序就是数值序**——用十进制字符串的话 "10" 会排在
// "9" 前面，于是补发的顺序与入队顺序不符，而 QoS 1 的使用者通常按顺序
// 处理（MQTT-127 也是这个理由）。
func queuedKey(clientID string, seq uint64) []byte {
	k := queuedPrefix(clientID)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	return append(k, b[:]...)
}

// Load 取出会话状态，并把离线队列装回去。
//
// 队列单独存（PrefixQueued）：入队要是一次追加，而不是把整份会话重写。
// 读的时候再拼回 State.Queued，于是上层看到的还是那个整块结构——
// session.State 是纯数据这件事不变（契约 04）。
func (s *SessionStore) Load(_ context.Context, clientID string) (*session.State, bool, error) {
	raw, err := s.kv.Get(sessionKey(clientID))
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	st, derr := decodeSession(raw)
	if derr != nil {
		return nil, false, fmt.Errorf("store: 会话记录损坏 %s: %w", clientID, derr)
	}
	queued, qerr := s.loadQueued(clientID)
	if qerr != nil {
		return nil, false, qerr
	}
	st.Queued = queued
	return st, true, nil
}

// loadQueued 按序号升序读出离线队列。
func (s *SessionStore) loadQueued(clientID string) ([]session.QueuedMsg, error) {
	var out []session.QueuedMsg
	var derr error
	err := s.kv.Scan(queuedPrefix(clientID), func(_, value []byte) bool {
		var m session.QueuedMsg
		if jerr := json.Unmarshal(value, &m); jerr != nil {
			derr = jerr
			return false
		}
		out = append(out, m)
		return true
	})
	if err != nil {
		return nil, err
	}
	if derr != nil {
		return nil, fmt.Errorf("store: 离线队列记录损坏 %s: %w", clientID, derr)
	}
	return out, nil
}

// AppendQueued 往离线队列尾部追加一条消息。
//
// 一次小写入，与会话状态无关——这正是把队列拆出来的目的。序号取"当前
// 最大加一"：扫前缀的最后一个键。扫一次的代价远小于重写整份会话，而且
// 它只发生在设备离线时。
func (s *SessionStore) AppendQueued(clientID string, m session.QueuedMsg) error {
	lane := s.lane(clientID)
	lane.Lock()
	defer lane.Unlock()
	_, err := s.appendQueuedLocked(clientID, m)
	return err
}

func (s *SessionStore) appendQueuedLocked(clientID string, m session.QueuedMsg) (uint64, error) {
	var last uint64
	prefix := queuedPrefix(clientID)
	if err := s.kv.Scan(prefix, func(key, _ []byte) bool {
		if len(key) == len(prefix)+8 {
			last = binary.BigEndian.Uint64(key[len(prefix):])
		}
		return true
	}); err != nil {
		return 0, err
	}
	enc, err := json.Marshal(&m)
	if err != nil {
		return 0, fmt.Errorf("store: 编码离线消息失败: %w", err)
	}
	seq := last + 1
	// 与会话状态同一个理由走合并提交：它在发布路径上。
	if err := s.kv.Set(queuedKey(clientID, seq), enc, false); err != nil {
		return 0, err
	}
	return seq, nil
}

// TakeQueued 取出并清空离线队列。
//
// 重连时调用（flushQueued）。取与删在同一把 lane 锁下：两个并发的重连
// 否则会各自取到同一批消息，于是同一条被投两遍。
func (s *SessionStore) TakeQueued(clientID string) ([]session.QueuedMsg, error) {
	lane := s.lane(clientID)
	lane.Lock()
	defer lane.Unlock()
	msgs, err := s.loadQueued(clientID)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return msgs, s.clearQueuedLocked(clientID)
}

func (s *SessionStore) clearQueuedLocked(clientID string) error {
	var keys [][]byte
	if err := s.kv.Scan(queuedPrefix(clientID), func(key, _ []byte) bool {
		keys = append(keys, append([]byte(nil), key...))
		return true
	}); err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.kv.Delete(k, false); err != nil {
			return err
		}
	}
	return nil
}

// Save 保存会话状态，拒绝来自已被接管会话的写入。
func (s *SessionStore) Save(_ context.Context, st *session.State) error {
	if st == nil || st.ClientID == "" {
		return errors.New("store: 会话缺少 clientId")
	}
	lane := s.lane(st.ClientID)
	lane.Lock()
	defer lane.Unlock()

	key := sessionKey(st.ClientID)
	raw, err := s.kv.Get(key)
	switch {
	case err == nil:
		cur, derr := decodeSession(raw)
		if derr != nil {
			// 记录损坏时允许覆盖：会话状态可以从新连接重建，
			// 而一个无法解析的记录会让该 clientId 永久无法连接。
			// 与影子不同——影子是设备数据，覆盖它等于丢数据。
			break
		}
		// 契约 01：已被接管的旧会话不得覆盖新会话的状态
		if st.Epoch < cur.Epoch {
			return cluster.ErrStaleEpoch
		}
		if s.staleAtEqualEpoch(cur, st) {
			return cluster.ErrStaleEpoch
		}
	case errors.Is(err, ErrNotFound):
	default:
		return err
	}

	// 队列不进这条记录：它单独存（见 Load 的说明）。拷一份再清空，
	// 免得改到调用方那个结构——State 是纯数据，调用方可能还在用它。
	bare := *st
	bare.Queued = nil
	enc, err := encodeSession(&bare)
	if err != nil {
		return err
	}
	// 队列按**传进来的那一份**对账，但只在它非 nil 时。
	//
	// nil 与空切片的区别在这里是有意义的（与 mesh 的通告同一条约定）：
	// nil 表示"这次不碰队列"，空切片表示"队列应当是空的"。少了这个区别，
	// 任何一个只想改订阅的 Save 都会把离线队列静默清掉——而那条路径
	// （Load 改字段再 Save）在 broker 里到处都是。
	if st.Queued != nil {
		if err := s.clearQueuedLocked(st.ClientID); err != nil {
			return err
		}
		for _, m := range st.Queued {
			if _, aerr := s.appendQueuedLocked(st.ClientID, m); aerr != nil {
				return aerr
			}
		}
	}
	// 落盘走合并提交（MQTT-118）。
	//
	// 持久性没有降低：Batch 仍在返回前提交并 fsync，只是把多个并发调用
	// 合进同一个事务（见 boltKV.write 的说明）。会话状态丢失意味着持久
	// 会话的订阅与离线队列消失，而客户端重连时会以 CleanStart=false
	// 期待它们仍在——所以不能走全局 NoSync，那才是真有丢失窗口的那种。
	//
	// 改成合并是因为这条写入在 **CONNECT 路径上**：逐条 fsync 时它就是
	// 建连速率的天花板。本机实测（darwin/arm64、Apple SSD）逐条 Update
	// 串行 191 次/秒、512 并发 328 次/秒，而 Batch 在 512 并发下是
	// 27268 次/秒。差的那两个数量级决定了「百万设备的重连风暴能不能
	// 收敛」，而 NFR-033 要求重启后 30 秒内恢复接受连接。
	//
	// 代价是单条写入的延迟从一次 fsync 变成最多一个 MaxBatchDelay
	// （bbolt 默认 10 ms）：只有一个设备在连的时候，它多等那 10 ms。
	//
	// 注意 Batch 在重试时会**再次调用**事务体，因此事务体必须幂等。
	// 这里是一次 Put，幂等；围栏判定（读当前 epoch 再比较）刻意留在事务
	// 之外、由 lane 锁保护——把它挪进事务体，重试就会拿到不同的判定。
	return s.kv.Set(key, enc, false)
}

// Replace atomically clears the old session (including its offline queue) and
// stores the new state. Clean Start with a non-zero expiry needs this semantic;
// doing Delete followed by Save would require two replicated proposals.
func (s *SessionStore) Replace(_ context.Context, st *session.State) error {
	if st == nil || st.ClientID == "" {
		return errors.New("store: 会话缺少 clientId")
	}
	lane := s.lane(st.ClientID)
	lane.Lock()
	defer lane.Unlock()
	cur, found, err := s.peek(s.kv, st.ClientID)
	if err != nil {
		return err
	}
	if found && st.Epoch < cur.Epoch {
		return cluster.ErrStaleEpoch
	}
	if found && s.staleAtEqualEpoch(cur, st) {
		return cluster.ErrStaleEpoch
	}
	ops, err := s.replaceStateOps(s.kv, st)
	if err != nil {
		return err
	}
	return s.kv.Batch(ops, false)
}

func (s *SessionStore) replaceStateOps(r kvReader, st *session.State) ([]Op, error) {
	keys, err := s.queuedKeys(r, st.ClientID)
	if err != nil {
		return nil, err
	}
	bare := *st
	bare.Queued = nil
	enc, err := encodeSession(&bare)
	if err != nil {
		return nil, err
	}
	ops := make([]Op, 0, len(keys)+1)
	for _, k := range keys {
		ops = append(ops, Op{Kind: OpDelete, Key: k})
	}
	ops = append(ops, Op{Kind: OpSet, Key: sessionKey(st.ClientID), Value: enc})
	return ops, nil
}

// Delete 删除会话状态。
func (s *SessionStore) Delete(_ context.Context, clientID string, e cluster.Epoch) error {
	lane := s.lane(clientID)
	lane.Lock()
	defer lane.Unlock()

	key := sessionKey(clientID)
	raw, err := s.kv.Get(key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	cur, derr := decodeSession(raw)
	if derr == nil && cur.Epoch != e {
		// 围栏用相等而非小于：删除是破坏性操作，
		// 只有持有当前 epoch 的那个连接才有权删。
		return cluster.ErrStaleEpoch
	}
	// 队列也要一起删：它单独存（PrefixQueued），不删就成了孤儿记录——
	// 占着盘，而且同一个 clientId 下次建新会话时会把它当成自己的离线
	// 消息补发出去，那是跨会话的串消息。
	if err := s.clearQueuedLocked(clientID); err != nil {
		return err
	}
	// 与 Save 同样走合并提交：清洁会话的断开与 CONNECT 成对出现，一侧
	// 合并而另一侧逐条 fsync，重连风暴的天花板不会变（MQTT-118）。
	return s.kv.Delete(key, false)
}

// Expire 清理已过期的会话。
func (s *SessionStore) Expire(_ context.Context, nowUnixMs int64) (int, error) {
	// 先扫出待删的键再删，不在扫描回调里改动存储：
	// 边遍历边删在不同引擎上的行为不一致，而这类不一致极难追。
	var expired []string
	err := s.kv.Scan([]byte{byte(PrefixSession)}, func(key, value []byte) bool {
		st, derr := decodeSession(value)
		if derr != nil {
			// 无法解析的记录视为垃圾，一并清理：留着它只会让
			// 对应 clientId 每次连接都读到一条坏记录。
			parts, ok := SplitKey(key, PrefixSession, 1)
			if ok {
				expired = append(expired, parts[0])
			}
			return true
		}
		if st.Expired(nowUnixMs) {
			expired = append(expired, st.ClientID)
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if len(expired) == 0 {
		return 0, nil
	}
	ops := make([]Op, 0, len(expired)*2)
	for _, id := range expired {
		ops = append(ops, Op{Kind: OpDelete, Key: sessionKey(id)})
		// 队列的记录也要进这一批。与 Delete 同一个理由：留着就是孤儿，
		// 而且会在同一个 clientId 的下一个会话里被当成它的离线消息补发。
		if err := s.kv.Scan(queuedPrefix(id), func(key, _ []byte) bool {
			ops = append(ops, Op{Kind: OpDelete, Key: append([]byte(nil), key...)})
			return true
		}); err != nil {
			return 0, err
		}
	}
	if err := s.kv.Batch(ops, true); err != nil {
		return 0, err
	}
	return len(expired), nil
}

// Count 返回已持久化的会话数，供启动日志与指标使用。
func (s *SessionStore) Count() int {
	n := 0
	_ = s.kv.Scan([]byte{byte(PrefixSession)}, func([]byte, []byte) bool {
		n++
		return true
	})
	return n
}

func encodeSession(st *session.State) ([]byte, error) {
	b, err := json.Marshal(&sessionRecord{V: sessionRecordVersion, State: st})
	if err != nil {
		return nil, fmt.Errorf("store: 编码会话失败: %w", err)
	}
	return b, nil
}

func decodeSession(raw []byte) (*session.State, error) {
	var rec sessionRecord
	dec := json.NewDecoder(bytesReaderOf(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("无法解析: %w", err)
	}
	if rec.V > sessionRecordVersion {
		return nil, fmt.Errorf("记录版本 %d 高于本程序支持的 %d",
			rec.V, sessionRecordVersion)
	}
	if rec.State == nil {
		return nil, errors.New("记录缺少会话状态")
	}
	return rec.State, nil
}
