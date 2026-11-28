package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

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
	sessionOpReplace uint8 = 6
	// sessionOpBatch 把同一分片的多条独立命令装进一条 Raft
	// 日志。它是持久会话 CONNECT 路径的合并提案（CLU-134）。
	sessionOpBatch uint8 = 7
)

// MergeSessionProposals 把多个 Access 节点已经形成的会话 batch 在 shard
// leader 上再次展平。总逻辑命令数的上限是 sessionProposalBatchMax；
// widths 让 cluster 层把状态机回来的按条判定位切回每个原始请求（每个请求
// 自己那一段不超过 64 条，所以跨节点回传的仍是一个 uint64）。
//
// 同一 clientId 可以在同一条日志里出现多次：事务按数组顺序应用，后一个
// epoch 会围栏前一个，stale 位也仍按原位置返回。禁止重复反而会让旧连接的
// 断开 Save 与新 CONNECT 的 Save 拆成两次 fsync，正是重连风暴里最常见的
// 相邻写；这里严格保留 leader 队列前缀顺序，不做重排或丢弃。
func MergeSessionProposals(items [][]byte) ([]byte, []uint8, int, error) {
	if len(items) == 0 {
		return nil, nil, 0, errors.New("store: 没有可合并的会话提案")
	}
	flat := make([]json.RawMessage, 0, sessionProposalBatchMax)
	widths := make([]uint8, 0, len(items))
	bytesUsed := 0
	consumed := 0
	for _, raw := range items {
		var partBuf [sessionProposalBatchMax]json.RawMessage
		cmd, err := decodeSessionMergeView(raw, true, partBuf[:0])
		if err != nil {
			if consumed == 0 {
				return nil, nil, 1, err
			}
			break
		}
		// Expire 是一次分片全库扫描，状态机刻意禁止它作为 batch 子命令；
		// 单独提交也避免一次事务里重复扫描同一分片。
		if cmd.op == sessionOpExpire {
			if consumed == 0 {
				return append([]byte(nil), raw...), []uint8{1}, 1, nil
			}
			break
		}
		parts := []json.RawMessage{json.RawMessage(raw)}
		if cmd.op == sessionOpBatch {
			parts = cmd.batch
		}
		valid := true
		for _, part := range parts {
			sub, derr := decodeSessionMergeView(part, false, nil)
			if derr != nil || sub.op == sessionOpBatch || sub.op == sessionOpExpire {
				valid = false
				break
			}
		}
		if !valid {
			if consumed == 0 {
				return append([]byte(nil), raw...), []uint8{uint8(len(parts))}, 1, nil
			}
			break
		}
		if len(parts) == 0 || len(flat)+len(parts) > sessionProposalBatchMax ||
			(consumed > 0 && bytesUsed+len(raw) > sessionProposalBatchMaxBytes) {
			break
		}
		flat = append(flat, parts...)
		widths = append(widths, uint8(len(parts)))
		bytesUsed += len(raw)
		consumed++
	}
	if consumed == 0 {
		return append([]byte(nil), items[0]...), []uint8{1}, 1, nil
	}
	if consumed == 1 {
		return append([]byte(nil), items[0]...), widths, 1, nil
	}
	// 每个 Access envelope 已在 decodeSessionMergeView(validate=true) 里做过
	// 完整 JSON 校验，split 出来的子切片天然也是合法 JSON；这里不能再让
	// json.Valid 把一条最多 1 MiB 的日志重复扫描一遍。
	return encodeSessionRawBatchUnchecked(flat), widths, consumed, nil
}

// sessionMergeHeader 是 leader 二次合批唯一需要解码的部分。
// 不能在这里解整个 session.State：它可含订阅、在途表和离线队列，
// 64 条合批若全量反序列化，会把 leader 变成 CONNECT 的 CPU/GC 瓶颈。
type sessionMergeHeader struct {
	V        int               `json:"v"`
	Op       uint8             `json:"op"`
	ClientID string            `json:"c"`
	Batch    []json.RawMessage `json:"b"`
}

type sessionMergeView struct {
	op       uint8
	clientID []byte
	batch    []json.RawMessage
}

func decodeSessionMergeView(raw []byte, validate bool,
	batchScratch []json.RawMessage,
) (sessionMergeView, error) {
	if validate && !json.Valid(raw) {
		return sessionMergeView{}, errors.New("store: 会话命令损坏")
	}
	if op, client, batchAt, ok := canonicalSessionHeader(raw); ok {
		view := sessionMergeView{op: op, clientID: client}
		if op == sessionOpBatch {
			parts, err := splitCanonicalSessionBatch(raw, batchAt, batchScratch)
			if err != nil {
				return sessionMergeView{}, err
			}
			view.batch = parts
		}
		return view, nil
	}
	return decodeSessionMergeViewFallback(raw)
}

func decodeSessionMergeViewFallback(raw []byte) (sessionMergeView, error) {
	var h sessionMergeHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return sessionMergeView{}, fmt.Errorf("store: 会话命令损坏: %w", err)
	}
	if h.V != sessionCmdVersion {
		return sessionMergeView{}, fmt.Errorf("store: 会话命令版本 %d 不支持", h.V)
	}
	return sessionMergeView{op: h.Op, clientID: []byte(h.ClientID), batch: h.Batch}, nil
}

// canonicalSessionHeader 只读 encoding/json 对 sessionCmd 产生的固定前缀。
// clientId 含 JSON 转义时回落通用解码，不为了快路径放宽线格式。
func canonicalSessionHeader(raw []byte) (op uint8, client []byte, batchAt int, ok bool) {
	const vPrefix = `{"v":`
	if !bytes.HasPrefix(raw, []byte(vPrefix)) {
		return 0, nil, 0, false
	}
	i := len(vPrefix)
	version, next, valid := parseCanonicalUint(raw, i)
	if !valid || version != sessionCmdVersion || !bytes.HasPrefix(raw[next:], []byte(`,"op":`)) {
		return 0, nil, 0, false
	}
	i = next + len(`,"op":`)
	op64, next, valid := parseCanonicalUint(raw, i)
	if !valid || op64 > 255 || !bytes.HasPrefix(raw[next:], []byte(`,"c":"`)) {
		return 0, nil, 0, false
	}
	i = next + len(`,"c":"`)
	start := i
	for i < len(raw) {
		switch raw[i] {
		case '\\':
			return 0, nil, 0, false
		case '"':
			return uint8(op64), raw[start:i], i + 1, true
		default:
			i++
		}
	}
	return 0, nil, 0, false
}

func parseCanonicalUint(raw []byte, start int) (uint64, int, bool) {
	if start >= len(raw) || raw[start] < '0' || raw[start] > '9' {
		return 0, start, false
	}
	var n uint64
	i := start
	for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
		n = n*10 + uint64(raw[i]-'0')
		i++
	}
	return n, i, true
}

func splitCanonicalSessionBatch(raw []byte, afterClient int,
	parts []json.RawMessage,
) ([]json.RawMessage, error) {
	const batchPrefix = `,"b":[`
	if afterClient > len(raw) || !bytes.HasPrefix(raw[afterClient:], []byte(batchPrefix)) {
		return nil, errors.New("store: 会话批命令缺少 b")
	}
	i := afterClient + len(batchPrefix)
	if i >= len(raw) {
		return nil, errors.New("store: 会话批命令截断")
	}
	if raw[i] == ']' {
		return parts, nil
	}
	start, depth := i, 0
	inString, escaped := false, false
	for ; i < len(raw); i++ {
		b := raw[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
			} else if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}':
			depth--
		case ']':
			if depth == 0 {
				parts = append(parts, json.RawMessage(raw[start:i]))
				return parts, nil
			}
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, json.RawMessage(raw[start:i]))
				start = i + 1
			}
		}
	}
	return nil, errors.New("store: 会话批命令截断")
}

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
	// Batch 仅在 Op=sessionOpBatch 时有值。用 RawMessage 避免把已经
	// 编码好的子命令再做一次 base64；子命令仍各自带版本号。
	Batch []json.RawMessage `json:"b,omitempty"`
}

const (
	// sessionProposalBatchMax 是一条 Raft 日志里最多装多少条逻辑命令。
	//
	// 这一栏的天花板是**物理日志条数**而不是单条日志的大小：同机多进程
	// 上一条日志至少要 leader 的 WAL、多数派的 WAL 与各副本 apply 侧各
	// 一次 fsync，而一条会话命令只有几百字节。64 条时实测三节点 4 分片
	// 4,376 条/秒，12,000 条逻辑命令用掉 204 条物理日志，其中 135 条是被
	// 这个上限截断的；放到 512 条后同一配置是 10,273 条/秒（3 轮中位数，
	// 区间 9,963–10,623），物理日志降到 49 至 52 条（CLU-193）。
	//
	// 再往上加没有意义：那一轮平均只装到 230 至 245 条，车道已经是
	// **供给受限**（队列取空）而不是宽度受限。
	//
	// 它曾经等于 64 是因为状态机的判定结果是一个 uint64 位图；现在按条的
	// 位图走 ShardEntry.ResultBits，所以这个数不再受 64 位约束。**每个
	// 请求自己那一段仍必须不超过 64 条**（跨节点回传的是切好的 uint64），
	// 而那由 sessionAccessProposalBatchMax 保证。
	sessionProposalBatchMax = 512
	// Access 侧用较小块交给 leader，leader 仍会将多块展平到
	// sessionProposalBatchMax 条。避免 40+30 这类不可拆的 Access 批让前
	// 一条 Raft 日志只装 40 条，又不降低最终的共识合批上限。
	// 它同时是「一个请求自己那一段」的位宽上限，必须不超过 64。
	sessionAccessProposalBatchMax = 16
	// 条数上限之外再限编码前大小，避免一批拥有大量订阅的会话合成一条
	// 超过 dragonboat 提案上限的命令。单条超过时仍单独提案，让下层给出
	// 权威的大小错误。条数上限放宽到 512 之后这一条是实际生效的那个
	// 约束：订阅多的会话在装满 512 条之前就先撞到 1 MiB。
	sessionProposalBatchMaxBytes = 1 << 20
	sessionProposalBatchDelay    = time.Millisecond
	// 每个 session shard 允许少量提案同时交给 Dragonboat。此前 Access
	// 合批器必须等一轮 SyncPropose 完整返回才会派下一批，8 个 shard 因而
	// 最多只有 8 个物理提案在途，重连风暴即使已经合批仍会在用户态排队。
	// 有界 pipeline 让共识库合并 WAL/传输，同时避免失去多数派时无界堆积。
	//
	// 它同时是 leader 二次合批的**供给**上限：取 8 时 leader 平均只能攒到
	// 137 条/日志，提到 32 之后是 230 至 245 条，三节点 4 分片从 7,660
	// 升到 10,273 条/秒（CLU-193）。**不要再往上取**：实测 96 更快
	// （10,855），但在途乘分片数必须留在对端连接池缓存的连接数
	// （peerPoolPerNode = 256，按对端摊）之内——8 分片下 96 正好让每个
	// 对端约 256 条并发转发，超出的部分退回「每条转发一次 TLS 握手」，
	// 也就是 CLU-120 那个形状。
	sessionProposalConcurrentMax = 32
	// pipeline 已有提案在途时，小尾批稍等一会儿，避免把持续到达的请求
	// 打散成大量单项日志；满批不受这个等待影响。
	sessionProposalConcurrentMin = 8
	sessionProposalMaxWait       = 2 * time.Millisecond
	// 重连风暴后不永久留住峰值数量的 request/channel。
	sessionProposalRequestCacheMax = 4096
	sessionProposalSliceCacheMax   = 4096
)

// SessionProposalBatchMax 导出给装配处（`MergeMaxItems`）。
//
// leader 侧实际展平到多少条，必须与状态机侧的校验上限是同一个数：写成两个
// 字面量时，某一次只调了一边的后果是 leader 提出一条状态机会拒绝的日志，
// 而那条日志已经被多数派提交了——apply 报错等于节点停下来。
const SessionProposalBatchMax = sessionProposalBatchMax

func encodeSessionCmd(c *sessionCmd) ([]byte, error) {
	c.V = sessionCmdVersion
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("store: 编码会话命令失败: %w", err)
	}
	return b, nil
}

func startSessionBatch(encodedBytes int) []byte {
	// 保持与 encoding/json 的 sessionCmd 字段顺序一致，便于抓包和旧节点排查。
	out := make([]byte, 0, encodedBytes+32)
	out = append(out, `{"v":`...)
	out = strconv.AppendInt(out, sessionCmdVersion, 10)
	out = append(out, `,"op":`...)
	out = strconv.AppendUint(out, uint64(sessionOpBatch), 10)
	out = append(out, `,"c":"","b":[`...)
	return out
}

func encodeSessionRawBatch(parts []json.RawMessage) ([]byte, error) {
	total := 0
	for _, raw := range parts {
		if !json.Valid(raw) {
			return nil, errors.New("store: 会话批命令含无效 JSON")
		}
		total += len(raw)
	}
	return encodeSessionRawBatchUncheckedSize(parts, total), nil
}

func encodeSessionRawBatchUnchecked(parts []json.RawMessage) []byte {
	total := 0
	for _, raw := range parts {
		total += len(raw)
	}
	return encodeSessionRawBatchUncheckedSize(parts, total)
}

func encodeSessionRawBatchUncheckedSize(parts []json.RawMessage, total int) []byte {
	out := startSessionBatch(total + len(parts))
	for i, raw := range parts {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, raw...)
	}
	out = append(out, ']', '}')
	return out
}

func encodeSessionRequestBatch(batch []*sessionProposalRequest) ([]byte, error) {
	total := 0
	for _, req := range batch {
		if !json.Valid(req.data) {
			return nil, errors.New("store: 会话提案含无效 JSON")
		}
		total += len(req.data)
	}
	out := startSessionBatch(total + len(batch))
	for i, req := range batch {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, req.data...)
	}
	out = append(out, ']', '}')
	return out, nil
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

// kvReader 是 ops 构造函数需要的读能力。
//
// 它的存在是为了让同一段逻辑既能从 KV 读（单条 apply、补发前的预读），
// 也能从一个**写事务**里读（按批 apply，CLU-125）。两者的区别正是这一
// 条改动的全部：事务里的读看得见同一批里前面几条命令的写入，而批内
// 后一条必须看得见前一条——入队序号是在 apply 时由当前状态推出的
// （CLU-115），看不见就撞号，而撞号的表现是一条消息静默覆盖另一条。
type kvReader interface {
	Get(key []byte) ([]byte, error)
	Scan(prefix []byte, fn func(key, value []byte) bool) error
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
	if cmd.Op == sessionOpBatch {
		var result uint64
		err := s.kv.InTx(false, func(tx Tx) error {
			v, aerr := s.applyBatchCmd(tx, cmd, owns)
			if aerr != nil {
				return aerr
			}
			result = v
			op := appliedIndexOp(shard, index)
			return tx.Set(op.Key, op.Value)
		})
		return result, err
	}
	lane := s.lane(cmd.ClientID)
	lane.Lock()
	defer lane.Unlock()

	ops, res, err := s.sessionCmdOps(s.kv, cmd, owns)
	if err != nil {
		return 0, err
	}
	ops = append(ops, appliedIndexOp(shard, index))
	if err := s.kv.Batch(ops, false); err != nil {
		return 0, err
	}
	return res, nil
}

// SessionEntry 是一批已提交会话命令中的一条。
//
// Result 由 ApplySessionBatch 回填：epoch 围栏的判定要逐条交回提案方
// （CLU-116），所以它不能是整批一个值。
type SessionEntry struct {
	Index  uint64
	Data   []byte
	Result uint64
	// ResultBits 是批命令按条的 stale 位图（一位一条），用于一条日志里
	// 装了超过 64 条命令的合并提案（CLU-193）。Result 那个 uint64 只装
	// 得下前 64 条，而合批宽度正是这一栏吞吐的主要杠杆。
	ResultBits []byte
}

// ApplySessionBatch 在**一个**写事务里应用一批已提交的会话命令（CLU-125）。
//
// 它与逐条调用 ApplySessionCmd 的区别只有一个：fsync 的次数。逐条时
// 每条命令一次事务提交，而可合并的并发度等于分片数（每个分片只有一个
// apply 协程，循环里是串行的）——实测单副本 8 分片 187 条/秒，而同一条
// 路径上把状态机换成不落盘的内存实现是 13772 条/秒，余量全在这里。
// 这正是 MQTT-119 当年在**本地**会话存储上做过的那件事（逐条 191 次/秒
// 对合并 27268 次/秒），复制路径把它丢了一次。
//
// 三件事不能丢，它们都只在多节点加并发时才显形：
//
//   - **批内后一条要看得见前一条的效果。** 所以读写都走同一个 Tx，而不是
//     先把全批的 ops 算完再一起写：入队序号是在 apply 时由当前状态推出的
//     （CLU-115），先算完的话同一批里两条 enqueue 会算出同一个序号，表现
//     是一条消息静默覆盖另一条。
//   - **每条命令的返回值要逐条保留**（CLU-116）。
//   - **applied index 要和数据进同一个事务**（CLU-109）。这里写的是这一批
//     **最后一条**的 index：中间任何一条的 index 都会让"报的位置"小于
//     "盘上的状态"，于是重启后那几条被重放，而 dragonboat 不保证重放
//     已被快照压掉的日志。
//
// 整批原子也让错误处理变简单：任何一条出错就整批回滚，由调用方把错误
// 交给 dragonboat 停掉本节点——它和逐条版一样，不跳过、不继续。
func (s *SessionStore) ApplySessionBatch(shard uint64, ents []SessionEntry,
	owns func(string) bool,
) error {
	if len(ents) == 0 {
		return nil
	}
	cmds := make([]*sessionCmd, len(ents))
	for i := range ents {
		cmd, err := decodeSessionCmd(ents[i].Data)
		if err != nil {
			// 解不开的命令是致命的（与逐条版同一条理由）：它已被多数派
			// 提交，别的节点会应用它，本节点跳过就是状态机分叉。
			return err
		}
		cmds[i] = cmd
	}
	// sync 传 false：允许与别的分片的事务合并，这正是要的效果。持久性
	// 不受影响——bbolt 的 Batch 仍然在返回前提交并 fsync（见 boltKV.write）。
	return s.kv.InTx(false, func(tx Tx) error {
		// 结果每次都从头填：sync=false 的事务体可能被重跑（bbolt 的
		// Batch 语义），累积就会串味。
		for i := range ents {
			ents[i].Result, ents[i].ResultBits = 0, nil
		}
		for i := range ents {
			// 批命令走位图那条路径：一条日志可以装超过 64 条，而提案方
			// 要的是按条的判定（CLU-193）。
			if cmds[i].Op == sessionOpBatch {
				bits, err := s.applyBatchCmdBits(tx, cmds[i], owns)
				if err != nil {
					return err
				}
				// Result 也要填：一条日志只装了一个 Access 批时，
				// cluster 层交回的是这个 uint64 而不是位图（那条路径
				// 还要给 Expire 回条数，所以不能改成按位切）。只填
				// 位图会让最常见的那条路径上的围栏判定静默变成
				// “没被拦”，而那是一次覆盖。
				ents[i].Result = resultBitsToUint64(bits, len(cmds[i].Batch))
				ents[i].ResultBits = bits
				continue
			}
			res, err := s.applyOneLocked(tx, cmds[i], owns)
			if err != nil {
				return err
			}
			ents[i].Result = res
		}
		op := appliedIndexOp(shard, ents[len(ents)-1].Index)
		return tx.Set(op.Key, op.Value)
	})
}

// applyOneLocked 在给定事务里应用一条命令，返回状态机的判定值。
//
// lane 锁一条一取、取完即放，不是整批持着：lane 的作用是挡住同一个
// clientId 上的并发读改写，而一次只持一把就不会死锁（整批持着要先对
// clientId 排序，而那只是为了一个这里不需要的性质）。
func (s *SessionStore) applyOneLocked(tx Tx, cmd *sessionCmd,
	owns func(string) bool,
) (uint64, error) {
	if cmd.Op == sessionOpBatch {
		return s.applyBatchCmd(tx, cmd, owns)
	}
	lane := s.lane(cmd.ClientID)
	lane.Lock()
	defer lane.Unlock()
	ops, res, err := s.sessionCmdOps(tx, cmd, owns)
	if err != nil {
		return 0, err
	}
	for i := range ops {
		switch ops[i].Kind {
		case OpSet:
			if err := tx.Set(ops[i].Key, ops[i].Value); err != nil {
				return 0, err
			}
		case OpDelete:
			if err := tx.Delete(ops[i].Key); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("store: 未知的操作类型 %d", ops[i].Kind)
		}
	}
	return res, nil
}

// applyBatchCmdBits 在同一个状态机事务里顺序应用子命令。
// 返回位图的第 i 位表示第 i 条被 epoch 围栏拒绝。
func (s *SessionStore) applyBatchCmdBits(tx Tx, cmd *sessionCmd,
	owns func(string) bool,
) ([]byte, error) {
	if len(cmd.Batch) == 0 || len(cmd.Batch) > sessionProposalBatchMax {
		return nil, fmt.Errorf("store: 会话批命令条数 %d 超出范围", len(cmd.Batch))
	}
	bits := make([]byte, (len(cmd.Batch)+7)/8)
	for i := range cmd.Batch {
		sub, err := decodeSessionCmd(cmd.Batch[i])
		if err != nil {
			return nil, err
		}
		if sub.Op == sessionOpBatch || sub.Op == sessionOpExpire {
			return nil, fmt.Errorf("store: 会话批命令不允许子操作 %d", sub.Op)
		}
		v, err := s.applyOneLocked(tx, sub, owns)
		if err != nil {
			return nil, err
		}
		if v == sessionResStale {
			bits[i/8] |= 1 << uint(i%8)
		}
	}
	return bits, nil
}

// resultBitsToUint64 把前 min(n, 64) 位判定位折进一个 uint64。
func resultBitsToUint64(bits []byte, n int) uint64 {
	var result uint64
	for i := 0; i < n && i < 64; i++ {
		if i/8 < len(bits) && bits[i/8]&(1<<uint(i%8)) != 0 {
			result |= uint64(1) << uint(i)
		}
	}
	return result
}

// applyBatchCmd 是只能回一个 uint64 的那条路径（逐条 apply 与用例）。
//
// 超过 64 条时**报错而不是截断**：丢掉的那几位会被读成“这条没被围栏
// 拦”，而那正是契约 01 要拦的覆盖。整批 apply 走 applyBatchCmdBits。
func (s *SessionStore) applyBatchCmd(tx Tx, cmd *sessionCmd,
	owns func(string) bool,
) (uint64, error) {
	if len(cmd.Batch) > 64 {
		return 0, fmt.Errorf("store: 逐条 apply 不支持 %d 条的批命令", len(cmd.Batch))
	}
	bits, err := s.applyBatchCmdBits(tx, cmd, owns)
	if err != nil {
		return 0, err
	}
	return resultBitsToUint64(bits, len(cmd.Batch)), nil
}

// sessionCmdOps 把一条命令翻成一组写入，不自己落盘。
//
// 拆出来是为了让 applied index 能和它们进同一个 Batch。所有分支都只
// **读**当前状态再算出写入——于是同一条命令在任何节点上算出的是同一组
// 写入，这正是复制日志要求的确定性。
func (s *SessionStore) sessionCmdOps(r kvReader, cmd *sessionCmd,
	owns func(string) bool,
) ([]Op, uint64, error) {
	switch cmd.Op {
	case sessionOpSave:
		return s.saveOps(r, cmd)
	case sessionOpDelete:
		return s.deleteOps(r, cmd)
	case sessionOpEnqueue:
		return s.enqueueOps(r, cmd)
	case sessionOpTake:
		return s.takeOps(r, cmd)
	case sessionOpExpire:
		return s.expireOps(r, cmd, owns)
	case sessionOpReplace:
		return s.replaceOps(r, cmd)
	case sessionOpBatch:
		return nil, 0, errors.New("store: 批命令必须在事务中应用")
	default:
		return nil, 0, fmt.Errorf("store: 未知的会话操作码 %d", cmd.Op)
	}
}

func (s *SessionStore) replaceOps(r kvReader, cmd *sessionCmd) ([]Op, uint64, error) {
	if cmd.State == nil || cmd.State.ClientID == "" {
		return nil, 0, errors.New("store: replace 命令缺少会话状态")
	}
	cur, found, err := s.peek(r, cmd.State.ClientID)
	if err != nil {
		return nil, 0, err
	}
	if found && cmd.State.Epoch < cur.Epoch {
		return nil, sessionResStale, nil
	}
	if found && s.staleAtEqualEpoch(cur, cmd.State) {
		return nil, sessionResStale, nil
	}
	ops, err := s.replaceStateOps(r, cmd.State)
	if err != nil {
		return nil, 0, err
	}
	return ops, sessionResOK, nil
}

func (s *SessionStore) saveOps(r kvReader, cmd *sessionCmd) ([]Op, uint64, error) {
	st := cmd.State
	if st == nil || st.ClientID == "" {
		return nil, 0, errors.New("store: save 命令缺少会话状态")
	}
	cur, found, err := s.peek(r, st.ClientID)
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
	// epoch 相等的那一种同样在这里判（CLU-131）。判定只看命令与当前
	// 状态，所以每个节点算出的结果相同——确定性不受影响，各节点各自
	// 累加自己的计数器。
	if found && s.staleAtEqualEpoch(cur, st) {
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
		clear, cerr := s.queuedKeys(r, st.ClientID)
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

func (s *SessionStore) deleteOps(r kvReader, cmd *sessionCmd) ([]Op, uint64, error) {
	cur, found, err := s.peek(r, cmd.ClientID)
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
	keys, kerr := s.queuedKeys(r, cmd.ClientID)
	if kerr != nil {
		return nil, 0, kerr
	}
	ops := make([]Op, 0, len(keys)+1)
	for _, k := range keys {
		ops = append(ops, Op{Kind: OpDelete, Key: k})
	}
	return append(ops, Op{Kind: OpDelete, Key: sessionKey(cmd.ClientID)}), sessionResOK, nil
}

func (s *SessionStore) enqueueOps(r kvReader, cmd *sessionCmd) ([]Op, uint64, error) {
	if cmd.Msg == nil {
		return nil, 0, errors.New("store: enqueue 命令缺少消息")
	}
	// 序号在**应用时**由当前状态推出，不由提案方带过来。
	//
	// 这一步是确定性的来源：各节点按同一个日志序应用，于是算出同一个
	// 序号。换成提案方分配，两个节点并发入队就会撞号——而撞号的表现是
	// 一条消息静默覆盖另一条（键相同），不是报错。
	last, err := s.lastQueuedSeq(r, cmd.ClientID)
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

func (s *SessionStore) takeOps(r kvReader, cmd *sessionCmd) ([]Op, uint64, error) {
	prefix := queuedPrefix(cmd.ClientID)
	var ops []Op
	err := r.Scan(prefix, func(key, _ []byte) bool {
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
func (s *SessionStore) expireOps(r kvReader, cmd *sessionCmd,
	owns func(string) bool,
) ([]Op, uint64, error) {
	if owns == nil {
		return nil, 0, errors.New("store: expire 命令缺少分片归属判定")
	}
	var ops []Op
	var ids []string
	err := r.Scan([]byte{byte(PrefixSession)}, func(key, value []byte) bool {
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
		keys, kerr := s.queuedKeys(r, id)
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
func (s *SessionStore) peek(r kvReader, clientID string) (*session.State, bool, error) {
	raw, err := r.Get(sessionKey(clientID))
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

func (s *SessionStore) queuedKeys(r kvReader, clientID string) ([][]byte, error) {
	var keys [][]byte
	err := r.Scan(queuedPrefix(clientID), func(key, _ []byte) bool {
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	return keys, err
}

func (s *SessionStore) lastQueuedSeq(r kvReader, clientID string) (uint64, error) {
	var last uint64
	prefix := queuedPrefix(clientID)
	err := r.Scan(prefix, func(key, _ []byte) bool {
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
	last, lerr := s.lastQueuedSeq(s.kv, clientID)
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
	var recordHeader [8]byte
	var writeErr error
	for _, prefix := range []Prefix{PrefixSession, PrefixQueued} {
		err := s.kv.Scan([]byte{byte(prefix)}, func(key, value []byte) bool {
			if !owns(clientOfKey(key, prefix)) {
				return true
			}
			writeErr = writeKVHeader(w, key, value, &recordHeader)
			return writeErr == nil
		})
		if err != nil {
			return err
		}
		if writeErr != nil {
			return writeErr
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
	return writeKVHeader(w, key, value, &hdr)
}

// writeKVHeader 允许大扫描复用头缓冲，避免每条记录因 io.Writer 接口逃逸
// 分配一个 8 字节对象。调用方串行使用 hdr。
func writeKVHeader(w io.Writer, key, value []byte, hdr *[8]byte) error {
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

// remoteSessionReader 是 RF<节点数时由 cluster.ShardGroup 提供的读路径。
// 单元测试里的轻量 proposer 不必实现它；当前 RF=all 时 HasLocalReplica
// 恒为真，因此生产路径也不会多一次 RPC。
type remoteSessionReader interface {
	HasLocalReplica(clientID string) bool
	ReadSession(clientID string) ([]byte, bool, error)
}

type claimedSessionReader interface {
	TakeClaimedSession(clientID string, epoch cluster.Epoch) (
		data []byte, found, cached bool, err error,
	)
}

// sessionReplicaSnapshot 是 session cell 的只读快照。cluster 只把它当成
// 不透明字节转发；编码放在 store，避免传输层依赖 session 包。
//
// QueueUpTo 必须和 Queue 来自同一次本地读取。TakeQueued 随后提交的是
// “删到该序号”，而不是清空队列，因此快照之后新入队的消息不会被误删。
type sessionReplicaSnapshot struct {
	State     *session.State      `json:"s,omitempty"`
	Queue     []session.QueuedMsg `json:"q,omitempty"`
	QueueUpTo uint64              `json:"u,omitempty"`
}

// ReadReplicaSnapshot 编码一份可供非副本节点读取的完整 session-cell
// 快照。found 表示会话或队列至少有一项；队列可以在会话记录暂时不存在时
// 独立存在，所以不能只用 Load 的 found。
func (s *SessionStore) ReadReplicaSnapshot(clientID string) ([]byte, bool, error) {
	// 一把 client lane 锁内读取状态、队列与水位：既避免 Load 和
	// PeekQueued 各扫描一次队列，也保证三者来自同一个本地视图。
	lane := s.lane(clientID)
	lane.Lock()
	defer lane.Unlock()
	var st *session.State
	stateFound := false
	rawState, err := s.kv.Get(sessionKey(clientID))
	if err == nil {
		st, err = decodeSession(rawState)
		stateFound = err == nil
	} else if errors.Is(err, ErrNotFound) {
		err = nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: 读取远程会话快照 %s 失败: %w", clientID, err)
	}
	msgs, err := s.loadQueued(clientID)
	if err != nil {
		return nil, false, err
	}
	var upTo uint64
	if len(msgs) > 0 {
		upTo, err = s.lastQueuedSeq(s.kv, clientID)
		if err != nil {
			return nil, false, err
		}
	}
	if !stateFound && len(msgs) == 0 {
		return nil, false, nil
	}
	raw, err := json.Marshal(sessionReplicaSnapshot{
		State: st, Queue: msgs, QueueUpTo: upTo,
	})
	if err != nil {
		return nil, false, fmt.Errorf("store: 编码远程会话快照 %s 失败: %w", clientID, err)
	}
	return raw, true, nil
}

func decodeReplicaSnapshot(clientID string, raw []byte) (sessionReplicaSnapshot, error) {
	var snap sessionReplicaSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return sessionReplicaSnapshot{}, fmt.Errorf("store: 远程会话快照损坏 %s: %w", clientID, err)
	}
	return snap, nil
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

	batchMu               sync.Mutex
	batch                 map[uint64]*sessionProposalLane
	reqFree               []*sessionProposalRequest
	proposalBackpressured uint64
	proposalQueued        int
	proposalActive        int
	proposalPeakQueued    int
	proposalPeakActive    int
}

// SessionProposalQueueStats 是 Access 侧会话提案 pipeline 的有界快照。
// 它与 ShardGroup 的 Raft 指标分开：这里回答请求是否仍堵在用户态队列，
// 后者回答真正进入共识的日志数与耗时。
type SessionProposalQueueStats struct {
	Backpressured       uint64
	Queued              int
	InFlight            int
	InFlightBatches     int
	PeakQueued          int
	PeakInFlightBatches int
	CachedRequests      int
	CachedBatches       int
}

func (r *ReplicatedSessionStore) ProposalQueueStats() SessionProposalQueueStats {
	r.batchMu.Lock()
	st := SessionProposalQueueStats{
		Backpressured:       r.proposalBackpressured,
		Queued:              r.proposalQueued,
		InFlightBatches:     r.proposalActive,
		PeakQueued:          r.proposalPeakQueued,
		PeakInFlightBatches: r.proposalPeakActive,
		CachedRequests:      len(r.reqFree),
	}
	for _, lane := range r.batch {
		st.InFlight += len(lane.inFlight)
		st.CachedBatches += len(lane.batchFree)
	}
	r.batchMu.Unlock()
	return st
}

type sessionProposalResult struct {
	stale bool
	err   error
}

type sessionProposalRequest struct {
	clientID string
	data     []byte
	queued   time.Time
	done     chan sessionProposalResult
}

type sessionProposalLane struct {
	running  bool
	active   int
	pending  []*sessionProposalRequest
	inFlight map[string]struct{}
	// scratch 与 pending 交换，筛选一批时不再每轮分配 remain。
	scratch []*sessionProposalRequest
	// seen 每条 lane 复用；每批清空，不延长 clientId 字符串生命。
	seen map[string]struct{}
	// batchFree 的上限等于并行提案上限，避免一次重连风暴永久保留
	// 大量临时指针数组。
	batchFree []*sessionProposalBatch
}

type sessionProposalBatch struct {
	requests []*sessionProposalRequest
}

// NewReplicatedSessionStore 包装一个本地会话存储。
func NewReplicatedSessionStore(local *SessionStore, prop SessionProposer,
) *ReplicatedSessionStore {
	return &ReplicatedSessionStore{
		local: local, prop: prop, batch: make(map[uint64]*sessionProposalLane),
	}
}

var _ session.Store = (*ReplicatedSessionStore)(nil)

// Local 返回底层的本地存储，供状态机与启动期统计使用。
func (r *ReplicatedSessionStore) Local() *SessionStore { return r.local }

// Load 从本地读。
func (r *ReplicatedSessionStore) Load(ctx context.Context, clientID string,
) (*session.State, bool, error) {
	if reader, ok := r.prop.(remoteSessionReader); ok && !reader.HasLocalReplica(clientID) {
		raw, found, err := reader.ReadSession(clientID)
		return decodeLoadedReplica(clientID, raw, found, err)
	}
	return r.local.Load(ctx, clientID)
}

// LoadClaimed 优先消费 ownership Claim RPC 顺带返回的一次性快照。
// 没有预读结果时退回 Load，故它是纯性能优化，不改变故障与重试语义。
func (r *ReplicatedSessionStore) LoadClaimed(ctx context.Context, clientID string,
	epoch cluster.Epoch,
) (*session.State, bool, error) {
	reader, remote := r.prop.(remoteSessionReader)
	if !remote || reader.HasLocalReplica(clientID) {
		return r.local.Load(ctx, clientID)
	}
	if claimed, ok := r.prop.(claimedSessionReader); ok {
		raw, found, cached, err := claimed.TakeClaimedSession(clientID, epoch)
		if cached {
			return decodeLoadedReplica(clientID, raw, found, err)
		}
	}
	return r.Load(ctx, clientID)
}

func decodeLoadedReplica(clientID string, raw []byte, found bool, err error) (
	*session.State, bool, error,
) {
	if err != nil || !found {
		return nil, found, err
	}
	snap, err := decodeReplicaSnapshot(clientID, raw)
	if err != nil {
		return nil, false, err
	}
	if snap.State != nil {
		snap.State.Queued = snap.Queue
	}
	return snap.State, snap.State != nil, nil
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

// Replace atomically replaces a persistent session in one shard proposal.
func (r *ReplicatedSessionStore) Replace(_ context.Context, st *session.State) error {
	if st == nil || st.ClientID == "" {
		return errors.New("store: 会话缺少 clientId")
	}
	data, err := encodeSessionCmd(&sessionCmd{
		Op: sessionOpReplace, ClientID: st.ClientID, State: st,
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
	var msgs []session.QueuedMsg
	var upTo uint64
	var err error
	if reader, ok := r.prop.(remoteSessionReader); ok && !reader.HasLocalReplica(clientID) {
		var raw []byte
		var found bool
		raw, found, err = reader.ReadSession(clientID)
		if err == nil && found {
			var snap sessionReplicaSnapshot
			snap, err = decodeReplicaSnapshot(clientID, raw)
			if err == nil {
				msgs, upTo = snap.Queue, snap.QueueUpTo
			}
		}
	} else {
		msgs, upTo, err = r.local.PeekQueued(clientID)
	}
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
	shard := r.prop.ShardOf(clientID)
	r.batchMu.Lock()
	var req *sessionProposalRequest
	if n := len(r.reqFree); n > 0 {
		req = r.reqFree[n-1]
		r.reqFree = r.reqFree[:n-1]
	} else {
		req = &sessionProposalRequest{done: make(chan sessionProposalResult, 1)}
	}
	// propose 是同步等待：收到 done 之前调用方不会改写 data，
	// 所以无需再把已编码命令复制一遍。
	req.clientID, req.data, req.queued = clientID, data, time.Now()
	lane := r.batch[shard]
	if lane == nil {
		lane = &sessionProposalLane{inFlight: make(map[string]struct{})}
		r.batch[shard] = lane
	}
	lane.pending = append(lane.pending, req)
	r.proposalQueued++
	if r.proposalQueued > r.proposalPeakQueued {
		r.proposalPeakQueued = r.proposalQueued
	}
	if lane.active >= sessionProposalConcurrentMax {
		r.proposalBackpressured++
	}
	if !lane.running {
		lane.running = true
		go r.runProposalLane(shard, lane)
	}
	r.batchMu.Unlock()
	result := <-req.done
	req.clientID, req.data, req.queued = "", nil, time.Time{}
	r.batchMu.Lock()
	if len(r.reqFree) < sessionProposalRequestCacheMax {
		r.reqFree = append(r.reqFree, req)
	}
	r.batchMu.Unlock()
	if result.err != nil {
		return result.err
	}
	if result.stale {
		return cluster.ErrStaleEpoch
	}
	return nil
}

// runProposalLane 将同一分片短时间内到达的写入合成一次
// SyncPropose，并把少量互不相关的批次并行交给 Dragonboat。每批一个
// clientId 最多一条，且同 clientId 在前一批完成前不会进入下一批；否则
// pipeline 的完成乱序会破坏同设备的 Save/Take/Enqueue 顺序。
func (r *ReplicatedSessionStore) runProposalLane(shard uint64, lane *sessionProposalLane) {
	for {
		time.Sleep(sessionProposalBatchDelay)
		r.batchMu.Lock()
		if len(lane.pending) == 0 {
			if lane.active == 0 {
				lane.running = false
				if cap(lane.pending) > sessionProposalSliceCacheMax {
					lane.pending = nil
				}
				if cap(lane.scratch) > sessionProposalSliceCacheMax {
					lane.scratch = nil
				}
				r.batchMu.Unlock()
				return
			}
			r.batchMu.Unlock()
			continue
		}
		batch, dispatch := selectSessionProposalBatch(lane, time.Now())
		if dispatch {
			r.proposalQueued -= len(batch.requests)
			r.proposalActive++
			if r.proposalActive > r.proposalPeakActive {
				r.proposalPeakActive = r.proposalActive
			}
		}
		r.batchMu.Unlock()
		if dispatch {
			go r.finishProposalBatch(shard, lane, batch)
		}
	}
}

// selectSessionProposalBatch 在 batchMu 下选出一批可并行的请求。
// now 可注入，既让尾批等待边界可测，也避免测试依赖调度时序。
func selectSessionProposalBatch(lane *sessionProposalLane, now time.Time) (
	*sessionProposalBatch, bool,
) {
	if len(lane.pending) == 0 || lane.active >= sessionProposalConcurrentMax {
		return nil, false
	}
	batch := lane.takeBatch()
	// remain 必须用与 pending 不同的底层数组；就地 append 会覆盖 range
	// 尚未读到的请求。scratch 在两个独立数组间交换。
	pending := lane.pending
	remain := lane.scratch[:0]
	seen := lane.seen
	if seen == nil {
		seen = make(map[string]struct{}, sessionAccessProposalBatchMax)
		lane.seen = seen
	}
	batchBytes := 0
	for _, req := range pending {
		_, duplicate := seen[req.clientID]
		_, blocked := lane.inFlight[req.clientID]
		fitsBytes := len(batch.requests) == 0 ||
			batchBytes+len(req.data) <= sessionProposalBatchMaxBytes
		if len(batch.requests) < sessionAccessProposalBatchMax && !duplicate && !blocked && fitsBytes {
			seen[req.clientID] = struct{}{}
			batch.requests = append(batch.requests, req)
			batchBytes += len(req.data)
		} else {
			remain = append(remain, req)
		}
	}
	if len(batch.requests) == 0 || (lane.active > 0 &&
		len(batch.requests) < sessionProposalConcurrentMin &&
		now.Sub(batch.requests[0].queued) < sessionProposalMaxWait) {
		clear(remain)
		clear(seen)
		lane.putBatch(batch)
		return nil, false
	}
	lane.pending = remain
	for id := range seen {
		lane.inFlight[id] = struct{}{}
	}
	clear(pending)
	lane.scratch = pending[:0]
	clear(seen)
	lane.active++
	return batch, true
}

func (lane *sessionProposalLane) takeBatch() *sessionProposalBatch {
	if n := len(lane.batchFree); n > 0 {
		batch := lane.batchFree[n-1]
		lane.batchFree = lane.batchFree[:n-1]
		return batch
	}
	return &sessionProposalBatch{
		requests: make([]*sessionProposalRequest, 0, sessionProposalBatchMax),
	}
}

func (lane *sessionProposalLane) putBatch(batch *sessionProposalBatch) {
	clear(batch.requests)
	batch.requests = batch.requests[:0]
	if len(lane.batchFree) < sessionProposalConcurrentMax {
		lane.batchFree = append(lane.batchFree, batch)
	}
}

func (r *ReplicatedSessionStore) finishProposalBatch(shard uint64,
	lane *sessionProposalLane, batch *sessionProposalBatch,
) {
	result, err := r.proposeBatch(shard, batch.requests)
	r.batchMu.Lock()
	for _, req := range batch.requests {
		delete(lane.inFlight, req.clientID)
	}
	lane.active--
	r.proposalActive--
	r.batchMu.Unlock()
	for i, req := range batch.requests {
		req.done <- sessionProposalResult{
			stale: result&(uint64(1)<<uint(i)) != 0,
			err:   err,
		}
	}
	r.batchMu.Lock()
	lane.putBatch(batch)
	r.batchMu.Unlock()
}

func (r *ReplicatedSessionStore) proposeBatch(shard uint64,
	batch []*sessionProposalRequest,
) (uint64, error) {
	if len(batch) == 1 {
		return r.prop.ProposeShard(shard, batch[0].data)
	}
	data, err := encodeSessionRequestBatch(batch)
	if err != nil {
		return 0, err
	}
	return r.prop.ProposeShard(shard, data)
}

// Count 返回本地已持久化的会话数。
func (r *ReplicatedSessionStore) Count() int { return r.local.Count() }
