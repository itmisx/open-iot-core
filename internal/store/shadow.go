package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/itmisx/open-iot-core/internal/shadow"
)

// 影子文档的持久化编码。
//
// 用 JSON 而非紧凑二进制，理由有三：
//   - 影子内容本身就是 JSON，再套一层二进制编码只是多一次转换；
//   - 影子不在热路径上（每 thing 20 次/秒，QTA-063）；
//   - 可以用通用工具直接检查库里的内容，排查影子语义问题时这很值。
//
// 带 v 字段使格式可演进；读到更高版本时拒绝而不是按旧语义解释。
type shadowRecord struct {
	V            int           `json:"v"`
	Desired      shadow.Object `json:"desired,omitempty"`
	Reported     shadow.Object `json:"reported,omitempty"`
	DesiredMeta  shadow.Object `json:"desiredMeta,omitempty"`
	ReportedMeta shadow.Object `json:"reportedMeta,omitempty"`
	Version      int64         `json:"version"`
	UpdatedAtSec int64         `json:"updatedAt"`
}

const shadowRecordVersion = 1

// ShadowStore 是 shadow.Store 的持久化实现。
//
// 它把 shadow 包的纯状态机套在 KV 之上：语义仍由 shadow.applyCommand
// 决定，本类型只负责"读出当前文档、应用命令、写回"这三步的原子性
// 与持久性。两者分开使语义可以被穷举测试，而不必搭一个存储。
type ShadowStore struct {
	kv KV

	// 固定条带锁实现同 thing 单写者。map[thing]*Mutex 会让写过一次的
	// 百万设备永久留下百万个键与 mutex；条带使这部分常驻内存固定。
	lanes [256]sync.Mutex
}

// NewShadowStore 创建持久化影子存储。
func NewShadowStore(kv KV) *ShadowStore {
	return &ShadowStore{kv: kv}
}

func (s *ShadowStore) lane(thing string) *sync.Mutex {
	// FNV-1a 的低 8 位用于选 256 条带；手写循环不构造 hasher。
	var h uint64 = 14695981039346656037
	for i := 0; i < len(thing); i++ {
		h ^= uint64(thing[i])
		h *= 1099511628211
	}
	return &s.lanes[byte(h)]
}

func shadowKey(thing, name string) []byte {
	return Key(PrefixShadow, thing, name)
}

// Get 读取一份影子。
func (s *ShadowStore) Get(thing, name string) (*shadow.Document, bool) {
	raw, err := s.kv.Get(shadowKey(thing, name))
	if err != nil {
		return nil, false
	}
	doc, err := decodeShadow(raw)
	if err != nil {
		// 记录损坏时按"不存在"处理会静默丢数据，但在读路径上
		// 没有返回错误的途径（shadow.Store 的接口如此）。
		// 这里的取舍是：读失败等同不存在，而写路径会在同一个键上
		// 覆盖掉损坏记录，使系统能自愈。损坏本身由 Verify 报告。
		return nil, false
	}
	return doc, true
}

// List 返回一个 thing 的全部命名影子名。
func (s *ShadowStore) List(thing string) []string {
	var out []string
	prefix := Key(PrefixShadow, thing)
	// 前缀须以分隔符结尾，否则 thing "d1" 会匹配到 "d10" 的影子。
	// 这是前缀扫描最容易出的错，且症状是"别的设备的影子出现在列表里"。
	prefix = append(prefix, KeySep)
	_ = s.kv.Scan(prefix, func(key, _ []byte) bool {
		parts, ok := SplitKey(key, PrefixShadow, 2)
		if !ok || parts[0] != thing || parts[1] == "" {
			return true
		}
		out = append(out, parts[1])
		return true
	})
	return out
}

// Apply 应用一条命令。
//
// 并发更新走 bbolt 的合并提交：每个调用仍然在自己的事务回调里完成
// 「读当前影子 -> 应用命令 -> 写回」，但底层可以把不同 thing 的回调装进
// 同一个写事务，只做一次 fsync。false 不表示异步或放松持久性；InTx 仍在
// 事务提交并同步落盘后才返回。它只允许底层等待一个有界窗口并合并并发写。
//
// lane 锁必须覆盖整个 InTx 调用，而不只覆盖事务回调。同一 thing 的后一条
// 命令只有在前一条已经提交后才能进入队列，否则 bbolt 在重试/拆批时可能改变
// 两条命令的执行顺序，破坏影子 version 与 expectedVersion 的语义。
//
// bbolt 的 Batch 在同批某个回调失败时可能重试其余回调，所以事务体必须幂等，
// 并且 Result 只能在 InTx 成功返回后交给调用方。applyShadowTo 每次都从传入的
// tx 重新读取状态，外部只保留最后一次执行产生的 Result，满足这个约束。
func (s *ShadowStore) Apply(_ context.Context, cmd shadow.Command) (*shadow.Result, error) {
	lane := s.lane(cmd.Thing)
	lane.Lock()
	defer lane.Unlock()
	var res *shadow.Result
	err := s.kv.InTx(false, func(tx Tx) error {
		var err error
		res, err = applyShadowTo(tx, cmd)
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ApplyTx 在调用方已有的写事务里应用影子命令。
// 调用方负责事务的提交与同步；批内后续命令能读到这里尚未提交的写入。
func (s *ShadowStore) ApplyTx(tx Tx, cmd shadow.Command) (*shadow.Result, error) {
	lane := s.lane(cmd.Thing)
	lane.Lock()
	defer lane.Unlock()
	return applyShadowTo(tx, cmd)
}

type shadowReadWriter interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
}

func applyShadowTo(dst shadowReadWriter, cmd shadow.Command) (*shadow.Result, error) {

	key := shadowKey(cmd.Thing, cmd.Shadow)
	var cur *shadow.Document
	raw, err := dst.Get(key)
	switch {
	case err == nil:
		if cur, err = decodeShadow(raw); err != nil {
			return nil, fmt.Errorf("store: 影子记录损坏 %s/%s: %w",
				cmd.Thing, cmd.Shadow, err)
		}
	case errors.Is(err, ErrNotFound):
	default:
		return nil, err
	}

	// cur 是刚从当前事务的私有字节解码出来的，没有任何外部观察者。
	// 交出所有权可避免为 Previous 与 Current 再深拷贝整棵影子树；next 在
	// 返回前已经编码入事务，之后只由不可变的响应路径读取。
	next, res, aerr := shadow.ApplyCommandOwned(cur, cmd)
	if aerr != nil {
		return nil, aerr
	}

	// 写入必须在 accepted 发出之前落盘。
	//
	// 影子更新一旦被确认，设备就认为状态已被接受；此时若因进程崩溃丢失，
	// 设备不会重发，云端与设备的认知就永久分叉。
	//
	// 这里不再有 per-call 的 sync 标志：写入经传入的 tx 进入外层事务，而
	// 那个事务（Apply 走 bbolt 的 Batch、分片状态机走整批提交）在提交时
	// fsync，InTx 在落盘之后才返回。合并并发写只省 fsync 次数，不放松
	// 这条约束——改动这一段时别把它读成「现在是异步写」。
	if next == nil {
		if err := dst.Delete(key); err != nil {
			return nil, err
		}
	} else {
		enc, err := encodeShadow(next)
		if err != nil {
			return nil, err
		}
		if err := dst.Set(key, enc); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Close 不关闭底层 KV：KV 的生命周期由装配方持有，
// 多个 Store 共享同一个 KV，任一方关闭它都会让其余的失效。
func (s *ShadowStore) Close() error { return nil }

// CountShadows 统计已持久化的影子数，供启动日志与指标使用。
func (s *ShadowStore) CountShadows() int {
	n := 0
	_ = s.kv.Scan([]byte{byte(PrefixShadow)}, func([]byte, []byte) bool {
		n++
		return true
	})
	return n
}

// PruneNonLocal 在 RF=all -> RF<N 切换后删除本节点不再承载的历史影子。
// layout 由已持久化的 Raft 放置表派生；同一布局只扫描一次。
func (s *ShadowStore) PruneNonLocal(layout string, keep func(thing []byte) bool) (
	deleted int, already bool, err error,
) {
	if layout == "" || keep == nil {
		return 0, false, errors.New("store: 影子裁剪缺少布局或归属函数")
	}
	marker := Key(PrefixSystem, "shadow-pruned-v1-"+layout)
	pruner, ok := s.kv.(interface {
		PruneMarked([]byte, []byte, func([]byte) (bool, error)) (int, bool, error)
	})
	if !ok {
		return 0, false, errors.New("store: 当前 KV 不支持原子影子裁剪")
	}
	return pruner.PruneMarked([]byte{byte(PrefixShadow)}, marker, func(key []byte) (bool, error) {
		if len(key) < 2 || key[0] != byte(PrefixShadow) {
			return false, nil
		}
		body := key[1:]
		sep := -1
		for i, b := range body {
			if b != KeySep {
				continue
			}
			if sep >= 0 {
				return false, nil
			}
			sep = i
		}
		// 损坏或未来版本的键不可因迁移被静默删除。
		return sep > 0 && !keep(body[:sep]), nil
	})
}

// ShadowAppliedKey 是独立影子 Raft 分片的持久化进度键。
func ShadowAppliedKey(shard uint64) []byte {
	return Key(PrefixSystem, fmt.Sprintf("shadow-applied-%d", shard))
}

func (s *ShadowStore) ShadowAppliedIndex(shard uint64) uint64 {
	raw, err := s.kv.Get(ShadowAppliedKey(shard))
	if err != nil || len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

// ShadowEntry 是一批已提交影子命令中的一条。Result/Err 在事务提交成功后
// 才可交付给请求方；Err 只承载确定性的影子 4xx，存储错误直接返回。
type ShadowEntry struct {
	Index  uint64
	Data   []byte
	Result *shadow.Result
	Err    error
}

// ApplyShadowBatch 在一个事务里顺序应用同一分片的一批命令，并把最终
// applied index 与文档一同提交。owns 防止错误路由让一个日志改写别组键。
func (s *ShadowStore) ApplyShadowBatch(shard uint64, ents []ShadowEntry,
	owns func(string) bool,
) error {
	if len(ents) == 0 {
		return nil
	}
	cmds := make([]shadow.Command, len(ents))
	for i := range ents {
		cmd, err := shadow.DecodeCommand(ents[i].Data)
		if err != nil {
			return err
		}
		if cmd.Thing == "" || owns == nil || !owns(cmd.Thing) {
			return fmt.Errorf("store: 影子命令 %q 不属于分片 %d", cmd.Thing, shard)
		}
		cmds[i] = cmd
	}
	return s.kv.InTx(false, func(tx Tx) error {
		for i := range ents {
			ents[i].Result, ents[i].Err = nil, nil
		}
		for i := range cmds {
			lane := s.lane(cmds[i].Thing)
			lane.Lock()
			res, err := applyShadowTo(tx, cmds[i])
			lane.Unlock()
			var domain *shadow.Error
			if err != nil && !errors.As(err, &domain) {
				return err
			}
			ents[i].Result, ents[i].Err = res, err
		}
		var applied [8]byte
		binary.BigEndian.PutUint64(applied[:], ents[len(ents)-1].Index)
		return tx.Set(ShadowAppliedKey(shard), applied[:])
	})
}

// SnapshotShadowShard 只输出由该分片拥有的 thing。命名影子与经典影子
// 共享 thingName 分区，因此 List 不需要跨分片扫描。
func (s *ShadowStore) SnapshotShadowShard(shard uint64, owns func(string) bool,
	w io.Writer,
) error {
	var head [8]byte
	binary.BigEndian.PutUint64(head[:], s.ShadowAppliedIndex(shard))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	var recordHeader [8]byte
	var writeErr error
	err := s.kv.Scan([]byte{byte(PrefixShadow)}, func(key, value []byte) bool {
		parts, ok := SplitKey(key, PrefixShadow, 2)
		if !ok || owns == nil || !owns(parts[0]) {
			return true
		}
		writeErr = writeKVHeader(w, key, value, &recordHeader)
		return writeErr == nil
	})
	if err != nil {
		return err
	}
	return writeErr
}

// RestoreShadowShard 用快照完整替换本分片键，并拒绝夹带其他分片的键。
func (s *ShadowStore) RestoreShadowShard(shard uint64, owns func(string) bool,
	r io.Reader,
) error {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return fmt.Errorf("store: 影子快照缺少头部: %w", err)
	}
	var ops []Op
	if err := s.kv.Scan([]byte{byte(PrefixShadow)}, func(key, _ []byte) bool {
		parts, ok := SplitKey(key, PrefixShadow, 2)
		if ok && owns != nil && owns(parts[0]) {
			ops = append(ops, Op{Kind: OpDelete, Key: append([]byte(nil), key...)})
		}
		return true
	}); err != nil {
		return err
	}
	for {
		key, value, err := readKV(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		parts, ok := SplitKey(key, PrefixShadow, 2)
		if !ok || owns == nil || !owns(parts[0]) {
			return fmt.Errorf("store: 影子快照含分片 %d 不拥有的键", shard)
		}
		ops = append(ops, Op{Kind: OpSet, Key: key, Value: value})
	}
	ops = append(ops, Op{Kind: OpSet, Key: ShadowAppliedKey(shard), Value: head[:]})
	return s.kv.Batch(ops, true)
}

func encodeShadow(d *shadow.Document) ([]byte, error) {
	rec := shadowRecord{
		V: shadowRecordVersion, Desired: d.Desired, Reported: d.Reported,
		DesiredMeta: d.DesiredMeta, ReportedMeta: d.ReportedMeta,
		Version: d.Version, UpdatedAtSec: d.UpdatedAtSec,
	}
	b, err := json.Marshal(&rec)
	if err != nil {
		return nil, fmt.Errorf("store: 编码影子失败: %w", err)
	}
	return b, nil
}

func decodeShadow(raw []byte) (*shadow.Document, error) {
	var rec shadowRecord
	dec := json.NewDecoder(bytesReaderOf(raw))
	// 数字必须解成 json.Number，否则存一轮回来 1.50 会变成 1.5、
	// 大整数会丢精度——那等于存储层偷偷改写了设备数据。
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("无法解析: %w", err)
	}
	if rec.V > shadowRecordVersion {
		return nil, fmt.Errorf("记录版本 %d 高于本程序支持的 %d",
			rec.V, shadowRecordVersion)
	}
	return &shadow.Document{
		Desired: rec.Desired, Reported: rec.Reported,
		DesiredMeta: rec.DesiredMeta, ReportedMeta: rec.ReportedMeta,
		Version: rec.Version, UpdatedAtSec: rec.UpdatedAtSec,
	}, nil
}
