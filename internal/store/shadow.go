package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	// 每 thing 一把锁，实现契约 06 的分区单写者。
	//
	// 锁的粒度是 thing 而不是全局：不同设备之间没有共享状态，
	// 用一把全局锁会让影子吞吐与设备数无关。
	mu    sync.Mutex
	lanes map[string]*sync.Mutex
}

// NewShadowStore 创建持久化影子存储。
func NewShadowStore(kv KV) *ShadowStore {
	return &ShadowStore{kv: kv, lanes: make(map[string]*sync.Mutex)}
}

func (s *ShadowStore) lane(thing string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.lanes[thing]
	if m == nil {
		m = &sync.Mutex{}
		s.lanes[thing] = m
	}
	return m
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
func (s *ShadowStore) Apply(_ context.Context, cmd shadow.Command) (*shadow.Result, error) {
	lane := s.lane(cmd.Thing)
	lane.Lock()
	defer lane.Unlock()

	key := shadowKey(cmd.Thing, cmd.Shadow)
	var cur *shadow.Document
	raw, err := s.kv.Get(key)
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

	next, res, aerr := shadow.ApplyCommand(cur, cmd)
	if aerr != nil {
		return nil, aerr
	}

	// 落盘用 sync：影子更新一旦被确认（accepted 已发出），设备就认为
	// 状态已被接受。此时若因进程崩溃丢失，设备不会重发，
	// 云端与设备的认知就永久分叉了。
	if next == nil {
		if err := s.kv.Delete(key, true); err != nil {
			return nil, err
		}
	} else {
		enc, err := encodeShadow(next)
		if err != nil {
			return nil, err
		}
		if err := s.kv.Set(key, enc, true); err != nil {
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
