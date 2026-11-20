package store

import (
	"encoding/json"
	"fmt"

	"github.com/itmisx/open-iot-core/internal/router"
)

// retainedRecordVersion 是记录格式版本。
//
// 2 起 Version 由计数器加节点 ID 两半组成（MQTT-123）。V=1 的旧记录
// 不需要迁移：它的 VNode 解出来是空串，而空串在平手判据里排在任何真实
// 节点 ID 之前，计数器的相对顺序又原样保留——于是旧数据与新数据能直接
// 比较，重启后也不会被一条更旧的记录盖掉。
const retainedRecordVersion = 2

// retainedRecord 是保留消息的持久化编码。
//
// 逐字段列出而不是内嵌 router.Message：后者含未导出字段（topicB 是
// 主题的字节形式缓存），序列化会丢掉它们，而反序列化时又不会报错——
// 那种"恢复出来的消息缺了内部缓存"的 bug 只在首次匹配时才崩。
type retainedRecord struct {
	V              int    `json:"v"`
	Topic          string `json:"topic"`
	Payload        []byte `json:"payload"`
	Props          []byte `json:"props,omitempty"`
	QoS            uint8  `json:"qos"`
	Version        uint64 `json:"version"`
	VNode          string `json:"vnode,omitempty"`
	ExpiryUnixMs   int64  `json:"expiry,omitempty"`
	StoredAtUnixMs int64  `json:"storedAt,omitempty"`
}

// RetainedStore 持久化保留消息，实现 router.RetainedSink。
type RetainedStore struct {
	kv KV
}

// NewRetainedStore 创建持久化保留消息存储。
func NewRetainedStore(kv KV) *RetainedStore { return &RetainedStore{kv: kv} }

func retainedKey(topic string) []byte {
	return Key(PrefixRetained, topic)
}

// PutRetained 实现 router.RetainedSink。
func (s *RetainedStore) PutRetained(topic string, m *router.Message, v router.Version) error {
	rec := retainedRecord{
		V: retainedRecordVersion, Topic: topic,
		Payload: m.Payload, Props: m.Props, QoS: m.QoS,
		Version: v.Counter, VNode: v.Node, ExpiryUnixMs: m.ExpiryUnixMs,
		StoredAtUnixMs: m.StoredAtUnixMs,
	}
	enc, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("store: 编码保留消息失败: %w", err)
	}
	// 不用 sync：保留消息丢一条的后果是新订阅者少收到一条历史消息，
	// 远轻于会话或影子丢失。换来的是发布路径不被每条消息的 fsync 拖住。
	// bbolt 的 Batch 仍然是持久的——它合并提交，不是不落盘。
	return s.kv.Set(retainedKey(topic), enc, false)
}

// DeleteRetained 实现 router.RetainedSink。
func (s *RetainedStore) DeleteRetained(topic string) error {
	return s.kv.Delete(retainedKey(topic), false)
}

// LoadInto 把持久化的保留消息装回内存存储，返回条数与最大版本号。
//
// 最大版本号必须回传给 Router.SeedVersion：计数器从 0 重新开始会让
// 新发布的保留消息被恢复出来的旧消息挡住（见 router.SeedVersion）。
//
// 用 Replace 整体替换而不是逐条 Set，有两个理由：Set 现在会触发跨节点
// 复制（MQTT-122），而这些数据正是本节点自己的、广播出去没有意义；
// 整体替换也让"快照安装后重载"（CLU-107）与启动装载走同一条路——装到
// 一半的中间状态不会被订阅者看见。
func (s *RetainedStore) LoadInto(dst *router.RetainedStore) (int, router.Version, error) {
	entries, maxV, err := s.Collect()
	if err != nil {
		return 0, router.Version{}, err
	}
	dst.Replace(entries)
	return len(entries), maxV, nil
}

// Collect 读出全部持久化的保留消息（含墓碑）与其中的最大版本号。
func (s *RetainedStore) Collect() ([]*router.Message, router.Version, error) {
	var out []*router.Message
	var maxV router.Version
	var decodeErr error
	err := s.kv.Scan([]byte{byte(PrefixRetained)}, func(_, value []byte) bool {
		rec, derr := decodeRetained(value)
		if derr != nil {
			decodeErr = derr
			return false
		}
		v := router.Version{Counter: rec.Version, Node: rec.VNode}
		out = append(out, &router.Message{
			Topic: rec.Topic, Payload: rec.Payload, Props: rec.Props,
			QoS: rec.QoS, Retain: true, Version: v,
			ExpiryUnixMs: rec.ExpiryUnixMs, StoredAtUnixMs: rec.StoredAtUnixMs,
		})
		if v.After(maxV) {
			maxV = v
		}
		return true
	})
	if err != nil {
		return nil, router.Version{}, err
	}
	if decodeErr != nil {
		return nil, router.Version{}, fmt.Errorf("store: 保留消息记录损坏: %w", decodeErr)
	}
	return out, maxV, nil
}

// Count 返回已持久化的保留消息条数，**不含墓碑**。
//
// 要解码才能分辨墓碑，而这里本来只数键。多出来的解码是值得的：这个数字
// 出现在控制台概览与 Admin 的统计里，把墓碑算进去会让已经清除的主题继续
// 计数，看起来像保留消息泄漏（与 router.RetainedStore.Len 同一个理由）。
func (s *RetainedStore) Count() int {
	n := 0
	_ = s.kv.Scan([]byte{byte(PrefixRetained)}, func(_, value []byte) bool {
		rec, err := decodeRetained(value)
		// 解不开的记录照旧计数：它是坏数据，不是墓碑，掩盖掉更糟。
		if err != nil || len(rec.Payload) > 0 {
			n++
		}
		return true
	})
	return n
}

func decodeRetained(raw []byte) (*retainedRecord, error) {
	var rec retainedRecord
	dec := json.NewDecoder(bytesReaderOf(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("无法解析: %w", err)
	}
	if rec.V > retainedRecordVersion {
		return nil, fmt.Errorf("记录版本 %d 高于本程序支持的 %d",
			rec.V, retainedRecordVersion)
	}
	if rec.Topic == "" {
		return nil, fmt.Errorf("记录缺少主题")
	}
	return &rec, nil
}
