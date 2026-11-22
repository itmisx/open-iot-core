package store

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
)

// MemKV 是 KV 的内存实现。
//
// 它存在的理由不只是测试快：它让 KV 接口的语义有第二个实现来对照。
// 只有一个实现的接口，其抽象边界往往是假的——所有隐含假设都会悄悄
// 依赖那唯一实现的行为，直到真要替换时才暴露。
//
// 它同时也是"不需要持久化"部署的可用选项（纯转发、无状态的边缘节点）。
type MemKV struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemKV 创建内存存储。
func NewMemKV() KV {
	kv := &MemKV{data: make(map[string][]byte)}
	_ = checkFormatVersion(kv)
	return kv
}

func (m *MemKV) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	v, ok := m.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	// 与持久实现保持同一约定：返回副本。
	// 两个实现若在这点上不同，依赖它的代码会在换实现时出错。
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (m *MemKV) Set(key, value []byte, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	v := make([]byte, len(value))
	copy(v, value)
	m.data[string(key)] = v
	return nil
}

func (m *MemKV) Delete(key []byte, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	delete(m.data, string(key))
	return nil
}

func (m *MemKV) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrClosed
	}
	// 在锁内把匹配的键值一并取出，避免回调期间持锁——
	// 回调可能很慢，甚至回头调用本存储。
	type kvPair struct {
		k string
		v []byte
	}
	pairs := make([]kvPair, 0, 16)
	for k, v := range m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			cp := make([]byte, len(v))
			copy(cp, v)
			pairs = append(pairs, kvPair{k, cp})
		}
	}
	m.mu.RUnlock()

	// 升序，与 bbolt 的游标顺序一致。顺序不一致会让依赖扫描顺序的
	// 代码在两个实现间表现不同，而那种差异极难追。
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	for _, p := range pairs {
		if !fn([]byte(p.k), p.v) {
			break
		}
	}
	return nil
}

func (m *MemKV) Batch(ops []Op, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	// 先校验再应用，使批量写入保持原子：中途失败不应留下部分写入
	for i := range ops {
		if ops[i].Kind != OpSet && ops[i].Kind != OpDelete {
			return fmt.Errorf("store: 未知的操作类型 %d", ops[i].Kind)
		}
	}
	for i := range ops {
		switch ops[i].Kind {
		case OpSet:
			v := make([]byte, len(ops[i].Value))
			copy(v, ops[i].Value)
			m.data[string(ops[i].Key)] = v
		case OpDelete:
			delete(m.data, string(ops[i].Key))
		}
	}
	return nil
}

// memTx 在内存实现上提供事务语义。
//
// 做法是"覆盖层 + 整批落地"：写先进 pending，读先查 pending 再回落到
// 主表，fn 返回错误时直接丢掉 pending。不直接改主表是因为 InTx 的约定
// 是出错回滚——而回滚一张 map 要么留快照要么留日志，覆盖层是两者里更
// 便宜的那个（它的大小是这一批的写入数，而不是全库）。
type memTx struct {
	m       *MemKV
	pending map[string][]byte // nil 值表示这个键在本事务内被删了
}

func (t *memTx) Get(key []byte) ([]byte, error) {
	if v, ok := t.pending[string(key)]; ok {
		if v == nil {
			return nil, ErrNotFound
		}
		return append([]byte(nil), v...), nil
	}
	v, ok := t.m.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (t *memTx) Set(key, value []byte) error {
	t.pending[string(key)] = append([]byte(nil), value...)
	return nil
}

func (t *memTx) Delete(key []byte) error {
	t.pending[string(key)] = nil
	return nil
}

func (t *memTx) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	// 把主表与覆盖层合起来再排序：Scan 的约定是**升序**，而遍历两张
	// map 的顺序是随机的。会话的入队序号就是靠升序扫出来的（取最后
	// 一条的序号加一），顺序错了就是撞号。
	merged := make(map[string][]byte, len(t.m.data)+len(t.pending))
	for k, v := range t.m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			merged[k] = v
		}
	}
	for k, v := range t.pending {
		if !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		if v == nil {
			delete(merged, k)
			continue
		}
		merged[k] = v
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !fn([]byte(k), merged[k]) {
			return nil
		}
	}
	return nil
}

func (m *MemKV) InTx(_ bool, fn func(Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	tx := &memTx{m: m, pending: map[string][]byte{}}
	if err := fn(tx); err != nil {
		return err
	}
	for k, v := range tx.pending {
		if v == nil {
			delete(m.data, k)
			continue
		}
		m.data[k] = v
	}
	return nil
}

// PruneMarked 与持久实现保持同样的原子语义，主要用于测试和无状态部署。
func (m *MemKV) PruneMarked(prefix, marker []byte,
	remove func(key []byte) (bool, error),
) (int, bool, error) {
	return pruneMarkedTx(m, prefix, marker, remove)
}

func (m *MemKV) Flush() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *MemKV) Close() error {
	m.mu.Lock()
	m.closed = true
	m.data = nil
	m.mu.Unlock()
	return nil
}
