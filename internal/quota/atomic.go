package quota

import "sync/atomic"

// 轻量的原子计数包装，使统计字段的读写意图在调用处清晰可见。

type atomicU64 struct{ v atomic.Uint64 }

func (a *atomicU64) add(n uint64) { a.v.Add(n) }
func (a *atomicU64) load() uint64 { return a.v.Load() }

type atomicI64 struct{ v atomic.Int64 }

func (a *atomicI64) add(n int64) { a.v.Add(n) }
func (a *atomicI64) load() int64 { return a.v.Load() }
