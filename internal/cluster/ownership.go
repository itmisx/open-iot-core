package cluster

import (
	"context"
	"sync"
)

// EvictReason 说明一个会话为何被驱逐。
type EvictReason uint8

const (
	// EvictTakenOver 是同 clientId 的新连接抢占了所有权。
	// 对应 MQTT 5 原因码 0x8E Session taken over。
	EvictTakenOver EvictReason = iota
	// EvictAdministrative 是运维主动踢出。
	EvictAdministrative
	// EvictLeaseLost 是本节点失去了该分区的归属，无法继续担保所有权。
	EvictLeaseLost
)

// Eviction 是一次所有权被夺走的通知。
type Eviction struct {
	ClientID string
	// PrevEpoch 是被驱逐会话持有的 epoch。
	PrevEpoch Epoch
	// NewEpoch 是抢占者获得的 epoch；EvictLeaseLost 时为 ZeroEpoch。
	NewEpoch Epoch
	// By 是抢占方所在节点；EvictLeaseLost 时为空。
	By     NodeID
	Reason EvictReason
}

// SessionOwnership 管理 clientId 的所有权与围栏（契约 01）。
//
// MQTT 要求同 clientId 的后连接者胜、前者被断开。单机下这是一次 map 操作，
// 集群下是分布式互斥，且必须防止网络分区造成双活——epoch 是唯一的解法，
// 它必须贯穿整个会话层。
//
// 实现须保证：Claim 成功后，原持有者要么收到 Eviction，要么因租约过期失效。
type SessionOwnership interface {
	// Claim 抢占 clientID 的所有权并返回新 epoch。
	// 新 epoch 严格大于此前为该 clientID 发放过的任何 epoch。
	Claim(ctx context.Context, clientID string, node NodeID) (Epoch, error)

	// Release 释放所有权。若 e 不是当前 epoch 则返回 ErrStaleEpoch，
	// 这可防止被接管的旧会话在清理时误删新会话的所有权。
	Release(ctx context.Context, clientID string, e Epoch) error

	// Lookup 查询当前归属。
	Lookup(ctx context.Context, clientID string) (NodeID, Epoch, bool)

	// Evictions 返回本节点上被夺走所有权的 clientId 流。
	// 通道由实现关闭。
	Evictions() <-chan Eviction

	Close() error
}

// LocalOwnership 是 SessionOwnership 的单节点实现。
//
// 它不是旁路实现：epoch 的生成、校验与驱逐通知都走与集群一致的语义，
// 使 epoch 相关的代码路径在单机 CI 中即被持续覆盖（PRD ARCH-057）。
// term 固定为 1，因为单节点不会发生 leader 切换。
type LocalOwnership struct {
	self NodeID

	mu      sync.Mutex
	owners  map[string]ownerRec
	nextSeq uint32
	evict   chan Eviction
	closed  bool
}

type ownerRec struct {
	node  NodeID
	epoch Epoch
}

// localTerm 是单节点模式的固定 term。
const localTerm uint32 = 1

// NewLocalOwnership 创建单节点所有权表。
// evictBuf 是驱逐通知的缓冲深度；缓冲满时丢弃最旧的通知并继续，
// 因为所有权本身已经转移，通知只是加速旧会话的清理。
func NewLocalOwnership(self NodeID, evictBuf int) *LocalOwnership {
	if evictBuf <= 0 {
		evictBuf = 64
	}
	return &LocalOwnership{
		self:   self,
		owners: make(map[string]ownerRec),
		evict:  make(chan Eviction, evictBuf),
	}
}

func (o *LocalOwnership) Claim(_ context.Context, clientID string, node NodeID) (Epoch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ZeroEpoch, ErrClosed
	}
	o.nextSeq++
	ep := MakeEpoch(localTerm, o.nextSeq)
	prev, had := o.owners[clientID]
	o.owners[clientID] = ownerRec{node: node, epoch: ep}
	if had {
		o.notify(Eviction{
			ClientID:  clientID,
			PrevEpoch: prev.epoch,
			NewEpoch:  ep,
			By:        node,
			Reason:    EvictTakenOver,
		})
	}
	return ep, nil
}

func (o *LocalOwnership) Release(_ context.Context, clientID string, e Epoch) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	cur, had := o.owners[clientID]
	if !had {
		return nil
	}
	// 被接管的旧会话在清理时不得删掉新会话的所有权。
	if cur.epoch != e {
		return ErrStaleEpoch
	}
	delete(o.owners, clientID)
	return nil
}

func (o *LocalOwnership) Lookup(_ context.Context, clientID string) (NodeID, Epoch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec, ok := o.owners[clientID]
	if !ok {
		return "", ZeroEpoch, false
	}
	return rec.node, rec.epoch, true
}

func (o *LocalOwnership) Evictions() <-chan Eviction { return o.evict }

// Evict 主动驱逐一个会话，用于运维踢出与证书吊销后的强制断连。
// 返回被驱逐会话的 epoch。
func (o *LocalOwnership) Evict(clientID string, r EvictReason) (Epoch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ZeroEpoch, false
	}
	rec, ok := o.owners[clientID]
	if !ok {
		return ZeroEpoch, false
	}
	delete(o.owners, clientID)
	o.notify(Eviction{
		ClientID:  clientID,
		PrevEpoch: rec.epoch,
		NewEpoch:  ZeroEpoch,
		Reason:    r,
	})
	return rec.epoch, true
}

// Len 返回当前持有所有权的 clientId 数量。
func (o *LocalOwnership) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.owners)
}

func (o *LocalOwnership) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	close(o.evict)
	return nil
}

// notify 必须在持锁状态下调用。缓冲满时丢弃最旧的一条，
// 保证 Claim 永不阻塞——所有权转移是同步完成的，通知只是加速清理。
func (o *LocalOwnership) notify(e Eviction) {
	for {
		select {
		case o.evict <- e:
			return
		default:
			select {
			case <-o.evict:
			default:
				return
			}
		}
	}
}
