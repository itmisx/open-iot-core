package cluster

import "sync"

// TermFence 按任期发放 clientId 的所有权（CLU-061）。
//
// 只有当前 leader 可以发放。任期升高之后，新 epoch 严格大于旧任期的
// 任何取值，旧 epoch 不能再释放或代表这份所有权。
type TermFence struct {
	mu     sync.Mutex
	term   uint32
	seq    uint32
	leader bool
	owners map[string]ownerRec
}

// NewTermFence 创建围栏。leader 为假时 Claim 直接拒绝。
func NewTermFence(leader bool) *TermFence {
	return &TermFence{leader: leader, owners: map[string]ownerRec{}}
}

// SetLeader 切换本节点是否还在发放所有权。
func (f *TermFence) SetLeader(v bool) {
	f.mu.Lock()
	f.leader = v
	f.mu.Unlock()
}

// SetTerm 抬高任期。任期不允许回头。
func (f *TermFence) SetTerm(term uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if term == 0 || term < f.term {
		return ErrStaleEpoch
	}
	if term > f.term {
		f.term = term
		f.seq = 0
	}
	return nil
}

// Claim 为 clientID 发放新 epoch。非 leader 返回 ErrNotLeader。
func (f *TermFence) Claim(clientID string, node NodeID) (Epoch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leader {
		return ZeroEpoch, ErrNotLeader
	}
	if f.term == 0 {
		return ZeroEpoch, ErrNotLeader
	}
	f.seq++
	ep := MakeEpoch(f.term, f.seq)
	f.owners[clientID] = ownerRec{node: node, epoch: ep}
	return ep, nil
}

// Release 释放所有权。epoch 不是当前值时返回 ErrStaleEpoch。
func (f *TermFence) Release(clientID string, e Epoch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.owners[clientID]
	if !ok {
		return nil
	}
	if cur.epoch != e {
		return ErrStaleEpoch
	}
	delete(f.owners, clientID)
	return nil
}

// Lookup 返回当前归属。
func (f *TermFence) Lookup(clientID string) (NodeID, Epoch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.owners[clientID]
	if !ok {
		return "", ZeroEpoch, false
	}
	return rec.node, rec.epoch, true
}
