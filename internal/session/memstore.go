package session

import (
	"context"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// MemStore 是 Store 的单节点内存实现。
//
// 它**不是**生产形态：进程退出即会话与离线队列全丢。实际跑起来的节点用
// `store.SessionStore`（单机，落盘）或 `store.ReplicatedSessionStore`
// （集群，经会话分区组的 raft 日志复制，CLU-111）。这里留着它是因为
// Store 这个接口要有一个无依赖的实现——broker 的用例几乎都用它，而让它们
// 各自开一个 bbolt 只会让包内测试变慢且需要临时目录。
//
// 原先这段注释写的是「这是默认的持久队列后端：节点故障时离线队列丢失，
// 该取舍必须在用户文档首屏声明（ARCH-005）」。那半句随 CLU-111 作废——
// 离线队列现在跨节点复制、节点故障不丢；ARCH-005 要声明的改成另外三条
// （节点挂着期间不再入队、在途不复制、retained 最终一致），见该需求的
// 修订说明。
type MemStore struct {
	mu   sync.RWMutex
	byID map[string]*State
}

// NewMemStore 创建内存会话存储。
func NewMemStore() *MemStore {
	return &MemStore{byID: make(map[string]*State)}
}

func (m *MemStore) Load(_ context.Context, clientID string) (*State, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.byID[clientID]
	if !ok {
		return nil, false, nil
	}
	// 返回拷贝，避免调用方绕过 epoch 校验直接改动存储中的状态。
	return st.Clone(), true, nil
}

func (m *MemStore) Save(_ context.Context, st *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 已被接管的旧会话不得覆盖新会话的状态。
	if cur, ok := m.byID[st.ClientID]; ok && st.Epoch < cur.Epoch {
		return cluster.ErrStaleEpoch
	}
	m.byID[st.ClientID] = st.Clone()
	return nil
}

func (m *MemStore) Delete(_ context.Context, clientID string, e cluster.Epoch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.byID[clientID]
	if !ok {
		return nil
	}
	if cur.Epoch != e {
		return cluster.ErrStaleEpoch
	}
	delete(m.byID, clientID)
	return nil
}

func (m *MemStore) Expire(_ context.Context, nowUnixMs int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, st := range m.byID {
		if st.Expired(nowUnixMs) {
			delete(m.byID, id)
			n++
		}
	}
	return n, nil
}

// Len 返回存储中的会话数。
func (m *MemStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byID)
}

// AppendQueued 往离线队列尾部追加一条消息。
//
// 内存实现里"队列单独存"这件事没有意义（整个 State 就在内存里），
// 所以直接改那个切片——持久实现那边它是独立的记录（见
// store.SessionStore.AppendQueued 的说明）。
func (m *MemStore) AppendQueued(clientID string, q QueuedMsg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.byID[clientID]
	if !ok {
		// 会话不存在时不报错：调用方（投递路径）刚刚读到它存在，
		// 中间它可能已经过期或被清掉。丢这一条比让发布失败好。
		return nil
	}
	st.Queued = append(st.Queued, q)
	return nil
}

// TakeQueued 取出并清空离线队列。
func (m *MemStore) TakeQueued(clientID string) ([]QueuedMsg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.byID[clientID]
	if !ok || len(st.Queued) == 0 {
		return nil, nil
	}
	out := st.Queued
	st.Queued = nil
	return out, nil
}
