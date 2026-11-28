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

// Save 写入会话状态。
//
// `st.Queued` 为 nil 表示**这次不碰队列**，空切片表示「队列应当是空的」
// ——与 store.SessionStore.Save 同一条约定（那边的理由写在它自己的注释里：
// 少了这个区别，任何一个只想改订阅的 Save 都会把离线队列静默清掉）。
//
// 这里原先没实现它：Clone 无条件把 Queued 做成**非 nil 的空切片**，于是
// 一次带 nil 的 Save 在内存实现上等于清空队列，而在持久实现上等于不动。
// 两个实现对同一个调用给出不同结果，而用例大多跑在内存那份上——于是
// 「盘上会不会丢」这件事在测试里根本看不出来（MQTT-135 的修复撞上了它：
// 按约定传 nil 之后，内存实现把队列清了，用例当场红）。
func (m *MemStore) Save(_ context.Context, st *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkWriteEpoch(st); err != nil {
		return err
	}
	next := st.Clone()
	if st.Queued == nil {
		// 接回原来那份；没有旧记录时保持 nil（空队列）。
		if cur, ok := m.byID[st.ClientID]; ok {
			next.Queued = cur.Queued
		} else {
			next.Queued = nil
		}
	}
	m.byID[st.ClientID] = next
	return nil
}

func (m *MemStore) Replace(_ context.Context, st *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Replace 只是 clean start 所需的「清队列+写新状态」原子性，
	// 不能绕过 Save 的 epoch 围栏。否则被接管的旧连接可以用
	// Clean Start 覆盖已经落盘的新一代会话。
	if err := m.checkWriteEpoch(st); err != nil {
		return err
	}
	m.byID[st.ClientID] = st.Clone()
	return nil
}

// checkWriteEpoch 在 m.mu 持有期间执行 Save/Replace 共用的围栏。
func (m *MemStore) checkWriteEpoch(st *State) error {
	cur, ok := m.byID[st.ClientID]
	if !ok {
		return nil
	}
	// 已被接管的旧会话不得覆盖新会话的状态。
	if st.Epoch < cur.Epoch {
		return cluster.ErrStaleEpoch
	}
	// epoch 相等时「活的压过死的」，与持久实现同一条判据
	// （见 store.SessionStore.staleAtEqualEpoch，CLU-131）。
	// 三个实现必须一致：不一致的话，broker 的用例在内存实现上
	// 全绿而集群里是另一套语义。
	if st.Epoch == cur.Epoch && st.Owner != cur.Owner &&
		st.DisconnectUnixMs != 0 && cur.DisconnectUnixMs == 0 {
		return cluster.ErrStaleEpoch
	}
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
