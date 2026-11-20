package cluster

import "sync"

// Mesh 是有界的节点间发送队列（CLU-062）。
//
// 队列有硬上限。对端跟不上时 Forward 返回 ErrBackpressure，
// 而不是继续把帧堆进内存。
type Mesh struct {
	mu    sync.Mutex
	limit int
	q     map[NodeID][]Frame
}

// NewMesh 创建队列。limit 是每个对端最多积压的帧数。
func NewMesh(limit int) *Mesh {
	if limit <= 0 {
		limit = 1
	}
	return &Mesh{limit: limit, q: map[NodeID][]Frame{}}
}

// Forward 把一批帧放进发往 to 的队列。放不下时整批拒绝，队列长度不变。
func (m *Mesh) Forward(to NodeID, batch []Frame) error {
	if len(batch) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.q[to])+len(batch) > m.limit {
		return ErrBackpressure
	}
	for _, f := range batch {
		f.Payload = append([]byte(nil), f.Payload...)
		f.Props = append([]byte(nil), f.Props...)
		m.q[to] = append(m.q[to], f)
	}
	return nil
}

// Pending 返回某个对端队列里的帧数。
func (m *Mesh) Pending(to NodeID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.q[to])
}
