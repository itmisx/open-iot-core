// Package router 实现消息路由（契约 02）与全局订阅状态（契约 10）。
//
// 契约 02 的要点：Route 从 v0.1 起就返回本地与远端两份目标。
// 单机下 remote 恒为空，但投递器的两条分支必须都存在——事后插入
// 「序列化加转发」这一步时，整条路径已经假设目标是本地指针，
// 届时 retained 下发、遗嘱、共享订阅、Shadow 回包全部要改。
package router

import (
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// InterestSet 记录远端节点的订阅兴趣。
//
// 只有通配符订阅需要跨节点传播（契约 03、ARCH-017）。设备域主题的归属
// 由 thingName 哈希直接算出，完全不进入这里——这是本项目相对通用 broker
// 的主要性能杠杆：EMQX 把整棵订阅树全局复制，在百万级不同 filter 下有
// 天花板，而我们不付这个代价。
//
// 兴趣传播的具体编码（全量 filter 列表、bloom、cuckoo 摘要）是可延后的
// 实现细节（ARCH-047），只要 Match 的签名不变。此处为最朴素的全量列表。
type InterestSet struct {
	mu sync.RWMutex
	// 过滤器以 []byte 保存，使匹配路径不需要每次从 string 转换。
	byNode map[cluster.NodeID][][]byte
	limits topic.Limits
}

// NewInterestSet 创建远端兴趣集。
func NewInterestSet(l topic.Limits) *InterestSet {
	return &InterestSet{byNode: make(map[cluster.NodeID][][]byte), limits: l}
}

// Advertise 替换某节点的兴趣摘要。
//
// 整体替换而非增量更新：摘要是幂等的全量快照，这使得通告丢失或重排
// 都不会让兴趣集永久偏离——最终一致即可（ARCH-012 的平面划分）。
func (s *InterestSet) Advertise(node cluster.NodeID, filters []string) {
	// 精确订阅留在本节点。跨节点只传播带通配符的过滤器（ARCH-017）。
	cp := make([][]byte, 0, len(filters))
	for _, f := range filters {
		b := []byte(f)
		if topic.HasWildcard(b) {
			cp = append(cp, b)
		}
	}
	s.mu.Lock()
	if len(cp) == 0 {
		delete(s.byNode, node)
	} else {
		s.byNode[node] = cp
	}
	s.mu.Unlock()
}

// Forget 移除某节点的兴趣，用于成员离开。
func (s *InterestSet) Forget(node cluster.NodeID) {
	s.mu.Lock()
	delete(s.byNode, node)
	s.mu.Unlock()
}

// MatchInto 把对该主题有兴趣的远端节点追加到 dst。
//
// name 为 []byte 而非 string：主题从解码缓冲区直接传入，
// 整条路由路径端到端使用 []byte，避免边界上的转换分配（LANG-007）。
// 传入复用的切片可避免分配。结果已去重（每节点至多出现一次）。
func (s *InterestSet) MatchInto(dst []cluster.NodeID, name []byte) []cluster.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for node, filters := range s.byNode {
		for _, f := range filters {
			if topic.Match(f, name, s.limits) {
				dst = append(dst, node)
				break
			}
		}
	}
	return dst
}

// Nodes 返回已通告兴趣的节点数。
func (s *InterestSet) Nodes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byNode)
}
