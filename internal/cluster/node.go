// Package cluster 定义跨节点关注点的接口，并提供它们的单节点实现。
//
// 本包是 PRD 原则 ARCH-001「集群是接口，单机是实现」的落地：单机不是
// 集群之前的版本，而是节点数为一的集群，两者共用同一条代码路径。
// 单节点实现不是旁路——它必须走完整的 epoch 校验与归属判定分支，
// 这样集群代码路径在 v0.1 就是被持续测试的。
package cluster

import (
	"errors"
	"hash/fnv"
)

// NodeID 是集群内节点的稳定标识。持久化于 data dir 的 manifest，
// 重启与运行期均不可变（PRD CLU-033）。
type NodeID string

// Epoch 是 clientId 所有权的单调围栏令牌（契约 01）。
//
// 高 32 位是产生它的分区 Raft term，低 32 位是该 term 内 leader 的本地序号：
//
//	epoch = (term << 32) | seq
//
// 正确性依赖两点：同一 term 内序号由单一 leader 递增，故单调；leader 切换后
// term 严格增大，即使新 leader 从 0 开始计序号，epoch 仍严格大于旧 term 的
// 任何取值。因此网络分区中仍自认 leader 的旧节点所发放的 epoch 必然更小，
// 会在校验中被拒，不可能出现同一 clientId 双活授权。
type Epoch uint64

// ZeroEpoch 表示「尚无所有权」。任何有效 epoch 都大于它。
const ZeroEpoch Epoch = 0

// MakeEpoch 由 term 与序号组装 epoch。序号从 1 开始，使 ZeroEpoch 保持无效。
func MakeEpoch(term, seq uint32) Epoch {
	return Epoch(uint64(term)<<32 | uint64(seq))
}

// Term 返回产生该 epoch 的 Raft term。
func (e Epoch) Term() uint32 { return uint32(e >> 32) }

// Seq 返回 term 内的本地序号。
func (e Epoch) Seq() uint32 { return uint32(e) }

// Valid 报告 epoch 是否代表一次真实的所有权授予。
func (e Epoch) Valid() bool { return e != ZeroEpoch }

// ErrStaleEpoch 表示操作携带的 epoch 已被更大的 epoch 取代。
//
// 所有改变会话状态的操作都必须校验 epoch 并在不匹配时返回本错误，
// 单机下的本地操作也不例外（PRD ARCH-011）。
var ErrStaleEpoch = errors.New("cluster: epoch 已过期，会话已被接管")

// ErrNotOwner 表示本节点不是该键的归属者，调用方应转发给 Owner。
var ErrNotOwner = errors.New("cluster: 本节点不是该键的归属节点")

// ErrClosed 表示组件已关闭。
var ErrClosed = errors.New("cluster: 组件已关闭")

// hashKey 为分区与路由提供稳定的 64 位哈希。
// 必须在所有节点上给出相同结果，因此固定为 FNV-1a 而非 Go 的 map 哈希
// （后者每进程随机加盐）。
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// ShardOf 把键映射到固定分区。同一键在所有节点上落到同一分区。
func ShardOf(key string, shards int) int {
	if shards <= 0 {
		return 0
	}
	return int(hashKey(key) % uint64(shards))
}

// MonotonicVersion 用提交序号作为影子和保留消息的版本。
// 新 leader 的序号只会更大，版本不会回头。
func MonotonicVersion(prev, commit uint64) (uint64, error) {
	if commit <= prev {
		return 0, errors.New("cluster: 版本没有前进")
	}
	return commit, nil
}
