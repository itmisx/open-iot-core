package cluster

// Partitioner 决定某个键的归属节点（契约 06）。
//
// Shadow 的 version 自增、delta 计算、409 冲突判定要求 per-thing 的单写者
// 语义，Jobs 的 execution 状态机同理。集群下这意味着按 thingName 分区，
// 并把请求路由到归属节点；分区 leader 同时就是 Raft leader，
// 因此 version 可直接由 Raft log index 派生，天然单调且跨 leader 变更安全。
//
// 分区数在集群创建时确定且之后不可变更（PRD CLU-006、ARCH-026），
// 因为改变哈希环需要数据迁移。
type Partitioner interface {
	// Owner 返回键的归属节点。
	Owner(key string) NodeID
	// IsLocal 报告键是否由本节点负责。
	//
	// 调用方必须在 v0.1 就写出「本地直接处理，否则转发 Owner」两条分支，
	// 单机下转发分支不会进入，但它必须存在——事后插入这条判断等于把
	// Shadow 与 Jobs 服务重写一遍（契约 06 的延后代价）。
	IsLocal(key string) bool
	// Shard 返回键所属的分区号，便于指标按分区下钻。
	Shard(key string) uint32
	// Count 返回分区总数。
	Count() int
}

// LocalPartitioner 是 Partitioner 的单节点实现：所有分区都归本节点。
//
// 它仍然真实计算分区号，使按分区聚合的指标在单机下就能工作，
// 也让分区相关的代码路径被持续覆盖。
type LocalPartitioner struct {
	self  NodeID
	count uint32
}

// DefaultShardCount 是默认分区数（PRD CLU-006）。
// 64 在 3 到 20 节点区间都合理。
const DefaultShardCount = 64

// NewLocalPartitioner 创建单节点分区器。count 小于 1 时取 DefaultShardCount。
func NewLocalPartitioner(self NodeID, count int) *LocalPartitioner {
	if count < 1 {
		count = DefaultShardCount
	}
	return &LocalPartitioner{self: self, count: uint32(count)}
}

func (p *LocalPartitioner) Owner(string) NodeID { return p.self }

func (p *LocalPartitioner) IsLocal(string) bool { return true }

func (p *LocalPartitioner) Shard(key string) uint32 {
	return uint32(hashKey(key) % uint64(p.count))
}

func (p *LocalPartitioner) Count() int { return int(p.count) }
