package cluster

import "errors"

/*
复制组对外的错误与成员类型。

单独一个文件：这些标识符被 broker、meta、server 与本包的多处引用，
而共识实现本身是可替换的（LANG-015）。原先它们混在自写的那套 Raft
实现里，于是"换共识库"顺带把一堆与共识无关的类型一起带走了。
*/

var (
	// ErrNotLeader 本节点不是 leader。上层据此转发给 leader，
	// 而不是把它变成 API 的错误（CLU-071）。
	ErrNotLeader = errors.New("cluster: 不是 leader，写入被拒绝")
	// ErrNoQuorum 没有多数派。写入失败而读取照常（CLU-042）：
	// 设备连接、订阅、收发都只读本地视图。
	ErrNoQuorum = errors.New("cluster: 没有多数派，写入被拒绝")
	// ErrDisk 日志落盘失败。
	ErrDisk = errors.New("cluster: 日志落盘失败")
	// ErrDraining 节点正在排空，不再接受新写入（CLU-063）。
	ErrDraining = errors.New("cluster: 节点正在排空，拒绝新写入")
	// ErrBackpressure 对端的发送队列已满（CLU-062）。
	// 对端跟不上时要拒绝，而不是把帧堆进内存。
	ErrBackpressure = errors.New("cluster: 对端队列已满")

	// ErrNotMember 要移除的节点不在成员列表里。
	ErrNotMember = errors.New("cluster: 该节点不是成员")
	// ErrAlreadyMember 要加入的节点已经是成员。
	ErrAlreadyMember = errors.New("cluster: 该节点已经是成员")
	// ErrLastMember 不能移除最后一个成员：那会让这个组再也选不出
	// leader，日志从此无法提交。
	ErrLastMember = errors.New("cluster: 不能移除最后一个成员")
	// ErrConfInFlight 上一次成员变更还没提交。
	//
	// 换成库管的共识之后这条基本不会出现：库把配置变更串行化在
	// 自己的通道上，第二次变更是排队而不是报错。保留它是因为管理面的
	// 错误映射需要一个"此刻做不了"的 503 分类（见 writeMemberError），
	// 而两次变更同时在途在 Raft 里确实是不安全的（新旧配置的多数派
	// 可能不相交）。
	ErrConfInFlight = errors.New("cluster: 上一次成员变更尚未提交")
)

// Member 是复制组的一个成员。
//
// 地址随成员列表一起复制（CLU-090）：新加入的节点不出现在别人的配置
// 文件里，别人只能从成员列表里知道怎么连它。
type Member struct {
	ID   NodeID
	Addr string
}
