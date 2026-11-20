package node

import (
	"fmt"
)

// IdentityRequest 是期望的节点身份。
type IdentityRequest struct {
	// NodeID 为空表示首次启动时生成。
	NodeID string
	// ClusterID 为空表示 bootstrap 或 standalone 时生成。
	ClusterID string
	// ShardCount 来自配置。
	ShardCount int
	// AllowCreate 为真时允许初始化一个空的 data dir。
	//
	// `openiotd init` 置为真；`serve` 置为假——serve 遇到未初始化的
	// data dir 应当明确报错并提示先 init，而不是悄悄建一个新集群
	// （CLU-055）。一个因为路径打错而自动新建的空集群，症状是
	// "所有设备都认证失败"，而真因（连错了目录）完全看不出来。
	AllowCreate bool
	// Bootstrap 为真表示创建新集群，要求 data dir 为空（PRD CLU-031）。
	Bootstrap bool
	// NowUnixMs 是注入的时钟（契约 08）。
	NowUnixMs int64
}

// ErrNotInitialized 表示 data dir 尚未初始化。
type ErrNotInitialized struct {
	Dir string
}

func (e *ErrNotInitialized) Error() string {
	return fmt.Sprintf("数据目录 %s 尚未初始化；请先执行 `openiotd init --data-dir %s`",
		e.Dir, e.Dir)
}

// ErrIdentityConflict 表示配置与已持久化的身份冲突（PRD CLU-031）。
type ErrIdentityConflict struct {
	// Field 是冲突的字段。
	Field string
	// OnDisk 与 Configured 分别是磁盘上与配置中的值。
	OnDisk, Configured string
	// Hint 说明如何处理。
	Hint string
}

func (e *ErrIdentityConflict) Error() string {
	return fmt.Sprintf("配置与数据目录中已持久化的身份冲突：%s 在磁盘上为 %s，"+
		"配置为 %s；%s", e.Field, e.OnDisk, e.Configured, e.Hint)
}

// Identify 确定本节点的身份。
//
// 这是 PRD CLU-031 的落点：启动须拒绝 cluster ID 冲突、已存在 data dir
// 再执行 bootstrap、shard count 不一致。这些都不是"尽量兼容"的场景——
// 任一条不匹配就意味着这个 data dir 不是配置所描述的那个集群的，
// 继续运行会读到错误的数据。
func Identify(d *DataDir, req IdentityRequest) (Manifest, error) {
	cur, exists, err := d.ReadManifest()
	if err != nil {
		return Manifest{}, err
	}

	if exists {
		// PRD CLU-031：已存在 data dir 再执行 bootstrap 须拒绝
		if req.Bootstrap {
			return Manifest{}, &ErrIdentityConflict{
				Field:      "node.mode",
				OnDisk:     "已初始化（cluster_id " + cur.ClusterID + "）",
				Configured: "bootstrap",
				Hint: "bootstrap 只能在空数据目录执行。" +
					"若要加入已有集群请用 mode: join；若要重建集群请先清空数据目录",
			}
		}
		if req.ClusterID != "" && req.ClusterID != cur.ClusterID {
			return Manifest{}, &ErrIdentityConflict{
				Field: "cluster_id", OnDisk: cur.ClusterID, Configured: req.ClusterID,
				Hint: "这个数据目录属于另一个集群；请确认 data_dir 是否指错",
			}
		}
		if req.NodeID != "" && req.NodeID != cur.NodeID {
			return Manifest{}, &ErrIdentityConflict{
				Field: "node.id", OnDisk: cur.NodeID, Configured: req.NodeID,
				Hint: "节点编号持久化后不可变；要换编号须使用新的数据目录，" +
					"并经成员变更 API 移除旧节点",
			}
		}
		if req.ShardCount != 0 && req.ShardCount != cur.ShardCount {
			return Manifest{}, &ErrIdentityConflict{
				Field:      "cluster.shard_count",
				OnDisk:     fmt.Sprint(cur.ShardCount),
				Configured: fmt.Sprint(req.ShardCount),
				Hint: "分区数在集群创建后不可变：改变它等于全量重分布，" +
					"按新值运行会读不到既有数据",
			}
		}
		return cur, nil
	}

	// data dir 为空
	if !req.AllowCreate {
		return Manifest{}, &ErrNotInitialized{Dir: d.Root()}
	}
	if req.ShardCount <= 0 {
		return Manifest{}, fmt.Errorf("node: 初始化须给出正的 shard_count")
	}

	m := Manifest{
		FormatVersion:   ManifestFormatVersion,
		ClusterID:       req.ClusterID,
		NodeID:          req.NodeID,
		ShardCount:      req.ShardCount,
		CreatedAtUnixMs: req.NowUnixMs,
	}
	if m.ClusterID == "" {
		if m.ClusterID, err = NewID("cluster"); err != nil {
			return Manifest{}, err
		}
	}
	if m.NodeID == "" {
		if m.NodeID, err = NewID("node"); err != nil {
			return Manifest{}, err
		}
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("node: 生成的 manifest 无效: %w", err)
	}
	if err := writeManifest(d.manifestPath(), m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
