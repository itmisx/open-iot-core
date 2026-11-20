// Package node 管理节点身份与数据目录。
//
// 节点身份必须跨重启稳定（PRD CLU-033）：node_id 一变，会话归属的
// 围栏世代、集群成员关系、审计里的来源标注全部对不上。因此它持久化在
// data dir 的 manifest 里，而不是每次启动生成。
package node

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ManifestFormatVersion 是 manifest 的格式版本。
//
// 显式带版本号是为了让磁盘格式变更有迁移的余地（PRD NFR-070）：
// 没有版本号的格式只能靠猜字段来判断新旧。
const ManifestFormatVersion = 1

// manifestName 是 manifest 文件名。
const manifestName = "manifest.json"

// Manifest 是 data dir 的不可变身份信息。
//
// 用 JSON 而非 YAML：它由程序读写、不由人编辑，JSON 走 stdlib 不需依赖。
type Manifest struct {
	FormatVersion int `json:"format_version"`
	// ClusterID 在集群创建时生成，此后不可变。
	ClusterID string `json:"cluster_id"`
	// NodeID 跨重启不可变（PRD CLU-033）。
	NodeID string `json:"node_id"`
	// ShardCount 集群创建后不可变（PRD CLU-033）。
	//
	// 它决定了数据的分区方式，变更等于全量重分布。
	// 把它写进 manifest 是为了让"改了配置就重启"这种操作被拒绝，
	// 而不是悄悄按新值运行并读不到旧数据。
	ShardCount int `json:"shard_count"`
	// CreatedAtUnixMs 仅供展示与排查，不参与任何判定（契约 08）。
	CreatedAtUnixMs int64 `json:"created_at_unix_ms"`
}

// Validate 校验 manifest 自身的完整性。
func (m *Manifest) Validate() error {
	switch {
	case m.FormatVersion <= 0:
		return errors.New("manifest 缺少 format_version")
	case m.FormatVersion > ManifestFormatVersion:
		// 高于本程序支持的版本须拒绝启动（CLU-057）。
		return fmt.Errorf(
			"manifest 的 format_version 为 %d，本程序最高支持 %d；"+
				"请升级 openiotd，或使用匹配的版本启动",
			m.FormatVersion, ManifestFormatVersion)
	case m.ClusterID == "":
		return errors.New("manifest 缺少 cluster_id")
	case m.NodeID == "":
		return errors.New("manifest 缺少 node_id")
	case m.ShardCount <= 0:
		return errors.New("manifest 缺少 shard_count")
	}
	return nil
}

// NewID 生成一个随机标识。
//
// 不用主机名或 MAC：容器里主机名会变，而 node_id 一变就等于换了一个节点。
func NewID(prefix string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("node: 生成标识失败: %w", err)
	}
	return prefix + "-" + hex.EncodeToString(b[:]), nil
}

// readManifest 读取 manifest。第二个返回值为假表示文件不存在。
func readManifest(path string) (Manifest, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, false, nil
		}
		return Manifest{}, false, fmt.Errorf("node: 读取 %s 失败: %w", path, err)
	}
	var m Manifest
	dec := json.NewDecoder(bytesReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, false, fmt.Errorf(
			"node: %s 无法解析（文件可能已损坏）: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, false, fmt.Errorf("node: %s: %w", path, err)
	}
	return m, true, nil
}

// writeManifest 原子地写入 manifest。
//
// 先写临时文件再 rename，再 fsync（CLU-056）：直接覆盖写一旦在中途
// 断电，data dir 会留下一个截断的 manifest，而节点身份文件损坏意味着
// 这个节点无法再启动。
func writeManifest(path string, m Manifest) error {
	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return fmt.Errorf("node: 编码 manifest 失败: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("node: 创建临时文件失败: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("node: 写入临时文件失败: %w", err)
	}
	// fsync 之后再 rename：rename 本身是原子的，但不保证内容已落盘。
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("node: 同步临时文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("node: 关闭临时文件失败: %w", err)
	}
	if err := os.Chmod(tmpName, dataFilePerm); err != nil {
		return fmt.Errorf("node: 设置文件权限失败: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("node: 替换 manifest 失败: %w", err)
	}
	return syncDir(dir)
}
