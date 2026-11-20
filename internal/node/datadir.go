package node

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// dataDirPerm 是 data dir 的权限。
	//
	// 0700 而非 0755：目录下有 **明文的** CA 私钥与 access key secret
	// （AUTH-092 去掉了静态加密）、会话状态与审计数据。也就是说这一行
	// 的权限位现在是这些材料唯一的保护，不是多一道。
	dataDirPerm = 0o700
	// dataFilePerm 是 data dir 内文件的权限。
	dataFilePerm = 0o600
)

// 子目录名。
const (
	DirPKI      = "pki"
	DirMeta     = "meta"
	DirSessions = "sessions"
	// DirRaft 放元数据复制日志与快照。
	//
	// 与 DirMeta 分开：冷层和日志的恢复语义不同——冷层可以从日志重建，
	// 反过来不行。放在一起会让「备份冷层」与「备份日志」变成同一次操作。
	DirRaft = "raft"
)

// DataDir 是已打开的数据目录。
type DataDir struct {
	root string
}

// Open 打开（必要时创建）数据目录。
func Open(root string) (*DataDir, error) {
	if root == "" {
		return nil, errors.New("node: 数据目录路径为空")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("node: 无法解析数据目录路径 %q: %w", root, err)
	}
	if err := os.MkdirAll(abs, dataDirPerm); err != nil {
		return nil, fmt.Errorf("node: 创建数据目录 %s 失败: %w", abs, err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("node: 检查数据目录 %s 失败: %w", abs, err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("node: %s 不是目录", abs)
	}
	return &DataDir{root: abs}, nil
}

// Root 返回数据目录的绝对路径。
func (d *DataDir) Root() string { return d.root }

// Path 拼出数据目录下的路径。
func (d *DataDir) Path(parts ...string) string {
	return filepath.Join(append([]string{d.root}, parts...)...)
}

// EnsureDir 确保子目录存在。
func (d *DataDir) EnsureDir(parts ...string) (string, error) {
	p := d.Path(parts...)
	if err := os.MkdirAll(p, dataDirPerm); err != nil {
		return "", fmt.Errorf("node: 创建目录 %s 失败: %w", p, err)
	}
	return p, nil
}

// manifestPath 返回 manifest 的路径。
func (d *DataDir) manifestPath() string { return d.Path(manifestName) }

// Initialized 判断数据目录是否已初始化。
func (d *DataDir) Initialized() (bool, error) {
	_, ok, err := readManifest(d.manifestPath())
	return ok, err
}

// ReadManifest 读取 manifest。
func (d *DataDir) ReadManifest() (Manifest, bool, error) {
	return readManifest(d.manifestPath())
}

// bytesReader 让 manifest.go 不必 import bytes。
func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// syncDir 同步目录项，使 rename 结果落盘。
//
// 不做这一步，rename 在断电后可能丢失：文件内容已落盘，但目录项还在
// 页缓存里。对身份文件而言，这意味着节点重启后找不到自己的 node_id。
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("node: 打开目录 %s 失败: %w", dir, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		// 某些文件系统不支持对目录 fsync，这不是致命错误
		return nil
	}
	return nil
}
