package pki

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// KeyStore 提供签发所用的私钥。
//
// 它存在的唯一理由是把 CA 私钥挡在复制状态之外：进入 Raft 的只有公开数据
// （已签发证书、状态、绑定关系、策略），CA 私钥永不写入日志、WAL 或快照
// （PRD CLU-028）。若把私钥放进复制状态，它会明文落在每个节点的磁盘上，
// 并永久留在日志历史里。
//
// 签发因此是一个特权角色：只有 role 含 signer 的节点持有密钥，
// 其余节点收到 CSR 后转发（PRD CLU-029）。
type KeyStore interface {
	// Signer 返回签名器。实现可以缓存，也可以每次向 HSM 或 KMS 取句柄。
	Signer(ctx context.Context) (crypto.Signer, error)
	// Kind 返回后端类型，供指标与审计标注。
	Kind() string
	Close() error
}

// MemoryStore 把私钥保存在进程内存中。
//
// 仅用于测试与一次性的引导流程。它不在进程之外留下任何东西，
// 而 FileStore 会把私钥写进文件（PEM，靠 0600 保护，见该类型注释）。
type MemoryStore struct {
	key crypto.Signer
}

// NewMemoryStore 创建内存密钥后端。
func NewMemoryStore(key crypto.Signer) *MemoryStore { return &MemoryStore{key: key} }

func (s *MemoryStore) Signer(context.Context) (crypto.Signer, error) {
	if s.key == nil {
		return nil, errors.New("pki: 内存后端没有密钥")
	}
	return s.key, nil
}

func (s *MemoryStore) Kind() string { return "memory" }

func (s *MemoryStore) Close() error {
	s.key = nil
	return nil
}

const (
	keyFilePerm = 0o600
	keyDirPerm  = 0o700
	// legacyKeyFileMagic 是口令加密时代（AUTH-092 之前）容器的头 8 个
	// 字节。本包不再解密，留着它只为把错误报准，见 ErrLegacyEncryptedKey。
	legacyKeyFileMagic = "OIOTKEY1"
)

// ErrLegacyEncryptedKey 表示这份密钥文件是旧版本口令加密的。
//
// 不认它的话，现象是 ParsePrivateKeyPEM 报"PEM 中没有可识别的私钥"
// ——那句话指向"文件坏了"，而真因是"这个版本不再读这种文件"，两者
// 的处置完全不同。升级时每一个既有数据目录都会撞上这一条，所以它值得
// 一个专门的判别。
var ErrLegacyEncryptedKey = errors.New(
	"pki: 这份私钥是旧版本用 CA 口令加密的，本版本不再支持（AUTH-092）。" +
		"处置：用旧版本的 openiotd 把它解出 PEM 放回原处，" +
		"或重新初始化数据目录——后者等于换掉整条信任链，已签发的设备证书全部作废")

// FileStore 从文件读取签发私钥。
//
// 私钥以普通 PEM 落盘，保护只有文件权限（AUTH-092）。**这是一个刻意的
// 取舍**：此前它用口令派生密钥做 AES-256-GCM 封装，而那个口令要穿过
// 四种部署方式、五个验收脚本与 CI 的冒烟送进容器，送错一次的现象是
// 节点起不来——去掉它换的是部署与运维的简单。放弃的保护也写在这里，
// 别让后来的人以为是漏实现：数据目录或它的任何一份备份泄露，等于
// 泄露整条信任链的签发能力，而那时吊销单张设备证书无济于事。需要静态
// 加密的部署应当从下面一层解决（卷加密、KMS 或 PKCS#11 后端），
// 而不是把口令重新加回这一层。
type FileStore struct {
	path string

	mu     sync.Mutex
	cached crypto.Signer
}

// NewFileStore 打开一个密钥文件。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("pki: 缺少密钥文件路径")
	}
	return &FileStore{path: path}, nil
}

// WriteKeyFile 把私钥以 PEM 写入文件。
//
// 文件权限 0600、目录 0700：不加密时它是唯一的保护，不能松。
func WriteKeyFile(path string, key crypto.Signer) error {
	blob, err := EncodePrivateKeyPEM(key)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, keyDirPerm); err != nil {
			return fmt.Errorf("pki: 创建密钥目录失败: %w", err)
		}
	}
	if err := os.WriteFile(path, blob, keyFilePerm); err != nil {
		return fmt.Errorf("pki: 写入密钥文件失败: %w", err)
	}
	return nil
}

func (s *FileStore) Signer(context.Context) (crypto.Signer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil {
		return s.cached, nil
	}
	blob, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("pki: 读取密钥文件失败: %w", err)
	}
	if len(blob) >= len(legacyKeyFileMagic) &&
		string(blob[:len(legacyKeyFileMagic)]) == legacyKeyFileMagic {
		return nil, ErrLegacyEncryptedKey
	}
	key, err := ParsePrivateKeyPEM(blob)
	if err != nil {
		return nil, err
	}
	s.cached = key
	return key, nil
}

func (s *FileStore) Kind() string { return "file" }

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cached = nil
	return nil
}

var (
	_ KeyStore = (*MemoryStore)(nil)
	_ KeyStore = (*FileStore)(nil)
)
