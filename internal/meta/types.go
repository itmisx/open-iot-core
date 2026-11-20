// Package meta 实现元数据的复制日志与本地全量物化视图（契约 05）。
//
// 核心契约是 MetaView 的方法签名里没有 context.Context、没有 error——
// 这在类型层面承诺「本地内存、不阻塞、不会失败」。认证鉴权位于 CONNECT
// 路径上，且每条 PUBLISH 与 SUBSCRIBE 都要做授权判定，按配额是 2 万次/秒
// 量级；一旦这里出现远程调用，握手吞吐会崩塌，而届时被迫重写认证路径并
// 补缓存与失效机制，其正确性问题比一开始就做物化视图难得多。
//
// 读写分离：写入经 Apply 走复制日志，读取只查本地视图。单机是一副本的
// 复制日志，与集群共用同一条读路径（PRD ARCH-001）。
package meta

import (
	"bytes"
	"encoding/hex"
	"sync"
)

// Revision 是复制日志的索引，是元数据的唯一版本来源。
//
// 契约 08 禁止用墙上时钟做排序或版本判定；元数据的先后一律由 Revision 确定。
type Revision uint64

// CertFingerprint 是证书的 SHA-256 指纹，用作证书的主键。
type CertFingerprint [32]byte

// String 返回十六进制表示。
func (f CertFingerprint) String() string { return hex.EncodeToString(f[:]) }

// ParseFingerprint 解析十六进制指纹。
func ParseFingerprint(s string) (CertFingerprint, bool) {
	var fp CertFingerprint
	if len(s) != 64 {
		return fp, false
	}
	n, err := hex.Decode(fp[:], []byte(s))
	if err != nil || n != 32 {
		return fp, false
	}
	return fp, true
}

// CertStatus 是证书的状态（PRD PKI-020）。
type CertStatus uint8

const (
	// CertUnknown 表示证书未注册。它是零值，使「查不到」与「未激活」
	// 在默认情况下都走拒绝分支。
	CertUnknown CertStatus = iota
	CertActive
	CertInactive
	CertRevoked
	CertPendingActivation
)

var certStatusName = [...]string{
	"UNKNOWN", "ACTIVE", "INACTIVE", "REVOKED", "PENDING_ACTIVATION",
}

func (s CertStatus) String() string {
	if int(s) >= len(certStatusName) {
		return "UNKNOWN"
	}
	return certStatusName[s]
}

// CanConnect 报告该状态是否允许建立新连接。
// 只有 ACTIVE 允许（PRD PKI-022）。
func (s CertStatus) CanConnect() bool { return s == CertActive }

// PolicySetID 指向一组已编译的策略。
//
// 策略在 apply 阶段一次性编译为匹配器，消息路径上不解析策略文档
// （PRD ARCH-024）。多个主体共享同一策略集时只占一个 ID，
// 因此策略集的数量通常是数十到数千，而非与设备数同阶。
type PolicySetID int32

// NoPolicySet 表示主体未绑定任何策略，等价于隐式拒绝。
const NoPolicySet PolicySetID = -1

// nameRef 是驻留后的名字引用。-1 表示无。
type nameRef = int32

const noRef nameRef = -1

// nameTable 把设备名等重复出现的字符串驻留为整型引用。
//
// 证书表与 Thing 表都会引用同一批设备名，驻留既去重又让表中的记录
// 保持无指针。按 PRD CLU-021，百万设备的热视图须控制在 150 MB 以内，
// 若每条记录各存一份字符串，仅字符串头就会吃掉大半预算。
type nameTable struct {
	mu    sync.RWMutex
	buf   []byte
	offs  []int32
	next  []nameRef
	index map[uint64]nameRef
}

func newNameTable() *nameTable {
	return &nameTable{
		offs:  []int32{0},
		index: make(map[uint64]nameRef),
	}
}

func (t *nameTable) at(r nameRef) string {
	if r < 0 {
		return ""
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return string(t.buf[t.offs[r]:t.offs[r+1]])
}

func (t *nameTable) find(s string) (nameRef, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.findLocked(s)
}

func (t *nameTable) findLocked(s string) (nameRef, bool) {
	h := hashString(s)
	head, ok := t.index[h]
	if !ok {
		return noRef, false
	}
	for i := head; i != noRef; i = t.next[i] {
		if bytes.Equal(t.buf[t.offs[i]:t.offs[i+1]], []byte(s)) {
			return i, true
		}
	}
	return noRef, false
}

func (t *nameTable) intern(s string) nameRef {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.findLocked(s); ok {
		return r
	}
	r := nameRef(len(t.offs) - 1)
	t.buf = append(t.buf, s...)
	t.offs = append(t.offs, int32(len(t.buf)))
	h := hashString(s)
	head, ok := t.index[h]
	if !ok {
		head = noRef
	}
	t.next = append(t.next, head)
	t.index[h] = r
	return r
}

// Len 返回已驻留的名字数量。
func (t *nameTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.offs) - 1
}

// hashString 是 FNV-1a。必须在所有节点上给出相同结果，
// 因此不使用 Go 的 map 哈希（后者每进程随机加盐）。
func hashString(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
