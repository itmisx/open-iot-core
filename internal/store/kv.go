// Package store 提供本地持久化。
//
// 本包是整个项目中唯一允许 import 存储引擎的地方（PRD ARCH-065，
// 与 ARCH-062 对 YAML 库、LANG-015 对共识库是同一模式）。上层只看见
// 本包定义的 KV 接口，换存储引擎只改本包。
//
// 当前引擎是 bbolt（LANG-021）。规则同时拦住 Pebble——它会在 v0.4
// 随 dragonboat 进来做 Raft 日志后端（LANG-014），那时也必须留在本包内。
//
// 这条隔离不是洁癖：引擎自带的类型（bbolt 的 Tx 与 Bucket、Pebble 的
// Batch 与 Iterator 与 WriteOptions）一旦出现在业务代码的函数签名里，
// "替换存储引擎"就从模块级改动变成全仓库改动。
//
// 它已经兑现过一次：冷层由 Pebble 改为 bbolt 的实际代价是一个文件。
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrNotFound 表示键不存在。
var ErrNotFound = errors.New("store: 键不存在")

// ErrClosed 表示存储已关闭。
var ErrClosed = errors.New("store: 存储已关闭")

// KV 是本项目需要的键值存储能力。
//
// 刻意只暴露必需的几项，而不是转发引擎的全部能力：接口越小，
// 可替换的实现越多，也越容易为测试提供内存实现。
type KV interface {
	// Get 读取一个键。不存在时返回 ErrNotFound。
	//
	// 返回的切片是副本，调用方可以随意持有与修改。
	// 不返回引擎内部缓冲的视图——那会让"忘记 Close 迭代器"
	// 这类错误表现为读到随机数据。
	Get(key []byte) ([]byte, error)
	// Set 写入一个键。sync 为真时在返回前落盘。
	Set(key, value []byte, sync bool) error
	// Delete 删除一个键。删除不存在的键不是错误。
	Delete(key []byte, sync bool) error
	// Scan 按前缀升序遍历。fn 返回 false 时提前结束。
	//
	// key 与 value 只在 fn 执行期间有效，fn 若要留存须自行复制。
	// 这个约定换来扫描过程零拷贝，而扫描是恢复路径上的热操作。
	Scan(prefix []byte, fn func(key, value []byte) bool) error
	// Batch 原子地应用一组写入。
	Batch(ops []Op, sync bool) error
	// Flush 把内存中的写入落盘。
	Flush() error
	Close() error
}

// OpKind 是批量写入中的操作类型。
type OpKind uint8

const (
	// OpSet 写入或覆盖。
	OpSet OpKind = iota
	// OpDelete 删除。
	OpDelete
)

// Op 是批量写入中的一项操作。
type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte
}

// Prefix 是一个键空间前缀。
//
// 用单字节前缀而不是可读的字符串前缀：键的数量级是设备数乘以若干，
// 每个键省下的字节会直接体现在磁盘与块缓存的占用上。
// 可读性的损失由本处的注释和 `store dump` 工具补偿。
//
// 全部前缀集中在此处分配，使"两个包各自选了同一个字母"不可能发生。
type Prefix byte

const (
	PrefixShadow  Prefix = 's' // s<thing>\x00<shadowName>
	PrefixSession Prefix = 'c' // c<clientID>
	// PrefixQueued 是离线队列：一条消息一条记录。
	//
	// 与会话状态分开存，是为了让入队是一次**追加**而不是把整份会话
	// 重写一遍。整块写的代价有两处：N 条消息累计写入 O(N²)（发布路径
	// 上），以及跨节点复制时每入队一条都要把上百 KB 的会话提进日志。
	PrefixQueued   Prefix = 'Q' // Q<clientID>\x00<8 字节大端序号>
	PrefixRetained Prefix = 'r' // r<topic>
	PrefixThing    Prefix = 't' // t<thingName>
	PrefixCert     Prefix = 'C' // C<fingerprint>
	PrefixPolicy   Prefix = 'p' // p<policyName>
	PrefixCounter  Prefix = 'n' // n<name>：单调计数器
	PrefixSystem   Prefix = 'z' // z<name>：格式版本等、applied index（见 MetaRevisionKey）
	PrefixCA       Prefix = 'A' // A<certificateId>：注册进来的 CA
	PrefixAlias    Prefix = 'L' // L<roleAlias>：角色别名
	PrefixAWSKey   Prefix = 'K' // K<accessKeyId>：兼容面的 access key（密文）
	// 下面两个只出现在复制日志那个库里（<data dir>/raft/log.db），
	// 与元数据冷层不同库。仍在此处分配：前缀的唯一性由这张表保证，
	// 而"反正不同库所以随便取"会在将来合库或共用扫描工具时撞上。
	PrefixRaftLog    Prefix = 'R' // R<8 字节大端序号>：复制日志的一条记录
	PrefixRaftStable Prefix = 'S' // S<name>：currentTerm、votedFor 这类必须落盘的状态
)

// KeySep 分隔键中的多个变长片段。
//
// 取 \x00 是因为它不可能出现在任何合法的设备名、影子名或主题名中
// （MQTT 主题禁止空字符，设备名与影子名的字符集已在各自包中校验）。
// 若分隔符可能出现在片段内，`a\x00b` 与 `a` + `\x00b` 会撞键。
const KeySep byte = 0x00

// FormatVersion 是存储格式版本。
//
// 与 manifest 的 format_version 分开：磁盘格式与节点身份的演进节奏不同，
// 合用一个版本号会让任一方的变更都迫使另一方升版本。
const FormatVersion = 1

// systemKeyFormat 存放格式版本。
var systemKeyFormat = []byte{byte(PrefixSystem), 'f', 'o', 'r', 'm', 'a', 't'}

// Key 拼出带前缀与分隔符的键。
//
// 各包用它而不是各自拼接：分隔符规则只能有一处定义，
// 两个包各写一套迟早会在某个含特殊字符的名字上撞键。
func Key(prefix Prefix, parts ...string) []byte {
	n := 1
	for i, p := range parts {
		if i > 0 {
			n++
		}
		n += len(p)
	}
	out := make([]byte, 0, n)
	out = append(out, byte(prefix))
	for i, p := range parts {
		if i > 0 {
			out = append(out, KeySep)
		}
		out = append(out, p...)
	}
	return out
}

// SplitKey 从键中解出各片段。
func SplitKey(key []byte, prefix Prefix, want int) ([]string, bool) {
	if len(key) == 0 || key[0] != byte(prefix) {
		return nil, false
	}
	body := key[1:]
	out := make([]string, 0, want)
	start := 0
	for i := 0; i <= len(body); i++ {
		if i == len(body) || body[i] == KeySep {
			out = append(out, string(body[start:i]))
			start = i + 1
		}
	}
	if len(out) != want {
		return nil, false
	}
	return out, true
}

// checkFormatVersion 校验磁盘格式版本，首次打开时写入。
func checkFormatVersion(kv KV) error {
	raw, err := kv.Get(systemKeyFormat)
	if errors.Is(err, ErrNotFound) {
		// 空库：写入当前版本
		return kv.Set(systemKeyFormat, []byte{byte(FormatVersion)}, true)
	}
	if err != nil {
		return err
	}
	if len(raw) != 1 {
		return fmt.Errorf("store: 格式版本记录损坏（长度 %d）", len(raw))
	}
	got := int(raw[0])
	if got > FormatVersion {
		return fmt.Errorf(
			"store: 磁盘格式版本为 %d，本程序最高支持 %d；"+
				"请升级 openiotd，或使用匹配的版本启动",
			got, FormatVersion)
	}
	// got < FormatVersion 时将来在此迁移。当前只有版本 1。
	return nil
}

// bytesReaderOf 让各记录解码器不必各自 import bytes。
func bytesReaderOf(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// ScanPrefix 按命名空间前缀遍历。
//
// 自动补上分隔符，避免"扫描 d1 时串到 d10"这类前缀串扫——
// 该错误的表现是别的实体的记录出现在列表里，属安全问题。
func ScanPrefix(kv KV, prefix Prefix, first string, fn func(parts []string, value []byte) bool) error {
	p := Key(prefix, first)
	if first != "" {
		p = append(p, KeySep)
	}
	want := 1
	if first != "" {
		want = 2
	}
	return kv.Scan(p, func(key, value []byte) bool {
		parts, ok := SplitKey(key, prefix, want)
		if !ok {
			return true
		}
		return fn(parts, value)
	})
}

// CounterKey 返回计数器的键。
//
// 暴露它是为了让计数器的推进可以放进复制日志：集群下若各节点各自
// 读-改-写自己的计数器，同一个编号会被分配两次，而编号正被证书与设备
// 记录引用着（REG-112）。调用方据此把新值作为一条冷层记录随命令提交。
func CounterKey(name string) []byte { return Key(PrefixCounter, name) }

// PeekCounter 读计数器当前值，不推进它。不存在时返回 0。
func PeekCounter(kv KV, name string) (uint64, error) {
	raw, err := kv.Get(CounterKey(name))
	switch {
	case err == nil:
		if len(raw) != 8 {
			return 0, fmt.Errorf("store: 计数器 %s 记录损坏（长度 %d）", name, len(raw))
		}
		return binary.BigEndian.Uint64(raw), nil
	case errors.Is(err, ErrNotFound):
		return 0, nil
	default:
		return 0, err
	}
}

// EncodeCounter 把计数器值编成记录正文。
func EncodeCounter(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

// NextCounter 取下一个计数器值并写回。
//
// 用于分配跨重启稳定且单调的编号（如策略集编号）。
// 不用"记录条数加一"：删除会让编号重复，而编号正被其他记录引用着。
//
// 调用方须自行串行化：本函数是读-改-写，并发调用可能分配出重复编号。
// 需要让推进本身被复制时用 PeekCounter 加 CounterKey/EncodeCounter。
func NextCounter(kv KV, name string) (uint64, error) {
	key := Key(PrefixCounter, name)
	var cur uint64
	raw, err := kv.Get(key)
	switch {
	case err == nil:
		if len(raw) != 8 {
			return 0, fmt.Errorf("store: 计数器 %s 记录损坏（长度 %d）", name, len(raw))
		}
		cur = binary.BigEndian.Uint64(raw)
	case errors.Is(err, ErrNotFound):
	default:
		return 0, err
	}
	cur++
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cur)
	if err := kv.Set(key, buf[:], true); err != nil {
		return 0, err
	}
	return cur, nil
}

// MetaRevisionKey 是元数据 applied index 在冷层里的键。
//
// 放在 PrefixSystem 下而不是另开一个前缀：它是"这个库自己的元信息"，
// 与格式版本同类，而前缀字节是会出现在备份与快照里的长期契约。
//
// 它与冷层记录在同一个库里，这是**有意的**：applied index 必须与它
// 所描述的那些记录一起被快照、一起被恢复。分两个库存的后果是装完快照
// 之后两者对不上，而那一刻没有任何东西会报错（P1）。
func MetaRevisionKey() []byte { return Key(PrefixSystem, "meta-applied-index") }
