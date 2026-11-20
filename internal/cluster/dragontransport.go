package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// 本文件把 dragonboat 的 raft 报文接到本项目既有的那条 TLS 连接上
// （LANG-014 第二次修订、CLU-002 修订）。
//
// 为什么不用它自带的传输：本项目的形态是**一个端口一套证书**，同一个 TLS
// 监听按第一个字节分流 raft 与自有协议，节点身份按证书 CommonName 认
// （CLU-069）。用自带传输就要再开一个端口、再配一套证书，而那是写进四份
// 部署文档的产品属性。
//
// 自定义传输的好处是**两端都是我们的**：帧格式自己定，不必与任何既有协议
// 兼容。于是这里只有一层最朴素的长度前缀。

const (
	// streamMessages 与 streamSnapshot 是 markRaft 之后的第二个字节，
	// 区分这条流送的是普通报文还是快照分片。
	//
	// 分开两条流是 dragonboat 的要求（GetConnection 与 GetSnapshotConnection
	// 是两个方法）：快照分片又大又慢，与心跳挤在一条流上会让选举超时。
	streamMessages byte = 'M'
	streamSnapshot byte = 'S'

	// maxFrameBytes 是单帧上限。
	//
	// 对端是经复制 CA 验过的节点，不是不可信输入；但一个被截断或写坏的
	// 长度前缀会让这里按它去分配内存，于是一条坏连接能把节点打到 OOM。
	// 上限取 64 MiB：快照分片是 MB 级，报文批更小，留足余量仍远低于
	// 单节点的内存预算。
	maxFrameBytes = 64 << 20

	// defaultRaftWriteTimeout 是 raft 帧的写超时。
	//
	// 五秒：比选举超时（ElectionRTT 乘 RTT，默认一秒）宽裕几倍，所以
	// 正常的拥塞不会误杀连接；又远小于"永远"，所以一条黑洞连接几秒内
	// 就会被丢掉并重拨。见 dragonConn.writeTO 的说明。
	defaultRaftWriteTimeout = 5 * time.Second
)

// raftDialer 返回一条**已完成对端身份校验、并已写过分流标记**的连接。
//
// 做成函数而不是直接依赖 TLSGroup：拨号那一套（按 CommonName 认身份、
// 自己建链）已经在 tlsnode.go 里验过，这里不重复一遍，也让传输层可以
// 独立测试。
type raftDialer func(target string) (net.Conn, error)

// dragonTransport 实现 raftio.ITransport。
type dragonTransport struct {
	dial   raftDialer
	msgs   raftio.MessageHandler
	chunks raftio.ChunkHandler

	closed atomic.Bool

	// writeTO 是逐帧的写超时，见 dragonConn.writeTO。
	writeTO time.Duration

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

var _ raftio.ITransport = (*dragonTransport)(nil)

// newDragonTransport 创建传输层。
func newDragonTransport(dial raftDialer,
	msgs raftio.MessageHandler, chunks raftio.ChunkHandler,
) *dragonTransport {
	return &dragonTransport{
		dial: dial, msgs: msgs, chunks: chunks,
		conns:   make(map[net.Conn]struct{}),
		writeTO: defaultRaftWriteTimeout,
	}
}

// Name 是这套传输的名字，出现在 dragonboat 的日志里。
func (t *dragonTransport) Name() string { return "openiotd-tls" }

// Start 不做事：监听是 TLSGroup 的，连接由它分流之后交进来（Incoming）。
//
// dragonboat 允许传输层在这里才开始监听，但本项目的监听早于共识组存在
// ——复制组要等注册表建好才能起（见 startReplicationGroup 的说明）。
func (t *dragonTransport) Start() error { return nil }

// Close 停掉传输层并断开在读的连接。
//
// 必须主动断开而不是只置标志：读协程阻塞在 Read 上，不关连接它们不会醒，
// 而 Close 返回之后调用方会认为传输层已经停了。
func (t *dragonTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	t.mu.Lock()
	for c := range t.conns {
		_ = c.Close()
	}
	t.conns = map[net.Conn]struct{}{}
	t.mu.Unlock()
	t.wg.Wait()
	return nil
}

// GetConnection 返回一条发往 target 的报文连接。
func (t *dragonTransport) GetConnection(_ context.Context,
	target string,
) (raftio.IConnection, error) {
	return t.open(target, streamMessages)
}

// GetSnapshotConnection 返回一条发往 target 的快照连接。
func (t *dragonTransport) GetSnapshotConnection(_ context.Context,
	target string,
) (raftio.ISnapshotConnection, error) {
	return t.open(target, streamSnapshot)
}

func (t *dragonTransport) open(target string, kind byte) (*dragonConn, error) {
	if t.closed.Load() {
		return nil, errors.New("cluster: 传输层已关闭")
	}
	c, err := t.dial(target)
	if err != nil {
		return nil, err
	}
	if _, err := c.Write([]byte{kind}); err != nil {
		_ = c.Close()
		return nil, err
	}
	return &dragonConn{c: c, writeTO: t.writeTO}, nil
}

// Incoming 接管一条已经分流过来的入站连接。
//
// 由 TLSGroup 在读到 markRaft 之后调用。连接的生命周期从这里起归传输层，
// 调用方不要再 Close：读协程会在连接断开时自己收尾。
func (t *dragonTransport) Incoming(c net.Conn) {
	if t.closed.Load() {
		_ = c.Close()
		return
	}
	t.mu.Lock()
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer func() {
			t.mu.Lock()
			delete(t.conns, c)
			t.mu.Unlock()
			_ = c.Close()
		}()
		t.read(c)
	}()
}

// read 读一条入站流，直到对端关闭或出错。
func (t *dragonTransport) read(c net.Conn) {
	var kind [1]byte
	if _, err := io.ReadFull(c, kind[:]); err != nil {
		return
	}
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(c, hdr[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 || n > maxFrameBytes {
			return
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
		switch kind[0] {
		case streamMessages:
			var batch pb.MessageBatch
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			t.msgs(batch)
		case streamSnapshot:
			var chunk pb.Chunk
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			// 返回 false 表示这条快照流不再接收分片，按约定停止读取。
			if !t.chunks(chunk) {
				return
			}
		default:
			return
		}
	}
}

// dragonConn 是一条出站流，同时满足 IConnection 与 ISnapshotConnection。
//
// 两个接口的差别只有方法名（SendMessageBatch / SendChunk），帧格式相同，
// 所以用同一个类型实现；分流在 open 时写的那个字节上已经做掉了。
type dragonConn struct {
	mu sync.Mutex
	c  net.Conn
	// writeTO 是单帧的写超时。
	//
	// **没有它就是一个真缺陷**（CLU-119，2026-11-20 在 Docker Desktop 的
	// k8s 上压出来）：dragonboat 对每个对端只持有一条连接，写入阻塞住
	// 那条连接的发送协程就永远不返回，于是 leader **从此不再跟那个
	// 跟随者说话，而且既不报错也不重拨**。
	//
	// 现象极具误导性：重建一个 Pod 之后，它的出站正常（对端收得到它的
	// 拉票），入站一条都没有；leader 那边关于这个对端的日志**完全是空
	// 的**——没有拨号、没有错误、没有重试。那个 Pod 于是一直选不上
	// （对端以"leader 尚在"丢弃拉票）又跟不上（任期比 leader 高，拒收
	// 心跳），形成永久分裂。
	//
	// 容器重启而 Pod IP 不变时最容易撞上：旧套接字是半开的，写入先被
	// 内核缓冲区接下来（不报错），之后要么阻塞要么很久才拿到 RST。
	// dragonboat 自带的 TCP 传输层正是为此逐帧设写超时。
	writeTO time.Duration
}

var (
	_ raftio.IConnection         = (*dragonConn)(nil)
	_ raftio.ISnapshotConnection = (*dragonConn)(nil)
)

func (d *dragonConn) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	_ = d.c.Close()
}

func (d *dragonConn) SendMessageBatch(batch pb.MessageBatch) error {
	buf, err := batch.Marshal()
	if err != nil {
		return fmt.Errorf("cluster: 编码 raft 报文批失败: %w", err)
	}
	return d.frame(buf)
}

func (d *dragonConn) SendChunk(chunk pb.Chunk) error {
	buf, err := chunk.Marshal()
	if err != nil {
		return fmt.Errorf("cluster: 编码快照分片失败: %w", err)
	}
	return d.frame(buf)
}

// frame 写一帧：四字节大端长度加负载。
//
// 加锁是必要的：dragonboat 允许同一条 IConnection 被并发使用，而两个
// 并发的 Write 会让两帧的字节交错——收端解出来的是垃圾，表现却是
// "对端不可达"，完全不指向这里。
func (d *dragonConn) frame(buf []byte) error {
	if len(buf) > maxFrameBytes {
		return fmt.Errorf("cluster: 单帧 %d 字节超过上限 %d", len(buf), maxFrameBytes)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
	d.mu.Lock()
	defer d.mu.Unlock()
	to := d.writeTO
	if to <= 0 {
		to = defaultRaftWriteTimeout
	}
	// contract:allow-time-now 写超时是 net.Conn 的绝对时间接口要求，
	// 与被测逻辑无关（同 broker/writer.go 的那一处）。注入时钟在这里
	// 没有意义：量的是"对端还收不收"，不是本项目的时间语义。
	if err := d.c.SetWriteDeadline(time.Now().Add(to)); err != nil {
		return err
	}
	// 超时即报错，报错即让 dragonboat 丢掉这条连接并重拨——这正是
	// 要的行为（见 writeTO 的说明）。
	if _, err := d.c.Write(hdr[:]); err != nil {
		return err
	}
	_, err := d.c.Write(buf)
	return err
}

// dragonTransportFactory 把上面这套交给 dragonboat。
type dragonTransportFactory struct {
	dial raftDialer
	// bound 回传建好的传输层：Incoming 要由 TLSGroup 调用，而工厂是
	// dragonboat 在 NewNodeHost 内部调的，拿不到返回值。
	bound func(*dragonTransport)
}

var _ config.TransportFactory = (*dragonTransportFactory)(nil)

func (f *dragonTransportFactory) Create(_ config.NodeHostConfig,
	msgs raftio.MessageHandler, chunks raftio.ChunkHandler,
) raftio.ITransport {
	t := newDragonTransport(f.dial, msgs, chunks)
	if f.bound != nil {
		f.bound(t)
	}
	return t
}

// Validate 判定一个地址是不是本项目认得的形状。
//
// 自定义传输层对地址格式有完全的控制权（dragonboat 的原话）。这里仍然
// 要求 host:port：复制地址本来就是这个形状，而成员列表与证书 CommonName
// 的对应关系建在它上面（CLU-069）。
func (f *dragonTransportFactory) Validate(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	return err == nil && host != "" && port != ""
}
