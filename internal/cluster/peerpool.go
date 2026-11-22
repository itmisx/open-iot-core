package cluster

import (
	"encoding/gob"
	"net"
	"sync"
	"time"
)

/*
本项目那条协议（markOwn）的长连接池（CLU-038）。

为什么要有它：原先 roundTrip 每次调用都现拨一条 TCP、做完整 TLS 握手、
发一条 gob 消息、读应答、关闭。而调用它的是消息热路径——`Broker.forward`
一条消息调一次，`Propose` 在非 leader 节点上一条写入调一次。代价是实测
出来的（docs/baseline/throughput.md，2026-11-20 那一轮）：

	同节点投递    10 万条/秒
	跨节点投递     2 千条/秒        ← 每条消息一次 RSA 2048 的握手
	集群建连         91 条/秒        ← 会话写入转发也走这条路
	过载时 p50     14 秒

PRD CLU-038 写的是「节点间使用 mTLS **长连接**」，所以上面那个形状是缺陷
而不是取舍。另一半后果更隐蔽：一轮里上千次握手会挤掉 raft 的心跳，表现是
偶发的选举抖动与 503「不是 leader」——那条错误指向多数派，不指向握手。

实现上有两个决定值得写下来：

**一、不做多路复用。** 线上协议仍是「一条连接上一问一答」，池里的每条连接
同一时刻只服务一个请求，用完归还。多路复用要给请求编号、要在两侧各维护一张
在途表，而本项目的并发度（每个对端几条连接）用不着它——池化已经把握手从
每条消息一次降到每条连接一次。

**二、gob 的编解码器跟着连接走，不跟着请求走。** gob 的类型定义只在每个
Encoder 的流里发一次，对面的 Decoder 也只认一次；在同一条连接上每个请求都
新建一对，第二个请求就会让对端解出「重复的类型定义」而报错。这件事在
dial 与 handleOwn 两侧必须是对称的，所以两侧都把编解码器建在循环之外。
*/

const (
	// peerPoolPerNode 是每个对端**缓存**的空闲连接数上限。
	//
	// 它不限制并发：池里取不到时照旧现拨一条，用完发现池满了就关掉。
	// 所以这个数的含义是「同时在跟这个对端说话的协程数，到多少为止还
	// 能摊掉握手」——超过的部分退回每条消息一次握手，也就是接池之前的
	// 样子。
	//
	// 取 256 而不是几条：mesh 转发是**同步**发生在发布者自己的读协程上
	// （`Broker.forward` → `Forward` → roundTrip 要等应答），所以并发度
	// 等于「此刻有多少条连接正在往远端主题发布」，而那是设备数量级的。
	// 第一版取 8，实测跨节点只从 2000 条/秒升到 5000 条/秒：100 个发布者
	// 里有 92 个每条消息仍在现拨。一条空闲连接的代价是一个 fd 加对端
	// 一个协程，而 30 秒不用就被淘汰掉（peerIdleTimeout）。
	peerPoolPerNode = 256
	// peerIdleTimeout 是客户端这侧的空闲淘汰时限。
	peerIdleTimeout = 30 * time.Second
	// peerServerIdleTimeout 是服务端这侧关掉一条空闲连接的时限。
	//
	// 它必须明显大于客户端那个：两者谁先动手，决定了「复用一条对端已经
	// 关掉的连接」有多常见。客户端先淘汰 → 池里的连接几乎总是活的；
	// 反过来的话，每过一个空闲期就有一次必然失败的复用，而 wirePropose
	// 这一类是不重试的（见 roundTrip 的说明）。
	peerServerIdleTimeout = 2 * time.Minute
)

// peerConn 是一条到某个对端的长连接。
type peerConn struct {
	c   net.Conn
	enc *gob.Encoder
	dec *gob.Decoder
	// addr 是建立这条连接时用的地址。成员变更把对端换到新地址之后，
	// 池里那几条还连着旧地址——取用时按地址比一次，不等于就丢掉。
	// 不比的话，一个换了地址的节点会在整个空闲期里收不到消息，
	// 而症状是「对端在，但就是不回话」。
	addr string
	// idleSince 是最后一次归还的时刻，用于空闲淘汰。
	idleSince time.Time
}

func (p *peerConn) close() { _ = p.c.Close() }

// exchange 在这条连接上发一个请求并读回应答。
//
// 读写都设绝对超时，而且是同一个：没有超时的写在对端停止读取时永不返回
// （CLU-119 是同一个形状——一条不设超时的写足以让一个方向彻底沉默），
// 而没有超时的读会把一个失联的对端变成一个永远挂着的调用方协程。
func (p *peerConn) exchange(req *wireMsg, timeout time.Duration) (wireResp, error) {
	// contract:allow-time-now 超时是 net.Conn 的绝对时间接口要求，与被测
	// 逻辑无关（同 dragontransport.go 的那一处）：量的是「对端还回不回话」。
	if err := p.c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return wireResp{}, err
	}
	if err := p.enc.Encode(req); err != nil {
		return wireResp{}, err
	}
	var resp wireResp
	if err := p.dec.Decode(&resp); err != nil {
		return wireResp{}, err
	}
	return resp, nil
}

// peerPool 按对端缓存空闲连接。
type peerPool struct {
	mu     sync.Mutex
	idle   map[NodeID][]*peerConn
	closed bool
}

// take 取一条可用的空闲连接，没有则返回 nil。
//
// 地址不符或空闲过久的在这里被丢掉，而不是留给调用方判断：那样每个调用点
// 都要记得判，而漏判的后果是把消息发进一条通往旧地址的连接。
func (pl *peerPool) take(to NodeID, addr string) *peerConn {
	pl.mu.Lock()
	var stale []*peerConn
	var pick *peerConn
	for pick == nil {
		list := pl.idle[to]
		if len(list) == 0 {
			break
		}
		pc := list[len(list)-1]
		pl.idle[to] = list[:len(list)-1]
		// contract:allow-time-now 空闲时限，契约列出的 TTL 用途。
		if pc.addr != addr || time.Since(pc.idleSince) > peerIdleTimeout {
			stale = append(stale, pc)
			continue
		}
		pick = pc
	}
	pl.mu.Unlock()
	for _, pc := range stale {
		pc.close()
	}
	return pick
}

// put 归还一条连接。池满或池已关闭时直接关掉它。
func (pl *peerPool) put(to NodeID, pc *peerConn) {
	// contract:allow-time-now 同上：记下归还时刻供空闲淘汰使用。
	pc.idleSince = time.Now()
	pl.mu.Lock()
	if pl.closed || len(pl.idle[to]) >= peerPoolPerNode {
		pl.mu.Unlock()
		pc.close()
		return
	}
	if pl.idle == nil {
		pl.idle = make(map[NodeID][]*peerConn)
	}
	pl.idle[to] = append(pl.idle[to], pc)
	pl.mu.Unlock()
}

// drop 丢掉某个对端的全部空闲连接。节点被移出成员列表时用。
func (pl *peerPool) drop(to NodeID) {
	pl.mu.Lock()
	list := pl.idle[to]
	delete(pl.idle, to)
	pl.mu.Unlock()
	for _, pc := range list {
		pc.close()
	}
}

// closeAll 关掉全部空闲连接，并让之后的归还一律直接关闭。
//
// 已经借出去的那些不在这里：它们归各自的调用方，正在等应答的连接被这里
// 关掉会让那次调用拿到一个与真因无关的错误。进程退出时监听一关，对端
// 自会收尾。
func (pl *peerPool) closeAll() {
	pl.mu.Lock()
	pl.closed = true
	all := pl.idle
	pl.idle = nil
	pl.mu.Unlock()
	for _, list := range all {
		for _, pc := range list {
			pc.close()
		}
	}
}
