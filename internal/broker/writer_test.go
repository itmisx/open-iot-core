package broker

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/session"
)

// newWriterTestBroker 造一个只够跑写出池的 broker。
//
// 写出池只碰连接的出站队列与套接字，不需要路由、元数据与事件总线：
// 把它们一并装起来会让这条用例的失败原因变得不唯一。
func newWriterTestBroker(workers int) *Broker {
	b := &Broker{}
	b.opt.ReadBufferSize = defaultReadBuffer
	b.opt.OutboundBuffer = defaultOutboundBuffer
	b.opt.MaxWriteBatch = defaultMaxWriteBatch
	b.writer = newWriterPool(workers, 30*time.Second)
	return b
}

// idleWriterPool 造一个没有工作协程的池，用来直接断言队列本身。
//
// 起了工作协程就断言不了队列内容：它们会在断言之前把队列取空。
func idleWriterPool() *writerPool {
	p := &writerPool{workers: 0, writeTimeout: time.Second}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// TestWriterPoolNeverDropsWake 钉住「唤醒不丢」（MQTT-120）。
//
// 连接数刻意远超旧实现那个 1024 槽的就绪队列。那一版在队列满时退回
// queued 标记、等"下一次入队重试"，而被退回的那次唤醒没有任何人重试：
// 连接出站队列里的字节要等它下一次有报文才被刷出去，没有下一次就一直
// 压着。所以判据是「每条连接最终都收到了它那份字节」，而不是「队列没满」
// ——后者在新实现里恒成立，证明不了任何东西。
func TestWriterPoolNeverDropsWake(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试，-short 下跳过")
	}
	const n = 3000
	b := newWriterTestBroker(2)
	defer b.writer.Close()

	got := make(chan int, n)
	conns := make([]*conn, n)
	for i := 0; i < n; i++ {
		srv, cli := net.Pipe()
		c := newConn(b, srv, ConnInfo{})
		c.rt = session.NewRuntime(&session.State{ClientID: fmt.Sprintf("c-%04d", i)},
			nil, defaultOutboundBuffer)
		conns[i] = c
		// 读端先就位：net.Pipe 是同步的，没有读端时刷写会阻塞在写上，
		// 那样失败原因就从"唤醒丢了"变成"没人读"。
		go func(i int, peer net.Conn) {
			defer func() { _ = peer.Close() }()
			buf := make([]byte, 4)
			if _, err := io.ReadFull(peer, buf); err != nil {
				return
			}
			got <- i
		}(i, cli)
	}
	for i, c := range conns {
		if err := c.enqueue([]byte{0xC0, 0x00, 0xD0, 0x00}); err != nil {
			t.Fatalf("第 %d 条连接入队失败: %v", i, err)
		}
	}

	deadline := time.After(60 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-got:
		case <-deadline:
			t.Fatalf("只有 %d/%d 条连接被刷出，其余连接的唤醒被丢弃了", i, n)
		}
	}
}

// TestWriterPoolQueueFIFO 钉住就绪队列的顺序与去重。
//
// 顺序要紧是因为队列里排的是连接而不是报文：乱序会让一条连接的刷写被
// 推后到不确定的时刻。去重要紧是因为同一连接若同时在队列里出现两次，
// 两个工作协程会并发写同一个套接字，字节交错。
func TestWriterPoolQueueFIFO(t *testing.T) {
	p := idleWriterPool()
	b := &Broker{}
	b.opt.ReadBufferSize = 64
	b.writer = p

	const n = 5
	conns := make([]*conn, n)
	for i := range conns {
		conns[i] = newConn(b, nil, ConnInfo{})
		p.wake(conns[i])
		// 第二次唤醒必须是空操作：标记已置起。
		p.wake(conns[i])
	}
	if got := p.stats().Pending; got != n {
		t.Fatalf("队列里有 %d 项，应为 %d（重复唤醒没有被去重？）", got, n)
	}
	for i := 0; i < n; i++ {
		p.mu.Lock()
		c := p.pop()
		p.mu.Unlock()
		if c != conns[i] {
			t.Fatalf("第 %d 次取出的不是入队顺序里的那条连接", i)
		}
	}
	if got := p.stats().Pending; got != 0 {
		t.Fatalf("取空之后队列长度为 %d", got)
	}
}

// TestWriterPoolCloseClearsQueued 钉住关机时退回入队标记。
//
// 不退回的后果很隐蔽：标记留着为真，关机之后任何 wake 都会以为这条连接
// 还在队列里而直接返回，于是它再也不会被刷写。
func TestWriterPoolCloseClearsQueued(t *testing.T) {
	p := idleWriterPool()
	b := &Broker{}
	b.opt.ReadBufferSize = 64
	b.writer = p
	c := newConn(b, nil, ConnInfo{})

	p.wake(c)
	if !c.queued.Load() {
		t.Fatal("入队后标记应为真")
	}
	p.Close()
	if c.queued.Load() {
		t.Fatal("关机后入队标记没有退回")
	}
	if got := p.stats().Pending; got != 0 {
		t.Fatalf("关机后队列长度为 %d", got)
	}
	// 关机之后的唤醒是空操作，且不得留下标记。
	p.wake(c)
	if c.queued.Load() || p.stats().Pending != 0 {
		t.Fatal("关机之后的唤醒不应入队")
	}
	p.Close() // 幂等
}

// TestDefaultWriterWorkersTracksCPU 钉住工作协程数按核数自适应（MQTT-119）。
//
// 此前是固定的 4，且调用方从不设置。固定值的问题不是"4 太小"，而是它与
// 机器无关——同一个二进制在 4 核和 64 核上起同样多的刷写协程。
func TestDefaultWriterWorkersTracksCPU(t *testing.T) {
	want := runtime.GOMAXPROCS(0) * 2
	if want < minWriterWorkers {
		want = minWriterWorkers
	}
	if got := defaultWriterWorkers(); got != want {
		t.Fatalf("默认工作协程数为 %d，应为 %d", got, want)
	}
	if got := defaultWriterWorkers(); got < minWriterWorkers {
		t.Fatalf("默认工作协程数 %d 低于下限 %d", got, minWriterWorkers)
	}
}

// TestWriterPoolHonorsConfiguredWorkers 钉住配置值被采纳且能报出来。
func TestWriterPoolHonorsConfiguredWorkers(t *testing.T) {
	p := newWriterPool(3, time.Second)
	defer p.Close()
	if got := p.stats().Workers; got != 3 {
		t.Fatalf("工作协程数报为 %d，应为 3", got)
	}
}
