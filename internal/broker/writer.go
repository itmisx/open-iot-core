// Package broker 把协议层、路由、会话、元数据、策略、配额与事件总线
// 接成一个可服务的 MQTT broker。
//
// 它是前面各个横切面的集成点，也是契约能否真正协同工作的检验场。
package broker

import (
	"sync"
	"sync/atomic"
	"time"
)

// writerPool 是共享的写出工作池。
//
// PRD LANG-005 要求每连接最多一个读 goroutine，写出走共享池：
// 每连接两个 goroutine 在百万连接下是数 GB 的栈内存。
//
// 做法是「写入唤醒」：有报文入队时把连接推入就绪队列（已入队的不重复推），
// 工作协程取出连接、把它的出站缓冲一次性排空并写出。
// goroutine 数因此是 O(工作协程数) 而非 O(连接数)。
type writerPool struct {
	// mu 保护就绪队列与 closing。
	//
	// 队列是侵入式单链表（用连接自己的 wnext 串起来）而不是有界通道，
	// 这一条是修缺陷改的，理由见 wake。
	mu      sync.Mutex
	cond    *sync.Cond
	head    *conn
	tail    *conn
	pending int
	closing bool

	// workers 是工作协程数。记下来是为了能报出生效值：它默认按
	// GOMAXPROCS 自适应，而「到底起了几个」是排查投递积压的第一个问题。
	workers int

	// writeTimeout 是单次刷写的上限。
	//
	// 没有它，一个停止读取的客户端会把一个工作协程永久占住：
	// 池只有固定数量的工作协程，若干个慢客户端就能让全部写出停摆。
	// 超时即视为该连接不可用并断开它——这是对慢客户端唯一正确的处置，
	// 服务端不能为它无界等待（PRD MQTT-035）。
	writeTimeout time.Duration
	wg           sync.WaitGroup
	// mergeBuffers 只在同一次 flush 有两帧以上时借用。缓冲归 writer 池而
	// 不是连接，避免活跃连接永久保留历史最大突发的 backing array。
	mergeBuffers sync.Pool

	// flushed 与 writeErrs 供指标使用。
	flushed   atomic.Uint64
	writeErrs atomic.Uint64
	timeouts  atomic.Uint64
}

// newWriterPool 创建写出池。
func newWriterPool(workers int, writeTimeout time.Duration) *writerPool {
	if workers <= 0 {
		workers = defaultWriterWorkers()
	}
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	p := &writerPool{workers: workers, writeTimeout: writeTimeout}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.run()
	}
	return p
}

func (p *writerPool) run() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for p.head == nil && !p.closing {
			p.cond.Wait()
		}
		// 关机优先：closing 一立就退出，不再管队列里还剩什么。
		//
		// 剩下的不需要刷：Broker.Close 是先逐个 evict 连接、再关本池的，
		// 而 evict 的 DISCONNECT 是直接写套接字的，不走这条队列。
		if p.closing {
			p.mu.Unlock()
			return
		}
		c := p.pop()
		p.mu.Unlock()
		p.flush(c)
	}
}

// push 把连接接到队尾。调用方须持有 mu。
func (p *writerPool) push(c *conn) {
	c.wnext = nil
	if p.tail == nil {
		p.head, p.tail = c, c
	} else {
		p.tail.wnext = c
		p.tail = c
	}
	p.pending++
}

// pop 取出队首。调用方须持有 mu，且队列非空。
func (p *writerPool) pop() *conn {
	c := p.head
	p.head = c.wnext
	if p.head == nil {
		p.tail = nil
	}
	c.wnext = nil
	p.pending--
	return c
}

// wake 把连接推入就绪队列。
//
// queued 标记保证同一连接不会被重复入队，否则多个工作协程会并发写同一个
// 连接，顺序将不可控。它同时是队列长度的上界：**队列里最多有连接数那么多
// 项**，每项一个指针。这正是队列可以无界的理由——它的内存已经被连接数
// 约束住了，而无界换来的是「唤醒不会丢」。
//
// 队列此前是容量 1024 的通道，满了就退回 queued 标记、等下一次入队重试。
// 那是个缺陷（MQTT-120）：被退回的那次唤醒没有任何人重试，连接的出站
// 缓冲里那几个字节要等它**下一次**有报文入队才被刷出去——没有下一次就
// 一直压着。1024 这个容量在百万连接的扇出峰值下是常态饱和，所以表现不是
// 偶发延迟，而是大面积的投递停滞；单机小规模的用例一条也压不到。
//
// 换成链表还顺带去掉了另一类错误。老实现用 close(ready) 停工，而 wake
// 跑在任意协程上（连接的读协程、投递路径、乃至工作协程自己的 flush
// 尾部），`closing` 的判定与随后的发送之间隔着一道缝：判定通过、Close
// 把通道关掉、那次发送就是**向已关闭的通道发送**——panic，而全仓库没有
// recover。现在判定与入队在同一个临界区内（与 ARCH-074 对出站队列的要求
// 同一条理由），那道缝不存在。
func (p *writerPool) wake(c *conn) {
	if c.queued.Swap(true) {
		return // 已在队列中
	}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		c.queued.Store(false)
		return
	}
	p.push(c)
	p.mu.Unlock()
	// 在锁外发信号：持锁唤醒会让刚醒的工作协程立刻在 mu 上再睡一次。
	p.cond.Signal()
}

// flush 把一个连接的出站缓冲排空并写出。
func (p *writerPool) flush(c *conn) {
	// 先直接借用队列第一项的字节。CONNECT 的 CONNACK、稳态 PINGRESP 和
	// 大多数 ACK 都是队列里唯一的一帧；若先 append 到连接合并缓冲，会为每条
	// 空闲连接额外分配并永久保留一块 backing array，还做一次无收益拷贝。
	// 只有确实存在第二项时才切到合并缓冲，仍保持突发下的一次 Write。
	first, ok := c.rt.Dequeue()
	if !ok {
		c.queued.Store(false)
		if c.rt.OutboundLen() > 0 {
			p.wake(c)
		}
		return
	}
	buf := first.Bytes
	var releases []func()
	if first.Release != nil {
		releases = append(releases, first.Release)
	}
	merged := false
	var mergeBuf *[]byte
	for len(buf) < c.b.opt.MaxWriteBatch {
		out, ok := c.rt.Dequeue()
		if !ok {
			break
		}
		if !merged {
			merged = true
			mergeBuf = p.takeMergeBuffer()
			merge := (*mergeBuf)[:0]
			merge = append(merge, buf...)
			buf = merge
		}
		buf = append(buf, out.Bytes...)
		if out.Release != nil {
			releases = append(releases, out.Release)
		}
		if len(buf) >= c.b.opt.MaxWriteBatch {
			break
		}
	}
	if len(buf) > 0 {
		// contract:allow-time-now 写超时是 net.Conn 的绝对时间接口要求，
		// 仅用于超时判定，不参与任何排序或版本比较。
		_ = c.nc.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		if _, err := c.nc.Write(buf); err != nil {
			p.writeErrs.Add(1)
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				p.timeouts.Add(1)
			}
			c.failWrite(err)
		} else {
			p.flushed.Add(1)
		}
	}
	for _, r := range releases {
		r()
	}
	if mergeBuf != nil {
		p.putMergeBuffer(mergeBuf, c.b.opt.MaxWriteBatch)
	}

	// 清除标记后重新检查：在写出期间可能又有报文入队。
	c.queued.Store(false)
	if c.rt.OutboundLen() > 0 {
		p.wake(c)
	}
}

func (p *writerPool) takeMergeBuffer() *[]byte {
	if v := p.mergeBuffers.Get(); v != nil {
		return v.(*[]byte)
	}
	return new([]byte)
}

func (p *writerPool) putMergeBuffer(buf *[]byte, maxWriteBatch int) {
	// 一条超大帧可以让 append 超过批量阈值；不把异常大 backing array
	// 永久留在共享池。两倍阈值容得下正常的“最后一帧越界”。
	if maxWriteBatch <= 0 || cap(*buf) > maxWriteBatch*2 {
		return
	}
	*buf = (*buf)[:0]
	p.mergeBuffers.Put(buf)
}

// Close 停止写出池。它不关闭连接，只停止刷写。幂等。
func (p *writerPool) Close() {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.closing = true
	// 清空队列并退回标记：留着标记会让关机后的 wake 以为连接还在队列里。
	for p.head != nil {
		p.pop().queued.Store(false)
	}
	p.mu.Unlock()
	p.cond.Broadcast()
	p.wg.Wait()
}

// Stats 是写出池的指标。
type writerStats struct {
	Flushed   uint64
	WriteErrs uint64
	Timeouts  uint64
	Pending   int
	Workers   int
}

func (p *writerPool) stats() writerStats {
	p.mu.Lock()
	pending := p.pending
	p.mu.Unlock()
	return writerStats{
		Flushed:   p.flushed.Load(),
		WriteErrs: p.writeErrs.Load(),
		Timeouts:  p.timeouts.Load(),
		Pending:   pending,
		Workers:   p.workers,
	}
}
