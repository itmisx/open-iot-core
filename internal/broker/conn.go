package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/session"
)

// conn 是一个 MQTT 连接。
//
// 每连接只有一个读 goroutine（PRD LANG-005）：写出由共享池完成，
// 后台监听由 broker 持有，因此连接本身不再派生协程。
type conn struct {
	b    *Broker
	nc   net.Conn
	info liveConnInfo
	ver  packet.Version
	// persistSession 为假表示协商出来的会话过期间隔是 0。
	// 这种会话只存在于连接运行时，不得产生任何复制写入。
	// 与 ver 一起填入 compact transport 后的两个对齐字节。
	persistSession bool
	// aux 合并 CONNECT 分段与短命认证输入。它只归读 goroutine 所有；
	// 稀有但会被踢线/管理 goroutine 读取的传输扩展放在原子 retry sidecar，
	// 避免释放 CONNECT sidecar 时与外部断连竞态。
	aux *connAux

	princ livePrincipal
	epoch cluster.Epoch
	rt    *session.Runtime
	// 读缓冲由读 goroutine 独占。
	// 默认 192 B 直接内联在连接对象里，避免百万连接再多一百万个
	// heap object 和 allocator span 开销。大报文仍在 readPacket 里按需扩容。
	rsmall [defaultReadBuffer]byte
	rbuf   []byte
	// roff 是已消费到的位置，rlen 是已读入的位置。
	//
	// 分成两个游标而不是读完就把缓冲压实，是因为 packet.Decode 是
	// 零拷贝的：解出的报文直接引用 rbuf。压实会把后续报文的字节搬到
	// 前一个报文的字段上，而那时调用方还没用它。见 readPacket。
	roff uint32
	rlen uint32

	// queued 是写出池的入队标记，保证同一连接不被重复入队。
	queued atomic.Bool
	// wnext 是写出池就绪队列的后继指针。
	//
	// 它放在连接上而不是队列里另分节点，是为了让入队零分配：唤醒发生在
	// 每条消息的投递路径上。所有权**归写出池的锁**，不归本连接的读协程
	// ——这是 ARCH-073 的例外而非违反：读协程从不碰它，它也不是连接状态，
	// 只是池子借连接的一个字段存链表。除 writerPool 的 push/pop 之外
	// 不得有任何地方读写它。
	wnext *conn

	// retry 仅在连接真的发出 QoS 1/2 消息后分配；百万条只保活或只发
	// QoS 0 的连接不应各自常驻一把 mutex 和一张 nil map。
	retry atomic.Pointer[connRetryState]

	connectedAtMs int64
	keepAliveMs   uint32

	closeOnce sync.Once
	closedRC  atomic.Uint32
	// evictWhy 是外部协程请求断开时登记的原因，存的是 why+1，
	// 因为 EvictTakenOver 的值就是 0，零值分不出"没登记"。
	evictWhy atomic.Uint32
}

type connAux struct {
	stages connectStages
	// auth 只活到 Authenticator 返回；成功连接不永久保留认证输入副本。
	auth *connAuthInfo
}

var connAuxPool = sync.Pool{New: func() any { return new(connAux) }}

type connRetryState struct {
	mu           sync.Mutex
	stops        map[uint16]func() bool
	nextPacketID atomic.Uint32
	// extra 与 QoS 重试同属按需、并发可见状态。复用 conn 上已有的原子
	// sidecar 指针，使普通空闲连接不因罕见 ALPN/WSS 字段涨回 512 B 档。
	extra *connExtras
}

const connWriteFailedMask uint32 = 1 << 31

func (c *conn) retryState() *connRetryState {
	if st := c.retry.Load(); st != nil {
		return st
	}
	st := new(connRetryState)
	if c.retry.CompareAndSwap(nil, st) {
		return st
	}
	return c.retry.Load()
}

func newConn(b *Broker, nc net.Conn, info ConnInfo) *conn {
	alpn, customALPN := compactALPN(info.ALPN)
	live, remoteZone := compactLiveConnInfo(info.Remote, info.TLSVersion, alpn)
	c := &conn{
		b:    b,
		nc:   nc,
		aux:  connAuxPool.Get().(*connAux),
		info: live,
	}
	c.aux.stages.handshakeMS = info.HandshakeMS
	if customALPN != "" || info.MaxLifetime != 0 || info.RedirectTo != "" || remoteZone != "" {
		c.retryState().extra = &connExtras{alpn: customALPN, maxLifetime: info.MaxLifetime,
			redirectTo: info.RedirectTo, remoteZone: remoteZone}
	}
	if b.opt.ReadBufferSize <= len(c.rsmall) {
		c.rbuf = c.rsmall[:b.opt.ReadBufferSize]
	} else {
		c.rbuf = make([]byte, b.opt.ReadBufferSize)
	}
	if info.HasCert || info.AccessKeyID != "" || info.CertSerial != "" ||
		info.CertSubject != (authz.DN{}) || info.CertIssuer != (authz.DN{}) {
		c.aux.auth = connAuthInfoPool.Get().(*connAuthInfo)
		*c.aux.auth = connAuthInfo{
			HasCert: info.HasCert, CertFP: info.CertFP,
			CertSubject: info.CertSubject, CertIssuer: info.CertIssuer,
			CertSerial: info.CertSerial, AccessKeyID: info.AccessKeyID,
		}
	}
	return c
}

func compactLiveConnInfo(remote netip.AddrPort, tlsVersion uint16, alpn connALPN) (liveConnInfo, string) {
	info := liveConnInfo{TLSVersion: tlsVersion, ALPN: alpn}
	if !remote.IsValid() {
		return info, ""
	}
	addr := remote.Addr()
	info.IP = addr.As16()
	info.Port = remote.Port()
	info.flags = connRemoteValid
	if addr.Is4() {
		info.flags |= connRemoteV4
	}
	return info, addr.Zone()
}

func (c *conn) remote() netip.AddrPort {
	if c.info.flags&connRemoteValid == 0 {
		return netip.AddrPort{}
	}
	addr := netip.AddrFrom16(c.info.IP)
	if c.info.flags&connRemoteV4 != 0 {
		addr = addr.Unmap()
	}
	if st := c.retry.Load(); st != nil && st.extra != nil && st.extra.remoteZone != "" {
		addr = addr.WithZone(st.extra.remoteZone)
	}
	return netip.AddrPortFrom(addr, c.info.Port)
}

func compactALPN(v string) (connALPN, string) {
	switch v {
	case "":
		return connALPNNone, ""
	case ALPNMQTTCert:
		return connALPNMQTTCert, ""
	case ALPNMQTTCustom:
		return connALPNMQTTCustom, ""
	case ALPNHTTPCert:
		return connALPNHTTPCert, ""
	default:
		return connALPNOther, v
	}
}

func (c *conn) alpn() string {
	switch c.info.ALPN {
	case connALPNMQTTCert:
		return ALPNMQTTCert
	case connALPNMQTTCustom:
		return ALPNMQTTCustom
	case connALPNHTTPCert:
		return ALPNHTTPCert
	case connALPNOther:
		if st := c.retry.Load(); st != nil && st.extra != nil {
			return st.extra.alpn
		}
	}
	return ""
}

func (c *conn) maxLifetime() time.Duration {
	st := c.retry.Load()
	if st == nil || st.extra == nil {
		return 0
	}
	return st.extra.maxLifetime
}

func (c *conn) redirectTo() string {
	st := c.retry.Load()
	if st == nil || st.extra == nil {
		return ""
	}
	return st.extra.redirectTo
}

func (c *conn) clientID() string { return c.princ.AuthzCtx.ClientID }

func (c *conn) releaseConnectStages() {
	if c.aux == nil {
		return
	}
	c.releaseAuthInfo()
	c.aux.stages = connectStages{}
	aux := c.aux
	c.aux = nil
	*aux = connAux{}
	connAuxPool.Put(aux)
}

func (c *conn) releaseAuthInfo() {
	if c.aux == nil || c.aux.auth == nil {
		return
	}
	auth := c.aux.auth
	c.aux.auth = nil
	// pool 不得延长证书 DN、序列号或 access key 标识的生命周期。
	*auth = connAuthInfo{}
	connAuthInfoPool.Put(auth)
}

func (c *conn) authenticationInfo() ConnInfo {
	handshakeMS := float64(0)
	if c.aux != nil {
		handshakeMS = c.aux.stages.handshakeMS
	}
	info := ConnInfo{
		Remote: c.remote(), TLSVersion: c.info.TLSVersion, ALPN: c.alpn(),
		HandshakeMS: handshakeMS, MaxLifetime: c.maxLifetime(),
		RedirectTo: c.redirectTo(),
	}
	if c.aux != nil && c.aux.auth != nil {
		auth := c.aux.auth
		info.HasCert, info.CertFP = auth.HasCert, auth.CertFP
		info.CertSubject, info.CertIssuer = auth.CertSubject, auth.CertIssuer
		info.CertSerial, info.AccessKeyID = auth.CertSerial, auth.AccessKeyID
	}
	return info
}

// serve 运行读循环。
func (c *conn) serve(ctx context.Context) error {
	defer c.cleanup()
	if err := c.serveConnect(ctx); err != nil {
		return err
	}
	return c.serveEstablished(ctx)
}

// serveConnect 只运行会撑大栈的 CONNECT 建连阶段。Listener 路径在它成功
// 后把连接交给一个从最小栈开始的新 goroutine；公开 Serve 的同步路径则继续
// 在当前 goroutine 运行，以保持嵌入调用方的等待语义。
func (c *conn) serveConnect(ctx context.Context) error {
	// 时长从握手交给 broker 的这一刻起算，包含还没发出 CONNECT 的时间。
	c.armLifetime()

	// 第一个报文必须是 CONNECT。
	p, err := c.readPacket(ctx)
	if err != nil {
		c.emitConn(events.ConnProtocolError, reason.MalformedPacket, "读取 CONNECT 失败")
		return err
	}
	cp, ok := p.(*packet.Connect)
	if !ok {
		c.emitConn(events.ConnProtocolError, reason.ProtocolError, "首个报文不是 CONNECT")
		return errors.New("broker: 首个报文不是 CONNECT")
	}
	if err := c.handleConnect(ctx, cp); err != nil {
		return err
	}
	return nil
}

func (c *conn) serveEstablished(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			c.close(reason.ServerShuttingDown, cluster.EvictAdministrative)
			return err
		}
		if c.closedRC.Load()&connWriteFailedMask != 0 {
			return errors.New("broker: 写出失败，连接终止")
		}
		p, err := c.readPacket(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				c.closeGraceful()
				return nil
			}
			var pe *reason.Error
			if errors.As(err, &pe) {
				c.close(pe.Code, cluster.EvictAdministrative)
				return err
			}
			c.close(reason.KeepAliveTimeout, cluster.EvictAdministrative)
			return err
		}
		if err := c.dispatch(p); err != nil {
			return err
		}
		if c.closedRC.Load() != 0 {
			return nil
		}
	}
}

// readPacket 从连接流式读取一个完整报文。
//
// 返回的报文**引用读缓冲**（packet.Decode 是零拷贝的），有效期到下一次
// 调用本函数为止。调用方 serve 是读一个、派发一个，正好吻合。
//
// 这里曾经在返回之前就把缓冲压实，于是一次读取里收到多个报文时，
// 后一个报文的字节会盖在前一个报文的主题、过滤器或负载上——而前一个
// 报文还没被处理。症状极其隐蔽：客户端订阅 pipe/x，服务端却给它订上了
// 另一个主题并回 Granted，两边都不报错。只有一次读取里真的来了多个
// 报文才会显形，所以单条用例几乎碰不到；它是确定性模拟跑出来的。
//
// 修法是把压实推迟到"确实需要更多字节"的时候：那时上一个报文早已派发完。
func (c *conn) readPacket(ctx context.Context) (packet.Packet, error) {
	for {
		if c.rlen > c.roff {
			p, n, err := packet.Decode(c.rbuf[c.roff:c.rlen], c.ver)
			switch {
			case err == nil:
				// 只推进游标，不搬动字节：p 还指着它们。
				c.roff += uint32(n)
				return p, nil
			case errors.Is(err, packet.ErrIncomplete):
				// 继续读。
			default:
				return nil, err
			}
		}
		// 要读更多了，此刻才压实：上一个报文已经派发完毕。
		if c.roff > 0 {
			copy(c.rbuf, c.rbuf[c.roff:c.rlen])
			c.rlen -= c.roff
			c.roff = 0
		}
		if c.rlen == uint32(len(c.rbuf)) {
			// 缓冲不足：按需扩张，上限由最大包长配额约束。
			max := c.b.opt.Limiter.Limit(quota.PacketSize)
			if max <= 0 {
				max = 146 * 1024
			}
			if len(c.rbuf) >= max {
				return nil, reason.Errorf(reason.PacketTooLarge,
					"报文超出最大包长 %d", max)
			}
			grown := make([]byte, min(len(c.rbuf)*2, max))
			copy(grown, c.rbuf[:c.rlen])
			c.rbuf = grown
		}
		if err := c.setReadDeadline(); err != nil {
			return nil, err
		}
		n, err := c.nc.Read(c.rbuf[c.rlen:])
		if n > 0 {
			c.rlen += uint32(n)
		}
		if err != nil {
			if n > 0 && errors.Is(err, io.EOF) {
				continue
			}
			return nil, err
		}
		_ = ctx
	}
}

// setReadDeadline 按保活间隔设置读超时。
//
// MQTT 要求服务端在 1.5 倍保活间隔内未收到任何报文时断开（MQTT-3.1.2.10）。
// 这里用注入时钟推导绝对时间，而不是直接取系统时间（契约 08）。
func (c *conn) setReadDeadline() error {
	if c.keepAliveMs <= 0 {
		return nil
	}
	d := time.Duration(c.keepAliveMs) * time.Millisecond
	// contract:allow-time-now 读超时是 net.Conn 的绝对时间接口要求，
	// 仅用于超时判定，不参与任何排序或版本比较。
	return c.nc.SetReadDeadline(time.Now().Add(d))
}

func (c *conn) dispatch(p packet.Packet) error {
	switch v := p.(type) {
	case *packet.Publish:
		return c.handlePublish(v)
	case *packet.Subscribe:
		return c.handleSubscribe(v)
	case *packet.Unsubscribe:
		return c.handleUnsubscribe(v)
	case *packet.Ack:
		return c.handleAck(v)
	case *packet.Pingreq:
		return c.send(&packet.Pingresp{})
	case *packet.Disconnect:
		return c.handleDisconnect(v)
	case *packet.Connect:
		// 一个连接上只能有一个 CONNECT（MQTT-3.1.0-2）。
		c.close(reason.ProtocolError, cluster.EvictAdministrative)
		return errors.New("broker: 收到第二个 CONNECT")
	case *packet.Auth:
		// 增强认证尚未实现（PRD AUTH-005，v0.3）。
		c.close(reason.BadAuthenticationMethod, cluster.EvictAdministrative)
		return errors.New("broker: 尚不支持增强认证")
	default:
		c.close(reason.ProtocolError, cluster.EvictAdministrative)
		return errors.New("broker: 收到服务端不应接收的报文")
	}
}

// send 编码并入队一个报文。
//
// 只能由连接自己的读协程调用：它借用连接的编码缓冲，而那归读协程独占
// （见 ebuf 的字段注释）。从别的协程投递请走 sendForeign。
func (c *conn) send(p packet.Packet) error {
	// 队列必须取得字节所有权，因此即使复用连接级编码缓冲也仍要再复制
	// 一次。直接编码到新缓冲保持每次发送一次分配，并省掉每条空闲连接
	// 常驻的 slice header 与首个报文后被长期保留的 backing array。
	buf, err := p.EncodeTo(nil, c.ver)
	if err != nil {
		return err
	}
	err = c.enqueue(buf)
	if errors.Is(err, session.ErrOutboundFull) {
		// 出站队列满：对慢客户端施加背压的唯一正确做法是断开它，
		// 否则服务端会为它无界积压（PRD MQTT-035）。
		c.close(reason.QuotaExceeded, cluster.EvictAdministrative)
	}
	return err
}

// sendForeign 由**其他协程**编码并入队一个报文。
//
// 投递路径（route → deliverTo → deliver）跑在**发布者**的读协程上，而
// 编码缓冲归订阅者自己的读协程独占。借用它是一条实打实的数据竞态：两个
// 发布者同时投给同一个订阅者时，两次 EncodeTo 写进同一块缓冲，各自再从
// 里面拷出来的那份就是两个报文的混合——订阅者收到一个畸形报文，然后按
// 协议错误断连。这不需要罕见时序，只要有两个发布者同时命中同一个订阅者，
// 而那是 IoT 里最普通的拓扑。竞态检测器实证，见
// TestConcurrentDeliveryDoesNotShareEncodeBuffer。
//
// 编码到一块新缓冲，分配次数没有增加：入队本来就要求一份副本，这里只是
// 把"复用缓冲再拷一份"换成"直接编码进那一份"。
//
// 这与 conn.evict 是同一条纪律的两面：外部协程不得碰连接的编码缓冲
// 和读缓冲，也不得绕过 Runtime 的锁去改会话状态。在途表是被允许的
// 那一处例外，投递与确认都走那把锁（见 trackAndSend）。
func (c *conn) sendForeign(p packet.Packet) error {
	buf, err := p.EncodeTo(nil, c.ver)
	if err != nil {
		return err
	}
	err = c.enqueue(buf)
	if errors.Is(err, session.ErrOutboundFull) {
		// 慢客户端照样要断开（MQTT-035），但拆解不能在这里跑——这是
		// 发布者的协程。evict 只发 DISCONNECT 并关套接字，持久化、遗嘱、
		// 路由清理留给对方的读循环自己走（见 conn.evict）。
		c.evict(reason.QuotaExceeded, cluster.EvictAdministrative)
	}
	return err
}

// enqueue 把已编码的字节放进出站队列并唤醒写出池。
//
// bytes 的所有权移交给队列：调用方不得再写它。
func (c *conn) enqueue(bytes []byte) error {
	out := session.Outbound{Bytes: bytes}
	if c.rt == nil {
		// CONNACK 之前还没有会话运行时，直接写出。
		_, err := c.nc.Write(out.Bytes)
		return err
	}
	// 队列满时怎么处置由调用方决定：自己的读协程可以直接拆解，别人的
	// 协程只能 evict。错放在这里会让投递路径在发布者的协程上跑完整的
	// close，而那正是 conn.evict 存在的理由。
	if err := c.rt.Enqueue(c.epoch, out); err != nil {
		return err
	}
	c.b.writer.wake(c)
	return nil
}

// sendDirect 在没有会话运行时的阶段直接写出（仅用于 CONNACK 失败路径）。
func (c *conn) sendDirect(p packet.Packet) {
	buf, err := p.EncodeTo(nil, c.ver)
	if err != nil {
		return
	}
	_, _ = c.nc.Write(buf)
}

// allocPacketID 分配一个非零、且不与在途表冲突的出站报文标识符。
//
// 续上会话时在途表里已经占了一批标识。计数器从 0 再数会把新消息编成
// 和还没确认的那条一样的号，客户端会把它们当成同一条。
func (c *conn) allocPacketID() uint16 {
	for {
		v := uint16(c.retryState().nextPacketID.Add(1))
		if v == 0 {
			continue
		}
		if c.rt != nil && c.rt.InflightHas(v) {
			continue
		}
		return v
	}
}

// closeGraceful 处理对端正常关闭连接。
func (c *conn) closeGraceful() {
	c.close(reason.NormalDisconnection, cluster.EvictAdministrative)
}

// close 断开连接。幂等。
//
// 会话状态在关闭套接字之前持久化。顺序颠倒会产生竞态：客户端一旦观察到
// 连接断开就可能立刻重连，而此时旧连接的清理尚未把会话写入存储，
// 重连会被当成新会话，持久会话的订阅与离线队列就丢了。
func (c *conn) close(rc reason.Code, why cluster.EvictReason) {
	c.closeOnce.Do(func() {
		if st := c.retry.Load(); st != nil && st.extra != nil && st.extra.lifetimeStop != nil {
			st.extra.lifetimeStop()
		}
		c.closedRC.Store(uint32(rc) + 1)
		c.stopRetries()
		now := c.b.opt.Now()
		c.flushWill()
		c.persist(now)
		// MQTT 5 的服务端主动断开须携带原因码（PRD MQTT-078）。
		if c.ver == packet.V5 && rc != reason.NormalDisconnection {
			c.sendDirect(c.disconnectPacket(rc))
		}
		if c.rt != nil {
			_ = c.rt.Close(now, why)
		} else {
			_ = c.nc.Close()
		}
	})
}

// armLifetime 登记 WebSocket 连接时长上限（NET-023）。
//
// 有 redirect 目标时，到期发出 MQTT 5 的 0x9C 并带上 Server Reference。
// 没有目标时只关连接：0x9C 的含义是「请连到另一台服务器」，
// 指不出来就不是这个原因码。
func (c *conn) armLifetime() {
	maxLifetime := c.maxLifetime()
	if maxLifetime <= 0 || c.b.opt.AfterFunc == nil {
		return
	}
	// maxLifetime > 0 保证 newConn 已经创建 extra。
	st := c.retry.Load()
	st.extra.lifetimeStop = c.b.opt.AfterFunc(maxLifetime, func() {
		rc := reason.NormalDisconnection
		if c.redirectTo() != "" {
			rc = reason.UseAnotherServer
		}
		c.evict(rc, cluster.EvictAdministrative)
	})
}

// disconnectPacket 构造服务端主动断开的 DISCONNECT。
func (c *conn) disconnectPacket(rc reason.Code) *packet.Disconnect {
	d := &packet.Disconnect{Code: rc}
	if redirectTo := c.redirectTo(); rc == reason.UseAnotherServer && redirectTo != "" {
		d.Props.ServerReference = []byte(redirectTo)
		d.Props.Set(packet.PropServerReference)
	}
	return d
}

// evict 由**其他协程**请求断开本连接。
//
// 它只做两件事：发出带原因码的 DISCONNECT（MQTT-078），然后关套接字。
// 持久化、遗嘱、路由清理一概不做——那些都要读写会话状态与编码缓冲。
// 在途表虽然改由 Runtime 的锁保护，编码缓冲和读缓冲仍然归读协程独占。
// 在外部协程上跑完整的 close 就是在违反这条独占：竞态检测器实证过，
// KickClient 的 persist 与连接自己的 flushQueued 撞在同一个 State.Queued 上。
//
// 套接字一关，读循环立刻醒来，带着这里登记的原因码走正常的拆解路径。
// 对客户端而言时序没有变化：DISCONNECT 与断开仍是即刻的。
func (c *conn) evict(rc reason.Code, why cluster.EvictReason) {
	if !c.closedRC.CompareAndSwap(0, uint32(rc)+1) {
		// 已经在关了：不重复发 DISCONNECT，但仍确保套接字关掉。
		_ = c.nc.Close()
		return
	}
	c.evictWhy.Store(uint32(why) + 1)
	// 编码到局部缓冲；外部协程不共享连接读协程的任何可变缓冲。
	if c.ver == packet.V5 && rc != reason.NormalDisconnection {
		if buf, err := c.disconnectPacket(rc).EncodeTo(nil, c.ver); err == nil {
			// contract:allow-time-now 写超时，不参与排序
			_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = c.nc.Write(buf)
		}
	}
	_ = c.nc.Close()
}

// failWrite 由写出池在写失败时调用。
func (c *conn) failWrite(error) {
	c.closedRC.Or(connWriteFailedMask)
	// 直接关闭底层连接，使读循环立刻解除阻塞。
	_ = c.nc.Close()
}

// cleanup 在读循环退出后释放连接占用的全部资源。
// persist 落盘会话状态并释放会话级资源。
//
// 它在 close 中、关闭套接字之前同步执行，使客户端观察到断开时
// 会话已经可被重连读取。幂等由 close 的 closeOnce 保证。
func (c *conn) persist(nowMs int64) {
	if c.rt == nil {
		return
	}
	// 落盘读的是快照。在途表会从发布者的协程追加，直接把 State 指针
	// 交给存储会让编码与追加撞在同一片切片上。
	c.rt.State().DisconnectUnixMs = nowMs

	// 被抢占后不得改动共享状态。
	//
	// 存储层的 epoch 围栏能拦住会话写入，但路由表与所有权没有 epoch——
	// 若不在此处判断，被抢占的旧连接会把新连接的订阅一并清掉。
	// 这是集成测试才暴露出的问题：单独看会话层，围栏是完整的。
	if c.b.lookup(c.clientID()) != c {
		return
	}

	ctx := context.Background()
	st := c.rt.CloneState()

	// 非持久会话只存在于连接的运行时里，断开时不得写一条复制的 Delete：
	// 存储里可能残留的那条旧记录，已经在 CONNECT 里、本运行时登记之前
	// 就删掉了（见 handleConnect）。
	if !c.persistSession {
		// 在途额度跟着消息走。清洁会话的消息不会再被重投，
		// 额度若只在 ACK 时归还，这个 clientId 的接收窗口就空不出来了。
		c.releaseInflightQuota(len(st.Inflight))
		// 全局订阅配额跟着"订阅是否还在路由表里"走，而不是跟着连接走。
		//
		// 此前它只在显式 UNSUBSCRIBE 时归还，于是最常见的那条路径——
		// 设备订阅完直接断开——每次都永久吃掉若干份额度。上限是全局
		// 50 万，耗尽之后任何设备都不能再订阅任何主题，且只能重启恢复。
		// 持久会话不在此列：它的订阅仍留在路由表里继续占用资源，
		// 额度自然也该继续占着。
		c.b.opt.Limiter.Release(quota.Subscriptions, "",
			c.b.opt.Router.RemoveClient(c.clientID()))
		_ = c.b.opt.Owners.Release(ctx, c.clientID(), c.epoch)
	} else {
		_ = c.b.opt.Sessions.Save(ctx, st)
	}
}

// cleanup 在读循环退出后释放连接占用的资源并产出断开事件。
func (c *conn) cleanup() {
	c.releaseConnectStages()
	c.releaseAuthInfo()
	// rt 为 nil 表示 CONNECT 未走到创建会话运行时那一步：
	// 认证失败、配额超限、策略拒绝都会落到这里，而此时 clientID 可能已设置。
	// 这条路径只有端到端测试能覆盖到——单元测试里 rt 总是存在的。
	if c.clientID() == "" || c.rt == nil {
		if c.clientID() != "" {
			c.b.unregister(c)
		}
		_ = c.nc.Close()
		return
	}
	now := c.b.opt.Now()
	nsubs := len(c.rt.State().Subscriptions)

	rc := reason.Code(0)
	if v := c.closedRC.Load() &^ connWriteFailedMask; v > 0 {
		rc = reason.Code(v - 1)
	}
	// 外部协程登记过原因就用它的——被接管与被踢出的清理语义不同。
	why := cluster.EvictAdministrative
	if v := c.evictWhy.Load(); v > 0 {
		why = cluster.EvictReason(v - 1)
	}
	// close 是幂等的；此处确保异常退出路径也走完持久化。
	c.close(rc, why)

	c.b.unregister(c)
	c.b.opt.Limiter.Release(quota.Connections, "", 1)
	c.b.opt.Limiter.Release(quota.SubsPerConn, c.clientID(), nsubs)
	c.emitDisconnect(rc, now)
}

// authzCtx 构造策略求值上下文。
func (c *conn) authzCtx() *authz.Context {
	return &c.princ.AuthzCtx
}

// allowed 判定一个动作是否被策略放行，并在拒绝时产出事件。
//
// 热路径传 nil 轨迹不付代价；拒绝时才填充完整轨迹，
// 使控制台能回放「为什么被拒」（契约 11、PRD AUTH-035）。
func (c *conn) allowed(act authz.Action, resource string) bool {
	req := authz.Request{Action: act, Resource: resource}
	if authz.Allowed(c.princ.Policies, c.authzCtx(), req) {
		return true
	}
	var tr authz.Trace
	authz.Eval(c.princ.Policies, c.authzCtx(), req, &tr)
	c.emitAuthz(act, resource, &tr)
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
