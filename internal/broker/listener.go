package broker

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// ALPN 协议名。
//
// 443 端口上的 mTLS 必须协商 x-amzn-mqtt-ca：这是官方 SDK 在受限网络下的
// 默认路径，缺了它等于不兼容（PRD NET-002）。
const (
	ALPNMQTTCert   = "x-amzn-mqtt-ca"
	ALPNMQTTCustom = "mqtt"
	ALPNHTTPCert   = "x-amzn-http-ca"
)

// TLSOptions 是 TLS 监听器的参数。
type TLSOptions struct {
	// Certificate 是服务端证书。
	Certificate tls.Certificate
	// ClientCAs 是校验客户端证书的信任池。
	ClientCAs *x509.CertPool
	// RequireClientCert 要求客户端出示并通过校验的证书。
	//
	// 8883 与 443-ALPN 两个端点都要求它；只有自定义认证与 SigV4
	// 的端点才允许无客户端证书（PRD NET-001 至 NET-005）。
	RequireClientCert bool
	// NextProtos 是允许的 ALPN 协议名。为空表示不限制。
	NextProtos []string
	// MinVersion 默认 TLS 1.2（PRD NET-010）。
	MinVersion uint16
	// CurvePreferences 控制 TLS 1.3 密钥交换。为空沿用 Go 默认（Go 1.24
	// 起包含 X25519+ML-KEM 混合组）；容量优先部署可显式只给 X25519/P-256。
	CurvePreferences []tls.CurveID
	// RequireSNI 在缺少 SNI 时拒绝连接。
	//
	// 与 AWS 行为一致：SNI 是多租户与自定义域名的分流依据。
	// 自建环境常无 SNI，因此提供开关（PRD NET-013）。
	RequireSNI bool
	// HandshakeTimeout 限制握手时长。
	//
	// 没有它，一个只完成 TCP 连接却不发 ClientHello 的对端会一直占用
	// 一个 goroutine 与一个文件描述符——这是最廉价的资源耗尽手段。
	HandshakeTimeout time.Duration
	// SessionTicketKeys 跨节点共享时，客户端经负载均衡切到另一节点也能
	// 恢复 TLS session。第一把用于加密，其余只解密旧 ticket，便于轮换。
	// 为空时保留 Go 的每进程随机自动轮换，安全但只能在原节点恢复。
	SessionTicketKeys [][32]byte
}

const defaultHandshakeTimeout = 10 * time.Second

// Listener 在一个地址上接受连接并交给 broker。
type Listener struct {
	b    *Broker
	ln   net.Listener
	tlsc *tls.Config
	opt  TLSOptions
	// proxy 非 nil 表示该明文套接字只接受受信 TLS 终结器发来的
	// PROXY protocol v2 连接。它与普通明文端点严格分开，不能在缺少
	// header 时回退，否则直连客户端可以绕过代理伪造传输身份。
	proxy *ProxyOptions
	// handshakeGate 只在 TLS/WSS listener 上存在。容量是硬上限；
	// Serve 在启动握手 goroutine 前取令牌，handle 完成 TLS 后归还。
	handshakeGate      chan struct{}
	handshakeOK        atomic.Uint64
	handshakeResumed   atomic.Uint64
	handshakeFailed    atomic.Uint64
	handshakeThrottled atomic.Uint64
	handshakeInFlight  atomic.Int64
	handshakePeak      atomic.Int64

	wg            sync.WaitGroup
	closed        sync.Once
	stop          chan struct{}
	acceptMu      sync.RWMutex
	acceptStopped bool

	// wss 非空时，TLS 之后走 WebSocket 升级。
	// 这条路径不要求客户端证书，身份在查询串的 SigV4 里。
	wss *webSocketConfig
}

// ListenTLS 在 addr 上启动一个 TLS 监听器。
func (b *Broker) ListenTLS(addr string, opt TLSOptions) (*Listener, error) {
	if len(opt.Certificate.Certificate) == 0 {
		return nil, errors.New("broker: 缺少服务端证书")
	}
	if opt.RequireClientCert && opt.ClientCAs == nil {
		return nil, errors.New("broker: 要求客户端证书但未提供信任池")
	}
	if opt.MinVersion == 0 {
		opt.MinVersion = tls.VersionTLS12
	}
	if opt.HandshakeTimeout <= 0 {
		opt.HandshakeTimeout = defaultHandshakeTimeout
	}
	clientAuth := tls.NoClientCert
	if opt.RequireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	cfg := &tls.Config{
		Certificates:     []tls.Certificate{opt.Certificate},
		ClientCAs:        opt.ClientCAs,
		ClientAuth:       clientAuth,
		MinVersion:       opt.MinVersion,
		CurvePreferences: opt.CurvePreferences,
		NextProtos:       opt.NextProtos,
	}
	if len(opt.SessionTicketKeys) > 0 {
		cfg.SetSessionTicketKeys(opt.SessionTicketKeys)
	}
	ln, err := b.listenTCP(addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{
		b: b, ln: ln, tlsc: cfg, opt: opt,
		handshakeGate: make(chan struct{}, b.opt.TLSHandshakeConcurrency),
		stop:          make(chan struct{}),
	}, nil
}

// ListenPlain 在 addr 上启动一个明文监听器。
//
// 它只应用于已在前置代理终结 TLS 的部署，或测试。
// 该路径下客户端身份须由代理经其他途径提供（PRD NET-017）。
func (b *Broker) ListenPlain(addr string) (*Listener, error) {
	ln, err := b.listenTCP(addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{b: b, ln: ln, stop: make(chan struct{})}, nil
}

// ListenProxyV2 在 addr 上启动一个仅供受信 TLS 终结器使用的监听器。
//
// 终结器必须发送 PROXY protocol v2 header 以及 OpenIoT 客户端证书 TLV；
// broker 会从叶证书重新计算指纹，再按当前 metadata 热视图重新执行证书
// 状态、吊销、Thing 与策略检查。普通 MQTT 字节不会被当成无 header 的
// 兼容路径接受。
func (b *Broker) ListenProxyV2(addr string, opt ProxyOptions) (*Listener, error) {
	if err := opt.validate(); err != nil {
		return nil, err
	}
	ln, err := b.listenTCP(addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{b: b, ln: ln, proxy: &opt, stop: make(chan struct{})}, nil
}

func (b *Broker) listenTCP(addr string) (net.Listener, error) {
	// net.Listen 会给 Accept 出的 TCP 连接启用 Go 默认 15s keepalive。
	// 显式传入容量化周期，避免空闲连接的内核定时器和探测包成为主负载。
	lc := net.ListenConfig{KeepAlive: b.opt.TCPKeepAlive}
	return lc.Listen(context.Background(), "tcp", addr)
}

// Addr 返回实际监听的地址。
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Serve 接受连接直到监听器关闭。
func (l *Listener) Serve(ctx context.Context) error {
	return l.acceptLoop(ctx)
}

func (l *Listener) acceptLoop(ctx context.Context) error {
	for {
		nc, err := l.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		// 登记与 StopAccepting 共用 acceptMu：Stop 返回后不可能再 Add，
		// 因此 Close 接下来的 Wait 不会撞上 WaitGroup 的 Add/Wait 禁区。
		l.acceptMu.RLock()
		if l.acceptStopped {
			l.acceptMu.RUnlock()
			_ = nc.Close()
			return nil
		}
		l.wg.Add(1)
		l.acceptMu.RUnlock()
		// TLS 速率配额必须在密码学之前执行。放到 handleConnect 里
		// 已经太晚：攻击者不发 MQTT CONNECT 也能耗完 TLS CPU。
		if l.tlsc != nil && l.b != nil && l.b.opt.Limiter != nil &&
			!l.b.opt.Limiter.AllowGlobal(ctx, quota.HandshakeRate, "", 1) {
			l.handshakeThrottled.Add(1)
			_ = nc.Close()
			l.wg.Done()
			continue
		}
		if l.handshakeGate != nil {
			select {
			case l.handshakeGate <- struct{}{}:
			case <-ctx.Done():
				_ = nc.Close()
				l.wg.Done()
				return nil
			case <-l.stop:
				_ = nc.Close()
				l.wg.Done()
				return nil
			}
		}
		go func() {
			defer l.wg.Done()
			if l.handshakeGate != nil {
				defer func() { <-l.handshakeGate }()
			}
			l.handle(ctx, nc)
		}()
	}
}

// handle 完成握手并把连接交给 broker。
func (l *Listener) handle(ctx context.Context, nc net.Conn) {
	info := ConnInfo{Remote: remoteAddr(nc)}

	if l.tlsc == nil {
		if l.proxy != nil {
			var err error
			info, err = readProxyV2Identity(nc, info.Remote, *l.proxy)
			if err != nil {
				_ = nc.Close()
				return
			}
		}
		l.serveBrokerHandoff(ctx, nc, info)
		return
	}

	tc := tls.Server(nc, l.tlsc)
	inFlight := l.handshakeInFlight.Add(1)
	for peak := l.handshakePeak.Load(); inFlight > peak &&
		!l.handshakePeak.CompareAndSwap(peak, inFlight); peak = l.handshakePeak.Load() {
	}
	// 握手必须显式完成并计时：计时值进入连接事件，是定位握手 CPU 瓶颈的
	// 唯一依据（PRD ARCH-036）。延迟握手则取不到这个数。
	hctx, cancel := context.WithTimeout(ctx, l.opt.HandshakeTimeout)
	start := nowMonotonic()
	err := tc.HandshakeContext(hctx)
	elapsed := nowMonotonic().Sub(start)
	cancel()
	l.handshakeInFlight.Add(-1)
	if err != nil {
		l.handshakeFailed.Add(1)
		// 握手失败的连接还没有 clientId，无法产出连接事件；
		// 握手层面的失败由监听器的指标与日志承载。
		_ = tc.Close()
		return
	}

	st := tc.ConnectionState()
	l.handshakeOK.Add(1)
	if st.DidResume {
		l.handshakeResumed.Add(1)
	}
	if l.opt.RequireSNI && st.ServerName == "" {
		_ = tc.Close()
		return
	}
	info = connInfoFromTLS(st, info.Remote, elapsed)
	// TLS 握手会把当前 goroutine 的可增长栈撑到数十 KiB。若继续在同一个
	// goroutine 里跑长连接读循环，这段栈会随连接常驻；6,000 条真实 mTLS
	// 实测 stack-in-use 增量约 19.8 KiB/连接，20 万连接就是约 3.7 GiB。
	//
	// 握手完成后交给一个新 goroutine，让当前短命 goroutine 退出并归还大栈；
	// 新 goroutine 从最小栈开始跑 MQTT。连接所有权没有并发重叠：spawn 后
	// 当前路径立即返回。wg 先 Add 再让旧 handler Done，Close 等待语义不变。
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		if l.wss != nil {
			l.acceptWebSocket(ctx, tc, info)
			return
		}
		l.serveBrokerHandoff(ctx, tc, info)
	}()
}

// TLSHandshakeStats 只包含 TLS 阶段，不等 MQTT CONNECT。因此它也能看到
// 握手成功但没有发 CONNECT 的慢客户端。
type TLSHandshakeStats struct {
	Succeeded uint64
	Resumed   uint64
	Failed    uint64
	Throttled uint64
	InFlight  int64
	Peak      int64
	Capacity  int
}

func (l *Listener) TLSHandshakeStats() TLSHandshakeStats {
	if l.handshakeGate == nil {
		return TLSHandshakeStats{}
	}
	return TLSHandshakeStats{
		Succeeded: l.handshakeOK.Load(), Resumed: l.handshakeResumed.Load(),
		Failed: l.handshakeFailed.Load(), Throttled: l.handshakeThrottled.Load(),
		InFlight: l.handshakeInFlight.Load(),
		Peak:     l.handshakePeak.Load(), Capacity: cap(l.handshakeGate),
	}
}

func (l *Listener) serveBrokerHandoff(ctx context.Context, nc net.Conn, info ConnInfo) {
	_ = l.b.serveHandoff(ctx, nc, info, func(run func()) {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			run()
		}()
	})
}

// StopAccepting 停止接受新连接，但不等待已接受的连接处理完毕。
//
// 必须与 Close 分开，否则优雅关闭会死锁：Close 等待的那些连接协程
// 正阻塞在读客户端套接字上，而只有 Broker.Close 能放开它们。
// 若按「先关监听器、再关 broker」的直觉顺序写，第一步就永远回不来。
//
// 正确顺序是：StopAccepting → Broker.Close → Close。
func (l *Listener) StopAccepting() error {
	var err error
	l.closed.Do(func() {
		l.acceptMu.Lock()
		l.acceptStopped = true
		if l.stop != nil {
			close(l.stop)
		}
		err = l.ln.Close()
		l.acceptMu.Unlock()
	})
	return err
}

// Close 停止接受新连接并等待已接受的连接处理完毕。
//
// 调用前应先关闭 Broker，否则仍在读取的连接会让本方法一直等待。
// 见 StopAccepting 的说明。
func (l *Listener) Close() error {
	err := l.StopAccepting()
	l.wg.Wait()
	return err
}

// connInfoFromTLS 从握手结果提取 broker 需要的传输层事实。
func connInfoFromTLS(st tls.ConnectionState, remote netip.AddrPort,
	handshake time.Duration,
) ConnInfo {
	info := ConnInfo{
		Remote:      remote,
		TLSVersion:  st.Version,
		ALPN:        st.NegotiatedProtocol,
		HandshakeMS: float64(handshake.Microseconds()) / 1000,
	}
	if len(st.PeerCertificates) == 0 {
		return info
	}
	leaf := st.PeerCertificates[0]
	info.HasCert = true
	info.CertFP = meta.CertFingerprint(sha256.Sum256(leaf.Raw))
	info.CertSerial = serialHex(leaf)
	info.CertSubject = dnFrom(leaf.Subject)
	info.CertIssuer = dnFrom(leaf.Issuer)
	return info
}

// serialHex 把证书序列号格式化为小写十六进制。
//
// 格式必须固定下来：它会被策略变量 ${iot:Certificate.SerialNumber} 引用，
// 而策略是按字符串精确匹配的——格式一变，所有引用该变量的策略全部失效。
func serialHex(c *x509.Certificate) string {
	if c.SerialNumber == nil {
		return ""
	}
	b := c.SerialNumber.Bytes()
	if len(b) == 0 {
		return "00"
	}
	return hex.EncodeToString(b)
}

// dnFrom 提取策略变量可引用的 DN 字段。
//
// 多值 RDN 只取第一个：AWS 为多值字段提供了 .1、.2 形式的变量，
// 本实现尚未支持（属路线图），此处取首个并在文档中说明。
func dnFrom(n pkix.Name) authz.DN {
	return authz.NewDN(authz.DNFields{
		CommonName:         n.CommonName,
		Country:            first(n.Country),
		Organization:       first(n.Organization),
		OrganizationalUnit: first(n.OrganizationalUnit),
		State:              first(n.Province),
		Locality:           first(n.Locality),
		SerialNumber:       n.SerialNumber,
	})
}

func first(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	return ss[0]
}

func remoteAddr(nc net.Conn) netip.AddrPort {
	if ta, ok := nc.RemoteAddr().(*net.TCPAddr); ok {
		if ap, ok := netip.AddrFromSlice(ta.IP); ok {
			return netip.AddrPortFrom(ap.Unmap(), uint16(ta.Port))
		}
	}
	return netip.AddrPort{}
}

// nowMonotonic 返回一个只用于测量时长的时间点。
//
// contract:allow-time-now 这里只做耗时的差值计算（握手，以及 CONNECT
// 的分段埋点 OBS-148），不参与任何排序或版本比较；注入时钟提供的是
// 毫秒墙上时间，分辨率不足以测握手，更不足以测微秒级的那几段。
func nowMonotonic() time.Time { return time.Now() }

// msSince 返回自 start 以来的毫秒数，供分段耗时埋点使用。
//
// 与 nowMonotonic 同一个理由：注入时钟只有毫秒墙上时间，而 CONNECT
// 里最快的那两段（认证、所有权抢占）是微秒级，毫秒分辨率量出来全是 0。
func msSince(start time.Time) float64 {
	return float64(nowMonotonic().Sub(start).Microseconds()) / 1000
}
