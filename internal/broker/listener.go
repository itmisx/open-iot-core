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
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
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
}

const defaultHandshakeTimeout = 10 * time.Second

// Listener 在一个地址上接受连接并交给 broker。
type Listener struct {
	b    *Broker
	ln   net.Listener
	tlsc *tls.Config
	opt  TLSOptions

	wg     sync.WaitGroup
	closed sync.Once

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
		Certificates: []tls.Certificate{opt.Certificate},
		ClientCAs:    opt.ClientCAs,
		ClientAuth:   clientAuth,
		MinVersion:   opt.MinVersion,
		NextProtos:   opt.NextProtos,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{b: b, ln: ln, tlsc: cfg, opt: opt}, nil
}

// ListenPlain 在 addr 上启动一个明文监听器。
//
// 它只应用于已在前置代理终结 TLS 的部署，或测试。
// 该路径下客户端身份须由代理经其他途径提供（PRD NET-017）。
func (b *Broker) ListenPlain(addr string) (*Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{b: b, ln: ln}, nil
}

// Addr 返回实际监听的地址。
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Serve 接受连接直到监听器关闭。
func (l *Listener) Serve(ctx context.Context) error {
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
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			l.handle(ctx, nc)
		}()
	}
}

// handle 完成握手并把连接交给 broker。
func (l *Listener) handle(ctx context.Context, nc net.Conn) {
	info := ConnInfo{Remote: remoteAddr(nc)}

	if l.tlsc == nil {
		_ = l.b.Serve(ctx, nc, info)
		return
	}

	tc := tls.Server(nc, l.tlsc)
	// 握手必须显式完成并计时：计时值进入连接事件，是定位握手 CPU 瓶颈的
	// 唯一依据（PRD ARCH-036）。延迟握手则取不到这个数。
	hctx, cancel := context.WithTimeout(ctx, l.opt.HandshakeTimeout)
	start := nowMonotonic()
	err := tc.HandshakeContext(hctx)
	elapsed := nowMonotonic().Sub(start)
	cancel()
	if err != nil {
		// 握手失败的连接还没有 clientId，无法产出连接事件；
		// 握手层面的失败由监听器的指标与日志承载。
		_ = tc.Close()
		return
	}

	st := tc.ConnectionState()
	if l.opt.RequireSNI && st.ServerName == "" {
		_ = tc.Close()
		return
	}
	info = connInfoFromTLS(st, info.Remote, elapsed)
	if l.wss != nil {
		l.acceptWebSocket(ctx, tc, info)
		return
	}
	_ = l.b.Serve(ctx, tc, info)
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
		err = l.ln.Close()
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
	return authz.DN{
		CommonName:         n.CommonName,
		Country:            first(n.Country),
		Organization:       first(n.Organization),
		OrganizationalUnit: first(n.OrganizationalUnit),
		State:              first(n.Province),
		Locality:           first(n.Locality),
		SerialNumber:       n.SerialNumber,
	}
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
// contract:allow-time-now 这里只做握手耗时的差值计算，不参与任何
// 排序或版本比较；注入时钟提供的是毫秒墙上时间，分辨率不足以测握手。
func nowMonotonic() time.Time { return time.Now() }
