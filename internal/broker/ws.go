package broker

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// WebSocket 在本仓库内实现，不引入 gorilla 或 x/net/websocket。
// MQTT 是字节流：一个报文可以跨多个帧，一帧里也可以有多个报文。
// 因此升级之后交给 broker 的是把二进制帧拼回去的 net.Conn，
// 控制帧（ping、pong、close）不进入这股字节流。

const (
	wsCont   = 0x0
	wsText   = 0x1
	wsBinary = 0x2
	wsClose  = 0x8
	wsPing   = 0x9
	wsPong   = 0xA

	wsGUID             = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	wsPath             = "/mqtt"
	maxWSPayload       = 1 << 20
	wsChunk            = 32 << 10
	defaultWSLifetime  = 24 * time.Hour
	maxEmptyWSFrames   = 32
	wsHandshakeBufSize = 64 << 10
)

// WebSocketOptions 是 MQTT over WebSocket 监听器的参数。
//
// 它与 443 上的 ALPN 客户端证书接入是两个监听器。两者都常写 443，
// 但不能绑在同一个套接字上。
type WebSocketOptions struct {
	Certificate      tls.Certificate
	MinVersion       uint16
	RequireSNI       bool
	HandshakeTimeout time.Duration
	// Checker 校验查询串里的 SigV4。Region 与 Secret 必填。
	// Services 为空时只接受 iotdevicegateway。
	Checker sigv4.Checker
	// Path 为空时是 /mqtt。
	Path string
	// MaxLifetime 为空时是 24 小时（NET-023）。
	MaxLifetime time.Duration
	// RedirectTo 到期时作为 MQTT 5 的 Server Reference。为空则只关连接。
	RedirectTo string
}

type webSocketConfig struct {
	checker     sigv4.Checker
	path        string
	maxLifetime time.Duration
	redirectTo  string
}

// ListenWebSocket 在 addr 上接受 WSS，验签通过后把字节流交给 broker。
func (b *Broker) ListenWebSocket(addr string, opt WebSocketOptions) (*Listener, error) {
	if len(opt.Certificate.Certificate) == 0 {
		return nil, errors.New("broker: 缺少服务端证书")
	}
	if opt.Checker.Region == "" || opt.Checker.Secret == nil {
		return nil, errors.New("broker: WebSocket 端点缺少 SigV4 区域或密钥")
	}
	if opt.Checker.Services == nil {
		opt.Checker.Services = map[string]bool{sigv4.ServiceDeviceGateway: true}
	}
	if opt.Path == "" {
		opt.Path = wsPath
	}
	if opt.MaxLifetime <= 0 {
		opt.MaxLifetime = defaultWSLifetime
	}
	if opt.MinVersion == 0 {
		opt.MinVersion = tls.VersionTLS12
	}
	if opt.HandshakeTimeout <= 0 {
		opt.HandshakeTimeout = defaultHandshakeTimeout
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{opt.Certificate},
		ClientAuth:   tls.NoClientCert,
		MinVersion:   opt.MinVersion,
		NextProtos:   []string{"http/1.1"},
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("broker: 监听 %s 失败: %w", addr, err)
	}
	return &Listener{
		b:    b,
		ln:   ln,
		tlsc: cfg,
		opt: TLSOptions{
			MinVersion:       opt.MinVersion,
			RequireSNI:       opt.RequireSNI,
			HandshakeTimeout: opt.HandshakeTimeout,
		},
		wss: &webSocketConfig{
			checker:     opt.Checker,
			path:        opt.Path,
			maxLifetime: opt.MaxLifetime,
			redirectTo:  opt.RedirectTo,
		},
	}, nil
}

// acceptWebSocket 完成 HTTP 升级，再把拼好的字节流交给 broker。
func (l *Listener) acceptWebSocket(ctx context.Context, tc net.Conn, info ConnInfo) {
	ws, accessKey, err := l.upgradeWebSocket(tc)
	if err != nil {
		_ = tc.Close()
		return
	}
	info.AccessKeyID = accessKey
	info.MaxLifetime = l.wss.maxLifetime
	info.RedirectTo = l.wss.redirectTo
	_ = l.b.Serve(ctx, ws, info)
}

func (l *Listener) upgradeWebSocket(tc net.Conn) (*wsConn, string, error) {
	// contract:allow-time-now 握手读超时，不参与排序
	_ = tc.SetDeadline(time.Now().Add(l.opt.HandshakeTimeout))
	defer func() { _ = tc.SetDeadline(time.Time{}) }()

	br := bufio.NewReaderSize(tc, wsHandshakeBufSize)
	req, err := http.ReadRequest(br)
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			writeHTTPStatus(tc, http.StatusBadRequest, "Bad Request")
		}
		return nil, "", err
	}
	if code, status := rejectUpgrade(req, l.wss.path); code != 0 {
		writeHTTPStatus(tc, code, status)
		return nil, "", errors.New("broker: websocket 升级被拒绝")
	}
	hdr := req.Header.Clone()
	if req.Host != "" {
		hdr.Set("Host", req.Host)
	}
	scope, verr := l.wss.checker.Verify(req.Method, req.URL.Path, req.URL.RawQuery, hdr, nil)
	if verr != nil || scope.Service != sigv4.ServiceDeviceGateway || scope.AccessKeyID == "" {
		writeHTTPStatus(tc, http.StatusForbidden, "Forbidden")
		return nil, "", errors.New("broker: websocket 验签失败")
	}
	accept, err := wsAccept(req.Header.Get("Sec-WebSocket-Key"))
	if err != nil {
		writeHTTPStatus(tc, http.StatusBadRequest, "Bad Request")
		return nil, "", err
	}
	if err := writeSwitching(tc, accept); err != nil {
		return nil, "", err
	}
	return newWSConn(tc, br, false), scope.AccessKeyID, nil
}

func rejectUpgrade(req *http.Request, path string) (int, string) {
	if req.URL == nil || req.URL.Path != path {
		return http.StatusNotFound, "Not Found"
	}
	if req.Method != http.MethodGet || req.ProtoMajor != 1 || req.ProtoMinor < 1 {
		return http.StatusBadRequest, "Bad Request"
	}
	if !headerHasToken(req.Header, "Upgrade", "websocket") ||
		!headerHasToken(req.Header, "Connection", "Upgrade") ||
		req.Header.Get("Sec-WebSocket-Version") != "13" ||
		!headerHasToken(req.Header, "Sec-WebSocket-Protocol", "mqtt") {
		return http.StatusBadRequest, "Bad Request"
	}
	if _, err := wsAccept(req.Header.Get("Sec-WebSocket-Key")); err != nil {
		return http.StatusBadRequest, "Bad Request"
	}
	return 0, ""
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, p := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(p), token) {
				return true
			}
		}
	}
	return false
}

func wsAccept(key string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 16 {
		return "", errors.New("broker: Sec-WebSocket-Key 不合法")
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

func writeHTTPStatus(nc net.Conn, code int, status string) {
	_, _ = fmt.Fprintf(nc, "HTTP/1.1 %d %s\r\nConnection: close\r\nContent-Length: 0\r\n\r\n", code, status)
}

func writeSwitching(nc net.Conn, accept string) error {
	_, err := fmt.Fprintf(nc, "HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: %s\r\n"+
		"Sec-WebSocket-Protocol: mqtt\r\n\r\n", accept)
	return err
}

// wsConn 把 WebSocket 帧还原成字节流。client 为真时写出带掩码，
// 读入要求对端不带掩码；服务端相反（RFC 6455）。
type wsConn struct {
	nc     net.Conn
	br     *bufio.Reader
	client bool

	wmu   sync.Mutex
	rmu   sync.Mutex
	dead  bool
	frag  bool
	pend  []byte
	stick error

	closeOnce sync.Once
	closeErr  error
}

func newWSConn(nc net.Conn, br *bufio.Reader, client bool) *wsConn {
	if br == nil {
		br = bufio.NewReader(nc)
	}
	return &wsConn{nc: nc, br: br, client: client}
}

func (c *wsConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if c.stick != nil {
		return 0, c.stick
	}
	empty := 0
	for len(c.pend) == 0 {
		payload, err := c.nextData()
		if err != nil {
			c.stick = err
			return 0, err
		}
		if len(payload) == 0 {
			empty++
			if empty > maxEmptyWSFrames {
				err = reason.Errorf(reason.ProtocolError, "websocket 空帧过多")
				c.stick = err
				return 0, err
			}
			continue
		}
		c.pend = payload
	}
	n := copy(p, c.pend)
	c.pend = c.pend[n:]
	return n, nil
}

// nextData 读到下一截二进制载荷。控制帧在这里消化。
// 调用方持有 rmu。
func (c *wsConn) nextData() ([]byte, error) {
	for {
		fin, op, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch op {
		case wsPing:
			c.wmu.Lock()
			err = c.writeFrameLocked(wsPong, payload, true)
			c.wmu.Unlock()
			if err != nil {
				return nil, err
			}
		case wsPong:
		case wsClose:
			c.wmu.Lock()
			_ = c.writeFrameLocked(wsClose, payload, true)
			c.wmu.Unlock()
			return nil, io.EOF
		case wsText:
			return nil, reason.Errorf(reason.ProtocolError, "websocket 文本帧不能承载 MQTT")
		case wsBinary, wsCont:
			if op == wsCont && !c.frag {
				return nil, reason.Errorf(reason.ProtocolError, "websocket 续帧没有起始帧")
			}
			if op == wsBinary && c.frag {
				return nil, reason.Errorf(reason.ProtocolError, "websocket 分片尚未结束")
			}
			// 立刻交出这一片，不等 FIN。MQTT 报文的后半段在下一帧里，
			// 读循环要能先看到前半段。
			c.frag = !fin
			return payload, nil
		default:
			return nil, reason.Errorf(reason.ProtocolError, "websocket 未知操作码")
		}
	}
}

func (c *wsConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	hdr, err := c.readFull(2)
	if err != nil {
		return false, 0, nil, err
	}
	if hdr[0]&0x70 != 0 {
		return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket RSV 必须为 0")
	}
	fin = hdr[0]&0x80 != 0
	op = hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	ln := int(hdr[1] & 0x7f)
	switch ln {
	case 126:
		b, err := c.readFull(2)
		if err != nil {
			return false, 0, nil, err
		}
		ln = int(binary.BigEndian.Uint16(b))
		if ln < 126 {
			return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 长度编码不是最短形式")
		}
	case 127:
		b, err := c.readFull(8)
		if err != nil {
			return false, 0, nil, err
		}
		u := binary.BigEndian.Uint64(b)
		if u > maxWSPayload || u&(1<<63) != 0 {
			return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 帧超过 1MiB")
		}
		ln = int(u)
		if ln <= 65535 {
			return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 长度编码不是最短形式")
		}
	}
	if ln > maxWSPayload {
		return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 帧超过 1MiB")
	}
	if op >= 8 && (!fin || ln > 125) {
		return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 控制帧不合法")
	}
	if c.client == masked {
		// 客户端读到掩码，或服务端读到未掩码，都是对端违反 RFC 6455。
		return false, 0, nil, reason.Errorf(reason.ProtocolError, "websocket 掩码方向错误")
	}
	var mask [4]byte
	if masked {
		b, err := c.readFull(4)
		if err != nil {
			return false, 0, nil, err
		}
		copy(mask[:], b)
	}
	payload, err = c.readFull(ln)
	if err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, op, payload, nil
}

func (c *wsConn) readFull(n int) ([]byte, error) {
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(c.br, buf)
	return buf, err
}

func (c *wsConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for off := 0; off < len(p); {
		n := len(p) - off
		if n > wsChunk {
			n = wsChunk
		}
		op := byte(wsBinary)
		if off > 0 {
			op = wsCont
		}
		if err := c.writeFrameLocked(op, p[off:off+n], off+n == len(p)); err != nil {
			return off, err
		}
		off += n
	}
	return len(p), nil
}

func (c *wsConn) writeFrame(op byte, payload []byte, fin bool) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeFrameLocked(op, payload, fin)
}

func (c *wsConn) writeFrameLocked(op byte, payload []byte, fin bool) error {
	if c.dead {
		return net.ErrClosed
	}
	var hdr [14]byte
	if fin {
		hdr[0] = 0x80 | op
	} else {
		hdr[0] = op
	}
	maskBit := byte(0)
	if c.client {
		maskBit = 0x80
	}
	n := 0
	ln := len(payload)
	switch {
	case ln > 65535:
		hdr[1] = maskBit | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(ln))
		n = 10
	case ln > 125:
		hdr[1] = maskBit | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(ln))
		n = 4
	default:
		hdr[1] = maskBit | byte(ln)
		n = 2
	}
	if c.client {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		copy(hdr[n:n+4], key[:])
		n += 4
		payload = maskCopy(key, payload)
	}
	if _, err := c.nc.Write(hdr[:n]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := c.nc.Write(payload)
	return err
}

func maskCopy(mask [4]byte, payload []byte) []byte {
	out := make([]byte, len(payload))
	for i, b := range payload {
		out[i] = b ^ mask[i%4]
	}
	return out
}

func (c *wsConn) Close() error {
	c.closeOnce.Do(func() {
		c.wmu.Lock()
		_ = c.writeFrameLocked(wsClose, []byte{0x03, 0xe8}, true)
		c.dead = true
		c.wmu.Unlock()
		c.closeErr = c.nc.Close()
	})
	return c.closeErr
}

func (c *wsConn) LocalAddr() net.Addr                { return c.nc.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.nc.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error      { return c.nc.SetDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.nc.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.nc.SetWriteDeadline(t) }

var _ net.Conn = (*wsConn)(nil)
