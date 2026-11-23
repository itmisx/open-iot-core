// 压测器自己的 MQTT 客户端：拨号、TLS、收发与各场景的循环。

package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

// ---------- 客户端 ----------

type client struct {
	c net.Conn
	// 每条虚拟设备长期持有的接收缓冲默认只有 256 B，且直接内联在 client
	// 对象里。旧实现的 16 KiB bufio.Reader + 16 KiB bufio.Writer 在百万
	// 连接时仅压测端就要约 30.5 GiB，压测机可能先于 broker OOM。
	rsmall [256]byte
	rbuf   []byte
	roff   int
	rlen   int
	// out 只活到紧随 send 的 flush；flush 后置 nil，不让 CONNECT 或大
	// PUBLISH 的编码容量随空闲长连接常驻。
	out []byte
	pid uint16
	// tlsResumed 来自本次握手的 ConnectionState，用来证明 reconnect
	// 测到的确实是 ticket 恢复，而不是第二次完整握手碰巧更快。
	tlsResumed bool
	// ver 是本连接协商的协议版本，编解码都要它。
	//
	// 不能在 send/recv 里写死 V311：持久会话那一档走 MQTT 5（过期间隔
	// 只有 5 能表达），而按 3.1.1 编码会把属性整段丢掉——于是服务端看到
	// 的是一个过期间隔为 0 的会话，那一档量的就还是非持久路径，而且
	// 完全不报错。
	ver packet.Version
}

func newClient(c net.Conn, ver packet.Version) *client {
	cl := &client{c: c, ver: ver}
	cl.rbuf = cl.rsmall[:]
	return cl
}

func (r *runner) dial(addr, clientID string) (*client, error) {
	dialer := r.dialerFor(clientID)
	var c net.Conn
	var err error
	if r.tlsConfig != nil {
		cfg := r.tlsConfigFor(addr, clientID)
		c, err = tls.DialWithDialer(dialer, "tcp", addr, cfg)
	} else {
		c, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	// net.Dialer.Timeout 只覆盖 TCP（tls.DialWithDialer 时也覆盖 TLS），
	// 不覆盖后面的 MQTT CONNECT/CONNACK。服务端已经收下套接字却卡在
	// ownership 或会话提案时，压测器过去会永久等在 recv，整轮既不失败
	// 也不出结果。把同一个上限延伸到协议握手，成功后再清掉。
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if len(r.proxyCertDER) != 0 {
		header, herr := proxyV2Header(c.LocalAddr(), c.RemoteAddr(), r.proxyCertDER,
			r.proxyFingerprintOnly)
		if herr != nil {
			_ = c.Close()
			return nil, herr
		}
		if herr = writeConnAll(c, header); herr != nil {
			_ = c.Close()
			return nil, fmt.Errorf("发送 PROXY v2 header: %w", herr)
		}
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	cl := newClient(c, packet.V311)
	if tc, ok := c.(*tls.Conn); ok {
		cl.tlsResumed = tc.ConnectionState().DidResume
	}
	// KeepAlive 取 0：压测器不发 PINGREQ，而非零的保活会让空闲的连接在
	// connect 场景的保持期内被服务端按超时断开——那样量到的「每连接内存」
	// 是一个正在缩小的连接数的平均值。
	cp := &packet.Connect{Version: packet.V311, ClientID: []byte(clientID), KeepAlive: 0, CleanStart: true}
	if r.persistent {
		cl.ver = packet.V5
		// 过期间隔只有 MQTT 5 能表达。3.1.1 的 clean=false 也是持久会话，
		// 但它的过期间隔由服务端定（见 broker 里的 defaultSessionExpiry311），
		// 压测器不该依赖那个默认值——它变了，这一档量的东西就悄悄变了。
		cp.Version = packet.V5
		cp.CleanStart = false
		cp.Props.SessionExpiry = r.expiry
		cp.Props.Set(packet.PropSessionExpiry)
	}
	if err := cl.send(cp); err != nil {
		cl.close()
		return nil, err
	}
	if err := cl.flush(); err != nil {
		cl.close()
		return nil, err
	}
	p, err := cl.recv()
	if err != nil {
		cl.close()
		return nil, err
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		cl.close()
		return nil, fmt.Errorf("期待 CONNACK，收到 %s", p.Type())
	}
	if ack.Code != 0 {
		cl.close()
		return nil, fmt.Errorf("连接被拒，原因码 0x%02x（clientId=%s）", byte(ack.Code), clientID)
	}
	_ = c.SetDeadline(time.Time{})
	return cl, nil
}

func (r *runner) initDialers() {
	r.dialer = net.Dialer{Timeout: 10 * time.Second}
	r.sourceDialers = make([]net.Dialer, len(r.sourceIPs))
	for i, ip := range r.sourceIPs {
		r.sourceDialers[i] = net.Dialer{
			Timeout:   10 * time.Second,
			LocalAddr: &net.TCPAddr{IP: ip},
		}
		configureSourcePortReuse(&r.sourceDialers[i], ip)
	}
}

func (r *runner) dialerFor(clientID string) *net.Dialer {
	if len(r.sourceDialers) == 0 {
		return &r.dialer
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))
	return &r.sourceDialers[h.Sum64()%uint64(len(r.sourceDialers))]
}

// tlsConfigFor 返回一次拨号使用的 TLS 配置。冷连且显式给了 ServerName 时，
// 基础配置在 runner 构造后不再修改，可以按 crypto/tls 的契约并发共享；过去
// 每设备 Clone 一次，在百万冷连时把一百万份短命 slice/config 留给负载机 GC。
// reconnect 必须克隆，因为每个 clientId 要安装隔离的 ticket cache；没有
// ServerName 时也必须克隆后按目标补齐，不能改写并发拨号共享的基础配置。
func (r *runner) tlsConfigFor(addr, clientID string) *tls.Config {
	cfg := r.tlsConfig
	if cfg == nil {
		return nil
	}
	if !r.reconnect && cfg.ServerName != "" {
		return cfg
	}
	cfg = cfg.Clone()
	if r.reconnect {
		cfg.ClientSessionCache = r.tlsSessionCache(clientID)
	}
	if cfg.ServerName == "" {
		cfg.ServerName, _, _ = net.SplitHostPort(addr)
	}
	return cfg
}

func writeConnAll(c net.Conn, p []byte) error {
	for len(p) > 0 {
		n, err := c.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// proxyV2Header 构造与 broker.ProxyV2TLVClientCert 相同的稳定线格式。
// 工具刻意不 import 整个 broker 包，避免压测二进制带入服务端依赖图。
func proxyV2Header(source, destination net.Addr, certDER []byte, fingerprintOnly bool) ([]byte, error) {
	src, ok := source.(*net.TCPAddr)
	if !ok {
		return nil, fmt.Errorf("PROXY v2 source address 类型为 %T", source)
	}
	dst, ok := destination.(*net.TCPAddr)
	if !ok {
		return nil, fmt.Errorf("PROXY v2 destination address 类型为 %T", destination)
	}
	const (
		clientCertTLV      = 0xe0
		certFingerprintTLV = 0xe1
		formatVersion      = 1
	)
	value := []byte{formatVersion, 0x03, 0x04, byte(len("x-amzn-mqtt-ca"))}
	value = append(value, "x-amzn-mqtt-ca"...)
	tlvType := byte(clientCertTLV)
	if fingerprintOnly {
		sum := sha256.Sum256(certDER)
		value = append(value, sum[:]...)
		tlvType = certFingerprintTLV
	} else {
		value = append(value, certDER...)
	}
	if len(value) > int(^uint16(0)) {
		return nil, fmt.Errorf("PROXY v2 certificate TLV 为 %d 字节，超过 uint16", len(value))
	}
	tlv := []byte{tlvType, 0, 0}
	binary.BigEndian.PutUint16(tlv[1:3], uint16(len(value)))
	tlv = append(tlv, value...)

	var family byte
	var addresses []byte
	if sip, dip := src.IP.To4(), dst.IP.To4(); sip != nil && dip != nil {
		family = 0x11
		addresses = make([]byte, 12)
		copy(addresses[:4], sip)
		copy(addresses[4:8], dip)
		binary.BigEndian.PutUint16(addresses[8:10], uint16(src.Port))
		binary.BigEndian.PutUint16(addresses[10:12], uint16(dst.Port))
	} else {
		sip, dip := src.IP.To16(), dst.IP.To16()
		if sip == nil || dip == nil {
			return nil, fmt.Errorf("PROXY v2 地址无效: %s -> %s", src, dst)
		}
		family = 0x21
		addresses = make([]byte, 36)
		copy(addresses[:16], sip)
		copy(addresses[16:32], dip)
		binary.BigEndian.PutUint16(addresses[32:34], uint16(src.Port))
		binary.BigEndian.PutUint16(addresses[34:36], uint16(dst.Port))
	}
	body := append(addresses, tlv...)
	if len(body) > int(^uint16(0)) {
		return nil, fmt.Errorf("PROXY v2 body 为 %d 字节，超过 uint16", len(body))
	}
	header := []byte("\r\n\r\n\x00\r\nQUIT\n")
	header = append(header, 0x21, family, 0, 0)
	binary.BigEndian.PutUint16(header[14:16], uint16(len(body)))
	return append(header, body...), nil
}

// sourceIPFor 让同一设备在冷连与重连时稳定落到同一个源 IP。用 clientId
// 哈希而不是全局计数器，避免并发调度改变第二波分布并把某个 IP 的端口打满。
func (r *runner) sourceIPFor(clientID string) net.IP {
	if len(r.sourceIPs) == 0 {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))
	return r.sourceIPs[h.Sum64()%uint64(len(r.sourceIPs))]
}

func (r *runner) tlsSessionCache(clientID string) tls.ClientSessionCache {
	return r.tlsSessions.forDevice(clientID)
}

func (cl *client) send(p packet.Packet) error {
	b, err := packet.Encode(p, cl.ver)
	if err != nil {
		return err
	}
	if len(cl.out) == 0 {
		cl.out = b
	} else {
		cl.out = append(cl.out, b...)
	}
	return nil
}

func (cl *client) recv() (packet.Packet, error) {
	for {
		if cl.rlen > cl.roff {
			p, n, err := packet.Decode(cl.rbuf[cl.roff:cl.rlen], cl.ver)
			switch {
			case err == nil:
				cl.roff += n
				return p, nil
			case errors.Is(err, packet.ErrIncomplete):
			default:
				return nil, err
			}
		}
		// 只有确实需要更多字节时才压实；上一条返回的 packet 到下一次
		// recv 前一直有效，粘包时不会被后续字节覆盖。
		if cl.roff > 0 {
			copy(cl.rbuf, cl.rbuf[cl.roff:cl.rlen])
			cl.rlen -= cl.roff
			cl.roff = 0
		}
		if cl.rlen == len(cl.rbuf) {
			if len(cl.rbuf) >= packet.MaxRemainingLength+5 {
				return nil, errors.New("MQTT 报文超过协议最大长度")
			}
			next := len(cl.rbuf) * 2
			if next > packet.MaxRemainingLength+5 {
				next = packet.MaxRemainingLength + 5
			}
			grown := make([]byte, next)
			copy(grown, cl.rbuf[:cl.rlen])
			cl.rbuf = grown
		}
		n, err := cl.c.Read(cl.rbuf[cl.rlen:])
		if n > 0 {
			cl.rlen += n
		}
		if err != nil {
			if n > 0 && errors.Is(err, io.EOF) {
				continue
			}
			return nil, err
		}
	}
}

// flush 发出缓冲里的字节，并带写超时。
//
// 没有写超时的话，服务端被打满、停止读取的那一刻，发布者会永久卡在
// flush 上：测量窗口结束后 wg.Wait() 再也不返回，整个压测器挂住而不报错。
// 这与 CLU-119 是同一个形状——一条不设超时的写入足以让一个方向彻底沉默。
func (cl *client) flush() error {
	if len(cl.out) == 0 {
		return nil
	}
	if err := cl.c.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	out := cl.out
	cl.out = nil
	for len(out) > 0 {
		n, err := cl.c.Write(out)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		out = out[n:]
	}
	return nil
}

// goodbye 在收尾时发一个 DISCONNECT。
//
// 直接 close 会让还有未读数据的那一侧发 RST，而 RST 会丢掉已经发出但
// 服务端尚未读完的那个报文——服务端把它记成 malformed（指标里看得到）。
// 那不是协议缺陷，是压测器收尾的产物，但一个没来由的 malformed 计数会
// 让下一个读指标的人白查一轮。
func (cl *client) goodbye() {
	_ = cl.send(&packet.Disconnect{})
	_ = cl.flush()
}

func (cl *client) subscribe(filter string, qos packet.QoS) error {
	cl.pid++
	sub := &packet.Subscribe{PacketID: cl.pid, Filters: []packet.Subscription{{Filter: []byte(filter), QoS: qos}}}
	if err := cl.send(sub); err != nil {
		return err
	}
	if err := cl.flush(); err != nil {
		return err
	}
	p, err := cl.recv()
	if err != nil {
		return err
	}
	sa, ok := p.(*packet.Suback)
	if !ok {
		return fmt.Errorf("期待 SUBACK，收到 %s", p.Type())
	}
	if len(sa.Codes) == 0 || byte(sa.Codes[0]) > 0x02 {
		return fmt.Errorf("订阅被拒，原因码 0x%02x（%s）", byte(sa.Codes[0]), filter)
	}
	return nil
}

func (cl *client) close() { _ = cl.c.Close() }

// drain 是订阅侧的读循环：计数、算延迟、回 PUBACK。
func (cl *client) drain(st *stats, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		_ = cl.c.SetReadDeadline(time.Now().Add(2 * time.Second))
		p, err := cl.recv()
		if err != nil {
			if isTimeout(err) {
				continue
			}
			select {
			case <-stop:
			default:
				st.errs.Add(1)
			}
			return
		}
		pub, ok := p.(*packet.Publish)
		if !ok {
			continue
		}
		// 接线探针不算测量样本。它的负载不是时间戳，按时间戳解会得到一个
		// 以 1970 年为起点的「延迟」，单独一条就足以把 p99 和最大值拉到
		// 五十多年——而那条曲线看上去只是「有个离群点」，不会报错。
		if bytes.Equal(pub.Payload, probeMarker) {
			continue
		}
		if len(pub.Payload) >= 8 {
			sent := int64(binary.BigEndian.Uint64(pub.Payload[:8]))
			st.observe(float64(time.Now().UnixNano()-sent) / 1e6)
		}
		st.delivered.Add(1)
		if pub.QoS == packet.QoS1 {
			if err := cl.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pub.PacketID}); err != nil {
				return
			}
			if err := cl.flush(); err != nil {
				return
			}
		}
	}
}

// pump 是发布侧的循环。QoS 1 时在途上限由 -inflight 控制，PUBACK 在同一个
// 协程里读——另起一个读协程会让「在途」的判定分散在两处，而那正是本仓库
// 反复修过的那类缺陷（第 6 节第 11 条）。
// phase 是这个发布者在一个发送周期内的相位，取 [0,1)。
//
// 它必须显式给出。一百个按同一速率发的发布者若同时起跑，就会在同一个
// 毫秒边界上齐发——服务端看到的是每周期一百条的突发，而不是稳态的每秒
// 十万条。量出来的丢弃是突发造成的队列尖峰，而且各轮之间不可复现：实测
// 同一档基础级的丢弃在 0% 与 10% 之间跳，阶梯还出现过 15 万比 20 万更差
// 的非单调。
//
// 旧版本里这件事是**偶然**成立的：建连与订阅是串行的，于是第 i 个发布者
// 天然比第 0 个晚起跑几毫秒。把全部建连提前到统一探完接线之后，那层偶然
// 的错相位就消失了，于是必须在这里补回来。
func (cl *client) pump(r *runner, topic string, phase float64, st *stats, stop <-chan struct{}) {
	payload := make([]byte, r.payload)
	var (
		interval time.Duration
		next     time.Time
	)
	if r.rate > 0 {
		interval = time.Second / time.Duration(r.rate)
		next = time.Now().Add(time.Duration(float64(interval) * phase))
	}
	inflight := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		if r.rate > 0 {
			now := time.Now()
			if now.Before(next) {
				time.Sleep(next.Sub(now))
			}
			next = next.Add(interval)
			// 落后超过一个发送间隔的 100 倍时重置节拍，不补发。
			//
			// 不重置的话，一次 GC 或调度抖动之后发布者会以不受限的速度
			// 追赶，于是「每秒 500 条」的设定在图上表现为一个尖峰加一段
			// 空白——而尖峰那一段量到的延迟是排队延迟，与设定的速率无关。
			if d := time.Since(next); d > 100*interval {
				next = time.Now()
			}
		}
		binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
		cl.pid++
		if cl.pid == 0 {
			cl.pid = 1
		}
		pub := &packet.Publish{Topic: []byte(topic), Payload: payload, QoS: r.qos}
		if r.qos == packet.QoS1 {
			pub.PacketID = cl.pid
		}
		if err := cl.send(pub); err != nil {
			st.errs.Add(1)
			return
		}
		if err := cl.flush(); err != nil {
			st.errs.Add(1)
			return
		}
		st.published.Add(1)
		if r.qos != packet.QoS1 {
			continue
		}
		inflight++
		for inflight >= r.inflight {
			_ = cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
			p, err := cl.recv()
			if err != nil {
				if isTimeout(err) {
					select {
					case <-stop:
						return
					default:
						continue
					}
				}
				st.errs.Add(1)
				return
			}
			if a, ok := p.(*packet.Ack); ok && a.Kind == packet.PUBACK {
				inflight--
				st.acked.Add(1)
			}
		}
	}
}

// shadowLoop 发影子更新并读回 accepted，两者在同一个协程里：accepted 的
// 到达就是「这条写入已经过了复制日志」的判据。
func (cl *client) shadowLoop(r *runner, topic string, st *stats, stop <-chan struct{}) {
	n := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		gen := st.shadowGeneration()
		n++
		t0 := time.Now()
		body := fmt.Sprintf(`{"state":{"reported":{"n":%d,"ts":%d}},"clientToken":"%d"}`,
			n, t0.UnixNano(), n)
		wantToken := []byte(fmt.Sprintf(`"clientToken":"%d"`, n))
		cl.pid++
		if cl.pid == 0 {
			cl.pid = 1
		}
		pub := &packet.Publish{Topic: []byte(topic), Payload: []byte(body), QoS: packet.QoS0}
		if err := cl.send(pub); err != nil {
			st.errs.Add(1)
			return
		}
		if err := cl.flush(); err != nil {
			st.errs.Add(1)
			return
		}
		st.shadowPublished(gen)
		// 等 accepted。一次一条：影子按 thing 串行，发快了只会堆在队列里，
		// 量出来的「吞吐」就变成了队列长度。
		for {
			_ = cl.c.SetReadDeadline(time.Now().Add(r.shadowResponseTimeout))
			p, err := cl.recv()
			if err != nil {
				if isTimeout(err) {
					select {
					case <-stop:
						// 已计入published却在排空期限内收不到accepted，必须作为
						// 测量错误留下；只返回会让JSON看起来是“零错误少一条”。
						st.shadowTimedOut(gen, time.Since(t0))
						return
					default:
					}
					// 不能直接开始下一条却不记账：旧accepted若稍后到达，会被
					// 误算成新请求的结果。clientToken负责隔离迟到回复。
					st.shadowTimedOut(gen, time.Since(t0))
					// 结果是歧义的：服务端可能已经提交。继续写同一个thing虽不是
					// 重发相同报文，却会覆盖尚未确认的状态并让迟到响应产生级联
					// 假超时。停止这个设备流，交给最终守恒门禁判失败。
					return
				}
				st.errs.Add(1)
				return
			}
			pb, ok := p.(*packet.Publish)
			if !ok {
				continue
			}
			// Shadow响应在服务端标为QoS 1；订阅方选择QoS 1时必须确认，
			// 包括迟到或不属于当前clientToken的响应。只确认当前token会让
			// 旧响应永久留在broker在途表，最终把连接拖进重传/配额故障。
			if pb.QoS == packet.QoS1 {
				if err := cl.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pb.PacketID}); err != nil {
					st.shadowFailed(gen)
					return
				}
				if err := cl.flush(); err != nil {
					st.shadowFailed(gen)
					return
				}
			}
			if !bytes.Contains(pb.Payload, wantToken) {
				st.shadowStaleReply(gen)
				continue
			}
			if bytes.Contains(pb.Topic, []byte("/rejected")) {
				st.shadowRejectedReply(gen)
				break
			}
			if !bytes.Contains(pb.Topic, []byte("/accepted")) {
				continue
			}
			// 延迟量的是「发出更新到收到 accepted」的往返：这条路径上有
			// 一次复制日志的提交，而那正是影子与普通主题的区别所在。
			st.shadowDelivered(gen, float64(time.Since(t0).Nanoseconds())/1e6)
			break
		}
	}
}
