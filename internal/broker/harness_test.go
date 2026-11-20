package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// testClock 是可推进的注入时钟（契约 08）。
type testClock struct {
	mu sync.Mutex
	ms int64
}

func newClock() *testClock { return &testClock{ms: 1_700_000_000_000} }

func (c *testClock) now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ms
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.ms += d.Milliseconds()
	c.mu.Unlock()
}

// fakeTimers 是由测试时钟驱动的定时器。
//
// 装置里的时钟是冻结的，真实定时器与它对不上：遗嘱延迟（MQTT-061）
// 这类行为就只能靠 sleep 去验，既慢又不稳，而"延迟 3600 秒"根本没法验。
// 这里让定时器跟着 rig.advance 走，时间推到哪里就触发到哪里。
type fakeTimers struct {
	mu      sync.Mutex
	clk     *testClock
	pending []*fakeTimer
}

type fakeTimer struct {
	at   int64
	f    func()
	done bool
}

func (ft *fakeTimers) afterFunc(d time.Duration, f func()) func() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	t := &fakeTimer{at: ft.clk.now() + d.Milliseconds(), f: f}
	ft.pending = append(ft.pending, t)
	return func() bool {
		ft.mu.Lock()
		defer ft.mu.Unlock()
		if t.done {
			return false
		}
		t.done = true
		return true
	}
}

// fire 触发所有到期的定时器。回调在锁外执行：它会回到 broker 里取
// broker 自己的锁，持锁调用是一条现成的锁序倒置。
func (ft *fakeTimers) fire() {
	ft.mu.Lock()
	now := ft.clk.now()
	var due []*fakeTimer
	keep := ft.pending[:0]
	for _, t := range ft.pending {
		switch {
		case t.done:
		case t.at <= now:
			t.done = true
			due = append(due, t)
		default:
			keep = append(keep, t)
		}
	}
	ft.pending = keep
	ft.mu.Unlock()
	for _, t := range due {
		t.f()
	}
}

// rig 是一套完整的 broker 测试装置。
//
// 它把前面各个横切面真实地接起来——这正是端到端测试的意义：
// 单元测试证明每个契约自身成立，集成测试证明它们协同成立。
type rig struct {
	t      *testing.T
	clk    *testClock
	store  *meta.LocalStore
	rt     *router.Router
	sess   *session.MemStore
	owners *cluster.LocalOwnership
	lim    *quota.Local
	bus    *events.Bus
	b      *Broker
	timers *fakeTimers

	nextPolicy atomic.Int32
	wg         sync.WaitGroup

	ln net.Listener
	// mu 保护 nextInfo：dial 与 Accept 循环之间的交接。
	mu       sync.Mutex
	nextInfo ConnInfo
}

// advance 推进测试时钟并触发到期的定时器。
//
// 直接用 r.clk.advance 的地方不需要定时器语义；凡是在等"过了多久之后
// 应该发生什么"的测试都该走这里，否则定时器永远不会响。
func (r *rig) advance(d time.Duration) {
	r.clk.advance(d)
	r.timers.fire()
}

// waitConns 等到在线连接数降到 n。
//
// 掐断套接字之后必须先等服务端把 EOF 处理完，再推进时钟：连接的清理是
// 异步的，而遗嘱的定时器正是在那一步登记的。先推时钟的话，表会以推完
// 之后的时刻为起点登记，后面怎么推都差着那一段——这个坑踩过一次。
func (r *rig) waitConns(n int64) {
	r.t.Helper()
	deadline := time.Now().Add(2 * time.Second) // contract:allow-time-now 测试超时
	for time.Now().Before(deadline) {
		if r.b.Conns() == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	r.t.Fatalf("等待连接数降到 %d 超时，当前 %d", n, r.b.Conns())
}

// insecureAuth 构造一个带指定策略的测试认证器。
func insecureAuth(t *testing.T, src string) InsecureNoAuth {
	t.Helper()
	cs, err := authz.CompileJSON("test", []byte(src), authz.DefaultEnv())
	if err != nil {
		t.Fatalf("编译策略失败: %v", err)
	}
	return InsecureNoAuth{Acknowledged: true, Policies: []*authz.CompiledSet{cs}}
}

// newPolicyRig 构造一套以 InsecureNoAuth 加给定策略为认证器的装置。
func newPolicyRig(t *testing.T, policy string, overrides map[quota.ID]int) *rig {
	t.Helper()
	return newRig(t, insecureAuth(t, policy), overrides)
}

func newRig(t *testing.T, auth Authenticator, overrides map[quota.ID]int) *rig {
	t.Helper()
	clk := newClock()
	limits := topic.DefaultLimits()

	r := &rig{
		t:   t,
		clk: clk,
		store: meta.NewLocalStore(meta.LocalOptions{
			Now: clk.now, ChangeBuffer: 64,
			// 策略现在随命令传文档，由 apply 编译（CLU-064）。
			// broker 读回来要断言成 *authz.CompiledSet，所以这里注入
			// 真的编译器而不是占位实现。
			Compile: func(name string, doc []byte) (any, error) {
				return authz.CompileJSON(name, doc, authz.DefaultEnv())
			},
		}),
		sess:   session.NewMemStore(),
		owners: cluster.NewLocalOwnership("node-1", 64),
		lim:    quota.NewLocal(quota.Options{Now: clk.now, Overrides: overrides}),
		bus:    events.New(events.Options{Node: "node-1", Now: clk.now}),
	}
	r.timers = &fakeTimers{clk: clk}
	r.rt = router.New(router.Options{Self: "node-1", Limits: limits, Now: clk.now})

	b, err := New(Options{
		Node:               "node-1",
		Meta:               r.store,
		Router:             r.rt,
		Sessions:           r.sess,
		Owners:             r.owners,
		Limiter:            r.lim,
		Bus:                r.bus,
		Auth:               auth,
		Limits:             limits,
		Now:                clk.now,
		AfterFunc:          r.timers.afterFunc,
		MaxViewStalenessMs: 0, // 单机测试不启用陈旧度判定，另有专项测试
		OutboundBuffer:     64,
	})
	if err != nil {
		t.Fatalf("创建 broker 失败: %v", err)
	}
	r.b = b
	r.listen()
	// 清理顺序很要紧：必须先关监听器让 Accept 循环退出，再关 broker
	// 断开连接，最后才等协程。顺序颠倒会死锁——wg.Wait 会等一个
	// 仍阻塞在 Accept 上的协程。
	t.Cleanup(func() {
		_ = r.ln.Close()
		_ = b.Close()
		r.wg.Wait()
		_ = r.store.Close()
		_ = r.owners.Close()
	})
	return r
}

// addPolicy 编译一份策略并写入元数据，返回其策略集编号。
func (r *rig) addPolicy(src string) meta.PolicySetID {
	r.t.Helper()
	id := meta.PolicySetID(r.nextPolicy.Add(1))
	if _, err := r.store.Apply(context.Background(), meta.Command{
		Kind:   meta.CmdSetPolicySet,
		Policy: &meta.PolicyCmd{ID: id, Name: "p", Doc: []byte(src)},
	}); err != nil {
		r.t.Fatalf("写入策略失败: %v", err)
	}
	return id
}

// addCert 注册一张证书并绑定设备与策略。
func (r *rig) addCert(fp meta.CertFingerprint, thing string, ps meta.PolicySetID, st meta.CertStatus) {
	r.t.Helper()
	if _, err := r.store.Apply(context.Background(), meta.Command{
		Kind: meta.CmdSetCert,
		Cert: &meta.CertCmd{Fingerprint: fp, Status: st, PolicySet: ps, Thing: thing},
	}); err != nil {
		r.t.Fatalf("注册证书失败: %v", err)
	}
	if thing != "" {
		if _, err := r.store.Apply(context.Background(), meta.Command{
			Kind:  meta.CmdSetThing,
			Thing: &meta.ThingCmd{Name: thing, PolicySet: ps},
		}); err != nil {
			r.t.Fatalf("注册设备失败: %v", err)
		}
	}
}

// revoke 吊销一张证书。
func (r *rig) revoke(fp meta.CertFingerprint, thing string, ps meta.PolicySetID, why string) {
	r.t.Helper()
	if _, err := r.store.Apply(context.Background(), meta.Command{
		Kind: meta.CmdSetCert,
		Cert: &meta.CertCmd{
			Fingerprint: fp, Status: meta.CertRevoked,
			PolicySet: ps, Thing: thing, Reason: why,
		},
	}); err != nil {
		r.t.Fatalf("吊销失败: %v", err)
	}
}

// listen 在回环地址上启动一个监听器。
//
// 用真实 TCP 而不是 net.Pipe：net.Pipe 完全同步、没有内部缓冲，
// 任何「服务端写了但客户端暂时不读」的场景都会直接阻塞写出池，
// 这既掩盖真实行为也让测试互相干扰。真实套接字有内核缓冲，
// 行为与生产一致。
func (r *rig) listen() {
	r.t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.t.Fatalf("监听失败: %v", err)
	}
	r.ln = ln
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			info := r.nextInfo
			r.mu.Unlock()
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				_ = r.b.Serve(context.Background(), nc, info)
			}()
		}
	}()
}

// dial 建立一条到 broker 的连接。
//
// 传输层事实经 nextInfo 交给 Accept 循环：真实部署里它来自 TLS 握手，
// 测试里由调用方直接指定，使 broker 可以在没有真实 TLS 的情况下被完整验证。
func (r *rig) dial(info ConnInfo) *testClient {
	r.t.Helper()
	r.mu.Lock()
	r.nextInfo = info
	r.mu.Unlock()

	nc, err := net.Dial("tcp", r.ln.Addr().String())
	if err != nil {
		r.t.Fatalf("连接失败: %v", err)
	}
	c := &testClient{t: r.t, nc: nc, ver: packet.V5}
	r.t.Cleanup(func() { _ = nc.Close() })
	return c
}

// dialCtx 像 dial，但用调用方给定的上下文服务该连接，
// 并返回一个在 Serve 返回时关闭的通道。
func (r *rig) dialCtx(ctx context.Context, info ConnInfo) (*testClient, <-chan struct{}) {
	r.t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.t.Fatalf("监听失败: %v", err)
	}
	done := make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(done)
		defer func() { _ = ln.Close() }()
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		_ = r.b.Serve(ctx, nc, info)
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		r.t.Fatalf("连接失败: %v", err)
	}
	c := &testClient{t: r.t, nc: nc, ver: packet.V5}
	r.t.Cleanup(func() { _ = nc.Close() })
	return c, done
}

// testClient 是一个最小的 MQTT 客户端，够用来驱动端到端测试。
type testClient struct {
	t    *testing.T
	nc   net.Conn
	ver  packet.Version
	buf  []byte
	rlen int
	ebuf []byte
	pid  uint16
}

func (c *testClient) nextPID() uint16 {
	c.pid++
	if c.pid == 0 {
		c.pid = 1
	}
	return c.pid
}

func (c *testClient) send(p packet.Packet) {
	c.t.Helper()
	buf, err := p.EncodeTo(c.ebuf[:0], c.ver)
	if err != nil {
		c.t.Fatalf("编码 %s 失败: %v", p.Type(), err)
	}
	c.ebuf = buf
	if err := c.nc.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.nc.Write(buf); err != nil {
		c.t.Fatalf("写出 %s 失败: %v", p.Type(), err)
	}
}

// trySend 发送一个报文，失败时返回错误而不终止测试。
//
// 模拟测试要用它：连接随时可能被服务端关掉（接管、吊销、踢出），
// 那不是测试失败而是被测行为之一，send 的 t.Fatalf 会把它变成误报。
func (c *testClient) trySend(p packet.Packet) error {
	buf, err := p.EncodeTo(c.ebuf[:0], c.ver)
	if err != nil {
		return err
	}
	c.ebuf = buf
	if err := c.nc.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = c.nc.Write(buf)
	return err
}

// recv 读取一个报文。超时或连接关闭时返回错误。
func (c *testClient) recv() (packet.Packet, error) {
	if c.buf == nil {
		c.buf = make([]byte, 8192)
	}
	for {
		if c.rlen > 0 {
			p, n, err := packet.Decode(c.buf[:c.rlen], c.ver)
			switch {
			case err == nil:
				// packet.Decode 是零拷贝的：返回的报文引用读缓冲。
				// 因此必须先把这一帧复制出来再压缩缓冲，否则
				// 紧随其后的 copy 会覆盖掉刚解出的主题与负载。
				//
				// 这个缺陷只在一次读取里收到多个报文时显形——写出池会把
				// 多条消息批量合进一次 TCP 写，于是缓冲里有残留数据。
				// 症状是主题被截断或带上后一帧的字节。
				frame := make([]byte, n)
				copy(frame, c.buf[:n])
				copy(c.buf, c.buf[n:c.rlen])
				c.rlen -= n
				p, _, err = packet.Decode(frame, c.ver)
				if err != nil {
					return nil, err
				}
				return p, nil
			case errors.Is(err, packet.ErrIncomplete):
			default:
				return nil, err
			}
		}
		if err := c.nc.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return nil, err
		}
		n, err := c.nc.Read(c.buf[c.rlen:])
		if n > 0 {
			c.rlen += n
			continue
		}
		if err != nil {
			return nil, err
		}
	}
}

// expect 读取一个报文并断言其类型。
func (c *testClient) expect(want packet.Type) packet.Packet {
	c.t.Helper()
	p, err := c.recv()
	if err != nil {
		c.t.Fatalf("期望收到 %s，读取失败: %v", want, err)
	}
	if p.Type() != want {
		c.t.Fatalf("收到 %s，期望 %s", p.Type(), want)
	}
	return p
}

// expectNothing 断言在给定时间内没有报文到达。
func (c *testClient) expectNothing(d time.Duration) {
	c.t.Helper()
	if err := c.nc.SetReadDeadline(time.Now().Add(d)); err != nil {
		c.t.Fatal(err)
	}
	var tmp [256]byte
	n, err := c.nc.Read(tmp[:])
	if n > 0 {
		c.t.Fatalf("不应收到任何报文，却收到 %d 字节", n)
	}
	if err == nil {
		c.t.Fatal("读取应超时")
	}
}

// expectClosed 断言连接被服务端关闭。
//
// MQTT 5 下服务端会先发一个带原因码的 DISCONNECT（PRD MQTT-078），
// 因此返回该原因码供断言。
func (c *testClient) expectClosed(d time.Duration) reason.Code {
	c.t.Helper()
	deadline := time.Now().Add(d)
	rc := reason.Code(0)
	for {
		if err := c.nc.SetReadDeadline(deadline); err != nil {
			c.t.Fatal(err)
		}
		p, err := c.recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
				errors.Is(err, net.ErrClosed) {
				return rc
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				c.t.Fatalf("连接未被关闭（已收到原因码 %s）", rc)
			}
			return rc
		}
		if d, ok := p.(*packet.Disconnect); ok {
			rc = d.Code
		}
	}
}

// connect 完成一次 CONNECT 握手并返回 CONNACK。
func (c *testClient) connect(clientID string, clean bool) *packet.Connack {
	c.t.Helper()
	cp := &packet.Connect{
		Version:    c.ver,
		ClientID:   []byte(clientID),
		CleanStart: clean,
		KeepAlive:  0,
	}
	if !clean && c.ver == packet.V5 {
		cp.Props.SessionExpiry = 3600
		cp.Props.Set(packet.PropSessionExpiry)
	}
	c.send(cp)
	return c.expect(packet.CONNACK).(*packet.Connack)
}

// subscribe 订阅一个过滤器并返回 SUBACK 的原因码。
func (c *testClient) subscribe(filter string, qos packet.QoS) reason.Code {
	c.t.Helper()
	c.send(&packet.Subscribe{
		PacketID: c.nextPID(),
		Filters:  []packet.Subscription{{Filter: []byte(filter), QoS: qos}},
	})
	ack := c.expect(packet.SUBACK).(*packet.Suback)
	if len(ack.Codes) != 1 {
		c.t.Fatalf("SUBACK 原因码数 = %d", len(ack.Codes))
	}
	return ack.Codes[0]
}

// publish 发布一条消息。QoS 大于 0 时等待并返回确认的原因码。
func (c *testClient) publish(name string, payload string, qos packet.QoS, retain bool) reason.Code {
	c.t.Helper()
	pp := &packet.Publish{
		Topic:   []byte(name),
		Payload: []byte(payload),
		QoS:     qos,
		Retain:  retain,
	}
	if qos > packet.QoS0 {
		pp.PacketID = c.nextPID()
	}
	c.send(pp)
	switch qos {
	case packet.QoS1:
		return c.expect(packet.PUBACK).(*packet.Ack).Code
	case packet.QoS2:
		rec := c.expect(packet.PUBREC).(*packet.Ack)
		c.send(&packet.Ack{Kind: packet.PUBREL, PacketID: rec.PacketID})
		c.expect(packet.PUBCOMP)
		return rec.Code
	}
	return reason.Success
}

// expectPublish 读取一条投递过来的消息。
func (c *testClient) expectPublish() *packet.Publish {
	c.t.Helper()
	pp := c.expect(packet.PUBLISH).(*packet.Publish)
	// 收到后按 QoS 回确认，避免服务端侧的在途表堆积。
	switch pp.QoS {
	case packet.QoS1:
		c.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pp.PacketID})
	case packet.QoS2:
		c.send(&packet.Ack{Kind: packet.PUBREC, PacketID: pp.PacketID})
		rel := c.expect(packet.PUBREL).(*packet.Ack)
		c.send(&packet.Ack{Kind: packet.PUBCOMP, PacketID: rel.PacketID})
	}
	return pp
}

// 常用的策略片段。
const (
	// policyFull 放行一切，用于验证非策略相关的行为。
	policyFull = `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
	// policyDeviceScoped 只放行设备自己名下的主题，是 IoT 的典型形态。
	policyDeviceScoped = `{"Statement":[
	  {"Effect":"Allow","Action":"iot:Connect",
	   "Resource":"arn:aws:iot:*:*:client/${iot:ClientId}"},
	  {"Effect":"Allow","Action":["iot:Publish","iot:Receive","iot:RetainPublish"],
	   "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"},
	  {"Effect":"Allow","Action":"iot:Subscribe",
	   "Resource":"arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"}
	]}`
)

func fp(n byte) meta.CertFingerprint {
	var f meta.CertFingerprint
	f[0], f[31] = n, n
	return f
}

// certInfo 构造一个带证书的连接信息。
func certInfo(f meta.CertFingerprint, cn string) ConnInfo {
	return ConnInfo{
		HasCert:     true,
		CertFP:      f,
		CertSerial:  "SN-" + cn,
		CertSubject: authz.DN{CommonName: cn, Organization: "Acme"},
		CertIssuer:  authz.DN{CommonName: "Acme CA"},
		TLSVersion:  0x0304,
		ALPN:        "x-amzn-mqtt-ca",
		HandshakeMS: 1.5,
	}
}

// collect 订阅事件总线并收集事件，返回取消函数。
func (r *rig) collect(f events.Filter) (func() []events.Event, func()) {
	ch, cancel := r.bus.Subscribe(f, 256)
	var mu sync.Mutex
	var got []events.Event
	done := make(chan struct{})
	go func() {
		for e := range ch {
			mu.Lock()
			got = append(got, e)
			mu.Unlock()
		}
		close(done)
	}()
	return func() []events.Event {
			mu.Lock()
			defer mu.Unlock()
			return append([]events.Event(nil), got...)
		}, func() {
			cancel()
			<-done
		}
}
