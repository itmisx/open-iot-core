package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/pki"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/shadow"
)

// devicePolicy 是测试用的设备域策略。
const devicePolicy = `{
  "Statement": [
    {"Effect":"Allow","Action":"iot:Connect",
     "Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"},
    {"Effect":"Allow","Action":["iot:Publish","iot:Receive"],
     "Resource":"arn:aws:iot:*:*:topic/things/${iot:Connection.Thing.ThingName}/*"},
    {"Effect":"Allow","Action":"iot:Subscribe",
     "Resource":"arn:aws:iot:*:*:topicfilter/things/${iot:Connection.Thing.ThingName}/*"}
  ]
}`

// testRig 是一套完整的、经真实 TCP 与 TLS 运行的节点。
type testRig struct {
	t      *testing.T
	cfg    config.Config
	dd     *node.DataDir
	mf     node.Manifest
	s      *Server
	done   chan error
	cancel context.CancelFunc
	logw   io.Writer
}

// logTo 把节点日志导向 w，供断言日志内容的测试使用。
// 须在 start 之前调用。
func (r *testRig) logTo(w io.Writer) { r.logw = w }

// newTestRig 走与 `openiotd init` 完全相同的代码路径建立一个节点。
//
// 刻意不绕过任何一步：绕过 init 的测试证明不了 init 之后的状态可用，
// 而"init 产出的目录 serve 能不能用"正是最值得测的衔接点。
func newTestRig(t *testing.T, mut func(*config.Config)) *testRig {
	t.Helper()
	root := filepath.Join(t.TempDir(), "data")

	cfg := config.Default()
	cfg.Node.DataDir = root
	cfg.Listeners.MQTTTLS.Addr = "127.0.0.1:0"
	cfg.Listeners.Admin.Addr = "127.0.0.1:0"
	cfg.Log.Level = "error"
	if mut != nil {
		mut(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("测试配置无效: %v", err)
	}

	dd, err := node.Open(cfg.Node.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	mf, err := node.Identify(dd, node.IdentityRequest{
		// 带上 node.id：集群模式下复制证书的 CommonName 必须等于它
		// （CLU-069），而 serve 也是这么传的。为空时行为不变。
		NodeID:     cfg.Node.ID,
		ShardCount: cfg.Cluster.ShardCount, AllowCreate: true,
		NowUnixMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PKI.Enabled {
		if err := InitPKI(dd, &cfg, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	reg := NewRegistry(dd.Root())
	if err := reg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(devicePolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	return &testRig{t: t, cfg: cfg, dd: dd, mf: mf}
}

// issue 签发一张设备证书并登记。
func (r *testRig) issue(thing string) tls.Certificate {
	r.t.Helper()
	ca, err := LoadCA(r.dd, &r.cfg, func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		r.t.Fatal(err)
	}
	out := filepath.Join(r.t.TempDir(), thing)
	res, err := IssueDevice(&r.cfg, ca, NewRegistry(r.dd.Root()), IssueDeviceRequest{
		Thing: thing, Policy: "device", OutDir: out,
	})
	if err != nil {
		r.t.Fatalf("签发 %s 失败: %v", thing, err)
	}
	cert, err := tls.LoadX509KeyPair(res.CertPath, res.KeyPath)
	if err != nil {
		r.t.Fatal(err)
	}
	return cert
}

// start 启动节点并等待就绪。
func (r *testRig) start() {
	r.t.Helper()
	opt := Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	}
	if r.logw != nil {
		opt.Logger = slog.New(slog.NewTextHandler(r.logw, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}
	s, err := New(opt)
	if err != nil {
		r.t.Fatalf("装配失败: %v", err)
	}
	r.s = s
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan error, 1)
	go func() { r.done <- s.Run(ctx) }()
	r.t.Cleanup(r.stop)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s.Ready() && len(s.MQTTAddrs()) > 0 {
			return
		}
		select {
		case err := <-r.done:
			r.t.Fatalf("节点启动即退出: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	r.t.Fatalf("节点未在 10 秒内就绪，未就绪组件: %v", s.ready.NotReady())
}

// stop 停止节点并等待 Run 返回。
//
// 必须经取消 context 而不是直接 Close：Run 阻塞在 <-ctx.Done() 上，
// 只调 Close 会让它永远不返回。这个区别在"重启"类测试里才显形。
func (r *testRig) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(15 * time.Second):
		r.t.Error("节点未在 15 秒内退出")
	}
	r.cancel = nil
}

// trustPool 返回信任服务端证书所需的证书池。
func (r *testRig) trustPool() *x509.CertPool {
	r.t.Helper()
	data, err := os.ReadFile(r.dd.Path(node.DirPKI, fileChain))
	if err != nil {
		r.t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		r.t.Fatal("CA 链中没有可用证书")
	}
	return pool
}

// dial 以设备证书建立真实 mTLS 连接。
func (r *testRig) dial(cert *tls.Certificate) (*mqttClient, error) {
	r.t.Helper()
	cfg := &tls.Config{
		RootCAs:    r.trustPool(),
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	nc, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second},
		"tcp", r.s.MQTTAddrs()[0], cfg)
	if err != nil {
		return nil, err
	}
	r.t.Cleanup(func() { _ = nc.Close() })
	return &mqttClient{t: r.t, nc: nc}, nil
}

// mqttClient 是测试用的最小 MQTT 5 客户端。
type mqttClient struct {
	t    *testing.T
	nc   net.Conn
	buf  []byte
	rlen int
	pid  uint16
}

func (c *mqttClient) send(p packet.Packet) {
	c.t.Helper()
	b, err := packet.Encode(p, packet.V5)
	if err != nil {
		c.t.Fatalf("编码 %s 失败: %v", p.Type(), err)
	}
	_ = c.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.nc.Write(b); err != nil {
		c.t.Fatalf("写出 %s 失败: %v", p.Type(), err)
	}
}

func (c *mqttClient) recv() (packet.Packet, error) {
	if c.buf == nil {
		c.buf = make([]byte, 8192)
	}
	for {
		if c.rlen > 0 {
			p, n, err := packet.Decode(c.buf[:c.rlen], packet.V5)
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
				p, _, err = packet.Decode(frame, packet.V5)
				if err != nil {
					return nil, err
				}
				return p, nil
			case errors.Is(err, packet.ErrIncomplete):
			default:
				return nil, err
			}
		}
		_ = c.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
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

func (c *mqttClient) expect(want packet.Type) packet.Packet {
	c.t.Helper()
	p, err := c.recv()
	if err != nil {
		c.t.Fatalf("期望 %s，读取失败: %v", want, err)
	}
	if p.Type() != want {
		c.t.Fatalf("收到 %s，期望 %s", p.Type(), want)
	}
	return p
}

func (c *mqttClient) connect(clientID string) *packet.Connack {
	c.t.Helper()
	c.send(&packet.Connect{Version: packet.V5, ClientID: []byte(clientID), CleanStart: true})
	return c.expect(packet.CONNACK).(*packet.Connack)
}

func (c *mqttClient) nextPID() uint16 {
	c.pid++
	if c.pid == 0 {
		c.pid = 1
	}
	return c.pid
}

func (c *mqttClient) subscribe(filter string) reason.Code {
	c.t.Helper()
	c.send(&packet.Subscribe{
		PacketID: c.nextPID(),
		Filters:  []packet.Subscription{{Filter: []byte(filter), QoS: packet.QoS1}},
	})
	ack := c.expect(packet.SUBACK).(*packet.Suback)
	return ack.Codes[0]
}

// expectClosed 等待连接被服务端关闭，返回 DISCONNECT 的原因码。
//
// 服务端可能先发 DISCONNECT 再关套接字，也可能直接关闭；
// 两者都算"被踢掉"，但带原因码的那种才能让设备知道为什么。
func (c *mqttClient) expectClosed(d time.Duration) reason.Code {
	c.t.Helper()
	deadline := time.Now().Add(d)
	code := reason.Success
	for time.Now().Before(deadline) {
		_ = c.nc.SetReadDeadline(deadline)
		p, err := c.recv()
		if err != nil {
			return code
		}
		if dc, ok := p.(*packet.Disconnect); ok {
			code = dc.Code
		}
	}
	c.t.Error("连接未在预期时间内被关闭")
	return code
}

func (c *mqttClient) publish(topic, payload string, qos packet.QoS) reason.Code {
	c.t.Helper()
	pp := &packet.Publish{Topic: []byte(topic), Payload: []byte(payload), QoS: qos}
	if qos > packet.QoS0 {
		pp.PacketID = c.nextPID()
	}
	c.send(pp)
	if qos == packet.QoS1 {
		return c.expect(packet.PUBACK).(*packet.Ack).Code
	}
	return reason.Success
}

// TestNodeLifecycle 是本轮的核心验收：init 产出的目录，serve 能跑通设备接入。
func TestNodeLifecycle(t *testing.T) {
	r := newTestRig(t, nil)
	cert := r.issue("dev-001")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("mTLS 握手失败: %v", err)
	}
	if ack := c.connect("dev-001"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("things/dev-001/+"); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	// 自己名下的主题可发布，且能收到自己的消息
	if rc := c.publish("things/dev-001/telemetry", "42", packet.QoS0); rc.IsError() {
		t.Errorf("发布失败: %s", rc)
	}
	got := c.expect(packet.PUBLISH).(*packet.Publish)
	if string(got.Payload) != "42" {
		t.Errorf("负载 = %q", got.Payload)
	}
	// 别人名下的主题被拒
	if rc := c.publish("things/dev-002/telemetry", "x", packet.QoS1); rc != reason.NotAuthorized {
		t.Errorf("越权发布应被拒，得到 %s", rc)
	}
}

// TestHealthAndReadyEndpoints 验证 PRD NFR-061。
func TestHealthAndReadyEndpoints(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-h")
	r.start()

	base := "http://" + r.s.AdminAddr()
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz 状态码 = %d", resp.StatusCode)
	}
	var health healthzResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health.NodeID != r.mf.NodeID {
		t.Errorf("node_id = %q，期望 %q", health.NodeID, r.mf.NodeID)
	}

	resp2, err := http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("/readyz 状态码 = %d", resp2.StatusCode)
	}
	var ready readyzResponse
	if err := json.NewDecoder(resp2.Body).Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if !ready.Ready {
		t.Errorf("应已就绪: %+v", ready.Components)
	}
	// 未就绪时要能看出卡在哪个组件，只回 503 的探针没有运维价值。
	// 断言的是具体组件集合而非数量：数量断言会在每次新增组件时
	// 以一个毫无信息量的失败打断构建。
	want := map[string]bool{
		compMeta: true, compStore: true, compRegistry: true, compView: true,
		compPKI: true, compShadow: true, compListeners: true,
		// shards 在单机下恒为就绪（没有分区组），但仍要出现在报告里：
		// 集群下它是"会话写得进去吗"的唯一出口（CLU-118），而一个只在
		// 集群下才出现的组件会让两种形态的探针输出不是同一个形状。
		compShards: true,
	}
	for _, c := range ready.Components {
		if !want[c.Name] {
			t.Errorf("出现未预期的组件 %q", c.Name)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("就绪报告缺少组件 %q", name)
	}
	if resp2.Header.Get("Cache-Control") != "no-store" {
		t.Error("就绪状态不应被缓存")
	}
}

// TestShutdownIsPrompt 锁定关闭顺序，防止回归。
//
// 这条测试来自一个真实缺陷：admin 的 Serve 协程登记在同一个 WaitGroup
// 上，而 admin.stop() 原先排在 wg.Wait() 之后，于是每次关闭都恰好耗满
// 30 秒超时。在进程随即退出的场景下这个缺陷完全不可见——
// 只有把装配逻辑放进可测包、并真的断言关闭耗时，它才会暴露。
func TestShutdownIsPrompt(t *testing.T) {
	r := newTestRig(t, nil)
	// 证书须在启动前登记：文件化注册表只在启动时载入
	// （这是 v0.1 内核阶段的已知限制，Admin API 落地后即时生效）
	cert := r.issue("dev-s")
	r.start()

	// 带一个活连接关闭，比空闲关闭更接近真实情况
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-s"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	start := time.Now()
	if err := r.s.Close(); err != nil {
		t.Errorf("关闭报错: %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("关闭耗时 %v，远超预期；几乎可以确定是关闭顺序错误"+
			"（某个协程在被叫停之前就被等待）", d)
	}
	if r.s.Ready() {
		t.Error("关闭后不应报告就绪")
	}
	// 幂等
	if err := r.s.Close(); err != nil {
		t.Errorf("重复关闭应幂等: %v", err)
	}
}

// TestStartupFailureReleasesResources 验证启动失败不留下监听与协程。
func TestStartupFailureReleasesResources(t *testing.T) {
	// 先占住一个端口，使 MQTT 监听必然失败
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()

	r := newTestRig(t, func(c *config.Config) {
		c.Listeners.MQTTTLS.Addr = busy.Addr().String()
	})
	r.issue("dev-f")

	s, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	})
	if err != nil {
		t.Fatal(err)
	}
	runErr := s.Run(context.Background())
	if runErr == nil {
		t.Fatal("端口被占用时启动应失败")
	}
	if !strings.Contains(runErr.Error(), "mqtt_tls") {
		t.Errorf("错误应指出是哪个端点: %v", runErr)
	}
	// 管理端点必须已被回收，否则同一地址无法再次监听
	if addr := s.AdminAddr(); addr != "" {
		if ln, err := net.Listen("tcp", addr); err != nil {
			t.Errorf("启动失败后管理端点未释放（%s）: %v", addr, err)
		} else {
			_ = ln.Close()
		}
	}
}

// TestClientWithoutCertRejected 验证未出示证书的客户端连不上。
func TestClientWithoutCertRejected(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-n")
	r.start()

	c, err := r.dial(nil)
	if err != nil {
		return // TLS 1.2 下握手阶段即失败
	}
	// TLS 1.3 下客户端证书校验是延后的，须真正读一次
	c.send(&packet.Connect{Version: packet.V5, ClientID: []byte("x"), CleanStart: true})
	if _, err := c.recv(); err == nil {
		t.Error("未出示客户端证书应被拒绝")
	}
}

// TestUnregisteredCertRejected 验证未登记的证书连不上。
//
// 证书由本 CA 签发因此能通过 TLS，但没有登记到注册表，
// 所以 MQTT 层必须拒绝——信任链有效不等于这台设备被授权。
func TestUnregisteredCertRejected(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-known")
	r.start()

	ca, err := LoadCA(r.dd, &r.cfg,
		func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-unknown"})
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		append(append([]byte(nil), issued.CertPEM...), issued.ChainPEM...), issued.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("TLS 握手应成功（证书由本 CA 签发）: %v", err)
	}
	ack := c.connect("dev-unknown")
	if ack.Code == reason.Success {
		t.Error("未登记的证书不应能建立会话")
	}
}

// TestSeedRejectsBadRegistry 验证注册表内容有问题时启动失败。
func TestSeedRejectsBadRegistry(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantMsg string
	}{
		{"绑定了不存在的策略",
			`{"format_version":1,"certs":[{"fingerprint":"` + strings.Repeat("a", 64) +
				`","thing":"t","policy":"missing","status":"ACTIVE"}]}`, "不存在"},
		{"状态非法",
			`{"format_version":1,"certs":[{"fingerprint":"` + strings.Repeat("a", 64) +
				`","thing":"t","policy":"device","status":"ENABLED"}]}`, "状态"},
		{"指纹非法",
			`{"format_version":1,"certs":[{"fingerprint":"xyz","thing":"t","policy":"device","status":"ACTIVE"}]}`,
			"指纹"},
		{"指纹大写",
			`{"format_version":1,"certs":[{"fingerprint":"` + strings.Repeat("A", 64) +
				`","thing":"t","policy":"device","status":"ACTIVE"}]}`, "小写"},
		{"未知字段",
			`{"format_version":1,"certs":[{"fingerprint":"` + strings.Repeat("a", 64) +
				`","thing":"t","policy":"device","status":"ACTIVE","extra":1}]}`, ""},
		{"格式版本过高",
			`{"format_version":99,"certs":[]}`, "format_version"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTestRig(t, nil)
			reg := NewRegistry(r.dd.Root())
			if err := os.WriteFile(reg.CertsPath(), []byte(c.content), 0o600); err != nil {
				t.Fatal(err)
			}
			s, err := New(Options{
				Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			err = s.Prepare(context.Background())
			if err == nil {
				t.Fatal("有问题的注册表应使启动失败")
			}
			if c.wantMsg != "" && !strings.Contains(err.Error(), c.wantMsg) {
				t.Errorf("错误 %q 未含 %q", err, c.wantMsg)
			}
		})
	}
}

// TestBadPolicyFailsStartup 验证语法错误的策略使启动失败。
//
// 带着一份编译不过的策略继续运行，等于把该策略下的所有设备静默降权或提权。
func TestBadPolicyFailsStartup(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "broken.json"),
		[]byte(`{"Statement":[{"Effect":"Maybe","Action":"iot:*","Resource":"*"}]}`),
		0o600); err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Prepare(context.Background()); err == nil {
		t.Error("语法错误的策略应使启动失败")
	}
}

// TestPolicySetIDsAreStableAcrossRestart 是一条安全关键的不变量。
//
// 策略集编号由策略文件名排序决定。若编号跨重启不稳定，重启后证书
// 绑定的编号会指向另一份策略——设备会拿到别的设备的权限，
// 而日志里什么异常都看不到。
func TestPolicySetIDsAreStableAcrossRestart(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	for _, n := range []string{"zeta", "alpha", "mid"} {
		if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), n+".json"),
			[]byte(devicePolicy), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	first, err := reg.LoadPolicies(authzEnv(&r.cfg))
	if err != nil {
		t.Fatal(err)
	}
	var order1 []string
	for _, p := range first {
		order1 = append(order1, p.Name)
	}
	// 重新加载若干次，顺序必须完全一致
	for i := 0; i < 5; i++ {
		again, err := reg.LoadPolicies(authzEnv(&r.cfg))
		if err != nil {
			t.Fatal(err)
		}
		var order2 []string
		for _, p := range again {
			order2 = append(order2, p.Name)
		}
		if strings.Join(order1, ",") != strings.Join(order2, ",") {
			t.Fatalf("策略顺序不稳定：%v 对 %v", order1, order2)
		}
	}
	if want := "alpha,device,mid,zeta"; strings.Join(order1, ",") != want {
		t.Errorf("策略顺序 = %v，期望按名字排序 %q", order1, want)
	}
}

func TestRegistryAddCertIsIdempotent(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	fp := strings.Repeat("ab", 32)
	rec := certRecord{Fingerprint: fp, Thing: "t1", Policy: "device", Status: "ACTIVE"}
	if err := reg.AddCert(rec); err != nil {
		t.Fatal(err)
	}
	// 同指纹再写入应覆盖而非追加：重复指纹会让"哪条生效"取决于遍历顺序
	rec.Thing = "t2"
	if err := reg.AddCert(rec); err != nil {
		t.Fatal(err)
	}
	certs, err := reg.LoadCerts()
	if err != nil {
		t.Fatal(err)
	}
	if len(certs) != 1 {
		t.Fatalf("证书条数 = %d，期望 1", len(certs))
	}
	if certs[0].Thing != "t2" {
		t.Errorf("未覆盖：thing = %q", certs[0].Thing)
	}

	if err := reg.SetCertStatus(fp, "REVOKED", "密钥泄露"); err != nil {
		t.Fatal(err)
	}
	certs, _ = reg.LoadCerts()
	if certs[0].Status != "REVOKED" || certs[0].Reason != "密钥泄露" {
		t.Errorf("吊销未生效: %+v", certs[0])
	}
	if err := reg.SetCertStatus(strings.Repeat("cd", 32), "REVOKED", ""); err == nil {
		t.Error("吊销未登记的指纹应报错")
	}
	if err := reg.SetCertStatus(fp, "BOGUS", ""); err == nil {
		t.Error("非法状态应报错")
	}
}

func TestRevokedCertCannotConnect(t *testing.T) {
	r := newTestRig(t, nil)
	cert := r.issue("dev-rev")
	reg := NewRegistry(r.dd.Root())
	certs, err := reg.LoadCerts()
	if err != nil || len(certs) == 0 {
		t.Fatalf("应有登记: %v", err)
	}
	if err := reg.SetCertStatus(certs[0].Fingerprint, "REVOKED", "测试"); err != nil {
		t.Fatal(err)
	}
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("TLS 握手应成功（吊销不走 CRL）: %v", err)
	}
	if ack := c.connect("dev-rev"); ack.Code == reason.Success {
		t.Error("已吊销的证书不应能建立会话")
	}
}

func TestInitPKIRefusesOverwrite(t *testing.T) {
	r := newTestRig(t, nil)
	// newTestRig 已经初始化过 PKI，再来一次必须拒绝：
	// 覆盖 CA 会让所有已签发的设备证书立刻失效
	err := InitPKI(r.dd, &r.cfg, time.Now().UnixMilli())
	if err == nil {
		t.Fatal("已存在 PKI 时应拒绝重建")
	}
	if !strings.Contains(err.Error(), "已存在") {
		t.Errorf("错误应说明原因: %v", err)
	}
}

func TestIssueDeviceValidation(t *testing.T) {
	r := newTestRig(t, nil)
	ca, err := LoadCA(r.dd, &r.cfg,
		func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(r.dd.Root())
	cases := []struct {
		name string
		req  IssueDeviceRequest
	}{
		{"缺少设备名", IssueDeviceRequest{Policy: "device"}},
		{"缺少策略", IssueDeviceRequest{Thing: "t"}},
		{"策略不存在", IssueDeviceRequest{Thing: "t", Policy: "nope"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := IssueDevice(&r.cfg, ca, reg, c.req); err == nil {
				t.Error("应报错")
			}
		})
	}
	// 未启用 PKI
	if _, err := IssueDevice(&r.cfg, nil, reg,
		IssueDeviceRequest{Thing: "t", Policy: "device"}); err == nil {
		t.Error("未启用 PKI 时应报错")
	}
}

func TestNewValidatesOptions(t *testing.T) {
	r := newTestRig(t, nil)
	if _, err := New(Options{Config: r.cfg, Manifest: r.mf}); err == nil {
		t.Error("缺少数据目录应报错")
	}
	bad := r.cfg
	bad.Log.Level = "trace"
	if _, err := New(Options{Config: bad, DataDir: r.dd, Manifest: r.mf}); err == nil {
		t.Error("非法配置应报错")
	}
	// CA 读不出来时应报错，而不是启动一个没有 CA 的节点。
	//
	// 此前这一格用的是"错误的口令"，口令随 AUTH-092 去掉之后，
	// 唯一还能让 LoadCA 失败的输入就是文件本身不可读。
	keyPath := filepath.Join(r.dd.Path(node.DirPKI), "issuing-ca.key")
	saved, rerr := os.ReadFile(keyPath)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := os.Remove(keyPath); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	}); err == nil {
		t.Error("签发 CA 私钥缺失时应报错")
	}
	if err := os.WriteFile(keyPath, saved, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestPlainListenerWorks 验证明文端点（前置代理终结 TLS 的部署）。
func TestPlainListenerWorks(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.Listeners.MQTTTLS.Enabled = false
		c.Listeners.MQTTPlain = config.PlainListener{Enabled: true, Addr: "127.0.0.1:0"}
		c.Auth.Mode = config.AuthInsecureNone
		c.Auth.AcknowledgeInsecure = true
	})
	r.start()

	nc, err := net.DialTimeout("tcp", r.s.MQTTAddrs()[0], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	c := &mqttClient{t: t, nc: nc}
	// 不认证模式下 broker 仍需一份策略；InsecureNoAuth 不带策略时拒绝一切，
	// 这里只验证端点可达与协议握通
	ack := c.connect("plain-dev")
	if ack.Code == 0 && !r.s.Ready() {
		t.Error("节点应就绪")
	}
}

func TestManifestSurfacedByServer(t *testing.T) {
	r := newTestRig(t, nil)
	r.start()
	if got := r.s.Manifest(); got.NodeID != r.mf.NodeID {
		t.Errorf("Manifest 不一致: %+v", got)
	}
	if r.s.CA() == nil {
		t.Error("启用 PKI 时 CA 不应为 nil")
	}
	if r.s.Registry() == nil {
		t.Error("Registry 不应为 nil")
	}
	if !strings.Contains(fmt.Sprint(r.s.MQTTAddrs()), "127.0.0.1") {
		t.Errorf("MQTTAddrs = %v", r.s.MQTTAddrs())
	}
}

func TestLoadClientCAs(t *testing.T) {
	r := newTestRig(t, nil)
	l := r.cfg.Listeners.MQTTTLS

	// 用内嵌 CA 的信任池
	ca, err := LoadCA(r.dd, &r.cfg,
		func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		t.Fatal(err)
	}
	if pool, err := LoadClientCAs(r.dd, &r.cfg, &l, ca); err != nil || pool == nil {
		t.Errorf("应返回内嵌 CA 的信任池: %v", err)
	}

	// 既无文件也无 CA
	if _, err := LoadClientCAs(r.dd, &r.cfg, &l, nil); err == nil {
		t.Error("无信任来源应报错")
	}

	// 显式信任文件
	l.ClientCAFile = r.dd.Path(node.DirPKI, fileChain)
	if pool, err := LoadClientCAs(r.dd, &r.cfg, &l, nil); err != nil || pool == nil {
		t.Errorf("应从文件载入: %v", err)
	}

	// 文件不存在
	l.ClientCAFile = filepath.Join(t.TempDir(), "nope.pem")
	if _, err := LoadClientCAs(r.dd, &r.cfg, &l, nil); err == nil {
		t.Error("文件不存在应报错")
	}

	// 文件里没有证书
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	l.ClientCAFile = empty
	if _, err := LoadClientCAs(r.dd, &r.cfg, &l, nil); err == nil {
		t.Error("文件中无证书应报错")
	}
}

func TestLoadServerCertPaths(t *testing.T) {
	r := newTestRig(t, nil)
	l := r.cfg.Listeners.MQTTTLS

	// 默认取 data dir 下 init 生成的那一对
	if _, err := LoadServerCert(r.dd, &r.cfg, &l); err != nil {
		t.Errorf("应能载入 init 生成的服务端证书: %v", err)
	}
	// 显式路径不存在时，错误须提示先 init
	l.CertFile = filepath.Join(t.TempDir(), "a.pem")
	l.KeyFile = filepath.Join(t.TempDir(), "a.key")
	if _, err := LoadServerCert(r.dd, &r.cfg, &l); err == nil {
		t.Error("证书文件不存在应报错")
	} else if !strings.Contains(err.Error(), "openiotd init") {
		t.Errorf("错误应提示先 init: %v", err)
	}
}

// TestResolvePKIPathsHonorsConfig 验证显式 CA 路径被采用。
func TestResolvePKIPathsHonorsConfig(t *testing.T) {
	r := newTestRig(t, nil)
	cfg := r.cfg
	def := resolvePKIPaths(r.dd, &cfg)
	if !strings.Contains(def.issuingKey, node.DirPKI) {
		t.Errorf("默认签发私钥路径 = %q，应在 pki 目录下", def.issuingKey)
	}

	cfg.PKI.RootCertFile = "/custom/root.pem"
	cfg.PKI.IssuingCertFile = "/custom/issuing.pem"
	cfg.PKI.IssuingKeyFile = "/custom/issuing.key"
	got := resolvePKIPaths(r.dd, &cfg)
	if got.root != "/custom/root.pem" || got.issuingKey != "/custom/issuing.key" {
		t.Errorf("未采用配置中的路径: %+v", got)
	}
	// 服务端证书路径不受 CA 路径配置影响
	if !strings.Contains(got.serverCert, node.DirPKI) {
		t.Errorf("服务端证书路径 = %q，不应被 CA 路径配置改变", got.serverCert)
	}
}

func TestLoadCAErrors(t *testing.T) {
	r := newTestRig(t, nil)
	// 私钥文件被换成垃圾：解析失败要报错，不能把垃圾当私钥往下传
	// （此前这里还有"口令错误"与"缺少口令"两格，随 AUTH-092 去掉）
	keyPath := filepath.Join(r.dd.Path(node.DirPKI), "issuing-ca.key")
	if err := os.WriteFile(keyPath, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(r.dd, &r.cfg,
		func() int64 { return time.Now().UnixMilli() }); err == nil {
		t.Error("私钥不是 PEM 时应报错")
	}
	// 证书文件缺失
	cfg := r.cfg
	cfg.PKI.RootCertFile = filepath.Join(t.TempDir(), "nope.pem")
	cfg.PKI.IssuingCertFile = cfg.PKI.RootCertFile
	cfg.PKI.IssuingKeyFile = cfg.PKI.RootCertFile
	if _, err := LoadCA(r.dd, &cfg,
		func() int64 { return time.Now().UnixMilli() }); err == nil {
		t.Error("证书文件缺失应报错")
	}
	// 证书文件内容非法
	bad := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.PKI.RootCertFile = bad
	cfg.PKI.IssuingCertFile = bad
	cfg.PKI.IssuingKeyFile = bad
	if _, err := LoadCA(r.dd, &cfg,
		func() int64 { return time.Now().UnixMilli() }); err == nil {
		t.Error("证书内容非法应报错")
	}
}

func TestHostSplitting(t *testing.T) {
	hosts := []string{"localhost", "127.0.0.1", "broker.example.com", "::1", "10.0.0.5"}
	dns := dnsHosts(hosts)
	ips := ipHosts(hosts)
	if len(dns) != 2 || dns[0] != "localhost" || dns[1] != "broker.example.com" {
		t.Errorf("DNS 名 = %v", dns)
	}
	if len(ips) != 3 {
		t.Errorf("IP = %v", ips)
	}
	if isIP("") {
		t.Error("空串不应被当作 IP")
	}
}

func TestIssueDeviceValidityDefaults(t *testing.T) {
	cfg := config.Default()
	if got := issueDeviceValidity(&cfg, 0); got != cfg.PKI.DefaultValidity.D() {
		t.Errorf("= %v，期望取默认有效期", got)
	}
	if got := issueDeviceValidity(&cfg, time.Hour); got != time.Hour {
		t.Errorf("= %v，期望取请求值", got)
	}
}

func TestStatusNameRoundTrip(t *testing.T) {
	for name, st := range statusNames {
		if got := statusName(st); got != name {
			t.Errorf("statusName(%v) = %q，期望 %q", st, got, name)
		}
	}
	// 未知状态落到一个保守的值而不是 ACTIVE
	if got := statusName(99); got == "ACTIVE" {
		t.Error("未知状态不得映射为 ACTIVE")
	}
}

func TestAdminAddrBeforeStart(t *testing.T) {
	r := newTestRig(t, nil)
	s, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if addr := s.AdminAddr(); addr != "" {
		t.Errorf("启动前 AdminAddr 应为空，得到 %q", addr)
	}
	if s.Ready() {
		t.Error("启动前不应就绪")
	}
}

func TestReadyzReports503WhenNotReady(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-nr")
	r.start()
	// 人为把一个组件置为未就绪，探针须回 503 且指出是哪个组件
	r.s.ready.Set(compView, false, "落后 leader 500 条")
	resp, err := http.Get("http://" + r.s.AdminAddr() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("状态码 = %d，期望 503", resp.StatusCode)
	}
	var out readyzResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range out.Components {
		if c.Name == compView && !c.Ready && c.Detail == "落后 leader 500 条" {
			found = true
		}
	}
	if !found {
		t.Errorf("未报告落后的组件明细: %+v", out.Components)
	}
	r.s.ready.Set(compView, true, "")
}

func TestRegistryEmptyDirs(t *testing.T) {
	// 空数据目录：没有策略也没有证书，不应报错
	root := filepath.Join(t.TempDir(), "empty")
	reg := NewRegistry(root)
	cfg := config.Default()
	if ps, err := reg.LoadPolicies(authzEnv(&cfg)); err != nil || ps != nil {
		t.Errorf("无策略目录时应返回空: %v %v", ps, err)
	}
	if cs, err := reg.LoadCerts(); err != nil || cs != nil {
		t.Errorf("无证书文件时应返回空: %v %v", cs, err)
	}
}

func TestRegistryAddCertValidation(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	bad := []certRecord{
		{Fingerprint: "short", Policy: "device", Status: "ACTIVE"},
		{Fingerprint: strings.Repeat("z", 64), Policy: "device", Status: "ACTIVE"},
		{Fingerprint: strings.Repeat("a", 64), Policy: "", Status: "ACTIVE"},
		{Fingerprint: strings.Repeat("a", 64), Policy: "device", Status: "WAT"},
	}
	for i, rec := range bad {
		if err := reg.AddCert(rec); err == nil {
			t.Errorf("第 %d 条非法登记应被拒: %+v", i, rec)
		}
	}
}

// shadowPolicy 放行设备自己名下的影子主题。
const shadowPolicy = `{
  "Statement": [
    {"Effect":"Allow","Action":"iot:Connect",
     "Resource":"arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"},
    {"Effect":"Allow","Action":["iot:Publish","iot:Receive"],
     "Resource":"arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"},
    {"Effect":"Allow","Action":"iot:Subscribe",
     "Resource":"arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/shadow/*"}
  ]
}`

// TestShadowOverMQTT 是 Shadow 的端到端验收。
//
// 设备经真实 mTLS 连接，订阅响应主题，发起一次 update，
// 随后应收到 /update/accepted、/update/documents，
// 以及（因 desired 与 reported 不一致）/update/delta。
func TestShadowOverMQTT(t *testing.T) {
	r := newTestRig(t, nil)
	// 用影子策略覆盖默认的 device.json
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-sh")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-sh"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	// 订阅全部影子响应主题
	if rc := c.subscribe("$aws/things/dev-sh/shadow/#"); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}

	// 发起更新：desired 与 reported 不一致，应产生 delta
	body := `{"state":{"reported":{"temp":20},"desired":{"temp":25}},"clientToken":"t1"}`
	if rc := c.publish("$aws/things/dev-sh/shadow/update", body, packet.QoS0); rc.IsError() {
		t.Fatalf("发布被拒: %s", rc)
	}

	// 收齐消息。请求本身也会被投递（请求主题可订阅，与 AWS 一致），
	// 因此按主题归类而不是按顺序断言。
	got := collectByTopic(t, c, 4)
	for _, want := range []string{
		"$aws/things/dev-sh/shadow/update",
		"$aws/things/dev-sh/shadow/update/accepted",
		"$aws/things/dev-sh/shadow/update/documents",
		"$aws/things/dev-sh/shadow/update/delta",
	} {
		if _, ok := got[want]; !ok {
			t.Errorf("未收到 %s（收到的主题: %v）", want, topicsOf(got))
		}
	}

	acc := mustJSON(t, got["$aws/things/dev-sh/shadow/update/accepted"])
	if acc["version"] != float64(1) {
		t.Errorf("accepted 的版本 = %v，期望 1", acc["version"])
	}
	if acc["clientToken"] != "t1" {
		t.Errorf("clientToken = %v，期望原样回传", acc["clientToken"])
	}
	d := mustJSON(t, got["$aws/things/dev-sh/shadow/update/delta"])
	state, _ := d["state"].(map[string]any)
	if state["temp"] != float64(25) {
		t.Errorf("delta 的 temp = %v，期望 25", state["temp"])
	}

	// get 应返回完整文档，含 delta
	if rc := c.publish("$aws/things/dev-sh/shadow/get", `{"clientToken":"g1"}`,
		packet.QoS0); rc.IsError() {
		t.Fatalf("get 被拒: %s", rc)
	}
	got = collectByTopic(t, c, 2)
	ga := mustJSON(t, got["$aws/things/dev-sh/shadow/get/accepted"])
	gs, _ := ga["state"].(map[string]any)
	if gs["delta"] == nil {
		t.Errorf("get 响应应含 delta: %v", ga)
	}
}

// TestShadowRejectionReachesDevice 验证被拒的请求也有响应。
func TestShadowRejectionReachesDevice(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-rj")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-rj"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-rj/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	// 读取一个不存在的影子
	if rc := c.publish("$aws/things/dev-rj/shadow/get", `{"clientToken":"x"}`,
		packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	got := collectByTopic(t, c, 2)
	raw, ok := got["$aws/things/dev-rj/shadow/get/rejected"]
	if !ok {
		t.Fatalf("未收到 rejected（收到: %v）", topicsOf(got))
	}
	rej := mustJSON(t, raw)
	if rej["code"] != float64(404) {
		t.Errorf("code = %v，期望 404", rej["code"])
	}
	if rej["clientToken"] != "x" {
		t.Errorf("clientToken = %v，期望原样回传", rej["clientToken"])
	}
}

// TestShadowNotDeliveredWithoutReceivePermission 验证响应的投递仍受
// iot:Receive 约束。
//
// 平台发布响应时不经发布方鉴权（发布者是平台自己），但每个订阅者的
// iot:Receive 必须被复查——否则一个只被授予 Publish 的设备会收到
// 它无权看到的影子内容。
func TestShadowNotDeliveredWithoutReceivePermission(t *testing.T) {
	// 只给 Connect、Publish 与 Subscribe，不给 Receive
	const noReceive = `{
	  "Statement": [
	    {"Effect":"Allow","Action":"iot:Connect","Resource":"*"},
	    {"Effect":"Allow","Action":"iot:Publish","Resource":"*"},
	    {"Effect":"Allow","Action":"iot:Subscribe","Resource":"*"}
	  ]
	}`
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(noReceive), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-nr2")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-nr2"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-nr2/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	if rc := c.publish("$aws/things/dev-nr2/shadow/update",
		`{"state":{"reported":{"a":1}}}`, packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	// 影子确实被更新了（Publish 被放行）
	deadline := time.Now().Add(2 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if _, ok := r.s.shadows.Get("dev-nr2", ""); ok {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !updated {
		t.Fatal("影子应已被更新")
	}
	// 但没有任何消息被投递给它
	_ = c.nc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if p, err := c.recv(); err == nil {
		t.Errorf("无 iot:Receive 权限不应收到任何消息，收到 %s", p.Type())
	}
}

// TestShadowDisabledWithoutService 验证没有保留主题服务时的行为。
//
// 此时影子主题只是普通主题：消息被投递给订阅者，但没有响应。
// 这正是不启用 Shadow 的部署应有的行为。
func TestShadowServiceSharedByRESTPath(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-rest")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-rest"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-rest/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}

	// 经服务层直接更新，模拟 REST 数据面（PRD SHDW-045）。
	// 它与 MQTT 路径共用同一个 shadow.Service，因此 delta 与 documents
	// 由同一份代码产出——这是 SHDW-044 的要点。
	sr := shadow.Request{Thing: "dev-rest", Op: shadow.OpUpdate}
	out := r.s.Shadow().Update(context.Background(), sr,
		[]byte(`{"state":{"reported":{"a":1},"desired":{"a":2}}}`))
	if out.Err != nil {
		t.Fatalf("经服务层更新失败: %s", out.Err)
	}
	// REST 路径须能拿到同一批待发布消息
	if len(out.Publishes) != 2 {
		t.Errorf("应产出 delta 与 documents 两条消息，实际 %d", len(out.Publishes))
	}
	if _, ok := r.s.shadows.Get("dev-rest", ""); !ok {
		t.Error("影子应已更新")
	}
}

// ---------- 测试辅助 ----------

// collectByTopic 读取 n 条 PUBLISH 并按主题归类。
func collectByTopic(t *testing.T, c *mqttClient, n int) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte, n)
	deadline := time.Now().Add(5 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		_ = c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		p, err := c.recv()
		if err != nil {
			break
		}
		pub, ok := p.(*packet.Publish)
		if !ok {
			continue
		}
		out[string(pub.Topic)] = append([]byte(nil), pub.Payload...)
		if pub.QoS == packet.QoS1 {
			c.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pub.PacketID})
		}
	}
	return out
}

func topicsOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mustJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("无法解析载荷: %v\n%s", err, b)
	}
	return m
}

// TestShadowSurvivesRestart 是持久化的端到端验收。
//
// 设备经真实 mTLS 写入影子，节点重启，再读回来必须一致且版本继续递增。
// 版本若回退，设备的乐观并发控制会永久失效——它拿着一个"未来"的版本号
// 去更新，永远得到 409。
func TestShadowSurvivesRestart(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(shadowPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-dur")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-dur"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("$aws/things/dev-dur/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	body := `{"state":{"reported":{"temp":20,"n":{"deep":1.50}}},"clientToken":"t1"}`
	if rc := c.publish("$aws/things/dev-dur/shadow/update", body, packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	got := collectByTopic(t, c, 3)
	acc := mustJSON(t, got["$aws/things/dev-dur/shadow/update/accepted"])
	if acc["version"] != float64(1) {
		t.Fatalf("首次版本 = %v", acc["version"])
	}

	// 停掉节点，再用同一个数据目录起一个新的
	r.stop()

	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()

	c2, err := r2.dial(&cert)
	if err != nil {
		t.Fatalf("重启后握手失败: %v", err)
	}
	if ack := c2.connect("dev-dur"); ack.Code != reason.Success {
		t.Fatalf("重启后连接被拒: %s", ack.Code)
	}
	if rc := c2.subscribe("$aws/things/dev-dur/shadow/#"); rc.IsError() {
		t.Fatal(rc)
	}
	if rc := c2.publish("$aws/things/dev-dur/shadow/get", `{"clientToken":"g"}`,
		packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	got = collectByTopic(t, c2, 2)
	raw, ok := got["$aws/things/dev-dur/shadow/get/accepted"]
	if !ok {
		t.Fatalf("重启后未读到影子（收到: %v）", topicsOf(got))
	}
	doc := mustJSON(t, raw)
	if doc["version"] != float64(1) {
		t.Errorf("重启后版本 = %v，期望 1", doc["version"])
	}
	// 数字字面量不得被存储层改写
	if !strings.Contains(string(raw), "1.50") {
		t.Errorf("数字字面量被改写了: %s", raw)
	}

	// 下一次更新须从 2 继续，不得回退到 1
	if rc := c2.publish("$aws/things/dev-dur/shadow/update",
		`{"state":{"reported":{"temp":21}}}`, packet.QoS0); rc.IsError() {
		t.Fatal(rc)
	}
	got = collectByTopic(t, c2, 3)
	acc2 := mustJSON(t, got["$aws/things/dev-dur/shadow/update/accepted"])
	if acc2["version"] != float64(2) {
		t.Errorf("重启后的下一个版本 = %v，期望 2", acc2["version"])
	}
}

// TestDataDirLockIsExclusive 验证同一数据目录不会被两个节点同时打开。
func TestDataDirLockIsExclusive(t *testing.T) {
	r := newTestRig(t, nil)
	r.start()

	// 第二个节点指向同一数据目录
	_, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	})
	if err == nil {
		t.Fatal("同一数据目录不应被两个节点同时打开")
	}
	if !strings.Contains(err.Error(), "占用") {
		t.Errorf("错误应说明目录被占用: %v", err)
	}
}

// TestAssemblyFailureReleasesStoreLock 验证装配失败时释放文件锁。
//
// bbolt 持有文件锁，装配中途失败若不释放，下一次启动会卡在
// "目录被占用"上，而真因是上一次装配失败——两条信息完全对不上。
func TestAssemblyFailureReleasesStoreLock(t *testing.T) {
	r := newTestRig(t, nil)
	// 失败点必须落在**打开存储之后**，否则这条用例什么都没验到：
	// 锁是 store.OpenSwappable 拿的，在它之前失败时根本没有锁可泄。
	// 这里用令牌文件不可读（读它排在打开存储之后），而不是此前的
	// "错误口令"——那一步排在打开存储之前，随 AUTH-092 一起去掉了。
	bad := r.cfg
	bad.Listeners.Admin.AuthTokenFile = r.dd.Root() // 目录不是文件，读必失败
	if _, err := New(Options{
		Config: bad, DataDir: r.dd, Manifest: r.mf,
	}); err == nil {
		t.Fatal("令牌文件不可读时装配应失败")
	}
	// 锁必须已释放：再次装配（这次配置正确）应成功
	s, err := New(Options{
		Config: r.cfg, DataDir: r.dd, Manifest: r.mf,
	})
	if err != nil {
		t.Fatalf("装配失败后文件锁未释放: %v", err)
	}
	_ = s.Close()
}

// TestRetainedSurvivesRestart 验证保留消息跨重启存活。
func TestRetainedSurvivesRestart(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		// 保留消息的限速默认每主题 1/秒，测试里放开
		c.Listeners.MQTTTLS.Addr = "127.0.0.1:0"
	})
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-ret")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-ret"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	// 发一条保留消息
	c.send(&packet.Publish{
		Topic: []byte("sensors/room1/temp"), Payload: []byte("21.5"),
		QoS: packet.QoS1, Retain: true, PacketID: c.nextPID(),
	})
	if ack := c.expect(packet.PUBACK).(*packet.Ack); ack.Code.IsError() {
		t.Fatalf("保留发布被拒: %s", ack.Code)
	}

	r.stop()

	// 重启，新订阅者应收到恢复出来的保留消息
	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()

	c2, err := r2.dial(&cert)
	if err != nil {
		t.Fatalf("重启后握手失败: %v", err)
	}
	if ack := c2.connect("dev-ret"); ack.Code != reason.Success {
		t.Fatalf("重启后连接被拒: %s", ack.Code)
	}
	if rc := c2.subscribe("sensors/+/temp"); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	// 订阅即应收到保留消息
	_ = c2.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	p, err := c2.recv()
	if err != nil {
		t.Fatalf("重启后未收到保留消息: %v", err)
	}
	pub, ok := p.(*packet.Publish)
	if !ok {
		t.Fatalf("收到 %s，期望 PUBLISH", p.Type())
	}
	if string(pub.Topic) != "sensors/room1/temp" || string(pub.Payload) != "21.5" {
		t.Errorf("= %q / %q", pub.Topic, pub.Payload)
	}
	if !pub.Retain {
		t.Error("投递给新订阅者的保留消息应置 Retain 标志")
	}
}

// TestPersistentSessionSurvivesRestart 验证持久会话的订阅跨重启存活。
func TestPersistentSessionSurvivesRestart(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-sess")
	r.start()

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	// CleanStart=false 加上非零会话过期间隔，即持久会话
	c.send(&packet.Connect{
		Version: packet.V5, ClientID: []byte("dev-sess"), CleanStart: false,
		Props: func() packet.Properties {
			var p packet.Properties
			p.SessionExpiry = 3600
			p.Set(packet.PropSessionExpiry)
			return p
		}(),
	})
	if ack := c.expect(packet.CONNACK).(*packet.Connack); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	if rc := c.subscribe("persist/topic"); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	_ = c.nc.Close()

	// 会话须已落盘
	deadline := time.Now().Add(3 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		if _, ok, _ := r.s.Sessions().Load(context.Background(), "dev-sess"); ok {
			found = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatal("持久会话应已落盘")
	}

	r.stop()
	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()

	// 重启后会话仍在，且订阅被保留
	got, ok, err := r2.s.Sessions().Load(context.Background(), "dev-sess")
	if err != nil || !ok {
		t.Fatalf("重启后会话应存在: %v, %v", ok, err)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0].Filter != "persist/topic" {
		t.Errorf("重启后的订阅 = %+v；客户端以 CleanStart=false 重连时"+
			"期待它仍在", got.Subscriptions)
	}
	if got.Epoch == 0 {
		t.Error("epoch 应一同恢复，否则所有权围栏失效")
	}
}

// policyFullAccess 放行一切，用于验证与策略无关的行为。
const policyFullAccess = `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`

// TestRevocationTakesEffectWithoutRestart 是 PRD PKI-030 的验收。
//
// 这一条此前被标为「待解决」：文件化注册表只在启动时载入，吊销要改
// certs.json 再重启才生效。控制面落地后，吊销经 meta.Store.Apply
// 发出一条变更，broker 的 watchMeta 据此踢掉在用连接——节点不重启。
//
// "仅改状态不断连视为未完成吊销"是 PKI-030 的原话，因此这条测试
// 同时断言两件事：在用连接被踢掉，且之后无法重连。
func TestRevocationTakesEffectWithoutRestart(t *testing.T) {
	r := newTestRig(t, nil)
	reg := NewRegistry(r.dd.Root())
	if err := os.WriteFile(filepath.Join(reg.PoliciesDir(), "device.json"),
		[]byte(policyFullAccess), 0o600); err != nil {
		t.Fatal(err)
	}
	cert := r.issue("dev-rev2")
	r.start()

	// 取出该证书的指纹
	certs, err := reg.LoadCerts()
	if err != nil || len(certs) == 0 {
		t.Fatalf("应有登记: %v", err)
	}
	fingerprint := certs[0].Fingerprint

	c, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	if ack := c.connect("dev-rev2"); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	// 连接可用
	if rc := c.publish("alive/check", "1", packet.QoS1); rc.IsError() {
		t.Fatalf("发布应成功: %s", rc)
	}

	// 经控制面吊销——不重启节点
	if err := r.s.ControlPlane().SetCertificateStatus(
		context.Background(), fingerprint, registry.StatusRevoked, "密钥泄露"); err != nil {
		t.Fatalf("吊销失败: %v", err)
	}

	// 在用连接须被踢掉
	if rc := c.expectClosed(5 * time.Second); rc != reason.NotAuthorized {
		t.Errorf("吊销后的断开原因码 = %s，期望 Not authorized；"+
			"仅改状态不断连视为未完成吊销（PKI-030）", rc)
	}

	// 且无法重连
	c2, err := r.dial(&cert)
	if err != nil {
		t.Fatalf("TLS 握手应仍成功（吊销不走 CRL）: %v", err)
	}
	if ack := c2.connect("dev-rev2"); ack.Code == reason.Success {
		t.Error("已吊销的证书不应能重新建立会话")
	}

	// 冷层也须已更新，使控制台与 API 看到一致的状态
	got, err := r.s.ControlPlane().GetCertificate(fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != registry.StatusRevoked || got.RevokeReason != "密钥泄露" {
		t.Errorf("= %+v", got)
	}
}

// TestFileRegistryImportIsIdempotent 验证迁移导入可重复执行。
func TestFileRegistryImportIsIdempotent(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-imp")
	r.start()

	cp := r.s.ControlPlane()
	// init 写出的示例策略加上 newTestRig 写的 device 策略
	names, err := cp.ListPolicies()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("导入后控制面应有策略")
	}
	certs, err := cp.ListCertificates(registry.CertQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(certs.Certificates) != 1 {
		t.Fatalf("导入后应有一张证书，实际 %d", len(certs.Certificates))
	}
	// 证书绑定的设备须被隐含创建
	if _, err := cp.GetThing("dev-imp"); err != nil {
		t.Errorf("证书绑定的设备应被创建: %v", err)
	}

	// 重启：导入不应重复创建，也不应报错
	r.stop()
	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()

	certs2, err := r2.s.ControlPlane().ListCertificates(registry.CertQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(certs2.Certificates) != 1 {
		t.Errorf("重启后证书数 = %d，期望 1；导入不是幂等的",
			len(certs2.Certificates))
	}
	names2, _ := r2.s.ControlPlane().ListPolicies()
	if len(names2) != len(names) {
		t.Errorf("重启后策略数 = %d，期望 %d", len(names2), len(names))
	}
}

// TestCLIIssuedCertImportsOnLaterStart 验证「先 serve 过一次、再命令行签发、
// 再重启」这条路能起来。
//
// 它与 TestFileRegistryImportIsIdempotent 的区别只有一处顺序：那条测试在
// **第一次启动之前**就把证书签好了，于是导入发生在一个空冷层上——策略由
// 导入自己写进去（PutPolicy 两层一起写），热视图当场就有。本测试签在第一次
// 启动**之后**，于是第二次启动时冷层已经有策略、导入会跳过它，而热视图要等
// Restore 才有。两件事的顺序一旦对调，这一次启动就会以「策略 "device" 不
// 存在」失败，而那份策略明明在冷层里——节点从此起不来，且报错指向策略而不
// 指向顺序。
//
// 这条路正是命令行签发的既定用法（`cert issue` 自己的提示就是「需重启才会
// 载入新证书」），所以它必须有专项覆盖。
func TestCLIIssuedCertImportsOnLaterStart(t *testing.T) {
	r := newTestRig(t, nil)
	r.start()
	r.stop()

	// 命令行签发只写文件化注册表，不碰控制面。
	r.issue("dev-later")

	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()
	defer r2.stop()

	cp := r2.s.ControlPlane()
	if _, err := cp.GetThing("dev-later"); err != nil {
		t.Errorf("重启后应导入命令行签发的设备: %v", err)
	}
	certs, err := cp.ListCertificates(registry.CertQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(certs.Certificates) != 1 {
		t.Errorf("重启后证书数 = %d，期望 1", len(certs.Certificates))
	}
}

// TestControlPlaneSurvivesRestart 验证控制面的内容跨重启存活。
func TestControlPlaneSurvivesRestart(t *testing.T) {
	r := newTestRig(t, nil)
	r.issue("dev-cp")
	r.start()

	ctx := context.Background()
	cp := r.s.ControlPlane()
	if err := cp.PutPolicy(ctx, "extra", []byte(policyFullAccess)); err != nil {
		t.Fatal(err)
	}
	if err := cp.PutThing(ctx, registry.Thing{
		Name: "dev-extra", PolicyName: "extra",
		Attributes: map[string]string{"site": "sh"},
	}); err != nil {
		t.Fatal(err)
	}

	r.stop()
	r2 := &testRig{t: t, cfg: r.cfg, dd: r.dd, mf: r.mf}
	r2.start()

	cp2 := r2.s.ControlPlane()
	if _, err := cp2.GetPolicy("extra"); err != nil {
		t.Errorf("策略未存活: %v", err)
	}
	got, err := cp2.GetThing("dev-extra")
	if err != nil {
		t.Fatalf("设备未存活: %v", err)
	}
	if got.Attributes["site"] != "sh" {
		t.Errorf("属性 = %v", got.Attributes)
	}
}
