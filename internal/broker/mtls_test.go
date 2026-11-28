package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/pki"
)

// tlsRig 在 rig 之上加一套真实的 PKI 与 TLS 监听器。
//
// 这是本轮的验收点：ConnInfo 不再由测试注入，而是从真实握手中提取。
// 测试注入能证明 broker 的逻辑正确，只有真实握手能证明提取逻辑正确——
// 指纹算法、DN 字段映射、序列号格式、ALPN 协商，这些都只有在这里才被检验。
type tlsRig struct {
	*rig
	ca     *pki.CA
	root   *pki.Bundle
	ln     *Listener
	addr   string
	cancel context.CancelFunc
}

func newTLSRig(t *testing.T, auth Authenticator, opt TLSOptions) *tlsRig {
	t.Helper()
	r := newRig(t, auth, nil)

	root, err := pki.CreateRootCA(pki.CARequest{
		CommonName: "Test Root CA", Organization: "Acme", MaxPathLen: 1,
	}, wallNowMs())
	if err != nil {
		t.Fatalf("创建根 CA 失败: %v", err)
	}
	issuing, err := pki.CreateIssuingCA(root, pki.CARequest{
		CommonName: "Test Issuing CA", MaxPathLen: 0,
	}, wallNowMs())
	if err != nil {
		t.Fatalf("创建签发 CA 失败: %v", err)
	}
	policy := pki.DefaultPolicy()
	ca, err := pki.New(issuing, []*x509.Certificate{issuing.Cert, root.Cert},
		policy, wallNowMs)
	if err != nil {
		t.Fatalf("创建 CA 失败: %v", err)
	}

	// 服务端证书
	srv, err := ca.IssueServerCert("broker.local",
		[]string{"broker.local", "localhost"}, []string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatalf("签发服务端证书失败: %v", err)
	}
	// 必须把签发 CA 一并呈现：客户端只信任根，若服务端只发叶子证书，
	// 校验会以"未知签发者"失败。这是分层 CA 最常见的部署错误。
	srvCert, err := tls.X509KeyPair(concatPEM(srv.CertPEM, srv.ChainPEM), srv.KeyPEM)
	if err != nil {
		t.Fatalf("组装服务端证书失败: %v", err)
	}

	if opt.MinVersion == 0 {
		opt.MinVersion = tls.VersionTLS12
	}
	opt.Certificate = srvCert
	if opt.ClientCAs == nil {
		opt.ClientCAs = ca.TrustPool()
	}

	ln, err := r.b.ListenTLS("127.0.0.1:0", opt)
	if err != nil {
		t.Fatalf("启动 TLS 监听失败: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = ln.Serve(ctx)
	}()

	tr := &tlsRig{rig: r, ca: ca, root: root, ln: ln,
		addr: ln.Addr().String(), cancel: cancel}
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
	})
	return tr
}

// issueDevice 为设备签发证书并在元数据中注册。
func (tr *tlsRig) issueDevice(t *testing.T, cn, thing string, ps meta.PolicySetID) tls.Certificate {
	t.Helper()
	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: cn})
	if err != nil {
		t.Fatalf("签发设备证书失败: %v", err)
	}
	// 注册时用的指纹就是 pki 算出来的那个；若两侧算法不一致，
	// 认证会在"证书未注册"上失败——这正是本测试要验证的衔接点。
	tr.addCert(meta.CertFingerprint(issued.Fingerprint), thing, ps, meta.CertActive)
	cert, err := tls.X509KeyPair(concatPEM(issued.CertPEM, issued.ChainPEM), issued.KeyPEM)
	if err != nil {
		t.Fatalf("组装设备证书失败: %v", err)
	}
	return cert
}

// dialTLS 以给定的客户端证书建立真实 TLS 连接。
func (tr *tlsRig) dialTLS(t *testing.T, certs []tls.Certificate, alpn []string,
	serverName string,
) (*testClient, error) {
	t.Helper()
	cfg := tr.clientTLSConfig(certs, alpn, serverName)
	d := &net.Dialer{Timeout: 3 * time.Second}
	nc, err := tls.DialWithDialer(d, "tcp", tr.addr, cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = nc.Close() })
	return &testClient{t: t, nc: nc, ver: packet.V5}, nil
}

func (tr *tlsRig) clientTLSConfig(certs []tls.Certificate, alpn []string,
	serverName string,
) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(tr.root.Cert)
	return &tls.Config{
		Certificates: certs,
		RootCAs:      pool,
		ServerName:   serverName,
		NextProtos:   alpn,
		MinVersion:   tls.VersionTLS12,
	}
}

// expectTLSRejected 断言服务端拒绝了这次握手。
//
// 不能只看 Dial 是否报错：TLS 1.3 下客户端发完自己的 Finished 就认为
// 握手完成，服务端对客户端证书的校验结果是之后才以 alert 形式送达的。
// 所以必须真正读一次——读到非超时错误才说明服务端拒绝了。
func (tr *tlsRig) expectTLSRejected(t *testing.T, certs []tls.Certificate,
	alpn []string, serverName string,
) {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(tr.root.Cert)
	nc, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", tr.addr,
		&tls.Config{
			Certificates: certs,
			RootCAs:      pool,
			ServerName:   serverName,
			NextProtos:   alpn,
			MinVersion:   tls.VersionTLS12,
		})
	if err != nil {
		return // 握手阶段即被拒，符合预期
	}
	defer func() { _ = nc.Close() }()

	_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
	// 先写一个 CONNECT，迫使服务端的 alert 或关闭送达
	_, _ = nc.Write([]byte{0x10, 0x00})
	var buf [64]byte
	_, err = nc.Read(buf[:])
	if err == nil {
		t.Error("服务端应拒绝这次握手，却收到了数据")
		return
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Error("服务端既未拒绝也未关闭连接，客户端证书校验未生效")
	}
}

// TestMTLSEndToEnd 是本轮的核心验收：真实 mTLS 握手后完成 MQTT 全流程。
func TestMTLSEndToEnd(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyDeviceScoped)
	cert := tr.issueDevice(t, "dev-001.acme", "dev-001", ps)

	c, err := tr.dialTLS(t, []tls.Certificate{cert},
		[]string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("TLS 握手失败: %v", err)
	}
	if ack := c.connect("dev-001", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	// 自己名下的主题可发布
	if rc := c.publish("$aws/things/dev-001/shadow/update", "{}", packet.QoS1, false); rc.IsError() {
		t.Errorf("自己名下的主题应放行，得到 %s", rc)
	}
	// 别人名下的主题被拒
	if rc := c.publish("$aws/things/dev-002/shadow/update", "{}", packet.QoS1, false); rc != reason.NotAuthorized {
		t.Errorf("别人名下的主题应被拒，得到 %s", rc)
	}
	// 订阅与自收自发
	if rc := c.subscribe("$aws/things/dev-001/shadow/update/accepted", packet.QoS1); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}
	// 用 QoS0 自发：QoS1 下 PUBACK 与自投递的 PUBLISH 到达顺序不确定，
	// 而测试客户端是顺序读取的。这是测试装置的限制，不是协议问题。
	c.publish("$aws/things/dev-001/shadow/update/accepted", "ok", packet.QoS0, false)
	if got := c.expectPublish(); string(got.Payload) != "ok" {
		t.Errorf("负载 = %q", got.Payload)
	}
}

// TestMTLSFingerprintMatchesPKI 固定住 pki 与 broker 之间的衔接点。
//
// 注册时用 pki 算出的指纹，认证时用握手提取的指纹，两者必须一致。
// 若指纹算法（DER 的 SHA-256）在任一侧改变，设备会以「证书未注册」
// 被拒——而那个错误信息完全指不出真正的原因，所以必须有测试锁住。
func TestMTLSFingerprintMatchesPKI(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyFull)

	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-fp"})
	if err != nil {
		t.Fatal(err)
	}
	fpFromPKI := meta.CertFingerprint(issued.Fingerprint)
	tr.addCert(fpFromPKI, "dev-fp", ps, meta.CertActive)

	cert, err := tls.X509KeyPair(issued.CertPEM, issued.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-fp", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒，可能是两侧指纹算法不一致: %s", ack.Code)
	}
}

// TestMTLSConnInfoComesFromHandshake 验证传输层事实确实来自握手。
func TestMTLSConnInfoComesFromHandshake(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
		MinVersion:        tls.VersionTLS13,
	})
	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-info.acme", "dev-info", ps)

	getEvents, stop := tr.collect(eventFilterConn())
	defer stop()

	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-info", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	deadline := time.Now().Add(testPatience)
	for time.Now().Before(deadline) {
		for _, e := range getEvents() {
			if e.ClientID != "dev-info" || e.Conn.ReasonCode != reason.Success {
				continue
			}
			if e.Conn.ALPN != ALPNMQTTCert {
				t.Errorf("ALPN = %q，期望 %q", e.Conn.ALPN, ALPNMQTTCert)
			}
			if e.Conn.TLSVersion != tls.VersionTLS13 {
				t.Errorf("TLS 版本 = %#x，期望 TLS 1.3", e.Conn.TLSVersion)
			}
			// 证书主题的通用名从真实证书提取，而非测试注入
			if e.Conn.CertCN != "dev-info.acme" {
				t.Errorf("证书通用名 = %q，期望 dev-info.acme", e.Conn.CertCN)
			}
			if len(e.Conn.CertFP) != 64 {
				t.Errorf("指纹长度 = %d，期望 64 个十六进制字符", len(e.Conn.CertFP))
			}
			// 握手耗时必须是真实测量值
			if e.Conn.HandshakeMS <= 0 {
				t.Errorf("握手耗时 = %v，应为真实测量的正值", e.Conn.HandshakeMS)
			}
			if !e.Conn.Remote.IsValid() {
				t.Error("远端地址应有效")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("未收到连接事件")
}

// TestMTLSCertSubjectDrivesPolicy 验证策略变量能引用真实证书字段。
func TestMTLSCertSubjectDrivesPolicy(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	// 策略按证书的通用名授权，而不是按 clientId
	ps := tr.addPolicy(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Connect","Resource":"*"},
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/by-cn/${iot:Certificate.Subject.CommonName}/*"}
	]}`)
	cert := tr.issueDevice(t, "dev-cn.acme", "dev-cn", ps)

	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-cn", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}
	// 证书通用名对应的主题放行
	if rc := c.publish("by-cn/dev-cn.acme/x", "v", packet.QoS1, false); rc.IsError() {
		t.Errorf("按证书通用名授权的主题应放行，得到 %s", rc)
	}
	// 别的通用名被拒
	if rc := c.publish("by-cn/other/x", "v", packet.QoS1, false); rc != reason.NotAuthorized {
		t.Errorf("其他通用名应被拒，得到 %s", rc)
	}
}

// TestMTLSRevocationDisconnects 验证吊销在真实 mTLS 链路上同样立即生效。
func TestMTLSRevocationDisconnects(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyFull)
	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-rev"})
	if err != nil {
		t.Fatal(err)
	}
	fp := meta.CertFingerprint(issued.Fingerprint)
	tr.addCert(fp, "dev-rev", ps, meta.CertActive)
	cert, _ := tls.X509KeyPair(concatPEM(issued.CertPEM, issued.ChainPEM), issued.KeyPEM)

	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-rev", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	tr.revoke(fp, "dev-rev", ps, "密钥泄露")
	if rc := c.expectClosed(3 * time.Second); rc != reason.NotAuthorized {
		t.Errorf("吊销后的断开原因码 = %s，期望 Not authorized", rc)
	}

	// TLS 层仍能握手成功（证书本身未过期），但 MQTT 层拒绝——
	// 这正是「吊销状态存在注册表而非 CRL」的表现（PRD PKI-001）。
	tr.clk.advance(2 * time.Second)
	again, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("吊销后 TLS 握手仍应成功（吊销不走 CRL）: %v", err)
	}
	if ack := again.connect("dev-rev", true); ack.Code == reason.Success {
		t.Error("已吊销的证书不应能建立 MQTT 会话")
	}
}

// TestResumedMTLSStillChecksRegistry 钉住 session ticket 只省 TLS 公钥运算，
// 不缓存 MQTT 认证结论。证书在两波之间被吊销时，恢复握手仍须把原证书
// 身份带给 broker，并由最新元数据视图拒绝 CONNECT。
func TestResumedMTLSStillChecksRegistry(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyFull)
	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-resume"})
	if err != nil {
		t.Fatal(err)
	}
	fp := meta.CertFingerprint(issued.Fingerprint)
	tr.addCert(fp, "dev-resume", ps, meta.CertActive)
	cert, err := tls.X509KeyPair(concatPEM(issued.CertPEM, issued.ChainPEM), issued.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	cfg := tr.clientTLSConfig([]tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	cfg.MinVersion = tls.VersionTLS13
	cfg.MaxVersion = tls.VersionTLS13
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	dial := func() (*tls.Conn, *testClient) {
		t.Helper()
		nc, derr := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second},
			"tcp", tr.addr, cfg)
		if derr != nil {
			t.Fatalf("TLS 拨号失败: %v", derr)
		}
		return nc, &testClient{t: t, nc: nc, ver: packet.V5}
	}

	firstTLS, first := dial()
	if firstTLS.ConnectionState().DidResume {
		t.Fatal("第一波冷握手不应报告 session 恢复")
	}
	if ack := first.connect("dev-resume", true); ack.Code != reason.Success {
		t.Fatalf("第一波连接被拒: %s", ack.Code)
	}
	_ = firstTLS.Close()

	tr.revoke(fp, "dev-resume", ps, "ticket 期间吊销")
	tr.clk.advance(2 * time.Second)
	secondTLS, second := dial()
	defer func() { _ = secondTLS.Close() }()
	if !secondTLS.ConnectionState().DidResume {
		t.Fatal("第二波没有恢复 TLS session，测试未覆盖 ticket 路径")
	}
	if ack := second.connect("dev-resume", true); ack.Code == reason.Success {
		t.Fatal("恢复的 TLS session 绕过了最新证书吊销状态")
	}
}

func TestSharedTicketKeysResumeAcrossListeners(t *testing.T) {
	var ticketKey [32]byte
	for i := range ticketKey {
		ticketKey[i] = byte(i + 1)
	}
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
		SessionTicketKeys: [][32]byte{ticketKey},
	})
	// 第二个监听器模拟负载均衡后的另一个节点：独立 tls.Config，但使用
	// 同一票据密钥环、证书信任与 MQTT broker 状态。
	second, err := tr.b.ListenTLS("127.0.0.1:0", tr.ln.opt)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr.wg.Add(1)
	go func() {
		defer tr.wg.Done()
		_ = second.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = second.Close()
	})

	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-cross", "dev-cross", ps)
	cfg := tr.clientTLSConfig([]tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	cfg.MinVersion = tls.VersionTLS13
	cfg.MaxVersion = tls.VersionTLS13
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	dial := func(addr string) (*tls.Conn, *testClient) {
		t.Helper()
		nc, derr := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, cfg)
		if derr != nil {
			t.Fatalf("TLS 拨号失败: %v", derr)
		}
		return nc, &testClient{t: t, nc: nc, ver: packet.V5}
	}

	firstTLS, first := dial(tr.addr)
	if ack := first.connect("dev-cross", true); ack.Code != reason.Success {
		t.Fatalf("第一节点 CONNECT 被拒: %s", ack.Code)
	}
	_ = firstTLS.Close()
	tr.clk.advance(2 * time.Second)

	secondTLS, client := dial(second.Addr().String())
	defer func() { _ = secondTLS.Close() }()
	if !secondTLS.ConnectionState().DidResume {
		t.Fatal("共享 ticket key 后切换监听器仍未恢复 TLS session")
	}
	if ack := client.connect("dev-cross", true); ack.Code != reason.Success {
		t.Fatalf("恢复后 CONNECT 被拒: %s", ack.Code)
	}
}

func TestSessionTicketKeyRotationAcceptsOldAndIssuesNew(t *testing.T) {
	var oldKey, newKey [32]byte
	for i := range oldKey {
		oldKey[i] = byte(i + 1)
		newKey[i] = byte(255 - i)
	}
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
		SessionTicketKeys: [][32]byte{oldKey},
	})
	start := func(keys [][32]byte) *Listener {
		t.Helper()
		opt := tr.ln.opt
		opt.SessionTicketKeys = keys
		ln, err := tr.b.ListenTLS("127.0.0.1:0", opt)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		tr.wg.Add(1)
		go func() {
			defer tr.wg.Done()
			_ = ln.Serve(ctx)
		}()
		t.Cleanup(func() { cancel(); _ = ln.Close() })
		return ln
	}
	rotating := start([][32]byte{newKey, oldKey})
	newOnly := start([][32]byte{newKey})

	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-rotate", "dev-rotate", ps)
	cfg := tr.clientTLSConfig([]tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	cfg.MinVersion = tls.VersionTLS13
	cfg.MaxVersion = tls.VersionTLS13
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	dial := func(addr string) (*tls.Conn, *testClient) {
		t.Helper()
		nc, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return nc, &testClient{t: t, nc: nc, ver: packet.V5}
	}
	connect := func(addr string, wantResume bool) {
		t.Helper()
		tlsConn, client := dial(addr)
		if got := tlsConn.ConnectionState().DidResume; got != wantResume {
			t.Fatalf("DidResume=%v，期望 %v", got, wantResume)
		}
		if ack := client.connect("dev-rotate", true); ack.Code != reason.Success {
			t.Fatalf("CONNECT 被拒: %s", ack.Code)
		}
		_ = tlsConn.Close()
	}

	connect(tr.addr, false) // old 签发
	tr.clk.advance(2 * time.Second)
	connect(rotating.Addr().String(), true) // [new, old] 解开 old，并签发 new
	tr.clk.advance(2 * time.Second)
	connect(newOnly.Addr().String(), true) // 只有 new 仍能恢复
}

// TestMTLSRequiresClientCert 验证未出示证书的客户端在 TLS 层即被拒。
func TestMTLSRequiresClientCert(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	tr.expectTLSRejected(t, nil, []string{ALPNMQTTCert}, "broker.local")
}

// TestMTLSRejectsUnknownCA 验证非本 CA 签发的证书被 TLS 层拒绝。
//
// 这一层拒绝先于 MQTT 认证发生：证书链不可信时，连注册表都不必查。
func TestMTLSRejectsUnknownCA(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	// 另建一套无关的 CA
	otherRoot, err := pki.CreateRootCA(pki.CARequest{CommonName: "Rogue Root"}, wallNowMs())
	if err != nil {
		t.Fatal(err)
	}
	otherCA, err := pki.New(otherRoot, []*x509.Certificate{otherRoot.Cert},
		pki.DefaultPolicy(), wallNowMs)
	if err != nil {
		t.Fatal(err)
	}
	rogue, err := otherCA.IssueKeyPair(pki.IssueRequest{CommonName: "rogue"})
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := tls.X509KeyPair(concatPEM(rogue.CertPEM, rogue.ChainPEM), rogue.KeyPEM)

	tr.expectTLSRejected(t, []tls.Certificate{cert},
		[]string{ALPNMQTTCert}, "broker.local")
}

// TestMTLSALPNMismatch 验证 ALPN 不匹配时握手失败。
//
// 443 端口的 mTLS 必须协商 x-amzn-mqtt-ca——这是官方 SDK 在受限网络下
// 的默认路径（PRD NET-002）。
func TestMTLSALPNMismatch(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-alpn", "dev-alpn", ps)

	if _, err := tr.dialTLS(t, []tls.Certificate{cert},
		[]string{"http/1.1"}, "broker.local"); err == nil {
		t.Error("ALPN 不匹配应导致握手失败")
	}
	// 协商正确的协议名则成功
	if _, err := tr.dialTLS(t, []tls.Certificate{cert},
		[]string{ALPNMQTTCert}, "broker.local"); err != nil {
		t.Errorf("正确的 ALPN 应成功: %v", err)
	}
}

// TestMTLSRequireSNI 验证缺少 SNI 时拒绝连接。
//
// 与 AWS 行为一致：SNI 是多租户与自定义域名的分流依据（PRD NET-013）。
func TestMTLSRequireSNI(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
		RequireSNI:        true,
	})
	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-sni", "dev-sni", ps)

	// 以 IP 连接时不会发送 SNI
	pool := x509.NewCertPool()
	pool.AddCert(tr.root.Cert)
	nc, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", tr.addr,
		&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			NextProtos:   []string{ALPNMQTTCert},
			MinVersion:   tls.VersionTLS12,
			// 不设 ServerName，且跳过服务端校验以便握手能走到服务端的判断
			InsecureSkipVerify: true,
		})
	if err == nil {
		defer func() { _ = nc.Close() }()
		// 握手本身可能成功，但服务端会立刻关闭连接
		_ = nc.SetReadDeadline(time.Now().Add(testPatience))
		var buf [16]byte
		if _, err := nc.Read(buf[:]); err == nil {
			t.Error("缺少 SNI 的连接应被关闭")
		}
		return
	}
	// 握手阶段就失败也是可接受的结果
}

// TestListenTLSValidation 验证监听器参数校验。
func TestListenTLSValidation(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	if _, err := r.b.ListenTLS("127.0.0.1:0", TLSOptions{}); err == nil {
		t.Error("缺少服务端证书应报错")
	}

	root, _ := pki.CreateRootCA(pki.CARequest{CommonName: "R"}, wallNowMs())
	ca, _ := pki.New(root, []*x509.Certificate{root.Cert}, pki.DefaultPolicy(), wallNowMs)
	srv, err := ca.IssueServerCert("localhost", []string{"localhost"}, nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := tls.X509KeyPair(concatPEM(srv.CertPEM, srv.ChainPEM), srv.KeyPEM)

	if _, err := r.b.ListenTLS("127.0.0.1:0", TLSOptions{
		Certificate: cert, RequireClientCert: true,
	}); err == nil {
		t.Error("要求客户端证书但无信任池应报错")
	}
	if _, err := r.b.ListenTLS("bad-address", TLSOptions{Certificate: cert}); err == nil {
		t.Error("非法地址应报错")
	}
	// 合法参数
	ln, err := r.b.ListenTLS("127.0.0.1:0", TLSOptions{Certificate: cert})
	if err != nil {
		t.Fatalf("合法参数应成功: %v", err)
	}
	if ln.Addr() == nil {
		t.Error("Addr 不应为 nil")
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Errorf("重复关闭应幂等: %v", err)
	}
}

// TestListenPlainWorks 验证明文监听器（供前置代理终结 TLS 的部署）。
func TestListenPlainWorks(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln, err := r.b.ListenPlain("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = ln.Serve(ctx)
	}()
	t.Cleanup(func() { _ = ln.Close() })

	nc, err := net.DialTimeout("tcp", ln.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	c := &testClient{t: t, nc: nc, ver: packet.V5}
	if ack := c.connect("plain-dev", true); ack.Code != reason.Success {
		t.Errorf("明文连接被拒: %s", ack.Code)
	}
}

// TestHandshakeTimeout 验证只连 TCP 不发 ClientHello 的对端会被超时清理。
//
// 没有这个超时，最廉价的资源耗尽手段就是开一堆 TCP 连接然后什么都不发。
func TestHandshakeTimeout(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
		HandshakeTimeout:  200 * time.Millisecond,
	})
	nc, err := net.DialTimeout("tcp", tr.addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()

	// 什么都不发，等待服务端超时关闭
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	var buf [1]byte
	_, err = nc.Read(buf[:])
	if err == nil {
		t.Error("服务端应因握手超时关闭连接")
		return
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Error("连接未被服务端关闭，握手超时未生效")
	}
}

func TestListenerCloseWaitsForHandedOffTLSConnection(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(policyFull)
	cert := tr.issueDevice(t, "dev-handoff", "dev-handoff", ps)
	cfg := tr.clientTLSConfig([]tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	nc, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", tr.addr, cfg)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- tr.ln.Close() }()
	select {
	case err := <-done:
		t.Fatalf("TLS 长连接仍在时 Listener.Close 提前返回: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	_ = nc.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("交接后的 TLS goroutine 退出后 Listener.Close 仍未返回")
	}
}

// TestSerialHexFormatIsStable 固定住证书序列号的字符串格式。
//
// 它会被策略变量 ${iot:Certificate.SerialNumber} 引用，而策略是按字符串
// 精确匹配的——格式一变，所有引用该变量的策略全部静默失效。
func TestSerialHexFormatIsStable(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	ps := tr.addPolicy(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Connect","Resource":"*"},
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/serial/${iot:Certificate.SerialNumber}"}
	]}`)
	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-serial"})
	if err != nil {
		t.Fatal(err)
	}
	tr.addCert(meta.CertFingerprint(issued.Fingerprint), "dev-serial", ps, meta.CertActive)
	cert, _ := tls.X509KeyPair(concatPEM(issued.CertPEM, issued.ChainPEM), issued.KeyPEM)

	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if ack := c.connect("dev-serial", true); ack.Code != reason.Success {
		t.Fatalf("连接被拒: %s", ack.Code)
	}

	// 期望的格式：小写十六进制、无分隔符、无前导零
	want := strings.ToLower(issued.Cert.SerialNumber.Text(16))
	if len(want)%2 == 1 {
		want = "0" + want
	}
	if rc := c.publish("serial/"+want, "v", packet.QoS1, false); rc.IsError() {
		t.Errorf("按序列号授权的主题应放行（格式应为小写无分隔十六进制），得到 %s", rc)
	}
}

// wallNowMs 是墙上时钟。
//
// 装置里其他一切都用注入时钟，唯独证书签发不行：crypto/tls 校验有效期
// 时读的是真实时间，拿假时钟签出来的证书在握手阶段就会被判为过期。
// 这个分界本身值得记下来——它意味着「证书有效期」这一层永远无法用
// 假时钟测试，只有吊销状态（在注册表里，走注入时钟）可以。
func wallNowMs() int64 { return time.Now().UnixMilli() }

// concatPEM 把叶子证书与其证书链拼成 TLS 要呈现的完整链。
func concatPEM(leaf, chain []byte) []byte {
	out := make([]byte, 0, len(leaf)+len(chain))
	out = append(out, leaf...)
	return append(out, chain...)
}

func eventFilterConn() events.Filter {
	return events.Filter{Kinds: events.KindConn.Mask()}
}

// TestSerialHexFormat 直接锁定序列号的字符串格式。
//
// 走 TLS 的那条测试每次只覆盖一个序列号，而格式分歧恰好出现在少数情形：
// 128 位随机序列号约有 6% 带前导全零字节，此时按字节编码与
// big.Int.Text(16) 的结果长度不同。所以格式必须直接、批量地验。
func TestSerialHexFormat(t *testing.T) {
	cases := []struct {
		serial *big.Int
		want   string
	}{
		{big.NewInt(0), "00"},
		{big.NewInt(1), "01"},
		{big.NewInt(255), "ff"},
		{big.NewInt(256), "0100"},
		{big.NewInt(0xdeadbeef), "deadbeef"},
		// 带前导全零字节：按字节编码会去掉它
		{new(big.Int).SetBytes([]byte{0x00, 0x00, 0x12, 0x34}), "1234"},
	}
	for _, c := range cases {
		got := serialHex(&x509.Certificate{SerialNumber: c.serial})
		if got != c.want {
			t.Errorf("serialHex(%v) = %q，期望 %q", c.serial, got, c.want)
		}
	}
	if got := serialHex(&x509.Certificate{}); got != "" {
		t.Errorf("序列号缺失时 = %q，期望空串", got)
	}

	// 批量验不变量：恒为小写十六进制、长度为偶数、无分隔符
	ca := func() *pki.CA {
		root, err := pki.CreateRootCA(pki.CARequest{CommonName: "Serial Root"}, wallNowMs())
		if err != nil {
			t.Fatal(err)
		}
		c, err := pki.New(root, []*x509.Certificate{root.Cert},
			pki.DefaultPolicy(), wallNowMs)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}()
	for i := 0; i < 200; i++ {
		issued, err := ca.IssueKeyPair(pki.IssueRequest{CommonName: "s"})
		if err != nil {
			t.Fatal(err)
		}
		got := serialHex(issued.Cert)
		if len(got)%2 != 0 {
			t.Fatalf("序列号 %q 长度为奇数", got)
		}
		if got != strings.ToLower(got) {
			t.Fatalf("序列号 %q 含大写字符", got)
		}
		if strings.ContainsAny(got, ":- ") {
			t.Fatalf("序列号 %q 含分隔符", got)
		}
		if got[0] == '0' && got[1] == '0' {
			t.Fatalf("序列号 %q 含前导全零字节，应已去除", got)
		}
	}
}

// TestUnregisteredCertReasonCode 固定住「证书未登记」的原因码。
//
// 这是设备接入最常见的失败，它的原因码直接决定排查方向。
// 曾经返回 ClientIdentifierNotValid（0x85），那会让设备方去查 clientId，
// 而真正的问题是证书没有登记。改为 NotAuthorized（0x87）后，
// 它与「已吊销」「未激活」同码——这既语义正确，也使外部无法
// 据原因码探测某个指纹是否已登记。
func TestUnregisteredCertReasonCode(t *testing.T) {
	tr := newTLSRig(t, CertAuthenticator{}, TLSOptions{
		RequireClientCert: true,
		NextProtos:        []string{ALPNMQTTCert},
	})
	// 由本 CA 签发但不登记
	issued, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-unreg"})
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := tls.X509KeyPair(
		concatPEM(issued.CertPEM, issued.ChainPEM), issued.KeyPEM)

	c, err := tr.dialTLS(t, []tls.Certificate{cert}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatalf("TLS 握手应成功（证书由本 CA 签发）: %v", err)
	}
	ack := c.connect("dev-unreg", true)
	if ack.Code != reason.NotAuthorized {
		t.Errorf("未登记证书的原因码 = %s（%#x），期望 Not authorized（0x87）",
			ack.Code, byte(ack.Code))
	}

	// 已登记但未激活的证书必须返回同一个码，否则原因码本身
	// 就成了探测指纹是否已登记的手段
	ps := tr.addPolicy(policyFull)
	inactive, err := tr.ca.IssueKeyPair(pki.IssueRequest{CommonName: "dev-inactive"})
	if err != nil {
		t.Fatal(err)
	}
	tr.addCert(meta.CertFingerprint(inactive.Fingerprint), "dev-inactive", ps, meta.CertInactive)
	cert2, _ := tls.X509KeyPair(
		concatPEM(inactive.CertPEM, inactive.ChainPEM), inactive.KeyPEM)
	c2, err := tr.dialTLS(t, []tls.Certificate{cert2}, []string{ALPNMQTTCert}, "broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if ack := c2.connect("dev-inactive", true); ack.Code != reason.NotAuthorized {
		t.Errorf("未激活证书的原因码 = %s，应与未登记一致", ack.Code)
	}
}
