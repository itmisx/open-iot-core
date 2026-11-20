package broker

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecret    = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testAmzDate   = "20150830T123600Z"
	emptySHA      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// RFC 6455 的样例。Accept 必须是文档里的那个值，自己重算一遍会把公式算错也测成对。
	testWSKey    = "dGhlIHNhbXBsZSBub25jZQ=="
	testWSAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
)

func testSigTime() time.Time {
	return time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
}

func testChecker(secret func(string) (string, bool)) sigv4.Checker {
	if secret == nil {
		secret = func(id string) (string, bool) {
			if id == testAccessKey {
				return testSecret, true
			}
			return "", false
		}
	}
	return sigv4.Checker{
		Region:   "us-east-1",
		Services: map[string]bool{sigv4.ServiceDeviceGateway: true},
		Now:      testSigTime,
		Secret:   secret,
	}
}

func signedQuery(t *testing.T, host, keyID, service string) string {
	t.Helper()
	if keyID == "" {
		keyID = testAccessKey
	}
	if service == "" {
		service = sigv4.ServiceDeviceGateway
	}
	cred := keyID + "/20150830/us-east-1/" + service + "/aws4_request"
	q := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=" + cred +
		"&X-Amz-Date=" + testAmzDate +
		"&X-Amz-SignedHeaders=host"
	canon, err := sigv4.CanonicalRequest("GET", "/mqtt", q,
		map[string][]string{"Host": {host}}, []string{"host"}, emptySHA)
	if err != nil {
		t.Fatal(err)
	}
	sig := sigv4.Sign(testSecret, sigv4.Scope{
		AccessKeyID: keyID,
		Date:        "20150830",
		Region:      "us-east-1",
		Service:     service,
	}, testAmzDate, canon)
	return q + "&X-Amz-Signature=" + sig
}

func testCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (r *rig) startWSS(opt WebSocketOptions) *Listener {
	r.t.Helper()
	if len(opt.Certificate.Certificate) == 0 {
		opt.Certificate = testCert(r.t)
	}
	if opt.Checker.Secret == nil {
		opt.Checker = testChecker(nil)
	}
	ln, err := r.b.ListenWebSocket("127.0.0.1:0", opt)
	if err != nil {
		r.t.Fatal(err)
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = ln.Serve(context.Background())
	}()
	r.t.Cleanup(func() { _ = ln.Close() })
	return ln
}

type wsRequest struct {
	path     string
	query    string
	protocol string
	header   map[string]string
}

func dialWSS(t *testing.T, addr string, req wsRequest) (int, map[string]string, *wsConn) {
	t.Helper()
	nc, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	if err := nc.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	host := addr
	path := req.path
	if path == "" {
		path = "/mqtt"
	}
	target := path
	if req.query != "" {
		target += "?" + req.query
	}
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n",
		target, host, testWSKey)
	if req.protocol != "omit" {
		proto := req.protocol
		if proto == "" {
			proto = "mqtt"
		}
		fmt.Fprintf(&b, "Sec-WebSocket-Protocol: %s\r\n", proto)
	}
	for k, v := range req.header {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	if _, err := nc.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	status, hdr, err := readHTTPHead(br)
	if err != nil {
		t.Fatal(err)
	}
	if status != 101 {
		return status, hdr, nil
	}
	if hdr["sec-websocket-accept"] != testWSAccept {
		t.Fatalf("Sec-WebSocket-Accept = %q，期望 RFC 6455 的样例值", hdr["sec-websocket-accept"])
	}
	if hdr["sec-websocket-protocol"] != "mqtt" {
		t.Fatalf("子协议 = %q", hdr["sec-websocket-protocol"])
	}
	ws := newWSConn(nc, br, true)
	_ = ws.SetDeadline(time.Time{})
	return status, hdr, ws
}

func readHTTPHead(br *bufio.Reader) (int, map[string]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return 0, nil, err
	}
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) < 2 {
		return 0, nil, fmt.Errorf("状态行 %q", line)
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, nil, err
	}
	hdr := map[string]string{}
	for {
		line, err = br.ReadString('\n')
		if err != nil {
			return 0, nil, err
		}
		if line == "\r\n" || line == "\n" {
			return code, hdr, nil
		}
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			return 0, nil, fmt.Errorf("头 %q", line)
		}
		hdr[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(val)
	}
}

func wsClient(t *testing.T, ws *wsConn) *testClient {
	t.Helper()
	c := &testClient{t: t, nc: ws, ver: packet.V5}
	t.Cleanup(func() { _ = ws.Close() })
	return c
}

// TestWebSocketFrameIsAStream 锁定帧和 MQTT 报文不是一一对应的。
//
// 第一片必须在 FIN 到来之前就能读到。若实现把一帧当成一条完整消息
// 攒齐再交，半个 CONNECT 会一直堵到超时。
func TestWebSocketFrameIsAStream(t *testing.T) {
	a, b := net.Pipe()
	srv := newWSConn(a, nil, false)
	cli := newWSConn(b, nil, true)
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 8)
		_ = srv.SetReadDeadline(time.Now().Add(time.Second))
		n, err := srv.Read(buf)
		if err != nil {
			done <- "err:" + err.Error()
			return
		}
		done <- string(buf[:n])
	}()
	if err := cli.writeFrame(wsBinary, []byte("hel"), false); err != nil {
		t.Fatal(err)
	}
	if got := <-done; got != "hel" {
		t.Fatalf("第一片 = %q，分片的前半段应立刻可读", got)
	}
	// net.Pipe 没有缓冲，写出必须和读出在不同的协程上。
	errCh := make(chan error, 1)
	go func() {
		errCh <- cli.writeFrame(wsCont, []byte("lo"), true)
	}()
	_ = srv.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 8)
	n, err := srv.Read(buf)
	if werr := <-errCh; werr != nil {
		t.Fatal(werr)
	}
	if err != nil || string(buf[:n]) != "lo" {
		t.Fatalf("续帧 = %q %v", buf[:n], err)
	}
}

func TestWebSocketPingAndRejects(t *testing.T) {
	a, b := net.Pipe()
	srv := newWSConn(a, nil, false)
	cli := newWSConn(b, nil, true)
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	go func() {
		buf := make([]byte, 4)
		_, _ = srv.Read(buf)
	}()
	if err := cli.writeFrame(wsPing, []byte("xyz"), true); err != nil {
		t.Fatal(err)
	}
	_ = cli.SetReadDeadline(time.Now().Add(time.Second))
	_, op, payload, err := cli.readFrame()
	if err != nil || op != wsPong || string(payload) != "xyz" {
		t.Fatalf("pong = %d %q %v", op, payload, err)
	}

	raw, peer := net.Pipe()
	bad := newWSConn(raw, nil, false)
	t.Cleanup(func() {
		_ = raw.Close()
		_ = peer.Close()
	})
	go func() {
		// 未掩码的二进制帧。客户端发给服务端必须带掩码。
		_, _ = peer.Write([]byte{0x82, 0x02, 'h', 'i'})
	}()
	buf := make([]byte, 4)
	_ = bad.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bad.Read(buf); err == nil {
		t.Fatal("未掩码的客户端帧应被拒绝")
	}

	a2, b2 := net.Pipe()
	srv2 := newWSConn(a2, nil, false)
	cli2 := newWSConn(b2, nil, true)
	t.Cleanup(func() {
		_ = a2.Close()
		_ = b2.Close()
	})
	go func() {
		_ = cli2.writeFrame(wsText, []byte("nope"), true)
	}()
	_ = srv2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := srv2.Read(buf); err == nil {
		t.Fatal("文本帧不能当成 MQTT 字节流")
	}
}

// TestWebSocketHandshakeRejects 锁定升级条件。
//
// 签名错、路径错、子协议错、服务名错、漏签的 x-amz 头，任何一条被拿掉，
// 对应的状态码就会变成 101。
func TestWebSocketHandshakeRejects(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln := r.startWSS(WebSocketOptions{})
	host := ln.Addr().String()
	q := signedQuery(t, host, "", "")

	bad := q[:len(q)-1] + flipHex(q[len(q)-1])
	if status, _, _ := dialWSS(t, host, wsRequest{query: bad}); status != 403 {
		t.Fatalf("坏签名状态 = %d，期望 403", status)
	}
	if status, _, _ := dialWSS(t, host, wsRequest{path: "/other", query: q}); status != 404 {
		t.Fatalf("错误路径状态 = %d，期望 404", status)
	}
	if status, _, _ := dialWSS(t, host, wsRequest{query: q, protocol: "omit"}); status != 400 {
		t.Fatalf("缺少子协议状态 = %d，期望 400", status)
	}
	if status, _, _ := dialWSS(t, host, wsRequest{query: q, protocol: "chat"}); status != 400 {
		t.Fatalf("错误子协议状态 = %d，期望 400", status)
	}
	if status, _, ws := dialWSS(t, host, wsRequest{
		query:  q,
		header: map[string]string{"X-Amz-Security-Token": "tok"},
	}); status != 403 || ws != nil {
		t.Fatalf("未签名的 x-amz 头状态 = %d", status)
	}

	both := testChecker(nil)
	both.Services = map[string]bool{
		sigv4.ServiceDeviceGateway: true,
		sigv4.ServiceIoT:           true,
	}
	ln2 := r.startWSS(WebSocketOptions{Checker: both})
	host2 := ln2.Addr().String()
	iot := signedQuery(t, host2, "", sigv4.ServiceIoT)
	if status, _, _ := dialWSS(t, host2, wsRequest{query: iot}); status != 403 {
		t.Fatalf("服务名 iot 状态 = %d，WebSocket 只接受 iotdevicegateway", status)
	}
}

func flipHex(c byte) string {
	if c == '0' {
		return "1"
	}
	return "0"
}

// TestWebSocketPublish 走通握手之后的 MQTT。
//
// 一帧里的两个 PUBLISH 都要送到。只消费帧里的第一个报文会丢掉后一个。
func TestWebSocketPublish(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln := r.startWSS(WebSocketOptions{})
	host := ln.Addr().String()

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("sensors/temp", packet.QoS0); rc != reason.Success {
		t.Fatalf("订阅 = %s", rc)
	}

	status, _, ws := dialWSS(t, host, wsRequest{query: signedQuery(t, host, "", "")})
	if status != 101 || ws == nil {
		t.Fatalf("握手状态 = %d", status)
	}
	pub := wsClient(t, ws)
	if ack := pub.connect("pub", true); ack.Code != reason.Success {
		t.Fatalf("CONNACK = %s", ack.Code)
	}
	if rc := pub.publish("sensors/temp", "one", packet.QoS0, false); rc != reason.Success {
		t.Fatal(rc)
	}
	if got := sub.expectPublish(); string(got.Payload) != "one" {
		t.Fatalf("负载 = %q", got.Payload)
	}

	p1 := &packet.Publish{Topic: []byte("sensors/temp"), Payload: []byte("two")}
	p2 := &packet.Publish{Topic: []byte("sensors/temp"), Payload: []byte("three")}
	b1, err := p1.EncodeTo(nil, packet.V5)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := p2.EncodeTo(nil, packet.V5)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.writeFrame(wsBinary, append(b1, b2...), true); err != nil {
		t.Fatal(err)
	}
	if got := sub.expectPublish(); string(got.Payload) != "two" {
		t.Fatalf("第一帧内第一条 = %q", got.Payload)
	}
	if got := sub.expectPublish(); string(got.Payload) != "three" {
		t.Fatalf("同一帧里的第二条 = %q", got.Payload)
	}
}

// TestWebSocketConnectSpansFrames 把 CONNECT 拆到两个帧里。
//
// 续帧被丢掉时服务端收不齐 CONNECT，CONNACK 不会出现。
func TestWebSocketConnectSpansFrames(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln := r.startWSS(WebSocketOptions{})
	host := ln.Addr().String()
	status, _, ws := dialWSS(t, host, wsRequest{query: signedQuery(t, host, "", "")})
	if status != 101 {
		t.Fatalf("握手状态 = %d", status)
	}
	t.Cleanup(func() { _ = ws.Close() })
	cp := &packet.Connect{
		Version: packet.V5, ClientID: []byte("split"), CleanStart: true,
	}
	raw, err := cp.EncodeTo(nil, packet.V5)
	if err != nil {
		t.Fatal(err)
	}
	mid := len(raw) / 2
	if err := ws.writeFrame(wsBinary, raw[:mid], false); err != nil {
		t.Fatal(err)
	}
	if err := ws.writeFrame(wsCont, raw[mid:], true); err != nil {
		t.Fatal(err)
	}
	c := &testClient{t: t, nc: ws, ver: packet.V5}
	ack := c.expect(packet.CONNACK).(*packet.Connack)
	if ack.Code != reason.Success {
		t.Fatalf("CONNACK = %s", ack.Code)
	}
}

const policyRecvOnly = `{"Statement":[
  {"Effect":"Allow","Action":["iot:Connect","iot:Subscribe","iot:Receive"],"Resource":"*"}
]}`

// TestWebSocketSigV4UsesThingPolicy 锁定验签通过的 access key 用设备上的策略。
//
// 旁边的认证器只允许订阅。发布能成功，说明走的是设备策略。
// 验签通过但没有绑定的钥匙必须被拒绝，不能落到旁边那个认证器上。
func TestWebSocketSigV4UsesThingPolicy(t *testing.T) {
	auth := TransportIdentity{
		Next: insecureAuth(t, policyRecvOnly),
		Key: func(id string) (string, bool) {
			if id == testAccessKey {
				return "lamp", true
			}
			return "", false
		},
	}
	r := newRig(t, auth, nil)
	ps := r.addPolicy(policyFull)
	if _, err := r.store.Apply(context.Background(), meta.Command{
		Kind:  meta.CmdSetThing,
		Thing: &meta.ThingCmd{Name: "lamp", PolicySet: ps},
	}); err != nil {
		t.Fatal(err)
	}
	secret := func(id string) (string, bool) {
		if id == testAccessKey || id == "ORPHAN" {
			return testSecret, true
		}
		return "", false
	}
	ln := r.startWSS(WebSocketOptions{Checker: testChecker(secret)})
	host := ln.Addr().String()

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("sensors/temp", packet.QoS0); rc != reason.Success {
		t.Fatalf("订阅 = %s", rc)
	}

	status, _, ws := dialWSS(t, host, wsRequest{query: signedQuery(t, host, testAccessKey, "")})
	if status != 101 {
		t.Fatalf("握手状态 = %d", status)
	}
	pub := wsClient(t, ws)
	if ack := pub.connect("lamp", true); ack.Code != reason.Success {
		t.Fatalf("CONNACK = %s", ack.Code)
	}
	if rc := pub.publish("sensors/temp", "from-key", packet.QoS1, false); rc != reason.Success {
		t.Fatalf("发布 = %s，设备策略应允许发布", rc)
	}
	if got := sub.expectPublish(); string(got.Payload) != "from-key" {
		t.Fatalf("负载 = %q", got.Payload)
	}

	status, _, ows := dialWSS(t, host, wsRequest{query: signedQuery(t, host, "ORPHAN", "")})
	if status != 101 {
		t.Fatalf("未绑定钥匙的握手状态 = %d", status)
	}
	orphan := wsClient(t, ows)
	if ack := orphan.connect("x", true); ack.Code != reason.NotAuthorized {
		t.Fatalf("未绑定钥匙的 CONNACK = %s", ack.Code)
	}
}

func TestTransportIdentityDelegatesOnlyWithoutKey(t *testing.T) {
	next := &stubAuth{}
	auth := TransportIdentity{Next: next, Key: func(string) (string, bool) { return "", false }}
	if _, rc := auth.Authenticate(nil, ConnInfo{}, "c", nil, nil); rc != reason.Success || !next.called {
		t.Fatalf("没有 access key 时应交给下一个认证器，rc=%s called=%v", rc, next.called)
	}
	if _, rc := auth.Authenticate(nil, ConnInfo{AccessKeyID: "x"}, "c", nil, nil); rc != reason.NotAuthorized || next.calls != 1 {
		t.Fatalf("有钥匙但绑不上设备时不应再问下一个认证器，rc=%s calls=%d", rc, next.calls)
	}
}

type stubAuth struct {
	called bool
	calls  int
}

func (s *stubAuth) Authenticate(*meta.View, ConnInfo, string, []byte, []byte) (Principal, reason.Code) {
	s.called = true
	s.calls++
	return Principal{}, reason.Success
}

// TestWebSocketLifetime 锁定 时长上限走注入的时钟。
//
// 没到点连接还在；到点就断。配了 redirect 才发 0x9C 和 Server Reference，
// 没配就只关连接。
func TestWebSocketLifetime(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln := r.startWSS(WebSocketOptions{MaxLifetime: time.Hour})
	host := ln.Addr().String()
	status, _, ws := dialWSS(t, host, wsRequest{query: signedQuery(t, host, "", "")})
	if status != 101 {
		t.Fatalf("握手状态 = %d", status)
	}
	c := wsClient(t, ws)
	if ack := c.connect("life", true); ack.Code != reason.Success {
		t.Fatal(ack.Code)
	}
	if rc := c.subscribe("sensors/temp", packet.QoS0); rc != reason.Success {
		t.Fatal(rc)
	}
	r.advance(30 * time.Minute)
	if rc := c.publish("sensors/temp", "still", packet.QoS0, false); rc != reason.Success {
		t.Fatal(rc)
	}
	if got := c.expectPublish(); string(got.Payload) != "still" {
		t.Fatalf("未到期却收不到自己的消息：%q", got.Payload)
	}
	r.advance(30 * time.Minute)
	if rc := c.expectClosed(2 * time.Second); rc != 0 {
		t.Fatalf("没有 redirect 目标时不应带原因码，得到 %s", rc)
	}
}

func TestWebSocketLifetimeRedirects(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	ln := r.startWSS(WebSocketOptions{
		MaxLifetime: time.Hour,
		RedirectTo:  "backup.example",
	})
	host := ln.Addr().String()
	status, _, ws := dialWSS(t, host, wsRequest{query: signedQuery(t, host, "", "")})
	if status != 101 {
		t.Fatalf("握手状态 = %d", status)
	}
	c := wsClient(t, ws)
	if ack := c.connect("move", true); ack.Code != reason.Success {
		t.Fatal(ack.Code)
	}
	r.advance(time.Hour)
	p := c.expect(packet.DISCONNECT).(*packet.Disconnect)
	if p.Code != reason.UseAnotherServer {
		t.Fatalf("原因码 = %s", p.Code)
	}
	if string(p.Props.ServerReference) != "backup.example" {
		t.Fatalf("Server Reference = %q", p.Props.ServerReference)
	}
}
