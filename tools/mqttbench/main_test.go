package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

var clientBufferBenchSink any

func TestWriteConnectProgress(t *testing.T) {
	var out bytes.Buffer
	writeConnectProgress(&out, "measured", 10, 7, 5, 4, 1, 2*time.Second)
	want := "connect-progress wave=measured total=10 started=7 completed=5 held=4 " +
		"errors=1 in_flight=2 rate=2.5/s elapsed=2s\n"
	if out.String() != want {
		t.Fatalf("progress = %q, want %q", out.String(), want)
	}
}

func TestClientUsesInlineReceiveBuffer(t *testing.T) {
	cl := newClient(nil, packet.V311)
	if len(cl.rbuf) != len(cl.rsmall) || &cl.rbuf[0] != &cl.rsmall[0] {
		t.Fatalf("接收缓冲未内联：len=%d inline=%d", len(cl.rbuf), len(cl.rsmall))
	}
	if cl.out != nil {
		t.Fatal("空闲客户端不应持有写缓冲")
	}
}

func TestProxyV2HeaderIPv4AndIPv6(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source *net.TCPAddr
		dest   *net.TCPAddr
		family byte
		addrN  int
	}{
		{"ipv4", &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234},
			&net.TCPAddr{IP: net.ParseIP("198.51.100.2"), Port: 1883}, 0x11, 12},
		{"ipv6", &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1234},
			&net.TCPAddr{IP: net.ParseIP("2001:db8::2"), Port: 1883}, 0x21, 36},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := []byte{1, 2, 3, 4}
			h, err := proxyV2Header(tc.source, tc.dest, cert, false)
			if err != nil {
				t.Fatal(err)
			}
			if string(h[:12]) != "\r\n\r\n\x00\r\nQUIT\n" || h[12] != 0x21 || h[13] != tc.family {
				t.Fatalf("fixed header = %x", h[:16])
			}
			if got := int(binary.BigEndian.Uint16(h[14:16])); got != len(h)-16 {
				t.Fatalf("body length = %d, want %d", got, len(h)-16)
			}
			tlv := h[16+tc.addrN:]
			if tlv[0] != 0xe0 || tlv[3] != 1 || binary.BigEndian.Uint16(tlv[4:6]) != tls.VersionTLS13 {
				t.Fatalf("identity TLV = %x", tlv)
			}
			if got := tlv[len(tlv)-len(cert):]; !bytes.Equal(got, cert) {
				t.Fatalf("cert DER = %x", got)
			}
		})
	}
}

func TestClientRecvHandlesFragmentationGrowthAndStickyPackets(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	cl := newClient(server, packet.V311)
	payload := bytes.Repeat([]byte("x"), 4096)
	first, err := packet.Encode(&packet.Publish{
		Topic: []byte("large/topic"), Payload: payload, QoS: packet.QoS0,
	}, packet.V311)
	if err != nil {
		t.Fatal(err)
	}
	second, err := packet.Encode(&packet.Pingresp{}, packet.V311)
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		all := append(append([]byte(nil), first...), second...)
		for len(all) > 0 {
			n := 7
			if n > len(all) {
				n = len(all)
			}
			if _, err := peer.Write(all[:n]); err != nil {
				written <- err
				return
			}
			all = all[n:]
		}
		written <- nil
	}()
	p, err := cl.recv()
	if err != nil {
		t.Fatal(err)
	}
	pub, ok := p.(*packet.Publish)
	if !ok || string(pub.Topic) != "large/topic" || !bytes.Equal(pub.Payload, payload) {
		t.Fatalf("大报文解码错误：%T topic=%q payload=%d", p, pub.Topic, len(pub.Payload))
	}
	p, err = cl.recv()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*packet.Pingresp); !ok {
		t.Fatalf("粘连的第二条报文=%T，期望 PINGRESP", p)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if len(cl.rbuf) < len(first) {
		t.Fatalf("接收缓冲未按需增长：%d < %d", len(cl.rbuf), len(first))
	}
}

func TestClientFlushWritesAllAndReleasesBuffer(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	cl := newClient(server, packet.V311)
	want, err := packet.Encode(&packet.Pingreq{}, packet.V311)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	read := make(chan error, 1)
	go func() { _, err := io.ReadFull(peer, got); read <- err }()
	if err := cl.send(&packet.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	if err := cl.flush(); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("写出=%x，期望 %x", got, want)
	}
	if cl.out != nil {
		t.Fatalf("flush 后仍保留 %d 字节写缓冲", cap(cl.out))
	}
}

func TestShadowLoopAcknowledgesQoS1Response(t *testing.T) {
	clientSide, brokerSide := net.Pipe()
	defer clientSide.Close()
	defer brokerSide.Close()
	cl := newClient(clientSide, packet.V311)
	broker := newClient(brokerSide, packet.V311)
	st := newStats()
	st.reset() // generation 1 is measured; generation 0 is warm-up.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		cl.shadowLoop(&runner{shadowResponseTimeout: time.Second},
			"$aws/things/d/shadow/update", st, stop)
		close(done)
	}()

	req, err := broker.recv()
	if err != nil {
		t.Fatal(err)
	}
	pub, ok := req.(*packet.Publish)
	if !ok || !bytes.Contains(pub.Payload, []byte(`"clientToken":"1"`)) {
		t.Fatalf("首条影子请求=%T %#v", req, req)
	}
	// 先关 stop，再发响应。
	//
	// 反过来写是有竞态的：shadowLoop 处理完响应就跳出内层循环并回到外层的
	// select，而那时 close(stop) 可能还没发生——于是它发出第 2 条请求、阻塞
	// 一个响应期限、记一次超时，下面三条断言（停止、published=1、零错误）
	// 全都落空。这一轮在 make gate 的全量 -race 下真的红过一次，而单独跑
	// 三次都是绿的。
	//
	// 这个顺序是确定的：此刻 shadowLoop 正阻塞在 recv() 上，关闭 stop 不会
	// 打断它；等它确认完响应跳出内层循环时，外层 select 必然已经可见。
	close(stop)
	if err := broker.send(&packet.Publish{
		Topic:   []byte("$aws/things/d/shadow/update/accepted"),
		Payload: []byte(`{"version":1,"clientToken":"1"}`),
		QoS:     packet.QoS1, PacketID: 77,
	}); err != nil {
		t.Fatal(err)
	}
	if err := broker.flush(); err != nil {
		t.Fatal(err)
	}
	ackPacket, err := broker.recv()
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := ackPacket.(*packet.Ack)
	if !ok || ack.Kind != packet.PUBACK || ack.PacketID != 77 {
		t.Fatalf("Shadow QoS1确认=%T %#v", ackPacket, ackPacket)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shadowLoop确认响应后没有停止")
	}
	pubN, delivered, _, errs := st.snapshot()
	if pubN != 1 || delivered != 1 || errs != 0 {
		t.Fatalf("统计 published=%d delivered=%d errors=%d", pubN, delivered, errs)
	}
}

func TestVerifyHeldConnectionsUsesApplicationPing(t *testing.T) {
	const good = 8
	r := &runner{conc: 4}
	conns := make([]*client, good+1)
	serverErr := make(chan error, good)
	for i := 0; i < good; i++ {
		clientSide, serverSide := net.Pipe()
		conns[i] = newClient(clientSide, packet.V311)
		go func() {
			defer serverSide.Close()
			var req [2]byte
			if _, err := io.ReadFull(serverSide, req[:]); err != nil {
				serverErr <- err
				return
			}
			if req != [2]byte{byte(packet.PINGREQ) << 4, 0} {
				serverErr <- fmt.Errorf("收到的PINGREQ=%x", req)
				return
			}
			_, err := serverSide.Write([]byte{byte(packet.PINGRESP) << 4, 0})
			serverErr <- err
		}()
	}
	deadClient, deadServer := net.Pipe()
	conns[good] = newClient(deadClient, packet.V311)
	_ = deadServer.Close()

	verified, failed, first := r.verifyHeldConnections(conns)
	if verified != good || failed != 1 || first == nil {
		t.Fatalf("verified=%d failed=%d first=%v", verified, failed, first)
	}
	if conns[good] != nil {
		t.Fatal("保持验证失败的连接没有关闭并清出held集合")
	}
	for i := 0; i < good; i++ {
		if err := <-serverErr; err != nil {
			t.Fatalf("PING服务端%d: %v", i, err)
		}
		conns[i].close()
	}
}

func BenchmarkClientIdleBuffers(b *testing.B) {
	b.Run("legacy-16k-reader-writer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clientBufferBenchSink = struct {
				r *bufio.Reader
				w *bufio.Writer
			}{bufio.NewReaderSize(nil, 16<<10), bufio.NewWriterSize(nil, 16<<10)}
		}
	})
	b.Run("inline-256-reader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clientBufferBenchSink = newClient(nil, packet.V311)
		}
	})
}

func BenchmarkTLSSessionCache100k(b *testing.B) {
	ids := make([]string, 100000)
	for i := range ids {
		ids[i] = fmt.Sprintf("tls-bench-%06d", i)
	}
	state := &tls.ClientSessionState{}
	b.Run("legacy-per-device-lru", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			var caches sync.Map
			for _, id := range ids {
				cache, _ := caches.LoadOrStore(id, tls.NewLRUClientSessionCache(1))
				cache.(tls.ClientSessionCache).Put("mqtt.example", state)
			}
			clientBufferBenchSink = &caches
		}
	})
	b.Run("sharded-state-table", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			var caches deviceTLSSessionCache
			for _, id := range ids {
				caches.forDevice(id).Put("mqtt.example", state)
			}
			clientBufferBenchSink = &caches
		}
	})
}

func BenchmarkTLSConfigForColdDial(b *testing.B) {
	base := &tls.Config{
		ServerName: "mqtt.example", MinVersion: tls.VersionTLS12,
		NextProtos: []string{"x-amzn-mqtt-ca"},
	}
	r := &runner{tlsConfig: base}
	b.Run("legacy-clone-per-connection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clientBufferBenchSink = base.Clone()
		}
	})
	b.Run("shared-immutable-config", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clientBufferBenchSink = r.tlsConfigFor("127.0.0.1:8883", "device")
		}
	})
}

func BenchmarkDialerForColdDial(b *testing.B) {
	ip := net.ParseIP("127.0.0.2")
	r := &runner{sourceIPs: []net.IP{ip}}
	r.initDialers()
	b.Run("legacy-new-per-connection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			d := &net.Dialer{Timeout: 10 * time.Second, LocalAddr: &net.TCPAddr{IP: ip}}
			configureSourcePortReuse(d, ip)
			clientBufferBenchSink = d
		}
	})
	b.Run("shared-source-dialer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clientBufferBenchSink = r.dialerFor("device")
		}
	})
}

// 这份用例只盯住压测器里「算错了也不会有任何现象」的那几处。
//
// 其余部分（建连、收发、预置）错了立刻就显形：一轮跑不起来，或者收到 0 条。
// 而直方图算错不会——它照样印出一个像样的 p99，而那个数会被抄进 README。

// TestHistogramPercentiles 用已知分布核对分位数。
//
// 判据是相对误差而不是相等：桶是 1.05 的等比数列，所以每个读数本来就带
// 最多 5% 的截断误差。要求相等的话这条用例只能通过把实现改成存全部样本
// 才过，而那正是刻意不做的事（一轮几百万个样本）。
func TestHistogramPercentiles(t *testing.T) {
	s := newStats()
	// 1 到 1000 毫秒各一个样本：p50 应在 500 附近，p99 在 990 附近。
	for i := 1; i <= 1000; i++ {
		s.observe(float64(i))
	}
	p50, p99, pmax := s.percentiles()
	if rel(p50, 500) > 0.05 {
		t.Fatalf("p50 = %.3f ms，期望 500 ms 上下 5%%", p50)
	}
	if rel(p99, 990) > 0.05 {
		t.Fatalf("p99 = %.3f ms，期望 990 ms 上下 5%%", p99)
	}
	// 最大值是单独记的真实值，不走桶，所以它必须精确。
	if pmax != 1000 {
		t.Fatalf("max = %.3f ms，期望 1000 ms（最大值不经过桶）", pmax)
	}
}

func TestSourceIPForIsStableAndDistributed(t *testing.T) {
	r := &runner{sourceIPs: []net.IP{
		net.ParseIP("127.0.0.2"), net.ParseIP("127.0.0.3"), net.ParseIP("127.0.0.4"),
	}}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("device-%d", i)
		a, b := r.sourceIPFor(id), r.sourceIPFor(id)
		if !a.Equal(b) {
			t.Fatalf("同一 clientId 的源 IP 不稳定: %v / %v", a, b)
		}
		seen[a.String()] = true
	}
	if len(seen) != len(r.sourceIPs) {
		t.Fatalf("100 个设备只分布到 %d/%d 个源 IP: %v", len(seen), len(r.sourceIPs), seen)
	}
}

func TestDialerForReusesStableSourceConfiguration(t *testing.T) {
	r := &runner{sourceIPs: []net.IP{
		net.ParseIP("127.0.0.2"), net.ParseIP("127.0.0.3"), net.ParseIP("127.0.0.4"),
	}}
	r.initDialers()
	if len(r.sourceDialers) != len(r.sourceIPs) {
		t.Fatalf("源dialer数=%d，期望%d", len(r.sourceDialers), len(r.sourceIPs))
	}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("device-%d", i)
		first, second := r.dialerFor(id), r.dialerFor(id)
		if first != second {
			t.Fatalf("同一clientId没有复用dialer: %p / %p", first, second)
		}
		addr, ok := first.LocalAddr.(*net.TCPAddr)
		if !ok || !addr.IP.Equal(r.sourceIPFor(id)) {
			t.Fatalf("clientId=%s dialer源地址=%v，期望%v", id, first.LocalAddr, r.sourceIPFor(id))
		}
		if first.Timeout != 10*time.Second {
			t.Fatalf("dialer timeout=%s", first.Timeout)
		}
	}

	plain := &runner{}
	plain.initDialers()
	if plain.dialerFor("a") != plain.dialerFor("b") || plain.dialer.Timeout != 10*time.Second {
		t.Fatal("无显式源IP时没有复用默认dialer")
	}
}

func TestAdminHTTPClientUsesDedicatedSourceIP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	remote := make(chan net.IP, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			remote <- nil
			return
		}
		remote <- conn.RemoteAddr().(*net.TCPAddr).IP
		_, _ = io.Copy(io.Discard, conn)
		_ = conn.Close()
	}()
	// 127.0.0.1 在所有测试平台都已配置；生产可传另一条本地地址，把管理面
	// 与设备临时端口池分开。这里验证 Dialer 确实使用所给地址。
	want := net.ParseIP("127.0.0.1")
	client := newAdminHTTPClient(want)
	defer client.CloseIdleConnections()
	req, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Accept 已经足以证明 Dialer 绑定的源地址；服务端不必实现 HTTP。
	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	_, _ = client.Do(req)
	select {
	case got := <-remote:
		if !got.Equal(want) {
			t.Fatalf("管理HTTP源IP = %v，期望 %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("管理HTTP未连接测试listener")
	}
}

// TestHistogramSubMillisecond 盯住亚毫秒那一段。
//
// 单节点的 p50 是 0.1 毫秒量级，而第一版如果把桶的基准取成毫秒，这一段会
// 全部落进 0 号桶、印出 0.00 ms——一个"快到无法测量"的假结论。桶以微秒
// 为基准正是为了这个。
func TestHistogramSubMillisecond(t *testing.T) {
	s := newStats()
	for i := 0; i < 100; i++ {
		s.observe(0.1)
	}
	p50, _, _ := s.percentiles()
	if rel(p50, 0.1) > 0.05 {
		t.Fatalf("p50 = %.5f ms，期望 0.1 ms 上下 5%%（亚毫秒不得塌成 0）", p50)
	}
}

// TestStatsResetClearsLatency 预热之后的清零必须连直方图一起清。
//
// 不清的话，建连与首轮订阅留下的那批高延迟样本会留在桶里，于是报告里的
// p99 实际上在描述建连阶段——而它看起来完全正常。
func TestStatsResetClearsLatency(t *testing.T) {
	s := newStats()
	s.observe(900)
	s.published.Add(7)
	s.reset()
	if p, d, a, e := s.snapshot(); p != 0 || d != 0 || a != 0 || e != 0 {
		t.Fatalf("计数未清零: %d %d %d %d", p, d, a, e)
	}
	p50, p99, pmax := s.percentiles()
	if p50 != 0 || p99 != 0 || pmax != 0 {
		t.Fatalf("直方图未清零: p50=%v p99=%v max=%v", p50, p99, pmax)
	}
}

// TestShadowStatsExcludeWarmupInFlight pins the boundary where a request is
// sent during warm-up but its accepted reply arrives after reset. It must not
// produce delivered > published in the measured window.
func TestShadowStatsExcludeWarmupInFlight(t *testing.T) {
	s := newStats()
	warmupGen := s.shadowGeneration()
	s.shadowPublished(warmupGen)
	s.shadowFailed(warmupGen)

	s.reset()
	s.shadowDelivered(warmupGen, 900)
	measureGen := s.shadowGeneration()
	s.shadowPublished(measureGen)
	s.shadowDelivered(measureGen, 12)
	s.shadowPublished(measureGen)
	s.shadowDelivered(measureGen, 12)
	s.shadowFailed(measureGen)

	pub, del, _, errs := s.snapshot()
	if pub != 2 || del != 2 || errs != 1 {
		t.Fatalf("测量窗口计数 = published %d delivered %d errors %d，期望 2/2/1", pub, del, errs)
	}
	p50, _, pmax := s.percentiles()
	if p50 == 0 || pmax != 12 {
		t.Fatalf("测量窗口延迟 = p50 %.3f max %.3f，预热样本不应进入直方图", p50, pmax)
	}
}

func TestShadowFailureStatsAreGenerationScoped(t *testing.T) {
	s := newStats()
	warmup := s.shadowGeneration()
	s.reset()
	measured := s.shadowGeneration()
	s.shadowRejectedReply(warmup)
	s.shadowTimedOut(warmup)
	s.shadowRejectedReply(measured)
	s.shadowTimedOut(measured)
	rejected, timeouts, _, _ := s.shadowFailures()
	_, _, _, errs := s.snapshot()
	if rejected != 1 || timeouts != 1 || errs != 2 {
		t.Fatalf("rejected=%d timeouts=%d errors=%d，期望1/1/2", rejected, timeouts, errs)
	}
}

func TestShadowNamesCanTargetOneShard(t *testing.T) {
	r := &runner{prefix: "failover", shadowShards: 4, shadowShard: 3}
	names := r.shadowNames("h", 1000)
	if len(names) != 1000 {
		t.Fatalf("设备数=%d，期望1000", len(names))
	}
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			t.Fatalf("重复设备名 %s", name)
		}
		seen[name] = true
		var h uint64 = 14695981039346656037
		for i := 0; i < len(name); i++ {
			h ^= uint64(name[i])
			h *= 1099511628211
		}
		if got := 2 + int(h%4); got != 3 {
			t.Fatalf("%s落到分片%d，期望3", name, got)
		}
	}
}

func TestSplitAddrs(t *testing.T) {
	got := splitAddrs(" a:1 , b:2 ,, ")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("splitAddrs = %q", got)
	}
	if n := len(splitAddrs("  ")); n != 0 {
		t.Fatalf("空输入应得到空列表，得到 %d 项", n)
	}
}

// TestNamesAreUnique 设备名必须逐个不同：clientId 等于设备名，撞名的表现
// 是两条连接互相挤掉，而计数看起来只是"连接数上不去"。
func TestNamesAreUnique(t *testing.T) {
	r := &runner{prefix: "bench"}
	seen := map[string]bool{}
	for _, n := range r.names("p", 1000) {
		if seen[n] {
			t.Fatalf("设备名重复: %s", n)
		}
		seen[n] = true
	}
}

func TestNamesHonorClientOffset(t *testing.T) {
	r := &runner{prefix: "density", clientOffset: 800_000}
	got := r.names("c", 3)
	want := []string{"density-c800000", "density-c800001", "density-c800002"}
	if !slices.Equal(got, want) {
		t.Fatalf("names with offset = %v, want %v", got, want)
	}
}

func TestConnectHoldDurationUsesSharedDeadline(t *testing.T) {
	now := time.Unix(1_000, 250_000_000)
	r := &runner{holdUntilUnix: 1_010}
	got, err := r.connectHoldDuration(now, time.Hour)
	if err != nil || got != 9*time.Second+750*time.Millisecond {
		t.Fatalf("shared hold = %s, err=%v", got, err)
	}
	r.holdUntilUnix = 1_000
	if _, err := r.connectHoldDuration(now, time.Second); err == nil {
		t.Fatal("已错过统一终点仍然接受本片")
	}
	r.holdUntilUnix = 0
	if got, err := r.connectHoldDuration(now, 3*time.Second); err != nil || got != 3*time.Second {
		t.Fatalf("普通 hold = %s, err=%v", got, err)
	}
}

func TestRecordConnectFailuresKeepsFirstConcreteError(t *testing.T) {
	res := &result{}
	recordConnectFailures(res, 1, io.ErrUnexpectedEOF)
	if res.Errors != 1 || res.FirstError != io.ErrUnexpectedEOF.Error() {
		t.Fatalf("errors=%d firstError=%q", res.Errors, res.FirstError)
	}
}

func TestTLSSessionCacheIsPerDevice(t *testing.T) {
	r := &runner{}
	a1 := r.tlsSessionCache("device-a")
	a2 := r.tlsSessionCache("device-a")
	b := r.tlsSessionCache("device-b")
	stateA := &tls.ClientSessionState{}
	a1.Put("mqtt.example", stateA)
	if got, ok := a2.Get("mqtt.example"); !ok || got != stateA {
		t.Fatal("同一设备没有读回自己的 TLS session state")
	}
	if _, ok := b.Get("mqtt.example"); ok {
		t.Fatal("不同设备共享了 TLS session state，会把单设备 ticket 当成设备群恢复")
	}
	stateOther := &tls.ClientSessionState{}
	a1.Put("other.example", stateOther)
	if got, ok := a2.Get("other.example"); !ok || got != stateOther {
		t.Fatal("同一设备的不同服务端 key 未隔离保存")
	}
	a2.Put("mqtt.example", nil)
	if _, ok := a1.Get("mqtt.example"); ok {
		t.Fatal("nil Put 没有删除 TLS session state")
	}
}

func TestTLSConfigColdDialReusesImmutableConfig(t *testing.T) {
	base := &tls.Config{ServerName: "mqtt.example", MinVersion: tls.VersionTLS12}
	r := &runner{tlsConfig: base}
	if got := r.tlsConfigFor("127.0.0.1:8883", "device-1"); got != base {
		t.Fatal("固定ServerName的冷连克隆了只读TLS配置")
	}
}

func TestTLSConfigPerDialStateNeverMutatesBase(t *testing.T) {
	base := &tls.Config{MinVersion: tls.VersionTLS12}
	r := &runner{tlsConfig: base}
	first := r.tlsConfigFor("broker-a.example:8883", "device-1")
	second := r.tlsConfigFor("broker-b.example:8883", "device-2")
	if first == base || second == base || first == second {
		t.Fatal("按目标补ServerName的拨号配置没有隔离")
	}
	if first.ServerName != "broker-a.example" || second.ServerName != "broker-b.example" {
		t.Fatalf("拨号ServerName错误: %q, %q", first.ServerName, second.ServerName)
	}
	if base.ServerName != "" || base.ClientSessionCache != nil {
		t.Fatal("拨号状态污染了共享TLS基础配置")
	}

	r.reconnect = true
	resumed := r.tlsConfigFor("broker-a.example:8883", "device-1")
	if resumed == base || resumed.ClientSessionCache == nil {
		t.Fatal("重连没有使用隔离的每设备ticket cache")
	}
	if base.ClientSessionCache != nil {
		t.Fatal("重连ticket cache污染了共享TLS基础配置")
	}
}

func TestTLSSessionCacheConcurrentIsolation(t *testing.T) {
	r := &runner{}
	const devices = 1024
	var wg sync.WaitGroup
	for i := 0; i < devices; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("tls-device-%04d", i)
			state := &tls.ClientSessionState{}
			cache := r.tlsSessionCache(id)
			cache.Put("mqtt.example", state)
			if got, ok := cache.Get("mqtt.example"); !ok || got != state {
				t.Errorf("设备 %s 的 session state 串线", id)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < devices; i++ {
		id := fmt.Sprintf("tls-device-%04d", i)
		if _, ok := r.tlsSessionCache(id).Get("mqtt.example"); !ok {
			t.Fatalf("设备 %s 的 session state 丢失", id)
		}
	}
}

func TestGaugesReadsOneScrapeAndAllowsMissingRSS(t *testing.T) {
	want := map[string]struct{}{
		"openiot_heap_alloc_bytes": {}, "openiot_runtime_sys_bytes": {},
		"openiot_stack_inuse_bytes": {}, "openiot_process_resident_memory_bytes": {},
	}
	got, err := parseGauges(strings.NewReader(
		"openiot_heap_alloc_bytes{node=\"n\"} 100\n"+
			"openiot_runtime_sys_bytes{node=\"n\"} 200\n"+
			"openiot_stack_inuse_bytes{node=\"n\"} 300\n"), want)
	if err != nil {
		t.Fatal(err)
	}
	if got["openiot_stack_inuse_bytes"] != 300 {
		t.Fatalf("got=%v", got)
	}
	if _, ok := got["openiot_process_resident_memory_bytes"]; ok {
		t.Fatal("非 Linux 可缺失的 RSS 不应被伪造为 0")
	}
}

func rel(got, want float64) float64 { return math.Abs(got-want) / want }

// TestDrainIgnoresWiringProbe 钉住「探针不进测量样本」。
//
// 探针负载不是时间戳。drain 若按时间戳解它，会得到一个以 1970 年为起点的
// 延迟——单独一条就足以把 p99 与最大值拉到五十多年，而结果表上它只是一个
// 离群点，不会有任何地方报错。
func TestDrainIgnoresWiringProbe(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	cl := newClient(server, packet.V311)
	st := newStats()
	stop := make(chan struct{})

	probe, err := packet.Encode(&packet.Publish{
		Topic: []byte("t"), Payload: probeMarker, QoS: packet.QoS0,
	}, packet.V311)
	if err != nil {
		t.Fatal(err)
	}
	real := make([]byte, 64)
	binary.BigEndian.PutUint64(real[:8], uint64(time.Now().UnixNano()))
	msg, err := packet.Encode(&packet.Publish{
		Topic: []byte("t"), Payload: real, QoS: packet.QoS0,
	}, packet.V311)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { defer close(done); cl.drain(st, stop) }()
	if _, err := peer.Write(append(append([]byte(nil), probe...), msg...)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, del, _, _ := st.snapshot(); del > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	_ = server.Close()
	<-done

	_, del, _, _ := st.snapshot()
	if del != 1 {
		t.Fatalf("投递计数=%d，期望只算那条真实消息", del)
	}
	_, p99, pmax := st.percentiles()
	if pmax > 60_000 {
		t.Fatalf("最大延迟=%.0f ms，探针被当成了带时间戳的样本（p99=%.0f）", pmax, p99)
	}
}

// TestMeasureAllowsZeroDeliveryOnlyAfterWiringProbe 钉住过载档的语义。
//
// 「收到 0 条」有两个完全不同的原因：没接通（接线错误），以及刻意过载把
// 消息全丢了。前者必须硬失败，后者是这一档要记录的结果本身。判据是
// WiringVerified——它由加压之前的那次探针置位。
func TestMeasureAllowsZeroDeliveryOnlyAfterWiringProbe(t *testing.T) {
	run := func(verified bool) error {
		r := &runner{warmup: time.Millisecond, duration: 50 * time.Millisecond}
		res := &result{WiringVerified: verified}
		st := newStats()
		stop := make(chan struct{})
		var wg sync.WaitGroup
		// 发布计数必须产生在测量窗口**之内**：measure 在预热之后会
		// reset，窗口之前加的那些一律不算。这个循环模拟一个发得出去、
		// 却一条也投不到的发布者，也就是过载那一档的形状。
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				st.published.Add(1)
				time.Sleep(time.Millisecond)
			}
		}()
		return r.measure(res, st, stop, &wg)
	}
	if err := run(false); err == nil {
		t.Fatal("未做接线判定时，投递 0 条必须报错")
	}
	if err := run(true); err != nil {
		t.Fatalf("已做接线判定时，投递 0 条是合法的过载结果，却报了错: %v", err)
	}
}

// TestMeasureRejectsSilentPublisher 钉住另一半：发布侧自己没干活时仍要报错。
// 否则压测器会把「一条都没发出去」记成一次 100% 丢弃的过载结果。
func TestMeasureRejectsSilentPublisher(t *testing.T) {
	r := &runner{warmup: time.Millisecond, duration: time.Millisecond}
	res := &result{WiringVerified: true}
	var wg sync.WaitGroup
	if err := r.measure(res, newStats(), make(chan struct{}), &wg); err == nil {
		t.Fatal("发布数为 0 时必须报错，它不是过载")
	}
}
