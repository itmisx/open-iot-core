package broker

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func proxyV2Frame(t *testing.T, family byte, source netip.AddrPort, certDER []byte) []byte {
	t.Helper()
	var addresses []byte
	switch family {
	case 0x11:
		addresses = make([]byte, 12)
		a := source.Addr().As4()
		copy(addresses[:4], a[:])
		copy(addresses[4:8], []byte{127, 0, 0, 1})
		binary.BigEndian.PutUint16(addresses[8:10], source.Port())
		binary.BigEndian.PutUint16(addresses[10:12], 1883)
	case 0x21:
		addresses = make([]byte, 36)
		a := source.Addr().As16()
		copy(addresses[:16], a[:])
		addresses[31] = 1
		binary.BigEndian.PutUint16(addresses[32:34], source.Port())
		binary.BigEndian.PutUint16(addresses[34:36], 1883)
	default:
		t.Fatalf("unsupported family %x", family)
	}
	value := []byte{proxyV2FormatVersion, 0x03, 0x04, byte(len(ALPNMQTTCert))}
	value = append(value, ALPNMQTTCert...)
	value = append(value, certDER...)
	tlv := []byte{ProxyV2TLVClientCert, 0, 0}
	binary.BigEndian.PutUint16(tlv[1:3], uint16(len(value)))
	tlv = append(tlv, value...)
	body := append(addresses, tlv...)
	fixed := append([]byte(nil), proxyV2Signature...)
	fixed = append(fixed, 0x21, family, 0, 0)
	binary.BigEndian.PutUint16(fixed[14:16], uint16(len(body)))
	return append(fixed, body...)
}

func proxyV2FingerprintFrame(t *testing.T, family byte, source netip.AddrPort, certDER []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(certDER)
	encoded := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(encoded, sum[:])
	frame := proxyV2Frame(t, family, source, encoded)
	addrLen := 12
	if family == 0x21 {
		addrLen = 36
	}
	frame[16+addrLen] = ProxyV2TLVCertFingerprint
	return frame
}

func readProxyFrame(t *testing.T, frame []byte, peer netip.AddrPort) (ConnInfo, error) {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		_, _ = client.Write(frame)
		_ = client.Close()
		close(done)
	}()
	info, err := readProxyV2Identity(server, peer, ProxyOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		HeaderTimeout:  time.Second,
	})
	_ = server.Close()
	<-done
	return info, err
}

func TestProxyV2IdentityIPv4AndIPv6(t *testing.T) {
	cert := testCert(t)
	wantFP := meta.CertFingerprint(sha256.Sum256(cert.Certificate[0]))
	for _, tc := range []struct {
		name   string
		family byte
		source netip.AddrPort
	}{
		{"ipv4", 0x11, netip.MustParseAddrPort("192.0.2.9:54321")},
		{"ipv6", 0x21, netip.MustParseAddrPort("[2001:db8::9]:54321")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := readProxyFrame(t,
				proxyV2Frame(t, tc.family, tc.source, cert.Certificate[0]),
				netip.MustParseAddrPort("127.0.0.1:40000"))
			if err != nil {
				t.Fatal(err)
			}
			if info.Remote != tc.source || !info.HasCert || info.CertFP != wantFP {
				t.Fatalf("identity = %+v, want remote=%s fp=%s", info, tc.source, wantFP)
			}
			if info.TLSVersion != tls.VersionTLS13 || info.ALPN != ALPNMQTTCert {
				t.Fatalf("TLS facts = version=%x alpn=%q", info.TLSVersion, info.ALPN)
			}
		})
	}
}

func TestProxyV2RejectsUntrustedPeerBeforeReading(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	_, err := readProxyV2Identity(server, netip.MustParseAddrPort("198.51.100.8:9"), ProxyOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		HeaderTimeout:  time.Second,
	})
	if err == nil {
		t.Fatal("untrusted peer should be rejected")
	}
}

func TestProxyV2DoesNotFallBackAndRequiresCertificate(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:40000")
	if _, err := readProxyFrame(t, []byte("\x10\x00"), peer); err == nil {
		t.Fatal("raw MQTT must not fall back when proxy mode is enabled")
	}

	body := make([]byte, 12) // valid IPv4 address block, but no identity TLV
	fixed := append([]byte(nil), proxyV2Signature...)
	fixed = append(fixed, 0x21, 0x11, 0, byte(len(body)))
	if _, err := readProxyFrame(t, append(fixed, body...), peer); err == nil {
		t.Fatal("missing client certificate TLV should be rejected")
	}
}

func TestProxyV2RejectsMalformedTLVs(t *testing.T) {
	bad := [][]byte{
		{ProxyV2TLVClientCert},
		{ProxyV2TLVClientCert, 0, 10, 1},
		{ProxyV2TLVClientCert, 0, 4, 9, 3, 4, 0},
		{ProxyV2TLVClientCert, 0, 4, 1, 3, 1, 0},
		{ProxyV2TLVClientCert, 0, 4, 1, 3, 4, 0},
	}
	for i, raw := range bad {
		if _, err := parseProxyV2TLVs(raw); err == nil {
			t.Errorf("case %d should fail", i)
		}
	}
}

func TestProxyV2FingerprintIdentity(t *testing.T) {
	cert := testCert(t)
	want := meta.CertFingerprint(sha256.Sum256(cert.Certificate[0]))
	info, err := readProxyFrame(t, proxyV2FingerprintFrame(t, 0x11,
		netip.MustParseAddrPort("192.0.2.10:1234"), cert.Certificate[0]),
		netip.MustParseAddrPort("127.0.0.1:40000"))
	if err != nil {
		t.Fatal(err)
	}
	if !info.HasCert || info.CertFP != want || info.CertSerial != "" || info.CertSubject.CommonName != "" {
		t.Fatalf("compact identity = %+v", info)
	}
}

func TestProxyV2CertificateAndFingerprintMustMatch(t *testing.T) {
	cert := testCert(t)
	source := netip.MustParseAddrPort("192.0.2.10:1234")
	derFrame := proxyV2Frame(t, 0x11, source, cert.Certificate[0])
	fpFrame := proxyV2FingerprintFrame(t, 0x11, source, cert.Certificate[0])
	// 固定头 16 B、IPv4 地址块 12 B；把第二条身份 TLV 接到同一 body。
	combined := append(append([]byte(nil), derFrame...), fpFrame[28:]...)
	binary.BigEndian.PutUint16(combined[14:16], uint16(len(combined)-16))
	peer := netip.MustParseAddrPort("127.0.0.1:40000")
	if _, err := readProxyFrame(t, combined, peer); err != nil {
		t.Fatalf("matching identities rejected: %v", err)
	}
	combined[len(combined)-1] ^= 0xff
	if _, err := readProxyFrame(t, combined, peer); err == nil {
		t.Fatal("mismatched DER and fingerprint should fail")
	}
}

func TestProxyOptionsRequireTrustBoundary(t *testing.T) {
	var opt ProxyOptions
	if err := opt.validate(); err == nil {
		t.Fatal("empty trusted proxy list should fail")
	}
	opt.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	if err := opt.validate(); err != nil || opt.HeaderTimeout != defaultProxyTimeout {
		t.Fatalf("validate = %v timeout=%s", err, opt.HeaderTimeout)
	}
}

func TestProxyV2EndToEndReauthenticatesCurrentCertificateState(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	cert := testCert(t)
	fingerprint := meta.CertFingerprint(sha256.Sum256(cert.Certificate[0]))
	ps := r.addPolicy(policyFull)
	r.addCert(fingerprint, "proxy-dev", ps, meta.CertActive)

	ln, err := r.b.ListenProxyV2("127.0.0.1:0", ProxyOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		HeaderTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = ln.Serve(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.StopAccepting()
		_ = r.b.Close()
		_ = ln.Close()
		<-done
	})

	dial := func() *testClient {
		nc, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		frame := proxyV2Frame(t, 0x11,
			netip.MustParseAddrPort("192.0.2.55:54321"), cert.Certificate[0])
		if _, err := nc.Write(frame); err != nil {
			_ = nc.Close()
			t.Fatal(err)
		}
		return &testClient{t: t, nc: nc, ver: packet.V5}
	}

	first := dial()
	if ack := first.connect("proxy-dev", true); ack.Code != reason.Success {
		t.Fatalf("active certificate rejected: %s", ack.Code)
	}
	_ = first.nc.Close()
	r.waitConns(0)

	// 身份来自同一张代理 TLV 证书，但每次 CONNECT 都重新查询当前热视图；
	// 不能把终结器此前验过证书误当成 broker 可缓存的授权结论。
	r.revoke(fingerprint, "proxy-dev", ps, "test revocation")
	r.advance(time.Second)
	second := dial()
	defer func() { _ = second.nc.Close() }()
	if ack := second.connect("proxy-dev", true); ack.Code != reason.NotAuthorized {
		t.Fatalf("revoked certificate result = %s", ack.Code)
	}
}

func FuzzParseProxyV2TLVs(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{ProxyV2TLVClientCert, 0, 4, 1, 3, 4, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = parseProxyV2TLVs(raw)
	})
}

var proxyIdentityBenchSink ConnInfo

func BenchmarkProxyV2Identity(b *testing.B) {
	cert := testCert(b)
	der := cert.Certificate[0]
	fp := meta.CertFingerprint(sha256.Sum256(der))
	source := netip.MustParseAddrPort("192.0.2.1:1234")
	for _, tc := range []struct {
		name     string
		identity proxyV2Identity
	}{
		{"der-x509", proxyV2Identity{certDER: der, tlsVersion: tls.VersionTLS13, alpn: ALPNMQTTCert}},
		{"fingerprint", proxyV2Identity{fingerprint: fp, hasFingerprint: true,
			tlsVersion: tls.VersionTLS13, alpn: ALPNMQTTCert}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				info, err := connInfoFromProxyIdentity(source, tc.identity)
				if err != nil {
					b.Fatal(err)
				}
				proxyIdentityBenchSink = info
			}
		})
	}
}
