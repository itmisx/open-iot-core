package broker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net"
	"net/netip"
	"testing"
	"time"
)

// PROXY v2 入口的拒绝分支。
//
// 这条路径把**传输身份**整个交给前置终结器，所以它是本仓库信任边界最靠外的
// 一处：每一条拒绝都是在挡「直连客户端伪造身份」。一条都不验的后果不是少测
// 了一点，而是改动时看不出有没有把某条拒绝改成了接受——而那种缺陷在功能上
// 完全看不出来，受影响的设备照样连得上，只是身份是它自己编的。

func proxyOpts() ProxyOptions {
	return ProxyOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
		HeaderTimeout:  time.Second,
	}
}

// readIdentityFrom 把 raw 喂给 readProxyV2Identity，peer 固定为受信地址。
func readIdentityFrom(t *testing.T, raw []byte) (ConnInfo, error) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	go func() { _, _ = client.Write(raw); _ = client.Close() }()
	return readProxyV2Identity(server, netip.MustParseAddrPort("127.0.0.1:5000"), proxyOpts())
}

// proxyHeader 组一个 IPv4 的 PROXY v2 header，body 直接给定。
func proxyHeader(verCmd, famProto byte, body []byte) []byte {
	out := append([]byte("\r\n\r\n\x00\r\nQUIT\n"), verCmd, famProto, 0, 0)
	binary.BigEndian.PutUint16(out[14:16], uint16(len(body)))
	return append(out, body...)
}

func ipv4Body() []byte {
	b := make([]byte, 12)
	copy(b[:4], []byte{10, 0, 0, 1})
	copy(b[4:8], []byte{10, 0, 0, 2})
	binary.BigEndian.PutUint16(b[8:10], 1883)
	binary.BigEndian.PutUint16(b[10:12], 8883)
	return b
}

// identityTLV 组一个指纹型身份 TLV。
func identityTLV(tlvType byte, tlsVer uint16, alpn string, tail []byte) []byte {
	val := []byte{1, 0, 0, byte(len(alpn))}
	binary.BigEndian.PutUint16(val[1:3], tlsVer)
	val = append(val, alpn...)
	val = append(val, tail...)
	out := []byte{tlvType, 0, 0}
	binary.BigEndian.PutUint16(out[1:3], uint16(len(val)))
	return append(out, val...)
}

// TestProxyV2AcceptsFingerprintIdentity 先确认正常路径能过。
//
// 没有这一条，下面那一堆拒绝可能是「全都拒绝」造成的——那种实现同样让
// 每条拒绝用例变绿。
func TestProxyV2AcceptsFingerprintIdentity(t *testing.T) {
	sum := sha256.Sum256([]byte("leaf"))
	raw := proxyHeader(0x21, 0x11, append(ipv4Body(),
		identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert, sum[:])...))
	info, err := readIdentityFrom(t, raw)
	if err != nil {
		t.Fatalf("合法 header 被拒: %v", err)
	}
	if !info.HasCert || info.CertFP != sum {
		t.Fatalf("身份没解出来: hasCert=%v fp=%x", info.HasCert, info.CertFP)
	}
	if info.ALPN != ALPNMQTTCert || info.TLSVersion != 0x0304 {
		t.Fatalf("TLS 事实没带过来: alpn=%q ver=%#x", info.ALPN, info.TLSVersion)
	}
	if info.Remote.Port() != 1883 {
		t.Fatalf("源地址没解出来: %v", info.Remote)
	}

	// 十六进制形式的指纹同样接受：HAProxy 一类终结器导出的就是它。
	hexFP := []byte(hex.EncodeToString(sum[:]))
	raw = proxyHeader(0x21, 0x11, append(ipv4Body(),
		identityTLV(ProxyV2TLVCertFingerprint, 0x0303, ALPNMQTTCert, hexFP)...))
	info, err = readIdentityFrom(t, raw)
	if err != nil || info.CertFP != sum {
		t.Fatalf("十六进制指纹 = %v %x", err, info.CertFP)
	}
}

// TestProxyV2RejectsMalformed 钉住每一条拒绝。
func TestProxyV2RejectsMalformed(t *testing.T) {
	sum := sha256.Sum256([]byte("leaf"))
	goodTLV := identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert, sum[:])

	cases := []struct {
		name string
		raw  []byte
	}{
		{"签名不对", append([]byte("NOT-A-PROXY-SIG!"), ipv4Body()...)},
		// LOCAL 命令（0x20）不携带受信身份：健康检查走的是它，而一个
		// 健康检查不该变成一台有身份的设备。
		{"LOCAL 命令", proxyHeader(0x20, 0x11, append(ipv4Body(), goodTLV...))},
		{"版本不是 2", proxyHeader(0x11, 0x11, append(ipv4Body(), goodTLV...))},
		{"不支持的 family", proxyHeader(0x21, 0x31, append(ipv4Body(), goodTLV...))},
		{"IPv4 地址被截断", proxyHeader(0x21, 0x11, make([]byte, 8))},
		{"没有身份 TLV", proxyHeader(0x21, 0x11, ipv4Body())},
		{"TLV 头被截断", proxyHeader(0x21, 0x11, append(ipv4Body(), 0xe1, 0x00))},
		{"TLV 长度超出 body", proxyHeader(0x21, 0x11,
			append(ipv4Body(), 0xe1, 0xff, 0xff))},
		{"身份 TLV 太短", proxyHeader(0x21, 0x11,
			append(ipv4Body(), 0xe1, 0x00, 0x02, 1, 0))},
		{"格式版本不认识", proxyHeader(0x21, 0x11, append(ipv4Body(),
			func() []byte {
				v := identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert, sum[:])
				v[3] = 9 // 把 format version 改掉
				return v
			}()...))},
		{"TLS 版本不支持", proxyHeader(0x21, 0x11, append(ipv4Body(),
			identityTLV(ProxyV2TLVCertFingerprint, 0x0301, ALPNMQTTCert, sum[:])...))},
		{"指纹长度不对", proxyHeader(0x21, 0x11, append(ipv4Body(),
			identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert, []byte{1, 2, 3})...))},
		{"指纹不是合法十六进制", proxyHeader(0x21, 0x11, append(ipv4Body(),
			identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert,
				[]byte("zz"+hex.EncodeToString(sum[:])[2:]))...))},
		{"身份 TLV 重复", proxyHeader(0x21, 0x11,
			append(append(ipv4Body(), goodTLV...), goodTLV...))},
		// 两个 TLV 对 TLS 事实说法不一致：说明终结器自己就不自洽，不能
		// 挑一个信。
		{"两个 TLV 的 TLS 事实冲突", proxyHeader(0x21, 0x11,
			append(append(ipv4Body(),
				identityTLV(ProxyV2TLVClientCert, 0x0304, ALPNMQTTCert, []byte{0x30, 0x00})...),
				identityTLV(ProxyV2TLVCertFingerprint, 0x0303, ALPNMQTTCert, sum[:])...))},
		{"证书 DER 解不开", proxyHeader(0x21, 0x11, append(ipv4Body(),
			identityTLV(ProxyV2TLVClientCert, 0x0304, ALPNMQTTCert,
				[]byte{0x30, 0x01, 0x02})...))},
	}
	for _, c := range cases {
		if _, err := readIdentityFrom(t, c.raw); err == nil {
			t.Fatalf("%s：必须被拒绝", c.name)
		}
	}
}

// TestProxyV2RejectsUntrustedPeer 钉住最外层那道判定。
//
// 受信与否按**原始 TCP peer 地址**判，而不是按 header 里的 source——否则
// 任何人都能声称自己来自受信网段，而那正是这条路径唯一的信任根。
func TestProxyV2RejectsUntrustedPeer(t *testing.T) {
	sum := sha256.Sum256([]byte("leaf"))
	raw := proxyHeader(0x21, 0x11, append(ipv4Body(),
		identityTLV(ProxyV2TLVCertFingerprint, 0x0304, ALPNMQTTCert, sum[:])...))
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	go func() { _, _ = client.Write(raw); _ = client.Close() }()
	if _, err := readProxyV2Identity(server,
		netip.MustParseAddrPort("203.0.113.9:5000"), proxyOpts()); err == nil {
		t.Fatal("非受信来源必须被拒绝，哪怕 header 本身完全合法")
	}
}

// TestProxyOptionsValidate 钉住空 allowlist 永远不允许启动。
//
// 空 allowlist 意味着「谁都受信」，而这个端点接受的是对端自称的身份。
func TestProxyOptionsValidate(t *testing.T) {
	var empty ProxyOptions
	if err := empty.validate(); err == nil {
		t.Fatal("空 trusted proxies 必须拒绝启动")
	}
	bad := ProxyOptions{TrustedProxies: []netip.Prefix{{}}}
	if err := bad.validate(); err == nil {
		t.Fatal("无效 CIDR 必须拒绝启动")
	}
	ok := proxyOpts()
	ok.HeaderTimeout = 0
	if err := ok.validate(); err != nil {
		t.Fatalf("合法配置被拒: %v", err)
	}
	if ok.HeaderTimeout != defaultProxyTimeout {
		t.Fatalf("超时为 0 时应取默认值，得到 %v", ok.HeaderTimeout)
	}
}
