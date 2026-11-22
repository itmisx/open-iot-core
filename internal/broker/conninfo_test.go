package broker

import (
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
)

func TestConnInfoSeparatesAuthenticationLifetime(t *testing.T) {
	var fp meta.CertFingerprint
	fp[0] = 7
	want := ConnInfo{
		Remote:  netip.MustParseAddrPort("192.0.2.1:1883"),
		HasCert: true, CertFP: fp,
		CertSubject: authz.DN{CommonName: "device-1", Organization: "tenant"},
		CertIssuer:  authz.DN{CommonName: "ca-1"}, CertSerial: "0102",
		TLSVersion: 0x0304, ALPN: "mqtt", HandshakeMS: 1.25,
		AccessKeyID: "AKID", MaxLifetime: time.Hour, RedirectTo: "mqtt.example",
	}
	b := &Broker{}
	b.opt.ReadBufferSize = defaultReadBuffer
	b.opt.MaxWriteBatch = defaultMaxWriteBatch
	c := newConn(b, nil, want)
	if got := c.authenticationInfo(); !reflect.DeepEqual(got, want) {
		t.Fatalf("认证信息往返不一致:\n got=%+v\nwant=%+v", got, want)
	}

	// 模拟 Authenticator 返回后的释放动作：运行期字段仍在，认证字段消失。
	c.releaseAuthInfo()
	got := c.authenticationInfo()
	if got.Remote != want.Remote || got.TLSVersion != want.TLSVersion ||
		got.ALPN != want.ALPN || got.HandshakeMS != want.HandshakeMS ||
		got.MaxLifetime != want.MaxLifetime || got.RedirectTo != want.RedirectTo {
		t.Fatalf("释放认证载荷破坏了运行期字段: %+v", got)
	}
	if got.HasCert || got.CertFP != (meta.CertFingerprint{}) ||
		got.CertSubject != (authz.DN{}) || got.CertIssuer != (authz.DN{}) ||
		got.CertSerial != "" || got.AccessKeyID != "" {
		t.Fatalf("认证载荷未释放干净: %+v", got)
	}
}

func TestCompactTransportInfoPreservesKnownAndCustomALPN(t *testing.T) {
	b := &Broker{opt: Options{ReadBufferSize: defaultReadBuffer}}
	known := newConn(b, nil, ConnInfo{ALPN: ALPNMQTTCert})
	if known.retry.Load() != nil || known.alpn() != ALPNMQTTCert {
		t.Fatalf("标准 ALPN 未内联编码: code=%d extra=%+v value=%q",
			known.info.ALPN, known.retry.Load(), known.alpn())
	}
	known.releaseConnectStages()

	custom := newConn(b, nil, ConnInfo{ALPN: "vendor.example/mqtt"})
	if custom.retry.Load() == nil || custom.retry.Load().extra == nil || custom.alpn() != "vendor.example/mqtt" {
		t.Fatalf("自定义 ALPN 往返失败: extra=%+v value=%q", custom.retry.Load(), custom.alpn())
	}
	custom.releaseConnectStages()
}

func TestCompactRemoteAddrRoundTrip(t *testing.T) {
	b := &Broker{opt: Options{ReadBufferSize: defaultReadBuffer}}
	for _, raw := range []string{
		"192.0.2.10:1883",
		"[2001:db8::10]:8883",
		"[::ffff:192.0.2.11]:443",
		"[fe80::1%en0]:8883",
	} {
		want := netip.MustParseAddrPort(raw)
		c := newConn(b, nil, ConnInfo{Remote: want})
		if got := c.remote(); got != want {
			t.Errorf("remote %q round trip = %q", want, got)
		}
		c.releaseConnectStages()
	}
	c := newConn(b, nil, ConnInfo{})
	if got := c.remote(); got.IsValid() {
		t.Fatalf("invalid remote round trip = %q", got)
	}
	c.releaseConnectStages()
}
