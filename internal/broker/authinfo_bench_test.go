package broker

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
)

var authInfoBenchSink *conn

func BenchmarkNewTLSConnAuthLifecycle(b *testing.B) {
	brk := &Broker{opt: Options{ReadBufferSize: defaultReadBuffer}}
	info := ConnInfo{
		HasCert: true, CertSerial: "0123456789abcdef", AccessKeyID: "AKID",
		CertSubject: authz.DN{CommonName: "device-000001"},
		CertIssuer:  authz.DN{CommonName: "openiot-device-ca"},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := newConn(brk, nil, info)
		_ = c.authenticationInfo()
		c.releaseAuthInfo()
		c.releaseConnectStages()
		authInfoBenchSink = c
	}
}
