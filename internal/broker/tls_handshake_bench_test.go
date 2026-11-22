package broker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/pki"
)

// newTLSHandshakeBenchConfigs 只生成一次密钥与证书，基准本体只量
// TLS 1.3 mTLS 握手。net.Pipe 不经内核 TCP，因此这个数用于判断
// 密码学/Go TLS 的 CPU 与分配形状，不能当作线上 CONNECT/s。
func newTLSHandshakeBenchConfigs(b *testing.B) (*tls.Config, *tls.Config) {
	b.Helper()
	now := time.Now().UnixMilli()
	root, err := pki.CreateRootCA(pki.CARequest{
		CommonName: "Benchmark Root", MaxPathLen: 1,
	}, now)
	if err != nil {
		b.Fatal(err)
	}
	issuing, err := pki.CreateIssuingCA(root, pki.CARequest{
		CommonName: "Benchmark Issuing", MaxPathLen: 0,
	}, now)
	if err != nil {
		b.Fatal(err)
	}
	ca, err := pki.New(issuing, []*x509.Certificate{issuing.Cert, root.Cert},
		pki.DefaultPolicy(), func() int64 { return now })
	if err != nil {
		b.Fatal(err)
	}
	server, err := ca.IssueServerCert("broker.local", []string{"broker.local"}, nil, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	serverCert, err := tls.X509KeyPair(concatPEM(server.CertPEM, server.ChainPEM), server.KeyPEM)
	if err != nil {
		b.Fatal(err)
	}
	device, err := ca.IssueKeyPair(pki.IssueRequest{CommonName: "bench-device"})
	if err != nil {
		b.Fatal(err)
	}
	deviceCert, err := tls.X509KeyPair(concatPEM(device.CertPEM, device.ChainPEM), device.KeyPEM)
	if err != nil {
		b.Fatal(err)
	}
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert}, ClientCAs: ca.TrustPool(),
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13, NextProtos: []string{ALPNMQTTCert},
	}
	var ticketKey [32]byte
	copy(ticketKey[:], "open-iot-core-handshake-benchmark")
	serverConfig.SetSessionTicketKeys([][32]byte{ticketKey})
	rootPool := x509.NewCertPool()
	rootPool.AddCert(root.Cert)
	clientConfig := &tls.Config{
		Certificates: []tls.Certificate{deviceCert}, RootCAs: rootPool,
		ServerName: "broker.local", MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13, NextProtos: []string{ALPNMQTTCert},
	}
	return serverConfig, clientConfig
}

func runTLS13MTLSHandshake(serverConfig, clientConfig *tls.Config) error {
	serverRaw, clientRaw := net.Pipe()
	server := tls.Server(serverRaw, serverConfig)
	client := tls.Client(clientRaw, clientConfig)
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	clientErr := client.Handshake()
	serverErr := <-done
	_ = clientRaw.Close()
	_ = serverRaw.Close()
	if clientErr != nil || serverErr != nil {
		return errors.Join(clientErr, serverErr)
	}
	return nil
}

func benchmarkTLS13MTLSHandshake(b *testing.B, parallel bool, maxConcurrent, parallelism int,
	curves []tls.CurveID,
) {
	serverConfig, clientConfig := newTLSHandshakeBenchConfigs(b)
	serverConfig.CurvePreferences = curves
	var gate chan struct{}
	if maxConcurrent > 0 {
		gate = make(chan struct{}, maxConcurrent)
	}
	handshake := func() {
		if gate != nil {
			gate <- struct{}{}
			defer func() { <-gate }()
		}
		if err := runTLS13MTLSHandshake(serverConfig, clientConfig); err != nil {
			b.Fatalf("TLS handshake: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		if parallelism > 0 {
			b.SetParallelism(parallelism)
		}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				handshake()
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		handshake()
	}
}

func BenchmarkTLS13MTLSHandshakeSerial(b *testing.B) {
	benchmarkTLS13MTLSHandshake(b, false, 0, 0, nil)
}

func BenchmarkTLS13MTLSHandshakeConventionalSerial(b *testing.B) {
	benchmarkTLS13MTLSHandshake(b, false, 0, 0,
		[]tls.CurveID{tls.X25519, tls.CurveP256})
}

func BenchmarkTLS13MTLSHandshakeParallel(b *testing.B) {
	benchmarkTLS13MTLSHandshake(b, true, 0, 1, nil)
}

func BenchmarkTLS13MTLSHandshakeConcurrency(b *testing.B) {
	for _, limit := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
		b.Run(strconv.Itoa(limit), func(b *testing.B) {
			benchmarkTLS13MTLSHandshake(b, true, limit, 32, nil)
		})
	}
}
