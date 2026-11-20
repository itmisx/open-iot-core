package packet

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func benchPublish(v Version) []byte {
	p := &Publish{
		Topic:    []byte("$aws/things/device-00123/shadow/update"),
		Payload:  []byte(`{"state":{"reported":{"temperature":21.5,"humidity":48}}}`),
		QoS:      QoS1,
		PacketID: 1234,
	}
	if v == V5 {
		p.Props.ContentType = []byte("application/json")
		p.Props.Set(PropContentType)
		p.Props.MessageExpiry = 300
		p.Props.Set(PropMessageExpiry)
	}
	b, err := Encode(p, v)
	if err != nil {
		panic(err)
	}
	return b
}

func BenchmarkDecodePublishV5(b *testing.B) {
	buf := benchPublish(V5)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(buf, V5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodePublishV311(b *testing.B) {
	buf := benchPublish(V311)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(buf, V311); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodePublishV5(b *testing.B) {
	p := &Publish{
		Topic:    []byte("$aws/things/device-00123/shadow/update"),
		Payload:  []byte(`{"state":{"reported":{"temperature":21.5,"humidity":48}}}`),
		QoS:      QoS1,
		PacketID: 1234,
	}
	p.Props.ContentType = []byte("application/json")
	p.Props.Set(PropContentType)
	// 复用输出缓冲，模拟服务端写出路径
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := p.EncodeTo(buf[:0], V5)
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}

func BenchmarkDecodeConnectV5(b *testing.B) {
	c := &Connect{
		Version: V5, ClientID: []byte("device-00123"), KeepAlive: 60, CleanStart: true,
		HasUsername: true, Username: []byte("device-00123"),
	}
	c.Props.SessionExpiry = 3600
	c.Props.Set(PropSessionExpiry)
	c.Props.ReceiveMaximum = 100
	c.Props.Set(PropReceiveMaximum)
	buf, err := Encode(c, V5)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Decode(buf, V5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeAckV5(b *testing.B) {
	a := &Ack{Kind: PUBACK, PacketID: 1234, Code: reason.Success}
	buf := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := a.EncodeTo(buf[:0], V5)
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}
