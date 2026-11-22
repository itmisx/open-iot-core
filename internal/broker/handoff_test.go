package broker

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

func TestServeHandoffTransfersEstablishedConnection(t *testing.T) {
	clk := newClock()
	store := meta.NewLocalStore(meta.LocalOptions{Now: clk.now})
	owners := cluster.NewLocalOwnership("node-1", 8)
	limits := topic.DefaultLimits()
	b, err := New(Options{
		Node: "node-1", Meta: store,
		Router:   router.New(router.Options{Self: "node-1", Limits: limits, Now: clk.now}),
		Sessions: session.NewMemStore(), Owners: owners,
		Limiter: quota.NewLocal(quota.Options{Now: clk.now}),
		Bus:     events.New(events.Options{Node: "node-1", Now: clk.now}),
		Auth:    insecureAuth(t, `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`),
		Limits:  limits, Now: clk.now, WriterWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = b.Close()
		_ = store.Close()
		_ = owners.Close()
	})

	srv, cli := net.Pipe()
	client := &testClient{t: t, nc: cli, ver: packet.V5}
	launched := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	serveReturned := make(chan error, 1)
	go func() {
		serveReturned <- b.serveHandoff(context.Background(), srv, ConnInfo{}, func(run func()) {
			go func() {
				once.Do(func() { close(launched) })
				run()
				close(done)
			}()
		})
	}()

	ack := client.connect("handoff-device", true)
	if ack.Code != 0 {
		t.Fatalf("CONNACK=%v", ack.Code)
	}
	select {
	case <-launched:
	case <-time.After(time.Second):
		t.Fatal("CONNECT 成功后没有启动稳态 goroutine")
	}
	select {
	case err := <-serveReturned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("建连 goroutine 未在交接后退出")
	}
	if b.Conns() != 1 {
		t.Fatalf("交接后连接数=%d，期望 1", b.Conns())
	}
	_ = cli.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("关闭客户端后稳态 goroutine 未退出")
	}
	if b.Conns() != 0 {
		t.Fatalf("稳态退出后连接数=%d，期望 0", b.Conns())
	}
}
