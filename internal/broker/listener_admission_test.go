package broker

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/quota"
)

type queuedTestListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func TestTLSHandshakeRateRejectsBeforeTLSWork(t *testing.T) {
	ql := newQueuedTestListener()
	lim := quota.NewLocal(quota.Options{
		Now: func() int64 { return 1 }, Overrides: map[quota.ID]int{quota.HandshakeRate: 1},
	})
	l := &Listener{
		b:  &Broker{opt: Options{Limiter: lim}},
		ln: ql, tlsc: &tls.Config{}, opt: TLSOptions{HandshakeTimeout: time.Hour},
		handshakeGate: make(chan struct{}, 2), stop: make(chan struct{}),
	}
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- l.Serve(ctx) }()

	server1, client1 := net.Pipe()
	ql.conns <- server1
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && l.TLSHandshakeStats().InFlight != 1 {
		time.Sleep(time.Millisecond)
	}
	server2, client2 := net.Pipe()
	ql.conns <- server2
	_ = client2.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := client2.Read(one[:]); err == nil {
		t.Fatal("第二条连接应在 TLS 前被 handshake_rate 拒绝")
	}
	for time.Now().Before(deadline) && l.TLSHandshakeStats().Throttled != 1 {
		time.Sleep(time.Millisecond)
	}
	st := l.TLSHandshakeStats()
	if st.Throttled != 1 || st.InFlight != 1 || st.Peak != 1 {
		t.Fatalf("握手限流读数 = %+v，被拒连接不应进入 TLS 工作集", st)
	}

	_ = l.StopAccepting()
	_ = client1.Close()
	_ = client2.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve 没有退出")
	}
	l.wg.Wait()
}

func newQueuedTestListener() *queuedTestListener {
	return &queuedTestListener{conns: make(chan net.Conn, 8), closed: make(chan struct{})}
}

func (l *queuedTestListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *queuedTestListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (*queuedTestListener) Addr() net.Addr { return testAddr("listener-admission") }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

// lateAcceptListener 故意让 Close 不立刻唤醒 Accept，把真网络上极窄的
// 「Accept 已取到 fd，Stop 同时开始」窗口确定性地放大。
type lateAcceptListener struct {
	entered chan struct{}
	release chan struct{}
	conn    net.Conn
}

func (l *lateAcceptListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.release
	return l.conn, nil
}
func (*lateAcceptListener) Close() error   { return nil }
func (*lateAcceptListener) Addr() net.Addr { return testAddr("late-accept") }

func TestStopAcceptingPreventsLateWaitGroupAdd(t *testing.T) {
	server, client := net.Pipe()
	late := &lateAcceptListener{
		entered: make(chan struct{}), release: make(chan struct{}), conn: server,
	}
	l := &Listener{ln: late, stop: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- l.Serve(context.Background()) }()
	<-late.entered
	if err := l.StopAccepting(); err != nil {
		t.Fatal(err)
	}
	close(late.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop 后迟到的 Accept 没有退出")
	}
	finished := make(chan struct{})
	go func() { l.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("迟到 Accept 在 Stop 之后向 WaitGroup Add")
	}
	_ = client.Close()
}

func TestTLSHandshakeAdmissionBoundsInFlight(t *testing.T) {
	ql := newQueuedTestListener()
	l := &Listener{
		ln: ql, tlsc: &tls.Config{}, opt: TLSOptions{HandshakeTimeout: time.Hour},
		handshakeGate: make(chan struct{}, 2),
		stop:          make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Serve(ctx) }()

	clients := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		server, client := net.Pipe()
		clients = append(clients, client)
		ql.conns <- server
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && l.TLSHandshakeStats().InFlight != 2 {
		time.Sleep(time.Millisecond)
	}
	st := l.TLSHandshakeStats()
	if st.InFlight != 2 || st.Peak != 2 || st.Capacity != 2 {
		t.Fatalf("握手背压读数 = %+v，期望 in-flight/peak/capacity 都为 2", st)
	}
	// 第三条套接字已可被 Accept，但不得启动第三个 TLS 工作集。
	time.Sleep(20 * time.Millisecond)
	if st = l.TLSHandshakeStats(); st.InFlight != 2 || st.Peak != 2 {
		t.Fatalf("饱和后仍扩大了 TLS 工作集: %+v", st)
	}

	// StopAccepting 必须能唤醒正卡在 gate 上的第三条已接收连接；
	// 否则 Close 会与 WaitGroup 互等。
	if err := l.StopAccepting(); err != nil {
		t.Fatal(err)
	}
	for _, c := range clients {
		_ = c.Close()
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve 没有在取消后退出")
	}
	l.wg.Wait()
	st = l.TLSHandshakeStats()
	if st.InFlight != 0 || st.Failed != 2 || st.Succeeded != 0 {
		t.Fatalf("关闭后 TLS 读数 = %+v，期望 in-flight=0 failed=2 succeeded=0", st)
	}
}
