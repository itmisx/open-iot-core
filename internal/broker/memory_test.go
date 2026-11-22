package broker

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/itmisx/open-iot-core/internal/baseline"
	"github.com/itmisx/open-iot-core/internal/session"
)

// TestConnStructBudget guards the allocator size class of the million-way
// steady-state object. A seemingly harmless pointer or slice header can move
// every live connection into the next class and cost tens of MiB per node.
func TestConnStructBudget(t *testing.T) {
	// 字段须不超过 480 B，才能留在 Go 的 480 B size class。
	const max = uintptr(480)
	if got := unsafe.Sizeof(conn{}); got > max {
		t.Fatalf("conn struct = %d bytes, want <= %d", got, max)
	} else {
		t.Logf("conn struct = %d bytes", got)
	}
}

var connAllocSink *conn

func BenchmarkConnStructAllocation(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connAllocSink = new(conn)
	}
}

// TestConnMemoryBudget 量一条刚建立连接在堆上的字节：连接结构、初始
// 读缓冲，以及出站队列深度为默认值的会话运行时。编码与写出缓冲
// 已改成首次使用时按需分配，不再计入百万空闲连接的基础常驻量。
//
// 不含读协程的栈。栈不在 HeapAlloc 里，而且随调用深度变，写进基线就是
// 无法复现的数字。去掉 baseline.Check 之后，下面的 64 KiB 绝对上限
// 仍放得过一次不小的膨胀。
func TestConnMemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试，-short 下跳过")
	}
	const n = 20_000
	b := &Broker{}
	b.opt.ReadBufferSize = defaultReadBuffer
	b.opt.OutboundBuffer = defaultOutboundBuffer

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	keep := make([]*conn, n)
	for i := 0; i < n; i++ {
		c := newConn(b, nil, ConnInfo{})
		st := &session.State{ClientID: fmt.Sprintf("c-%07d", i)}
		c.rt = session.NewRuntime(st, nil, defaultOutboundBuffer)
		// 长连接已经完成 CONNECT；分段耗时 sidecar 在结果事件发出后
		// 立即回收，不属于空闲连接的常驻内存。
		c.releaseConnectStages()
		keep[i] = c
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	per := int((after.HeapAlloc - before.HeapAlloc) / uint64(n))
	t.Logf("%d 条连接，每条约 %d 字节", n, per)
	if per > 64*1024 {
		t.Errorf("每连接 %d 字节，超出 64 KiB 的绝对上限", per)
	}
	if err := baseline.Check("connBytes", per); err != nil {
		t.Error(err)
	}
	runtime.KeepAlive(keep)
}
