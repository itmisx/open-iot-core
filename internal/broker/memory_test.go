package broker

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/itmisx/open-iot-core/internal/baseline"
	"github.com/itmisx/open-iot-core/internal/session"
)

// TestConnMemoryBudget 量一条连接在堆上的字节：连接结构、读缓冲、
// 编码缓冲、写出缓冲，以及出站队列深度为默认值的会话运行时。
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
