package router

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/itmisx/open-iot-core/internal/baseline"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// TestTrieBytesPerSubscriptionBaseline 把百万订阅规模下的每订阅字节数
// 与 docs/baseline/memory.json 做 10% 比较（TST-127）。
//
// 测量的结构属于 internal/mqtt/topic，比较却放在这里：ARCH-052 不许协议层
// 导入任何兄弟包，而这次比较要读 internal/baseline。协议层那侧留的是
// 400 字节/订阅的量级断言（TestTrieScaleSanity），它放得过一次不小的膨胀；
// 真正钉住公开数字的是这一条。router 是服务端里持有这棵树的那一层，
// 放在这里不必为一次比较新开一个包。
//
// 订阅形状与协议层那条测试刻意相同：每设备四条精确订阅加少量后端通配符。
// 形状变了两处都要改——基线里的数字是对这个形状量出来的，换个形状得出的
// 字节不能和它比。
func TestTrieBytesPerSubscriptionBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试，-short 下跳过")
	}
	const devices = 250_000

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	tr := buildBaselineFleet(devices)

	wantSubs := devices*4 + 4
	if tr.Len() != wantSubs {
		t.Fatalf("订阅数 = %d，期望 %d", tr.Len(), wantSubs)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	delta := after.HeapAlloc - before.HeapAlloc
	perSub := int(delta / uint64(tr.Len()))
	t.Logf("订阅 %d 条，节点 %d 个，增量堆 %.1f MB（每订阅约 %d 字节）",
		tr.Len(), tr.NodeCount(), float64(delta)/(1<<20), perSub)

	// 订阅节点里加一个指针，GC 扫描就不再与订阅数无关，这里会先在字节上失败。
	if err := baseline.Check("trieBytesPerSubscription", perSub); err != nil {
		t.Error(err)
	}
	runtime.KeepAlive(tr)
}

// buildBaselineFleet 构造基线所对应的订阅形状：每个设备订阅自己的四条
// 保留主题，另有少量后端应用的通配符订阅。
func buildBaselineFleet(devices int) *topic.Trie {
	tr := topic.New(topic.DefaultLimits())
	h := topic.Handle(0)
	for i := 0; i < devices; i++ {
		thing := fmt.Sprintf("dev-%07d", i)
		for _, suffix := range []string{
			"shadow/update/accepted", "shadow/update/delta",
			"shadow/get/accepted", "jobs/notify-next",
		} {
			f := "$aws/things/" + thing + "/" + suffix
			if err := tr.Subscribe([]byte(f), topic.Subscription{Handle: h, QoS: 1}); err != nil {
				panic(err)
			}
			h++
		}
	}
	for _, f := range []string{
		"$aws/things/+/shadow/update/accepted",
		"$aws/things/+/jobs/notify-next",
		"$aws/events/presence/connected/+",
		"telemetry/#",
	} {
		if err := tr.Subscribe([]byte(f), topic.Subscription{Handle: h, QoS: 0}); err != nil {
			panic(err)
		}
		h++
	}
	return tr
}
