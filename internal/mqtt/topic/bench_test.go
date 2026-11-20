package topic

import (
	"fmt"
	"runtime"
	"testing"
)

// buildFleet 构造一棵接近真实 IoT 负载形状的订阅树：
// 每个设备订阅自己的若干保留主题，少量后端应用订阅通配符。
func buildFleet(devices int) (*Trie, []string) {
	tr := New(DefaultLimits())
	h := Handle(0)
	for i := 0; i < devices; i++ {
		thing := fmt.Sprintf("dev-%07d", i)
		for _, suffix := range []string{
			"shadow/update/accepted", "shadow/update/delta",
			"shadow/get/accepted", "jobs/notify-next",
		} {
			f := "$aws/things/" + thing + "/" + suffix
			if err := tr.Subscribe([]byte(f), Subscription{Handle: h, QoS: 1}); err != nil {
				panic(err)
			}
			h++
		}
	}
	// 后端应用的通配符订阅
	backends := []string{
		"$aws/things/+/shadow/update/accepted",
		"$aws/things/+/jobs/notify-next",
		"$aws/events/presence/connected/+",
		"telemetry/#",
	}
	for _, f := range backends {
		if err := tr.Subscribe([]byte(f), Subscription{Handle: h, QoS: 0}); err != nil {
			panic(err)
		}
		h++
	}
	topics := []string{
		"$aws/things/dev-0000001/shadow/update/accepted",
		"$aws/things/dev0500000/shadow/update/delta",
		"telemetry/site-1/line-2/temp",
		"unmatched/topic/name",
	}
	return tr, topics
}

func BenchmarkTrieMatchDeviceTopic(b *testing.B) {
	tr, _ := buildFleet(10000)
	topic := []byte("$aws/things/dev-0005000/shadow/update/accepted")
	buf := make([]SubRef, 0, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = tr.MatchAppend(buf[:0], topic)
	}
	if len(buf) == 0 {
		b.Fatal("应有命中")
	}
}

func BenchmarkTrieMatchWildcardTopic(b *testing.B) {
	tr, _ := buildFleet(10000)
	topic := []byte("telemetry/site-1/line-2/temp")
	buf := make([]SubRef, 0, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = tr.MatchAppend(buf[:0], topic)
	}
}

func BenchmarkTrieMatchNoHit(b *testing.B) {
	tr, _ := buildFleet(10000)
	topic := []byte("no/such/topic/here")
	buf := make([]SubRef, 0, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = tr.MatchAppend(buf[:0], topic)
	}
}

func BenchmarkTrieSubscribe(b *testing.B) {
	filters := make([][]byte, b.N)
	for i := range filters {
		filters[i] = []byte(fmt.Sprintf("$aws/things/dev-%07d/shadow/update/delta", i))
	}
	tr := New(DefaultLimits())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tr.Subscribe(filters[i], Subscription{Handle: Handle(i)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMatchPairwise(b *testing.B) {
	l := DefaultLimits()
	filter := []byte("$aws/things/+/shadow/update/accepted")
	topic := []byte("$aws/things/dev-0005000/shadow/update/accepted")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !Match(filter, topic, l) {
			b.Fatal("应匹配")
		}
	}
}

// BenchmarkGCScan 报告不同订阅规模下的 GC 单轮耗时与堆占用，
// 用于契约 13 与 PRD ARCH-043、NFR-022 的性能基线（TST-060 至 TST-064）。
//
// 注意：这是报告而非断言。契约 13 的不变量由 TestNodesArePointerFree 的
// 静态断言强制——那是确定性的；而 GC 耗时受调度、竞态检测器、CI 共享
// 机器负载影响，用它做门禁必然 flaky。两者分工明确：
// 静态断言保证正确性，本基准提供可复现的数字。
func BenchmarkGCScan(b *testing.B) {
	for _, devices := range []int{25_000, 250_000} {
		b.Run(fmt.Sprintf("订阅%d条", devices*4), func(b *testing.B) {
			tr, _ := buildFleet(devices)
			runtime.GC()
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			b.ReportMetric(float64(ms.HeapAlloc)/(1<<20), "堆MB")
			b.ReportMetric(float64(tr.NodeCount()), "节点数")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runtime.GC()
			}
			b.StopTimer()
			runtime.KeepAlive(tr)
		})
	}
}

// TestTrieScaleSanity 在百万订阅规模下验证功能正确性与内存量级，
// 不做时序断言。-short 下跳过。
func TestTrieScaleSanity(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试，-short 下跳过")
	}
	const devices = 250_000
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	tr, topics := buildFleet(devices)

	wantSubs := devices*4 + 4
	if tr.Len() != wantSubs {
		t.Fatalf("订阅数 = %d，期望 %d", tr.Len(), wantSubs)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := after.HeapAlloc - before.HeapAlloc
	heapMB := float64(delta) / (1 << 20)
	t.Logf("订阅 %d 条，节点 %d 个，增量堆 %.1f MB（每订阅约 %.0f 字节）",
		tr.Len(), tr.NodeCount(), heapMB,
		float64(delta)/float64(tr.Len()))

	// 功能性抽查：设备主题与通配符主题都应命中预期数量
	for _, topic := range topics {
		n := 0
		tr.Match([]byte(topic), func(SubRef) bool { n++; return true })
		t.Logf("主题 %q 命中 %d 条", topic, n)
	}
	if n := len(tr.MatchAppend(nil, []byte(topics[0]))); n != 2 {
		// 设备自身的精确订阅 + 后端的通配符订阅
		t.Errorf("%q 命中 %d 条，期望 2", topics[0], n)
	}

	// 每订阅的内存开销应在合理量级：node 16 字节 + sub 28 字节加上
	// 段表与两个 map 的开销，放宽到 400 字节/订阅。
	perSub := float64(delta) / float64(tr.Len())
	if perSub > 400 {
		t.Errorf("每订阅内存 %.0f 字节，超出预期量级", perSub)
	}
	// 400 字节只是量级，它放得过一次不小的膨胀。与
	// docs/baseline/memory.json 做 10% 比较的那一条在
	// internal/router/trie_memory_test.go：那次比较要读 internal/baseline，
	// 而 ARCH-052 不许协议层导入任何兄弟包（协议层须可独立 fuzz 与开源）。
	// 把 baseline 挪到仓库根来让 contractcheck 看不见，是同样地破坏契约。
	runtime.KeepAlive(tr)
}
