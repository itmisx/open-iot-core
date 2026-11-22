package quota

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type fakeClock struct {
	mu sync.Mutex
	ms int64
}

func (c *fakeClock) now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ms
}

func (c *fakeClock) advance(ms int64) {
	c.mu.Lock()
	c.ms += ms
	c.mu.Unlock()
}

func newTestLimiter(ov map[ID]int) (*Local, *fakeClock) {
	c := &fakeClock{ms: 1_700_000_000_000}
	return NewLocal(Options{Now: c.now, Overrides: ov}), c
}

// TestQuotaTableIsComplete 保证配额表没有空洞。
//
// 每一项都必须有名字、作用域与种类——ARCH-031 要求每个配额在 v0.1
// 就确定走本地还是全局，含糊的条目会在集群化时变成「配额放大 N 倍」。
func TestQuotaTableIsComplete(t *testing.T) {
	seenName := map[string]ID{}
	for id := ID(0); id < numQuotas; id++ {
		d, ok := Lookup(id)
		if !ok {
			t.Fatalf("配额 %d 查不到定义", id)
		}
		if d.ID != id {
			t.Errorf("配额 %d 的 ID 字段为 %d，与下标不符", id, d.ID)
		}
		if d.Name == "" {
			t.Errorf("配额 %d 缺少名字，配额表有空洞", id)
		}
		if d.PRD == "" {
			t.Errorf("配额 %s 未标注 PRD 编号，无法从告警反查规格", d.Name)
		}
		if prev, dup := seenName[d.Name]; dup {
			t.Errorf("配额名 %q 被 %d 与 %d 重复使用", d.Name, prev, id)
		}
		seenName[d.Name] = id
		if d.Kind == Rate && d.Limit > 0 && d.Burst <= 0 {
			t.Errorf("速率配额 %s 未设置 Burst", d.Name)
		}
	}
	if _, ok := Lookup(numQuotas); ok {
		t.Error("越界编号不应有定义")
	}
	if QuotaCount() != int(numQuotas) || len(Defaults()) != int(numQuotas) {
		t.Error("配额总数不一致")
	}
}

// TestScopeMismatchIsRejected 固定住一条设计决策：
// 用错作用域是编码错误，而不是运行时情况。
//
// 直接拒绝并计数，使它在压测与指标中立刻可见——如果悄悄放行，
// 集群下该配额会被放大 N 倍，而这类问题只在线上才暴露。
func TestScopeMismatchIsRejected(t *testing.T) {
	l, _ := newTestLimiter(nil)
	ctx := context.Background()

	// 全局配额走 AllowLocal 应被拒绝
	if l.AllowLocal(ConnectRate, "", 1) {
		t.Error("全局配额经 AllowLocal 判定应被拒绝")
	}
	// 本地配额走 AllowGlobal 应被拒绝
	if l.AllowGlobal(ctx, PublishRatePerConn, "c1", 1) {
		t.Error("本地配额经 AllowGlobal 判定应被拒绝")
	}
	// 各自走对的方法则通过
	if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("本地配额经 AllowLocal 应通过")
	}
	if !l.AllowGlobal(ctx, ConnectRate, "", 1) {
		t.Error("全局配额经 AllowGlobal 应通过")
	}
	// 越界编号拒绝
	if l.AllowLocal(numQuotas, "", 1) || l.AllowGlobal(ctx, numQuotas, "", 1) {
		t.Error("越界编号应被拒绝")
	}
}

func TestRateLimitTokenBucket(t *testing.T) {
	// 每连接发布速率设为 10/秒，桶容量同为 10
	l, clk := newTestLimiter(map[ID]int{PublishRatePerConn: 10})

	// 初始满桶：连续 10 次通过，第 11 次拒绝
	for i := 0; i < 10; i++ {
		if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
			t.Fatalf("第 %d 次应通过", i+1)
		}
	}
	if l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("超出桶容量应被拒绝")
	}

	// 时间不前进则不补充
	if l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("时钟未前进不应补充令牌")
	}

	// 前进 100 毫秒，按 10/秒应补充 1 个令牌
	clk.advance(100)
	if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("前进 100ms 应补充 1 个令牌")
	}
	if l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("只补充了 1 个令牌，第二次应被拒绝")
	}

	// 前进很久，补充量应被桶容量钳住
	clk.advance(1_000_000)
	n := 0
	for l.AllowLocal(PublishRatePerConn, "c1", 1) {
		n++
		if n > 100 {
			t.Fatal("补充量未被桶容量钳住")
		}
	}
	if n != 10 {
		t.Errorf("长时间空闲后可用令牌 = %d，期望等于桶容量 10", n)
	}
}

func TestRateLimitPerSubjectIsolated(t *testing.T) {
	l, _ := newTestLimiter(map[ID]int{PublishRatePerConn: 2})
	// 耗尽 c1 的额度
	for i := 0; i < 2; i++ {
		if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
			t.Fatal("c1 应有额度")
		}
	}
	if l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Error("c1 额度应已耗尽")
	}
	// c2 不受影响
	if !l.AllowLocal(PublishRatePerConn, "c2", 1) {
		t.Error("不同作用对象的额度应相互隔离")
	}
}

func TestSweepRateBucketsRemovesOnlyRefilledBuckets(t *testing.T) {
	l, clk := newTestLimiter(map[ID]int{PublishRatePerConn: 10})
	for _, clientID := range []string{"c1", "c2"} {
		if !l.AllowLocal(PublishRatePerConn, clientID, 1) {
			t.Fatal("初次请求应通过")
		}
	}
	if got := l.RateBucketCount(); got != 2 {
		t.Fatalf("速率桶数 = %d", got)
	}

	// 99ms 只补回 0.99 个令牌，尚未满桶，不能提前删除并放宽限制。
	clk.advance(99)
	if got := l.SweepRateBuckets(bucketShards); got != 0 {
		t.Fatalf("未满桶被回收 %d 个", got)
	}
	if l.RateBucketCount() != 2 {
		t.Fatal("未满桶不应消失")
	}

	clk.advance(1)
	if got := l.SweepRateBuckets(bucketShards); got != 2 {
		t.Fatalf("满桶回收数 = %d，期望 2", got)
	}
	if l.RateBucketCount() != 0 || l.RateBucketsSwept() != 2 {
		t.Fatalf("回收后 count=%d swept=%d", l.RateBucketCount(), l.RateBucketsSwept())
	}

	// 删除后的语义等于保留一个满桶：仍能完整使用 10 个 burst。
	for i := 0; i < 10; i++ {
		if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
			t.Fatalf("回收后第 %d 个令牌被拒", i+1)
		}
	}
	if l.AllowLocal(PublishRatePerConn, "c1", 1) {
		t.Fatal("回收后不应放宽 burst 上限")
	}
}

func TestSweepRateBucketsIsIncremental(t *testing.T) {
	l, clk := newTestLimiter(map[ID]int{PublishRatePerConn: 1})
	for i := 0; i < 1000; i++ {
		if !l.AllowLocal(PublishRatePerConn, fmt.Sprintf("device-%d", i), 1) {
			t.Fatal("初次请求应通过")
		}
	}
	clk.advance(1000)
	first := l.SweepRateBuckets(1)
	if first <= 0 || first >= 1000 {
		t.Fatalf("单分片回收数 = %d，应只回收总量的一部分", first)
	}
	for i := 1; i < bucketShards; i++ {
		l.SweepRateBuckets(1)
	}
	if got := l.RateBucketCount(); got != 0 {
		t.Fatalf("扫完一轮仍有 %d 个桶", got)
	}
}

func TestSweepRateBucketsConcurrentWithAllow(t *testing.T) {
	l, clk := newTestLimiter(map[ID]int{ConnectRatePerClient: 1000})
	for i := 0; i < 1000; i++ {
		l.AllowGlobal(context.Background(), ConnectRatePerClient,
			fmt.Sprintf("device-%d", i), 1)
	}
	clk.advance(1000)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := worker; i < 1000; i += 8 {
				l.AllowGlobal(context.Background(), ConnectRatePerClient,
					fmt.Sprintf("device-%d", i), 1)
			}
		}()
	}
	for i := 0; i < bucketShards; i++ {
		l.SweepRateBuckets(1)
	}
	wg.Wait()
	if got := l.RateBucketCount(); got < 0 || got > 1000 {
		t.Fatalf("并发回收后的桶数非法: %d", got)
	}
}

func TestRateLimitMultipleTokens(t *testing.T) {
	l, _ := newTestLimiter(map[ID]int{ThroughputPerConn: 1000})
	// 一次申请多个令牌（吞吐配额按字节计）
	if !l.AllowLocal(ThroughputPerConn, "c1", 600) {
		t.Fatal("600 应通过")
	}
	if l.AllowLocal(ThroughputPerConn, "c1", 600) {
		t.Error("剩余不足 600 应被拒绝")
	}
	if !l.AllowLocal(ThroughputPerConn, "c1", 400) {
		t.Error("剩余 400 应通过")
	}
}

func TestRateLimitUnlimited(t *testing.T) {
	// Limit 为 0 表示取消限制
	l, _ := newTestLimiter(map[ID]int{PublishRatePerConn: 0})
	for i := 0; i < 1000; i++ {
		if !l.AllowLocal(PublishRatePerConn, "c1", 1) {
			t.Fatalf("取消限制后第 %d 次仍被拒绝", i)
		}
	}
}

func TestRateLimitWithoutClock(t *testing.T) {
	// 未注入时钟时速率配额无法补充，首个桶仍是满的
	l := NewLocal(Options{Overrides: map[ID]int{PublishRatePerConn: 3}})
	n := 0
	for l.AllowLocal(PublishRatePerConn, "c", 1) {
		n++
		if n > 10 {
			t.Fatal("无时钟时不应无限放行")
		}
	}
	if n != 3 {
		t.Errorf("无时钟时可用令牌 = %d，期望 3", n)
	}
}

func TestCountQuota(t *testing.T) {
	l, _ := newTestLimiter(map[ID]int{SubsPerConn: 3})

	for i := 1; i <= 3; i++ {
		if !l.Acquire(SubsPerConn, "c1", 1) {
			t.Fatalf("第 %d 次占用应成功", i)
		}
		if got := l.Current(SubsPerConn, "c1"); got != int64(i) {
			t.Errorf("当前占用 = %d，期望 %d", got, i)
		}
	}
	if l.Acquire(SubsPerConn, "c1", 1) {
		t.Error("超出上限应被拒绝")
	}
	// 失败时不得占用额度
	if got := l.Current(SubsPerConn, "c1"); got != 3 {
		t.Errorf("失败后当前占用 = %d，期望仍为 3", got)
	}
	// 一次申请多个，超限时整体失败
	l.Release(SubsPerConn, "c1", 2)
	if got := l.Current(SubsPerConn, "c1"); got != 1 {
		t.Fatalf("归还后当前占用 = %d，期望 1", got)
	}
	if l.Acquire(SubsPerConn, "c1", 3) {
		t.Error("剩余 2 时申请 3 应整体失败")
	}
	if got := l.Current(SubsPerConn, "c1"); got != 1 {
		t.Errorf("整体失败后当前占用 = %d，期望仍为 1", got)
	}
	if !l.Acquire(SubsPerConn, "c1", 2) {
		t.Error("剩余 2 时申请 2 应成功")
	}
}

func TestCountQuotaCleansUpOnZero(t *testing.T) {
	l, _ := newTestLimiter(nil)
	l.Acquire(SubsPerConn, "c1", 5)
	l.Release(SubsPerConn, "c1", 5)
	if got := l.Current(SubsPerConn, "c1"); got != 0 {
		t.Errorf("归零后当前占用 = %d", got)
	}
	// 归零即删除条目，避免长期运行后 map 无界增长
	sh := &l.counters[shardOf(SubsPerConn, "c1")]
	sh.mu.Lock()
	_, exists := sh.m[bucketKey{id: SubsPerConn, subject: "c1"}]
	sh.mu.Unlock()
	if exists {
		t.Error("计数归零后应删除条目")
	}
	// 归还多于占用不应让计数漂移为负
	l.Release(SubsPerConn, "c1", 10)
	if got := l.Current(SubsPerConn, "c1"); got != 0 {
		t.Errorf("过量归还后当前占用 = %d，期望 0", got)
	}
}

func TestCountQuotaWrongKind(t *testing.T) {
	l, _ := newTestLimiter(nil)
	// 速率配额不能走 Acquire
	if l.Acquire(PublishRatePerConn, "c", 1) {
		t.Error("速率配额走 Acquire 应被拒绝")
	}
	l.Release(PublishRatePerConn, "c", 1) // 不应 panic
	if l.Acquire(numQuotas, "c", 1) {
		t.Error("越界编号应被拒绝")
	}
	l.Release(numQuotas, "c", 1) // 不应 panic
}

func TestCountQuotaUnlimited(t *testing.T) {
	l, _ := newTestLimiter(map[ID]int{SubsPerConn: 0})
	for i := 0; i < 1000; i++ {
		if !l.Acquire(SubsPerConn, "c", 1) {
			t.Fatalf("取消限制后第 %d 次占用失败", i)
		}
	}
}

func TestSizeQuota(t *testing.T) {
	l, _ := newTestLimiter(nil)
	if !l.Within(PayloadSize, 128*1024) {
		t.Error("恰好等于上限应通过")
	}
	if l.Within(PayloadSize, 128*1024+1) {
		t.Error("超出上限应被拒绝")
	}
	if !l.Within(TopicSlashes, 7) || l.Within(TopicSlashes, 8) {
		t.Error("斜杠数配额判定不正确")
	}
	// 非大小型配额不能走 Within
	if l.Within(PublishRatePerConn, 1) {
		t.Error("速率配额走 Within 应被拒绝")
	}
	if l.Within(numQuotas, 1) {
		t.Error("越界编号应被拒绝")
	}
	// 取消限制
	l2, _ := newTestLimiter(map[ID]int{PayloadSize: 0})
	if !l2.Within(PayloadSize, 1<<30) {
		t.Error("取消限制后任意大小都应通过")
	}
}

func TestDefaultsMatchAWS(t *testing.T) {
	// 默认值对齐 AWS 以保证行为可预期（PRD QTA-001）。
	// 固定住若干关键项，防止被无意改动。
	l, _ := newTestLimiter(nil)
	for id, want := range map[ID]int{
		PayloadSize:          128 * 1024,
		PacketSize:           146 * 1024,
		TopicLen:             256,
		TopicSlashes:         7,
		ClientIDLen:          128,
		PublishRatePerConn:   100,
		InboundInflight:      100,
		OutboundInflight:     100,
		Connections:          500_000,
		ConnectRate:          3_000,
		ConnectRatePerClient: 1, // 防重连风暴的关键，不应放宽
		InboundPublishRate:   20_000,
		RetainedCount:        500_000,
		RetainedRatePerTopic: 1,
		ShadowDocSize:        8 * 1024,
		ShadowDepth:          8,
		CertRegisterRate:     15,
	} {
		if got := l.Limit(id); got != want {
			d, _ := Lookup(id)
			t.Errorf("%s 的默认值 = %d，期望 %d（%s）", d.Name, got, want, d.PRD)
		}
	}
	// 明确放宽于 AWS 的项
	for id, want := range map[ID]int{
		SubsPerConn:   200, // AWS 为 50
		TopicAliasMax: 16,  // AWS 为 8
	} {
		if got := l.Limit(id); got != want {
			d, _ := Lookup(id)
			t.Errorf("%s 的默认值 = %d，期望放宽为 %d", d.Name, got, want)
		}
	}
	if l.Limit(numQuotas) != 0 {
		t.Error("越界编号的限制值应为 0")
	}
}

func TestStats(t *testing.T) {
	l, _ := newTestLimiter(map[ID]int{PublishRatePerConn: 1})
	ctx := context.Background()

	l.AllowLocal(PublishRatePerConn, "c", 1) // 通过
	l.AllowLocal(PublishRatePerConn, "c", 1) // 拒绝
	l.Acquire(SubsPerConn, "c", 3)
	l.Within(PayloadSize, 1)
	l.AllowGlobal(ctx, ConnectRate, "", 1)

	byID := map[ID]Stat{}
	for _, s := range l.Stats() {
		byID[s.ID] = s
	}
	if s := byID[PublishRatePerConn]; s.Allowed != 1 || s.Rejected != 1 {
		t.Errorf("速率统计 = 通过 %d 拒绝 %d，期望 1 与 1", s.Allowed, s.Rejected)
	}
	if s := byID[SubsPerConn]; s.Current != 3 {
		t.Errorf("计数统计 Current = %d，期望 3", s.Current)
	}
	l.Release(SubsPerConn, "c", 3)
	for _, s := range l.Stats() {
		if s.ID == SubsPerConn && s.Current != 0 {
			t.Errorf("归还后 Current = %d，期望 0", s.Current)
		}
	}
	if s := byID[PayloadSize]; s.Allowed != 1 {
		t.Errorf("大小统计 Allowed = %d", s.Allowed)
	}
	if s := byID[ConnectRate]; s.Allowed != 1 {
		t.Errorf("全局统计 Allowed = %d", s.Allowed)
	}
	if len(l.Stats()) != int(numQuotas) {
		t.Error("统计条数应等于配额总数")
	}
}

func TestStringers(t *testing.T) {
	if ScopeLocal.String() != "本地" || ScopeGlobal.String() != "全局" {
		t.Error("Scope.String 不正确")
	}
	d, _ := Lookup(PublishRatePerConn)
	s := d.String()
	for _, want := range []string{"publish_rate_per_conn", "本地", "速率", "QTA-016"} {
		if !contains(s, want) {
			t.Errorf("Def.String() = %q，应含 %q", s, want)
		}
	}
	cd, _ := Lookup(SubsPerConn)
	if !contains(cd.String(), "计数") {
		t.Errorf("计数型配额的描述不正确: %s", cd.String())
	}
	sd, _ := Lookup(PayloadSize)
	if !contains(sd.String(), "大小") {
		t.Errorf("大小型配额的描述不正确: %s", sd.String())
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestConcurrentUse(t *testing.T) {
	l, clk := newTestLimiter(map[ID]int{PublishRatePerConn: 1000, SubsPerConn: 0})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subject := string(rune('a' + i%8))
			for j := 0; j < 200; j++ {
				l.AllowLocal(PublishRatePerConn, subject, 1)
				l.AllowGlobal(ctx, InboundPublishRate, "", 1)
				l.Acquire(SubsPerConn, subject, 1)
				l.Release(SubsPerConn, subject, 1)
				l.Within(PayloadSize, 1024)
				if j%50 == 0 {
					clk.advance(1)
				}
			}
		}(i)
	}
	wg.Wait()
	_ = l.Stats()
}

func BenchmarkAllowLocal(b *testing.B) {
	clk := &fakeClock{ms: 1}
	l := NewLocal(Options{Now: clk.now, Overrides: map[ID]int{PublishRatePerConn: 1 << 30}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.AllowLocal(PublishRatePerConn, "device-00123", 1) {
			b.Fatal("应通过")
		}
	}
}

func BenchmarkAllowLocalUniqueSubjects(b *testing.B) {
	clk := &fakeClock{ms: 1}
	l := NewLocal(Options{Now: clk.now, Overrides: map[ID]int{ConnectRatePerClient: 1}})
	subjects := make([]string, b.N)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("device-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range subjects {
		if !l.AllowGlobal(context.Background(), ConnectRatePerClient, subjects[i], 1) {
			b.Fatal("初次连接应通过")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(l.RateBucketCount()), "resident_buckets")
}

func BenchmarkSweepRateBuckets200K(b *testing.B) {
	const subjects = 200_000
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		clk := &fakeClock{ms: 1}
		l := NewLocal(Options{Now: clk.now, Overrides: map[ID]int{ConnectRatePerClient: 1}})
		for i := 0; i < subjects; i++ {
			l.AllowGlobal(context.Background(), ConnectRatePerClient,
				fmt.Sprintf("device-%d", i), 1)
		}
		clk.advance(1000)
		b.StartTimer()
		removed := l.SweepRateBuckets(bucketShards)
		b.StopTimer()
		if removed != subjects || l.RateBucketCount() != 0 {
			b.Fatalf("removed=%d remaining=%d", removed, l.RateBucketCount())
		}
	}
	b.ReportMetric(subjects, "buckets/op")
}

func BenchmarkWithin(b *testing.B) {
	l, _ := newTestLimiter(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !l.Within(PayloadSize, 1024) {
			b.Fatal("应通过")
		}
	}
}

func BenchmarkAcquireRelease(b *testing.B) {
	l, _ := newTestLimiter(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Acquire(SubsPerConn, "device-00123", 1)
		l.Release(SubsPerConn, "device-00123", 1)
	}
}
