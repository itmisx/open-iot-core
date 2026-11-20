package meta

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/itmisx/open-iot-core/internal/baseline"
)

// TestRevocationRequiresDisconnect 覆盖吊销链路的判定逻辑。
//
// 吊销必须立即断开所有在用该证书的活连接——仅改状态不断连视为未完成吊销
// （PRD PKI-030）。各节点在自己的 apply 循环里消费 Change 并断开本节点的
// 连接，广播由日志复制本身完成，不需要额外的跨节点失效通知机制（CLU-019）。
func TestRevocationRequiresDisconnect(t *testing.T) {
	cases := []struct {
		name string
		ch   Change
		want bool
	}{
		{"ACTIVE 转 REVOKED 须断连",
			Change{Kind: CmdSetCert, PrevStatus: CertActive, Status: CertRevoked}, true},
		{"ACTIVE 转 INACTIVE 须断连",
			Change{Kind: CmdSetCert, PrevStatus: CertActive, Status: CertInactive}, true},
		{"删除在用证书须断连",
			Change{Kind: CmdDeleteCert, PrevStatus: CertActive}, true},
		{"INACTIVE 转 ACTIVE 不须断连",
			Change{Kind: CmdSetCert, PrevStatus: CertInactive, Status: CertActive}, false},
		{"REVOKED 转 REVOKED 不须断连",
			Change{Kind: CmdSetCert, PrevStatus: CertRevoked, Status: CertRevoked}, false},
		{"新注册证书不须断连",
			Change{Kind: CmdSetCert, PrevStatus: CertUnknown, Status: CertActive}, false},
		{"删除未激活证书不须断连",
			Change{Kind: CmdDeleteCert, PrevStatus: CertInactive}, false},
		{"设备变更不须断连",
			Change{Kind: CmdSetThing}, false},
	}
	for _, tc := range cases {
		if got := tc.ch.RequiresDisconnect(); got != tc.want {
			t.Errorf("%s: RequiresDisconnect = %v，期望 %v", tc.name, got, tc.want)
		}
	}
}

// TestRevocationEndToEnd 走完吊销的端到端链路：
// 提交命令 → 视图更新 → 变更发布 → 判定须断连。
func TestRevocationEndToEnd(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	v := s.View()
	ctx := context.Background()
	f := fp(1)

	if _, err := s.Apply(ctx, Command{
		ID: "reg", Kind: CmdSetCert,
		Cert: &CertCmd{Fingerprint: f, Status: CertActive, Thing: "dev-1"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := <-s.Changes()
	if reg.RequiresDisconnect() {
		t.Error("新注册不应要求断连")
	}
	if !v.CertStatus(f).CanConnect() {
		t.Fatal("注册后应可连接")
	}

	if _, err := s.Apply(ctx, Command{
		ID: "rev", Kind: CmdSetCert,
		Cert: &CertCmd{
			Fingerprint: f, Status: CertRevoked, Thing: "dev-1",
			Reason: "密钥泄露",
		},
	}); err != nil {
		t.Fatal(err)
	}
	ch := <-s.Changes()
	if !ch.RequiresDisconnect() {
		t.Error("吊销应要求断连")
	}
	if ch.Fingerprint != f || ch.Reason != "密钥泄露" {
		t.Errorf("变更记录 = %+v", ch)
	}
	if v.CertStatus(f).CanConnect() {
		t.Error("吊销后不应可连接")
	}
	// 已吊销证书不得被重新激活（PRD PKI-035）——该约束由控制面在提交前
	// 校验，本层不阻止覆盖写，故此处只记录当前行为。
	if _, err := s.Apply(ctx, Command{
		ID: "re", Kind: CmdSetCert,
		Cert: &CertCmd{Fingerprint: f, Status: CertActive},
	}); err != nil {
		t.Fatal(err)
	}
	if !v.CertStatus(f).CanConnect() {
		t.Error("状态机本身不阻止覆盖写，该约束在控制面实施")
	}
}

// TestChangeBufferNeverBlocks 验证变更通道满时 apply 不被阻塞。
// 变更已经落到视图上，通知只是用于加速连接清理（PRD OBS-005）。
func TestChangeBufferNeverBlocks(t *testing.T) {
	s := NewLocalStore(LocalOptions{ChangeBuffer: 2})
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if _, err := s.Apply(ctx, Command{
			ID: fmt.Sprintf("c%d", i), Kind: CmdNop,
		}); err != nil {
			t.Fatalf("第 %d 次提交被阻塞或失败: %v", i, err)
		}
	}
	if s.View().Revision() != 50 {
		t.Errorf("revision = %d，期望 50", s.View().Revision())
	}
	if s.Dropped() == 0 {
		t.Error("通道满时应计数被丢弃的通知")
	}
}

// TestViewStaleFailClosed 覆盖视图陈旧的 fail-closed 行为。
//
// 这是自建场景独有而托管服务不存在的问题：网络分区中的少数派节点，
// 其视图冻结在最后提交的 revision 上，可能仍接受一张在多数派侧已吊销的
// 证书。因此超过阈值后对新连接 fail-closed，但不得断开已有连接
// （PRD CLU-023 至 CLU-025）。
func TestViewStaleFailClosed(t *testing.T) {
	const maxStaleMs = 30_000
	v := NewView()

	// 从未确认过追平：视为陈旧，节点在 ready 之前不应接受设备连接（CLU-032）
	if !v.Stale(1_700_000_000_000, maxStaleMs) {
		t.Error("从未确认追平时应判为陈旧")
	}

	const t0 = 1_700_000_000_000
	v.confirmedAtUnixMs.Store(t0)
	if v.LastConfirmedUnixMs() != t0 {
		t.Errorf("确认时间 = %d", v.LastConfirmedUnixMs())
	}
	for _, tc := range []struct {
		now  int64
		want bool
	}{
		{t0, false},
		{t0 + maxStaleMs, false},    // 恰好到阈值仍可用
		{t0 + maxStaleMs + 1, true}, // 超过阈值
		{t0 + 10*maxStaleMs, true},
	} {
		if got := v.Stale(tc.now, maxStaleMs); got != tc.want {
			t.Errorf("now=%d 时 Stale = %v，期望 %v", tc.now, got, tc.want)
		}
	}
	// 阈值为 0 或负数表示关闭陈旧度检查
	if v.Stale(t0+1<<40, 0) {
		t.Error("阈值为 0 时应关闭陈旧度检查")
	}
}

func TestMarkConfirmed(t *testing.T) {
	s := NewLocalStore(LocalOptions{})
	defer s.Close()
	v := s.View()
	// 未注入时钟时初始未确认
	if v.LastConfirmedUnixMs() != 0 {
		t.Error("未注入时钟时初始应未确认")
	}
	if !v.Stale(1000, 100) {
		t.Error("未确认时应判为陈旧")
	}
	s.MarkConfirmed(1000)
	if v.Stale(1050, 100) {
		t.Error("确认后不应判为陈旧")
	}
}

func TestLocalStoreMarksConfirmedOnApply(t *testing.T) {
	now := int64(1000)
	s := NewLocalStore(LocalOptions{Now: func() int64 { return now }})
	defer s.Close()
	v := s.View()
	// 单副本永远是追平状态，初始即已确认
	if v.LastConfirmedUnixMs() != 1000 {
		t.Errorf("初始确认时间 = %d，期望 1000", v.LastConfirmedUnixMs())
	}
	now = 5000
	if _, err := s.Apply(context.Background(), Command{ID: "a", Kind: CmdNop}); err != nil {
		t.Fatal(err)
	}
	if v.LastConfirmedUnixMs() != 5000 {
		t.Errorf("Apply 后确认时间 = %d，期望 5000", v.LastConfirmedUnixMs())
	}
}

func TestNameTableInterning(t *testing.T) {
	tb := newNameTable()
	if tb.Len() != 0 {
		t.Errorf("初始长度 = %d", tb.Len())
	}
	if _, ok := tb.find("nope"); ok {
		t.Error("未驻留的名字不应被找到")
	}
	if tb.at(noRef) != "" {
		t.Error("noRef 应解析为空串")
	}

	refs := make(map[string]nameRef)
	for i := 0; i < 3000; i++ {
		name := fmt.Sprintf("dev-%06d", i)
		r := tb.intern(name)
		refs[name] = r
		if tb.at(r) != name {
			t.Fatalf("引用 %d 回取为 %q，期望 %q", r, tb.at(r), name)
		}
	}
	if tb.Len() != 3000 {
		t.Errorf("长度 = %d，期望 3000", tb.Len())
	}
	// 去重：重复驻留返回同一引用
	for name, r := range refs {
		if got := tb.intern(name); got != r {
			t.Fatalf("重复驻留 %q 返回 %d，期望 %d", name, got, r)
		}
		if got, ok := tb.find(name); !ok || got != r {
			t.Fatalf("find(%q) = (%d, %v)", name, got, ok)
		}
	}
}

// TestViewMemoryBudget 验证 PRD CLU-021 与 NFR-020：
// 百万设备的热视图内存须控制在 250 MB 以内。
//
// 这是「每节点全量元数据」这一设计可行性的前提：热路径只需要元数据的
// 极小子集（指纹到状态、指纹到策略集、策略变量引用的字段），
// 而不是完整的 Thing 记录。两张表都以 int32 的名字引用为键，
// 设备名在驻留缓冲里只存一份；证书的到期时间与签发 CA 不入热视图
// （CLU-053），设备属性经侧表间接引用（CLU-054）。
//
// 预算中约三分之一是 Go map 按 2 的幂扩容造成的槽位浪费，预分配无法规避；
// 若需进一步压低须改用定长开放寻址表，属后续优化。
func TestViewMemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("规模测试，-short 下跳过")
	}
	const devices = 1_000_000

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	v := NewView()
	for i := 0; i < devices; i++ {
		name := fmt.Sprintf("dev-%07d", i)
		var f CertFingerprint
		// 构造分布均匀的指纹
		f[0] = byte(i)
		f[1] = byte(i >> 8)
		f[2] = byte(i >> 16)
		f[31] = byte(i * 31)
		v.putCert(&CertCmd{
			Fingerprint: f, Status: CertActive, PolicySet: PolicySetID(i % 100),
			Thing: name, NotAfterUnixMs: 1_800_000_000_000,
		})
		v.putThing(&ThingCmd{Name: name, PolicySet: PolicySetID(i % 100)})
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	heapMB := float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)

	st := v.Stats()
	t.Logf("证书 %d 条、设备 %d 条、名字 %d 个，热视图 %.1f MB（每设备约 %.0f 字节）",
		st.Certs, st.Things, st.Names, heapMB,
		float64(after.HeapAlloc-before.HeapAlloc)/float64(devices))

	if st.Certs != devices || st.Things != devices {
		t.Fatalf("规模不符: certs=%d things=%d", st.Certs, st.Things)
	}
	// 名字只驻留一份，证书表与设备表共用
	if st.Names != devices {
		t.Errorf("驻留名字数 = %d，期望 %d（两张表应共用同一份名字）", st.Names, devices)
	}
	if heapMB > 250 {
		t.Errorf("热视图 %.1f MB，超出 PRD CLU-021 的 250 MB 预算", heapMB)
	}
	// 250 MB 是绝对预算，留了很大余量。10% 比较的是公开基线里的每设备字节，
	// 热视图多塞一个字段就会过线。拿掉这次比较，预算放宽到 250 MB 仍是绿的。
	per := int((after.HeapAlloc - before.HeapAlloc) / uint64(devices))
	if err := baseline.Check("hotViewBytesPerDevice", per); err != nil {
		t.Error(err)
	}
	runtime.KeepAlive(v)
}

func BenchmarkCertStatus(b *testing.B) {
	v := NewView()
	const n = 100_000
	fps := make([]CertFingerprint, n)
	for i := 0; i < n; i++ {
		var f CertFingerprint
		f[0], f[1], f[31] = byte(i), byte(i>>8), byte(i*31)
		fps[i] = f
		v.putCert(&CertCmd{Fingerprint: f, Status: CertActive, PolicySet: 1})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !v.CertStatus(fps[i%n]).CanConnect() {
			b.Fatal("应可连接")
		}
	}
}

func BenchmarkCertStatusParallel(b *testing.B) {
	v := NewView()
	const n = 100_000
	fps := make([]CertFingerprint, n)
	for i := 0; i < n; i++ {
		var f CertFingerprint
		f[0], f[1], f[31] = byte(i), byte(i>>8), byte(i*31)
		fps[i] = f
		v.putCert(&CertCmd{Fingerprint: f, Status: CertActive, PolicySet: 1})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = v.CertStatus(fps[i%n])
			i++
		}
	})
}

func BenchmarkThingLookup(b *testing.B) {
	v := NewView()
	const n = 100_000
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("dev-%07d", i)
		v.putThing(&ThingCmd{Name: names[i], PolicySet: 1})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := v.Thing(names[i%n]); !ok {
			b.Fatal("应找到")
		}
	}
}

func BenchmarkApplySetCert(b *testing.B) {
	s := NewLocalStore(LocalOptions{ChangeBuffer: 1})
	defer s.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var f CertFingerprint
		// f[2] 恒为 1：仅靠 i 的三个字节在 i=65536 处会同时归零，
		// 而全零指纹会被 Validate 正确拒绝。
		f[0], f[1], f[2], f[31] = byte(i), byte(i>>8), 1, byte(i*31)
		if _, err := s.Apply(ctx, Command{
			Kind: CmdSetCert,
			Cert: &CertCmd{Fingerprint: f, Status: CertActive},
		}); err != nil {
			b.Fatal(err)
		}
	}
}
