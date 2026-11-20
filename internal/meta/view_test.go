package meta

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestViewSignaturesHaveNoContextOrError 是契约 05 与 PRD ARCH-021、
// ARCH-054 的断言，也是本包最重要的一条测试。
//
// MetaView 的方法签名里不得出现 context.Context 或 error：这个签名本身
// 就是「本地内存、不阻塞、不会失败」的承诺。一旦允许其中之一，就有人会在
// CONNECT 路径上加一发远程调用；而按配额，认证鉴权是 2 万次/秒量级，
// 届时握手吞吐会崩塌，并被迫重写认证路径补缓存与失效机制——
// 那比一开始就做物化视图难得多。
//
// CI 中另有同等的自定义 go vet 分析器。
func TestViewSignaturesHaveNoContextOrError(t *testing.T) {
	typ := reflect.TypeOf((*View)(nil))
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errType := reflect.TypeOf((*error)(nil)).Elem()

	if typ.NumMethod() == 0 {
		t.Fatal("View 没有导出方法，测试本身失效")
	}
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		ft := m.Type
		for j := 0; j < ft.NumIn(); j++ {
			if ft.In(j) == ctxType {
				t.Errorf("View.%s 的参数含 context.Context，违反契约 05", m.Name)
			}
		}
		for j := 0; j < ft.NumOut(); j++ {
			if ft.Out(j) == errType {
				t.Errorf("View.%s 的返回值含 error，违反契约 05", m.Name)
			}
		}
	}
}

func TestFingerprint(t *testing.T) {
	var fp CertFingerprint
	for i := range fp {
		fp[i] = byte(i)
	}
	s := fp.String()
	if len(s) != 64 {
		t.Fatalf("十六进制表示长度 = %d，期望 64", len(s))
	}
	got, ok := ParseFingerprint(s)
	if !ok || got != fp {
		t.Errorf("往返失败: ok=%v", ok)
	}
	for _, bad := range []string{"", "abc", s[:63], s + "0", "zz" + s[2:]} {
		if _, ok := ParseFingerprint(bad); ok {
			t.Errorf("%q 应被拒绝", bad)
		}
	}
}

func TestCertStatus(t *testing.T) {
	// 零值必须是 CertUnknown，使「查不到」与「未激活」默认都走拒绝分支
	var zero CertStatus
	if zero != CertUnknown {
		t.Error("CertStatus 的零值应为 CertUnknown")
	}
	if CertUnknown.CanConnect() || CertInactive.CanConnect() ||
		CertRevoked.CanConnect() || CertPendingActivation.CanConnect() {
		t.Error("只有 ACTIVE 可以建立新连接")
	}
	if !CertActive.CanConnect() {
		t.Error("ACTIVE 应可连接")
	}
	if CertRevoked.String() != "REVOKED" || CertStatus(99).String() != "UNKNOWN" {
		t.Error("CertStatus.String 不正确")
	}
}

func fp(n byte) CertFingerprint {
	var f CertFingerprint
	f[0] = n
	f[31] = n
	return f
}

func newTestStore() *LocalStore {
	now := int64(1_700_000_000_000)
	return NewLocalStore(LocalOptions{
		Now: func() int64 { return now }, Compile: stubCompile,
	})
}

func TestApplyCertAndRead(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	v := s.View()
	ctx := context.Background()

	f := fp(1)
	if v.CertStatus(f) != CertUnknown {
		t.Error("未注册证书应为 CertUnknown")
	}
	if v.CertPolicySet(f) != NoPolicySet {
		t.Error("未注册证书应无策略集")
	}
	if _, ok := v.Cert(f); ok {
		t.Error("未注册证书不应被找到")
	}

	rev, err := s.Apply(ctx, Command{
		ID: "c1", Kind: CmdSetCert,
		Cert: &CertCmd{
			Fingerprint: f, Status: CertActive, PolicySet: 7,
			Thing: "dev-001", CA: "ca-1", NotAfterUnixMs: 1_800_000_000_000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 || v.Revision() != 1 {
		t.Fatalf("revision = %d / %d，期望 1", rev, v.Revision())
	}
	if v.CertStatus(f) != CertActive {
		t.Errorf("状态 = %s", v.CertStatus(f))
	}
	if v.CertPolicySet(f) != 7 {
		t.Errorf("策略集 = %d", v.CertPolicySet(f))
	}
	c, ok := v.Cert(f)
	if !ok || c.Thing != "dev-001" || c.Status != CertActive {
		t.Errorf("证书热信息 = %+v ok=%v", c, ok)
	}

	// 未绑定设备时名字应为空而非占位值
	g := fp(2)
	if _, err := s.Apply(ctx, Command{
		ID: "c2", Kind: CmdSetCert,
		Cert: &CertCmd{Fingerprint: g, Status: CertPendingActivation},
	}); err != nil {
		t.Fatal(err)
	}
	c2, _ := v.Cert(g)
	if c2.Thing != "" {
		t.Errorf("未绑定时设备名应为空，得到 %+v", c2)
	}
}

func TestApplyIdempotent(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	cmd := Command{
		ID: "same", Kind: CmdSetCert,
		Cert: &CertCmd{Fingerprint: fp(1), Status: CertActive},
	}
	rev1, err := s.Apply(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	// 同一命令 ID 重复提交：返回首次 revision 且不推进日志
	for i := 0; i < 5; i++ {
		rev, err := s.Apply(ctx, cmd)
		if err != nil {
			t.Fatal(err)
		}
		if rev != rev1 {
			t.Fatalf("重复提交返回 revision %d，期望 %d", rev, rev1)
		}
	}
	if s.View().Revision() != rev1 {
		t.Errorf("revision 被重复提交推进到 %d", s.View().Revision())
	}

	// 空 ID 不做去重
	noID := Command{Kind: CmdNop}
	a, _ := s.Apply(ctx, noID)
	b, _ := s.Apply(ctx, noID)
	if a == b {
		t.Error("空 ID 的命令不应被去重")
	}
}

func TestApplyDedupWindowEviction(t *testing.T) {
	s := NewLocalStore(LocalOptions{DedupWindow: 3})
	defer s.Close()
	ctx := context.Background()

	first, _ := s.Apply(ctx, Command{ID: "a", Kind: CmdNop})
	for _, id := range []string{"b", "c", "d"} {
		if _, err := s.Apply(ctx, Command{ID: id, Kind: CmdNop}); err != nil {
			t.Fatal(err)
		}
	}
	// "a" 已被淘汰出窗口，重复提交会再次应用并得到新 revision。
	// 这与真实 Raft 状态机的有界去重窗口一致，因此命令本身应设计为幂等。
	again, err := s.Apply(ctx, Command{ID: "a", Kind: CmdNop})
	if err != nil {
		t.Fatal(err)
	}
	if again == first {
		t.Error("超出去重窗口的命令应被再次应用")
	}
}

func TestApplyRevisionConflict(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	if _, err := s.Apply(ctx, Command{ID: "a", Kind: CmdNop}); err != nil {
		t.Fatal(err)
	}
	// 当前 revision 为 1；要求 5 应冲突
	_, err := s.Apply(ctx, Command{
		ID: "b", Kind: CmdNop, RequestedRevision: 5,
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Errorf("应返回 ErrRevisionConflict，得到 %v", err)
	}
	// 要求当前值应成功
	if _, err := s.Apply(ctx, Command{
		ID: "c", Kind: CmdNop, RequestedRevision: 1,
	}); err != nil {
		t.Errorf("要求当前 revision 应成功，得到 %v", err)
	}
}

func TestApplyValidation(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	bad := []Command{
		{Kind: CmdSetCert},
		{Kind: CmdSetCert, Cert: &CertCmd{}},
		{Kind: CmdDeleteCert},
		{Kind: CmdSetThing},
		{Kind: CmdSetThing, Thing: &ThingCmd{}},
		{Kind: CmdDeleteThing},
		{Kind: CmdSetPolicySet},
		{Kind: CmdSetPolicySet, Policy: &PolicyCmd{ID: NoPolicySet}},
		// 缺文档：它在别的节点上编译不出匹配器，而进了视图之后
		// 表现为「这份策略什么都不放行」，没有任何报错
		{Kind: CmdSetPolicySet, Policy: &PolicyCmd{ID: 7, Name: "p"}},
		{Kind: CmdDeletePolicySet},
		{Kind: CmdKind(200)},
	}
	for i, cmd := range bad {
		if _, err := s.Apply(ctx, cmd); err == nil {
			t.Errorf("第 %d 条非法命令应被拒绝: %+v", i, cmd)
		}
	}
	if s.View().Revision() != 0 {
		t.Error("非法命令不应推进 revision")
	}
	if CmdSetCert.String() != "SetCert" || CmdKind(200).String() != "Unknown" {
		t.Error("CmdKind.String 不正确")
	}
}

func TestApplyReadOnlyAndClosed(t *testing.T) {
	s := newTestStore()
	ctx := context.Background()

	// 丢失 quorum 的行为：写拒绝，读继续（PRD CLU-042、NFR-030）
	if _, err := s.Apply(ctx, Command{
		ID: "a", Kind: CmdSetCert,
		Cert: &CertCmd{Fingerprint: fp(1), Status: CertActive},
	}); err != nil {
		t.Fatal(err)
	}
	s.SetReadOnly(true)
	if _, err := s.Apply(ctx, Command{ID: "b", Kind: CmdNop}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("只读下应返回 ErrReadOnly，得到 %v", err)
	}
	// 读路径不受影响
	if s.View().CertStatus(fp(1)) != CertActive {
		t.Error("只读下读取应继续可用")
	}
	s.SetReadOnly(false)
	if _, err := s.Apply(ctx, Command{ID: "b", Kind: CmdNop}); err != nil {
		t.Errorf("恢复后写入应成功，得到 %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("重复关闭应幂等，得到 %v", err)
	}
	if _, err := s.Apply(ctx, Command{ID: "z", Kind: CmdNop}); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("关闭后应返回 ErrStoreClosed，得到 %v", err)
	}
}

func TestThingAndPolicySet(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	v := s.View()
	ctx := context.Background()

	if _, ok := v.Thing("dev-1"); ok {
		t.Error("未注册设备不应被找到")
	}
	if v.ThingPolicySet("dev-1") != NoPolicySet {
		t.Error("未注册设备应无策略集")
	}

	// 先设定属性槽位，再写入设备
	if _, err := s.Apply(ctx, Command{
		ID: "slots", Kind: CmdSetAttrSlots,
		AttrSlots: []string{"site", "line"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := v.AttrSlots(); len(got) != 2 || got[0] != "site" {
		t.Fatalf("属性槽位 = %v", got)
	}
	if _, err := s.Apply(ctx, Command{
		ID: "t1", Kind: CmdSetThing,
		Thing: &ThingCmd{Name: "dev-1", PolicySet: 3, Attrs: []string{"sh", "l2"}},
	}); err != nil {
		t.Fatal(err)
	}
	th, ok := v.Thing("dev-1")
	if !ok || th.Name != "dev-1" || th.PolicySet != 3 || len(th.Attrs) != 2 {
		t.Fatalf("设备热信息 = %+v ok=%v", th, ok)
	}
	if v.ThingPolicySet("dev-1") != 3 {
		t.Errorf("策略集 = %d", v.ThingPolicySet("dev-1"))
	}
	if a, ok := v.ThingAttr("dev-1", "line"); !ok || a != "l2" {
		t.Errorf("属性 line = %q ok=%v", a, ok)
	}
	// 未被策略引用的属性不在热视图中可见
	if _, ok := v.ThingAttr("dev-1", "未引用"); ok {
		t.Error("未在槽位中的属性不应可见")
	}
	if _, ok := v.ThingAttr("不存在", "site"); ok {
		t.Error("不存在的设备不应返回属性")
	}

	// 策略集
	if _, ok := v.PolicySet(NoPolicySet); ok {
		t.Error("NoPolicySet 不应被找到")
	}
	if _, ok := v.PolicySet(3); ok {
		t.Error("尚未写入的策略集不应被找到")
	}
	if _, err := s.Apply(ctx, Command{
		ID: "p3", Kind: CmdSetPolicySet,
		Policy: &PolicyCmd{ID: 3, Name: "device-policy", Doc: []byte("占位匹配器")},
	}); err != nil {
		t.Fatal(err)
	}
	ps, ok := v.PolicySet(3)
	if !ok || ps.Name != "device-policy" || ps.Compiled != "占位匹配器" {
		t.Fatalf("策略集 = %+v ok=%v", ps, ok)
	}
	if ps.Revision == 0 {
		t.Error("策略集应记录其 revision")
	}

	// 删除
	for _, cmd := range []Command{
		{ID: "d1", Kind: CmdDeleteThing, Thing: &ThingCmd{Name: "dev-1"}},
		{ID: "d2", Kind: CmdDeletePolicySet, Policy: &PolicyCmd{ID: 3}},
	} {
		if _, err := s.Apply(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := v.Thing("dev-1"); ok {
		t.Error("删除后设备不应被找到")
	}
	if _, ok := v.PolicySet(3); ok {
		t.Error("删除后策略集不应被找到")
	}
	// 删除不存在的记录不应报错
	for _, cmd := range []Command{
		{ID: "d3", Kind: CmdDeleteThing, Thing: &ThingCmd{Name: "没有这个"}},
		{ID: "d4", Kind: CmdDeletePolicySet, Policy: &PolicyCmd{ID: 99}},
		{ID: "d5", Kind: CmdDeleteCert, Cert: &CertCmd{Fingerprint: fp(9)}},
	} {
		if _, err := s.Apply(ctx, cmd); err != nil {
			t.Errorf("删除不存在的记录应无错误: %v", err)
		}
	}
}

func TestThingWithoutAttrs(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Apply(ctx, Command{
		ID: "t", Kind: CmdSetThing, Thing: &ThingCmd{Name: "d", PolicySet: 1},
	}); err != nil {
		t.Fatal(err)
	}
	th, _ := s.View().Thing("d")
	if th.Attrs != nil {
		t.Errorf("无属性时应为 nil，得到 %v", th.Attrs)
	}
}
