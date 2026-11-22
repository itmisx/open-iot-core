package registry

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

const testNowMs = 1_700_000_000_000

type rig struct {
	t    *testing.T
	kv   store.KV
	meta *meta.LocalStore
	r    *Registry
	ms   int64
}

func newRig(t *testing.T) *rig {
	t.Helper()
	g := &rig{t: t, ms: testNowMs}
	g.kv = store.NewMemKV()
	g.meta = meta.NewLocalStore(meta.LocalOptions{
		Now: g.now, ChangeBuffer: 256, Cold: g.kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	r, err := New(Options{KV: g.kv, Meta: g.meta, Env: authz.DefaultEnv(), Now: g.now})
	if err != nil {
		t.Fatal(err)
	}
	g.r = r
	t.Cleanup(func() {
		_ = g.meta.Close()
		_ = g.kv.Close()
	})
	return g
}

func (g *rig) now() int64 { return g.ms }

// reopen 用同一个 KV 重建注册表与热视图，模拟重启。
func (g *rig) reopen() *rig {
	g.t.Helper()
	_ = g.meta.Close()
	n := &rig{t: g.t, kv: g.kv, ms: g.ms}
	n.meta = meta.NewLocalStore(meta.LocalOptions{
		Now: n.now, ChangeBuffer: 256, Cold: n.kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	r, err := New(Options{KV: n.kv, Meta: n.meta, Env: authz.DefaultEnv(), Now: n.now})
	if err != nil {
		g.t.Fatal(err)
	}
	n.r = r
	if _, err := r.Restore(context.Background()); err != nil {
		g.t.Fatalf("恢复失败: %v", err)
	}
	g.t.Cleanup(func() { _ = n.meta.Close() })
	return n
}

const simplePolicy = `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`

func TestPutThingsIsOneAtomicRevision(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	before := g.meta.View().Revision()
	if err := g.r.PutThings(ctx, []Thing{
		{Name: "batch-a", PolicyName: "p1", Attributes: map[string]string{"site": "sh"}},
		{Name: "batch-b", PolicyName: "p1"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := g.meta.View().Revision(); got != before+1 {
		t.Fatalf("批量写推进了 %d 条日志，期望 1", got-before)
	}
	for _, name := range []string{"batch-a", "batch-b"} {
		got, err := g.r.GetThing(name)
		if err != nil || got.Version != 1 || got.PolicyName != "p1" {
			t.Fatalf("设备 %s = %+v, %v", name, got, err)
		}
	}

	before = g.meta.View().Revision()
	err := g.r.PutThings(ctx, []Thing{{Name: "would-write"}, {Name: "bad/name"}})
	if err == nil {
		t.Fatal("非法批次应失败")
	}
	if g.meta.View().Revision() != before {
		t.Fatal("非法批次不应提交日志")
	}
	if _, err := g.r.GetThing("would-write"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("非法批次发生部分写入: %v", err)
	}
}

// fp 构造一个合法且非全零的指纹，且必含字母，使大小写相关的用例有意义。
//
// 全零指纹会被 meta 正确拒绝（它是零值，用于区分"查不到"与"未激活"），
// 所以这里刻意避开。
func fp(b byte) string {
	hexd := "abcdef0123456789"
	return strings.Repeat(string(hexd[b%16]), 63) + "a"
}

// ---------- 策略 ----------

func TestPolicyCRUD(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "p1" || string(got.Document) != simplePolicy {
		t.Errorf("= %+v", got)
	}
	// 热视图里应能查到
	id, _ := g.r.PolicySetID("p1")
	if _, ok := g.meta.View().PolicySet(id); !ok {
		t.Error("策略未进入热视图")
	}

	names, err := g.r.ListPolicies()
	if err != nil || len(names) != 1 || names[0] != "p1" {
		t.Errorf("= %v, %v", names, err)
	}
	if _, err := g.r.GetPolicy("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("= %v，期望 ErrNotFound", err)
	}
	if err := g.r.DeletePolicy(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetPolicy("p1"); !errors.Is(err, ErrNotFound) {
		t.Error("删除后应查不到")
	}
}

// TestBadPolicyRejected 验证编译不过的策略被拒绝写入。
//
// 写进去等于让该策略下的所有设备在下次重启时静默降权或提权（REG-103）。
func TestBadPolicyRejected(t *testing.T) {
	g := newRig(t)
	bad := []string{
		`{"Statement":[{"Effect":"Maybe","Action":"iot:*","Resource":"*"}]}`,
		`{not json`,
		`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
		`{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*","Condition":{}}]}`,
	}
	for _, doc := range bad {
		if err := g.r.PutPolicy(context.Background(), "p", []byte(doc)); err == nil {
			t.Errorf("应被拒: %s", doc)
		}
	}
	if _, err := g.r.GetPolicy("p"); !errors.Is(err, ErrNotFound) {
		t.Error("被拒的策略不应留下记录")
	}
}

// TestPolicyIDsStableAcrossRestart 是一条安全关键的不变量。
//
// 编号被证书与设备记录引用。重用已删除策略的编号会让旧引用指向
// 新策略——设备拿到别人的权限，而日志里什么异常都看不到。
func TestPolicyIDsStableAcrossRestart(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	for _, n := range []string{"zeta", "alpha", "mid"} {
		if err := g.r.PutPolicy(ctx, n, []byte(simplePolicy)); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]meta.PolicySetID{}
	for _, n := range []string{"zeta", "alpha", "mid"} {
		id, ok := g.r.PolicySetID(n)
		if !ok {
			t.Fatalf("策略 %s 没有编号", n)
		}
		before[n] = id
	}

	g2 := g.reopen()
	for n, want := range before {
		if got, _ := g2.r.PolicySetID(n); got != want {
			t.Errorf("策略 %s 的编号 %d != %d；编号跨重启必须稳定", n, got, want)
		}
	}

	// 新增一个名字排在最前的策略，不得改动既有编号
	if err := g2.r.PutPolicy(ctx, "aaa", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for n, want := range before {
		if got, _ := g2.r.PolicySetID(n); got != want {
			t.Errorf("新增策略后 %s 的编号从 %d 变成 %d", n, want, got)
		}
	}
}

// TestDeletePolicyInUseRejected 验证被引用的策略不能删。
func TestDeletePolicyInUseRejected(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	err := g.r.DeletePolicy(ctx, "p1")
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("= %v，期望 ErrInUse", err)
	}
	// 错误须指出是谁在引用，否则运维不知道该去哪里解绑
	if !strings.Contains(err.Error(), "d1") {
		t.Errorf("错误应指出引用者: %v", err)
	}

	// 证书引用同样挡住删除
	if err := g.r.PutThing(ctx, Thing{Name: "d1"}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(1), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.DeletePolicy(ctx, "p1"); !errors.Is(err, ErrInUse) {
		t.Errorf("被证书引用时也应拒绝: %v", err)
	}
}

// ---------- 设备 ----------

func TestThingCRUD(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}

	in := Thing{Name: "dev-1", PolicyName: "p1",
		Attributes: map[string]string{"site": "sh", "zone": "a"}}
	if err := g.r.PutThing(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetThing("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.PolicyName != "p1" || got.Attributes["site"] != "sh" {
		t.Errorf("= %+v", got)
	}
	// 热视图里应能查到策略绑定
	if ps := g.meta.View().ThingPolicySet("dev-1"); ps == meta.NoPolicySet {
		t.Error("设备未进入热视图")
	}

	// 更新保留创建时间
	g.ms += 5000
	in.Attributes = map[string]string{"site": "bj"}
	if err := g.r.PutThing(ctx, in); err != nil {
		t.Fatal(err)
	}
	got2, _ := g.r.GetThing("dev-1")
	if got2.CreatedAtUnixMs != got.CreatedAtUnixMs {
		t.Error("更新不应改动创建时间")
	}
	if got2.UpdatedAtUnixMs == got.UpdatedAtUnixMs {
		t.Error("更新应刷新更新时间")
	}

	if err := g.r.DeleteThing(ctx, "dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetThing("dev-1"); !errors.Is(err, ErrNotFound) {
		t.Error("删除后应查不到")
	}
	if err := g.r.DeleteThing(ctx, "dev-1"); !errors.Is(err, ErrNotFound) {
		t.Error("删除不存在的设备应返回 ErrNotFound")
	}
}

func TestThingValidation(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	cases := []struct {
		name string
		t    Thing
	}{
		{"名字为空", Thing{}},
		{"名字过长", Thing{Name: strings.Repeat("a", 129)}},
		{"名字含非法字符", Thing{Name: "dev/1"}},
		{"名字含空格", Thing{Name: "dev 1"}},
		{"属性过多", Thing{Name: "d", Attributes: map[string]string{
			"a": "1", "b": "2", "c": "3", "d": "4",
		}}},
		{"绑定不存在的策略", Thing{Name: "d", PolicyName: "nope"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := g.r.PutThing(ctx, c.t); err == nil {
				t.Error("应被拒")
			}
		})
	}
}

// TestDeleteThingWithCertificatesRejected 是 PRD REG-005。
//
// 直接删掉会留下指向不存在设备的证书，那些证书下次连接时解析不出
// thingName，策略变量求值失败而被拒，而错误完全看不出根因是这次删除。
func TestDeleteThingWithCertificatesRejected(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(2), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	err := g.r.DeleteThing(ctx, "d1")
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("= %v，期望 ErrInUse", err)
	}
	if !strings.Contains(err.Error(), "证书") {
		t.Errorf("错误应说明是证书绑定: %v", err)
	}

	// 解除绑定后可删
	if err := g.r.DeleteCertificate(ctx, fp(2)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.DeleteThing(ctx, "d1"); err != nil {
		t.Errorf("解除绑定后应可删: %v", err)
	}
}

func TestListThingsPaging(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		name := "dev-" + string(rune('a'+i/5)) + string(rune('0'+i%5))
		if err := g.r.PutThing(ctx, Thing{Name: name, PolicyName: "p1"}); err != nil {
			t.Fatal(err)
		}
	}
	// 分页须服务端完成（REG-044）
	seen := map[string]bool{}
	after := ""
	pages := 0
	for {
		page, err := g.r.ListThings(Query{Limit: 10, After: after})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, th := range page.Things {
			if seen[th.Name] {
				t.Errorf("设备 %s 出现在多页里", th.Name)
			}
			seen[th.Name] = true
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
		if pages > 10 {
			t.Fatal("分页未收敛")
		}
	}
	if len(seen) != 25 {
		t.Errorf("共见到 %d 个设备，期望 25", len(seen))
	}
	if pages < 3 {
		t.Errorf("分页数 = %d，limit=10 下 25 条应至少 3 页", pages)
	}

	// 前缀筛选
	page, err := g.r.ListThings(Query{Prefix: "dev-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Things) != 5 {
		t.Errorf("前缀筛选得到 %d 条，期望 5", len(page.Things))
	}
	// 单页上限
	if got := (Query{Limit: 99999}).limit(); got != MaxPageLimit {
		t.Errorf("单页上限 = %d，期望 %d", got, MaxPageLimit)
	}
}

// ---------- 属性槽位 ----------

// TestAttrSlotsOnlyMaterializePolicyReferenced 是 PRD CLU-053、REG-006。
//
// 热视图只物化策略真正引用的属性。百万设备下，materialize 全部属性
// 是几十 MB 的无用内存，而绝大多数属性从不被策略引用。
func TestAttrSlotsOnlyMaterializePolicyReferenced(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	// 策略只引用 site
	if err := g.r.PutPolicy(ctx, "p1", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.Attributes[site]}/*"}
	]}`)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.SyncAttrSlots(ctx); err != nil {
		t.Fatal(err)
	}
	slots := g.meta.View().AttrSlots()
	if len(slots) != 1 || slots[0] != "site" {
		t.Fatalf("槽位 = %v，期望 [site]", slots)
	}

	if err := g.r.PutThing(ctx, Thing{
		Name: "d1", PolicyName: "p1",
		Attributes: map[string]string{"site": "sh", "unused": "x"},
	}); err != nil {
		t.Fatal(err)
	}
	// 被引用的属性在热视图里
	if v, ok := g.meta.View().ThingAttr("d1", "site"); !ok || v != "sh" {
		t.Errorf("site = %q, %v", v, ok)
	}
	// 未被引用的属性不在热视图里，但在冷层里
	if _, ok := g.meta.View().ThingAttr("d1", "unused"); ok {
		t.Error("未被策略引用的属性不应进入热视图")
	}
	got, err := g.r.GetThing("d1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Attributes["unused"] != "x" {
		t.Error("未被引用的属性仍应保存在冷层")
	}
}

// TestAttrSlotChangeRewritesThings 验证槽位变更后设备投影被重算。
//
// 投影是按槽位下标的。槽位顺序一变，旧的投影结果就指向了别的属性名——
// 策略会读到错的属性值，是一类静默的权限错误。
func TestAttrSlotChangeRewritesThings(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.Attributes[site]}/*"}
	]}`)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.SyncAttrSlots(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{
		Name: "d1", PolicyName: "p1",
		Attributes: map[string]string{"site": "sh", "zone": "a"},
	}); err != nil {
		t.Fatal(err)
	}

	// 新增一个引用 zone 的策略，槽位变为 [site zone]
	if err := g.r.PutPolicy(ctx, "p2", []byte(`{"Statement":[
	  {"Effect":"Allow","Action":"iot:Publish",
	   "Resource":"arn:aws:iot:*:*:topic/${iot:Connection.Thing.Attributes[zone]}/*"}
	]}`)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.SyncAttrSlots(ctx); err != nil {
		t.Fatal(err)
	}
	if slots := g.meta.View().AttrSlots(); len(slots) != 2 {
		t.Fatalf("槽位 = %v，期望两项", slots)
	}
	// 两个属性都须解析正确——若未重写设备记录，zone 会读到空值
	for attr, want := range map[string]string{"site": "sh", "zone": "a"} {
		if v, ok := g.meta.View().ThingAttr("d1", attr); !ok || v != want {
			t.Errorf("%s = %q, %v，期望 %q；槽位变更后未重写设备投影",
				attr, v, ok, want)
		}
	}
}

func TestProjectAttrs(t *testing.T) {
	// 尾部空值被省略，节省内存
	got := projectAttrs([]string{"a", "b", "c"}, map[string]string{"a": "1"})
	if len(got) != 1 || got[0] != "1" {
		t.Errorf("= %v，期望 [1]", got)
	}
	// 中间的空值须保留，否则下标会错位
	got = projectAttrs([]string{"a", "b", "c"}, map[string]string{"a": "1", "c": "3"})
	if len(got) != 3 || got[0] != "1" || got[1] != "" || got[2] != "3" {
		t.Errorf("= %v，期望 [1 \"\" 3]；中间的空值不能省略", got)
	}
	if projectAttrs(nil, map[string]string{"a": "1"}) != nil {
		t.Error("无槽位时应返回 nil")
	}
	if projectAttrs([]string{"a"}, nil) != nil {
		t.Error("无属性时应返回 nil")
	}
}

// ---------- 证书 ----------

func TestCertificateCRUD(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}

	in := Certificate{
		Fingerprint: fp(3), PolicyName: "p1", Thing: "d1",
		Subject: "CN=d1", Issuer: "CN=CA", NotAfterUnixMs: testNowMs + 1000,
	}
	if err := g.r.PutCertificate(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCertificate(fp(3))
	if err != nil {
		t.Fatal(err)
	}
	// 未指定状态时默认 ACTIVE
	if got.Status != StatusActive {
		t.Errorf("状态 = %q，期望 %q", got.Status, StatusActive)
	}
	if got.Subject != "CN=d1" || got.NotAfterUnixMs != testNowMs+1000 {
		t.Errorf("= %+v", got)
	}
	// 热视图里应可连接
	mfp, _ := parseFingerprint(fp(3))
	if st := g.meta.View().CertStatus(mfp); !st.CanConnect() {
		t.Errorf("热视图中的状态 = %v，应可连接", st)
	}

	certs, err := g.r.CertificatesOfThing("d1")
	if err != nil || len(certs) != 1 {
		t.Errorf("= %v, %v", certs, err)
	}
}

func TestCertificateValidation(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		c    Certificate
	}{
		{"指纹过短", Certificate{Fingerprint: "abc", PolicyName: "p1"}},
		{"指纹大写", Certificate{Fingerprint: strings.ToUpper(fp(4)), PolicyName: "p1"}},
		{"指纹非十六进制", Certificate{Fingerprint: strings.Repeat("z", 64), PolicyName: "p1"}},
		{"未绑定策略", Certificate{Fingerprint: fp(4)}},
		{"策略不存在", Certificate{Fingerprint: fp(4), PolicyName: "nope"}},
		{"设备不存在", Certificate{Fingerprint: fp(4), PolicyName: "p1", Thing: "nope"}},
		{"状态非法", Certificate{Fingerprint: fp(4), PolicyName: "p1", Status: "ENABLED"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := g.r.PutCertificate(ctx, c.c); err == nil {
				t.Error("应被拒")
			}
		})
	}
}

// TestSetCertificateStatusUpdatesHotViewFirst 是 PKI-030 的落点。
//
// 状态变更必须先进热视图：顺序反过来的话，两步之间的窗口里冷层说
// "已吊销"而热视图还放行——那个窗口正是吊销失效的样子。
func TestSetCertificateStatusUpdatesHotViewFirst(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(5), PolicyName: "p1",
	}); err != nil {
		t.Fatal(err)
	}
	mfp, _ := parseFingerprint(fp(5))

	// 消费掉登记产生的变更
	drain(g.meta)

	if err := g.r.SetCertificateStatus(ctx, fp(5), StatusRevoked, "密钥泄露"); err != nil {
		t.Fatal(err)
	}
	// 热视图已不可连接
	if st := g.meta.View().CertStatus(mfp); st.CanConnect() {
		t.Error("吊销后热视图仍放行")
	}
	// 冷层也已更新
	got, err := g.r.GetCertificate(fp(5))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusRevoked || got.RevokeReason != "密钥泄露" {
		t.Errorf("= %+v", got)
	}

	// 须发出一条要求断连的变更——broker 据此踢掉在用连接
	found := false
	for _, ch := range drain(g.meta) {
		if ch.Fingerprint == mfp && ch.RequiresDisconnect() {
			found = true
			if ch.Reason != "密钥泄露" {
				t.Errorf("变更中的原因 = %q", ch.Reason)
			}
		}
	}
	if !found {
		t.Error("吊销须发出要求断连的变更，否则在用连接不会被踢掉")
	}
}

// drain 取出当前已积压的变更。
func drain(s *meta.LocalStore) []meta.Change {
	var out []meta.Change
	for {
		select {
		case ch, ok := <-s.Changes():
			if !ok {
				return out
			}
			out = append(out, ch)
		default:
			return out
		}
	}
}

// TestDeleteCertificateRevokesFirst 验证删除登记前先吊销。
//
// 仅删除登记不会触发"变为不可连接"的转换，于是在用的连接会一直留着，
// 直到它自己断开——那等于一次未完成的吊销。
func TestDeleteCertificateRevokesFirst(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(6), PolicyName: "p1",
	}); err != nil {
		t.Fatal(err)
	}
	drain(g.meta)

	if err := g.r.DeleteCertificate(ctx, fp(6)); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ch := range drain(g.meta) {
		if ch.RequiresDisconnect() {
			found = true
		}
	}
	if !found {
		t.Error("删除证书登记前须先发出要求断连的变更")
	}
	if _, err := g.r.GetCertificate(fp(6)); !errors.Is(err, ErrNotFound) {
		t.Error("删除后应查不到")
	}
}

func TestListCertificatesFiltering(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"d1", "d2"} {
		if err := g.r.PutThing(ctx, Thing{Name: n, PolicyName: "p1"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6; i++ {
		thing := "d1"
		status := StatusActive
		if i%2 == 1 {
			thing = "d2"
			status = StatusRevoked
		}
		if err := g.r.PutCertificate(ctx, Certificate{
			Fingerprint: fpN(i), PolicyName: "p1", Thing: thing, Status: status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := g.r.ListCertificates(CertQuery{Status: StatusRevoked})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Certificates) != 3 {
		t.Errorf("按状态筛选得到 %d 条，期望 3", len(page.Certificates))
	}
	page, err = g.r.ListCertificates(CertQuery{Thing: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Certificates) != 3 {
		t.Errorf("按设备筛选得到 %d 条，期望 3", len(page.Certificates))
	}
}

// fpN 构造第 n 个互不相同的合法指纹。
//
// 前缀固定为 "a"，避免产出全零指纹——那是 meta 的零值，会被拒绝。
func fpN(n int) string {
	hexd := "0123456789abcdef"
	return "a" + strings.Repeat("0", 61) +
		string(hexd[(n/16)%16]) + string(hexd[n%16])
}

// TestStatusToMetaUnknownIsNotConnectable 验证未知状态最保守处理。
//
// 返回可连接会让一个拼错的状态值变成放行。
func TestStatusToMetaUnknownIsNotConnectable(t *testing.T) {
	if statusToMeta("FROBNICATED").CanConnect() {
		t.Error("未知状态不得映射为可连接")
	}
	if !statusToMeta(StatusActive).CanConnect() {
		t.Error("ACTIVE 应可连接")
	}
	for _, s := range []string{StatusInactive, StatusRevoked, StatusPendingActivation} {
		if statusToMeta(s).CanConnect() {
			t.Errorf("%s 不应可连接", s)
		}
	}
}

// ---------- 恢复 ----------

func TestRestoreRebuildsHotView(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{
		Name: "d1", PolicyName: "p1", Attributes: map[string]string{"site": "sh"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(7), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(8), PolicyName: "p1", Status: StatusRevoked,
	}); err != nil {
		t.Fatal(err)
	}

	g2 := g.reopen()
	mfp7, _ := parseFingerprint(fp(7))
	mfp8, _ := parseFingerprint(fp(8))
	if st := g2.meta.View().CertStatus(mfp7); !st.CanConnect() {
		t.Errorf("恢复后 ACTIVE 证书的状态 = %v", st)
	}
	if st := g2.meta.View().CertStatus(mfp8); st.CanConnect() {
		t.Error("恢复后 REVOKED 证书不应可连接；吊销状态必须跨重启保持")
	}
	if ps := g2.meta.View().ThingPolicySet("d1"); ps == meta.NoPolicySet {
		t.Error("恢复后设备的策略绑定丢失")
	}

	st, err := g2.r.Restore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Policies != 1 || st.Things != 1 || st.Certificates != 2 || st.ActiveCertificates != 1 {
		t.Errorf("统计 = %+v", st)
	}
}

// TestRestoreFailsOnBadPolicy 验证损坏或编译不过的策略使恢复失败。
//
// 跳过它等于让该策略下的所有设备静默失去全部权限。
func TestRestoreFailsOnBadPolicy(t *testing.T) {
	g := newRig(t)
	// 直接往冷层塞一份编译不过的策略
	raw := []byte(`{"v":1,"name":"bad","doc":"eyJTdGF0ZW1lbnQiOlt7IkVmZmVjdCI6Ik1heWJlIn1dfQ==","id":9,"created":1,"updated":1}`)
	if err := g.kv.Set(store.Key(store.PrefixPolicy, "bad"), raw, true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.Restore(context.Background()); err == nil {
		t.Error("编译不过的策略应使恢复失败")
	}
}

func TestRestoreFailsOnCorruptRecords(t *testing.T) {
	for _, c := range []struct {
		name   string
		prefix store.Prefix
	}{
		{"策略", store.PrefixPolicy},
		{"设备", store.PrefixThing},
		{"证书", store.PrefixCert},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newRig(t)
			if err := g.kv.Set(store.Key(c.prefix, "x"), []byte(`{not json`), true); err != nil {
				t.Fatal(err)
			}
			if _, err := g.r.Restore(context.Background()); err == nil {
				t.Error("损坏记录应使恢复失败，而不是静默跳过")
			}
		})
	}
}

func TestNewValidatesOptions(t *testing.T) {
	kv := store.NewMemKV()
	defer func() { _ = kv.Close() }()
	ms := meta.NewLocalStore(meta.LocalOptions{
		Now: func() int64 { return testNowMs }, Cold: kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	defer func() { _ = ms.Close() }()
	now := func() int64 { return testNowMs }
	cases := []struct {
		name string
		opt  Options
	}{
		{"缺少 KV", Options{Meta: ms, Now: now}},
		{"缺少 Meta", Options{KV: kv, Now: now}},
		{"缺少时钟", Options{KV: kv, Meta: ms}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.opt); err == nil {
				t.Error("应报错")
			}
		})
	}
}

func TestValidateHelpers(t *testing.T) {
	if err := ValidatePolicyName(""); err == nil {
		t.Error("空策略名应非法")
	}
	if err := ValidatePolicyName("a/b"); err == nil {
		t.Error("含斜杠的策略名应非法")
	}
	if err := ValidatePolicyName(strings.Repeat("a", 129)); err == nil {
		t.Error("过长的策略名应非法")
	}
	if err := ValidateStatus("WAT"); err == nil {
		t.Error("非法状态应被拒")
	}
	if err := ValidateFingerprint(fp(1)); err != nil {
		t.Errorf("合法指纹应通过: %v", err)
	}
	if got := shortFP("abc"); got != "abc" {
		t.Errorf("= %q", got)
	}
	// 省略号在 UTF-8 下是 3 字节，因此按符文数断言
	if got := shortFP(fp(1)); len([]rune(got)) != 17 {
		t.Errorf("= %q，期望 16 个字符加省略号", got)
	}
}

// ---------- 错误路径 ----------

// errKV 在指定操作上注入错误，用于覆盖存储失败分支。
type errKV struct {
	store.KV
	failGet  bool
	failSet  bool
	failScan bool
	failDel  bool
}

func (e *errKV) Get(k []byte) ([]byte, error) {
	if e.failGet {
		return nil, errors.New("注入的读取错误")
	}
	return e.KV.Get(k)
}

func (e *errKV) Set(k, v []byte, s bool) error {
	if e.failSet {
		return errors.New("注入的写入错误")
	}
	return e.KV.Set(k, v, s)
}

func (e *errKV) Delete(k []byte, s bool) error {
	if e.failDel {
		return errors.New("注入的删除错误")
	}
	return e.KV.Delete(k, s)
}

func (e *errKV) Scan(p []byte, fn func(k, v []byte) bool) error {
	if e.failScan {
		return errors.New("注入的遍历错误")
	}
	return e.KV.Scan(p, fn)
}

func newErrRig(t *testing.T, mut func(*errKV)) *Registry {
	t.Helper()
	base := store.NewMemKV()
	t.Cleanup(func() { _ = base.Close() })
	ekv := &errKV{KV: base}
	// 冷层写入口就是被注错的那个 KV：注册表的写入现在把冷层记录
	// 放进命令，所以"写入失败"这件事要在这一层注入才看得见。
	ms := meta.NewLocalStore(meta.LocalOptions{
		Now: func() int64 { return testNowMs }, Cold: ekv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	t.Cleanup(func() { _ = ms.Close() })
	r, err := New(Options{KV: ekv, Meta: ms, Env: authz.DefaultEnv(),
		Now: func() int64 { return testNowMs }})
	if err != nil {
		t.Fatal(err)
	}
	// 先在正常状态下建一份策略与设备，再注入错误
	ctx := context.Background()
	if err := r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(9), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	mut(ekv)
	return r
}

func TestStoreErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		mut  func(*errKV)
		call func(*Registry) error
	}{
		{"读策略失败", func(e *errKV) { e.failGet = true },
			func(r *Registry) error { _, err := r.GetPolicy("p1"); return err }},
		{"写策略失败", func(e *errKV) { e.failSet = true },
			func(r *Registry) error { return r.PutPolicy(ctx, "p2", []byte(simplePolicy)) }},
		{"列举策略失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, err := r.ListPolicies(); return err }},
		{"读设备失败", func(e *errKV) { e.failGet = true },
			func(r *Registry) error { _, err := r.GetThing("d1"); return err }},
		{"写设备失败", func(e *errKV) { e.failSet = true },
			func(r *Registry) error { return r.PutThing(ctx, Thing{Name: "d2", PolicyName: "p1"}) }},
		{"列举设备失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, err := r.ListThings(Query{}); return err }},
		{"删除设备失败", func(e *errKV) { e.failDel = true },
			func(r *Registry) error {
				_ = r.DeleteCertificate(ctx, fp(9))
				return r.DeleteThing(ctx, "d1")
			}},
		{"读证书失败", func(e *errKV) { e.failGet = true },
			func(r *Registry) error { _, err := r.GetCertificate(fp(9)); return err }},
		{"列举证书失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, err := r.ListCertificates(CertQuery{}); return err }},
		{"改证书状态失败", func(e *errKV) { e.failSet = true },
			func(r *Registry) error {
				return r.SetCertificateStatus(ctx, fp(9), StatusRevoked, "x")
			}},
		{"删除证书失败", func(e *errKV) { e.failDel = true },
			func(r *Registry) error { return r.DeleteCertificate(ctx, fp(9)) }},
		{"恢复时遍历失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, err := r.Restore(ctx); return err }},
		{"统计设备证书失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, err := r.CertificatesOfThing("d1"); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newErrRig(t, c.mut)
			if err := c.call(r); err == nil {
				t.Error("存储错误应被传出，而不是静默忽略")
			}
		})
	}
}

func TestCorruptRecordsReported(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		prefix store.Prefix
		key    string
		call   func(*Registry) error
	}{
		{"策略记录损坏", store.PrefixPolicy, "bad",
			func(r *Registry) error { _, err := r.GetPolicy("bad"); return err }},
		{"策略记录损坏时写入", store.PrefixPolicy, "bad",
			func(r *Registry) error { return r.PutPolicy(ctx, "bad", []byte(simplePolicy)) }},
		{"设备记录损坏", store.PrefixThing, "bad",
			func(r *Registry) error { _, err := r.GetThing("bad"); return err }},
		{"证书记录损坏", store.PrefixCert, fp(10),
			func(r *Registry) error { _, err := r.GetCertificate(fp(10)); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig(t)
			if err := g.kv.Set(store.Key(c.prefix, c.key), []byte(`{not json`), true); err != nil {
				t.Fatal(err)
			}
			if err := c.call(g.r); err == nil {
				t.Error("损坏记录应被报告，而不是当作不存在")
			}
		})
	}
}

func TestSetCertificateStatusValidation(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.SetCertificateStatus(ctx, "short", StatusRevoked, ""); err == nil {
		t.Error("非法指纹应被拒")
	}
	if err := g.r.SetCertificateStatus(ctx, fp(11), "ENABLED", ""); err == nil {
		t.Error("非法状态应被拒")
	}
	if err := g.r.SetCertificateStatus(ctx, fp(11), StatusRevoked, ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("未登记的指纹应返回 ErrNotFound，得到 %v", err)
	}
	if err := g.r.DeleteCertificate(ctx, "short"); err == nil {
		t.Error("非法指纹的删除应被拒")
	}
	if err := g.r.DeleteCertificate(ctx, fp(11)); !errors.Is(err, ErrNotFound) {
		t.Errorf("= %v，期望 ErrNotFound", err)
	}
}

func TestCertQueryLimits(t *testing.T) {
	if got := (CertQuery{}).limit(); got != DefaultPageLimit {
		t.Errorf("= %d，期望默认值 %d", got, DefaultPageLimit)
	}
	if got := (CertQuery{Limit: 10}).limit(); got != 10 {
		t.Errorf("= %d", got)
	}
	if got := (CertQuery{Limit: 99999}).limit(); got != MaxPageLimit {
		t.Errorf("= %d，期望上限 %d", got, MaxPageLimit)
	}
}

func TestListCertificatesPaging(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := g.r.PutCertificate(ctx, Certificate{
			Fingerprint: fpN(i + 1), PolicyName: "p1",
		}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	after := ""
	for i := 0; i < 10; i++ {
		page, err := g.r.ListCertificates(CertQuery{Limit: 5, After: after})
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page.Certificates {
			if seen[c.Fingerprint] {
				t.Errorf("证书 %s 出现在多页里", shortFP(c.Fingerprint))
			}
			seen[c.Fingerprint] = true
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if len(seen) != 12 {
		t.Errorf("共见到 %d 张证书，期望 12", len(seen))
	}
}

func TestParseFingerprintErrors(t *testing.T) {
	if _, err := parseFingerprint("zz"); err == nil {
		t.Error("非十六进制应报错")
	}
	if _, err := parseFingerprint("abcd"); err == nil {
		t.Error("长度不符应报错")
	}
}

// TestPolicyCounterNotReusedAfterDelete 验证编号不被重用。
//
// 重用已删除策略的编号会让旧引用指向新策略——设备拿到别人的权限。
func TestPolicyCounterNotReusedAfterDelete(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	first, _ := g.r.PolicySetID("p1")
	if err := g.r.DeletePolicy(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutPolicy(ctx, "p2", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if second, _ := g.r.PolicySetID("p2"); second == first {
		t.Errorf("新策略重用了已删除策略的编号 %d；旧引用会指向新策略", second)
	}
}

// TestPutPolicyPreservesCreatedAt 验证更新不改动创建时间。
func TestPutPolicyPreservesCreatedAt(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	before, err := g.r.GetPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	g.ms += 5000
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	after, err := g.r.GetPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	if after.CreatedAtUnixMs != before.CreatedAtUnixMs {
		t.Error("更新不应改动创建时间")
	}
	if after.UpdatedAtUnixMs == before.UpdatedAtUnixMs {
		t.Error("更新应刷新更新时间")
	}
	// 编号不变
	if id, ok := g.r.PolicySetID("p1"); !ok || id == 0 {
		t.Error("编号应保留")
	}
}

func TestDeletePolicyNotFound(t *testing.T) {
	g := newRig(t)
	if err := g.r.DeletePolicy(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("= %v，期望 ErrNotFound", err)
	}
}

func TestSyncAttrSlotsIdempotent(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	// 不引用属性的策略：槽位为空，重复同步不应报错
	for i := 0; i < 3; i++ {
		if err := g.r.SyncAttrSlots(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if slots := g.meta.View().AttrSlots(); len(slots) != 0 {
		t.Errorf("槽位 = %v，期望空", slots)
	}
}

// ---------- 热视图写入失败 ----------

// errMeta 在 Apply 上注入错误，覆盖"冷层写成功但热视图失败"的分支。
type errMeta struct {
	*meta.LocalStore
	fail bool
}

func (e *errMeta) Apply(ctx context.Context, cmd meta.Command) (meta.Revision, error) {
	if e.fail {
		return 0, errors.New("注入的热视图错误")
	}
	return e.LocalStore.Apply(ctx, cmd)
}

// TestHotViewWriteFailureAborts 验证热视图写入失败时不写冷层。
//
// 先热视图后冷层的顺序（REG-111）使这一点自然成立：热视图失败时
// 冷层还没动过，不会留下"冷层说已吊销而热视图还放行"的状态。
func TestHotViewWriteFailureAborts(t *testing.T) {
	kv := store.NewMemKV()
	defer func() { _ = kv.Close() }()
	base := meta.NewLocalStore(meta.LocalOptions{
		Now: func() int64 { return testNowMs }, Cold: kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	defer func() { _ = base.Close() }()
	em := &errMeta{LocalStore: base}
	r, err := New(Options{KV: kv, Meta: em, Env: authz.DefaultEnv(),
		Now: func() int64 { return testNowMs }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}

	em.fail = true
	cases := []struct {
		name string
		call func() error
	}{
		{"写策略", func() error { return r.PutPolicy(ctx, "p2", []byte(simplePolicy)) }},
		{"写设备", func() error { return r.PutThing(ctx, Thing{Name: "d2", PolicyName: "p1"}) }},
		{"写证书", func() error {
			return r.PutCertificate(ctx, Certificate{Fingerprint: fp(12), PolicyName: "p1"})
		}},
		{"删策略", func() error { return r.DeletePolicy(ctx, "p1") }},
		{"删设备", func() error { return r.DeleteThing(ctx, "d1") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.call(); err == nil {
				t.Error("热视图写入失败应使操作失败")
			}
		})
	}
	em.fail = false
	// 冷层未被污染：失败的写入不应留下记录
	if _, err := r.GetPolicy("p2"); !errors.Is(err, ErrNotFound) {
		t.Error("失败的策略写入不应留下冷层记录")
	}
	if _, err := r.GetThing("d2"); !errors.Is(err, ErrNotFound) {
		t.Error("失败的设备写入不应留下冷层记录")
	}
	if _, err := r.GetCertificate(fp(12)); !errors.Is(err, ErrNotFound) {
		t.Error("失败的证书写入不应留下冷层记录")
	}
	// 失败的删除也不应真的删掉
	if _, err := r.GetPolicy("p1"); err != nil {
		t.Errorf("失败的删除不应删掉记录: %v", err)
	}

	// 恢复时热视图失败同样应传出
	em.fail = true
	if _, err := r.Restore(ctx); err == nil {
		t.Error("恢复时热视图失败应传出")
	}
}

// TestApplyWithMissingPolicyFails 覆盖绑定已消失策略的分支。
//
// 正常路径下不会发生（写入时已校验），但冷层被外部改动或
// 策略被并发删除时会走到这里，此时必须报错而不是静默绑定空策略。
func TestApplyWithMissingPolicyFails(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(13), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	// 直接把策略记录从冷层删掉，模拟并发删除。
	// 策略的权威是冷层记录加热视图里的编译结果（policyEntryLocked），
	// 所以"策略不存在"这件事要在那一层制造。
	if err := g.kv.Delete(store.Key(store.PrefixPolicy, "p1"), true); err != nil {
		t.Fatal(err)
	}

	if err := g.r.applyThingLocked(ctx, &thingRecord{
		Name: "d1", PolicyName: "p1",
	}, true); err == nil {
		t.Error("绑定不存在的策略应报错")
	}
	if err := g.r.applyCertLocked(ctx, &certRecord{
		Fingerprint: fp(13), PolicyName: "p1", Status: StatusActive,
	}, true); err == nil {
		t.Error("绑定不存在的策略应报错")
	}
}

func TestSameStrings(t *testing.T) {
	if !sameStrings(nil, nil) || !sameStrings([]string{"a"}, []string{"a"}) {
		t.Error("相同切片应相等")
	}
	if sameStrings([]string{"a"}, []string{"b"}) ||
		sameStrings([]string{"a"}, nil) ||
		sameStrings([]string{"a", "b"}, []string{"a"}) {
		t.Error("不同切片应不相等")
	}
}

func TestValidateAttributesBytes(t *testing.T) {
	// 数量在限内但总大小超限
	big := map[string]string{"a": strings.Repeat("x", MaxAttributesBytes)}
	if err := validateAttributes(big); err == nil {
		t.Error("属性总大小超限应被拒")
	}
	if err := validateAttributes(nil); err != nil {
		t.Errorf("空属性应合法: %v", err)
	}
}

func TestPutCertificatePreservesCreatedAt(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(14), PolicyName: "p1",
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := g.r.GetCertificate(fp(14))
	g.ms += 5000
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(14), PolicyName: "p1", Subject: "CN=x",
	}); err != nil {
		t.Fatal(err)
	}
	after, _ := g.r.GetCertificate(fp(14))
	if after.CreatedAtUnixMs != before.CreatedAtUnixMs {
		t.Error("更新不应改动创建时间")
	}
	if after.Subject != "CN=x" {
		t.Errorf("主题 = %q", after.Subject)
	}
}

func TestDeleteThingNotFound(t *testing.T) {
	g := newRig(t)
	if err := g.r.DeleteThing(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("= %v，期望 ErrNotFound", err)
	}
}

func TestListThingsPolicyFilter(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	for _, p := range []string{"pa", "pb"} {
		if err := g.r.PutPolicy(ctx, p, []byte(simplePolicy)); err != nil {
			t.Fatal(err)
		}
	}
	for i, p := range []string{"pa", "pb", "pa"} {
		if err := g.r.PutThing(ctx, Thing{
			Name: "d" + string(rune('1'+i)), PolicyName: p,
		}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := g.r.ListThings(Query{Policy: "pa"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Things) != 2 {
		t.Errorf("按策略筛选得到 %d 条，期望 2", len(page.Things))
	}
}

// testCompile 是注入给 meta 的策略编译器。
//
// 冷层记录与策略文档现在都随命令走（CLU-065、CLU-064），所以建 LocalStore
// 时必须给它冷层写入口与编译器——这两样为 nil 时带记录的命令会被拒绝，
// 而那正是「忘了装配」该有的反应。
func testCompile(env authz.Env) meta.Compiler {
	return func(name string, doc []byte) (any, error) {
		return authz.CompileJSON(name, doc, env)
	}
}
