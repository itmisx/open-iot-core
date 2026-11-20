package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/itmisx/open-iot-core/internal/store"
)

// 策略版本（API-106）与设备版本（API-102）此前在包内没有测试。
// 两者都是「兼容面照抄 AWS 的语义」，而错掉的方式都是静默的：
// 版本切换没进热视图，设备的权限就和控制台显示的不是同一份文档。

const denyPolicy = `{
  "Version": "2012-10-17",
  "Statement": [{"Effect": "Deny", "Action": "iot:Connect", "Resource": "*"}]
}`

// TestPolicyVersionLifecycle 钉住追加、切换、读取、列举与删除。
func TestPolicyVersionLifecycle(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.CreatePolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	// 创建即建立版本 1 并设为默认
	vs, err := g.r.ListPolicyVersions("p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].ID != "1" || !vs[0].IsDefault {
		t.Fatalf("创建后的版本 = %+v", vs)
	}
	// 重复创建要拒：覆盖会把别人的默认版连同正文一起换掉
	if err := g.r.CreatePolicy(ctx, "p1", []byte(simplePolicy)); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("重复 CreatePolicy = %v，期望 ErrAlreadyExists", err)
	}

	// setDefault=false：只保存文档，热视图不动。
	// 立刻生效会让一次"仅保存"变成一次权限变更。
	before, _ := g.r.CompiledPolicy("p1")
	ver, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(denyPolicy), false)
	if err != nil {
		t.Fatal(err)
	}
	if ver != "2" {
		t.Fatalf("新版本号 = %q，期望 2", ver)
	}
	after, _ := g.r.CompiledPolicy("p1")
	if after != before {
		t.Error("setDefault=false 时不该换掉已编译的策略")
	}
	if p, err := g.r.GetPolicy("p1"); err != nil || p.DefaultVersion != "1" {
		t.Errorf("默认版 = %q, %v，期望仍是 1", p.DefaultVersion, err)
	}

	// 读某一版拿到的是那一版的正文，而不是默认版的
	v2, err := g.r.GetPolicyVersion("p1", "2")
	if err != nil {
		t.Fatal(err)
	}
	if string(v2.Document) != denyPolicy || v2.IsDefault {
		t.Errorf("版本 2 = %+v", v2)
	}

	// 切换默认版：热视图必须跟着换，否则控制台显示的和设备用的不是一份
	g.ms += 100
	if err := g.r.SetDefaultPolicyVersion(ctx, "p1", "2"); err != nil {
		t.Fatal(err)
	}
	switched, _ := g.r.CompiledPolicy("p1")
	if switched == before {
		t.Error("切换默认版后热视图里的策略没换")
	}
	p, err := g.r.GetPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	if p.DefaultVersion != "2" || string(p.Document) != denyPolicy {
		t.Errorf("切换后 = 默认版 %q 正文 %q", p.DefaultVersion, p.Document)
	}
	// 切到当前已经是默认的那一版：幂等，不报错也不重复提交
	if err := g.r.SetDefaultPolicyVersion(ctx, "p1", "2"); err != nil {
		t.Errorf("切到同一版 = %v", err)
	}

	// 默认版不能删（删掉之后这份策略就没有生效文档了）
	if err := g.r.DeletePolicyVersion("p1", "2"); !errors.Is(err, ErrDefaultVersion) {
		t.Errorf("删默认版 = %v，期望 ErrDefaultVersion", err)
	}
	if err := g.r.DeletePolicyVersion("p1", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetPolicyVersion("p1", "1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("删掉的版本仍能读到: %v", err)
	}
	if err := g.r.DeletePolicyVersion("p1", "1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("重复删 = %v，期望 ErrNotFound", err)
	}
	// 删掉之后版本号不回填：复用号会让审计里的"版本 1"指向两份不同文档
	ver3, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(simplePolicy), false)
	if err != nil {
		t.Fatal(err)
	}
	if ver3 != "3" {
		t.Errorf("删掉版本 1 之后新版本 = %q，期望 3（不得回填空号）", ver3)
	}
}

// TestPolicyVersionLimit 每份策略最多五个版本。
//
// 没有上限的话，一个按 CI 频率调用 CreatePolicyVersion 的流水线会
// 把记录撑到无界，而这条记录是整体读写的。
func TestPolicyVersionLimit(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.CreatePolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxPolicyVersions-1; i++ {
		if _, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(simplePolicy), false); err != nil {
			t.Fatalf("第 %d 个版本: %v", i+2, err)
		}
	}
	if _, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(simplePolicy), false); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("第 %d 个版本 = %v，期望 ErrLimitExceeded", maxPolicyVersions+1, err)
	}
	vs, err := g.r.ListPolicyVersions("p1")
	if err != nil || len(vs) != maxPolicyVersions {
		t.Fatalf("版本数 = %d, %v", len(vs), err)
	}
	// 列举按版本号顺序，不是按 map 顺序或字符串序（"10" 要排在 "9" 之后）
	for i, v := range vs {
		if want := fmt.Sprint(i + 1); v.ID != want {
			t.Errorf("第 %d 个版本号 = %q，期望 %q", i, v.ID, want)
		}
	}
}

// TestPolicyVersionRejections 不存在的策略、不存在的版本、编译不过的文档都要拒。
func TestPolicyVersionRejections(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if _, err := g.r.CreatePolicyVersion(ctx, "nope", []byte(simplePolicy), false); !errors.Is(err, ErrNotFound) {
		t.Errorf("给不存在的策略加版本 = %v", err)
	}
	if err := g.r.SetDefaultPolicyVersion(ctx, "nope", "1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("切不存在的策略 = %v", err)
	}
	if err := g.r.DeletePolicyVersion("nope", "1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("删不存在的策略的版本 = %v", err)
	}
	if _, err := g.r.ListPolicyVersions("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("列举不存在的策略 = %v", err)
	}
	if _, err := g.r.GetPolicyVersion("nope", "1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("读不存在的策略的版本 = %v", err)
	}

	if err := g.r.CreatePolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetPolicyVersion("p1", "99"); !errors.Is(err, ErrNotFound) {
		t.Errorf("读不存在的版本 = %v", err)
	}
	if err := g.r.SetDefaultPolicyVersion(ctx, "p1", "99"); !errors.Is(err, ErrNotFound) {
		t.Errorf("切到不存在的版本 = %v", err)
	}
	// 编译不过的文档在追加时就拒：存下去等于让这一版在被切为默认时
	// 才报错，而那时报错的位置离真正的原因已经很远。
	if _, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(`{"Version":`), false); !errors.Is(err, ErrBadPolicy) {
		t.Errorf("编译不过的版本 = %v，期望 ErrBadPolicy", err)
	}
	if vs, err := g.r.ListPolicyVersions("p1"); err != nil || len(vs) != 1 {
		t.Errorf("被拒的版本写进去了: %d 个, %v", len(vs), err)
	}
}

// TestPolicyVersionsNormalizedForOldRecords 版本字段是后加的，旧记录要补成「只有版本 1」。
//
// 不补的话，升级前写进去的策略在列举版本时是空的，而它其实正在生效——
// 控制台会显示"这份策略没有任何版本"。
func TestPolicyVersionsNormalizedForOldRecords(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	// 摘掉版本字段，模拟升级前的记录
	rec, err := g.r.policyRecord("p1")
	if err != nil {
		t.Fatal(err)
	}
	rec.Versions = nil
	rec.DefaultVersion = ""
	op, err := setOp(store.Key(store.PrefixPolicy, "p1"), rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.r.commitRecordsLocked(ctx, op); err != nil {
		t.Fatal(err)
	}

	vs, err := g.r.ListPolicyVersions("p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].ID != "1" || !vs[0].IsDefault {
		t.Fatalf("旧记录读出的版本 = %+v", vs)
	}
	if p, err := g.r.GetPolicy("p1"); err != nil || p.DefaultVersion != "1" {
		t.Errorf("默认版 = %q, %v", p.DefaultVersion, err)
	}
	// 在补齐之后还能照常追加
	if ver, err := g.r.CreatePolicyVersion(ctx, "p1", []byte(simplePolicy), true); err != nil || ver != "2" {
		t.Errorf("= %q, %v", ver, err)
	}
}

// TestCreateAndUpdateThingVersions 设备版本只增，expectedVersion 不符即拒。
func TestCreateAndUpdateThingVersions(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.CreateThing(ctx, Thing{
		Name: "d1", PolicyName: "p1", Attributes: map[string]string{"site": "sh"},
	}); err != nil {
		t.Fatal(err)
	}
	th, err := g.r.GetThing("d1")
	if err != nil {
		t.Fatal(err)
	}
	if th.Version != 1 {
		t.Fatalf("新建设备版本 = %d，期望 1", th.Version)
	}
	if err := g.r.CreateThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("重复 CreateThing = %v，期望 ErrAlreadyExists", err)
	}
	if err := g.r.UpdateThing(ctx, "nope", nil, false, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("更新不存在的设备 = %v", err)
	}

	// expectedVersion 不符：拒绝且不写入。写入了才报错等于没有乐观锁。
	if err := g.r.UpdateThing(ctx, "d1", map[string]string{"site": "bj"}, false, 7); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("版本不符 = %v，期望 ErrVersionConflict", err)
	}
	th, _ = g.r.GetThing("d1")
	if th.Attributes["site"] != "sh" || th.Version != 1 {
		t.Fatalf("被拒的更新改了记录: %+v", th)
	}

	// 合并语义：只给出的键被改写，其余保留
	if err := g.r.UpdateThing(ctx, "d1", map[string]string{"line": "a"}, false, 1); err != nil {
		t.Fatal(err)
	}
	th, _ = g.r.GetThing("d1")
	if th.Attributes["site"] != "sh" || th.Attributes["line"] != "a" || th.Version != 2 {
		t.Fatalf("合并后 = %+v", th)
	}
	// 策略绑定不受 UpdateThing 影响
	if th.PolicyName != "p1" {
		t.Errorf("UpdateThing 动了策略绑定: %q", th.PolicyName)
	}

	// attrs 为 nil 且不是替换：属性原样保留，只有版本加一
	if err := g.r.UpdateThing(ctx, "d1", nil, false, 2); err != nil {
		t.Fatal(err)
	}
	th, _ = g.r.GetThing("d1")
	if len(th.Attributes) != 2 || th.Version != 3 {
		t.Fatalf("nil 合并后 = %+v", th)
	}

	// 替换语义：nil 当空表，原属性全部清掉
	if err := g.r.UpdateThing(ctx, "d1", nil, true, 0); err != nil {
		t.Fatal(err)
	}
	th, _ = g.r.GetThing("d1")
	if len(th.Attributes) != 0 || th.Version != 4 {
		t.Fatalf("替换后 = %+v", th)
	}
	// expected=0 表示不检查
	if err := g.r.UpdateThing(ctx, "d1", map[string]string{"x": "1"}, true, 0); err != nil {
		t.Fatal(err)
	}
	th, _ = g.r.GetThing("d1")
	if th.Attributes["x"] != "1" || th.Version != 5 {
		t.Fatalf("= %+v", th)
	}
	// 创建时间不随更新变
	if th.CreatedAtUnixMs != testNowMs {
		t.Errorf("创建时间被动了: %d", th.CreatedAtUnixMs)
	}
}

// TestUpdateThingDefaultsVersionForOldRecords 旧记录没有 version 字段时按 1 处理。
//
// 按 0 处理会让 expectedVersion=1 的调用永远冲突，而调用方看到的
// 版本号正是 1——它从列举接口拿到的。
func TestUpdateThingDefaultsVersionForOldRecords(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "d1", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	// 摘掉 version 字段
	rec, err := g.r.getThingRecordLocked("d1")
	if err != nil {
		t.Fatal(err)
	}
	rec.Version = 0
	op, err := setOp(store.Key(store.PrefixThing, "d1"), rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.r.commitRecordsLocked(ctx, op); err != nil {
		t.Fatal(err)
	}
	if th, err := g.r.GetThing("d1"); err != nil || th.Version != 1 {
		t.Fatalf("旧记录读出的版本 = %d, %v", th.Version, err)
	}
	if err := g.r.UpdateThing(ctx, "d1", map[string]string{"a": "1"}, false, 1); err != nil {
		t.Fatalf("按 1 更新旧记录 = %v", err)
	}
	if th, _ := g.r.GetThing("d1"); th.Version != 2 {
		t.Errorf("更新后版本 = %d，期望 2", th.Version)
	}
}
