package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/itmisx/open-iot-core/internal/meta"
)

// 策略附加/卸下与「这份策略都附到谁身上」此前在包内没有测试。
// 兼容面的 AttachPolicy、DetachPolicy 与 ListTargetsForPolicy 落在
// 这三个方法上（API-107）。

// newBindingRig 预置两份策略、两台设备和两张证书。
func newBindingRig(t *testing.T) *rig {
	t.Helper()
	g := newRig(t)
	ctx := context.Background()
	for _, p := range []string{"p1", "p2"} {
		if err := g.r.PutPolicy(ctx, p, []byte(simplePolicy)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"d1", "d2"} {
		if err := g.r.PutThing(ctx, Thing{Name: name, PolicyName: "p1"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, fingerprint := range []string{fp(1), fp(2)} {
		if err := g.r.PutCertificate(ctx, Certificate{
			Fingerprint: fingerprint, PolicyName: "p1",
			Thing: fmt.Sprintf("d%d", i+1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	drain(g.meta)
	return g
}

// TestSetThingPolicy 改设备上的策略：热视图要跟着换，属性不能被清掉。
//
// 更新属性那条路径刻意保留原策略（UpdateThing），所以改策略只能走这里；
// 要是这里反过来把属性当成"没给就清空"，一次附加策略就会把设备属性抹掉，
// 而依赖属性的策略变量随之失效——设备连得上但什么都发不了。
func TestSetThingPolicy(t *testing.T) {
	g := newBindingRig(t)
	ctx := context.Background()
	if err := g.r.PutThing(ctx, Thing{
		Name: "d1", PolicyName: "p1", Attributes: map[string]string{"site": "sh"},
	}); err != nil {
		t.Fatal(err)
	}
	drain(g.meta)
	before, err := g.r.GetThing("d1")
	if err != nil {
		t.Fatal(err)
	}

	g.ms += 10
	if err := g.r.SetThingPolicy(ctx, "d1", "p2"); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetThing("d1")
	if err != nil {
		t.Fatal(err)
	}
	if got.PolicyName != "p2" {
		t.Fatalf("策略 = %q，期望 p2", got.PolicyName)
	}
	if got.Attributes["site"] != "sh" {
		t.Errorf("改策略把属性弄丢了: %+v", got.Attributes)
	}
	// 版本要加一：兼容面的 expectedVersion 把附加策略也算一次修改
	if got.Version != before.Version+1 {
		t.Errorf("版本 = %d，期望 %d", got.Version, before.Version+1)
	}
	// 热视图里设备指向的策略集必须换掉，否则权限变更不生效
	id2, ok := g.r.PolicySetID("p2")
	if !ok {
		t.Fatal("p2 没有策略集 ID")
	}
	if ps := g.meta.View().ThingPolicySet("d1"); ps != id2 {
		t.Errorf("热视图里的策略集 = %v，期望 %v", ps, id2)
	}

	// 幂等：再附一次同一份策略不产生变更也不动版本
	if err := g.r.SetThingPolicy(ctx, "d1", "p2"); err != nil {
		t.Fatal(err)
	}
	if again, _ := g.r.GetThing("d1"); again.Version != got.Version {
		t.Errorf("重复附加动了版本: %d -> %d", got.Version, again.Version)
	}

	// 解除绑定：热视图里应当没有策略集
	if err := g.r.SetThingPolicy(ctx, "d1", ""); err != nil {
		t.Fatal(err)
	}
	if ps := g.meta.View().ThingPolicySet("d1"); ps != meta.NoPolicySet {
		t.Errorf("解除后热视图里的策略集 = %v，期望无", ps)
	}
	if cur, _ := g.r.GetThing("d1"); cur.PolicyName != "" {
		t.Errorf("解除后 = %q", cur.PolicyName)
	}
}

// TestSetThingPolicyRejections 设备名非法、设备不存在、策略不存在都要拒。
func TestSetThingPolicyRejections(t *testing.T) {
	g := newBindingRig(t)
	ctx := context.Background()
	if err := g.r.SetThingPolicy(ctx, "", "p1"); err == nil {
		t.Error("空设备名应当被拒")
	}
	if err := g.r.SetThingPolicy(ctx, "nope", "p1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("不存在的设备 = %v，期望 ErrNotFound", err)
	}
	// 绑定不存在的策略会让设备连不上，而错误信息指向设备而不是这次绑定
	if err := g.r.SetThingPolicy(ctx, "d1", "nope"); err == nil {
		t.Error("不存在的策略应当被拒")
	}
	if got, _ := g.r.GetThing("d1"); got.PolicyName != "p1" {
		t.Errorf("被拒的绑定动了记录: %q", got.PolicyName)
	}
}

// TestSetCertificatePolicy 改证书上的策略同样要进热视图。
func TestSetCertificatePolicy(t *testing.T) {
	g := newBindingRig(t)
	ctx := context.Background()
	mfp, _ := parseFingerprint(fp(1))

	g.ms += 10
	if err := g.r.SetCertificatePolicy(ctx, fp(1), "p2"); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCertificate(fp(1))
	if err != nil {
		t.Fatal(err)
	}
	if got.PolicyName != "p2" || got.UpdatedAtUnixMs != testNowMs+10 {
		t.Fatalf("= %+v", got)
	}
	// 绑定不能被策略改写顺手丢掉
	if got.Thing != "d1" {
		t.Errorf("改策略把设备绑定弄丢了: %q", got.Thing)
	}
	id2, ok := g.r.PolicySetID("p2")
	if !ok {
		t.Fatal("p2 没有策略集 ID")
	}
	if ps := g.meta.View().CertPolicySet(mfp); ps != id2 {
		t.Errorf("热视图里的策略集 = %v，期望 %v", ps, id2)
	}

	// 幂等
	drain(g.meta)
	if err := g.r.SetCertificatePolicy(ctx, fp(1), "p2"); err != nil {
		t.Fatal(err)
	}
	if chs := drain(g.meta); len(chs) != 0 {
		t.Errorf("重复附加产生了 %d 条变更", len(chs))
	}

	// 解除
	if err := g.r.SetCertificatePolicy(ctx, fp(1), ""); err != nil {
		t.Fatal(err)
	}
	if ps := g.meta.View().CertPolicySet(mfp); ps != meta.NoPolicySet {
		t.Errorf("解除后 = %v，期望无", ps)
	}

	// 拒绝路径
	if err := g.r.SetCertificatePolicy(ctx, "zz", "p1"); err == nil {
		t.Error("指纹非法应当被拒")
	}
	if err := g.r.SetCertificatePolicy(ctx, fp(9), "p1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("不存在的证书 = %v", err)
	}
	if err := g.r.SetCertificatePolicy(ctx, fp(2), "nope"); err == nil {
		t.Error("不存在的策略应当被拒")
	}
	if got, _ := g.r.GetCertificate(fp(2)); got.PolicyName != "p1" {
		t.Errorf("被拒的绑定动了记录: %q", got.PolicyName)
	}
}

// TestListPolicyTargets 先设备后证书，分页标记跨阶段带回。
//
// 两类主体混在一张分页里，标记必须带上类别：只带 ID 的话，翻到第二页时
// 无从知道该从设备继续还是从证书继续，于是要么漏掉全部证书，要么把设备
// 再列一遍。
func TestListPolicyTargets(t *testing.T) {
	g := newBindingRig(t)

	all, next, err := g.r.ListPolicyTargets("p1", "", 250)
	if err != nil {
		t.Fatal(err)
	}
	if next != "" {
		t.Errorf("一页装得下时还给了标记: %q", next)
	}
	want := []PolicyTarget{
		{Kind: "thing", ID: "d1"}, {Kind: "thing", ID: "d2"},
		{Kind: "cert", ID: fp(1)}, {Kind: "cert", ID: fp(2)},
	}
	if len(all) != len(want) {
		t.Fatalf("= %+v", all)
	}
	for i, w := range want {
		if all[i] != w {
			t.Fatalf("第 %d 项 = %+v，期望 %+v（先设备后证书）", i, all[i], w)
		}
	}

	// 逐页翻，每页一条：顺序与一次取完必须一致
	var paged []PolicyTarget
	token := ""
	for i := 0; i < 10; i++ {
		page, nxt, err := g.r.ListPolicyTargets("p1", token, 1)
		if err != nil {
			t.Fatal(err)
		}
		paged = append(paged, page...)
		if nxt == "" {
			break
		}
		token = nxt
	}
	if len(paged) != len(want) {
		t.Fatalf("翻页共 %d 项: %+v", len(paged), paged)
	}
	for i, w := range want {
		if paged[i] != w {
			t.Fatalf("翻页第 %d 项 = %+v，期望 %+v", i, paged[i], w)
		}
	}

	// 另一份策略上没有主体
	if got, _, err := g.r.ListPolicyTargets("p2", "", 25); err != nil || len(got) != 0 {
		t.Errorf("p2 上 = %+v, %v", got, err)
	}
	// 不存在的策略要报错，而不是返回空表——空表会让调用方以为
	// 这份策略存在且没人用，于是放心删掉它引用的东西。
	if _, _, err := g.r.ListPolicyTargets("nope", "", 25); !errors.Is(err, ErrNotFound) {
		t.Errorf("不存在的策略 = %v", err)
	}
	// 形状不对的分页标记要报错
	for _, bad := range []string{"d1", "group/d1", "thing/", "/d1"} {
		if _, _, err := g.r.ListPolicyTargets("p1", bad, 25); err == nil {
			t.Errorf("标记 %q 应当被拒", bad)
		}
	}
	// limit 两端夹取
	if got, _, err := g.r.ListPolicyTargets("p1", "", 0); err != nil || len(got) != 4 {
		t.Errorf("limit=0 时 = %d 项, %v", len(got), err)
	}
	if got, _, err := g.r.ListPolicyTargets("p1", "", 1000); err != nil || len(got) != 4 {
		t.Errorf("limit=1000 时 = %d 项, %v", len(got), err)
	}
}
