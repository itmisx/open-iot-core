package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/meta"
)

// 证书与设备的关联此前在包内没有测试：兼容面有 HTTP 用例，但
// 「改绑必须断连」（PKI-109）这条安全语义只在这一层看得见——
// 它表现为一条带 PrevThing 的变更，而 HTTP 响应里没有这个东西。

// newPrincipalRig 预置一份策略、两台设备和一张绑在 d1 上的证书。
func newPrincipalRig(t *testing.T) *rig {
	t.Helper()
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"d1", "d2"} {
		if err := g.r.PutThing(ctx, Thing{Name: name, PolicyName: "p1"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(7), PolicyName: "p1", Thing: "d1",
	}); err != nil {
		t.Fatal(err)
	}
	drain(g.meta)
	return g
}

// TestAttachThingPrincipalRebindDisconnects 改绑必须发出要求断连的变更。
//
// 连接的设备身份在握手时固定（策略变量 ${iot:Connection.Thing.ThingName}
// 是那一刻解析的）。改绑后不断开，等于凭据已经移到 d2 名下，而那条连接
// 还在用 d1 的权限收发——这是 PKI-109 存在的全部理由。
func TestAttachThingPrincipalRebindDisconnects(t *testing.T) {
	g := newPrincipalRig(t)
	ctx := context.Background()
	mfp, _ := parseFingerprint(fp(7))

	if err := g.r.AttachThingPrincipal(ctx, "d2", fp(7)); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCertificate(fp(7))
	if err != nil {
		t.Fatal(err)
	}
	if got.Thing != "d2" {
		t.Fatalf("绑定 = %q，期望 d2（Attach 是覆盖而不是追加）", got.Thing)
	}

	var rebind *meta.Change
	for _, ch := range drain(g.meta) {
		if ch.Fingerprint == mfp && ch.RequiresDisconnect() {
			c := ch
			rebind = &c
		}
	}
	if rebind == nil {
		t.Fatal("改绑须发出要求断连的变更，否则在用连接会继续用原设备的权限")
	}

	// 反查两个方向都应跟着改
	if names, err := g.r.ListPrincipalThings(fp(7)); err != nil || len(names) != 1 || names[0] != "d2" {
		t.Errorf("ListPrincipalThings = %v, %v", names, err)
	}
	if fps, err := g.r.ListThingPrincipals("d2"); err != nil || len(fps) != 1 || fps[0] != fp(7) {
		t.Errorf("ListThingPrincipals(d2) = %v, %v", fps, err)
	}
	if fps, err := g.r.ListThingPrincipals("d1"); err != nil || len(fps) != 0 {
		t.Errorf("ListThingPrincipals(d1) = %v, %v，原设备上不该再挂着", fps, err)
	}
}

// TestAttachThingPrincipalIdempotent 绑成同一台设备不产生变更。
//
// 空跑一次写入会在事件流与审计里留下一条"看起来发生过改绑"的记录，
// 而实际什么都没变。排查时这种记录比没有更坏。
func TestAttachThingPrincipalIdempotent(t *testing.T) {
	g := newPrincipalRig(t)
	ctx := context.Background()
	before, err := g.r.GetCertificate(fp(7))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.r.AttachThingPrincipal(ctx, "d1", fp(7)); err != nil {
		t.Fatal(err)
	}
	if chs := drain(g.meta); len(chs) != 0 {
		t.Errorf("重复绑定产生了 %d 条变更，应当一条都没有", len(chs))
	}
	after, err := g.r.GetCertificate(fp(7))
	if err != nil {
		t.Fatal(err)
	}
	if after.UpdatedAtUnixMs != before.UpdatedAtUnixMs {
		t.Errorf("更新时间被动了: %d -> %d", before.UpdatedAtUnixMs, after.UpdatedAtUnixMs)
	}
}

// TestAttachDetachRejections 逐条断言被拒的入参。
func TestAttachDetachRejections(t *testing.T) {
	g := newPrincipalRig(t)
	ctx := context.Background()

	if err := g.r.AttachThingPrincipal(ctx, "", fp(7)); err == nil {
		t.Error("设备名为空应当被拒")
	}
	if err := g.r.AttachThingPrincipal(ctx, "d2", fp(8)); !errors.Is(err, ErrNotFound) {
		t.Errorf("绑定不存在的证书 = %v，期望 ErrNotFound", err)
	}
	if _, err := g.r.ListPrincipalThings(fp(8)); !errors.Is(err, ErrNotFound) {
		t.Errorf("反查不存在的证书 = %v，期望 ErrNotFound", err)
	}
	if err := g.r.DetachThingPrincipal(ctx, "d1", fp(8)); !errors.Is(err, ErrNotFound) {
		t.Errorf("解绑不存在的证书 = %v，期望 ErrNotFound", err)
	}
	// 给出的设备名与当前绑定不符：报错而不是静默成功。调用方多半拿的是
	// 过期信息，让它以为解绑了某个其实没动的关联更危险。
	if err := g.r.DetachThingPrincipal(ctx, "d2", fp(7)); err == nil {
		t.Error("设备名与当前绑定不符应当被拒")
	}
	if got, _ := g.r.GetCertificate(fp(7)); got.Thing != "d1" {
		t.Errorf("被拒的解绑动了绑定: %q", got.Thing)
	}
}

// TestDetachThingPrincipal 解绑之后证书还在、状态不变，但不再反查到设备。
func TestDetachThingPrincipal(t *testing.T) {
	g := newPrincipalRig(t)
	ctx := context.Background()
	mfp, _ := parseFingerprint(fp(7))

	if err := g.r.DetachThingPrincipal(ctx, "d1", fp(7)); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCertificate(fp(7))
	if err != nil {
		t.Fatal(err)
	}
	if got.Thing != "" {
		t.Errorf("解绑后绑定 = %q", got.Thing)
	}
	// 状态不变：彻底停用要走吊销，解绑只是摘掉设备变量
	if got.Status != StatusActive {
		t.Errorf("解绑改了证书状态: %q", got.Status)
	}
	if names, err := g.r.ListPrincipalThings(fp(7)); err != nil || names != nil {
		t.Errorf("ListPrincipalThings = %v, %v，期望空", names, err)
	}
	// 解绑同样要断连：原连接的设备变量已经失去依据
	found := false
	for _, ch := range drain(g.meta) {
		if ch.Fingerprint == mfp && ch.RequiresDisconnect() {
			found = true
		}
	}
	if !found {
		t.Error("解绑须发出要求断连的变更")
	}

	// 已经没有绑定了，再解一次是 ErrNotFound 而不是静默成功
	if err := g.r.DetachThingPrincipal(ctx, "d1", fp(7)); !errors.Is(err, ErrNotFound) {
		t.Errorf("重复解绑 = %v，期望 ErrNotFound", err)
	}
}

// TestDetachWithEmptyThingSkipsCheck 不给设备名时按当前绑定解。
//
// AWS 的 DetachThingPrincipal 要求给出设备名，本项目允许省略，
// 因为一对一绑定下"当前绑的是谁"是确定的；省略时跳过比对那一步。
func TestDetachWithEmptyThingSkipsCheck(t *testing.T) {
	g := newPrincipalRig(t)
	ctx := context.Background()
	if err := g.r.DetachThingPrincipal(ctx, "", fp(7)); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCertificate(fp(7))
	if err != nil {
		t.Fatal(err)
	}
	if got.Thing != "" {
		t.Errorf("解绑后绑定 = %q", got.Thing)
	}
}
