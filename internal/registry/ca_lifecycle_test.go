package registry

import (
	"errors"
	"fmt"
	"testing"

	"github.com/itmisx/open-iot-core/internal/store"
)

// registerCA 登记一张自有 CA 并返回它的 PEM 与记录。
func registerCA(t *testing.T, g *rig, cn string, active bool) (string, *CACertificate) {
	t.Helper()
	code, err := g.r.RegistrationCode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, key := issueCA(t, cn)
	rec, err := g.r.RegisterCA(ctx, caPEM, issueLeaf(t, mustCert(t, caPEM), key, code), active)
	if err != nil {
		t.Fatal(err)
	}
	return caPEM, rec
}

// TestRegistrationCodeIsStable 注册码只生成一次。
//
// 每次调用换一个码，上一次拿着码去签验证证书的人就再也登记不上，
// 而失败信息看起来像是"使用者与注册码不一致"——指向完全错误的方向。
func TestRegistrationCodeIsStable(t *testing.T) {
	g := newCARig(t, 10)
	first, err := g.r.RegistrationCode(ctx)
	if err != nil || first == "" {
		t.Fatal(err)
	}
	second, err := g.r.RegistrationCode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("两次取到不同注册码: %q 与 %q", first, second)
	}
}

// TestCAStatusLifecycle 钉住状态改写与删除，以及它们在不存在的 ID 上的行为。
//
// 状态值只认 ACTIVE 与 INACTIVE：接受第三种取值会让这张 CA 既不能用来
// 注册设备证书（判定只认 ACTIVE），又看不出是被谁停用的。
func TestCAStatusLifecycle(t *testing.T) {
	g := newCARig(t, 10)
	caPEM, rec := registerCA(t, g, "factory", true)

	if err := g.r.UpdateCAStatus(ctx, rec.ID, "PAUSED"); err == nil {
		t.Error("ACTIVE/INACTIVE 之外的状态应当被拒")
	}
	if err := g.r.UpdateCAStatus(ctx, "ffff", StatusActive); !errors.Is(err, ErrNotFound) {
		t.Errorf("改不存在的 CA = %v，期望 ErrNotFound", err)
	}
	if _, err := g.r.GetCA("ffff"); !errors.Is(err, ErrNotFound) {
		t.Errorf("读不存在的 CA = %v，期望 ErrNotFound", err)
	}
	if err := g.r.DeleteCA(ctx, "ffff"); !errors.Is(err, ErrNotFound) {
		t.Errorf("删不存在的 CA = %v，期望 ErrNotFound", err)
	}

	g.ms += 500
	if err := g.r.UpdateCAStatus(ctx, rec.ID, StatusInactive); err != nil {
		t.Fatal(err)
	}
	got, err := g.r.GetCA(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusInactive {
		t.Errorf("状态 = %s，期望 %s", got.Status, StatusInactive)
	}
	if got.Updated != testNowMs+500 || got.Created != testNowMs {
		t.Errorf("时间戳 = %d/%d，期望 %d/%d", got.Created, got.Updated, testNowMs, testNowMs+500)
	}
	if got.Subject == "" || got.NotAfter == 0 || got.PEM != caPEM {
		t.Errorf("记录字段不全: %+v", got)
	}

	if err := g.r.DeleteCA(ctx, rec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetCA(rec.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("删除后读回 = %v，期望 ErrNotFound", err)
	}
}

// TestRegisterCARejectsDuplicateAndNonCA 重复登记与非 CA 证书都要拒。
//
// ID 是 CA 证书的 SHA-256，重复登记必须是 ErrAlreadyExists 而不是覆盖：
// 覆盖会把状态悄悄改回参数里的那个值，于是一次"重新登记"就把管理员
// 停用过的 CA 重新打开。
func TestRegisterCARejectsDuplicateAndNonCA(t *testing.T) {
	g := newCARig(t, 10)
	code, err := g.r.RegistrationCode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, key := issueCA(t, "factory")
	proof := issueLeaf(t, mustCert(t, caPEM), key, code)
	if _, err := g.r.RegisterCA(ctx, caPEM, proof, true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.RegisterCA(ctx, caPEM, proof, false); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("重复登记 = %v，期望 ErrAlreadyExists", err)
	}

	// 叶子证书不是 CA：IsCA 判定被拿掉的话，任何一张设备证书都能登记成 CA，
	// 随后用它去给别的证书背书。
	if _, err := g.r.RegisterCA(ctx, proof, proof, true); err == nil {
		t.Error("非 CA 证书应当被拒")
	}
	if _, err := g.r.RegisterCA(ctx, "not pem at all", proof, true); err == nil {
		t.Error("不是 PEM 的 CA 应当被拒")
	}
	if _, err := g.r.RegisterCA(ctx, caPEM, "not pem at all", true); err == nil {
		t.Error("不是 PEM 的验证证书应当被拒")
	}
}

// TestRegisterExternalCertRejections 逐条断言外部证书登记被拒的理由。
func TestRegisterExternalCertRejections(t *testing.T) {
	g := newCARig(t, 10)
	caPEM, _ := registerCA(t, g, "factory", true)
	unknownPEM, unknownKey := issueCA(t, "unknown")
	leafOfUnknown := issueLeaf(t, mustCert(t, unknownPEM), unknownKey, "dev")

	if _, err := g.r.RegisterExternalCert("not pem", "", ""); err == nil {
		t.Error("不是 PEM 的证书应当被拒")
	}
	if _, err := g.r.RegisterExternalCert(leafOfUnknown, "", "WHATEVER"); err == nil {
		t.Error("无法识别的状态应当被拒")
	}
	if _, err := g.r.RegisterExternalCert(leafOfUnknown, "not pem", ""); err == nil {
		t.Error("不是 PEM 的 CA 应当被拒")
	}
	// 给出的 CA 不是这张证书的签发者
	if _, err := g.r.RegisterExternalCert(leafOfUnknown, caPEM, ""); err == nil {
		t.Error("签发关系不符应当被拒")
	}
	// 签发关系对，但这张 CA 没登记过——没登记就不能用它背书，
	// 否则「登记 CA」这一步可以被跳过。
	if _, err := g.r.RegisterExternalCert(leafOfUnknown, unknownPEM, ""); err == nil {
		t.Error("未注册的 CA 应当被拒")
	}

	// 状态可以显式给成 PENDING_ACTIVATION：证书登记上了但还不能连，
	// 与签发路径同一套状态机。
	ok, err := g.r.RegisterExternalCert(leafOfUnknown, "", StatusPendingActivation)
	if err != nil {
		t.Fatal(err)
	}
	if ok.Status != StatusPendingActivation {
		t.Errorf("状态 = %s，期望 %s", ok.Status, StatusPendingActivation)
	}
}

// TestListCAsPaging 钉住 CA 列举的分页与两端夹取。
func TestListCAsPaging(t *testing.T) {
	g := newCARig(t, 10)
	for i := 0; i < 4; i++ {
		registerCA(t, g, fmt.Sprintf("factory-%d", i), true)
	}

	first, next, err := g.r.ListCAs("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("第一页 %d 条", len(first))
	}
	if next != first[len(first)-1].ID {
		t.Fatalf("nextToken = %q，期望本页最后一条的 ID %q", next, first[len(first)-1].ID)
	}

	seen := map[string]bool{}
	page := first
	for {
		for _, c := range page {
			if seen[c.ID] {
				t.Fatalf("ID %s 翻页时重复出现", c.ID)
			}
			seen[c.ID] = true
		}
		if next == "" {
			break
		}
		if page, next, err = g.r.ListCAs(next, 2); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("翻完共 %d 条，期望 4", len(seen))
	}

	if all, _, err := g.r.ListCAs("", 0); err != nil || len(all) != 4 {
		t.Errorf("limit=0 时 = %d 条, %v", len(all), err)
	}
	if all, _, err := g.r.ListCAs("", 1000); err != nil || len(all) != 4 {
		t.Errorf("limit=1000 时 = %d 条, %v", len(all), err)
	}
}

// TestCACorruptRecordReported 损坏的 CA 记录要报出来。
//
// 列举那一侧例外：一条读不出来的记录不该让整张表取不到，
// 它被跳过，而读它本身仍然报错。
func TestCACorruptRecordReported(t *testing.T) {
	g := newCARig(t, 10)
	if err := g.kv.Set(store.Key(store.PrefixCA, "bad"), []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetCA("bad"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("读损坏记录 = %v，应当报损坏", err)
	}
	if err := g.r.UpdateCAStatus(ctx, "bad", StatusActive); err == nil {
		t.Error("在损坏记录上改状态应当被拒")
	}
	list, _, err := g.r.ListCAs("", 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Errorf("列举 = %d 条，损坏记录应被跳过", len(list))
	}
}
