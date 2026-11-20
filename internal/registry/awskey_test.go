package registry

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/store"
)

var testSecret = []byte("\x00secret-bytes\xff")

func TestAWSKeyCRUD(t *testing.T) {
	g := newRig(t)

	got, err := g.r.PutAWSKey(ctx, "AKIAEXAMPLE", "operator", "CI 用", testSecret, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != "operator" || got.Created != testNowMs || got.Disabled {
		t.Fatalf("钥匙 = %+v", got)
	}
	if _, err := g.r.PutAWSKey(ctx, "AKIAEXAMPLE", "reader", "", testSecret, 0); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("重复创建 = %v，期望 ErrAlreadyExists", err)
	}

	secret, role, err := g.r.KeySecret("AKIAEXAMPLE")
	if err != nil {
		t.Fatal(err)
	}
	if role != "operator" || !bytes.Equal(secret, testSecret) {
		t.Fatalf("取回 = %q %q", role, secret)
	}
	// 返回的必须是副本：调用方改了它不能影响存储里的那份。
	secret[0] ^= 0xff
	again, _, err := g.r.KeySecret("AKIAEXAMPLE")
	if err != nil || !bytes.Equal(again, testSecret) {
		t.Fatalf("第二次取回 = %q %v", again, err)
	}

	list, err := g.r.ListAWSKeys()
	if err != nil || len(list) != 1 || list[0].ID != "AKIAEXAMPLE" {
		t.Fatalf("列表 = %+v %v", list, err)
	}

	if err := g.r.DeleteAWSKey(ctx, "AKIAEXAMPLE"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetAWSKey("AKIAEXAMPLE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("删后读 = %v", err)
	}
	if err := g.r.DeleteAWSKey(ctx, "AKIAEXAMPLE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("删两次 = %v", err)
	}
}

// TestAWSKeyUnusable 钉住停用与过期。
//
// 两者都必须让 KeySecret 取不到：停用若只改了列表上的一个字段而
// 验签照样能过，那个开关就是个摆设——而界面上它看起来已经生效了。
func TestAWSKeyUnusable(t *testing.T) {
	g := newRig(t)
	if _, err := g.r.PutAWSKey(ctx, "AKIADISABLE", "reader", "", testSecret, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.SetAWSKeyDisabled(ctx, "AKIADISABLE", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.r.KeySecret("AKIADISABLE"); !errors.Is(err, ErrKeyUnusable) {
		t.Fatalf("停用后 = %v，期望 ErrKeyUnusable", err)
	}
	// 停用的记录还在，列表上看得见——这正是它和删除的区别。
	k, err := g.r.GetAWSKey("AKIADISABLE")
	if err != nil || !k.Disabled {
		t.Fatalf("停用后的记录 = %+v %v", k, err)
	}
	if _, err := g.r.SetAWSKeyDisabled(ctx, "AKIADISABLE", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.r.KeySecret("AKIADISABLE"); err != nil {
		t.Fatalf("恢复后 = %v", err)
	}

	if _, err := g.r.PutAWSKey(ctx, "AKIAEXPIRE", "reader", "", testSecret, testNowMs+1000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.r.KeySecret("AKIAEXPIRE"); err != nil {
		t.Fatalf("没到期时 = %v", err)
	}
	g.ms = testNowMs + 1000
	if _, _, err := g.r.KeySecret("AKIAEXPIRE"); !errors.Is(err, ErrKeyUnusable) {
		t.Fatalf("到期那一刻 = %v，期望已失效", err)
	}
}

func TestAWSKeyRejectsBadInput(t *testing.T) {
	g := newRig(t)
	cases := []struct {
		name          string
		id, role, not string
		secret        []byte
		expires       int64
		want          string
	}{
		{"id 为空", "", "reader", "", testSecret, 0, "access key id"},
		{"id 带符号", "AKIA-1", "reader", "", testSecret, 0, "字母和数字"},
		{"id 带空白", "AKIA 1", "reader", "", testSecret, 0, "字母和数字"},
		{"角色为空", "AKIAOK", "", "", testSecret, 0, "角色"},
		{"备注过长", "AKIAOK", "reader", strings.Repeat("x", awsKeyNoteMax+1), testSecret, 0, "备注"},
		{"密钥材料为空", "AKIAOK", "reader", "", nil, 0, "密钥材料"},
		{"过期时刻为负", "AKIAOK", "reader", "", testSecret, -1, "过期时刻"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := g.r.PutAWSKey(ctx, c.id, c.role, c.not, c.secret, c.expires)
			if err == nil {
				t.Fatal("应当被拒")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("错误是 %q，里面应含 %q", err, c.want)
			}
		})
	}
}

// TestAWSKeySurvivesRestart 验证记录走的是复制日志、落在冷层里。
//
// 这是整件事的理由：此前钥匙在一份节点本地的 JSON 里，集群下每个节点
// 各有一份，只加在 A 上的钥匙打到 B 是 401。
func TestAWSKeySurvivesRestart(t *testing.T) {
	g := newRig(t)
	if _, err := g.r.PutAWSKey(ctx, "AKIAKEEP", "admin", "", testSecret, 0); err != nil {
		t.Fatal(err)
	}
	g2 := g.reopen()
	secret, role, err := g2.r.KeySecret("AKIAKEEP")
	if err != nil || role != "admin" || !bytes.Equal(secret, testSecret) {
		t.Fatalf("重开之后 = %q %q %v", secret, role, err)
	}
}

// TestListAWSKeysSurvivesCorruptRecord 验证一条坏记录不会让整张表读不出来。
//
// 读不出来的话，「删掉它」这个唯一的修法也做不了。
func TestListAWSKeysSurvivesCorruptRecord(t *testing.T) {
	g := newRig(t)
	if _, err := g.r.PutAWSKey(ctx, "AKIAGOOD", "reader", "", testSecret, 0); err != nil {
		t.Fatal(err)
	}
	if err := g.kv.Set(store.Key(store.PrefixAWSKey, "AKIABAD"), []byte("{not json"), false); err != nil {
		t.Fatal(err)
	}
	list, err := g.r.ListAWSKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("列表 = %+v，坏记录也应当出现", list)
	}
}
