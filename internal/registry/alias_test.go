package registry

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/store"
)

const testRoleARN = "arn:aws:iam::123456789012:role/device-role"

// TestRoleAliasCRUD 钉住一条别名的生命周期，以及 create 的两个方向。
//
// create 与更新共用一个入口，区别只在「已存在怎么办」：建两次必须撞
// ErrAlreadyExists，否则 `CreateRoleAlias` 会静默盖掉别人的角色绑定；
// 改一条不存在的必须是 ErrNotFound，否则 `UpdateRoleAlias` 会凭空造出
// 一条调用方以为早就存在的记录。
func TestRoleAliasCRUD(t *testing.T) {
	g := newRig(t)

	got, err := g.r.PutRoleAlias(ctx, "prod-devices", testRoleARN, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	// duration 省略取默认值，而不是 0：0 秒的凭证有效期没有任何意义，
	// 把它原样存下去会让调用方以为自己设成了"立刻过期"。
	if got.Duration != aliasDurationDefault {
		t.Errorf("省略 duration 时 = %d，期望默认 %d", got.Duration, aliasDurationDefault)
	}
	if got.Created != testNowMs || got.Updated != testNowMs {
		t.Errorf("时间戳 = %d/%d，期望 %d", got.Created, got.Updated, testNowMs)
	}

	if _, err := g.r.PutRoleAlias(ctx, "prod-devices", testRoleARN, 0, true); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("重复创建 = %v，期望 ErrAlreadyExists", err)
	}

	g.ms += 1000
	updated, err := g.r.PutRoleAlias(ctx, "prod-devices", testRoleARN+"-2", 900, false)
	if err != nil {
		t.Fatal(err)
	}
	if updated.RoleARN != testRoleARN+"-2" || updated.Duration != 900 {
		t.Errorf("更新后 = %+v", updated)
	}
	// 更新不得重置创建时间：它是这条记录的出生时间，不是最后一次写入时间。
	if updated.Created != testNowMs {
		t.Errorf("更新后 Created = %d，期望仍是 %d", updated.Created, testNowMs)
	}
	if updated.Updated != testNowMs+1000 {
		t.Errorf("更新后 Updated = %d，期望 %d", updated.Updated, testNowMs+1000)
	}

	read, err := g.r.GetRoleAlias("prod-devices")
	if err != nil {
		t.Fatal(err)
	}
	if *read != *updated {
		t.Errorf("读回 = %+v，写入 = %+v", read, updated)
	}

	if _, err := g.r.PutRoleAlias(ctx, "never-created", testRoleARN, 0, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("更新不存在的别名 = %v，期望 ErrNotFound", err)
	}
	if _, err := g.r.GetRoleAlias("never-created"); !errors.Is(err, ErrNotFound) {
		t.Errorf("读不存在的别名 = %v，期望 ErrNotFound", err)
	}
	if err := g.r.DeleteRoleAlias(ctx, "never-created"); !errors.Is(err, ErrNotFound) {
		t.Errorf("删不存在的别名 = %v，期望 ErrNotFound", err)
	}

	if err := g.r.DeleteRoleAlias(ctx, "prod-devices"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetRoleAlias("prod-devices"); !errors.Is(err, ErrNotFound) {
		t.Errorf("删除后读回 = %v，期望 ErrNotFound", err)
	}
}

// TestRoleAliasValidation 逐条断言被拒的入参。
//
// 名字与 ARN 的校验在写入之前：一条形状不对的记录存下去之后，
// 报错的会是读它的那一侧，而那时已经看不出是谁写坏的。
func TestRoleAliasValidation(t *testing.T) {
	g := newRig(t)
	cases := []struct {
		name     string
		alias    string
		roleARN  string
		duration int
	}{
		{"名字为空", "", testRoleARN, 0},
		{"名字超长", strings.Repeat("a", 129), testRoleARN, 0},
		{"名字含非法字符", "prod devices", testRoleARN, 0},
		{"名字含斜杠", "prod/devices", testRoleARN, 0},
		{"ARN 太短", "ok", "arn:", 0},
		{"ARN 不是 arn: 开头", "ok", "iam::123456789012:role/r", 0},
		{"ARN 超长", "ok", "arn:" + strings.Repeat("a", 2048), 0},
		{"有效期低于下限", "ok", testRoleARN, aliasDurationMin - 1},
		{"有效期高于上限", "ok", testRoleARN, aliasDurationMax + 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := g.r.PutRoleAlias(ctx, c.alias, c.roleARN, c.duration, true); err == nil {
				t.Error("应当被拒")
			}
		})
	}
	// 合法字符集按 AWS 的别名命名：字母、数字与 _-:@
	if _, err := g.r.PutRoleAlias(ctx, "prod_devices-1:a@b", testRoleARN, aliasDurationMax, true); err != nil {
		t.Errorf("合法名字被拒: %v", err)
	}
}

// TestListRoleAliasesPaging 钉住分页：limit 的两端夹取、after 的排除语义，
// 以及 nextToken 必须是本页最后一个名字。
//
// token 取错一位的后果是漏掉或重复一条，而调用方无从发现——它只看到
// 一张少了一行的表。
func TestListRoleAliasesPaging(t *testing.T) {
	g := newRig(t)
	for i := 0; i < 5; i++ {
		if _, err := g.r.PutRoleAlias(ctx, fmt.Sprintf("a%02d", i), testRoleARN, 0, true); err != nil {
			t.Fatal(err)
		}
	}

	page, next, err := g.r.ListRoleAliases("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0] != "a00" || page[1] != "a01" {
		t.Fatalf("第一页 = %v", page)
	}
	if next != "a01" {
		t.Fatalf("nextToken = %q，期望本页最后一个名字 a01", next)
	}

	var all []string
	all = append(all, page...)
	for next != "" {
		page, next, err = g.r.ListRoleAliases(next, 2)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
	}
	if len(all) != 5 {
		t.Fatalf("翻完共 %d 条: %v", len(all), all)
	}
	for i, name := range all {
		if want := fmt.Sprintf("a%02d", i); name != want {
			t.Fatalf("第 %d 条 = %q，期望 %q（既不得漏也不得重）", i, name, want)
		}
	}

	// limit <= 0 取默认 25，> 100 夹到 100。两端都不夹的话，
	// limit=0 会返回空页而调用方以为表是空的。
	if page, _, err = g.r.ListRoleAliases("", 0); err != nil || len(page) != 5 {
		t.Errorf("limit=0 时 = %v, %v", page, err)
	}
	if page, _, err = g.r.ListRoleAliases("", 1000); err != nil || len(page) != 5 {
		t.Errorf("limit=1000 时 = %v, %v", page, err)
	}
	// after 排除它本身
	if page, _, err = g.r.ListRoleAliases("a03", 25); err != nil || len(page) != 1 || page[0] != "a04" {
		t.Errorf("after=a03 时 = %v, %v", page, err)
	}
}

// TestRoleAliasCorruptRecordReported 验证损坏的记录被报出来，
// 而不是当作"这条不存在"。当作不存在会让一次更新把剩下的字段也抹掉。
func TestRoleAliasCorruptRecordReported(t *testing.T) {
	g := newRig(t)
	if err := g.kv.Set(store.Key(store.PrefixAlias, "bad"), []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.GetRoleAlias("bad"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("读损坏记录 = %v，应当报损坏", err)
	}
	if _, err := g.r.PutRoleAlias(ctx, "bad", testRoleARN, 0, true); err == nil || errors.Is(err, ErrAlreadyExists) {
		t.Errorf("在损坏记录上创建 = %v，应当报损坏", err)
	}
	if _, err := g.r.PutRoleAlias(ctx, "bad", testRoleARN, 0, false); err == nil {
		t.Error("在损坏记录上更新应当被拒")
	}
}

// TestRoleAliasStoreErrorsPropagate 存储失败必须传出去。
// 吞掉写入失败会让调用方拿到 200，而那条别名从来没落盘。
func TestRoleAliasStoreErrorsPropagate(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*errKV)
		call func(*Registry) error
	}{
		{"读别名失败", func(e *errKV) { e.failGet = true },
			func(r *Registry) error { _, err := r.GetRoleAlias("a"); return err }},
		{"写别名失败", func(e *errKV) { e.failSet = true },
			func(r *Registry) error { _, err := r.PutRoleAlias(ctx, "a", testRoleARN, 0, true); return err }},
		{"列举别名失败", func(e *errKV) { e.failScan = true },
			func(r *Registry) error { _, _, err := r.ListRoleAliases("", 25); return err }},
		{"删除别名失败", func(e *errKV) { e.failDel = true },
			func(r *Registry) error {
				if _, err := r.PutRoleAlias(ctx, "a", testRoleARN, 0, true); err != nil {
					return err
				}
				return r.DeleteRoleAlias(ctx, "a")
			}},
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
