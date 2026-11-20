package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeKeys(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDeviceKeys(t *testing.T) {
	dir := t.TempDir()
	path := writeKeys(t, dir, "keys.json", `[
		{"id":"AKID","secret":"s3cret","thing":"lamp"},
		{"id":"OTHER","secret":"s2","thing":"meter"}
	]`)
	got, err := loadDeviceKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	if got["AKID"].Thing != "lamp" || got["AKID"].Secret != "s3cret" {
		t.Fatalf("钥匙 = %+v", got["AKID"])
	}
	if len(got) != 2 {
		t.Fatalf("钥匙数 = %d", len(got))
	}
}

func TestLoadDeviceKeysRejectsBadFiles(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		body string
		// want 是错误信息里必须出现的片段。只断言"报错了"不够：
		// 这几条的价值全在于**报的是哪个错**——"缺 thing" 与
		// "多了 role" 指向两种完全不同的改法。
		want string
	}{
		{"重复", `[{"id":"A","secret":"s","thing":"t"},{"id":"A","secret":"s","thing":"t"}]`, "重复"},
		{"缺设备", `[{"id":"A","secret":"s","thing":""}]`, "缺少 thing"},
		{"缺密钥", `[{"id":"A","secret":"","thing":"t"}]`, "缺少 id 或 secret"},
		// 带 role 的是控制面的钥匙填错了地方（AUTH-086）。
		{"带角色", `[{"id":"A","secret":"s","thing":"t","role":"admin"}]`, "不该有 role"},
		{"空数组", `[]`, "没有 access key"},
		{"未知字段", `[{"id":"A","secret":"s","thing":"t","extra":1}]`, "解析"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeKeys(t, dir, tc.name+".json", tc.body)
			_, err := loadDeviceKeys(path)
			if err == nil {
				t.Fatal("应拒绝这份密钥文件")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误是 %q，里面应含 %q", err, tc.want)
			}
		})
	}
}
