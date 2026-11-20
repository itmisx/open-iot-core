package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNightlyFuzzIsScheduled 确认长时 fuzz 会在没人推代码的夜里跑，
// 并且四个包的失败语料都会被留下。
//
// 去掉 cron，任务就只在推 main 时跑，仓库闲几天就断了。
// 构件路径若只剩 packet，主题、策略、影子的崩溃输入不会被上传。
func TestNightlyFuzzIsScheduled(t *testing.T) {
	root := filepath.Dir(filepath.Dir(prdDir(t)))
	b, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, needle := range []string{
		"schedule:",
		"cron: '0 18 * * *'",
		"make fuzz-long",
		"internal/mqtt/packet/testdata/fuzz/",
		"internal/mqtt/topic/testdata/fuzz/",
		"internal/authz/testdata/fuzz/",
		"internal/shadow/testdata/fuzz/",
	} {
		if !strings.Contains(text, needle) {
			t.Errorf("ci.yml 缺少 %q", needle)
		}
	}
}
