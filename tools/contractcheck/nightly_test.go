package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFuzzWiring 钉住 fuzz 的接线方式（TST-239，2026-11-28 取代 TST-128）。
//
// 这条用例此前叫 TestNightlyFuzzIsScheduled，断言的是「ci.yml 里有 cron 的长时
// fuzz 任务，且四个包的失败语料都会被上传」。那条规矩已经撤了，理由记在 TST-239：
// 长时 fuzz 是**搜索**而不是检查，收益来自搜索时间累积，而 Go 的工作语料在
// `$GOCACHE/fuzz`，CI 上 `actions/setup-go` 的缓存按 go.sum 哈希命中、命中就不
// 回写——于是每晚都从同一批种子重新开始搜，代码没变、起点没变，只有随机数变。
//
// 撤掉之后要守的变成三件，少任何一件都会让 fuzz 这一层静默失效：
//
//  1. **门禁必须跑 `make fuzz-short`。** 它是现在唯一的自动检查，而它最重要的
//     作用不是搜索，是**重放已入库的崩溃语料**（`go test` 对 `testdata/fuzz`
//     下的种子一律重放，不需要 `-fuzz`）。它掉了，仓库里那两份靠 fuzz 抓到的
//     输入就再也没人验。
//  2. **四个包都要在 `fuzz-short` 里。** 少一个，那个包的崩溃语料就不再被重放
//     ——而缺的那一个不会有任何现象，正因为它检查的是「不要再出现」。
//  3. **长时 fuzz 不得回到 CI**，但 `make fuzz-long` 这个目标必须留着：深搜改成
//     按需手动跑（改动 `internal/mqtt`、`internal/authz`、`internal/shadow` 之后
//     值得跑一次）。把目标也删掉的话，TST-239 的补偿办法就不存在了。
//
// 夜间定时本身仍要在（确定性模拟靠它，TST-034），所以 cron 那两行照旧检查。
func TestFuzzWiring(t *testing.T) {
	root := filepath.Dir(filepath.Dir(prdDir(t)))
	readFile := func(parts ...string) string {
		b, err := os.ReadFile(filepath.Join(append([]string{root}, parts...)...))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	ci := readFile(".github", "workflows", "ci.yml")
	for _, needle := range []string{
		"make fuzz-short", // 门禁里的那一档（第 1 条）
		"schedule:",       // 夜间定时仍在（确定性模拟靠它）
		"cron: '0 18 * * *'",
		"make sim",
	} {
		if !strings.Contains(ci, needle) {
			t.Errorf("ci.yml 缺少 %q", needle)
		}
	}
	// 第 3 条：长时 fuzz 不得回到 CI。它一旦回来，每次触发就是 11 个目标
	// 串行、FUZZTIME=5m 约 55 分钟，而买到的只是重复摇同一把骰子。
	//
	// 只看**非注释行**：ci.yml 里那段注释刻意写着「要深搜时手动跑
	// make fuzz-long FUZZTIME=5m」，那是给人看的说明，不是接线。按整篇文本
	// 匹配会把这句说明判成违规——第一版就是这样，而那种失败最误导人：
	// 它报「又出现了」，而其实谁都没加回来。
	if strings.Contains(stripYAMLComments(ci), "make fuzz-long") {
		t.Error("ci.yml 又出现了 make fuzz-long：长时 fuzz 按 TST-239 不进 CI，" +
			"按需手动跑；要改回定时请先修订 TST-239 而不是直接加回来")
	}

	mk := readFile("Makefile")
	shortBody, ok := makeTargetBody(mk, "fuzz-short:")
	if !ok {
		t.Fatal("Makefile 里找不到 fuzz-short 目标：门禁那一档的唯一来源")
	}
	// 第 2 条：四个包都要在 fuzz-short 里。按 Makefile 变量名检查而不是按
	// 路径，因为包路径是用变量拼的。
	for _, pkg := range []string{"$(PACKET_PKG)", "$(TOPIC_PKG)", "$(AUTHZ_PKG)", "$(SHADOW_PKG)"} {
		if !strings.Contains(shortBody, pkg) {
			t.Errorf("fuzz-short 不再覆盖 %s：那个包已入库的崩溃语料从此不再被重放", pkg)
		}
	}
	longBody, ok := makeTargetBody(mk, "fuzz-long:")
	if !ok {
		t.Fatal("Makefile 里找不到 fuzz-long 目标：TST-239 的补偿办法就是手动跑它")
	}
	for _, pkg := range []string{"$(PACKET_PKG)", "$(TOPIC_PKG)", "$(AUTHZ_PKG)", "$(SHADOW_PKG)"} {
		if !strings.Contains(longBody, pkg) {
			t.Errorf("fuzz-long 不再覆盖 %s：手动深搜时会漏掉这个包", pkg)
		}
	}
}

// makeTargetBody 取一个 make 目标的配方（到下一个空行为止）。
//
// 锚点找不到时返回 false 而不是空串：空串会让上面每一条 Contains 都失败并
// 报成「四个包全都缺」，而真因是目标被改名了——这正是第 6 节第 3 条说的
// 「锚点找不到一律 Fatal，不返回空串」。
func makeTargetBody(mk, target string) (string, bool) {
	i := strings.Index(mk, "\n"+target)
	if i < 0 {
		return "", false
	}
	rest := mk[i+1:]
	if body, _, ok := strings.Cut(rest, "\n\n"); ok {
		return body, true
	}
	return rest, true
}

// stripYAMLComments 去掉整行注释，保留其余行。
//
// 只处理整行注释（trim 后以 # 开头）：行尾注释在这份 workflow 里没有出现，
// 而「# 在引号里」这种情况一旦要处理就得真的解析 YAML，那是另一回事。
func stripYAMLComments(s string) string {
	lines := strings.Split(s, "\n")
	out := lines[:0]
	for _, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "#") {
			continue
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}
