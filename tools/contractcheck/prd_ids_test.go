package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// idHit 是需求 ID 的一次分配：表格行首的那一格。
// 正文里的「见 MQTT-100」不算分配。
type idHit struct {
	id   string
	file string
	line int
}

// requirementIDPattern 与 docs/TODO.md 末尾的核对脚本使用同一条规则。
var requirementIDPattern = regexp.MustCompile(`^\|\s*([A-Z]{3,4}-\d{3}[a-z]?)\s*\|`)

// requirementIDHits 抽出目录下全部分配行。
func requirementIDHits(dir string) ([]idHit, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var hits []idHit
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		for i, line := range strings.Split(string(b), "\n") {
			m := requirementIDPattern.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			hits = append(hits, idHit{id: m[1], file: name, line: i + 1})
		}
	}
	return hits, nil
}

// duplicateRequirementIDs 返回出现多于一次的 ID，按 ID 排序。
func duplicateRequirementIDs(dir string) (map[string][]idHit, error) {
	hits, err := requirementIDHits(dir)
	if err != nil {
		return nil, err
	}
	by := make(map[string][]idHit)
	for _, h := range hits {
		by[h.id] = append(by[h.id], h)
	}
	dups := make(map[string][]idHit)
	for id, hs := range by {
		if len(hs) > 1 {
			dups[id] = hs
		}
	}
	return dups, nil
}

func formatDuplicates(dups map[string][]idHit) string {
	ids := make([]string, 0, len(dups))
	for id := range dups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "%s 出现 %d 次:", id, len(dups[id]))
		for _, h := range dups[id] {
			fmt.Fprintf(&b, " %s:%d", h.file, h.line)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// prdDir 从测试的工作目录向上找 docs/prd。
func prdDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		cand := filepath.Join(dir, "docs", "prd")
		st, err := os.Stat(cand)
		if err == nil && st.IsDir() {
			return cand
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("找不到 docs/prd")
		}
		dir = parent
	}
}

// TestRequirementIDsAreUnique 是 TST-125。
// 同一个 ID 出现两行时，「以 PRD 为准」失去指向。
func TestRequirementIDsAreUnique(t *testing.T) {
	dups, err := duplicateRequirementIDs(prdDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(dups) > 0 {
		t.Fatalf("需求 ID 重复:\n%s", formatDuplicates(dups))
	}
}

// TestDuplicateRequirementIDIsReported 确认检查本身会抓到重复。
// 一条对空目录永远通过的检查，抓不住下一次按小节回填。
func TestDuplicateRequirementIDIsReported(t *testing.T) {
	dir := t.TempDir()
	body := "| ID | 需求 |\n| --- | --- |\n| MQTT-001 | 甲 |\n| MQTT-001 | 乙 |\n| 见 MQTT-001 | 这格不是分配 |\n"
	if err := os.WriteFile(filepath.Join(dir, "a.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	dups, err := duplicateRequirementIDs(dir)
	if err != nil {
		t.Fatal(err)
	}
	hs := dups["MQTT-001"]
	if len(hs) != 2 {
		t.Fatalf("MQTT-001 命中 %d 次，期望 2:\n%s", len(hs), formatDuplicates(dups))
	}
	if len(dups) != 1 {
		t.Fatalf("多余的重复:\n%s", formatDuplicates(dups))
	}
}
