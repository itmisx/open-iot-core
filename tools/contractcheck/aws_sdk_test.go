package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBoto3SuiteStaysOutOfGate 确认控制面对照不进合并门禁，并且脚本还在。
//
// 把 tests/aws-sdk 加进 gate，没装 Python 的构建就会红。
// 删掉 boto3 或 aws iot，对照就不再是那两个客户端。
func TestBoto3SuiteStaysOutOfGate(t *testing.T) {
	root := filepath.Dir(filepath.Dir(prdDir(t)))
	makeText, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	gate := makefileTarget(string(makeText), "gate")
	if strings.Contains(gate, "aws-sdk") || strings.Contains(gate, "boto3") {
		t.Fatalf("gate 不应包含控制面 SDK 对照:\n%s", gate)
	}
	flow, err := os.ReadFile(filepath.Join(root, "tests", "aws-sdk", "flow.py"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(flow)
	for _, needle := range []string{"boto3", "create_thing", "create_keys_and_certificate", "attach_policy", "update_certificate", "REVOKED"} {
		if !strings.Contains(text, needle) {
			t.Errorf("flow.py 缺少 %s", needle)
		}
	}
	sh, err := os.ReadFile(filepath.Join(root, "tests", "aws-sdk", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sh), "iot describe-thing") {
		t.Fatal("run.sh 没有调用 aws CLI")
	}
}

// makefileTarget 取出一个目标的配方，到下一个以字母开头的目标为止。
func makefileTarget(text, name string) string {
	lines := strings.Split(text, "\n")
	var out []string
	on := false
	for _, line := range lines {
		if strings.HasPrefix(line, name+":") {
			on = true
			out = append(out, line)
			continue
		}
		if on && len(line) > 0 && line[0] != '\t' && !strings.HasPrefix(line, "#") && strings.Contains(line, ":") {
			break
		}
		if on {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
