package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// 容器入口脚本的用例（TST-154）。
//
// 它测的是 deploy/docker-entrypoint.sh，不是 Go 代码——但那段 shell 决定
// 「什么时候自动初始化」，而把它改成无条件初始化会让 CLU-055 要防的那个
// 症状（卷挂错 → 静默建出空节点 → 所有设备认证失败）回来，且在容器外
// 没有任何东西会发现。它又短又没有分支之外的逻辑，所以用桩跑一遍真脚本，
// 比读源码匹配字符串有判别力：后者在脚本重写成等价写法时报假红，在逻辑
// 反过来时又可能照样通过。
//
// 门禁只依赖 Go 工具链（见 CLAUDE.md 第 4 节），所以这里不起容器，
// 只用 /bin/sh 跑脚本本身；没有 sh 的平台直接跳过。

// entrypointPath 是被测脚本。
func entrypointPath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", "..", "deploy", "docker-entrypoint.sh"))
	if err != nil {
		t.Fatalf("解析脚本路径失败: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("入口脚本不存在（镜像的 ENTRYPOINT 指向它）: %v", err)
	}
	return p
}

// stubBinDir 造一个只含 openiotd 桩的目录，用来顶替 PATH 上的真二进制。
//
// 桩把每次调用的参数追加到一个文件里，于是「调了几次、每次什么参数、
// 顺序如何」都能断言。failOn 非空时，该子命令以非零退出——用来验证
// init 失败不会继续往 serve 走（脚本里的 set -e）。
func stubBinDir(t *testing.T, logPath, failOn string) string {
	t.Helper()
	dir := t.TempDir()
	stub := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + logPath + "\n" +
		"if [ \"$1\" = \"" + failOn + "\" ]; then exit 7; fi\n" +
		"exit 0\n"
	if failOn == "" {
		// 空串会匹配不上任何子命令，但写进脚本里读着像个缺陷，所以省掉。
		stub = "#!/bin/sh\n" +
			"printf '%s\\n' \"$*\" >> " + logPath + "\n" +
			"exit 0\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "openiotd"), []byte(stub), 0o755); err != nil {
		t.Fatalf("写桩失败: %v", err)
	}
	return dir
}

// runEntrypoint 跑一次入口脚本，返回桩记录下的调用序列与退出码。
func runEntrypoint(t *testing.T, env []string, failOn string, args ...string) ([]string, int) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "calls.log")
	bin := stubBinDir(t, logPath, failOn)

	cmd := exec.Command("/bin/sh", append([]string{entrypointPath(t)}, args...)...)
	// PATH 只留桩目录：脚本按 PATH 找 openiotd，这样它找到的一定是桩。
	cmd.Env = append([]string{"PATH=" + bin}, env...)
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("执行入口脚本失败: %v（输出 %s）", err, out)
	}

	raw, rerr := os.ReadFile(logPath)
	if rerr != nil && !os.IsNotExist(rerr) {
		t.Fatalf("读调用记录失败: %v", rerr)
	}
	var calls []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line != "" {
			calls = append(calls, line)
		}
	}
	return calls, code
}

func TestEntrypointAutoInit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("入口脚本是 POSIX sh，只在类 Unix 上跑")
	}
	cases := []struct {
		name string
		env  []string
		args []string
		want []string
	}{
		{
			// 默认关闭是这组用例里最重要的一条：开着它等于 serve 自动
			// 初始化（CLU-055）。
			name: "默认不自动初始化",
			args: []string{"serve", "--config", "/etc/openiotd/openiotd.yaml"},
			want: []string{"serve --config /etc/openiotd/openiotd.yaml"},
		},
		{
			name: "AUTO_INIT=1 时 serve 之前补一次 init",
			env:  []string{"OPENIOTD_AUTO_INIT=1"},
			args: []string{"serve", "--config", "/etc/openiotd/openiotd.yaml"},
			// init 必须拿到与 serve 相同的配置参数：两个子命令共用一套
			// flag，少传 --config 时 init 会按另一份配置去签服务端证书，
			// 而 SAN 只在那一次签出。
			want: []string{
				"init --if-needed --config /etc/openiotd/openiotd.yaml",
				"serve --config /etc/openiotd/openiotd.yaml",
			},
		},
		{
			name: "AUTO_INIT=true 同样打开",
			env:  []string{"OPENIOTD_AUTO_INIT=true"},
			args: []string{"serve"},
			want: []string{"init --if-needed", "serve"},
		},
		{
			// 「非空即为真」那种写法会把这两个值读成打开，而写下它们的人
			// 意思恰好相反。
			name: "AUTO_INIT=0 不打开",
			env:  []string{"OPENIOTD_AUTO_INIT=0"},
			args: []string{"serve"},
			want: []string{"serve"},
		},
		{
			name: "AUTO_INIT=false 不打开",
			env:  []string{"OPENIOTD_AUTO_INIT=false"},
			args: []string{"serve"},
			want: []string{"serve"},
		},
		{
			// 编排里显式写着 init 的那一步（两份 compose 都是）不能被执行两遍。
			name: "显式 init 子命令只执行一次",
			env:  []string{"OPENIOTD_AUTO_INIT=1"},
			args: []string{"init", "--config", "/etc/openiotd/openiotd.yaml", "--if-needed"},
			want: []string{"init --config /etc/openiotd/openiotd.yaml --if-needed"},
		},
		{
			// 一条只读命令不该有副作用：开着开关也不许它去建数据目录。
			name: "只对 serve 生效",
			env:  []string{"OPENIOTD_AUTO_INIT=1"},
			args: []string{"config", "print"},
			want: []string{"config print"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			calls, code := runEntrypoint(t, c.env, "", c.args...)
			if code != 0 {
				t.Fatalf("退出码 %d，期望 0", code)
			}
			if len(calls) != len(c.want) {
				t.Fatalf("调用序列 %q，期望 %q", calls, c.want)
			}
			for i := range c.want {
				if calls[i] != c.want[i] {
					t.Errorf("第 %d 次调用为 %q，期望 %q", i+1, calls[i], c.want[i])
				}
			}
		})
	}
}

// TestEntrypointInitFailureStopsServe 钉住 set -e。
//
// 没有它，init 失败会被咽掉，接着 serve 又会因为目录没初始化而失败
// ——容器的退出原因变成第二个错误，而真因（init 为什么没成）在日志里
// 被后面那条盖过去。
func TestEntrypointInitFailureStopsServe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("入口脚本是 POSIX sh，只在类 Unix 上跑")
	}
	calls, code := runEntrypoint(t,
		[]string{"OPENIOTD_AUTO_INIT=1"}, "init", "serve")
	if code != 7 {
		t.Fatalf("退出码 %d，期望把 init 的 7 透传出来", code)
	}
	if len(calls) != 1 || calls[0] != "init --if-needed" {
		t.Fatalf("init 失败后不应再调 serve，实际调用序列 %q", calls)
	}
}
