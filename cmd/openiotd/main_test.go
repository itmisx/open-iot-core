package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/server"
)

func TestRunDispatch(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCode int
		inStdout string
		inStderr string
	}{
		{"无参数", nil, 2, "", "用法"},
		{"未知命令", []string{"frobnicate"}, 2, "", "未知命令"},
		{"version", []string{"version"}, 0, "openiotd", ""},
		{"help", []string{"help"}, 0, "openiotd", ""},
		{"--help", []string{"--help"}, 0, "用法", ""},
		{"cert 缺子命令", []string{"cert"}, 1, "", "子命令"},
		{"cert 未知子命令", []string{"cert", "rotate"}, 1, "", "未知"},
		{"config 缺子命令", []string{"config"}, 1, "", "子命令"},
		{"config 未知子命令", []string{"config", "dump"}, 1, "", "未知"},
		{"config env", []string{"config", "env"}, 0, "OPENIOTD_", ""},
		{"serve 配置文件不存在", []string{"serve", "--config", "/no/such.yaml"}, 1, "",
			"配置文件不存在"},
		{"revoke 缺指纹", []string{"cert", "revoke"}, 1, "", "fingerprint"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errb strings.Builder
			code := run(c.args, &out, &errb)
			if code != c.wantCode {
				t.Errorf("退出码 = %d，期望 %d（stderr: %s）", code, c.wantCode, errb.String())
			}
			if c.inStdout != "" && !strings.Contains(out.String(), c.inStdout) {
				t.Errorf("stdout 未含 %q:\n%s", c.inStdout, out.String())
			}
			if c.inStderr != "" && !strings.Contains(errb.String(), c.inStderr) {
				t.Errorf("stderr 未含 %q:\n%s", c.inStderr, errb.String())
			}
		})
	}
}

// TestConfigPrintRoundTrip 验证打印出来的配置能被自己读回去。
//
// 不能往返的 `config print` 没有用：运维拿它当起点改配置，
// 若输出的格式自己都解析不了，第一步就卡住。
func TestConfigPrintRoundTrip(t *testing.T) {
	var out strings.Builder
	if code := run([]string{"config", "print"}, &out, io.Discard); code != 0 {
		t.Fatalf("退出码 = %d", code)
	}
	cfg, err := config.Decode(strings.NewReader(out.String()))
	if err != nil {
		t.Fatalf("自己打印的配置应能被自己解析: %v\n%s", err, out.String())
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("往返后应仍合法: %v", err)
	}
}

func TestNewLoggerLevelsAndFormats(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "unknown"} {
		for _, format := range []string{"json", "text", "other"} {
			cfg := config.Default()
			cfg.Log.Level = level
			cfg.Log.Format = format
			if log := newLogger(&cfg); log == nil {
				t.Errorf("level=%s format=%s 应返回日志器", level, format)
			}
		}
	}
}

func TestWriteSamplePolicyIsIdempotentAndValid(t *testing.T) {
	reg := server.NewRegistry(t.TempDir())
	if err := reg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := writeSamplePolicy(reg); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(reg.PoliciesDir(), "device-scoped.json")

	// 示例策略必须能编译：init 写出一份编译不过的策略，
	// 等于让每个新用户的第一次 serve 都失败
	policies, err := reg.LoadPolicies(authz.Env{Partition: "aws", Region: "local", Account: "000000000000"})
	if err != nil {
		t.Fatalf("示例策略应能编译: %v", err)
	}
	if len(policies) != 1 || policies[0].Name != "device-scoped" {
		t.Errorf("策略 = %+v", policies)
	}

	// 不覆盖用户的修改
	if err := os.WriteFile(path, []byte(`{"Statement":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeSamplePolicy(reg); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "iot:Connect") {
		t.Error("已存在的策略文件不应被覆盖")
	}
}

// TestFirstRunSequence 验证 usage 里写的首次使用流程真的能跑通。
//
// 这条测试守的是文档与实现的一致性：usage 里那三行命令是新用户照着
// 敲的第一件事，它们任何一步失败，用户对这个项目的第一印象就是坏的。
func TestFirstRunSequence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	var out, errb strings.Builder
	// 1. init
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		&out, &errb); code != 0 {
		t.Fatalf("init 失败（退出码 %d）: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "数据目录已就绪") {
		t.Errorf("init 未给出后续指引:\n%s", out.String())
	}
	for _, f := range []string{
		"manifest.json", "pki/ca-chain.pem", "pki/server.pem",
		"pki/issuing-ca.key", "policies/device-scoped.json",
	} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("init 未产出 %s: %v", f, err)
		}
	}
	// 根 CA 私钥绝不能落盘
	if _, err := os.Stat(filepath.Join(dir, "pki/root-ca.key")); err == nil {
		t.Error("根 CA 私钥不得落盘（PRD PKI-014）")
	}

	// 2. cert issue
	devices := filepath.Join(t.TempDir(), "devices")
	out.Reset()
	errb.Reset()
	if code := run([]string{"cert", "issue", "--data-dir", dir,
		"--thing", "dev-001", "--policy", "device-scoped",
		"--out", devices}, &out, &errb); code != 0 {
		t.Fatalf("cert issue 失败（退出码 %d）: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "私钥只在此处出现一次") {
		t.Errorf("签发输出未提示私钥不留存:\n%s", out.String())
	}
	key := filepath.Join(devices, "dev-001.key")
	fi, err := os.Stat(key)
	if err != nil {
		t.Fatalf("未产出设备私钥: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("设备私钥权限 = %o，期望 600", fi.Mode().Perm())
	}

	// 3. 重复 init 必须拒绝：覆盖 CA 会让已签发的证书全部失效
	out.Reset()
	errb.Reset()
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		&out, &errb); code == 0 {
		t.Error("已初始化的目录再 init 应失败")
	}

	// 4. cert revoke
	reg := server.NewRegistry(dir)
	certs, err := reg.LoadCerts()
	if err != nil || len(certs) != 1 {
		t.Fatalf("注册表应有一条登记: %v %v", certs, err)
	}
	out.Reset()
	errb.Reset()
	if code := run([]string{"cert", "revoke", "--data-dir", dir,
		"--fingerprint", certs[0].Fingerprint, "--reason", "测试"},
		&out, &errb); code != 0 {
		t.Fatalf("cert revoke 失败: %s", errb.String())
	}
	certs, _ = reg.LoadCerts()
	if certs[0].Status != "REVOKED" {
		t.Errorf("吊销未生效: %+v", certs[0])
	}
}

// TestCertCommandsWriteAudit 验证命令行签发和吊销进 audit.log，且私钥不进。
//
// 命令行和节点不是同一个进程，去掉 AppendAudit 调用，这张证书就只剩
// 注册表里的一行，审计文件里什么都没有。失败的命令也不记：否则一次
// 打错策略名的签发会变成"签发成功"。
func TestCertCommandsWriteAudit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		io.Discard, io.Discard); code != 0 {
		t.Fatal("init 失败")
	}
	auditPath := filepath.Join(dir, "audit.log")

	var errb strings.Builder
	if code := run([]string{"cert", "issue", "--data-dir", dir,
		"--thing", "dev-001", "--policy", "missing"},
		io.Discard, &errb); code == 0 {
		t.Fatal("策略不存在时签发应失败")
	}
	if _, err := os.Stat(auditPath); err == nil {
		t.Fatal("失败的签发不应留下审计行")
	}

	devices := filepath.Join(t.TempDir(), "devices")
	errb.Reset()
	if code := run([]string{"cert", "issue", "--data-dir", dir,
		"--thing", "dev-001", "--policy", "device-scoped", "--out", devices},
		io.Discard, &errb); code != 0 {
		t.Fatalf("签发失败: %s", errb.String())
	}
	key, err := os.ReadFile(filepath.Join(devices, "dev-001.key"))
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("审计文件权限 = %o，期望 600", fi.Mode().Perm())
	}

	reg := server.NewRegistry(dir)
	certs, err := reg.LoadCerts()
	if err != nil || len(certs) != 1 {
		t.Fatalf("注册表 = %v %v", certs, err)
	}
	fp := certs[0].Fingerprint
	var manifest struct {
		NodeID string `json:"node_id"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}

	lines := readAuditLines(t, auditPath)
	if len(lines) != 1 {
		t.Fatalf("签发后应有 1 行，得到 %d", len(lines))
	}
	if lines[0].Actor != "cli" || lines[0].Method != "cli" || lines[0].Path != "cert issue" ||
		lines[0].Action != "IssueCertificate" || lines[0].Kind != "Audit" ||
		lines[0].Node != manifest.NodeID || lines[0].At == 0 {
		t.Fatalf("签发审计行 = %+v", lines[0])
	}
	if !strings.Contains(lines[0].Target, fp) || !strings.Contains(lines[0].Target, "dev-001") {
		t.Fatalf("目标应含指纹和设备名: %s", lines[0].Target)
	}
	body := mustAuditText(t, auditPath)
	if strings.Contains(body, "PRIVATE KEY") || strings.Contains(body, string(key)) {
		t.Fatal("审计文件带上了私钥")
	}

	errb.Reset()
	if code := run([]string{"cert", "issue", "--data-dir", dir,
		"--thing", "dev-002", "--policy", "missing"},
		io.Discard, &errb); code == 0 {
		t.Fatal("第二次失败的签发应失败")
	}
	if again := readAuditLines(t, auditPath); len(again) != 1 {
		t.Fatalf("失败的签发不应追加审计行，得到 %d 行", len(again))
	}

	errb.Reset()
	if code := run([]string{"cert", "revoke", "--data-dir", dir,
		"--fingerprint", fp, "--reason", "测试吊销"},
		io.Discard, &errb); code != 0 {
		t.Fatalf("吊销失败: %s", errb.String())
	}
	lines = readAuditLines(t, auditPath)
	if len(lines) != 2 {
		t.Fatalf("吊销后应有 2 行，得到 %d", len(lines))
	}
	rev := lines[1]
	if rev.Action != "RevokeCertificate" || rev.Path != "cert revoke" || rev.Actor != "cli" {
		t.Fatalf("吊销审计行 = %+v", rev)
	}
	if !strings.Contains(rev.Target, fp) || !strings.Contains(rev.Target, "测试吊销") {
		t.Fatalf("吊销目标 = %s", rev.Target)
	}
	body = mustAuditText(t, auditPath)
	if strings.Contains(body, "PRIVATE KEY") || strings.Contains(body, string(key)) {
		t.Fatal("吊销之后审计文件带上了私钥")
	}

	before := len(readAuditLines(t, auditPath))
	if code := run([]string{"cert", "revoke", "--data-dir", dir,
		"--fingerprint", strings.Repeat("ab", 32)},
		io.Discard, io.Discard); code == 0 {
		t.Fatal("未登记的指纹应拒绝吊销")
	}
	if len(readAuditLines(t, auditPath)) != before {
		t.Fatal("失败的吊销不应追加审计行")
	}
}

func readAuditLines(t *testing.T, path string) []auditRow {
	t.Helper()
	text := mustAuditText(t, path)
	var rows []auditRow
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row auditRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("审计行无法解析: %v\n%s", err, line)
		}
		rows = append(rows, row)
	}
	return rows
}

func mustAuditText(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

type auditRow struct {
	At     int64  `json:"at"`
	Node   string `json:"node"`
	Kind   string `json:"kind"`
	Actor  string `json:"actor"`
	Method string `json:"method"`
	Path   string `json:"path"`
	Action string `json:"action"`
	Target string `json:"target"`
}

// TestInitIfNeededIsIdempotent 验证 --if-needed 可以被反复执行。
//
// 这是编排里的初始化步骤（compose 的 init 服务、k8s 的 initContainer）
// 依赖的性质：每次起容器都会跑一遍，第二次必须成功而不是报「PKI 已存在」。
// 三份清单此前各自用一段 `if [ -f manifest.json ]` 的 shell 绕开它。
//
// 同时钉住两件事：不带开关时**仍然**报错（CLU-055 的脾气不变，重新
// 初始化等于换掉整条信任链），以及已存在时不重新生成 CA。
func TestInitIfNeededIsIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	args := []string{"init", "--data-dir", dir, "--log-level", "error", "--if-needed"}

	var out, errb strings.Builder
	if code := run(args, &out, &errb); code != 0 {
		t.Fatalf("首次 init --if-needed 失败: %s", errb.String())
	}
	caPath := filepath.Join(dir, "pki", "issuing-ca.key")
	first := mustAuditText(t, caPath)

	// 第二次：必须成功，且不能动已有的 CA。
	out.Reset()
	errb.Reset()
	if code := run(args, &out, &errb); code != 0 {
		t.Fatalf("重跑 init --if-needed 应当成功: %s", errb.String())
	}
	if got := mustAuditText(t, caPath); got != first {
		t.Error("重跑 --if-needed 不得重新生成签发 CA——那等于换掉整条信任链")
	}
	// 反复执行时不该把整屏「下一步」再打一遍。
	if strings.Contains(out.String(), "下一步") {
		t.Errorf("无需改动时输出应当只有一行:\n%s", out.String())
	}

	// 不带开关仍然失败（CLU-055）。
	errb.Reset()
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		io.Discard, &errb); code == 0 {
		t.Error("不带 --if-needed 时重跑 init 应当失败")
	}
}

// TestInitIfNeededCompletesPartialDir 验证 --if-needed 会补齐只建到一半的目录。
//
// 来路是 PKI 那一步中断：身份文件与策略目录已经建好，PKI 没有。这里用
// 一个占住 `pki` 位置的同名文件制造这个中断（原先用的是"第一次忘了给
// CA 口令"，口令随 AUTH-092 去掉之后那条来路不存在了，但半初始化的
// 目录仍然会出现——磁盘满、容器被杀、权限不对都够）。
//
// 这正是 `if [ -f manifest.json ]` 那种守卫做不到的——它看见身份文件就
// 跳过全部步骤，于是那个目录永远补不齐，而下一次启动看起来是正常跳过的。
func TestInitIfNeededCompletesPartialDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	// 第一次：pki 的位置被一个同名文件占住，建 PKI 这一步失败，
	// 而身份文件在那之前已经写下。
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, "pki")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		io.Discard, io.Discard); code == 0 {
		t.Fatal("pki 目录建不出来时 init 应失败")
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("身份文件应当已经写下: %v", err)
	}

	// 移开障碍再跑一次，PKI 要被补齐。
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	var errb strings.Builder
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error", "--if-needed"},
		io.Discard, &errb); code != 0 {
		t.Fatalf("--if-needed 应当补齐半初始化的目录: %s", errb.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "pki", "issuing-ca.key")); err != nil {
		t.Errorf("PKI 应当已被补齐: %v", err)
	}
}

// TestAdminTokenCommand 验证管理面令牌的生成、读取与轮换。
func TestAdminTokenCommand(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	// init 应生成令牌并打印用法
	var out, errb strings.Builder
	if code := run([]string{"init", "--data-dir", dir, "--log-level", "error"},
		&out, &errb); code != 0 {
		t.Fatalf("init 失败: %s", errb.String())
	}
	if !strings.Contains(out.String(), "管理面令牌") {
		t.Errorf("init 输出未提及管理面令牌:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Authorization: Bearer") {
		t.Errorf("init 应给出 curl 用法:\n%s", out.String())
	}
	tokenPath := filepath.Join(dir, server.AdminTokenFileName)
	fi, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatalf("令牌文件未生成: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("令牌文件权限 = %o，期望 600", fi.Mode().Perm())
	}

	// admin token 打印当前令牌
	out.Reset()
	if code := run([]string{"admin", "token", "--data-dir", dir}, &out, io.Discard); code != 0 {
		t.Fatal("admin token 应成功")
	}
	first := strings.TrimSpace(out.String())
	if len(first) < 32 {
		t.Errorf("令牌过短: %q", first)
	}

	// 轮换
	out.Reset()
	if code := run([]string{"admin", "token", "--data-dir", dir, "--rotate"},
		&out, io.Discard); code != 0 {
		t.Fatal("轮换应成功")
	}
	if !strings.Contains(out.String(), "须重启节点才生效") {
		t.Errorf("轮换输出应说明生效时机:\n%s", out.String())
	}
	out.Reset()
	run([]string{"admin", "token", "--data-dir", dir}, &out, io.Discard)
	if strings.TrimSpace(out.String()) == first {
		t.Error("轮换后令牌应改变")
	}

	// 重复 init 不应覆盖已有令牌
	out.Reset()
	run([]string{"admin", "token", "--data-dir", dir}, &out, io.Discard)
	second := strings.TrimSpace(out.String())
	run([]string{"init", "--data-dir", dir, "--log-level", "error"}, io.Discard, io.Discard)
	out.Reset()
	run([]string{"admin", "token", "--data-dir", dir}, &out, io.Discard)
	if strings.TrimSpace(out.String()) != second {
		t.Error("重复 init 不应覆盖已有令牌——那会让正在使用的客户端突然失效")
	}
}

func TestAdminCommandErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"缺子命令", []string{"admin"}},
		{"未知子命令", []string{"admin", "frob"}},
		{"令牌文件不存在", []string{"admin", "token", "--data-dir",
			filepath.Join(t.TempDir(), "empty")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var errb strings.Builder
			if code := run(c.args, io.Discard, &errb); code == 0 {
				t.Error("应失败")
			}
		})
	}
	// 令牌文件不存在时须提示怎么办
	var errb strings.Builder
	run([]string{"admin", "token", "--data-dir", filepath.Join(t.TempDir(), "x")},
		io.Discard, &errb)
	if !strings.Contains(errb.String(), "openiotd init") &&
		!strings.Contains(errb.String(), "--rotate") {
		t.Errorf("错误应给出可操作的下一步: %s", errb.String())
	}
}

func TestGenerateAdminTokenIsRandom(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		tok, err := server.GenerateAdminToken()
		if err != nil {
			t.Fatal(err)
		}
		if len(tok) < 32 {
			t.Fatalf("令牌过短: %q", tok)
		}
		if seen[tok] {
			t.Fatal("生成的令牌重复")
		}
		seen[tok] = true
	}
}

// 起步策略必须真的放行它声称放行的东西。
//
// 一份不生效的示例策略比没有示例更糟：用户会以为自己的用法错了，
// 而真正错的是我们发出去的那份文档。init 的输出里逐条写了放行范围，
// 这个测试就是用来保证那段文案不会变成谎话。
func TestSamplePolicyGrantsWhatItClaims(t *testing.T) {
	set, err := authz.CompileJSON("device-scoped", []byte(samplePolicy), authz.DefaultEnv())
	if err != nil {
		t.Fatalf("起步策略无法编译，init 产出的就是坏的: %v", err)
	}
	sets := []*authz.CompiledSet{set}
	ctx := &authz.Context{
		ClientID: "dev-001", Thing: "dev-001", ThingAttached: true,
		ThingAttr: func(string) (string, bool) { return "", false },
	}

	for _, c := range []struct {
		name   string
		action authz.Action
		res    string
		want   authz.Decision
	}{
		// 与 init 输出里的放行范围逐条对应
		{"clientId 等于设备名可连接", authz.ActConnect, "dev-001", authz.Allow},
		{"影子主题可发布", authz.ActPublish, "$aws/things/dev-001/shadow/update", authz.Allow},
		{"影子主题可订阅", authz.ActSubscribe, "$aws/things/dev-001/shadow/#", authz.Allow},
		// 业务主题与影子同在 $aws/things/<设备名>/ 前缀下：非影子的子主题
		// 在 HandleReserved 返回 ok=false 之后落回普通路由，所以第一次
		// 业务发布与订阅一样能成功（AUTH-069 修订后的形状）。
		{"业务主题可发布", authz.ActPublish, "$aws/things/dev-001/telemetry", authz.Allow},
		{"业务主题可订阅", authz.ActSubscribe, "$aws/things/dev-001/cmd/#", authz.Allow},
		{"业务主题可接收", authz.ActReceive, "$aws/things/dev-001/cmd/reboot", authz.Allow},

		// 这几条是新用户最常撞的墙，必须确实被拒
		{"随机 clientId 不可连接", authz.ActConnect, "mqttx_a1b2c3", authz.ImplicitDeny},
		{"平坦主题不可订阅", authz.ActSubscribe, "sensors/temp", authz.ImplicitDeny},
		// 旧起步策略放行过 devices/<设备名>/*，现在不再放行（AUTH-069 修订）。
		// 钉住它是为了让"去掉了"这件事有据可查，而不是悄悄变了。
		{"旧的 devices 分支不再放行", authz.ActPublish, "devices/dev-001/telemetry", authz.ImplicitDeny},
		{"别的设备的主题不可发布", authz.ActPublish, "$aws/things/dev-002/telemetry", authz.ImplicitDeny},
		{"别的设备的影子不可订阅", authz.ActSubscribe, "$aws/things/dev-002/shadow/#", authz.ImplicitDeny},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := authz.Eval(sets, ctx, authz.Request{Action: c.action, Resource: c.res}, nil)
			if got != c.want {
				t.Errorf("%s %q = %s，想要 %s", c.action, c.res, got, c.want)
			}
		})
	}
}

// 设备未绑定时，带设备变量的语句一条都不能放行。
//
// 这是 AUTH-036：变量求值失败必须拒绝，不得退化为空串匹配——否则
// $aws/things/${ThingName}/* 会在未绑定时意外匹配 $aws/things//*，
// 而后者是攻击者可以自由构造的主题。
func TestSamplePolicyDeniesUnattachedDevice(t *testing.T) {
	set, err := authz.CompileJSON("device-scoped", []byte(samplePolicy), authz.DefaultEnv())
	if err != nil {
		t.Fatal(err)
	}
	ctx := &authz.Context{ClientID: "dev-001"} // 未绑定设备
	for _, c := range []struct {
		action authz.Action
		res    string
	}{
		{authz.ActConnect, "dev-001"},
		{authz.ActPublish, "devices//telemetry"},
		{authz.ActSubscribe, "devices//#"},
		{authz.ActPublish, "$aws/things//shadow/update"},
	} {
		got := authz.Eval([]*authz.CompiledSet{set}, ctx,
			authz.Request{Action: c.action, Resource: c.res}, nil)
		if got == authz.Allow {
			t.Errorf("设备未绑定却放行了 %s %q", c.action, c.res)
		}
	}
}

// TestDataDirEnvAppliesToEverySubcommand 数据目录的环境变量对所有子命令生效。
//
// 由来：`cert issue` 与 `admin token` 此前不看 OPENIOTD_DATA_DIR——
// cert 无条件把 flag 的默认值 "./data" 传下去（命令行优先级高于环境
// 变量，于是环境变量永远被盖住），admin 压根不走配置解析。
//
// 容器里数据目录正是靠环境变量指向卷的，所以这两条的表现是：要么因
// 没有权限而失败，要么在 ./data **另建一套 CA**，签出来的证书连不上
// 正在跑的节点——而两边都不报错。
func TestDataDirEnvAppliesToEverySubcommand(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OPENIOTD_DATA_DIR", dir)
	// 切到一个没有 data 子目录的工作目录：子命令若忽略环境变量，
	// 就会去碰 ./data，从而失败或建出第二套目录。
	t.Chdir(t.TempDir())

	var out bytes.Buffer
	if code := run([]string{"init"}, &out, io.Discard); code != 0 {
		t.Fatalf("init 失败: %s", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("init 没写到环境变量指定的目录: %v", err)
	}

	// cert issue 必须认同一个目录（能签出来就说明它找到了那套 CA）
	out.Reset()
	certOut := filepath.Join(t.TempDir(), "dev")
	if code := run([]string{
		"cert", "issue", "--thing", "env-thing",
		"--policy", "device-scoped", "--out", certOut,
	}, &out, io.Discard); code != 0 {
		t.Fatalf("cert issue 没用上 OPENIOTD_DATA_DIR: %s", out.String())
	}

	// admin token 必须读到同一个目录里的令牌
	out.Reset()
	if code := run([]string{"admin", "token"}, &out, io.Discard); code != 0 {
		t.Fatalf("admin token 没用上 OPENIOTD_DATA_DIR: %s", out.String())
	}
	want, err := os.ReadFile(filepath.Join(dir, "admin-token"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), strings.TrimSpace(string(want))) {
		t.Errorf("admin token 打印的不是这个数据目录里的令牌")
	}

	// 全程不得在当前目录下建出第二个数据目录
	if _, err := os.Stat("data"); err == nil {
		t.Error("子命令在工作目录下另建了 ./data——那会是第二套 CA")
	}
}
