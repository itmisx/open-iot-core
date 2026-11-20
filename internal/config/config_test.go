package config

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// noEnv 是一个什么都没设置的环境。
func noEnv(string) (string, bool) { return "", false }

// envOf 构造一个固定的环境查找函数。
func envOf(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := kv[k]
		return v, ok
	}
}

func TestDefaultIsValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("默认配置应合法: %v", err)
	}
	// 默认值的安全取向必须被锁定：这些是"开箱即安全"的具体含义
	if !c.Listeners.MQTTTLS.RequireClientCert {
		t.Error("默认须要求客户端证书")
	}
	if c.Auth.Mode != AuthCert {
		t.Error("默认认证方式须为 cert")
	}
	if c.Listeners.MQTTPlain.Enabled {
		t.Error("明文端点默认须关闭")
	}
	if !isLoopbackAddr(c.Listeners.Admin.Addr) {
		t.Errorf("管理端点默认须绑回环，当前 %s", c.Listeners.Admin.Addr)
	}
	if c.Listeners.MQTTTLS.MinTLSVersion != "1.2" {
		t.Errorf("默认最低 TLS 版本 = %q", c.Listeners.MQTTTLS.MinTLSVersion)
	}
}

// TestUnknownFieldRejected 是本包最重要的一条测试。
//
// 配置键打错字若被静默忽略，一个本想开启客户端证书校验的部署会在
// 无人察觉的情况下以关闭状态运行。这类故障没有任何可观测症状，
// 直到被人利用。
func TestUnknownFieldRejected(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"顶层未知键", "unknown_section:\n  a: 1\n"},
		{"嵌套未知键", "node:\n  nonexistent: x\n"},
		{"字段名拼错", "listeners:\n  mqtt_tls:\n    require_client_certs: true\n"},
		{"用了连字符而非下划线", "node:\n  data-dir: /x\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Decode(strings.NewReader(c.yaml)); err == nil {
				t.Fatal("未知字段必须报错，不得忽略")
			}
		})
	}
}

func TestDecodePartialKeepsDefaults(t *testing.T) {
	cfg, err := Decode(strings.NewReader("node:\n  data_dir: /var/lib/openiotd\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.DataDir != "/var/lib/openiotd" {
		t.Errorf("data_dir = %q", cfg.Node.DataDir)
	}
	// 未出现的字段必须保留默认值，而不是变成零值
	if cfg.Node.Mode != ModeStandalone {
		t.Errorf("mode = %q，期望保留默认值", cfg.Node.Mode)
	}
	if !cfg.Listeners.MQTTTLS.RequireClientCert {
		t.Error("require_client_cert 应保留默认的 true")
	}
	if cfg.Cluster.ShardCount != 64 {
		t.Errorf("shard_count = %d，期望保留默认值 64", cfg.Cluster.ShardCount)
	}
}

func TestDecodeEmptyIsDefault(t *testing.T) {
	cfg, err := Decode(strings.NewReader(""))
	if err != nil {
		t.Fatalf("空配置应合法: %v", err)
	}
	if cfg.Node.Mode != ModeStandalone {
		t.Error("空配置应等同默认配置")
	}
}

func TestDecodeRejectsMultiDocument(t *testing.T) {
	// 第二个文档会静默覆盖第一个的部分字段，这不是任何人想要的
	_, err := Decode(strings.NewReader("node:\n  id: a\n---\nnode:\n  id: b\n"))
	if err == nil {
		t.Fatal("多文档 YAML 应报错")
	}
}

func TestDurationParsing(t *testing.T) {
	cfg, err := Decode(strings.NewReader("cluster:\n  max_view_staleness: 45s\nview:\n  resync_interval: 2m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.MaxViewStaleness.D() != 45*time.Second {
		t.Errorf("max_view_staleness = %s", cfg.Cluster.MaxViewStaleness)
	}
	if cfg.View.ResyncInterval.D() != 2*time.Minute {
		t.Errorf("resync_interval = %s", cfg.View.ResyncInterval)
	}
}

func TestDurationErrors(t *testing.T) {
	cases := []string{
		"cluster:\n  max_view_staleness: 30\n",      // 裸数字
		"cluster:\n  max_view_staleness: forever\n", // 无法解析
		"cluster:\n  max_view_staleness: -5s\n",     // 负数
		"cluster:\n  max_view_staleness: [1,2]\n",   // 类型不符
	}
	for _, in := range cases {
		if _, err := Decode(strings.NewReader(in)); err == nil {
			t.Errorf("应报错: %q", strings.TrimSpace(in))
		}
	}
}

// TestPrecedence 锁定「默认值 → 文件 → 环境变量 → flags」的顺序。
func TestPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	if err := os.WriteFile(path, []byte(
		"node:\n  id: from-file\n  data_dir: /from/file\nlog:\n  level: warn\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	f := NewFlags("t", io.Discard)
	if err := f.Parse([]string{"--config", path, "--node-id", "from-flag"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(f, envOf(map[string]string{
		EnvPrefix + "NODE_ID":  "from-env",
		EnvPrefix + "DATA_DIR": "/from/env",
	}))
	if err != nil {
		t.Fatal(err)
	}

	// flag 压过环境变量
	if cfg.Node.ID != "from-flag" {
		t.Errorf("node.id = %q，期望 flag 优先", cfg.Node.ID)
	}
	// 环境变量压过文件
	if cfg.Node.DataDir != "/from/env" {
		t.Errorf("node.data_dir = %q，期望环境变量压过文件", cfg.Node.DataDir)
	}
	// 文件压过默认值
	if cfg.Log.Level != "warn" {
		t.Errorf("log.level = %q，期望取文件值", cfg.Log.Level)
	}
	// 三层都没给的保持默认
	if cfg.Log.Format != "json" {
		t.Errorf("log.format = %q，期望默认值", cfg.Log.Format)
	}
}

// TestUnsetFlagDoesNotOverride 验证未给出的 flag 不会以零值覆盖文件。
//
// 这是 flag 覆盖最容易写错的地方：若直接把 flag 变量赋值过去，
// 一个未给出的 --data-dir 会把文件里的正确路径清成空串。
func TestUnsetFlagDoesNotOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	if err := os.WriteFile(path, []byte("node:\n  data_dir: /keep/me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := NewFlags("t", io.Discard)
	if err := f.Parse([]string{"--config", path}); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(f, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.DataDir != "/keep/me" {
		t.Errorf("node.data_dir = %q，未给出的 flag 不应覆盖", cfg.Node.DataDir)
	}
}

func TestEmptyEnvValueIgnored(t *testing.T) {
	cfg := Default()
	cfg.Node.DataDir = "/keep"
	// 空赋值几乎总是脚本里变量展开失败，按空串覆盖会比报错更难排查
	cfg.ApplyEnv(envOf(map[string]string{EnvPrefix + "DATA_DIR": ""}))
	if cfg.Node.DataDir != "/keep" {
		t.Errorf("data_dir = %q，空环境变量应被忽略", cfg.Node.DataDir)
	}
	cfg.ApplyEnv(envOf(map[string]string{EnvPrefix + "DATA_DIR": "   "}))
	if cfg.Node.DataDir != "/keep" {
		t.Errorf("data_dir = %q，纯空白应被忽略", cfg.Node.DataDir)
	}
}

func TestEnvVarsListMatchesBindings(t *testing.T) {
	vars := EnvVars()
	if len(vars) == 0 {
		t.Fatal("环境变量清单不应为空")
	}
	for _, v := range vars {
		if !strings.HasPrefix(v.Name, EnvPrefix) {
			t.Errorf("环境变量 %q 缺少前缀 %s", v.Name, EnvPrefix)
		}
		if v.Field == "" {
			t.Errorf("环境变量 %q 未标注对应配置项", v.Name)
		}
	}
	// 返回的须是副本：外部修改不得影响内部绑定表
	vars[0].Name = "TAMPERED"
	if EnvVars()[0].Name == "TAMPERED" {
		t.Error("EnvVars 应返回副本")
	}
}

func TestValidationErrorsNameTheField(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(*Config)
		field string
	}{
		{"模式无效", func(c *Config) { c.Node.Mode = "cluster" }, "node.mode"},
		{"数据目录为空", func(c *Config) { c.Node.DataDir = "" }, "node.data_dir"},
		{"角色为空", func(c *Config) { c.Node.Role = nil }, "node.role"},
		{"角色无效", func(c *Config) { c.Node.Role = []Role{"worker"} }, "node.role[0]"},
		{"join 无种子", func(c *Config) { c.Node.Mode = ModeJoin }, "node.seeds"},
		{"单机却有种子", func(c *Config) { c.Node.Seeds = []string{"a:1"} }, "node.seeds"},
		{"种子地址非法", func(c *Config) {
			c.Node.Mode = ModeJoin
			c.Node.Seeds = []string{"no-port"}
		}, "node.seeds[0]"},
		{"分区数越界", func(c *Config) { c.Cluster.ShardCount = 0 }, "cluster.shard_count"},
		{"副本数为偶数", func(c *Config) { c.Cluster.Voters = 4 }, "cluster.voters"},
		{"后端不支持", func(c *Config) { c.Meta.Backend = "postgres" }, "meta.backend"},
		{"无任何 MQTT 端点", func(c *Config) {
			c.Listeners.MQTTTLS.Enabled = false
			c.Listeners.MQTTALPN.Enabled = false
			c.Listeners.MQTTPlain.Enabled = false
		}, "listeners"},
		{"WebSocket 没配区域", func(c *Config) {
			c.Listeners.MQTTWSS.Enabled = true
			c.Listeners.MQTTWSS.KeysFile = "k.json"
		}, "listeners.mqtt_wss.region"},
		{"WebSocket 没配密钥文件", func(c *Config) {
			c.Listeners.MQTTWSS.Enabled = true
			c.Listeners.MQTTWSS.Region = "us-east-1"
		}, "listeners.mqtt_wss.keys_file"},
		{"WebSocket 与 ALPN 抢同一端口", func(c *Config) {
			c.Listeners.MQTTALPN.Enabled = true
			c.Listeners.MQTTWSS.Enabled = true
			c.Listeners.MQTTWSS.Region = "us-east-1"
			c.Listeners.MQTTWSS.KeysFile = "k.json"
		}, "listeners.mqtt_wss.addr"},
		{"兼容面没配区域", func(c *Config) {
			c.AWSAPI.Enabled = true
			c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
		}, "aws_api.region"},
		{"兼容面没配端点", func(c *Config) {
			c.AWSAPI.Enabled = true
			c.AWSAPI.Region = "us-east-1"
		}, "aws_api.endpoint_address"},
		// 旧配置里的密钥文件要报出来，不能静默忽略（AUTH-086）。
		{"兼容面还配着密钥文件", func(c *Config) {
			c.AWSAPI.Enabled = true
			c.AWSAPI.Region = "us-east-1"
			c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
			c.AWSAPI.KeysFile = "k.json"
		}, "aws_api.keys_file"},
		// 旧配置里的 CA 口令文件要报出来，不能静默忽略（AUTH-092）：
		// 静默忽略之后，运维以为私钥是加密的，而磁盘上那份是明文。
		{"还配着 CA 口令文件", func(c *Config) {
			c.PKI.PassphraseFile = "/etc/openiotd/ca.pass"
		}, "pki.passphrase_file"},
		{"关了 pki 也要报出口令文件", func(c *Config) {
			c.PKI.Enabled = false
			c.Listeners.MQTTTLS.ClientCAFile = "/ca.pem"
			c.PKI.PassphraseFile = "/etc/openiotd/ca.pass"
		}, "pki.passphrase_file"},
		{"追踪没配地址", func(c *Config) { c.OTel.Enabled = true }, "otel.endpoint"},
		{"追踪地址不是 URL", func(c *Config) {
			c.OTel.Enabled = true
			c.OTel.Endpoint = "collector:4318"
		}, "otel.endpoint"},
		{"监听地址缺端口", func(c *Config) { c.Listeners.MQTTTLS.Addr = "0.0.0.0" },
			"listeners.mqtt_tls.addr"},
		{"端口非数字", func(c *Config) { c.Listeners.MQTTTLS.Addr = "0.0.0.0:mqtts" },
			"listeners.mqtt_tls.addr"},
		{"TLS 版本过低", func(c *Config) { c.Listeners.MQTTTLS.MinTLSVersion = "1.1" },
			"listeners.mqtt_tls.min_tls_version"},
		{"只给了证书没给私钥", func(c *Config) { c.Listeners.MQTTTLS.CertFile = "/a.pem" },
			"listeners.mqtt_tls.cert_file"},
		{"要求客户端证书但无信任来源", func(c *Config) {
			c.PKI.Enabled = false
			c.Listeners.MQTTTLS.RequireClientCert = true
			c.Auth.Mode = AuthInsecureNone
			c.Auth.AcknowledgeInsecure = true
		}, "listeners.mqtt_tls.require_client_cert"},
		{"默认有效期超过上限", func(c *Config) {
			c.PKI.MaxValidity = Duration(time.Hour)
		}, "pki.default_validity"},
		{"CA 文件只给了一部分", func(c *Config) { c.PKI.RootCertFile = "/r.pem" },
			"pki.issuing_cert_file"},
		{"认证方式无效", func(c *Config) { c.Auth.Mode = "oauth" }, "auth.mode"},
		{"日志级别无效", func(c *Config) { c.Log.Level = "trace" }, "log.level"},
		{"日志格式无效", func(c *Config) { c.Log.Format = "logfmt" }, "log.format"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Default()
			c.mut(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("应报错")
			}
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("错误应为 *FieldError，得到 %T", err)
			}
			if fe.Field != c.field {
				t.Errorf("指向字段 %q，期望 %q", fe.Field, c.field)
			}
			// NFR-065：关键故障须给出可操作信息
			if fe.Hint == "" {
				t.Errorf("字段 %s 的错误缺少修复提示", fe.Field)
			}
			if !strings.Contains(err.Error(), c.field) {
				t.Errorf("错误文本未含字段名: %s", err)
			}
		})
	}
}

// TestInsecureAuthNeedsTwoAcknowledgements 验证不认证模式的两道闸。
//
// 两种风险不同，因此不合并成一个开关：一是"根本不认证"，
// 二是"不认证且监听在任何人都能访问的地址上"。后者是把一个
// 可任意收发的 broker 放到网络上，后果与前者不是一个量级。
func TestInsecureAuthNeedsTwoAcknowledgements(t *testing.T) {
	// 第一道：没有确认就不允许
	cfg := Default()
	cfg.Auth.Mode = AuthInsecureNone
	cfg.Listeners.MQTTTLS.Enabled = false
	cfg.Listeners.MQTTPlain = PlainListener{Enabled: true, Addr: "127.0.0.1:1883"}
	err := cfg.Validate()
	var fe *FieldError
	if !errors.As(err, &fe) || fe.Field != "auth.acknowledge_insecure" {
		t.Fatalf("应要求显式确认，得到 %v", err)
	}

	// 确认之后，绑回环是允许的
	cfg.Auth.AcknowledgeInsecure = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("确认后绑回环应允许: %v", err)
	}

	// 第二道：绑非回环地址须拒绝
	for _, addr := range []string{"0.0.0.0:1883", "192.168.1.10:1883", ":1883", "[::]:1883"} {
		cfg.Listeners.MQTTPlain.Addr = addr
		err := cfg.Validate()
		if !errors.As(err, &fe) || fe.Field != "auth.mode" {
			t.Errorf("地址 %s 下应拒绝，得到 %v", addr, err)
			continue
		}
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("错误应指出具体地址 %s: %s", addr, err)
		}
	}

	// localhost 与 ::1 视为回环
	for _, addr := range []string{"localhost:1883", "[::1]:1883", "127.0.0.2:1883"} {
		cfg.Listeners.MQTTPlain.Addr = addr
		if err := cfg.Validate(); err != nil {
			t.Errorf("地址 %s 应视为回环: %v", addr, err)
		}
	}
}

func TestLoadFileErrors(t *testing.T) {
	f := NewFlags("t", io.Discard)
	if err := f.Parse([]string{"--config", "/no/such/file.yaml"}); err != nil {
		t.Fatal(err)
	}
	_, err := Load(f, noEnv)
	if !errors.Is(err, ErrNoConfigFile) {
		t.Errorf("应返回 ErrNoConfigFile，得到 %v", err)
	}

	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("node: [this is not a map]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f2 := NewFlags("t", io.Discard)
	if err := f2.Parse([]string{"--config", bad}); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(f2, noEnv); err == nil {
		t.Error("类型不符的配置应报错")
	} else if !strings.Contains(err.Error(), bad) {
		t.Errorf("错误应含文件路径: %s", err)
	}
}

func TestFlagsRejectPositionalArgs(t *testing.T) {
	f := NewFlags("t", io.Discard)
	// 多余的位置参数通常是 flag 名写错（如 -config=x y），必须报错
	if err := f.Parse([]string{"serve", "extra"}); err == nil {
		t.Error("多余的位置参数应报错")
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	cfg := Default()
	cfg.Node.ID = "node-7"
	cfg.Cluster.MaxViewStaleness = Duration(30 * time.Second)

	var buf bytes.Buffer
	if err := cfg.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	// 时长须以人类可读形式输出，否则 `config print` 的输出不能再喂回去
	if !strings.Contains(buf.String(), "30s") {
		t.Errorf("时长应输出为 30s:\n%s", buf.String())
	}
	back, err := Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("自己输出的配置应能被自己解析: %v", err)
	}
	if back.Node.ID != "node-7" || back.Cluster.MaxViewStaleness != cfg.Cluster.MaxViewStaleness {
		t.Error("往返后配置不一致")
	}
	if err := back.Validate(); err != nil {
		t.Errorf("往返后应仍合法: %v", err)
	}
}

func TestHasRole(t *testing.T) {
	c := Default()
	if !c.HasRole(RoleBroker) || !c.HasRole(RoleSigner) {
		t.Error("默认应同时具备 broker 与 signer")
	}
	c.Node.Role = []Role{RoleBroker}
	if c.HasRole(RoleSigner) {
		t.Error("不应具备 signer")
	}
}

// TestDefaultEnablesConsole 锁定控制台默认开启。
//
// 这条测试来自一个真实缺陷：Console 字段加进结构体后，Default()
// 里漏了对应的初始化，于是它落到零值 Enabled=false，控制台被静默跳过。
// 那种缺陷不会让任何测试失败——它只是让功能不存在。
func TestWSSAloneIsAnEndpoint(t *testing.T) {
	c := Default()
	c.Listeners.MQTTTLS.Enabled = false
	c.Listeners.MQTTALPN.Enabled = false
	c.Listeners.MQTTPlain.Enabled = false
	c.Listeners.MQTTWSS.Enabled = true
	c.Listeners.MQTTWSS.Region = "us-east-1"
	c.Listeners.MQTTWSS.KeysFile = "keys.json"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultEnablesConsole(t *testing.T) {
	c := Default()
	if !c.Console.Enabled {
		t.Error("控制台默认应开启")
	}
	if c.Console.BasePath != "/" {
		t.Errorf("默认挂载路径 = %q，期望 /", c.Console.BasePath)
	}
}

// TestEveryConfigSectionHasNonZeroDefault 防止"新增配置段忘了给默认值"。
//
// 逐个字段断言会在每次新增时漏掉；这里检查每个顶层段都不是纯零值，
// 使"加了结构体字段却没在 Default() 里初始化"立刻失败。
func TestEveryConfigSectionHasNonZeroDefault(t *testing.T) {
	c := Default()
	sections := map[string]bool{
		"node":      c.Node.Mode != "",
		"cluster":   c.Cluster.ShardCount != 0,
		"meta":      c.Meta.Backend != "",
		"view":      c.View.ResyncInterval != 0,
		"listeners": c.Listeners.MQTTTLS.Addr != "",
		"pki":       c.PKI.DefaultValidity != 0,
		"iot":       c.IoT.Partition != "",
		"console":   c.Console.BasePath != "",
		"auth":      c.Auth.Mode != "",
		"log":       c.Log.Level != "",
	}
	for name, ok := range sections {
		if !ok {
			t.Errorf("配置段 %s 在 Default() 中没有默认值；"+
				"落到零值的段会让对应功能被静默跳过", name)
		}
	}
}
