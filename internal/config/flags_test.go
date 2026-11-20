package config

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// failWriter 在写入时报错，用于覆盖编码失败分支。
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("写入失败") }

// TestEveryFlagOverrides 逐个验证每个 flag 都真的生效。
//
// 不逐个验证就等于没验证：Apply 是一个 switch，漏掉一个 case 的后果是
// 那个 flag 被静默忽略——用户在命令行写了 --mqtt-tls-addr，进程却监听
// 在配置文件里的地址上，而没有任何提示。
func TestEveryFlagOverrides(t *testing.T) {
	cases := []struct {
		flag  string
		value string
		check func(*Config) bool
	}{
		{"node-id", "n9", func(c *Config) bool { return c.Node.ID == "n9" }},
		{"mode", "bootstrap", func(c *Config) bool { return c.Node.Mode == ModeBootstrap }},
		{"data-dir", "/d", func(c *Config) bool { return c.Node.DataDir == "/d" }},
		{"seeds", "a:1, b:2 ,", func(c *Config) bool {
			return len(c.Node.Seeds) == 2 && c.Node.Seeds[0] == "a:1" && c.Node.Seeds[1] == "b:2"
		}},
		{"mqtt-tls-addr", "1.2.3.4:1", func(c *Config) bool {
			return c.Listeners.MQTTTLS.Addr == "1.2.3.4:1" && c.Listeners.MQTTTLS.Enabled
		}},
		{"mqtt-alpn-addr", "1.2.3.4:2", func(c *Config) bool {
			return c.Listeners.MQTTALPN.Addr == "1.2.3.4:2" && c.Listeners.MQTTALPN.Enabled
		}},
		{"mqtt-plain-addr", "1.2.3.4:3", func(c *Config) bool {
			return c.Listeners.MQTTPlain.Addr == "1.2.3.4:3" && c.Listeners.MQTTPlain.Enabled
		}},
		{"admin-addr", "1.2.3.4:4", func(c *Config) bool {
			return c.Listeners.Admin.Addr == "1.2.3.4:4" && c.Listeners.Admin.Enabled
		}},
		{"auth-mode", "insecure-none", func(c *Config) bool {
			return c.Auth.Mode == AuthInsecureNone
		}},
		{"log-level", "debug", func(c *Config) bool { return c.Log.Level == "debug" }},
		{"log-format", "text", func(c *Config) bool { return c.Log.Format == "text" }},
	}
	for _, c := range cases {
		t.Run(c.flag, func(t *testing.T) {
			f := NewFlags("t", io.Discard)
			if err := f.Parse([]string{"--" + c.flag, c.value}); err != nil {
				t.Fatal(err)
			}
			cfg := Default()
			f.Apply(&cfg)
			if !c.check(&cfg) {
				t.Errorf("--%s %s 未生效", c.flag, c.value)
			}
		})
	}
}

// TestEveryEnvVarOverrides 逐个验证每个环境变量都真的生效。
func TestEveryEnvVarOverrides(t *testing.T) {
	values := map[string]string{
		EnvPrefix + "NODE_ID":         "env-node",
		EnvPrefix + "MODE":            "join",
		EnvPrefix + "DATA_DIR":        "/env",
		EnvPrefix + "SEEDS":           "s1:1,s2:2",
		EnvPrefix + "MQTT_TLS_ADDR":   "9.9.9.9:1",
		EnvPrefix + "MQTT_ALPN_ADDR":  "9.9.9.9:2",
		EnvPrefix + "MQTT_PLAIN_ADDR": "9.9.9.9:3",
		EnvPrefix + "ADMIN_ADDR":      "9.9.9.9:4",
		EnvPrefix + "AUTH_MODE":       "insecure-none",
		EnvPrefix + "LOG_LEVEL":       "warn",
		EnvPrefix + "LOG_FORMAT":      "text",
	}
	// 清单与绑定表必须一一对应：少一条就有一个环境变量是死的
	for _, v := range EnvVars() {
		if _, ok := values[v.Name]; !ok {
			t.Errorf("环境变量 %s 未被本测试覆盖；绑定表新增项时须同步补测", v.Name)
		}
	}
	cfg := Default()
	cfg.ApplyEnv(envOf(values))

	checks := []struct {
		name string
		ok   bool
	}{
		{"node.id", cfg.Node.ID == "env-node"},
		{"node.mode", cfg.Node.Mode == ModeJoin},
		{"node.data_dir", cfg.Node.DataDir == "/env"},
		{"node.seeds", len(cfg.Node.Seeds) == 2},
		{"mqtt_tls.addr", cfg.Listeners.MQTTTLS.Addr == "9.9.9.9:1"},
		{"mqtt_alpn.addr", cfg.Listeners.MQTTALPN.Addr == "9.9.9.9:2" && cfg.Listeners.MQTTALPN.Enabled},
		{"mqtt_plain.addr", cfg.Listeners.MQTTPlain.Addr == "9.9.9.9:3" && cfg.Listeners.MQTTPlain.Enabled},
		{"admin.addr", cfg.Listeners.Admin.Addr == "9.9.9.9:4"},
		{"auth.mode", cfg.Auth.Mode == AuthInsecureNone},
		{"log.level", cfg.Log.Level == "warn"},
		{"log.format", cfg.Log.Format == "text"},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("%s 未被环境变量覆盖", c.name)
		}
	}
}

func TestApplyEnvNilLookup(t *testing.T) {
	cfg := Default()
	cfg.ApplyEnv(nil) // 不应 panic
	if cfg.Node.Mode != ModeStandalone {
		t.Error("nil 查找函数不应改动配置")
	}
}

func TestSplitList(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 0}, {"   ", 0}, {"a", 1}, {"a,b", 2},
		{" a , b ", 2}, {"a,,b", 2}, {",", 0}, {"a,", 1},
	}
	for _, c := range cases {
		if got := splitList(c.in); len(got) != c.want {
			t.Errorf("splitList(%q) 条数 = %d，期望 %d（%v）", c.in, len(got), c.want, got)
		}
	}
}

func TestFlagsUsageAndParseErrors(t *testing.T) {
	var sb strings.Builder
	f := NewFlags("t", &sb)
	f.Usage()
	// 用法说明须列出 flag，否则 --help 没有意义
	if !strings.Contains(sb.String(), "-config") {
		t.Errorf("用法说明未列出 flag:\n%s", sb.String())
	}
	f2 := NewFlags("t", io.Discard)
	if err := f2.Parse([]string{"--no-such-flag"}); err == nil {
		t.Error("未知 flag 应报错")
	}
}

func TestEncodeFailure(t *testing.T) {
	cfg := Default()
	if err := cfg.Encode(failWriter{}); err == nil {
		t.Error("写入失败应报错")
	}
}

func TestDecodeReadError(t *testing.T) {
	if _, err := Decode(failReader{}); err == nil {
		t.Error("读取失败应报错")
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("读取失败") }

// TestValidateRemainingBranches 覆盖校验里剩余的分支。
func TestValidateRemainingBranches(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(*Config)
		field string
	}{
		{"advertise 地址非法", func(c *Config) { c.Node.AdvertiseAddr = "bad" },
			"node.advertise_addr"},
		{"mesh 地址非法", func(c *Config) { c.Node.MeshAddr = "bad" }, "node.mesh_addr"},
		{"分区数过大", func(c *Config) { c.Cluster.ShardCount = 100000 },
			"cluster.shard_count"},
		{"副本数为零", func(c *Config) { c.Cluster.Voters = 0 }, "cluster.voters"},
		{"ALPN 端点地址非法", func(c *Config) {
			c.Listeners.MQTTALPN.Enabled = true
			c.Listeners.MQTTALPN.Addr = "nope"
		}, "listeners.mqtt_alpn.addr"},
		{"明文端点地址非法", func(c *Config) {
			c.Listeners.MQTTPlain = PlainListener{Enabled: true, Addr: "nope"}
		}, "listeners.mqtt_plain.addr"},
		{"管理端点地址非法", func(c *Config) { c.Listeners.Admin.Addr = "nope" },
			"listeners.admin.addr"},
		{"服务端主机名为空", func(c *Config) { c.PKI.ServerHosts = nil },
			"pki.server_hosts"},
		{"默认有效期非正", func(c *Config) { c.PKI.DefaultValidity = 0 },
			"pki.default_validity"},
		{"最长有效期非正", func(c *Config) { c.PKI.MaxValidity = 0 },
			"pki.max_validity"},
		{"分区为空", func(c *Config) { c.IoT.Partition = "" }, "iot.partition"},
		{"区域为空", func(c *Config) { c.IoT.Region = "" }, "iot.region"},
		{"账号为空", func(c *Config) { c.IoT.Account = "" }, "iot.account"},
		{"要求客户端证书但无信任来源", func(c *Config) { c.PKI.Enabled = false },
			"listeners.mqtt_tls.require_client_cert"},
		// 要走到 auth 的这条分支，监听器本身不能要求客户端证书——
		// 否则更具体的监听器错误会先触发，而那个顺序是对的
		{"cert 模式无信任来源", func(c *Config) {
			c.PKI.Enabled = false
			c.Listeners.MQTTTLS.RequireClientCert = false
			c.Listeners.MQTTALPN.Enabled = false
		}, "auth.mode"},
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
			if !errors.As(err, &fe) || fe.Field != c.field {
				t.Errorf("错误 = %v，期望指向 %s", err, c.field)
			}
		})
	}
}

// TestPKIFilesDisabledSkipsValidation 验证关闭 PKI 时跳过其校验。
func TestPKIFilesDisabledSkipsValidation(t *testing.T) {
	cfg := Default()
	cfg.PKI = PKIConfig{Enabled: false}
	cfg.Listeners.MQTTTLS.ClientCAFile = "/ca.pem"
	if err := cfg.Validate(); err != nil {
		t.Errorf("关闭 PKI 且提供了信任文件时应合法: %v", err)
	}
}

// TestPKIFilesAllThreeGiven 验证三个 CA 路径齐备时通过。
func TestPKIFilesAllThreeGiven(t *testing.T) {
	cfg := Default()
	cfg.PKI.RootCertFile = "/r.pem"
	cfg.PKI.IssuingCertFile = "/i.pem"
	cfg.PKI.IssuingKeyFile = "/i.key"
	if err := cfg.Validate(); err != nil {
		t.Errorf("三个路径齐备时应合法: %v", err)
	}
}

func TestCheckHostPortEdgeCases(t *testing.T) {
	bad := []string{"", "nohost", "host:", "h:abc", "h o s t:1"}
	for _, a := range bad {
		if err := checkHostPort(a); err == nil {
			t.Errorf("%q 应报错", a)
		}
	}
	good := []string{"127.0.0.1:1", ":8883", "example.com:443", "[::1]:1883"}
	for _, a := range good {
		if err := checkHostPort(a); err != nil {
			t.Errorf("%q 应合法: %v", a, err)
		}
	}
}

// TestAdminListenerValidation 验证管理端点的两道闸。
//
// 管理面能吊销证书、踢连接、改写策略。即便已配令牌，把它绑到网络上
// 仍须是一个有意识的决定。
func TestAdminListenerValidation(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(*Config)
		field string
	}{
		{"地址非法", func(c *Config) { c.Listeners.Admin.Addr = "nope" },
			"listeners.admin.addr"},
		{"绑 0.0.0.0 未确认", func(c *Config) {
			c.Listeners.Admin.Addr = "0.0.0.0:9090"
		}, "listeners.admin.acknowledge_public_exposure"},
		{"绑具体外网地址未确认", func(c *Config) {
			c.Listeners.Admin.Addr = "192.168.1.10:9090"
		}, "listeners.admin.acknowledge_public_exposure"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Default()
			c.mut(&cfg)
			err := cfg.Validate()
			var fe *FieldError
			if !errors.As(err, &fe) || fe.Field != c.field {
				t.Fatalf("= %v，期望指向 %s", err, c.field)
			}
			if fe.Hint == "" {
				t.Error("错误须给出修复提示")
			}
		})
	}

	// 确认后允许
	cfg := Default()
	cfg.Listeners.Admin.Addr = "0.0.0.0:9090"
	cfg.Listeners.Admin.AcknowledgePublicExposure = true
	if err := cfg.Validate(); err != nil {
		t.Errorf("确认后应允许: %v", err)
	}
	// 回环地址无需确认
	cfg2 := Default()
	cfg2.Listeners.Admin.Addr = "127.0.0.1:9090"
	if err := cfg2.Validate(); err != nil {
		t.Errorf("回环地址应无需确认: %v", err)
	}
	// 关闭时不校验
	cfg3 := Default()
	cfg3.Listeners.Admin = AdminListener{Enabled: false, Addr: "nope"}
	if err := cfg3.Validate(); err != nil {
		t.Errorf("关闭的端点不应被校验: %v", err)
	}
}
