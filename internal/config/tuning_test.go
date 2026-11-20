package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestDecodeBrokerAndQuota 钉住两个新小节能被解出来。
func TestDecodeBrokerAndQuota(t *testing.T) {
	cfg, err := Decode(strings.NewReader(`
broker:
  writer_workers: 32
  outbound_buffer: 256
  read_buffer: 8192
  max_write_batch: 131072
  write_timeout: 2s
quota:
  connections: 1000000
  connect_rate: 20000
`))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	b := cfg.Broker
	if b.WriterWorkers != 32 || b.OutboundBuffer != 256 || b.ReadBuffer != 8192 ||
		b.MaxWriteBatch != 131072 || b.WriteTimeout.D() != 2*time.Second {
		t.Fatalf("broker 小节解出来是 %+v", b)
	}
	if cfg.Quota["connections"] != 1_000_000 || cfg.Quota["connect_rate"] != 20_000 {
		t.Fatalf("quota 小节解出来是 %v", cfg.Quota)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("这份配置应当有效: %v", err)
	}
}

// TestDecodeBrokerUnknownField 钉住打错的键会报错而不是被忽略。
//
// 与 Decode 对其余小节的处理同一条理由：一个把 writer_workers 写成
// writer_worker 的部署，若被忽略就会以默认值运行，而运维以为自己调过了。
func TestDecodeBrokerUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader("broker:\n  writer_worker: 32\n"))
	if err == nil {
		t.Fatal("未知字段应当报错")
	}
}

// TestValidateBrokerRejects 钉住会让节点自毁的取值被拒。
func TestValidateBrokerRejects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mut   func(*Config)
		field string
	}{
		{"协程数为负", func(c *Config) { c.Broker.WriterWorkers = -1 }, "broker.writer_workers"},
		{"协程数过大", func(c *Config) { c.Broker.WriterWorkers = maxWriterWorkers + 1 }, "broker.writer_workers"},
		{"出站队列为负", func(c *Config) { c.Broker.OutboundBuffer = -8 }, "broker.outbound_buffer"},
		{"读缓冲过小", func(c *Config) { c.Broker.ReadBuffer = 8 }, "broker.read_buffer"},
		{"合并上限过小", func(c *Config) { c.Broker.MaxWriteBatch = 16 }, "broker.max_write_batch"},
		{"刷写超时过短", func(c *Config) { c.Broker.WriteTimeout = Duration(time.Millisecond) }, "broker.write_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mut(&cfg)
			err := cfg.Validate()
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("应当报出指向字段的错误，得到 %v", err)
			}
			if fe.Field != tc.field {
				t.Fatalf("错误指向 %s，应为 %s", fe.Field, tc.field)
			}
			if fe.Hint == "" {
				t.Fatal("配置错误须给出改法（NFR-065）")
			}
		})
	}
}

// TestValidateBrokerAcceptsZero 钉住留空合法：零值表示取 broker 的默认值。
func TestValidateBrokerAcceptsZero(t *testing.T) {
	cfg := Default()
	if cfg.Broker != (BrokerConfig{}) {
		t.Fatalf("默认配置里 broker 小节应当全为零值，得到 %+v", cfg.Broker)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("全零的 broker 小节应当有效: %v", err)
	}
}

// TestValidateQuotaRejectsNegative 钉住负值被拒，且多项都坏时报的字段稳定。
//
// 稳定要紧：map 的遍历顺序是随机的，同一份坏配置两次启动报不同的字段，
// 会让人以为改掉一处又冒出一处。
func TestValidateQuotaRejectsNegative(t *testing.T) {
	cfg := Default()
	cfg.Quota = map[string]int{"connections": -1, "connect_rate": -2, "subscriptions": -3}
	for i := 0; i < 20; i++ {
		var fe *FieldError
		if !errors.As(cfg.Validate(), &fe) {
			t.Fatal("负值应当报出指向字段的错误")
		}
		if fe.Field != "quota.connect_rate" {
			t.Fatalf("第 %d 次报的是 %s，应恒为字典序最小的那项 quota.connect_rate", i, fe.Field)
		}
	}
}

// TestValidateQuotaAcceptsZero 钉住 0 合法：它的语义是取消该项限制。
func TestValidateQuotaAcceptsZero(t *testing.T) {
	cfg := Default()
	cfg.Quota = map[string]int{"connections": 0}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("0 应当合法: %v", err)
	}
}

// TestEncodeRoundTripsBrokerAndQuota 钉住 `config print` 打得出这两小节。
//
// 打印生效配置是排障的第一步（见 Encode 的说明），新加的小节若不在输出
// 里，那一步就会得到一份与实际不符的配置。
func TestEncodeRoundTripsBrokerAndQuota(t *testing.T) {
	cfg := Default()
	cfg.Broker.WriterWorkers = 16
	cfg.Quota = map[string]int{"connections": 123456}
	var sb strings.Builder
	if err := cfg.Encode(&sb); err != nil {
		t.Fatal(err)
	}
	out := sb.String()
	for _, want := range []string{"broker:", "writer_workers: 16", "quota:", "connections: 123456"} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出里没有 %q：\n%s", want, out)
		}
	}
	back, err := Decode(strings.NewReader(out))
	if err != nil {
		t.Fatalf("打印出来的配置应当能被解回去: %v", err)
	}
	if back.Broker != cfg.Broker || back.Quota["connections"] != 123456 {
		t.Fatalf("往返之后不一致: %+v / %v", back.Broker, back.Quota)
	}
}
