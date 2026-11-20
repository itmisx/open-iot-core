package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// TestQuotaOverridesResolve 钉住按名字写的覆盖解析成编号表。
func TestQuotaOverridesResolve(t *testing.T) {
	got, err := quotaOverrides(map[string]int{
		"connections":  1_000_000,
		"connect_rate": 20_000,
		"payload_size": 0,
	})
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	want := map[quota.ID]int{
		quota.Connections: 1_000_000,
		quota.ConnectRate: 20_000,
		quota.PayloadSize: 0,
	}
	if len(got) != len(want) {
		t.Fatalf("解出 %d 项，应为 %d 项", len(got), len(want))
	}
	for id, v := range want {
		if got[id] != v {
			t.Fatalf("编号 %d 解出 %d，应为 %d", id, got[id], v)
		}
	}
	if ov, err := quotaOverrides(nil); err != nil || ov != nil {
		t.Fatalf("空覆盖应解出 nil，得到 %v / %v", ov, err)
	}
}

// TestQuotaOverridesRejectsUnknown 钉住未知配额名拒绝启动（QTA-110）。
//
// 不静默忽略：一个把 connections 写成 connection 的部署会以默认的 50 万
// 上限运行，而运维以为配的是一百万——这个错误只在真打到上限那天显形，
// 那时它看起来像一次容量故障，不像一行配置打错了字。
func TestQuotaOverridesRejectsUnknown(t *testing.T) {
	_, err := quotaOverrides(map[string]int{"connection": 10, "connections": 10})
	var fe *config.FieldError
	if !errors.As(err, &fe) {
		t.Fatalf("应报出指向字段的错误，得到 %v", err)
	}
	if fe.Field != "quota.connection" {
		t.Fatalf("错误指向 %s，应为 quota.connection", fe.Field)
	}
	// 提示里要列出可用名字：只说"未知"等于让人去翻 PRD 猜拼写。
	if !strings.Contains(fe.Hint, "connections") {
		t.Fatalf("提示里应列出可用配额名，得到 %q", fe.Hint)
	}
}

// TestQuotaOverridesUnknownIsDeterministic 钉住多项都未知时报的字段稳定。
func TestQuotaOverridesUnknownIsDeterministic(t *testing.T) {
	m := map[string]int{"zzz_quota": 1, "aaa_quota": 1, "mmm_quota": 1}
	for i := 0; i < 20; i++ {
		_, err := quotaOverrides(m)
		var fe *config.FieldError
		if !errors.As(err, &fe) {
			t.Fatal("应报出指向字段的错误")
		}
		if fe.Field != "quota.aaa_quota" {
			t.Fatalf("第 %d 次报的是 %s，应恒为字典序最小的那项", i, fe.Field)
		}
	}
}

// TestBrokerOptionsMapping 钉住配置项逐个落到 broker 选项上。
//
// 逐字段断言而不是比较整个结构：选项里还有装配期填的依赖，比较整体就要
// 在这里重复造一遍它们，而那正是会随装配变化而腐坏的那种断言。
func TestBrokerOptionsMapping(t *testing.T) {
	c := config.BrokerConfig{
		WriterWorkers:  32,
		OutboundBuffer: 256,
		ReadBuffer:     8192,
		MaxWriteBatch:  131072,
		WriteTimeout:   config.Duration(2 * time.Second),
	}
	var opt broker.Options
	brokerOptions(&c, &opt)
	if opt.WriterWorkers != 32 || opt.OutboundBuffer != 256 ||
		opt.ReadBufferSize != 8192 || opt.MaxWriteBatch != 131072 ||
		opt.WriteTimeout != 2*time.Second {
		t.Fatalf("映射结果是 %+v", opt)
	}
	// 零值必须保持为零：默认值由 broker 包补齐，这里补一遍就是第二个真相。
	var zero config.BrokerConfig
	opt = broker.Options{}
	brokerOptions(&zero, &opt)
	if opt.WriterWorkers != 0 || opt.OutboundBuffer != 0 ||
		opt.ReadBufferSize != 0 || opt.MaxWriteBatch != 0 || opt.WriteTimeout != 0 {
		t.Fatalf("零值不该被填上默认值，得到 %+v", opt)
	}
}

// TestQuotaOverrideTakesEffect 走一次真装配，钉住配置里的配额真的生效。
//
// 这条用例的理由是本仓库最典型的缺口形状（见 CLAUDE.md 第 6 节第 3 条）：
// 配额表一直支持 Overrides，而装配处从来没传过——代码语法与类型都正确，
// 任何单元测试都不会红，表现是「配了却不生效」。
func TestQuotaOverrideTakesEffect(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.Quota = map[string]int{"connections": 123_456}
		c.Broker.WriterWorkers = 3
	})
	r.start()

	if got := r.s.lim.Limit(quota.Connections); got != 123_456 {
		t.Fatalf("并发连接上限生效值为 %d，应为配置里的 123456", got)
	}
	if got := r.s.brk.Stats().Writer.Workers; got != 3 {
		t.Fatalf("写出池起了 %d 个工作协程，应为配置里的 3", got)
	}
}

// TestQuotaOverrideDefaultsUnchanged 钉住不配时仍是默认值。
func TestQuotaOverrideDefaultsUnchanged(t *testing.T) {
	r := newTestRig(t, nil)
	r.start()

	def, _ := quota.Lookup(quota.Connections)
	if got := r.s.lim.Limit(quota.Connections); got != def.Limit {
		t.Fatalf("没配覆盖时生效值为 %d，应为默认的 %d", got, def.Limit)
	}
	// 写出池按核数自适应，至少起到下限那么多。
	if got := r.s.brk.Stats().Writer.Workers; got < 8 {
		t.Fatalf("写出池只起了 %d 个工作协程", got)
	}
}

// TestUnknownQuotaNameFailsStartup 钉住坏名字在装配期就失败，而不是起来之后。
func TestUnknownQuotaNameFailsStartup(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.Quota = map[string]int{"no_such_quota": 1}
	})
	_, err := New(Options{Config: r.cfg, DataDir: r.dd, Manifest: r.mf})
	var fe *config.FieldError
	if !errors.As(err, &fe) {
		t.Fatalf("装配应当失败并指向字段，得到 %v", err)
	}
	if fe.Field != "quota.no_such_quota" {
		t.Fatalf("错误指向 %s", fe.Field)
	}
}
