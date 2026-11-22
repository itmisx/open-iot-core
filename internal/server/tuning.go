package server

import (
	"fmt"
	"sort"
	"strings"

	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// quotaOverrides 把配置里按名字写的配额覆盖解析为编号表（QTA-109）。
//
// 解析放在这里而不是 config 包里，是因为名字表的权威是 quota 包的配额
// 定义，而 config 刻意只依赖标准库与 YAML 库（ARCH-062）。在 config 里
// 另抄一份名字映射就是第二个真相：加一项配额要改两处，漏改的表现是
// 「配置写了却没生效」，而那不会报错。
//
// 未知名字一律拒绝启动（QTA-110），不静默忽略。理由与 config.Decode 对
// 未知字段的处理相同：一个把 connections 写成 connection 的部署，若被
// 忽略就会以默认的 50 万上限运行，而运维以为自己配的是一百万——这个
// 错误只会在真打到上限那天显形，那时它看起来像一次容量故障。
func quotaOverrides(m map[string]int) (map[quota.ID]int, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make(map[quota.ID]int, len(m))
	// 遍历顺序取自 quota.Names()（字典序）而不是 map：同一份坏配置两次
	// 启动必须报同一个字段，否则会让人以为改掉一处又冒出一处。
	known := quota.Names()
	seen := make(map[string]bool, len(m))
	for _, name := range known {
		v, ok := m[name]
		if !ok {
			continue
		}
		seen[name] = true
		id, _ := quota.LookupName(name)
		out[id] = v
	}
	if len(seen) == len(m) {
		return out, nil
	}
	// 报第一个未知名字（仍按字典序，使错误可复现）。
	var unknown []string
	for name := range m {
		if !seen[name] {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return nil, &config.FieldError{
		Field:   "quota." + unknown[0],
		Problem: "不是已定义的配额名",
		Hint: fmt.Sprintf("可用的配额名共 %d 项：%s（也见 docs/prd/17-quotas.md）",
			len(known), strings.Join(known, "、")),
	}
}

// brokerOptions 把配置里的接入层参数填进 broker 选项。
//
// 零值一律保持为零，由 broker 侧按自己的默认值补齐（MQTT-121）：默认值
// 在 broker 包里只有一份，这里不复制。
func brokerOptions(c *config.BrokerConfig, opt *broker.Options) {
	opt.WriterWorkers = c.WriterWorkers
	opt.OutboundBuffer = c.OutboundBuffer
	opt.ReadBufferSize = c.ReadBuffer
	opt.MaxWriteBatch = c.MaxWriteBatch
	opt.WriteTimeout = c.WriteTimeout.D()
	opt.TCPKeepAlive = c.TCPKeepAlive.D()
	opt.TLSHandshakeConcurrency = c.TLSHandshakeConcurrency
}
