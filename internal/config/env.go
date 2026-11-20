package config

import "strings"

// EnvPrefix 是环境变量前缀。
const EnvPrefix = "OPENIOTD_"

// EnvVar 是一条环境变量到配置项的绑定。
//
// 用显式表而不是反射自动绑定：自动绑定会让"哪些键可以用环境变量覆盖"
// 取决于结构体当下的样子，重命名一个字段就会静默改掉环境变量名，
// 而那是会出现在用户 systemd 单元与容器编排文件里的契约。
type EnvVar struct {
	// Name 是完整的环境变量名，含前缀。
	Name string
	// Field 是等价的 YAML 路径，供文档与 `openiotd config env` 列出。
	Field string
	apply func(c *Config, v string)
}

var envBindings = []EnvVar{
	{EnvPrefix + "NODE_ID", "node.id", func(c *Config, v string) { c.Node.ID = v }},
	{EnvPrefix + "MODE", "node.mode", func(c *Config, v string) { c.Node.Mode = Mode(v) }},
	{EnvPrefix + "DATA_DIR", "node.data_dir", func(c *Config, v string) { c.Node.DataDir = v }},
	{EnvPrefix + "SEEDS", "node.seeds", func(c *Config, v string) { c.Node.Seeds = splitList(v) }},
	{EnvPrefix + "MQTT_TLS_ADDR", "listeners.mqtt_tls.addr", func(c *Config, v string) {
		c.Listeners.MQTTTLS.Addr = v
		c.Listeners.MQTTTLS.Enabled = true
	}},
	{EnvPrefix + "MQTT_ALPN_ADDR", "listeners.mqtt_alpn.addr", func(c *Config, v string) {
		c.Listeners.MQTTALPN.Addr = v
		c.Listeners.MQTTALPN.Enabled = true
	}},
	{EnvPrefix + "MQTT_PLAIN_ADDR", "listeners.mqtt_plain.addr", func(c *Config, v string) {
		c.Listeners.MQTTPlain.Addr = v
		c.Listeners.MQTTPlain.Enabled = true
	}},
	{EnvPrefix + "ADMIN_ADDR", "listeners.admin.addr", func(c *Config, v string) {
		c.Listeners.Admin.Addr = v
		c.Listeners.Admin.Enabled = true
	}},
	{EnvPrefix + "AUTH_MODE", "auth.mode", func(c *Config, v string) { c.Auth.Mode = AuthMode(v) }},
	{EnvPrefix + "LOG_LEVEL", "log.level", func(c *Config, v string) { c.Log.Level = v }},
	{EnvPrefix + "LOG_FORMAT", "log.format", func(c *Config, v string) { c.Log.Format = v }},
}

// EnvVars 返回全部可用的环境变量及其对应配置项，供 `openiotd config env` 列出。
//
// 让这份清单可被程序读取，是为了让文档不可能与实现脱节：
// 用法输出直接由它生成，而不是另写一份手工维护的列表。
func EnvVars() []EnvVar {
	out := make([]EnvVar, len(envBindings))
	copy(out, envBindings)
	return out
}

// ApplyEnv 把环境变量覆盖到配置上。
//
// lookup 由调用方注入（通常是 os.LookupEnv），以便测试不依赖进程环境。
// 空值视为未设置：一个 OPENIOTD_DATA_DIR= 的空赋值几乎总是脚本里的
// 变量展开失败，按空串覆盖掉正确配置会比报错更难排查。
func (c *Config) ApplyEnv(lookup func(string) (string, bool)) {
	if lookup == nil {
		return
	}
	for _, b := range envBindings {
		v, ok := lookup(b.Name)
		if !ok || strings.TrimSpace(v) == "" {
			continue
		}
		b.apply(c, v)
	}
}
