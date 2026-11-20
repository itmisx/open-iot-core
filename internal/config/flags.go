package config

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// Flags 绑定可经命令行覆盖的配置项。
//
// NFR-060 与 CLU-034 要求 flags 优先于文件。实现上必须区分"显式设置"
// 与"取了零值"：若不区分，一个未给出的 --mqtt-tls-addr 会以空串覆盖掉
// 文件里的正确配置。stdlib 的 flag.FlagSet.Visit 只遍历显式设置过的项，
// 正是为此。
type Flags struct {
	// ConfigFile 为空表示不读配置文件，只用默认值加 flags。
	//
	// 不做隐式路径搜索：自动在若干目录里找配置文件，会直接导致
	// "到底读的是哪个文件"这类排查，而这正是 `config print` 要消除的。
	ConfigFile string

	fs *flag.FlagSet

	nodeID    string
	mode      string
	dataDir   string
	seeds     string
	tlsAddr   string
	alpnAddr  string
	plainAddr string
	adminAddr string
	authMode  string
	logLevel  string
	logFormat string
}

// NewFlags 创建 flag 绑定。
func NewFlags(name string, out io.Writer) *Flags {
	f := &Flags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
	f.fs.SetOutput(out)
	f.fs.StringVar(&f.ConfigFile, "config", "", "YAML 配置文件路径（不指定则只用默认值与 flags）")
	f.fs.StringVar(&f.nodeID, "node-id", "", "节点编号（覆盖 node.id）")
	f.fs.StringVar(&f.mode, "mode", "", "启动模式 standalone|bootstrap|join（覆盖 node.mode）")
	f.fs.StringVar(&f.dataDir, "data-dir", "", "数据目录（覆盖 node.data_dir）")
	f.fs.StringVar(&f.seeds, "seeds", "", "逗号分隔的种子节点（覆盖 node.seeds）")
	f.fs.StringVar(&f.tlsAddr, "mqtt-tls-addr", "", "8883 mTLS 监听地址（覆盖 listeners.mqtt_tls.addr）")
	f.fs.StringVar(&f.alpnAddr, "mqtt-alpn-addr", "", "443 ALPN 监听地址（覆盖 listeners.mqtt_alpn.addr）")
	f.fs.StringVar(&f.plainAddr, "mqtt-plain-addr", "", "明文 MQTT 监听地址（覆盖 listeners.mqtt_plain.addr，并启用它）")
	f.fs.StringVar(&f.adminAddr, "admin-addr", "", "管理端点监听地址（覆盖 listeners.admin.addr）")
	f.fs.StringVar(&f.authMode, "auth-mode", "", "认证方式 cert|insecure-none（覆盖 auth.mode）")
	f.fs.StringVar(&f.logLevel, "log-level", "", "日志级别（覆盖 log.level）")
	f.fs.StringVar(&f.logFormat, "log-format", "", "日志格式 json|text（覆盖 log.format）")
	return f
}

// Parse 解析参数。
func (f *Flags) Parse(args []string) error {
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	if n := f.fs.NArg(); n > 0 {
		return fmt.Errorf("config: 无法识别的参数: %s", strings.Join(f.fs.Args(), " "))
	}
	return nil
}

// Usage 打印用法。
func (f *Flags) Usage() { f.fs.Usage() }

// Bool 在本命令自己的 flag 集合上登记一个开关。
//
// 命令专属的 flag 不进 Apply，也不进环境变量清单（NFR-060c）：它们不对应
// 任何配置项，只影响这一次命令怎么跑。放在这里而不是让各命令另起一个
// FlagSet，是为了让 `--config`、`--data-dir` 这些公共 flag 仍然可用——
// 另起一个的话每个命令都要把它们重新登记一遍。
func (f *Flags) Bool(name string, value bool, usage string) *bool {
	return f.fs.Bool(name, value, usage)
}

// Apply 把显式设置过的 flags 覆盖到配置上。
func (f *Flags) Apply(c *Config) {
	f.fs.Visit(func(fl *flag.Flag) {
		switch fl.Name {
		case "node-id":
			c.Node.ID = f.nodeID
		case "mode":
			c.Node.Mode = Mode(f.mode)
		case "data-dir":
			c.Node.DataDir = f.dataDir
		case "seeds":
			c.Node.Seeds = splitList(f.seeds)
		case "mqtt-tls-addr":
			c.Listeners.MQTTTLS.Addr = f.tlsAddr
			c.Listeners.MQTTTLS.Enabled = true
		case "mqtt-alpn-addr":
			c.Listeners.MQTTALPN.Addr = f.alpnAddr
			c.Listeners.MQTTALPN.Enabled = true
		case "mqtt-plain-addr":
			c.Listeners.MQTTPlain.Addr = f.plainAddr
			c.Listeners.MQTTPlain.Enabled = true
		case "admin-addr":
			c.Listeners.Admin.Addr = f.adminAddr
			c.Listeners.Admin.Enabled = true
		case "auth-mode":
			c.Auth.Mode = AuthMode(f.authMode)
		case "log-level":
			c.Log.Level = f.logLevel
		case "log-format":
			c.Log.Format = f.logFormat
		}
	})
}

// splitList 解析逗号分隔的列表，忽略空项与首尾空白。
func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Load 按「默认值 → 配置文件 → 环境变量 → flags」的顺序构建配置并校验。
//
// flags 优先级最高（NFR-060、CLU-034），环境变量居中：容器编排里用环境
// 变量注入、临时排查时用 flag 压过去，是最常见的两种用法。
//
// lookupEnv 由调用方注入而非直接读 os.Getenv，使配置加载可被完整测试。
func Load(f *Flags, lookupEnv func(string) (string, bool)) (Config, error) {
	cfg := Default()
	if f.ConfigFile != "" {
		loaded, err := LoadFile(f.ConfigFile)
		if err != nil {
			return Config{}, err
		}
		cfg = loaded
	}
	cfg.ApplyEnv(lookupEnv)
	f.Apply(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
