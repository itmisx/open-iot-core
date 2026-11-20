package main

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/itmisx/open-iot-core/internal/config"
)

const configUsage = `用法：
  openiotd config print  [--config <文件>]   打印生效配置
  openiotd config env                        列出可用的环境变量
`

func runConfig(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, configUsage)
		return fmt.Errorf("须给出子命令 print 或 env")
	}
	switch args[0] {
	case "print":
		return runConfigPrint(args[1:], stdout)
	case "env":
		return runConfigEnv(stdout)
	default:
		fmt.Fprint(stderr, configUsage)
		return fmt.Errorf("未知的 config 子命令 %q", args[0])
	}
}

// runConfigPrint 打印三层叠加后的生效配置。
//
// 这不是便利功能：配置来自默认值、文件、环境变量、flags 四层，
// 出问题时第一个要回答的问题就是"到底生效的是什么"。
func runConfigPrint(args []string, stdout io.Writer) error {
	f := config.NewFlags("openiotd config print", stdout)
	if err := f.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(f, osLookupEnv)
	if err != nil {
		return err
	}
	return cfg.Encode(stdout)
}

func runConfigEnv(stdout io.Writer) error {
	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "环境变量\t对应配置项")
	for _, v := range config.EnvVars() {
		fmt.Fprintf(w, "%s\t%s\n", v.Name, v.Field)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "\n优先级：默认值 < 配置文件 < 环境变量 < 命令行 flags")
	return nil
}
