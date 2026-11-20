package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/server"
)

const adminUsage = `用法：
  openiotd admin token [--data-dir <目录>] [--rotate]
`

func runAdmin(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, adminUsage)
		return fmt.Errorf("须给出子命令 token")
	}
	switch args[0] {
	case "token":
		return runAdminToken(args[1:], stdout)
	default:
		fmt.Fprint(stderr, adminUsage)
		return fmt.Errorf("未知的 admin 子命令 %q", args[0])
	}
}

// runAdminToken 打印或轮换管理面令牌。
func runAdminToken(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("openiotd admin token", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dataDir := fs.String("data-dir", "", "数据目录（默认 ./data，可用 OPENIOTD_DATA_DIR 覆盖）")
	rotate := fs.Bool("rotate", false, "生成新令牌并替换旧的")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// 走一遍配置解析而不是直接用 flag 值：数据目录在容器里由
	// OPENIOTD_DATA_DIR 指定，而这个子命令此前完全不看环境变量，
	// 于是在容器里 `openiotd admin token` 读的是 ./data 下那份并不
	// 存在的令牌——报的是"文件不存在"，指向完全错误的方向。
	resolved, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	dd, err := node.Open(resolved)
	if err != nil {
		return err
	}
	path := dd.Path(server.AdminTokenFileName)

	if *rotate {
		tok, err := server.GenerateAdminToken()
		if err != nil {
			return err
		}
		if err := server.WriteAdminTokenFile(path, tok); err != nil {
			return err
		}
		fmt.Fprintf(stdout, `管理面令牌已轮换

  %s

写入：%s（权限 600）

轮换后须重启节点才生效——令牌在启动时读入一次。
正在使用旧令牌的客户端会在重启后收到 401。
`, tok, path)
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf(
				"管理面令牌文件不存在：%s\n请先执行 `openiotd init`，"+
					"或用 `openiotd admin token --rotate` 生成一个", path)
		}
		return fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	fmt.Fprint(stdout, string(data))
	return nil
}

// resolveDataDir 按「命令行 > 环境变量 > 默认」定出数据目录。
//
// 复用 config 的解析而不是自己读一次环境变量：优先级规则只该有一处
// 定义，两处迟早会在某个子命令上不一致。
func resolveDataDir(flagValue string) (string, error) {
	f := config.NewFlags("openiotd admin", io.Discard)
	var args []string
	if flagValue != "" {
		args = append(args, "--data-dir", flagValue)
	}
	if err := f.Parse(args); err != nil {
		return "", err
	}
	cfg, err := config.Load(f, os.LookupEnv)
	if err != nil {
		return "", err
	}
	return cfg.Node.DataDir, nil
}
