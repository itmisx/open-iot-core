// 命令 openiotd 是 open-iot-core 的节点程序。
//
// 本包刻意保持很薄：参数解析在这里，装配与运行在 internal/server。
// cmd 包里的代码只能经 exec 整个二进制来测试，那种测试又慢又难断言。
package main

import (
	"fmt"
	"io"
	"os"
)

// version 由构建时注入：go build -ldflags "-X main.version=v0.1.0"。
var (
	version = "dev"
	commit  = "unknown"
)

const usage = `openiotd —— open-iot-core 节点

用法：
  openiotd <命令> [参数]

命令：
  init          初始化数据目录、生成 CA 与服务端证书（--if-needed 可反复执行）
  serve         启动节点
  cert issue    签发设备证书并登记
  cert revoke   吊销设备证书
  config print  打印生效配置
  config env    列出可用的环境变量
  admin token   重新生成管理面令牌
  version       打印版本

以 openiotd <命令> --help 查看各命令的参数。

首次使用：
  openiotd init --data-dir ./data
  openiotd cert issue --data-dir ./data --thing dev-001 --policy device-scoped
  openiotd serve --data-dir ./data
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// osLookupEnv 是 os.LookupEnv 的别名。
//
// 各处统一经它取环境变量，使"配置从哪里来"只有一个入口；
// 测试则直接调用 internal/config 的注入接口，不碰进程环境。
var osLookupEnv = os.LookupEnv

// run 是可测的入口。
//
// main 只负责把进程的参数与标准流接过来、把返回值变成退出码。
// 把分发逻辑留在 main 里就只能 exec 整个二进制来测，那种测试
// 又慢又难断言——与把装配逻辑放进 internal/server 是同一个理由。
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, rest := args[0], args[1:]

	var err error
	switch cmd {
	case "init":
		err = runInit(rest, stdout)
	case "serve":
		err = runServe(rest)
	case "cert":
		err = runCert(rest, stdout)
	case "config":
		err = runConfig(rest, stdout, stderr)
	case "admin":
		err = runAdmin(rest, stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "openiotd %s (%s)\n", version, commit)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
	default:
		fmt.Fprintf(stderr, "未知命令 %q\n\n%s", cmd, usage)
		return 2
	}
	if err != nil {
		// 错误信息直接打到 stderr。不加"error:"前缀——本项目的错误信息
		// 本身就被要求说明发生了什么与如何恢复（PRD NFR-065），
		// 再加一层前缀只是噪声。
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
