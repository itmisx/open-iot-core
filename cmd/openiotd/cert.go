package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/server"
)

const certUsage = `用法：
  openiotd cert issue  --thing <名字> --policy <策略名> [参数]
  openiotd cert revoke --fingerprint <指纹> [--reason <原因>]
`

func runCert(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, certUsage)
		return fmt.Errorf("须给出子命令 issue 或 revoke")
	}
	switch args[0] {
	case "issue":
		return runCertIssue(args[1:], stdout)
	case "revoke":
		return runCertRevoke(args[1:], stdout)
	default:
		fmt.Fprint(os.Stderr, certUsage)
		return fmt.Errorf("未知的 cert 子命令 %q", args[0])
	}
}

func runCertIssue(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("openiotd cert issue", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	thing := fs.String("thing", "", "设备名（必填）")
	cn := fs.String("cn", "", "证书通用名，默认取设备名")
	policy := fs.String("policy", "", "策略文档名（必填）")
	validity := fs.Duration("validity", 0, "有效期，默认取 pki.default_validity")
	outDir := fs.String("out", ".", "证书与私钥的输出目录")
	dataDir := fs.String("data-dir", "", "数据目录（默认 ./data，可用 OPENIOTD_DATA_DIR 覆盖）")
	cfgFile := fs.String("config", "", "配置文件")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, dd, reg, nodeID, err := prepare(*cfgFile, *dataDir)
	if err != nil {
		return err
	}
	ca, err := server.LoadCA(dd, &cfg, nowMs)
	if err != nil {
		return err
	}

	res, err := server.IssueDevice(&cfg, ca, reg, server.IssueDeviceRequest{
		Thing: *thing, CommonName: *cn, Policy: *policy,
		Validity: *validity, OutDir: *outDir,
	})
	if err != nil {
		return err
	}
	if err := server.AppendAudit(dd.Root(), nowMs(), nodeID, "cert issue",
		"IssueCertificate", res.Fingerprint+" thing="+*thing); err != nil {
		return fmt.Errorf("证书已签发，但审计日志写入失败: %w", err)
	}
	fmt.Fprintf(stdout, `已签发设备证书

  设备名   %s
  策略     %s
  指纹     %s
  到期     %s

  证书     %s
  私钥     %s（权限 600）
  CA 链    %s

私钥只在此处出现一次，服务端不留存（PRD PKI-010）——请立即交给设备并妥善保管。
若节点正在运行，需重启才会载入新证书（文件化注册表是 v0.1 内核阶段的临时实现）。
`, *thing, *policy, res.Fingerprint, res.NotAfter.Format(time.RFC3339),
		res.CertPath, res.KeyPath, res.ChainPath)
	return nil
}

func runCertRevoke(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("openiotd cert revoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fp := fs.String("fingerprint", "", "证书指纹（必填）")
	reason := fs.String("reason", "", "吊销原因，进入审计")
	dataDir := fs.String("data-dir", "", "数据目录（默认 ./data，可用 OPENIOTD_DATA_DIR 覆盖）")
	cfgFile := fs.String("config", "", "配置文件")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *fp == "" {
		return fmt.Errorf("须给出 --fingerprint")
	}
	_, dd, reg, nodeID, err := prepare(*cfgFile, *dataDir)
	if err != nil {
		return err
	}
	if err := reg.SetCertStatus(*fp, "REVOKED", *reason); err != nil {
		return err
	}
	target := *fp
	if *reason != "" {
		target += " reason=" + *reason
	}
	if err := server.AppendAudit(dd.Root(), nowMs(), nodeID, "cert revoke",
		"RevokeCertificate", target); err != nil {
		return fmt.Errorf("证书已吊销，但审计日志写入失败: %w", err)
	}
	// 这里刻意不说"重启就会生效"：它不会。
	//
	// 这条命令改的是文件化注册表，而启动时的导入（server.importFileRegistry）
	// 只补"控制面里还没有的指纹"——证书一旦进了控制面（节点起过一次就进了），
	// 这个 REVOKED 永远不会被采纳，重启也不会。原先这段文案写的是"需重启
	// 节点才会生效"，那是一句比没有更糟的提示：按它做完会以为吊销了。
	fmt.Fprintf(stdout, `已将证书 %s 在文件化注册表里标记为 REVOKED

注意：**这不等于吊销已生效**。PKI-030 要求吊销即时断连，而生效只有一条路：
运行中的节点经管理面应用它。

  curl -H "Authorization: Bearer $TOKEN" -X PUT \
    -H 'Content-Type: application/json' -d '{"status":"REVOKED"}' \
    http://127.0.0.1:9090/api/admin/v1/certificates/%s/status

控制台的证书页是同一条路径。这条命令只在节点**从未启动过**（证书还没进
控制面）时才会随下一次启动被采纳；证书已经在控制面里的话，重启也不会生效。
`, *fp, *fp)
	return nil
}

// prepare 载入配置与数据目录。
func prepare(cfgFile, dataDir string) (config.Config, *node.DataDir, *server.Registry, string, error) {
	f := config.NewFlags("openiotd", os.Stderr)
	// 只在用户真的给了 --data-dir 时才传下去。
	//
	// 以前这里无条件传一个默认值 "./data"，而命令行的优先级高于环境
	// 变量——于是 OPENIOTD_DATA_DIR 对 cert 子命令永远不生效。容器里
	// 这条尤其致命：数据目录靠环境变量指向卷，而 cert issue 会去动
	// ./data，要么因没有权限而失败，要么**另建一套 CA**，签出来的证书
	// 连不上正在跑的那个节点，而两边都不会报错。
	var args []string
	if dataDir != "" {
		args = append(args, "--data-dir", dataDir)
	}
	if cfgFile != "" {
		args = append(args, "--config", cfgFile)
	}
	if err := f.Parse(args); err != nil {
		return config.Config{}, nil, nil, "", err
	}
	cfg, err := config.Load(f, os.LookupEnv)
	if err != nil {
		return config.Config{}, nil, nil, "", err
	}
	dd, err := node.Open(cfg.Node.DataDir)
	if err != nil {
		return cfg, nil, nil, "", err
	}
	mf, err := node.Identify(dd, node.IdentityRequest{
		NodeID: cfg.Node.ID, ShardCount: cfg.Cluster.ShardCount,
	})
	if err != nil {
		return cfg, dd, nil, "", err
	}
	return cfg, dd, server.NewRegistry(dd.Root()), mf.NodeID, nil
}

func nowMs() int64 { return time.Now().UnixMilli() }
