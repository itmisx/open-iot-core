package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/server"
)

// runInit 初始化数据目录。
//
// 单列一个命令而不是让 serve 自动初始化，是刻意的（CLU-055）：serve
// 遇到未初始化的目录应当明确报错。一个因为 data_dir 打错而自动新建空
// 集群的 serve，症状是"所有设备认证失败"，而真因完全看不出来。
func runInit(args []string, stdout io.Writer) error {
	f := config.NewFlags("openiotd init", os.Stderr)
	// --if-needed 把 init 变成幂等的：已经做过的部分跳过，整体仍然成功。
	//
	// 它是给编排用的。compose 的 init 服务与 k8s 的 initContainer 每次
	// 起容器都会执行一遍，而不带这个开关时第二次会以「PKI 已存在」失败。
	// 三份清单此前各自用一段 `if [ -f manifest.json ]` 的 shell 把它绕开，
	// 那有两个毛病：同一段守卫手工重复三遍，而且它判的是「身份文件在不在」
	// 就跳过**全部**步骤——于是一个只建到一半的目录（比如上一次在签
	// 服务端证书时中断）永远补不齐，而下一次启动看起来是正常跳过的。
	//
	// 这不与 CLU-055 冲突：那条禁止的是 **serve** 自动初始化，理由是
	// data_dir 打错时会静默新建一个空集群。init 仍然是一个显式的步骤，
	// 这里只是让它可以被反复执行。
	ifNeeded := f.Bool("if-needed", false,
		"已初始化的部分跳过并以成功退出（供编排反复执行）")
	if err := f.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(f, os.LookupEnv)
	if err != nil {
		return err
	}
	log := newLogger(&cfg)

	dd, err := node.Open(cfg.Node.DataDir)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()

	mf, err := node.Identify(dd, node.IdentityRequest{
		NodeID:      cfg.Node.ID,
		ShardCount:  cfg.Cluster.ShardCount,
		AllowCreate: true,
		Bootstrap:   cfg.Node.Mode == config.ModeBootstrap,
		NowUnixMs:   now,
	})
	if err != nil {
		return err
	}
	log.Info("数据目录已初始化", "路径", dd.Root(),
		"node_id", mf.NodeID, "cluster_id", mf.ClusterID,
		"shard_count", mf.ShardCount)

	reg := server.NewRegistry(dd.Root())
	if err := reg.EnsureDirs(); err != nil {
		return err
	}

	pkiSkipped := false

	if cfg.PKI.Enabled {
		switch err := server.InitPKI(dd, &cfg, now); {
		case err == nil:
			log.Info("PKI 已生成", "目录", dd.Path(node.DirPKI),
				"服务端证书主机名", cfg.PKI.ServerHosts)
			log.Info("根 CA 私钥未落盘",
				"说明", "根 CA 仅用于签发在线签发 CA，其私钥在本次运行后即不可达")
		case *ifNeeded && errors.Is(err, server.ErrPKIExists):
			// 已经有 CA 了就不动它：重建等于换掉整条信任链，
			// 已签发的设备证书全部作废。
			log.Info("PKI 已存在，跳过", "目录", dd.Path(node.DirPKI))
			pkiSkipped = true
		default:
			return err
		}
	}

	if err := writeSamplePolicy(reg); err != nil {
		return err
	}

	// 生成管理面令牌。
	//
	// init 时自动生成而不是让用户自己造，是因为"安全"不该以"难用"为代价：
	// 要求用户手工设一个令牌，多数人会设成 admin。
	tokenPath := dd.Path(server.AdminTokenFileName)
	adminToken := ""
	if _, err := os.Stat(tokenPath); os.IsNotExist(err) {
		adminToken, err = server.GenerateAdminToken()
		if err != nil {
			return err
		}
		if err := server.WriteAdminTokenFile(tokenPath, adminToken); err != nil {
			return err
		}
		log.Info("管理面令牌已生成", "文件", tokenPath, "权限", "600")
	}
	// 编排里反复执行时，什么都没新建就只说一句。
	//
	// 否则每次起容器都会把整屏「下一步」与策略说明重打一遍，而那一屏是
	// 写给第一次用的人看的——反复出现只会把真正的日志挤出去，还让人
	// 以为节点又被重新初始化了一次。
	if *ifNeeded && pkiSkipped && adminToken == "" {
		fmt.Fprintf(stdout, "数据目录已就绪（无需改动）：%s\n", dd.Root())
		return nil
	}

	fmt.Fprintf(stdout, `
数据目录已就绪：%s

下一步：
  1. 签发一张设备证书
       openiotd cert issue --data-dir %s --thing dev-001 --policy device-scoped
  2. 启动节点
       openiotd serve --data-dir %s

设备需要信任的 CA 链在：%s
策略文档目录：%s
管理面令牌：  %s

device-scoped 策略放行的范围（按设备名收敛，各设备互不可见）：
  连接   clientId 必须等于设备名
  发布   $aws/things/<设备名>/*
  订阅   $aws/things/<设备名>/*

影子等保留主题与自己的业务主题都在这个前缀下，例如
  $aws/things/dev-001/shadow/update      影子
  $aws/things/dev-001/telemetry          业务

因此 clientId 填随机值会被拒（MQTTX 等图形客户端默认如此），
订阅 sensors/temp 这类平坦主题也会被拒——它无法按设备收敛。
两种情况都返回 NotAuthorized，真实原因见服务端日志的
"连接被拒"/"授权被拒"，或用控制台的策略调试器验证。
`, dd.Root(), dd.Root(), dd.Root(),
		dd.Path(node.DirPKI, "ca-chain.pem"), reg.PoliciesDir(), tokenPath)

	if adminToken != "" {
		fmt.Fprintf(stdout, `
管理面用法：
  curl -H "Authorization: Bearer $(cat %s)" \
       http://127.0.0.1:9090/api/admin/v1/node
`, tokenPath)
	}
	return nil
}

// samplePolicy 是一份典型的设备域策略。
//
// init 时写出它而不是留一个空目录：一个空的策略目录会让新用户
// 卡在"证书签出来了但连不上"，而原因是没有任何策略可绑。
//
// 只放行 $aws/things/<设备名>/* 这一条前缀（AUTH-069 修订）。
//
// 它同时覆盖两类主题：影子等保留主题走 ReservedService，其余子主题
// （$aws/things/<设备名>/telemetry 之类）在 HandleReserved 返回 ok=false
// 之后落回普通路由，因此业务发布与订阅一样能成功——新用户不会卡在
// "连上了但发什么都被拒"。
//
// 起步策略只给一条前缀而不是两条：两条就要解释"什么时候用哪条"，
// 而设备域的全部能力在 $aws/things/<设备名>/ 下已经齐了。少一条前缀
// 也少一处要讲清的东西，示例策略的首要职责是示范**形状**。
//
// 形状的关键是按设备名分层，而不是 sensors/temp 这样的平坦主题——
// 平坦主题无法按设备收敛：任何设备都能读写同一个主题，策略变量就
// 失去意义了。
const samplePolicy = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}
`

func writeSamplePolicy(reg *server.Registry) error {
	path := reg.PoliciesDir() + "/device-scoped.json"
	if _, err := os.Stat(path); err == nil {
		return nil // 已存在，不覆盖用户的修改
	}
	if err := os.WriteFile(path, []byte(samplePolicy), 0o600); err != nil {
		return fmt.Errorf("写入示例策略失败: %w", err)
	}
	return nil
}
