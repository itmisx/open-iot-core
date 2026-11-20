package config_test

import (
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/config"
)

// TestReplicasMappingIsValidated 钉住副本号映射的三条校验（CLU-002 修订）。
//
// 三条都要在启动时拦，因为它们在运行时的症状都不指向这张表：缺一个节点
// 那个节点没有共识身份；号重复则两个节点抢同一个身份，表现是日志复制
// 错乱；号为 0 会被共识库当成"未指定"。
func TestReplicasMappingIsValidated(t *testing.T) {
	base := func() *config.Config {
		cfg := config.Default()
		cfg.Node.ID = "n1"
		cfg.Node.Mode = config.ModeBootstrap
		cfg.Node.DataDir = t.TempDir()
		cfg.Cluster.Voters = 3
		rep := &cfg.Cluster.Replication
		rep.Addr = "127.0.0.1:7000"
		rep.Peers = map[string]string{
			"n1": "127.0.0.1:7000", "n2": "127.0.0.1:7001", "n3": "127.0.0.1:7002",
		}
		rep.CertFile, rep.KeyFile, rep.CAFile = "c.pem", "k.pem", "ca.pem"
		return &cfg
	}

	// 缺省不再放行：共识库按副本号认成员，没有这张表它起不来。
	if err := base().Validate(); err == nil {
		t.Fatal("没写 replicas 应当被拒")
	}

	cases := []struct {
		name string
		rep  map[string]uint64
		want string
	}{
		{"条数对不上", map[string]uint64{"n1": 1, "n2": 2}, "peers"},
		{"缺一个节点", map[string]uint64{"n1": 1, "n2": 2, "nX": 3}, "n3"},
		{"副本号为 0", map[string]uint64{"n1": 0, "n2": 2, "n3": 3}, "0"},
		{"副本号重复", map[string]uint64{"n1": 1, "n2": 1, "n3": 3}, "都是 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base()
			cfg.Cluster.Replication.Replicas = c.rep
			err := cfg.Validate()
			if err == nil {
				t.Fatal("应当被拒")
			}
			if !strings.Contains(err.Error(), "replicas") {
				t.Errorf("错误没指向那个字段: %v", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误里没说清是哪一条（期望含 %q）: %v", c.want, err)
			}
		})
	}

	// 写对了要放行。
	cfg := base()
	cfg.Cluster.Replication.Replicas = map[string]uint64{"n1": 1, "n2": 2, "n3": 3}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("写对了不该失败：%v", err)
	}
}
