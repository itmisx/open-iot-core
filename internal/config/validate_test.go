package config_test

import (
	"strings"
	"testing"
	"time"

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

// TestReplicationRTTRange 钉住 CLU-132 的两端。
//
// 下限 1 毫秒是库的要求（tick 的单位是毫秒）；上限 1 秒是运维上的——
// 心跳与选举按它的倍数算，RTT 取十秒意味着选举超时上百秒，而一次
// leader 故障期间设备会全部重连完毕。两端都拦住比「配了就生效」好：
// 这一项的取值范围不是显然的。
func TestReplicationRTTRange(t *testing.T) {
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
		rep.Replicas = map[string]uint64{"n1": 1, "n2": 2, "n3": 3}
		rep.CertFile, rep.KeyFile, rep.CAFile = "c.pem", "k.pem", "ca.pem"
		return &cfg
	}
	for _, c := range []struct {
		rtt  time.Duration
		want bool
	}{
		{0, true}, // 0 表示取默认值（100 毫秒）
		{time.Millisecond, true},
		{5 * time.Millisecond, true},
		{time.Second, true},
		{500 * time.Microsecond, false},
		{2 * time.Second, false},
	} {
		cfg := base()
		cfg.Cluster.Replication.RTT = config.Duration(c.rtt)
		err := cfg.Validate()
		if (err == nil) != c.want {
			t.Errorf("rtt=%s 的校验结果 err=%v，期望通过=%v", c.rtt, err, c.want)
		}
	}
}

// TestStandaloneRejectsRTT 单机写了 rtt 也要报错，与复制配置的其余各项
// 同一条规矩（CLU-070）：那份配置看起来像在跑集群，而进程其实是单机。
func TestStandaloneRejectsRTT(t *testing.T) {
	cfg := config.Default()
	cfg.Node.Mode = config.ModeStandalone
	cfg.Node.DataDir = t.TempDir()
	cfg.Cluster.Replication.RTT = config.Duration(5 * time.Millisecond)
	if err := cfg.Validate(); err == nil {
		t.Error("单机配了 cluster.replication.rtt 应被拒")
	}
}

func TestSessionReplicationFactorValidation(t *testing.T) {
	base := func() config.Config {
		cfg := config.Default()
		cfg.Node.Mode = config.ModeBootstrap
		cfg.Node.ID = "n1"
		cfg.Node.DataDir = t.TempDir()
		cfg.Cluster.Voters = 5
		cfg.Cluster.Replication.Addr = "127.0.0.1:7001"
		cfg.Cluster.Replication.Peers = map[string]string{
			"n1": "127.0.0.1:7001", "n2": "127.0.0.1:7002",
			"n3": "127.0.0.1:7003", "n4": "127.0.0.1:7004",
			"n5": "127.0.0.1:7005",
		}
		cfg.Cluster.Replication.Replicas = map[string]uint64{
			"n1": 1, "n2": 2, "n3": 3, "n4": 4, "n5": 5,
		}
		cfg.Cluster.Replication.CertFile = "c.pem"
		cfg.Cluster.Replication.KeyFile = "k.pem"
		cfg.Cluster.Replication.CAFile = "ca.pem"
		return cfg
	}
	for _, tc := range []struct {
		rf   uint64
		want bool
	}{{0, true}, {1, false}, {2, false}, {3, true}, {4, false}, {5, true}, {6, false}} {
		cfg := base()
		cfg.Cluster.Replication.SessionReplicationFactor = tc.rf
		err := cfg.Validate()
		if (err == nil) != tc.want {
			t.Errorf("rf=%d err=%v，期望通过=%v", tc.rf, err, tc.want)
		}
	}
	for _, tc := range []struct {
		shards uint64
		want   bool
	}{{0, true}, {1, true}, {8, true}, {14, true}, {15, false}} {
		cfg := base()
		cfg.Cluster.Replication.ShadowShards = tc.shards
		err := cfg.Validate()
		if (err == nil) != tc.want {
			t.Errorf("shadow_shards=%d err=%v，期望通过=%v", tc.shards, err, tc.want)
		}
	}
	for _, tc := range []struct {
		shards uint64
		rf     uint64
		want   bool
	}{{0, 3, false}, {8, 0, true}, {8, 1, false}, {8, 2, false}, {8, 3, true}, {8, 4, false}, {8, 5, true}, {8, 7, false}} {
		cfg := base()
		cfg.Cluster.Replication.ShadowShards = tc.shards
		cfg.Cluster.Replication.ShadowReplicationFactor = tc.rf
		err := cfg.Validate()
		if (err == nil) != tc.want {
			t.Errorf("shadow_shards=%d rf=%d err=%v，期望通过=%v", tc.shards, tc.rf, err, tc.want)
		}
	}
}
