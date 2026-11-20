package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/store"
)

// 集群模式的装配（CLU-067 至 CLU-074）。
//
// 这里是**单节点的集群**：voters 为 1，投票者名单只有自己。它和
// standalone 不是同一条路径——命令要经过真的共识组、线格式、TLS 监听与
// 追平循环，只是多数派恰好是一个人。三个进程的那套在
// tests/cluster/run.sh，不进门禁（要 openssl，要等选举）。

// freeAddr 取一个当前空闲的本地地址。
//
// 复制组的监听地址要写进投票者名单，所以不能用 :0 让内核挑——
// 名单里必须是别人能拨过来的那个地址。
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// mintReplicationPKI 签一条复制用的信任链。
//
// 与设备 CA 无关（CLU-068）：共用的话，拿一张设备证书就能冒充节点。
// 证书的 CommonName 是节点 ID（CLU-069 要求握手时按它认身份），
// SAN 含 localhost 与 127.0.0.1 供拨号侧做主机名校验。
func mintReplicationPKI(t *testing.T, nodeID string) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "replication-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certFile = filepath.Join(dir, "node.pem")
	keyFile = filepath.Join(dir, "node.key")
	caFile = filepath.Join(dir, "ca.pem")
	write := func(path, blockType string, der []byte) {
		if err := os.WriteFile(path,
			pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(certFile, "CERTIFICATE", der)
	write(keyFile, "EC PRIVATE KEY", keyDER)
	write(caFile, "CERTIFICATE", caDER)
	return certFile, keyFile, caFile
}

// clusterConfig 把一份配置改成单节点集群。
func clusterConfig(t *testing.T, cfg *config.Config, nodeID string) {
	t.Helper()
	certFile, keyFile, caFile := mintReplicationPKI(t, nodeID)
	cfg.Node.ID = nodeID
	cfg.Node.Mode = config.ModeBootstrap
	cfg.Cluster.Voters = 1
	cfg.Cluster.Replication = config.ReplicationConfig{
		Addr:         freeAddr(t),
		CertFile:     certFile,
		KeyFile:      keyFile,
		CAFile:       caFile,
		PumpInterval: config.Duration(50 * time.Millisecond),
	}
	cfg.Cluster.Replication.Peers = map[string]string{
		nodeID: cfg.Cluster.Replication.Addr,
	}
	// 副本号映射：共识库按 uint64 认成员（CLU-002 修订）。
	cfg.Cluster.Replication.Replicas = map[string]uint64{nodeID: 1}
}

// TestClusterModeWritesThroughTheLog 集群模式下写入经复制日志，
// 冷层与热视图都跟着走。
//
// 单机那条路径（LocalStore 直接追加一副本组）不经过线格式、不经过
// 共识组、不经过追平循环。这条测试是唯一在门禁里走那三样的。
func TestClusterModeWritesThroughTheLog(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()

	ctx := context.Background()
	// 起步策略由 init 写成文件，集群下由 bootstrap 节点导入（CLU-074）。
	// 它应当已经在日志里，于是能直接拿来绑设备。
	if _, ok := r.s.reg2.PolicySetID("device"); !ok {
		t.Fatal("bootstrap 节点没有导入文件化注册表里的策略")
	}

	if err := r.s.reg2.PutThing(ctx, registry.Thing{
		Name: "lamp-1", PolicyName: "device",
		Attributes: map[string]string{"site": "sh"},
	}); err != nil {
		t.Fatalf("集群模式下写设备失败: %v", err)
	}

	// 冷层读得到完整记录（CLU-065：记录与热视图在同一条日志里）
	got, err := r.s.reg2.GetThing("lamp-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Attributes["site"] != "sh" || got.PolicyName != "device" {
		t.Fatalf("读回 = %+v", got)
	}
	// 热视图也要有
	if _, ok := r.s.store.View().Thing("lamp-1"); !ok {
		t.Error("热视图里没有这台设备")
	}
	// revision 推进了，而且是日志序号
	if rev := r.s.store.View().Revision(); rev < 2 {
		t.Errorf("revision = %d，至少该有导入的策略加这台设备", rev)
	}
	// 复制日志落在 data dir 的 raft/ 下，与冷层分开（DirRaft 的理由：
	// 冷层可以从日志重建，反过来不行，所以"备份冷层"与"备份日志"
	// 不该是同一次操作）。
	//
	// 只断言那个目录非空，不钉具体文件名：日志的内部布局归共识库管
	// （换库之后它是 dragonboat 的 LogDB），钉文件名等于把库的实现
	// 细节写进用例，而那会在下一次升级时报假红。
	ents, err := os.ReadDir(filepath.Join(r.dd.Root(), "raft"))
	if err != nil || len(ents) == 0 {
		t.Errorf("raft 目录里没有复制日志: %v（%d 项）", err, len(ents))
	}
}

// TestClusterModeRecoversFromItsOwnLog 重启之后状态从日志重建。
//
// 集群下热视图不是从冷层 Restore 出来的，而是重放已提交日志（那是
// 权威）。日志重放不回来，节点就带着一份空视图开始服务——设备全都
// 连不上，而冷层里什么都在。
func TestClusterModeRecoversFromItsOwnLog(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	if err := r.s.reg2.PutThing(context.Background(), registry.Thing{
		Name: "lamp-1", PolicyName: "device",
	}); err != nil {
		t.Fatal(err)
	}
	wantRev := r.s.store.View().Revision()
	addr := r.cfg.Cluster.Replication.Addr
	r.stop()

	// 换一个 Server，同一个 data dir、同一份 manifest 与同一份复制配置。
	//
	// 不走 newTestRig：它会再 init 一次 PKI，而那个目录已经有 CA 了
	// （重建会被拒，这是对的——重建 CA 等于把全部已签发证书作废）。
	cfg := r.cfg
	clusterConfig(t, &cfg, "n1")
	cfg.Node.DataDir = r.cfg.Node.DataDir
	cfg.Cluster.Replication.Addr = addr
	cfg.Cluster.Replication.Peers = map[string]string{"n1": addr}
	cfg.Cluster.Replication.Replicas = map[string]uint64{"n1": 1}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	again := &testRig{t: t, cfg: cfg, dd: r.dd, mf: r.mf}
	again.start()

	if _, ok := again.s.store.View().Thing("lamp-1"); !ok {
		t.Error("重启后热视图里没有这台设备（日志没有被重放）")
	}
	if got := again.s.store.View().Revision(); got != wantRev {
		t.Errorf("重启后 revision = %d，期望 %d", got, wantRev)
	}
}

// TestStandaloneRejectsReplicationConfig 单机写了复制配置要报错。
//
// 忽略它的后果是：那份配置看起来像在跑集群，而进程其实是单机——
// 「不能把单机进程说成高可用」（RMAP-004）只有在配置层面也拦住
// 才守得住（CLU-070）。
func TestStandaloneRejectsReplicationConfig(t *testing.T) {
	cfg := config.Default()
	cfg.Node.DataDir = t.TempDir()
	cfg.Cluster.Replication.Addr = "127.0.0.1:7000"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("standalone 写了复制配置应当被拒")
	}
	if !strings.Contains(err.Error(), "cluster.replication") {
		t.Errorf("错误没指向那个字段: %v", err)
	}
}

// TestLoadReplicationTLSRejectsBadMaterial 证书材料不对要在启动时报错。
func TestLoadReplicationTLSRejectsBadMaterial(t *testing.T) {
	certFile, keyFile, caFile := mintReplicationPKI(t, "n1")
	bogus := filepath.Join(t.TempDir(), "bogus.pem")
	if err := os.WriteFile(bogus, []byte("这不是 PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		rep  config.ReplicationConfig
	}{
		{"证书读不了", config.ReplicationConfig{
			CertFile: filepath.Join(t.TempDir(), "missing.pem"), KeyFile: keyFile, CAFile: caFile,
		}},
		{"私钥不匹配", config.ReplicationConfig{
			CertFile: certFile, KeyFile: bogus, CAFile: caFile,
		}},
		{"CA 读不了", config.ReplicationConfig{
			CertFile: certFile, KeyFile: keyFile,
			CAFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
		}},
		{"CA 里没有证书", config.ReplicationConfig{
			CertFile: certFile, KeyFile: keyFile, CAFile: bogus,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := loadReplicationTLS(&c.rep); err == nil {
				t.Error("应当被拒")
			}
		})
	}
	// 正常材料要能装起来
	ok := config.ReplicationConfig{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	cfg, err := loadReplicationTLS(&ok)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Certificates) != 1 || cfg.RootCAs == nil || cfg.ClientCAs == nil {
		t.Errorf("装出来的 TLS 配置不完整: %+v", cfg)
	}
}

// TestAppliedIndexIsPersistedWithTheColdLayer 钉住 P1：applied index 随冷层落盘。
//
// 没有它，dragonboat 的 IOnDiskStateMachine 在 Open 时只能报 0，而它**不会
// 重放早于该序号的日志**（那些已经被快照压掉了）——后果是丢数据，不是慢。
// 这一条不只为换库而存在：它让重启时间与冷层大小成正比，
// 而不是与日志长度成正比。
//
// 两条判据：键真的在冷层里（而不是只在内存），以及它等于视图的 revision
// ——两者不等就意味着"状态机以为自己应用到哪"与"实际应用到哪"对不上，
// 而那一刻没有任何东西会报错。
func TestAppliedIndexIsPersistedWithTheColdLayer(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	ctx := context.Background()

	if err := r.s.reg2.PutPolicy(ctx, "device", []byte(policyFullAccess)); err != nil {
		t.Fatal(err)
	}
	if err := r.s.reg2.PutThing(ctx, registry.Thing{
		Name: "lamp-1", PolicyName: "device",
	}); err != nil {
		t.Fatal(err)
	}

	wantRev := r.s.store.View().Revision()
	raw, err := r.s.kv.Get(store.MetaRevisionKey())
	if err != nil {
		t.Fatalf("冷层里没有 applied index：%v", err)
	}
	if got := meta.DecodeRevision(raw); got != wantRev {
		t.Fatalf("冷层里的 applied index = %d，视图 revision = %d；"+
			"两者不等意味着状态机以为自己应用到哪与实际应用到哪对不上",
			got, wantRev)
	}
}

// TestRestartKeepsEveryRecordKind 重启后三类记录都要在热视图里（P2）。
//
// 只验一台设备不够：热视图是由 registry 扫冷层重建的，而"重建时漏了
// 一类记录"的表现是那一类对象静默消失——策略漏了，所有设备都连不上；
// 证书漏了，整批设备连不上。所以三类都要断言。
func TestRestartKeepsEveryRecordKind(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	ctx := context.Background()

	if err := r.s.reg2.PutPolicy(ctx, "device", []byte(policyFullAccess)); err != nil {
		t.Fatal(err)
	}
	if err := r.s.reg2.PutThing(ctx, registry.Thing{
		Name: "lamp-1", PolicyName: "device",
	}); err != nil {
		t.Fatal(err)
	}
	wantRev := r.s.store.View().Revision()
	addr := r.cfg.Cluster.Replication.Addr
	r.stop()

	cfg := r.cfg
	clusterConfig(t, &cfg, "n1")
	cfg.Node.DataDir = r.cfg.Node.DataDir
	cfg.Cluster.Replication.Addr = addr
	cfg.Cluster.Replication.Peers = map[string]string{"n1": addr}
	cfg.Cluster.Replication.Replicas = map[string]uint64{"n1": 1}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	again := &testRig{t: t, cfg: cfg, dd: r.dd, mf: r.mf}
	again.start()

	view := again.s.store.View()
	if _, ok := view.Thing("lamp-1"); !ok {
		t.Error("重启后热视图里没有设备")
	}
	// 设备绑的那套策略必须既有编号（冷层给）又有编译结果（热视图给）：
	// 少任何一层，registry 的写入前校验就会判"策略不存在"，而它明明在
	// 冷层里（CLU-072 是同一类）。
	psid := view.ThingPolicySet("lamp-1")
	if psid == 0 {
		t.Fatal("重启后设备没有策略集编号")
	}
	if _, ok := view.PolicySet(psid); !ok {
		t.Error("重启后热视图里没有策略集；策略漏了的表现是所有设备都连不上")
	}
	if got := view.Revision(); got != wantRev {
		t.Errorf("重启后 revision = %d，期望 %d", got, wantRev)
	}
}

// TestClusterModeOnDragonboat 单节点集群跑在 dragonboat 上（LANG-014 第二次修订）。
//
// 与 TestClusterModeWritesThroughTheLog 是同一件事的两个实现：命令经真的
// 共识组、线格式与 TLS 监听，只是多数派恰好是一个人。两条都留着，直到
// 三进程验收把 dragonboat 跑通、旧实现删掉为止——那时这条就是唯一的。
func TestClusterModeOnDragonboat(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) {
		clusterConfig(t, cfg, "n1")
		cfg.Cluster.Replication.Replicas = map[string]uint64{"n1": 1}
	})
	r.start()
	ctx := context.Background()

	if err := r.s.reg2.PutPolicy(ctx, "device", []byte(policyFullAccess)); err != nil {
		t.Fatal(err)
	}
	if err := r.s.reg2.PutThing(ctx, registry.Thing{
		Name: "lamp-1", PolicyName: "device",
	}); err != nil {
		t.Fatalf("写入失败（命令没有经 dragonboat 的日志提交）：%v", err)
	}
	view := r.s.store.View()
	if _, ok := view.Thing("lamp-1"); !ok {
		t.Fatal("热视图里没有这台设备")
	}
	if view.Revision() == 0 {
		t.Error("revision 为 0：日志序号没有被当成 revision 用")
	}
	// applied index 同样要落盘——换了共识实现，CLU-109 的那条规矩不变。
	raw, err := r.s.kv.Get(store.MetaRevisionKey())
	if err != nil {
		t.Fatalf("冷层里没有 applied index：%v", err)
	}
	if got := meta.DecodeRevision(raw); got != view.Revision() {
		t.Errorf("冷层 applied index = %d，视图 revision = %d", got, view.Revision())
	}
}

// TestBootstrapNodeRestartsWhenItIsNoLongerLeader 钉住一条只在真 k8s 上
// 才显形的缺陷（2026-11-20 实测发现）。
//
// `node.mode` 在编排里是按序号算死的：k8s 的 0 号 Pod 永远是 bootstrap。
// 而它被重建时集群早已存在、leader 多半是别人。启动路径原先无条件等
// "自己是 leader"，于是永远等不到——进程 15 秒后退出，重启再等 15 秒，
// CrashLoopBackOff，而错误说的是"选举未完成或对端不可达"，看起来像网络。
//
// 判据是：带着已有数据（applied index 非零）的 bootstrap 节点能再次起来。
// 用例里起不了第二个节点来抢 leader，所以验的是那个判据本身——
// 启动不再要求"是 leader"，只要求"追平"。
func TestBootstrapNodeRestartsWhenItIsNoLongerLeader(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	if err := r.s.reg2.PutThing(context.Background(), registry.Thing{
		Name: "lamp-1", PolicyName: "device",
	}); err != nil {
		t.Fatal(err)
	}
	if r.s.initialRev != 0 {
		t.Fatalf("首次启动的 initialRev = %d，期望 0", r.s.initialRev)
	}
	addr := r.cfg.Cluster.Replication.Addr
	r.stop()

	cfg := r.cfg
	clusterConfig(t, &cfg, "n1")
	cfg.Node.DataDir = r.cfg.Node.DataDir
	cfg.Node.Mode = config.ModeBootstrap // 编排按序号算死，重建时仍是它
	cfg.Cluster.Replication.Addr = addr
	cfg.Cluster.Replication.Peers = map[string]string{"n1": addr}
	cfg.Cluster.Replication.Replicas = map[string]uint64{"n1": 1}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	again := &testRig{t: t, cfg: cfg, dd: r.dd, mf: r.mf}
	again.start()

	// 重启那次必须认出"这不是第一次起这个集群"。
	if again.s.initialRev == 0 {
		t.Fatal("重启后 initialRev 仍是 0：冷层里的 applied index 没读回来，" +
			"启动路径会把这次当成首次 bootstrap 而去等自己当选")
	}
	if _, ok := again.s.store.View().Thing("lamp-1"); !ok {
		t.Error("重启后热视图里没有这台设备")
	}
}
