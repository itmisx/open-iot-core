package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

func capacityManifestPath(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", "..", "deploy", "cluster", "k8s-million", name))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func clusterK8sPath(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", "..", "deploy", "cluster", "k8s", name))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func benchScriptPath(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", "..", "tests", "bench", name))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestMillionCapacityConfig 锁住五节点容量overlay的拓扑。这里只检查静态部署
// 输入；真实百万并发仍必须由 verify-million-density.sh 的五机结果证明。
func TestMillionCapacityConfig(t *testing.T) {
	raw, err := os.ReadFile(capacityManifestPath(t, "10-config-patch.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(raw, &cm); err != nil {
		t.Fatal(err)
	}
	embedded := cm.Data["openiotd.yaml"]
	if embedded == "" {
		t.Fatal("容量ConfigMap缺少openiotd.yaml")
	}
	cfg, err := Decode(strings.NewReader(embedded))
	if err != nil {
		t.Fatalf("容量配置不能被openiotd解析: %v", err)
	}
	// StatefulSet在进程启动前注入这两项；补齐后必须通过完整语义校验，
	// 不能只证明YAML字段拼写正确。
	cfg.Node.ID = "openiotd-0"
	cfg.Node.Mode = ModeBootstrap
	if err := cfg.Validate(); err != nil {
		t.Fatalf("容量配置不能通过语义校验: %v", err)
	}
	rep := cfg.Cluster.Replication
	if cfg.Cluster.Voters != 5 || len(rep.Peers) != 5 || len(rep.Replicas) != 5 {
		t.Fatalf("容量拓扑不是五节点: voters=%d peers=%d replicas=%d",
			cfg.Cluster.Voters, len(rep.Peers), len(rep.Replicas))
	}
	if rep.SessionShards != 8 || rep.SessionReplicationFactor != 3 ||
		rep.ShadowShards != 8 || rep.ShadowReplicationFactor != 3 {
		t.Fatalf("容量分片/RF错误: session=%d/%d shadow=%d/%d",
			rep.SessionShards, rep.SessionReplicationFactor,
			rep.ShadowShards, rep.ShadowReplicationFactor)
	}
	if cfg.Quota["connections"] < 200000 || cfg.Broker.TLSHandshakeConcurrency != 256 {
		t.Fatalf("容量接入配置错误: connections=%d tlsConcurrency=%d",
			cfg.Quota["connections"], cfg.Broker.TLSHandshakeConcurrency)
	}
	if cfg.Auth.Mode != AuthCert {
		t.Fatalf("百万容量验收没有显式启用证书认证: auth.mode=%q", cfg.Auth.Mode)
	}
}

// TestConnectStageMTLSUsesCertificateAuth 防止归因脚本只在传输层要求客户端
// 证书，却在MQTT CONNECT阶段使用insecure-none。后者能量TLS密码学成本，但会
// 跳过证书状态、吊销、Thing与策略检查，不能叫完整mTLS建连结果。
func TestConnectStageMTLSUsesCertificateAuth(t *testing.T) {
	raw, err := os.ReadFile(benchScriptPath(t, "connect-stage-ab.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		`if [ "$MQTT_TLS" = 1 ] || [ "$MQTT_PROXY" = 1 ]; then`,
		"auth_mode=cert",
		"mode: $auth_mode",
		"transport=mtls-cert-auth",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("mTLS归因脚本可能绕过证书认证: 缺少 %q", want)
		}
	}
	if strings.Contains(script,
		`mode: $([ "$MQTT_PROXY" = 1 ] && echo cert || echo insecure-none)`) {
		t.Fatal("mTLS归因脚本仍只为PROXY启用证书认证")
	}
}

func TestMillionCapacityResourcesAndIngress(t *testing.T) {
	raw, err := os.ReadFile(capacityManifestPath(t, "30-statefulset-patch.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var sts struct {
		Spec struct {
			Replicas int `yaml:"replicas"`
			Template struct {
				Spec struct {
					Termination int `yaml:"terminationGracePeriodSeconds"`
					Containers  []struct {
						Name      string `yaml:"name"`
						Resources struct {
							Requests map[string]string `yaml:"requests"`
							Limits   map[string]string `yaml:"limits"`
						} `yaml:"resources"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &sts); err != nil {
		t.Fatal(err)
	}
	if sts.Spec.Replicas != 5 || len(sts.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("容量StatefulSet形状错误: replicas=%d containers=%d",
			sts.Spec.Replicas, len(sts.Spec.Template.Spec.Containers))
	}
	c := sts.Spec.Template.Spec.Containers[0]
	if c.Name != "openiotd" || c.Resources.Requests["cpu"] != "8" ||
		c.Resources.Requests["memory"] != "32Gi" || c.Resources.Limits["memory"] != "40Gi" ||
		sts.Spec.Template.Spec.Termination < 120 {
		t.Fatalf("容量资源错误: name=%s requests=%v limits=%v grace=%d",
			c.Name, c.Resources.Requests, c.Resources.Limits, sts.Spec.Template.Spec.Termination)
	}

	raw, err = os.ReadFile(capacityManifestPath(t, "20-mqtt-loadbalancer.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var svc struct {
		Spec struct {
			Type                  string `yaml:"type"`
			ExternalTrafficPolicy string `yaml:"externalTrafficPolicy"`
			Ports                 []struct {
				Port int `yaml:"port"`
			} `yaml:"ports"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &svc); err != nil {
		t.Fatal(err)
	}
	if svc.Spec.Type != "LoadBalancer" || svc.Spec.ExternalTrafficPolicy != "Local" ||
		len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8883 {
		t.Fatalf("容量入口不安全或多一跳: type=%s policy=%s ports=%v",
			svc.Spec.Type, svc.Spec.ExternalTrafficPolicy, svc.Spec.Ports)
	}
}

// TestMillionCapacitySharesTLSTicketKeys 锁住跨节点TLS恢复的部署链：prepare
// 生成一把集群共享key放进Secret，initContainer再把同一Secret复制到每个PVC。
// 任一环断掉都不影响正确性，只会让经LoadBalancer切换节点的重连静默退化成
// 全握手；单节点mTLS测试无法发现这种容量回退。
func TestMillionCapacitySharesTLSTicketKeys(t *testing.T) {
	raw, err := os.ReadFile(capacityManifestPath(t, "10-config-patch.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw),
		"session_ticket_keys_file: /var/lib/openiotd/pki/session-ticket.keys") {
		t.Fatal("百万配置没有读取PVC中的共享TLS ticket key")
	}

	prepare, err := os.ReadFile(clusterK8sPath(t, "prepare.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"openssl rand -hex 32", `--from-file="${OUT}/pki/session-ticket.keys"`,
		"create secret generic openiotd-device-ca",
	} {
		if !strings.Contains(string(prepare), want) {
			t.Fatalf("prepare.sh没有把共享ticket key写入Secret: 缺少 %q", want)
		}
	}

	stateful, err := os.ReadFile(clusterK8sPath(t, "30-statefulset.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"cp -L /etc/openiotd/device-ca/* /var/lib/openiotd/pki/",
		"secretName: openiotd-device-ca",
		"mountPath: /etc/openiotd/device-ca",
	} {
		if !strings.Contains(string(stateful), want) {
			t.Fatalf("StatefulSet没有把共享ticket key复制到每个节点: 缺少 %q", want)
		}
	}
}
