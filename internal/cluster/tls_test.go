package cluster

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/gob"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// startTLSGroup 起一个跨进程复制组（测试用）。
//
// 日志落在各自的临时目录里（共识库自带 LogDB），用例之间互不影响。
func startTLSGroup(t *testing.T, id NodeID, addrs map[NodeID]string, cfg *tls.Config, bootstrap bool, fsm *testFSM) *TLSGroup {
	t.Helper()
	g, err := ListenTLS(TLSOptions{
		ID: id, Addr: addrs[id], Peers: addrs, Replicas: replicasOf(addrs),
		TLS: cfg, Dir: t.TempDir(),
		Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
		SnapshotTo: fsm.snapshotTo, RestoreFrom: fsm.restoreFrom,
		Bootstrap: bootstrap,
		// 超时调小：这些用例要真的触发选举，而库的默认值是秒级。
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    100 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
		ApplyTimeout:       5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// waitLeader 等组里选出 leader。
func waitLeader(t *testing.T, groups []*TLSGroup, within time.Duration) *TLSGroup {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, g := range groups {
			if g.IsLeader() {
				return g
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s 内没有选出 leader", within)
	return nil
}

// waitApplied 等所有节点的状态机都看到这条命令。
func waitApplied(t *testing.T, fsms []*testFSM, payload string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ok := true
		for _, f := range fsms {
			if !f.has(payload) {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, f := range fsms {
		if !f.has(payload) {
			t.Errorf("第 %d 个节点没有 %q", i, payload)
		}
	}
	t.FailNow()
}

// TestTLSQuorumSnapshotAndDrain 三个真 TLS 的进程内节点：复制、快照、
// 空白节点追平、排空、以及冒充身份被拒。
func TestTLSQuorumSnapshotAndDrain(t *testing.T) {
	ca, caKey, caPEM := mintCA(t)
	ids := []NodeID{"a", "b", "c"}
	addrs := map[NodeID]string{}
	cfgs := map[NodeID]*tls.Config{}
	for i, id := range ids {
		addrs[id] = freeAddr(t)
		cfgs[id] = mintNode(t, ca, caKey, caPEM, string(id), int64(i+2))
	}
	var groups []*TLSGroup
	var fsms []*testFSM
	for i, id := range ids {
		f := newTestFSM()
		fsms = append(fsms, f)
		groups = append(groups, startTLSGroup(t, id, addrs, cfgs[id], i == 0, f))
	}

	lead := waitLeader(t, groups, 10*time.Second)

	// 从非 leader 提交：转发给 leader（CLU-071）。
	var follower *TLSGroup
	for _, g := range groups {
		if g != lead {
			follower = g
			break
		}
	}
	if err := follower.Propose([]byte("one")); err != nil {
		t.Fatalf("跟随者提交失败（应转发给 leader）: %v", err)
	}
	waitApplied(t, fsms, "one", 10*time.Second)

	// 做一次快照：日志从此可以截断，而这是自写那套实现做不到的
	// （它的"快照"是全部历史命令的副本，与日志在同一个文件里）。
	if err := lead.Snapshot(); err != nil {
		t.Fatalf("做快照失败: %v", err)
	}

	if err := lead.Propose([]byte("two")); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, fsms, "two", 10*time.Second)

	// 排空之后拒绝新写入，已提交的照常可读。
	lead.Drain()
	if err := lead.Propose([]byte("no")); !errors.Is(err, ErrDraining) {
		t.Fatalf("排空后写入 = %v，期望 ErrDraining", err)
	}
	if !fsms[0].has("one") {
		t.Error("排空后已提交的命令应当还在状态机里")
	}

	// 用 b 的证书冒充 a：服务端按 CommonName 认身份，必须拒绝。
	if err := dialWrongIdentity(t, addrs["a"], cfgs["b"]); err == nil ||
		!strings.Contains(err.Error(), "node id") {
		t.Fatalf("用 b 的证书冒充 a = %v", err)
	}
}

func TestMultiProcessRaft(t *testing.T) {
	if os.Getenv("OIOT_RAFT_CHILD") == "1" {
		if err := serveRaftChild(); err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	ca, caKey, caPEM := mintCA(t)
	ids := []NodeID{"a", "b", "c"}
	addrs := map[NodeID]string{}
	dirs := map[NodeID]string{}
	https := map[NodeID]string{}
	cfgs := map[NodeID]string{}
	for i, id := range ids {
		addrs[id] = freeAddr(t)
		https[id] = freeAddr(t)
		dirs[id] = t.TempDir()
		certDir := t.TempDir()
		writeNodeFiles(t, ca, caKey, caPEM, string(id), int64(i+2), certDir)
		cfgs[id] = certDir
	}
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	peers := ""
	for i, id := range ids {
		if i > 0 {
			peers += ","
		}
		peers += string(id) + "=" + addrs[id]
	}
	cmds := map[NodeID]*exec.Cmd{}
	for _, id := range ids {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMultiProcessRaft$")
		cmd.Env = append(os.Environ(),
			"OIOT_RAFT_CHILD=1",
			"OIOT_ID="+string(id),
			"OIOT_LISTEN="+addrs[id],
			"OIOT_PEERS="+peers,
			"OIOT_DIR="+dirs[id],
			"OIOT_CA="+caPath,
			"OIOT_CERT="+filepath.Join(cfgs[id], "node.pem"),
			"OIOT_KEY="+filepath.Join(cfgs[id], "node.key"),
			"OIOT_HTTP="+https[id],
		)
		var buf bytes.Buffer
		cmd.Stdout = &buf
		cmd.Stderr = &buf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds[id] = cmd
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		})
		id := id
		t.Cleanup(func() {
			if t.Failed() {
				t.Logf("%s: %s", id, buf.String())
			}
		})
	}
	for _, id := range ids {
		waitHTTP(t, "http://"+https[id]+"/health")
	}
	// 不再有"发起竞选"这一步：选举由库按超时自己跑。
	waitLeaderHTTP(t, https, ids)
	postOK(t, "http://"+https["b"]+"/propose", []byte("one"))
	for _, id := range ids {
		waitAppliedHTTP(t, "http://"+https[id]+"/applied", 1)
	}
	// 杀掉一个节点：多数派仍在，写入照常。
	_ = cmds["c"].Process.Kill()
	_, _ = cmds["c"].Process.Wait()
	waitLeaderHTTP(t, https, []NodeID{"a", "b"})
	postProposeOK(t, https, []NodeID{"a", "b"}, []byte("two"))
	for _, id := range []NodeID{"a", "b"} {
		waitAppliedHTTP(t, "http://"+https[id]+"/applied", 2)
	}

	// 再杀掉一个：失去多数派，写入必须失败而读取照常。
	//
	// 判据是「计数不再变化」，而不是某个绝对条数。postProposeOK 会跨节点
	// 重试到成功，而提案**不带 ID**：负载高时一次提案可能客户端超时（记为
	// 失败）而服务端已经提交，重试就又提交一份，于是绝对条数会是 3 而不是
	// 2——那是仓库自己记过的「模糊写失败分不清请求没到还是应答丢了」，不是
	// 本用例要验的东西。先记下此刻的条数，失去多数派之后它必须一个字节都
	// 不涨。这一条在 make gate 的全量插桩下真的红过。
	before := len(getApplied(t, "http://"+https["a"]+"/applied"))
	_ = cmds["b"].Process.Kill()
	_, _ = cmds["b"].Process.Wait()
	deadline := time.Now().Add(10 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if last = postErr("http://"+https["a"]+"/propose", []byte("no")); last != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == nil {
		t.Fatal("只剩一个节点时写入应当失败")
	}
	if got := getApplied(t, "http://"+https["a"]+"/applied"); len(got) != before {
		t.Fatalf("失去多数派之后状态机从 %d 条变成了 %d 条：写入没有真的被拒",
			before, len(got))
	}
	if before < 2 {
		t.Fatalf("失去多数派之前只应用了 %d 条，前面的提交没生效", before)
	}
}

// waitLeaderHTTP 等子进程里选出 leader。
func waitLeaderHTTP(t *testing.T, https map[NodeID]string, ids []NodeID) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if got, err := httpGet("http://" + https[id] + "/leader"); err == nil && got != "" {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("子进程里没有选出 leader")
}

// postProposeOK 往任意一个还活着的节点提交，由它转发给 leader。
func postProposeOK(t *testing.T, https map[NodeID]string, ids []NodeID, body []byte) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if last = postErr("http://"+https[id]+"/propose", body); last == nil {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("提交失败: %v", last)
}

// waitAppliedHTTP 等某个节点的状态机至少有 n 条命令。
func waitAppliedHTTP(t *testing.T, url string, n int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if got := getApplied(t, url); len(got) >= n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s 的状态机没有攒到 %d 条", url, n)
}

func serveRaftChild() error {
	id := NodeID(os.Getenv("OIOT_ID"))
	cfg, err := loadNodeConfig(os.Getenv("OIOT_CA"), os.Getenv("OIOT_CERT"), os.Getenv("OIOT_KEY"))
	if err != nil {
		return err
	}
	peers := map[NodeID]string{}
	for _, part := range splitComma(os.Getenv("OIOT_PEERS")) {
		name, addr, ok := splitEq(part)
		if !ok {
			continue
		}
		peers[NodeID(name)] = addr
	}
	fsm := newTestFSM()
	g, err := ListenTLS(TLSOptions{
		ID: id, Addr: os.Getenv("OIOT_LISTEN"), Peers: peers, TLS: cfg,
		Dir: os.Getenv("OIOT_DIR"), Replicas: replicasOf(peers),
		Apply: fsm.apply, AppliedIndex: fsm.appliedIndex,
		SnapshotTo: fsm.snapshotTo, RestoreFrom: fsm.restoreFrom,
		// 只有 a 做 bootstrap：三个节点各自 bootstrap 会形成三个
		// 各自以为自己是全集群的组。
		Bootstrap:          id == "a",
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 200 * time.Millisecond,
		ApplyTimeout:       5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer g.Close()
	mux := httpServe(g, fsm)
	return httpListen(os.Getenv("OIOT_HTTP"), mux)
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpGet(url)
		if err == nil && resp == "ok" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等不到 %s", url)
}

func postOK(t *testing.T, url string, body []byte) {
	t.Helper()
	if err := postErr(url, body); err != nil {
		t.Fatal(err)
	}
}

func postErr(url string, body []byte) error {
	resp, err := httpPost(url, body)
	if err != nil {
		return err
	}
	if resp != "ok" {
		return errors.New(resp)
	}
	return nil
}

// getApplied 读子进程状态机里已应用的负载。
//
// 返回 []string 而不是 [][]byte：后者在 JSON 里是 base64，读起来
// 看不出内容，而排查这条用例时最想看的就是"到底应用了什么"。
func getApplied(t *testing.T, url string) []string {
	t.Helper()
	resp, err := httpGet(url)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("%s 返回的不是字符串数组: %v（原文 %q）", url, err, resp)
	}
	return out
}

func dialWrongIdentity(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer raw.Close()
	c := cfg.Clone()
	c.ServerName = "localhost"
	conn := tls.Client(raw, c)
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		return err
	}
	// 先写分流字节，再发一条声称自己是 a 的本项目协议请求：
	// 服务端按客户端证书的 CommonName 认身份，声称与证书不符要被拒。
	if _, err := conn.Write([]byte{markOwn}); err != nil {
		return err
	}
	if err := gob.NewEncoder(conn).Encode(&wireMsg{Kind: wireMesh, From: "a"}); err != nil {
		return err
	}
	var resp wireResp
	if err := gob.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if resp.Err == "" {
		return nil
	}
	return errors.New(resp.Err)
}

func httpServe(g *TLSGroup, fsm *testFSM) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/leader", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(g.LeaderID()))
	})
	mux.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := g.Propose(body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/applied", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(fsm.payloads())
	})
	return mux
}

func httpListen(addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h}
	return srv.ListenAndServe()
}

func httpGet(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

func httpPost(url string, body []byte) (string, error) {
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(b))
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(text)
	}
	return text, nil
}

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

func mintCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "oiot-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key, pemBytes
}

func mintNode(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caPEM []byte, cn string, serial int64) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	writeNodeFiles(t, ca, caKey, caPEM, cn, serial, dir)
	cfg, err := loadNodeConfig(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func writeNodeFiles(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caPEM []byte, cn string, serial int64, dir string) {
	t.Helper()
	// 默认带 localhost 与 127.0.0.1。
	//
	// 注意这**不是**复制层依赖的东西：节点身份按 CommonName 认（CLU-069）。
	// 曾经有一段时间它是必需的——拨号侧把 ServerName 钉成 "localhost"，
	// 于是 SAN 里没有 localhost 的证书一律握手失败；而这个装置恰好这么签，
	// 所以本包全绿而 k8s 的清单起不来（CLU-102）。要钉住那条规则的用例
	// 得显式不带它，见 writeNodeFilesSAN 与 TestTLSPeerCertNeedsNoLocalhostSAN。
	//
	// 两份部署清单的脚本已经不签拨号用的名字了（CLU-103）：
	// gen-certs.sh 只签 DNS:<节点 ID>，prepare.sh 只签 DNS:<Pod 名>。
	// 这里的默认值仍留着，是为了让「SAN 与拨号名对不上」只由上面那条
	// 专门的用例钉住——每个用例都不带 SAN 的话，这条规则就没有一处
	// 显式的落点，下一个人会以为那是巧合。
	writeNodeFilesSAN(t, ca, caKey, caPEM, cn, serial, dir,
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
}

// writeNodeFilesSAN 与 writeNodeFiles 相同，但由调用方给出 SAN。
func writeNodeFilesSAN(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caPEM []byte,
	cn string, serial int64, dir string, dns []string, ips []net.IP,
) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func loadNodeConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("cluster: CA 解析失败")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	out := []string{}
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

func splitEq(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// mintNodeSAN 签一张只带指定 SAN 的节点证书。
func mintNodeSAN(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caPEM []byte,
	cn string, serial int64, dns []string,
) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	writeNodeFilesSAN(t, ca, caKey, caPEM, cn, serial, dir, dns, nil)
	cfg, err := loadNodeConfig(filepath.Join(dir, "ca.pem"),
		filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestTLSPeerCertNeedsNoLocalhostSAN 钉住 CLU-102：节点证书的 SAN 里
// **不含** localhost 也要能组成集群。
//
// 节点身份按 CommonName 认，不按主机名（CLU-069），所以拨号侧不得对
// SAN 有任何要求。这条用例的 SAN 只有节点 ID——正是 k8s 清单里
// prepare.sh 签出来的形状（`DNS:openiotd-0` 一类），而拨号走的是
// 127.0.0.1，两者对不上。
//
// 它要拦的回归很具体：拨号侧曾把 ServerName 钉成 "localhost" 而没有
// 关掉 Go 的默认主机名校验，于是 SAN 里必须有 localhost。本包原先的
// 装置与 compose 的 gen-certs.sh 都恰好那么签，所以三节点验收一直绿，
// 而真实 k8s 上三个节点一致地报「选举未完成或对端不可达」。
func TestTLSPeerCertNeedsNoLocalhostSAN(t *testing.T) {
	ca, caKey, caPEM := mintCA(t)
	ids := []NodeID{"a", "b", "c"}
	addrs := map[NodeID]string{}
	cfgs := map[NodeID]*tls.Config{}
	for i, id := range ids {
		addrs[id] = freeAddr(t)
		// SAN 只有节点 ID，没有 localhost、没有 127.0.0.1。
		cfgs[id] = mintNodeSAN(t, ca, caKey, caPEM, string(id), int64(i+2),
			[]string{string(id)})
	}
	var groups []*TLSGroup
	var fsms []*testFSM
	for i, id := range ids {
		f := newTestFSM()
		fsms = append(fsms, f)
		groups = append(groups, startTLSGroup(t, id, addrs, cfgs[id], i == 0, f))
	}

	lead := waitLeader(t, groups, 15*time.Second)
	if err := lead.Propose([]byte("no-localhost")); err != nil {
		t.Fatalf("SAN 里没有 localhost 时写入失败: %v", err)
	}
	waitApplied(t, fsms, "no-localhost", 15*time.Second)
}

// TestTLSPeerCertFromOtherCAIsRejected 钉住另一半：关掉主机名校验之后
// **链的校验不能跟着没了**。
//
// 拨号侧为了不校验主机名用了 InsecureSkipVerify，而那个开关一开，Go
// 既不建链也不填 verifiedChains——链校验得自己做。漏掉的后果正是
// CLU-068 要防的：任何一张自签证书都能冒充一个节点，等于把元数据
// 写入权发给任何人。
//
// 这里真起一个监听器，用**另一条信任链**签的证书应答，CommonName 故意
// 取成期望的节点 ID——所以只比 CommonName 的实现会放它过去。不起监听器
// 直接拨一个空地址是没有证明力的：那时失败在连接被拒，与证书无关。
func TestTLSPeerCertFromOtherCAIsRejected(t *testing.T) {
	ca, caKey, caPEM := mintCA(t)
	otherCA, otherKey, otherPEM := mintCA(t)

	addrs := map[NodeID]string{"a": freeAddr(t)}
	ga := startTLSGroup(t, "a", addrs, mintNode(t, ca, caKey, caPEM, "a", 2), true, newTestFSM())

	serve := func(cfg *tls.Config) string {
		t.Helper()
		ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				c, aerr := ln.Accept()
				if aerr != nil {
					return
				}
				// 握手由拨号侧驱动；这里只要不立刻关掉连接。
				go func() { _, _ = io.Copy(io.Discard, c) }()
			}
		}()
		return ln.Addr().String()
	}

	// 1) 假 CA 签的证书，CommonName 对得上 —— 必须被拒。
	fake := mintNodeSANWithCA(t, otherCA, otherKey, otherPEM, "b", 3, []string{"localhost"})
	if _, err := ga.dialPeer(serve(fake), "b"); err == nil {
		t.Error("对端证书不是复制 CA 签的，拨号必须失败")
	}

	// 2) 真 CA 签的证书，但 CommonName 是别人 —— 也必须被拒（CLU-069）。
	wrongID := mintNodeSAN(t, ca, caKey, caPEM, "c", 4, []string{"localhost"})
	if _, err := ga.dialPeer(serve(wrongID), "b"); err == nil {
		t.Error("对端 CommonName 不等于期望的节点 ID，拨号必须失败")
	}

	// 3) 真 CA、CommonName 也对 —— 必须通过，否则上面两条可能是因为
	//    别的原因失败的（装置本身坏了）。
	good := mintNodeSAN(t, ca, caKey, caPEM, "b", 5, []string{"localhost"})
	conn, err := ga.dialPeer(serve(good), "b")
	if err != nil {
		t.Fatalf("真 CA 签的、CommonName 正确的对端应当能连上: %v", err)
	}
	_ = conn.Close()
}

// mintNodeSANWithCA 与 mintNodeSAN 相同，但信任池取给定的那条链——
// 用来造"对端拿着另一条链的证书"这种情形。
func mintNodeSANWithCA(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caPEM []byte,
	cn string, serial int64, dns []string,
) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	writeNodeFilesSAN(t, ca, caKey, caPEM, cn, serial, dir, dns, nil)
	cfg, err := loadNodeConfig(filepath.Join(dir, "ca.pem"),
		filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	// 对端自己不要求客户端证书：本用例只验拨号侧对**对端**证书的判定。
	cfg.ClientAuth = tls.NoClientCert
	return cfg
}

// replicasOf 按节点 ID 的字典序派副本号，供用例用。
//
// 生产里这张表是显式配置的（CLU-002 修订：不得从 ID 推导，撞号的症状
// 不指向那张表）。用例里所有节点看到的是同一份 addrs，排序又是确定的，
// 所以派号在各节点上一致——这正是生产里要靠配置保证的那个性质。
func replicasOf(addrs map[NodeID]string) map[NodeID]uint64 {
	ids := make([]string, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make(map[NodeID]uint64, len(ids))
	for i, id := range ids {
		out[NodeID(id)] = uint64(i + 1)
	}
	return out
}
