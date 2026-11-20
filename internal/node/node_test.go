package node

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testNowMs = 1_700_000_000_000

func openTemp(t *testing.T) *DataDir {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func initOnce(t *testing.T, d *DataDir) Manifest {
	t.Helper()
	m, err := Identify(d, IdentityRequest{
		ShardCount: 64, AllowCreate: true, NowUnixMs: testNowMs,
	})
	if err != nil {
		t.Fatalf("初始化失败: %v", err)
	}
	return m
}

func TestOpenCreatesDirWithTightPerms(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b", "data")
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(d.Root())
	if err != nil {
		t.Fatal(err)
	}
	// 目录下有 CA 私钥与会话状态，权限须收紧
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("数据目录权限 = %o，期望 700", fi.Mode().Perm())
	}
	if !filepath.IsAbs(d.Root()) {
		t.Errorf("Root 应为绝对路径: %s", d.Root())
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Error("空路径应报错")
	}
	// 路径指向一个文件而非目录
	f := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(f); err == nil {
		t.Error("路径指向文件时应报错")
	}
}

func TestIdentifyGeneratesStableIdentity(t *testing.T) {
	d := openTemp(t)
	first := initOnce(t, d)

	if first.NodeID == "" || first.ClusterID == "" {
		t.Fatal("应生成 node_id 与 cluster_id")
	}
	if first.FormatVersion != ManifestFormatVersion {
		t.Errorf("format_version = %d", first.FormatVersion)
	}
	if first.CreatedAtUnixMs != testNowMs {
		t.Errorf("created_at = %d，应取注入时钟", first.CreatedAtUnixMs)
	}

	// 再次启动（不允许创建）须读回同一身份——这是 CLU-033 的核心
	second, err := Identify(d, IdentityRequest{ShardCount: 64, NowUnixMs: testNowMs + 1000})
	if err != nil {
		t.Fatalf("重启应成功: %v", err)
	}
	if second.NodeID != first.NodeID {
		t.Errorf("node_id 变了：%s -> %s", first.NodeID, second.NodeID)
	}
	if second.ClusterID != first.ClusterID {
		t.Errorf("cluster_id 变了：%s -> %s", first.ClusterID, second.ClusterID)
	}
	if second.CreatedAtUnixMs != first.CreatedAtUnixMs {
		t.Error("created_at 不应被重启改写")
	}
}

func TestIdentifyGeneratesDistinctIDs(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		m := initOnce(t, openTemp(t))
		if seen[m.NodeID] {
			t.Fatalf("node_id 重复: %s", m.NodeID)
		}
		seen[m.NodeID] = true
	}
}

// TestServeRefusesUninitializedDir 验证 serve 不会悄悄新建集群。
//
// 若 data_dir 打错一个字母就自动初始化一个空集群，症状是"所有设备
// 认证失败"，而真因（连错目录）完全看不出来。
func TestServeRefusesUninitializedDir(t *testing.T) {
	d := openTemp(t)
	_, err := Identify(d, IdentityRequest{ShardCount: 64, NowUnixMs: testNowMs})
	var ni *ErrNotInitialized
	if !errors.As(err, &ni) {
		t.Fatalf("应返回 ErrNotInitialized，得到 %v", err)
	}
	// 错误须给出可操作的下一步（NFR-065）
	if !strings.Contains(err.Error(), "openiotd init") {
		t.Errorf("错误应提示如何初始化: %s", err)
	}
	if !strings.Contains(err.Error(), d.Root()) {
		t.Errorf("错误应含目录路径: %s", err)
	}
}

// TestIdentityConflictsRejected 是 PRD CLU-031 的落点。
func TestIdentityConflictsRejected(t *testing.T) {
	cases := []struct {
		name  string
		req   func(m Manifest) IdentityRequest
		field string
	}{
		{"已初始化的目录再 bootstrap", func(m Manifest) IdentityRequest {
			return IdentityRequest{ShardCount: 64, Bootstrap: true, AllowCreate: true}
		}, "node.mode"},
		{"cluster_id 冲突", func(m Manifest) IdentityRequest {
			return IdentityRequest{ClusterID: "cluster-other", ShardCount: 64}
		}, "cluster_id"},
		{"node_id 冲突", func(m Manifest) IdentityRequest {
			return IdentityRequest{NodeID: "node-other", ShardCount: 64}
		}, "node.id"},
		{"shard_count 不一致", func(m Manifest) IdentityRequest {
			return IdentityRequest{ShardCount: 128}
		}, "cluster.shard_count"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := openTemp(t)
			m := initOnce(t, d)
			_, err := Identify(d, c.req(m))
			var ic *ErrIdentityConflict
			if !errors.As(err, &ic) {
				t.Fatalf("应返回 ErrIdentityConflict，得到 %v", err)
			}
			if ic.Field != c.field {
				t.Errorf("冲突字段 = %q，期望 %q", ic.Field, c.field)
			}
			if ic.Hint == "" {
				t.Error("冲突错误须给出处理建议")
			}
		})
	}
}

// TestIdentifyAcceptsMatchingConfig 验证配置与磁盘一致时正常启动。
func TestIdentifyAcceptsMatchingConfig(t *testing.T) {
	d := openTemp(t)
	m := initOnce(t, d)
	got, err := Identify(d, IdentityRequest{
		NodeID: m.NodeID, ClusterID: m.ClusterID, ShardCount: m.ShardCount,
	})
	if err != nil {
		t.Fatalf("一致的配置应被接受: %v", err)
	}
	if got.NodeID != m.NodeID {
		t.Error("身份不一致")
	}
}

func TestIdentifyHonorsExplicitIDs(t *testing.T) {
	d := openTemp(t)
	m, err := Identify(d, IdentityRequest{
		NodeID: "node-fixed", ClusterID: "cluster-fixed",
		ShardCount: 32, AllowCreate: true, NowUnixMs: testNowMs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.NodeID != "node-fixed" || m.ClusterID != "cluster-fixed" || m.ShardCount != 32 {
		t.Errorf("显式给出的身份未被采用: %+v", m)
	}
}

func TestIdentifyRequiresShardCountOnCreate(t *testing.T) {
	d := openTemp(t)
	if _, err := Identify(d, IdentityRequest{AllowCreate: true}); err == nil {
		t.Error("初始化须给出 shard_count")
	}
}

func TestManifestIsAtomicAndPermissioned(t *testing.T) {
	d := openTemp(t)
	initOnce(t, d)
	fi, err := os.Stat(d.manifestPath())
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("manifest 权限 = %o，期望 600", fi.Mode().Perm())
	}
	// 临时文件不应残留
	entries, err := os.ReadDir(d.Root())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".manifest-") {
			t.Errorf("残留临时文件: %s", e.Name())
		}
	}
}

func TestManifestCorruptionIsReported(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"非 JSON", "{这不是 JSON"},
		{"截断", `{"format_version":1,"cluster_id":"c"`},
		{"缺少 node_id", `{"format_version":1,"cluster_id":"c","shard_count":64}`},
		{"缺少 cluster_id", `{"format_version":1,"node_id":"n","shard_count":64}`},
		{"缺少 shard_count", `{"format_version":1,"cluster_id":"c","node_id":"n"}`},
		{"format_version 为零", `{"cluster_id":"c","node_id":"n","shard_count":64}`},
		{"未知字段", `{"format_version":1,"cluster_id":"c","node_id":"n","shard_count":64,"x":1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := openTemp(t)
			if err := os.WriteFile(d.manifestPath(), []byte(c.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := d.ReadManifest(); err == nil {
				t.Error("损坏的 manifest 应报错")
			}
		})
	}
}

// TestManifestFutureVersionRejected 验证不会用旧程序读新格式。
//
// 静默忽略不认识的版本号，等于用旧的字段语义解释新格式的数据。
func TestManifestFutureVersionRejected(t *testing.T) {
	d := openTemp(t)
	m := Manifest{
		FormatVersion: ManifestFormatVersion + 1,
		ClusterID:     "c", NodeID: "n", ShardCount: 64,
	}
	data, _ := json.Marshal(&m)
	if err := os.WriteFile(d.manifestPath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := d.ReadManifest()
	if err == nil {
		t.Fatal("更高的格式版本应报错")
	}
	if !strings.Contains(err.Error(), "升级") {
		t.Errorf("错误应提示升级: %s", err)
	}
}

func TestInitializedReportsState(t *testing.T) {
	d := openTemp(t)
	if ok, err := d.Initialized(); err != nil || ok {
		t.Errorf("新目录应未初始化: %v %v", ok, err)
	}
	initOnce(t, d)
	if ok, err := d.Initialized(); err != nil || !ok {
		t.Errorf("初始化后应为真: %v %v", ok, err)
	}
}

func TestEnsureDir(t *testing.T) {
	d := openTemp(t)
	p, err := d.EnsureDir(DirPKI)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("子目录权限 = %o，期望 700", fi.Mode().Perm())
	}
	// 幂等
	if _, err := d.EnsureDir(DirPKI); err != nil {
		t.Errorf("重复创建应幂等: %v", err)
	}
}

func TestReadiness(t *testing.T) {
	r := NewReadiness("meta", "view", "pki")
	if r.Ready() {
		t.Error("初始不应就绪")
	}
	if len(r.NotReady()) != 3 {
		t.Errorf("未就绪组件数 = %d，期望 3", len(r.NotReady()))
	}

	r.Set("meta", true, "")
	r.Set("view", true, "")
	if r.Ready() {
		t.Error("仍有组件未就绪")
	}
	if got := r.NotReady(); len(got) != 1 || got[0] != "pki" {
		t.Errorf("未就绪组件 = %v，期望 [pki]", got)
	}

	r.Set("pki", true, "")
	if !r.Ready() {
		t.Error("全部就绪后应为真")
	}

	// 未登记的组件名被忽略：否则一个拼错的组件名会让节点永不就绪，
	// 或反过来让它在某组件尚未注册时就被判为就绪
	r.Set("typo", false, "x")
	if !r.Ready() {
		t.Error("未登记的组件不应影响整体判定")
	}

	// 可以退回未就绪（视图落后时）
	r.Set("view", false, "落后 leader 1200 条")
	if r.Ready() {
		t.Error("组件退回未就绪后整体应未就绪")
	}
	rep := r.Report()
	// 输出须按名字排序，使 /readyz 的 diff 不含噪声
	for i := 1; i < len(rep); i++ {
		if rep[i-1].Name > rep[i].Name {
			t.Errorf("Report 未按名字排序: %v", rep)
			break
		}
	}
	for _, s := range rep {
		if s.Name == "view" && s.Detail != "落后 leader 1200 条" {
			t.Errorf("未保留 detail: %+v", s)
		}
	}
}

func TestReadinessConcurrent(t *testing.T) {
	r := NewReadiness("a", "b")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			r.Set("a", i%2 == 0, "x")
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = r.Ready()
		_ = r.Report()
	}
	<-done
}

func TestNewID(t *testing.T) {
	id, err := NewID("node")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "node-") {
		t.Errorf("标识 = %q，应含前缀", id)
	}
	if len(id) != len("node-")+16 {
		t.Errorf("标识长度 = %d", len(id))
	}
}

func TestIdentityConflictErrorText(t *testing.T) {
	e := &ErrIdentityConflict{
		Field: "node.id", OnDisk: "a", Configured: "b", Hint: "换个目录",
	}
	msg := e.Error()
	for _, want := range []string{"node.id", "a", "b", "换个目录"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误文本缺少 %q: %s", want, msg)
		}
	}
}

// TestWriteManifestFailsOnReadOnlyDir 覆盖写入失败分支。
//
// 身份文件写不进去必须报错而不是继续启动：一个没有持久化 node_id 的
// 节点，重启后就是另一个节点。
func TestWriteManifestFailsOnReadOnlyDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	// 去掉写权限，使临时文件无法创建
	if err := os.Chmod(root, 0o500); err != nil {
		t.Skipf("无法改变目录权限: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })

	_, err = Identify(d, IdentityRequest{
		ShardCount: 64, AllowCreate: true, NowUnixMs: testNowMs,
	})
	if err == nil {
		t.Error("无法写入 manifest 时应报错")
	}
}

func TestEnsureDirFailsOnFile(t *testing.T) {
	d := openTemp(t)
	// 先占一个同名文件，使创建目录必然失败
	p := d.Path("occupied")
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.EnsureDir("occupied"); err == nil {
		t.Error("同名文件存在时应报错")
	}
}

func TestOpenRelativePathResolves(t *testing.T) {
	// 相对路径须被解析为绝对路径：data dir 的路径会进日志与错误信息，
	// 相对路径在不同工作目录下指向不同位置，排查时会误导
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	d, err := Open("./rel")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(d.Root()) {
		t.Errorf("Root = %q，应为绝对路径", d.Root())
	}
}

func TestSyncDirOnMissingDir(t *testing.T) {
	if err := syncDir(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("目录不存在应报错")
	}
}
