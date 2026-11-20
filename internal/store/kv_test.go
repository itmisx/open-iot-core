package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// kvFactory 构造一个待测的 KV 实现。
type kvFactory struct {
	name string
	open func(t *testing.T) KV
	// reopen 为非 nil 表示该实现支持重开，可用于测试持久性。
	reopen func(t *testing.T, prev KV) KV
}

// factories 列出全部 KV 实现。
//
// 同一套契约测试跑在两个实现上，这才真正验证了接口：
// 只有一个实现的接口，其抽象边界往往是假的——所有隐含假设都
// 悄悄依赖那一个实现的行为。
func factories(t *testing.T) []kvFactory {
	t.Helper()
	return []kvFactory{
		{
			name: "MemKV",
			open: func(t *testing.T) KV { return NewMemKV() },
		},
		{
			name: "bbolt",
			open: func(t *testing.T) KV {
				dir := t.TempDir()
				kv, err := Open(Options{Dir: dir})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = kv.Close() })
				// 记下目录，供 reopen 使用
				t.Setenv("TEST_STORE_DIR", dir)
				return kv
			},
			reopen: func(t *testing.T, prev KV) KV {
				dir := os.Getenv("TEST_STORE_DIR")
				if err := prev.Close(); err != nil {
					t.Fatal(err)
				}
				kv, err := Open(Options{Dir: dir})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = kv.Close() })
				return kv
			},
		},
	}
}

func TestKVContract(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)

			// 不存在的键
			if _, err := kv.Get([]byte("nope")); !errors.Is(err, ErrNotFound) {
				t.Errorf("= %v，期望 ErrNotFound", err)
			}
			// 写入与读回
			if err := kv.Set([]byte("k1"), []byte("v1"), true); err != nil {
				t.Fatal(err)
			}
			got, err := kv.Get([]byte("k1"))
			if err != nil || string(got) != "v1" {
				t.Fatalf("= %q, %v", got, err)
			}
			// 覆盖
			if err := kv.Set([]byte("k1"), []byte("v2"), false); err != nil {
				t.Fatal(err)
			}
			got, _ = kv.Get([]byte("k1"))
			if string(got) != "v2" {
				t.Errorf("覆盖后 = %q", got)
			}
			// 删除
			if err := kv.Delete([]byte("k1"), true); err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Get([]byte("k1")); !errors.Is(err, ErrNotFound) {
				t.Errorf("删除后 = %v，期望 ErrNotFound", err)
			}
			// 删除不存在的键不是错误
			if err := kv.Delete([]byte("gone"), false); err != nil {
				t.Errorf("删除不存在的键应成功: %v", err)
			}
			// 空值可写可读
			if err := kv.Set([]byte("empty"), []byte{}, false); err != nil {
				t.Fatal(err)
			}
			if got, err := kv.Get([]byte("empty")); err != nil || len(got) != 0 {
				t.Errorf("空值 = %q, %v", got, err)
			}
			if err := kv.Flush(); err != nil {
				t.Errorf("Flush: %v", err)
			}
		})
	}
}

// TestKVGetReturnsCopy 验证返回值与引擎内部缓冲无关。
//
// 两个实现都必须返回副本。bbolt 的 Get 指向 mmap 区域，
// 事务结束后即失效——返回该视图会让"事务外使用"表现为
// 读到随机数据或直接崩溃，而那种 bug 只在压力下显形。
func TestKVGetReturnsCopy(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)
			if err := kv.Set([]byte("k"), []byte("original"), true); err != nil {
				t.Fatal(err)
			}
			got, err := kv.Get([]byte("k"))
			if err != nil {
				t.Fatal(err)
			}
			// 改动返回值不得影响存储
			copy(got, "TAMPERED")
			again, err := kv.Get([]byte("k"))
			if err != nil {
				t.Fatal(err)
			}
			if string(again) != "original" {
				t.Errorf("Get 未返回副本：存储中的值变成了 %q", again)
			}
		})
	}
}

func TestKVScan(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)
			for _, k := range []string{"a/1", "a/2", "a/3", "b/1", "ab/1"} {
				if err := kv.Set([]byte(k), []byte(k), false); err != nil {
					t.Fatal(err)
				}
			}
			var got []string
			if err := kv.Scan([]byte("a/"), func(k, v []byte) bool {
				if string(k) != string(v) {
					t.Errorf("键值不匹配: %q / %q", k, v)
				}
				got = append(got, string(k))
				return true
			}); err != nil {
				t.Fatal(err)
			}
			// 前缀 "a/" 不应匹配 "ab/1"
			if len(got) != 3 {
				t.Fatalf("= %v，期望三条", got)
			}
			// 两个实现的遍历顺序都须升序，否则依赖顺序的测试
			// 会在两者之间表现不同
			for i := 1; i < len(got); i++ {
				if got[i-1] >= got[i] {
					t.Errorf("遍历顺序非升序: %v", got)
				}
			}

			// 提前结束
			n := 0
			if err := kv.Scan([]byte("a/"), func([]byte, []byte) bool {
				n++
				return false
			}); err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("提前结束后回调次数 = %d，期望 1", n)
			}

			// 无匹配前缀
			if err := kv.Scan([]byte("zzz"), func([]byte, []byte) bool {
				t.Error("不应有匹配")
				return true
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestKVBatch(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)
			if err := kv.Set([]byte("old"), []byte("x"), false); err != nil {
				t.Fatal(err)
			}
			err := kv.Batch([]Op{
				{Kind: OpSet, Key: []byte("n1"), Value: []byte("a")},
				{Kind: OpSet, Key: []byte("n2"), Value: []byte("b")},
				{Kind: OpDelete, Key: []byte("old")},
			}, true)
			if err != nil {
				t.Fatal(err)
			}
			for k, want := range map[string]string{"n1": "a", "n2": "b"} {
				got, err := kv.Get([]byte(k))
				if err != nil || string(got) != want {
					t.Errorf("%s = %q, %v", k, got, err)
				}
			}
			if _, err := kv.Get([]byte("old")); !errors.Is(err, ErrNotFound) {
				t.Error("批量中的删除未生效")
			}
			// 空批量是合法的 no-op
			if err := kv.Batch(nil, false); err != nil {
				t.Errorf("空批量应成功: %v", err)
			}
			// 非法操作类型须整批拒绝，不得留下部分写入
			err = kv.Batch([]Op{
				{Kind: OpSet, Key: []byte("p1"), Value: []byte("a")},
				{Kind: OpKind(99), Key: []byte("p2")},
			}, false)
			if err == nil {
				t.Fatal("非法操作类型应报错")
			}
			if _, err := kv.Get([]byte("p1")); !errors.Is(err, ErrNotFound) {
				t.Error("批量写入须原子：失败后不应留下部分写入")
			}
		})
	}
}

func TestKVClosedRejects(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)
			if err := kv.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Get([]byte("k")); !errors.Is(err, ErrClosed) {
				t.Errorf("Get = %v，期望 ErrClosed", err)
			}
			if err := kv.Set([]byte("k"), []byte("v"), false); !errors.Is(err, ErrClosed) {
				t.Errorf("Set = %v，期望 ErrClosed", err)
			}
			if err := kv.Delete([]byte("k"), false); !errors.Is(err, ErrClosed) {
				t.Errorf("Delete = %v，期望 ErrClosed", err)
			}
			if err := kv.Batch([]Op{{Kind: OpSet, Key: []byte("k")}}, false); !errors.Is(err, ErrClosed) {
				t.Errorf("Batch = %v，期望 ErrClosed", err)
			}
			if err := kv.Scan([]byte("k"), func([]byte, []byte) bool { return true }); !errors.Is(err, ErrClosed) {
				t.Errorf("Scan = %v，期望 ErrClosed", err)
			}
			if err := kv.Flush(); !errors.Is(err, ErrClosed) {
				t.Errorf("Flush = %v，期望 ErrClosed", err)
			}
			// 重复关闭须幂等
			if err := kv.Close(); err != nil {
				t.Errorf("重复关闭应幂等: %v", err)
			}
		})
	}
}

func TestKVConcurrent(t *testing.T) {
	for _, f := range factories(t) {
		t.Run(f.name, func(t *testing.T) {
			kv := f.open(t)
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						k := []byte(fmt.Sprintf("w%d/k%d", w, i))
						if err := kv.Set(k, []byte("v"), false); err != nil {
							t.Errorf("写入失败: %v", err)
							return
						}
						if _, err := kv.Get(k); err != nil {
							t.Errorf("读取失败: %v", err)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			n := 0
			_ = kv.Scan([]byte("w"), func([]byte, []byte) bool { n++; return true })
			if n != 8*50 {
				t.Errorf("写入条数 = %d，期望 %d", n, 8*50)
			}
		})
	}
}

// ---------- bbolt 专项 ----------

func TestBoltPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set([]byte("durable"), []byte("yes"), true); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.Close() }()
	got, err := again.Get([]byte("durable"))
	if err != nil || string(got) != "yes" {
		t.Errorf("重开后 = %q, %v", got, err)
	}
}

func TestBoltOpenErrors(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Error("缺少数据目录应报错")
	}
	// 目录位置被一个文件占住
	f := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: f}); err == nil {
		t.Error("路径指向文件时应报错")
	}
}

// TestBoltFileLockIsExclusive 验证两个进程不会同时打开同一数据目录。
//
// 没有这条保护，两个 openiotd 指向同一目录会互相覆盖写入。
// 超时必须有限：永久挂住的现象是"进程起来了但什么都不做"，
// 排查成本远高于一条启动失败。
func TestBoltFileLockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()

	start := time.Now()
	_, err = Open(Options{Dir: dir, OpenTimeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("第二次打开同一数据目录应失败")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("等待锁耗时 %v，超时未生效", d)
	}
	// 错误须指出真正的原因，否则运维会去查磁盘损坏
	if !bytes.Contains([]byte(err.Error()), []byte("占用")) {
		t.Errorf("错误应说明目录被占用: %v", err)
	}
}

func TestBoltReadOnly(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set([]byte("k"), []byte("v"), true); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if got, err := ro.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Errorf("只读读取 = %q, %v", got, err)
	}
	if err := ro.Set([]byte("k2"), []byte("v"), false); err == nil {
		t.Error("只读模式下写入应失败")
	}
}

// TestBoltSnapshot 验证全库快照（CLU-003、NFR-063）。
func TestBoltSnapshot(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	for i := 0; i < 100; i++ {
		if err := kv.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	snap, ok := kv.(Snapshotter)
	if !ok {
		t.Fatal("bbolt 实现应支持快照")
	}
	var buf bytes.Buffer
	n, err := snap.Snapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n <= 0 || int64(buf.Len()) != n {
		t.Fatalf("快照字节数 = %d，缓冲长度 = %d", n, buf.Len())
	}

	// 快照须是一个可直接打开的库文件
	restoreDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(restoreDir, "store.db"), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(Options{Dir: restoreDir})
	if err != nil {
		t.Fatalf("快照应可直接作为库文件打开: %v", err)
	}
	defer func() { _ = restored.Close() }()
	cnt := 0
	_ = restored.Scan([]byte("k"), func([]byte, []byte) bool { cnt++; return true })
	if cnt != 100 {
		t.Errorf("恢复后条数 = %d，期望 100", cnt)
	}
}

// ---------- 键空间 ----------

func TestKeyEncoding(t *testing.T) {
	k := Key(PrefixShadow, "dev-1", "cfg")
	if k[0] != byte(PrefixShadow) {
		t.Errorf("前缀 = %q", k[0])
	}
	parts, ok := SplitKey(k, PrefixShadow, 2)
	if !ok || parts[0] != "dev-1" || parts[1] != "cfg" {
		t.Errorf("= %v, %v", parts, ok)
	}
	// 经典影子的名字为空
	k2 := Key(PrefixShadow, "dev-1", "")
	parts, ok = SplitKey(k2, PrefixShadow, 2)
	if !ok || parts[0] != "dev-1" || parts[1] != "" {
		t.Errorf("= %v, %v", parts, ok)
	}
	// 前缀不符
	if _, ok := SplitKey(k, PrefixSession, 2); ok {
		t.Error("前缀不符应失败")
	}
	// 片段数不符
	if _, ok := SplitKey(k, PrefixShadow, 3); ok {
		t.Error("片段数不符应失败")
	}
	if _, ok := SplitKey(nil, PrefixShadow, 2); ok {
		t.Error("空键应失败")
	}
}

// TestKeyPrefixDoesNotBleed 验证前缀扫描不会串到相邻设备。
//
// 这是前缀扫描最容易出的错：扫描 "d1" 时若不带分隔符，
// 会把 "d10"、"d1x" 的记录一并捞出来。症状是
// "别的设备的影子出现在列表里"，属安全问题。
func TestKeyPrefixDoesNotBleed(t *testing.T) {
	kv := NewMemKV()
	for _, thing := range []string{"d1", "d10", "d1x"} {
		if err := kv.Set(Key(PrefixShadow, thing, "cfg"), []byte("x"), false); err != nil {
			t.Fatal(err)
		}
	}
	prefix := append(Key(PrefixShadow, "d1"), KeySep)
	n := 0
	_ = kv.Scan(prefix, func(key, _ []byte) bool {
		parts, ok := SplitKey(key, PrefixShadow, 2)
		if !ok || parts[0] != "d1" {
			t.Errorf("扫到了别的设备: %v", parts)
		}
		n++
		return true
	})
	if n != 1 {
		t.Errorf("匹配条数 = %d，期望 1", n)
	}
}

func TestFormatVersionRejectsFuture(t *testing.T) {
	kv := NewMemKV()
	// 伪造一个更高的版本
	if err := kv.Set(systemKeyFormat, []byte{byte(FormatVersion + 1)}, true); err != nil {
		t.Fatal(err)
	}
	err := checkFormatVersion(kv)
	if err == nil {
		t.Fatal("更高的格式版本应报错")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("升级")) {
		t.Errorf("错误应提示升级: %v", err)
	}
	// 损坏的版本记录
	if err := kv.Set(systemKeyFormat, []byte{1, 2, 3}, true); err != nil {
		t.Fatal(err)
	}
	if err := checkFormatVersion(kv); err == nil {
		t.Error("损坏的版本记录应报错")
	}
}
