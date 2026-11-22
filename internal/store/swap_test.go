package store

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestSwapKVRestoreReplacesWholeDatabase 整库替换之后，旧键消失、
// 新键出现，而持有同一个句柄的调用方不需要知道这件事。
//
// 后半句是这个包装存在的唯一理由：注册表、会话、保留消息、影子各自
// 持有一个 KV，直接换掉 Open 返回的值的话，它们手里仍是旧句柄——
// 会继续读一个已经被删掉的文件，现象是"数据回到了快照之前"。
func TestSwapKVRestoreReplacesWholeDatabase(t *testing.T) {
	src, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	if err := src.Set([]byte("k-new"), []byte("from-snapshot"), true); err != nil {
		t.Fatal(err)
	}
	var snap bytes.Buffer
	if _, err := src.Snapshot(&snap); err != nil {
		t.Fatal(err)
	}

	dst, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := dst.Set([]byte("k-old"), []byte("before"), true); err != nil {
		t.Fatal(err)
	}

	// 调用方拿到的是接口值，替换前后都是同一个。
	var handle KV = dst

	if err := dst.RestoreFrom(bytes.NewReader(snap.Bytes())); err != nil {
		t.Fatal(err)
	}

	if got, err := handle.Get([]byte("k-new")); err != nil || string(got) != "from-snapshot" {
		t.Errorf("替换后读不到快照里的键: %q, %v", got, err)
	}
	if _, err := handle.Get([]byte("k-old")); !errors.Is(err, ErrNotFound) {
		t.Errorf("替换后旧键还在: %v", err)
	}

	// 替换后仍可写。
	if err := handle.Set([]byte("k-after"), []byte("x"), true); err != nil {
		t.Errorf("替换后写入失败: %v", err)
	}
}

// TestSwapKVRestoreKeepsOldDatabaseOnBadStream 流读坏时不得毁掉当前库。
//
// 快照来自网络（leader 推过来的流），传输中断是常态。一边收一边覆盖
// 目标文件的话，中断后旧库已经没了而新库是半份的——节点再也起不来。
func TestSwapKVRestoreKeepsOldDatabaseOnBadStream(t *testing.T) {
	dst, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	if err := dst.Set([]byte("k"), []byte("v"), true); err != nil {
		t.Fatal(err)
	}

	err = dst.RestoreFrom(&failingReader{after: 16})
	if err == nil {
		t.Fatal("读坏的流被当成了一份合法快照")
	}
	if !strings.Contains(err.Error(), "接收快照失败") {
		t.Errorf("错误没说清是哪一步失败: %v", err)
	}
	if got, gerr := dst.Get([]byte("k")); gerr != nil || string(got) != "v" {
		t.Errorf("流读坏之后当前库被毁了: %q, %v", got, gerr)
	}
}

func filteredShardKey(key []byte) bool {
	return len(key) > 0 && (key[0] == byte(PrefixSession) || key[0] == byte(PrefixShadow))
}

func TestFilteredSnapshotRestorePreservesShardKeyspaces(t *testing.T) {
	src, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	_ = src.Set(Key(PrefixThing, "new-meta"), []byte("new"), true)
	_ = src.Set(Key(PrefixSession, "source-session"), []byte("must-not-travel"), true)
	_ = src.Set(Key(PrefixShadow, "source-shadow", ""), []byte("must-not-travel"), true)
	var snap bytes.Buffer
	if _, err := src.SnapshotFiltered(&snap, filteredShardKey); err != nil {
		t.Fatal(err)
	}

	dst, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	_ = dst.Set(Key(PrefixThing, "old-meta"), []byte("old"), true)
	_ = dst.Set(Key(PrefixSession, "local-session"), []byte("keep"), true)
	_ = dst.Set(Key(PrefixShadow, "local-shadow", ""), []byte("keep"), true)
	if err := dst.RestoreFiltered(bytes.NewReader(snap.Bytes()), filteredShardKey); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Get(Key(PrefixThing, "old-meta")); !errors.Is(err, ErrNotFound) {
		t.Fatal("stale metadata survived filtered restore")
	}
	if got, err := dst.Get(Key(PrefixThing, "new-meta")); err != nil || string(got) != "new" {
		t.Fatalf("new metadata = %q, %v", got, err)
	}
	for _, key := range [][]byte{Key(PrefixSession, "local-session"), Key(PrefixShadow, "local-shadow", "")} {
		if got, err := dst.Get(key); err != nil || string(got) != "keep" {
			t.Fatalf("local shard key %x = %q, %v", key, got, err)
		}
	}
	for _, key := range [][]byte{Key(PrefixSession, "source-session"), Key(PrefixShadow, "source-shadow", "")} {
		if _, err := dst.Get(key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("source shard key %x leaked through metadata snapshot", key)
		}
	}
}

func TestFilteredRestoreAcceptsLegacyWholeBoltSnapshotWithoutOverwritingShards(t *testing.T) {
	src, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	_ = src.Set(Key(PrefixThing, "meta"), []byte("new"), true)
	_ = src.Set(Key(PrefixSession, "session"), []byte("foreign"), true)
	var legacy bytes.Buffer
	if _, err := src.Snapshot(&legacy); err != nil {
		t.Fatal(err)
	}

	dst, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	_ = dst.Set(Key(PrefixSession, "session"), []byte("local"), true)
	if err := dst.RestoreFiltered(bytes.NewReader(legacy.Bytes()), filteredShardKey); err != nil {
		t.Fatal(err)
	}
	if got, err := dst.Get(Key(PrefixSession, "session")); err != nil || string(got) != "local" {
		t.Fatalf("legacy restore overwrote local shard: %q, %v", got, err)
	}
	if got, err := dst.Get(Key(PrefixThing, "meta")); err != nil || string(got) != "new" {
		t.Fatalf("legacy metadata not restored: %q, %v", got, err)
	}
}

func TestFilteredRestoreRejectsShardKeyAndRollsBack(t *testing.T) {
	dst, err := OpenSwappable(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	metaKey := Key(PrefixThing, "before")
	_ = dst.Set(metaKey, []byte("keep-on-error"), true)
	var bad bytes.Buffer
	bad.Write(filteredSnapshotMagic)
	if err := writeKV(&bad, Key(PrefixSession, "smuggled"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := dst.RestoreFiltered(&bad, filteredShardKey); err == nil {
		t.Fatal("logical metadata snapshot accepted a shard key")
	}
	if got, err := dst.Get(metaKey); err != nil || string(got) != "keep-on-error" {
		t.Fatalf("failed restore did not roll back: %q, %v", got, err)
	}
}

type failingReader struct {
	after int
	n     int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n >= f.after {
		return 0, errors.New("连接断了")
	}
	n := len(p)
	if n > f.after-f.n {
		n = f.after - f.n
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	f.n += n
	return n, nil
}
