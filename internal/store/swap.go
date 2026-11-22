package store

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
)

/*
可整库替换的 KV（CLU-099）。

装一份状态机快照意味着**用另一个库文件替换当前这个**。难点不在替换本身，
而在"谁拿着这个库":注册表、会话、保留消息、影子各持有一个 KV，热视图的
重建也要读它。若直接把 store.Open 返回的值换掉，那几处手里仍是旧句柄——
它们会继续读一个已经被删掉的文件，而现象是"数据回到了快照之前"。

所以对外发出去的是这个包装：它把真正的实现放在一层 RWMutex 之后，
替换时原地换掉内层。读写各自只多一次 RLock，而冷层本来就不在热路径上
（热路径只读内存里的视图，契约 05）。
*/

// SwapKV 是一个内层可整体替换的 KV。
type SwapKV struct {
	mu    sync.RWMutex
	inner KV
	dir   string
}

var _ KV = (*SwapKV)(nil)

// OpenSwappable 打开一个可整库替换的 KV。
func OpenSwappable(opt Options) (*SwapKV, error) {
	inner, err := Open(opt)
	if err != nil {
		return nil, err
	}
	return &SwapKV{inner: inner, dir: opt.Dir}, nil
}

// Dir 是库所在的目录。
func (s *SwapKV) Dir() string { return s.dir }

func (s *SwapKV) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Get(key)
}

func (s *SwapKV) Set(key, value []byte, sync bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Set(key, value, sync)
}

func (s *SwapKV) Delete(key []byte, sync bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Delete(key, sync)
}

func (s *SwapKV) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Scan(prefix, fn)
}

func (s *SwapKV) Batch(ops []Op, sync bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Batch(ops, sync)
}

func (s *SwapKV) InTx(sync bool, fn func(Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.InTx(sync, fn)
}

// PruneMarked 在一个持久化事务里删除 prefix 下命中 remove 的键，
// 然后写入 marker。marker 已存在时不再扫描，用于百万键迁移的一次性裁剪。
// 删除与 marker 必须同事务：崩溃只能看到“全部完成”或“全部没做”。
func (s *SwapKV) PruneMarked(prefix, marker []byte,
	remove func(key []byte) (bool, error),
) (deleted int, already bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.inner == nil {
		return 0, false, ErrClosed
	}
	if p, ok := s.inner.(interface {
		PruneMarked([]byte, []byte, func([]byte) (bool, error)) (int, bool, error)
	}); ok {
		return p.PruneMarked(prefix, marker, remove)
	}
	return pruneMarkedTx(s.inner, prefix, marker, remove)
}

func pruneMarkedTx(kv interface {
	InTx(bool, func(Tx) error) error
}, prefix, marker []byte, remove func([]byte) (bool, error)) (deleted int, already bool, err error) {
	err = kv.InTx(true, func(tx Tx) error {
		if _, getErr := tx.Get(marker); getErr == nil {
			already = true
			return nil
		} else if !errors.Is(getErr, ErrNotFound) {
			return getErr
		}
		var callbackErr error
		if err := tx.Scan(prefix, func(key, _ []byte) bool {
			shouldRemove := false
			if remove != nil {
				shouldRemove, callbackErr = remove(key)
				if callbackErr != nil {
					return false
				}
			}
			if shouldRemove {
				if delErr := tx.Delete(key); delErr != nil {
					callbackErr = delErr
					return false
				}
				deleted++
			}
			return true
		}); err != nil {
			return err
		}
		if callbackErr != nil {
			return callbackErr
		}
		return tx.Set(marker, []byte{1})
	})
	if err != nil {
		deleted = 0
	}
	return deleted, already, err
}

func (s *SwapKV) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner.Flush()
}

func (s *SwapKV) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner == nil {
		return nil
	}
	err := s.inner.Close()
	s.inner = nil
	return err
}

// Snapshot 把当前库流式写出，满足 Snapshotter。
func (s *SwapKV) Snapshot(w interface{ Write([]byte) (int, error) }) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.inner.(Snapshotter)
	if !ok {
		return 0, errors.New("store: 当前实现不支持快照")
	}
	return snap.Snapshot(w)
}

var filteredSnapshotMagic = []byte("OICMETA2")

// SnapshotFiltered 流式输出不由 preserve 拥有的键。metadata 状态机用它
// 排除 session/shadow shard keyspace，避免 metadata 快照把 RF=3 数据重新
// 复制到每个节点。格式独立于 bbolt，后续更换引擎仍可恢复。
func (s *SwapKV) SnapshotFiltered(w io.Writer, preserve func([]byte) bool) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.inner == nil {
		return 0, ErrClosed
	}
	cw := &countWriter{w: w}
	if _, err := cw.Write(filteredSnapshotMagic); err != nil {
		return cw.n, err
	}
	var recordHeader [8]byte
	var writeErr error
	err := s.inner.Scan(nil, func(key, value []byte) bool {
		if preserve != nil && preserve(key) {
			return true
		}
		writeErr = writeKVHeader(cw, key, value, &recordHeader)
		return writeErr == nil
	})
	if err != nil {
		return cw.n, err
	}
	return cw.n, writeErr
}

type countWriter struct {
	w io.Writer
	n int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

// RestoreFiltered 原子替换 metadata 拥有的键并保留 preserve keyspace。
// 它同时接受旧版 bbolt 整库快照：先落临时只读库再逐键导入，因此升级后
// 尚未压缩掉的旧快照不会覆盖新的 shard 数据。
func (s *SwapKV) RestoreFiltered(r io.Reader, preserve func([]byte) bool) error {
	br := bufio.NewReader(r)
	header, err := br.Peek(len(filteredSnapshotMagic))
	if err == nil && bytes.Equal(header, filteredSnapshotMagic) {
		_, _ = br.Discard(len(filteredSnapshotMagic))
		return s.restoreLogicalFiltered(br, preserve)
	}
	return s.restoreLegacyFiltered(br, preserve)
}

func (s *SwapKV) restoreLogicalFiltered(r io.Reader, preserve func([]byte) bool) error {
	return s.replaceFiltered(preserve, func(set func([]byte, []byte) error) error {
		for {
			key, value, err := readKV(r)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if preserve != nil && preserve(key) {
				return fmt.Errorf("store: metadata 快照夹带 shard 键 %x", key)
			}
			if err := set(key, value); err != nil {
				return err
			}
		}
	})
}

func (s *SwapKV) restoreLegacyFiltered(r io.Reader, preserve func([]byte) bool) (err error) {
	tmp, err := os.CreateTemp(s.dir, ".legacy-snapshot-*.db")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	src, err := bolt.Open(name, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("store: 打开旧 metadata 快照失败: %w", err)
	}
	defer func() { _ = src.Close() }()
	return s.replaceFiltered(preserve, func(set func([]byte, []byte) error) error {
		return src.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			if b == nil {
				return nil
			}
			return b.ForEach(func(key, value []byte) error {
				if preserve != nil && preserve(key) {
					return nil
				}
				return set(key, value)
			})
		})
	})
}

func (s *SwapKV) replaceFiltered(preserve func([]byte) bool,
	load func(set func([]byte, []byte) error) error,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner == nil {
		return ErrClosed
	}
	if boltStore, ok := s.inner.(*boltKV); ok {
		return boltStore.replaceFiltered(preserve, load)
	}
	return s.inner.InTx(true, func(tx Tx) error {
		var keys [][]byte
		if err := tx.Scan(nil, func(key, _ []byte) bool {
			if preserve == nil || !preserve(key) {
				keys = append(keys, append([]byte(nil), key...))
			}
			return true
		}); err != nil {
			return err
		}
		for _, key := range keys {
			if err := tx.Delete(key); err != nil {
				return err
			}
		}
		return load(tx.Set)
	})
}

// RestoreFrom 用 r 里的整库内容替换当前库。
//
// 步骤是「先落临时文件、再关当前库、再改名、再打开」。顺序不能换：
//
//   - 先写临时文件，是因为 r 来自网络（leader 推过来的快照流）。一边收
//     一边覆盖目标文件的话，传输中断就留下一个半份的库，而那时旧库已经
//     没了——节点再也起不来。
//   - 改名之后要 fsync 目录。只 fsync 临时文件不够：掉电后目录项可能还
//     指着旧的 inode，于是"替换成功"在重启后变成没发生过。这一条是旧的
//     自写实现里漏掉的同一个毛病。
//
// 失败时尽力把旧库重新打开，让节点停在"视图陈旧"而不是"没有存储"。
func (s *SwapKV) RestoreFrom(r io.Reader) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner == nil {
		return ErrClosed
	}

	tmp, err := os.CreateTemp(s.dir, ".restore-*.db")
	if err != nil {
		return fmt.Errorf("store: 建临时库文件失败: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("store: 接收快照失败: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("store: 临时库落盘失败: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("store: 关闭临时库失败: %w", err)
	}

	path := filepath.Join(s.dir, "store.db")
	if err = s.inner.Close(); err != nil {
		return fmt.Errorf("store: 关闭当前库失败: %w", err)
	}
	s.inner = nil

	if err = os.Rename(tmpName, path); err != nil {
		// 改名失败时旧库还在原地，重新打开它。
		if back, oerr := Open(Options{Dir: s.dir}); oerr == nil {
			s.inner = back
		}
		return fmt.Errorf("store: 用快照替换库文件失败: %w", err)
	}
	if err = syncDir(s.dir); err != nil {
		return fmt.Errorf("store: 同步数据目录失败: %w", err)
	}

	next, err := Open(Options{Dir: s.dir})
	if err != nil {
		return fmt.Errorf("store: 打开替换后的库失败: %w", err)
	}
	s.inner = next
	return nil
}

// syncDir 把目录项落盘。
//
// 单独一个函数而不是内联：这一步最容易被当成多余的，而漏掉它的现象
// 要掉电才会出现——重启之后替换"没有发生过"。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
