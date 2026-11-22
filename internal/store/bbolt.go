package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bucketName 是存放全部数据的桶名。
//
// 只用一个桶、靠键前缀分命名空间（见 kv.go），而不是每个命名空间一个桶：
// 这样 Scan 的语义与键设计在任何引擎上都一致，换引擎时不必重新设计
// 键空间。桶只是 bbolt 的概念，不该渗进本项目的键设计。
var bucketName = []byte("oiot")

// Options 是打开本地存储的参数。
type Options struct {
	// Dir 是数据目录，库文件为其下的 store.db。
	Dir string
	// OpenTimeout 是获取文件锁的等待上限。
	//
	// 必须有限：两个 openiotd 指向同一数据目录时，第二个应当得到一条
	// 明确的错误，而不是永久挂住。后者的现象是"进程起来了但什么都不做"，
	// 排查成本远高于一条启动失败。
	OpenTimeout time.Duration
	// ReadOnly 以只读方式打开，用于离线检查与备份校验。
	ReadOnly bool
	// NoFreelistSync 跳过 freelist 落盘以加快打开速度，代价是
	// 异常退出后的下次打开需要重建 freelist。默认关闭。
	NoFreelistSync bool
}

// boltKV 是 KV 的 bbolt 实现。
type boltKV struct {
	db   *bolt.DB
	path string

	mu     sync.RWMutex
	closed bool
}

// Open 打开（必要时创建）本地存储。
func Open(opt Options) (KV, error) {
	if opt.Dir == "" {
		return nil, errors.New("store: 缺少数据目录")
	}
	if opt.OpenTimeout <= 0 {
		opt.OpenTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(opt.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: 创建数据目录 %s 失败: %w", opt.Dir, err)
	}
	path := filepath.Join(opt.Dir, "store.db")

	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:        opt.OpenTimeout,
		ReadOnly:       opt.ReadOnly,
		NoFreelistSync: opt.NoFreelistSync,
	})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf(
				"store: 获取 %s 的文件锁超时（%s）；"+
					"该数据目录可能已被另一个 openiotd 进程占用",
				path, opt.OpenTimeout)
		}
		return nil, fmt.Errorf("store: 打开 %s 失败: %w", path, err)
	}

	kv := &boltKV{db: db, path: path}
	if !opt.ReadOnly {
		if err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		}); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: 初始化存储失败: %w", err)
		}
		if err := checkFormatVersion(kv); err != nil {
			_ = kv.Close()
			return nil, err
		}
	}
	return kv, nil
}

// Path 返回库文件路径，供日志与备份使用。
func (k *boltKV) Path() string { return k.path }

func (k *boltKV) Get(key []byte) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return nil, ErrClosed
	}
	var out []byte
	err := k.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return ErrNotFound
		}
		v := b.Get(key)
		if v == nil {
			return ErrNotFound
		}
		// 必须复制：v 指向 mmap 区域，事务结束后即失效。
		// 返回该视图会让"事务外使用"表现为读到随机数据或崩溃。
		out = make([]byte, len(v))
		copy(out, v)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("store: 读取失败: %w", err)
	}
	return out, nil
}

func (k *boltKV) Set(key, value []byte, sync bool) error {
	return k.write(sync, func(b *bolt.Bucket) error { return b.Put(key, value) })
}

func (k *boltKV) Delete(key []byte, sync bool) error {
	return k.write(sync, func(b *bolt.Bucket) error { return b.Delete(key) })
}

func (k *boltKV) Batch(ops []Op, sync bool) error {
	if len(ops) == 0 {
		return nil
	}
	// 先校验再应用：批量写入须原子，中途遇到非法操作不应留下部分写入
	for i := range ops {
		if ops[i].Kind != OpSet && ops[i].Kind != OpDelete {
			return fmt.Errorf("store: 未知的操作类型 %d", ops[i].Kind)
		}
	}
	return k.write(sync, func(b *bolt.Bucket) error {
		for i := range ops {
			var err error
			switch ops[i].Kind {
			case OpSet:
				err = b.Put(ops[i].Key, ops[i].Value)
			case OpDelete:
				err = b.Delete(ops[i].Key)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// write 在一个写事务中执行 fn。
//
// sync 的语义映射到 bbolt 的两种提交方式：
//   - sync 为真走 Update，事务提交即 fsync，返回时数据已落盘；
//   - sync 为假走 Batch，多个并发调用被合并进同一个事务，fsync 被摊薄。
//
// 注意 Batch 仍然是持久的——它只是推迟并合并，不是不落盘。
// 这比全局 NoSync 好：后者有一个真实的数据丢失窗口，而这里没有。
func (k *boltKV) write(sync bool, fn func(*bolt.Bucket) error) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return ErrClosed
	}
	body := func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("store: 数据桶不存在")
		}
		return fn(b)
	}
	var err error
	if sync {
		err = k.db.Update(body)
	} else {
		err = k.db.Batch(body)
	}
	if err != nil {
		return fmt.Errorf("store: 写入失败: %w", err)
	}
	return nil
}

// boltTx 把 bbolt 的桶包成 Tx。
//
// 只持桶而不持 *bolt.Tx：Tx 接口不需要提交与回滚（见它的说明），
// 而少暴露一个引擎类型就少一处把引擎泄到业务代码里的机会（ARCH-065）。
type boltTx struct{ b *bolt.Bucket }

func (t boltTx) Get(key []byte) ([]byte, error) {
	v := t.b.Get(key)
	if v == nil {
		return nil, ErrNotFound
	}
	// 复制：bbolt 返回的是 mmap 视图，事务一关就不再有效，而 KV.Get
	// 的约定是"调用方可以随意持有与修改"。
	return append([]byte(nil), v...), nil
}

func (t boltTx) Set(key, value []byte) error { return t.b.Put(key, value) }

func (t boltTx) Delete(key []byte) error { return t.b.Delete(key) }

func (t boltTx) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	c := t.b.Cursor()
	for key, val := c.Seek(prefix); key != nil &&
		bytes.HasPrefix(key, prefix); key, val = c.Next() {
		if !fn(key, val) {
			return nil
		}
	}
	return nil
}

func (k *boltKV) InTx(sync bool, fn func(Tx) error) error {
	return k.write(sync, func(b *bolt.Bucket) error { return fn(boltTx{b: b}) })
}

// replaceFiltered 在一个 bbolt 写事务里流式删除非保留键并装入新记录。
// Cursor.Delete 避免先把百万 metadata 键复制进内存。
func (k *boltKV) replaceFiltered(preserve func([]byte) bool,
	load func(set func([]byte, []byte) error) error,
) error {
	return k.write(true, func(b *bolt.Bucket) error {
		c := b.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			if preserve != nil && preserve(key) {
				continue
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return load(b.Put)
	})
}

// PruneMarked 用 bbolt cursor 就地删除，不把百万个键先复制到堆上。
func (k *boltKV) PruneMarked(prefix, marker []byte,
	remove func(key []byte) (bool, error),
) (deleted int, already bool, err error) {
	err = k.write(true, func(b *bolt.Bucket) error {
		if b.Get(marker) != nil {
			already = true
			return nil
		}
		c := b.Cursor()
		for key, _ := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = c.Next() {
			shouldRemove := false
			if remove != nil {
				var removeErr error
				shouldRemove, removeErr = remove(key)
				if removeErr != nil {
					return removeErr
				}
			}
			if !shouldRemove {
				continue
			}
			if err := c.Delete(); err != nil {
				return err
			}
			deleted++
		}
		return b.Put(marker, []byte{1})
	})
	if err != nil {
		deleted = 0
	}
	return deleted, already, err
}

func (k *boltKV) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return ErrClosed
	}
	err := k.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for key, v := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, v = c.Next() {
			if !fn(key, v) {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store: 遍历失败: %w", err)
	}
	return nil
}

func (k *boltKV) Flush() error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return ErrClosed
	}
	if err := k.db.Sync(); err != nil {
		return fmt.Errorf("store: 刷盘失败: %w", err)
	}
	return nil
}

func (k *boltKV) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return nil
	}
	k.closed = true
	if err := k.db.Close(); err != nil {
		return fmt.Errorf("store: 关闭失败: %w", err)
	}
	return nil
}

// Snapshot 把一致的全库快照写出到 w。
//
// 这是 CLU-003 的落点：百万设备量级的快照不得先读进内存。
// bbolt 的实现是在一个读事务中把整个库文件流式写出，内存占用与
// 库大小无关——它同时也是备份命令的实现（NFR-063）。
func (k *boltKV) Snapshot(w interface{ Write([]byte) (int, error) }) (int64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return 0, ErrClosed
	}
	var n int64
	err := k.db.View(func(tx *bolt.Tx) error {
		written, werr := tx.WriteTo(writerFunc(w.Write))
		n = written
		return werr
	})
	if err != nil {
		return n, fmt.Errorf("store: 生成快照失败: %w", err)
	}
	return n, nil
}

// writerFunc 把一个 Write 方法适配成 io.Writer。
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// Snapshotter 是能产出一致全库快照的存储。
//
// 单列一个接口而不是放进 KV：不是每个 KV 实现都需要它
// （内存实现就不需要），而 KV 越小越容易替换。
type Snapshotter interface {
	Snapshot(w interface{ Write([]byte) (int, error) }) (int64, error)
}
