package meta

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// 单机写入合批（CLU-190）的语义。
//
// 合批只该改变**落盘的次数**，不该改变任何一条命令的语义。下面四条是它必须
// 保住的，每一条都对应一个不这样做就会出现的缺陷。

// newBatchStore 造一个注入了整批事务的存储，并记录事务次数。
func newBatchStore(t *testing.T) (*LocalStore, map[string][]byte, *int) {
	t.Helper()
	vals := map[string][]byte{}
	txCalls := 0
	var mu sync.Mutex
	s := NewLocalStore(LocalOptions{
		Now:     func() int64 { return 1_700_000_000_000 },
		Compile: stubCompile,
		Cold:    &fakeCold{vals: vals},
		InTransaction: func(fn func(Transaction) error) error {
			mu.Lock()
			txCalls++
			mu.Unlock()
			return fn(&testMetaTx{vals: vals})
		},
	})
	t.Cleanup(func() { _ = s.Close() })
	return s, vals, &txCalls
}

func recordCmd(id, key, val string) Command {
	return Command{
		ID: id, Kind: CmdNop,
		Records: []RecordOp{{Key: []byte(key), Value: []byte(val)}},
	}
}

// TestApplyBatchMergesConcurrentWrites 钉住并发写入真的合进了更少的事务。
//
// 判据是**事务次数少于命令数**，而不是吞吐——吞吐随机器变化，而事务次数是
// 确定的。没有这条判定，合批退化成「每条一个事务」时不会有任何用例失败：
// 功能完全正常，只是慢回去了。
func TestApplyBatchMergesConcurrentWrites(t *testing.T) {
	s, vals, txCalls := newBatchStore(t)
	ctx := context.Background()

	const n = 64
	var wg sync.WaitGroup
	errs := make([]error, n)
	revs := make([]Revision, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 一起起跑，否则合批无从发生
			revs[i], errs[i] = s.Apply(ctx, recordCmd("", keyOf(i), "v"))
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("第 %d 条: %v", i, errs[i])
		}
		if revs[i] == 0 {
			t.Fatalf("第 %d 条没拿到 revision", i)
		}
	}
	// 每条命令的记录都要落地，一条不少。
	for i := 0; i < n; i++ {
		if string(vals[keyOf(i)]) != "v" {
			t.Fatalf("第 %d 条的记录没落地", i)
		}
	}
	// revision 必须互不相同：它是调用方拿到的那条命令的身份。
	seen := map[Revision]bool{}
	for _, r := range revs {
		if seen[r] {
			t.Fatalf("revision %d 被两条命令共用", r)
		}
		seen[r] = true
	}
	if *txCalls >= n {
		t.Fatalf("事务次数 = %d，%d 条命令一条都没合上", *txCalls, n)
	}
	t.Logf("%d 条命令合成 %d 个事务", n, *txCalls)
}

func keyOf(i int) string { return "k" + string(rune('A'+i%26)) + string(rune('a'+i/26)) }

// TestApplyBatchKeepsIdempotency 钉住幂等窗口在合批之后仍然生效。
//
// 相同命令 ID 必须返回首次应用的 revision 且不重复改动状态。合批里最容易
// 丢掉这一条：批内两条同 ID 的命令如果都执行，冷层会被写两次，而第二次的
// revision 是新的——调用方据此以为自己的那次写入生效了。
func TestApplyBatchKeepsIdempotency(t *testing.T) {
	s, vals, _ := newBatchStore(t)
	ctx := context.Background()

	first, err := s.Apply(ctx, recordCmd("same", "k1", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// 同 ID 但内容不同：必须返回首次的 revision，而且不得改动冷层。
	again, err := s.Apply(ctx, recordCmd("same", "k1", "v2"))
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("幂等命中返回 %d，期望首次的 %d", again, first)
	}
	if string(vals["k1"]) != "v1" {
		t.Fatalf("幂等命中却改了冷层：%q", vals["k1"])
	}
}

// TestApplyBatchKeepsOptimisticConcurrency 钉住乐观并发判定。
//
// RequestedRevision 比的是**此刻**的 revision，而它随批内前一条命令推进，
// 所以这条判定必须按批内顺序逐条做。一次性用批开始时的 revision 去比，
// 同批的第二条就会被错判成冲突（或错判成不冲突）。
func TestApplyBatchKeepsOptimisticConcurrency(t *testing.T) {
	s, vals, _ := newBatchStore(t)
	ctx := context.Background()

	rev, err := s.Apply(ctx, recordCmd("", "k1", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// 用当前 revision 作为前置条件：应当成功。
	cmd := recordCmd("", "k2", "v2")
	cmd.RequestedRevision = rev
	if _, err := s.Apply(ctx, cmd); err != nil {
		t.Fatalf("前置条件正确却被拒: %v", err)
	}
	// 用过期的 revision：必须冲突，而且不得写冷层。
	stale := recordCmd("", "k3", "v3")
	stale.RequestedRevision = rev
	if _, err := s.Apply(ctx, stale); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("过期前置条件 = %v，期望 ErrRevisionConflict", err)
	}
	if _, ok := vals["k3"]; ok {
		t.Fatal("被判冲突的命令写了冷层")
	}
}

// TestApplyBypassesBatchWithoutPersistence 钉住「只重建热视图的命令不开事务」。
//
// 这不是优化而是正确性要求：启动期的 Registry.Restore 在一个**打开的 bbolt
// 读游标**里逐条调 Apply，而 bbolt 的写事务一旦需要扩文件就要拿 mmaplock，
// 那把锁要求没有打开的读事务。无条件开写事务会在那里死锁——实测节点起不来，
// worker 卡在 bbolt 的 mmap 上。
func TestApplyBypassesBatchWithoutPersistence(t *testing.T) {
	s, _, txCalls := newBatchStore(t)
	ctx := context.Background()

	// 不带记录也不带影子：一个事务都不该开。
	for i := 0; i < 8; i++ {
		if _, err := s.Apply(ctx, Command{Kind: CmdNop}); err != nil {
			t.Fatalf("第 %d 条: %v", i, err)
		}
	}
	if *txCalls != 0 {
		t.Fatalf("只重建热视图的命令开了 %d 个事务，期望 0", *txCalls)
	}

	// 带记录的那一条才进合批。
	if _, err := s.Apply(ctx, recordCmd("", "k1", "v")); err != nil {
		t.Fatal(err)
	}
	if *txCalls != 1 {
		t.Fatalf("带记录的命令开了 %d 个事务，期望 1", *txCalls)
	}
}

// TestApplyBatchRejectsAfterClose 钉住关闭与只读之后的拒绝。
//
// 排队中的请求必须拿到错误而不是永远等下去：调用方在拆解过程中仍会写入，
// 而那时装配可能已经开始关停。
func TestApplyBatchRejectsAfterClose(t *testing.T) {
	s, _, _ := newBatchStore(t)
	ctx := context.Background()
	if _, err := s.Apply(ctx, recordCmd("", "k1", "v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, recordCmd("", "k2", "v")); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("关闭后 = %v，期望 ErrStoreClosed", err)
	}
}

// TestApplyBatchFallsBackPerCommand 钉住「整批回滚之后逐条退回」。
//
// 这些调用方互不相关，一条坏命令不该把同批的其他写入一起拖掉。集群那一侧
// 可以整批失败，因为那条日志已经被多数派提交、节点停下来是正确反应
// （CLU-066）；单机这一侧不是——调用方还在等一个属于自己的答复。
//
// 判据：第一次事务失败后，同批的其他命令仍然落地，而坏的那条拿到错误。
func TestApplyBatchFallsBackPerCommand(t *testing.T) {
	vals := map[string][]byte{}
	var mu sync.Mutex
	txCalls := 0
	// 第一次事务（合批那次）失败，之后放行：退回的逐条路径不走 inTx，
	// 所以这里只需要让合批那一次失败。
	boom := errors.New("模拟事务失败")
	s := NewLocalStore(LocalOptions{
		Now:     func() int64 { return 1_700_000_000_000 },
		Compile: stubCompile,
		Cold:    &fakeCold{vals: vals},
		InTransaction: func(fn func(Transaction) error) error {
			mu.Lock()
			txCalls++
			first := txCalls == 1
			mu.Unlock()
			if first {
				return boom
			}
			return fn(&testMetaTx{vals: vals})
		},
	})
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	// 单条也会走合批路径（它带记录），于是第一次事务失败并退回逐条。
	rev, err := s.Apply(ctx, recordCmd("", "k1", "v1"))
	if err != nil {
		t.Fatalf("退回路径应当成功：%v", err)
	}
	if rev == 0 {
		t.Fatal("退回之后仍要给出 revision：日志序号已经分配出去了")
	}
	// 逐条退回走的是不带事务的老路径，所以记录经 Cold 写入。
	if string(vals["k1"]) != "v1" {
		t.Fatalf("退回之后记录没落地：%#v", vals)
	}

	// 热视图的 revision 必须跟上，否则视图会被判为陈旧而对新连接
	// fail-closed（CLU-023）。
	if got := s.View().Revision(); got != rev {
		t.Fatalf("热视图 revision = %d，期望 %d", got, rev)
	}
}
