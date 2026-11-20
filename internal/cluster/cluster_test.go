package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestEpochPacking(t *testing.T) {
	cases := []struct{ term, seq uint32 }{
		{1, 1}, {1, 4294967295}, {2, 1}, {4294967295, 4294967295},
	}
	for _, c := range cases {
		e := MakeEpoch(c.term, c.seq)
		if e.Term() != c.term || e.Seq() != c.seq {
			t.Errorf("MakeEpoch(%d,%d) 回取为 (%d,%d)", c.term, c.seq, e.Term(), e.Seq())
		}
		if !e.Valid() {
			t.Errorf("MakeEpoch(%d,%d) 应有效", c.term, c.seq)
		}
	}
	if ZeroEpoch.Valid() {
		t.Error("ZeroEpoch 应无效")
	}
	if MakeEpoch(0, 0) != ZeroEpoch {
		t.Error("term 与序号都为 0 应等于 ZeroEpoch")
	}
}

// TestEpochMonotonicAcrossTerm 是契约 01 围栏正确性的核心性质：
// leader 切换后 term 严格增大，即使新 leader 从序号 1 重新开始计数，
// 其 epoch 仍严格大于旧 term 的任何取值。
func TestEpochMonotonicAcrossTerm(t *testing.T) {
	oldTermMax := MakeEpoch(1, ^uint32(0)) // 旧 term 内的最大 epoch
	newTermMin := MakeEpoch(2, 1)          // 新 term 内的最小 epoch
	if newTermMin <= oldTermMax {
		t.Fatalf("新 term 的最小 epoch %d 未大于旧 term 的最大 epoch %d",
			newTermMin, oldTermMax)
	}
	// 任意 term 递增都保持该性质
	for term := uint32(1); term < 100; term++ {
		hi := MakeEpoch(term, ^uint32(0))
		lo := MakeEpoch(term+1, 0)
		if lo <= hi {
			t.Fatalf("term %d→%d 的 epoch 不单调: %d 未大于 %d", term, term+1, lo, hi)
		}
	}
}

func TestLocalOwnershipClaimAndTakeover(t *testing.T) {
	o := NewLocalOwnership("node-1", 8)
	defer o.Close()
	ctx := context.Background()

	e1, err := o.Claim(ctx, "dev-1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if !e1.Valid() {
		t.Fatal("首次抢占应返回有效 epoch")
	}
	// 尚无其他持有者，不应产生驱逐通知
	select {
	case ev := <-o.Evictions():
		t.Fatalf("首次抢占不应产生驱逐通知: %+v", ev)
	default:
	}

	e2, err := o.Claim(ctx, "dev-1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if e2 <= e1 {
		t.Errorf("再次抢占的 epoch %d 应大于 %d", e2, e1)
	}
	ev := <-o.Evictions()
	if ev.ClientID != "dev-1" || ev.PrevEpoch != e1 || ev.NewEpoch != e2 ||
		ev.Reason != EvictTakenOver {
		t.Errorf("驱逐通知不正确: %+v", ev)
	}

	// 查询应返回最新 epoch
	node, ep, ok := o.Lookup(ctx, "dev-1")
	if !ok || node != "node-1" || ep != e2 {
		t.Errorf("Lookup = (%q, %d, %v)，期望 (node-1, %d, true)", node, ep, ok, e2)
	}
	if _, _, ok := o.Lookup(ctx, "不存在"); ok {
		t.Error("不存在的 clientId 应返回 false")
	}
}

// TestLocalOwnershipReleaseFencing 验证被接管的旧会话在清理时
// 不能删掉新会话的所有权——这是 epoch 围栏最实际的一个作用。
func TestLocalOwnershipReleaseFencing(t *testing.T) {
	o := NewLocalOwnership("node-1", 8)
	defer o.Close()
	ctx := context.Background()

	e1, _ := o.Claim(ctx, "dev-1", "node-1")
	e2, _ := o.Claim(ctx, "dev-1", "node-1")

	// 旧会话以旧 epoch 释放，必须被拒绝
	if err := o.Release(ctx, "dev-1", e1); !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("以旧 epoch 释放应返回 ErrStaleEpoch，得到 %v", err)
	}
	if _, _, ok := o.Lookup(ctx, "dev-1"); !ok {
		t.Fatal("所有权不应被旧会话删除")
	}
	// 新会话以当前 epoch 释放，应成功
	if err := o.Release(ctx, "dev-1", e2); err != nil {
		t.Errorf("以当前 epoch 释放应成功，得到 %v", err)
	}
	if _, _, ok := o.Lookup(ctx, "dev-1"); ok {
		t.Error("释放后所有权应消失")
	}
	// 释放不存在的所有权不是错误
	if err := o.Release(ctx, "dev-1", e2); err != nil {
		t.Errorf("释放不存在的所有权应无错误，得到 %v", err)
	}
}

// TestNoDoubleLiveSession 是 PRD TST-050 的不变量断言：
// 任意时刻同一 clientId 至多一个会话持有有效授权（epoch 最大者）。
// 并发抢占下，所有返回的 epoch 必须互不相同，且只有最大者与当前归属一致。
func TestNoDoubleLiveSession(t *testing.T) {
	o := NewLocalOwnership("node-1", 1024)
	defer o.Close()
	ctx := context.Background()

	const n = 200
	epochs := make([]Epoch, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			e, err := o.Claim(ctx, "dev-1", "node-1")
			if err != nil {
				t.Error(err)
				return
			}
			epochs[i] = e
		}(i)
	}
	wg.Wait()

	seen := make(map[Epoch]bool, n)
	var max Epoch
	for _, e := range epochs {
		if !e.Valid() {
			t.Fatal("出现无效 epoch")
		}
		if seen[e] {
			t.Fatalf("epoch %d 被重复发放，违反单调性", e)
		}
		seen[e] = true
		if e > max {
			max = e
		}
	}
	_, cur, ok := o.Lookup(ctx, "dev-1")
	if !ok {
		t.Fatal("应存在归属")
	}
	if cur != max {
		t.Errorf("当前 epoch %d 不是发放过的最大值 %d", cur, max)
	}
	// 除最大者外，任何 epoch 的持有者都应被围栏拒绝
	for _, e := range epochs {
		if e == max {
			continue
		}
		if err := o.Release(ctx, "dev-1", e); !errors.Is(err, ErrStaleEpoch) {
			t.Fatalf("epoch %d 不是当前持有者，释放应被拒绝", e)
		}
	}
}

func TestLocalOwnershipEvict(t *testing.T) {
	o := NewLocalOwnership("node-1", 8)
	defer o.Close()
	ctx := context.Background()

	if _, ok := o.Evict("不存在", EvictAdministrative); ok {
		t.Error("驱逐不存在的会话应返回 false")
	}
	e, _ := o.Claim(ctx, "dev-1", "node-1")
	got, ok := o.Evict("dev-1", EvictAdministrative)
	if !ok || got != e {
		t.Errorf("Evict = (%d, %v)，期望 (%d, true)", got, ok, e)
	}
	ev := <-o.Evictions()
	if ev.Reason != EvictAdministrative || ev.NewEpoch != ZeroEpoch {
		t.Errorf("驱逐通知不正确: %+v", ev)
	}
	if o.Len() != 0 {
		t.Errorf("驱逐后应无归属，得到 %d", o.Len())
	}
}

// TestEvictionBufferNeverBlocks 验证通知缓冲满时 Claim 不被阻塞：
// 所有权转移是同步完成的，通知只用于加速旧会话清理，不能成为瓶颈。
func TestEvictionBufferNeverBlocks(t *testing.T) {
	o := NewLocalOwnership("node-1", 2)
	defer o.Close()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if _, err := o.Claim(ctx, "dev-1", "node-1"); err != nil {
			t.Fatalf("第 %d 次抢占被阻塞或失败: %v", i, err)
		}
	}
	if o.Len() != 1 {
		t.Errorf("归属数 = %d，期望 1", o.Len())
	}
}

func TestLocalOwnershipClosed(t *testing.T) {
	o := NewLocalOwnership("node-1", 4)
	ctx := context.Background()
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Errorf("重复关闭应幂等，得到 %v", err)
	}
	if _, err := o.Claim(ctx, "d", "node-1"); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后抢占应返回 ErrClosed，得到 %v", err)
	}
	if err := o.Release(ctx, "d", MakeEpoch(1, 1)); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后释放应返回 ErrClosed，得到 %v", err)
	}
	if _, ok := o.Evict("d", EvictAdministrative); ok {
		t.Error("关闭后驱逐应返回 false")
	}
}

func TestLocalPartitioner(t *testing.T) {
	p := NewLocalPartitioner("node-1", 64)
	if p.Count() != 64 {
		t.Errorf("分区数 = %d，期望 64", p.Count())
	}
	if NewLocalPartitioner("n", 0).Count() != DefaultShardCount {
		t.Error("非法分区数应回落到默认值")
	}
	// 单节点下所有键都归本节点，但分区号仍真实计算
	for _, k := range []string{"dev-1", "dev-2", "", "很长的设备名-0000000001"} {
		if !p.IsLocal(k) {
			t.Errorf("%q 应归本节点", k)
		}
		if p.Owner(k) != "node-1" {
			t.Errorf("%q 的归属 = %q", k, p.Owner(k))
		}
		if s := p.Shard(k); s >= 64 {
			t.Errorf("%q 的分区号 %d 超出范围", k, s)
		}
	}
	// 同一键必须稳定落在同一分区
	for i := 0; i < 100; i++ {
		if p.Shard("dev-42") != p.Shard("dev-42") {
			t.Fatal("分区号不稳定")
		}
	}
	// 分区应大致均匀，避免哈希退化
	hits := make(map[uint32]int)
	for i := 0; i < 6400; i++ {
		hits[p.Shard(string(rune('a'+i%26))+"-dev-"+string(rune('0'+i%10))+itoa(i))]++
	}
	if len(hits) < 50 {
		t.Errorf("6400 个键只命中 %d 个分区，哈希分布可能退化", len(hits))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func TestLoopbackTransport(t *testing.T) {
	tr := NewLoopbackTransport("node-1")
	ctx := context.Background()

	var got []Frame
	var from NodeID
	tr.OnReceive(func(f NodeID, batch []Frame) { from = f; got = append(got, batch...) })

	batch := []Frame{{Version: FrameVersion, Origin: "node-1", Seq: 1, Topic: "a/b"}}
	// 投向本节点应回灌给接收回调，使接收路径同样被覆盖
	if err := tr.Forward(ctx, []NodeID{"node-1"}, batch); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || from != "node-1" || got[0].Topic != "a/b" {
		t.Errorf("本节点转发未回灌: from=%q got=%+v", from, got)
	}
	// 投向不存在的节点是空操作，单机下该分支不会进入但必须不报错
	got = nil
	if err := tr.Forward(ctx, []NodeID{"node-9"}, batch); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("投向其他节点不应回灌，得到 %+v", got)
	}
	// 空批次不触发回调
	if err := tr.Forward(ctx, []NodeID{"node-1"}, nil); err != nil {
		t.Fatal(err)
	}
	if tr.Self() != "node-1" {
		t.Errorf("Self = %q", tr.Self())
	}
}

func TestLoopbackTransportQuery(t *testing.T) {
	tr := NewLoopbackTransport("node-1")
	ctx := context.Background()

	// 未注册处理器时返回一个空结果，而不是报错
	res, err := tr.Query(ctx, nil, AdminQuery{Kind: "connections", Scope: ScopeAllNodes})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Node != "node-1" {
		t.Fatalf("扇出结果 = %+v", res)
	}

	tr.SetLocalQuery(func(q AdminQuery) ([]any, string, error) {
		return []any{q.Kind, q.Cursor["node-1"]}, "cursor-2", nil
	})
	res, err = tr.Query(ctx, []NodeID{"node-1"}, AdminQuery{
		Kind:   "connections",
		Scope:  ScopeAllNodes,
		Cursor: map[NodeID]string{"node-1": "cursor-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].NextCursor != "cursor-2" {
		t.Fatalf("扇出结果 = %+v", res)
	}
	if len(res[0].Items) != 2 || res[0].Items[1] != "cursor-1" {
		t.Errorf("游标未按节点传入: %+v", res[0].Items)
	}
	// 目标不含本节点时不返回结果
	res, err = tr.Query(ctx, []NodeID{"node-9"}, AdminQuery{Scope: ScopeLocalOnly})
	if err != nil || len(res) != 0 {
		t.Errorf("目标不含本节点应无结果: res=%+v err=%v", res, err)
	}

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Forward(ctx, []NodeID{"node-1"}, batchOf("x")); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后转发应返回 ErrClosed，得到 %v", err)
	}
	if _, err := tr.Query(ctx, nil, AdminQuery{}); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后查询应返回 ErrClosed，得到 %v", err)
	}
}

func batchOf(topic string) []Frame {
	return []Frame{{Version: FrameVersion, Topic: topic}}
}

func TestHashKeyStable(t *testing.T) {
	// 分区哈希必须在所有节点上一致，因此不能使用 Go 的 map 哈希
	// （后者每进程随机加盐）。这里固定住几个取值以防实现被替换。
	if hashKey("") != 14695981039346656037 {
		t.Errorf("空串的哈希 = %d", hashKey(""))
	}
	if hashKey("a") == hashKey("b") {
		t.Error("不同输入的哈希不应相同")
	}
	if hashKey("dev-1") != hashKey("dev-1") {
		t.Error("哈希不稳定")
	}
}
