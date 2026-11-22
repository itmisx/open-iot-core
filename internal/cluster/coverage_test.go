package cluster

import (
	"context"
	"errors"
	"testing"
)

// 这个文件补的是那些「只在真集群里才走到、于是一直没有用例」的分支。
//
// 它们都有一个共同点：返回的是**拒绝**而不是结果——没有 leader、组还没起、
// 所有权已关闭、这个键不属于本副本。拒绝路径没有用例的后果不是「少测了一点」，
// 而是改动时看不出有没有把拒绝改成了静默成功：那种缺陷在单机上永远不显形。

// TestSwitchableOwnershipRejectsAfterClose 钉住关闭之后的四个入口。
//
// 关掉之后仍然可调用是刻意的：连接在拆解过程中还会归还所有权，而那时
// 装配可能已经开始关停。此时必须返回 ErrClosed 而不是 panic，也不能假装
// 成功——假装成功会让一条记录永远留着它的 owner。
func TestSwitchableOwnershipRejectsAfterClose(t *testing.T) {
	s := NewSwitchableOwnership(NewLocalOwnership("a", 4), 4)
	ctx := context.Background()

	e, err := s.Claim(ctx, "dev-1", "a")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if node, got, ok := s.Lookup(ctx, "dev-1"); !ok || node != "a" || got != e {
		t.Fatalf("Lookup = %q/%v/%v，期望 a/%v/true", node, got, ok, e)
	}
	// 单机所有权没有远程 Claim 队列，所以这里必须是零值而不是「没实现」。
	if st := s.Stats(); st != (OwnershipStats{}) {
		t.Fatalf("单机所有权的 Stats = %+v，期望零值", st)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 必须幂等: %v", err)
	}
	if _, err := s.Claim(ctx, "dev-2", "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Claim = %v，期望 ErrClosed", err)
	}
	if err := s.Release(ctx, "dev-1", e); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Release = %v，期望 ErrClosed", err)
	}
	if _, _, ok := s.Lookup(ctx, "dev-1"); ok {
		t.Fatal("关闭后 Lookup 必须返回 false")
	}
	if err := s.Use(NewLocalOwnership("b", 4)); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Use = %v，期望 ErrClosed", err)
	}
}

// TestSwitchableOwnershipRejectsNilTarget 钉住「切到 nil」必须报错。
//
// Use 只在装配后期调用一次，而那时 nil 意味着上一步构造失败却没被发现。
// 悄悄接受 nil 的后果是此后每次 Claim 都走 ErrClosed 分支，看起来像是
// 所有权被关了。
func TestSwitchableOwnershipRejectsNilTarget(t *testing.T) {
	s := NewSwitchableOwnership(NewLocalOwnership("a", 4), 4)
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Use(nil); err == nil {
		t.Fatal("Use(nil) 必须报错")
	}
}

// TestTLSGroupMemberChangeRequiresLeader 钉住成员变更的两道前置判定。
//
// 组还没起来时必须回 ErrNotLeader：成员变更要先被当前多数派接受，而一个
// 没有组的节点连多数派是谁都不知道。空的 ID 或地址要在更早一步拦住——
// 它们会变成一条谁也拨不通的成员记录，而那条记录是进复制日志的。
func TestTLSGroupMemberChangeRequiresLeader(t *testing.T) {
	var g TLSGroup // 没有启动的组：g() 返回 nil

	if err := g.AddMember("", "127.0.0.1:1", 2); err == nil {
		t.Fatal("空节点 ID 必须被拒绝")
	}
	if err := g.AddMember("b", "", 2); err == nil {
		t.Fatal("空地址必须被拒绝")
	}
	if err := g.AddMember("b", "127.0.0.1:1", 2); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("未启动的组 AddMember = %v，期望 ErrNotLeader", err)
	}
	if err := g.RemoveMember("b"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("未启动的组 RemoveMember = %v，期望 ErrNotLeader", err)
	}
}

// TestShardGroupLocalReplicaAndBalanceWithoutLocalShards 钉住「本副本不承载
// 任何分片」这一支。
//
// 它不是假想情形：RF 小于节点数时，每个节点都只承载一部分分片，而
// LeadersBalanced 要能在不碰 NodeHost 的前提下对「与我无关的分片」直接跳过。
// 少了这一支的后果是非副本节点上的就绪判定去读一个它没有的分片。
func TestShardGroupLocalReplicaAndBalanceWithoutLocalShards(t *testing.T) {
	// 三个副本、四个分片、RF=1：每个分片只落在一个副本上。
	p, err := newShardPlacement(1024, map[NodeID]uint64{"a": 1, "b": 2, "c": 3}, 4, 1)
	if err != nil {
		t.Fatalf("放置表: %v", err)
	}
	// replica 9 不在任何分片的副本集里，于是每一轮都 continue。
	s := &ShardGroup{base: 1024, count: 4, replica: 9, placement: p}
	if !s.LeadersBalanced() {
		t.Fatal("本副本不承载任何分片时，leader 必须算作已均衡")
	}
	if s.HasLocalReplicaBytes([]byte("dev-1")) {
		t.Fatal("本副本不承载任何分片时，没有键属于它")
	}

	// 换成真的承载者：同一个键在它的副本上必须为真。
	owner, ok := p.leader(shardForBytes(1024, 4, []byte("dev-1")))
	if !ok {
		t.Fatal("取不到该键所属分片的首选副本")
	}
	s2 := &ShardGroup{base: 1024, count: 4, replica: owner, placement: p}
	if !s2.HasLocalReplicaBytes([]byte("dev-1")) {
		t.Fatalf("副本 %d 承载该键所在分片，HasLocalReplicaBytes 却为假", owner)
	}
}

// TestShardPlacementRejectsInvalidInput 钉住放置表的四种无效输入。
//
// 放置表一经落盘不可更改（见 ensureShardLayout），所以它必须在构造时就拒绝
// 这些输入；放过去之后再发现，要做的是 Raft 副本迁移而不是改配置。
func TestShardPlacementRejectsInvalidInput(t *testing.T) {
	ok := map[NodeID]uint64{"a": 1, "b": 2}
	cases := []struct {
		name     string
		base     uint64
		replicas map[NodeID]uint64
		count    uint64
		rf       int
	}{
		{"起始号为 0", 0, ok, 4, 1},
		{"分片数为 0", 1024, ok, 0, 1},
		{"没有可用副本", 1024, map[NodeID]uint64{"a": 0}, 4, 1},
		{"重复副本号", 1024, map[NodeID]uint64{"a": 1, "b": 1}, 4, 1},
		{"副本因子超出节点数", 1024, ok, 4, 3},
		{"副本因子为负", 1024, ok, 4, -1},
	}
	for _, c := range cases {
		if _, err := newShardPlacement(c.base, c.replicas, c.count, c.rf); err == nil {
			t.Fatalf("%s：期望报错", c.name)
		}
	}
}
