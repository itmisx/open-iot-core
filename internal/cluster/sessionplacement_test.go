package cluster

import (
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"
)

func TestSessionPlacementRF3IsDeterministicAndBalanced(t *testing.T) {
	replicas := map[NodeID]uint64{
		"n6": 60, "n2": 20, "n4": 40,
		"n1": 10, "n5": 50, "n3": 30,
	}
	p, err := newSessionPlacement(replicas, 12, 3)
	if err != nil {
		t.Fatal(err)
	}
	// map 的遍历顺序不能影响放置；第一个成员同时是目标 leader。
	wantFirst := []uint64{10, 20, 30}
	if got := p.members(shardSessionBase); !slices.Equal(got, wantFirst) {
		t.Fatalf("首分片成员 = %v，期望 %v", got, wantFirst)
	}
	if leader, ok := p.leader(shardSessionBase + 5); !ok || leader != 60 {
		t.Fatalf("第六个分片 leader = %d, %v，期望 60", leader, ok)
	}

	memberships := map[uint64]int{}
	leaders := map[uint64]int{}
	for i := uint64(0); i < 12; i++ {
		shard := shardSessionBase + i
		members := p.members(shard)
		if len(members) != 3 {
			t.Fatalf("分片 %d 有 %d 个副本，期望 3", shard, len(members))
		}
		for _, id := range members {
			memberships[id]++
			if !p.contains(shard, id) {
				t.Fatalf("contains 漏掉分片 %d 的副本 %d", shard, id)
			}
		}
		leader, _ := p.leader(shard)
		leaders[leader]++
	}
	for _, id := range []uint64{10, 20, 30, 40, 50, 60} {
		if memberships[id] != 6 {
			t.Errorf("副本 %d 承载 %d 个分片，期望 6", id, memberships[id])
		}
		if leaders[id] != 2 {
			t.Errorf("副本 %d 领导 %d 个分片，期望 2", id, leaders[id])
		}
	}
}

func TestSessionPlacementRFAllPreservesCurrentLayout(t *testing.T) {
	replicas := map[NodeID]uint64{"c": 3, "a": 1, "b": 2}
	p, err := newSessionPlacement(replicas, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]uint64{{1, 2, 3}, {2, 3, 1}, {3, 1, 2}, {1, 2, 3}}
	for i := range want {
		if got := p.members(shardSessionBase + uint64(i)); !slices.Equal(got, want[i]) {
			t.Fatalf("分片 %d 成员 = %v，期望 %v", i, got, want[i])
		}
	}
	if got := p.leaders(); !slices.Equal(got, []uint64{1, 2, 3, 1}) {
		t.Fatalf("leader 布局 = %v", got)
	}
}

func TestShardGroupExposesPlacementNodes(t *testing.T) {
	replicas := map[NodeID]uint64{"c": 30, "a": 10, "b": 20}
	p, err := newSessionPlacement(replicas, 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	s := &ShardGroup{
		count: 8, placement: p,
		replicaNode: map[uint64]NodeID{10: "a", 20: "b", 30: "c"},
	}
	clientID := "device-42"
	shard := s.ShardOf(clientID)
	members := p.members(shard)
	want := make([]NodeID, len(members))
	for i, id := range members {
		want[i] = s.replicaNode[id]
	}
	if got := s.ReplicaNodes(clientID); !slices.Equal(got, want) {
		t.Fatalf("ReplicaNodes = %v，期望 %v", got, want)
	}
	if got, ok := s.PreferredNode(clientID); !ok || got != want[0] {
		t.Fatalf("PreferredNode = %q, %v，期望 %q", got, ok, want[0])
	}
}

func TestIndependentShardPlacementUsesConfiguredBase(t *testing.T) {
	replicas := map[NodeID]uint64{"a": 10, "b": 20, "c": 30}
	p, err := newShardPlacement(2, replicas, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.leader(shardSessionBase); ok {
		t.Fatal("shadow placement must not accept a session shard id")
	}
	if got, ok := p.leader(2); !ok || got != 10 {
		t.Fatalf("first independent shard leader = %d, %v", got, ok)
	}
	s := &ShardGroup{base: 2, count: 4, placement: p}
	for _, shard := range s.ShardIDs() {
		if shard < 2 || shard >= 6 {
			t.Fatalf("configured range leaked: %v", s.ShardIDs())
		}
	}
	if shard := s.ShardOf("thing-1"); shard < 2 || shard >= 6 {
		t.Fatalf("hashed shard outside configured range: %d", shard)
	}
}

func TestTLSGroupRoutesMultipleShardGroupsWithoutOverlap(t *testing.T) {
	g := &TLSGroup{}
	sessions := &ShardGroup{base: shardSessionBase, count: 2}
	shadows := &ShardGroup{base: 2, count: 3}
	if err := g.registerShardGroup(sessions); err != nil {
		t.Fatal(err)
	}
	if err := g.registerShardGroup(shadows); err != nil {
		t.Fatal(err)
	}
	if got := g.shardGroup(16); got != sessions {
		t.Fatal("session shard routed to wrong group")
	}
	if got := g.shardGroup(4); got != shadows {
		t.Fatal("shadow shard routed to wrong group")
	}
	if err := g.registerShardGroup(&ShardGroup{base: 4, count: 2}); err == nil {
		t.Fatal("overlapping shard ranges must be rejected")
	}
	g.unregisterShardGroup(shadows)
	if got := g.shardGroup(4); got != nil {
		t.Fatal("closed group remains routable")
	}
	if got := g.shardGroup(16); got != sessions {
		t.Fatal("closing one group removed another")
	}
}

func TestShardGroupImplementsStablePartitioner(t *testing.T) {
	replicas := map[NodeID]uint64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
	p, err := newShardPlacement(2, replicas, 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	g := &ShardGroup{
		base: 2, count: 8, replica: 2, placement: p,
		replicaNode: map[uint64]NodeID{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
	}
	var part Partitioner = g
	if part.Count() != 8 || part.Shard("thing") >= 8 {
		t.Fatalf("partition shape count=%d shard=%d", part.Count(), part.Shard("thing"))
	}
	owner := part.Owner("thing")
	if owner == "" || part.IsLocal("thing") != g.HasLocalReplica("thing") {
		t.Fatalf("owner=%q local=%v", owner, part.IsLocal("thing"))
	}
	// replica 2 是 shard 2 的二级副本（首选是 replica 1），仍应当本地处理。
	var secondaryKey string
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("secondary-%d", i)
		if g.ShardOf(key) == 2 {
			secondaryKey = key
			break
		}
	}
	if secondaryKey == "" || g.Owner(secondaryKey) != "a" || !g.IsLocal(secondaryKey) {
		t.Fatalf("secondary replica not local: key=%q owner=%q local=%v",
			secondaryKey, g.Owner(secondaryKey), g.IsLocal(secondaryKey))
	}
}

func TestPlacementFingerprintIsStableAndNodeSpecific(t *testing.T) {
	replicas := map[NodeID]uint64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
	p1, err := newShardPlacement(1024, replicas, 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := newShardPlacement(1024, map[NodeID]uint64{
		"e": 5, "c": 3, "a": 1, "d": 4, "b": 2,
	}, 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	a := (&ShardGroup{base: 1024, count: 8, replica: 1, placement: p1}).PlacementFingerprint()
	b := (&ShardGroup{base: 1024, count: 8, replica: 1, placement: p2}).PlacementFingerprint()
	c := (&ShardGroup{base: 1024, count: 8, replica: 2, placement: p2}).PlacementFingerprint()
	if a == "" || a != b {
		t.Fatalf("same placement fingerprint changed: %q != %q", a, b)
	}
	if a == c {
		t.Fatal("different local replicas shared a prune fingerprint")
	}
}

func TestShadowWireSeparatesReadRetryFromWriteAmbiguity(t *testing.T) {
	g := &TLSGroup{}
	g.SetShadowHandlers(
		func(raw []byte) ([]byte, error) { return append([]byte("w:"), raw...), nil },
		func(raw []byte) ([]byte, error) { return append([]byte("r:"), raw...), nil },
	)
	write := g.dispatch(wireMsg{Kind: wireShadowWriteKind, Data: []byte("x")})
	read := g.dispatch(wireMsg{Kind: wireShadowReadKind, Data: []byte("y")})
	if !write.OK || string(write.Data) != "w:x" || !read.OK || string(read.Data) != "r:y" {
		t.Fatalf("write=%+v read=%+v", write, read)
	}
	if retryableKind(wireShadowWriteKind) {
		t.Fatal("ambiguous shadow write became transport-retryable")
	}
	if !retryableKind(wireShadowReadKind) {
		t.Fatal("idempotent shadow read is not retryable")
	}
	g.Drain()
	write = g.dispatch(wireMsg{Kind: wireShadowWriteKind, Data: []byte("z")})
	read = g.dispatch(wireMsg{Kind: wireShadowReadKind, Data: []byte("z")})
	if write.Err != ErrDraining.Error() {
		t.Fatalf("drain期间影子写 = %+v，期望ErrDraining", write)
	}
	if !read.OK || string(read.Data) != "r:z" {
		t.Fatalf("drain期间只读影子RPC不应被拒: %+v", read)
	}
}

func TestShardLeaderCacheInvalidatesOnlyMatchingTarget(t *testing.T) {
	p, err := newShardPlacement(2, map[NodeID]uint64{
		"a": 1, "b": 2, "c": 3, "d": 4, "e": 5,
	}, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	s := &ShardGroup{
		base: 2, count: 4, replica: 4, placement: p,
		replicaNode: map[uint64]NodeID{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"},
		leaderCache: make([]atomic.Uint64, 4),
	}
	thing := "cache-target"
	shard := s.ShardOf(thing)
	s.rememberLeader(shard, p.members(shard)[1])
	leader, ok := s.LeaderNode(thing)
	if !ok || leader == "" {
		t.Fatalf("缓存leader = %q, %v", leader, ok)
	}
	s.ForgetLeader(thing, "not-the-leader")
	if got, ok := s.LeaderNode(thing); !ok || got != leader {
		t.Fatalf("无关目标清掉leader缓存: %q, %v", got, ok)
	}
	s.ForgetLeader(thing, leader)
	off := shard - s.shardBase()
	if got := s.leaderCache[off].Load(); got != 0 {
		t.Fatalf("匹配目标未清掉leader缓存: %d", got)
	}
}

func TestDrainingNodeDoesNotAdvertiseStaleShardLeader(t *testing.T) {
	g := &TLSGroup{}
	g.Drain()
	resp := g.dispatch(wireMsg{Kind: wireShardStatusKind, Shard: 2})
	if resp.Err != ErrDraining.Error() {
		t.Fatalf("drain节点的shard状态 = %+v，期望ErrDraining", resp)
	}
}

func TestSessionPlacementRejectsInvalidInputs(t *testing.T) {
	if _, err := newSessionPlacement(nil, 8, 3); err == nil {
		t.Fatal("空成员表应被拒绝")
	}
	if _, err := newSessionPlacement(map[NodeID]uint64{"a": 1}, 0, 1); err == nil {
		t.Fatal("零分片应被拒绝")
	}
	if _, err := newSessionPlacement(map[NodeID]uint64{"a": 1, "b": 1}, 8, 1); err == nil {
		t.Fatal("重复副本号应被拒绝")
	}
	if _, err := newSessionPlacement(map[NodeID]uint64{"a": 1, "b": 2}, 8, 3); err == nil {
		t.Fatal("RF 大于节点数应被拒绝")
	}
}

func TestSessionLayoutIsImmutableAndRejectsDirectRFMigration(t *testing.T) {
	replicas := map[NodeID]uint64{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
	all := makeSessionLayout(8, 0, replicas)
	dir := t.TempDir()
	if err := ensureSessionLayout(dir, all, false); err != nil {
		t.Fatalf("首次写布局: %v", err)
	}
	if err := ensureSessionLayout(dir, all, true); err != nil {
		t.Fatalf("相同布局重启: %v", err)
	}
	if _, err := os.Stat(dir + "/" + sessionLayoutName); err != nil {
		t.Fatalf("布局文件不存在: %v", err)
	}
	if err := ensureSessionLayout(dir, makeSessionLayout(8, 3, replicas), true); err == nil {
		t.Fatal("已有 RF=all 布局直接切 RF=3 应被拒绝")
	}
	if err := ensureSessionLayout(dir, makeSessionLayout(16, 0, replicas), true); err == nil {
		t.Fatal("已有布局直接改分片数应被拒绝")
	}

	legacy := t.TempDir()
	if err := ensureSessionLayout(legacy, makeSessionLayout(8, 3, replicas), true); err == nil {
		t.Fatal("无布局清单但已有日志时直接启用 RF=3 应被拒绝")
	}
	if err := ensureSessionLayout(t.TempDir(), makeSessionLayout(8, 3, replicas), false); err != nil {
		t.Fatalf("新集群启用 RF=3: %v", err)
	}
}
