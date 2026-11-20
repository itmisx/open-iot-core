package cluster

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// dragonGroup 的接口面验收。
//
// 单节点仍然走真的共识路径（与 ARCH-057「单机也走一副本组」同一个理由）：
// 提案要进日志、要提交、要经状态机，只是多数派恰好是一个人。这条用例
// 要钉的是**换库之后上层看到的东西没变**——十个方法与 RaftGroup 一一对应，
// 身份映射两个方向都对得上。
func TestDragonGroupSurface(t *testing.T) {
	addr := freeLocalAddr(t)
	cold := &fakeCold{}
	g, err := newDragonGroup(DragonOptions{
		ID:           "node-a",
		Dir:          filepath.Join(t.TempDir(), "nh"),
		Replicas:     map[NodeID]uint64{"node-a": 1},
		Peers:        map[NodeID]string{"node-a": addr},
		Apply:        cold.apply,
		AppliedIndex: cold.index,
		SnapshotTo:   cold.snapshot,
		RestoreFrom:  cold.restore,
		Transport: &dragonTransportFactory{
			dial: func(string) (net.Conn, error) {
				return nil, errors.New("单节点不该拨号")
			},
		},
		Bootstrap:    true,
		RTT:          50 * time.Millisecond,
		ApplyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("建组失败: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// 等它当选。单节点也要走一次选举，不是立刻就是 leader。
	deadline := time.Now().Add(15 * time.Second)
	for !g.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !g.IsLeader() {
		t.Fatal("15 秒内没有当选 leader")
	}
	if got := g.LeaderID(); got != "node-a" {
		t.Fatalf("LeaderID = %q，期望 node-a；副本号到节点 ID 的反查没接对", got)
	}

	if err := g.Propose([]byte("x")); err != nil {
		t.Fatalf("提案失败: %v", err)
	}
	if got := cold.seen(); len(got) != 1 || got[0] != "x" {
		t.Fatalf("状态机收到 %v，期望 [x]", got)
	}
	if g.AppliedIndex() == 0 {
		t.Error("AppliedIndex 为 0：它该取自状态机（与视图 revision 一致）")
	}

	// Barrier 走 read index。它要是没接对（Lookup 把查询拒了），
	// 这里会拿到那条"读取不经状态机"的错误。
	if err := g.Barrier(10 * time.Second); err != nil {
		t.Fatalf("Barrier 失败: %v", err)
	}

	members := g.Members()
	if len(members) != 1 || members[0].ID != "node-a" || members[0].Addr != addr {
		t.Fatalf("Members = %+v，期望一条 node-a/%s", members, addr)
	}

	if err := g.Snapshot(); err != nil {
		t.Fatalf("主动快照失败: %v", err)
	}

	// 非 leader 才能做的事在单节点上拿不到，但"没有副本号的节点加不进来"
	// 这条可以验——它是配置写错时的那条路。
	if err := g.AddMember("node-zzz", "127.0.0.1:1", 0); err == nil {
		t.Error("没有副本号的节点不该能加进来")
	}
}

// TestDragonGroupRejectsMissingReplicaID 本节点不在映射里要直接失败。
//
// 不拦的后果是它用一个零副本号去启动，而共识库把 0 当成"未指定"——
// 症状是组起不来或身份错乱，都不指向那张映射表。
func TestDragonGroupRejectsMissingReplicaID(t *testing.T) {
	_, err := newDragonGroup(DragonOptions{
		ID:       "node-a",
		Dir:      filepath.Join(t.TempDir(), "nh"),
		Replicas: map[NodeID]uint64{"node-b": 2},
		Peers:    map[NodeID]string{"node-a": freeLocalAddr(t)},
	})
	if err == nil {
		t.Fatal("本节点没有副本号时应当直接失败")
	}
}
