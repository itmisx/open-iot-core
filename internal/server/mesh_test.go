package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/config"
)

/*
消息面接线的用例（CLU-075 至 CLU-080、CLU-101、MQTT-116）。

这几条盯的都是"接线断了而语法与类型都对"的那种缺口：通告收下来却没
喂给路由器、帧收下来却没人投、节点走了却没人忘掉它——三者的现象都是
消息静默消失，而没有任何报错。
*/

// TestLateMeshCarriesCallbackAcrossBind 绑定之前注册的接收回调要被转交。
//
// broker 在装配期（New）就注册回调，而真正的 mesh 要等复制组起来才有
// （复制组必须在注册表之后才能起，因为装快照要重建热视图）。丢掉这个
// 回调的后果是跨节点来的帧永远没人投，而现象与"策略漏了 iot:Receive"
// 一模一样——排查方向却完全不同。
func TestLateMeshIsNoopBeforeBind(t *testing.T) {
	m := &lateMesh{self: "n1"}
	m.OnReceive(func(cluster.NodeID, []cluster.Frame) {
		t.Error("绑定之前不该有帧到达")
	})

	// 绑定之前：转发与查询都是空操作，但不得报错——那段时间里
	// Route 算出的远端集合恒为空，转发分支根本不会进入。
	if err := m.Forward(context.Background(), []cluster.NodeID{"n2"}, []cluster.Frame{{}}); err != nil {
		t.Errorf("绑定前转发报错: %v", err)
	}
	if res, err := m.Query(context.Background(), []cluster.NodeID{"n2"}, cluster.AdminQuery{}); err != nil || res != nil {
		t.Errorf("绑定前查询 = %v, %v", res, err)
	}
	if m.Self() != "n1" {
		t.Errorf("Self = %q，broker 用它判断这一跳是不是自己", m.Self())
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close 报错: %v", err)
	}
}

// TestLateMeshBoundAfterGroupStarts 复制组起来之后包装要指向真正的传输。
//
// 不指向的后果是 broker 手里永远是那个空操作包装：跨节点转发静默失效，
// 而本地订阅者照常收到消息，所以单机看起来一切正常。
func TestLateMeshBoundAfterGroupStarts(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	s := r.s

	if s.lateMesh == nil {
		t.Fatal("集群模式下 broker 没有拿到消息面包装")
	}
	if got := s.lateMesh.get(); got == nil || got != s.mesh {
		t.Fatalf("包装指向 %p，真正的传输是 %p", got, s.mesh)
	}
	// 投一帧进去：回调已经转交给真正的传输，这里不该 panic，
	// 也不该因为没有订阅者而报错。
	s.mesh.Deliver("n2", []cluster.Frame{{
		Version: cluster.FrameVersion, Topic: "telemetry/x", Origin: "n2",
	}})
}

// TestOnMeshFeedsRouterAndOwnership 通告收下来要喂给路由器与归属表。
func TestOnMeshFeedsRouterAndOwnership(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	s := r.s

	s.onMesh("n2", cluster.MeshPayload{
		Filters: []string{"telemetry/#"},
		Things:  []string{"lamp-远端"},
		Shares:  []cluster.ShareCount{{Share: "workers", Filter: "jobs/#", Online: 2}},
	})

	// 1) 兴趣：发布匹配主题时 n2 要出现在远端集合里。
	_, remote := s.rt.Route([]byte("telemetry/site-1"))
	found := false
	for _, node := range remote {
		if node == "n2" {
			found = true
		}
	}
	if !found {
		t.Errorf("通告过的通配符订阅没进兴趣表，远端集合 = %v", remote)
	}

	// 2) 设备归属：设备域主题要定位到 n2。
	if s.remoteThings != nil {
		if node, ok := s.remoteThings.Locate([]byte("lamp-远端")); !ok || node != "n2" {
			t.Errorf("设备归属 = %q, %v，期望 n2", node, ok)
		}
	}

	// 3) 共享订阅组：本节点没有成员，必须选到远端节点（MQTT-116）。
	tg, ok := s.rt.RouteShared([]byte("jobs/j1")), false
	for _, cand := range tg {
		if cand.Remote && cand.Node == "n2" && cand.Share == "workers" {
			ok = true
		}
	}
	if !ok {
		t.Errorf("共享订阅组没有选到远端节点：%+v", tg)
	}
}

func TestOnMeshThingDeltaDetectsGapAndRequestsSnapshot(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	s := r.s

	s.onMesh("n2", cluster.MeshPayload{Things: []string{"legacy"}})
	s.thingSyncMu.Lock()
	legacy := s.thingLegacyPeers["n2"]
	s.thingSyncMu.Unlock()
	if !legacy {
		t.Fatal("旧格式对端没有进入滚动升级兼容模式")
	}

	s.onMesh("n2", cluster.MeshPayload{
		ThingEpoch: 7, ThingVersion: 1, ThingSnapshot: true,
		Things: []string{"lamp-1"},
	})
	s.thingSyncMu.Lock()
	legacy = s.thingLegacyPeers["n2"]
	s.thingSyncMu.Unlock()
	if legacy {
		t.Fatal("对端升级到版本增量后仍停留在全量兼容模式")
	}
	s.onMesh("n2", cluster.MeshPayload{
		ThingEpoch: 7, ThingFrom: 1, ThingVersion: 2,
		ThingChanges: []cluster.ThingChange{
			{Name: "lamp-1", Present: false},
			{Name: "lamp-2", Present: true},
		},
	})
	if _, ok := s.remoteThings.Locate([]byte("lamp-1")); ok {
		t.Fatal("连续增量没有删除旧设备")
	}
	if node, ok := s.remoteThings.Locate([]byte("lamp-2")); !ok || node != "n2" {
		t.Fatalf("连续增量没有加入新设备: node=%q ok=%v", node, ok)
	}

	// from=1 落后于本地水位 2，必须拒绝并请求快照，不能破坏旧视图。
	s.onMesh("n2", cluster.MeshPayload{
		ThingEpoch: 7, ThingFrom: 1, ThingVersion: 3,
		ThingChanges: []cluster.ThingChange{{Name: "bad", Present: true}},
	})
	if !s.remoteThings.NeedsSync("n2") {
		t.Fatal("版本断档后没有登记快照请求")
	}
	if _, ok := s.remoteThings.Locate([]byte("bad")); ok {
		t.Fatal("版本断档的增量被应用了")
	}

	s.onMesh("n2", cluster.MeshPayload{ThingSync: true})
	s.thingSyncMu.Lock()
	requested := s.thingSyncRequests["n2"]
	s.thingSyncMu.Unlock()
	if !requested {
		t.Fatal("对端的快照请求没有交给 gossip 循环")
	}
}

// TestForgetDepartedNodesDropsGossip 不再是成员的节点要被忘掉（CLU-101）。
//
// 不忘的三个后果都不报错：往不在了的节点转发、它承载的设备的消息被转
// 过去后消失、共享订阅组里轮到它的消息没人补投（组内只投一个）。
func TestForgetDepartedNodesDropsGossip(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	s := r.s

	s.onMesh("ghost", cluster.MeshPayload{
		Filters: []string{"telemetry/#"},
		Things:  []string{"lamp-ghost"},
		Shares:  []cluster.ShareCount{{Share: "workers", Filter: "jobs/#", Online: 1}},
	})
	if _, remote := s.rt.Route([]byte("telemetry/x")); len(remote) == 0 {
		t.Fatal("前置条件不成立：通告没有进兴趣表")
	}

	// ghost 不在成员列表里（这是个单节点集群），所以一轮回收就该清掉它。
	s.forgetDepartedNodes()

	if _, remote := s.rt.Route([]byte("telemetry/x")); len(remote) != 0 {
		t.Errorf("离开的节点还在兴趣表里：%v", remote)
	}
	if s.remoteThings != nil {
		if node, ok := s.remoteThings.Locate([]byte("lamp-ghost")); ok {
			t.Errorf("离开的节点还在归属表里：%q", node)
		}
	}
	for _, tg := range s.rt.RouteShared([]byte("jobs/j1")) {
		if tg.Remote && tg.Node == "ghost" {
			t.Error("离开的节点还在共享订阅组里——轮到它的消息会没人补投")
		}
	}
}

// TestForgetDepartedNodesKeepsMembers 成员列表读不出来时什么都不做。
//
// 空列表当成"所有人都走了"会在一次读取失败之后清空全部路由状态，
// 而那比多留一会儿陈旧条目坏得多。
func TestForgetDepartedNodesKeepsMembers(t *testing.T) {
	r := newTestRig(t, func(cfg *config.Config) { clusterConfig(t, cfg, "n1") })
	r.start()
	s := r.s

	// 本节点自己是成员：通告过的"自己"不该被清掉。
	s.onMesh(cluster.NodeID(s.mf.NodeID), cluster.MeshPayload{Filters: []string{"keep/#"}})
	s.forgetDepartedNodes()
	s.gossipMu.Lock()
	_, kept := s.gossipSeen[cluster.NodeID(s.mf.NodeID)]
	s.gossipMu.Unlock()
	if !kept {
		t.Error("本节点自己被当成离开的节点清掉了")
	}
}

// TestFanoutTargetsSkipsFinishedNodes 续页只问游标里还有位置的节点
// （ARCH-040）。
//
// 一个上一页已经读完的节点在续页里没有游标，若仍然去问它，它会从自己
// 的头部重新列一遍——于是同一批连接在第二页又出现一次。这类重复在
// 聚合分页里很难被看出来，因为每个节点各自都是对的。
func TestFanoutTargetsSkipsFinishedNodes(t *testing.T) {
	all := []cluster.NodeID{"a", "b", "c"}

	// 首页：问所有人，游标表是空的。
	if got := fanoutTargets(all, map[cluster.NodeID]string{}, true); len(got) != 3 {
		t.Errorf("首页问了 %v，期望全部三个节点", got)
	}

	// 续页：只有 b 还有位置。
	got := fanoutTargets(all, map[cluster.NodeID]string{"b": "cursor-b"}, false)
	if len(got) != 1 || got[0] != "b" {
		t.Errorf("续页问了 %v，期望只有 b", got)
	}

	// 空串游标等于"读完了"，不该再问。
	got = fanoutTargets(all, map[cluster.NodeID]string{"b": ""}, false)
	if len(got) != 0 {
		t.Errorf("游标为空串的节点仍被问了：%v", got)
	}
}

// TestMergeConnResultsMarksPartialAndKeepsCursor 单节点失败是部分失败，
// 且它的游标要留着（ARCH-041、ARCH-040）。
func TestMergeConnResultsMarksPartialAndKeepsCursor(t *testing.T) {
	rowA, err := json.Marshal(broker.ConnSnapshot{ClientID: "z-last"})
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := json.Marshal(broker.ConnSnapshot{ClientID: "a-first"})
	if err != nil {
		t.Fatal(err)
	}
	cursor := map[cluster.NodeID]string{"a": "cur-a", "b": "cur-b", "c": "cur-c"}
	results := []cluster.AdminResult{
		{Node: "a", Items: []any{rowA}, NextCursor: "next-a"},
		{Node: "b", Items: []any{rowB}},
		{Node: "c", Err: errors.New("不可达")},
	}

	conns, nextPer, partial, err := mergeConnResults(results, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 || conns[0].ClientID != "a-first" {
		t.Errorf("合并结果 = %+v，期望按 clientId 排序的两行", conns)
	}
	if len(partial) != 1 || partial[0] != "c" {
		t.Errorf("部分失败 = %v，期望只有 c", partial)
	}
	if nextPer["a"] != "next-a" {
		t.Errorf("a 的续读位置 = %q", nextPer["a"])
	}
	if _, ok := nextPer["b"]; ok {
		t.Error("b 已经读完，不该再有游标——否则下一页还会去问它")
	}
	if nextPer["c"] != "cur-c" {
		t.Errorf("失败节点 c 的游标 = %q，期望保留原值（清掉会让它重复一批行）", nextPer["c"])
	}
}

// TestMergeConnResultsRejectsBadRow 行解不开要报错，不得当成空结果。
//
// 当成空结果的后果是"那个节点上的连接一个都不显示"，而列表看起来是完整的。
func TestMergeConnResultsRejectsBadRow(t *testing.T) {
	results := []cluster.AdminResult{{Node: "a", Items: []any{[]byte("这不是 JSON")}}}
	if _, _, _, err := mergeConnResults(results, nil); err == nil {
		t.Error("解不开的行被静默忽略了")
	}
	results = []cluster.AdminResult{{Node: "a", Items: []any{42}}}
	if _, _, _, err := mergeConnResults(results, nil); err == nil {
		t.Error("类型不对的行被静默忽略了")
	}
}
