package router

import (
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

/*
跨节点的共享订阅组（MQTT-096、MQTT-116）。

此前组成员只在本节点内选取：订阅者全在别的节点上时，`$share/g/...` 的
消息在本节点被当成"组内没有可投的成员"丢掉，而发布者看不出任何异常——
PUBACK 照常返回，因为本地订阅者确实都投过了。
*/

// TestSharedPrefersLocalMember 本地有在线成员时不走网络（MQTT-096）。
func TestSharedPrefersLocalMember(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	s.Join("g", "telemetry/#", Member{ClientID: "local-1", Node: "a", Online: true})
	s.AdvertiseRemote("b", []cluster.ShareCount{{Share: "g", Filter: "telemetry/#", Online: 5}})

	for i := 0; i < 10; i++ {
		tg, ok := s.Pick("g", "telemetry/#")
		if !ok {
			t.Fatal("组里有成员却选不出目标")
		}
		if tg.Remote || tg.ClientID != "local-1" {
			t.Fatalf("第 %d 次选到了 %+v，本地有在线成员时应当省这一跳", i, tg)
		}
	}
}

// TestSharedFallsBackToRemoteNode 本地没有成员时选一个远端节点。
func TestSharedFallsBackToRemoteNode(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	s.AdvertiseRemote("b", []cluster.ShareCount{{Share: "g", Filter: "telemetry/#", Online: 1}})
	s.AdvertiseRemote("c", []cluster.ShareCount{{Share: "g", Filter: "telemetry/#", Online: 2}})

	// 本节点一个成员都没有：groups 里没有这个键，而匹配仍须命中。
	targets := s.MatchInto(nil, []byte("telemetry/t1"))
	if len(targets) != 1 {
		t.Fatalf("匹配到 %d 个目标，期望 1（订阅者全在别的节点上）", len(targets))
	}
	if !targets[0].Remote || targets[0].ClientID != "" {
		t.Fatalf("目标 = %+v，期望是一个远端节点", targets[0])
	}
	if targets[0].Share != "g" || targets[0].Filter != "telemetry/#" {
		t.Errorf("目标没带组标识：%+v（接收侧据此只投本地成员）", targets[0])
	}

	// 远端节点之间要轮转，而不是永远选同一个。
	seen := map[cluster.NodeID]int{}
	for i := 0; i < 10; i++ {
		tg, ok := s.Pick("g", "telemetry/#")
		if !ok {
			t.Fatal("选不出远端目标")
		}
		seen[tg.Node]++
	}
	if len(seen) != 2 {
		t.Errorf("十次只选到了 %v，远端节点之间没有轮转", seen)
	}
}

// TestSharedRemoteSnapshotReplaces 通告是全量快照。
//
// 增量的后果是丢一条就永久偏离：一个节点的最后一个成员退订之后，
// 别人还会继续往它投，而消息就停在那儿（组内只投一个，所以没人补上）。
func TestSharedRemoteSnapshotReplaces(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	s.AdvertiseRemote("b", []cluster.ShareCount{
		{Share: "g", Filter: "x/#", Online: 1},
		{Share: "g", Filter: "y/#", Online: 1},
	})
	if _, ok := s.Pick("g", "y/#"); !ok {
		t.Fatal("前置条件不成立")
	}
	// 新快照里只剩 x：y 必须立刻消失。
	s.AdvertiseRemote("b", []cluster.ShareCount{{Share: "g", Filter: "x/#", Online: 1}})
	if _, ok := s.Pick("g", "y/#"); ok {
		t.Error("新快照里没有 y，它却还在")
	}
	if _, ok := s.Pick("g", "x/#"); !ok {
		t.Error("新快照里有 x，它却没了")
	}
	// 节点离开：条目清空。
	s.ForgetRemote("b")
	if _, ok := s.Pick("g", "x/#"); ok {
		t.Error("节点已被忘掉，它的组成员却还在")
	}
}

// TestSharedPickLocalDoesNotLeaveNode 收到转来的帧时只在本地成员里选。
//
// 这一条是防环路的那道闸：帧已经由发布侧选定了本节点，再做一次跨节点
// 选取会把它又转出去，而组内只投一个——弹一次语义就破了。
func TestSharedPickLocalDoesNotLeaveNode(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	s.AdvertiseRemote("b", []cluster.ShareCount{{Share: "g", Filter: "t/#", Online: 3}})

	if _, ok := s.PickLocal("g", "t/#"); ok {
		t.Error("本地没有成员，PickLocal 却选出了目标（会把帧又转出去）")
	}
	s.Join("g", "t/#", Member{ClientID: "local-1", Node: "a", Online: true})
	tg, ok := s.PickLocal("g", "t/#")
	if !ok || tg.Remote || tg.ClientID != "local-1" {
		t.Errorf("PickLocal = %+v, %v，期望本地成员", tg, ok)
	}
}

// TestSharedLocalCountsAreSnapshot 通告内容只含本节点的在线成员。
func TestSharedLocalCountsAreSnapshot(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	s.Join("g", "t/#", Member{ClientID: "c1", Node: "a", Online: true})
	s.Join("g", "t/#", Member{ClientID: "c2", Node: "a", Online: true})
	// 离线的持久会话成员不算：别人按这个数判断"往这儿投有人收"。
	s.Join("g", "t/#", Member{ClientID: "c3", Node: "a", Persistent: true, Online: false})
	// 远端通告来的计数不能回流到自己的通告里，否则两个节点互相放大。
	s.AdvertiseRemote("b", []cluster.ShareCount{{Share: "g", Filter: "t/#", Online: 9}})

	got := s.LocalCounts()
	if len(got) != 1 {
		t.Fatalf("通告了 %d 组：%+v", len(got), got)
	}
	if got[0].Online != 2 {
		t.Errorf("在线成员数 = %d，期望 2（离线的与远端的都不算）", got[0].Online)
	}
}

// TestSharedRemotePickDoesNotAllocate 投递路径上选远端节点不得有分配
// （LANG-007）。
//
// 第一版在这里每条消息都重建并排序一次节点列表、还把过滤器从 string
// 转成 []byte——代价随"只在远端有成员的组数"线性放大，而发布是最热的
// 那条路径。现在这两样都在通告时算好（约每秒一次）。
func TestSharedRemotePickDoesNotAllocate(t *testing.T) {
	s := NewSharedGroups("a", topic.DefaultLimits())
	for _, node := range []cluster.NodeID{"b", "c", "d"} {
		s.AdvertiseRemote(node, []cluster.ShareCount{
			{Share: "workers", Filter: "jobs/#", Online: 2},
			{Share: "audit", Filter: "telemetry/#", Online: 1},
		})
	}
	name := []byte("jobs/j1")
	dst := make([]Target, 0, 4)

	// 预热一次，让可能的惰性初始化不计进来。
	dst = s.MatchInto(dst[:0], name)
	if len(dst) != 1 {
		t.Fatalf("前置条件不成立：匹配到 %d 个目标", len(dst))
	}

	got := testing.AllocsPerRun(200, func() {
		dst = s.MatchInto(dst[:0], name)
	})
	if got > 0 {
		t.Errorf("每次匹配分配 %.1f 次，期望 0（LANG-007：发布是热路径）", got)
	}
}
