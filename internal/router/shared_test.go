package router

import (
	"sort"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

func newShared() *SharedGroups {
	return NewSharedGroups("node-1", topic.DefaultLimits())
}

func online(id string) Member {
	return Member{ClientID: id, Node: "node-1", QoS: 1, Online: true}
}

func TestSharedJoinLeave(t *testing.T) {
	s := newShared()
	s.Join("grp", "a/b", online("c1"))
	s.Join("grp", "a/b", online("c2"))
	if s.Groups() != 1 {
		t.Errorf("组数 = %d，期望 1", s.Groups())
	}
	if n := len(s.Members("grp", "a/b")); n != 2 {
		t.Errorf("成员数 = %d，期望 2", n)
	}
	// 重复加入更新属性而非新增
	s.Join("grp", "a/b", Member{ClientID: "c1", Node: "node-1", QoS: 2, Online: true})
	ms := s.Members("grp", "a/b")
	if len(ms) != 2 {
		t.Fatalf("重复加入应更新而非新增，成员数 = %d", len(ms))
	}
	for _, m := range ms {
		if m.ClientID == "c1" && m.QoS != 2 {
			t.Errorf("c1 的 QoS = %d，期望 2", m.QoS)
		}
	}

	if !s.Leave("grp", "a/b", "c1") {
		t.Error("离组应成功")
	}
	if s.Leave("grp", "a/b", "c1") {
		t.Error("重复离组应返回 false")
	}
	if s.Leave("other", "a/b", "c2") {
		t.Error("不存在的组应返回 false")
	}
	// 最后一个成员离组后组被删除
	s.Leave("grp", "a/b", "c2")
	if s.Groups() != 0 {
		t.Errorf("空组应被删除，组数 = %d", s.Groups())
	}
	if s.Members("grp", "a/b") != nil {
		t.Error("不存在的组成员应为 nil")
	}
}

// TestSharedSameNameDifferentFilterAreDistinctGroups 固定住 MQTT-4.8.2：
// 同一 {ShareName}/{TopicFilter} 才属同一组。
func TestSharedSameNameDifferentFilterAreDistinctGroups(t *testing.T) {
	s := newShared()
	s.Join("grp", "a/b", online("c1"))
	s.Join("grp", "c/d", online("c2"))
	if s.Groups() != 2 {
		t.Errorf("组名相同但过滤器不同应是两个组，组数 = %d", s.Groups())
	}
}

// TestSharedPickIsLoadBalanced 验证组内负载均衡。
func TestSharedPickIsLoadBalanced(t *testing.T) {
	s := newShared()
	for _, id := range []string{"c1", "c2", "c3"} {
		s.Join("grp", "a/b", online(id))
	}
	hits := map[string]int{}
	for i := 0; i < 30; i++ {
		tg, ok := s.Pick("grp", "a/b")
		if !ok {
			t.Fatal("应能选出目标")
		}
		hits[tg.ClientID]++
	}
	if len(hits) != 3 {
		t.Errorf("30 次投递只命中 %d 个成员，负载未均衡: %v", len(hits), hits)
	}
	for id, n := range hits {
		if n != 10 {
			t.Errorf("%s 收到 %d 次，轮转应均分为 10 次", id, n)
		}
	}
	if _, ok := s.Pick("nope", "a/b"); ok {
		t.Error("不存在的组应返回 false")
	}
}

func TestSharedPickSkipsOffline(t *testing.T) {
	s := newShared()
	s.Join("grp", "a/b", online("c1"))
	s.Join("grp", "a/b", Member{ClientID: "c2", Node: "node-1", Online: false})

	for i := 0; i < 10; i++ {
		tg, ok := s.Pick("grp", "a/b")
		if !ok || tg.ClientID != "c1" {
			t.Fatalf("应只投给在线成员，得到 %+v ok=%v", tg, ok)
		}
	}
	// 上线后参与轮转
	if !s.SetOnline("grp", "a/b", "c2", true) {
		t.Fatal("设置在线状态应成功")
	}
	hits := map[string]int{}
	for i := 0; i < 10; i++ {
		tg, _ := s.Pick("grp", "a/b")
		hits[tg.ClientID]++
	}
	if len(hits) != 2 {
		t.Errorf("上线后应参与轮转，命中 %v", hits)
	}
	if s.SetOnline("nope", "a/b", "c2", true) || s.SetOnline("grp", "a/b", "nope", true) {
		t.Error("不存在的组或成员应返回 false")
	}
}

// TestSharedQueuesWhenAllOffline 固定住 PRD MQTT-095：
// 组内有持久会话成员时，全部离线应排队而非丢弃。
func TestSharedQueuesWhenAllOffline(t *testing.T) {
	s := newShared()
	s.Join("grp", "a/b", Member{ClientID: "c1", Node: "node-1", Persistent: true, Online: false})
	tg, ok := s.Pick("grp", "a/b")
	if !ok || !tg.Queue {
		t.Errorf("有持久会话成员时应排队，得到 %+v ok=%v", tg, ok)
	}
	if tg.ClientID != "" {
		t.Errorf("排队目标不应指向具体客户端，得到 %q", tg.ClientID)
	}

	// 全是清洁会话且离线则丢弃
	s2 := newShared()
	s2.Join("grp", "a/b", Member{ClientID: "c1", Node: "node-1", Persistent: false, Online: false})
	if _, ok := s2.Pick("grp", "a/b"); ok {
		t.Error("无持久会话成员时应丢弃")
	}
}

func TestSharedPickPrefersLocalNode(t *testing.T) {
	s := NewSharedGroups("node-1", topic.DefaultLimits())
	s.Join("grp", "a/b", Member{ClientID: "remote", Node: "node-2", Online: true})
	s.Join("grp", "a/b", Member{ClientID: "local", Node: "node-1", Online: true})
	// 集群下优先本节点成员以省一跳（PRD MQTT-096）
	for i := 0; i < 10; i++ {
		tg, ok := s.Pick("grp", "a/b")
		if !ok || tg.ClientID != "local" {
			t.Fatalf("应优先选择本节点成员，得到 %+v", tg)
		}
	}
}

// TestSharedWildcardGroupsEachGetCopy 固定住 PRD MQTT-093：
// 多个匹配组各投一份副本——组之间广播，组内负载均衡。
func TestSharedWildcardGroupsEachGetCopy(t *testing.T) {
	s := newShared()
	s.Join("consumer1", "sports/tennis", online("c1"))
	s.Join("consumer1", "sports/tennis", online("c2"))
	s.Join("consumer1", "sports/#", online("c3"))
	s.Join("consumer2", "sports/tennis", online("c4"))

	targets := s.MatchInto(nil, []byte("sports/tennis"))
	if len(targets) != 3 {
		t.Fatalf("应有 3 个组各投一份，得到 %d: %+v", len(targets), targets)
	}
	seen := map[string]bool{}
	for _, tg := range targets {
		seen[tg.Share+"/"+tg.Filter] = true
	}
	for _, want := range []string{
		"consumer1/sports/tennis", "consumer1/sports/#", "consumer2/sports/tennis",
	} {
		if !seen[want] {
			t.Errorf("缺少组 %q 的投递目标", want)
		}
	}
	// 不匹配的主题无目标
	if got := s.MatchInto(nil, []byte("music/jazz")); len(got) != 0 {
		t.Errorf("不匹配应无目标，得到 %+v", got)
	}
}

func TestSharedLeaveAll(t *testing.T) {
	s := newShared()
	s.Join("g1", "a/b", online("c1"))
	s.Join("g2", "c/d", online("c1"))
	s.Join("g1", "a/b", online("c2"))

	if n := s.LeaveAll("c1"); n != 2 {
		t.Errorf("移除 %d 条，期望 2", n)
	}
	if n := s.LeaveAll("c1"); n != 0 {
		t.Errorf("重复移除应返回 0，得到 %d", n)
	}
	if s.Groups() != 1 {
		t.Errorf("组数 = %d，期望 1（g2 应已清空删除）", s.Groups())
	}
	if ms := s.Members("g1", "a/b"); len(ms) != 1 || ms[0].ClientID != "c2" {
		t.Errorf("剩余成员 = %+v", ms)
	}
}

func TestSharedRoundRobinIndexStaysValidAfterLeave(t *testing.T) {
	// 轮转下标在成员减少后必须被钳住，否则会越界
	s := newShared()
	for _, id := range []string{"c1", "c2", "c3"} {
		s.Join("grp", "a/b", online(id))
	}
	for i := 0; i < 3; i++ {
		s.Pick("grp", "a/b")
	}
	s.Leave("grp", "a/b", "c3")
	s.Leave("grp", "a/b", "c2")
	got := []string{}
	for i := 0; i < 3; i++ {
		tg, ok := s.Pick("grp", "a/b")
		if !ok {
			t.Fatal("应能选出目标")
		}
		got = append(got, tg.ClientID)
	}
	sort.Strings(got)
	if got[0] != "c1" {
		t.Errorf("剩余成员应为 c1，得到 %v", got)
	}
}

func TestSharedIntegrationWithRouter(t *testing.T) {
	r := newTestRouter(false)
	for _, id := range []string{"w1", "w2"} {
		if err := r.Subscribe(Subscription{
			ClientID: id, Filter: "$share/workers/jobs/#", QoS: 1, Persistent: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 共享订阅不入订阅树
	local, _ := r.Route([]byte("jobs/build"))
	if len(local) != 0 {
		t.Errorf("共享订阅不应出现在普通路由结果中，得到 %v", clientsOf(local))
	}
	// 走 RouteShared
	targets := r.RouteShared([]byte("jobs/build"))
	if len(targets) != 1 {
		t.Fatalf("应有 1 个共享组目标，得到 %+v", targets)
	}
	if targets[0].ClientID != "w1" && targets[0].ClientID != "w2" {
		t.Errorf("目标客户端 = %q", targets[0].ClientID)
	}
	// 退订经 SharedGroups
	if !r.Unsubscribe("w1", "$share/workers/jobs/#") {
		t.Error("共享订阅退订应成功")
	}
	if n := len(r.Shared().Members("workers", "jobs/#")); n != 1 {
		t.Errorf("成员数 = %d，期望 1", n)
	}
	// RemoveClient 同时清理共享订阅
	if n := r.RemoveClient("w2"); n != 1 {
		t.Errorf("移除 %d 条，期望 1", n)
	}
	if r.Shared().Groups() != 0 {
		t.Errorf("组应已清空，组数 = %d", r.Shared().Groups())
	}

	buf := make([]Target, 0, 4)
	if got := r.RouteSharedInto(buf[:0], []byte("jobs/build")); len(got) != 0 {
		t.Errorf("无组时应无目标，得到 %+v", got)
	}
}
