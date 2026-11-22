package meta

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"
)

/*
多副本元数据存储（CLU-064 至 CLU-074）。

这些用例原先架在仓库自写的那套单组 Raft（cluster.MemNet）上。那套实现
已经换成 hashicorp/raft，而本包要验的东西与共识实现无关：同一条已提交
日志喂给三个状态机之后，热视图、冷层与 revision 必须一致。所以这里用
一条"共享日志"的假组，它按序号把命令推给每个节点——共识本身的用例
（选举、故障转移、快照追平）在 internal/cluster 那边。
*/

// fanoutLog 是一条共享日志：提交一条就按序号推给所有未隔离的节点。
type fanoutLog struct {
	mu       sync.Mutex
	index    uint64
	members  map[string]*ReplicatedStore
	isolated map[string]bool
	// fail 非空时 Propose 直接失败，用来模拟丢掉多数派。
	fail error
}

func newFanoutLog() *fanoutLog {
	return &fanoutLog{members: map[string]*ReplicatedStore{}, isolated: map[string]bool{}}
}

func (l *fanoutLog) isolate(id string) {
	l.mu.Lock()
	l.isolated[id] = true
	l.mu.Unlock()
}

// commit 推一条日志。返回第一个应用失败的错误。
func (l *fanoutLog) commit(data []byte) error {
	l.mu.Lock()
	if l.fail != nil {
		err := l.fail
		l.mu.Unlock()
		return err
	}
	l.index++
	idx := l.index
	type target struct {
		id string
		rs *ReplicatedStore
	}
	var targets []target
	for id, rs := range l.members {
		if !l.isolated[id] {
			targets = append(targets, target{id, rs})
		}
	}
	l.mu.Unlock()

	for _, t := range targets {
		if err := t.rs.ApplyCommitted(idx, data); err != nil {
			return err
		}
	}
	return nil
}

// fanoutMember 是一个节点看到的组。
type fanoutMember struct {
	log *fanoutLog
	id  string
}

// Propose 把命令交给共享日志。
//
// 不区分 leader：转发给 leader 这件事在 cluster.TLSGroup 里做，
// 本包只看到"提交成功"或"提交失败"。
func (m *fanoutMember) Propose(data []byte) error { return m.log.commit(data) }

// node 是测试里的一个节点：状态机加它自己的冷层。
type node struct {
	id    string
	store *ReplicatedStore
	cold  *fakeCold
}

// newCluster 建 n 个共享同一条日志的节点。
func newCluster(t *testing.T, ids ...string) (*fanoutLog, map[string]*node) {
	t.Helper()
	log := newFanoutLog()
	nodes := map[string]*node{}
	for _, id := range ids {
		cold := &fakeCold{vals: map[string][]byte{}}
		local := NewLocalStore(LocalOptions{Cold: cold, Compile: stubCompile})
		rs, err := NewReplicatedStore(ReplicatedOptions{
			Group: &fanoutMember{log: log, id: id}, Node: id, Local: local,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rs.Close() })
		log.members[id] = rs
		nodes[id] = &node{id: id, store: rs, cold: cold}
	}
	return log, nodes
}

// TestReplicatedStoreKeepsHotAndColdInStep 三个节点吃同一条日志，
// 热视图与冷层都要一致。
//
// 这是 v0.4 阶段 B 的那条底线：只复制热视图的话，follower 上鉴权能过，
// 而 GetThing、证书正文、策略正文全是空的——空白节点追平之后看起来
// 是追上了，控制面读出来却什么都没有（CLU-065）。
func TestReplicatedStoreKeepsHotAndColdInStep(t *testing.T) {
	_, nodes := newCluster(t, "a", "b", "c")

	// 从跟随者提交：转发给 leader 这件事在传输层做，本包只看结果。
	rev, err := nodes["b"].store.Apply(ctx(), Command{
		Kind:    CmdSetThing,
		Thing:   &ThingCmd{Name: "lamp-1", PolicySet: 3},
		Records: []RecordOp{{Key: []byte("tlamp-1"), Value: []byte(`{"name":"lamp-1","site":"sh"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d，期望 1", rev)
	}

	if _, err := nodes["a"].store.Apply(ctx(), Command{
		Kind:   CmdSetPolicySet,
		Policy: &PolicyCmd{ID: 3, Name: "p1", Doc: []byte("策略正文")},
		Records: []RecordOp{
			{Key: []byte("pp1"), Value: []byte(`{"name":"p1","doc":"策略正文"}`)},
			{Key: []byte("Kpolicyset"), Value: []byte{0, 0, 0, 0, 0, 0, 0, 3}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	for id, n := range nodes {
		v := n.store.View()
		if v.Revision() != 2 {
			t.Errorf("%s 的 revision = %d，期望 2", id, v.Revision())
		}
		if _, ok := v.Thing("lamp-1"); !ok {
			t.Errorf("%s 的热视图里没有 lamp-1", id)
		}
		ps, ok := v.PolicySet(3)
		if !ok {
			t.Errorf("%s 的热视图里没有策略集 3", id)
		} else if ps.Compiled != "策略正文" {
			// 文档随日志走，各节点各自编译（CLU-064）
			t.Errorf("%s 上编译出的匹配器 = %v", id, ps.Compiled)
		}
		// 冷层：三样记录都要在
		if got := n.cold.vals["tlamp-1"]; string(got) != `{"name":"lamp-1","site":"sh"}` {
			t.Errorf("%s 的冷层里设备记录 = %q", id, got)
		}
		if _, ok := n.cold.vals["pp1"]; !ok {
			t.Errorf("%s 的冷层里没有策略正文", id)
		}
		// 计数器也必须复制，否则两个节点会把同一个策略集编号分配两次
		// （REG-112），而编号正被证书与设备记录引用着
		if got := n.cold.vals["Kpolicyset"]; len(got) != 8 || got[7] != 3 {
			t.Errorf("%s 的冷层里计数器 = %v", id, got)
		}
	}
}

// TestReplicatedStoreIsIdempotent 同一条命令提两次只应用一次。
func TestReplicatedStoreIsIdempotent(t *testing.T) {
	_, nodes := newCluster(t, "a", "b", "c")
	cmd := Command{
		ID: "fixed", Kind: CmdSetThing, Thing: &ThingCmd{Name: "lamp-1"},
		Records: []RecordOp{{Key: []byte("tlamp-1"), Value: []byte("v1")}},
	}
	first, err := nodes["a"].store.Apply(ctx(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	again, err := nodes["a"].store.Apply(ctx(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatalf("重复提交拿到不同 revision: %d 与 %d", first, again)
	}
	if got := nodes["c"].store.View().Revision(); got != 1 {
		t.Errorf("重复提交推进了 revision: %d", got)
	}
}

// TestReplicatedStoreRejectsWritesWithoutQuorum 丢掉多数派后写入失败，
// 已经提交的仍然可读。
//
// 这是内嵌共识的固有取舍（CLU-042）：设备连接、订阅、收发都只读本地
// 视图，所以数据面不受影响；只有注册、吊销、策略变更这类写操作失败。
func TestReplicatedStoreRejectsWritesWithoutQuorum(t *testing.T) {
	log, nodes := newCluster(t, "a", "b", "c")
	if _, err := nodes["a"].store.Apply(ctx(), Command{
		Kind:    CmdSetThing,
		Thing:   &ThingCmd{Name: "lamp-1"},
		Records: []RecordOp{{Key: []byte("tlamp-1"), Value: []byte("v")}},
	}); err != nil {
		t.Fatal(err)
	}

	log.isolate("b")
	log.isolate("c")
	log.mu.Lock()
	log.fail = errors.New("cluster: 没有多数派，写入被拒绝")
	log.mu.Unlock()

	if _, err := nodes["a"].store.Apply(ctx(), Command{
		Kind:  CmdSetThing,
		Thing: &ThingCmd{Name: "lamp-2"},
	}); err == nil {
		t.Error("丢掉多数派之后写入应当失败")
	}
	// 已提交的照常可读
	if _, ok := nodes["a"].store.View().Thing("lamp-1"); !ok {
		t.Error("丢掉多数派之后已提交的记录应当还能读到")
	}
	if got := nodes["a"].cold.vals["tlamp-1"]; string(got) != "v" {
		t.Errorf("冷层记录 = %q", got)
	}
}

// TestCommittedCommandsMatchAcrossStores 三个状态机吃同一条日志之后
// 必须完全一致。
//
// 删掉 ApplyCommitted 里对命令的应用，三个视图就对不上——这条用例
// 原先架在自写的那套 Raft 上，现在只验状态机这一侧。
func TestCommittedCommandsMatchAcrossStores(t *testing.T) {
	_, nodes := newCluster(t, "a", "b", "c")
	for _, name := range []string{"lamp", "pump"} {
		if _, err := nodes["b"].store.Apply(ctx(), Command{
			ID: "cmd-" + name, Kind: CmdSetThing, Thing: &ThingCmd{Name: name},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for id, n := range nodes {
		view := n.store.View()
		if view.Revision() != 2 {
			t.Errorf("%s revision = %d，期望 2", id, view.Revision())
		}
		for _, name := range []string{"lamp", "pump"} {
			if _, ok := view.Thing(name); !ok {
				t.Errorf("%s 缺少 %s", id, name)
			}
		}
	}
}

func TestSetThingsReplicatesAsOneEntry(t *testing.T) {
	_, nodes := newCluster(t, "a", "b", "c")
	rev, err := nodes["b"].store.Apply(ctx(), Command{
		Kind:   CmdSetThings,
		Things: []ThingCmd{{Name: "batch-a"}, {Name: "batch-b"}},
		Records: []RecordOp{
			{Key: []byte("tbatch-a"), Value: []byte("a")},
			{Key: []byte("tbatch-b"), Value: []byte("b")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("revision = %d，批次应只占一条日志", rev)
	}
	for id, n := range nodes {
		if n.store.View().Revision() != 1 {
			t.Errorf("%s revision = %d", id, n.store.View().Revision())
		}
		for _, name := range []string{"batch-a", "batch-b"} {
			if _, ok := n.store.View().Thing(name); !ok {
				t.Errorf("%s 热视图缺少 %s", id, name)
			}
			if got := string(n.cold.vals["t"+name]); got != string(name[len("batch-"):]) {
				t.Errorf("%s 冷层 %s = %q", id, name, got)
			}
		}
	}
}

// TestNewReplicatedStoreValidatesOptions 缺依赖要在构造时报错。
func TestNewReplicatedStoreValidatesOptions(t *testing.T) {
	local := NewLocalStore(LocalOptions{})
	defer func() { _ = local.Close() }()
	group := &fanoutMember{log: newFanoutLog(), id: "a"}
	cases := []ReplicatedOptions{
		{Node: "a", Local: local},
		{Group: group, Local: local},
		{Group: group, Node: "a"},
	}
	for i, opt := range cases {
		if _, err := NewReplicatedStore(opt); err == nil {
			t.Errorf("第 %d 组缺依赖的参数应被拒", i)
		}
	}
}

// TestApplyCommittedSingleEntryMatchesBatch 钉住单条入口与批量入口等价。
//
// ApplyCommitted 是 ApplyCommittedBatch 的单条包装。它存在是因为调用方有
// 两种形状（dragonboat 推一批、启动期补一条），而两者必须产生同一个结果
// ——否则同一条日志在不同调用方那里会落到不同的 revision 上。
func TestApplyCommittedSingleEntryMatchesBatch(t *testing.T) {
	vals := map[string][]byte{}
	txCalls := 0
	revKey := []byte("revision")
	s := NewLocalStore(LocalOptions{
		Cold:        &fakeCold{vals: vals},
		RevisionKey: revKey,
		InTransaction: func(fn func(Transaction) error) error {
			txCalls++
			return fn(&testMetaTx{vals: vals})
		},
	})
	defer s.Close()

	if err := s.ApplyCommitted(Command{
		ID: "one", Kind: CmdNop,
		Records: []RecordOp{{Key: []byte("k1"), Value: []byte("v1")}},
	}, 7); err != nil {
		t.Fatalf("单条 ApplyCommitted: %v", err)
	}
	if txCalls != 1 {
		t.Fatalf("单条入口用了 %d 个事务，期望 1", txCalls)
	}
	if string(vals["k1"]) != "v1" {
		t.Fatalf("冷层记录没写进去: %#v", vals)
	}
	if got := binary.BigEndian.Uint64(vals[string(revKey)]); got != 7 {
		t.Fatalf("revision = %d，期望 7", got)
	}

	// revision 的判据是严格递增而不是恰好加一（LANG-014 修订：成员变更会
	// 占掉日志序号）。跳号必须接受。
	if err := s.ApplyCommitted(Command{
		ID: "two", Kind: CmdNop,
		Records: []RecordOp{{Key: []byte("k2"), Value: []byte("v2")}},
	}, 11); err != nil {
		t.Fatalf("跳号的 ApplyCommitted: %v", err)
	}
	if got := binary.BigEndian.Uint64(vals[string(revKey)]); got != 11 {
		t.Fatalf("跳号后 revision = %d，期望 11", got)
	}
}
