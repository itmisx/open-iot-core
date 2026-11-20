package meta

import (
	"errors"
	"strings"
	"testing"
)

/*
推模型的 seam（CLU-097）。

这几条针对的是换共识库之后新出现的两件事：上层看到的日志序号**不再连续**
（成员变更与当选 no-op 也占序号），以及已提交条目由组推过来而不是由本包
去拉。拉模型那一版的代价是实现侧必须把全部历史留在内存里并在每次追平时
整体深拷贝，所以这不只是风格改动。
*/

// pushGroup 是一个推模型的假组：Propose 直接按给定序号应用。
//
// 不用真的 raft：本包要验的是 seam 的语义（序号可跳、解不开要报错、
// 应用失败要原样交回），而不是共识本身——那一层在 internal/cluster
// 自己的用例里。
type pushGroup struct {
	store *ReplicatedStore
	// index 是下一条日志的序号。步长可以大于 1，用来模拟成员变更
	// 占掉的那些序号。
	index uint64
	step  uint64
	err   error
}

func (g *pushGroup) Propose(data []byte) error {
	if g.err != nil {
		return g.err
	}
	step := g.step
	if step == 0 {
		step = 1
	}
	g.index += step
	return g.store.ApplyCommitted(g.index, data)
}

func newPushStore(t *testing.T, step uint64) (*ReplicatedStore, *pushGroup, *fakeCold) {
	t.Helper()
	cold := &fakeCold{vals: map[string][]byte{}}
	local := NewLocalStore(LocalOptions{Cold: cold, Compile: stubCompile})
	g := &pushGroup{step: step}
	rs, err := NewReplicatedStore(ReplicatedOptions{Group: g, Node: "a", Local: local})
	if err != nil {
		t.Fatal(err)
	}
	g.store = rs
	t.Cleanup(func() { _ = rs.Close() })
	return rs, g, cold
}

// TestPushSeamAcceptsSparseIndexes 序号可以跳号。
//
// 换库之前这里要求「恰好加一」，而 hashicorp/raft 的成员变更与当选 no-op
// 也占日志序号且不进状态机。仍然要求的是严格递增——跳号合法，倒退不合法。
func TestPushSeamAcceptsSparseIndexes(t *testing.T) {
	rs, _, _ := newPushStore(t, 7) // 每条命令之间空出 6 个序号

	var last Revision
	for i := 0; i < 3; i++ {
		rev, err := rs.Apply(ctx(), Command{
			Kind:    CmdSetThing,
			Thing:   &ThingCmd{Name: "lamp-" + string(rune('a'+i))},
			Records: nil,
		})
		if err != nil {
			t.Fatalf("第 %d 条失败: %v", i, err)
		}
		if rev <= last {
			t.Fatalf("第 %d 条的 revision = %d，没有比上一条（%d）大", i, rev, last)
		}
		last = rev
	}
	if last != 21 {
		t.Errorf("最后一条的 revision = %d，期望 21（步长 7 × 3 条）", last)
	}
	if got := rs.View().Revision(); got != last {
		t.Errorf("视图 revision = %d，期望 %d", got, last)
	}
}

// TestPushSeamRejectsGoingBackwards 倒退的序号不得改动视图。
//
// 快照重放与重复投递都会出现「又来一遍旧的」，那是安全的；但它不能把
// 视图往回带——视图 revision 是对外承诺的单调量（CLU-059 靠它判断
// 三个节点是否一致）。
func TestPushSeamRejectsGoingBackwards(t *testing.T) {
	rs, g, _ := newPushStore(t, 1)
	for _, name := range []string{"lamp-01", "lamp-02"} {
		if _, err := rs.Apply(ctx(), Command{
			Kind:  CmdSetThing,
			Thing: &ThingCmd{Name: name},
		}); err != nil {
			t.Fatal(err)
		}
	}
	before := rs.View().Revision()
	if before < 2 {
		t.Fatalf("前置条件不成立：revision = %d", before)
	}

	raw, err := EncodeCommand(Command{
		ID:    "old-one",
		Kind:  CmdSetThing,
		Thing: &ThingCmd{Name: "lamp-stale"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 相同序号（重复投递）与更旧的序号（快照重放）都必须被忽略，
	// 而且不是报错——这两种情况在至少一次投递下是正常的。
	for _, idx := range []uint64{uint64(before), uint64(before) - 1} {
		if err := rs.ApplyCommitted(idx, raw); err != nil {
			t.Fatalf("序号 %d 应当被忽略而不是报错: %v", idx, err)
		}
	}
	if got := rs.View().Revision(); got != before {
		t.Errorf("视图 revision 被旧序号改成了 %d，原先是 %d", got, before)
	}
	if _, ok := rs.View().Thing("lamp-stale"); ok {
		t.Error("旧序号的命令被应用了")
	}
	_ = g
}

// TestPushSeamReportsUndecodableEntry 解不开的条目要报错，不能跳过。
//
// 跳过的后果是本节点的视图比别人少一条变更而没有任何报错（CLU-066）。
// 报出来之后由组把本节点停掉（cluster.ErrApplyFailed）。
func TestPushSeamReportsUndecodableEntry(t *testing.T) {
	rs, _, _ := newPushStore(t, 1)
	err := rs.ApplyCommitted(1, []byte("这不是一条命令"))
	if err == nil {
		t.Fatal("解不开的条目被静默接受了")
	}
	if !strings.Contains(err.Error(), "无法解析") {
		t.Errorf("错误没说清是解析失败: %v", err)
	}
	if got := rs.View().Revision(); got != 0 {
		t.Errorf("解不开的条目推进了 revision 到 %d", got)
	}
}

// TestPushSeamPropagatesProposeError 组拒绝提案时，错误要原样上来。
func TestPushSeamPropagatesProposeError(t *testing.T) {
	rs, g, _ := newPushStore(t, 1)
	sentinel := errors.New("没有多数派")
	g.err = sentinel
	_, err := rs.Apply(ctx(), Command{Kind: CmdSetThing, Thing: &ThingCmd{Name: "x"}})
	if !errors.Is(err, sentinel) {
		t.Errorf("错误 = %v，期望包住 %v", err, sentinel)
	}
}
