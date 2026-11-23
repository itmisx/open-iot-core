package shadow

import "testing"

func TestApplyCommandOwnedReusesPrivateDocuments(t *testing.T) {
	cur := &Document{
		Reported:     Object{"old": int64(1)},
		ReportedMeta: Object{"old": Object{"timestamp": int64(1)}},
		Version:      3,
	}
	next, res, err := ApplyCommandOwned(cur, Command{
		Kind: CmdUpdate,
		Patch: Object{"reported": Object{
			"new": int64(2),
		}},
		NowSec: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Previous != cur {
		t.Fatal("owned 路径没有复用调用方交出的 previous 文档")
	}
	if res.Current != next {
		t.Fatal("owned 路径没有复用新状态作为 current 结果")
	}
	if _, ok := cur.Reported["new"]; ok {
		t.Fatal("计算 next 时意外改写了 previous")
	}
	if got := next.Version; got != 4 {
		t.Fatalf("version=%d，期望4", got)
	}
}

func TestApplyCommandKeepsResultIsolated(t *testing.T) {
	cur := &Document{Reported: Object{"old": int64(1)}, Version: 3}
	next, res, err := ApplyCommand(cur, Command{
		Kind: CmdUpdate,
		Patch: Object{"reported": Object{
			"new": int64(2),
		}},
		NowSec: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	res.Previous.Reported["old"] = int64(9)
	res.Current.Reported["new"] = int64(9)
	if got := cur.Reported["old"]; got != int64(1) {
		t.Fatalf("普通入口的 previous 与输入别名：%v", got)
	}
	if got := next.Reported["new"]; got != int64(2) {
		t.Fatalf("普通入口的 current 与 next 别名：%v", got)
	}
}
