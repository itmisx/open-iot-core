package meta

import (
	"context"
	"testing"
)

// TestSingleReplicaCommitsBeforeApply 验证单机元数据写入走一副本 Raft 组。
//
// 去掉 Append，或者在校验失败时也追加，提交序号就和视图 revision 对不上。
func TestSingleReplicaCommitsBeforeApply(t *testing.T) {
	s := NewLocalStore(LocalOptions{})
	g := s.Group()
	if g == nil || g.Voters() != 1 || g.Term() != 1 || g.CommitIndex() != 0 {
		t.Fatalf("组 = voters %d term %d commit %d", g.Voters(), g.Term(), g.CommitIndex())
	}
	ctx := context.Background()
	if _, err := s.Apply(ctx, Command{Kind: CmdSetCert}); err == nil {
		t.Fatal("缺载荷的命令不应提交")
	}
	if g.CommitIndex() != 0 {
		t.Fatalf("非法命令写进了日志，commit = %d", g.CommitIndex())
	}
	rev, err := s.Apply(ctx, Command{ID: "a", Kind: CmdNop})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 || g.CommitIndex() != 1 || s.View().Revision() != 1 {
		t.Fatalf("rev=%d commit=%d view=%d", rev, g.CommitIndex(), s.View().Revision())
	}
	again, err := s.Apply(ctx, Command{ID: "a", Kind: CmdNop})
	if err != nil || again != rev || g.CommitIndex() != 1 {
		t.Fatalf("重复命令又写了一条日志: rev=%d commit=%d err=%v", again, g.CommitIndex(), err)
	}
	rev2, err := s.Apply(ctx, Command{Kind: CmdNop})
	if err != nil || rev2 != 2 || g.CommitIndex() != 2 {
		t.Fatalf("第二条 = %d commit %d err %v", rev2, g.CommitIndex(), err)
	}
}
