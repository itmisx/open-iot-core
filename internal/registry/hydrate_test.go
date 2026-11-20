package registry

import (
	"context"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
)

/*
HydrateHot 的用例（CLU-098）。

它针对的是一个实测存在的缺陷：启动时的 Restore 把冷层里的每条记录都走一遍
r.meta.Apply，也就是**往复制日志里提一条新命令**。集群下一个冷层里已有
一百万条记录的节点重启时，会把这一百万条重新提进日志——命令没有 ID，
幂等窗口认不出它们是同一件事，而在跟随者上这些提案还要逐条转发给 leader。

HydrateHot 走的是 meta.LocalStore.Hydrate：只改热视图，不写冷层、不提日志。
*/

// TestHydrateHotRebuildsViewWithoutWriting 重建后热视图齐备，而冷层
// 与日志都没被动过。
func TestHydrateHotRebuildsViewWithoutWriting(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutThing(ctx, Thing{Name: "lamp-01", PolicyName: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := g.r.PutCertificate(ctx, Certificate{
		Fingerprint: fp(1), PolicyName: "p1", Thing: "lamp-01",
	}); err != nil {
		t.Fatal(err)
	}

	revBefore := g.meta.Revision()
	coldBefore := countKeys(t, g)

	// 另起一份热视图（模拟装完快照之后的那一刻：冷层是权威，热视图是空的）。
	fresh := meta.NewLocalStore(meta.LocalOptions{
		Now: g.now, ChangeBuffer: 256, Cold: g.kv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	defer func() { _ = fresh.Close() }()
	reg2, err := New(Options{KV: g.kv, Meta: fresh, Env: authz.DefaultEnv(), Now: g.now})
	if err != nil {
		t.Fatal(err)
	}

	const snapRev = meta.Revision(4242)
	if err := fresh.Hydrate(snapRev, func(apply func(meta.Command) error) error {
		return reg2.HydrateHot(apply)
	}); err != nil {
		t.Fatalf("重建失败: %v", err)
	}

	// 1) 热视图齐备。
	if _, ok := fresh.View().Thing("lamp-01"); !ok {
		t.Error("重建后热视图里没有设备")
	}
	fpv, err := parseFingerprint(fp(1))
	if err != nil {
		t.Fatal(err)
	}
	c, ok := fresh.View().Cert(fpv)
	if !ok {
		t.Fatal("重建后热视图里没有证书")
	}
	if c.Thing != "lamp-01" || c.PolicySet == meta.NoPolicySet {
		t.Errorf("重建后的证书热信息不完整: %+v", c)
	}
	if _, ok := fresh.View().PolicySet(c.PolicySet); !ok {
		t.Error("重建后没有编译好的策略集——那等于什么都不放行")
	}

	// 2) revision 必须是快照里的那个。CLU-059 判断三个节点是否一致
	//    的依据就是它，刚装完快照却报 0 的节点看起来像什么都没追上。
	if got := fresh.View().Revision(); got != snapRev {
		t.Errorf("重建后视图 revision = %d，期望 %d", got, snapRev)
	}

	// 3) 冷层键数没变：重建不写冷层。写一遍的代价在百万设备下是
	//    把刚装上的整个库重写一次。
	if got := countKeys(t, g); got != coldBefore {
		t.Errorf("重建写了冷层：键数 %d → %d", coldBefore, got)
	}

	// 4) 原来那份状态机的 revision 没动：重建没有提日志。
	if got := g.meta.Revision(); got != revBefore {
		t.Errorf("重建提了日志：原状态机 revision %d → %d", revBefore, got)
	}
}

// TestHydrateRejectsCommandsWithRecords 重建的命令不得带冷层记录。
//
// 这是一道防呆：带记录就意味着把刚装上的库原样重写一遍，而那在百万设备下
// 是几百 MB 的无用写入，且不会有任何报错。
func TestHydrateRejectsCommandsWithRecords(t *testing.T) {
	g := newRig(t)
	err := g.meta.Hydrate(1, func(apply func(meta.Command) error) error {
		return apply(meta.Command{
			Kind:    meta.CmdSetThing,
			Thing:   &meta.ThingCmd{Name: "lamp-01"},
			Records: []meta.RecordOp{{Key: []byte("x"), Value: []byte("y")}},
		})
	})
	if err == nil {
		t.Fatal("带冷层记录的重建命令被接受了")
	}
}

func countKeys(t *testing.T, g *rig) int {
	t.Helper()
	n := 0
	if err := g.kv.Scan(nil, func(_, _ []byte) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return n
}
