package meta

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

/*
状态机快照的往返（CLU-098、CLU-099）。

形状是「8 字节 revision + 冷层整库流」。这里用一份内存冷层把两端对上：
真实部署里写出的是 bbolt 的整库流（store.SwapKV），而本包要验的是
版本头、revision 的传递与重建的接线——那三样与引擎无关。
*/

// coldSnap 给 fakeCold 加上整库导出与替换。
type coldSnap struct{ c *fakeCold }

func (s coldSnap) snapshot(w io.Writer) error {
	return json.NewEncoder(w).Encode(s.c.vals)
}

func (s coldSnap) restore(r io.Reader) error {
	vals := map[string][]byte{}
	if err := json.NewDecoder(r).Decode(&vals); err != nil {
		return err
	}
	s.c.vals = vals
	return nil
}

// TestSnapshotRoundTripCarriesRevision 快照装到另一个节点上之后，
// 热视图与 revision 都要对上。
//
// revision 必须随快照走：CLU-059 判断三个节点是否一致的依据就是它，
// 而一个刚装完快照却报 0 的节点看起来像什么都没追上——就绪度会据此
// 对新连接 fail-closed，而它其实是追上了的。
func TestSnapshotRoundTripCarriesRevision(t *testing.T) {
	srcCold := &fakeCold{vals: map[string][]byte{}}
	src := NewLocalStore(LocalOptions{Cold: srcCold, Compile: stubCompile})
	srcGroup := &pushGroup{step: 3}
	srcStore, err := NewReplicatedStore(ReplicatedOptions{
		Group: srcGroup, Node: "a", Local: src,
		ColdSnapshot: coldSnap{srcCold}.snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	srcGroup.store = srcStore

	for _, name := range []string{"lamp-01", "lamp-02", "lamp-03"} {
		if _, err := srcStore.Apply(ctx(), Command{
			Kind:    CmdSetThing,
			Thing:   &ThingCmd{Name: name},
			Records: []RecordOp{{Key: []byte("t:" + name), Value: []byte(`{"name":"` + name + `"}`)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	wantRev := srcStore.View().Revision()

	var snap bytes.Buffer
	if err := srcStore.SnapshotTo(&snap); err != nil {
		t.Fatal(err)
	}

	// 接收端：空白状态机，冷层也是空的。
	dstCold := &fakeCold{vals: map[string][]byte{}}
	dst := NewLocalStore(LocalOptions{Cold: dstCold, Compile: stubCompile})
	dstStore, err := NewReplicatedStore(ReplicatedOptions{
		Group: &pushGroup{}, Node: "b", Local: dst,
		ColdRestore: coldSnap{dstCold}.restore,
		// 重建：扫冷层，把每条记录翻成一条只改热视图的命令。
		// 真实实现在 internal/registry（记录的解码器在那边）。
		Hydrate: func(apply func(Command) error) error {
			for key := range dstCold.vals {
				name, ok := strings.CutPrefix(key, "t:")
				if !ok {
					continue
				}
				if err := apply(Command{Kind: CmdSetThing, Thing: &ThingCmd{Name: name}}); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := dstStore.RestoreFrom(&snap); err != nil {
		t.Fatal(err)
	}

	if got := dstStore.View().Revision(); got != wantRev {
		t.Errorf("装完快照后 revision = %d，期望 %d", got, wantRev)
	}
	for _, name := range []string{"lamp-01", "lamp-02", "lamp-03"} {
		if _, ok := dstStore.View().Thing(name); !ok {
			t.Errorf("装完快照后热视图里没有 %s", name)
		}
	}
	if len(dstCold.vals) != 3 {
		t.Errorf("冷层里有 %d 条记录，期望 3", len(dstCold.vals))
	}

	// 装完之后继续吃日志：早于快照 revision 的条目要被当成重复投递忽略。
	//
	// 这不是边角情况：raft 记的快照序号是它调 Snapshot() 那一刻的
	// lastApplied，而真正把库写出去是在 Persist 里，两者之间本节点
	// 可能又应用了几条。于是重放一定会带来"已经在视图里"的条目。
	raw, err := EncodeCommand(Command{Kind: CmdSetThing, Thing: &ThingCmd{Name: "lamp-stale"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := dstStore.ApplyCommitted(uint64(wantRev)-1, raw); err != nil {
		t.Fatalf("重放早于快照的条目应当被忽略: %v", err)
	}
	if _, ok := dstStore.View().Thing("lamp-stale"); ok {
		t.Error("早于快照 revision 的条目被应用了")
	}
	// 而新的条目照常应用。
	if err := dstStore.ApplyCommitted(uint64(wantRev)+1, raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := dstStore.View().Thing("lamp-stale"); !ok {
		t.Error("快照之后的新条目没有被应用")
	}
}

// TestSnapshotRejectsUnknownVersion 读到别的格式版本要明确报错。
//
// 静默往下解的后果是把 revision 的八个字节当成库内容的一部分，
// 而那在 bbolt 那边表现为"库文件损坏"——与真因差得很远。
func TestSnapshotRejectsUnknownVersion(t *testing.T) {
	cold := &fakeCold{vals: map[string][]byte{}}
	local := NewLocalStore(LocalOptions{Cold: cold, Compile: stubCompile})
	rs, err := NewReplicatedStore(ReplicatedOptions{
		Group: &pushGroup{}, Node: "a", Local: local,
		ColdRestore: coldSnap{cold}.restore,
		Hydrate:     func(func(Command) error) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte{snapshotVersion + 1}, make([]byte, 8)...)
	if err := rs.RestoreFrom(bytes.NewReader(bad)); err == nil {
		t.Fatal("未知的快照格式版本被接受了")
	}
	if err := rs.RestoreFrom(bytes.NewReader([]byte{snapshotVersion})); err == nil {
		t.Fatal("截断的快照头被接受了")
	}
}

// TestSnapshotNeedsInjectedHooks 没注入钩子时要报错而不是产出一份空快照。
//
// 空快照是最坏的结果：它会被当成"这个节点上什么都没有"装到别处去。
func TestSnapshotNeedsInjectedHooks(t *testing.T) {
	local := NewLocalStore(LocalOptions{Cold: &fakeCold{vals: map[string][]byte{}}, Compile: stubCompile})
	rs, err := NewReplicatedStore(ReplicatedOptions{Group: &pushGroup{}, Node: "a", Local: local})
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.SnapshotTo(io.Discard); err == nil {
		t.Error("没注入冷层快照函数却产出了快照")
	}
	if err := rs.RestoreFrom(bytes.NewReader(nil)); err == nil {
		t.Error("没注入恢复函数却接受了快照")
	}
}
