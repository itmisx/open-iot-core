package shadow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// 影子的复制层（CLU-082）。
//
// 它错掉的方式都很安静：命令应用了而发起方拿不到结果（设备看到超时而
// 影子其实变了）、冲突被当成故障（409 变成 500）、或者各节点算出不同的
// 版本号（状态机不再确定，而没有任何报错）。

// fakeLog 把命令直接交给一组 applier，模拟「提交并应用」。
type fakeLog struct {
	mu       sync.Mutex
	appliers []func(string, []byte) error
	err      error
	// dropped 为真时命令被"提交"但不交给任何 applier，
	// 模拟「日志说成功而本节点没应用」。
	dropped bool
}

func (l *fakeLog) AppendShadow(_ context.Context, id string, raw []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	if l.dropped {
		return nil
	}
	for _, fn := range l.appliers {
		if err := fn(id, raw); err != nil {
			return err
		}
	}
	return nil
}

// newReplicatedTrio 建三个节点：各有自己的本地存储，共用一条日志。
func newReplicatedTrio(t *testing.T) (*fakeLog, []*ReplicatedStore) {
	t.Helper()
	log := &fakeLog{}
	var out []*ReplicatedStore
	for _, id := range []string{"a", "b", "c"} {
		rs, err := NewReplicatedStore(NewMemStore(), log, id)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rs)
		log.appliers = append(log.appliers, rs.ApplyCommitted)
	}
	return log, out
}

// TestReplicatedShadowStaysInStepAcrossNodes 三个节点吃同一条日志，
// 影子与版本号都要一致。
//
// 不复制的话，设备连 A 更新影子，控制台在 B 上读到的是空。
func TestReplicatedShadowStaysInStepAcrossNodes(t *testing.T) {
	_, nodes := newReplicatedTrio(t)
	ctx := context.Background()

	res, err := nodes[0].Apply(ctx, Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"interval": 30.0}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.Version != 1 {
		t.Fatalf("结果 = %+v", res)
	}

	for i, n := range nodes {
		doc, ok := n.Get("lamp-1", "")
		if !ok {
			t.Fatalf("第 %d 个节点上没有这份影子", i)
		}
		if doc.Version != 1 {
			t.Errorf("第 %d 个节点上版本 = %d，期望 1", i, doc.Version)
		}
		if len(n.List("lamp-1")) != 0 {
			// 经典影子不出现在命名影子列表里
			t.Errorf("第 %d 个节点的命名影子列表非空", i)
		}
	}
}

// TestReplicatedShadowConflictIsNotAFailure 版本冲突要交回发起方，
// 不得当成 apply 失败。
//
// 给定同样的日志前缀，所有节点都会算出同样的冲突——它是确定性的
// 状态机结果。当成失败会让 apply 返回错误，而日志已经提交，
// 于是日志与状态不一致。
func TestReplicatedShadowConflictIsNotAFailure(t *testing.T) {
	log, nodes := newReplicatedTrio(t)
	ctx := context.Background()

	if _, err := nodes[0].Apply(ctx, Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"a": 1.0}}},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := nodes[0].Apply(ctx, Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1701, ExpectVersion: 99,
		Patch: Object{"state": Object{"desired": Object{"a": 2.0}}},
	})
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("冲突 = %v，期望一个 *Error（409）", err)
	}
	if e.Code != 409 {
		t.Errorf("错误码 = %d，期望 409", e.Code)
	}
	// 日志本身没有失败：AppendShadow 返回的是 nil，冲突从 pending 里取回
	if log.err != nil {
		t.Errorf("日志记录了失败: %v", log.err)
	}
	// 三个节点的状态仍然一致（都停在版本 1）
	for i, n := range nodes {
		doc, ok := n.Get("lamp-1", "")
		if !ok || doc.Version != 1 {
			t.Errorf("第 %d 个节点 = %+v, ok=%v", i, doc, ok)
		}
	}
}

// TestReplicatedShadowReportsMissingApply 日志说提交了而本节点没应用，
// 必须报错而不是返回空结果。
//
// 返回空结果会让设备收到一条「成功但什么都没变」的响应。
func TestReplicatedShadowReportsMissingApply(t *testing.T) {
	log := &fakeLog{dropped: true}
	rs, err := NewReplicatedStore(NewMemStore(), log, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = rs.Apply(context.Background(), Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"a": 1.0}}},
	})
	if err == nil || !strings.Contains(err.Error(), "没有应用") {
		t.Fatalf("= %v，期望报「已提交但本节点没有应用」", err)
	}
}

// TestReplicatedShadowPropagatesLogFailure 提交失败要原样传出。
//
// 吞掉它会让调用方拿到 200，而那条更新从来没进日志。
func TestReplicatedShadowPropagatesLogFailure(t *testing.T) {
	log := &fakeLog{err: errors.New("注入的提交失败")}
	rs, err := NewReplicatedStore(NewMemStore(), log, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Apply(context.Background(), Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"a": 1.0}}},
	}); err == nil {
		t.Fatal("提交失败应当传出")
	}
}

// TestReplicatedShadowDeleteAndClose 删除同样走日志，Close 关到本地。
func TestReplicatedShadowDeleteAndClose(t *testing.T) {
	_, nodes := newReplicatedTrio(t)
	ctx := context.Background()
	if _, err := nodes[0].Apply(ctx, Command{
		Kind: CmdUpdate, Thing: "lamp-1", Shadow: "cfg", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"a": 1.0}}},
	}); err != nil {
		t.Fatal(err)
	}
	// 命名影子要出现在列表里，且三个节点一致
	for i, n := range nodes {
		got := n.List("lamp-1")
		if len(got) != 1 || got[0] != "cfg" {
			t.Errorf("第 %d 个节点的命名影子 = %v", i, got)
		}
	}
	if _, err := nodes[1].Apply(ctx, Command{
		Kind: CmdDelete, Thing: "lamp-1", Shadow: "cfg", NowSec: 1702,
	}); err != nil {
		t.Fatal(err)
	}
	for i, n := range nodes {
		if _, ok := n.Get("lamp-1", "cfg"); ok {
			t.Errorf("第 %d 个节点上影子没被删掉", i)
		}
	}
	for _, n := range nodes {
		if err := n.Close(); err != nil {
			t.Errorf("Close = %v", err)
		}
	}
}

// TestNewReplicatedStoreValidatesDeps 缺依赖要在构造时报错。
func TestNewReplicatedStoreValidatesDeps(t *testing.T) {
	log := &fakeLog{}
	cases := []struct {
		name  string
		local Store
		log   CommandLog
		node  string
	}{
		{"缺本地存储", nil, log, "a"},
		{"缺日志", NewMemStore(), nil, "a"},
		{"缺节点 ID", NewMemStore(), log, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewReplicatedStore(c.local, c.log, c.node); err == nil {
				t.Error("应当被拒")
			}
		})
	}
}

// TestShadowCommandWireRoundTrip 命令的线格式要保住数字字面量。
//
// 影子的数字按字面量保留（SHDW-026a）：解析为浮点会让 1.50 变成 1.5、
// 大整数丢精度，而影子是设备与云之间逐字段比对的契约。线格式经过
// json.Marshal(any) 的话这条就破了。
func TestShadowCommandWireRoundTrip(t *testing.T) {
	patch, err := Decode([]byte(`{"state":{"reported":{"ratio":1.50,"big":123456789012345678901,"on":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{
		Kind: CmdUpdate, Thing: "lamp-1", Shadow: "cfg",
		Patch: patch, ExpectVersion: 7, NowSec: 1700, ClientToken: "tok",
	}
	raw, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	// 字面量要原样出现在日志里
	for _, want := range []string{"1.50", "123456789012345678901"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("线格式里丢了字面量 %s: %s", want, raw)
		}
	}
	got, err := DecodeCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != cmd.Kind || got.Thing != cmd.Thing || got.Shadow != cmd.Shadow ||
		got.ExpectVersion != cmd.ExpectVersion || got.NowSec != cmd.NowSec ||
		got.ClientToken != cmd.ClientToken {
		t.Fatalf("往返不等：\n原 = %+v\n回 = %+v", cmd, got)
	}
	back, err := Encode(got.Patch)
	if err != nil {
		t.Fatal(err)
	}
	orig, err := Encode(cmd.Patch)
	if err != nil {
		t.Fatal(err)
	}
	if string(back) != string(orig) {
		t.Errorf("补丁往返后变了：\n原 = %s\n回 = %s", orig, back)
	}
}

// TestDecodeShadowCommandRejectsBadEntries 解不开或版本不认识要报错。
func TestDecodeShadowCommandRejectsBadEntries(t *testing.T) {
	cases := []string{
		`{not json`,
		`{"v":99,"kind":1,"thing":"x"}`,
		`{"v":1,"kind":1,"thing":"x","patch":"不是对象"}`,
	}
	for _, c := range cases {
		if _, err := DecodeCommand([]byte(c)); err == nil {
			t.Errorf("%s 应当被拒", c)
		}
	}
}

// failingStore 在 Apply 上注入存储故障。
type failingStore struct {
	Store
	err error
}

func (f failingStore) Apply(context.Context, Command) (*Result, error) {
	return nil, f.err
}

// TestReplicatedShadowSurfacesStoreFailure 存储故障要让 apply 失败。
//
// 与冲突相反：冲突是确定性的状态机结果（交回发起方），而写不进去是
// 这个节点自己的问题——吞掉它会让这个节点的影子比别人少一次更新，
// 而没有任何报错。
func TestReplicatedShadowSurfacesStoreFailure(t *testing.T) {
	boom := errors.New("注入的存储故障")
	rs, err := NewReplicatedStore(failingStore{Store: NewMemStore(), err: boom},
		&fakeLog{}, "a")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCommand(Command{Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700})
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.ApplyCommitted("id-1", raw); !errors.Is(err, boom) {
		t.Fatalf("= %v，期望传出存储故障", err)
	}
}

// TestReplicatedShadowIgnoresUnawaitedResults 没人等结果时不报错。
//
// 跟随者同样应用每一条命令，而它们没有发起方在等——那是正常情形，
// 不是错误。
func TestReplicatedShadowIgnoresUnawaitedResults(t *testing.T) {
	rs, err := NewReplicatedStore(NewMemStore(), &fakeLog{}, "b")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCommand(Command{
		Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700,
		Patch: Object{"state": Object{"desired": Object{"a": 1.0}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.ApplyCommitted("别人发起的", raw); err != nil {
		t.Fatalf("= %v，期望没人等结果时照常应用", err)
	}
	if _, ok := rs.Get("lamp-1", ""); !ok {
		t.Error("影子没有被应用")
	}
}

func TestReplicatedShadowTransactionalDeliveryWaitsForCommit(t *testing.T) {
	rs, err := NewReplicatedStore(NewMemStore(), &fakeLog{}, "a")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCommand(Command{Kind: CmdUpdate, Thing: "lamp-1", NowSec: 1700})
	if err != nil {
		t.Fatal(err)
	}
	rs.pending["id-1"] = nil
	post, err := rs.ApplyCommittedTx("id-1", raw, func(cmd Command) (*Result, error) {
		return rs.local.Apply(context.Background(), cmd)
	})
	if err != nil {
		t.Fatal(err)
	}
	if rs.pending["id-1"] != nil {
		t.Fatal("事务提交前已经交付 pending 结果")
	}
	post()
	if rs.pending["id-1"] == nil || rs.pending["id-1"].res == nil {
		t.Fatal("事务提交后没有交付 pending 结果")
	}
}

func TestReplicatedShadowWaitCommittedUsesLocalApplySignal(t *testing.T) {
	rs, err := NewReplicatedStore(NewMemStore(), &fakeLog{}, "a")
	if err != nil {
		t.Fatal(err)
	}
	id := "id-wait"
	rs.mu.Lock()
	rs.pending[id] = nil
	rs.waiters[id] = make(chan struct{})
	rs.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- rs.WaitCommitted(context.Background(), id) }()
	select {
	case err := <-done:
		t.Fatalf("本地 apply 前等待提前结束: %v", err)
	default:
	}

	rs.DeliverCommitted(id, &Result{Version: 7}, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	rs.mu.Lock()
	got := rs.pending[id]
	rs.mu.Unlock()
	if got == nil || got.res == nil || got.res.Version != 7 {
		t.Fatalf("交付结果 = %+v", got)
	}
}

func TestReplicatedShadowWaitCommittedHonorsContext(t *testing.T) {
	rs, err := NewReplicatedStore(NewMemStore(), &fakeLog{}, "a")
	if err != nil {
		t.Fatal(err)
	}
	id := "id-timeout"
	rs.mu.Lock()
	rs.pending[id] = nil
	rs.waiters[id] = make(chan struct{})
	rs.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rs.WaitCommitted(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("等待错误 = %v，期望 context.Canceled", err)
	}
}

// TestReplicatedShadowRefusesBrokenEntry 解不开的条目要让 apply 失败。
func TestReplicatedShadowRefusesBrokenEntry(t *testing.T) {
	rs, err := NewReplicatedStore(NewMemStore(), &fakeLog{}, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.ApplyCommitted("id", []byte("这不是命令")); err == nil {
		t.Error("解不开的条目应当让 apply 失败")
	}
}
