package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/shadow"
	bolt "go.etcd.io/bbolt"
)

const nowSec = 1_700_000_000

func patch(t *testing.T, body string) shadow.Object {
	t.Helper()
	o, err := shadow.Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func upd(thing, name string, p shadow.Object) shadow.Command {
	return shadow.Command{
		Kind: shadow.CmdUpdate, Thing: thing, Shadow: name,
		Patch: p, NowSec: nowSec,
	}
}

func TestShadowStoreRoundTrip(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	ctx := context.Background()

	if _, ok := s.Get("d1", ""); ok {
		t.Error("空存储不应有影子")
	}
	res, err := s.Apply(ctx, upd("d1", "", patch(t, `{"reported":{"a":1,"n":{"b":2}}}`)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 1 {
		t.Errorf("版本 = %d，期望 1", res.Version)
	}

	doc, ok := s.Get("d1", "")
	if !ok {
		t.Fatal("应能读回影子")
	}
	if doc.Version != 1 || doc.UpdatedAtSec != nowSec {
		t.Errorf("= %+v", doc)
	}
	b, _ := json.Marshal(doc.Reported)
	if string(b) != `{"a":1,"n":{"b":2}}` {
		t.Errorf("reported = %s", b)
	}
	// 元数据须一同持久化，否则重启后所有字段的时间戳都丢了
	mb, _ := json.Marshal(doc.ReportedMeta)
	if !strings.Contains(string(mb), "1700000000") {
		t.Errorf("元数据 = %s", mb)
	}
}

func TestShadowStoreApplyTxSeesEarlierBatchWrite(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	err := kv.InTx(true, func(tx Tx) error {
		first, err := s.ApplyTx(tx, upd("d1", "", patch(t, `{"reported":{"a":1}}`)))
		if err != nil {
			return err
		}
		second, err := s.ApplyTx(tx, upd("d1", "", patch(t, `{"reported":{"b":2}}`)))
		if err != nil {
			return err
		}
		if first.Version != 1 || second.Version != 2 {
			t.Fatalf("批内版本 = %d,%d，期望 1,2", first.Version, second.Version)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	doc, ok := s.Get("d1", "")
	if !ok || doc.Version != 2 {
		t.Fatalf("提交后的影子 = %+v, ok=%v", doc, ok)
	}
}

// TestShadowStoreConcurrentApplyMergesTransactions 是影子吞吐的防回退门禁。
// 功能测试无法发现「每条更新重新退化成一次 fsync」：数据仍然完全正确，只是
// 吞吐会重新钉在单盘同步延迟的倒数。这里直接数 bbolt 的提交序号，要求并发
// 更新产生的物理事务数严格少于逻辑更新数。
func TestShadowStoreConcurrentApplyMergesTransactions(t *testing.T) {
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	bkv := kv.(*boltKV)
	// 给慢CI足够时间让64个goroutine都进入同一批；生产仍使用bbolt默认值。
	bkv.db.MaxBatchDelay = 100 * time.Millisecond
	s := NewShadowStore(kv)

	txID := func() int {
		t.Helper()
		var id int
		if err := bkv.db.View(func(tx *bolt.Tx) error {
			id = tx.ID()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}

	const n = 64
	before := txID()
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = s.Apply(context.Background(), upd(
				fmt.Sprintf("batch-device-%d", i), "",
				shadow.Object{"reported": shadow.Object{"online": true}},
			))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 条更新失败: %v", i, err)
		}
	}
	commits := txID() - before
	if commits >= n {
		t.Fatalf("%d 条并发更新产生 %d 个事务，合并提交未生效", n, commits)
	}
	for i := 0; i < n; i++ {
		if doc, ok := s.Get(fmt.Sprintf("batch-device-%d", i), ""); !ok || doc.Version != 1 {
			t.Fatalf("第 %d 条更新未正确提交: doc=%+v ok=%v", i, doc, ok)
		}
	}
}

func TestShadowStoreBatchIsolatesVersionConflict(t *testing.T) {
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	if _, err := s.Apply(context.Background(), upd("conflict", "",
		shadow.Object{"reported": shadow.Object{"n": 1}})); err != nil {
		t.Fatal(err)
	}

	const n = 32
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			cmd := upd(fmt.Sprintf("valid-%d", i), "",
				shadow.Object{"reported": shadow.Object{"n": i}})
			if i == 0 {
				cmd.Thing = "conflict"
				cmd.ExpectVersion = 99
			}
			_, errs[i] = s.Apply(context.Background(), cmd)
		}(i)
	}
	close(start)
	wg.Wait()
	var domain *shadow.Error
	if !errors.As(errs[0], &domain) || domain.Code != 409 {
		t.Fatalf("版本冲突 = %v，期望409", errs[0])
	}
	for i := 1; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("有效更新 %d 被同批冲突拖累: %v", i, errs[i])
		}
		if doc, ok := s.Get(fmt.Sprintf("valid-%d", i), ""); !ok || doc.Version != 1 {
			t.Fatalf("有效更新 %d 未提交: doc=%+v ok=%v", i, doc, ok)
		}
	}
	if doc, _ := s.Get("conflict", ""); doc == nil || doc.Version != 1 {
		t.Fatalf("冲突更新改写了影子: %+v", doc)
	}
}

type shadowCommitFailKV struct {
	*MemKV
	err error
}

func (k *shadowCommitFailKV) InTx(_ bool, fn func(Tx) error) error {
	k.MemKV.mu.Lock()
	defer k.MemKV.mu.Unlock()
	tx := &memTx{m: k.MemKV, pending: map[string][]byte{}}
	if err := fn(tx); err != nil {
		return err
	}
	// 模拟事务体已经算完，但存储引擎在提交阶段失败；pending 故意不落主表。
	return k.err
}

func TestShadowStoreCommitFailureNeverReturnsResult(t *testing.T) {
	boom := errors.New("注入的提交失败")
	kv := &shadowCommitFailKV{MemKV: NewMemKV().(*MemKV), err: boom}
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	res, err := s.Apply(context.Background(), upd("device", "",
		shadow.Object{"reported": shadow.Object{"online": true}}))
	if !errors.Is(err, boom) || res != nil {
		t.Fatalf("提交失败返回 result=%+v err=%v", res, err)
	}
	if _, ok := s.Get("device", ""); ok {
		t.Fatal("提交失败仍写入了影子")
	}
}

// TestShadowPersistsAcrossRestart 是本轮的核心验收。
//
// 一个重启就忘掉设备影子的 broker 不能部署：设备会以为云端已知其状态，
// 而云端什么都不记得，delta 的计算从此全错。
func TestShadowPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	kv1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	s1 := NewShadowStore(kv1)
	if _, err := s1.Apply(ctx, upd("d1", "", patch(t,
		`{"reported":{"temp":20},"desired":{"temp":25}}`))); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Apply(ctx, upd("d1", "cfg", patch(t, `{"reported":{"mode":"a"}}`))); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Apply(ctx, upd("d2", "", patch(t, `{"reported":{"x":1}}`))); err != nil {
		t.Fatal(err)
	}
	if err := kv1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启
	kv2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv2.Close() }()
	s2 := NewShadowStore(kv2)

	doc, ok := s2.Get("d1", "")
	if !ok {
		t.Fatal("重启后应能读回经典影子")
	}
	if doc.Version != 1 {
		t.Errorf("版本 = %d", doc.Version)
	}
	rb, _ := json.Marshal(doc.Reported)
	db, _ := json.Marshal(doc.Desired)
	if string(rb) != `{"temp":20}` || string(db) != `{"temp":25}` {
		t.Errorf("reported = %s, desired = %s", rb, db)
	}

	if _, ok := s2.Get("d1", "cfg"); !ok {
		t.Error("重启后应能读回命名影子")
	}
	if names := s2.List("d1"); len(names) != 1 || names[0] != "cfg" {
		t.Errorf("命名影子列表 = %v", names)
	}
	if _, ok := s2.Get("d2", ""); !ok {
		t.Error("重启后应能读回其他设备的影子")
	}
	if n := s2.CountShadows(); n != 3 {
		t.Errorf("影子总数 = %d，期望 3", n)
	}

	// 版本须从持久化的值继续递增，不得从 1 重新开始
	res, err := s2.Apply(ctx, upd("d1", "", patch(t, `{"reported":{"temp":21}}`)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 2 {
		t.Errorf("重启后的下一个版本 = %d，期望 2；版本回退会让设备的"+
			"乐观并发控制永久失效", res.Version)
	}
}

// TestNumberFidelityAcrossStorage 验证存储层不改写设备数据。
//
// 若记录解码时用默认的 float64，设备上报的 1.50 存一轮回来会变成 1.5、
// 大整数会丢精度。那等于存储层偷偷改写了设备数据，而影子是设备与云
// 之间逐字段比对的契约。
func TestNumberFidelityAcrossStorage(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const body = `{"reported":{"a":1.50,"b":10000000000000000001,"c":1e3,"d":0.1}}`
	if _, err := NewShadowStore(kv).Apply(context.Background(),
		upd("d1", "", patch(t, body))); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	kv2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv2.Close() }()
	doc, ok := NewShadowStore(kv2).Get("d1", "")
	if !ok {
		t.Fatal("应能读回")
	}
	got, _ := json.Marshal(doc.Reported)
	if string(got) != `{"a":1.50,"b":10000000000000000001,"c":1e3,"d":0.1}` {
		t.Errorf("数字字面量被改写了：%s", got)
	}
}

func TestShadowStoreDelete(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	ctx := context.Background()

	if _, err := s.Apply(ctx, upd("d1", "", patch(t, `{"reported":{"a":1}}`))); err != nil {
		t.Fatal(err)
	}
	res, err := s.Apply(ctx, shadow.Command{
		Kind: shadow.CmdDelete, Thing: "d1", NowSec: nowSec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 1 {
		t.Errorf("删除回传的版本 = %d", res.Version)
	}
	if _, ok := s.Get("d1", ""); ok {
		t.Error("删除后不应读到影子")
	}
	// 重建须从 1 开始（PRD SHDW-028）
	res, err = s.Apply(ctx, upd("d1", "", patch(t, `{"reported":{"a":1}}`)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 1 {
		t.Errorf("重建后的版本 = %d，期望 1", res.Version)
	}
	// 删除不存在的影子
	if _, err := s.Apply(ctx, shadow.Command{
		Kind: shadow.CmdDelete, Thing: "nobody", NowSec: nowSec,
	}); err == nil {
		t.Error("删除不存在的影子应报错")
	}
}

// TestShadowStoreSharesSemanticsWithMemStore 验证两个 Store 行为一致。
//
// 持久化实现复用 shadow.ApplyCommand，因此语义必然一致——
// 这条测试守的是"将来有人在存储层另写一遍合并逻辑"。
func TestShadowStoreSharesSemanticsWithMemStore(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	persistent := NewShadowStore(kv)
	inmem := shadow.NewMemStore()
	defer func() { _ = inmem.Close() }()
	ctx := context.Background()

	seq := []string{
		`{"reported":{"a":1,"n":{"x":1,"y":2}}}`,
		`{"reported":{"n":{"x":null}}}`,
		`{"desired":{"a":2}}`,
		`{"reported":{"list":[1,2,3]}}`,
		`{"reported":{"list":[9]}}`,
		`{"reported":{"a":{"deep":1}}}`,
	}
	for i, body := range seq {
		p1 := patch(t, body)
		p2 := patch(t, body)
		r1, e1 := persistent.Apply(ctx, upd("d1", "", p1))
		r2, e2 := inmem.Apply(ctx, upd("d1", "", p2))
		if (e1 == nil) != (e2 == nil) {
			t.Fatalf("第 %d 步错误不一致: %v 对 %v", i, e1, e2)
		}
		if e1 != nil {
			continue
		}
		if r1.Version != r2.Version || r1.Changed != r2.Changed {
			t.Errorf("第 %d 步结果不一致: %+v 对 %+v", i, r1, r2)
		}
		d1, _ := persistent.Get("d1", "")
		d2, _ := inmem.Get("d1", "")
		b1, _ := json.Marshal(d1)
		b2, _ := json.Marshal(d2)
		if string(b1) != string(b2) {
			t.Fatalf("第 %d 步（%s）后文档不一致\n持久: %s\n内存: %s",
				i, body, b1, b2)
		}
	}
}

func TestShadowStoreConcurrentSameThing(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	ctx := context.Background()
	const n = 100

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Apply(ctx, upd("d1", "", patch(t, `{"reported":{"a":1}}`)))
			if err != nil {
				t.Errorf("更新失败: %v", err)
				return
			}
			mu.Lock()
			seen[res.Version]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	// 版本不得重复：重复意味着一次更新被静默丢弃
	for v, c := range seen {
		if c != 1 {
			t.Errorf("版本 %d 被分配了 %d 次，写入未串行化", v, c)
		}
	}
	if len(seen) != n {
		t.Errorf("得到 %d 个不同版本，期望 %d", len(seen), n)
	}
}

func TestShadowStoreCorruptRecord(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)

	key := shadowKey("d1", "")
	if err := kv.Set(key, []byte(`{not json`), true); err != nil {
		t.Fatal(err)
	}
	// 读路径把损坏当作不存在：接口没有返回错误的途径，
	// 而写路径会覆盖掉损坏记录，使系统能自愈
	if _, ok := s.Get("d1", ""); ok {
		t.Error("损坏记录不应被当作有效影子返回")
	}
	// 写路径须明确报错而不是悄悄覆盖——覆盖会丢掉设备的既有状态
	if _, err := s.Apply(context.Background(),
		upd("d1", "", patch(t, `{"reported":{"a":1}}`))); err == nil {
		t.Error("遇到损坏记录时写入应报错")
	}
}

func TestShadowRecordFutureVersion(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	raw, err := json.Marshal(&shadowRecord{V: shadowRecordVersion + 1, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(shadowKey("d1", ""), raw, true); err != nil {
		t.Fatal(err)
	}
	if _, derr := decodeShadow(raw); derr == nil {
		t.Error("更高的记录版本应报错")
	}
	_, err = NewShadowStore(kv).Apply(context.Background(),
		upd("d1", "", patch(t, `{"reported":{"a":1}}`)))
	if err == nil {
		t.Error("遇到更高版本的记录时应拒绝写入，而不是按旧语义覆盖")
	}
}

func TestShadowStoreClosedKV(t *testing.T) {
	kv := NewMemKV()
	s := NewShadowStore(kv)
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(context.Background(),
		upd("d1", "", patch(t, `{"reported":{"a":1}}`))); !errors.Is(err, ErrClosed) {
		t.Errorf("= %v，期望 ErrClosed", err)
	}
	if _, ok := s.Get("d1", ""); ok {
		t.Error("已关闭的 KV 不应返回数据")
	}
	// Close 不关闭底层 KV：KV 由装配方持有，多个 Store 共享它
	if err := s.Close(); err != nil {
		t.Errorf("ShadowStore.Close 应为 no-op: %v", err)
	}
}

func TestShadowStoreListIsolation(t *testing.T) {
	kv := NewMemKV()
	defer func() { _ = kv.Close() }()
	s := NewShadowStore(kv)
	ctx := context.Background()
	// d1 与 d10 的键前缀相邻，List 不得串
	for _, thing := range []string{"d1", "d10"} {
		for _, name := range []string{"", "cfg"} {
			if _, err := s.Apply(ctx, upd(thing, name, patch(t, `{"reported":{"a":1}}`))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if names := s.List("d1"); len(names) != 1 || names[0] != "cfg" {
		t.Errorf("d1 的命名影子 = %v，期望 [cfg]；串到相邻设备属安全问题", names)
	}
	if names := s.List("nobody"); len(names) != 0 {
		t.Errorf("未知设备 = %v，期望空", names)
	}
}
