package store

import (
	"context"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

// 整批 apply 的验收（CLU-125）。
//
// 这一组钉的是"一批已提交日志进同一个写事务"之后**没有**丢掉的那几条
// 性质。它们的共同点是：逐条 apply 时天然成立，合批之后要靠实现主动
// 维持，而失手的表现都不是报错。
//
//   - 批内后一条要看得见前一条的效果（CLU-115）。先算完全批 ops 再一起
//     写的话，同一批里两条 enqueue 会算出同一个序号，于是一条消息静默
//     覆盖另一条。
//   - 每条命令的判定值要逐条回填（CLU-116）。整批一个值会让"我这条被旧
//     epoch 拦了"变成"这批里有人被拦了"。
//   - applied index 取这一批**最后一条**。取中间任何一条都会让"报的位置"
//     小于"盘上的状态"，重启后那几条被重放。
//   - 一条出错整批不生效。半批落盘的状态机与别的节点对不上，而那是分叉。

// batchCmd 编出一条会话命令，供整批 apply 使用。
func batchCmd(t *testing.T, c *sessionCmd) []byte {
	t.Helper()
	data, err := encodeSessionCmd(c)
	if err != nil {
		t.Fatalf("编码会话命令失败: %v", err)
	}
	return data
}

func TestBatchSeesItsOwnEarlierEntries(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	// 同一批里：先建会话，再连入三条消息。三条的序号必须是 1、2、3。
	ents := []SessionEntry{
		{Index: 1, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 1),
		})},
	}
	for i := 0; i < 3; i++ {
		ents = append(ents, SessionEntry{
			Index: uint64(2 + i),
			Data: batchCmd(t, &sessionCmd{
				Op: sessionOpEnqueue, ClientID: "dev-1",
				Msg: &session.QueuedMsg{Topic: "t", Payload: []byte{byte(i)}, QoS: 1},
			}),
		})
	}
	if err := s.ApplySessionBatch(16, ents, func(string) bool { return true }); err != nil {
		t.Fatalf("整批 apply 失败: %v", err)
	}

	msgs, last, err := s.PeekQueued("dev-1")
	if err != nil {
		t.Fatalf("读队列失败: %v", err)
	}
	// 判据是**条数**而不是最后一条的内容：撞号的表现正是"内容对、条数少"。
	if len(msgs) != 3 {
		t.Fatalf("队列里有 %d 条，期望 3——同一批里的入队撞号了（CLU-115）", len(msgs))
	}
	if last != 3 {
		t.Errorf("最后一条的序号是 %d，期望 3", last)
	}
	for i := range msgs {
		if len(msgs[i].Payload) != 1 || msgs[i].Payload[0] != byte(i) {
			t.Errorf("第 %d 条的负载是 %v，顺序或内容不对", i, msgs[i].Payload)
		}
	}
}

func TestReplaceClearsOldOfflineQueue(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	ctx := context.Background()
	old := replState("dev-replace", 1)
	old.Queued = []session.QueuedMsg{{Topic: "old", Payload: []byte("stale")}}
	if err := s.Save(ctx, old); err != nil {
		t.Fatalf("保存旧会话失败: %v", err)
	}
	next := replState("dev-replace", 2)
	if err := s.Replace(ctx, next); err != nil {
		t.Fatalf("替换会话失败: %v", err)
	}
	got, found, err := s.Load(ctx, "dev-replace")
	if err != nil || !found {
		t.Fatalf("读取替换后的会话失败: %v（found=%v）", err, found)
	}
	if got.Epoch != 2 || len(got.Queued) != 0 {
		t.Fatalf("替换结果 epoch=%d queued=%d，期望 epoch=2、队列为空", got.Epoch, len(got.Queued))
	}
}

func TestReplaceRejectsStaleEpoch(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	ctx := context.Background()
	current := replState("dev-replace-stale", 2)
	current.Queued = []session.QueuedMsg{{Topic: "new", Payload: []byte("keep")}}
	if err := s.Save(ctx, current); err != nil {
		t.Fatalf("保存新会话失败: %v", err)
	}
	if err := s.Replace(ctx, replState("dev-replace-stale", 1)); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("旧 epoch 替换应被拒绝，得到 %v", err)
	}
	got, found, err := s.Load(ctx, "dev-replace-stale")
	if err != nil || !found {
		t.Fatalf("读取新会话失败: %v（found=%v）", err, found)
	}
	if got.Epoch != 2 || len(got.Queued) != 1 || string(got.Queued[0].Payload) != "keep" {
		t.Fatalf("旧 Replace 改写了新会话: epoch=%d queued=%v", got.Epoch, got.Queued)
	}
}

func TestBatchKeepsPerEntryResults(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	owns := func(string) bool { return true }
	// 先让 dev-1 以 epoch 5 落地。
	first := []SessionEntry{{Index: 1, Data: batchCmd(t, &sessionCmd{
		Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 5),
	})}}
	if err := s.ApplySessionBatch(16, first, owns); err != nil {
		t.Fatalf("前置 apply 失败: %v", err)
	}

	// 一批三条：旧 epoch 的 save（该被拦）、新 epoch 的 save（该过）、
	// 另一个 clientId 的 save（该过）。
	ents := []SessionEntry{
		{Index: 2, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 3),
		})},
		{Index: 3, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 7),
		})},
		{Index: 4, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-2", State: replState("dev-2", 1),
		})},
	}
	if err := s.ApplySessionBatch(16, ents, owns); err != nil {
		t.Fatalf("整批 apply 失败: %v", err)
	}
	if ents[0].Result != sessionResStale {
		t.Errorf("第 1 条的判定是 %d，期望 stale（%d）——围栏判定没有逐条回填（CLU-116）",
			ents[0].Result, sessionResStale)
	}
	if ents[1].Result != sessionResOK || ents[2].Result != sessionResOK {
		t.Errorf("第 2、3 条的判定是 %d 与 %d，期望都是 ok（%d）",
			ents[1].Result, ents[2].Result, sessionResOK)
	}
	// 被拦的那条不得改变状态：epoch 应当是同一批里新的那个 7。
	st, found, err := s.Load(context.Background(), "dev-1")
	if err != nil || !found {
		t.Fatalf("读会话失败: %v（found=%v）", err, found)
	}
	if st.Epoch != 7 {
		t.Errorf("epoch 是 %d，期望 7", st.Epoch)
	}
}

func TestBatchPersistsOnlyTheLastIndex(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	ents := []SessionEntry{
		{Index: 11, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 1),
		})},
		{Index: 12, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-2", State: replState("dev-2", 1),
		})},
		{Index: 17, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-3", State: replState("dev-3", 1),
		})},
	}
	if err := s.ApplySessionBatch(16, ents, func(string) bool { return true }); err != nil {
		t.Fatalf("整批 apply 失败: %v", err)
	}
	if got := s.SessionAppliedIndex(16); got != 17 {
		t.Errorf("applied index 是 %d，期望 17——报小了的那几条重启后会被重放", got)
	}
}

func TestBatchRollsBackWholeBatchOnError(t *testing.T) {
	s := NewSessionStore(NewMemKV())
	// 第二条的操作码是未知的：它在逐条版里是致命错误（分叉），整批版
	// 里要连第一条一起回滚。
	ents := []SessionEntry{
		{Index: 1, Data: batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 1),
		})},
		{Index: 2, Data: batchCmd(t, &sessionCmd{Op: 99, ClientID: "dev-2"})},
	}
	err := s.ApplySessionBatch(16, ents, func(string) bool { return true })
	if err == nil {
		t.Fatal("整批 apply 没有报错，期望未知操作码是致命的")
	}
	if _, found, lerr := s.Load(context.Background(), "dev-1"); lerr != nil || found {
		t.Errorf("第一条的写入留了下来（found=%v, err=%v）：半批落盘就是分叉", found, lerr)
	}
	if got := s.SessionAppliedIndex(16); got != 0 {
		t.Errorf("applied index 是 %d，期望 0", got)
	}
}

// TestBatchMatchesOneByOne 是等价性判据：同一串命令，整批与逐条
// 必须落出同一份状态。
//
// 它存在的理由是整批那条路径换了读的来源（事务而不是 KV），而那种
// 差别不会让任何断言失败——它让两个节点落出不同的状态，而那要到
// 跨节点对账时才显形。
func TestBatchMatchesOneByOne(t *testing.T) {
	cmds := [][]byte{
		batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-1", State: replState("dev-1", 1),
		}),
		batchCmd(t, &sessionCmd{
			Op: sessionOpEnqueue, ClientID: "dev-1",
			Msg: &session.QueuedMsg{Topic: "a", Payload: []byte("1"), QoS: 1},
		}),
		batchCmd(t, &sessionCmd{
			Op: sessionOpEnqueue, ClientID: "dev-1",
			Msg: &session.QueuedMsg{Topic: "b", Payload: []byte("2"), QoS: 1},
		}),
		batchCmd(t, &sessionCmd{
			Op: sessionOpSave, ClientID: "dev-2", State: replState("dev-2", 2),
		}),
		batchCmd(t, &sessionCmd{Op: sessionOpTake, ClientID: "dev-1", UpTo: 1}),
		batchCmd(t, &sessionCmd{
			Op: sessionOpDelete, ClientID: "dev-2", Epoch: cluster.Epoch(2),
		}),
	}
	owns := func(string) bool { return true }

	one := NewSessionStore(NewMemKV())
	oneRes := make([]uint64, len(cmds))
	for i := range cmds {
		res, err := one.ApplySessionCmd(16, uint64(i+1), cmds[i], owns)
		if err != nil {
			t.Fatalf("逐条 apply 第 %d 条失败: %v", i, err)
		}
		oneRes[i] = res
	}

	all := NewSessionStore(NewMemKV())
	ents := make([]SessionEntry, len(cmds))
	for i := range cmds {
		ents[i] = SessionEntry{Index: uint64(i + 1), Data: cmds[i]}
	}
	if err := all.ApplySessionBatch(16, ents, owns); err != nil {
		t.Fatalf("整批 apply 失败: %v", err)
	}

	for i := range cmds {
		if ents[i].Result != oneRes[i] {
			t.Errorf("第 %d 条的判定：整批 %d，逐条 %d", i, ents[i].Result, oneRes[i])
		}
	}
	if !sameKeyspace(t, one, all) {
		t.Error("整批与逐条落出的状态不同：两条路径不等价")
	}
}

// sameKeyspace 比较两个存储的全部键值。
func sameKeyspace(t *testing.T, a, b *SessionStore) bool {
	t.Helper()
	dump := func(s *SessionStore) map[string]string {
		out := map[string]string{}
		for _, prefix := range []Prefix{PrefixSession, PrefixQueued} {
			if err := s.kv.Scan([]byte{byte(prefix)}, func(k, v []byte) bool {
				out[string(k)] = string(v)
				return true
			}); err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
		}
		return out
	}
	x, y := dump(a), dump(b)
	if len(x) != len(y) {
		t.Logf("键数不同：%d 对 %d", len(x), len(y))
		return false
	}
	for k, v := range x {
		if y[k] != v {
			t.Logf("键 %q 的值不同", k)
			return false
		}
	}
	return true
}

// TestInTxReadsItsOwnWrites 钉的是 InTx 这个入口本身的语义。
//
// 它是整批 apply 的前提：看不见自己的写入，批内的入队序号就全错。
// 两个实现各测一遍——MemKV 的覆盖层与 bbolt 的事务是两段独立的代码，
// 而 KV 接口的意义正是"两个实现同一套语义"。
func TestInTxReadsItsOwnWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		kv   KV
	}{
		{"内存", NewMemKV()},
		{"bbolt", openTestKV(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.kv.InTx(true, func(tx Tx) error {
				if err := tx.Set([]byte("k1"), []byte("v1")); err != nil {
					return err
				}
				got, gerr := tx.Get([]byte("k1"))
				if gerr != nil {
					return gerr
				}
				if string(got) != "v1" {
					t.Errorf("事务内读到 %q，期望 v1", got)
				}
				// 扫描也要看得见，而且必须是升序：入队序号靠它推出。
				if err := tx.Set([]byte("k3"), []byte("v3")); err != nil {
					return err
				}
				if err := tx.Set([]byte("k2"), []byte("v2")); err != nil {
					return err
				}
				var order []string
				if err := tx.Scan([]byte("k"), func(k, _ []byte) bool {
					order = append(order, string(k))
					return true
				}); err != nil {
					return err
				}
				if len(order) != 3 || order[0] != "k1" || order[1] != "k2" || order[2] != "k3" {
					t.Errorf("事务内扫描的顺序是 %v，期望升序 k1 k2 k3", order)
				}
				// 删除在事务内也要立刻看不见。
				if err := tx.Delete([]byte("k2")); err != nil {
					return err
				}
				if _, gerr := tx.Get([]byte("k2")); !errors.Is(gerr, ErrNotFound) {
					t.Errorf("删掉的键还读得到，err=%v", gerr)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("InTx 失败: %v", err)
			}
			if got, gerr := tc.kv.Get([]byte("k1")); gerr != nil || string(got) != "v1" {
				t.Errorf("提交之后读到 %q（err=%v），期望 v1", got, gerr)
			}
			if _, gerr := tc.kv.Get([]byte("k2")); !errors.Is(gerr, ErrNotFound) {
				t.Errorf("事务内删掉的键提交之后还在，err=%v", gerr)
			}
		})
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		kv   KV
	}{
		{"内存", NewMemKV()},
		{"bbolt", openTestKV(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boom := errors.New("故意失败")
			err := tc.kv.InTx(true, func(tx Tx) error {
				if serr := tx.Set([]byte("k1"), []byte("v1")); serr != nil {
					return serr
				}
				return boom
			})
			if !errors.Is(err, boom) {
				t.Fatalf("InTx 返回 %v，期望把 fn 的错误交回来", err)
			}
			if _, gerr := tc.kv.Get([]byte("k1")); !errors.Is(gerr, ErrNotFound) {
				t.Errorf("回滚之后键还在，err=%v", gerr)
			}
		})
	}
}

// openTestKV 开一个落在临时目录里的 bbolt 存储。
//
// 两个实现都要测：MemKV 的覆盖层与 bbolt 的事务是两段独立的代码，
// 而只验内存那一个等于没验真正跑的那一个。
func openTestKV(t *testing.T) KV {
	t.Helper()
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}
