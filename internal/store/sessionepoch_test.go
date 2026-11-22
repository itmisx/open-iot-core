package store

import (
	"context"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
)

// TestEpochCollidesAcrossNodes 正面断言两个节点会为同一个 clientId 发出
// **相同**的 epoch（CLU-131）。
//
// 这条用例断言的是缺陷本身，而不是修好之后的样子——与
// TestAWSKeySecretIsOnDiskInClear 同一个用法（TST-153）：把一个刻意的
// 现状写成判据，否则它就是个没人知道的默认。
//
// 为什么会相同：集群下每个节点用的都是 cluster.LocalOwnership，它的
// term 恒为 1（单节点不会换 leader，所以那里不需要任期），序号由各节点
// 自己从 0 开始递增。于是"两个节点各自服务过同样多次连接"时，同一个
// clientId 在两边拿到的 epoch 一模一样——而相等的 epoch 之间没有先后，
// 契约 01 的围栏判的是 `<`，判不出来。
//
// 真正的修法是分片 ownership 服务（CLU-009：epoch 的高位用分片 leader
// 的 raft 任期），那一步要等 v0.4。接上之后这条用例会红，而它该红：
// 届时把它改成断言两者**不相等**。
func TestEpochCollidesAcrossNodes(t *testing.T) {
	ctx := context.Background()
	a := cluster.NewLocalOwnership("node-a", 8)
	b := cluster.NewLocalOwnership("node-b", 8)
	defer func() { _ = a.Close(); _ = b.Close() }()

	ea, err := a.Claim(ctx, "dev-1", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	eb, err := b.Claim(ctx, "dev-1", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if ea != eb {
		t.Fatalf("两个节点的 epoch 竟然不同（%d 与 %d）——"+
			"若是因为接上了分片 ownership 服务（CLU-009），"+
			"请把这条用例改成断言不相等，并一并更新 CLU-131", ea, eb)
	}
	if ea.Term() != 1 {
		t.Errorf("LocalOwnership 的 term = %d，应为 1", ea.Term())
	}
}

// TestEqualEpochLiveSessionWins 钉住 epoch 撞号时的平手判据：
// 已经断开的那一份不得覆盖还连着的那一份（CLU-131）。
//
// 去掉 staleAtEqualEpoch 这条判定，第一个子用例立刻变成"覆盖成功"，
// 而那就是设备的持久会话被一条早已被接管的旧连接悄悄盖掉——不报错，
// 现象是重连之后订阅回到了上一代。
func TestEqualEpochLiveSessionWins(t *testing.T) {
	ctx := context.Background()
	kv, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	s := NewSessionStore(kv)

	ep := cluster.MakeEpoch(1, 7)
	live := &session.State{
		ClientID: "dev-1", Epoch: ep, Owner: "node-b",
		ExpiryInterval: 3600, DisconnectUnixMs: 0,
	}
	if err := s.Save(ctx, live); err != nil {
		t.Fatalf("写入活着的那份失败: %v", err)
	}

	// 另一个节点上一条早已被接管的连接在拆解时落盘，epoch 恰好撞号。
	stale := &session.State{
		ClientID: "dev-1", Epoch: ep, Owner: "node-a",
		ExpiryInterval: 3600, DisconnectUnixMs: 1_700_000_000_000,
	}
	if err := s.Save(ctx, stale); err != cluster.ErrStaleEpoch {
		t.Fatalf("撞号的断开写入应被拒，得到 %v", err)
	}
	got, ok, err := s.Load(ctx, "dev-1")
	if err != nil || !ok {
		t.Fatalf("读回失败: %v %v", err, ok)
	}
	if got.Owner != "node-b" || got.DisconnectUnixMs != 0 {
		t.Errorf("存储里那份变成了 owner=%s disc=%d，应仍是 node-b 与 0",
			got.Owner, got.DisconnectUnixMs)
	}
	if n := s.EpochTies(); n != 1 {
		t.Errorf("撞号计数 = %d，应为 1", n)
	}

	// 反方向要放过去：活着的那份盖掉已断开的那份是正常的接管顺序。
	if err := s.Delete(ctx, "dev-1", ep); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, stale); err != nil {
		t.Fatalf("写入已断开的那份失败: %v", err)
	}
	if err := s.Save(ctx, live); err != nil {
		t.Fatalf("活着的那份应当能覆盖已断开的那份，得到 %v", err)
	}
}
