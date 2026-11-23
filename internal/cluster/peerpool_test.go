package cluster

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestOwnProtocolReusesOneConnection 连接必须复用（CLU-038）。
//
// 判据是**服务端接了几条连接**，不是「消息有没有送到」：每条消息现拨一条
// TLS 的那个版本功能完全正常，所有既有用例都是绿的，差别只在吞吐——那个
// 版本跨节点只有 2000 条/秒，接池之后零丢弃到 6 万条/秒（这两个数都在
// docs/baseline/throughput.md 的前后对比表里，前者是缺陷期的数）。
// 所以这条用例要数握手，而握手只有被连的那一侧数得到。
func TestOwnProtocolReusesOneConnection(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["b"].SetMeshHandler(func(NodeID, MeshPayload) {})

	const sends = 50
	for i := 0; i < sends; i++ {
		if err := groups["a"].SendMesh("b", MeshPayload{
			Frames: []Frame{{Version: FrameVersion, Origin: "a", Topic: "t", QoS: 0}},
		}); err != nil {
			t.Fatalf("第 %d 条 mesh 消息失败: %v", i, err)
		}
	}
	if got := groups["b"].ownAccepted.Load(); got != 1 {
		t.Fatalf("%d 次问答用掉 %d 条连接，期望 1 条（CLU-038 要长连接）", sends, got)
	}
}

// TestOwnProtocolRetriesOnDeadPooledConnection 复用的连接坏掉时，幂等的
// 那几类要换一条新连接重试。
//
// 对端在空闲期关掉连接是正常事件（peerServerIdleTimeout），不该让一条
// mesh 帧因此丢掉——消息面是至少一次语义，重发一次是安全的。
func TestOwnProtocolRetriesOnDeadPooledConnection(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["b"].SetMeshHandler(func(NodeID, MeshPayload) {})
	a := groups["a"]

	payload := MeshPayload{Frames: []Frame{{Version: FrameVersion, Origin: "a", Topic: "t"}}}
	if err := a.SendMesh("b", payload); err != nil {
		t.Fatal(err)
	}
	killPooled(t, a, "b")

	if err := a.SendMesh("b", payload); err != nil {
		t.Fatalf("池里那条连接已死时应当换一条重试，得到: %v", err)
	}
	if got := groups["b"].ownAccepted.Load(); got != 2 {
		t.Fatalf("服务端接了 %d 条连接，期望 2 条（第一条被杀，第二条是重试）", got)
	}
}

// TestProposeIsNotRetriedOnDeadPooledConnection 写入转发**不**自动重试。
//
// 这条用例钉的是一个刻意的决定，不是一个优化：一次失败的问答既可能是
// 「请求没到」，也可能是「到了、应用了，应答丢了」。自动重发第二种就是把
// 一条命令应用两遍，而命令不带 ID，幂等窗口认不出它们是同一件事。
// 失败照旧交回调用方（ErrNoQuorum → 503），而**下一次调用必须自愈**——
// 坏连接已经被丢出池子，所以第二次走的是新连接。
func TestProposeIsNotRetriedOnDeadPooledConnection(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["b"].SetMeshHandler(func(NodeID, MeshPayload) {})
	a := groups["a"]

	// 先把一条连接放进池子。
	if err := a.SendMesh("b", MeshPayload{
		Frames: []Frame{{Version: FrameVersion, Origin: "a", Topic: "t"}},
	}); err != nil {
		t.Fatal(err)
	}
	killPooled(t, a, "b")

	if err := a.forward("b", []byte("x")); err != ErrNoQuorum {
		t.Fatalf("第一次转发应当失败在 ErrNoQuorum（不重试），得到: %v", err)
	}
	// 第二次：池子里已经没有那条坏连接，必须重新拨号并拿到一个真实答复
	// （b 不是 leader 时是 ErrNotLeader，是 leader 时是 nil）——只要不再是
	// ErrNoQuorum，就说明这一跳本身通了。
	if err := a.forward("b", []byte("x")); err == ErrNoQuorum {
		t.Fatal("第二次转发仍然 ErrNoQuorum：坏连接没有被丢出池子")
	}
}

// TestShardProposeIsNotRetriedOnDeadPooledConnection 会话分片的直达
// leader 写入与元数据写入遵守同一条不重试边界。
func TestShardProposeIsNotRetriedOnDeadPooledConnection(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["b"].SetMeshHandler(func(NodeID, MeshPayload) {})
	a := groups["a"]

	if err := a.SendMesh("b", MeshPayload{
		Frames: []Frame{{Version: FrameVersion, Origin: "a", Topic: "t"}},
	}); err != nil {
		t.Fatal(err)
	}
	killPooled(t, a, "b")
	accepted := groups["b"].ownAccepted.Load()

	if _, err := a.forwardShard("b", shardSessionBase, []byte("x")); err != ErrNoQuorum {
		t.Fatalf("死连接上的分片提案应当不重试并返回 ErrNoQuorum，得到: %v", err)
	}
	if got := groups["b"].ownAccepted.Load(); got != accepted {
		t.Fatalf("分片写入在死连接后自动重试了：入站 %d -> %d", accepted, got)
	}
	// 下一次独立调用要能新建连接并拿到对端的真实答复。
	if _, err := a.forwardShard("b", shardSessionBase, []byte("x")); err == ErrNoQuorum {
		t.Fatal("第二次分片提案仍然 ErrNoQuorum：坏连接没有被丢出池子")
	}
}

// 拨号失败发生在任何请求字节写出之前，可以由分片层安全地等待新 leader；
// 它必须与死连接上的模糊失败分开，否则换届期间每个新请求都会立即 503。
func TestShardProposeMarksDialFailureAsUnsent(t *testing.T) {
	g := &TLSGroup{
		id: "a", peers: map[NodeID]string{"b": "127.0.0.1:1"},
		dialTO: 20 * time.Millisecond, reqTO: 20 * time.Millisecond,
	}
	if _, err := g.forwardShard("b", shardSessionBase, []byte("x")); !errors.Is(err, ErrRequestUnsent) {
		t.Fatalf("拨号失败 = %v，期望确定未发送", err)
	}
}

func TestShadowWriteMarksDialFailureAsUnsent(t *testing.T) {
	g := &TLSGroup{
		id: "a", peers: map[NodeID]string{"b": "127.0.0.1:1"},
		dialTO: 20 * time.Millisecond, reqTO: 20 * time.Millisecond,
	}
	if _, err := g.ForwardShadowWrite("b", []byte(`{"v":1}`)); !errors.Is(err, ErrRequestUnsent) {
		t.Fatalf("影子RPC拨号失败 = %v，期望确定未发送", err)
	}
}

// 影子写已经到达对端后，应答超时属于“可能已经提交”的模糊结果：调用方要
// 尽快释放 MQTT 工作协程，但传输层绝不能换连接自动再发一次。设备只有携带
// version 才能在收到失败后自行确认或重试。
func TestShadowWriteTimeoutIsBoundedAndNotRetried(t *testing.T) {
	groups, _ := newTLSPair(t)
	a, b := groups["a"], groups["b"]

	// 先完成握手并把连接放进池中，让被测超时只覆盖应答等待，而不是
	// 把本机 TLS 调度抖动混进这个语义测试。
	b.SetShadowHandlers(func(raw []byte) ([]byte, error) { return raw, nil }, nil)
	if _, err := a.ForwardShadowWrite("b", []byte("warmup")); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	b.SetShadowHandlers(func(raw []byte) ([]byte, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return raw, nil
	}, nil)
	a.dialTO = 40 * time.Millisecond

	result := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := a.ForwardShadowWrite("b", []byte("possibly-committed"))
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("影子写未到达对端")
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrNoQuorum) {
			close(release)
			t.Fatalf("应答超时 = %v，期望模糊失败 ErrNoQuorum", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("影子写未受 dial timeout 约束")
	}
	elapsed := time.Since(started)
	close(release)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("影子写超时耗时 %v，未按短预算返回", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("模糊影子写被自动发送 %d 次，期望 1 次", got)
	}
}

func TestDrainClosesInboundOwnConnections(t *testing.T) {
	groups, _ := newTLSPair(t)
	a, b := groups["a"], groups["b"]
	b.SetMeshHandler(func(NodeID, MeshPayload) {})
	if err := a.SendMesh("b", MeshPayload{Frames: []Frame{{
		Version: FrameVersion, Origin: "a", Topic: "t",
	}}}); err != nil {
		t.Fatal(err)
	}
	// 再放一条同代空闲连接。第一条失败必须把它一起淘汰；否则第二次
	// 独立写仍会撞上陈旧连接，拿不到新连接上的明确 ErrDraining。
	pc, err := a.dialOwn("b", a.addrOf("b"))
	if err != nil {
		t.Fatal(err)
	}
	a.pool.put("b", pc)
	deadline := time.Now().Add(time.Second)
	for b.ownAccepted.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.ownAccepted.Load() < 2 {
		t.Fatal("第二条节点连接未在drain前完成入站登记")
	}
	b.Drain()
	// 池中连接在对端 drain 时被主动关闭。写请求不能在这条模糊边界
	// 自动重发，因此第一次必须立即失败；第二次新连接得到明确未接收的
	// ErrDraining，供 ShardGroup 安全等待新 leader。
	if _, err := a.forwardShard("b", shardSessionBase, []byte("x")); err != ErrNoQuorum {
		t.Fatalf("drain 断开的在途边界 = %v，期望 ErrNoQuorum", err)
	}
	if _, err := a.forwardShard("b", shardSessionBase, []byte("x")); err != ErrDraining {
		t.Fatalf("drain 后的新请求 = %v，期望 ErrDraining", err)
	}
}

// killPooled 把池子里通往 to 的那条连接掐断，再放回池子。
//
// 模拟的是「对端按空闲关掉了它，而本侧还不知道」——这是接连接池之后
// 唯一新增的失败形态，两条用例都要用它。
func killPooled(t *testing.T, g *TLSGroup, to NodeID) {
	t.Helper()
	pc := g.pool.take(to, g.addrOf(to))
	if pc == nil {
		t.Fatal("池子里没有连接，前一步的问答没有归还它")
	}
	pc.close()
	g.pool.put(to, pc)
}

// TestPeerPoolDropsStaleAndCapsSize 池子自己的三条规则：按地址判废、
// 按空闲时限判废、有容量上限。
//
// 地址那一条最容易被当成多余：成员变更把对端换到新地址之后，池里那几条
// 还连着旧地址，而症状是「对端在，但就是不回话」，直到空闲期过完。
func TestPeerPoolDropsStaleAndCapsSize(t *testing.T) {
	var pl peerPool
	defer pl.closeAll()

	mk := func(addr string) *peerConn {
		c, other := net.Pipe()
		t.Cleanup(func() { _ = c.Close(); _ = other.Close() })
		return &peerConn{c: c, addr: addr}
	}

	// 容量上限。
	for i := 0; i < peerPoolPerNode+4; i++ {
		pl.put("b", mk("1.1.1.1:1"))
	}
	pl.mu.Lock()
	n := len(pl.idle["b"])
	pl.mu.Unlock()
	if n != peerPoolPerNode {
		t.Fatalf("池里有 %d 条，期望上限 %d 条", n, peerPoolPerNode)
	}

	// 地址不符：一条都不该被取出来。
	if pc := pl.take("b", "2.2.2.2:2"); pc != nil {
		t.Fatal("地址不符的连接不该被取出")
	}
	pl.mu.Lock()
	n = len(pl.idle["b"])
	pl.mu.Unlock()
	if n != 0 {
		t.Fatalf("地址不符的 %d 条连接没有被丢掉", n)
	}

	// 空闲过久。
	old := mk("1.1.1.1:1")
	old.idleSince = time.Now().Add(-2 * peerIdleTimeout)
	pl.mu.Lock()
	pl.idle["b"] = []*peerConn{old}
	pl.mu.Unlock()
	if pc := pl.take("b", "1.1.1.1:1"); pc != nil {
		t.Fatal("空闲过久的连接不该被取出")
	}

	// 正常的那条要取得出来，而且取完池子就空了。
	fresh := mk("1.1.1.1:1")
	pl.put("b", fresh)
	if pc := pl.take("b", "1.1.1.1:1"); pc != fresh {
		t.Fatalf("取出的不是刚放进去那条: %v", pc)
	}
	if pc := pl.take("b", "1.1.1.1:1"); pc != nil {
		t.Fatal("池子应当已经空了")
	}

	// drop 之后池里也该是空的。
	pl.put("b", mk("1.1.1.1:1"))
	pl.drop("b")
	if pc := pl.take("b", "1.1.1.1:1"); pc != nil {
		t.Fatal("drop 之后不该还取得出连接")
	}
}
