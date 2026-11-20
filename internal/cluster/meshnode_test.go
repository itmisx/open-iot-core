package cluster

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"time"
)

// TestMeshRidesTheReplicationConnection mesh 消息走复制组那条 TLS 连接。
//
// 复用而不是另开一条：节点之间只有一条互信通道，另开的代价是运维要配
// 两个端口两套证书，而安全边界完全一样。
func TestMeshRidesTheReplicationConnection(t *testing.T) {
	groups, _ := newTLSPair(t)

	var mu sync.Mutex
	var gotFrom NodeID
	var gotFrames []Frame
	var gotFilters, gotThings []string
	done := make(chan struct{}, 4)
	groups["b"].SetMeshHandler(func(from NodeID, p MeshPayload) {
		mu.Lock()
		gotFrom = from
		if p.Frames != nil {
			gotFrames = p.Frames
		}
		if p.Filters != nil {
			gotFilters = p.Filters
		}
		if p.Things != nil {
			gotThings = p.Things
		}
		mu.Unlock()
		done <- struct{}{}
	})

	frame := Frame{
		Version: FrameVersion, Origin: "a", Seq: 7,
		Topic: "telemetry/site-1/temp", Payload: []byte("25.5"), QoS: 1,
	}
	if err := groups["a"].SendMesh("b", MeshPayload{Frames: []Frame{frame}}); err != nil {
		t.Fatal(err)
	}
	if err := groups["a"].SendMesh("b", MeshPayload{
		Filters: []string{"telemetry/#"}, Things: []string{"lamp-1"},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("mesh 消息没有送达")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if gotFrom != "a" {
		t.Errorf("来源 = %q，期望 a", gotFrom)
	}
	if len(gotFrames) != 1 || gotFrames[0].Topic != frame.Topic ||
		string(gotFrames[0].Payload) != "25.5" || gotFrames[0].Seq != 7 {
		t.Errorf("帧 = %+v", gotFrames)
	}
	if len(gotFilters) != 1 || gotFilters[0] != "telemetry/#" {
		t.Errorf("兴趣 = %v", gotFilters)
	}
	if len(gotThings) != 1 || gotThings[0] != "lamp-1" {
		t.Errorf("归属 = %v", gotThings)
	}
}

// TestMeshRefusesWithoutHandler 没人处理 mesh 消息时要明确报错。
//
// 静默回 OK 会让发送方以为对端收到了，而那条消息从此不存在。
func TestMeshRefusesWithoutHandler(t *testing.T) {
	groups, _ := newTLSPair(t)
	// b 没有设处理函数
	err := groups["a"].SendMesh("b", MeshPayload{
		Frames: []Frame{{Version: FrameVersion, Topic: "x"}},
	})
	if err == nil {
		t.Fatal("对端没有处理函数时应当报错")
	}
}

// TestMeshTransportForwardSkipsSelfAndEmpty 自己不转给自己，空批次不发。
func TestMeshTransportForwardSkipsSelfAndEmpty(t *testing.T) {
	groups, _ := newTLSPair(t)
	tp := NewMeshTransport(groups["a"])
	if tp.Self() != "a" {
		t.Fatalf("Self = %q", tp.Self())
	}
	ctx := context.Background()
	// 空批次：不该发，也不该报错
	if err := tp.Forward(ctx, []NodeID{"b"}, nil); err != nil {
		t.Errorf("空批次 = %v", err)
	}
	// 只发给自己：跳过（自己的订阅者在本地投递路径里已经收到了，
	// 再转一次会让同一条消息投两遍）
	if err := tp.Forward(ctx, []NodeID{"a"},
		[]Frame{{Version: FrameVersion, Topic: "x"}}); err != nil {
		t.Errorf("只发给自己 = %v", err)
	}
	if err := tp.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	// 对端没装查询处理函数：那一个节点的结果带 Err，整体仍是 nil
	// （ARCH-041 的部分失败）
	res, err := tp.Query(ctx, []NodeID{"b"}, AdminQuery{Kind: "conns"})
	if err != nil {
		t.Fatalf("扇出整体不该失败: %v", err)
	}
	if len(res) != 1 || res[0].Err == nil {
		t.Errorf("= %+v，期望那个节点带 Err", res)
	}
}

// TestMeshTransportDelivery 收到的帧交给回调。
func TestMeshTransportDelivery(t *testing.T) {
	groups, _ := newTLSPair(t)
	tp := NewMeshTransport(groups["b"])
	got := make(chan []Frame, 1)
	tp.OnReceive(func(_ NodeID, batch []Frame) { got <- batch })
	groups["b"].SetMeshHandler(func(from NodeID, p MeshPayload) {
		tp.Deliver(from, p.Frames)
	})

	if err := groups["a"].SendMesh("b", MeshPayload{
		Frames: []Frame{{Version: FrameVersion, Topic: "telemetry/x", Payload: []byte("v")}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-got:
		if len(batch) != 1 || batch[0].Topic != "telemetry/x" {
			t.Errorf("= %+v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("帧没有交给回调")
	}
}

// newTLSPair 起两个真 TLS 的进程内节点，a 当选 leader。
func newTLSPair(t *testing.T) (map[NodeID]*TLSGroup, map[NodeID]string) {
	t.Helper()
	ca, caKey, caPEM := mintCA(t)
	ids := []NodeID{"a", "b"}
	addrs := map[NodeID]string{}
	cfgs := map[NodeID]*tls.Config{}
	for i, id := range ids {
		addrs[id] = freeAddr(t)
		cfgs[id] = mintNode(t, ca, caKey, caPEM, string(id), int64(i+20))
	}
	groups := map[NodeID]*TLSGroup{}
	for i, id := range ids {
		g := startTLSGroup(t, id, addrs, cfgs[id], i == 0, newTestFSM())
		groups[id] = g
	}
	return groups, addrs
}

// TestPeersExcludesSelf 对端列表不含自己。
//
// 含自己的话，通告与转发都会多走一次本机往返——而本机那条本来就在
// 本地投递路径里处理过了。
func TestPeersExcludesSelf(t *testing.T) {
	groups, _ := newTLSPair(t)
	peers := groups["a"].Peers()
	if len(peers) != 1 || peers[0] != "b" {
		t.Fatalf("a 的对端 = %v，期望只有 b", peers)
	}
}
