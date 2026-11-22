package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
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
	var gotEpoch, gotVersion uint64
	var gotChanges []ThingChange
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
		if p.ThingEpoch != 0 {
			gotEpoch, gotVersion = p.ThingEpoch, p.ThingVersion
			gotChanges = p.ThingChanges
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
		ThingEpoch: 9, ThingFrom: 3, ThingVersion: 4,
		ThingChanges: []ThingChange{{Name: "lamp-2", Present: true}},
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
	if gotEpoch != 9 || gotVersion != 4 || len(gotChanges) != 1 ||
		gotChanges[0].Name != "lamp-2" || !gotChanges[0].Present {
		t.Errorf("归属增量未完整往返: epoch=%d version=%d changes=%+v",
			gotEpoch, gotVersion, gotChanges)
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
	// 对端没装查询处理函数：那一个节点的结果带 Err，整体仍是 nil
	// （ARCH-041 的部分失败）
	res, err := tp.Query(ctx, []NodeID{"b"}, AdminQuery{Kind: "conns"})
	if err != nil {
		t.Fatalf("扇出整体不该失败: %v", err)
	}
	if len(res) != 1 || res[0].Err == nil {
		t.Errorf("= %+v，期望那个节点带 Err", res)
	}
	if err := tp.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if err := tp.Forward(ctx, []NodeID{"b"}, []Frame{{Topic: "x"}}); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Forward = %v，期望 ErrClosed", err)
	}
	if _, err := tp.Query(ctx, nil, AdminQuery{}); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后 Query = %v，期望 ErrClosed", err)
	}
}

func TestMeshTransportForwardIsAsyncBatchedAndOwnsBytes(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sent := make(chan []Frame, 4)
	var calls atomic.Int32
	tp := newMeshTransport(nil, "a", func(_ NodeID, p MeshPayload) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		sent <- append([]Frame(nil), p.Frames...)
		return nil
	})
	tp.coalesce = 0
	defer tp.Close()

	payload := []byte("original")
	if err := tp.Forward(context.Background(), []NodeID{"b"}, []Frame{{Topic: "first", Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("同步发送 worker 没有启动")
	}
	// Forward 返回后调用方可以立即复用解码缓冲。
	copy(payload, "mutated!")
	for i := 0; i < 20; i++ {
		if err := tp.Forward(context.Background(), []NodeID{"b"}, []Frame{{Topic: "queued"}}); err != nil {
			t.Fatal(err)
		}
	}
	close(release)

	first := <-sent
	if got := string(first[0].Payload); got != "original" {
		t.Fatalf("异步队列没有接管 payload：%q", got)
	}
	select {
	case batch := <-sent:
		if len(batch) <= 1 {
			t.Fatalf("积压的帧没有合批：len=%d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("积压批次没有发出")
	}
}

func TestMeshTransportBackpressureIsPerPeerAndBounded(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	tp := newMeshTransport(nil, "a", func(_ NodeID, _ MeshPayload) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	tp.coalesce = 0
	tp.queueFrames = 2
	tp.queueBytes = 1 << 20

	if err := tp.Forward(context.Background(), []NodeID{"slow"}, []Frame{{Topic: "1"}}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := tp.Forward(context.Background(), []NodeID{"slow"}, []Frame{{Topic: "2"}}); err != nil {
		t.Fatal(err)
	}
	if err := tp.Forward(context.Background(), []NodeID{"slow"}, []Frame{{Topic: "3"}}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("满队列 = %v，期望 ErrBackpressure", err)
	}
	// slow 满了不影响为另一对端创建独立队列。
	if err := tp.Forward(context.Background(), []NodeID{"healthy"}, []Frame{{Topic: "x"}}); err != nil {
		t.Fatalf("慢对端阻塞了健康对端: %v", err)
	}
	close(release)
	if err := tp.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMeshTransportRetriesHeadBatch(t *testing.T) {
	delivered := make(chan struct{}, 1)
	var calls atomic.Int32
	tp := newMeshTransport(nil, "a", func(_ NodeID, _ MeshPayload) error {
		if calls.Add(1) == 1 {
			return errors.New("短暂故障")
		}
		delivered <- struct{}{}
		return nil
	})
	tp.coalesce = 0
	defer tp.Close()
	if err := tp.Forward(context.Background(), []NodeID{"b"}, []Frame{{Topic: "retry"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
		if calls.Load() != 2 {
			t.Fatalf("发送次数 = %d，期望 2", calls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("失败的队首批次没有重试")
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
	return newTLSCluster(t, []NodeID{"a", "b"})
}

func newTLSCluster(t *testing.T, ids []NodeID) (map[NodeID]*TLSGroup, map[NodeID]string) {
	t.Helper()
	ca, caKey, caPEM := mintCA(t)
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
