package server

import (
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/router"
)

// 保留消息跨节点同步的收敛不变量（TST-155）。
//
// 这里不起真的三节点：那一套在 tests/cluster/run.sh（要 openssl、要等
// 选举，不进门禁）。可以分叉的东西只有一个——**覆盖判据**，所以这条用例
// 把两个真的 router.RetainedStore 接在真的转换函数两头，再把投递刻意打乱
// 并重复，然后断言两边逐条相同。
//
// 判据是"两边的集合相同"，不是"两边各有一条"：后者在分叉时同样成立，
// 证明不了任何东西。

// retainedNode 是模拟里的一个节点：一个真路由器加一条发往对端的队列。
type retainedNode struct {
	id    string
	rt    *router.Router
	srv   *Server
	outTo *[]cluster.RetainedUpdate
}

func newRetainedNode(t *testing.T, id string, out *[]cluster.RetainedUpdate) *retainedNode {
	t.Helper()
	rt := router.New(router.Options{
		Self: cluster.NodeID(id), Limits: topic.DefaultLimits(),
		Now: func() int64 { return 1 },
	})
	srv := &Server{
		rt:  rt,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// 容量给足：这条用例测的是定序，不是丢弃（丢弃另有用例）。
		retaineds: make(chan cluster.RetainedUpdate, 4096),
	}
	rt.Retained().SetReplicator(srv.broadcastRetained)
	return &retainedNode{id: id, rt: rt, srv: srv, outTo: out}
}

// drain 把本节点排出的广播搬到发往对端的队列里。
func (n *retainedNode) drain() {
	for {
		select {
		case u := <-n.srv.retaineds:
			*n.outTo = append(*n.outTo, u)
		default:
			return
		}
	}
}

// snapshot 取本节点全部条目（含墓碑）的可比较形式。
func (n *retainedNode) snapshot() map[string]string {
	out := map[string]string{}
	for _, m := range n.rt.Retained().All() {
		kind := "live:" + string(m.Payload)
		if m.Tombstone() {
			kind = "tomb"
		}
		out[m.Topic] = kind + "@" + itoaU(m.Version.Counter) + ":" + m.Version.Node
	}
	return out
}

func itoaU(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

func TestRetainedConvergesAcrossNodes(t *testing.T) {
	topics := []string{"a/1", "a/2", "b/1"}
	// 固定种子集，与确定性模拟同一个路子：失败时可直接复现。
	for _, seed := range []int64{1, 2, 3, 7, 11, 23, 101} {
		t.Run("seed="+itoaU(uint64(seed)), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			var aToB, bToA []cluster.RetainedUpdate
			a := newRetainedNode(t, "node-a", &aToB)
			b := newRetainedNode(t, "node-b", &bToA)
			nodes := []*retainedNode{a, b}

			for step := 0; step < 200; step++ {
				n := nodes[rng.Intn(len(nodes))]
				name := topics[rng.Intn(len(topics))]
				switch rng.Intn(3) {
				case 0, 1:
					// 本地发布一条 retained。
					n.rt.Retained().Set(name, &router.Message{
						Topic:   name,
						Payload: []byte(n.id + "-" + itoaU(uint64(step))),
					}, n.rt.NextVersion())
				default:
					// 本地清除。
					n.rt.Retained().Set(name, &router.Message{Topic: name},
						n.rt.NextVersion())
				}
				n.drain()

				// 投递一部分，刻意**乱序并重复**：收端必须靠 Version 定序，
				// 而不是靠到达顺序。
				if rng.Intn(2) == 0 {
					deliverSome(rng, &aToB, b)
				}
				if rng.Intn(2) == 0 {
					deliverSome(rng, &bToA, a)
				}
			}

			// 收敛窗口：把两边的队列彻底排空，互投到不再有新东西。
			// 这几轮同样打乱顺序——两个节点收到的是同一批更新而顺序不同，
			// 这正是"谁后到谁赢"会分叉、而确定性全序不会的那个情形。
			for i := 0; i < 8; i++ {
				a.drain()
				b.drain()
				deliverAll(rng, &aToB, b)
				deliverAll(rng, &bToA, a)
			}

			sa, sb := a.snapshot(), b.snapshot()
			if len(sa) != len(sb) {
				t.Fatalf("条数不同: a=%d b=%d\na=%v\nb=%v", len(sa), len(sb), sa, sb)
			}
			keys := make([]string, 0, len(sa))
			for k := range sa {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if sa[k] != sb[k] {
					t.Fatalf("主题 %q 分叉: a=%q b=%q（覆盖判据不是确定性全序时就会这样）",
						k, sa[k], sb[k])
				}
			}
		})
	}
}

// deliverSome 投递队列里的一部分，顺序打乱，并有一定概率重复投递一条。
func deliverSome(rng *rand.Rand, q *[]cluster.RetainedUpdate, to *retainedNode) {
	if len(*q) == 0 {
		return
	}
	n := rng.Intn(len(*q)) + 1
	batch := (*q)[:n]
	*q = (*q)[n:]
	rng.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
	to.srv.applyRemoteRetained(batch)
	// 重复投递一次：广播会重复（事件广播与 dump 会撞在一起），
	// 合并必须是幂等的。
	if rng.Intn(3) == 0 {
		to.srv.applyRemoteRetained(batch)
	}
	// 远端更新**不得**再被广播回去，否则两个节点之间无限弹。
	to.drainExpectEmpty()
}

func deliverAll(rng *rand.Rand, q *[]cluster.RetainedUpdate, to *retainedNode) {
	if len(*q) == 0 {
		return
	}
	batch := *q
	*q = nil
	rng.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
	to.srv.applyRemoteRetained(batch)
	to.drainExpectEmpty()
}

// drainExpectEmpty 丢掉应用远端更新后本不该存在的广播。
//
// 它不断言为空，而是把它们丢掉——断言放在 TestRemoteRetainedIsNotReflected
// 里单独做，这样这条用例失败时指向的是收敛，不是转发。
func (n *retainedNode) drainExpectEmpty() {
	for {
		select {
		case <-n.srv.retaineds:
		default:
			return
		}
	}
}

// TestRemoteRetainedIsNotReflected 钉住"远端来的更新不再广播回去"。
//
// 反过来写的后果是两个节点之间无限弹（与帧的不再转发同理，CLU-077），
// 而现象是 mesh 被一条 retained 打满。
func TestRemoteRetainedIsNotReflected(t *testing.T) {
	var out []cluster.RetainedUpdate
	n := newRetainedNode(t, "node-a", &out)
	n.srv.applyRemoteRetained([]cluster.RetainedUpdate{{
		Topic: "a/1", Payload: []byte("x"), Counter: 9, Node: "node-b",
	}})
	select {
	case u := <-n.srv.retaineds:
		t.Fatalf("远端更新被广播回去了: %+v", u)
	default:
	}
	// 顺带验 Lamport 的接收规则：本地计数器被抬到对端之上（MQTT-123）。
	if v := n.rt.NextVersion(); v.Counter <= 9 {
		t.Fatalf("收到远端版本 9 之后的下一个版本 = %d，应大于 9；"+
			"不抬的后果是本节点新发的 retained 被更老的远端那条盖掉", v.Counter)
	}
}

// TestRetainedBroadcastDropsAreCounted 钉住队列满时的计数（MQTT-126）。
//
// 丢弃本身是允许的（发布路径不能等网络），但它是这条路径上唯一的外部
// 信号——不计数就等于"部分节点缺几条 retained"这件事没有任何地方能看出来。
func TestRetainedBroadcastDropsAreCounted(t *testing.T) {
	var out []cluster.RetainedUpdate
	n := newRetainedNode(t, "node-a", &out)
	// 换一条容量为 1 的队列，第二条起必丢。
	n.srv.retaineds = make(chan cluster.RetainedUpdate, 1)
	for i := 0; i < 5; i++ {
		n.rt.Retained().Set("a/1", &router.Message{
			Topic: "a/1", Payload: []byte("x"),
		}, n.rt.NextVersion())
	}
	if got := n.srv.retainedDrops.Load(); got != 4 {
		t.Fatalf("丢弃计数 = %d，期望 4", got)
	}
}
