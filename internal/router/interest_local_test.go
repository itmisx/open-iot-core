package router

import (
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
)

// 本节点要通告给集群的那份兴趣（阶段 D）。
//
// 它错掉的方式都是静默的：少一条过滤器，别的节点就不会把消息转过来，
// 而本节点的订阅者一直在等；多一条，消息白转一趟（无害但浪费）。
// 真正危险的是**提前少一条**——订阅者还在，兴趣已经撤了。

func newInterestRig() *Router {
	return New(Options{Self: "n1", Limits: topic.DefaultLimits()})
}

// TestLocalWildcardsOnlyTracksWildcards 精确订阅不进通告。
//
// 设备域主题的归属由名字算出，百万条精确订阅不跨节点传播——
// 这是本项目相对通用 broker 的主要性能杠杆（ARCH-017）。
func TestLocalWildcardsOnlyTracksWildcards(t *testing.T) {
	r := newInterestRig()
	subs := []string{
		"telemetry/#",
		"$aws/things/+/shadow/update/accepted",
		"devices/lamp-1/cmd", // 精确，不该进
		"$aws/things/lamp-1/shadow/get/accepted",
	}
	for _, f := range subs {
		if err := r.Subscribe(Subscription{ClientID: "c1", Filter: f}); err != nil {
			t.Fatalf("订阅 %s: %v", f, err)
		}
	}
	got := r.LocalWildcards()
	want := []string{"$aws/things/+/shadow/update/accepted", "telemetry/#"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("= %v，期望 %v（精确订阅不该进通告）", got, want)
	}
}

// TestLocalWildcardsCountsReferences 两个客户端订阅同一个过滤器时，
// 一个退订不该把这条兴趣从集群里撤掉。
//
// 这是「提前少一条」的那个危险形状：撤早了，另一个客户端就再也收不到
// 跨节点的消息，而它的订阅还在本地订阅树里。
func TestLocalWildcardsCountsReferences(t *testing.T) {
	r := newInterestRig()
	for _, cid := range []string{"c1", "c2"} {
		if err := r.Subscribe(Subscription{ClientID: cid, Filter: "telemetry/#"}); err != nil {
			t.Fatal(err)
		}
	}
	if !r.Unsubscribe("c1", "telemetry/#") {
		t.Fatal("退订应当成功")
	}
	if got := r.LocalWildcards(); len(got) != 1 {
		t.Fatalf("一个客户端退订后 = %v，期望兴趣仍在（c2 还订着）", got)
	}
	if !r.Unsubscribe("c2", "telemetry/#") {
		t.Fatal("退订应当成功")
	}
	if got := r.LocalWildcards(); len(got) != 0 {
		t.Fatalf("最后一个退订后 = %v，期望空", got)
	}
}

// TestLocalWildcardsIgnoresDuplicateSubscribe 重复订阅同一过滤器只算一次。
//
// 订阅树对重复订阅是就地替换（见 TestDuplicateSubscribeDoesNotLeakQuota
// 那条教训），引用计数必须跟着就地替换——否则退订一次就少算一次，
// 兴趣提前消失。
func TestLocalWildcardsIgnoresDuplicateSubscribe(t *testing.T) {
	r := newInterestRig()
	for i := 0; i < 3; i++ {
		if err := r.Subscribe(Subscription{ClientID: "c1", Filter: "telemetry/#", QoS: uint8(i % 2)}); err != nil {
			t.Fatal(err)
		}
	}
	if !r.Unsubscribe("c1", "telemetry/#") {
		t.Fatal("退订应当成功")
	}
	if got := r.LocalWildcards(); len(got) != 0 {
		t.Fatalf("= %v，期望空（三次订阅只该算一次）", got)
	}
}

// TestLocalWildcardsClearedWithSession 会话结束时兴趣跟着撤掉。
//
// 不撤的后果是别的节点一直往这边转消息，而订阅者已经走了——
// 那些消息在这边被丢掉，看起来像"消息莫名丢失"。
func TestLocalWildcardsClearedWithSession(t *testing.T) {
	r := newInterestRig()
	for _, f := range []string{"telemetry/#", "events/+/raw", "devices/lamp-1/cmd"} {
		if err := r.Subscribe(Subscription{ClientID: "c1", Filter: f}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Subscribe(Subscription{ClientID: "c2", Filter: "telemetry/#"}); err != nil {
		t.Fatal(err)
	}
	if n := r.RemoveClient("c1"); n != 3 {
		t.Fatalf("移除了 %d 条订阅，期望 3", n)
	}
	// c2 还订着 telemetry/#，这条兴趣要留着
	got := r.LocalWildcards()
	if len(got) != 1 || got[0] != "telemetry/#" {
		t.Fatalf("= %v，期望只剩 c2 的 telemetry/#", got)
	}
	if n := r.RemoveClient("c2"); n != 1 {
		t.Fatalf("移除了 %d 条", n)
	}
	if got := r.LocalWildcards(); len(got) != 0 {
		t.Fatalf("= %v，期望空", got)
	}
}

// TestUnsubscribeUnknownFilterDoesNotDropInterest 退订一条没订过的过滤器
// 不得影响兴趣计数。
//
// 第一版按「过滤器带不带通配符」判断该不该减，于是
// 「订阅时已经有了所以没计数、退订时却减了一次」会让兴趣提前消失。
func TestUnsubscribeUnknownFilterDoesNotDropInterest(t *testing.T) {
	r := newInterestRig()
	if err := r.Subscribe(Subscription{ClientID: "c1", Filter: "telemetry/#"}); err != nil {
		t.Fatal(err)
	}
	// c2 从没订过，退订应当返回 false 且不动计数
	if r.Unsubscribe("c2", "telemetry/#") {
		t.Error("退订没订过的过滤器应当返回 false")
	}
	if got := r.LocalWildcards(); len(got) != 1 {
		t.Fatalf("= %v，期望兴趣仍在", got)
	}
}

// TestGossipLocatorPrefersLocal 本节点有这台设备就判本地。
//
// 判成远端的后果是消息绕一圈回来（或者更糟：对端也认为在这边，
// 两个节点互相转发同一条消息）。
func TestGossipLocatorPrefersLocal(t *testing.T) {
	remote := cluster.NewRemoteThings("n1")
	remote.Advertise("n2", []string{"lamp-1", "lamp-2"})
	loc := GossipLocator{
		Self:   "n1",
		Local:  func(thing []byte) bool { return string(thing) == "lamp-1" },
		Remote: remote,
	}
	if node, ok := loc.LocateThing([]byte("lamp-1")); !ok || node != "n1" {
		t.Errorf("本节点承载的设备 = %q, %v，期望 n1", node, ok)
	}
	if node, ok := loc.LocateThing([]byte("lamp-2")); !ok || node != "n2" {
		t.Errorf("远端设备 = %q, %v，期望 n2", node, ok)
	}
	// 谁都没有：返回 false，而不是随便挑一个节点转过去
	if node, ok := loc.LocateThing([]byte("lamp-9")); ok {
		t.Errorf("谁都没有的设备 = %q, %v，期望 false", node, ok)
	}
}
