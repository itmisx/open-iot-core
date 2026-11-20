package cluster

import "testing"

// TestAdvertiseReplacesWholeSnapshot 通告是全量替换，不是增量。
//
// 增量的后果是：丢一条「设备走了」的通告，这台设备就永远被记成在那个
// 节点上，消息一直转给它而那边早就没有这个连接了。全量快照是幂等的，
// 丢一次或乱序都会在下一次通告时自愈（ARCH-012）。
func TestAdvertiseReplacesWholeSnapshot(t *testing.T) {
	rt := NewRemoteThings("a")
	rt.Advertise("b", []string{"lamp-1", "lamp-2"})
	if got, ok := rt.Locate([]byte("lamp-1")); !ok || got != "b" {
		t.Fatalf("lamp-1 = %q ok=%v，期望 b", got, ok)
	}
	if rt.Len() != 2 || rt.Nodes() != 1 {
		t.Fatalf("设备数 = %d 节点数 = %d，期望 2 1", rt.Len(), rt.Nodes())
	}

	// 第二次通告里没有 lamp-1：它必须消失，而不是留着
	rt.Advertise("b", []string{"lamp-2"})
	if _, ok := rt.Locate([]byte("lamp-1")); ok {
		t.Error("上一次通告里的 lamp-1 没被撤掉——通告被当成增量处理了")
	}
	if got, ok := rt.Locate([]byte("lamp-2")); !ok || got != "b" {
		t.Errorf("lamp-2 = %q ok=%v", got, ok)
	}
	if rt.Len() != 1 {
		t.Errorf("设备数 = %d，期望 1", rt.Len())
	}

	// 空快照：这个节点不再承载任何设备，连它的条目一起清掉
	rt.Advertise("b", nil)
	if rt.Len() != 0 || rt.Nodes() != 0 {
		t.Errorf("空通告后设备数 = %d 节点数 = %d，期望 0 0", rt.Len(), rt.Nodes())
	}
}

// TestAdvertiseIgnoresSelf 自己的通告不进归属表。
//
// 进了的话，本机设备的消息会被"转发"给本机，走一圈网络回来——而本地
// 投递路径里已经处理过了，于是投两遍。
func TestAdvertiseIgnoresSelf(t *testing.T) {
	rt := NewRemoteThings("a")
	rt.Advertise("a", []string{"lamp-1"})
	if _, ok := rt.Locate([]byte("lamp-1")); ok {
		t.Error("本节点的设备不该出现在远端归属表里")
	}
	if rt.Nodes() != 0 {
		t.Errorf("节点数 = %d，期望 0", rt.Nodes())
	}
}

// TestLastAdvertiserWins 同一台设备被两个节点声明时以后到的为准。
//
// 这是设备重连到另一个节点的样子，而两边的通告有先后。指错的后果是
// 一条消息转给旧节点并在那里被丢掉；固守旧值的后果是新连上的设备
// 一直收不到消息，直到旧节点的通告过期——后者更糟。
func TestLastAdvertiserWins(t *testing.T) {
	rt := NewRemoteThings("a")
	rt.Advertise("b", []string{"lamp-1"})
	rt.Advertise("c", []string{"lamp-1"})
	if got, _ := rt.Locate([]byte("lamp-1")); got != "c" {
		t.Fatalf("lamp-1 = %q，期望后到的 c", got)
	}
	// b 重新通告一份不含 lamp-1 的快照：它不得把 c 的归属抹掉
	rt.Advertise("b", []string{"lamp-9"})
	if got, ok := rt.Locate([]byte("lamp-1")); !ok || got != "c" {
		t.Errorf("lamp-1 = %q ok=%v——b 的撤销误删了 c 的归属", got, ok)
	}
}

// TestForgetDropsOnlyThatNode 成员离开时只清它自己的归属。
func TestForgetDropsOnlyThatNode(t *testing.T) {
	rt := NewRemoteThings("a")
	rt.Advertise("b", []string{"lamp-1"})
	rt.Advertise("c", []string{"lamp-2"})
	rt.Forget("b")
	if _, ok := rt.Locate([]byte("lamp-1")); ok {
		t.Error("b 的归属没被清掉")
	}
	if got, ok := rt.Locate([]byte("lamp-2")); !ok || got != "c" {
		t.Errorf("c 的归属被连带清掉了: %q ok=%v", got, ok)
	}
	// 清一个从没通告过的节点：不该 panic，也不该动别人
	rt.Forget("zzz")
	if rt.Len() != 1 {
		t.Errorf("设备数 = %d，期望 1", rt.Len())
	}
}

// TestLocateMissIsNotAnError 查不到就是查不到。
//
// 返回一个零值 NodeID 而 ok=true 会让调用方把消息转给 ""，那是一次
// 必然失败的拨号。
func TestLocateMissIsNotAnError(t *testing.T) {
	rt := NewRemoteThings("a")
	if node, ok := rt.Locate([]byte("没这台")); ok || node != "" {
		t.Errorf("= %q ok=%v，期望空且 false", node, ok)
	}
}
