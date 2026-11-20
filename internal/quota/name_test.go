package quota

import "testing"

// TestLookupName 钉住名字到编号的查找（QTA-109 的配置覆盖靠它）。
func TestLookupName(t *testing.T) {
	for _, tc := range []struct {
		name string
		want ID
	}{
		{"connections", Connections},
		{"connect_rate", ConnectRate},
		{"payload_size", PayloadSize},
		{"subs_per_conn", SubsPerConn},
		{"handshake_rate", HandshakeRate},
	} {
		got, ok := LookupName(tc.name)
		if !ok {
			t.Fatalf("%q 没查到", tc.name)
		}
		if got != tc.want {
			t.Fatalf("%q 查到编号 %d，应为 %d", tc.name, got, tc.want)
		}
	}
	for _, bad := range []string{"", "connection", "Connections", "unknown_quota"} {
		if _, ok := LookupName(bad); ok {
			t.Fatalf("%q 不应查到；大小写与单复数都不做宽容匹配，"+
				"否则配置里的错拼会被悄悄接受", bad)
		}
	}
}

// TestNames 钉住名字清单覆盖全表且有序。
//
// 有序是因为它要进配置错误的提示文本：顺序随机会让同一个错误两次报出
// 不同的字符串。
func TestNames(t *testing.T) {
	names := Names()
	if len(names) != QuotaCount() {
		t.Fatalf("名字清单 %d 项，配额表 %d 项", len(names), QuotaCount())
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("名字清单未按字典序：%q 在 %q 之前", names[i-1], names[i])
		}
	}
	// 每个名字都必须能查回一个编号：两个方向不一致意味着表里有重名。
	seen := make(map[ID]bool, len(names))
	for _, n := range names {
		id, ok := LookupName(n)
		if !ok {
			t.Fatalf("%q 在清单里却查不到编号", n)
		}
		if seen[id] {
			t.Fatalf("编号 %d 对应了多个名字", id)
		}
		seen[id] = true
	}
}

// TestOverridesApply 钉住覆盖确实改掉生效值，且速率型的 burst 跟着走。
//
// burst 跟着走是必需的：令牌桶的容量若仍是默认值，把速率调高之后的突发
// 仍按旧容量削顶，现象是"配了两万却只放行三千的瞬时量"。
func TestOverridesApply(t *testing.T) {
	l := NewLocal(Options{
		Now:       func() int64 { return 0 },
		Overrides: map[ID]int{Connections: 1_000_000, ConnectRate: 20_000},
	})
	if got := l.Limit(Connections); got != 1_000_000 {
		t.Fatalf("connections 生效值 %d", got)
	}
	if got := l.Limit(ConnectRate); got != 20_000 {
		t.Fatalf("connect_rate 生效值 %d", got)
	}
	if got := l.defs[ConnectRate].Burst; got != 20_000 {
		t.Fatalf("connect_rate 的 burst 为 %d，应随 limit 一起抬到 20000", got)
	}
	// 未被覆盖的项保持默认。
	if got, want := l.Limit(Subscriptions), defaults[Subscriptions].Limit; got != want {
		t.Fatalf("subscriptions 被动了：%d，应为 %d", got, want)
	}
}

// TestOverrideZeroMeansUnlimited 钉住 0 的语义是"取消限制"而不是"禁止一切"。
//
// 这条值得单独写：把一项配额配成 0 却得到"全部拒绝"，是最容易被误解的
// 一种失败，而两种解释都讲得通，所以必须由用例固定下来。
func TestOverrideZeroMeansUnlimited(t *testing.T) {
	l := NewLocal(Options{
		Now:       func() int64 { return 0 },
		Overrides: map[ID]int{Connections: 0},
	})
	for i := 0; i < defaults[Connections].Limit+10; i++ {
		if !l.Acquire(Connections, "", 1) {
			t.Fatalf("第 %d 次占用被拒，0 应表示不限制", i)
		}
	}
}
