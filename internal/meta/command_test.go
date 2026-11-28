package meta

import (
	"testing"
	"time"
)

// testPatience 是测试等**真实时钟**的上限：死锁探测器，不是性能断言。
//
// 原先这里等 2 秒，而 `make cover-check` 把整包带覆盖率插桩再跑一遍、CI 又是
// 4 核共享 runner，于是「等两条变更到达」会在实现完全正常时超时（2026-11-28
// 的门禁就红在这一条）。等不到说明通道真的没发出来，等得到就只是慢——
// 中间没有需要被这个值拦住的情形。
const testPatience = 30 * time.Second

// RequiresDisconnect 的真值表。
//
// 这个判定决定一次元数据变更会不会把设备踢下线，两个方向都错不起：
// 漏判是安全问题（凭据已移走而权限还在），误判是可用性问题
// （一次无关的写入把全网设备踢掉）。
func TestRequiresDisconnectTruthTable(t *testing.T) {
	for _, c := range []struct {
		name string
		ch   Change
		want bool
	}{
		// 吊销一族（PKI-030）
		{"激活转吊销", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertRevoked}, true},
		{"激活转停用", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertInactive}, true},
		{"删除可连接的证书", Change{Kind: CmdDeleteCert,
			PrevStatus: CertActive}, true},
		{"删除本就不可连接的证书", Change{Kind: CmdDeleteCert,
			PrevStatus: CertRevoked}, false},
		{"停用转激活", Change{Kind: CmdSetCert,
			PrevStatus: CertInactive, Status: CertActive}, false},

		// 绑定一族（PKI-109…111）
		{"改绑到另一台设备", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertActive,
			PrevThing: "a", Thing: "b"}, true},
		{"解除绑定", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertActive,
			PrevThing: "a", Thing: ""}, true},
		{"绑定到同一台设备", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertActive,
			PrevThing: "a", Thing: "a"}, false},
		// 原本没有绑定：连接的 AuthzCtx 在握手时已固定为"无设备"，
		// 新增绑定不会让它多拿到任何东西
		{"从无绑定到有绑定", Change{Kind: CmdSetCert,
			PrevStatus: CertActive, Status: CertActive,
			PrevThing: "", Thing: "a"}, false},
		// 本就不可连接时没有活连接可踢
		{"不可连接时改绑", Change{Kind: CmdSetCert,
			PrevStatus: CertRevoked, Status: CertRevoked,
			PrevThing: "a", Thing: "b"}, false},

		// 其他命令不触发
		{"设备变更", Change{Kind: CmdSetThing, Thing: "a"}, false},
		{"策略集变更", Change{Kind: CmdSetPolicySet}, false},
		{"空操作", Change{Kind: CmdNop}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.ch.RequiresDisconnect(); got != c.want {
				t.Errorf("RequiresDisconnect() = %v，想要 %v", got, c.want)
			}
		})
	}
}

// 吊销与改绑同时发生时，仍须断连（PKI-112）。
//
// 两条判定不能互相短路：先判出"状态仍可连接"就直接返回 false 的写法
// 会漏掉"既改了状态又改了绑定"的情形。
func TestRequiresDisconnectOnCombinedChange(t *testing.T) {
	ch := Change{
		Kind: CmdSetCert, PrevStatus: CertActive, Status: CertRevoked,
		PrevThing: "a", Thing: "b",
	}
	if !ch.RequiresDisconnect() {
		t.Error("同时吊销并改绑时须断连")
	}
}

// putCert 须把旧的设备绑定一并返回，否则上面的判定拿不到 PrevThing。
func TestPutCertReturnsPrevThing(t *testing.T) {
	s := NewLocalStore(LocalOptions{})
	// Changes 是累积通道，先取出来，否则读到的是上一条变更
	changes := s.Changes()
	fp := CertFingerprint{1, 2, 3}
	for _, thing := range []string{"a", "b"} {
		if _, err := s.Apply(t.Context(), Command{Kind: CmdSetCert, Cert: &CertCmd{
			Fingerprint: fp, Status: CertActive, Thing: thing,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	var ch Change
	for i := 0; i < 2; i++ {
		select {
		case ch = <-changes:
		case <-time.After(testPatience):
			t.Fatalf("只收到 %d 条变更，期望 2 条", i)
		}
	}
	if ch.PrevThing != "a" {
		t.Errorf("PrevThing = %q，想要 \"a\"", ch.PrevThing)
	}
	if ch.Thing != "b" {
		t.Errorf("Thing = %q，想要 \"b\"", ch.Thing)
	}
	if !ch.RequiresDisconnect() {
		t.Error("改绑须要求断连")
	}
}
