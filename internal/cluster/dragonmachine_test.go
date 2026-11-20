package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// fakeCold 是用例里的"冷层"：一份数据加一个持久化的 applied index。
//
// 它模拟的正是 CLU-109 定下的那条规矩——**先写记录、再写 index**，
// 两者在同一份状态里，重启时一起回来。
type fakeCold struct {
	mu      sync.Mutex
	entries []string // 按应用顺序记下每条命令
	indexes []uint64 // 与 entries 对应的日志序号
	applied uint64   // 已持久化到哪一条
}

func (f *fakeCold) apply(index uint64, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index <= f.applied {
		// 与 meta.LocalStore.ApplyCommitted 同一条判定：重放到已应用的
		// 那一段时跳过。用例里它**不该**被触发——dragonboat 根本不会
		// 把那些推过来，这里留着是为了让"真被推过来了"能看出来。
		f.entries = append(f.entries, "REPLAY:"+string(data))
		return nil
	}
	f.entries = append(f.entries, string(data))
	f.indexes = append(f.indexes, index)
	f.applied = index
	return nil
}

func (f *fakeCold) index() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

func (f *fakeCold) snapshot(w io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if _, err := io.WriteString(w, e+"\n"); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeCold) restore(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = nil
	for _, line := range bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n")) {
		if len(line) > 0 {
			f.entries = append(f.entries, string(line))
		}
	}
	return nil
}

func (f *fakeCold) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.entries...)
	return out
}

// startSingleNode 起一个单节点的 dragonboat 组，状态机是 metaMachine。
//
// 单节点仍然走真的共识路径（与 ARCH-057 的"单机也走一副本组"同一个
// 理由）：提案要进日志、要提交、要经状态机，只是多数派恰好是一个人。
// addr 要跨重启保持不变：dragonboat 把 RaftAddress 记进数据目录，
// 换了地址再打开会报 "not the owner of the data directory"。
// 这不是用例的怪癖，是部署上的约束——见 dragonmachine.go 顶上的说明。
func startSingleNode(t *testing.T, dir, addr string, cold *fakeCold) *dragonboat.NodeHost {
	t.Helper()
	factory := &dragonTransportFactory{
		// 单节点不拨号；给一个会报错的拨号器，万一它真拨了就能看出来。
		dial: func(string) (net.Conn, error) {
			return nil, errors.New("单节点不该拨号")
		},
	}
	nhc := dbconfig.NodeHostConfig{
		NodeHostDir: dir, RTTMillisecond: 50, RaftAddress: addr,
	}
	nhc.Expert.TransportFactory = factory
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		t.Fatalf("NewNodeHost 失败: %v", err)
	}
	machine := &metaMachine{
		apply: func(index uint64, data []byte) (uint64, error) {
			return 0, cold.apply(index, data)
		},
		appliedIndex: cold.index,
		snapshotTo:   cold.snapshot,
		restoreFrom:  cold.restore,
	}
	cfg := dbconfig.Config{
		ReplicaID: 1, ShardID: 1,
		ElectionRTT: 10, HeartbeatRTT: 1, CheckQuorum: true,
	}
	if err := nh.StartOnDiskReplica(map[uint64]dragonboat.Target{1: addr}, false,
		func(uint64, uint64) sm.IOnDiskStateMachine { return machine }, cfg); err != nil {
		nh.Close()
		t.Fatalf("StartOnDiskReplica 失败: %v", err)
	}
	return nh
}

func proposeWait(t *testing.T, nh *dragonboat.NodeHost, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		_, err = nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte(payload))
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("提交 %q 失败: %v", payload, err)
}

// TestOnDiskMachineHonoursPersistedIndex 钉住适配器的支点（CLU-109）。
//
// 重启之后 dragonboat 向 Open 要 applied index，并且**只重放它之后的
// 那一段**。两条判据：
//
//   - 重启后不重复应用已经应用过的命令（否则冷层里会出现 REPLAY: 前缀）；
//   - 重启后新提交的命令照样到达，且序号接着来（不丢中间那一段）。
//
// 这条用例在 Open 报 0 的实现上会**红**——那时整条日志被重放一遍，
// 第一条判据立刻失败。那正是 P1 存在的理由。
func TestOnDiskMachineHonoursPersistedIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nh")
	addr := freeLocalAddr(t)
	cold := &fakeCold{}

	nh := startSingleNode(t, dir, addr, cold)
	proposeWait(t, nh, "a")
	proposeWait(t, nh, "b")
	beforeIdx := cold.index()
	before := cold.seen()
	if len(before) != 2 {
		t.Fatalf("重启前应用了 %v，期望两条", before)
	}
	nh.Close()

	// 换一个进程内的 NodeHost，同一个目录、同一份"冷层"。
	nh2 := startSingleNode(t, dir, addr, cold)
	defer nh2.Close()
	proposeWait(t, nh2, "c")

	got := cold.seen()
	for _, e := range got {
		if len(e) > 7 && e[:7] == "REPLAY:" {
			t.Fatalf("重启后重放了已应用的日志：%v；"+
				"Open 报的 applied index 没被采纳（或报了 0）", got)
		}
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("应用序列 = %v，期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("应用序列 = %v，期望 %v", got, want)
		}
	}
	if cold.index() <= beforeIdx {
		t.Fatalf("重启后 applied index = %d，未超过重启前的 %d",
			cold.index(), beforeIdx)
	}
	if !sort.SliceIsSorted(cold.indexes, func(i, j int) bool {
		return cold.indexes[i] < cold.indexes[j]
	}) {
		t.Fatalf("日志序号不是严格递增的：%v", cold.indexes)
	}
}
