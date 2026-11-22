package server

import (
	"context"
	"errors"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
	"github.com/itmisx/open-iot-core/internal/store"
)

// recordingProposer 是一条"立刻应用"的会话分片日志，只为验证接线。
//
// 它不验共识——那半在 internal/cluster 与 tests/cluster/run.sh。这里要
// 回答的是另一件事：broker 拿到的那个壳有没有真的切到复制实现上。
// 切漏了的后果最隐蔽：单机与集群都"能用"，而集群下每个节点各写自己的
// 盘，于是会话不跨节点——正是这一整件事要修的那个症状。
type recordingProposer struct {
	local *store.SessionStore
	ops   int
	index uint64
}

type remoteRecordingProposer struct {
	*recordingProposer
	reads int
	err   error
}

func (p *remoteRecordingProposer) HasLocalReplica(string) bool { return false }
func (p *remoteRecordingProposer) ReadSession(string) ([]byte, bool, error) {
	p.reads++
	return nil, false, p.err
}

func (p *recordingProposer) ShardOf(string) uint64 { return 16 }

func (p *recordingProposer) ShardIDs() []uint64 { return []uint64{16} }

func (p *recordingProposer) Propose(clientID string, data []byte) (uint64, error) {
	return p.ProposeShard(16, data)
}

func (p *recordingProposer) ProposeShard(shard uint64, data []byte) (uint64, error) {
	p.ops++
	p.index++
	return p.local.ApplySessionCmd(shard, p.index, data, func(string) bool { return true })
}

func TestSessionSwitchPassesThroughUntilClusterIsWiredUp(t *testing.T) {
	local := store.NewSessionStore(store.NewMemKV())
	sw := newSessionSwitch(local)
	ctx := context.Background()

	st := &session.State{ClientID: "dev-1", Epoch: 1, ExpiryInterval: 3600}
	if err := sw.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := sw.AppendQueued("dev-1", session.QueuedMsg{
		Topic: "t", Payload: []byte("a"), QoS: 1,
	}); err != nil {
		t.Fatalf("AppendQueued: %v", err)
	}
	got, _, err := local.Load(ctx, "dev-1")
	if err != nil || len(got.Queued) != 1 {
		t.Fatalf("单机下写入没有落到本地存储: %v %+v", err, got)
	}
	if n := sw.Count(); n != 1 {
		t.Fatalf("Count = %d", n)
	}
}

func TestSessionSwitchRoutesWritesThroughTheShardLog(t *testing.T) {
	local := store.NewSessionStore(store.NewMemKV())
	sw := newSessionSwitch(local)
	prop := &recordingProposer{local: local}
	sw.use(store.NewReplicatedSessionStore(local, prop))
	ctx := context.Background()

	st := &session.State{ClientID: "dev-1", Epoch: 2, ExpiryInterval: 3600}
	if err := sw.Save(ctx, st); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := sw.AppendQueued("dev-1", session.QueuedMsg{
		Topic: "t", Payload: []byte("a"), QoS: 1,
	}); err != nil {
		t.Fatalf("AppendQueued: %v", err)
	}
	msgs, err := sw.TakeQueued("dev-1")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("TakeQueued: %v 条数 %d", err, len(msgs))
	}
	if _, err := sw.Expire(ctx, 1); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	// 四种写入各一条命令（take 也是一条：它要把删除复制出去）。
	if prop.ops != 4 {
		t.Fatalf("只有 %d 条命令进了复制日志，期望 4：有写入绕过了它", prop.ops)
	}

	// 读仍然是本地读（契约 05：连接路径不发 RPC）。
	before := prop.ops
	if _, _, err := sw.Load(ctx, "dev-1"); err != nil {
		t.Fatal(err)
	}
	if prop.ops != before {
		t.Fatal("Load 提了命令：读不应经过复制层")
	}

	// 围栏判定要原样交回调用方，不能被壳吞掉。
	stale := &session.State{ClientID: "dev-1", Epoch: 1, ExpiryInterval: 3600}
	if err := sw.Save(ctx, stale); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("旧 epoch 的写入应回 ErrStaleEpoch，得到 %v", err)
	}
	if err := sw.Delete(ctx, "dev-1", 9); !errors.Is(err, cluster.ErrStaleEpoch) {
		t.Fatalf("非当前 epoch 的删除应回 ErrStaleEpoch，得到 %v", err)
	}
}

func TestSessionSwitchRoutesNonReplicaLoadThroughClusterReader(t *testing.T) {
	local := store.NewSessionStore(store.NewMemKV())
	sw := newSessionSwitch(local)
	wantErr := errors.New("remote session read reached")
	prop := &remoteRecordingProposer{
		recordingProposer: &recordingProposer{local: local},
		err:               wantErr,
	}
	sw.use(store.NewReplicatedSessionStore(local, prop))

	if _, _, err := sw.Load(context.Background(), "remote-dev"); !errors.Is(err, wantErr) {
		t.Fatalf("Load 错误 = %v，期望远程 reader 的 %v", err, wantErr)
	}
	if prop.reads != 1 {
		t.Fatalf("远程读取次数 = %d，期望 1", prop.reads)
	}
}
