package server

import (
	"context"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/session"
	"github.com/itmisx/open-iot-core/internal/store"
)

// sessionSwitch 是会话存储的后置切换点。
//
// 为什么需要它：broker 的参数在 New 时就定了，而会话分区组要等复制组
// 起来之后才有（分片建在它的 NodeHost 上）。装配顺序没法倒过来——分片
// 需要 NodeHost，NodeHost 需要监听地址与证书，那些在 New 的后半段才就位。
//
// 于是 broker 一律拿到这个壳，集群模式下在 startReplicationGroup 里把
// 复制实现挂进来。单机模式下它永远是直通，开销是一次原子读。
//
// 不写成"装配期间报错"（像 lateGroup 那样）：会话写入在 CONNECT 路径
// 上，而 HTTP 与 MQTT 监听在复制组之后才开，所以这个窗口里不会有写入。
// 真要在那一刻有写入，直通到本地存储是正确行为而不是错误——单机就是
// 这么跑的。
type sessionSwitch struct {
	local *store.SessionStore
	repl  atomic.Pointer[store.ReplicatedSessionStore]
}

var _ session.Store = (*sessionSwitch)(nil)

func newSessionSwitch(local *store.SessionStore) *sessionSwitch {
	return &sessionSwitch{local: local}
}

// use 把复制实现挂上。只在集群装配期调用一次。
func (s *sessionSwitch) use(r *store.ReplicatedSessionStore) { s.repl.Store(r) }

// target 返回当前生效的实现。
func (s *sessionSwitch) target() session.Store {
	if r := s.repl.Load(); r != nil {
		return r
	}
	return s.local
}

func (s *sessionSwitch) Load(ctx context.Context, clientID string,
) (*session.State, bool, error) {
	// RF=all 时复制实现仍会直接读本地；RF<节点数时，非副本 Access 节点
	// 必须经它的合批只读路径去 session cell。强制读 local 会把远端已有
	// 持久会话误判成不存在，并让 CLU-147 的远程读合批成为死代码。
	return s.target().Load(ctx, clientID)
}

// LoadClaimed 让 broker 把刚取得的 epoch 交给复制会话层，以命中 Claim
// 应答携带的一次性快照。单机或尚未切换时与普通 Load 完全相同。
func (s *sessionSwitch) LoadClaimed(ctx context.Context, clientID string,
	epoch cluster.Epoch,
) (*session.State, bool, error) {
	if r := s.repl.Load(); r != nil {
		return r.LoadClaimed(ctx, clientID, epoch)
	}
	return s.local.Load(ctx, clientID)
}

func (s *sessionSwitch) Save(ctx context.Context, st *session.State) error {
	return s.target().Save(ctx, st)
}

func (s *sessionSwitch) Replace(ctx context.Context, st *session.State) error {
	return s.target().Replace(ctx, st)
}

func (s *sessionSwitch) Delete(ctx context.Context, clientID string,
	e cluster.Epoch,
) error {
	return s.target().Delete(ctx, clientID, e)
}

func (s *sessionSwitch) Expire(ctx context.Context, nowUnixMs int64) (int, error) {
	return s.target().Expire(ctx, nowUnixMs)
}

func (s *sessionSwitch) AppendQueued(clientID string, m session.QueuedMsg) error {
	return s.target().AppendQueued(clientID, m)
}

func (s *sessionSwitch) TakeQueued(clientID string) ([]session.QueuedMsg, error) {
	return s.target().TakeQueued(clientID)
}

func (s *sessionSwitch) proposalQueueStats() store.SessionProposalQueueStats {
	if r := s.repl.Load(); r != nil {
		return r.ProposalQueueStats()
	}
	return store.SessionProposalQueueStats{}
}

// Count 报告本地已持久化的会话数。
func (s *sessionSwitch) Count() int { return s.local.Count() }
