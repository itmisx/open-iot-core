package broker

import (
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// 遗嘱消息（PRD MQTT-060 至 MQTT-064）。
//
// 遗嘱集中在这一个文件里，是因为它的正确性取决于三处配合：CONNECT 时
// 记下、连接终止时判断要不要发、发之前还要等多久。此前这三块分散在
// connect.go、conn.go 与 pubsub.go，而缺口恰好出在它们的接缝上——
// 只有"客户端主动要求发遗嘱"那一条路径被接上了。

// applyWill 从 CONNECT 记下遗嘱。
func (c *conn) applyWill(st *session.State, cp *packet.Connect) {
	if cp.Will == nil {
		st.HasWill = false
		st.Will = session.WillMsg{}
		return
	}
	st.HasWill = true
	st.Will = session.WillMsg{
		Topic:   string(cp.Will.Topic),
		Payload: append([]byte(nil), cp.Will.Payload...),
		QoS:     uint8(cp.Will.QoS),
		Retain:  cp.Will.Retain,
	}
	if c.ver == packet.V5 {
		st.Will.DelaySeconds = cp.Will.Props.WillDelay
		st.Will.ExpirySeconds = cp.Will.Props.MessageExpiry
	}
}

// flushWill 在连接终止时了结遗嘱（MQTT-064，MQTT-3.1.2.5）。
//
// 遗嘱存在的全部理由就是"设备没能说再见"：掉电、断网、保活超时、
// 协议错误被踢。规范因此要求，除了收到 DISCONNECT 之外的任何连接关闭
// 都要发遗嘱。
//
// 判据是 HasWill 而不是原因码：正常 DISCONNECT 已由 handleDisconnect
// 清掉它，剩下的就都是"没说再见"。用原因码判断做不到这件事——对端直接
// 关套接字走的是 closeGraceful，拿到的原因码与正常断开完全相同。
//
// 在 persist 之前调用：持久化会把清洁会话从路由表摘掉，之后再投递遗嘱
// 就少了一步可用的路由。
func (c *conn) flushWill() {
	if c.rt == nil {
		return
	}
	st := c.rt.State()
	if !st.HasWill {
		return
	}
	// 先记成"已了结"，再决定怎么发。遗嘱至多发一次，而 close 是幂等的
	// 入口、handleDisconnect 是另一个——把这件事记在状态里，两条路径就
	// 不可能叠加成两条遗嘱。
	st.HasWill = false
	w := cloneWill(st.Will)

	// 遗嘱主题要过发布授权（MQTT-068）。
	//
	// 遗嘱是客户端在 CONNECT 里指定的任意主题，由服务端替它发。不判的话，
	// 一台只被允许发自己名下主题的设备，可以把遗嘱设到别的设备的命令主题
	// 上，然后断一次线就让服务端代发——凭据是自己的，权限却是别人的。
	//
	// 判定必须在这里做而不是定时器触发时：延迟发布时连接早已不在，
	// 主体与策略也跟着没了；而正确的策略本来就是连接认证时的那一份。
	//
	// 丢弃而不是拒绝连接，与 AWS 一致。c.allowed 会产出带完整轨迹的拒绝
	// 事件，所以丢弃是看得见的——否则运维只会看到"设备下线却没有下线消息"
	// 而无从查起。
	if !c.allowedWill(w) {
		return
	}

	delay := willDelayMs(st)
	if delay <= 0 {
		c.b.publishWill(c, w)
		return
	}
	c.b.scheduleWill(c.clientID, c.epoch, w, delay)
}

// allowedWill 判定遗嘱主题是否被策略放行。
//
// 带 retain 的遗嘱按 iot:RetainPublish 判，与普通的保留发布同一条规则
// （PRD AUTH-020）：遗嘱的 retain 会在主题上留下一条持久的消息，
// 权限要求没有理由比设备自己发一条更松。
func (c *conn) allowedWill(w session.WillMsg) bool {
	act := authz.ActPublish
	if w.Retain {
		act = authz.ActRetainPublish
	}
	return c.allowed(act, w.Topic)
}

// willDelayMs 推导遗嘱的发布延迟（MQTT-061，MQTT-3.1.3.2.2）。
//
// 取「延迟间隔」与「会话过期间隔」的较小者：规范要求遗嘱在延迟走完
// 或会话结束时发出，以先到者为准。这个取小值让"会话先结束"这一半不需要
// 额外的过期轮询器——当前的会话过期是惰性判定的，没有可挂的调度。
//
// 会话过期为 0 时会话随连接一起结束，遗嘱即刻发出，延迟不起作用。
// MQTT 3.1.1 没有延迟间隔，DelaySeconds 恒为 0，也走即刻那条路径。
func willDelayMs(st *session.State) int64 {
	d := int64(st.Will.DelaySeconds) * 1000
	if d <= 0 {
		return 0
	}
	if exp := int64(st.ExpiryInterval) * 1000; exp < d {
		return exp
	}
	return d
}

func cloneWill(w session.WillMsg) session.WillMsg {
	w.Payload = append([]byte(nil), w.Payload...)
	w.Props = append([]byte(nil), w.Props...)
	return w
}

// pendingWill 是一条等待延迟发布的遗嘱。
//
// 记下 epoch 是为了分辨"同一个 clientId 的哪一次断开"：设备可能在延迟
// 窗口里连上又断开，那时旧的待发遗嘱已经被取消，新的一条用新 epoch 登记。
// 定时器回调拿着自己那一次的 epoch 来核对，对不上就什么也不做。
type pendingWill struct {
	epoch cluster.Epoch
	stop  func() bool
}

// scheduleWill 安排延迟发布。
func (b *Broker) scheduleWill(clientID string, epoch cluster.Epoch,
	w session.WillMsg, delayMs int64,
) {
	b.willMu.Lock()
	defer b.willMu.Unlock()
	if b.closed.Load() {
		return
	}
	if prev, ok := b.pendingWills[clientID]; ok {
		prev.stop()
	}
	pw := pendingWill{epoch: epoch}
	pw.stop = b.opt.AfterFunc(time.Duration(delayMs)*time.Millisecond, func() {
		b.fireWill(clientID, epoch, w)
	})
	b.pendingWills[clientID] = pw
}

// fireWill 是延迟到期后的发布。
func (b *Broker) fireWill(clientID string, epoch cluster.Epoch, w session.WillMsg) {
	b.willMu.Lock()
	cur, ok := b.pendingWills[clientID]
	// 取消与"被下一次断开取代"都在这里被挡住。停表与回调之间存在固有的
	// 竞态（表可能已经在跑），所以不能只靠 stop() 的返回值。
	if !ok || cur.epoch != epoch {
		b.willMu.Unlock()
		return
	}
	delete(b.pendingWills, clientID)
	b.willMu.Unlock()
	b.publishWill(nil, w)
}

// cancelWill 取消待发的遗嘱。
//
// 客户端在延迟走完之前回来了，规范要求不得发出（MQTT-3.1.2.5）：
// 延迟的用意正是"短暂掉线不要惊动别人"。
func (b *Broker) cancelWill(clientID string) {
	b.willMu.Lock()
	pw, ok := b.pendingWills[clientID]
	if ok {
		delete(b.pendingWills, clientID)
	}
	b.willMu.Unlock()
	if ok {
		pw.stop()
	}
}

// dropPendingWills 在关闭时停掉全部待发遗嘱。
//
// 不在关闭时补发：此刻订阅方也正在被断开，发出去没有人收，
// 而定时器留着会在 broker 已经关闭之后触碰路由表。
func (b *Broker) dropPendingWills() {
	b.willMu.Lock()
	pending := make([]pendingWill, 0, len(b.pendingWills))
	for _, pw := range b.pendingWills {
		pending = append(pending, pw)
	}
	b.pendingWills = make(map[string]pendingWill)
	b.willMu.Unlock()
	for _, pw := range pending {
		pw.stop()
	}
}

// publishWill 把遗嘱投递出去。
//
// from 为发布者所在连接，仅用于 No Local 判定；延迟发布时连接早已不在，
// 传 nil——route 对平台自己发布的消息本就支持这一点。
func (b *Broker) publishWill(from *conn, w session.WillMsg) {
	msg := &router.Message{
		Topic:          w.Topic,
		Payload:        w.Payload,
		Props:          w.Props,
		QoS:            w.QoS,
		Retain:         w.Retain,
		StoredAtUnixMs: b.opt.Now(),
	}
	if w.Retain {
		b.opt.Router.Retained().Set(msg.Topic, msg, b.opt.Router.NextVersion())
	}
	b.route(from, msg, nil)
}
