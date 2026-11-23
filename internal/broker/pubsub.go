package broker

import (
	"context"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// handleSubscribe 处理 SUBSCRIBE。
//
// 每个过滤器独立鉴权并独立返回一个原因码：部分失败是正常情形，
// 不能因为一条被拒就整包失败。
func (c *conn) handleSubscribe(sp *packet.Subscribe) error {
	ctx := context.Background()
	if !c.b.opt.Limiter.Within(quota.SubsPerRequest, len(sp.Filters)) {
		c.close(reason.QuotaExceeded, cluster.EvictAdministrative)
		return reason.Errorf(reason.QuotaExceeded, "单次订阅数超限")
	}
	if !c.b.opt.Limiter.AllowGlobal(ctx, quota.SubscribeRate, "", len(sp.Filters)) {
		c.close(reason.QuotaExceeded, cluster.EvictAdministrative)
		return reason.Errorf(reason.QuotaExceeded, "订阅速率超限")
	}

	ack := &packet.Suback{PacketID: sp.PacketID, Codes: make([]reason.Code, 0, len(sp.Filters))}
	granted := make([]router.Subscription, 0, len(sp.Filters))

	for _, f := range sp.Filters {
		filter := string(f.Filter)
		rc := c.grantSubscription(ctx, filter, f)
		ack.Codes = append(ack.Codes, rc)
		if rc.IsError() {
			// 被拒的订阅也要产出事件。
			//
			// 不发的后果是实打实的不可观测：一个非法过滤器导致的
			// 订阅失败既不会进 Subscription 事件（这里被跳过），
			// 也到不了授权判定（根本没走到），于是客户端拿到
			// SUBACK 0x80 而服务端的事件、日志、指标三处全是空的。
			c.emitSub(filter, uint8(f.QoS), false, rc)
			continue
		}
		sub := router.Subscription{
			ClientID:       c.clientID(),
			Filter:         filter,
			QoS:            uint8(f.QoS),
			NoLocal:        f.NoLocal,
			RetainAsPub:    f.RetainAsPublished,
			RetainHandling: f.RetainHandling,
			ID:             f.ID,
			Persistent:     c.rt.State().ExpiryInterval > 0,
		}
		granted = append(granted, sub)
	}
	if err := c.send(ack); err != nil {
		return err
	}

	// 先回 SUBACK 再下发 retained：客户端须先知道订阅已被接受。
	for _, sub := range granted {
		c.recordSubscription(sub)
		c.sendRetained(sub)
		c.emitSub(sub.Filter, sub.QoS, false, reason.Code(sub.QoS))
	}
	return nil
}

// grantSubscription 校验并鉴权一个过滤器，返回 SUBACK 原因码。
func (c *conn) grantSubscription(ctx context.Context, filter string, f packet.Subscription) reason.Code {
	if err := topic.ValidateFilter(f.Filter, c.b.opt.Limits); err != nil {
		var pe *reason.Error
		if ok := asReasonError(err, &pe); ok {
			return pe.Code
		}
		return reason.TopicFilterInvalid
	}
	// iot:Subscribe 校验 topicfilter/ 资源（PRD AUTH-023）。
	if !c.allowed(authz.ActSubscribe, filter) {
		return reason.NotAuthorized
	}
	// 重复订阅同一过滤器不再占一份配额。
	//
	// MQTT 允许客户端重复订阅以修改 QoS，路由表与会话都是就地替换；
	// 而配额此前每次都加一，永不对消。于是一个按常规在重连后重订阅的
	// 客户端，会一路吃掉全局订阅配额（上限 50 万，到顶之后**整个 broker**
	// 都不能再新增订阅），而它自始至终只持有一条订阅。
	// 这条是确定性模拟跑出来的：几百步之后计数器对不上。
	//
	// 两份额度各问各的权威，不能共用一个判定——这与 handleUnsubscribe
	// 的归还侧是同一条原则。每连接额度属会话，所以问会话；全局额度数的是
	// 路由表里的订阅条数，所以必须问路由表。此前两边都问会话，于是会话与
	// 路由表一旦不一致（接管、会话过期都会），这一条就被当成新订阅占掉
	// 一份全局额度，而路由表只是就地替换——额度自此永久多出一份。
	// 随机种子深扫报的「配额比路由表多」就是这里，而它只在深扫才触发，
	// 正因为前提是两者先得不一致。
	share, inner := splitShared(filter)
	acquiredConn := false
	if !c.rt.HasSubscription(inner, share) {
		if !c.b.opt.Limiter.Acquire(quota.SubsPerConn, c.clientID(), 1) {
			c.emitQuota(quota.SubsPerConn, c.clientID())
			return reason.QuotaExceeded
		}
		acquiredConn = true
	}
	if !c.b.opt.Router.Has(c.clientID(), filter) {
		// Subscriptions 是计数型配额，须用 Acquire 而非 AllowGlobal。
		//
		// 这里曾经用错：AllowGlobal 只处理速率型，对计数型一律拒绝并计数。
		// 契约 09 的作用域与种类分离正是为了让这类错误大声失败，
		// 而不是悄悄把配额放宽 N 倍。
		if !c.b.opt.Limiter.Acquire(quota.Subscriptions, "", 1) {
			if acquiredConn {
				c.b.opt.Limiter.Release(quota.SubsPerConn, c.clientID(), 1)
			}
			c.emitQuota(quota.Subscriptions, "")
			return reason.QuotaExceeded
		}
	}
	// QoS 2 是本实现的超集；AWS 严格模式下降级为 QoS 1。
	if f.QoS == packet.QoS2 && c.b.opt.Limits.AWSStrict {
		return reason.GrantedQoS1
	}
	return reason.Code(f.QoS)
}

// recordSubscription 把订阅写入路由表与会话状态。
func (c *conn) recordSubscription(sub router.Subscription) {
	share, inner := splitShared(sub.Filter)
	if err := c.b.opt.Router.Subscribe(sub); err != nil {
		// 装不进路由表就要把 grantSubscription 占下的额度还回去。
		//
		// 两份各自对账：额度是按"路由表/会话里还没有"占的，所以也按
		// 同一个判据还——重复订阅那次本来就没占，这里也就不会多还。
		//
		// 理论上到不了这里：过滤器在 grantSubscription 里已经用同一套
		// limits 校验过了。但"理论上到不了"的路径一旦真的走到（校验与
		// 入表两处的 limits 哪天不再是同一份），留下的是一份永久挂着、
		// 查不出来源的额度泄漏，而全局额度到顶会让整个节点不能再订阅。
		if !c.b.opt.Router.Has(sub.ClientID, sub.Filter) {
			c.b.opt.Limiter.Release(quota.Subscriptions, "", 1)
		}
		if !c.rt.HasSubscription(inner, share) {
			c.b.opt.Limiter.Release(quota.SubsPerConn, c.clientID(), 1)
		}
		return
	}
	_, _ = c.rt.Subscribe(c.epoch, session.Subscription{
		Filter:         inner,
		Share:          share,
		QoS:            sub.QoS,
		NoLocal:        sub.NoLocal,
		RetainAsPub:    sub.RetainAsPub,
		RetainHandling: sub.RetainHandling,
		ID:             sub.ID,
	})
}

// sendRetained 按订阅选项下发 retained 消息。
func (c *conn) sendRetained(sub router.Subscription) {
	// RetainHandling 为 2 表示订阅时不发送 retained（MQTT-3.8.3.1）。
	if sub.RetainHandling == 2 {
		return
	}
	for _, m := range c.b.opt.Router.RetainedFor([]byte(sub.Filter)) {
		// retained 的投递同样受 iot:Receive 鉴权约束。
		if !c.allowed(authz.ActReceive, m.Topic) {
			continue
		}
		ref := router.SubRef{ClientID: c.clientID(), QoS: sub.QoS, ID: sub.ID}
		c.deliverRetained(m, ref)
	}
}

// handleUnsubscribe 处理 UNSUBSCRIBE。
func (c *conn) handleUnsubscribe(up *packet.Unsubscribe) error {
	ack := &packet.Unsuback{PacketID: up.PacketID}
	for _, f := range up.Filters {
		filter := string(f)
		removed := c.b.opt.Router.Unsubscribe(c.clientID(), filter)
		share, inner := splitShared(filter)
		inSession, _ := c.rt.Unsubscribe(c.epoch, inner, share)
		// 两份额度各跟各的权威，不能共用一个条件。
		//
		// 每连接额度是会话的事，全局额度是路由表的事，而两者会不一致：
		// 接管、clean start 都可能让路由表里还留着上一代会话的订阅，而
		// 当前会话并不知道它。此前两份都挂在会话的结果上，于是"路由表
		// 删掉了、会话里没有"的那一次退订，全局额度就永久少一份——
		// 上限是全局 50 万，耗尽之后整个 broker 不能再新增订阅。
		// 这条是确定性模拟定位的：它每一步都在查"全局额度是否等于
		// 路由表订阅数"，分叉的那一步就是这里。
		if inSession {
			c.b.opt.Limiter.Release(quota.SubsPerConn, c.clientID(), 1)
		}
		if removed {
			c.b.opt.Limiter.Release(quota.Subscriptions, "", 1)
		}
		if removed {
			ack.Codes = append(ack.Codes, reason.Success)
			c.emitSub(filter, 0, true, reason.Success)
		} else {
			ack.Codes = append(ack.Codes, reason.NoSubscriptionExisted)
		}
	}
	return c.send(ack)
}

// handlePublish 处理入站 PUBLISH。
func (c *conn) handlePublish(pp *packet.Publish) error {
	ctx := context.Background()

	if !c.b.opt.Limiter.Within(quota.PayloadSize, len(pp.Payload)) {
		c.close(reason.PacketTooLarge, cluster.EvictAdministrative)
		return reason.Errorf(reason.PacketTooLarge, "负载超出上限")
	}
	if !c.b.opt.Limiter.AllowLocal(quota.PublishRatePerConn, c.clientID(), 1) {
		c.emitQuota(quota.PublishRatePerConn, c.clientID())
		return c.rejectPublish(pp, reason.QuotaExceeded)
	}
	if !c.b.opt.Limiter.AllowLocal(quota.ThroughputPerConn, c.clientID(), len(pp.Payload)) {
		c.emitQuota(quota.ThroughputPerConn, c.clientID())
		return c.rejectPublish(pp, reason.QuotaExceeded)
	}
	if !c.b.opt.Limiter.AllowGlobal(ctx, quota.InboundPublishRate, "", 1) {
		c.emitQuota(quota.InboundPublishRate, "")
		return c.rejectPublish(pp, reason.QuotaExceeded)
	}

	name := string(pp.Topic)
	if err := topic.ValidateName(pp.Topic, c.b.opt.Limits); err != nil {
		c.close(reason.TopicNameInvalid, cluster.EvictAdministrative)
		return err
	}

	// 发布与保留发布是两个不同的动作（PRD AUTH-020）。
	act := authz.ActPublish
	if pp.Retain {
		act = authz.ActRetainPublish
	}
	if !c.allowed(act, name) {
		return c.rejectPublish(pp, reason.NotAuthorized)
	}

	msg := &router.Message{
		Topic:   name,
		Payload: pp.Payload,
		QoS:     uint8(pp.QoS),
		Retain:  pp.Retain,
	}
	if c.ver == packet.V5 && !pp.Props.Empty() {
		msg.Props = encodeForwardProps(&pp.Props)
		if pp.Props.Has(packet.PropMessageExpiry) {
			msg.ExpiryUnixMs = c.b.opt.Now() + int64(pp.Props.MessageExpiry)*1000
		}
	}
	msg.StoredAtUnixMs = c.b.opt.Now()

	// 保留消息的写入先于投递：新订阅者应看到最新的 retained。
	if pp.Retain {
		if !c.b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRatePerTopic, name, 1) ||
			!c.b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRate, "", 1) {
			return c.rejectPublish(pp, reason.QuotaExceeded)
		}
		c.b.opt.Router.Retained().Set(name, msg, c.b.opt.Router.NextVersion())
	}

	delivered := c.b.route(c, msg, pp)

	// 保留主题服务在正常路由之后介入。
	//
	// 顺序是有意的：请求消息本身仍按普通主题投递给订阅了它的客户端
	// （与 AWS 一致——请求主题是可订阅的），服务产出的响应随后作为
	// 新消息投递。反过来（先服务再路由）会让响应早于请求到达订阅方。
	c.serveReserved(ctx, name, pp.Payload)

	// 确认。QoS 2 的 PUBREC 之后还有 PUBREL/PUBCOMP 往返。
	switch pp.QoS {
	case packet.QoS1:
		rc := reason.Success
		if delivered == 0 {
			rc = reason.NoMatchingSubscribers
		}
		return c.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pp.PacketID, Code: rc})
	case packet.QoS2:
		rc := reason.Success
		if delivered == 0 {
			rc = reason.NoMatchingSubscribers
		}
		return c.send(&packet.Ack{Kind: packet.PUBREC, PacketID: pp.PacketID, Code: rc})
	}
	return nil
}

// serveReserved 把保留主题上的请求交给平台服务，并投递其响应。
//
// 响应由平台发布，因此不经发布方的 iot:Publish 判定——发布者是平台
// 自己。但投递仍走正常路由，所以每个订阅者的 iot:Receive 会被复查
// （见 deliverTo），一个无权接收 /accepted 的客户端拿不到响应。
func (c *conn) serveReserved(ctx context.Context, name string, payload []byte) {
	svc := c.b.opt.Reserved
	if svc == nil {
		return
	}
	// contract:allow-time-now 只做保留主题服务耗时的差值计算（影子写入那一段，
	// OBS-148 的同类）。注入时钟只有毫秒墙上时间，而这里要分辨的是「时间在
	// 平台服务里还是在投递上」——1,200 并发那一轮的 10 秒尾部就是靠这一段
	// 与 Raft proposal 的对照定位到 WAL 的。它不参与排序或版本比较。
	started := time.Now()
	msgs, ok := svc.HandleReserved(ctx, ReservedRequest{
		Topic: name, Payload: payload,
		ClientID: c.clientID(), Thing: c.princ.AuthzCtx.Thing,
	})
	elapsed := uint64(time.Since(started))
	for old := c.b.reservedServiceMaxNs.Load(); elapsed > old; old = c.b.reservedServiceMaxNs.Load() {
		if c.b.reservedServiceMaxNs.CompareAndSwap(old, elapsed) {
			break
		}
	}
	if elapsed > uint64(time.Second) {
		c.b.reservedServiceOver1s.Add(1)
	}
	if !ok {
		return
	}
	for i := range msgs {
		m := &router.Message{
			Topic:   msgs[i].Topic,
			Payload: msgs[i].Payload,
		}
		if msgs[i].QoS1 {
			m.QoS = 1
		}
		delivered := c.b.route(nil, m, nil)
		if i == 0 {
			c.b.reservedResponses.Add(1)
			if delivered == 0 {
				c.b.reservedNoSubscriber.Add(1)
			}
		}
	}
}

// rejectPublish 以带原因码的确认拒绝一条发布。
//
// QoS 0 没有确认报文，只能记录事件——这是 MQTT 的固有限制：
// 服务端无法告知 QoS 0 的发布者它被拒绝了。
func (c *conn) rejectPublish(pp *packet.Publish, rc reason.Code) error {
	switch pp.QoS {
	case packet.QoS1:
		return c.send(&packet.Ack{Kind: packet.PUBACK, PacketID: pp.PacketID, Code: rc})
	case packet.QoS2:
		return c.send(&packet.Ack{Kind: packet.PUBREC, PacketID: pp.PacketID, Code: rc})
	}
	return nil
}

// route 把消息投递给本地订阅者，并把远端目标交给传输层。
//
// 契约 02 的两条分支在此汇合：单机下 remote 恒为空，但代码路径存在，
// 集群化时填充 Transport 实现即可，不必改动调用方。
func (b *Broker) route(from *conn, msg *router.Message, pp *packet.Publish) int {
	// 抓包在投递之前看一眼。它不占订阅，所以不能等「有没有订阅者」再决定记不记。
	b.fanoutCapture(msg)
	local, remote := b.opt.Router.Route([]byte(msg.Topic))
	shared := b.opt.Router.RouteShared([]byte(msg.Topic))

	n := 0
	for _, ref := range local {
		// No Local：不把消息投回发布者自己（MQTT-3.8.3.1）。
		//
		// from 为 nil 表示消息由平台自己发布（Shadow 的响应、
		// 经 REST 触发的更新通知、遗嘱消息）。这类消息没有"发布者"，
		// 因此 No Local 不适用——漏掉这个判断会在平台发布时解引用空指针。
		if from != nil && ref.NoLocal && ref.ClientID == from.clientID() {
			continue
		}
		if b.deliverTo(ref, msg) {
			n++
		}
	}
	for _, tg := range shared {
		if tg.Remote {
			// 选中的成员在别的节点上（MQTT-096、MQTT-116）：带上
			// Share/Filter 转过去，由那个节点在自己的本地成员里选一个。
			n += b.forwardShared(tg, msg)
			continue
		}
		if tg.Queue || tg.ClientID == "" {
			// 组内无在线成员：有持久会话成员时应排队。
			// 排队后端在 v0.4 提供（PRD MQTT-095），此处记录为未投递。
			continue
		}
		ref := router.SubRef{ClientID: tg.ClientID, QoS: tg.QoS}
		if b.deliverTo(ref, msg) {
			n++
		}
	}
	n += b.forward(remote, msg)
	return n
}

// forwardShared 把一条消息转给承载某个共享订阅组成员的那个节点。
//
// 与 forward 分开是因为帧里要带上组标识：接收侧据此只在本地成员里选，
// 不再做跨节点选取（否则两个各自有成员的节点会把同一条消息来回推，
// 而共享订阅的语义是组内只投一个）。
func (b *Broker) forwardShared(tg router.Target, msg *router.Message) int {
	if b.opt.Mesh == nil || tg.Node == "" || tg.Node == b.opt.Node {
		return 0
	}
	frame := cluster.Frame{
		Version: cluster.FrameVersion,
		Origin:  b.opt.Node,
		Seq:     b.meshSeq.Add(1),
		Topic:   msg.Topic,
		Payload: msg.Payload,
		Props:   msg.Props,
		QoS:     msg.QoS,
		Retain:  msg.Retain,
		Share:   tg.Share,
		Filter:  tg.Filter,
	}
	if err := b.opt.Mesh.Forward(context.Background(),
		[]cluster.NodeID{tg.Node}, []cluster.Frame{frame}); err != nil {
		b.opt.Bus.Emit(&events.Event{
			Kind: events.KindCluster,
			Cluster: events.ClusterFields{
				Member: tg.Node, Role: "mesh",
				Detail: "共享订阅转发失败: " + err.Error(),
			},
		})
		return 0
	}
	return 1
}

// forward 把消息交给远端节点（契约 07、阶段 D）。
//
// 返回的是「交出去了几个节点」而不是「对端投给了几个订阅者」：
// 发送侧不知道对端有多少订阅者，而本地计数的用途是 PUBACK 之前的
// 「有没有人要这条消息」。把它算成 1 更诚实——至少有一个节点声明了兴趣。
//
// 失败只记一次事件，不影响本地投递：消息面是至少一次语义，
// 一个节点收不到不该让发布者拿到错误（它的本地订阅者已经收到了）。
func (b *Broker) forward(remote []cluster.NodeID, msg *router.Message) int {
	if len(remote) == 0 || b.opt.Mesh == nil {
		return 0
	}
	frame := cluster.Frame{
		Version: cluster.FrameVersion,
		Origin:  b.opt.Node,
		Seq:     b.meshSeq.Add(1),
		Topic:   msg.Topic,
		Payload: msg.Payload,
		Props:   msg.Props,
		QoS:     msg.QoS,
		Retain:  msg.Retain,
	}
	if err := b.opt.Mesh.Forward(context.Background(), remote, []cluster.Frame{frame}); err != nil {
		b.opt.Bus.Emit(&events.Event{
			Kind: events.KindCluster,
			Cluster: events.ClusterFields{
				Role:   "mesh",
				Detail: "转发失败: " + err.Error(),
			},
		})
		return 0
	}
	return len(remote)
}

// DeliverRemote 投递一批来自其他节点的帧。
//
// 只投本地订阅者，**不再转发**：帧已经由发布侧算过一次远端集合，
// 这里再转一次就会在两个互相有兴趣的节点之间无限弹（而 Frame.Origin
// 的存在正是为了让这类环路可被识别——但最省的办法是这条路径根本不转发）。
func (b *Broker) DeliverRemote(from cluster.NodeID, batch []cluster.Frame) {
	for i := range batch {
		f := &batch[i]
		if f.Version != cluster.FrameVersion {
			// 版本不认识就丢掉并记一次：尽力解析会让一条字段含义变了的
			// 帧被当成正常消息投给设备。
			b.opt.Bus.Emit(&events.Event{
				Kind: events.KindCluster,
				Cluster: events.ClusterFields{
					Member: from, Role: "mesh",
					Detail: "帧格式版本不认识，已丢弃",
				},
			})
			continue
		}
		msg := &router.Message{
			Topic: f.Topic, Payload: f.Payload, Props: f.Props,
			QoS: f.QoS, Retain: f.Retain, StoredAtUnixMs: b.opt.Now(),
		}
		if f.Share != "" {
			// 这一帧是投给某个共享订阅组的（MQTT-116）：发布侧已经选定
			// 本节点，这里只在**本地成员**里选一个，不再走跨节点选取——
			// 否则它会被又转出去，而共享订阅的语义是组内只投一个。
			// 普通订阅在这条路径上也不投：那一份由发布侧按兴趣表另发。
			if tg, ok := b.opt.Router.PickLocalShared(f.Share, f.Filter); ok && tg.ClientID != "" {
				b.deliverTo(router.SubRef{ClientID: tg.ClientID, QoS: tg.QoS}, msg)
			}
			continue
		}
		local, _ := b.opt.Router.Route([]byte(msg.Topic))
		for _, ref := range local {
			// 远端来的消息没有本地发布者，No Local 不适用
			b.deliverTo(ref, msg)
		}
		for _, tg := range b.opt.Router.RouteShared([]byte(msg.Topic)) {
			// 远端目标在这条路径上要跳过：帧已经由发布侧算过一次远端
			// 集合，这里再转一次就是那个无限弹的环路。
			if tg.Remote || tg.Queue || tg.ClientID == "" {
				continue
			}
			b.deliverTo(router.SubRef{ClientID: tg.ClientID, QoS: tg.QoS}, msg)
		}
	}
}

// deliverTo 把消息投给一个本地订阅者。
func (b *Broker) deliverTo(ref router.SubRef, msg *router.Message) bool {
	target := b.lookup(ref.ClientID)
	if target == nil {
		// 控制台测试客户端没有连接对象。先问它，再谈离线队列：
		// 它在线，只是不占一条设备连接。
		if b.deliverTap(ref, msg) {
			return true
		}
		// 订阅者当前离线：持久会话应入离线队列。
		return b.enqueueOffline(ref, msg)
	}
	// 投递同样受 iot:Receive 鉴权约束——订阅时放行不等于此刻仍放行，
	// 策略可能已变更（PRD AUTH-023 把 Subscribe 与 Receive 分开判定）。
	if !target.allowed(authz.ActReceive, msg.Topic) {
		return false
	}
	var subIDs []uint32
	if ref.ID != 0 {
		subIDs = []uint32{ref.ID}
	}
	target.deliver(msg, ref, subIDs)
	return true
}

// enqueueOffline 把消息放入离线会话的队列。
func (b *Broker) enqueueOffline(ref router.SubRef, msg *router.Message) bool {
	ctx := context.Background()
	st, ok, err := b.opt.Sessions.Load(ctx, ref.ClientID)
	if err != nil || !ok || st.ExpiryInterval == 0 {
		return false
	}
	if msg.QoS == 0 {
		// QoS 0 不入队：规范不要求，且会让离线队列被无价值的消息填满。
		return false
	}
	// 队列有上限（QTA-111）。没有上限的后果不是报错而是慢性的：一台持久
	// 会话的设备下线后，发给它的每一条 QoS 1 都往队列里堆，占内存也占盘，
	// 而且每入队一条都要把整块会话状态重写一遍——N 条消息累计写入是
	// O(N²)，这还发生在发布路径上。
	//
	// 满了丢**新的**而不是挤掉最旧的（MQTT-127）：这样设备重连后收到的
	// 是已发布序列的一个前缀，顺序不被打乱；丢最旧的那种做法会让"收到了
	// 第 1、3、4 条"这类空洞出现，而 QoS 1 的使用者通常按顺序处理。
	// 丢弃由 quota_rejected_total{quota="queued_messages_per_session"} 暴露。
	if !b.opt.Limiter.Within(quota.QueuedPerSession, len(st.Queued)+1) {
		return false
	}
	// 走**追加**而不是"改 State.Queued 再 Save 整份会话"。
	//
	// 后者有两处代价，都在发布路径上：N 条消息累计写入 O(N²)（每次都把
	// 整份会话重新编码落盘），以及跨节点复制时每入队一条都要把上百 KB
	// 的会话提进日志。队列单独存就是为了让这一步是一次小写入。
	return b.opt.Sessions.AppendQueued(ref.ClientID, session.QueuedMsg{
		QoS:          minU8(msg.QoS, ref.QoS),
		Retain:       msg.Retain,
		Topic:        msg.Topic,
		Payload:      append([]byte(nil), msg.Payload...),
		Props:        append([]byte(nil), msg.Props...),
		ExpiryUnixMs: msg.ExpiryUnixMs,
	}) == nil
}

// deliver 把一条消息写给本连接。
func (c *conn) deliver(msg *router.Message, ref router.SubRef, subIDs []uint32) {
	qos := minU8(msg.QoS, ref.QoS)
	out := &packet.Publish{
		Topic:   []byte(msg.Topic),
		Payload: msg.Payload,
		QoS:     packet.QoS(qos),
	}
	// Retain As Published：未设置时出站 RETAIN 标志一律清零（MQTT-3.3.1.3）。
	if ref.RetainAsPub {
		out.Retain = msg.Retain
	}
	if c.ver == packet.V5 {
		applyForwardProps(&out.Props, msg.Props)
		for _, id := range subIDs {
			out.Props.SubscriptionIDs = append(out.Props.SubscriptionIDs, id)
			out.Props.Set(packet.PropSubscriptionID)
		}
		if msg.ExpiryUnixMs != 0 {
			// 出站须递减剩余时间（PRD MQTT-071）。
			remain := msg.ExpiryUnixMs - c.b.opt.Now()
			if remain <= 0 {
				return // 已过期，不投递
			}
			secs := uint32(remain / 1000)
			if remain%1000 != 0 && secs == 0 {
				// 剩余不足 1 秒时置 0，与规范一致。
				secs = 0
			}
			out.Props.MessageExpiry = secs
			out.Props.Set(packet.PropMessageExpiry)
		}
	}
	if !c.b.opt.Limiter.AllowGlobal(context.Background(), quota.OutboundPublishRate, "", 1) {
		return
	}
	// 报文标识与在途记账放在 trackAndSend：窗口满时还没有标识可分配，
	// QoS 0 则根本不进在途表。
	c.trackAndSend(out, msg, subIDs)
}

// deliverRetained 投递一条 retained 消息。
//
// 与普通投递的唯一区别是 RETAIN 标志必须置位，使客户端能区分
// 「订阅时收到的历史状态」与「刚刚发生的事件」。
func (c *conn) deliverRetained(msg *router.Message, ref router.SubRef) {
	qos := minU8(msg.QoS, ref.QoS)
	out := &packet.Publish{
		Topic:   []byte(msg.Topic),
		Payload: msg.Payload,
		QoS:     packet.QoS(qos),
		Retain:  true,
	}
	var subIDs []uint32
	if c.ver == packet.V5 {
		applyForwardProps(&out.Props, msg.Props)
		if ref.ID != 0 {
			subIDs = []uint32{ref.ID}
			out.Props.SubscriptionIDs = subIDs
			out.Props.Set(packet.PropSubscriptionID)
		}
	}
	c.trackAndSend(out, msg, subIDs)
}

// handleAck 处理 PUBACK、PUBREC、PUBREL、PUBCOMP。
func (c *conn) handleAck(a *packet.Ack) error {
	switch a.Kind {
	case packet.PUBACK, packet.PUBCOMP:
		// 出站 QoS 1/2 的最终确认：从在途表移除。
		c.ackInflight(a.PacketID)
		return nil
	case packet.PUBREC:
		// 出站 QoS 2 的第一段确认，回 PUBREL。此后的重试发的是 PUBREL，
		// 不是再来一条 PUBLISH。
		if c.rt.MarkInflight(c.epoch, a.PacketID, session.AwaitPubcomp) {
			c.armRetry(a.PacketID)
		}
		return c.send(&packet.Ack{Kind: packet.PUBREL, PacketID: a.PacketID})
	case packet.PUBREL:
		// 入站 QoS 2 的第二段，回 PUBCOMP。
		return c.send(&packet.Ack{Kind: packet.PUBCOMP, PacketID: a.PacketID})
	}
	return nil
}

func (c *conn) ackInflight(id uint16) {
	// 不认识的或重复的确认不得归还额度：额度只跟一条真实的在途消息走。
	if c.rt == nil || !c.rt.RemoveInflight(c.epoch, id) {
		return
	}
	c.clearRetry(id)
	c.releaseInflightQuota(1)
	c.promoteQueued()
}

// handleDisconnect 处理 DISCONNECT。
func (c *conn) handleDisconnect(d *packet.Disconnect) error {
	st := c.rt.State()
	// MQTT 5 允许在断开时修改会话过期间隔（MQTT-3.14.2.2）。
	if c.ver == packet.V5 && d.Props.Has(packet.PropSessionExpiry) {
		st.ExpiryInterval = d.Props.SessionExpiry
	}
	// 正常断开不触发遗嘱；reason 0x04 要求触发（MQTT-3.14.2.1）。
	//
	// 两个分支都要落到"遗嘱已了结"：收到 DISCONNECT 本身就意味着
	// 客户端是主动走的，而 close 会给所有没了结的遗嘱补发（MQTT-064）。
	// 这里不清，正常断开也会被当成异常掉线。
	//
	// 0x04 走的是与异常终止同一个 flushWill，延迟间隔因此一样适用：
	// 规范没有给 0x04 开豁免，它改变的是"发不发"，不是"等多久"。
	if d.Code == reason.DisconnectWithWill {
		c.flushWill()
	} else {
		st.HasWill = false
	}
	c.close(reason.NormalDisconnection, cluster.EvictAdministrative)
	return nil
}

// --- 辅助 ---

func minU8(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

// splitShared 把过滤器拆成共享组名与内层过滤器。
func splitShared(filter string) (share, inner string) {
	s, rest, ok := topic.ParseShared([]byte(filter))
	if !ok {
		return "", filter
	}
	return string(s), string(rest)
}

// encodeForwardProps 提取须随消息转发的 MQTT 5 属性。
//
// 只转发与消息内容相关的属性；会话级与连接级属性不随消息走。
func encodeForwardProps(p *packet.Properties) []byte {
	var fwd packet.Properties
	if p.Has(packet.PropPayloadFormat) {
		fwd.PayloadFormat = p.PayloadFormat
		fwd.Set(packet.PropPayloadFormat)
	}
	if p.Has(packet.PropContentType) {
		fwd.ContentType = p.ContentType
		fwd.Set(packet.PropContentType)
	}
	if p.Has(packet.PropResponseTopic) {
		fwd.ResponseTopic = p.ResponseTopic
		fwd.Set(packet.PropResponseTopic)
	}
	if p.Has(packet.PropCorrelationData) {
		fwd.CorrelationData = p.CorrelationData
		fwd.Set(packet.PropCorrelationData)
	}
	if p.Has(packet.PropUserProperty) {
		fwd.UserProps = p.UserProps
		fwd.Set(packet.PropUserProperty)
	}
	if fwd.Empty() {
		return nil
	}
	return fwd.AppendTo(nil)
}

// applyForwardProps 把转发属性还原到出站报文上。
func applyForwardProps(dst *packet.Properties, raw []byte) {
	if len(raw) == 0 {
		return
	}
	_ = packet.DecodeProps(raw, packet.PublishContext, dst)
}

func asReasonError(err error, out **reason.Error) bool {
	if pe, ok := err.(*reason.Error); ok {
		*out = pe
		return true
	}
	return false
}
