package broker

import (
	"context"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// handleConnect 处理 CONNECT 报文。
//
// 顺序是刻意的：先定版本（决定后续编解码），再查视图陈旧度与配额
// （廉价且能挡住大部分异常流量），然后才做认证与策略解析（相对昂贵），
// 最后才抢占所有权（有副作用，会踢掉别人）。
func (c *conn) handleConnect(ctx context.Context, cp *packet.Connect) error {
	c.ver = cp.Version
	c.connectedAtMs = c.b.opt.Now()

	// 视图陈旧时对新连接 fail-closed，但不断开已有连接。
	//
	// 这是自建场景独有的问题：网络分区中的少数派节点，其视图冻结在最后
	// 提交的 revision 上，可能仍接受一张在多数派侧已被吊销的证书
	// （PRD CLU-023 至 CLU-025）。
	v := c.b.opt.Meta.View()
	if c.b.opt.MaxViewStalenessMs > 0 &&
		v.Stale(c.b.opt.Now(), c.b.opt.MaxViewStalenessMs) {
		return c.rejectConnect(events.ConnViewStale, reason.ServerUnavailable,
			"元数据视图已陈旧，为避免放行已吊销的凭据而拒绝新连接")
	}

	// clientId 校验。MQTT 5 允许空 clientId 由服务端分配。
	clientID := string(cp.ClientID)
	if !c.b.opt.Limiter.Within(quota.ClientIDLen, len(cp.ClientID)) {
		return c.rejectConnect(events.ConnAuthFailed, reason.ClientIdentifierNotValid,
			"clientId 超出长度上限")
	}
	assigned := false
	if clientID == "" {
		if c.ver != packet.V5 {
			return c.rejectConnect(events.ConnAuthFailed, reason.ClientIdentifierNotValid,
				"3.1.1 下空 clientId 需配合 clean session")
		}
		clientID = c.b.assignClientID()
		assigned = true
	}
	c.clientID = clientID

	// 接入速率配额。每 clientId 1/秒是防重连风暴的关键（PRD QTA-028）。
	if !c.b.opt.Limiter.AllowGlobal(ctx, quota.ConnectRatePerClient, clientID, 1) {
		c.emitQuota(quota.ConnectRatePerClient, clientID)
		return c.rejectConnect(events.ConnThrottled, reason.ConnectionRateExceeded,
			"同一 clientId 的连接速率超限")
	}
	if !c.b.opt.Limiter.AllowGlobal(ctx, quota.ConnectRate, "", 1) {
		c.emitQuota(quota.ConnectRate, "")
		return c.rejectConnect(events.ConnThrottled, reason.ConnectionRateExceeded,
			"节点连接速率超限")
	}
	if !c.b.opt.Limiter.Acquire(quota.Connections, "", 1) {
		c.emitQuota(quota.Connections, "")
		return c.rejectConnect(events.ConnThrottled, reason.QuotaExceeded,
			"并发连接数超限")
	}
	// 自此之后的失败路径都必须归还连接数配额。
	releaseConn := true
	defer func() {
		if releaseConn {
			c.b.opt.Limiter.Release(quota.Connections, "", 1)
		}
	}()

	// 认证。只读热视图，不触网（契约 05）。
	princ, rc := c.b.opt.Auth.Authenticate(v, c.info, clientID, cp.Username, cp.Password)
	if rc != reason.Success {
		return c.rejectConnect(events.ConnAuthFailed, rc, "认证失败")
	}
	c.princ = princ

	// 补全策略求值上下文：clientId 由连接决定，属性查询器接本地热视图。
	c.princ.AuthzCtx.ClientID = clientID
	if c.princ.AuthzCtx.Thing == "" {
		c.princ.AuthzCtx.Thing = princ.Thing
	}
	thing := c.princ.AuthzCtx.Thing
	if thing != "" {
		c.princ.AuthzCtx.ThingAttr = func(name string) (string, bool) {
			return c.b.opt.Meta.View().ThingAttr(thing, name)
		}
	}
	if len(c.princ.Policies) == 0 {
		c.princ.Policies = policiesFor(v, princ.CertFP, princ.HasFP, thing)
	}

	// iot:Connect 鉴权。资源是 clientId（PRD AUTH-023 的 client/ 类型）。
	if !c.allowedConnect(clientID) {
		return c.rejectConnect(events.ConnAuthFailed, reason.NotAuthorized,
			"策略不允许该 clientId 连接")
	}

	// 抢占所有权。这是第一个有副作用的步骤，故放在最后。
	epoch, err := c.b.opt.Owners.Claim(ctx, clientID, c.b.opt.Node)
	if err != nil {
		return c.rejectConnect(events.ConnAuthFailed, reason.ServerUnavailable,
			"无法取得会话所有权")
	}
	c.epoch = epoch

	// 客户端回来了，取消它上一次断开留下的待发遗嘱（MQTT-3.1.2.5）。
	// 放在抢占成功之后、恢复会话之前：此刻这个 clientId 已经归本连接，
	// 再晚就可能与定时器触发擦肩而过。
	c.b.cancelWill(clientID)

	// 恢复或新建会话。
	st, present := c.restoreSession(ctx, cp, clientID, epoch)
	c.rt = session.NewRuntime(st, c.nc, c.b.opt.OutboundBuffer)
	c.rt.SetOnClose(func(cluster.EvictReason) { _ = c.nc.Close() })

	c.keepAliveMs = c.effectiveKeepAliveMs(cp.KeepAlive)

	// 登记索引并同步断开被抢占的旧连接。
	//
	// 本地抢占必须在这里同步处理，不能依赖所有权的驱逐通知：
	// 通知是异步的，等它到达时索引已经指向新连接，按 clientId 就再也
	// 找不到旧连接了。驱逐通知仍然有用——它覆盖新连接落在另一个节点
	// 以及运维主动踢出这两种情形。
	prev, adopted := c.b.register(c)
	releaseConn = false

	// 接管本节点上的同 clientId 连接时，会话的权威是它的运行时而不是存储。
	// 快照在 register 里、索引可见之前取走。
	if adopted {
		present = true
	}

	// 没有续上任何会话时，路由表里为这个 clientId 残留的订阅都是上一代的
	// 孤儿，必须连同它们占的全局额度一起清掉。
	//
	// 到这里 present 的含义已经确定：要么从存储恢复，要么接管了本节点上
	// 的活连接，两种情形下路由表里的订阅都正是本会话当前持有的那些。
	// 反过来，present 为假就意味着本会话是全新的，而路由表里仍可能留着
	// 旧订阅——clean start 是一种，更隐蔽的一种是**存储中的会话已过期**：
	// 过期只是让恢复落空，没有人去摘掉路由表里的订阅，于是订阅连同额度
	// 一起永久留下。表现为新会话订阅同一个主题时额度加一而路由表不变
	// （就地替换），全局额度就此多出一份；上限是全局 50 万，耗尽之后
	// 整个 broker 不能再新增订阅。确定性模拟在随机种子深扫里报的
	// 「配额比路由表多」就是它。
	//
	// 放在登记与接管**之后**而不是 restoreSession 里：那时还不知道会不会
	// 接管到一个活连接，而接管的订阅集正是从路由表之外（对方运行时）来的，
	// 先清就把它们一起清掉了。
	if !present {
		c.b.opt.Limiter.Release(quota.Subscriptions, "",
			c.b.opt.Router.RemoveClient(clientID))
	}

	// 先把新会话以新 epoch 落盘，再断开旧连接。
	//
	// 顺序颠倒会让旧连接的清理覆盖掉新会话：存储的 epoch 围栏比较的是
	// 「已存储的 epoch」，只有新 epoch 先落盘，旧连接后到的写入才会被拒。
	_ = c.b.opt.Sessions.Save(ctx, st)
	if prev != nil && prev != c {
		prev.evict(reason.SessionTakenOver, cluster.EvictTakenOver)
	}
	// 通知其他节点：这个 clientId 归本节点了。
	//
	// 放在本地抢占之后：先把本节点收拾干净，再去管别人，否则
	// 「广播出去了而本地还双着」这种状态会在日志里很难解释。
	if c.b.opt.OnTakeover != nil {
		c.b.opt.OnTakeover(clientID)
	}

	ack := &packet.Connack{SessionPresent: present, Code: reason.Success}
	if c.ver == packet.V5 {
		c.fillConnackProps(ack, assigned, clientID)
	}
	if err := c.send(ack); err != nil {
		return err
	}
	c.emitConn(events.ConnAccepted, reason.Success, "")

	// 持久会话恢复后重放其订阅到路由表，重投未确认的出站消息，再下发离线队列。
	// 先重投再刷队列：在途的那些已经占着接收窗口，队列里的要等窗口有空位。
	if present {
		c.replaySubscriptions(st)
		c.redeliverInflight()
		c.flushQueued()
	}
	return nil
}

// allowedConnect 对 iot:Connect 做鉴权。
func (c *conn) allowedConnect(clientID string) bool {
	return c.allowed(authz.ActConnect, clientID)
}

// rejectConnect 发送失败的 CONNACK 并关闭连接。
func (c *conn) rejectConnect(res events.ConnResult, rc reason.Code, text string) error {
	c.sendDirect(&packet.Connack{Code: rc})
	c.emitConn(res, rc, text)
	c.close(rc, cluster.EvictAdministrative)
	return reason.Errorf(rc, "%s", text)
}

// restoreSession 恢复持久会话或新建一个。
//
// 返回的第二个值即 CONNACK 的 Session Present 标志。
func (c *conn) restoreSession(ctx context.Context, cp *packet.Connect,
	clientID string, epoch cluster.Epoch,
) (*session.State, bool) {
	expiry := uint32(0)
	if c.ver == packet.V5 {
		if cp.Props.Has(packet.PropSessionExpiry) {
			expiry = cp.Props.SessionExpiry
		}
	} else if !cp.CleanStart {
		// 3.1.1 没有过期间隔：非 clean session 即永久保留，
		// 这里以一个明确的默认值表达，避免「0 表示立即删除」的歧义。
		expiry = defaultSessionExpiry311
	}

	if old, ok, err := c.b.opt.Sessions.Load(ctx, clientID); err == nil && ok {
		if !cp.CleanStart && !old.Expired(c.b.opt.Now()) {
			old.Epoch = epoch
			old.Owner = c.b.opt.Node
			old.ProtoVer = byte(c.ver)
			old.CleanStart = false
			old.ExpiryInterval = expiry
			old.KeepAlive = cp.KeepAlive
			old.DisconnectUnixMs = 0
			c.applyWill(old, cp)
			return old, true
		}
		// 会话不再续上（clean start 或已过期）。存储里还记着的在途额度
		// 要在这里还：这两条路径都不会走到旧连接的 persist。进程重启后
		// 计数器是空的，Release 钳到 0，多还无害。
		c.releaseInflightQuota(len(old.Inflight))
	}
	// 新会话：clean start 时须先清掉存储里可能残留的旧状态。
	//
	// 路由表里的残留订阅不在这里摘：它与「会话已过期」是同一件事，
	// 统一由 handleConnect 在接管判定之后处理（见那里的说明）。
	if cp.CleanStart {
		_ = c.b.opt.Sessions.Delete(ctx, clientID, epoch)
	}
	st := &session.State{
		ClientID:       clientID,
		Epoch:          epoch,
		Owner:          c.b.opt.Node,
		ProtoVer:       byte(c.ver),
		CleanStart:     cp.CleanStart,
		ExpiryInterval: expiry,
		KeepAlive:      cp.KeepAlive,
		Thing:          c.princ.AuthzCtx.Thing,
		CreatedUnixMs:  c.b.opt.Now(),
	}
	if c.ver == packet.V5 {
		st.Props = session.Props{
			ReceiveMaximum:    cp.Props.ReceiveMaximum,
			MaximumPacketSize: cp.Props.MaximumPacketSize,
			TopicAliasMaximum: cp.Props.TopicAliasMaximum,
			RequestProblem:    cp.Props.RequestProblem != 0,
			RequestResponse:   cp.Props.RequestResponse != 0,
		}
	}
	c.applyWill(st, cp)
	return st, false
}

// adoptLive 从被抢占的本地连接接过会话的订阅集，返回是否接管成功。
//
// 为什么不回存储里读：订阅与退订只改内存里的会话状态，落盘发生在断开时
// （见 conn.persist）。同 clientId 的新连接若从存储恢复，拿到的是上一次
// 断开时的那份快照——本次连接期间的订阅与退订全部丢失，随后
// replaySubscriptions 又把这份过时的集合重新装回路由表。
//
// 真正的问题是**设备的订阅状态被悄悄回退**：设备明明退订了，接管之后
// 又开始收到那个主题；反之刚订阅的主题接管后收不到。记账不符只是它的
// 症状——全局订阅配额跟着路由表走，而过时集合比当前集合或多或少，
// 两个方向都会分叉。确定性模拟报的就是这个症状（TST-034），
// 复现种子见 docs/TODO.md 第 4 节。
//
// 不改成「每次订阅都落盘」是因为那会把一次写放大加到订阅热路径上，
// 而接管是罕见路径；把权威放在运行时则两边都不付代价。
//
// 订阅集、在途表、待发队列都在运行时的锁下，可以从本协程取快照。
// 别名表仍由对方的读协程独占，不在此列。
func (c *conn) adoptLive(prev *conn) bool {
	if prev == nil || prev == c || prev.rt == nil {
		return false
	}
	// clean start 要的是丢弃旧会话，不是接管它。路由表里旧连接留下的
	// 订阅由 handleConnect 在 present 落定之后清掉。
	//
	// 在途额度在这里还。旧连接的 persist 随后会发现索引已经指向新连接
	// 而提前返回，不会再还一次。
	if c.rt.State().CleanStart {
		c.releaseInflightQuota(len(prev.rt.InflightSnapshot()))
		return false
	}
	// 正在拆解的连接同样要接：它的运行时**永远**比存储新。
	//
	// 这里曾经按"已开始关闭就退回去读存储"处理，那是错的，而且错得很隐蔽：
	// 旧连接在本次连接期间新增的订阅只在内存里，它的 persist 又因为索引
	// 已指向新连接而提前返回（不得改动共享状态），于是那几条订阅永远不会
	// 落盘。新连接读存储拿到的是更旧的集合，会话从此比路由表少几条——
	// 等设备重新订阅那几条时，会话以为是新订阅而路由表就地替换，全局额度
	// 多出一份。深扫里那个"再等 3 秒也不收敛"的永久分叉就是这条路径。
	//
	// 接过来之后额度仍然对得上：重放按路由表缺不缺来占（见
	// replaySubscriptions），所以清洁会话那半边——persist 已摘掉订阅并
	// 归还额度——也会被正确地重新占上。
	if err := c.rt.ReplaceSubscriptions(c.epoch, prev.rt.SubscriptionsSnapshot()); err != nil {
		return false
	}
	// 在途表与待发队列同样以运行时为准，不回存储读。
	//
	// 存储里那份是本次连接开始时落的盘。此后的投递与确认只改内存，
	// 被抢占的连接 persist 又会提前返回，不会把更新写回去。重放存储
	// 等于把已经确认过的再投一遍，或者把还没确认的弄丢（MQTT-106、MQTT-114）。
	inflight := prev.rt.InflightSnapshot()
	if err := c.rt.ReplaceInflight(c.epoch, inflight); err != nil {
		return false
	}
	if err := c.rt.ReplaceQueued(c.epoch, prev.rt.QueuedSnapshot()); err != nil {
		return false
	}
	c.seedPacketIDs(inflight)
	return true
}

// defaultSessionExpiry311 是 3.1.1 非 clean session 的默认过期间隔。
//
// 规范本身没有过期概念，但服务端不能无限保留；取与 AWS 默认一致的 1 小时，
// 并在用户文档说明（PRD QTA-024）。
const defaultSessionExpiry311 = 3600

// effectiveKeepAliveMs 推导读超时。
//
// 客户端请求小于 30 秒时提升到 30 秒；0 表示不启用保活。
// 超时窗口按规范取 1.5 倍（MQTT-3.1.2.10）。
func (c *conn) effectiveKeepAliveMs(requested uint16) int64 {
	ka := int64(requested)
	if ka == 0 {
		return 0
	}
	if ka < minKeepAliveSeconds {
		ka = minKeepAliveSeconds
	}
	if ka > maxKeepAliveSeconds {
		ka = maxKeepAliveSeconds
	}
	return int64(float64(ka) * 1000 * c.b.opt.KeepAliveGraceFactor)
}

const (
	minKeepAliveSeconds = 30
	maxKeepAliveSeconds = 1200
)

// fillConnackProps 填充 MQTT 5 的 CONNACK 属性，向客户端声明服务端能力。
//
// 注意没有设置 Maximum QoS：该属性只能取 0 或 1，而本实现支持 QoS 2
// （相对 AWS 的超集，PRD CMP-010）。按规范，服务端支持 QoS 2 时应省略
// 该属性而不是写一个会误导客户端的值。
func (c *conn) fillConnackProps(ack *packet.Connack, assigned bool, clientID string) {
	lim := c.b.opt.Limiter
	if assigned {
		ack.Props.AssignedClientID = []byte(clientID)
		ack.Props.Set(packet.PropAssignedClientID)
	}
	if n := lim.Limit(quota.OutboundInflight); n > 0 {
		ack.Props.ReceiveMaximum = uint16(n)
		ack.Props.Set(packet.PropReceiveMaximum)
	}
	if n := lim.Limit(quota.PacketSize); n > 0 {
		ack.Props.MaximumPacketSize = uint32(n)
		ack.Props.Set(packet.PropMaximumPacketSize)
	}
	if n := lim.Limit(quota.TopicAliasMax); n > 0 {
		ack.Props.TopicAliasMaximum = uint16(n)
		ack.Props.Set(packet.PropTopicAliasMaximum)
	}
	ack.Props.RetainAvailable = 1
	ack.Props.Set(packet.PropRetainAvailable)
	ack.Props.WildcardSubAvail = 1
	ack.Props.Set(packet.PropWildcardSubAvail)
	ack.Props.SubIDAvail = 1
	ack.Props.Set(packet.PropSubIDAvail)
	ack.Props.SharedSubAvail = 1
	ack.Props.Set(packet.PropSharedSubAvail)
}

// replaySubscriptions 把恢复的会话订阅重放到路由表。
func (c *conn) replaySubscriptions(st *session.State) {
	// 每连接额度要跟着重放一起恢复。
	//
	// 它在断开时按会话订阅数归还了，重连后若不补回来，这个连接就能在
	// 已有 N 条恢复订阅之上再加满一整份额度。
	if n := len(st.Subscriptions); n > 0 {
		c.b.opt.Limiter.Acquire(quota.SubsPerConn, st.ClientID, n)
	}
	for _, s := range st.Subscriptions {
		filter := s.Filter
		if s.Share != "" {
			filter = "$share/" + s.Share + "/" + s.Filter
		}
		// 全局额度按"这一条是不是真的要往路由表里新加"来占。
		//
		// 常见情形是不占：持久会话断开期间订阅一直留在路由表里，额度也
		// 从未归还，重放只是就地替换。但有两种情形是真的要新加，而它们
		// 此前都在白拿路由表条目——① 节点重启：路由表是内存的，从 bbolt
		// 恢复出来的持久会话重放时路由表是空的，于是额度永久少记（下次
		// 重订阅同一条时才被当成新订阅补上，到那时账已经歪了）；
		// ② 接管一个清洁会话：它的 persist 已经把订阅从路由表摘掉并归还
		// 了额度，而接过来的运行时状态里还有。
		// 两种都只能问路由表，问会话会给出相反的答案。
		if !c.b.opt.Router.Has(st.ClientID, filter) {
			c.b.opt.Limiter.Acquire(quota.Subscriptions, "", 1)
		}
		_ = c.b.opt.Router.Subscribe(router.Subscription{
			ClientID:       st.ClientID,
			Filter:         filter,
			QoS:            s.QoS,
			NoLocal:        s.NoLocal,
			RetainAsPub:    s.RetainAsPub,
			RetainHandling: s.RetainHandling,
			ID:             s.ID,
			Persistent:     st.ExpiryInterval > 0,
		})
	}
}

// flushQueued 下发离线队列。
//
// 先在锁里把队列取走再投递。投递时窗口若已满会把消息重新排回去，
// 边遍历边追加会把同一条消息再看到一遍。
func (c *conn) flushQueued() {
	if c.rt == nil {
		return
	}
	// 盘上那一份是权威，内存里那一份（CONNECT 时由 Load 填进来的）丢掉。
	//
	// 两个理由。一是**不能重投**：队列现在是独立的记录（PrefixQueued），
	// 只从内存取走不删盘上的，下次重连会把同一批再投一遍——而断开时的
	// Save 带的是 nil（约定：nil 表示不碰队列），正好不会清掉它们。
	// 二是**不能漏投**：CONNECT 读盘与这里之间还可能有新的入队（设备刚
	// 连上、登记完成之前的那个窗口），盘上那份包含它们，内存那份不包含。
	queued, terr := c.b.opt.Sessions.TakeQueued(c.clientID)
	if terr != nil {
		// 取不出来只能放弃这一批：报错而不是重投（重投会把已经投过的
		// 也再投一遍）。队列记录留在盘上，下次重连再试。
		return
	}
	// 内存里那份一并清掉：留着它，断开时的 Save 会把它重新写回盘上。
	_ = c.rt.TakeQueued(c.epoch)
	for i := range queued {
		m := &queued[i]
		if m.ExpiryUnixMs != 0 && c.b.opt.Now() >= m.ExpiryUnixMs {
			// 过期消息在会话恢复后不再投递（PRD MQTT-071）。
			continue
		}
		c.deliver(&router.Message{
			Topic:        m.Topic,
			Payload:      m.Payload,
			Props:        m.Props,
			QoS:          m.QoS,
			Retain:       m.Retain,
			ExpiryUnixMs: m.ExpiryUnixMs,
		}, router.SubRef{ClientID: c.clientID, QoS: m.QoS}, m.SubIDs)
	}
}

// assignClientID 为空 clientId 的 MQTT 5 连接分配一个标识。
func (b *Broker) assignClientID() string {
	n := b.assigned.Add(1)
	var buf [24]byte
	out := append(buf[:0], "auto-"...)
	out = appendUint(out, uint64(n))
	return string(out)
}

func appendUint(dst []byte, v uint64) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	var tmp [20]byte
	i := len(tmp)
	for v > 0 {
		i--
		tmp[i] = byte('0' + v%10)
		v /= 10
	}
	return append(dst, tmp[i:]...)
}
