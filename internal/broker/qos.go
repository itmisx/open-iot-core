package broker

import (
	"errors"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// 重试间隔与放弃时限（PRD MQTT-033、MQTT-111）。
//
// 首次间隔规范没有写死。这里取 1 秒再翻倍，上限 960 秒。放弃从首次
// 发出起算，不从最后一次重试起算：否则一个一直不应答的客户端会让
// 这条消息和它占的额度永远留在窗口里。
const (
	retryInitial  = time.Second
	retryMax      = 960 * time.Second
	retryBudgetV3 = time.Hour
	retryBudgetV5 = 16 * time.Minute
)

func retryDelay(retries uint16) time.Duration {
	d := retryInitial
	for i := uint16(0); i < retries && d < retryMax; i++ {
		d *= 2
	}
	if d > retryMax {
		return retryMax
	}
	return d
}

// inflightDeadline 是放弃这条消息的时刻。消息自己的过期更早就用那个。
func (c *conn) inflightDeadline(msgExpiry int64) int64 {
	budget := retryBudgetV3
	if c.ver == packet.V5 {
		budget = retryBudgetV5
	}
	deadline := c.b.opt.Now() + budget.Milliseconds()
	if msgExpiry != 0 && msgExpiry < deadline {
		return msgExpiry
	}
	return deadline
}

// trackAndSend 发出一条出站 PUBLISH。
//
// QoS 0 不占窗口。QoS 1/2 先占出站在途额度再分配报文标识：占不上就留在
// 会话队列里，等一次确认腾出空位，而不是丢掉或超出窗口发出去。
func (c *conn) trackAndSend(out *packet.Publish, src *router.Message, subIDs []uint32) {
	if out.QoS == packet.QoS0 {
		_ = c.sendForeign(out)
		return
	}
	if c.closedRC.Load() != 0 {
		return
	}
	if c.clientID() != "" && c.b.lookup(c.clientID()) != c {
		return
	}
	if !c.b.opt.Limiter.Acquire(quota.OutboundInflight, c.clientID(), 1) {
		if src != nil {
			c.pushQueued(src, uint8(out.QoS), subIDs, out.Retain)
		}
		return
	}
	out.PacketID = c.allocPacketID()
	st := session.AwaitPuback
	if out.QoS == packet.QoS2 {
		st = session.AwaitPubrec
	}
	var msgExpiry int64
	if src != nil {
		msgExpiry = src.ExpiryUnixMs
	}
	m := session.InflightMsg{
		PacketID:     out.PacketID,
		QoS:          uint8(out.QoS),
		State:        st,
		Topic:        string(out.Topic),
		Payload:      out.Payload,
		Props:        out.Props.AppendTo(nil),
		SentUnixMs:   c.b.opt.Now(),
		ExpiryUnixMs: c.inflightDeadline(msgExpiry),
	}
	if c.rt == nil || c.rt.AddInflight(c.epoch, m) != nil {
		c.releaseInflightQuota(1)
		return
	}
	// 占上额度之后连接可能已经在关，或者已经被接管。这种消息不会再被
	// 重投，留在表里就是一笔没人还的额度。
	if c.closedRC.Load() != 0 || (c.clientID() != "" && c.b.lookup(c.clientID()) != c) {
		c.undoInflight(m.PacketID)
		return
	}
	if err := c.sendForeign(out); err != nil {
		if errors.Is(err, session.ErrDetached) || (c.clientID() != "" && c.b.lookup(c.clientID()) != c) {
			c.undoInflight(m.PacketID)
		}
		return
	}
	if c.closedRC.Load() == 0 {
		c.armRetry(m.PacketID)
	}
}

func (c *conn) pushQueued(msg *router.Message, qos uint8, subIDs []uint32, retain bool) {
	if c.rt == nil {
		return
	}
	_ = c.rt.PushQueued(c.epoch, session.QueuedMsg{
		QoS:          qos,
		Retain:       retain,
		Topic:        msg.Topic,
		Payload:      msg.Payload,
		Props:        msg.Props,
		SubIDs:       subIDs,
		ExpiryUnixMs: msg.ExpiryUnixMs,
	})
}

// redeliverInflight 按原来的报文标识、带 DUP 重投未确认的出站消息。
//
// 同一进程里额度还占着，不能再占一次。进程重启后计数器是空的，
// 重投之前先补上差额。已经过了放弃时刻的直接丢掉并归还。
func (c *conn) redeliverInflight() {
	if c.rt == nil {
		return
	}
	c.dropExpiredInflight()
	live := c.rt.InflightSnapshot()
	c.seedPacketIDs(live)
	if len(live) == 0 {
		return
	}
	held := c.outboundHeld()
	deficit := len(live) - int(held)
	if deficit < 0 {
		deficit = 0
	}
	sendN := int(held) + c.acquireDeficit(deficit)
	if sendN > len(live) {
		sendN = len(live)
	}
	now := c.b.opt.Now()
	for i, m := range live {
		if i >= sendN {
			// 差额没占上。发出去就没有额度托底，宁可丢掉。
			_ = c.rt.RemoveInflight(c.epoch, m.PacketID)
			continue
		}
		cur, ok := c.rt.InflightByID(m.PacketID)
		if !ok {
			continue
		}
		cur.Dup = true
		c.resend(cur)
		_ = c.rt.NoteRetry(c.epoch, cur.PacketID, now)
		c.armRetry(cur.PacketID)
	}
}

func (c *conn) dropExpiredInflight() {
	now := c.b.opt.Now()
	for _, m := range c.rt.InflightSnapshot() {
		if m.ExpiryUnixMs == 0 || now < m.ExpiryUnixMs {
			continue
		}
		if c.rt.RemoveInflight(c.epoch, m.PacketID) {
			c.clearRetry(m.PacketID)
			c.releaseInflightQuota(1)
		}
	}
}

func (c *conn) resend(m session.InflightMsg) {
	if m.State == session.AwaitPubcomp {
		_ = c.sendForeign(&packet.Ack{Kind: packet.PUBREL, PacketID: m.PacketID})
		return
	}
	p := &packet.Publish{
		Dup:      true,
		QoS:      packet.QoS(m.QoS),
		PacketID: m.PacketID,
		Topic:    []byte(m.Topic),
		Payload:  m.Payload,
	}
	applyForwardProps(&p.Props, m.Props)
	_ = c.sendForeign(p)
}

func (c *conn) onRetry(id uint16) {
	if c.closedRC.Load() != 0 || c.b.lookup(c.clientID()) != c {
		return
	}
	m, ok := c.rt.InflightByID(id)
	if !ok {
		return
	}
	now := c.b.opt.Now()
	if m.ExpiryUnixMs != 0 && now >= m.ExpiryUnixMs {
		c.undoInflight(id)
		c.promoteQueued()
		return
	}
	m.Dup = true
	c.resend(m)
	_ = c.rt.NoteRetry(c.epoch, id, now)
	c.armRetry(id)
}

func (c *conn) armRetry(id uint16) {
	if c.closedRC.Load() != 0 || c.rt == nil {
		return
	}
	m, ok := c.rt.InflightByID(id)
	if !ok {
		return
	}
	delay := retryDelay(m.Retries)
	now := c.b.opt.Now()
	if m.ExpiryUnixMs != 0 {
		remain := time.Duration(m.ExpiryUnixMs-now) * time.Millisecond
		if remain < delay {
			delay = remain
		}
		if delay < 0 {
			delay = 0
		}
	}
	stop := c.b.opt.AfterFunc(delay, func() { c.onRetry(id) })
	retry := c.retryState()
	retry.mu.Lock()
	defer retry.mu.Unlock()
	if c.closedRC.Load() != 0 {
		stop()
		return
	}
	if retry.stops == nil {
		retry.stops = make(map[uint16]func() bool)
	}
	if prev := retry.stops[id]; prev != nil {
		prev()
	}
	retry.stops[id] = stop
}

func (c *conn) clearRetry(id uint16) {
	retry := c.retry.Load()
	if retry == nil {
		return
	}
	retry.mu.Lock()
	stop := retry.stops[id]
	delete(retry.stops, id)
	retry.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (c *conn) stopRetries() {
	retry := c.retry.Load()
	if retry == nil {
		return
	}
	retry.mu.Lock()
	stops := retry.stops
	retry.stops = nil
	retry.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
}

func (c *conn) undoInflight(id uint16) {
	c.clearRetry(id)
	if c.rt != nil && c.rt.RemoveInflight(c.epoch, id) {
		c.releaseInflightQuota(1)
	}
}

// promoteQueued 在一次确认腾出窗口后发出队列里的下一条。
func (c *conn) promoteQueued() {
	if c.rt == nil {
		return
	}
	for {
		m, ok := c.rt.PopQueued(c.epoch)
		if !ok {
			return
		}
		if m.ExpiryUnixMs != 0 && c.b.opt.Now() >= m.ExpiryUnixMs {
			continue
		}
		c.deliver(&router.Message{
			Topic:        m.Topic,
			Payload:      m.Payload,
			Props:        m.Props,
			QoS:          m.QoS,
			Retain:       m.Retain,
			ExpiryUnixMs: m.ExpiryUnixMs,
		}, router.SubRef{ClientID: c.clientID(), QoS: m.QoS, RetainAsPub: true}, m.SubIDs)
		return
	}
}

// seedPacketIDs 把分配器抬到在途表里最大的标识之上。
func (c *conn) seedPacketIDs(msgs []session.InflightMsg) {
	var max uint32
	for _, m := range msgs {
		if uint32(m.PacketID) > max {
			max = uint32(m.PacketID)
		}
	}
	if max == 0 {
		return
	}
	next := &c.retryState().nextPacketID
	for {
		cur := next.Load()
		if cur >= max || next.CompareAndSwap(cur, max) {
			return
		}
	}
}

func (c *conn) outboundHeld() int64 {
	q, ok := c.b.opt.Limiter.(interface {
		Current(quota.ID, string) int64
	})
	if !ok {
		return 0
	}
	return q.Current(quota.OutboundInflight, c.clientID())
}

func (c *conn) acquireDeficit(n int) int {
	if n <= 0 {
		return 0
	}
	if c.b.opt.Limiter.Acquire(quota.OutboundInflight, c.clientID(), n) {
		return n
	}
	got := 0
	for i := 0; i < n; i++ {
		if !c.b.opt.Limiter.Acquire(quota.OutboundInflight, c.clientID(), 1) {
			break
		}
		got++
	}
	return got
}

func (c *conn) releaseInflightQuota(n int) {
	if n <= 0 {
		return
	}
	c.b.opt.Limiter.Release(quota.OutboundInflight, c.clientID(), n)
}
