package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
)

// 控制台测试客户端的上限。它不是设备：不占连接表，也不进全局订阅额度。
// 全局订阅额度耗尽只能重启恢复（QTA-108），一个调试页面不该有这个能力。
const (
	tapQueue   = 64
	tapMaxSubs = 32
)

// TapMessage 是投递给控制台的一条消息。负载与属性都是副本，
// 调用方可以留着，不会被下一条发布改掉。
type TapMessage struct {
	Topic        string
	Payload      []byte
	QoS          uint8
	Retain       bool
	Props        []byte
	ExpiryUnixMs int64
}

// Tap 是控制台的进程内订阅者。
//
// 它不做成一条 conn：投递跑在发布者的协程上，而 conn 的编码缓冲归
// 连接自己的读协程。再造一条连接等于把控制台塞进那条并发约束里。
type Tap struct {
	b  *Broker
	id string

	mu      sync.Mutex
	closed  bool
	filters map[string]uint8
	ch      chan TapMessage
	done    chan struct{}
	dropped atomic.Int64
}

// ConsolePublish 是控制台发出的一条消息。
type ConsolePublish struct {
	Topic   string
	Payload []byte
	QoS     uint8
	Retain  bool
	Props   *packet.Properties
	ExpiryS uint32
}

// OpenTap 打开一个控制台订阅者。用完要 Close。
func (b *Broker) OpenTap() (*Tap, error) {
	if b.closed.Load() {
		return nil, ErrClosed
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, err
	}
	t := &Tap{
		b:       b,
		id:      "console-" + hex.EncodeToString(buf[:]),
		filters: make(map[string]uint8),
		ch:      make(chan TapMessage, tapQueue),
		done:    make(chan struct{}),
	}
	b.tapMu.Lock()
	b.taps[t.id] = t
	b.tapMu.Unlock()
	return t, nil
}

func (b *Broker) closeTaps() {
	b.tapMu.Lock()
	all := make([]*Tap, 0, len(b.taps))
	for _, t := range b.taps {
		all = append(all, t)
	}
	b.tapMu.Unlock()
	for _, t := range all {
		t.Close()
	}
}

// Messages 是收到的消息。Close 之后这个通道不再有新消息，已在里面的仍可读。
func (t *Tap) Messages() <-chan TapMessage { return t.ch }

// Done 在 Close 时关闭。
func (t *Tap) Done() <-chan struct{} { return t.done }

// Dropped 是因队列满而丢掉的条数。
func (t *Tap) Dropped() int64 { return t.dropped.Load() }

// Subscribe 登记一条过滤器。已有的同名过滤器按新的 QoS 替换。
// 共享订阅用 $share/组名/过滤器，与设备走同一条路由。
func (t *Tap) Subscribe(filter string, qos uint8) error {
	if qos > 2 {
		return errors.New("broker: qos 只接受 0、1 或 2")
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	_, exists := t.filters[filter]
	if !exists && len(t.filters) >= tapMaxSubs {
		t.mu.Unlock()
		return errors.New("broker: 控制台订阅已达上限")
	}
	t.mu.Unlock()

	if exists {
		t.b.opt.Router.Unsubscribe(t.id, filter)
	}
	if err := t.b.opt.Router.Subscribe(router.Subscription{
		ClientID: t.id, Filter: filter, QoS: qos, RetainAsPub: true,
	}); err != nil {
		return err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.b.opt.Router.Unsubscribe(t.id, filter)
		return ErrClosed
	}
	t.filters[filter] = qos
	t.mu.Unlock()

	inner := filter
	if _, rest, ok := topic.ParseShared([]byte(filter)); ok {
		inner = string(rest)
	}
	var matched []*router.Message
	matched = t.b.opt.Router.Retained().MatchInto(matched, []byte(inner))
	for _, m := range matched {
		t.enqueue(TapMessage{
			Topic: m.Topic, Payload: append([]byte(nil), m.Payload...),
			QoS: qos, Retain: true, Props: append([]byte(nil), m.Props...),
			ExpiryUnixMs: m.ExpiryUnixMs,
		})
	}
	return nil
}

// Unsubscribe 去掉一条过滤器。
func (t *Tap) Unsubscribe(filter string) bool {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return false
	}
	if _, ok := t.filters[filter]; !ok {
		t.mu.Unlock()
		return false
	}
	delete(t.filters, filter)
	t.mu.Unlock()
	return t.b.opt.Router.Unsubscribe(t.id, filter)
}

// Close 撤掉全部订阅。可以多次调用。
func (t *Tap) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	close(t.done)
	t.mu.Unlock()

	t.b.opt.Router.RemoveClient(t.id)
	t.b.tapMu.Lock()
	delete(t.b.taps, t.id)
	t.b.tapMu.Unlock()
}

func (t *Tap) enqueue(m TapMessage) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	select {
	case t.ch <- m:
		return true
	default:
		t.dropped.Add(1)
		return false
	}
}

// deliverTap 把消息交给控制台。返回 false 表示这个 clientId 不是控制台。
func (b *Broker) deliverTap(ref router.SubRef, msg *router.Message) bool {
	b.tapMu.RLock()
	t := b.taps[ref.ClientID]
	b.tapMu.RUnlock()
	if t == nil {
		return false
	}
	if msg.ExpiryUnixMs != 0 && msg.ExpiryUnixMs <= b.opt.Now() {
		return true
	}
	return t.enqueue(TapMessage{
		Topic:        msg.Topic,
		Payload:      append([]byte(nil), msg.Payload...),
		QoS:          minU8(msg.QoS, ref.QoS),
		Retain:       msg.Retain,
		Props:        append([]byte(nil), msg.Props...),
		ExpiryUnixMs: msg.ExpiryUnixMs,
	})
}

// PublishConsole 由控制台发布一条消息。
//
// 与数据面 Publish 的差别：这里接受 QoS 2，并带上 MQTT 5 里随消息走的属性。
// 不查 IoT 策略。操作者是已经过管理面令牌的人，不是一台设备。
func (b *Broker) PublishConsole(ctx context.Context, in ConsolePublish) error {
	if b.closed.Load() {
		return ErrClosed
	}
	if in.QoS > 2 {
		return errors.New("broker: qos 只接受 0、1 或 2")
	}
	if !b.opt.Limiter.Within(quota.PayloadSize, len(in.Payload)) {
		return ErrPayloadTooLarge
	}
	if err := topic.ValidateName([]byte(in.Topic), b.opt.Limits); err != nil {
		return err
	}
	msg := &router.Message{
		Topic: in.Topic, Payload: in.Payload, QoS: in.QoS, Retain: in.Retain,
		StoredAtUnixMs: b.opt.Now(),
	}
	if in.Props != nil {
		msg.Props = encodeForwardProps(in.Props)
	}
	if in.ExpiryS > 0 {
		msg.ExpiryUnixMs = b.opt.Now() + int64(in.ExpiryS)*1000
	}
	if in.Retain {
		if !b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRatePerTopic, in.Topic, 1) ||
			!b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRate, "", 1) {
			return ErrRateLimited
		}
		b.opt.Router.Retained().Set(in.Topic, msg, b.opt.Router.NextVersion())
	}
	b.route(nil, msg, nil)
	return nil
}
