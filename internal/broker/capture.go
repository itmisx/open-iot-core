package broker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/router"
)

// 主题抓包是观察者，不是订阅者。
//
// 如果把它登记进路由表，它会占全局订阅额度，也会在共享订阅组里抢走一条消息。
// 抓包的人看的是「经过这里的发布」，不该改变投递给谁。

const (
	captureQueue   = 64
	captureMaxSubs = 8
	captureRateMax = 200
)

// Capture 是一条抓包会话。
type Capture struct {
	b  *Broker
	id string

	mu       sync.Mutex
	closed   bool
	filters  map[string]struct{}
	sample   uint32
	rate     int
	tokens   float64
	lastUnix int64
	seen     uint64
	ch       chan TapMessage
	done     chan struct{}
	dropped  atomic.Int64
}

// OpenCapture 打开一条抓包会话。sample 小于 2 表示全收。rate 是每秒条数，越界时收成 50。
func (b *Broker) OpenCapture(sample uint32, rate int) (*Capture, error) {
	if b.closed.Load() {
		return nil, ErrClosed
	}
	if sample < 1 {
		sample = 1
	}
	if rate <= 0 {
		rate = 50
	}
	if rate > captureRateMax {
		rate = captureRateMax
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return nil, err
	}
	c := &Capture{
		b: b, id: "capture-" + hex.EncodeToString(buf[:]),
		filters: make(map[string]struct{}),
		sample:  sample, rate: rate, tokens: float64(rate),
		ch:   make(chan TapMessage, captureQueue),
		done: make(chan struct{}),
	}
	b.capMu.Lock()
	b.caps[c.id] = c
	b.capMu.Unlock()
	return c, nil
}

func (b *Broker) closeCaptures() {
	b.capMu.Lock()
	all := make([]*Capture, 0, len(b.caps))
	for _, c := range b.caps {
		all = append(all, c)
	}
	b.capMu.Unlock()
	for _, c := range all {
		c.Close()
	}
}

// Messages 是抓到的消息。
func (c *Capture) Messages() <-chan TapMessage { return c.ch }

// Done 在 Close 时关闭。
func (c *Capture) Done() <-chan struct{} { return c.done }

// Dropped 是因采样之外的速率或队列满丢掉的条数。
func (c *Capture) Dropped() int64 { return c.dropped.Load() }

// Watch 加上一条过滤器。
func (c *Capture) Watch(filter string) error {
	if strings.HasPrefix(filter, "$share/") {
		return errors.New("broker: 抓包不接受共享订阅")
	}
	if err := topic.ValidateFilter([]byte(filter), c.b.opt.Limits); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if _, ok := c.filters[filter]; !ok && len(c.filters) >= captureMaxSubs {
		return errCaptureFull
	}
	c.filters[filter] = struct{}{}
	return nil
}

// Unwatch 去掉一条过滤器。
func (c *Capture) Unwatch(filter string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, ok := c.filters[filter]; !ok {
		return false
	}
	delete(c.filters, filter)
	return true
}

// Close 结束会话。可以多次调用。
func (c *Capture) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	c.b.capMu.Lock()
	delete(c.b.caps, c.id)
	c.b.capMu.Unlock()
}

func (b *Broker) fanoutCapture(msg *router.Message) {
	b.capMu.RLock()
	if len(b.caps) == 0 {
		b.capMu.RUnlock()
		return
	}
	all := make([]*Capture, 0, len(b.caps))
	for _, c := range b.caps {
		all = append(all, c)
	}
	b.capMu.RUnlock()
	now := b.opt.Now()
	for _, c := range all {
		c.consider(msg.Topic, msg.Payload, msg.QoS, now)
	}
}

func (c *Capture) consider(name string, payload []byte, qos uint8, now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.filters) == 0 {
		return
	}
	matched := false
	for f := range c.filters {
		if topic.Match([]byte(f), []byte(name), c.b.opt.Limits) {
			matched = true
			break
		}
	}
	if !matched {
		return
	}
	c.seen++
	if c.sample > 1 && c.seen%uint64(c.sample) != 0 {
		return
	}
	if !c.takeToken(now) {
		c.dropped.Add(1)
		return
	}
	select {
	case c.ch <- TapMessage{
		Topic: name, Payload: append([]byte(nil), payload...), QoS: qos,
	}:
	default:
		c.dropped.Add(1)
	}
}

func (c *Capture) takeToken(now int64) bool {
	if c.lastUnix != 0 {
		elapsed := now - c.lastUnix
		if elapsed > 0 {
			c.tokens += float64(elapsed) * float64(c.rate) / 1000
		}
	}
	if c.tokens > float64(c.rate) {
		c.tokens = float64(c.rate)
	}
	c.lastUnix = now
	if c.tokens < 1 {
		return false
	}
	c.tokens--
	return true
}

var errCaptureFull = errString("broker: 抓包过滤器已达上限")

type errString string

func (e errString) Error() string { return string(e) }
