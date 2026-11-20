// Package trace 把控制面请求导出成 OpenTelemetry 追踪。
//
// 不引入官方 SDK。指标那边已经拒绝过 prometheus/client_golang：
// 它会带进几十个模块。OTLP/HTTP 的 JSON 映射本身是稳定的，
// 控制面的请求量也小，自己编码这一种导出就够了。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	kindInternal = 1
	kindServer   = 2
)

// Span 是一条进行中的追踪。Nop 实现的方法什么都不做。
type Span interface {
	Set(key, value string)
	SetInt(key string, value int)
	SetError()
	End()
	TraceID() string
	SpanID() string
}

type nopSpan struct{}

func (nopSpan) Set(string, string) {}
func (nopSpan) SetInt(string, int) {}
func (nopSpan) SetError()          {}
func (nopSpan) End()               {}
func (nopSpan) TraceID() string    { return "" }
func (nopSpan) SpanID() string     { return "" }

// Options 是追踪器的装配参数。
type Options struct {
	// Now 是 Unix 毫秒。导出时换算成纳秒。未启用时可以不给。
	Now func() int64
	// Service 写入 resource 的 service.name。空则用 open-iot-core。
	Service string
	// Endpoint 是收集器基址，例如 http://127.0.0.1:4318。
	// 导出路径固定为 /v1/traces。
	Endpoint string
	Enabled  bool
	// OnError 在导出失败时调用。不要在里面再发起会被追踪的请求。
	OnError func(error)
	// Client 供测试替换。为空时用带超时的默认客户端。
	Client *http.Client
}

// Tracer 收集 span 并异步导出。未启用时 Start 返回空操作。
type Tracer struct {
	disabled bool
	now      func() int64
	service  string
	endpoint string
	onError  func(error)
	client   *http.Client

	ch      chan finished
	dropped atomic.Uint64
	once    sync.Once
}

// New 创建追踪器。未启用时返回的值可以安全使用，不会导出。
func New(opt Options) *Tracer {
	if !opt.Enabled {
		return &Tracer{disabled: true}
	}
	svc := opt.Service
	if svc == "" {
		svc = "open-iot-core"
	}
	now := opt.Now
	if now == nil {
		now = func() int64 { return 0 }
	}
	client := opt.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Tracer{
		now: now, service: svc, endpoint: strings.TrimRight(opt.Endpoint, "/"),
		onError: opt.OnError, client: client,
		ch: make(chan finished, 64),
	}
}

// Enabled 报告追踪是否会导出。
func (t *Tracer) Enabled() bool { return t != nil && !t.disabled }

// Run 导出已结束的 span，直到 ctx 取消，然后把队列里剩下的也送出。
func (t *Tracer) Run(ctx context.Context) {
	if !t.Enabled() {
		return
	}
	for {
		select {
		case <-ctx.Done():
			t.drain()
			return
		case sp := <-t.ch:
			t.exportOne(sp)
		}
	}
}

func (t *Tracer) drain() {
	for {
		select {
		case sp := <-t.ch:
			t.exportOne(sp)
		default:
			return
		}
	}
}

// Start 开始一条 span。ctx 里若已有 span，新 span 成为它的子级。
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, Span) {
	if !t.Enabled() {
		return ctx, nopSpan{}
	}
	return t.start(ctx, name, kindInternal, "")
}

// Start 从 ctx 里取出追踪器再开一条 span。没有追踪器时什么都不做。
func Start(ctx context.Context, name string) (context.Context, Span) {
	t, _ := ctx.Value(tracerKey{}).(*Tracer)
	if t == nil {
		return ctx, nopSpan{}
	}
	return t.Start(ctx, name)
}

func (t *Tracer) start(ctx context.Context, name string, kind int, traceparent string) (context.Context, Span) {
	traceID, parentID := "", ""
	if p, ok := ctx.Value(spanKey{}).(*live); ok {
		traceID, parentID = p.traceID, p.spanID
	} else if tid, sid, ok := parseTraceparent(traceparent); ok {
		traceID, parentID = tid, sid
	}
	if traceID == "" {
		id, err := newHex(16)
		if err != nil {
			return ctx, nopSpan{}
		}
		traceID = id
	}
	spanID, err := newHex(8)
	if err != nil {
		return ctx, nopSpan{}
	}
	sp := &live{
		tr: t, name: name, kind: kind,
		traceID: traceID, spanID: spanID, parentID: parentID,
		startMs: t.now(),
	}
	ctx = context.WithValue(ctx, tracerKey{}, t)
	ctx = context.WithValue(ctx, spanKey{}, sp)
	return ctx, sp
}

type tracerKey struct{}
type spanKey struct{}

type live struct {
	tr       *Tracer
	name     string
	kind     int
	traceID  string
	spanID   string
	parentID string
	startMs  int64
	attrs    []attr
	err      bool
	ended    bool
}

type attr struct {
	key   string
	value string
	isInt bool
}

func (s *live) Set(key, value string) {
	if s.ended || !allowedAttr(key, value) {
		return
	}
	s.attrs = append(s.attrs, attr{key: key, value: value})
}

func (s *live) SetInt(key string, value int) {
	if s.ended || !allowedAttr(key, "") {
		return
	}
	s.attrs = append(s.attrs, attr{key: key, value: itoa(value), isInt: true})
}

func (s *live) SetError() {
	if !s.ended {
		s.err = true
	}
}

func (s *live) End() {
	if s.ended {
		return
	}
	s.ended = true
	end := s.tr.now()
	if end < s.startMs {
		end = s.startMs
	}
	sp := finished{
		name: s.name, kind: s.kind,
		traceID: s.traceID, spanID: s.spanID, parentID: s.parentID,
		startMs: s.startMs, endMs: end,
		attrs: append([]attr(nil), s.attrs...),
		err:   s.err,
	}
	select {
	case s.tr.ch <- sp:
	default:
		s.tr.dropped.Add(1)
	}
}

func (s *live) TraceID() string { return s.traceID }
func (s *live) SpanID() string  { return s.spanID }

func (s *live) rename(name string) {
	if !s.ended && name != "" {
		s.name = name
	}
}

// allowedAttr 挡住凭证与私钥。这些字段一旦进了收集器，就和写进审计文件一样收不回来。
func allowedAttr(key, value string) bool {
	k := strings.ToLower(key)
	switch {
	case strings.Contains(k, "authorization"),
		strings.Contains(k, "token"),
		strings.Contains(k, "secret"),
		strings.Contains(k, "private"),
		strings.Contains(k, "password"),
		strings.Contains(k, "signature"):
		return false
	}
	if strings.Contains(value, "PRIVATE KEY") || strings.Contains(value, "AWS4-HMAC-SHA256") {
		return false
	}
	return true
}

func newHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// parseTraceparent 解析 W3C traceparent。非法或全零的标识返回 false。
func parseTraceparent(h string) (traceID, spanID string, ok bool) {
	h = strings.TrimSpace(h)
	parts := strings.Split(h, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", false
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", false
	}
	traceID = strings.ToLower(parts[1])
	spanID = strings.ToLower(parts[2])
	if !isHex(traceID) || !isHex(spanID) || allZero(traceID) || allZero(spanID) {
		return "", "", false
	}
	return traceID, spanID, true
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
