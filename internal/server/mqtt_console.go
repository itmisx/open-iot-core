package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

// 控制台的 MQTT 测试客户端走管理面，不走设备的 /mqtt。
// 浏览器的 WebSocket 不能设置 Authorization 头，令牌因此在查询参数里，
// 与事件流同一条理由。

const (
	consoleWSGUID   = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxConsoleFrame = 256 << 10
)

func upgradeWS(w http.ResponseWriter, r *http.Request) (*consoleWS, bool) {
	if !headerHasToken(r.Header, "Upgrade", "websocket") {
		writeAPIError(w, http.StatusBadRequest, "需要 WebSocket", "")
		return nil, false
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	rawKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(rawKey) != 16 {
		writeAPIError(w, http.StatusBadRequest, "缺少 WebSocket 密钥", "")
		return nil, false
	}
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "无法升级连接", "")
		return nil, false
	}
	accept := sha1.Sum([]byte(key + consoleWSGUID))
	if _, err := rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(accept[:]) + "\r\n\r\n"); err != nil {
		_ = conn.Close()
		return nil, false
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, false
	}
	return &consoleWS{conn: conn, rw: rw}, true
}

func (a *adminServer) handleMQTTConsole(w http.ResponseWriter, r *http.Request) {
	ws, ok := upgradeWS(w, r)
	if !ok {
		return
	}
	defer func() { _ = ws.conn.Close() }()

	tap, err := a.s.brk.OpenTap()
	if err != nil {
		return
	}
	defer tap.Close()

	if err := ws.writeJSON(map[string]any{"op": "ready"}); err != nil {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.writeConsoleMessages(ws, tap)
	}()

	a.readConsoleCommands(r, ws, tap)
	tap.Close()
	_ = ws.writeFrame(0x8, nil)
	<-done
}

type consoleWS struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	mu   sync.Mutex
}

func (c *consoleWS) writeJSON(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.writeFrame(0x1, raw)
}

func (c *consoleWS) writeFrame(op byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var hdr []byte
	hdr = append(hdr, 0x80|op)
	n := len(payload)
	switch {
	case n < 126:
		hdr = append(hdr, byte(n))
	case n <= 0xffff:
		hdr = append(hdr, 126, byte(n>>8), byte(n))
	default:
		return errors.New("帧过大")
	}
	if _, err := c.rw.Write(hdr); err != nil {
		return err
	}
	if _, err := c.rw.Write(payload); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *consoleWS) readFrame() (byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.rw, hdr[:]); err != nil {
		return 0, nil, err
	}
	if hdr[0]&0x70 != 0 || hdr[0]&0x80 == 0 {
		return 0, nil, errors.New("不支持的 WebSocket 帧")
	}
	if hdr[1]&0x80 == 0 {
		return 0, nil, errors.New("客户端帧必须掩码")
	}
	n := int(hdr[1] & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.rw, ext[:]); err != nil {
			return 0, nil, err
		}
		n = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.rw, ext[:]); err != nil {
			return 0, nil, err
		}
		u := binary.BigEndian.Uint64(ext[:])
		if u > maxConsoleFrame {
			return 0, nil, errors.New("帧过大")
		}
		n = int(u)
	}
	if n > maxConsoleFrame {
		return 0, nil, errors.New("帧过大")
	}
	var mask [4]byte
	if _, err := io.ReadFull(c.rw, mask[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.rw, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return hdr[0] & 0x0f, payload, nil
}

type consoleCmd struct {
	Op              string      `json:"op"`
	Filter          string      `json:"filter"`
	Topic           string      `json:"topic"`
	Payload         string      `json:"payload"`
	QoS             uint8       `json:"qos"`
	Retain          bool        `json:"retain"`
	ContentType     string      `json:"contentType"`
	PayloadFormat   *uint8      `json:"payloadFormat"`
	ResponseTopic   string      `json:"responseTopic"`
	CorrelationData string      `json:"correlationData"`
	UserProperties  [][2]string `json:"userProperties"`
	MessageExpiry   uint32      `json:"messageExpiry"`
}

func (a *adminServer) readConsoleCommands(r *http.Request, ws *consoleWS, tap *broker.Tap) {
	for {
		op, payload, err := ws.readFrame()
		if err != nil {
			return
		}
		switch op {
		case 0x8:
			return
		case 0x9:
			if err := ws.writeFrame(0xA, payload); err != nil {
				return
			}
			continue
		case 0xA:
			continue
		case 0x1:
		default:
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "只接受文本帧"})
			continue
		}
		dec := json.NewDecoder(strings.NewReader(string(payload)))
		dec.DisallowUnknownFields()
		var cmd consoleCmd
		if err := dec.Decode(&cmd); err != nil {
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "请求无法解析"})
			continue
		}
		switch cmd.Op {
		case "subscribe":
			if err := tap.Subscribe(cmd.Filter, cmd.QoS); err != nil {
				_ = ws.writeJSON(map[string]string{"op": "error", "error": err.Error()})
				continue
			}
			a.s.emitAudit("admin", r.RemoteAddr, "WS", r.URL.Path, "ConsoleSubscribe", cmd.Filter)
			_ = ws.writeJSON(map[string]any{"op": "suback", "filter": cmd.Filter, "qos": cmd.QoS})
		case "unsubscribe":
			if !tap.Unsubscribe(cmd.Filter) {
				_ = ws.writeJSON(map[string]string{"op": "error", "error": "没有这条订阅"})
				continue
			}
			a.s.emitAudit("admin", r.RemoteAddr, "WS", r.URL.Path, "ConsoleUnsubscribe", cmd.Filter)
			_ = ws.writeJSON(map[string]any{"op": "unsuback", "filter": cmd.Filter})
		case "publish":
			if err := a.consolePublish(r, cmd); err != nil {
				_ = ws.writeJSON(map[string]string{"op": "error", "error": err.Error()})
				continue
			}
			_ = ws.writeJSON(map[string]any{"op": "published", "topic": cmd.Topic})
		default:
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "未知操作"})
		}
	}
}

func (a *adminServer) consolePublish(r *http.Request, cmd consoleCmd) error {
	if p, ok := principalFrom(r.Context()); ok && p.Role == roleReader {
		return errors.New("当前角色不能发布")
	}
	var payload []byte
	var err error
	if cmd.Payload != "" {
		payload, err = base64.StdEncoding.DecodeString(cmd.Payload)
		if err != nil {
			return errors.New("负载不是 base64")
		}
	}
	var corr []byte
	if cmd.CorrelationData != "" {
		corr, err = base64.StdEncoding.DecodeString(cmd.CorrelationData)
		if err != nil {
			return errors.New("correlationData 不是 base64")
		}
	}
	props := &packet.Properties{}
	if cmd.ContentType != "" {
		props.ContentType = []byte(cmd.ContentType)
		props.Set(packet.PropContentType)
	}
	if cmd.PayloadFormat != nil {
		props.PayloadFormat = *cmd.PayloadFormat
		props.Set(packet.PropPayloadFormat)
	}
	if cmd.ResponseTopic != "" {
		props.ResponseTopic = []byte(cmd.ResponseTopic)
		props.Set(packet.PropResponseTopic)
	}
	if cmd.CorrelationData != "" {
		props.CorrelationData = corr
		props.Set(packet.PropCorrelationData)
	}
	if len(cmd.UserProperties) > 0 {
		for _, kv := range cmd.UserProperties {
			props.UserProps = append(props.UserProps, packet.UserProperty{
				Key: []byte(kv[0]), Value: []byte(kv[1]),
			})
		}
		props.Set(packet.PropUserProperty)
	}
	if props.Empty() {
		props = nil
	}
	err = a.s.brk.PublishConsole(r.Context(), broker.ConsolePublish{
		Topic: cmd.Topic, Payload: payload, QoS: cmd.QoS, Retain: cmd.Retain,
		Props: props, ExpiryS: cmd.MessageExpiry,
	})
	if err != nil {
		return err
	}
	// 审计只记主题。负载可能是设备数据，甚至是操作者贴进来的私钥。
	a.s.emitAudit("admin", r.RemoteAddr, "WS", r.URL.Path, "ConsolePublish", cmd.Topic)
	return nil
}

func (a *adminServer) writeConsoleMessages(ws *consoleWS, tap *broker.Tap) {
	for {
		select {
		case <-tap.Done():
			return
		case m := <-tap.Messages():
			body := map[string]any{
				"op":      "message",
				"topic":   m.Topic,
				"payload": base64.StdEncoding.EncodeToString(m.Payload),
				"qos":     m.QoS,
				"retain":  m.Retain,
			}
			if dropped := tap.Dropped(); dropped > 0 {
				body["dropped"] = dropped
			}
			fillConsoleProps(body, m, a.s.brk)
			if err := ws.writeJSON(body); err != nil {
				return
			}
		}
	}
}

func fillConsoleProps(body map[string]any, m broker.TapMessage, b *broker.Broker) {
	if len(m.Props) > 0 {
		var p packet.Properties
		if packet.DecodeProps(m.Props, packet.PublishContext, &p) == nil {
			if p.Has(packet.PropContentType) {
				body["contentType"] = string(p.ContentType)
			}
			if p.Has(packet.PropPayloadFormat) {
				body["payloadFormat"] = p.PayloadFormat
			}
			if p.Has(packet.PropResponseTopic) {
				body["responseTopic"] = string(p.ResponseTopic)
			}
			if p.Has(packet.PropCorrelationData) {
				body["correlationData"] = base64.StdEncoding.EncodeToString(p.CorrelationData)
			}
			if p.Has(packet.PropUserProperty) && len(p.UserProps) > 0 {
				pairs := make([][2]string, 0, len(p.UserProps))
				for _, kv := range p.UserProps {
					if utf8.Valid(kv.Key) && utf8.Valid(kv.Value) {
						pairs = append(pairs, [2]string{string(kv.Key), string(kv.Value)})
					}
				}
				body["userProperties"] = pairs
			}
		}
	}
	if m.ExpiryUnixMs > 0 {
		remain := m.ExpiryUnixMs - b.Now()
		if remain < 0 {
			remain = 0
		}
		body["messageExpiry"] = uint32(remain / 1000)
	}
}

func headerHasToken(h http.Header, name, want string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), want) {
				return true
			}
		}
	}
	return false
}
