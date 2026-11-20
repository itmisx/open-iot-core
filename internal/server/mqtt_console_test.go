package server

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// TestConsoleMQTTRoundTrip 验证管理面测试客户端能订阅、发布，并收到设备的发布。
//
// 去掉进程内订阅之后，WebSocket 仍能升级，但消息不会回来。
// 审计里出现负载，说明把设备数据写进了审计文件。
func TestConsoleMQTTRoundTrip(t *testing.T) {
	r := newAdminRig(t, nil)
	cert := r.issue("dev-m")
	r.start()

	ws := dialConsoleMQTT(t, r.s.AdminAddr(), testAdminToken)
	defer ws.Close()
	ready := ws.readJSON(t)
	if ready["op"] != "ready" {
		t.Fatalf("首帧 = %v", ready)
	}

	ws.sendJSON(t, map[string]any{"op": "subscribe", "filter": "things/dev-m/#", "qos": 1})
	ack := ws.readJSON(t)
	if ack["op"] != "suback" {
		t.Fatalf("订阅应答 = %v", ack)
	}

	secret := "-----BEGIN PRIVATE KEY-----\nnot-a-real-key"
	ws.sendJSON(t, map[string]any{
		"op": "publish", "topic": "things/dev-m/temp", "qos": 2,
		"payload":        base64.StdEncoding.EncodeToString([]byte(secret)),
		"contentType":    "application/json",
		"userProperties": [][2]string{{"src", "console"}},
	})
	var got, echo map[string]any
	seenPub, seenEcho := false, false
	for i := 0; i < 6 && !(seenPub && seenEcho); i++ {
		got = ws.readJSON(t)
		switch got["op"] {
		case "published":
			seenPub = true
		case "message":
			seenEcho = true
			echo = got
		default:
			t.Fatalf("意外的帧: %v", got)
		}
	}
	if !seenEcho {
		t.Fatalf("没有收到自己发布的消息: %v", got)
	}
	raw, err := base64.StdEncoding.DecodeString(echo["payload"].(string))
	if err != nil || string(raw) != secret {
		t.Fatalf("负载 = %q %v", raw, err)
	}
	if echo["contentType"] != "application/json" {
		t.Fatalf("contentType = %v", echo["contentType"])
	}
	if echo["qos"].(float64) != 1 {
		t.Fatalf("出站 QoS 应按订阅降到 1，得到 %v", echo["qos"])
	}

	dev, err := r.dial(&cert)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.nc.Close()
	if ack := dev.connect("dev-m"); ack.Code != reason.Success {
		t.Fatalf("设备连接 = %s", ack.Code)
	}
	if code := dev.publish("things/dev-m/from-device", "from-device", packet.QoS0); code != reason.Success {
		t.Fatalf("设备发布 = %s", code)
	}
	msg := ws.readJSON(t)
	if msg["op"] != "message" || msg["topic"] != "things/dev-m/from-device" {
		t.Fatalf("没有收到设备的发布: %v", msg)
	}

	body := waitAudit(t, r.dd.Path(auditLogName), `"action":"ConsolePublish"`)
	if strings.Contains(body, "PRIVATE KEY") || strings.Contains(body, secret) {
		t.Fatalf("审计带上了负载: %s", body)
	}
	if !strings.Contains(body, "things/dev-m/temp") {
		t.Fatalf("审计没有主题: %s", body)
	}
}

func TestConsoleMQTTRejectsMissingToken(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	conn, err := net.Dial("tcp", r.s.AdminAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	_, err = io.WriteString(conn, "GET "+adminAPIPrefix+"/mqtt HTTP/1.1\r\n"+
		"Host: "+r.s.AdminAddr()+"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: "+key+"\r\nSec-WebSocket-Version: 13\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "401") {
		t.Fatalf("无令牌应是 401，得到 %s", line)
	}
}

type consoleDial struct {
	c net.Conn
	r *bufio.Reader
}

func dialConsoleMQTT(t *testing.T, addr, token string) *consoleDial {
	t.Helper()
	return dialAdminWS(t, addr, adminAPIPrefix+"/mqtt", token)
}

func dialAdminWS(t *testing.T, addr, path, token string) *consoleDial {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	var keyBuf [16]byte
	if _, err := rand.Read(keyBuf[:]); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(keyBuf[:])
	_, err = io.WriteString(conn, "GET "+path+"?token="+token+" HTTP/1.1\r\n"+
		"Host: "+addr+"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: "+key+"\r\nSec-WebSocket-Version: 13\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("升级状态 = %s", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &consoleDial{c: conn, r: br}
}

func (d *consoleDial) Close() { _ = d.c.Close() }

func (d *consoleDial) sendJSON(t *testing.T, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	_ = d.c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := d.c.Write(maskClientFrame(0x1, raw)); err != nil {
		t.Fatal(err)
	}
}

func (d *consoleDial) readJSON(t *testing.T) map[string]any {
	t.Helper()
	_ = d.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	payload := readServerFrame(t, d.r)
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("无法解析 %q: %v", payload, err)
	}
	return m
}

func maskClientFrame(op byte, payload []byte) []byte {
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	hdr := []byte{0x80 | op}
	n := len(payload)
	switch {
	case n < 126:
		hdr = append(hdr, 0x80|byte(n))
	default:
		hdr = append(hdr, 0x80|126, byte(n>>8), byte(n))
	}
	hdr = append(hdr, mask[:]...)
	out := append([]byte(nil), hdr...)
	for i, b := range payload {
		out = append(out, b^mask[i%4])
	}
	return out
}

func readServerFrame(t *testing.T, r *bufio.Reader) []byte {
	t.Helper()
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatal(err)
	}
	if hdr[1]&0x80 != 0 {
		t.Fatal("服务端帧不应掩码")
	}
	n := int(hdr[1] & 0x7f)
	if n == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			t.Fatal(err)
		}
		n = int(binary.BigEndian.Uint16(ext[:]))
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
