package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/itmisx/open-iot-core/internal/broker"
)

// 抓包看的是经过本节点的发布，不是控制台自己订的一份副本。
// 因此它不走测试客户端那条订阅，也不会占设备的订阅额度。

func (a *adminServer) handleCapture(w http.ResponseWriter, r *http.Request) {
	ws, ok := upgradeWS(w, r)
	if !ok {
		return
	}
	defer func() { _ = ws.conn.Close() }()

	cap, err := a.s.brk.OpenCapture(1, 50)
	if err != nil {
		return
	}
	defer cap.Close()

	if err := ws.writeJSON(map[string]any{"op": "ready"}); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeCapture(ws, cap)
	}()
	readCapture(ws, cap)
	cap.Close()
	_ = ws.writeFrame(0x8, nil)
	<-done
}

func writeCapture(ws *consoleWS, cap *broker.Capture) {
	for {
		select {
		case <-cap.Done():
			return
		case msg, ok := <-cap.Messages():
			if !ok {
				return
			}
			if err := ws.writeJSON(map[string]any{
				"op": "message", "topic": msg.Topic, "qos": msg.QoS,
				"payload": base64.StdEncoding.EncodeToString(msg.Payload),
				"dropped": cap.Dropped(),
			}); err != nil {
				return
			}
		}
	}
}

func readCapture(ws *consoleWS, cap *broker.Capture) {
	for {
		op, payload, err := ws.readFrame()
		if err != nil {
			return
		}
		if op == 0x8 {
			return
		}
		if op == 0x9 {
			_ = ws.writeFrame(0xA, payload)
			continue
		}
		if op != 0x1 {
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "只接受文本帧"})
			continue
		}
		var cmd struct {
			Op     string `json:"op"`
			Filter string `json:"filter"`
			Sample uint32 `json:"sample"`
			Rate   int    `json:"rate"`
		}
		if json.Unmarshal(payload, &cmd) != nil {
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "命令无法解析"})
			continue
		}
		switch cmd.Op {
		case "watch":
			if err := cap.Watch(cmd.Filter); err != nil {
				_ = ws.writeJSON(map[string]string{"op": "error", "error": err.Error()})
				continue
			}
			_ = ws.writeJSON(map[string]any{"op": "watching", "filter": cmd.Filter})
		case "unwatch":
			_ = ws.writeJSON(map[string]any{
				"op": "unwatched", "filter": cmd.Filter, "removed": cap.Unwatch(cmd.Filter),
			})
		default:
			_ = ws.writeJSON(map[string]string{"op": "error", "error": "未知操作"})
		}
	}
}
