package server

import (
	"context"

	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/shadow"
)

// shadowAdapter 把 shadow.Service 接到 broker 的保留主题扩展点上。
//
// 适配器很薄，而这正是重点：两侧都不知道对方。broker 只知道
// "保留主题的请求会产出一组待投递的消息"，shadow 只知道
// "我产出一组 Publish"。REST 数据面将来接的是同一个 shadow.Service，
// 因此 delta 与 documents 的发布逻辑不会出现第二份实现
// （PRD SHDW-044、SHDW-045）。
type shadowAdapter struct {
	svc *shadow.Service
}

// HandleReserved 实现 broker.ReservedService。
func (a shadowAdapter) HandleReserved(ctx context.Context, req broker.ReservedRequest) (
	[]broker.ReservedMessage, bool,
) {
	sr, ok := shadow.ParseRequest(req.Topic)
	if !ok {
		return nil, false
	}
	out := a.svc.Handle(ctx, sr, req.Payload)

	// 响应（accepted 或 rejected）与随之产生的通知一并返回。
	// 容量预分配：响应一条，加上最多 delta 与 documents 两条。
	msgs := make([]broker.ReservedMessage, 0, 1+len(out.Publishes))
	msgs = append(msgs, broker.ReservedMessage{
		Topic:   out.ResponseTopic,
		Payload: out.Response,
		// 响应以 QoS 1 投递：设备通常订阅 /accepted 来确认请求结果，
		// 丢掉它会让设备一直等待，直到超时重发整条请求。
		QoS1: true,
	})
	for _, p := range out.Publishes {
		msgs = append(msgs, broker.ReservedMessage{
			Topic: p.Topic, Payload: p.Payload, QoS1: p.QoS1,
		})
	}
	return msgs, true
}
