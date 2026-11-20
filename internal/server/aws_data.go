package server

import (
	"context"
	"errors"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/router"
)

func (a *adminServer) publishData(ctx context.Context, topic string, payload []byte, qos uint8, retain bool) error {
	err := a.s.brk.PublishData(ctx, topic, payload, qos, retain)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, broker.ErrPayloadTooLarge):
		return &awsapi.DataError{Type: "RequestEntityTooLargeException", Message: "负载超出上限"}
	case errors.Is(err, broker.ErrRateLimited):
		return &awsapi.DataError{Type: "ThrottlingException", Message: "请求过多"}
	default:
		return &awsapi.DataError{Type: "InvalidRequestException", Message: err.Error()}
	}
}

// retainedView 把路由层的保留表适配成数据面的读取接口。
type retainedView struct {
	s *router.RetainedStore
}

func (v retainedView) Get(topic string) (awsapi.RetainedMessage, bool) {
	m, ok := v.s.Get([]byte(topic))
	if !ok {
		return awsapi.RetainedMessage{}, false
	}
	return awsapi.RetainedMessage{
		Topic: m.Topic, Payload: m.Payload, PayloadSize: len(m.Payload),
		QoS: m.QoS, ModifiedUnixMs: m.StoredAtUnixMs,
	}, true
}

func (v retainedView) List(after string, limit int) ([]awsapi.RetainedMessage, string) {
	page, next := v.s.List(after, limit)
	out := make([]awsapi.RetainedMessage, 0, len(page))
	for _, it := range page {
		out = append(out, awsapi.RetainedMessage{
			Topic: it.Topic, PayloadSize: it.PayloadSize,
			QoS: it.QoS, ModifiedUnixMs: it.StoredAtUnixMs,
		})
	}
	return out, next
}
