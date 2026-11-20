package broker

import "context"

// ReservedRequest 是一条落在保留主题上的请求。
type ReservedRequest struct {
	Topic   string
	Payload []byte
	// ClientID 与 Thing 来自已认证的主体，供服务做归属判断与审计。
	ClientID string
	Thing    string
}

// ReservedMessage 是保留主题服务要求投递的一条消息。
type ReservedMessage struct {
	Topic   string
	Payload []byte
	// QoS1 为真表示以 QoS 1 投递。
	QoS1 bool
}

// ReservedService 处理 $aws/ 保留主题上的平台请求（Shadow、Jobs 等）。
//
// 定义成接口而不是直接依赖 shadow 包，有两个理由：
//   - broker 不应随平台功能增长而膨胀。它只需知道"某些保留主题由平台
//     服务处理，处理结果是一组待投递的消息"，不需要知道 Shadow 的语义。
//   - 保持 broker 可独立测试。注入一个假服务就能验证接线，
//     不必把 Shadow 的全部语义拖进 broker 的测试里。
type ReservedService interface {
	// HandleReserved 处理一条发布。
	//
	// ok 为假表示该主题不由本服务负责，broker 按普通主题处理。
	// 返回的消息由 broker 经正常路由投递——因此订阅匹配、
	// iot:Receive 复查、QoS 与离线排队都自动适用，服务层不必重复实现。
	HandleReserved(ctx context.Context, req ReservedRequest) (resp []ReservedMessage, ok bool)
}
