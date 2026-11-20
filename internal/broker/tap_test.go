package broker

import (
	"context"
	"testing"
	"time"
)

// TestTapReceivesPublish 验证控制台订阅者不经过设备连接也能收到消息。
//
// 去掉 deliverTap 之后，这个 clientId 在连接表里不存在，消息会掉进
// 离线队列然后消失——测试客户端就成了哑的。
func TestTapReceivesPublish(t *testing.T) {
	r := newPolicyRig(t, `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`, nil)
	tap, err := r.b.OpenTap()
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	if err := tap.Subscribe("floor/temp", 1); err != nil {
		t.Fatal(err)
	}
	if err := r.b.PublishConsole(context.Background(), ConsolePublish{
		Topic: "floor/temp", Payload: []byte("21"), QoS: 2,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-tap.Messages():
		if m.Topic != "floor/temp" || string(m.Payload) != "21" || m.QoS != 1 {
			t.Fatalf("消息 = %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("控制台订阅者没有收到消息")
	}
}

func TestTapSharedAndRetain(t *testing.T) {
	r := newPolicyRig(t, `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`, nil)
	if err := r.b.PublishConsole(context.Background(), ConsolePublish{
		Topic: "keep/me", Payload: []byte("old"), QoS: 0, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	tap, err := r.b.OpenTap()
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	if err := tap.Subscribe("$share/ops/keep/#", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-tap.Messages():
		if m.Topic != "keep/me" || string(m.Payload) != "old" || !m.Retain {
			t.Fatalf("保留消息 = %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("订阅时没有收到保留消息")
	}
}

func TestTapRejectsBadQoS(t *testing.T) {
	r := newPolicyRig(t, `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`, nil)
	if err := r.b.PublishConsole(context.Background(), ConsolePublish{
		Topic: "a", QoS: 3,
	}); err == nil {
		t.Fatal("QoS 3 应拒绝")
	}
}
