package broker

import (
	"context"
	"testing"
	"time"
)

// TestCaptureSeesUnsubscribedPublish 验证抓包不依赖路由表里的订阅者。
//
// 去掉 fanoutCapture 之后，这条会话不在连接表里，也没有订阅，
// 发布会在投递时被丢掉，测试在一秒内收不到消息。
func TestCaptureSeesUnsubscribedPublish(t *testing.T) {
	r := newPolicyRig(t, `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`, nil)
	c, err := r.b.OpenCapture(1, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Watch("$share/ops/floor/#"); err == nil {
		t.Fatal("抓包不该接受共享订阅")
	}
	if err := c.Watch("floor/#"); err != nil {
		t.Fatal(err)
	}
	if err := r.b.PublishConsole(context.Background(), ConsolePublish{
		Topic: "floor/temp", Payload: []byte("21"), QoS: 0,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-c.Messages():
		if m.Topic != "floor/temp" || string(m.Payload) != "21" {
			t.Fatalf("抓到的消息 = %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("没有订阅者时抓包没有看到发布")
	}
}
