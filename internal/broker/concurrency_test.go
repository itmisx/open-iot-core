package broker

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
)

// TestConcurrentDeliveryDoesNotShareEncodeBuffer 钉住"外部协程不得碰连接自己的
// 缓冲与队列"这条纪律在**投递路径**上也成立。
//
// 投递跑在发布者的读协程上（route → deliverTo → deliver），而编码缓冲归
// 订阅者自己的读协程独占。此前投递直接借用了它，于是两个发布者同时投给
// 同一个订阅者时，两次 EncodeTo 写进同一块缓冲，拷出来的那份是两个报文的
// 混合——订阅者收到畸形报文然后断连。拓扑上这是 IoT 里最普通的情形
// （两个设备发同一个主题、后端订阅它），不需要任何罕见时序。
//
// 这条用例在 -race 下直接报竞态；不开 -race 时靠"主题与负载必须自洽"
// 抓污染——两者都要有，因为门禁的 race 档只跑一次，而污染本身是数据错误，
// 没有竞态检测器也该被抓住。
func TestConcurrentDeliveryDoesNotShareEncodeBuffer(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	sub := r.dial(ConnInfo{})
	sub.connect("sub", true)
	if rc := sub.subscribe("race/#", packet.QoS0); rc.IsError() {
		t.Fatalf("订阅失败: %s", rc)
	}

	// 两个发布者的主题与负载刻意取不同长度且互不为前缀：缓冲被另一个
	// 报文盖过之后，要么解码失败，要么主题与负载不再自洽。
	const perPub = 20
	kinds := []struct{ topic, payload string }{
		{"race/alpha/" + strings.Repeat("a", 40), strings.Repeat("A", 60)},
		{"race/beta/" + strings.Repeat("b", 90), strings.Repeat("B", 20)},
	}
	want := map[string]string{
		kinds[0].topic: kinds[0].payload,
		kinds[1].topic: kinds[1].payload,
	}

	// 订阅者必须同时在读，否则出站队列会被填满而触发背压断连——
	// 那会把被测的投递路径提前关掉。
	done := make(chan int, 1)
	go func() {
		got := 0
		for got < perPub*len(kinds) {
			p, err := sub.recv()
			if err != nil {
				break // 读超时或连接关闭，按已收到的数量校验
			}
			pp, ok := p.(*packet.Publish)
			if !ok {
				continue
			}
			topic, payload := string(pp.Topic), string(pp.Payload)
			exp, known := want[topic]
			if !known {
				t.Errorf("收到未知主题 %q（编码缓冲被另一个报文污染）", topic)
				break
			}
			if payload != exp {
				t.Errorf("主题 %q 的负载 = %q，期望 %q（报文被混合）",
					topic, payload, exp)
				break
			}
			got++
		}
		done <- got
	}()

	var wg sync.WaitGroup
	for _, k := range kinds {
		wg.Add(1)
		go func(topic, payload string) {
			defer wg.Done()
			c := r.dial(ConnInfo{})
			c.connect("pub-"+topic[5:9], true)
			for n := 0; n < perPub; n++ {
				if err := c.trySend(&packet.Publish{
					Topic:   []byte(topic),
					Payload: []byte(payload),
					QoS:     packet.QoS0,
				}); err != nil {
					return
				}
			}
		}(k.topic, k.payload)
	}
	wg.Wait()

	got := <-done
	// QoS 0 允许在背压下丢弃，所以不要求全收到；但一条都没收到说明
	// 这条用例根本没压到投递路径上，等于没测。
	if got == 0 {
		t.Error("一条消息都没收到，投递路径未被覆盖")
	}
}

// TestDeliveryToClosingConnectionDoesNotPanic 钉住"客户端在别人正给它投消息时
// 断开"不会把进程带走。
//
// 出站队列的入队此前是「取锁读 detached、放锁、再入队」。那道缝里订阅者
// 自己的协程会把队列通道关掉，于是发布者协程的那次发送是**向已关闭的通道
// 发送**——panic。全仓库没有 recover，所以一次完全正常的断开就能让 broker
// 整个退出。写出池的关机也曾是同一个形状（close 就绪队列）。
//
// 用例的判据是"事后 broker 还能用"：panic 会直接带走测试进程，而
// 不 panic 也要证明连接与路由没被这一串并发拆解弄坏。
func TestDeliveryToClosingConnectionDoesNotPanic(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	for round := 0; round < 12; round++ {
		sub := r.dial(ConnInfo{})
		sub.connect("victim", true)
		sub.subscribe("boom/#", packet.QoS0)

		pub := r.dial(ConnInfo{})
		pub.connect("sender", true)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for n := 0; n < 40; n++ {
				if err := pub.trySend(&packet.Publish{
					Topic:   []byte("boom/x"),
					Payload: []byte("payload"),
					QoS:     packet.QoS0,
				}); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			// 投递正在进行时掐断：这正是那道缝的触发条件。
			_ = sub.nc.Close()
		}()
		wg.Wait()

		_ = pub.nc.Close()
		r.waitConns(0)
		// 每轮换一个时刻，避开每 clientId 的连接速率配额。
		r.clk.advance(2 * time.Second)
	}

	// 事后 broker 必须完好：连得上、订阅得了、收发得到。
	a := r.dial(ConnInfo{})
	a.connect("after", true)
	a.subscribe("after/#", packet.QoS1)
	b := r.dial(ConnInfo{})
	b.connect("after-pub", true)
	if rc := b.publish("after/ok", "alive", packet.QoS1, false); rc.IsError() {
		t.Fatalf("事后发布失败: %s", rc)
	}
	if got := a.expectPublish(); string(got.Payload) != "alive" {
		t.Errorf("事后投递负载 = %q", got.Payload)
	}
}

// TestShutdownDuringDeliveryDoesNotPanic 钉住关机与投递重叠时不 panic。
//
// 写出池曾经用 close(就绪队列) 来停工，而入队发生在任意协程上——连接的
// 读协程、投递路径、乃至工作协程 flush 的尾部（刷完发现又有积压就再入队
// 一次，那一次离 Close 最近）。于是"关机时刚好有报文入队"就是一次向已
// 关闭通道的发送，panic 掉整个进程。关机路径上的 panic 格外难查：
// 它只在有流量的时候出现，而重启脚本会把现场一起带走。
//
// 要压到这条缝，关机必须与投递**真正**重叠，所以：订阅者一直在读（否则
// 队列填满会触发背压断连，投递就停了），发布者一直在发，而 Close 等到
// 订阅者确实收到了消息——管道在流动——才调用。固定 sleep 换不来这个保证。
// 多跑几轮是因为这条缝很窄。
func TestShutdownDuringDeliveryDoesNotPanic(t *testing.T) {
	for round := 0; round < 6; round++ {
		t.Run("第"+string(rune('1'+round))+"轮", func(t *testing.T) {
			r := newPolicyRig(t, policyFull, nil)

			sub := r.dial(ConnInfo{})
			sub.connect("sub", true)
			sub.subscribe("down/#", packet.QoS0)

			pub := r.dial(ConnInfo{})
			pub.connect("pub", true)

			flowing := make(chan struct{})
			var once sync.Once
			var wg sync.WaitGroup
			wg.Add(2)
			// 订阅者持续读：队列不被填满，投递才会一直进行。
			go func() {
				defer wg.Done()
				for {
					p, err := sub.recv()
					if err != nil {
						return
					}
					if _, ok := p.(*packet.Publish); ok {
						once.Do(func() { close(flowing) })
					}
				}
			}()
			// 发布者一直发到发不动（关机会把连接断掉）。
			//
			// **投递开始之前的写失败必须报出来。** 原先这里一律 return，于是
			// 流量停了而下面的等待只会走到超时——失败现场写着「等待投递开始
			// 超时」，看上去像投递路径卡住了，而真因可能是第一次写就失败。
			// 两者的处置完全不同，所以把这一段的错误单独回传。
			pubErr := make(chan error, 1)
			go func() {
				defer wg.Done()
				for {
					if err := pub.trySend(&packet.Publish{
						Topic:   []byte("down/x"),
						Payload: []byte("payload-payload-payload"),
						QoS:     packet.QoS0,
					}); err != nil {
						select {
						case <-flowing:
							// 关机之后写不动是这条用例要的结果，不是缺陷。
						default:
							select {
							case pubErr <- err:
							default:
							}
						}
						return
					}
				}
			}()

			select {
			case <-flowing:
			case err := <-pubErr:
				t.Fatalf("投递还没开始，发布侧就失败了: %v", err)
			case <-time.After(testPatience): // contract:allow-time-now 测试超时
				t.Fatalf("等待投递开始超时（耐心值 %s，可用 OPENIOT_TEST_PATIENCE 调整）", testPatience)
			}
			// 流量正在跑的时候关机。Close 幂等，装置的 cleanup 再关一次无妨。
			if err := r.b.Close(); err != nil {
				t.Fatalf("关闭 broker 失败: %v", err)
			}
			_ = sub.nc.Close()
			_ = pub.nc.Close()
			wg.Wait()
		})
	}
}
