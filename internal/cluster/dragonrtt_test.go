package cluster

import (
	"testing"
	"time"
)

// TestTicksForKeepsTimeoutsConstant 钉住 CLU-132：RTT 变了之后，心跳与
// 选举的**时长**不变。
//
// 这条用例拦的是一个很容易犯的改动：把 RTT 调小而刻度数留在 10 与 1。
// 那样选举超时会从 1 秒缩到几十毫秒，后果不是「更快」，而是节点一忙
// 就开始换 leader——而换届要重放未提交的日志，于是越忙越慢。症状在
// 日志里是「频繁的 leader 变更」，不会有人把它联系到一个 tick 参数上。
func TestTicksForKeepsTimeoutsConstant(t *testing.T) {
	for _, rtt := range []time.Duration{
		time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond,
		defaultRTT, 250 * time.Millisecond, time.Second,
	} {
		ht, et := raftTicks(rtt)
		// RTT 能整除目标时长时，时长必须一个毫秒都不差。整除不了时
		// （RTT 比目标时长还大）一个 tick 就是能表达的最细粒度，那时
		// 只要求不短于目标时长——短了才是危险的那一侧。
		if hb := time.Duration(ht) * rtt; rtt <= defaultHeartbeatInterval &&
			hb != defaultHeartbeatInterval {
			t.Errorf("RTT=%s 时心跳间隔 = %s，应为 %s", rtt, hb, defaultHeartbeatInterval)
		} else if hb < defaultHeartbeatInterval && rtt > defaultHeartbeatInterval {
			t.Errorf("RTT=%s 时心跳间隔 = %s，短于 %s", rtt, hb, defaultHeartbeatInterval)
		}
		if el := time.Duration(et) * rtt; el < defaultElectionTimeout {
			t.Errorf("RTT=%s 时选举超时 = %s，短于 %s", rtt, el, defaultElectionTimeout)
		}
		if rtt <= defaultHeartbeatInterval {
			if el := time.Duration(et) * rtt; el != defaultElectionTimeout {
				t.Errorf("RTT=%s 时选举超时 = %s，应为 %s", rtt, el, defaultElectionTimeout)
			}
		}
		// 选举超时至少是心跳的 10 倍，否则一次心跳晚到就触发选举。
		if et < ht*electionHeartbeatRatio {
			t.Errorf("RTT=%s 时选举刻度 %d 不足心跳刻度 %d 的 %d 倍",
				rtt, et, ht, electionHeartbeatRatio)
		}
	}
	// 默认 RTT 下必须恰好是历史上那两个值：既有部署的行为一个字节都不能变。
	if ht, et := raftTicks(defaultRTT); ht != 1 || et != 10 {
		t.Errorf("默认 RTT 下心跳/选举刻度 = %d/%d，应为 1/10", ht, et)
	}
	// RTT 大于目标时长时不得退化成 0 个刻度（0 在库那边表示"取默认"）。
	if n := ticksFor(50*time.Millisecond, time.Second); n != 1 {
		t.Errorf("RTT 大于目标时长时刻度 = %d，应为 1", n)
	}
}
