package broker

import (
	"context"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func TestConnectStageSidecarIsReleasedAfterResultEvent(t *testing.T) {
	bus := events.New(events.Options{RingSize: -1})
	ch, cancel := bus.Subscribe(events.Filter{Kinds: events.KindConn.Mask()}, 1)
	defer cancel()
	b := &Broker{opt: Options{Bus: bus, ReadBufferSize: defaultReadBuffer}}
	c := newConn(b, nil, ConnInfo{})
	c.aux.stages.handshakeMS = 0.5
	c.aux.stages.authMS = 1
	c.aux.stages.sessionLoadMS = 2
	c.aux.stages.ownerClaimMS = 3
	c.aux.stages.sessionWriteMS = 4
	c.aux.stages.sessionWrites = 1
	c.emitConn(events.ConnAccepted, reason.Success, "")
	if c.aux != nil {
		t.Fatal("CONNECT 结果发出后仍常驻分段耗时 sidecar")
	}
	select {
	case ev := <-ch:
		if ev.Conn.HandshakeMS != 0.5 || ev.Conn.AuthMS != 1 || ev.Conn.SessionLoadMS != 2 ||
			ev.Conn.OwnerClaimMS != 3 || ev.Conn.SessionWriteMS != 4 ||
			ev.Conn.SessionWrites != 1 {
			t.Fatalf("CONNECT 分段事件丢失: %+v", ev.Conn)
		}
	case <-time.After(time.Second):
		t.Fatal("未收到 CONNECT 结果事件")
	}
	// cleanup 会再次走释放路径，必须保持幂等，不能把同一个对象放回池两次。
	c.releaseConnectStages()
}

func TestUnusedCertificateContextReleasedAfterConnect(t *testing.T) {
	r := newRig(t, CertAuthenticator{}, nil)
	ps := r.addPolicy(policyFull)
	f := fp(179)
	r.addCert(f, "dev-cert-sidecar", ps, meta.CertActive)

	cl := r.dial(certInfo(f, "dev-cert-sidecar.example"))
	if ack := cl.connect("dev-cert-sidecar", true); ack.Code != reason.Success {
		t.Fatalf("连接失败: %s", ack.Code)
	}
	live := r.b.lookup("dev-cert-sidecar")
	if live == nil {
		t.Fatal("已接受连接未登记")
	}
	if live.princ.AuthzCtx.Cert != nil {
		t.Fatal("不引用证书变量的策略仍让 CertContext 随长连接常驻")
	}
}

// TestPersistentSessionRejectedWhenStoreFails 钉住 CLU-130：持久会话
// 落不了盘就不许回成功的 CONNACK。
//
// 这条用例的价值在于它钉的是**不作为**的后果。原先那行是
// `_ = c.b.opt.Sessions.Save(ctx, st)`，功能上完全看不出问题——单机的
// 本地写几乎不会失败，而集群下一次 leader 选举窗口里的失败会让设备
// 拿到一个成功的 CONNACK，然后以为自己的会话可以跨节点恢复。
func TestPersistentSessionRejectedWhenStoreFails(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	r.sessFault.armWriteFailure(true)
	c := r.dial(ConnInfo{})
	ack := c.connect("dev-1", false)
	if ack.Code != reason.ServerUnavailable {
		t.Fatalf("CONNACK 原因码 = %s，应为 ServerUnavailable", ack.Code)
	}
	c.expectClosed(time.Second)

	if n := r.bus.ConnCount(events.ConnStoreFailed, events.AuthNone, 5); n != 1 {
		t.Errorf("StoreFailed 计数 = %d，应为 1", n)
	}

	// 所有权要已经还掉：否则同一个 clientId 重试时会先撞上一次接管，
	// 而被拒的那次连接从来没有真正持有过会话。
	r.sessFault.armWriteFailure(false)
	r.clk.advance(2 * time.Second)
	c2 := r.dial(ConnInfo{})
	if ack := c2.connect("dev-1", false); ack.Code != reason.Success {
		t.Fatalf("恢复后重连的原因码 = %s", ack.Code)
	}
	if ack := r.sess.Len(); ack != 1 {
		t.Errorf("恢复后会话记录数 = %d，应为 1", ack)
	}
}

func TestPersistentSessionRejectedWhenLoadFails(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	r.sessFault.armReadFailure(true)

	c := r.dial(ConnInfo{})
	ack := c.connect("dev-read-fail", false)
	if ack.Code != reason.ServerUnavailable {
		t.Fatalf("CONNACK 原因码 = %s，应为 ServerUnavailable", ack.Code)
	}
	c.expectClosed(time.Second)
	if _, _, ok := r.owners.Lookup(context.Background(), "dev-read-fail"); ok {
		t.Fatal("会话读取失败后仍占有 ownership")
	}
	if n := r.bus.ConnCount(events.ConnStoreFailed, events.AuthNone, 5); n != 1 {
		t.Errorf("StoreFailed 计数 = %d，应为 1", n)
	}
}

func TestAuthoritativeStaleEpochRejectsPersistentConnect(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)
	r.b.opt.AuthoritativeEpoch = true
	r.sessFault.armStaleWrite(true)

	c := r.dial(ConnInfo{})
	ack := c.connect("dev-stale", false)
	if ack.Code != reason.ServerUnavailable {
		t.Fatalf("全局有序 epoch 被围栏拒绝后 CONNACK=%s，期望 ServerUnavailable", ack.Code)
	}
	c.expectClosed(time.Second)
	if n := r.bus.ConnCount(events.ConnStoreFailed, events.AuthNone, 5); n != 1 {
		t.Fatalf("StoreFailed 计数=%d，期望 1", n)
	}
}

// TestCleanSessionWritesNothing 钉住 CLU-129：非持久会话在整条
// CONNECT 路径上一次会话写入都不发。
//
// 判据取的是分段埋点里的写入次数（OBS-148）而不是"存储里有几条记录"：
// 后者在 clean start 顶掉旧记录那一支下仍然是 0，看不出中间提了几条
// 日志，而集群下每一条都是一轮共识。
func TestCleanSessionWritesNothing(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	c := r.dial(ConnInfo{})
	if ack := c.connect("dev-1", true); ack.Code != reason.Success {
		t.Fatalf("CONNACK 原因码 = %s", ack.Code)
	}
	c.subscribe("a/b", packet.QoS1)
	c.send(&packet.Disconnect{})
	c.expectClosed(time.Second)

	if n := r.bus.SessionWrites(); n != 0 {
		t.Errorf("清洁会话的会话写入次数 = %d，应为 0", n)
	}
	if n := r.sess.Len(); n != 0 {
		t.Errorf("清洁会话不应留下记录，当前 %d 条", n)
	}

	// 分段耗时必须有读数：四段都记，包括真的是 0 的那一段——
	// 否则"这一段没走到"与"这一段很快"在指标上无法区分。
	for st := events.ConnStage(0); st < events.NumConnStages; st++ {
		if snap := r.bus.ConnStage(st); snap.Count == 0 {
			t.Errorf("分段 %s 没有观测值", st)
		}
	}
}

// TestCleanSessionReplacingStoredRecordDeletesIt 钉住 CLU-129 的另一半：
// 切到清洁会话的设备，存储里那条旧的持久记录必须真的被删掉。
//
// 它曾经删不掉：删除围栏用的是相等而非小于（只有持有当前 epoch 的连接
// 才有权删），而 CONNECT 拿的是刚抢到的新 epoch，于是每次都返回
// ErrStaleEpoch，错误又被丢弃。后果是存储里留下一条持有订阅的记录，
// 而那些订阅不在路由表里——确定性模拟报的"路由泄漏"。
func TestCleanSessionReplacingStoredRecordDeletesIt(t *testing.T) {
	r := newPolicyRig(t, policyFull, nil)

	first := r.dial(ConnInfo{})
	first.connect("dev-1", false)
	first.subscribe("a/b", packet.QoS1)
	first.send(&packet.Disconnect{})
	first.expectClosed(time.Second)
	if n := r.sess.Len(); n != 1 {
		t.Fatalf("持久会话应留下 1 条记录，当前 %d 条", n)
	}
	r.clk.advance(2 * time.Second)

	second := r.dial(ConnInfo{})
	second.connect("dev-1", true)
	if n := r.sess.Len(); n != 0 {
		t.Fatalf("清洁会话顶掉旧记录后应为 0 条，当前 %d 条", n)
	}
	// 两次写入：第一次 CONNECT 的持久会话 Save，加这一次删除。
	//
	// 断开时 persist 里那次 Save 不在这个计数里——这个计数器的分母是
	// 被接受的连接数，所以它只统计 CONNECT 路径（见 metrics.go 的说明）。
	// 全量提案数在集群侧的 session_proposals_total 里。
	if n := r.bus.SessionWrites(); n != 2 {
		t.Errorf("会话写入次数 = %d，应为 2（一次 Save 加一次删除）", n)
	}
}
