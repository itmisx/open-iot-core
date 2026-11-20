package broker

import (
	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// 本文件是连接对事件总线的产出面。
//
// 契约 11 要求一个事件源服务四个消费者：$aws/events 保留主题、
// 控制台实时流、审计日志、指标聚合。绝不为各消费者分别埋点。
// 因此这里是连接层唯一的事件出口。

// emitConn 产出连接事件。
func (c *conn) emitConn(res events.ConnResult, rc reason.Code, text string) {
	e := events.Event{
		Kind:     events.KindConn,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Conn: events.ConnFields{
			Result:      res,
			ReasonCode:  rc,
			ReasonText:  text,
			Epoch:       c.epoch,
			AuthMethod:  c.princ.AuthMethod,
			ProtoVer:    byte(c.ver),
			Remote:      c.info.Remote,
			TLSVersion:  c.info.TLSVersion,
			ALPN:        c.info.ALPN,
			HandshakeMS: c.info.HandshakeMS,
		},
	}
	if c.princ.HasFP {
		e.Conn.CertFP = c.princ.CertFP.String()
		e.Conn.CertCN = c.princ.AuthzCtx.Cert.Subject.CommonName
	}
	c.b.opt.Bus.Emit(&e)
}

// emitDisconnect 产出断开事件，含连接存续时长。
func (c *conn) emitDisconnect(rc reason.Code, nowMs int64) {
	e := events.Event{
		Kind:     events.KindConn,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Conn: events.ConnFields{
			Result:     events.ConnClosed,
			ReasonCode: rc,
			ReasonText: rc.String(),
			Epoch:      c.epoch,
			AuthMethod: c.princ.AuthMethod,
			ProtoVer:   byte(c.ver),
			Remote:     c.info.Remote,
			DurationMS: nowMs - c.connectedAtMs,
		},
	}
	if c.princ.HasFP {
		e.Conn.CertFP = c.princ.CertFP.String()
	}
	c.b.opt.Bus.Emit(&e)
}

// emitMetaKick 产出因元数据变更而断连的事件。
//
// 吊销后立即踢连接走这条链路：控制台据此能直接告诉运维
// 「这次吊销断开了哪些连接」，而不必去翻日志（PRD PKI-033）。
func (c *conn) emitMetaKick(fp meta.CertFingerprint, why string) {
	e := events.Event{
		Kind:     events.KindMeta,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Meta: events.MetaFields{
			CertFP:             fp.String(),
			RequiresDisconnect: true,
			Thing:              c.princ.AuthzCtx.Thing,
			Reason:             why,
		},
	}
	c.b.opt.Bus.Emit(&e)
}

// emitSub 产出订阅与退订事件。
func (c *conn) emitSub(filter string, qos uint8, unsub bool, granted reason.Code) {
	share, inner := splitShared(filter)
	e := events.Event{
		Kind:     events.KindSubscription,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Sub: events.SubFields{
			Filter:      inner,
			Share:       share,
			QoS:         qos,
			Unsubscribe: unsub,
			Granted:     granted,
		},
	}
	c.b.opt.Bus.Emit(&e)
}

// emitAuthz 产出授权判定事件。
//
// 轨迹里的变量求值结果是控制台策略调试器能够存在的唯一前提——
// 这些中间结果如果不在判定时捕获就无法事后重建（PRD AUTH-035）。
func (c *conn) emitAuthz(act authz.Action, resource string, tr *authz.Trace) {
	e := events.Event{
		Kind:     events.KindAuthz,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Authz: events.AuthzFields{
			Action:         mapAction(act),
			Resource:       resource,
			Decision:       mapDecision(tr.Decision),
			PolicyName:     tr.PolicyName,
			StatementIndex: tr.StatementIndex,
		},
	}
	for _, v := range tr.VarList() {
		val := v.Value
		if !v.Resolved {
			// 求值失败的变量以显式标记呈现，而不是空串——
			// 控制台要能区分「求值为空」与「无法求值」。
			val = "<无法求值>"
		}
		e.Authz.SetVar(v.Name, val)
	}
	e.Authz.VarsTruncated = e.Authz.VarsTruncated || tr.VarsTruncated
	c.b.opt.Bus.Emit(&e)
}

// emitQuota 产出配额拒绝事件。
//
// 配额名与限制值从配额表解析，使告警里能直接看到「哪一项、上限多少」，
// 而不是一个光秃秃的「超限」（PRD OBS-029）。
func (c *conn) emitQuota(id quota.ID, subject string) {
	d, ok := quota.Lookup(id)
	if !ok {
		return
	}
	e := events.Event{
		Kind:     events.KindQuota,
		ClientID: c.clientID,
		Thing:    c.princ.AuthzCtx.Thing,
		Quota: events.QuotaFields{
			QuotaName: d.Name,
			Subject:   subject,
			// 取生效值而非默认值：配置覆盖后两者不同，
			// 而运维在告警里需要看到实际生效的那个。
			Limit: c.b.opt.Limiter.Limit(id),
		},
	}
	c.b.opt.Bus.Emit(&e)
}

// mapAction 把 authz 的动作映射到事件的动作。
//
// 两个包各自定义枚举，是为了让 authz 保持纯函数、不依赖事件总线；
// 这里的映射是为此付出的小代价。
func mapAction(a authz.Action) events.Action {
	switch a {
	case authz.ActConnect:
		return events.ActConnect
	case authz.ActPublish:
		return events.ActPublish
	case authz.ActSubscribe:
		return events.ActSubscribe
	case authz.ActReceive:
		return events.ActReceive
	case authz.ActRetainPublish:
		return events.ActRetainPublish
	default:
		return events.ActConnect
	}
}

func mapDecision(d authz.Decision) events.Decision {
	switch d {
	case authz.Allow:
		return events.Allow
	case authz.ExplicitDeny:
		return events.ExplicitDeny
	default:
		return events.ImplicitDeny
	}
}
