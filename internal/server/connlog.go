package server

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// 被拒操作的日志。
//
// broker 的输出面只有事件总线（契约 11），它不持有 logger——这是对的：
// 协议层不该知道日志的存在。但"设备做不了某件事而服务端日志一行都没有"
// 对自建部署是很糟的体验：运维不会先去订阅事件流，他会先看日志。
//
// 因此由 server 订阅事件，把被拒的那些写进日志。日志放在它该在的
// 地方，而 broker 的单一输出面保持不变。
//
// 覆盖两类拒绝，它们是设备接入问题的全部来源：
//   - 连接被拒（证书未登记/已吊销、策略不允许该 clientId、限流）
//   - 授权被拒（发布、订阅、接收的主题不被策略放行）
//
// 只记拒绝。成功的操作量级太大，它属于指标而不是日志。

// rejectLogRate 是每秒最多写入的被拒日志条数。
//
// 需要上限：一个配置错误的设备会以秒级频率重连或重试发布，而每次都
// 写一行会让日志被一个设备刷满，真正的问题反而被埋掉。被压制的条数
// 与原因分布会汇总上报，所以信息不丢。
//
// 授权拒绝的量级可以比连接拒绝高出几个数量级（一个循环发布的设备），
// 因此两类共用同一个预算，而不是各自一份。
const rejectLogRate = 10

// rejectLogSummary 是汇总上报的间隔。
const rejectLogSummary = 30 * time.Second

// watchRejections 订阅事件并记录被拒的操作。
func (s *Server) watchRejections(ctx context.Context) {
	defer s.wg.Done()

	ch, cancel := s.bus.Subscribe(events.Filter{
		Kinds: events.KindConn.Mask() | events.KindAuthz.Mask() |
			events.KindSubscription.Mask(),
		// 只要授权拒绝。放行的判定事件在热路径上按采样产出，
		// 量级与消息数同阶，不该进日志。
		OnlyDenied: true,
	}, 256)
	defer cancel()

	var (
		mu         sync.Mutex
		windowSec  int64
		inWindow   int
		suppressed int
		byReason   = make(map[string]int)
	)

	ticker := time.NewTicker(rejectLogSummary)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mu.Lock()
			n, reasons := suppressed, byReason
			suppressed = 0
			byReason = make(map[string]int)
			mu.Unlock()
			if n > 0 {
				s.log.Warn("被拒操作的日志有压制",
					"被压制条数", n, "各原因计数", reasons,
					"完整记录", "订阅 /api/admin/v1/events 或看控制台的事件流")
			}
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if !isRejection(&ev) {
				continue
			}

			mu.Lock()
			sec := ev.AtUnixMs / 1000
			if sec != windowSec {
				windowSec, inWindow = sec, 0
			}
			inWindow++
			over := inWindow > rejectLogRate
			if over {
				suppressed++
				byReason[rejectionReason(&ev)]++
			}
			mu.Unlock()
			if over {
				continue
			}
			s.logRejection(&ev)
		}
	}
}

// isRejection 判断一条事件是否为被拒的操作。
func isRejection(ev *events.Event) bool {
	switch ev.Kind {
	case events.KindConn:
		// 断开事件带存续时长，它不是拒绝
		return ev.Conn.Result != events.ConnAccepted && ev.Conn.DurationMS == 0
	case events.KindAuthz:
		return ev.Authz.Decision != events.Allow
	case events.KindSubscription:
		// 只要被拒的订阅，且排掉授权拒绝。
		//
		// 授权拒绝已经由 KindAuthz 记过，且那条带策略名、语句号与
		// 变量求值结果，信息远多于这里。两条都打会让同一件事在日志
		// 里出现两次，而第二条更没用。
		return !ev.Sub.Unsubscribe && ev.Sub.Granted.IsError() &&
			ev.Sub.Granted != reason.NotAuthorized
	}
	return false
}

// rejectionReason 取一条拒绝的原因，用于汇总分布。
func rejectionReason(ev *events.Event) string {
	switch ev.Kind {
	case events.KindAuthz:
		return "iot:" + ev.Authz.Action.String() + " " + ev.Authz.Decision.String()
	case events.KindSubscription:
		return "订阅 " + ev.Sub.Granted.String()
	}
	return ev.Conn.ReasonText
}

// logRejection 写一行被拒日志。
//
// 字段取齐排障要用的那几项：谁、做什么、为什么被拒。凑齐之后，
// 多数接入问题不必再去翻别的地方。
func (s *Server) logRejection(ev *events.Event) {
	if ev.Kind == events.KindAuthz {
		// 变量求值结果是关键线索：多数"策略看起来写对了却被拒"
		// 都是某个变量求出的值与预期不同（见 AUTH-035）。
		args := []any{
			"clientId", orDash(ev.ClientID),
			"thing", orDash(ev.Thing),
			// 用策略文档里的拼写（iot:Subscribe）而不是事件里的短名
			// （Subscribe）：运维可以直接拿这个串去 grep 策略文件，
			// 或贴进策略调试器。日志该说策略的词汇。
			"动作", "iot:" + ev.Authz.Action.String(),
			"资源", ev.Authz.Resource,
			"判定", ev.Authz.Decision.String(),
		}
		if ev.Authz.Decision == events.ExplicitDeny {
			args = append(args,
				"命中策略", ev.Authz.PolicyName,
				"命中语句", ev.Authz.StatementIndex)
		}
		if vars := ev.Authz.VarList(); len(vars) > 0 {
			kv := make(map[string]string, len(vars))
			for _, v := range vars {
				kv[v.Name] = v.Value
			}
			args = append(args, "策略变量", kv)
		}
		args = append(args, "如何排查",
			"控制台的策略调试器，或 POST /api/admin/v1/authz/simulate")
		s.log.Warn("授权被拒", args...)
		return
	}

	if ev.Kind == events.KindSubscription {
		s.log.Warn("订阅被拒",
			"clientId", orDash(ev.ClientID),
			"thing", orDash(ev.Thing),
			"过滤器", ev.Sub.Filter,
			"原因", ev.Sub.Granted.String(),
			"原因码", "0x"+strconv.FormatUint(uint64(ev.Sub.Granted), 16))
		return
	}

	s.log.Warn("连接被拒",
		"clientId", orDash(ev.ClientID),
		"thing", orDash(ev.Thing),
		"原因", ev.Conn.ReasonText,
		"原因码", ev.Conn.ReasonCode.String(),
		"来源", connRemote(ev),
		"证书指纹", orDash(ev.Conn.CertFP),
		"证书通用名", orDash(ev.Conn.CertCN),
		"认证方式", ev.Conn.AuthMethod.String())
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func connRemote(ev *events.Event) string {
	if !ev.Conn.Remote.IsValid() {
		return "-"
	}
	return ev.Conn.Remote.String()
}
