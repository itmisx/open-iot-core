package events

import (
	"sync/atomic"

	"github.com/itmisx/open-iot-core/internal/metrics"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

// 指标计数器。
//
// 为什么不能从事件流推导指标：流有两处失真，而指标不能失真。
//
//   - 采样（OBS-003）。授权放行在高吞吐下按分母抽取，从流里数出来的
//     速率会按采样倍数偏小。
//   - 丢弃（OBS-005）。订阅者跟不上时总线丢弃并计数，不阻塞发布者。
//     从流里数出来的值因此是个下界，而且下界的幅度随负载变化。
//
// 所以指标必须由 Emit 自己在**采样闸门之前**累加。这不违反 OBS-001：
// 埋点仍然只有 Emit 一处，只是指标消费者读总线累积的状态，而不读流。
//
// 维度全部是有界枚举，用定长数组而非 map：
//   - 没有分配，可以放在热路径上；
//   - 基数在编译期就封死，不可能因为某个字段混进 clientId 而爆炸。
//
// 唯独不按 clientId、thing、主题拆分。那些是无界维度，Prometheus 侧
// 一个高基数标签就足以拖垮采集端——需要按设备下钻时用事件流，不用指标。
type counters struct {
	// conn 按 结果 × 认证方式 × 协议版本 计数。
	conn [NumConnResults][NumAuthMethods][MaxProtoVer + 1]atomic.Uint64
	// connReject 与 disconnect 按原因码计数。
	//
	// 用 [256] 覆盖原因码全域：它是单字节，定长数组才 2 KiB，
	// 而这样就不存在"遇到未登记的原因码怎么办"。
	connReject [256]atomic.Uint64
	disconnect [256]atomic.Uint64

	// authz 按 动作 × 判定 计数。
	authz [NumActions][NumDecisions]atomic.Uint64

	// sub 按 订阅/退订 × 是否成功 计数。
	sub [2][2]atomic.Uint64

	// handshake 是 TLS 握手耗时分布（OBS-021）。
	handshake *metrics.Histogram

	// connStage 是 CONNECT 路径各段的耗时分布（OBS-148）。
	//
	// 按阶段定长数组而不是 map：阶段是编译期封死的枚举，而这些观测
	// 在建连热路径上。
	connStage [NumConnStages]*metrics.Histogram
	// connWriteFail 是累计的会话存储写入失败次数。
	connWriteFail atomic.Uint64
	// connWriteStale 是累计被 epoch 围栏判为陈旧的会话写入次数。
	connWriteStale atomic.Uint64
	// connWrites 是累计的会话存储写入次数。
	//
	// 分母用 connections_total{result="accepted"}，商即"一次建连提了
	// 几条复制日志"。不做成直方图：取值只有 0、1、2 三种，一个计数器
	// 配已有的连接计数就够，而直方图要十个桶。
	connWrites atomic.Uint64
}

// ConnStage 是 CONNECT 路径的分段。
type ConnStage uint8

const (
	// StageAuth 是认证与 iot:Connect 鉴权，只读本地热视图。
	StageAuth ConnStage = iota
	// StageSessionLoad 是从本地存储恢复会话，集群下也是本地读（契约 05）。
	StageSessionLoad
	// StageOwnerClaim 是抢占 clientId 所有权。
	StageOwnerClaim
	// StageSessionWrite 是会话落盘；集群下每次都是一轮共识。
	StageSessionWrite
	// NumConnStages 是分段数，用于维度展开。
	NumConnStages
)

var connStageName = [NumConnStages]string{"auth", "session_load", "owner_claim", "session_write"}

// String 返回分段名，用作指标标签。
func (s ConnStage) String() string {
	if s >= NumConnStages {
		return "unknown"
	}
	return connStageName[s]
}

// MaxProtoVer 是协议版本维度的上限。
//
// MQTT 3.1.1 是 4，5.0 是 5。留到 5 即可；未来版本须同时扩这里，
// 否则会落在 0 桶里——TestProtoVerDimensionCoversAllVersions 会拦住。
const MaxProtoVer = 5

func newCounters() *counters {
	c := &counters{handshake: metrics.NewHistogram(metrics.LatencyBuckets()...)}
	for i := range c.connStage {
		c.connStage[i] = metrics.NewHistogram(metrics.LatencyBuckets()...)
	}
	return c
}

// record 累加一条事件的指标。
//
// 在 Emit 的采样闸门之前调用，因此它看到的是全量事件。
func (c *counters) record(e *Event) {
	switch e.Kind {
	case KindConn:
		f := &e.Conn
		if f.Result < NumConnResults && f.AuthMethod < NumAuthMethods && f.ProtoVer <= MaxProtoVer {
			c.conn[f.Result][f.AuthMethod][f.ProtoVer].Add(1)
		}
		// 分段耗时取走完全部四段的那些连接：被接受的，以及因为会话
		// 写不进去而被拒的（CLU-130）。
		//
		// 为什么后者也要：它那一段恰恰是最慢的一次（通常是共识超时），
		// 把它排除掉会让 session_write 的分位数在复制层出问题时反而
		// 变好看。而半路返回的拒绝（认证失败、限流）一律不记——它的
		// "会话写入耗时"是 0 而不是"很快"，混进分布会把分位数拉向零。
		//
		// 零值也记：一个真的零（非持久会话不写会话）与"这一段没走到"
		// 在走完的路径上是同一件事，而把零排除掉会让 count 在不同
		// 阶段之间不可比——那恰恰是定位瓶颈时第一个要看的东西。
		if f.Result == ConnAccepted || (f.Result == ConnStoreFailed && f.SessionWrites > 0) {
			c.connStage[StageAuth].Observe(f.AuthMS)
			c.connStage[StageSessionLoad].Observe(f.SessionLoadMS)
			c.connStage[StageOwnerClaim].Observe(f.OwnerClaimMS)
			c.connStage[StageSessionWrite].Observe(f.SessionWriteMS)
			c.connWrites.Add(uint64(f.SessionWrites))
			if f.SessionWriteFailed {
				c.connWriteFail.Add(1)
			}
			if f.SessionWriteStale {
				c.connWriteStale.Add(1)
			}
		}
		switch {
		case f.Result == ConnAccepted:
			// 握手耗时只对成功的连接有意义：被拒的连接可能在握手
			// 完成前就断了，把它们混进分布会把分位数拉向零。
			if f.HandshakeMS > 0 {
				c.handshake.Observe(f.HandshakeMS)
			}
		case f.DurationMS > 0:
			// 带存续时长的是断开，不是拒绝。
			c.disconnect[uint8(f.ReasonCode)].Add(1)
		default:
			c.connReject[uint8(f.ReasonCode)].Add(1)
		}
	case KindAuthz:
		if e.Authz.Action < NumActions && e.Authz.Decision < NumDecisions {
			c.authz[e.Authz.Action][e.Authz.Decision].Add(1)
		}
	case KindSubscription:
		op, ok := 0, 0
		if e.Sub.Unsubscribe {
			op = 1
		}
		if e.Sub.Granted < 0x80 {
			ok = 1
		}
		c.sub[op][ok].Add(1)
	}
}

// ConnCount 返回某个 结果/认证方式/协议版本 组合的连接计数。
func (b *Bus) ConnCount(r ConnResult, m AuthMethod, protoVer uint8) uint64 {
	if r >= NumConnResults || m >= NumAuthMethods || protoVer > MaxProtoVer {
		return 0
	}
	return b.c.conn[r][m][protoVer].Load()
}

// ConnRejectCount 与 DisconnectCount 返回按原因码的计数。
func (b *Bus) ConnRejectCount(code reason.Code) uint64 {
	return b.c.connReject[uint8(code)].Load()
}

func (b *Bus) DisconnectCount(code reason.Code) uint64 {
	return b.c.disconnect[uint8(code)].Load()
}

// AuthzCount 返回某个 动作/判定 组合的判定计数。
func (b *Bus) AuthzCount(a Action, d Decision) uint64 {
	if a >= NumActions || d >= NumDecisions {
		return 0
	}
	return b.c.authz[a][d].Load()
}

// SubCount 返回订阅或退订的计数。unsub 为真时取退订。
func (b *Bus) SubCount(unsub, granted bool) uint64 {
	op, ok := 0, 0
	if unsub {
		op = 1
	}
	if granted {
		ok = 1
	}
	return b.c.sub[op][ok].Load()
}

// Handshake 返回握手耗时分布。
func (b *Bus) Handshake() metrics.Snapshot { return b.c.handshake.Snapshot() }

// ConnStage 返回某一段 CONNECT 耗时的分布（OBS-148）。
func (b *Bus) ConnStage(s ConnStage) metrics.Snapshot {
	if s >= NumConnStages {
		return metrics.Snapshot{}
	}
	return b.c.connStage[s].Snapshot()
}

// SessionWrites 返回累计的会话存储写入次数（OBS-148）。
func (b *Bus) SessionWrites() uint64 { return b.c.connWrites.Load() }

// SessionWriteFailures 返回累计的会话存储写入失败次数（OBS-148）。
//
// 它只统计被接受的连接——被拒的那些在 connections_total{result="StoreFailed"}
// 里，两处相加才是复制层写入失败的全量。
func (b *Bus) SessionWriteFailures() uint64 { return b.c.connWriteFail.Load() }

// SessionWriteStale 返回被 epoch 围栏判为陈旧的会话写入次数（CLU-131）。
//
// 集群下它非零是正常的，而且正是要暴露的那件事：设备换节点重连时新节点
// 发出的 epoch 可能比旧节点小。单节点下它应当恒为 0——那里的序号严格
// 递增，非零意味着围栏或所有权表出了问题。
func (b *Bus) SessionWriteStale() uint64 { return b.c.connWriteStale.Load() }

// ReasonCodesSeen 返回出现过的原因码，供采集端只导出非零的那些。
//
// 不导出全部 256 个零值：那会让每次抓取多出五百行恒为零的样本，
// 而原因码的全域里实际只会用到十几个。
func (b *Bus) ReasonCodesSeen() (rejects, disconnects []reason.Code) {
	for i := 0; i < 256; i++ {
		if b.c.connReject[i].Load() > 0 {
			rejects = append(rejects, reason.Code(i))
		}
		if b.c.disconnect[i].Load() > 0 {
			disconnects = append(disconnects, reason.Code(i))
		}
	}
	return rejects, disconnects
}
