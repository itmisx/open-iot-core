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
}

// MaxProtoVer 是协议版本维度的上限。
//
// MQTT 3.1.1 是 4，5.0 是 5。留到 5 即可；未来版本须同时扩这里，
// 否则会落在 0 桶里——TestProtoVerDimensionCoversAllVersions 会拦住。
const MaxProtoVer = 5

func newCounters() *counters {
	return &counters{handshake: metrics.NewHistogram(metrics.LatencyBuckets()...)}
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
