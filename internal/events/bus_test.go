package events

import (
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
)

func newTestBus() (*Bus, *int64) {
	now := int64(1_700_000_000_000)
	return New(Options{Node: "node-1", Now: func() int64 { return now }}), &now
}

func TestEmitFillsEnvelope(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{}, 4)
	defer cancel()

	b.Emit(&Event{Kind: KindConn, ClientID: "dev-1"})
	e := <-ch
	if e.Node != "node-1" {
		t.Errorf("Node = %q", e.Node)
	}
	if e.Seq != 1 {
		t.Errorf("Seq = %d，期望 1", e.Seq)
	}
	if e.AtUnixMs != 1_700_000_000_000 {
		t.Errorf("AtUnixMs = %d", e.AtUnixMs)
	}

	// 序号严格递增，是事件的顺序来源（契约 08）
	b.Emit(&Event{Kind: KindConn})
	if e2 := <-ch; e2.Seq != 2 {
		t.Errorf("第二条 Seq = %d，期望 2", e2.Seq)
	}
}

func TestEmitRejectsUnknownKind(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{}, 4)
	defer cancel()
	b.Emit(&Event{Kind: numKinds})
	select {
	case e := <-ch:
		t.Errorf("越界类型不应被发布: %+v", e)
	default:
	}
}

func TestEmitWithoutSubscribers(t *testing.T) {
	b, _ := newTestBus()
	for i := 0; i < 100; i++ {
		b.Emit(&Event{Kind: KindConn})
	}
	st := b.Stats()
	if st.Emitted[KindConn] != 100 {
		t.Errorf("已发布数 = %d，期望 100", st.Emitted[KindConn])
	}
	if st.Dropped != 0 {
		t.Errorf("无订阅者时不应计入丢弃，得到 %d", st.Dropped)
	}
}

func TestFilterByKind(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{Kinds: KindAuthz.Mask() | KindMeta.Mask()}, 8)
	defer cancel()

	b.Emit(&Event{Kind: KindConn})
	b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Decision: ExplicitDeny}})
	b.Emit(&Event{Kind: KindSubscription})
	b.Emit(&Event{Kind: KindMeta})

	var kinds []Kind
	for i := 0; i < 2; i++ {
		kinds = append(kinds, (<-ch).Kind)
	}
	if len(kinds) != 2 || kinds[0] != KindAuthz || kinds[1] != KindMeta {
		t.Errorf("收到 %v，期望 [Authz Meta]", kinds)
	}
	select {
	case e := <-ch:
		t.Errorf("不应再有事件: %+v", e)
	default:
	}
}

func TestFilterByClientAndThing(t *testing.T) {
	b, _ := newTestBus()
	byClient, c1 := b.Subscribe(Filter{ClientID: "dev-1"}, 8)
	defer c1()
	byThing, c2 := b.Subscribe(Filter{Thing: "thing-a"}, 8)
	defer c2()

	b.Emit(&Event{Kind: KindConn, ClientID: "dev-1", Thing: "thing-b"})
	b.Emit(&Event{Kind: KindConn, ClientID: "dev-2", Thing: "thing-a"})

	if e := <-byClient; e.ClientID != "dev-1" {
		t.Errorf("按 clientId 过滤得到 %q", e.ClientID)
	}
	select {
	case e := <-byClient:
		t.Errorf("按 clientId 过滤不应收到第二条: %+v", e)
	default:
	}
	if e := <-byThing; e.Thing != "thing-a" {
		t.Errorf("按 thing 过滤得到 %q", e.Thing)
	}
}

func TestFilterOnlyDenied(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{Kinds: KindAuthz.Mask(), OnlyDenied: true}, 8)
	defer cancel()

	b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Decision: Allow}})
	b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Decision: ExplicitDeny}})
	b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Decision: ImplicitDeny}})

	for i, want := range []Decision{ExplicitDeny, ImplicitDeny} {
		if e := <-ch; e.Authz.Decision != want {
			t.Errorf("第 %d 条判定 = %s，期望 %s", i, e.Authz.Decision, want)
		}
	}
	// OnlyDenied 不影响其他类型
	ch2, cancel2 := b.Subscribe(Filter{OnlyDenied: true}, 8)
	defer cancel2()
	b.Emit(&Event{Kind: KindConn})
	if e := <-ch2; e.Kind != KindConn {
		t.Errorf("OnlyDenied 不应过滤非授权事件，得到 %s", e.Kind)
	}
}

// TestEmitNeverBlocks 固定住一条设计决策：事件是观测数据，
// 不能让它反压业务路径。订阅者通道满时丢弃并计数（PRD OBS-005）。
func TestEmitNeverBlocks(t *testing.T) {
	b, _ := newTestBus()
	_, cancel := b.Subscribe(Filter{}, 2)
	defer cancel()

	for i := 0; i < 1000; i++ {
		b.Emit(&Event{Kind: KindConn})
	}
	st := b.Stats()
	if st.Emitted[KindConn] != 1000 {
		t.Errorf("已发布数 = %d，期望 1000（发布不应被阻塞）", st.Emitted[KindConn])
	}
	if st.Dropped == 0 {
		t.Error("通道满时应计入丢弃")
	}
	if st.Dropped != 998 {
		t.Errorf("丢弃数 = %d，期望 998（缓冲 2 条）", st.Dropped)
	}
}

func TestSampling(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{}, 64)
	defer cancel()

	b.SetSampling(KindAuthz, 10)
	// 放行事件按 1/10 抽取
	for i := 0; i < 30; i++ {
		b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Decision: Allow}})
	}
	n := 0
	for {
		select {
		case <-ch:
			n++
			continue
		default:
		}
		break
	}
	if n != 3 {
		t.Errorf("采样后收到 %d 条，期望 3", n)
	}
}

// TestSamplingNeverDropsDenials 固定住一条安全相关的决策：
// 拒绝、失败与须断连的变更是排障与审计的关键输入，
// 任何采样配置都不得丢弃它们。
func TestSamplingNeverDropsDenials(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{}, 256)
	defer cancel()

	for k := Kind(0); k < numKinds; k++ {
		b.SetSampling(k, 1000)
	}
	mustArrive := []Event{
		{Kind: KindAuthz, Authz: AuthzFields{Decision: ExplicitDeny}},
		{Kind: KindAuthz, Authz: AuthzFields{Decision: ImplicitDeny}},
		{Kind: KindConn, Conn: ConnFields{Result: ConnAuthFailed}},
		{Kind: KindConn, Conn: ConnFields{Result: ConnThrottled}},
		{Kind: KindConn, Conn: ConnFields{Result: ConnViewStale}},
		{Kind: KindMeta, Meta: MetaFields{RequiresDisconnect: true}},
		{Kind: KindQuota},
		{Kind: KindCluster},
	}
	for i := range mustArrive {
		b.Emit(&mustArrive[i])
	}
	got := 0
	for {
		select {
		case <-ch:
			got++
			continue
		default:
		}
		break
	}
	if got != len(mustArrive) {
		t.Errorf("即使采样分母为 1000，关键事件也应全部到达：收到 %d 条，期望 %d",
			got, len(mustArrive))
	}
}

func TestSetSamplingGuards(t *testing.T) {
	b, _ := newTestBus()
	b.SetSampling(numKinds, 5) // 越界，不应 panic
	b.SetSampling(KindConn, 0) // 0 视为全量
	ch, cancel := b.Subscribe(Filter{}, 8)
	defer cancel()
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{Result: ConnAccepted}})
	if _, ok := <-ch; !ok {
		t.Error("采样分母为 0 时应全量发布")
	}
}

func TestSubscribeCancelIsIdempotent(t *testing.T) {
	b, _ := newTestBus()
	ch, cancel := b.Subscribe(Filter{}, 4)
	if b.Stats().Subscribers != 1 {
		t.Fatalf("订阅者数 = %d", b.Stats().Subscribers)
	}
	for i := 0; i < 5; i++ {
		cancel()
	}
	if b.Stats().Subscribers != 0 {
		t.Errorf("取消后订阅者数 = %d", b.Stats().Subscribers)
	}
	if _, ok := <-ch; ok {
		t.Error("取消后通道应已关闭")
	}
	// 取消后发布不应 panic（不得向已关闭通道写入）
	b.Emit(&Event{Kind: KindConn})
}

func TestSubscribeDefaultBuffer(t *testing.T) {
	b, _ := newTestBus()
	_, cancel := b.Subscribe(Filter{}, 0)
	defer cancel()
	for i := 0; i < 256; i++ {
		b.Emit(&Event{Kind: KindConn})
	}
	if b.Stats().Dropped != 0 {
		t.Errorf("默认缓冲应为 256，丢弃数 = %d", b.Stats().Dropped)
	}
}

// TestAuthzVarsCapture 验证策略调试器的数据前提。
//
// 变量求值的中间结果如果不在判定时捕获就无法事后重建——
// 这个字段是控制台策略调试器能够存在的唯一前提（PRD AUTH-035）。
func TestAuthzVarsCapture(t *testing.T) {
	var a AuthzFields
	a.SetVar("iot:ClientId", "dev-001")
	a.SetVar("iot:Connection.Thing.ThingName", "dev-001")
	a.SetVar("iot:Certificate.Subject.CommonName", "dev-001.example")

	if a.NVars != 3 || a.VarsTruncated {
		t.Fatalf("NVars = %d truncated = %v", a.NVars, a.VarsTruncated)
	}
	list := a.VarList()
	if len(list) != 3 || list[0].Name != "iot:ClientId" || list[0].Value != "dev-001" {
		t.Errorf("变量列表 = %+v", list)
	}

	// 超出上限时截断并置标记，而不是静默丢弃
	for i := 0; i < maxVars; i++ {
		a.SetVar("extra", "x")
	}
	if a.NVars != maxVars {
		t.Errorf("NVars = %d，期望钳到 %d", a.NVars, maxVars)
	}
	if !a.VarsTruncated {
		t.Error("超出上限应置截断标记")
	}
}

func TestStringers(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{KindConn.String(), "Conn"},
		{Kind(99).String(), "Unknown"},
		{ConnAuthFailed.String(), "AuthFailed"},
		{ConnResult(99).String(), "Unknown"},
		{AuthMTLS.String(), "mTLS"},
		{AuthMethod(99).String(), "Unknown"},
		{ExplicitDeny.String(), "ExplicitDeny"},
		{Decision(99).String(), "Unknown"},
		{ActRetainPublish.String(), "RetainPublish"},
		{Action(99).String(), "Unknown"},
	} {
		if tc.got != tc.want {
			t.Errorf("得到 %q，期望 %q", tc.got, tc.want)
		}
	}
	if Kind(99).Mask() != 0 {
		t.Error("越界类型的掩码应为 0")
	}
	if AllKinds&KindCluster.Mask() == 0 {
		t.Error("AllKinds 应包含所有类型")
	}
}

func TestConcurrentEmitAndSubscribe(t *testing.T) {
	b, _ := newTestBus()
	var wg sync.WaitGroup

	// 发布者
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				b.Emit(&Event{
					Kind:     KindConn,
					ClientID: "dev-1",
					Conn:     ConnFields{Result: ConnAccepted, ReasonCode: reason.Success},
				})
			}
		}()
	}
	// 订阅者反复订阅与取消
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ch, cancel := b.Subscribe(Filter{}, 16)
				for k := 0; k < 4; k++ {
					select {
					case <-ch:
					default:
					}
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	if st := b.Stats(); st.Emitted[KindConn] != 4000 {
		t.Errorf("已发布数 = %d，期望 4000", st.Emitted[KindConn])
	}
}

// BenchmarkEmitNoSubscribers 验证无订阅者时发布是零分配的——
// 这是把总线放在连接与授权路径上的前提。
func BenchmarkEmitNoSubscribers(b *testing.B) {
	bus, _ := newTestBus()
	e := Event{Kind: KindConn, ClientID: "device-00123"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Emit(&e)
	}
}

func BenchmarkEmitOneSubscriber(b *testing.B) {
	bus, _ := newTestBus()
	ch, cancel := bus.Subscribe(Filter{}, 1024)
	defer cancel()
	// 持续排空，避免测量到丢弃路径
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()
	e := Event{Kind: KindConn, ClientID: "device-00123"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Emit(&e)
	}
	b.StopTimer()
	close(done)
}

func BenchmarkEmitFilteredOut(b *testing.B) {
	bus, _ := newTestBus()
	_, cancel := bus.Subscribe(Filter{Kinds: KindCluster.Mask()}, 16)
	defer cancel()
	e := Event{Kind: KindConn, ClientID: "device-00123"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Emit(&e)
	}
}

// 回填：订阅时应拿到之前发生的事件。
func TestRecentBackfill(t *testing.T) {
	b := New(Options{RingSize: 4})
	for i := 0; i < 3; i++ {
		b.Emit(&Event{Kind: KindConn, ClientID: "c"})
	}
	got := b.Recent(Filter{}, 0)
	if len(got) != 3 {
		t.Fatalf("回填条数 = %d，想要 3", len(got))
	}
	// 升序
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Errorf("回填未按时间升序: %v", []uint64{got[i-1].Seq, got[i].Seq})
		}
	}

	// 环形覆盖：写满之后只保留最近 4 条
	for i := 0; i < 10; i++ {
		b.Emit(&Event{Kind: KindConn, ClientID: "c"})
	}
	got = b.Recent(Filter{}, 0)
	if len(got) != 4 {
		t.Fatalf("写满后回填 = %d 条，想要 4（环形容量）", len(got))
	}
	if got[len(got)-1].Seq != b.Stats().Seq {
		t.Errorf("回填的最后一条应是最新事件，得到 Seq=%d，最新为 %d",
			got[len(got)-1].Seq, b.Stats().Seq)
	}

	// limit 超出时保留**最新**的：排障关心最近发生了什么
	got = b.Recent(Filter{}, 2)
	if len(got) != 2 || got[1].Seq != b.Stats().Seq {
		t.Errorf("limit 应保留最新的若干条，得到 %d 条，末条 Seq=%d",
			len(got), got[len(got)-1].Seq)
	}
}

// 回填必须遵守筛选条件。
func TestRecentRespectsFilter(t *testing.T) {
	b := New(Options{RingSize: 16})
	b.Emit(&Event{Kind: KindConn, ClientID: "a"})
	b.Emit(&Event{Kind: KindConn, ClientID: "b"})
	b.Emit(&Event{Kind: KindAuthz, ClientID: "a", Authz: AuthzFields{Decision: Allow}})
	b.Emit(&Event{Kind: KindAuthz, ClientID: "a", Authz: AuthzFields{Decision: ImplicitDeny}})

	if got := b.Recent(Filter{ClientID: "a"}, 0); len(got) != 3 {
		t.Errorf("按 clientId 筛选 = %d 条，想要 3", len(got))
	}
	if got := b.Recent(Filter{Kinds: KindConn.Mask()}, 0); len(got) != 2 {
		t.Errorf("按类型筛选 = %d 条，想要 2", len(got))
	}
	if got := b.Recent(Filter{Kinds: KindAuthz.Mask(), OnlyDenied: true}, 0); len(got) != 1 {
		t.Errorf("只要拒绝 = %d 条，想要 1", len(got))
	}
}

// 普通的 Subscribe 不得回填。
//
// 这一点至关重要：被拒连接的日志协程用的是 Subscribe，若它拿到历史事件，
// 每次重启都会把旧的拒绝重新打一遍日志。
func TestSubscribeDoesNotBackfill(t *testing.T) {
	b := New(Options{RingSize: 8})
	b.Emit(&Event{Kind: KindConn, ClientID: "old"})

	ch, cancel := b.Subscribe(Filter{}, 8)
	defer cancel()
	select {
	case e := <-ch:
		t.Fatalf("Subscribe 不应回填，却收到了历史事件 clientId=%s", e.ClientID)
	default:
	}
}

// SubscribeWithRecent 不得丢事件；允许重复，但必须能用 Seq 滤掉。
func TestSubscribeWithRecentLosesNothing(t *testing.T) {
	b := New(Options{RingSize: 64})
	for i := 0; i < 5; i++ {
		b.Emit(&Event{Kind: KindConn, ClientID: "c"})
	}
	ch, recent, cancel := b.SubscribeWithRecent(Filter{}, 64, 0)
	defer cancel()
	if len(recent) != 5 {
		t.Fatalf("回填 = %d 条，想要 5", len(recent))
	}
	var last uint64
	for _, e := range recent {
		if e.Seq > last {
			last = e.Seq
		}
	}
	b.Emit(&Event{Kind: KindConn, ClientID: "new"})

	// 按 SSE 端点的做法滤重，应恰好剩下新发的那一条
	var live []Event
	for {
		select {
		case e := <-ch:
			if e.Seq > last {
				live = append(live, e)
			}
			continue
		default:
		}
		break
	}
	if len(live) != 1 || live[0].ClientID != "new" {
		t.Errorf("滤重后应只剩新事件，得到 %d 条", len(live))
	}
}

// 不保留历史的总线也必须能用。
func TestRingDisabled(t *testing.T) {
	b := New(Options{RingSize: -1})
	b.Emit(&Event{Kind: KindConn})
	if got := b.Recent(Filter{}, 0); got != nil {
		t.Errorf("禁用环形后应无回填，得到 %d 条", len(got))
	}
}

// 采样不得影响指标。
//
// 这是本设计的核心不变量：事件流按分母抽取（OBS-003），而指标必须
// 精确。若指标在采样闸门之后累加，一个 1/100 的采样配置会让授权
// 速率显示成实际值的百分之一——而那个数字看起来完全正常。
func TestMetricsUnaffectedBySampling(t *testing.T) {
	b := New(Options{})
	b.SetSampling(KindAuthz, 100)

	const n = 500
	for i := 0; i < n; i++ {
		b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{
			Action: ActPublish, Decision: Allow,
		}})
	}
	if got := b.AuthzCount(ActPublish, Allow); got != n {
		t.Errorf("指标被采样影响了：计数 = %d，想要 %d", got, n)
	}
	// 而流确实被采样了，否则这个测试没有验证到东西
	if got := b.Stats().Emitted[KindAuthz]; got >= n {
		t.Errorf("流未被采样，本测试无效：发布数 = %d", got)
	}
}

// 订阅者丢弃不得影响指标。
func TestMetricsUnaffectedByDrops(t *testing.T) {
	b := New(Options{})
	// 缓冲深度 1，且不消费，使后续投递全部被丢弃
	_, cancel := b.Subscribe(Filter{}, 1)
	defer cancel()

	const n = 50
	for i := 0; i < n; i++ {
		b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
			Result: ConnAccepted, AuthMethod: AuthMTLS, ProtoVer: 5,
		}})
	}
	if b.Stats().Dropped == 0 {
		t.Fatal("没有发生丢弃，本测试无效")
	}
	if got := b.ConnCount(ConnAccepted, AuthMTLS, 5); got != n {
		t.Errorf("指标被丢弃影响了：计数 = %d，想要 %d", got, n)
	}
}

// 拒绝与断开必须分开计数。
//
// 二者都带原因码，区别只在有没有存续时长。混在一起会让"连接被拒"
// 和"正常断开"落进同一个计数器，而前者是故障、后者是常态。
func TestConnRejectAndDisconnectAreSeparate(t *testing.T) {
	b := New(Options{})
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
		Result: ConnAuthFailed, ReasonCode: reason.NotAuthorized,
	}})
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
		Result: ConnClosed, ReasonCode: reason.NotAuthorized, DurationMS: 1234,
	}})
	if got := b.ConnRejectCount(reason.NotAuthorized); got != 1 {
		t.Errorf("拒绝计数 = %d，想要 1", got)
	}
	if got := b.DisconnectCount(reason.NotAuthorized); got != 1 {
		t.Errorf("断开计数 = %d，想要 1", got)
	}
}

// 握手耗时只统计成功的连接。
//
// 被拒的连接可能在握手完成前就断了，混进分布会把分位数拉向零，
// 从而掩盖真实的握手变慢。
func TestHandshakeHistogramOnlyCountsAccepted(t *testing.T) {
	b := New(Options{})
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
		Result: ConnAccepted, HandshakeMS: 7,
	}})
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
		Result: ConnAuthFailed, HandshakeMS: 999,
	}})
	s := b.Handshake()
	if s.Count != 1 {
		t.Errorf("握手分布计数 = %d，想要 1", s.Count)
	}
	if s.Sum < 6.9 || s.Sum > 7.1 {
		t.Errorf("握手分布 Sum = %v，想要约 7", s.Sum)
	}
}

// 指标维度必须覆盖全部枚举值。
//
// 维度用定长数组，若上界与枚举脱钩，新增的枚举值会静默落进 0 号桶
// 或被整条丢掉——而那是个看起来正常的错误。
func TestMetricDimensionsCoverAllEnumValues(t *testing.T) {
	b := New(Options{})
	for r := ConnResult(0); r < NumConnResults; r++ {
		for m := AuthMethod(0); m < NumAuthMethods; m++ {
			b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
				Result: r, AuthMethod: m, ProtoVer: MaxProtoVer,
			}})
			if got := b.ConnCount(r, m, MaxProtoVer); got != 1 {
				t.Errorf("维度 result=%s auth=%s 未被计数", r, m)
			}
		}
	}
	for a := Action(0); a < NumActions; a++ {
		for d := Decision(0); d < NumDecisions; d++ {
			b.Emit(&Event{Kind: KindAuthz, Authz: AuthzFields{Action: a, Decision: d}})
			if got := b.AuthzCount(a, d); got != 1 {
				t.Errorf("维度 action=%s decision=%s 未被计数", a, d)
			}
		}
	}
}

// TestAuditEventsIgnoreSampling 验证写操作审计不受采样影响。
//
// 采样是给高频的放行判定用的。审计若被抽掉，磁盘上的记录就不再是
// 全部写操作，出了事对不上。
func TestAuditEventsIgnoreSampling(t *testing.T) {
	b := New(Options{})
	b.SetSampling(KindAudit, 100)
	ch, cancel := b.Subscribe(Filter{Kinds: KindAudit.Mask()}, 128)
	defer cancel()
	for i := 0; i < 100; i++ {
		b.Emit(&Event{Kind: KindAudit, Audit: AuditFields{Action: "PutThing", Target: "lamp"}})
	}
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			if n != 100 {
				t.Fatalf("审计事件 = %d，采样不得丢掉写操作", n)
			}
			return
		}
	}
}

// 上界常量必须与名字数组同长。
//
// 两者脱钩时 String() 与指标维度会各自用不同的基数，而症状是某个
// 枚举值在指标里消失但在日志里正常。
func TestEnumBoundsMatchNames(t *testing.T) {
	if int(NumConnResults) != len(connResultName) {
		t.Errorf("NumConnResults=%d 与名字数组长度 %d 不符", NumConnResults, len(connResultName))
	}
	if int(NumAuthMethods) != len(authMethodName) {
		t.Errorf("NumAuthMethods=%d 与名字数组长度 %d 不符", NumAuthMethods, len(authMethodName))
	}
	if int(NumActions) != len(actionName) {
		t.Errorf("NumActions=%d 与名字数组长度 %d 不符", NumActions, len(actionName))
	}
	if int(NumDecisions) != len(decisionName) {
		t.Errorf("NumDecisions=%d 与名字数组长度 %d 不符", NumDecisions, len(decisionName))
	}
}

// ReasonCodesSeen 只返回出现过的原因码。
func TestReasonCodesSeenSkipsZero(t *testing.T) {
	b := New(Options{})
	b.Emit(&Event{Kind: KindConn, Conn: ConnFields{
		Result: ConnAuthFailed, ReasonCode: reason.NotAuthorized,
	}})
	rejects, disconnects := b.ReasonCodesSeen()
	if len(rejects) != 1 || rejects[0] != reason.NotAuthorized {
		t.Errorf("拒绝原因码 = %v，想要 [NotAuthorized]", rejects)
	}
	if len(disconnects) != 0 {
		t.Errorf("无断开却返回了原因码: %v", disconnects)
	}
}
