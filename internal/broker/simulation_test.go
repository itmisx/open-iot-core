package broker

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/packet"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// 确定性模拟测试（PRD TST-003、TST-017、TST-030 至 TST-034）。
//
// 做的是什么：用种子驱动一串随机操作去打真实的 broker——真实的会话存储、
// 路由表、配额、元数据视图，只有时钟和定时器是注入的。每一步之后断言一组
// 核心不变量（TST-050 至 TST-058）。失败时打印种子与完整操作日志，
// 同一个种子重跑就是同一串操作。
//
// 不做的是什么，要说清楚免得高估：**协程调度不是确定的**。broker 每条
// 连接一个协程、还有写出池，要让调度可重放得换掉整个运行时，那不在
// 现实范围内。所以 TST-030 这里只落实了"时钟与定时器可注入"，网络、磁盘
// 与调度仍是真实的；TST-031 的"可重放"指的是**操作序列**可重放，
// 而不是逐条指令的交错可重放。故障注入（TST-032）标的是 v0.2，这里没有。
//
// 即便如此它仍然值得写：真正难找的 bug 多数不是"某一步做错了"，
// 而是"几十步之后某个计数器对不上了"——而那正是这里每一步都在查的东西。

var (
	simOps = flag.Int("sim.ops", 300,
		"每个种子执行的操作数")
	simSeeds = flag.String("sim.seeds", "",
		"逗号分隔的种子列表，覆盖默认固定种子集（用于重放失败场景）")
	simRandom = flag.Int("sim.random", 0,
		"追加 N 个随机种子。夜间运行用，失败种子会打印在日志里")
)

// 固定种子集每次 CI 都跑（TST-034）。挑了几个互不相同的小数，
// 没有魔力——重点是它们固定，使 CI 的结果可比较。
var defaultSimSeeds = []int64{1, 2, 3, 5, 8}

// policySim 刻意带一条显式 Deny。
//
// TST-056 要求"显式 Deny 的请求绝不放行"，而只有策略里真有一条 Deny
// 时这条不变量才检验得到。sim/forbidden/* 就是那条。
const policySim = `{"Statement":[
  {"Effect":"Allow","Action":"iot:Connect",
   "Resource":"arn:aws:iot:*:*:client/${iot:ClientId}"},
  {"Effect":"Allow","Action":["iot:Publish","iot:Receive","iot:RetainPublish"],
   "Resource":"arn:aws:iot:*:*:topic/sim/*"},
  {"Effect":"Allow","Action":"iot:Subscribe",
   "Resource":"arn:aws:iot:*:*:topicfilter/sim/*"},
  {"Effect":"Deny","Action":["iot:Publish","iot:Receive","iot:RetainPublish"],
   "Resource":"arn:aws:iot:*:*:topic/sim/forbidden/*"},
  {"Effect":"Deny","Action":"iot:Subscribe",
   "Resource":"arn:aws:iot:*:*:topicfilter/sim/forbidden/*"}
]}`

const (
	simDevices = 5
	simTopics  = 4
)

func TestSimulation(t *testing.T) {
	for _, seed := range simSeedSet(t) {
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			runSimulation(t, seed, *simOps)
		})
	}
}

func simSeedSet(t *testing.T) []int64 {
	t.Helper()
	var seeds []int64
	if *simSeeds != "" {
		for _, s := range strings.Split(*simSeeds, ",") {
			v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				t.Fatalf("种子 %q 解析失败: %v", s, err)
			}
			seeds = append(seeds, v)
		}
		return seeds
	}
	seeds = append(seeds, defaultSimSeeds...)
	for i := 0; i < *simRandom; i++ {
		// 随机种子取自固定基数加序号，而不是时间：没有 Date.now
		// 一样能生成互不相同的一批，且打印出来就能原样重放。
		seeds = append(seeds, int64(0x5EED0000)+int64(i)*7919)
	}
	return seeds
}

// sim 是一次模拟运行的全部状态。
type sim struct {
	t    *testing.T
	r    *rig
	rnd  *rand.Rand
	seed int64

	// live 是每个设备当前的连接。同一设备至多一条——再连就是接管，
	// 旧的那条必须被服务端关掉（TST-050）。
	live map[string]*testClient
	// certOf 与 revoked 跟踪证书状态，用于判断连接应当成功还是被拒。
	certOf  map[string]meta.CertFingerprint
	revoked map[string]bool
	policy  meta.PolicySetID

	lastRevision meta.Revision
	ops          []string
	// curStep 让日志一步一行：一步里可能记好几笔（推时钟、再操作），
	// 不带步号的话日志行号与"第 N 步失败"对不上，定位全靠猜。
	curStep int

	// authzEvents 收集授权拒绝事件。
	//
	// 模拟报"本该放行的操作被拒了"的时候，只说结论等于把人扔回起点；
	// 而判定轨迹（命中哪条语句、变量求出了什么）本来就在事件里，
	// 顺手打出来就是一份现成的尸检报告。
	authzEvents func() []events.Event
	subEvents   func() []events.Event
}

func runSimulation(t *testing.T, seed int64, nops int) {
	r := newRig(t, CertAuthenticator{}, nil)
	s := &sim{
		t:       t,
		r:       r,
		rnd:     rand.New(rand.NewSource(seed)), //nolint:gosec // 测试用确定性随机
		seed:    seed,
		live:    make(map[string]*testClient),
		certOf:  make(map[string]meta.CertFingerprint),
		revoked: make(map[string]bool),
	}
	getAuthz, stopAuthz := r.collect(events.Filter{
		Kinds: events.KindAuthz.Mask(), OnlyDenied: true,
	})
	defer stopAuthz()
	s.authzEvents = getAuthz
	getSubs, stopSubs := r.collect(events.Filter{Kinds: events.KindSubscription.Mask()})
	defer stopSubs()
	s.subEvents = getSubs

	s.policy = r.addPolicy(policySim)
	for i := 0; i < simDevices; i++ {
		name := s.device(i)
		f := fp(byte(i + 1))
		s.certOf[name] = f
		r.addCert(f, name, s.policy, meta.CertActive)
	}
	s.lastRevision = r.store.View().Revision()

	for i := 0; i < nops; i++ {
		s.curStep = i
		s.step()
		s.checkInvariants(i)
		if t.Failed() {
			s.dump()
			return
		}
	}
	s.finish()
	if t.Failed() {
		s.dump()
	}
}

func (s *sim) device(i int) string { return fmt.Sprintf("dev-%d", i) }
func (s *sim) randDevice() string  { return s.device(s.rnd.Intn(simDevices)) }
func (s *sim) topic() string       { return fmt.Sprintf("sim/t%d", s.rnd.Intn(simTopics)) }
func (s *sim) forbidden() string   { return fmt.Sprintf("sim/forbidden/t%d", s.rnd.Intn(simTopics)) }
func (s *sim) logf(f string, a ...any) {
	s.ops = append(s.ops, fmt.Sprintf("[步 %d] ", s.curStep)+fmt.Sprintf(f, a...))
}

// dump 打印种子与操作日志。
//
// 失败时没有这个就只剩一个"第 173 步不变量不成立"，而那等于没有信息。
func (s *sim) dump() {
	s.t.Logf("重放：go test ./internal/broker/ -run TestSimulation -sim.seeds=%d -sim.ops=%d",
		s.seed, *simOps)
	from := 0
	if len(s.ops) > 60 {
		from = len(s.ops) - 60
		s.t.Logf("（省略前 %d 条）", from)
	}
	for i := from; i < len(s.ops); i++ {
		s.t.Logf("  %s", s.ops[i])
	}
}

// step 执行一次随机操作。权重按"哪些组合更容易出问题"分配：
// 连接与断开占大头，因为状态泄漏几乎都出在那条路径上。
func (s *sim) step() {
	// 每一步都推进时钟。不推的话连接速率配额会把重连全挡掉，
	// 整个模拟退化成"连不上"。
	s.advanceClock()
	switch n := s.rnd.Intn(100); {
	case n < 22:
		s.opConnect()
	case n < 34:
		s.opDisconnectAbrupt()
	case n < 44:
		s.opDisconnectClean()
	case n < 60:
		s.opSubscribe()
	case n < 68:
		s.opUnsubscribe()
	case n < 84:
		s.opPublish()
	case n < 89:
		s.opForbidden()
	case n < 94:
		s.opRevokeOrActivate()
	default:
		s.opKick()
	}
}

// advanceClock 推进注入时钟，偶尔向后跳。
//
// 向后跳是 TST-045 要求的：时钟回拨不得影响正确性。回拨在真实部署里
// 比想象中常见——NTP 步进、虚拟机快照恢复都会产生。
func (s *sim) advanceClock() {
	switch {
	case s.rnd.Intn(50) == 0:
		s.r.advance(-2 * time.Second)
		s.logf("时钟回拨 2s")
	case s.rnd.Intn(20) == 0:
		s.r.advance(2 * time.Minute)
		s.logf("时钟前跳 2min")
	default:
		s.r.advance(time.Duration(300+s.rnd.Intn(2000)) * time.Millisecond)
		s.logf("（推进时钟）")
	}
}

func (s *sim) opConnect() {
	name := s.randDevice()
	clean := s.rnd.Intn(2) == 0
	prev := s.live[name]

	c := s.r.dial(certInfo(s.certOf[name], name))
	cp := &packet.Connect{
		Version: packet.V5, ClientID: []byte(name), CleanStart: clean,
	}
	if !clean {
		cp.Props.SessionExpiry = 3600
		cp.Props.Set(packet.PropSessionExpiry)
	}
	if err := c.trySend(cp); err != nil {
		s.logf("connect %s 写出失败 %v", name, err)
		return
	}
	p, err := c.recv()
	if err != nil {
		s.t.Errorf("connect %s 没有收到 CONNACK: %v", name, err)
		return
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		s.t.Errorf("connect %s 收到 %s，期望 CONNACK", name, p.Type())
		return
	}

	switch {
	case ack.Code == reason.Success:
		if s.revoked[name] {
			s.t.Errorf("被吊销的证书竟然连上了（TST-055）：%s", name)
			return
		}
		s.logf("connect %s clean=%v -> Success", name, clean)
		// 接管：旧连接必须被服务端关掉，绝不双活（TST-050、TST-043）。
		if prev != nil {
			if rc := s.closedWithin(prev, testPatience); rc == nil {
				s.t.Errorf("%s 被接管后旧连接仍然活着（TST-050）", name)
			}
		}
		s.live[name] = c
	case s.revoked[name] && ack.Code == reason.NotAuthorized:
		s.logf("connect %s -> 被拒（证书已吊销，符合预期）", name)
	case isLegalConnectRefusal(ack.Code):
		// 配额类拒绝是合法结果，不是缺陷：连接速率配额故意收得很紧。
		s.logf("connect %s -> %s（配额）", name, ack.Code)
	default:
		s.t.Errorf("connect %s 被拒，原因码 %s 不在允许之列", name, ack.Code)
	}
}

// isLegalConnectRefusal 列出模拟中可以接受的拒绝原因。
//
// 白名单而不是黑名单：新增一种拒绝原因时必须有人来这里想一想它是否
// 真的合法，而不是默默被放过。
func isLegalConnectRefusal(rc reason.Code) bool {
	switch rc {
	case reason.QuotaExceeded, reason.ConnectionRateExceeded, reason.ServerBusy:
		return true
	}
	return false
}

// closedWithin 等连接被服务端关闭，返回收到的原因码；超时返回 nil。
func (s *sim) closedWithin(c *testClient, d time.Duration) *reason.Code {
	deadline := time.Now().Add(d) // contract:allow-time-now 测试超时
	rc := reason.Code(0)
	for time.Now().Before(deadline) {
		p, err := c.recv()
		if err != nil {
			return &rc
		}
		if d, ok := p.(*packet.Disconnect); ok {
			rc = d.Code
		}
	}
	return nil
}

func (s *sim) opDisconnectAbrupt() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	_ = c.nc.Close()
	delete(s.live, name)
	s.logf("掐断 %s", name)
	s.waitConnsSettle()
}

func (s *sim) opDisconnectClean() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	_ = c.trySend(&packet.Disconnect{Code: reason.NormalDisconnection})
	delete(s.live, name)
	s.logf("正常断开 %s", name)
	s.waitConnsSettle()
}

// waitConnsSettle 等服务端把断开处理完。
//
// 不等的话，下一步的不变量断言会撞上一个"正在清理中"的中间态，
// 得到的是一个与被测行为无关的假阳性。
func (s *sim) waitConnsSettle() {
	deadline := time.Now().Add(testPatience) // contract:allow-time-now 测试超时
	for time.Now().Before(deadline) {
		if int64(len(s.live)) == s.r.b.Conns() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *sim) pickLive() (string, *testClient) {
	if len(s.live) == 0 {
		s.logf("（无存活连接，空转）")
		return "", nil
	}
	names := make([]string, 0, len(s.live))
	for n := range s.live {
		names = append(names, n)
	}
	// 排序后再取，使同一种子下的选择可重放：map 遍历顺序是随机的。
	sortSimNames(names)
	n := names[s.rnd.Intn(len(names))]
	return n, s.live[n]
}

// sortSimNames 排序设备名，使同一种子下的选择可重放。
//
// map 遍历顺序在 Go 里是随机的，不排序的话「同一个种子跑出同一串操作」
// 这个前提就不成立了，而那是整套模拟能用来重放的基础。
// 用插入排序是因为这里最多五个名字，引 sort 包不值当。
func sortSimNames(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}

func (s *sim) opSubscribe() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	filter := s.topic()
	rc, err := s.subscribe(c, filter, packet.QoS1)
	if err != nil {
		s.logf("subscribe %s %s 失败 %v", name, filter, err)
		return
	}
	if rc.IsError() {
		s.t.Errorf("策略允许的订阅被拒：%s %s -> %s", name, filter, rc)
		s.explainLastDenial(filter)
		return
	}
	s.logf("subscribe %s %s", name, filter)
}

func (s *sim) opUnsubscribe() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	filter := s.topic()
	pid := c.nextPID()
	if err := c.trySend(&packet.Unsubscribe{
		PacketID: pid, Filters: [][]byte{[]byte(filter)},
	}); err != nil {
		return
	}
	if _, err := s.await(c, packet.UNSUBACK, pid); err != nil {
		s.logf("unsubscribe %s %s 无应答 %v", name, filter, err)
		return
	}
	s.logf("unsubscribe %s %s", name, filter)
}

func (s *sim) opPublish() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	qos := packet.QoS(s.rnd.Intn(3))
	name2 := s.topic()
	rc, err := s.publish(c, name2, qos, s.rnd.Intn(8) == 0)
	if err != nil {
		s.logf("publish %s %s qos%d 失败 %v", name, name2, qos, err)
		return
	}
	if rc.IsError() && !isLegalPublishRefusal(rc) {
		s.t.Errorf("策略允许的发布被拒：%s %s -> %s", name, name2, rc)
		s.explainLastDenial(name2)
		return
	}
	s.logf("publish %s %s qos%d -> %s", name, name2, qos, rc)
}

func isLegalPublishRefusal(rc reason.Code) bool {
	return rc == reason.QuotaExceeded
}

// opForbidden 打显式 Deny 覆盖的主题。它必须永远被拒（TST-056）。
func (s *sim) opForbidden() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	target := s.forbidden()
	if s.rnd.Intn(2) == 0 {
		rc, err := s.subscribe(c, target, packet.QoS1)
		if err != nil {
			return
		}
		if !rc.IsError() {
			s.t.Errorf("显式 Deny 的订阅被放行（TST-056）：%s %s -> %s", name, target, rc)
			return
		}
		s.logf("subscribe(forbidden) %s %s -> 被拒", name, target)
		return
	}
	rc, err := s.publish(c, target, packet.QoS1, false)
	if err != nil {
		return
	}
	if !rc.IsError() {
		s.t.Errorf("显式 Deny 的发布被放行（TST-056）：%s %s -> %s", name, target, rc)
		return
	}
	s.logf("publish(forbidden) %s %s -> 被拒", name, target)
}

// opRevokeOrActivate 吊销或恢复一张证书。
//
// 吊销要立刻断开在用连接（PKI-030），恢复之后又应当能连回来。
// 把两件事放在同一个操作里，是为了让模拟能走出"吊销—恢复—重连"这种
// 单条测试不会去构造的序列。
func (s *sim) opRevokeOrActivate() {
	name := s.randDevice()
	f := s.certOf[name]
	if s.revoked[name] {
		s.r.addCert(f, name, s.policy, meta.CertActive)
		s.revoked[name] = false
		s.logf("恢复证书 %s", name)
		return
	}
	s.r.revoke(f, name, s.policy, "模拟吊销")
	s.revoked[name] = true
	s.logf("吊销证书 %s", name)
	if c := s.live[name]; c != nil {
		if rc := s.closedWithin(c, testPatience); rc == nil {
			s.t.Errorf("证书吊销后 %s 的连接仍然活着（PKI-030）", name)
		}
		delete(s.live, name)
	}
	s.waitConnsSettle()
}

func (s *sim) opKick() {
	name, c := s.pickLive()
	if c == nil {
		return
	}
	if !s.r.b.KickClient(name, reason.AdministrativeAction) {
		return
	}
	s.logf("踢出 %s", name)
	if rc := s.closedWithin(c, testPatience); rc == nil {
		s.t.Errorf("踢出之后 %s 的连接仍然活着", name)
	}
	delete(s.live, name)
	s.waitConnsSettle()
}

// --- 收发的非致命封装 ---

// await 读到指定类型且报文标识相符的应答，中途收到的投递照常处理掉。
//
// 两件事都是必需的：
//
// 订阅者随时可能收到 PUBLISH，而我们在等 SUBACK/PUBACK——不把投递消化掉
// 就会把它误当成"应答类型不对"。单条用例里收发是编排好的，这里不是。
//
// 报文标识必须核对。第一版只认类型，于是一次超时之后，迟到的那个应答会
// 留在套接字缓冲里，被下一次 await 当成自己的——比如上一步"越权订阅"的
// 拒绝码，被下一步合法订阅读走，模拟就报"策略允许的订阅被拒"。
// 那是假阳性，而且看起来非常像真的：报错指向授权，查下去却发现
// 根本没有产生过拒绝事件。
func (s *sim) await(c *testClient, want packet.Type, pid uint16) (packet.Packet, error) {
	for i := 0; i < 64; i++ {
		p, err := c.recv()
		if err != nil {
			return nil, err
		}
		if p.Type() == want && packetID(p) == pid {
			return p, nil
		}
		switch v := p.(type) {
		case *packet.Publish:
			// 投递过来的消息按 QoS 回确认，免得服务端在途表堆积。
			if v.QoS == packet.QoS1 {
				_ = c.trySend(&packet.Ack{Kind: packet.PUBACK, PacketID: v.PacketID})
			}
		case *packet.Ack:
			if v.Kind == packet.PUBREL {
				_ = c.trySend(&packet.Ack{Kind: packet.PUBCOMP, PacketID: v.PacketID})
			}
		case *packet.Disconnect:
			return nil, fmt.Errorf("服务端断开：%s", v.Code)
		}
	}
	return nil, fmt.Errorf("等 %s(id=%d) 等了 64 个报文仍未到", want, pid)
}

// packetID 取出应答类报文的报文标识。
func packetID(p packet.Packet) uint16 {
	switch v := p.(type) {
	case *packet.Suback:
		return v.PacketID
	case *packet.Unsuback:
		return v.PacketID
	case *packet.Ack:
		return v.PacketID
	}
	return 0
}

func (s *sim) subscribe(c *testClient, filter string, qos packet.QoS) (reason.Code, error) {
	pid := c.nextPID()
	if err := c.trySend(&packet.Subscribe{
		PacketID: pid,
		Filters:  []packet.Subscription{{Filter: []byte(filter), QoS: qos}},
	}); err != nil {
		return 0, err
	}
	p, err := s.await(c, packet.SUBACK, pid)
	if err != nil {
		return 0, err
	}
	ack := p.(*packet.Suback)
	if len(ack.Codes) != 1 {
		return 0, fmt.Errorf("SUBACK 原因码数 = %d", len(ack.Codes))
	}
	return ack.Codes[0], nil
}

func (s *sim) publish(c *testClient, name string, qos packet.QoS, retain bool) (reason.Code, error) {
	pp := &packet.Publish{
		Topic: []byte(name), Payload: []byte("x"), QoS: qos, Retain: retain,
	}
	if qos > packet.QoS0 {
		pp.PacketID = c.nextPID()
	}
	if err := c.trySend(pp); err != nil {
		return 0, err
	}
	switch qos {
	case packet.QoS1:
		p, err := s.await(c, packet.PUBACK, pp.PacketID)
		if err != nil {
			return 0, err
		}
		return p.(*packet.Ack).Code, nil
	case packet.QoS2:
		p, err := s.await(c, packet.PUBREC, pp.PacketID)
		if err != nil {
			return 0, err
		}
		rec := p.(*packet.Ack)
		if err := c.trySend(&packet.Ack{Kind: packet.PUBREL, PacketID: rec.PacketID}); err != nil {
			return rec.Code, err
		}
		if _, err := s.await(c, packet.PUBCOMP, rec.PacketID); err != nil {
			return rec.Code, err
		}
		return rec.Code, nil
	}
	return reason.Success, nil
}

// explainLastDenial 打印某个资源最近一次拒绝的判定轨迹。
func (s *sim) explainLastDenial(resource string) {
	if s.authzEvents == nil {
		return
	}
	evs := s.authzEvents()
	for i := len(evs) - 1; i >= 0; i-- {
		a := evs[i].Authz
		if a.Resource != resource {
			continue
		}
		vars := make([]string, 0, a.NVars)
		for j := 0; j < int(a.NVars); j++ {
			vars = append(vars, fmt.Sprintf("%s=%q",
				a.Vars[j].Name, a.Vars[j].Value))
		}
		s.t.Logf("  判定轨迹：action=%v decision=%v policy=%q 语句#%d 变量[%s]",
			a.Action, a.Decision, a.PolicyName, a.StatementIndex,
			strings.Join(vars, " "))
		return
	}
	s.t.Logf("  判定轨迹：没有找到 %s 的拒绝事件——说明拒绝不是授权判定做出的",
		resource)
	s.dumpSubEvents()
}

// dumpSubEvents 打印最近几条订阅事件。
//
// 被拒的订阅也会产出事件，里面带着**服务端实际看到的**过滤器字符串。
// 当拒绝不是授权判定做出的时候，第一个要排除的就是"两边看到的不是
// 同一个过滤器"。
func (s *sim) dumpSubEvents() {
	if s.subEvents == nil {
		return
	}
	evs := s.subEvents()
	from := 0
	if len(evs) > 5 {
		from = len(evs) - 5
	}
	for _, e := range evs[from:] {
		s.t.Logf("  订阅事件：filter=%q share=%q qos=%d 退订=%v granted=%v",
			e.Sub.Filter, e.Sub.Share, e.Sub.QoS, e.Sub.Unsubscribe, e.Sub.Granted)
	}
}

// --- 不变量 ---

// checkInvariants 每一步之后跑一遍。
//
// 选的都是"能在几十步之后才对不上"的量：连接索引、配额计数、视图版本。
// 它们在任何单条用例里都不会出错，出错的总是长序列。
func (s *sim) checkInvariants(step int) {
	b := s.r.b

	// TST-050：同一 clientId 至多一个活会话。client 分片内的 map
	// 保证键唯一性；真正要查的是索引与计数器没有分家。
	index := b.clientIndexSnapshot()
	indexed := len(index)
	for id, c := range index {
		if c.clientID() != id {
			s.t.Errorf("第 %d 步：索引键 %q 指向 clientID %q", step, id, c.clientID())
		}
	}

	// 三个计数之间是包含关系，不是相等关系——这一点第一版写错了，
	// 五个种子全红，而红的是断言不是实现：
	//   Conns()  数的是已接受的套接字，CONNECT 之前就加一；
	//   配额计数 在 handleConnect 的配额闸门处加一，鉴权之前；
	//   client 索引只收录走完 CONNECT 的连接。
	// 于是握手中与清理中的连接会让前两者短暂地大于后者，那是正常的。
	// 真正不该发生的是反过来：登记了却没被计数，说明有一条路径绕开了
	// 计数或释放，而那正是"跑一天之后连不上"的成因。
	conns := b.Conns()
	if conns < int64(indexed) {
		s.t.Errorf("第 %d 步：连接计数 %d 小于索引规模 %d（有连接未被计数）",
			step, conns, indexed)
	}
	cur := s.r.lim.Current(quota.Connections, "")
	if cur < int64(indexed) {
		s.t.Errorf("第 %d 步：连接配额计数 %d 小于索引规模 %d（有连接未占配额）",
			step, cur, indexed)
	}
	// 配额在套接字计数之内取得、在它之外释放，所以永远不该超过它。
	if cur > conns {
		s.t.Errorf("第 %d 步：连接配额计数 %d 超过套接字计数 %d（配额未释放）",
			step, cur, conns)
	}

	// TST-052：元数据视图的 revision 单调不减。
	if rev := s.r.store.View().Revision(); rev < s.lastRevision {
		s.t.Errorf("第 %d 步：视图 revision 回退 %d -> %d", step, s.lastRevision, rev)
	} else {
		s.lastRevision = rev
	}

	// 全局订阅配额必须镜像路由表里的订阅数。
	//
	// 放在每一步而不是只在收尾查：只在收尾查的话，拿到的是"几百步之后
	// 对不上了"，而分叉发生在哪一步无从知晓。放在每一步，操作日志的
	// 最后一行就是罪魁。
	s.checkSubscriptionAccounting(step)

	// TST-058：会话状态可序列化并往返相等。抽一个查，不必每个都查。
	s.checkSessionRoundTrip(step)
}

// checkSubscriptionAccounting 断言全局订阅配额与路由表一致。
//
// 需要一点容让：SUBACK 是在把订阅写进路由表**之前**发出的
// （客户端须先知道订阅被接受），所以刚收到 SUBACK 的那一刻
// 配额可能短暂地多出一条。给它一个收敛窗口。
//
// 窗口放到秒级而不是原先的 200 毫秒：收敛后循环立刻返回，所以窗口只在
// 不变量**真的**不成立时才花掉墙上时间，而那种情形一次运行只发生一次
// （随后模拟就停了）。窗口给小了反过来要付代价——几个模拟挤在一台机器上
// 时，连接的读协程在 send(SUBACK) 与写路由表之间被调度器搁置几百毫秒是
// 常事，于是报出一条"配额比路由表多一条"的假阳性。
//
// 代价不对称是选大的理由，但它也掩盖过一次真分叉：窗口到期后报出的那条
// 分叉曾被当成"又是时序"，直到在失败处再等 3 秒复查，才看出它根本不收敛
// （记在 docs/TODO.md 第 4 节）。所以**凡是窗口到期后仍不符的，都要当成
// 真分叉去查**，不要因为它只在深扫里偶现就归到时序上。
func (s *sim) checkSubscriptionAccounting(step int) {
	var cur int64
	var subs int
	deadline := time.Now().Add(testPatience) // contract:allow-time-now 测试超时
	for time.Now().Before(deadline) {
		cur = s.r.lim.Current(quota.Subscriptions, "")
		subs = s.r.rt.Stats().Subscriptions
		if cur == int64(subs) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	s.t.Errorf("第 %d 步：全局订阅配额 %d 与路由表订阅数 %d 不符", step, cur, subs)
	for i := 0; i < simDevices; i++ {
		name := s.device(i)
		st, ok, _ := s.r.sess.Load(s.t.Context(), name)
		nsub := 0
		expiry := uint32(0)
		if ok {
			nsub, expiry = len(st.Subscriptions), st.ExpiryInterval
		}
		s.t.Logf("  %s: 存储中会话=%v 订阅数=%d 过期间隔=%d 每连接配额=%d 在线=%v",
			name, ok, nsub, expiry,
			s.r.lim.Current(quota.SubsPerConn, name), s.live[name] != nil)
	}
}

func (s *sim) checkSessionRoundTrip(step int) {
	if len(s.live) == 0 {
		return
	}
	name, _ := s.pickLive()
	st, ok, err := s.r.sess.Load(s.t.Context(), name)
	if err != nil || !ok {
		return
	}
	clone := st.Clone()
	if clone.ClientID != st.ClientID || clone.Epoch != st.Epoch ||
		len(clone.Subscriptions) != len(st.Subscriptions) ||
		clone.ExpiryInterval != st.ExpiryInterval {
		s.t.Errorf("第 %d 步：会话 %s 往返不相等（TST-058）", step, name)
	}
}

// finish 收尾：断开全部连接，然后查泄漏。
//
// 这是整个模拟里最可能抓到东西的一步。前面几百步制造出各种中间状态，
// 全部退场之后账必须平——平不了就说明某条路径漏了释放，而那正是
// "跑一天之后 broker 连不上了"的成因。
//
// 注意"平"不等于"归零"：持久会话的订阅本来就该留在路由表里等重连，
// 额度也该继续占着。第一版把它写成归零，于是把正确行为报成了泄漏。
// 真正的不变量是**配额计数与路由表里的订阅数相等**，且两者都等于
// 存活会话实际持有的订阅数。
func (s *sim) finish() {
	for name, c := range s.live {
		_ = c.trySend(&packet.Disconnect{Code: reason.NormalDisconnection})
		_ = c.nc.Close()
		delete(s.live, name)
	}
	s.waitConnsSettle()

	if n := s.r.b.Conns(); n != 0 {
		s.t.Errorf("收尾后仍有 %d 条连接", n)
	}
	if cur := s.r.lim.Current(quota.Connections, ""); cur != 0 {
		s.t.Errorf("收尾后连接配额计数 = %d，应为 0（配额泄漏）", cur)
	}

	// 存活会话实际持有多少条订阅。
	persisted := 0
	for i := 0; i < simDevices; i++ {
		st, ok, err := s.r.sess.Load(s.t.Context(), s.device(i))
		if err != nil || !ok || st.ExpiryInterval == 0 {
			continue
		}
		persisted += len(st.Subscriptions)
	}

	if st := s.r.rt.Stats(); st.Subscriptions != persisted {
		s.t.Errorf("收尾后路由表有 %d 条订阅，存活会话持有 %d 条（路由泄漏）",
			st.Subscriptions, persisted)
	}
	if cur := s.r.lim.Current(quota.Subscriptions, ""); cur != int64(persisted) {
		s.t.Errorf("收尾后订阅配额计数 = %d，存活会话持有 %d 条（配额泄漏）",
			cur, persisted)
	}
}
