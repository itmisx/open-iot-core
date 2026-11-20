package broker

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/session"
)

// ConnInfo 携带传输层的事实，broker 自身看不到这些。
//
// TLS 监听器填充证书与握手信息；明文接入时相关字段为零值。
// 把它作为显式参数而不是从 net.Conn 反射，使 broker 可以在
// net.Pipe 之上被完整测试，不必依赖真实 TLS。
type ConnInfo struct {
	Remote netip.AddrPort

	HasCert     bool
	CertFP      meta.CertFingerprint
	CertSubject authz.DN
	CertIssuer  authz.DN
	CertSerial  string

	TLSVersion  uint16
	ALPN        string
	HandshakeMS float64

	// AccessKeyID 由 WebSocket 监听器在 SigV4 验签通过后写入。
	// 认证器可以据此绑定设备：监听器是这条字段的唯一来源。
	AccessKeyID string
	// MaxLifetime 非零时，从 Serve 开始计算连接时长，到期断开。
	MaxLifetime time.Duration
	// RedirectTo 是到期时 MQTT 5 DISCONNECT 携带的 Server Reference。
	// 为空则到期只关连接，不发 0x9C：没有可指向的服务器。
	RedirectTo string
}

// PrincipalKind 是认证主体的种类。
type PrincipalKind uint8

const (
	PrincipalNone PrincipalKind = iota
	PrincipalCert
	PrincipalSigV4
	PrincipalWASM
)

// Principal 是认证成功后解析出的主体。
type Principal struct {
	Kind   PrincipalKind
	CertFP meta.CertFingerprint
	HasFP  bool
	// Thing 是绑定的设备名，为空表示未绑定。
	Thing string
	// Policies 是该主体生效的已编译策略集。
	Policies []*authz.CompiledSet
	// AuthzCtx 是策略求值上下文，连接建立时填充一次，之后每条消息复用。
	AuthzCtx   authz.Context
	AuthMethod events.AuthMethod
}

// Authenticator 在 CONNECT 时认证连接。
//
// 它只读 MetaView，不得触网——认证在连接路径上，而 MetaView 的签名
// 本身就承诺了本地内存读取（契约 05）。
// 返回 reason.Success 表示通过，其余原因码表示拒绝。
type Authenticator interface {
	Authenticate(v *meta.View, info ConnInfo, clientID string,
		username, password []byte) (Principal, reason.Code)
}

// Options 是 Broker 的构造参数。
type Options struct {
	Node cluster.NodeID
	// Meta 提供元数据写入与热视图（契约 05）。
	Meta meta.Store
	// Router 提供路由与全局订阅状态（契约 02、10）。
	Router *router.Router
	// Sessions 是会话状态存储（契约 04）。
	Sessions session.Store
	// Owners 管理 clientId 所有权与围栏（契约 01）。
	Owners cluster.SessionOwnership
	// Limiter 判定配额（契约 09）。
	Limiter quota.Limiter
	// Bus 是事件总线（契约 11）。
	Bus *events.Bus
	// Auth 认证连接。
	Auth Authenticator
	// Reserved 处理 $aws/ 保留主题上的平台请求，可为 nil。
	//
	// 为 nil 时保留主题按普通主题处理——这正是没有 Shadow 的部署
	// 应有的行为：设备发到 $aws/things/x/shadow/update 的消息
	// 只是一条普通消息，不会得到响应。
	Reserved ReservedService

	Limits topic.Limits
	Env    authz.Env
	// Now 提供当前时间（Unix 毫秒）。核心包不直接调用 time.Now（契约 08）。
	Now func() int64

	// AfterFunc 在 d 之后执行 f，返回的 stop 取消尚未触发的执行。
	//
	// 可注入是必需的而不是讲究：唯一的用户是遗嘱延迟（MQTT-061），
	// 而测试用的是冻结时钟。用真实定时器的话，"延迟 10 秒发遗嘱"
	// 这类行为就只能靠 sleep 去验，既慢又不稳。
	// 留空时取 time.AfterFunc。
	AfterFunc func(d time.Duration, f func()) (stop func() bool)
	// MaxViewStalenessMs 是视图陈旧度上限。超过时对新连接 fail-closed，
	// 但不断开已有连接（PRD CLU-023 至 CLU-025）。
	MaxViewStalenessMs int64

	// WriterWorkers 是写出池的工作协程数。
	WriterWorkers int
	// OutboundBuffer 是每连接的出站队列深度，必须有界。
	OutboundBuffer int
	// MaxWriteBatch 是单次写出合并的字节上限。
	MaxWriteBatch int
	// ReadBufferSize 是每连接读缓冲的初始大小。
	ReadBufferSize int
	// KeepAliveGraceFactor 是保活超时的宽限倍数，MQTT 规范建议 1.5。
	KeepAliveGraceFactor float64
	// WriteTimeout 是单次刷写的上限。停止读取的客户端不能占住写出工作协程。
	WriteTimeout time.Duration

	// Mesh 把消息转发给远端节点（契约 07）。为 nil 时不转发——
	// 单机下 Route 返回的远端集合恒为空，这条分支不会进入。
	Mesh cluster.Transport
	// OnTakeover 在本节点接管一个 clientId 之后被调用。
	//
	// 集群下用来通知其他节点断开同名连接（CLU-081）：本地抢占已经在
	// register 里同步处理了，而「新连接落在另一个节点」这种情形只能
	// 靠通知——那个节点的索引里根本没有这条连接。
	//
	// 在连接路径上调用，所以实现必须是非阻塞的（广播要异步做）。
	OnTakeover func(clientID string)
}

// Broker 是一个 MQTT 服务端。
type Broker struct {
	opt    Options
	writer *writerPool

	mu sync.RWMutex
	// byClient 是 clientId 到连接的索引，用于抢占与运维踢出。
	byClient map[string]*conn
	// byCert 是证书指纹到连接的索引。
	//
	// 吊销后必须立即断开所有在用该证书的活连接——仅改状态不断连
	// 视为未完成吊销（PRD PKI-030）。没有这个索引就只能全表扫描。
	byCert map[meta.CertFingerprint]map[*conn]struct{}

	closed atomic.Bool

	// meshSeq 给转发出去的帧编号。(Origin, Seq) 是跨节点的顺序来源
	// （契约 08 禁止用墙上时钟排序）。
	meshSeq atomic.Uint64

	// willMu 保护 pendingWills。单独一把锁而不是复用 mu：遗嘱定时器的
	// 回调要在持锁时核对 epoch，而 mu 在连接登记的热路径上。
	willMu       sync.Mutex
	pendingWills map[string]pendingWill
	cancel       context.CancelFunc
	watchWG      sync.WaitGroup

	conns atomic.Int64
	// assigned 为空 clientId 的 MQTT 5 连接分配标识。
	assigned atomic.Uint64

	// tapMu 保护 taps。它与 mu 分开：投递发生在发布者的协程上，
	// 不能为了找一个控制台订阅者去抢连接表的锁。
	tapMu sync.RWMutex
	taps  map[string]*Tap

	// capMu 保护抓包会话。抓包不进路由表，因此也不去拿连接表的锁。
	capMu sync.RWMutex
	caps  map[string]*Capture
}

// 默认参数。
const (
	defaultOutboundBuffer = 64
	defaultMaxWriteBatch  = 64 * 1024
	defaultReadBuffer     = 4 * 1024
	defaultKeepAliveGrace = 1.5
	defaultWriteTimeout   = 5 * time.Second

	// minWriterWorkers 是写出池的协程数下限。
	//
	// 不取 1 或 2：一次刷写最长占住一个工作协程 WriteTimeout 那么久
	// （慢客户端或对端不读），池子太小时少数几个这样的客户端就能让全部
	// 投递停摆。下限给到 8，使这种拖累需要同时出现八个才开始显形。
	minWriterWorkers = 8
)

// defaultWriterWorkers 是写出池的默认协程数。
//
// 此前是固定的 4，且调用方从不设置，于是所有部署都跑 4 个——而每个工作
// 协程做的是 TLS 加密加一次写系统调用，这件事与机器有几个核直接相关
// （MQTT-119）。按 GOMAXPROCS 的两倍取：乘二是因为协程会阻塞在写上
// （netpoll 挂起，不占 CPU 但占住一个池位），纯按核数会让「有核闲着而
// 投递在排队」。
//
// 它不是常量，所以也不在 config 的默认值里重复一份：生效值由
// Stats().Writer.Workers 报出。
func defaultWriterWorkers() int {
	n := runtime.GOMAXPROCS(0) * 2
	if n < minWriterWorkers {
		return minWriterWorkers
	}
	return n
}

// ErrClosed 表示 broker 已关闭。
var ErrClosed = errors.New("broker: 已关闭")

// New 创建一个 broker 并启动后台监听。
func New(opt Options) (*Broker, error) {
	switch {
	case opt.Meta == nil:
		return nil, errors.New("broker: 缺少 Meta")
	case opt.Router == nil:
		return nil, errors.New("broker: 缺少 Router")
	case opt.Sessions == nil:
		return nil, errors.New("broker: 缺少 Sessions")
	case opt.Owners == nil:
		return nil, errors.New("broker: 缺少 Owners")
	case opt.Limiter == nil:
		return nil, errors.New("broker: 缺少 Limiter")
	case opt.Bus == nil:
		return nil, errors.New("broker: 缺少 Bus")
	case opt.Auth == nil:
		return nil, errors.New("broker: 缺少 Auth")
	case opt.Now == nil:
		return nil, errors.New("broker: 缺少 Now（契约 08 要求注入时钟）")
	}
	if opt.WriterWorkers <= 0 {
		opt.WriterWorkers = defaultWriterWorkers()
	}
	if opt.OutboundBuffer <= 0 {
		opt.OutboundBuffer = defaultOutboundBuffer
	}
	if opt.MaxWriteBatch <= 0 {
		opt.MaxWriteBatch = defaultMaxWriteBatch
	}
	if opt.ReadBufferSize <= 0 {
		opt.ReadBufferSize = defaultReadBuffer
	}
	if opt.KeepAliveGraceFactor <= 0 {
		opt.KeepAliveGraceFactor = defaultKeepAliveGrace
	}
	if opt.Limits.MaxNameLen == 0 {
		opt.Limits = topic.DefaultLimits()
	}
	if opt.Env == (authz.Env{}) {
		opt.Env = authz.DefaultEnv()
	}
	if opt.AfterFunc == nil {
		opt.AfterFunc = func(d time.Duration, f func()) func() bool {
			// contract:allow-time-now 定时器只用于"等多久"，
			// 不参与任何排序或版本比较（与 conn.setReadDeadline 同理）。
			t := time.AfterFunc(d, f)
			return t.Stop
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		opt:      opt,
		writer:   newWriterPool(opt.WriterWorkers, opt.WriteTimeout),
		byClient: make(map[string]*conn),
		byCert:   make(map[meta.CertFingerprint]map[*conn]struct{}),
		cancel:   cancel,

		pendingWills: make(map[string]pendingWill),
		taps:         make(map[string]*Tap),
		caps:         make(map[string]*Capture),
	}
	b.watchWG.Add(2)
	go b.watchMeta(ctx)
	go b.watchEvictions(ctx)
	return b, nil
}

// Now 返回注入的时钟，供管理面计算消息剩余有效期。
func (b *Broker) Now() int64 { return b.opt.Now() }

// Conns 返回当前连接数。
func (b *Broker) Conns() int64 { return b.conns.Load() }

// Close 关闭 broker：停止后台监听与写出池，并断开全部连接。
func (b *Broker) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.cancel()
	b.watchWG.Wait()

	b.mu.Lock()
	conns := make([]*conn, 0, len(b.byClient))
	for _, c := range b.byClient {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	for _, c := range conns {
		c.evict(reason.ServerShuttingDown, cluster.EvictAdministrative)
	}
	// 在断开连接之后：那一步会为带遗嘱的连接登记新的待发项。
	b.dropPendingWills()
	b.closeTaps()
	b.closeCaptures()
	b.writer.Close()
	return nil
}

// watchMeta 消费元数据变更并在必要时断开连接。
//
// 吊销的广播由复制日志完成：每个节点在自己的 apply 循环里更新视图并发出
// 变更，再由本节点断开受影响的连接。不需要额外的跨节点失效通知机制
// （PRD CLU-019）。
func (b *Broker) watchMeta(ctx context.Context) {
	defer b.watchWG.Done()
	ch := b.opt.Meta.Changes()
	for {
		select {
		case <-ctx.Done():
			return
		case ch, ok := <-ch:
			if !ok {
				return
			}
			if !ch.RequiresDisconnect() {
				continue
			}
			b.kickByCert(ch.Fingerprint, ch.Reason)
		}
	}
}

// watchEvictions 消费所有权驱逐通知。
//
// 同 clientId 的新连接抢占所有权后，旧连接须被断开（MQTT 要求，
// 且契约 01 的 epoch 围栏已保证旧连接无法再改状态，此处只是尽快释放资源）。
func (b *Broker) watchEvictions(ctx context.Context) {
	defer b.watchWG.Done()
	ch := b.opt.Owners.Evictions()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b.mu.RLock()
			c := b.byClient[ev.ClientID]
			b.mu.RUnlock()
			// 只断开持有被驱逐 epoch 的那个连接：若索引里已是新连接，
			// 说明抢占已完成，不能把新连接误断。
			if c != nil && c.epoch == ev.PrevEpoch {
				rc := reason.SessionTakenOver
				if ev.Reason == cluster.EvictAdministrative {
					rc = reason.AdministrativeAction
				}
				c.evict(rc, ev.Reason)
			}
		}
	}
}

// kickByCert 断开全部在用某证书的连接。
func (b *Broker) kickByCert(fp meta.CertFingerprint, why string) int {
	b.mu.RLock()
	set := b.byCert[fp]
	victims := make([]*conn, 0, len(set))
	for c := range set {
		victims = append(victims, c)
	}
	b.mu.RUnlock()

	for _, c := range victims {
		c.emitMetaKick(fp, why)
		c.evict(reason.NotAuthorized, cluster.EvictAdministrative)
	}
	return len(victims)
}

// LocalThings 返回本节点此刻承载的设备名。
//
// 供集群的归属通告使用（CLU-075）：设备域主题不进全局订阅表，它们的
// 路由靠这张表（ARCH-017 的性能杠杆）。
//
// 返回全量快照而不是增量：通告是幂等的，丢一次或乱序都不会让归属表
// 永久偏离。设备数很大时这份切片会很大——到那一步要换成摘要
// （ARCH-047 把编码留作可延后的实现细节）。
func (b *Broker) LocalThings() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.byClient))
	seen := make(map[string]bool, len(b.byClient))
	for _, c := range b.byClient {
		name := c.princ.Thing
		if name == "" {
			// 没绑设备的连接（控制台测试客户端、未绑定证书）
			// 不参与设备域路由。
			continue
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// HoldsThing 报告本节点是否承载该设备。
//
// 参数是 []byte：它来自主题分类的结果，转成 string 会在路由路径上
// 分配一次（LANG-007）。
func (b *Broker) HoldsThing(thing []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, c := range b.byClient {
		if c.princ.Thing == string(thing) {
			return true
		}
	}
	return false
}

// KickClient 按 clientId 断开连接，供运维与 Admin API 使用。
func (b *Broker) KickClient(clientID string, rc reason.Code) bool {
	b.mu.RLock()
	c := b.byClient[clientID]
	b.mu.RUnlock()
	if c == nil {
		return false
	}
	c.evict(rc, cluster.EvictAdministrative)
	return true
}

// register 把连接登记到索引中，并返回被抢占的旧连接（若有）。
//
// 接管发生在索引对投递可见之前。顺序反了的话，新连接刚记下的在途消息
// 会被旧连接的快照盖掉，或者旧连接在快照之后又收进一条再也没人归还的额度。
// 快照要等正在进行的投递放开运行时的锁，所以整段放在索引锁里。
func (b *Broker) register(c *conn) (prev *conn, adopted bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	prev = b.byClient[c.clientID]
	adopted = c.adoptLive(prev)
	b.byClient[c.clientID] = c
	if c.princ.HasFP {
		set := b.byCert[c.princ.CertFP]
		if set == nil {
			set = make(map[*conn]struct{})
			b.byCert[c.princ.CertFP] = set
		}
		set[c] = struct{}{}
	}
	return prev, adopted
}

// unregister 把连接从索引中移除。
//
// 只在索引中仍指向本连接时才删除：抢占发生后索引已指向新连接，
// 旧连接的清理不能把新连接从索引里抹掉。
func (b *Broker) unregister(c *conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.byClient[c.clientID]; ok && cur == c {
		delete(b.byClient, c.clientID)
	}
	if c.princ.HasFP {
		if set := b.byCert[c.princ.CertFP]; set != nil {
			delete(set, c)
			if len(set) == 0 {
				delete(b.byCert, c.princ.CertFP)
			}
		}
	}
}

// lookup 按 clientId 查连接。
func (b *Broker) lookup(clientID string) *conn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.byClient[clientID]
}

// Serve 在一个连接上提供服务，直到连接关闭或出错。
//
// 调用方负责 Accept 与 TLS 握手，并把传输层事实填入 info。
func (b *Broker) Serve(ctx context.Context, nc net.Conn, info ConnInfo) error {
	if b.closed.Load() {
		_ = nc.Close()
		return ErrClosed
	}
	c := newConn(b, nc, info)
	b.conns.Add(1)
	defer b.conns.Add(-1)
	return c.serve(ctx)
}

// Stats 是 broker 的运行指标。
type Stats struct {
	Conns  int64
	Writer writerStats
	Router router.Stats
}

// Stats 返回运行指标。
func (b *Broker) Stats() Stats {
	return Stats{
		Conns:  b.conns.Load(),
		Writer: b.writer.stats(),
		Router: b.opt.Router.Stats(),
	}
}

// policiesFor 解析主体生效的策略集。
//
// 策略可附加到证书或设备（PRD AUTH-024），两者都要取。
// meta 层把已编译的匹配器存为不透明值，由此处断言为 authz 类型——
// 这个间接层使策略编译器可以独立演进而不触动 meta 包。
func policiesFor(v *meta.View, fp meta.CertFingerprint, hasFP bool, thing string) []*authz.CompiledSet {
	var out []*authz.CompiledSet
	add := func(id meta.PolicySetID) {
		if id == meta.NoPolicySet {
			return
		}
		ps, ok := v.PolicySet(id)
		if !ok {
			return
		}
		cs, ok := ps.Compiled.(*authz.CompiledSet)
		if !ok || cs == nil {
			return
		}
		for _, e := range out {
			if e == cs {
				return // 证书与设备绑定了同一策略集时只取一次
			}
		}
		out = append(out, cs)
	}
	if hasFP {
		add(v.CertPolicySet(fp))
	}
	if thing != "" {
		add(v.ThingPolicySet(thing))
	}
	return out
}

// ConnSnapshot 是一条连接的运维视图（PRD API-050）。
//
// 它是值拷贝而非指向 conn 的指针：Admin API 的响应构造发生在
// HTTP 处理协程里，而连接随时可能被关闭。返回指针会让一次列举
// 变成一组对正在消失的对象的竞态读取。
type ConnSnapshot struct {
	ClientID          string `json:"clientId"`
	Thing             string `json:"thing,omitempty"`
	CertFP            string `json:"certFingerprint,omitempty"`
	ProtoVer          uint8  `json:"protoVer"`
	Remote            string `json:"remote"`
	ConnectedAtUnixMs int64  `json:"connectedAt"`
	KeepAliveMs       int64  `json:"keepAliveMs"`
	TLSVersion        uint16 `json:"tlsVersion,omitempty"`
	ALPN              string `json:"alpn,omitempty"`
	AuthMethod        string `json:"authMethod"`
	Epoch             uint64 `json:"epoch"`
}

// ConnQuery 是连接列举的筛选条件。
type ConnQuery struct {
	// ClientIDPrefix 按 clientId 前缀筛选。
	ClientIDPrefix string
	// Thing 按设备名精确筛选。
	Thing string
	// After 是上一页的末尾 clientId。
	After string
	// Limit 是每页条数。
	Limit int
}

// ListConns 分页列举在线连接。
//
// 筛选与分页在服务端完成（PRD REG-044 的同类约束）：十万连接下
// "拉全量到前端过滤"是一次几十 MB 的响应，而调用方只要一页。
func (b *Broker) ListConns(q ConnQuery) ([]ConnSnapshot, string) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	if q.Limit > 500 {
		q.Limit = 500
	}

	// 在锁内只收集 clientId，不构造快照：构造要读每个连接的多个字段，
	// 在锁内做会把一次列举的耗时加到所有新连接的建立路径上。
	b.mu.RLock()
	ids := make([]string, 0, len(b.byClient))
	for id := range b.byClient {
		if q.Thing == "" && !hasPrefix(id, q.ClientIDPrefix) {
			continue
		}
		if id <= q.After {
			continue
		}
		ids = append(ids, id)
	}
	b.mu.RUnlock()

	sortStrings(ids)

	out := make([]ConnSnapshot, 0, q.Limit)
	next := ""
	for _, id := range ids {
		b.mu.RLock()
		c := b.byClient[id]
		b.mu.RUnlock()
		if c == nil {
			continue // 列举期间断开了，跳过
		}
		snap := c.snapshot()
		if q.Thing != "" && snap.Thing != q.Thing {
			continue
		}
		if len(out) == q.Limit {
			next = out[len(out)-1].ClientID
			break
		}
		out = append(out, snap)
	}
	return out, next
}

// GetConn 返回一条连接的详情。
func (b *Broker) GetConn(clientID string) (ConnSnapshot, bool) {
	b.mu.RLock()
	c := b.byClient[clientID]
	b.mu.RUnlock()
	if c == nil {
		return ConnSnapshot{}, false
	}
	return c.snapshot(), true
}

// SubSnap 是一条订阅的运维视图。不含会话里的其他状态。
type SubSnap struct {
	Filter string `json:"filter"`
	Share  string `json:"share,omitempty"`
	QoS    uint8  `json:"qos"`
}

// InflightSnap 是一条出站未确认消息的运维视图。
//
// 不带负载和属性。这条接口给运维看「卡在哪条主题上」，
// 负载是设备数据，放进管理面响应就会被日志和浏览器留住。
type InflightSnap struct {
	PacketID uint16 `json:"packetId"`
	QoS      uint8  `json:"qos"`
	Dup      bool   `json:"dup,omitempty"`
	State    string `json:"state"`
	Topic    string `json:"topic"`
}

// ConnDetail 是一条连接的详情，含订阅和在途。
type ConnDetail struct {
	ConnSnapshot
	Subscriptions []SubSnap      `json:"subscriptions"`
	Inflight      []InflightSnap `json:"inflight"`
}

// ConnDetail 返回一条在线连接的订阅和出站在途。
//
// 订阅和在途都从会话运行时的快照读。那份快照自己加锁，
// 管理面协程不去碰连接的读缓冲和编码缓冲。
func (b *Broker) ConnDetail(clientID string) (ConnDetail, bool) {
	b.mu.RLock()
	c := b.byClient[clientID]
	b.mu.RUnlock()
	if c == nil {
		return ConnDetail{}, false
	}
	d := ConnDetail{
		ConnSnapshot:  c.snapshot(),
		Subscriptions: []SubSnap{},
		Inflight:      []InflightSnap{},
	}
	if c.rt == nil {
		return d, true
	}
	for _, s := range c.rt.SubscriptionsSnapshot() {
		d.Subscriptions = append(d.Subscriptions, SubSnap{
			Filter: s.Filter, Share: s.Share, QoS: s.QoS,
		})
	}
	for _, m := range c.rt.InflightSnapshot() {
		d.Inflight = append(d.Inflight, InflightSnap{
			PacketID: m.PacketID, QoS: m.QoS, Dup: m.Dup,
			State: inflightState(m.State), Topic: m.Topic,
		})
	}
	return d, true
}

func inflightState(st session.InflightState) string {
	switch st {
	case session.AwaitPubrec:
		return "pubrec"
	case session.AwaitPubcomp:
		return "pubcomp"
	default:
		return "puback"
	}
}

// snapshot 构造连接的运维视图。
func (c *conn) snapshot() ConnSnapshot {
	s := ConnSnapshot{
		ClientID:          c.clientID,
		Thing:             c.princ.Thing,
		ProtoVer:          uint8(c.ver),
		Remote:            c.info.Remote.String(),
		ConnectedAtUnixMs: c.connectedAtMs,
		KeepAliveMs:       c.keepAliveMs,
		TLSVersion:        c.info.TLSVersion,
		ALPN:              c.info.ALPN,
		AuthMethod:        c.princ.AuthMethod.String(),
		Epoch:             uint64(c.epoch),
	}
	if c.princ.HasFP {
		s.CertFP = c.princ.CertFP.String()
	}
	return s
}

func hasPrefix(s, prefix string) bool {
	return prefix == "" || (len(s) >= len(prefix) && s[:len(prefix)] == prefix)
}

// sortStrings 就地升序排序。
//
// 不 import sort 是因为本文件在契约 02 的 import 约束下；
// 列举是运维路径，插入排序在一页的规模上足够。
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// PublishInternal 由平台自己发布一条消息。
//
// 用于把控制面产生的通知（Shadow 的 delta 与 documents、将来的
// Jobs 通知）投递给订阅者。它走正常路由，因此订阅匹配、
// iot:Receive 复查、QoS 与离线排队都自动适用。
//
// 不经发布方鉴权——发布者是平台自己。但每个订阅者的接收权限
// 仍会被复查（见 deliverTo）。
func (b *Broker) PublishInternal(topic string, payload []byte, qos1 bool) int {
	msg := &router.Message{Topic: topic, Payload: payload, StoredAtUnixMs: b.opt.Now()}
	if qos1 {
		msg.QoS = 1
	}
	return b.route(nil, msg, nil)
}

// ErrPayloadTooLarge 表示负载超过配额。
var ErrPayloadTooLarge = errors.New("broker: 负载超出上限")

// ErrRateLimited 表示保留消息的写入速率已用完。
var ErrRateLimited = errors.New("broker: 请求过多")

// PublishData 是数据面 Publish 的入口。
//
// retain 为真时先写入保留表再投递，与设备自己发布的顺序一致：
// 新订阅者应看到这次发布，而不是上一条。空负载加 retain 会清掉该主题。
func (b *Broker) PublishData(ctx context.Context, topicName string, payload []byte, qos uint8, retain bool) error {
	if qos > 1 {
		return errors.New("broker: qos 只接受 0 或 1")
	}
	if !b.opt.Limiter.Within(quota.PayloadSize, len(payload)) {
		return ErrPayloadTooLarge
	}
	if err := topic.ValidateName([]byte(topicName), b.opt.Limits); err != nil {
		return err
	}
	msg := &router.Message{
		Topic: topicName, Payload: payload, QoS: qos, Retain: retain,
		StoredAtUnixMs: b.opt.Now(),
	}
	if retain {
		if !b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRatePerTopic, topicName, 1) ||
			!b.opt.Limiter.AllowGlobal(ctx, quota.RetainedRate, "", 1) {
			return ErrRateLimited
		}
		b.opt.Router.Retained().Set(topicName, msg, b.opt.Router.NextVersion())
	}
	b.route(nil, msg, nil)
	return nil
}

// RetainedStore 返回保留消息表，供数据面读取。
func (b *Broker) RetainedStore() *router.RetainedStore {
	return b.opt.Router.Retained()
}
