// Package server 把各个契约包装配成一个可运行的节点。
//
// 真实逻辑放在这里而不是 cmd 包，是为了它能被完整测试：
// cmd 包里的代码只能经 exec 整个二进制来测，那种测试又慢又难断言。
package server

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/events"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/topic"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/pki"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/sigv4"
	"github.com/itmisx/open-iot-core/internal/store"
	"github.com/itmisx/open-iot-core/internal/trace"
)

// 就绪判定的组件名。
const (
	compMeta      = "meta"
	compView      = "view"
	compPKI       = "pki"
	compStore     = "store"
	compRegistry  = "registry"
	compShadow    = "shadow"
	compListeners = "listeners"
	// compShards 是会话分区组（集群模式才有，CLU-118）。
	compShards = "shards"
)

// ShutdownTimeout 是优雅关闭的上限（PRD NFR-064）。
const ShutdownTimeout = 30 * time.Second

// Options 是节点装配参数。
type Options struct {
	Config   config.Config
	DataDir  *node.DataDir
	Manifest node.Manifest
	Logger   *slog.Logger
	// Now 提供当前时间（Unix 毫秒）。为 nil 时取墙上时钟。
	//
	// 可注入是契约 08 的要求，也让集成测试不必真的等待。
	Now func() int64
	// LookupEnv 用于取环境变量，为 nil 时不读进程环境。
	//
	// 注入而非直接读 os.LookupEnv，使测试能构造独立的环境，
	// 不受运行测试的那台机器上已有的变量影响。
	LookupEnv func(string) (string, bool)
}

// Server 是一个装配好的节点。
type Server struct {
	cfg   config.Config
	log   *slog.Logger
	dd    *node.DataDir
	mf    node.Manifest
	now   func() int64
	ready *node.Readiness

	// online 是总览页的在线连接采样（UI-012、API-123）。
	// 每次读总览记一个点，最多留 120 个。进程内环形缓冲，重启即空。
	onlineMu sync.Mutex
	online   []onlinePoint

	store metaStore
	// repl、replStore 与 mesh 只在集群模式下非 nil。
	repl      *cluster.TLSGroup
	replStore *meta.ReplicatedStore
	mesh      *cluster.MeshTransport
	// lateMesh 是交给 broker 的那个传输包装：broker 在装配期就要它，
	// 而真正的 mesh 要等复制组起来（见 lateMesh）。
	lateMesh *lateMesh
	// initialRev 是启动时从冷层读回的 applied index（P1）。
	// 非零表示冷层已经是权威，热视图要从它重建而不是靠重放日志（P2）。
	initialRev meta.Revision
	// gossipSeen 是通告过的节点。被移出成员之后要把它的通告内容忘掉
	// （CLU-101，见 forgetDepartedNodes）。
	gossipMu   sync.Mutex
	gossipSeen map[cluster.NodeID]bool
	// thingEpoch 区分同一 node ID 的不同进程生命周期；thingSyncRequests
	// 是对端发现增量断档后发来的快照请求。
	thingEpoch        uint64
	thingSyncMu       sync.Mutex
	thingSyncRequests map[cluster.NodeID]bool
	thingLegacyPeers  map[cluster.NodeID]bool
	// retaineds 是待广播的保留消息变更（MQTT-122）。有界：发布路径上
	// 只能入队不能等网络（契约 05），满了就丢并计数——边界见 MQTT-125。
	retaineds chan cluster.RetainedUpdate
	// retainedDrops 计数因队列满而丢弃的保留消息广播（MQTT-126）。
	retainedDrops atomic.Uint64
	// takeovers 是待广播的接管通知（CLU-081）。有界：连接路径上只能
	// 投递不能阻塞。
	takeovers chan string
	// shadowRepl 只在集群模式下非 nil（CLU-082）。
	shadowRepl *shadow.ReplicatedStore
	// 下面几项是复制组的装配材料。复制组本身要等注册表建好之后才能起
	// （装快照要调 registry.HydrateHot），所以材料先存着，见
	// startReplicationGroup。
	replLate  *lateGroup
	replTLS   *tls.Config
	replPeers map[cluster.NodeID]string
	replDir   string
	rt        *router.Router
	owners    *cluster.SwitchableOwnership
	// remoteThings 是通告来的设备归属表，只在集群模式下非 nil。
	remoteThings *cluster.RemoteThings
	lim          *quota.Local
	bus          *events.Bus
	tracer       *trace.Tracer
	ca           *pki.CA
	brk          *broker.Broker
	reg          *Registry
	kv           store.KV
	shadows      *store.ShadowStore
	// sessions 是会话存储。集群模式下它在装配后期被换成复制实现
	// （见 sessionSwitch），所以这里是壳而不是本地存储本身。
	sessions     *sessionSwitch
	sessionLocal *store.SessionStore
	// shards 是会话分区组，只在集群模式下非空。
	shards       *cluster.ShardGroup
	shadowShards atomic.Pointer[cluster.ShardGroup]
	shadowRoute  shadowRouteStats
	retained     *store.RetainedStore
	reg2         *registry.Registry
	shadowSvc    *shadow.Service
	access       *access

	listeners []*broker.Listener
	admin     *adminServer
	wssKeys   map[string]accessKey

	// bgCtx 与 bgStop 控制全部后台协程的生命周期。
	//
	// 它独立于 Run 收到的 ctx：shutdown 可能由 Close 直接触发
	// （嵌入使用与测试里就是如此），此时 Run 的 ctx 并未取消。
	// 若后台协程只响应 Run 的 ctx，它们会在 wg.Wait 处把关闭
	// 拖到超时——这个坑在本项目已经踩过三次（管理端点、监听器、
	// 被拒连接的日志）。
	//
	// 由 shutdown 统一取消，使"登记在 wg 上的协程一定会被叫停"
	// 成为结构性保证，而不是每次新增协程时要记得的纪律。
	bgCtx  context.Context
	bgStop context.CancelFunc

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New 装配一个节点，但不开始监听。
//
// 装配与监听分开，是 PRD CLU-032 的要求：节点在就绪之前不得监听设备
// 端口。半空的视图会导致误拒（证书还没载入）或更糟的误放
// （吊销状态还没同步到）。
func New(opt Options) (*Server, error) {
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.DiscardHandler)
	}
	if opt.DataDir == nil {
		return nil, errors.New("server: 缺少数据目录")
	}
	if opt.Now == nil {
		opt.Now = wallClockMs
	}
	if err := opt.Config.Validate(); err != nil {
		return nil, err
	}

	bgCtx, bgStop := context.WithCancel(context.Background())
	s := &Server{
		cfg: opt.Config, log: opt.Logger, dd: opt.DataDir,
		mf: opt.Manifest, now: opt.Now,
		bgCtx: bgCtx, bgStop: bgStop,
		ready: node.NewReadiness(compMeta, compStore, compRegistry, compView,
			compPKI, compShadow, compListeners, compShards),
		reg: NewRegistry(opt.DataDir.Root()),
	}
	s.access = newAccess(s.dd.Root(), s.now)

	limits := topic.DefaultLimits()
	env := authzEnv(&s.cfg)

	localOwners := cluster.NewLocalOwnership(cluster.NodeID(s.mf.NodeID), s.cfg.Cluster.ShardCount)
	s.owners = cluster.NewSwitchableOwnership(localOwners, s.cfg.Cluster.ShardCount)
	if s.cfg.Node.Mode != config.ModeStandalone {
		var epochBytes [8]byte
		if _, err := cryptorand.Read(epochBytes[:]); err != nil {
			return nil, fmt.Errorf("server: 生成设备归属 epoch 失败: %w", err)
		}
		s.thingEpoch = binary.LittleEndian.Uint64(epochBytes[:])
		if s.thingEpoch == 0 {
			s.thingEpoch = 1
		}
		s.thingSyncRequests = make(map[cluster.NodeID]bool)
		s.thingLegacyPeers = make(map[cluster.NodeID]bool)
		s.remoteThings = cluster.NewRemoteThings(cluster.NodeID(s.mf.NodeID))
		s.takeovers = make(chan string, 1024)
		s.retaineds = make(chan cluster.RetainedUpdate, 1024)
	}
	qov, qerr := quotaOverrides(s.cfg.Quota)
	if qerr != nil {
		return nil, qerr
	}
	s.lim = quota.NewLocal(quota.Options{Now: s.now, Overrides: qov})
	s.bus = events.New(events.Options{Node: cluster.NodeID(s.mf.NodeID), Now: s.now})
	ropt := router.Options{
		Self: cluster.NodeID(s.mf.NodeID), Limits: limits, Now: s.now,
	}
	if s.remoteThings != nil {
		// 设备域主题的归属：本节点有就判本地，否则查通告来的表。
		// brk 此刻还没建，所以 Local 是一个延后求值的闭包——
		// 它只在路由时被调用，那时 brk 一定在了。
		ropt.Locator = router.GossipLocator{
			Self:   cluster.NodeID(s.mf.NodeID),
			Local:  func(thing []byte) bool { return s.brk != nil && s.brk.HoldsThing(thing) },
			Remote: s.remoteThings,
		}
	}
	s.rt = router.New(ropt)

	if s.cfg.PKI.Enabled {
		ca, err := LoadCA(s.dd, &s.cfg, s.now)
		if err != nil {
			return nil, err
		}
		s.ca = ca
		s.ready.Set(compPKI, true, "")
	} else {
		s.ready.Set(compPKI, true, "未启用")
	}

	auth, err := s.buildAuthenticator()
	if err != nil {
		return nil, err
	}

	// 本地存储。影子、会话与保留消息都落在这里，重启后恢复。
	storeDir, err := s.dd.EnsureDir(node.DirMeta)
	if err != nil {
		return nil, err
	}
	// 冷层用可整库替换的包装：装一份状态机快照就是用另一个库文件
	// 替换当前这个，而注册表、会话、保留消息、影子各持一个句柄
	// （见 store.SwapKV 的说明）。
	kv, err := store.OpenSwappable(store.Options{Dir: storeDir})
	if err != nil {
		return nil, err
	}
	s.kv = kv
	s.ready.Set(compStore, true, "")

	// 元数据层要在冷层之后建：命令同时携带热视图的改动与冷层记录，
	// 两者在同一条已提交日志里应用（REG-110、CLU-065）。
	ms, err := s.buildMetaStore(kv, env)
	if err != nil {
		return nil, err
	}
	s.store = ms
	if s.replStore == nil {
		// 单机：一副本组，追加即提交，视图永不落后。
		s.ready.Set(compMeta, true, "")
		// 单机没有会话分区组，这一项恒为就绪。不置它会让 /readyz
		// 永久为假，而单机是默认形态。
		s.ready.Set(compShards, true, "单机无分区组")
	} else {
		// 集群：先追平一次再说就绪。还没追上就放设备进来，等于让它们
		// 用一份可能漏掉了吊销的视图通过鉴权（CLU-023）。
		s.ready.Set(compMeta, false, "等待追平已提交日志")
		s.ready.Set(compShards, false, "等待会话分区组选出 leader")
	}

	// 会话与保留消息都落在同一个本地存储里。
	s.sessionLocal = store.NewSessionStore(kv)
	s.sessions = newSessionSwitch(s.sessionLocal)
	s.retained = store.NewRetainedStore(kv)

	// 控制面：设备、证书与策略。它是冷层与热视图之间的唯一写入口。
	reg2, err := registry.New(registry.Options{
		KV: kv, Meta: s.store, Env: env, Now: s.now,
	})
	if err != nil {
		return nil, err
	}
	s.reg2 = reg2

	// Shadow 的分区键是 thingName（契约 06）。单机下 LocalPartitioner
	// 恒判为本地，但转发分支在 shadow.Service 中已经存在。
	s.tracer = trace.New(trace.Options{
		Now: s.now, Service: s.cfg.OTel.ServiceName,
		Endpoint: s.cfg.OTel.Endpoint, Enabled: s.cfg.OTel.Enabled,
		OnError: func(err error) { s.log.Error("追踪导出失败", "错误", err.Error()) },
	})
	s.shadows = store.NewShadowStore(kv)
	// 集群下影子命令走复制日志（CLU-082），各节点在 apply 阶段用同一份
	// 状态机算出同样的结果。单机直接用本地的。
	shadowStore, err := s.buildShadowStore(s.shadows)
	if err != nil {
		return nil, err
	}
	shadowSvc, err := shadow.New(shadow.Options{
		Store:       shadowStore,
		Partitioner: cluster.NewLocalPartitioner(cluster.NodeID(s.mf.NodeID), s.cfg.Cluster.ShardCount),
		Limiter:     s.lim,
		Now:         s.now,
		Forward:     s.applyForwardedShadow,
		LocalGet:    s.localShadowGet,
		LocalList:   s.localShadowList,
		ForwardGet:  s.forwardShadowGet,
		ForwardList: s.forwardShadowList,
		OnForward:   s.traceShadow,
		OnChange: func(thing, op string) {
			reason := "shadow-update"
			if op == "delete" {
				reason = "shadow-delete"
			}
			s.bus.Emit(&events.Event{
				Kind: events.KindMeta, Thing: thing,
				Meta: events.MetaFields{Thing: thing, Reason: reason},
			})
		},
	})
	if err != nil {
		return nil, err
	}
	s.shadowSvc = shadowSvc
	s.ready.Set(compShadow, true, "")

	// 装配后续步骤失败时须释放已打开的存储：bbolt 持有文件锁，
	// 不释放会让下一次启动卡在"目录被占用"上，而真因是上一次装配失败。
	assembled := false
	defer func() {
		if !assembled {
			s.bgStop()
			if s.kv != nil {
				_ = s.kv.Close()
			}
		}
	}()

	bopt := broker.Options{
		Node:               cluster.NodeID(s.mf.NodeID),
		Meta:               s.store,
		Router:             s.rt,
		Sessions:           s.sessions,
		Owners:             s.owners,
		AuthoritativeEpoch: s.cfg.Node.Mode != config.ModeStandalone,
		Limiter:            s.lim,
		Bus:                s.bus,
		Auth:               auth,
		Limits:             limits,
		Env:                env,
		Now:                s.now,
		MaxViewStalenessMs: s.cfg.Cluster.MaxViewStaleness.D().Milliseconds(),
		Reserved:           shadowAdapter{svc: shadowSvc},
		// 单机下 s.mesh 为 nil，而 Route 返回的远端集合恒为空，
		// 转发分支不会进入（契约 02、07）。
		Mesh:       s.meshTransport(),
		OnTakeover: s.broadcastTakeover,
	}
	brokerOptions(&s.cfg.Broker, &bopt)
	brk, err := broker.New(bopt)
	if err != nil {
		return nil, fmt.Errorf("server: 装配 broker 失败: %w", err)
	}
	s.brk = brk

	// 管理面令牌。没有令牌时 /api/admin/v1 不挂载——
	// 而不是挂载一个无鉴权的版本（见 adminauth.go）。
	adminToken, terr := ResolveAdminToken(&s.cfg, s.dd.Root(), opt.LookupEnv)
	if terr != nil && !errors.Is(terr, ErrNoAdminToken) {
		return nil, terr
	}
	s.admin = newAdminServer(s, adminToken)
	assembled = true
	return s, nil
}

// buildAuthenticator 按配置选择认证器。
func (s *Server) buildAuthenticator() (broker.Authenticator, error) {
	var base broker.Authenticator
	switch s.cfg.Auth.Mode {
	case config.AuthCert:
		base = broker.CertAuthenticator{}
	case config.AuthInsecureNone:
		// 配置校验已经确认了两道确认（acknowledge_insecure 加回环绑定），
		// 这里再要求 broker 层的显式确认——不认证这件事不该只有一处闸门。
		s.log.Warn("以不认证模式运行：接受任何客户端，仅可用于本地开发")
		base = broker.InsecureNoAuth{Acknowledged: true}
	default:
		return nil, fmt.Errorf("server: 未知的认证方式 %q", s.cfg.Auth.Mode)
	}
	if !s.cfg.Listeners.MQTTWSS.Enabled {
		return base, nil
	}
	keys, err := loadDeviceKeys(s.cfg.Listeners.MQTTWSS.KeysFile)
	if err != nil {
		return nil, err
	}
	s.wssKeys = keys
	return broker.TransportIdentity{Next: base, Key: s.wssThing}, nil
}

func (s *Server) wssSecret(id string) (string, bool) {
	k, ok := s.wssKeys[id]
	if !ok {
		return "", false
	}
	return k.Secret, true
}

func (s *Server) wssThing(id string) (string, bool) {
	k, ok := s.wssKeys[id]
	if !ok || k.Thing == "" {
		return "", false
	}
	return k.Thing, true
}

// Prepare 载入注册表并使节点进入就绪态。
func (s *Server) Prepare(ctx context.Context) error {
	if err := s.reg.EnsureDirs(); err != nil {
		return err
	}
	// 从冷层重放热视图**只在单机**做。
	//
	// 集群下状态机的内容由复制日志（与它的快照）决定，而 Restore 是经
	// 写入路径重建的——每条记录都会被重新提进日志。一个冷层里已有一百万
	// 条记录的节点重启时那是一百万条新日志，而命令不带 ID，幂等窗口认不出
	// 它们是同一件事；在跟随者上这些提案还要逐条转发给 leader。
	// 装快照时的重建走 registry.HydrateHot（只改热视图、不写日志）。
	var res registry.Stats
	if s.replStore == nil {
		var err error
		res, err = s.reg2.Restore(ctx)
		if err != nil {
			return err
		}
	} else if err := s.hydrateHotAtBoot(); err != nil {
		// 集群下的重建走 HydrateHot（只改热视图、不写日志，P2）。
		// 它与 applied index 落盘是一对，见 hydrateHotAtBoot 的说明。
		return err
	}
	// 再把文件化注册表（v0.1 内核的临时实现）里的内容导入控制面，
	// 之后一切读写都走 registry。导入是幂等的，重复启动不会重复创建。
	//
	// **顺序不能与上面的重建对调。** 导入要写设备与证书，而写入前的校验
	// 要求绑定的策略同时在冷层与热视图里（registry.policyEntryLocked：
	// 冷层给编号、热视图给编译结果，少一层就拒绝）。第一次启动时冷层是
	// 空的，策略由导入自己写进去，PutPolicy 两层一起写，所以那一次反着
	// 也能过；而第二次启动时冷层已经有策略、导入会跳过它，热视图却要等
	// 重建才有——于是「serve 过一次、再 cert issue、再重启」这条路会以
	// 「策略不存在」失败，而那份策略明明在冷层里，节点从此起不来。
	//
	// 这与 CLU-072 是同一类：一份逻辑状态摊在两层上，而写入前的校验读的
	// 是两层的交集，因此**重建必须发生在任何写入之前**。
	//
	// 集群下只有 bootstrap 节点导入：那些文件是「一次性的导入来源」
	// （REG-105），而元数据的权威是复制日志。三个节点各自导入会把同一份
	// 策略写进日志三次——幂等键不同，状态机认不出它们是同一件事。
	// join 节点什么都不做，内容会随日志到来。
	var imported int
	if s.replStore == nil || s.cfg.Node.Mode == config.ModeBootstrap {
		n, err := s.importFileRegistry(ctx)
		if err != nil {
			return err
		}
		imported = n
	}
	// 导入进来的记录不在上面那次重建的统计里，补数一次。只有真导入了
	// 东西才数：常规启动的 imported 是 0，不该为此多扫一遍整个冷层。
	if imported > 0 && s.replStore == nil {
		st, err := s.reg2.CountCold()
		if err != nil {
			return err
		}
		res = st
	}
	// 属性槽位是一次写入，只有能写的节点做：单机，或集群里的 bootstrap
	// 节点（join 节点的槽位随日志到来）。
	if s.replStore == nil || s.cfg.Node.Mode == config.ModeBootstrap {
		if err := s.reg2.SyncAttrSlots(ctx); err != nil {
			return err
		}
	}
	s.ready.Set(compRegistry, true, "")
	s.store.MarkConfirmed(s.now())
	s.ready.Set(compView, true, "")

	// 保留消息装回内存，并把版本计数器抬到持久化的高水位之上。
	//
	// SeedVersion 不是可选的：计数器从 0 重新开始会让新发布的保留消息
	// 拿到版本 1，而覆盖判定是"版本更大才覆盖"——新消息会被恢复出来的
	// 旧消息挡住并静默丢失（见 router.SeedVersion）。
	// 复制钩子在装载之后才挂：装载走 Replace 本来就不触发复制，但顺序
	// 写成"先装载后挂钩"是为了让这件事不依赖 Replace 的实现细节。
	if s.retaineds != nil {
		s.rt.Retained().SetReplicator(s.broadcastRetained)
	}
	nRetained, maxVer, err := s.retained.LoadInto(s.rt.Retained())
	if err != nil {
		return err
	}
	s.rt.SeedVersion(maxVer)
	s.rt.Retained().SetSink(s.retained)

	// 清理已过期的会话。放在接受流量之前：带着一批过期会话开始服务，
	// 会让它们各自的离线队列占住内存直到下一次清理。
	//
	// 集群下它是一次**复制写入**，而启动这一刻分片可能还没选出 leader
	// （CLU-118）。那时不要让启动失败：清理是周期任务，下一轮会补上，
	// 而为它退出进程会把一次瞬态变成 CrashLoopBackOff。
	expired, err := s.sessions.Expire(ctx, s.now())
	if err != nil {
		if !errors.Is(err, cluster.ErrNotLeader) {
			return err
		}
		s.log.Warn("启动期的会话过期清理被推迟",
			"原因", "会话分区组尚未选出 leader", "说明", "周期清理会补上")
		expired = 0
	}

	s.log.Info("本地存储已就绪", "目录", s.dd.Path(node.DirMeta),
		"影子数", s.shadows.CountShadows(),
		"会话数", s.sessions.Count(), "已清理过期会话", expired,
		"保留消息数", nRetained, "保留版本水位", maxVer.Counter, "版本来源节点", maxVer.Node)
	s.log.Info("控制面已载入",
		"策略数", res.Policies, "设备数", res.Things,
		"证书数", res.Certificates, "可连接证书数", res.ActiveCertificates,
		"本次导入", imported)
	if res.ActiveCertificates == 0 && s.cfg.Auth.Mode == config.AuthCert {
		// 这不是错误，但几乎总是意料之外：没有任何可用证书的 broker
		// 会拒绝所有设备，而被拒的日志里只会看到"证书未注册"。
		s.log.Warn("没有任何处于 ACTIVE 的证书，所有设备都将被拒绝",
			"如何签发", "openiotd cert issue --thing <名字> --policy <策略名>",
			"策略目录", s.reg.PoliciesDir())
	}
	return nil
}

// Run 启动监听并阻塞直到 ctx 取消，随后优雅关闭。
func (s *Server) Run(ctx context.Context) error {
	// 启动过程中任一步失败都必须回收已经起来的部分。
	//
	// 二进制里进程随即退出，看不出区别；但嵌入使用与集成测试里，
	// 一个"启动失败"却留着管理端点和后台协程的 Server 就是泄漏。
	// 这正是把装配逻辑放在可测包里才会发现的问题。
	started := false
	defer func() {
		if !started {
			_ = s.shutdown()
		}
	}()

	// 管理端点先起：它要在设备端口之前可用，否则就绪探针在节点
	// 最需要被观测的那段时间里是不可达的。
	if s.cfg.Listeners.Admin.Enabled {
		if err := s.admin.start(ctx); err != nil {
			return err
		}
		s.log.Info("管理端点已监听", "地址", s.admin.addr())
	}

	// 集群模式下先把元数据组带起来，再做需要写入的准备工作。
	//
	// 顺序颠倒过一次，症状很明确：bootstrap 节点在选举之前就去导入
	// 文件化策略，而那是一次写入——"不是 leader，写入被拒绝"。
	if s.replStore != nil {
		// 顺序：先起复制组，再等就绪。组必须在注册表建好之后才能起——
		// NewRaft 会把最新的快照装进状态机，而那一步要扫冷层重建热视图
		// （registry.HydrateHot）。
		if err := s.startReplicationGroup(); err != nil {
			return err
		}
		if err := s.startReplication(ctx); err != nil {
			return err
		}
		// 显式启用的影子分片必须在 metadata Barrier 之后启动：旧路径里
		// 可能还有已提交但未应用的影子命令，先起新日志会让新旧两条日志
		// 对同一文档的顺序不确定。
		if s.cfg.Cluster.Replication.ShadowShards != 0 {
			replicas := make(map[cluster.NodeID]uint64, len(s.cfg.Cluster.Replication.Replicas))
			for id, replica := range s.cfg.Cluster.Replication.Replicas {
				replicas[cluster.NodeID(id)] = replica
			}
			if err := s.startShadowShards(replicas); err != nil {
				return err
			}
		}
	}

	if err := s.Prepare(ctx); err != nil {
		return err
	}
	// 被拒操作的日志。在开始接受流量之前订阅，否则最初几次失败的
	// 接入——往往正是配置刚写错的那几次——不会被记录。
	// 审计订阅也放在这里：管理端点已经在听，测试会在就绪之后立刻写。
	s.wg.Add(3)
	go s.watchAudit(s.bgCtx)
	go s.watchRejections(s.bgCtx)
	go s.watchEvents(s.bgCtx.Done())
	if s.replStore != nil {
		s.wg.Add(3)
		go func() {
			defer s.wg.Done()
			s.runMetaConfirm(s.bgCtx)
		}()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runShardReadiness(s.bgCtx)
		}()
		go func() {
			defer s.wg.Done()
			s.runGossip(s.bgCtx)
		}()
		go func() {
			defer s.wg.Done()
			s.runTakeovers(s.bgCtx)
		}()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runRetained(s.bgCtx)
		}()
	}
	// 回收不分单机与集群：过期的保留消息两边都要回收，而墓碑在单机下
	// 也会产生（清除走的是同一条路）。
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runRetainedSweep(s.bgCtx)
	}()
	// 速率桶在恢复满额后与“没有桶”等价。分片增量回收避免百万个
	// clientId 的一次性 CONNECT 把限速器 map 永久留在峰值。
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runQuotaSweep(s.bgCtx)
	}()
	if s.tracer.Enabled() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.tracer.Run(s.bgCtx)
		}()
	}

	if err := s.startMQTTListeners(ctx); err != nil {
		return err
	}
	started = true
	s.ready.Set(compListeners, true, "")
	s.log.Info("节点已就绪",
		"node_id", s.mf.NodeID, "cluster_id", s.mf.ClusterID,
		"shard_count", s.mf.ShardCount)

	<-ctx.Done()
	return s.shutdown()
}

const quotaSweepInterval = 100 * time.Millisecond

// runQuotaSweep 每次只扫一个令牌桶分片；64 个分片约 6.4 秒一轮。
// 这样百万设备重连后能及时回收，又不会一次长时间挡住 CONNECT 热路径。
func (s *Server) runQuotaSweep(ctx context.Context) {
	t := time.NewTicker(quotaSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.lim.SweepRateBuckets(1)
		}
	}
}

// startMQTTListeners 启动设备端口。只在就绪后调用（PRD CLU-032）。
//
// 会话分区组是这里唯一"等而不强求"的一项（CLU-118）。等它是因为常态下
// 它一秒内就选完，等到了可以少掉一批"不是 leader"的 CONNECT；不强求是
// 因为**等不到时退出会让收敛更难**：端口一关，对端从"连接超时"变成
// "连接被拒绝"，断路器一直开着，于是永远等不到——k8s 上重建一个 Pod
// 就会卡成 CrashLoopBackOff（实测，见 startReplication 的说明）。
//
// 等不到就照常监听，由 `/readyz` 把这个 Pod 挡在 Service 之外。CLU-032
// 要的是"未就绪的节点不接流量"，就绪探针正是执行这件事的地方。
func (s *Server) startMQTTListeners(ctx context.Context) error {
	if s.shards != nil {
		deadline := time.Now().Add(replicationStartTimeout)
		for !s.shardsReady() && time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(replicationRetryInterval):
			}
		}
		if s.shardsReady() {
			s.ready.Set(compShards, true, "")
		} else {
			s.log.Warn("会话分区组尚未选出 leader，设备端口照常监听但节点不报就绪",
				"等待", replicationStartTimeout.String(),
				"说明", "进程留着让 raft 自己收敛；收敛前会话写入会被拒")
		}
	}
	// compShards 不在这条硬判定里：它由上面那段与 runShardReadiness 维护，
	// 而"没就绪就不监听"在它身上会变成死锁（见本函数开头）。
	nr := s.ready.NotReady()
	hard := nr[:0]
	for _, c := range nr {
		if c != compListeners && c != compShards {
			hard = append(hard, c)
		}
	}
	if len(hard) > 0 {
		return fmt.Errorf("server: 节点未就绪（%v），不得监听设备端口", hard)
	}
	type spec struct {
		name string
		l    *config.TLSListener
	}
	for _, sp := range []spec{
		{"mqtt_tls", &s.cfg.Listeners.MQTTTLS},
		{"mqtt_alpn", &s.cfg.Listeners.MQTTALPN},
	} {
		if !sp.l.Enabled {
			continue
		}
		if err := s.startTLSListener(ctx, sp.name, sp.l); err != nil {
			return err
		}
	}
	if err := s.startWSSListener(ctx); err != nil {
		return err
	}
	if s.cfg.Listeners.MQTTPlain.Enabled {
		plain := &s.cfg.Listeners.MQTTPlain
		var ln *broker.Listener
		var err error
		if plain.ProxyProtocol == "v2" {
			trusted := make([]netip.Prefix, 0, len(plain.TrustedProxies))
			for _, raw := range plain.TrustedProxies {
				// Config.Validate 已经严格检查；这里仍返回错误而不是 panic，
				// 使直接构造 Server 的调用方也拿到可诊断结果。
				p, perr := netip.ParsePrefix(raw)
				if perr != nil {
					return fmt.Errorf("mqtt_plain trusted proxy %q: %w", raw, perr)
				}
				trusted = append(trusted, p)
			}
			ln, err = s.brk.ListenProxyV2(plain.Addr, broker.ProxyOptions{
				TrustedProxies: trusted,
				HeaderTimeout:  plain.ProxyHeaderTimeout.D(),
			})
		} else {
			ln, err = s.brk.ListenPlain(plain.Addr)
		}
		if err != nil {
			return err
		}
		s.serve(ctx, ln)
		if plain.ProxyProtocol == "v2" {
			s.log.Info("可信 TLS 终结器 MQTT 端点已监听", "地址", ln.Addr().String(),
				"trusted_proxies", plain.TrustedProxies)
		} else {
			s.log.Warn("明文 MQTT 端点已监听（无传输加密）", "地址", ln.Addr().String())
		}
	}
	return nil
}

func (s *Server) startTLSListener(ctx context.Context, name string, l *config.TLSListener) error {
	cert, err := LoadServerCert(s.dd, &s.cfg, l)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	opt := broker.TLSOptions{
		Certificate:       cert,
		RequireClientCert: l.RequireClientCert,
		NextProtos:        l.ALPN,
		MinVersion:        tlsVersion(l.MinTLSVersion),
		CurvePreferences:  tlsCurvePreferences(l.PostQuantumKeyExchange),
		RequireSNI:        l.RequireSNI,
		HandshakeTimeout:  l.HandshakeTimeout.D(),
	}
	if l.RequireClientCert {
		pool, err := LoadClientCAs(s.dd, &s.cfg, l, s.ca)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		opt.ClientCAs = pool
	}
	if l.SessionTicketKeysFile != "" {
		keys, err := LoadSessionTicketKeys(l.SessionTicketKeysFile)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		opt.SessionTicketKeys = keys
	}
	ln, err := s.brk.ListenTLS(l.Addr, opt)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	s.serve(ctx, ln)
	s.log.Info("MQTT 端点已监听", "端点", name, "地址", ln.Addr().String(),
		"要求客户端证书", l.RequireClientCert, "ALPN", l.ALPN)
	return nil
}

func (s *Server) startWSSListener(ctx context.Context) error {
	l := &s.cfg.Listeners.MQTTWSS
	if !l.Enabled {
		return nil
	}
	cert, err := LoadServerCert(s.dd, &s.cfg, &config.TLSListener{
		CertFile: l.CertFile,
		KeyFile:  l.KeyFile,
	})
	if err != nil {
		return fmt.Errorf("mqtt_wss: %w", err)
	}
	ln, err := s.brk.ListenWebSocket(l.Addr, broker.WebSocketOptions{
		Certificate:      cert,
		MinVersion:       tlsVersion(l.MinTLSVersion),
		CurvePreferences: tlsCurvePreferences(l.PostQuantumKeyExchange),
		RequireSNI:       l.RequireSNI,
		HandshakeTimeout: l.HandshakeTimeout.D(),
		Checker: sigv4.Checker{
			Region:   l.Region,
			Secret:   s.wssSecret,
			Services: map[string]bool{sigv4.ServiceDeviceGateway: true},
		},
		MaxLifetime: l.MaxConnection.D(),
		RedirectTo:  l.RedirectTo,
	})
	if err != nil {
		return fmt.Errorf("mqtt_wss: %w", err)
	}
	s.serve(ctx, ln)
	s.log.Info("MQTT over WebSocket 已监听", "地址", ln.Addr().String(), "区域", l.Region)
	return nil
}

func tlsCurvePreferences(postQuantum bool) []tls.CurveID {
	if postQuantum {
		// nil 明确交还 Go 的版本化默认值；Go 1.24 起客户端与服务端共同
		// 支持时会选择 X25519+ML-KEM 混合组。
		return nil
	}
	return []tls.CurveID{tls.X25519, tls.CurveP256}
}

func (s *Server) serve(ctx context.Context, ln *broker.Listener) {
	s.listeners = append(s.listeners, ln)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := ln.Serve(ctx); err != nil {
			s.log.Error("监听器退出", "地址", ln.Addr().String(), "错误", err)
		}
	}()
}

// shutdown 按 PRD NFR-064 的顺序优雅关闭。
//
// 顺序是有讲究的，而且不止一种顺序能跑通、只有一种是对的：
//
//  1. 先把就绪置为假，使编排系统的探针立刻看到节点要下线；
//  2. **只停止接受**新连接，不等待已有连接——等待必须排在断开之后，
//     因为连接协程正阻塞在读客户端套接字上，只有 Broker.Close 能
//     放开它们。按「关监听器、再关 broker」的直觉顺序写会直接死锁；
//  3. 断开设备连接并等在途消息写出；
//  4. 这时才等监听器的处理协程收尾；
//  5. **停掉管理端点**——它的 Serve 协程也登记在同一个 WaitGroup 上，
//     若放在等待之后关，下一步会一直等一个还没被叫停的协程，
//     直到撞上超时。这个顺序错误的症状是"每次关闭都恰好耗满超时时长"，
//     而它在进程随即退出的场景下完全看不出来；
//  6. 等全部协程退出，超时则强制继续；
//  7. 最后关元数据与所有权——放在最后是因为正在断开的连接仍会访问它们。
//
// 这段顺序里有两处都是"直觉顺序恰好是错的"，且两者在生产日志里的
// 表现都只是"关闭慢"或"进程没了"。它们能被发现，只因为装配逻辑
// 放在了可测的包里，并且真的断言了关闭耗时。
func (s *Server) shutdown() error {
	var firstErr error
	s.closeOnce.Do(func() {
		s.log.Info("开始优雅关闭", "超时", ShutdownTimeout)
		s.ready.Set(compListeners, false, "正在关闭")

		// 先叫停全部后台协程。它们登记在同一个 WaitGroup 上，
		// 不先取消就会把第 5 步的等待拖到超时。
		s.bgStop()

		for _, ln := range s.listeners {
			if err := ln.StopAccepting(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := s.brk.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		for _, ln := range s.listeners {
			if err := ln.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.admin != nil {
			if err := s.admin.stop(); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		done := make(chan struct{})
		go func() { s.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(ShutdownTimeout):
			s.log.Error("关闭超时，强制退出", "超时", ShutdownTimeout)
		}

		// 会话分区组在复制组之前关：它的分片建在复制组的 NodeHost 上，
		// 而 TLSGroup.Close 会把那个 NodeHost 一起停掉——反过来的顺序是
		// 对着已停的 NodeHost 调 StopReplica，那只会得到一个没有意义的
		// 错误（分片其实已经停了），把它当成关闭失败报出去。
		if shadowShards := s.shadowShards.Load(); shadowShards != nil {
			if err := shadowShards.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.shards != nil {
			if err := s.shards.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		// broker 已在上面停止产生新帧；先停 mesh worker，避免它们
		// 在复制组关闭后继续重试已经不可达的对端。
		if s.mesh != nil {
			if err := s.mesh.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		// 复制组在状态机之前关：它的读协程还会往状态机上应用日志，
		// 反过来的顺序会让一条正在应用的命令撞上已关闭的存储。
		if s.repl != nil {
			if err := s.repl.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := s.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		// 存储最后关：正在收尾的连接与影子写入仍会用到它。
		if s.kv != nil {
			if err := s.kv.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := s.owners.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if n := s.rt.Retained().PersistFailures(); n > 0 {
			// 非零意味着部分保留消息可能无法跨重启存活。
			// 静默掉它会让"重启后少了几条保留消息"变成无法解释的现象。
			s.log.Error("保留消息持久化曾失败", "次数", n)
		}
		s.log.Info("已关闭")
	})
	return firstErr
}

// Close 使 Server 可以在未调用 Run 的情况下释放资源。
func (s *Server) Close() error { return s.shutdown() }

// Ready 返回就绪状态，供测试与管理端点使用。
func (s *Server) Ready() bool { return s.ready.Ready() }

// Manifest 返回节点身份。
func (s *Server) Manifest() node.Manifest { return s.mf }

// AdminAddr 返回管理端点的实际地址，便于测试取随机端口。
func (s *Server) AdminAddr() string {
	if s.admin == nil {
		return ""
	}
	return s.admin.addr()
}

// MQTTAddrs 返回已监听的设备端口地址。
func (s *Server) MQTTAddrs() []string {
	out := make([]string, 0, len(s.listeners))
	for _, ln := range s.listeners {
		out = append(out, ln.Addr().String())
	}
	return out
}

// CA 返回签发 CA，未启用 PKI 时为 nil。
func (s *Server) CA() *pki.CA { return s.ca }

// Registry 返回文件化注册表（v0.1 内核的临时实现，仅供导入与 CLI 使用）。
func (s *Server) Registry() *Registry { return s.reg }

// ControlPlane 返回设备、证书与策略的控制面。
func (s *Server) ControlPlane() *registry.Registry { return s.reg2 }

// Shadow 返回影子服务，供 REST 数据面与测试使用。
func (s *Server) Shadow() *shadow.Service { return s.shadowSvc }

// Sessions 返回会话存储，供 Admin API 与测试使用。
func (s *Server) Sessions() *store.SessionStore { return s.sessionLocal }

func tlsVersion(s string) uint16 {
	if s == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

// traceShadow 给跨节点影子转发包一条 span。单机不会进入这条路径。
func (s *Server) traceShadow(ctx context.Context, owner, thing string) (context.Context, func(error)) {
	ctx, sp := s.tracer.Start(ctx, "shadow.forward")
	sp.Set("peer", owner)
	sp.Set("thing", thing)
	return ctx, func(err error) {
		if err != nil {
			sp.SetError()
		}
		sp.End()
	}
}

// wallClockMs 是默认时钟。
//
//contract:allow-time-now
func wallClockMs() int64 { return time.Now().UnixMilli() }

// authzEnv 由配置构造策略求值环境。
func authzEnv(cfg *config.Config) authz.Env {
	return authz.Env{
		Partition: cfg.IoT.Partition,
		Region:    cfg.IoT.Region,
		Account:   cfg.IoT.Account,
	}
}
