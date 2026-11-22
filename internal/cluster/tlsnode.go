package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	dbconfig "github.com/lni/dragonboat/v4/config"
)

/*
跨进程的复制组(CLU-060、CLU-096)。

一个节点对外只开**一个**端口、只用**一套**证书(CLU-076)。这条约束换成
hashicorp/raft 之后仍然成立，做法是在同一个 TLS 监听上按连接的第一个字节
分流:

	'R' → 交给 raft 的 NetworkTransport(选举、复制、快照、领导权转移)
	'O' → 本项目自己的请求(写入转发、mesh 消息帧、Admin 扇出查询)

分流而不是给 raft 另开一个端口，理由不只是"少开一个端口":部署清单里
每多一个端口就多一处要在 k8s Service、防火墙与证书 SAN 里对齐的东西,
而漏掉其中任何一处的现象都是"集群起不来",不指向真因。

证书的 CommonName 必须等于节点 ID,两个方向都查：服务端查客户端证书,
客户端查服务端证书。少查一个方向的后果是一个拿着合法证书的节点可以
冒充另一个节点的身份参与投票。
*/

const (
	// 连接的第一个字节标明这条连接是给谁的。
	//
	// 用一个字节而不是握个手:raft 的 NetworkTransport 拿到连接之后
	// 立刻开始说它自己的协议，我们没有插入握手的余地。
	markRaft byte = 'R'
	markOwn  byte = 'O'
)

// TLSOptions 是跨进程复制组的装配参数。
type TLSOptions struct {
	// ID 是本节点 ID,须与本节点证书的 CommonName 一致。
	ID NodeID
	// Addr 是监听地址，同时是告知对端的地址。
	Addr string
	// Replicas 是节点 ID 到副本号的映射（cluster.replication.replicas）。
	// 共识库按 uint64 认成员，而本项目的身份是字符串节点 ID（CLU-069）。
	Replicas map[NodeID]uint64
	// Peers 是**初始**成员：节点 ID 到地址。之后以日志里的成员变更为准
	// (CLU-090),所以这张表只在 bootstrap 那一次起作用。
	Peers map[NodeID]string
	// TLS 是节点之间的互信材料。
	TLS *tls.Config
	// Dir 是快照目录。
	Dir string

	// 状态机的三个回调，见 RaftOptions。
	Apply      func(index uint64, data []byte) error
	ApplyBatch func(entries []ShardEntry) error
	// AppliedIndex 回报已持久化到哪一条（CLU-109）：on-disk 状态机在
	// Open 时要这个数字，报 0 就是丢数据。
	AppliedIndex func() uint64
	SnapshotTo   func(w io.Writer) error
	RestoreFrom  func(r io.Reader) error

	// Bootstrap 为真时用 Peers 创建新集群。
	Bootstrap bool
	// Output 是库内部日志的出口。
	Output io.Writer

	// 下面几项为 0 时取默认值。
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
	ApplyTimeout       time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
	SnapshotInterval   time.Duration
	// DialTimeout 是连对端的上限。0 表示 2 秒。
	DialTimeout time.Duration
	// RTT 是共识库的 tick 间隔（cluster.replication.rtt）。0 表示 100
	// 毫秒。心跳与选举的刻度数跟着它缩放，故障检测的时长不随它变
	// （见 newDragonGroup，CLU-132）。
	RTT time.Duration
}

// TLSGroup 是一个通过 TLS 与其它进程复制日志的节点。
type TLSGroup struct {
	id  NodeID
	dir string
	// group 是原子的：hraft.NewRaft 在返回**之前**就起了协程并开始拨号,
	// 而拨号要反查成员(idOf)。装配协程这边 g.group = group 的赋值与
	// 那些协程的读取是并发的——-race 当场抓到过这一条。
	group   atomic.Pointer[groupRef]
	ln      net.Listener
	tlsCfg  *tls.Config
	peers   map[NodeID]string
	dialTO  time.Duration
	applyTO time.Duration
	// reqTO 是本项目那条协议一次问答的上限（读写同一个值）。
	reqTO time.Duration
	// pool 是 markOwn 连接的池子（CLU-038）。见 peerpool.go。
	pool peerPool
	// ownAccepted 是入站的 markOwn 连接计数，见 intake 里的说明。
	ownAccepted atomic.Int64
	draining    atomic.Bool
	// learned 是拨号时学到的"地址到节点 ID"。
	//
	// 新加入的节点配置里只有自己（peers 只需写自己，见 Peers 的说明），
	// 而它要回话的那个 leader 地址既不在 peers 里、也读不到成员列表
	// （读成员要先入组，入组又要先能回话）。于是拨号在地址未知时按
	// "链校验通过就认下它报的 CommonName"处理，并把结果记在这里——
	// 之后同一个地址就有名字了。
	//
	// 这不放宽 CLU-068：证书仍须由复制 CA 签出，拿设备证书照样冒充不了
	// 节点。放宽的只是"必须事先知道对面叫什么"，而那一条在加入那一刻
	// 本来就不可能成立。
	learnedMu sync.RWMutex
	learned   map[string]NodeID
	// dragon 是共识库的传输层，分流到 markRaft 的连接交给它。
	//
	// 装配期为空是正常状态：复制组要等注册表建好才能起（装快照要重建
	// 热视图），而监听在那之前就开着了。那段时间里进来的 raft 连接被
	// 直接关掉，对端会重试。
	dragon atomic.Pointer[dragonTransport]

	// mesh 消息的处理函数，见 meshnode.go。
	meshMu sync.RWMutex
	meshFn func(NodeID, MeshPayload)

	// Admin 扇出查询的处理函数，见 query.go。
	queryMu sync.RWMutex
	queryFn QueryHandler

	// shadow handlers 只接收上层自描述字节，cluster 不依赖 shadow 包。
	// 读写分成两种 wire kind，因为只有只读请求可在坏连接上安全重试。
	shadowMu    sync.RWMutex
	shadowWrite func([]byte) ([]byte, error)
	shadowRead  func([]byte) ([]byte, error)

	// shards 是会话分片组。放在 TLSGroup 上是为了让已有的
	// markOwn 协议能把一个合并好的提案直接送到分片 leader，
	// 省掉先交给本地 follower、再由 Dragonboat 转发的一跳。
	shardMu     sync.RWMutex
	shards      *ShardGroup
	shardGroups map[uint64]*ShardGroup
	// ownership 是分片 leader 内存所有权服务（CLU-008/009）。
	ownershipMu sync.RWMutex
	ownership   *ShardOwnership

	closeOnce sync.Once
}

// wireMsg 是本项目自己那条协议的请求。
//
// raft 自己的 RPC(投票、追加、快照)**不在这里**:它们走库的
// NetworkTransport。这里只剩六类本项目的消息。
type wireMsg struct {
	Kind uint8
	From string
	// Data 是转发给 leader 的那条命令。
	Data []byte
	// Shard 是会话分片提案的分片号。
	Shard uint64
	// Mesh 承载消息面的负载(消息帧、兴趣通告、设备归属),见 meshnode.go。
	Mesh *MeshPayload
	// Query 是 Admin 面的扇出查询(契约 12),见 query.go。
	Query *wireQuery
	// Ownership 是不写日志的会话所有权 RPC。
	Ownership *wireOwnership
	// SessionRead 是未承载会话分片的 Access 节点发起的只读请求。
	SessionRead *wireSessionRead
}

type wireResp struct {
	OK  bool
	Err string
	// Value 是状态机返回的判定值。会话批量提案用它携带
	// stale bitmap，所以不能在跨节点路由时丢掉。
	Value uint64
	// Rows 是扇出查询的结果行，已经编码成字节。
	//
	// 不用 []any：gob 要求注册具体类型，而每种查询的行类型不同——
	// 让传输层认识每一种，等于把它和每个查询绑在一起。
	Rows [][]byte
	// NextCursor 是本节点的续读位置，空表示读完了。
	NextCursor string
	// Owner/PrevValue/Found 是所有权 Claim/Lookup 的返回值。
	Owner            string
	PrevValue        uint64
	Found            bool
	OwnershipResults []wireOwnershipResult
	// Data 是上层自描述编码的会话快照；Found 区分不存在与空内容。
	Data               []byte
	SessionReadResults []wireSessionReadResult
}

const (
	wirePropose uint8 = iota + 1
	wireMesh
	// wireQueryKind 与类型 wireQuery 区分开：同名会让「消息类型」与
	// 「消息载荷」撞在一个标识符上。
	wireQueryKind
	// wireShardPropose 只路由一跳，且与 wirePropose 一样不重试。
	wireShardPropose
	wireOwnershipKind
	wireSessionReadKind
	wireShardStatusKind
	wireShardBarrierKind
	wireShadowWriteKind
	wireShadowReadKind
)

type wireSessionRead struct {
	Shard     uint64
	ClientID  string
	ClientIDs []string
}

type wireSessionReadResult struct {
	Data  []byte
	Found bool
	Err   string
}

type wireOwnership struct {
	Op       uint8
	ClientID string
	Node     string
	Epoch    uint64
	Eviction *Eviction
	Claims   []wireOwnershipClaimItem
}

type wireOwnershipClaimItem struct {
	ClientID string
	Node     string
}

type wireOwnershipResult struct {
	Epoch     uint64
	PrevNode  string
	PrevEpoch uint64
	Had       bool
	Err       string
	// Claim 与 session cell 使用同一个 shard。非副本 Access 节点可在这次
	// 应答里顺带拿到只读快照，避免 CONNECT 随后再发一轮 SessionRead RPC。
	SessionData  []byte
	SessionFound bool
	SessionRead  bool
}

const (
	wireOwnershipClaim uint8 = iota + 1
	wireOwnershipRelease
	wireOwnershipLookup
	wireOwnershipEvict
	wireOwnershipClaimBatch
)

// ListenTLS 在 opt.Addr 上接受集群连接并启动复制组。
func ListenTLS(opt TLSOptions) (*TLSGroup, error) {
	switch {
	case opt.ID == "":
		return nil, errors.New("cluster: 缺少节点 ID")
	case opt.TLS == nil:
		return nil, errors.New("cluster: 缺少节点间互信材料")
	}
	serverCfg := opt.TLS.Clone()
	serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
	if serverCfg.ClientCAs == nil {
		serverCfg.ClientCAs = serverCfg.RootCAs
	}
	ln, err := tls.Listen("tcp", opt.Addr, serverCfg)
	if err != nil {
		return nil, fmt.Errorf("cluster: 监听 %s 失败: %w", opt.Addr, err)
	}

	peers := make(map[NodeID]string, len(opt.Peers))
	for id, addr := range opt.Peers {
		peers[id] = addr
	}
	g := &TLSGroup{
		id: opt.ID, dir: opt.Dir, ln: ln, tlsCfg: opt.TLS, peers: peers,
		dialTO: opt.DialTimeout, applyTO: opt.ApplyTimeout,
	}
	if g.dialTO <= 0 {
		g.dialTO = 2 * time.Second
	}
	if g.applyTO <= 0 {
		g.applyTO = 10 * time.Second
	}
	// 一次问答的上限要容得下对端那一侧最慢的那件事：wirePropose
	// 与 wireShardPropose 会在对端等一次共识提交，而那件事自己的
	// 上限就是 applyTO。只给 applyTO
	// 的话，一次正好用满额度的提交会在本侧先超时，于是「对端其实提交
	// 成功了而发起方报失败」——而那是最难查的一类不一致。
	g.reqTO = g.applyTO + g.dialTO

	group, derr := newDragonGroup(DragonOptions{
		ID: opt.ID, Dir: opt.Dir,
		Replicas: opt.Replicas, Peers: peers,
		Apply: opt.Apply, ApplyBatch: opt.ApplyBatch, AppliedIndex: opt.AppliedIndex,
		SnapshotTo: opt.SnapshotTo, RestoreFrom: opt.RestoreFrom,
		Transport:    g.DragonTransportFactory(),
		Bootstrap:    opt.Bootstrap,
		ApplyTimeout: opt.ApplyTimeout,
		RTT:          opt.RTT,
	})
	if derr != nil {
		_ = ln.Close()
		return nil, derr
	}
	g.group.Store(&groupRef{g: group})
	go g.serve()
	return g, nil
}

// g 取当前的复制组。装配未完成时为 nil。
//
// 返回 nil 而不是阻塞等装配完成：拨号发生在库自己的协程上，让它在那里
// 等一个装配锁会把启动顺序变成"谁先拿到锁"。拿不到组就让这次拨号失败,
// 库会重试。
func (t *TLSGroup) g() replicationGroup {
	ref := t.group.Load()
	if ref == nil {
		return nil
	}
	return ref.g
}

// Addr 是实际监听地址。
func (g *TLSGroup) Addr() string {
	if g.ln == nil {
		return ""
	}
	return g.ln.Addr().String()
}

// Propose 提交一条命令。
//
// 非 leader 时转发给 leader 走本项目自己那条连接，而不是让调用方重试:
// 控制面的写入可以落在任何一个节点上(负载均衡器后面哪台都行),而
// 「这台不是 leader」是集群内部的事，不该变成 API 的错误(CLU-071)。
//
// 转发只做一跳：收到转发的节点若也不是 leader,直接把 ErrNotLeader 回去。
// 再转一次就可能在两个都以为对方是 leader 的节点之间来回弹。
func (g *TLSGroup) Propose(data []byte) error {
	if g.draining.Load() {
		return ErrDraining
	}
	grp := g.g()
	if grp == nil {
		return ErrNotLeader
	}
	err := grp.Propose(data)
	if !errors.Is(err, ErrNotLeader) {
		return err
	}
	lead := grp.LeaderID()
	if lead == "" || lead == g.id {
		return ErrNotLeader
	}
	return g.forward(lead, data)
}

// Barrier 等本节点把已提交的日志应用完。
func (g *TLSGroup) Barrier(timeout time.Duration) error {
	grp := g.g()
	if grp == nil {
		return ErrNotLeader
	}
	return grp.Barrier(timeout)
}

// IsLeader 报告本节点此刻是否是 leader。
func (g *TLSGroup) IsLeader() bool {
	grp := g.g()
	return grp != nil && grp.IsLeader()
}

// LeaderID 返回当前 leader,未知时为空。
func (g *TLSGroup) LeaderID() NodeID {
	grp := g.g()
	if grp == nil {
		return ""
	}
	return grp.LeaderID()
}

// AppliedIndex 本节点已应用到第几条日志。
func (g *TLSGroup) AppliedIndex() uint64 {
	grp := g.g()
	if grp == nil {
		return 0
	}
	return grp.AppliedIndex()
}

// Snapshot 立刻做一次快照，供运维与备份使用。
func (g *TLSGroup) Snapshot() error {
	grp := g.g()
	if grp == nil {
		return ErrNotLeader
	}
	return grp.Snapshot()
}

// Drain 标记排空。之后 Propose 返回 ErrDraining。
func (g *TLSGroup) Drain() { g.draining.Store(true) }

// Close 停掉复制组与监听。
func (g *TLSGroup) Close() error {
	var err error
	g.closeOnce.Do(func() {
		if grp := g.g(); grp != nil {
			err = grp.Close()
		}
		if dt := g.dragon.Load(); dt != nil {
			_ = dt.Close()
		}
		g.pool.closeAll()
		if g.ln != nil {
			if cerr := g.ln.Close(); err == nil {
				err = cerr
			}
		}
	})
	return err
}

// serve 接受连接并按第一个字节分流。
func (g *TLSGroup) serve() {
	for {
		c, err := g.ln.Accept()
		if err != nil {
			return
		}
		go g.intake(c)
	}
}

// intake 校验对端身份，读一个字节，把连接交给对应的协议。
func (g *TLSGroup) intake(c net.Conn) {
	peer, err := handshakePeer(c)
	if err != nil {
		_ = c.Close()
		return
	}
	var mark [1]byte
	if _, err := io.ReadFull(c, mark[:]); err != nil {
		_ = c.Close()
		return
	}
	switch mark[0] {
	case markRaft:
		// 交给库之后这条连接的生命周期归它管，这里不能 Close。
		if dt := g.dragon.Load(); dt != nil {
			dt.Incoming(c)
			return
		}
		// 传输层还没登记：复制组尚未装配完成。关掉让对端重试，而不是
		// 把连接挂着——挂着的那条在对端看来是"连上了但不回话"。
		_ = c.Close()
	case markOwn:
		// 数一下接了多少条本项目协议的连接。
		//
		// 唯一的读者是 peerpool_test.go 里那条「连接要复用」的回归用例：
		// 握手次数只有服务端数得到，而「每条消息一次握手」这个缺陷
		// （CLU-038）在功能上完全看不出来——它只在吞吐上显形，而那正是
		// 现有三套验收都不看的东西。
		g.ownAccepted.Add(1)
		defer func() { _ = c.Close() }()
		g.handleOwn(c, peer)
	default:
		_ = c.Close()
	}
}

// handshakePeer 完成握手并返回对端证书里的 CommonName。
func handshakePeer(c net.Conn) (string, error) {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return "", nil
	}
	if err := tc.Handshake(); err != nil {
		return "", err
	}
	st := tc.ConnectionState()
	if len(st.PeerCertificates) == 0 {
		return "", errors.New("cluster: 对端没有证书")
	}
	return st.PeerCertificates[0].Subject.CommonName, nil
}

// handleOwn 处理一条连接上的请求，一问一答，直到对端关闭或空闲超时。
//
// 一条连接一个请求是接连接池之前的样子（CLU-038）。编解码器建在循环之外
// 是必须的，不是省事：gob 的类型定义每条流只发一次，每个请求新建一对的话
// 第二个请求就会解出「重复的类型定义」。
func (g *TLSGroup) handleOwn(c net.Conn, peer string) {
	dec := gob.NewDecoder(c)
	enc := gob.NewEncoder(c)
	for {
		// 空闲超时：对端进程消失时这条连接不会有任何事件，不设上限的话
		// 一个协程与一条连接就永久留在这里。上限明显大于客户端那侧的
		// 淘汰时限，见 peerServerIdleTimeout 的说明。
		//
		// contract:allow-time-now 空闲时限与写超时，契约列出的用途。
		if err := c.SetDeadline(time.Now().Add(peerServerIdleTimeout)); err != nil {
			return
		}
		var req wireMsg
		if err := dec.Decode(&req); err != nil {
			return // 对端正常关闭（io.EOF）、空闲超时，或者流坏了
		}
		// 声称的身份必须与证书一致：否则一个拿着合法证书的节点可以冒充
		// 另一个节点发消息（mesh 的驱逐通知就是以 From 为准的）。
		if peer != "" && peer != req.From {
			_ = enc.Encode(wireResp{Err: "node id"})
			return
		}
		resp := g.dispatch(req)
		// contract:allow-time-now 应答的写超时，同上。
		if err := c.SetDeadline(time.Now().Add(g.reqTO)); err != nil {
			return
		}
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

func (g *TLSGroup) dispatch(req wireMsg) wireResp {
	switch req.Kind {
	case wireMesh:
		if req.Mesh == nil {
			return wireResp{Err: "empty mesh payload"}
		}
		return g.deliverMesh(NodeID(req.From), *req.Mesh)
	case wireQueryKind:
		if req.Query == nil {
			return wireResp{Err: "empty query"}
		}
		return g.runQuery(*req.Query)
	case wirePropose:
		if g.draining.Load() {
			return wireResp{Err: ErrDraining.Error()}
		}
		// 不再往下转：收到转发的节点若也不是 leader，回 ErrNotLeader。
		grp := g.g()
		if grp == nil {
			return wireResp{Err: ErrNotLeader.Error()}
		}
		if err := grp.Propose(append([]byte(nil), req.Data...)); err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true}
	case wireShardPropose:
		if g.draining.Load() {
			return wireResp{Err: ErrDraining.Error()}
		}
		shards := g.shardGroup(req.Shard)
		if shards == nil {
			return wireResp{Err: ErrNotLeader.Error()}
		}
		// 只允许当前 leader 在本地提交：如果路由期间发生了
		// leader 转移，返错给原调用方，绝不再转发一跳。
		value, err := shards.proposeLocalLeader(req.Shard, append([]byte(nil), req.Data...))
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true, Value: value}
	case wireOwnershipKind:
		if req.Ownership == nil {
			return wireResp{Err: "empty ownership payload"}
		}
		g.ownershipMu.RLock()
		ownership := g.ownership
		g.ownershipMu.RUnlock()
		if ownership == nil {
			return wireResp{Err: ErrNotLeader.Error()}
		}
		return ownership.dispatchWire(*req.Ownership)
	case wireSessionReadKind:
		if req.SessionRead == nil {
			return wireResp{Err: "empty session read"}
		}
		g.shardMu.RLock()
		shards := g.shards
		g.shardMu.RUnlock()
		if shards == nil {
			return wireResp{Err: ErrNotMember.Error()}
		}
		if len(req.SessionRead.ClientIDs) > 0 {
			if len(req.SessionRead.ClientIDs) > sessionReadBatchMax {
				return wireResp{Err: "session read batch too large"}
			}
			results := make([]wireSessionReadResult, len(req.SessionRead.ClientIDs))
			for i, clientID := range req.SessionRead.ClientIDs {
				if clientID == "" {
					results[i].Err = "empty session client id"
					continue
				}
				data, found, err := shards.readLocalSession(req.SessionRead.Shard, clientID)
				if err != nil {
					results[i].Err = err.Error()
					continue
				}
				results[i].Data, results[i].Found = data, found
			}
			return wireResp{OK: true, SessionReadResults: results}
		}
		if req.SessionRead.ClientID == "" {
			return wireResp{Err: "empty session read"}
		}
		data, found, err := shards.readLocalSession(req.SessionRead.Shard, req.SessionRead.ClientID)
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true, Found: found, Data: data}
	case wireShardStatusKind:
		shards := g.shardGroup(req.Shard)
		if shards == nil {
			return wireResp{Err: ErrNotMember.Error()}
		}
		leader, ready, err := shards.localShardStatus(req.Shard)
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true, Found: ready, Value: leader}
	case wireShardBarrierKind:
		shards := g.shardGroup(req.Shard)
		if shards == nil {
			return wireResp{Err: ErrNotMember.Error()}
		}
		if err := shards.barrierLocal(req.Shard, g.reqTO); err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true}
	case wireShadowWriteKind, wireShadowReadKind:
		g.shadowMu.RLock()
		fn := g.shadowWrite
		if req.Kind == wireShadowReadKind {
			fn = g.shadowRead
		}
		g.shadowMu.RUnlock()
		if fn == nil {
			return wireResp{Err: "shadow handler unavailable"}
		}
		data, err := fn(append([]byte(nil), req.Data...))
		if err != nil {
			return wireResp{Err: err.Error()}
		}
		return wireResp{OK: true, Data: data}
	default:
		return wireResp{Err: "unknown"}
	}
}

func (g *TLSGroup) shardGroup(shard uint64) *ShardGroup {
	g.shardMu.RLock()
	s := g.shardGroups[shard]
	g.shardMu.RUnlock()
	return s
}

func (g *TLSGroup) registerShardGroup(s *ShardGroup) error {
	g.shardMu.Lock()
	defer g.shardMu.Unlock()
	if g.shardGroups == nil {
		g.shardGroups = make(map[uint64]*ShardGroup)
	}
	for _, shard := range s.ShardIDs() {
		if old := g.shardGroups[shard]; old != nil && old != s {
			return fmt.Errorf("cluster: shard %d 已属于另一分片组", shard)
		}
	}
	for _, shard := range s.ShardIDs() {
		g.shardGroups[shard] = s
	}
	return nil
}

func (g *TLSGroup) ensureShardRangeAvailable(base, count uint64) error {
	g.shardMu.RLock()
	defer g.shardMu.RUnlock()
	for shard := base; shard < base+count; shard++ {
		if g.shardGroups[shard] != nil {
			return fmt.Errorf("cluster: shard %d 已属于另一分片组", shard)
		}
	}
	return nil
}

func (g *TLSGroup) unregisterShardGroup(s *ShardGroup) {
	g.shardMu.Lock()
	for _, shard := range s.ShardIDs() {
		if g.shardGroups[shard] == s {
			delete(g.shardGroups, shard)
		}
	}
	if g.shards == s {
		g.shards = nil
	}
	g.shardMu.Unlock()
}

// forward 把一条命令转发给 leader。
func (g *TLSGroup) forward(to NodeID, data []byte) error {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wirePropose, From: string(g.id), Data: data,
	})
	if !ok {
		return ErrNoQuorum
	}
	return errFromWire(resp)
}

// SetShadowHandlers 注册上层影子 RPC。载荷由 server/shadow 自己编码，
// cluster 只负责认证连接、路由以及读写不同的重试语义。
func (g *TLSGroup) SetShadowHandlers(write, read func([]byte) ([]byte, error)) {
	g.shadowMu.Lock()
	g.shadowWrite, g.shadowRead = write, read
	g.shadowMu.Unlock()
}

func (g *TLSGroup) ForwardShadowWrite(to NodeID, data []byte) ([]byte, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireShadowWriteKind, From: string(g.id), Data: data,
	})
	if !ok {
		return nil, ErrNoQuorum
	}
	if err := errFromWire(resp); err != nil {
		return nil, err
	}
	return append([]byte(nil), resp.Data...), nil
}

func (g *TLSGroup) ForwardShadowRead(to NodeID, data []byte) ([]byte, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireShadowReadKind, From: string(g.id), Data: data,
	})
	if !ok {
		return nil, ErrNoQuorum
	}
	if err := errFromWire(resp); err != nil {
		return nil, err
	}
	return append([]byte(nil), resp.Data...), nil
}

// forwardShard 把已经在应用层合并好的会话提案直接送到
// 分片 leader。它不重试：连接断开时无法区分「未送达」与
// 「已提交但应答丢失」，自动重发会破坏写入语义。
func (g *TLSGroup) forwardShard(to NodeID, shard uint64, data []byte) (uint64, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireShardPropose, From: string(g.id), Shard: shard, Data: data,
	})
	if !ok {
		return 0, ErrNoQuorum
	}
	if err := errFromWire(resp); err != nil {
		return 0, err
	}
	return resp.Value, nil
}

func (g *TLSGroup) forwardOwnership(to NodeID, own wireOwnership) (wireResp, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireOwnershipKind, From: string(g.id), Ownership: &own,
	})
	if !ok {
		return wireResp{}, ErrNoQuorum
	}
	if err := errFromWire(resp); err != nil {
		return wireResp{}, err
	}
	return resp, nil
}

func (g *TLSGroup) forwardOwnershipClaimBatch(to NodeID, claims []wireOwnershipClaimItem) ([]wireOwnershipResult, error) {
	resp, err := g.forwardOwnership(to, wireOwnership{Op: wireOwnershipClaimBatch, Claims: claims})
	if err != nil {
		return nil, err
	}
	if len(resp.OwnershipResults) != len(claims) {
		return nil, errors.New("cluster: 所有权批量应答数量不符")
	}
	return resp.OwnershipResults, nil
}

func (g *TLSGroup) forwardOwnershipRelease(to NodeID, clientID string, e Epoch) error {
	_, err := g.forwardOwnership(to, wireOwnership{
		Op: wireOwnershipRelease, ClientID: clientID, Epoch: uint64(e),
	})
	return err
}

func (g *TLSGroup) forwardOwnershipLookup(to NodeID, clientID string) (NodeID, Epoch, bool, error) {
	resp, err := g.forwardOwnership(to, wireOwnership{Op: wireOwnershipLookup, ClientID: clientID})
	if err != nil {
		return "", ZeroEpoch, false, err
	}
	return NodeID(resp.Owner), Epoch(resp.Value), resp.Found, nil
}

func (g *TLSGroup) forwardOwnershipEviction(to NodeID, ev Eviction) error {
	_, err := g.forwardOwnership(to, wireOwnership{Op: wireOwnershipEvict, Eviction: &ev})
	return err
}

// addrOf 查一个节点的地址。
//
// 先查 raft 的成员列表：新加入的节点不出现在别人的配置文件里，
// 它的地址是随成员变更复制过来的（CLU-090）。
func (g *TLSGroup) addrOf(id NodeID) string {
	for _, m := range g.members() {
		if m.ID == id {
			return m.Addr
		}
	}
	return g.peers[id]
}

// members 是当前成员列表。装配未完成时回退到配置里的初始成员——
// 那正是第一次拨号需要的那份地址表。
func (g *TLSGroup) members() []Member {
	if grp := g.g(); grp != nil {
		if m := grp.Members(); len(m) > 0 {
			return m
		}
	}
	out := make([]Member, 0, len(g.peers))
	for id, addr := range g.peers {
		out = append(out, Member{ID: id, Addr: addr})
	}
	return out
}

// peerIDs 是除自己之外的成员。
func (g *TLSGroup) peerIDs() []NodeID {
	members := g.members()
	out := make([]NodeID, 0, len(members))
	for _, m := range members {
		if m.ID != g.id {
			out = append(out, m.ID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// dialPeer 连一个节点并校验它的身份。
func (g *TLSGroup) dialPeer(addr string, expect NodeID) (net.Conn, error) {
	raw, err := (&net.Dialer{Timeout: g.dialTO}).Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cfg := g.tlsCfg.Clone()
	// 节点之间按 CommonName 认身份，不按主机名：复制地址可能是 IP，
	// 也可能在 k8s 里随 Pod 重建而变，而节点 ID 不变（CLU-060、CLU-069）。
	//
	// 要真的不校验主机名，只能走 InsecureSkipVerify；而这个开关一开，
	// Go 既不建链也不填 verifiedChains，**链的校验就得自己做**。
	// 少做这一步的后果正是 CLU-068 要防的那件事：任何一张自签证书都能
	// 冒充一个节点，等于把元数据写入权发给任何人。所以下面先用 RootCAs
	// 建链，再比 CommonName，两道都过才算。
	//
	// 原先的写法是 `ServerName = "localhost"` 加一个只比 CommonName 的
	// 回调。那并没有关掉主机名校验——InsecureSkipVerify 是 false，Go 的
	// 默认校验照样按 ServerName 去比 SAN，于是**每张节点证书的 SAN 里都
	// 必须有 localhost**，而这件事没有写在任何需求里。compose 的
	// gen-certs.sh 恰好那么签，所以 `make cluster-test` 一直是绿的；
	// k8s 的复制证书只含 Pod 的 DNS 名，于是 requestVote 的 TLS 握手
	// 全部失败，三个节点一致地报「选举未完成或对端不可达」——而那条
	// 错误指向网络，不指向证书（CLU-102）。
	roots := cfg.RootCAs
	cfg.InsecureSkipVerify = true
	cfg.ServerName = ""
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("cluster: 没有对端证书")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			c, perr := x509.ParseCertificate(raw)
			if perr != nil {
				return fmt.Errorf("cluster: 对端证书解不开: %w", perr)
			}
			certs = append(certs, c)
		}
		inter := x509.NewCertPool()
		for _, c := range certs[1:] {
			inter.AddCert(c)
		}
		// roots 为 nil 时 Verify 会回落到系统根，而复制链是自签的——
		// 那等于把它拒掉，是安全的方向，但错误会含混。显式判掉。
		if roots == nil {
			return errors.New("cluster: 没有复制 CA，无法校验对端证书")
		}
		if _, verr := certs[0].Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: inter,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); verr != nil {
			return fmt.Errorf("cluster: 对端证书不是复制 CA 签的: %w", verr)
		}
		cn := certs[0].Subject.CommonName
		if expect == "" {
			// 还不知道这个地址属于谁：链已经验过，把它报的名字记下来。
			// 见 TLSGroup.learned 的说明。
			if cn == "" {
				return errors.New("cluster: 对端证书没有 CommonName")
			}
			g.learnedMu.Lock()
			if g.learned == nil {
				g.learned = map[string]NodeID{}
			}
			g.learned[addr] = NodeID(cn)
			g.learnedMu.Unlock()
			return nil
		}
		if cn != string(expect) {
			return fmt.Errorf("cluster: 对端身份是 %q，期望 %q", cn, expect)
		}
		return nil
	}
	conn := tls.Client(raw, cfg)
	if err := conn.Handshake(); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

// dialRaftConn 是共识库传输层的拨号出口。
//
// 它只是 dialRaft 的一层薄包装，存在的意义是让传输层不认识 TLSGroup
// （见 raftDialer）。拨号本身那套——按 CommonName 认身份、自己建链、
// 写分流标记——只有一份：CLU-102 那一轮的教训正是"三处证书装置各自演进"。
func (g *TLSGroup) dialRaftConn(target string) (net.Conn, error) {
	return g.dialRaft(target)
}

// DragonTransportFactory 返回接在本监听上的 dragonboat 传输层工厂。
//
// 调用它即把本组的 markRaft 分流改接到 dragonboat（见 intake）：工厂被
// dragonboat 在 NewNodeHost 内部调用，建好的传输层经 bound 回传并登记
// 在这里——否则入站连接没有去处，而症状是"选不出 leader"。
func (g *TLSGroup) DragonTransportFactory() dbconfig.TransportFactory {
	return &dragonTransportFactory{
		dial:  g.dialRaftConn,
		bound: func(t *dragonTransport) { g.dragon.Store(t) },
	}
}

// dialRaft 是 raft 传输用的拨号：连上之后写一个 'R'。
func (g *TLSGroup) dialRaft(addr string) (net.Conn, error) {
	// 对端身份按地址反查节点 ID：raft 只告诉我们地址。
	// expect 为空表示还不知道这个地址属于谁——新加入的节点要回话给
	// leader 时就是这种。那时按"链校验通过就认下它报的名字"处理，
	// 而不是直接失败：直接失败的后果是它永远入不了组，而错误说的是
	// "地址不属于任何已知成员"，看起来像配置写漏了。
	expect := g.idOf(addr)
	conn, err := g.dialPeer(addr, expect)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte{markRaft}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// idOf 按地址反查节点 ID。
func (g *TLSGroup) idOf(addr string) NodeID {
	for _, m := range g.members() {
		if m.Addr == addr {
			return m.ID
		}
	}
	for id, a := range g.peers {
		if a == addr {
			return id
		}
	}
	g.learnedMu.RLock()
	id := g.learned[addr]
	g.learnedMu.RUnlock()
	return id
}

// roundTrip 发一次本项目协议的请求并等应答，连接取自池（CLU-038）。
//
// 失败处理分两类，分界是这条请求**重发一次安不安全**：
//
//   - mesh 帧、扇出查询与会话读取可以重发。消息面本就是至少一次语义
//     （CLU-077 那条「收到的帧不再转发」正是按这个前提写的），后两者是
//     只读的。于是复用的连接坏掉时，换一条新连接再试一次——对端在空闲期
//     关掉连接是正常事件，不该让一次安全读取因此失败。
//   - wirePropose、wireShardPropose 与 wireOwnershipKind **不重试**。一次失败的
//     exchange 有两种可能：请求没到，
//     或者到了、应用了，而应答丢在路上。自动重发第二种就是把一条命令
//     应用两遍，而命令不带 ID，幂等窗口认不出它们是同一件事。这里返回
//     失败，上层照旧得到 ErrNoQuorum → 503 → 由调用方决定重试，与接池
//     之前的行为完全一样。
//
// 第二条是「复用一条已被对端关掉的连接」这件事必须罕见的理由，而那由
// peerIdleTimeout 明显小于 peerServerIdleTimeout 来保证。
func (g *TLSGroup) roundTrip(to NodeID, req wireMsg) (wireResp, bool) {
	addr := g.addrOf(to)
	if addr == "" {
		return wireResp{}, false
	}
	if pc := g.pool.take(to, addr); pc != nil {
		resp, err := pc.exchange(&req, g.reqTO)
		if err == nil {
			g.pool.put(to, pc)
			return resp, true
		}
		pc.close()
		if !retryableKind(req.Kind) {
			return wireResp{}, false
		}
	}
	pc, err := g.dialOwn(to, addr)
	if err != nil {
		return wireResp{}, false
	}
	resp, err := pc.exchange(&req, g.reqTO)
	if err != nil {
		pc.close()
		return wireResp{}, false
	}
	g.pool.put(to, pc)
	return resp, true
}

// retryableKind 报告这类请求能不能在新连接上重发一次。见 roundTrip。
func retryableKind(kind uint8) bool {
	return kind == wireMesh || kind == wireQueryKind || kind == wireSessionReadKind ||
		kind == wireShardStatusKind || kind == wireShardBarrierKind || kind == wireShadowReadKind
}

func (g *TLSGroup) forwardShardStatus(to NodeID, shard uint64) (uint64, bool, error) {
	resp, ok := g.roundTrip(to, wireMsg{Kind: wireShardStatusKind, From: string(g.id), Shard: shard})
	if !ok {
		return 0, false, ErrNoQuorum
	}
	if err := errFromWire(resp); err != nil {
		return 0, false, err
	}
	return resp.Value, resp.Found, nil
}

func (g *TLSGroup) forwardShardBarrier(to NodeID, shard uint64, timeout time.Duration) error {
	// roundTrip 使用组级 reqTO；timeout 目前只决定调用方允许的语义上限，
	// 服务端仍受同一个有界请求超时保护。
	_ = timeout
	resp, ok := g.roundTrip(to, wireMsg{Kind: wireShardBarrierKind, From: string(g.id), Shard: shard})
	if !ok {
		return ErrNoQuorum
	}
	return errFromWire(resp)
}

func (g *TLSGroup) forwardSessionReadBatch(to NodeID, shard uint64,
	clientIDs []string,
) ([]sessionReadResult, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireSessionReadKind, From: string(g.id),
		SessionRead: &wireSessionRead{Shard: shard, ClientIDs: clientIDs},
	})
	if !ok {
		return nil, fmt.Errorf("cluster: 无法从 %s 批量读取会话", to)
	}
	if err := errFromWire(resp); err != nil {
		return nil, err
	}
	if len(resp.SessionReadResults) != len(clientIDs) {
		return nil, errors.New("cluster: 会话批量读取应答数量不符")
	}
	out := make([]sessionReadResult, len(resp.SessionReadResults))
	for i := range resp.SessionReadResults {
		item := resp.SessionReadResults[i]
		out[i] = sessionReadResult{
			data: append([]byte(nil), item.Data...), found: item.Found,
		}
		if item.Err != "" {
			out[i].err = errFromWire(wireResp{Err: item.Err})
		}
	}
	return out, nil
}

// dialOwn 拨一条本项目协议的连接。
//
// 编解码器在这里建好并跟着连接走，不跟着请求走——理由见 peerpool.go 开头
// 那段的第二条（gob 的类型定义一条流只发一次）。
func (g *TLSGroup) dialOwn(to NodeID, addr string) (*peerConn, error) {
	conn, err := g.dialPeer(addr, to)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte{markOwn}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &peerConn{
		c: conn, enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn), addr: addr,
	}, nil
}

func errFromWire(resp wireResp) error {
	switch resp.Err {
	case "":
		if resp.OK {
			return nil
		}
		return ErrNoQuorum
	case ErrNotLeader.Error():
		return ErrNotLeader
	case ErrNoQuorum.Error():
		return ErrNoQuorum
	case ErrDraining.Error():
		return ErrDraining
	case ErrStaleEpoch.Error():
		return ErrStaleEpoch
	case ErrClosed.Error():
		return ErrClosed
	case ErrNotMember.Error():
		return ErrNotMember
	default:
		return errors.New(resp.Err)
	}
}
