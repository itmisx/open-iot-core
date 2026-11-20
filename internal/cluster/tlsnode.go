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
	Apply func(index uint64, data []byte) error
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
}

// TLSGroup 是一个通过 TLS 与其它进程复制日志的节点。
type TLSGroup struct {
	id NodeID
	// group 是原子的：hraft.NewRaft 在返回**之前**就起了协程并开始拨号,
	// 而拨号要反查成员(idOf)。装配协程这边 g.group = group 的赋值与
	// 那些协程的读取是并发的——-race 当场抓到过这一条。
	group    atomic.Pointer[groupRef]
	ln       net.Listener
	tlsCfg   *tls.Config
	peers    map[NodeID]string
	dialTO   time.Duration
	applyTO  time.Duration
	draining atomic.Bool
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

	closeOnce sync.Once
}

// wireMsg 是本项目自己那条协议的请求。
//
// raft 自己的 RPC(投票、追加、快照)**不在这里**:它们走库的
// NetworkTransport。这里只剩三类本项目的消息。
type wireMsg struct {
	Kind uint8
	From string
	// Data 是转发给 leader 的那条命令。
	Data []byte
	// Mesh 承载消息面的负载(消息帧、兴趣通告、设备归属),见 meshnode.go。
	Mesh *MeshPayload
	// Query 是 Admin 面的扇出查询(契约 12),见 query.go。
	Query *wireQuery
}

type wireResp struct {
	OK  bool
	Err string
	// Rows 是扇出查询的结果行，已经编码成字节。
	//
	// 不用 []any：gob 要求注册具体类型，而每种查询的行类型不同——
	// 让传输层认识每一种，等于把它和每个查询绑在一起。
	Rows [][]byte
	// NextCursor 是本节点的续读位置，空表示读完了。
	NextCursor string
}

const (
	wirePropose uint8 = iota + 1
	wireMesh
	// wireQueryKind 与类型 wireQuery 区分开：同名会让「消息类型」与
	// 「消息载荷」撞在一个标识符上。
	wireQueryKind
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
		id: opt.ID, ln: ln, tlsCfg: opt.TLS, peers: peers,
		dialTO: opt.DialTimeout, applyTO: opt.ApplyTimeout,
	}
	if g.dialTO <= 0 {
		g.dialTO = 2 * time.Second
	}
	if g.applyTO <= 0 {
		g.applyTO = 10 * time.Second
	}

	group, derr := newDragonGroup(DragonOptions{
		ID: opt.ID, Dir: opt.Dir,
		Replicas: opt.Replicas, Peers: peers,
		Apply: opt.Apply, AppliedIndex: opt.AppliedIndex,
		SnapshotTo: opt.SnapshotTo, RestoreFrom: opt.RestoreFrom,
		Transport:    g.DragonTransportFactory(),
		Bootstrap:    opt.Bootstrap,
		ApplyTimeout: opt.ApplyTimeout,
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

// handleOwn 处理一次本项目协议的请求。一条连接一个请求。
func (g *TLSGroup) handleOwn(c net.Conn, peer string) {
	dec := gob.NewDecoder(c)
	enc := gob.NewEncoder(c)
	var req wireMsg
	if err := dec.Decode(&req); err != nil {
		return
	}
	// 声称的身份必须与证书一致：否则一个拿着合法证书的节点可以冒充
	// 另一个节点发消息（mesh 的驱逐通知就是以 From 为准的）。
	if peer != "" && peer != req.From {
		_ = enc.Encode(wireResp{Err: "node id"})
		return
	}
	_ = enc.Encode(g.dispatch(req))
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
	default:
		return wireResp{Err: "unknown"}
	}
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

// roundTrip 发一次本项目协议的请求并等应答。
func (g *TLSGroup) roundTrip(to NodeID, req wireMsg) (wireResp, bool) {
	addr := g.addrOf(to)
	if addr == "" {
		return wireResp{}, false
	}
	conn, err := g.dialPeer(addr, to)
	if err != nil {
		return wireResp{}, false
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte{markOwn}); err != nil {
		return wireResp{}, false
	}
	if err := gob.NewEncoder(conn).Encode(&req); err != nil {
		return wireResp{}, false
	}
	var resp wireResp
	if err := gob.NewDecoder(conn).Decode(&resp); err != nil {
		return wireResp{}, false
	}
	return resp, true
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
	default:
		return errors.New(resp.Err)
	}
}
