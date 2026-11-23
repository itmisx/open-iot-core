package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"
	"sort"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/broker"
	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/mqtt/reason"
	"github.com/itmisx/open-iot-core/internal/node"
	"github.com/itmisx/open-iot-core/internal/router"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/store"
)

// 元数据层的装配（契约 05、CLU-067）。
//
// 单机与集群的区别只有一层：命令是直接追加到一副本组，还是先过共识。
// 命令的校验、应用、冷层写入、变更发布都是同一份代码——按 ARCH-057，
// 单机是节点数为一的集群，不是集群之前的版本。

// metaStore 是 server 用到的元数据层。
//
// 比 meta.Store 多一个 MarkConfirmed：视图陈旧度是节点自己的事
// （CLU-023 的 fail-closed 判定读它），而「什么时候算追平」在单机与
// 集群下由不同的人决定。
type metaStore interface {
	meta.Store
	MarkConfirmed(nowUnixMs int64)
	// Hydrate 用冷层的内容重建热视图，不写日志（CLU-098）。
	// 启动时由 hydrateHotAtBoot 调用，见那里的说明（P2）。
	Hydrate(rev meta.Revision, emit func(apply func(meta.Command) error) error) error
}

// defaultPumpInterval 是追平已提交日志的默认间隔。
//
// 200 毫秒是个折中：跟随者上的吊销要在 max_view_staleness 之内生效
// （CLU-049 要求全集群在那个窗口内断开在用连接），而每次追平都是一次
// 本地读，没有网络开销。
const defaultPumpInterval = 200 * time.Millisecond

// buildMetaStore 按节点模式装出元数据层。
func (s *Server) buildMetaStore(kv *store.SwapKV, env authz.Env) (metaStore, error) {
	// 读回上次落盘的 applied index（P1）。
	//
	// **只在集群模式下做。** 单机的 revision 来自 SingleReplica 的日志
	// 序号，而那个日志每次重启都从 0 起；把盘上的 revision 喂给它，
	// 新命令会拿到比视图当前值更小的序号——视图 revision 倒退，而
	// 就绪度与变更通知都按它判断。单机本来也没有"跳过已应用日志"这件事
	// 要做，它的热视图由 registry.Restore 经写入路径重建。
	//
	// 读不到当 0：首次启动、以及从本条改动之前的数据目录升上来的节点
	// 都是这种。那时日志会被整条重放，与改动之前的行为相同。
	var initialRev meta.Revision
	clustered := s.cfg.Node.Mode != config.ModeStandalone
	if clustered {
		if raw, err := kv.Get(store.MetaRevisionKey()); err == nil {
			initialRev = meta.DecodeRevision(raw)
		}
	}
	s.initialRev = initialRev
	revKey := []byte(nil)
	if clustered {
		revKey = store.MetaRevisionKey()
	}
	local := meta.NewLocalStore(meta.LocalOptions{
		Now:  s.now,
		Cold: kv,
		ApplyRecords: func(records []meta.RecordOp) error {
			ops := make([]store.Op, len(records))
			for i, record := range records {
				kind := store.OpSet
				if record.Delete {
					kind = store.OpDelete
				}
				ops[i] = store.Op{Kind: kind, Key: record.Key, Value: record.Value}
			}
			return kv.Batch(ops, true)
		},
		InTransaction: func(fn func(meta.Transaction) error) error {
			return kv.InTx(true, func(tx store.Tx) error { return fn(tx) })
		},
		RevisionKey:     revKey,
		InitialRevision: initialRev,
		Compile: func(name string, doc []byte) (any, error) {
			return authz.CompileJSON(name, doc, env)
		},
		// 影子命令的 apply：单机下不会有带影子载荷的命令（影子直接写
		// 本地存储），集群下每个节点都要执行它。
		ApplyShadow:   s.applyShadowCommand,
		ApplyShadowTx: s.applyShadowCommandTx,
	})
	if s.cfg.Node.Mode == config.ModeStandalone {
		return local, nil
	}

	rep := &s.cfg.Cluster.Replication
	tlsCfg, err := loadReplicationTLS(rep)
	if err != nil {
		_ = local.Close()
		return nil, err
	}
	peers := make(map[cluster.NodeID]string, len(rep.Peers))
	for id, addr := range rep.Peers {
		peers[cluster.NodeID(id)] = addr
	}
	// 日志与冷层分开放：复制日志是复制层的东西，混进 meta/ 会让
	// 「备份冷层」和「备份日志」变成同一次操作，而它们的恢复语义不同。
	dir, err := s.dd.EnsureDir(node.DirRaft)
	if err != nil {
		_ = local.Close()
		return nil, err
	}
	// 复制日志由共识库自己管（它的 LogDB 落在 dir 下）。本项目不再实现
	// LogStore——dragonboat 的 LogDB 接口是批量多组的，用单写者 B 树去
	// 实现它等于把多组批量提交的收益原地交回去（LANG-023 修订）。

	// 复制组与注册表互相需要对方，顺序由这个后置绑定解开：
	//
	//   状态机的"装快照"要调 registry.HydrateHot 扫冷层重建热视图，
	//   而 registry 要先有元数据层才能建。raft 在 NewRaft 里就会把
	//   最新的快照装进状态机，那时注册表还不存在。
	//
	// 于是先用 lateGroup 占位把 ReplicatedStore 建出来，注册表建好之后
	// 再把真正的组塞进去（见 startReplicationGroup）。
	late := &lateGroup{}
	rs, err := meta.NewReplicatedStore(meta.ReplicatedOptions{
		Group: late, Node: s.mf.NodeID, Local: local,
		// 跟随者提交之后要等 leader 把那条日志推回来才看得见
		// （一次网络往返，不是错误）。
		Settle: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(replicationRetryInterval):
				return nil
			}
		},
		ColdSnapshot: func(w io.Writer) error {
			_, serr := kv.SnapshotFiltered(w, s.preserveShardKey)
			return serr
		},
		// 冷层整库替换之后必须重载内存里的保留消息（CLU-107）：retained
		// 与冷层在同一个库里，而装载只在启动时跑一次——不重载的话，磁盘
		// 已经是 leader 的那一份，内存还是自己的，下次重启才换过去。
		ColdRestore: func(r io.Reader) error {
			if err := kv.RestoreFiltered(r, s.preserveShardKey); err != nil {
				return err
			}
			return s.reloadRetained()
		},
		Hydrate: func(apply func(meta.Command) error) error {
			if s.reg2 == nil {
				return errors.New("server: 注册表还没建好，无法重建热视图")
			}
			return s.reg2.HydrateHot(apply)
		},
	})
	if err != nil {
		_ = local.Close()
		return nil, err
	}
	s.replLate = late
	s.replTLS = tlsCfg
	s.replPeers = peers
	s.replDir = dir
	s.replStore = rs
	return rs, nil
}

// preserveShardKey 标出不属于 metadata 状态机的共享冷库键。metadata 快照
// 恢复只能替换自己的 keyspace；session/shadow 分片各由自己的 Raft 快照负责。
func (s *Server) preserveShardKey(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	switch store.Prefix(key[0]) {
	case store.PrefixSession, store.PrefixQueued:
		return true
	case store.PrefixShadow:
		return s.cfg.Cluster.Replication.ShadowShards != 0
	case store.PrefixSystem:
		body := key[1:]
		if bytes.HasPrefix(body, []byte("session-applied-")) {
			return true
		}
		return s.cfg.Cluster.Replication.ShadowShards != 0 &&
			(bytes.HasPrefix(body, []byte("shadow-applied-")) ||
				bytes.HasPrefix(body, []byte("shadow-pruned-v1-")))
	default:
		return false
	}
}

// startReplicationGroup 建起真正的复制组。
//
// 必须在注册表建好之后调：NewRaft 会把最新的快照装进状态机，而那一步
// 要扫冷层重建热视图（registry.HydrateHot）。
func (s *Server) startReplicationGroup() error {
	if s.replStore == nil {
		return nil
	}
	rep := &s.cfg.Cluster.Replication
	replicas := make(map[cluster.NodeID]uint64, len(rep.Replicas))
	for id, n := range rep.Replicas {
		replicas[cluster.NodeID(id)] = n
	}
	group, err := cluster.ListenTLS(cluster.TLSOptions{
		ID:       cluster.NodeID(s.mf.NodeID),
		Addr:     rep.Addr,
		Peers:    s.replPeers,
		Replicas: replicas,
		// on-disk 状态机在 Open 时要这个数字（CLU-109）。取视图的
		// revision：它只在 applied index 落盘成功之后才推进。
		AppliedIndex: func() uint64 {
			return uint64(s.replStore.View().Revision())
		},
		TLS:   s.replTLS,
		Dir:   s.replDir,
		Apply: s.replStore.ApplyCommitted,
		ApplyBatch: func(entries []cluster.ShardEntry) error {
			batch := make([]meta.CommittedData, len(entries))
			for i := range entries {
				batch[i] = meta.CommittedData{Index: entries[i].Index, Data: entries[i].Data}
			}
			return s.replStore.ApplyCommittedBatch(batch)
		},
		SnapshotTo:  s.replStore.SnapshotTo,
		RestoreFrom: s.replStore.RestoreFrom,
		Bootstrap:   s.cfg.Node.Mode == config.ModeBootstrap,
		Output:      raftLogWriter{log: s.log},
		// 0 表示取库那一侧的默认值（100 毫秒）。会话分片共用同一个
		// NodeHost，所以 RTT 只在这里给一次就对两组都生效。
		RTT: time.Duration(rep.RTT),
	})
	if err != nil {
		return fmt.Errorf("server: 复制组监听 %s 失败: %w", rep.Addr, err)
	}
	s.repl = group
	s.replLate.set(group)
	// 消息面复用同一条 TLS 连接（契约 07）。mesh 处理函数在这里统一分发：
	// 消息帧交给 broker 投本地订阅者，兴趣与设备归属的快照更新路由表。
	s.mesh = cluster.NewMeshTransport(group)
	if s.lateMesh != nil {
		s.lateMesh.bind(s.mesh)
	}
	// 跨节点来的帧只投本地订阅者，不再转发（DeliverRemote 的说明）。
	s.mesh.OnReceive(s.brk.DeliverRemote)
	group.SetMeshHandler(s.onMesh)
	group.SetQueryHandler(s.runLocalQuery)
	// 会话分区组建在同一个 NodeHost 上，所以必须在复制组之后
	// （见 cluster.StartSessionShards）。
	if err := s.startSessionShards(replicas); err != nil {
		return err
	}
	return nil
}

const (
	shadowShardBase uint64 = 2
)

type shadowShardEnvelope struct {
	ID    string                `json:"id,omitempty"`
	Raw   json.RawMessage       `json:"raw,omitempty"`
	Batch []shadowShardEnvelope `json:"batch,omitempty"`
}

type shadowRPCRead struct {
	Op     string `json:"op"`
	Thing  string `json:"thing"`
	Shadow string `json:"shadow,omitempty"`
}

type shadowRouteStats struct {
	remoteReads         atomic.Uint64
	remoteReadRPCs      atomic.Uint64
	remoteReadFallbacks atomic.Uint64
	remoteReadErrors    atomic.Uint64
	remoteWrites        atomic.Uint64
	remoteWriteErrors   atomic.Uint64
}

type shadowRPCResponse struct {
	Result   *shadow.Result   `json:"result,omitempty"`
	Document *shadow.Document `json:"document,omitempty"`
	Names    []string         `json:"names,omitempty"`
	Found    bool             `json:"found,omitempty"`
	Domain   *shadow.Error    `json:"domain,omitempty"`
}

func decodeShadowRPCResponse(raw []byte) (shadowRPCResponse, error) {
	var resp shadowRPCResponse
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return shadowRPCResponse{}, err
	}
	return resp, nil
}

func shadowRPCTimeout(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return 0, context.DeadlineExceeded
		}
	}
	return timeout, nil
}

func decodeShadowShardEnvelopes(raw []byte) ([]shadowShardEnvelope, error) {
	var env shadowShardEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	if len(env.Batch) > 0 {
		if env.ID != "" || len(env.Raw) != 0 || len(env.Batch) > shadowProposalBatchMax {
			return nil, errors.New("影子分片批命令格式无效")
		}
		for i := range env.Batch {
			if env.Batch[i].ID == "" || len(env.Batch[i].Raw) == 0 || len(env.Batch[i].Batch) != 0 {
				return nil, errors.New("影子分片批命令含无效子命令")
			}
		}
		return env.Batch, nil
	}
	if env.ID == "" || len(env.Raw) == 0 {
		return nil, errors.New("影子分片命令缺少 id/raw")
	}
	return []shadowShardEnvelope{env}, nil
}

const shadowProposalBatchMax = 256

func mergeShadowShardProposals(items [][]byte) ([]byte, []uint8, int, error) {
	if len(items) == 0 {
		return nil, nil, 0, errors.New("没有可合并的影子提案")
	}
	batch := make([]shadowShardEnvelope, 0, shadowProposalBatchMax)
	widths := make([]uint8, 0, len(items))
	bytesUsed, consumed := 0, 0
	for _, item := range items {
		envs, err := decodeShadowShardEnvelopes(item)
		if err != nil {
			if consumed == 0 {
				return nil, nil, 1, err
			}
			break
		}
		if len(batch)+len(envs) > shadowProposalBatchMax ||
			(consumed > 0 && bytesUsed+len(item) > 1<<20) {
			break
		}
		batch = append(batch, envs...)
		widths = append(widths, uint8(len(envs)))
		bytesUsed += len(item)
		consumed++
	}
	if consumed <= 1 {
		return append([]byte(nil), items[0]...), widths, 1, nil
	}
	raw, err := json.Marshal(shadowShardEnvelope{Batch: batch})
	return raw, widths, consumed, err
}

// startShadowShards 把新影子写入从单一 metadata 日志拆到配置数量的独立日志。
// 先保持 RF=all，使现有任意节点本地读与滚动升级语义不变；下一阶段再在
// 服务转发与远程读完成后切 RF=3，届时才减少每节点存储。
func (s *Server) startShadowShards(replicas map[cluster.NodeID]uint64) error {
	if s.shadowRepl == nil || s.shadows == nil {
		return errors.New("server: 影子分片启动前复制存储尚未装配")
	}
	count := s.cfg.Cluster.Replication.ShadowShards
	owns := func(shard uint64) func(string) bool {
		return func(thing string) bool {
			return thing != "" && cluster.ShardFor(shadowShardBase, count, thing) == shard
		}
	}
	g, err := s.repl.StartShardGroup(cluster.ShardOptions{
		Base: shadowShardBase, Count: count,
		LayoutName: "shadow-layout.json", Label: "影子",
		ReplicationFactor: int(s.cfg.Cluster.Replication.ShadowReplicationFactor),
		Replicas:          replicas, Peers: s.replPeers,
		Bootstrap: s.cfg.Node.Mode == config.ModeBootstrap,
		ApplyBatch: func(shard uint64, ents []cluster.ShardEntry) error {
			batch := make([]store.ShadowEntry, 0, len(ents))
			ids := make([]string, 0, len(ents))
			for i := range ents {
				envs, err := decodeShadowShardEnvelopes(ents[i].Data)
				if err != nil {
					return fmt.Errorf("server: 影子分片命令损坏: %w", err)
				}
				for _, env := range envs {
					ids = append(ids, env.ID)
					batch = append(batch, store.ShadowEntry{Index: ents[i].Index, Data: env.Raw})
				}
			}
			if err := s.shadows.ApplyShadowBatch(shard, batch, owns(shard)); err != nil {
				return err
			}
			// ApplyShadowBatch 返回时事务已经提交；绝不能提前唤醒请求方。
			for i := range batch {
				s.shadowRepl.DeliverCommitted(ids[i], batch[i].Result, batch[i].Err)
			}
			return nil
		},
		AppliedIndex: s.shadows.ShadowAppliedIndex,
		SnapshotTo: func(shard uint64, w io.Writer) error {
			return s.shadows.SnapshotShadowShard(shard, owns(shard), w)
		},
		RestoreFrom: func(shard uint64, r io.Reader) error {
			return s.shadows.RestoreShadowShard(shard, owns(shard), r)
		},
		MergeProposals: mergeShadowShardProposals,
		MergeMaxItems:  shadowProposalBatchMax,
		// 影子结果由ApplyBatch在事务提交后按pending ID逐条交付，不使用
		// 状态机的uint64结果位图，所以二级合批可以安全超过会话的64条上限。
		MergeIgnoresResult: true,
	})
	if err != nil {
		return fmt.Errorf("server: 启动影子分区组失败: %w", err)
	}
	s.repl.SetShadowHandlers(s.handleShadowWriteRPC, s.handleShadowReadRPC)
	if err := s.shadowSvc.UsePartitionerAndActivate(g, func() error {
		// 服务写锁将旧 metadata 路径与新分片路径隔开：
		// 裁剪事务提交后才切路由，避免旧路径又在非本地分片写回一条。
		if s.cfg.Cluster.Replication.ShadowReplicationFactor != 0 {
			deleted, already, err := s.shadows.PruneNonLocal(
				g.PlacementFingerprint(), g.HasLocalReplicaBytes)
			if err != nil {
				return fmt.Errorf("server: 裁剪非本地影子失败: %w", err)
			}
			s.log.Info("影子历史数据裁剪完成", "删除", deleted, "已执行", already)
		}
		s.shadowShards.Store(g)
		return nil
	}); err != nil {
		_ = g.Close()
		s.shadowShards.Store(nil)
		return err
	}
	rf := s.cfg.Cluster.Replication.ShadowReplicationFactor
	if rf == 0 {
		rf = uint64(len(replicas))
	}
	s.log.Info("影子分区组已启动", "分片数", g.ShardCount(), "副本因子", rf)
	return nil
}

func (s *Server) handleShadowWriteRPC(raw []byte) ([]byte, error) {
	cmd, err := shadow.DecodeCommand(raw)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.bgCtx, 10*time.Second)
	defer cancel()
	res, err := s.shadowRepl.Apply(ctx, cmd)
	resp := shadowRPCResponse{Result: res}
	if err != nil {
		var domain *shadow.Error
		if !errors.As(err, &domain) {
			return nil, err
		}
		resp.Domain = domain
	}
	return json.Marshal(resp)
}

func (s *Server) handleShadowReadRPC(raw []byte) ([]byte, error) {
	var req shadowRPCRead
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.bgCtx, 10*time.Second)
	defer cancel()
	timeout, err := shadowRPCTimeout(ctx)
	if err != nil {
		return nil, err
	}
	shards := s.shadowShards.Load()
	if shards == nil {
		return nil, errors.New("server: 影子分片未启动")
	}
	if err := shards.Barrier(req.Thing, timeout); err != nil {
		return nil, err
	}
	resp := shadowRPCResponse{}
	switch req.Op {
	case "get":
		resp.Document, resp.Found = s.shadows.Get(req.Thing, req.Shadow)
	case "list":
		resp.Names = s.shadows.List(req.Thing)
	default:
		return nil, errors.New("server: 未知影子读操作")
	}
	return json.Marshal(resp)
}

func (s *Server) applyForwardedShadow(ctx context.Context, _ cluster.NodeID,
	cmd shadow.Command,
) (*shadow.Result, error) {
	shards := s.shadowShards.Load()
	if shards == nil {
		return nil, errors.New("server: 影子分片未启动")
	}
	leader, ready := shards.LeaderNode(cmd.Thing)
	if !ready {
		return nil, cluster.ErrNotLeader
	}
	// 只有本节点就是 leader 时才走本地 Apply。旧路径只判断“有本地副本”，
	// 因而接入落在 follower 时会先把提案转给 leader，leader 已提交后还要
	// 同步等这个 follower 的本地状态机追平；共享盘或 follower 短暂停顿会把
	// 极少数 accepted 拖到客户端超时，而写其实早已成功。
	//
	// 非 leader 直接转发完整影子写并使用 leader 状态机算出的结果。读路径
	// 本来就先做 Barrier，所以不需要拿 accepted 充当本地 follower 已追平的
	// 屏障；复制与持久化语义不变，也没有重试歧义写。
	if leader == cluster.NodeID(s.mf.NodeID) {
		return s.shadowRepl.Apply(ctx, cmd)
	}
	raw, err := shadow.EncodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	response, err := s.repl.ForwardShadowWrite(leader, raw)
	s.shadowRoute.remoteWrites.Add(1)
	if err != nil {
		shards.ForgetLeader(cmd.Thing, leader)
		s.shadowRoute.remoteWriteErrors.Add(1)
		return nil, err
	}
	resp, err := decodeShadowRPCResponse(response)
	if err != nil {
		s.shadowRoute.remoteWriteErrors.Add(1)
		return nil, err
	}
	if resp.Domain != nil {
		return nil, resp.Domain
	}
	if resp.Result == nil {
		s.shadowRoute.remoteWriteErrors.Add(1)
		return nil, errors.New("server: 影子写应答缺少结果")
	}
	return resp.Result, nil
}

func (s *Server) localShadowGet(ctx context.Context, thing, name string) (*shadow.Document, bool, error) {
	shards := s.shadowShards.Load()
	if shards == nil {
		doc, ok := s.shadows.Get(thing, name)
		return doc, ok, nil
	}
	timeout, err := shadowRPCTimeout(ctx)
	if err != nil {
		return nil, false, err
	}
	if !shards.HasLocalReplica(thing) {
		resp, err := s.forwardShadowRead(ctx, thing, name, "get")
		return resp.Document, resp.Found, err
	}
	if err := shards.Barrier(thing, timeout); err != nil {
		return nil, false, err
	}
	doc, ok := s.shadows.Get(thing, name)
	return doc, ok, nil
}

func (s *Server) localShadowList(ctx context.Context, thing string) ([]string, error) {
	shards := s.shadowShards.Load()
	if shards == nil {
		return s.shadows.List(thing), nil
	}
	timeout, err := shadowRPCTimeout(ctx)
	if err != nil {
		return nil, err
	}
	if !shards.HasLocalReplica(thing) {
		resp, err := s.forwardShadowRead(ctx, thing, "", "list")
		return resp.Names, err
	}
	if err := shards.Barrier(thing, timeout); err != nil {
		return nil, err
	}
	return s.shadows.List(thing), nil
}

func (s *Server) forwardShadowRead(ctx context.Context, thing, name, op string) (shadowRPCResponse, error) {
	shards := s.shadowShards.Load()
	if shards == nil {
		return shadowRPCResponse{}, errors.New("server: 影子分片未启动")
	}
	if shards.HasLocalReplica(thing) {
		if op == "get" {
			doc, found, err := s.localShadowGet(ctx, thing, name)
			return shadowRPCResponse{Document: doc, Found: found}, err
		}
		names, err := s.localShadowList(ctx, thing)
		return shadowRPCResponse{Names: names}, err
	}
	raw, err := json.Marshal(shadowRPCRead{Op: op, Thing: thing, Shadow: name})
	if err != nil {
		return shadowRPCResponse{}, err
	}
	s.shadowRoute.remoteReads.Add(1)
	return s.tryShadowRead(shards.ReplicaNodes(thing), raw, s.repl.ForwardShadowRead)
}

func (s *Server) tryShadowRead(nodes []cluster.NodeID, raw []byte,
	forward func(cluster.NodeID, []byte) ([]byte, error),
) (shadowRPCResponse, error) {
	var lastErr error
	for i, node := range nodes {
		if i > 0 {
			s.shadowRoute.remoteReadFallbacks.Add(1)
		}
		s.shadowRoute.remoteReadRPCs.Add(1)
		response, rerr := forward(node, raw)
		if rerr != nil {
			lastErr = rerr
			continue
		}
		resp, rerr := decodeShadowRPCResponse(response)
		if rerr != nil {
			lastErr = rerr
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = cluster.ErrNotMember
	}
	s.shadowRoute.remoteReadErrors.Add(1)
	return shadowRPCResponse{}, lastErr
}

func (s *Server) forwardShadowGet(ctx context.Context, _ cluster.NodeID,
	thing, name string,
) (*shadow.Document, bool, error) {
	resp, err := s.forwardShadowRead(ctx, thing, name, "get")
	return resp.Document, resp.Found, err
}

func (s *Server) forwardShadowList(ctx context.Context, _ cluster.NodeID,
	thing string,
) ([]string, error) {
	resp, err := s.forwardShadowRead(ctx, thing, "", "list")
	return resp.Names, err
}

// startSessionShards 起会话分区组，并把会话写入切到复制实现上（CLU-111）。
//
// 这一步让会话与离线队列不再钉在接入节点的盘上：设备连到另一个节点时
// 读到的是同一份会话，节点死掉时它的离线队列在其余节点上仍然完整。
//
// 副本因子等于节点数（每个节点承载每个分片），于是读仍然是本地读——
// 契约 05 不许连接路径发 RPC，而"先问哪个节点有这个会话"恰好是一次 RPC。
func (s *Server) startSessionShards(replicas map[cluster.NodeID]uint64) error {
	rep := &s.cfg.Cluster.Replication
	// owns 回答"这个 clientId 归哪个分片"，过期清理与快照都要它：
	// 一个分片的日志只许改它自己拥有的键，否则另一个分片的快照会把
	// 被删的会话复活（CLU-114）。
	var shards *cluster.ShardGroup
	owns := func(shard uint64) func(string) bool {
		return func(clientID string) bool {
			return clientID != "" && shards.ShardOf(clientID) == shard
		}
	}
	local := s.sessionLocal
	g, err := s.repl.StartSessionShards(cluster.ShardOptions{
		Count:             rep.SessionShards,
		ReplicationFactor: int(rep.SessionReplicationFactor),
		Replicas:          replicas,
		Peers:             s.replPeers,
		Bootstrap:         s.cfg.Node.Mode == config.ModeBootstrap,
		// 整批 apply（CLU-125）：一批已提交日志进同一个写事务，于是
		// fsync 从"每条一次"变成"每批一次"。逐条那条路径（Apply）留着
		// 没接——它仍被单条的用例与 RestoreSessionShard 之外的调用方
		// 使用，而两条并存要有一条是权威的：这里接的是整批。
		ApplyBatch: func(shard uint64, ents []cluster.ShardEntry) error {
			batch := make([]store.SessionEntry, len(ents))
			for i := range ents {
				batch[i] = store.SessionEntry{Index: ents[i].Index, Data: ents[i].Data}
			}
			if err := local.ApplySessionBatch(shard, batch, owns(shard)); err != nil {
				return err
			}
			for i := range ents {
				ents[i].Result = batch[i].Result
			}
			return nil
		},
		AppliedIndex: local.SessionAppliedIndex,
		SnapshotTo: func(shard uint64, w io.Writer) error {
			return local.SnapshotSessionShard(shard, owns(shard), w)
		},
		RestoreFrom: func(shard uint64, r io.Reader) error {
			return local.RestoreSessionShard(shard, owns(shard), r)
		},
		ReadSession: func(clientID string) ([]byte, bool, error) {
			return local.ReadReplicaSnapshot(clientID)
		},
		MergeProposals: store.MergeSessionProposals,
		// ApplyTimeout 留 0 取分片层的默认值（10 秒），与元数据组一致：
		// 两者都没有单独的配置项，超时只在"分片还没选出 leader"时触发。
	})
	if err != nil {
		return fmt.Errorf("server: 启动会话分区组失败: %w", err)
	}
	// shards 先声明后赋值：owns 闭包要用它，而它要等 StartSessionShards
	// 返回。闭包在那之前不会被调用——状态机的回调只在分片起来之后才有
	// 日志可应用。
	shards = g
	s.shards = g
	owners, err := cluster.NewShardOwnership(s.repl, g, s.cfg.Cluster.ShardCount)
	if err != nil {
		_ = g.Close()
		return fmt.Errorf("server: 启动分片所有权服务失败: %w", err)
	}
	if err := s.owners.Use(owners); err != nil {
		_ = owners.Close()
		_ = g.Close()
		return fmt.Errorf("server: 切换分片所有权服务失败: %w", err)
	}
	s.sessions.use(store.NewReplicatedSessionStore(local, g))
	rf := rep.SessionReplicationFactor
	if rf == 0 {
		rf = uint64(len(replicas))
	}
	s.log.Info("会话分区组已启动", "分片数", g.ShardCount(), "副本因子", rf)
	return nil
}

// lateGroup 是一个后置绑定的共识组。
//
// 它只在装配期间短暂为空。为空时 Propose 明确报错而不是静默丢弃——
// 那一刻若真有人写入，说明装配顺序错了，而静默丢弃的现象是
// 「写入返回成功但数据不见了」。
type lateGroup struct {
	mu sync.RWMutex
	g  meta.Group
}

func (l *lateGroup) set(g meta.Group) {
	l.mu.Lock()
	l.g = g
	l.mu.Unlock()
}

func (l *lateGroup) get() meta.Group {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.g
}

func (l *lateGroup) Propose(data []byte) error {
	g := l.get()
	if g == nil {
		return errors.New("server: 复制组还没启动")
	}
	return g.Propose(data)
}

// raftLogWriter 把共识库的日志接到本项目的统一出口。
//
// 库要的是 io.Writer（它内部自己建 hclog）。接成一个 writer 而不是直接
// 传它的 Logger 类型，是为了不让 go-hclog 变成本项目的直接依赖——
// 我们只需要"把它的话接到统一出口"这一件事。
type raftLogWriter struct{ log *slog.Logger }

func (w raftLogWriter) Write(p []byte) (int, error) {
	if w.log != nil {
		w.log.Debug("复制组", "日志", strings.TrimRight(string(p), "\n"))
	}
	return len(p), nil
}

// meshTransport 返回消息面传输。单机下为 nil。
//
// 返回 nil 而不是一个空实现：broker 的 Route 在单机下算出的远端集合恒为空，
// 转发分支根本不会进入（契约 02、07），而一个"什么都不做的传输"会让
// 「这里本该有东西吗」变成每次读代码都要重新判断的问题。
func (s *Server) meshTransport() cluster.Transport {
	if s.cfg.Node.Mode == config.ModeStandalone {
		// 单机下不给 broker 任何传输：Route 算出的远端集合恒为空，
		// 转发分支不会进入（契约 02、07）。给一个"什么都不做的传输"
		// 会让「这里本该有东西吗」变成每次读代码都要重新判断的问题。
		return nil
	}
	if s.lateMesh == nil {
		s.lateMesh = &lateMesh{self: cluster.NodeID(s.mf.NodeID)}
	}
	return s.lateMesh
}

// startReplication 把元数据组带到「可以写入」的状态。
//
// 这里没有"发起竞选"这一步：选举由共识库按超时自己跑，leader 挂掉也会
// 自动重选。自写的那套实现里只有 bootstrap 节点在启动路径上
// 竞选一次，跟随者永远不会超时竞选——于是 leader 进程一挂，剩下的多数派
// 选不出新 leader，写入一直 503，而验收脚本只杀过跟随者所以从没红过。
//
// 这里只等两件事：集群里出现了 leader，以及本节点已经追平。不等就往下走
// 的后果是启动期的写入（文件化注册表的导入）撞上"不是 leader"——
// 而那条错误看起来像配置问题，其实只是选举还没完成。
//
// **会话分区组的就绪刻意不在这里等**（CLU-118）。它是另外 N 条日志，各自
// 跑自己的选举；把它做成启动硬等之后，k8s 上重建一个 Pod 会变成死锁：
// 等不到就在 15 秒后退出，端口随之关闭，于是对端从"连接超时"变成"连接
// 被拒绝"、断路器一直开着，更等不到——而每轮重启还把任期推高一截，
// 对端又因为"leader 尚在"丢弃它的拉票。**退出让收敛更难，不是更容易。**
// 这与 CLU-106 那条（bootstrap 节点无条件等自己当 leader）是同一个形状：
// 启动路径上不该有"等别人"的硬条件。
//
// 改成就绪探针的一个分项（compShards）：进程活着、端口开着、raft 自己
// 收敛，收敛完 Pod 才进 Service。期间的会话写入会拿到"不是 leader"，
// 那正是 503 该出现的地方。
func (s *Server) startReplication(ctx context.Context) error {
	began := time.Now()
	deadline := began.Add(replicationStartTimeout)
	lastLog := began
	for {
		if s.repl.LeaderID() != "" {
			if err := s.repl.Barrier(replicationRetryInterval * 10); err == nil {
				s.replStore.MarkConfirmed(s.now())
				s.ready.Set(compMeta, true, "")
				// bootstrap 节点还要等到自己真的是 leader：紧接着的
				// 文件化注册表导入是一次写入。
				//
				// **但只在真的"第一次起这个集群"时等**（判据是冷层里
				// 还没有 applied index，CLU-109）。`node.mode` 在编排里
				// 是按序号算死的——k8s 的 0 号 Pod 永远是 bootstrap——
				// 而它被重建时集群早已存在、leader 多半是别人。继续等
				// 自己当选就是永远等不到：进程 15 秒后退出，重启再等
				// 15 秒，于是 CrashLoopBackOff，而错误说的是"选举未完成
				// 或对端不可达"，看起来像网络问题。
				//
				// 这个场景在三进程验收里碰不到（那里杀掉的 leader 不会
				// 被自动拉起来），只有真的在 k8s 上删一次 leader 的 Pod
				// 才会显形。
				firstBoot := s.initialRev == 0
				if s.cfg.Node.Mode != config.ModeBootstrap || !firstBoot ||
					s.repl.IsLeader() {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("server: 元数据组在 %s 内没有就绪（选举未完成或对端不可达）",
				replicationStartTimeout)
		}
		if time.Since(lastLog) >= replicationWaitLogEvery {
			lastLog = time.Now()
			s.log.Warn("等待元数据组就绪",
				"leader", string(s.repl.LeaderID()),
				"已等待", time.Since(began).Round(time.Second).String(),
				"上限", replicationStartTimeout.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(replicationRetryInterval):
		}
	}
}

// shardsReady 报告会话分区组是否全部选出了 leader。
//
// 没有分区组时（单机，或装配尚未走到那一步）回真：这个判据只在集群下
// 有意义，而在单机下让它恒假会把就绪探针永久钉死。
func (s *Server) shardsReady() bool {
	shadowShards := s.shadowShards.Load()
	return (s.shards == nil || s.shards.Ready()) &&
		(shadowShards == nil || shadowShards.Ready())
}

// runShardReadiness 把会话分区组的就绪持续写进就绪探针（CLU-118）。
//
// 为什么是持续的而不是启动时判一次：分片的 leader 会随故障转移变化，
// 而"本节点此刻能不能写会话"这件事要一直有人回答——它决定这个 Pod
// 该不该在 Service 里。判一次的版本会让一个失去全部分片 leader 的节点
// 继续接设备，而那些设备的 CONNECT 会在读会话之后的第一次写入上失败。
func (s *Server) runShardReadiness(ctx context.Context) {
	interval := time.Duration(s.cfg.Cluster.Replication.PumpInterval)
	if interval <= 0 {
		interval = defaultPumpInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	was := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := s.shardsReady()
			if now == was {
				continue
			}
			was = now
			if now {
				s.ready.Set(compShards, true, "")
				if n, err := s.shards.BalanceLeaders(); err != nil {
					s.log.Warn("会话分片 leader 均衡请求部分失败", "错误", err)
				} else if n > 0 {
					s.log.Info("会话分片 leader 开始均衡", "转移数", n)
				}
				if shadowShards := s.shadowShards.Load(); shadowShards != nil {
					if n, err := shadowShards.BalanceLeaders(); err != nil {
						s.log.Warn("影子分片 leader 均衡请求部分失败", "错误", err)
					} else if n > 0 {
						s.log.Info("影子分片 leader 开始均衡", "转移数", n)
					}
				}
			} else {
				s.ready.Set(compShards, false, "会话分区组尚未选出 leader")
			}
		}
	}
}

// 启动期等待元数据组就绪的上限与重试间隔。
//
// 这里用墙上时钟而不是注入的时钟：它量的是「别的进程起没起来」，
// 与被测逻辑无关，而注入时钟在启动路径上不会被推进（契约 08 的边界，
// 与 TST-090 的理由同一类）。
//
// 上限两分钟，而不是原先的 15 秒（CLU-118）。实测出来的教训是**退出本身
// 会阻断收敛**：进程一退，端口随之关闭，对端的拨号从"超时"变成"连接
// 被拒绝"，断路器因此一直开着；而每一轮重启都把本节点的任期推高一截，
// 对端又会以"leader 尚在"为由丢弃它的拉票。于是 k8s 上重建一个 Pod 会
// 稳定地卡在 CrashLoopBackOff，错误说的是"选举未完成或对端不可达"——
// 看起来像证书或地址配错了，而那两样都是好的。
//
// 放长是安全的：`/healthz` 在这之前就已经可用（管理端点先起），所以
// liveness 与 startup 探针不会在等待期间杀掉进程；而 `/readyz` 一直为假，
// Pod 不会进 Service。两分钟之后仍不成的才是真故障——那时退出是对的，
// 一个永远等下去的进程会把"配置写错了"藏成"启动很慢"。
const (
	replicationStartTimeout  = 2 * time.Minute
	replicationRetryInterval = 150 * time.Millisecond
	// replicationWaitLogEvery 是等待期间打一次进度日志的间隔。
	//
	// 不打的话这两分钟在外面看是完全安静的，和卡死没有区别。
	replicationWaitLogEvery = 5 * time.Second
)

// runMetaConfirm 周期性确认本节点已经追平已提交的日志。
//
// 推模型下应用由共识库驱动（CLU-097），所以这里不再"追平"，只负责回答
// 「本节点此刻是不是跟上了」——而那个答案要有人周期性地写进视图，
// 因为陈旧度判定读的是它（CLU-023 的 fail-closed）。
//
// 判据是一次 Barrier：它等到本节点把当前已提交的日志都应用完。失败时
// 不推进确认时间，于是视图会被判为陈旧、新连接 fail-closed。那正是该有的
// 反应——一个追不上日志的节点不知道哪张证书已经被吊销了（CLU-049）。
func (s *Server) runMetaConfirm(ctx context.Context) {
	interval := time.Duration(s.cfg.Cluster.Replication.PumpInterval)
	if interval <= 0 {
		interval = defaultPumpInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.repl.Barrier(interval * 10); err != nil {
				// 同一个错误只报一次：追不上日志通常会连续失败，
				// 每 200 毫秒一行会把日志冲掉。
				if msg := err.Error(); msg != lastErr {
					lastErr = msg
					s.log.Error("确认追平已提交日志失败", "错误", msg)
				}
				continue
			}
			if lastErr != "" {
				s.log.Info("已重新追上已提交日志")
				lastErr = ""
			}
			s.replStore.MarkConfirmed(s.now())
			s.ready.Set(compMeta, true, "")
		}
	}
}

/*
以下这一段是**重建**的(2026-10-31)。

原文在一次错误的编辑里被连带删掉：用两个注释锚点之间的切片替换
buildMetaStore,而第二个锚点在四百行之后，于是中间的函数一起被替换。
仓库当时没有任何 commit,文件也没有备份，所以这些函数是依据
调用点的签名、internal/cluster 的接口、现存测试与 docs/prd/03-cluster.md
重写的，行为意图与原实现一致，但措辞与细节可能有出入。

受影响的是六块接线：复制组的 TLS 材料、影子跨节点、mesh 分发与通告、
会话围栏广播、Admin 扇出、成员变更。
*/

// loadReplicationTLS 读复制组的互信材料(CLU-060)。
//
// 三份文件都必须给：节点之间是双向认证，少任何一份都不是"退化成单向",
// 而是整条通道建不起来。证书的 CommonName 必须是节点 ID,那一条由
// cluster 在握手时校验——这里只负责把材料读出来。
func loadReplicationTLS(rep *config.ReplicationConfig) (*tls.Config, error) {
	switch {
	case rep.CertFile == "":
		return nil, errors.New("server: 复制组缺少 cert_file")
	case rep.KeyFile == "":
		return nil, errors.New("server: 复制组缺少 key_file")
	case rep.CAFile == "":
		return nil, errors.New("server: 复制组缺少 ca_file")
	}
	cert, err := tls.LoadX509KeyPair(rep.CertFile, rep.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("server: 读复制证书失败: %w", err)
	}
	caPEM, err := os.ReadFile(rep.CAFile)
	if err != nil {
		return nil, fmt.Errorf("server: 读复制 CA 失败: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("server: 复制 CA 文件里没有可用证书")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ---------- 影子跨节点（CLU-082 至 CLU-085） ----------

// buildShadowStore 按节点模式装出影子存储。
//
// 集群下影子命令走**同一条**元数据复制日志：各节点在 apply 阶段用同一份
// shadow.ApplyCommand 算出同样的版本号。没选"按分区转发"是因为分区要有
// 副本就得每分区一个 raft 组，而单副本分区在归属节点永久故障时直接丢影子,
// 违反 SHDW-070。
func (s *Server) buildShadowStore(local shadow.Store) (shadow.Store, error) {
	if s.replStore == nil {
		return local, nil
	}
	rs, err := shadow.NewReplicatedStore(local, shadowLog{s: s}, s.mf.NodeID)
	if err != nil {
		return nil, err
	}
	s.shadowRepl = rs
	return rs, nil
}

// shadowLog 把影子命令装进元数据命令里提交。
//
// 共用一条日志而不是另开一个组：两条日志之间没有顺序关系，而影子的
// 幂等键要和元数据的在同一个状态机里判重。
type shadowLog struct{ s *Server }

func (l shadowLog) AppendShadow(ctx context.Context, id string, raw []byte) error {
	if shadowShards := l.s.shadowShards.Load(); shadowShards != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		cmd, err := shadow.DecodeCommand(raw)
		if err != nil {
			return err
		}
		env, err := json.Marshal(shadowShardEnvelope{ID: id, Raw: raw})
		if err != nil {
			return err
		}
		if _, err := shadowShards.Propose(cmd.Thing, env); err != nil {
			return err
		}
		// 提案可能直达远端 leader；SyncPropose 返回不代表发起节点的本地副本
		// 已经 apply。直接等本地状态机提交事务时交付 pending 结果，避免每条
		// 写再做一次 ReadIndex/Barrier，同时保持 accepted 前已经本地落盘。
		return l.s.shadowRepl.WaitCommitted(ctx, id)
	}
	_, err := l.s.replStore.Apply(ctx, meta.Command{
		ID:     id,
		Kind:   meta.CmdNop,
		Shadow: raw,
	})
	return err
}

// applyShadowCommand 在 apply 阶段把影子命令作用到本节点。
//
// 它由 meta 的状态机在每个节点上调用(包括 leader 自己):影子的版本号
// 必须由各节点算出同样的值，所以计算只能发生在 apply 之后，不能在提交之前。
func (s *Server) applyShadowCommand(id string, raw []byte) error {
	if s.shadowRepl == nil {
		return errors.New("server: 影子复制未装配")
	}
	return s.shadowRepl.ApplyCommitted(id, raw)
}

func (s *Server) applyShadowCommandTx(id string, raw []byte, tx meta.Transaction) (func(), error) {
	if s.shadowRepl == nil || s.shadows == nil {
		return nil, errors.New("server: 影子复制未装配")
	}
	return s.shadowRepl.ApplyCommittedTx(id, raw, func(cmd shadow.Command) (*shadow.Result, error) {
		return s.shadows.ApplyTx(tx, cmd)
	})
}

// ---------- mesh 分发与通告（CLU-075 至 CLU-080） ----------

// onMesh 分发一条 mesh 消息。
//
// 四类负载按非空字段区分。帧只投本地订阅者、**不再转发**:否则两个
// 互相有兴趣的节点之间会无限弹。
func (s *Server) onMesh(from cluster.NodeID, p cluster.MeshPayload) {
	// 记下通告过的节点：它被移出成员之后要把它的通告内容忘掉（CLU-101）。
	if from != "" {
		s.gossipMu.Lock()
		if s.gossipSeen == nil {
			s.gossipSeen = map[cluster.NodeID]bool{}
		}
		s.gossipSeen[from] = true
		s.gossipMu.Unlock()
	}
	if len(p.Frames) > 0 && s.mesh != nil {
		s.mesh.Deliver(from, p.Frames)
	}
	// 通告是**全量快照**:丢一次或乱序都不会让兴趣集永久偏离
	// （增量通告丢一条就永久少一个过滤器，而现象是"偶尔收不到"）。
	if p.Filters != nil && s.rt != nil {
		s.rt.Interest().Advertise(from, p.Filters)
	}
	if s.remoteThings != nil {
		switch {
		case p.ThingSnapshot:
			s.remoteThings.ApplySnapshot(from, p.ThingEpoch, p.ThingVersion, p.Things)
			s.thingSyncMu.Lock()
			delete(s.thingLegacyPeers, from)
			s.thingSyncMu.Unlock()
		case p.ThingEpoch != 0:
			s.remoteThings.ApplyDelta(from, p.ThingEpoch, p.ThingFrom,
				p.ThingVersion, p.ThingChanges)
			s.thingSyncMu.Lock()
			delete(s.thingLegacyPeers, from)
			s.thingSyncMu.Unlock()
		case p.Things != nil:
			// 兼容旧格式的全量快照。下一条带版本的增量会因没有
			// cursor 请求一次新格式快照，之后进入增量模式。
			s.remoteThings.Advertise(from, p.Things)
			s.thingSyncMu.Lock()
			if s.thingLegacyPeers == nil {
				s.thingLegacyPeers = make(map[cluster.NodeID]bool)
			}
			s.thingLegacyPeers[from] = true
			s.thingSyncMu.Unlock()
		}
	}
	if p.ThingSync && from != "" {
		s.thingSyncMu.Lock()
		if s.thingSyncRequests == nil {
			s.thingSyncRequests = make(map[cluster.NodeID]bool)
		}
		s.thingSyncRequests[from] = true
		s.thingSyncMu.Unlock()
	}
	if p.Shares != nil && s.rt != nil {
		// 共享订阅组的成员数（MQTT-116）。同样是全量快照：丢一次通告
		// 不会让视图永久偏离，而增量丢一条就永久少一个节点——现象是
		// 那一组里有一台机器再也收不到消息。
		s.rt.AdvertiseSharedRemote(from, p.Shares)
	}
	// 保留消息的跨节点更新（MQTT-122）。走 Apply 而不是 Set：再广播回去
	// 会让两个节点之间无限弹（与帧的"不再转发"同理，CLU-077）。
	if len(p.Retained) > 0 && s.rt != nil {
		s.applyRemoteRetained(p.Retained)
	}
	for _, clientID := range p.Evict {
		// 对端刚接管了这个 clientId：本地若还有同名连接就断掉
		// （CLU-081，最终一致的互斥，窗口是一次 mesh 往返）。
		if s.brk != nil && s.brk.KickClient(clientID, reason.SessionTakenOver) {
			s.log.Info("按接管通知断开本地连接", "clientId", clientID, "接管节点", string(from))
		}
	}
}

// runGossip 周期性把本节点的通配符订阅与设备归属通告给对端。
//
// 通配符订阅才进全局表：百万条精确的设备订阅不传播，这是相对通用
// broker 的主要性能杠杆（ARCH-017）。
func (s *Server) runGossip(ctx context.Context) {
	interval := time.Duration(s.cfg.Cluster.Replication.PumpInterval)
	if interval <= 0 {
		interval = defaultPumpInterval
	}
	// 设备归属走带版本增量；通配符与共享组数量很小，仍发全量快照。
	t := time.NewTicker(interval * 5)
	defer t.Stop()
	// known 记住上一轮见到的对端，用来发现"新出现的对端"并给它发一次
	// 保留消息的全量 dump（CLU-108）。mesh 没有"对端已连上"的回调，而
	// 这个循环本来就每周期跑一次，所以判据放在这里，不另起一个协程。
	//
	// 只有这个协程读写它，因此不加锁。
	known := map[cluster.NodeID]bool{}
	thingSynced := map[cluster.NodeID]bool{}
	thingPeerVersion := map[cluster.NodeID]uint64{}
	var thingPublishedVersion uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.repl == nil {
				continue
			}
			thingVersion, thingChanges := s.brk.DrainThingChanges()
			thingFrom := thingPublishedVersion
			base := cluster.MeshPayload{
				Filters: s.rt.LocalWildcards(),
				Shares:  s.rt.LocalSharedCounts(),
			}
			// 两个小快照都要给出非 nil 的切片：nil 表示"这次没带这类
			// 负载"，而空切片表示"本节点一个都没有"。混同的后果是
			// 最后一个订阅退订之后对端永远记着旧的那一份。
			if base.Filters == nil {
				base.Filters = []string{}
			}
			if base.Shares == nil {
				base.Shares = []cluster.ShareCount{}
			}
			s.thingSyncMu.Lock()
			syncRequests := s.thingSyncRequests
			s.thingSyncRequests = make(map[cluster.NodeID]bool)
			legacyPeers := make(map[cluster.NodeID]bool, len(s.thingLegacyPeers))
			for peer := range s.thingLegacyPeers {
				legacyPeers[peer] = true
			}
			s.thingSyncMu.Unlock()
			peers := s.repl.Peers()
			for _, peer := range peers {
				p := base
				p.ThingEpoch = s.thingEpoch
				p.ThingSync = s.remoteThings != nil && s.remoteThings.NeedsSync(peer)
				peerVersion := thingPeerVersion[peer]
				switch {
				case legacyPeers[peer] || syncRequests[peer] || !thingSynced[peer]:
					v, things := s.brk.ThingSnapshot()
					p.ThingSnapshot = true
					p.ThingFrom, p.ThingVersion, p.Things = v, v, things
				case peerVersion == thingFrom:
					p.ThingFrom, p.ThingVersion = thingFrom, thingVersion
					p.ThingChanges = thingChanges
				case peerVersion >= thingVersion:
					// 对端刚拿过一份比当前已发布水位更新的快照。
					// 不倒灌旧增量，只发送它已经看到的版本心跳。
					p.ThingFrom, p.ThingVersion = peerVersion, peerVersion
				default:
					v, things := s.brk.ThingSnapshot()
					p.ThingSnapshot = true
					p.ThingFrom, p.ThingVersion, p.Things = v, v, things
				}
				if err := s.repl.SendMesh(peer, p); err != nil {
					s.log.Debug("通告失败", "对端", string(peer), "错误", err.Error())
					thingSynced[peer] = false
					continue
				}
				thingSynced[peer] = true
				thingPeerVersion[peer] = p.ThingVersion
			}
			thingPublishedVersion = thingVersion
			// 新出现的对端要补一次保留消息全量（CLU-108）。两端各自
			// 做这件事，所以 dump 天然是双向的——这同时把一个刚重启、
			// Lamport 计数器落后的节点抬上来（MQTT-123 的接收规则）。
			seen := make(map[cluster.NodeID]bool, len(peers))
			for _, peer := range peers {
				seen[peer] = true
				if !known[peer] {
					s.dumpRetainedTo(peer)
				}
			}
			for peer := range thingSynced {
				if !seen[peer] {
					delete(thingSynced, peer)
					delete(thingPeerVersion, peer)
				}
			}
			known = seen
			s.forgetDepartedNodes()
		}
	}
}

// forgetDepartedNodes 把已经不是成员的节点的通告内容丢掉（CLU-101）。
//
// 不丢的后果有三条，而它们都不报错：
//
//   - 兴趣表里留着它的通配符订阅，于是每条匹配的消息都往一个不在了的
//     节点转发一次（每次失败记一条 mesh 事件，看起来像网络抖动）；
//   - 设备归属表里留着它承载的设备，于是那些设备的消息被转给它而不是
//     在本地查无归属时放弃——消息就此消失；
//   - 共享订阅组里留着它的成员数，而组内**只投一个**：轮到它的那些
//     消息没有人补投。
//
// 判据取 raft 的成员列表而不是"最近有没有收到它的通告"：后者会把
// 一次网络抖动当成节点离开，而成员列表是复制状态，三个节点看到的一样。
func (s *Server) forgetDepartedNodes() {
	if s.repl == nil {
		return
	}
	members := make(map[cluster.NodeID]bool)
	for _, m := range s.repl.Members() {
		members[m.ID] = true
	}
	if len(members) == 0 {
		// 成员列表读不出来时什么都不做：空列表当成"所有人都走了"
		// 会在一次读取失败之后清空全部路由状态。
		return
	}
	s.gossipMu.Lock()
	var stale []cluster.NodeID
	for node := range s.gossipSeen {
		if !members[node] {
			stale = append(stale, node)
			delete(s.gossipSeen, node)
		}
	}
	s.gossipMu.Unlock()

	for _, node := range stale {
		if s.rt != nil {
			s.rt.Interest().Forget(node)
			s.rt.ForgetSharedRemote(node)
		}
		if s.remoteThings != nil {
			s.remoteThings.Forget(node)
		}
		s.thingSyncMu.Lock()
		delete(s.thingLegacyPeers, node)
		delete(s.thingSyncRequests, node)
		s.thingSyncMu.Unlock()
		s.log.Info("已忘掉离开成员列表的节点的通告", "节点", string(node))
	}
}

// ---------- 会话围栏（CLU-081） ----------

// broadcastTakeover 把一次接管排进待广播队列。
//
// 它在**连接路径**上被调用，所以只入队不阻塞：队列满就丢并记一条
// （契约 05 禁止连接路径等网络，而丢一条通知的后果是那一个 clientId
// 的互斥窗口变长，不是正确性问题——围栏本身是最终一致的）。
func (s *Server) broadcastTakeover(clientID string) {
	if s.takeovers == nil || clientID == "" {
		return
	}
	select {
	case s.takeovers <- clientID:
	default:
		s.log.Warn("接管通知队列已满，丢弃一条", "clientId", clientID)
	}
}

// runTakeovers 把排队的接管通知批量广播出去。
//
// 批量而不是一条一发：接管在设备大面积重连时是突发的，一条一个
// 往返会把 mesh 打满。
func (s *Server) runTakeovers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first, ok := <-s.takeovers:
			if !ok {
				return
			}
			batch := []string{first}
			// 把此刻队列里已有的一起带走，不等新的。
			for drained := false; !drained; {
				select {
				case next := <-s.takeovers:
					batch = append(batch, next)
				default:
					drained = true
				}
			}
			if s.repl == nil {
				continue
			}
			p := cluster.MeshPayload{Evict: batch}
			for _, peer := range s.repl.Peers() {
				if err := s.repl.SendMesh(peer, p); err != nil {
					s.log.Debug("接管通知发送失败", "对端", string(peer), "错误", err.Error())
				}
			}
		}
	}
}

// hydrateHotAtBoot 启动时用冷层重建热视图（P2）。
//
// 它与 applied index 落盘（P1）是**一对**，单独上任何一条都是错的：
//
//   - 只上 P1：状态机从 initialRev 起步，于是已经应用过的日志被跳过
//     （ApplyCommitted 里的 rev <= s.rev），而热视图原先正是靠重放
//     那一段建出来的——结果是控制面空着，设备全部连不上。
//   - 只上 P2：重建完又被重放盖一遍，白做功，但不出错。
//
// 为什么要这一对：hashicorp/raft 重启时把整条日志推给状态机，而
// dragonboat 的 IOnDiskStateMachine 只推 applied index 之后的那一段
// （更早的已经被快照压掉了）。这一对改动把恢复路径换成后者要的那种，
// 而它在前者上同样正确——重放变成空操作。
//
// 顺带的收益：重启时间从"与日志长度成正比"变成"与冷层大小成正比"。
func (s *Server) hydrateHotAtBoot() error {
	if s.initialRev == 0 || s.reg2 == nil || s.store == nil {
		// 读不到 applied index 说明是首次启动或旧数据目录：那时日志会
		// 被整条重放，热视图照旧从重放里来，不必也不能在这里重建
		// （冷层这时还是空的）。
		return nil
	}
	if err := s.store.Hydrate(s.initialRev, s.reg2.HydrateHot); err != nil {
		return fmt.Errorf("server: 用冷层重建热视图失败: %w", err)
	}
	s.log.Info("热视图已从冷层重建", "applied index", uint64(s.initialRev))
	return nil
}

// ---------- 保留消息的跨节点同步（MQTT-122 至 MQTT-126） ----------

// retainedBatchLimit 是一条 mesh 消息里最多装多少条保留消息。
//
// dump 不能一条消息装下整个集合（CLU-108）：集合可能上万条、每条 KB 级，
// 而 mesh 与消息帧共用同一条 TLS 连接——一条巨型消息会把正常的投递堵在
// 后面。按条数而不是字节数分片，是因为条数在发送侧是现成的。
const retainedBatchLimit = 64

// broadcastRetained 把一条保留消息的变更排进待广播队列。
//
// 它在**发布路径**上被调用（router 的复制钩子），所以只入队不阻塞：
// 契约 05 禁止发布路径等网络。队列满就丢并计数——丢一条的后果是那个
// 主题在部分节点上缺失，直到下一次 dump 或下一次发布（MQTT-125 把这条
// 边界写进了文档，digest 对账是下一阶段的事）。
func (s *Server) broadcastRetained(topic string, m *router.Message) {
	if s.retaineds == nil || topic == "" {
		return
	}
	u := cluster.RetainedUpdate{
		Topic: topic, Payload: m.Payload, Props: m.Props, QoS: m.QoS,
		Counter: m.Version.Counter, Node: m.Version.Node,
		ExpiryUnixMs: m.ExpiryUnixMs, StoredAtUnixMs: m.StoredAtUnixMs,
	}
	select {
	case s.retaineds <- u:
	default:
		s.retainedDrops.Add(1)
		s.log.Warn("保留消息广播队列已满，丢弃一条", "主题", topic)
	}
}

// runRetained 把排队的保留消息变更批量广播出去。
//
// 批量而不是一条一发：设备批量上线时 retained 发布是突发的，一条一个
// 往返会把 mesh 打满（与 runTakeovers 同一个理由）。
func (s *Server) runRetained(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first, ok := <-s.retaineds:
			if !ok {
				return
			}
			batch := []cluster.RetainedUpdate{first}
			for drained := false; !drained && len(batch) < retainedBatchLimit; {
				select {
				case next := <-s.retaineds:
					batch = append(batch, next)
				default:
					drained = true
				}
			}
			if s.repl == nil {
				continue
			}
			p := cluster.MeshPayload{Retained: batch}
			for _, peer := range s.repl.Peers() {
				if err := s.repl.SendMesh(peer, p); err != nil {
					s.log.Debug("保留消息广播失败",
						"对端", string(peer), "条数", len(batch), "错误", err.Error())
				}
			}
		}
	}
}

// applyRemoteRetained 把远端送来的保留消息更新合并进本地。
func (s *Server) applyRemoteRetained(ups []cluster.RetainedUpdate) {
	store := s.rt.Retained()
	for _, u := range ups {
		if u.Topic == "" {
			continue
		}
		v := router.Version{Counter: u.Counter, Node: u.Node}
		// Lamport 的接收规则：先把本地计数器抬到不低于对端（MQTT-123）。
		// 不抬的后果是一个刚重启的节点发出的新 retained 被对端更老的那条
		// 盖掉，而它自己不知道。
		s.rt.SeedVersion(v)
		store.Apply(u.Topic, &router.Message{
			Topic: u.Topic, Payload: u.Payload, Props: u.Props,
			QoS: u.QoS, Retain: true,
			ExpiryUnixMs: u.ExpiryUnixMs, StoredAtUnixMs: u.StoredAtUnixMs,
		}, v)
	}
}

// dumpRetainedTo 把本节点的保留消息全量发给一个对端（CLU-108）。
//
// 含墓碑：对端若在离线期间漏掉了一次清除，只有看到墓碑才会撤掉它本地
// 那条（MQTT-124）。
func (s *Server) dumpRetainedTo(peer cluster.NodeID) {
	if s.repl == nil || s.rt == nil {
		return
	}
	all := s.rt.Retained().All()
	if len(all) == 0 {
		return
	}
	sent := 0
	batch := make([]cluster.RetainedUpdate, 0, retainedBatchLimit)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.repl.SendMesh(peer, cluster.MeshPayload{Retained: batch}); err != nil {
			s.log.Debug("保留消息全量发送失败",
				"对端", string(peer), "条数", len(batch), "错误", err.Error())
			return
		}
		sent += len(batch)
	}
	for _, m := range all {
		batch = append(batch, cluster.RetainedUpdate{
			Topic: m.Topic, Payload: m.Payload, Props: m.Props, QoS: m.QoS,
			Counter: m.Version.Counter, Node: m.Version.Node,
			ExpiryUnixMs: m.ExpiryUnixMs, StoredAtUnixMs: m.StoredAtUnixMs,
		})
		if len(batch) == retainedBatchLimit {
			flush()
			batch = batch[:0]
		}
	}
	flush()
	s.log.Info("已向新对端发送保留消息全量",
		"对端", string(peer), "条数", sent, "总数", len(all))
}

// reloadRetained 从持久层重新装载保留消息（CLU-107）。
//
// Raft 快照安装是整库替换，而冷层、会话与 retained 在同一个 bbolt 里；
// 装载只在启动时跑一次。于是装完快照后磁盘上的 retained 已经是 leader 的，
// 而内存里还是自己的——这个不一致今天就在，只是 retained 不跨节点所以
// 没人看得见，接上之后它的表现是"重启后 retained 变了"。
func (s *Server) reloadRetained() error {
	if s.retained == nil || s.rt == nil {
		return nil
	}
	n, maxV, err := s.retained.LoadInto(s.rt.Retained())
	if err != nil {
		return err
	}
	s.rt.SeedVersion(maxV)
	s.log.Info("快照安装后已重载保留消息", "条数", n, "版本水位", maxV.Counter)
	return nil
}

// runRetainedSweep 周期性回收过期的保留消息与到期的墓碑。
//
// 两件事以前都没有人做：Sweep 存在但全仓库没有调用方，于是过期的
// retained 虽然读侧看不见、内存却一直占着。墓碑让这件事从"内存慢慢长"
// 变成"内存一定会长"，所以这个协程是跟着墓碑一起必须有的。
func (s *Server) runRetainedSweep(ctx context.Context) {
	t := time.NewTicker(retainedSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.rt.Retained().Sweep(); n > 0 {
				s.log.Debug("已回收保留消息", "条数", n)
			}
		}
	}
}

// retainedSweepInterval 是回收周期。
//
// 五分钟：墓碑的 TTL 是一小时，过期判定本来就在读侧即时生效，所以这里
// 只是回收内存，不需要密集。
const retainedSweepInterval = 5 * time.Minute

// ---------- Admin 扇出（CLU-086 至 CLU-089） ----------

// runLocalQuery 在本节点执行一次扇出查询。
//
// 行以字节返回：传输层不认识任何具体查询的行类型（见 cluster 的
// wireResp.Rows），那样它才不必随每个新查询改一次。
func (s *Server) runLocalQuery(kind string, params map[string]string, cursor string, limit int) ([][]byte, string, error) {
	switch kind {
	case queryKindConns:
		conns, next := s.brk.ListConns(broker.ConnQuery{
			ClientIDPrefix: params["prefix"],
			Thing:          params["thing"],
			After:          cursor,
			Limit:          limit,
		})
		rows := make([][]byte, 0, len(conns))
		for _, c := range conns {
			raw, err := json.Marshal(c)
			if err != nil {
				return nil, "", err
			}
			rows = append(rows, raw)
		}
		return rows, next, nil
	default:
		return nil, "", fmt.Errorf("server: 不认识的查询 %q", kind)
	}
}

// queryKindConns 是"列出在线连接"这个查询的名字。
const queryKindConns = "conns"

// fanoutTargets 算出这一页该问哪些节点（ARCH-040）。
//
// 首页问所有节点；续页**只问游标里还有位置的那些**。差别不是优化：
// 一个上一页已经读完的节点在续页里没有游标，若仍然去问它，它会从自己
// 的头部重新列一遍——于是同一批连接在第二页又出现一次。这类重复在
// 聚合分页里很难被看出来，因为每个节点各自都是对的。
func fanoutTargets(all []cluster.NodeID, cursor map[cluster.NodeID]string, first bool) []cluster.NodeID {
	if first {
		return all
	}
	out := make([]cluster.NodeID, 0, len(all))
	for _, node := range all {
		if cur, ok := cursor[node]; ok && cur != "" {
			out = append(out, node)
		}
	}
	return out
}

// fanoutConns 跨节点列出在线连接。
//
// 三条约束来自 PRD：
//
//   - 游标编码每个节点各自的位置（ARCH-040）。单一 offset 下合并分页
//     无法正确推进：各节点行数不同，按全局偏移翻页会跳过某些节点的行、
//     重复另一些。
//   - 单节点失败是**部分失败**而不是整体失败（ARCH-041）：运维工具在
//     故障时必须仍然可用，那正是最需要它的时刻。
//   - 本节点也走同一条路径，只是不绕网络。
func (s *Server) fanoutConns(ctx context.Context, params map[string]string, after string, limit int) (
	conns []broker.ConnSnapshot, next string, partial []string, err error,
) {
	cursor, err := cluster.DecodeCursor(after)
	if err != nil {
		return nil, "", nil, err
	}
	all := append([]cluster.NodeID{cluster.NodeID(s.mf.NodeID)}, s.repl.Peers()...)
	targets := fanoutTargets(all, cursor, after == "")
	if len(targets) == 0 {
		return nil, "", nil, nil
	}
	results, err := s.mesh.Query(ctx, targets, cluster.AdminQuery{
		Kind: queryKindConns, Params: params, Cursor: cursor, Limit: limit,
	})
	if err != nil {
		return nil, "", nil, err
	}

	conns, nextPer, partial, err := mergeConnResults(results, cursor)
	if err != nil {
		return nil, "", nil, err
	}
	next, err = cluster.EncodeCursor(nextPer)
	if err != nil {
		return nil, "", nil, err
	}
	return conns, next, partial, nil
}

// mergeConnResults 把各节点的结果并成一页（ARCH-041）。
//
// 两条规则值得单独说：
//
//   - 失败的节点记进 partial 而不是让整次查询失败。运维工具在故障时
//     必须仍然可用，那正是最需要它的时刻。
//   - 失败的节点**保留它原来的游标**。清掉的话下一页会从它的头部重新
//     读，于是同一批连接出现两次——而每个节点各自都是对的，这种重复
//     在聚合分页里很难被看出来。
func mergeConnResults(results []cluster.AdminResult, cursor map[cluster.NodeID]string) (
	[]broker.ConnSnapshot, map[cluster.NodeID]string, []string, error,
) {
	var conns []broker.ConnSnapshot
	var partial []string
	nextPer := map[cluster.NodeID]string{}
	for _, res := range results {
		if res.Err != nil {
			partial = append(partial, string(res.Node))
			if cur, ok := cursor[res.Node]; ok && cur != "" {
				nextPer[res.Node] = cur
			}
			continue
		}
		for _, item := range res.Items {
			raw, ok := item.([]byte)
			if !ok {
				return nil, nil, nil, fmt.Errorf("server: %s 返回的行不是字节", res.Node)
			}
			var snap broker.ConnSnapshot
			if derr := json.Unmarshal(raw, &snap); derr != nil {
				return nil, nil, nil, fmt.Errorf("server: 解析 %s 的连接行失败: %w", res.Node, derr)
			}
			conns = append(conns, snap)
		}
		if res.NextCursor != "" {
			nextPer[res.Node] = res.NextCursor
		}
	}
	// 顺序确定：各节点并发返回，不排序的话同一份数据每次的页内顺序都不同，
	// 而分页的下一页是按 clientId 续读的。
	sort.Slice(conns, func(i, j int) bool { return conns[i].ClientID < conns[j].ClientID })
	sort.Strings(partial)
	return conns, nextPer, partial, nil
}

// ---------- 成员变更（CLU-090 至 CLU-095） ----------

// AddClusterMember 把一个节点加入复制组（CLU-090）。
func (s *Server) AddClusterMember(id, addr string, replicaID uint64) error {
	if s.repl == nil {
		return errors.New("server: 单机模式没有复制组")
	}
	if s.cfg.Cluster.Replication.SessionReplicationFactor != 0 {
		return errors.New("server: RF 会话分片尚不支持在线成员变更；须先迁移各分片的 Raft 副本")
	}
	if err := s.repl.AddMember(cluster.NodeID(id), addr, replicaID); err != nil {
		return err
	}
	s.log.Info("已加入成员", "节点", id, "地址", addr)
	s.emitAudit("cluster", "", "POST", "/cluster/members", "AddMember", id)
	return nil
}

// RemoveClusterMember 把一个节点移出复制组。
func (s *Server) RemoveClusterMember(id string) error {
	if s.repl == nil {
		return errors.New("server: 单机模式没有复制组")
	}
	if s.cfg.Cluster.Replication.SessionReplicationFactor != 0 {
		return errors.New("server: RF 会话分片尚不支持在线成员变更；须先迁移各分片的 Raft 副本")
	}
	if err := s.repl.RemoveMember(cluster.NodeID(id)); err != nil {
		return err
	}
	s.log.Info("已移除成员", "节点", id)
	s.emitAudit("cluster", "", "DELETE", "/cluster/members", "RemoveMember", id)
	return nil
}

// ClusterMembers 返回当前成员列表。单机返回只有本节点的一条。
func (s *Server) ClusterMembers() []cluster.Member {
	if s.repl == nil {
		return []cluster.Member{{ID: cluster.NodeID(s.mf.NodeID)}}
	}
	return s.repl.Members()
}

/*
lateMesh 是后置绑定的消息面传输。

它与 lateGroup 解的是同一个顺序问题，只是方向相反：broker 在装配期
（New）就要拿到 Transport，而真正的 mesh 要等复制组起来（Run）才有，
因为复制组必须在注册表之后才能起（装快照要重建热视图）。

绑定前所有方法都是空操作而不是报错：那段时间里 Route 算出的远端集合
恒为空（兴趣通告还没到），转发分支根本不会进入。Self 必须立刻可用，
broker 用它判断"这一跳是不是我自己"。
*/
type lateMesh struct {
	self cluster.NodeID
	mu   sync.RWMutex
	impl *cluster.MeshTransport
	recv func(cluster.NodeID, []cluster.Frame)
}

var _ cluster.Transport = (*lateMesh)(nil)

// bind 把真正的传输接上，并把先前注册的接收回调转交给它。
func (m *lateMesh) bind(impl *cluster.MeshTransport) {
	m.mu.Lock()
	m.impl = impl
	fn := m.recv
	m.mu.Unlock()
	if fn != nil {
		impl.OnReceive(fn)
	}
}

func (m *lateMesh) get() *cluster.MeshTransport {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.impl
}

func (m *lateMesh) Forward(ctx context.Context, to []cluster.NodeID, batch []cluster.Frame) error {
	if impl := m.get(); impl != nil {
		return impl.Forward(ctx, to, batch)
	}
	return nil
}

// OnReceive 记下回调。绑定之后转交给真正的传输。
//
// 记下来而不是丢掉：broker 在装配期就注册它，而那时传输还没绑上——
// 丢掉的后果是跨节点来的帧永远没人投，现象是"订阅成功但收不到"
// （与策略漏了 iot:Receive 的现象一模一样，排查方向却完全不同）。
func (m *lateMesh) OnReceive(fn func(from cluster.NodeID, batch []cluster.Frame)) {
	m.mu.Lock()
	m.recv = fn
	impl := m.impl
	m.mu.Unlock()
	if impl != nil {
		impl.OnReceive(fn)
	}
}

func (m *lateMesh) Query(ctx context.Context, to []cluster.NodeID, q cluster.AdminQuery) ([]cluster.AdminResult, error) {
	if impl := m.get(); impl != nil {
		return impl.Query(ctx, to, q)
	}
	return nil, nil
}

func (m *lateMesh) Self() cluster.NodeID { return m.self }

func (m *lateMesh) Close() error { return nil }
