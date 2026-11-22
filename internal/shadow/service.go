package shadow

import (
	"context"
	"fmt"
	"sync"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// Publish 是一条待发布的 MQTT 消息。
//
// 服务层只产出消息，不自己发布：它不该知道 broker 的存在。
// 这也使 REST 路径能拿到同一批消息去扇出（PRD SHDW-045）——
// 若由 MQTT 层负责产出 delta，REST 更新就不会触发 delta，
// 而那正是 SHDW-045 要防的分叉。
type Publish struct {
	Topic   string
	Payload []byte
	// QoS1 为真表示须以 QoS 1 投递。
	//
	// delta 与 documents 用 QoS 1：它们是状态变更通知，丢一条意味着
	// 设备会一直停留在旧状态直到下一次变更。
	QoS1 bool
}

// Outcome 是一次影子请求的完整结果。
type Outcome struct {
	// ResponseTopic 是 accepted 或 rejected 主题，REST 路径可忽略。
	ResponseTopic string
	// Response 是响应载荷。
	Response []byte
	// Publishes 是须扇出的消息（delta、documents）。
	Publishes []Publish
	// Err 非空表示请求被拒，Response 为对应的 rejected 载荷。
	Err *Error
	// HTTPStatus 是 REST 路径的状态码。
	HTTPStatus int
}

// Options 是服务装配参数。
type Options struct {
	Store Store
	// Partitioner 判定影子的归属节点（契约 06、PRD SHDW-060）。
	Partitioner cluster.Partitioner
	Limiter     quota.Limiter
	// Now 提供当前时间（Unix 毫秒）。核心包不直接读时钟（契约 08）。
	Now func() int64
	// Forward 把命令转发给分区 owner。
	//
	// 单机下为 nil，转发分支不会进入——但这条分支必须在 v0.1 就存在
	// （契约 06）。事后插入它等于把本服务重写一遍：它会改变
	// 每一个写入方法的错误路径与返回时机。
	Forward func(ctx context.Context, owner cluster.NodeID, cmd Command) (*Result, error)
	// LocalGet/LocalList 可在读取本地副本前执行线性一致 Barrier。
	// ForwardGet/ForwardList 用于非 owner 节点；读是幂等的，可由装配层
	// 在多个副本间回退。留空保持单机直接读 Store 的行为。
	LocalGet    func(context.Context, string, string) (*Document, bool, error)
	LocalList   func(context.Context, string) ([]string, error)
	ForwardGet  func(context.Context, cluster.NodeID, string, string) (*Document, bool, error)
	ForwardList func(context.Context, cluster.NodeID, string) ([]string, error)
	// OnForward 在真正转发前后被调用，用来包一层追踪。
	// 返回的函数在转发结束后调用。单机不进入转发分支，因此不会调用它。
	OnForward func(ctx context.Context, owner, thing string) (context.Context, func(error))
	// OnChange 在更新或删除已经写入之后调用。
	//
	// 时间线要看到影子变更，而它的数据源只能是事件总线（OBS-062）。
	// 回调由装配方拿去 Emit，影子包本身不依赖事件。
	OnChange func(thing, op string)
}

// Service 是 MQTT 与 REST 共用的影子状态机（PRD SHDW-044）。
type Service struct {
	store     Store
	partMu    sync.RWMutex
	part      cluster.Partitioner
	lim       quota.Limiter
	now       func() int64
	fwd       func(ctx context.Context, owner cluster.NodeID, cmd Command) (*Result, error)
	localGet  func(context.Context, string, string) (*Document, bool, error)
	localList func(context.Context, string) ([]string, error)
	fwdGet    func(context.Context, cluster.NodeID, string, string) (*Document, bool, error)
	fwdList   func(context.Context, cluster.NodeID, string) ([]string, error)
	onFwd     func(ctx context.Context, owner, thing string) (context.Context, func(error))
	onChange  func(thing, op string)
}

// New 创建影子服务。
func New(opt Options) (*Service, error) {
	switch {
	case opt.Store == nil:
		return nil, fmt.Errorf("shadow: 缺少 Store")
	case opt.Partitioner == nil:
		return nil, fmt.Errorf("shadow: 缺少 Partitioner")
	case opt.Limiter == nil:
		return nil, fmt.Errorf("shadow: 缺少 Limiter")
	case opt.Now == nil:
		return nil, fmt.Errorf("shadow: 缺少时钟（契约 08 要求注入）")
	}
	return &Service{
		store: opt.Store, part: opt.Partitioner,
		lim: opt.Limiter, now: opt.Now, fwd: opt.Forward, onFwd: opt.OnForward,
		localGet: opt.LocalGet, localList: opt.LocalList,
		fwdGet: opt.ForwardGet, fwdList: opt.ForwardList,
		onChange: opt.OnChange,
	}, nil
}

// UsePartitioner 在启动期把兼容的本地分区器切到持久化 shard 布局。
// 管理端点可能已经监听，因此用锁保证切换与并发读写无数据竞争。
func (s *Service) UsePartitioner(p cluster.Partitioner) error {
	return s.UsePartitionerAndActivate(p, nil)
}

// UsePartitionerAndActivate 在阻止全部在途影子读写的同一临界区内执行
// activate 并切换路由。装配层用它原子地把命令日志与分区表一起切到新组，
// 避免一条请求按旧 owner 判定、却写入新日志。
func (s *Service) UsePartitionerAndActivate(p cluster.Partitioner, activate func() error) error {
	if p == nil {
		return fmt.Errorf("shadow: 缺少 Partitioner")
	}
	s.partMu.Lock()
	defer s.partMu.Unlock()
	if activate != nil {
		if err := activate(); err != nil {
			return err
		}
	}
	s.part = p
	return nil
}

// nowSec 返回当前 Unix 秒。
//
// 影子文档的时间戳单位是秒（与 AWS 一致），而项目内部统一用毫秒，
// 所以换算只在这一处发生。
func (s *Service) nowSec() int64 { return s.now() / 1000 }

// Handle 按操作分派，是 MQTT 路径的入口。
func (s *Service) Handle(ctx context.Context, req Request, payload []byte) *Outcome {
	switch req.Op {
	case OpGet:
		return s.Get(ctx, req, payload)
	case OpUpdate:
		return s.Update(ctx, req, payload)
	case OpDelete:
		return s.Delete(ctx, req, payload)
	default:
		return s.reject(req, "", ErrInternal)
	}
}

// reject 构造一个被拒的结果。
func (s *Service) reject(req Request, token string, e *Error) *Outcome {
	body, err := Encode(e.Response(token))
	if err != nil {
		// 连错误响应都编码不出来，只能退回一个最小的固定载荷
		body = []byte(`{"code":500,"message":"Internal service failure"}`)
	}
	return &Outcome{
		ResponseTopic: req.RejectedTopic(),
		Response:      body,
		Err:           e,
		HTTPStatus:    e.Code,
	}
}

// accept 构造一个被接受的结果。
func (s *Service) accept(req Request, v any) *Outcome {
	body, err := Encode(v)
	if err != nil {
		return s.reject(req, "", ErrInternal)
	}
	return &Outcome{
		ResponseTopic: req.AcceptedTopic(),
		Response:      body,
		HTTPStatus:    200,
	}
}

// precheck 做与操作无关的前置校验与限速。
//
// 顺序有讲究：先校验名字（廉价、纯本地），再查限速。反过来会让
// 一串格式错误的请求消耗掉设备的限速额度，把一个客户端 bug
// 变成该设备的拒绝服务。
func (s *Service) precheck(ctx context.Context, req Request) *Error {
	if err := ValidateThingName(req.Thing); err != nil {
		return ErrInvalidThingName
	}
	if err := ValidateShadowName(req.Shadow); err != nil {
		return ErrInvalidShadowName
	}
	// 每 thing 速率（QTA-063，本地作用域）
	if !s.lim.AllowLocal(quota.ShadowRatePerThing, req.Thing, 1) {
		return ErrTooManyRequests
	}
	// 全局 API 速率（QTA-065，全局作用域）
	if !s.lim.AllowGlobal(ctx, quota.ShadowAPIRate, "", 1) {
		return ErrTooManyRequests
	}
	return nil
}

// parseToken 从请求载荷中取 clientToken。
//
// get 与 delete 的载荷通常为空；若非空则须是合法 JSON 对象，
// 因为 AWS 会从其中读取 clientToken。
func parseToken(payload []byte) (string, Object, *Error) {
	if len(payload) == 0 {
		return "", nil, nil
	}
	obj, err := Decode(payload)
	if err != nil {
		return "", nil, ErrInvalidJSON
	}
	tok := ""
	if v, ok := obj["clientToken"]; ok {
		sv, isStr := v.(string)
		if !isStr {
			return "", nil, ErrInvalidClientToken
		}
		tok = sv
	}
	if err := validateClientToken(tok); err != nil {
		return "", nil, ErrInvalidClientToken
	}
	return tok, obj, nil
}

// Get 读取影子（PRD SHDW-010、SHDW-040）。
func (s *Service) Get(ctx context.Context, req Request, payload []byte) *Outcome {
	token, _, e := parseToken(payload)
	if e != nil {
		return s.reject(req, "", e)
	}
	if e := s.precheck(ctx, req); e != nil {
		return s.reject(req, token, e)
	}
	doc, ok, err := s.get(ctx, req.Thing, req.Shadow)
	if err != nil {
		return s.reject(req, token, ErrInternal)
	}
	if !ok {
		return s.reject(req, token, errNotFound(req.Shadow))
	}
	return s.accept(req, doc.get(token, s.nowSec()))
}

func (s *Service) get(ctx context.Context, thing, name string) (*Document, bool, error) {
	s.partMu.RLock()
	defer s.partMu.RUnlock()
	owner, local := s.part.Owner(thing), s.part.IsLocal(thing)
	if !local {
		if s.fwdGet == nil {
			// ForwardGet 是分片集群装配才提供的能力。留空时保持旧版
			// 单机/全量副本语义：即使写 owner 在远端，本节点仍可读取
			// 自己的完整物化视图。这样滚动升级期间不会把正常的 404
			// 误报成 500；真正的 RF=3 装配始终会注入 ForwardGet。
			doc, ok := s.store.Get(thing, name)
			return doc, ok, nil
		}
		return s.fwdGet(ctx, owner, thing, name)
	}
	if s.localGet != nil {
		return s.localGet(ctx, thing, name)
	}
	doc, ok := s.store.Get(thing, name)
	return doc, ok, nil
}

// Update 更新影子（PRD SHDW-012、SHDW-041）。
func (s *Service) Update(ctx context.Context, req Request, payload []byte) *Outcome {
	// 大小检查在解析之前：先解析一个 10 MB 的文档再说它超限，
	// 等于让攻击者用一个请求换走服务端的解析开销（QTA-060）。
	if !s.lim.Within(quota.ShadowDocSize, len(payload)) {
		return s.reject(req, "", ErrPayloadTooLarge)
	}
	obj, err := Decode(payload)
	if err != nil {
		return s.reject(req, "", ErrInvalidJSON)
	}
	token := ""
	if v, ok := obj["clientToken"]; ok {
		sv, isStr := v.(string)
		if !isStr || validateClientToken(sv) != nil {
			return s.reject(req, "", ErrInvalidClientToken)
		}
		token = sv
	}
	if e := s.precheck(ctx, req); e != nil {
		return s.reject(req, token, e)
	}

	state, e := extractState(obj)
	if e != nil {
		return s.reject(req, token, e)
	}
	if d := Depth(state); d > MaxDepth {
		return s.reject(req, token, errDepthExceeded())
	}

	expect, e := extractVersion(obj)
	if e != nil {
		return s.reject(req, token, e)
	}

	cmd := Command{
		Kind: CmdUpdate, Thing: req.Thing, Shadow: req.Shadow,
		Patch: state, ExpectVersion: expect,
		NowSec: s.nowSec(), ClientToken: token,
	}
	res, e := s.apply(ctx, cmd)
	if e != nil {
		return s.reject(req, token, e)
	}

	out := s.accept(req, &UpdateResponse{
		// 回传的是请求中的那些段，不是完整文档
		State: stateSection{
			Desired:  cloneSection(state, "desired"),
			Reported: cloneSection(state, "reported"),
		},
		// 元数据由**请求**构造，因此包含 null 叶子的时间戳
		// （见 merge.go 规则 6）。用已存储的元数据会漏掉这些叶子，
		// 而 SDK 据此确认"我发的每个字段都被处理了"。
		Metadata: metaSection{
			Desired:  metaSectionOf(state, "desired", cmd.NowSec),
			Reported: metaSectionOf(state, "reported", cmd.NowSec),
		},
		Version:     res.Version,
		Timestamp:   cmd.NowSec,
		ClientToken: token,
	})
	out.Publishes = s.updatePublishes(req, res, cmd)
	s.changed(req.Thing, "update")
	return out
}

// updatePublishes 构造 delta 与 documents 两条消息。
func (s *Service) updatePublishes(req Request, res *Result, cmd Command) []Publish {
	var out []Publish

	// PRD SHDW-027：仅在 desired 与 reported 存在实际差异时发 delta
	if len(res.Delta) > 0 {
		if body, err := Encode(&DeltaMessage{
			Version: res.Version, Timestamp: cmd.NowSec,
			State: res.Delta, Metadata: res.DeltaMeta,
			ClientToken: cmd.ClientToken,
		}); err == nil {
			out = append(out, Publish{
				Topic: req.DeltaTopic(), Payload: body, QoS1: true,
			})
		}
	}

	// PRD SHDW-015：更新成功后发布 previous 与 current 双文档
	msg := &DocumentsMessage{
		Current: res.Current.snapshot(), Timestamp: cmd.NowSec,
		ClientToken: cmd.ClientToken,
	}
	if res.Previous != nil {
		msg.Previous = res.Previous.snapshot()
	}
	if body, err := Encode(msg); err == nil {
		out = append(out, Publish{
			Topic: req.DocumentsTopic(), Payload: body, QoS1: true,
		})
	}
	return out
}

// Delete 删除影子（PRD SHDW-016、SHDW-042）。
func (s *Service) Delete(ctx context.Context, req Request, payload []byte) *Outcome {
	token, obj, e := parseToken(payload)
	if e != nil {
		return s.reject(req, "", e)
	}
	if e := s.precheck(ctx, req); e != nil {
		return s.reject(req, token, e)
	}
	expect := int64(0)
	if obj != nil {
		if expect, e = extractVersion(obj); e != nil {
			return s.reject(req, token, e)
		}
	}
	res, e := s.apply(ctx, Command{
		Kind: CmdDelete, Thing: req.Thing, Shadow: req.Shadow,
		ExpectVersion: expect, NowSec: s.nowSec(), ClientToken: token,
	})
	if e != nil {
		return s.reject(req, token, e)
	}
	out := s.accept(req, &DeleteResponse{
		Version: res.Version, Timestamp: s.nowSec(), ClientToken: token,
	})
	s.changed(req.Thing, "delete")
	return out
}

func (s *Service) changed(thing, op string) {
	if s.onChange != nil {
		s.onChange(thing, op)
	}
}

// ListNamedShadows 列出一个 thing 的命名影子（PRD SHDW-043）。
func (s *Service) ListNamedShadows(ctx context.Context, thing string) ([]string, *Error) {
	if err := ValidateThingName(thing); err != nil {
		return nil, ErrInvalidThingName
	}
	if !s.lim.AllowLocal(quota.ShadowRatePerThing, thing, 1) {
		return nil, ErrTooManyRequests
	}
	if !s.lim.AllowGlobal(ctx, quota.ShadowAPIRate, "", 1) {
		return nil, ErrTooManyRequests
	}
	s.partMu.RLock()
	defer s.partMu.RUnlock()
	owner, local := s.part.Owner(thing), s.part.IsLocal(thing)
	var names []string
	var err error
	switch {
	case !local && s.fwdList == nil:
		// 与 get 相同，未装配远程读表示旧版全量物化视图。
		names = s.store.List(thing)
	case !local:
		names, err = s.fwdList(ctx, owner, thing)
	case s.localList != nil:
		names, err = s.localList(ctx, thing)
	default:
		names = s.store.List(thing)
	}
	if err != nil {
		if e, ok := err.(*Error); ok {
			return nil, e
		}
		return nil, ErrInternal
	}
	return names, nil
}

// apply 落实一条命令，必要时转发给分区 owner。
//
// 这里就是契约 06 的两条分支（PRD SHDW-060）。单机下
// Partitioner.IsLocal 恒为真，转发分支不会进入，但它存在——
// 它决定了本函数的签名带 ctx、返回 error，以及调用方必须处理
// "写入可能发生在别的节点上"这件事。
func (s *Service) apply(ctx context.Context, cmd Command) (*Result, *Error) {
	s.partMu.RLock()
	defer s.partMu.RUnlock()
	key := cmd.Thing
	owner, local := s.part.Owner(key), s.part.IsLocal(key)
	if !local {
		if s.fwd == nil {
			return nil, errForwardFailed(string(owner))
		}
		end := func(error) {}
		if s.onFwd != nil {
			ctx, end = s.onFwd(ctx, string(owner), cmd.Thing)
		}
		res, err := s.fwd(ctx, owner, cmd)
		if end != nil {
			end(err)
		}
		if err != nil {
			if e, ok := err.(*Error); ok {
				return nil, e
			}
			return nil, errForwardFailed(string(owner))
		}
		return res, nil
	}
	res, err := s.store.Apply(ctx, cmd)
	if err != nil {
		if e, ok := err.(*Error); ok {
			return nil, e
		}
		return nil, ErrInternal
	}
	return res, nil
}

// extractState 取出并校验 state 节点。
func extractState(obj Object) (Object, *Error) {
	v, ok := obj["state"]
	if !ok {
		return nil, ErrMissingState
	}
	if v == nil {
		return nil, ErrStateNotObject
	}
	state, ok := v.(map[string]any)
	if !ok {
		return nil, ErrStateNotObject
	}
	if len(state) == 0 {
		return nil, ErrEmptyState
	}
	// 只接受 desired 与 reported。未知段须报错而不是忽略——
	// 一个把 "reported" 拼成 "report" 的设备会以为上报成功了，
	// 而影子里什么都没变，这种故障没有任何可观测症状。
	for k := range state {
		if k != "desired" && k != "reported" {
			return nil, ErrUnsupportedSection
		}
	}
	return state, nil
}

// extractVersion 取出请求携带的 version。
func extractVersion(obj Object) (int64, *Error) {
	v, ok := obj["version"]
	if !ok || v == nil {
		return 0, nil
	}
	num, ok := v.(interface{ Int64() (int64, error) })
	if !ok {
		return 0, &Error{400, "Invalid version"}
	}
	n, err := num.Int64()
	if err != nil || n < 0 {
		return 0, &Error{400, "Invalid version"}
	}
	return n, nil
}

// cloneSection 取出 patch 中一个段的副本。
func cloneSection(state Object, name string) Object {
	v, ok := state[name]
	if !ok {
		return nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return CloneObject(obj)
}

// metaSectionOf 为 patch 中的一个段构造元数据。
func metaSectionOf(state Object, name string, nowSec int64) Object {
	v, ok := state[name]
	if !ok {
		return nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	m, _ := metaFor(obj, nowSec).(map[string]any)
	return m
}
