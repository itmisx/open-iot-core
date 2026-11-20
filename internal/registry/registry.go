package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

// ErrNotFound 表示记录不存在。
var ErrNotFound = errors.New("registry: 记录不存在")

// ErrInUse 表示记录仍被引用，不能删除。
var ErrInUse = errors.New("registry: 记录仍被引用")

// ErrAlreadyExists 表示要创建的记录已经存在。
var ErrAlreadyExists = errors.New("registry: 记录已存在")

// ErrVersionConflict 表示调用方看到的版本已经过期。
var ErrVersionConflict = errors.New("registry: 版本冲突")

// ErrLimitExceeded 表示已经达到本节点的数量上限。
var ErrLimitExceeded = errors.New("registry: 超出上限")

// ErrIndexOff 表示检索索引没有打开。
var ErrIndexOff = errors.New("registry: 检索未开启")

// ErrBadQuery 表示检索语句无法执行。
var ErrBadQuery = errors.New("registry: 检索语句无法解析")

// ErrBadPolicy 表示策略文档编译不过。
var ErrBadPolicy = errors.New("registry: 策略文档无法编译")

// ErrDefaultVersion 表示调用方要删掉当前生效的策略版本。
var ErrDefaultVersion = errors.New("registry: 不能删除默认策略版本")

// maxPolicyVersions 与 AWS IoT 的每份策略五个版本一致。
const maxPolicyVersions = 5

// Options 是注册表装配参数。
type Options struct {
	KV   store.KV
	Meta meta.Store
	// Env 是策略编译环境（ARN 的分区、区域、账号）。
	Env authz.Env
	// Now 提供当前时间（Unix 毫秒）。核心包不直接读时钟（契约 08）。
	Now func() int64
	// CASubjectLimit 是同一 subject 可以注册的 CA 数。0 表示 10（PKI-041）。
	CASubjectLimit int
}

// Registry 是设备、证书与策略的控制面。
type Registry struct {
	kv             store.KV
	meta           meta.Store
	env            authz.Env
	now            func() int64
	caSubjectLimit int

	// mu 串行化全部写入。
	//
	// 写入要同时更新冷层与热视图，这两步必须原子：中间的窗口里
	// 冷层说"已吊销"而热视图还放行，正是吊销失效的样子。
	// 控制面的写入频率很低（人工操作与开通流程），
	// 一把全局锁完全够用，换来的是推理上的简单。
	mu sync.Mutex
}

// policyEntryLocked 查一份策略的编号与编译结果。
//
// 两处都不是本包的内存状态：编号来自冷层记录（权威），编译结果来自
// 热视图（由 apply 阶段编译并装进去）。
//
// 这里曾经是一张内存映射，启动时从冷层重建、写入时更新。集群下它立刻
// 成了第二个真相：跟随者的热视图与冷层都靠日志追平，而那张映射只在
// 本节点自己写入时才更新——于是在跟随者上建设备会说「策略不存在」，
// 而那份策略明明在冷层里躺着（CLU-072）。同一类教训在订阅状态上吃过
// 一次（MQTT-105 至 MQTT-107）：同一份事实散在几处而没有一处是权威。
func (r *Registry) policyEntryLocked(name string) (meta.PolicySetID, *authz.CompiledSet, bool) {
	if name == "" {
		return meta.NoPolicySet, nil, false
	}
	raw, err := r.kv.Get(store.Key(store.PrefixPolicy, name))
	if err != nil {
		return meta.NoPolicySet, nil, false
	}
	var rec policyRecord
	if decodeJSON(raw, &rec) != nil {
		return meta.NoPolicySet, nil, false
	}
	id := meta.PolicySetID(rec.ID)
	ps, ok := r.meta.View().PolicySet(id)
	if !ok {
		// 冷层有记录而热视图没有：热视图还没追平到这条命令。
		// 返回 false 让调用方拒绝写入，而不是拿一个空匹配器往下走。
		return id, nil, false
	}
	cs, _ := ps.Compiled.(*authz.CompiledSet)
	return id, cs, cs != nil
}

// New 创建注册表并从冷层恢复。
func New(opt Options) (*Registry, error) {
	switch {
	case opt.KV == nil:
		return nil, errors.New("registry: 缺少 KV")
	case opt.Meta == nil:
		return nil, errors.New("registry: 缺少 Meta")
	case opt.Now == nil:
		return nil, errors.New("registry: 缺少时钟（契约 08 要求注入）")
	}
	limit := opt.CASubjectLimit
	if limit <= 0 {
		limit = 10
	}
	r := &Registry{
		kv: opt.KV, meta: opt.Meta, env: opt.Env, now: opt.Now,
		caSubjectLimit: limit,
	}
	return r, nil
}

// ---------- 策略 ----------

type policyVersionRec struct {
	ID      string `json:"id"`
	Doc     []byte `json:"doc"`
	Created int64  `json:"created"`
}

type policyRecord struct {
	V              int                `json:"v"`
	Name           string             `json:"name"`
	Doc            []byte             `json:"doc"`
	ID             uint64             `json:"id"`
	Created        int64              `json:"created"`
	Updated        int64              `json:"updated"`
	DefaultVersion string             `json:"defaultVersion,omitempty"`
	Versions       []policyVersionRec `json:"versions,omitempty"`
}

// normalizeVersions 把旧记录补成「只有版本 1」的形状。
//
// 版本字段是后加的。不补的话，升级前写进去的策略在列举版本时是空的，
// 而它其实正在生效。
func (rec *policyRecord) normalizeVersions() {
	if len(rec.Versions) > 0 {
		if rec.DefaultVersion == "" {
			rec.DefaultVersion = rec.Versions[0].ID
		}
		return
	}
	rec.DefaultVersion = "1"
	rec.Versions = []policyVersionRec{{
		ID: "1", Doc: rec.Doc, Created: rec.Created,
	}}
}

func (rec *policyRecord) version(id string) (policyVersionRec, bool) {
	for _, v := range rec.Versions {
		if v.ID == id {
			return v, true
		}
	}
	return policyVersionRec{}, false
}

func nextVersionID(vs []policyVersionRec) string {
	max := 0
	for _, v := range vs {
		n := 0
		for i := 0; i < len(v.ID); i++ {
			c := v.ID[i]
			if c < '0' || c > '9' {
				n = 0
				break
			}
			n = n*10 + int(c-'0')
		}
		if n > max {
			max = n
		}
	}
	return fmt.Sprintf("%d", max+1)
}

// PutPolicy 创建或更新一份策略。
//
// 编译失败即拒绝：把一份编译不过的策略写进去，等于让该策略下的
// 所有设备在下次重启时静默降权或提权（PRD REG-103）。
func (r *Registry) PutPolicy(ctx context.Context, name string, doc []byte) error {
	return r.writePolicy(ctx, name, doc, false)
}

// CreatePolicy 只在策略不存在时创建，并把它的第一版设为默认。
func (r *Registry) CreatePolicy(ctx context.Context, name string, doc []byte) error {
	return r.writePolicy(ctx, name, doc, true)
}

func (r *Registry) writePolicy(ctx context.Context, name string, doc []byte, createOnly bool) error {
	if err := ValidatePolicyName(name); err != nil {
		return err
	}
	cs, err := authz.CompileJSON(name, doc, r.env)
	if err != nil {
		return fmt.Errorf("%w: 策略 %s 编译失败: %w", ErrBadPolicy, name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	id, rec, counters, err := r.policyIDLocked(name)
	if err != nil {
		return err
	}
	now := r.now()
	if rec == nil {
		rec = &policyRecord{
			V: policyRecordVersion, Name: name, ID: uint64(id), Created: now,
			DefaultVersion: "1",
			Versions: []policyVersionRec{{
				ID: "1", Doc: append([]byte(nil), doc...), Created: now,
			}},
		}
	} else if createOnly {
		return ErrAlreadyExists
	} else {
		// 管理面的更新改写当前默认版的正文，不另占一个版本号。
		// 版本号留给兼容面的 CreatePolicyVersion，否则几次保存就把五个名额用完。
		rec.normalizeVersions()
		replaced := false
		for i := range rec.Versions {
			if rec.Versions[i].ID == rec.DefaultVersion {
				rec.Versions[i].Doc = append([]byte(nil), doc...)
				replaced = true
				break
			}
		}
		if !replaced {
			rec.Versions = append(rec.Versions, policyVersionRec{
				ID: rec.DefaultVersion, Doc: append([]byte(nil), doc...), Created: now,
			})
		}
	}
	rec.Doc = append([]byte(nil), doc...)
	rec.Updated = now
	return r.commitPolicyLocked(ctx, id, rec, cs, counters...)
}

// commitPolicyLocked 把策略的热视图与冷层改动放进同一条命令。
//
// counters 带上计数器的推进（新分配编号时），它也必须被复制：
// 各节点各自读-改-写计数器会把同一个编号分配两次，而编号正被证书与
// 设备记录引用着（REG-112）。
func (r *Registry) commitPolicyLocked(ctx context.Context, id meta.PolicySetID,
	rec *policyRecord, cs *authz.CompiledSet, counters ...meta.RecordOp,
) error {
	op, err := setOp(store.Key(store.PrefixPolicy, rec.Name), rec)
	if err != nil {
		return err
	}
	if _, err := r.meta.Apply(ctx, meta.Command{
		Kind:    meta.CmdSetPolicySet,
		Policy:  &meta.PolicyCmd{ID: id, Name: rec.Name, Doc: rec.Doc},
		Records: append([]meta.RecordOp{op}, counters...),
	}); err != nil {
		return fmt.Errorf("registry: 写入策略热视图失败: %w", err)
	}
	return r.syncAttrSlotsLocked(ctx)
}

// policyIDLocked 取得策略的编号；已存在时连记录一起返回。
//
// 编号跨重启稳定且单调：它被证书与设备记录引用，
// 重用已删除策略的编号会让旧引用指向新策略——设备拿到别人的权限。
//
// 第三个返回值是「若要用这个新编号，必须一起提交的计数器推进」。
// 它不在这里写进 KV：那样计数器的推进就不在复制日志里，而集群下
// 两个节点会各自算出同一个编号（REG-112）。已存在的策略返回 nil。
func (r *Registry) policyIDLocked(name string) (meta.PolicySetID, *policyRecord, []meta.RecordOp, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixPolicy, name))
	switch {
	case err == nil:
		var rec policyRecord
		if derr := decodeJSON(raw, &rec); derr != nil {
			return 0, nil, nil, fmt.Errorf("registry: 策略记录损坏 %s: %w", name, derr)
		}
		return meta.PolicySetID(rec.ID), &rec, nil, nil
	case errors.Is(err, store.ErrNotFound):
	default:
		return 0, nil, nil, err
	}
	cur, err := store.PeekCounter(r.kv, "policyset")
	if err != nil {
		return 0, nil, nil, err
	}
	next := cur + 1
	return meta.PolicySetID(next), nil, []meta.RecordOp{{
		Key: store.CounterKey("policyset"), Value: store.EncodeCounter(next),
	}}, nil
}

// GetPolicy 读取一份策略。
func (r *Registry) GetPolicy(name string) (*Policy, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixPolicy, name))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec policyRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return nil, fmt.Errorf("registry: 策略记录损坏 %s: %w", name, err)
	}
	rec.normalizeVersions()
	doc := rec.Doc
	if v, ok := rec.version(rec.DefaultVersion); ok {
		doc = v.Doc
	}
	return &Policy{Name: rec.Name, Document: doc,
		CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated,
		DefaultVersion: rec.DefaultVersion}, nil
}

// CreatePolicyVersion 追加一个版本。setDefault 为真时它成为生效版本。
//
// 不设为默认时热视图不动：调用方要的是先把文档存下来，再决定要不要切换。
// 立刻编译进去会让一次「仅保存」变成一次权限变更。
func (r *Registry) CreatePolicyVersion(ctx context.Context, name string, doc []byte, setDefault bool) (string, error) {
	if _, err := authz.CompileJSON(name, doc, r.env); err != nil {
		return "", fmt.Errorf("%w: 策略 %s 编译失败: %w", ErrBadPolicy, name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id, rec, _, err := r.policyIDLocked(name)
	if err != nil {
		return "", err
	}
	if rec == nil {
		return "", ErrNotFound
	}
	rec.normalizeVersions()
	if len(rec.Versions) >= maxPolicyVersions {
		return "", fmt.Errorf("%w: 策略 %s 已有 %d 个版本", ErrLimitExceeded, name, maxPolicyVersions)
	}
	ver := nextVersionID(rec.Versions)
	now := r.now()
	rec.Versions = append(rec.Versions, policyVersionRec{
		ID: ver, Doc: append([]byte(nil), doc...), Created: now,
	})
	rec.Updated = now
	if !setDefault {
		// 只保存文档，热视图不动：立刻生效会让一次"仅保存"变成一次权限变更。
		op, oerr := setOp(store.Key(store.PrefixPolicy, name), rec)
		if oerr != nil {
			return "", oerr
		}
		if cerr := r.commitRecordsLocked(ctx, op); cerr != nil {
			return "", cerr
		}
		return ver, nil
	}
	cs, err := authz.CompileJSON(name, doc, r.env)
	if err != nil {
		return "", fmt.Errorf("%w: 策略 %s 编译失败: %w", ErrBadPolicy, name, err)
	}
	rec.DefaultVersion = ver
	rec.Doc = append([]byte(nil), doc...)
	if err := r.commitPolicyLocked(ctx, id, rec, cs); err != nil {
		return "", err
	}
	return ver, nil
}

// GetPolicyVersion 读取一个版本。
func (r *Registry) GetPolicyVersion(name, versionID string) (*PolicyVersion, error) {
	rec, err := r.policyRecord(name)
	if err != nil {
		return nil, err
	}
	v, ok := rec.version(versionID)
	if !ok {
		return nil, ErrNotFound
	}
	return &PolicyVersion{
		ID: v.ID, Document: v.Doc, IsDefault: v.ID == rec.DefaultVersion,
		CreatedAtUnixMs: v.Created,
	}, nil
}

// ListPolicyVersions 按版本号顺序列出全部版本。
func (r *Registry) ListPolicyVersions(name string) ([]PolicyVersion, error) {
	rec, err := r.policyRecord(name)
	if err != nil {
		return nil, err
	}
	out := make([]PolicyVersion, 0, len(rec.Versions))
	for _, v := range rec.Versions {
		out = append(out, PolicyVersion{
			ID: v.ID, Document: v.Doc, IsDefault: v.ID == rec.DefaultVersion,
			CreatedAtUnixMs: v.Created,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return versionLess(out[i].ID, out[j].ID)
	})
	return out, nil
}

func versionLess(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

// SetDefaultPolicyVersion 切换生效版本。热视图改成这一版的文档。
func (r *Registry) SetDefaultPolicyVersion(ctx context.Context, name, versionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, rec, _, err := r.policyIDLocked(name)
	if err != nil {
		return err
	}
	if rec == nil {
		return ErrNotFound
	}
	rec.normalizeVersions()
	v, ok := rec.version(versionID)
	if !ok {
		return ErrNotFound
	}
	if rec.DefaultVersion == versionID {
		return nil
	}
	cs, err := authz.CompileJSON(name, v.Doc, r.env)
	if err != nil {
		return fmt.Errorf("%w: 策略 %s 编译失败: %w", ErrBadPolicy, name, err)
	}
	rec.DefaultVersion = versionID
	rec.Doc = append([]byte(nil), v.Doc...)
	rec.Updated = r.now()
	return r.commitPolicyLocked(ctx, id, rec, cs)
}

// DeletePolicyVersion 删掉一个非默认版本。
func (r *Registry) DeletePolicyVersion(name, versionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, rec, _, err := r.policyIDLocked(name)
	if err != nil {
		return err
	}
	if rec == nil {
		return ErrNotFound
	}
	rec.normalizeVersions()
	if versionID == rec.DefaultVersion {
		return ErrDefaultVersion
	}
	kept := rec.Versions[:0]
	found := false
	for _, v := range rec.Versions {
		if v.ID == versionID {
			found = true
			continue
		}
		kept = append(kept, v)
	}
	if !found {
		return ErrNotFound
	}
	rec.Versions = kept
	rec.Updated = r.now()
	op, err := setOp(store.Key(store.PrefixPolicy, name), rec)
	if err != nil {
		return err
	}
	return r.commitRecordsLocked(context.Background(), op)
}

func (r *Registry) policyRecord(name string) (*policyRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixPolicy, name))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec policyRecord
	if err := decodeJSON(raw, &rec); err != nil {
		return nil, fmt.Errorf("registry: 策略记录损坏 %s: %w", name, err)
	}
	rec.normalizeVersions()
	return &rec, nil
}

// ListPolicies 列出全部策略名，按字典序。
func (r *Registry) ListPolicies() ([]string, error) {
	var out []string
	err := store.ScanPrefix(r.kv, store.PrefixPolicy, "", func(parts []string, _ []byte) bool {
		out = append(out, parts[0])
		return true
	})
	sort.Strings(out)
	return out, err
}

// DeletePolicy 删除一份策略。
//
// 仍被证书或设备引用时拒绝（PRD REG-005 的同类约束）：
// 删掉被引用的策略会让那些设备在下次连接时因"策略不存在"被拒，
// 而错误信息指向设备而不是指向这次删除。
func (r *Registry) DeletePolicy(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	users, err := r.policyUsersLocked(name, 3)
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return fmt.Errorf("%w: 策略 %s 仍被 %s 引用；请先解除绑定",
			ErrInUse, name, strings.Join(users, "、"))
	}
	raw, err := r.kv.Get(store.Key(store.PrefixPolicy, name))
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	del := deleteOp(store.Key(store.PrefixPolicy, name))
	var rec policyRecord
	if err := decodeJSON(raw, &rec); err == nil {
		if _, err := r.meta.Apply(ctx, meta.Command{
			Kind:    meta.CmdDeletePolicySet,
			Policy:  &meta.PolicyCmd{ID: meta.PolicySetID(rec.ID), Name: name},
			Records: []meta.RecordOp{del},
		}); err != nil {
			return fmt.Errorf("registry: 删除策略热视图失败: %w", err)
		}
	} else if cerr := r.commitRecordsLocked(ctx, del); cerr != nil {
		// 记录坏到连编号都读不出来时热视图里没有对应的策略集，
		// 但冷层那条坏记录仍要删掉，否则它会一直挡着同名策略的创建。
		return cerr
	}
	return nil
}

// policyUsersLocked 找出引用某策略的实体，最多返回 limit 个。
//
// 只取前几个即可：错误信息里列三个足以让人知道该去哪里解绑，
// 而全量扫描在百万设备下会让一次删除请求挂很久。
func (r *Registry) policyUsersLocked(name string, limit int) ([]string, error) {
	var out []string
	err := store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
		var rec thingRecord
		if decodeJSON(value, &rec) == nil && rec.PolicyName == name {
			out = append(out, "设备 "+parts[0])
		}
		return len(out) < limit
	})
	if err != nil || len(out) >= limit {
		return out, err
	}
	err = store.ScanPrefix(r.kv, store.PrefixCert, "", func(parts []string, value []byte) bool {
		var rec certRecord
		if decodeJSON(value, &rec) == nil && rec.PolicyName == name {
			out = append(out, "证书 "+shortFP(parts[0]))
		}
		return len(out) < limit
	})
	return out, err
}

// ---------- 恢复 ----------

// Restore 从冷层把全部记录装回热视图。
//
// 启动时调用（仅在元数据层没有复制日志时，见 HydrateHot 的说明）。
// 顺序是策略在前：证书与设备按策略名绑定，策略尚未登记时它们的绑定
// 会落空，而落空的表现是"设备连不上"。
func (r *Registry) Restore(ctx context.Context) (Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scanColdLocked(func(cmd meta.Command) error {
		_, err := r.meta.Apply(ctx, cmd)
		return err
	})
}

/*
HydrateHot 用冷层的内容重建热视图，**不经过写入路径**（CLU-098）。

它与 Restore 的区别只有一处，而那一处很要紧：Restore 把每条记录都走一遍
r.meta.Apply，也就是**往复制日志里提一条新命令**。单机下那只是让 revision
每次重启跳一截；集群下的后果严重得多——一个冷层里已经有一百万条记录的
节点重启时，会把这一百万条重新提进日志（命令没有 ID，幂等窗口认不出它们
是同一件事），而在跟随者上这些提案还要逐条转发给 leader。

这个回调装给 meta.ReplicatedStore 的 Hydrate：装快照时冷层已经是权威，
热视图只需从它派生，一条日志都不该写。
*/
func (r *Registry) HydrateHot(apply func(meta.Command) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.scanColdLocked(apply)
	return err
}

// CountCold 只数冷层里有多少条记录，不碰热视图、不写日志。
//
// 它存在的唯一用处是启动日志里的那几个数字：文件化注册表导入之后，
// 重建时拿到的统计已经过期（少了刚导入的那些），而重新走一遍重建
// 只为了数数是把一次全库扫描变成两次写入路径重放。
//
// 复用 scanColdLocked 而不另写一个计数器：两份扫描的结局是只改一份，
// 而「数漏了一类记录」的表现是启动日志上的数字与实际不符——那正是
// 运维用来确认「证书都在」的那个数字。
func (r *Registry) CountCold() (Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scanColdLocked(func(meta.Command) error { return nil })
}

// scanColdLocked 扫冷层并按「策略、设备、证书」的顺序发出热视图命令。
//
// 抽出来是为了让 Restore 与 HydrateHot 共用同一份扫描：两处各写一遍的
// 结局是只改一份，而「重建时漏了一类记录」的表现是那一类对象在重建后
// 的节点上静默消失——证书漏了就是整批设备连不上。
func (r *Registry) scanColdLocked(apply func(meta.Command) error) (Stats, error) {
	var st Stats
	// 1. 策略
	var policyErr error
	err := store.ScanPrefix(r.kv, store.PrefixPolicy, "", func(parts []string, value []byte) bool {
		var rec policyRecord
		if derr := decodeJSON(value, &rec); derr != nil {
			policyErr = fmt.Errorf("策略 %s 记录损坏: %w", parts[0], derr)
			return false
		}
		if _, cerr := authz.CompileJSON(rec.Name, rec.Doc, r.env); cerr != nil {
			// 启动时编译失败必须让启动失败，而不是跳过这份策略：
			// 跳过等于让该策略下的所有设备静默失去全部权限。
			// 这里只为提前报错——真正装进热视图的那次编译在 apply 阶段。
			policyErr = fmt.Errorf("策略 %s 编译失败: %w", rec.Name, cerr)
			return false
		}
		if aerr := apply(meta.Command{
			Kind:   meta.CmdSetPolicySet,
			Policy: &meta.PolicyCmd{ID: meta.PolicySetID(rec.ID), Name: rec.Name, Doc: rec.Doc},
		}); aerr != nil {
			policyErr = aerr
			return false
		}
		st.Policies++
		return true
	})
	if err != nil {
		return st, err
	}
	if policyErr != nil {
		return st, fmt.Errorf("registry: %w", policyErr)
	}

	// 2. 设备
	var thingErr error
	err = store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
		var rec thingRecord
		if derr := decodeJSON(value, &rec); derr != nil {
			thingErr = fmt.Errorf("设备 %s 记录损坏: %w", parts[0], derr)
			return false
		}
		if aerr := r.emitThingLocked(apply, &rec); aerr != nil {
			thingErr = aerr
			return false
		}
		st.Things++
		return true
	})
	if err != nil {
		return st, err
	}
	if thingErr != nil {
		return st, fmt.Errorf("registry: %w", thingErr)
	}

	// 3. 证书
	var certErr error
	err = store.ScanPrefix(r.kv, store.PrefixCert, "", func(parts []string, value []byte) bool {
		var rec certRecord
		if derr := decodeJSON(value, &rec); derr != nil {
			certErr = fmt.Errorf("证书 %s 记录损坏: %w", shortFP(parts[0]), derr)
			return false
		}
		if aerr := r.emitCertLocked(apply, &rec); aerr != nil {
			certErr = aerr
			return false
		}
		st.Certificates++
		if rec.Status == StatusActive {
			st.ActiveCertificates++
		}
		return true
	})
	if err != nil {
		return st, err
	}
	if certErr != nil {
		return st, fmt.Errorf("registry: %w", certErr)
	}
	return st, nil
}

// Stats 是注册表的内容统计，供启动日志与控制台总览使用。
type Stats struct {
	Policies           int
	Things             int
	Certificates       int
	ActiveCertificates int
}

// ---------- 小工具 ----------

// setOp 把一条记录编成随命令复制的冷层写入。
//
// 控制面不再自己写 KV：冷层记录与热视图的改动必须落在同一条已提交日志里
// （REG-110、CLU-065）。单机下这条日志是一副本组，行为与过去逐个 Set
// 完全一致；集群下它才是 follower 能读到记录的唯一原因。
func setOp(key []byte, v any) (meta.RecordOp, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return meta.RecordOp{}, fmt.Errorf("registry: 编码失败: %w", err)
	}
	return meta.RecordOp{Key: key, Value: b}, nil
}

// deleteOp 把一次删除编成随命令复制的冷层写入。
func deleteOp(key []byte) meta.RecordOp {
	return meta.RecordOp{Key: key, Delete: true}
}

// commitRecordsLocked 提交只动冷层的记录。
//
// 角色别名、已注册 CA、检索开关这几样不进热视图，但在集群里同样必须
// 复制——否则它们只存在于收到请求的那一个节点上。CmdNop 正是为此：
// 它只推进 revision，而 Records 跟着同一条日志走。
func (r *Registry) commitRecordsLocked(ctx context.Context, ops ...meta.RecordOp) error {
	if len(ops) == 0 {
		return nil
	}
	if _, err := r.meta.Apply(ctx, meta.Command{
		Kind: meta.CmdNop, Records: ops,
	}); err != nil {
		return fmt.Errorf("registry: 提交记录失败: %w", err)
	}
	return nil
}

func decodeJSON(raw []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// shortFP 截短指纹用于日志与错误信息。
func shortFP(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16] + "…"
}

// PolicySetID 返回策略名对应的编号。
//
// 供需要直接对策略集求值的调用方使用（如策略干跑）。
func (r *Registry) PolicySetID(name string) (meta.PolicySetID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, _, ok := r.policyEntryLocked(name)
	return id, ok
}

// CompiledPolicy 返回策略名对应的编译结果。
func (r *Registry) CompiledPolicy(name string) (*authz.CompiledSet, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, cs, ok := r.policyEntryLocked(name)
	return cs, ok
}
