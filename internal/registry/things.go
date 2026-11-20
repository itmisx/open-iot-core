package registry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

type thingRecord struct {
	V          int               `json:"v"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attrs,omitempty"`
	PolicyName string            `json:"policy,omitempty"`
	Created    int64             `json:"created"`
	Updated    int64             `json:"updated"`
	Version    int64             `json:"version,omitempty"`
}

// PutThing 创建或更新一台设备（PRD REG-001）。
//
// 已存在时保留创建时间并把版本加一。管理面的写入走这里。
func (r *Registry) PutThing(ctx context.Context, t Thing) error {
	return r.writeThing(ctx, t, thingUpsert, 0, false)
}

// CreateThing 只在设备不存在时创建。已存在返回 ErrAlreadyExists。
func (r *Registry) CreateThing(ctx context.Context, t Thing) error {
	return r.writeThing(ctx, t, thingCreate, 0, false)
}

// UpdateThing 更新已有设备的属性。
//
// replace 为真时用 attrs 换掉原属性，nil 视为空。为假时把 attrs 合并进原属性；
// attrs 为 nil 则属性保持原样。expected 非 0 且与当前版本不同则返回
// ErrVersionConflict，且不写入。策略绑定保持不变。
func (r *Registry) UpdateThing(ctx context.Context, name string, attrs map[string]string, replace bool, expected int64) error {
	return r.writeThing(ctx, Thing{Name: name, Attributes: attrs}, thingUpdate, expected, replace)
}

const (
	thingUpsert = iota
	thingCreate
	thingUpdate
)

func (r *Registry) writeThing(ctx context.Context, t Thing, mode int, expected int64, replace bool) error {
	if err := ValidateThingName(t.Name); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	old, err := r.getThingRecordLocked(t.Name)
	var created, version int64
	policy := t.PolicyName
	attrs := t.Attributes
	switch {
	case err == nil:
		if mode == thingCreate {
			return ErrAlreadyExists
		}
		version = old.Version
		if version < 1 {
			version = 1
		}
		if mode == thingUpdate && expected != 0 && expected != version {
			return ErrVersionConflict
		}
		created = old.Created
		version++
		if mode == thingUpdate {
			policy = old.PolicyName
			switch {
			case replace:
				if attrs == nil {
					attrs = map[string]string{}
				}
			case attrs != nil:
				attrs = mergeAttrs(old.Attributes, attrs)
			default:
				attrs = old.Attributes
			}
		}
	case errors.Is(err, ErrNotFound):
		if mode == thingUpdate {
			return ErrNotFound
		}
		created = r.now()
		version = 1
	default:
		return err
	}
	if err := validateAttributes(attrs); err != nil {
		return err
	}
	if policy != "" {
		if _, _, ok := r.policyEntryLocked(policy); !ok {
			// 绑定不存在的策略会让设备连不上，而错误信息指向设备
			// 而不是指向这次绑定。在写入时拒绝，错误才落在真正的原因上。
			return fmt.Errorf("registry: 策略 %q 不存在；请先创建它", policy)
		}
	}

	now := r.now()
	rec := &thingRecord{
		V: thingRecordVersion, Name: t.Name, Attributes: attrs,
		PolicyName: policy, Created: created, Updated: now, Version: version,
	}
	return r.applyThingLocked(ctx, rec, true)
}

func mergeAttrs(old, add map[string]string) map[string]string {
	out := make(map[string]string, len(old)+len(add))
	for k, v := range old {
		out[k] = v
	}
	for k, v := range add {
		out[k] = v
	}
	return out
}

// applyThingLocked 把设备记录写进热视图，withRecord 为真时同一条命令
// 带上冷层记录。
//
// 恢复路径（Restore）与槽位重投影传 false：前者的记录正是从冷层读出来的；
// 后者只改热视图里的投影，冷层的完整属性没有变。
func (r *Registry) applyThingLocked(ctx context.Context, rec *thingRecord, withRecord bool) error {
	cmd, err := r.thingCmdLocked(rec)
	if err != nil {
		return err
	}
	if withRecord {
		op, err := setOp(store.Key(store.PrefixThing, rec.Name), rec)
		if err != nil {
			return err
		}
		cmd.Records = []meta.RecordOp{op}
	}
	if _, err := r.meta.Apply(ctx, cmd); err != nil {
		return fmt.Errorf("registry: 写入设备热视图失败: %w", err)
	}
	return nil
}

// thingCmdLocked 把设备记录翻译成一条热视图命令（不带冷层记录）。
// 分开构造的理由见 certCmdLocked。
func (r *Registry) thingCmdLocked(rec *thingRecord) (meta.Command, error) {
	ps := meta.NoPolicySet
	if rec.PolicyName != "" {
		id, _, ok := r.policyEntryLocked(rec.PolicyName)
		if !ok {
			return meta.Command{}, fmt.Errorf("registry: 设备 %s 绑定的策略 %q 不存在",
				rec.Name, rec.PolicyName)
		}
		ps = id
	}
	return meta.Command{
		Kind: meta.CmdSetThing,
		Thing: &meta.ThingCmd{
			Name: rec.Name, PolicySet: ps,
			Attrs: projectAttrs(r.meta.View().AttrSlots(), rec.Attributes),
		},
	}, nil
}

// emitThingLocked 把设备记录交给重建回调，不写冷层也不提日志。
func (r *Registry) emitThingLocked(apply func(meta.Command) error, rec *thingRecord) error {
	cmd, err := r.thingCmdLocked(rec)
	if err != nil {
		return err
	}
	return apply(cmd)
}

// projectAttrs 把属性映射投影到当前的属性槽位顺序。
//
// 热视图只物化策略真正引用的属性（PRD CLU-053、REG-006），槽位即那份
// 清单。按槽位投影而不是把整个映射塞进去，是这条优化的实际做法：
// 百万设备下，materialize 全部属性是几十 MB 的无用内存，
// 而绝大多数属性从不被策略引用。
//
// 槽位之外的属性只存在冷层，控制台与 API 从那里读。
func projectAttrs(slots []string, attrs map[string]string) []string {
	if len(slots) == 0 || len(attrs) == 0 {
		return nil
	}
	out := make([]string, len(slots))
	for i, name := range slots {
		out[i] = attrs[name]
	}
	// 尾部的空值可以省略（ThingCmd.Attrs 允许短于槽位数），
	// 这在多数设备只设了前几个槽位时省下可观的内存。
	for len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

// SyncAttrSlots 依当前全部策略引用的属性名重设槽位。
//
// 槽位变更后须重写全部设备记录：投影是按槽位下标的，
// 槽位顺序一变，旧的投影结果就指向了别的属性名——
// 那意味着策略会读到错的属性值，是一类静默的权限错误。
func (r *Registry) SyncAttrSlots(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.syncAttrSlotsLocked(ctx)
}

func (r *Registry) syncAttrSlotsLocked(ctx context.Context) error {
	seen := make(map[string]bool)
	var slots []string
	// 扫冷层而不是读一张内存映射：策略的权威是冷层记录加热视图里的
	// 编译结果（见 policyEntryLocked 的说明）。策略的量级是几十到几百，
	// 而这条路径只在策略变更时走，不在连接路径上。
	err := store.ScanPrefix(r.kv, store.PrefixPolicy, "", func(parts []string, _ []byte) bool {
		_, cs, ok := r.policyEntryLocked(parts[0])
		if !ok || cs == nil {
			return true
		}
		for _, a := range cs.AttrRefs() {
			if !seen[a] {
				seen[a] = true
				slots = append(slots, a)
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	sort.Strings(slots)

	cur := r.meta.View().AttrSlots()
	if sameStrings(cur, slots) {
		return nil
	}
	if _, err := r.meta.Apply(ctx, meta.Command{
		Kind: meta.CmdSetAttrSlots, AttrSlots: slots,
	}); err != nil {
		return fmt.Errorf("registry: 设定属性槽位失败: %w", err)
	}
	// 槽位变了，全部设备的投影都要重算
	var reErr error
	err = store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
		var rec thingRecord
		if decodeJSON(value, &rec) != nil {
			return true
		}
		// 只改热视图里的投影：冷层那份完整属性没有变，
		// 重写它只会在每次槽位变更时把整张设备表又落一遍盘。
		if aerr := r.applyThingLocked(ctx, &rec, false); aerr != nil {
			reErr = aerr
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return reErr
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SetThingPolicy 改写设备上的那一份策略。空字符串表示解除。
//
// 兼容面的 AttachPolicy 可以指向设备。更新设备属性的那条路径
// 会保留原有策略，所以改策略要走这里，避免一次附加把属性清掉。
func (r *Registry) SetThingPolicy(ctx context.Context, name, policy string) error {
	if err := ValidateThingName(name); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getThingRecordLocked(name)
	if err != nil {
		return err
	}
	if policy != "" {
		if _, _, ok := r.policyEntryLocked(policy); !ok {
			return fmt.Errorf("registry: 策略 %q 不存在；请先创建它", policy)
		}
	}
	if rec.PolicyName == policy {
		return nil
	}
	rec.PolicyName = policy
	ver := rec.Version
	if ver < 1 {
		ver = 1
	}
	rec.Version = ver + 1
	rec.Updated = r.now()
	return r.applyThingLocked(ctx, rec, true)
}

// PolicyTarget 是一份策略附加上去的主体。
type PolicyTarget struct {
	// Kind 取 thing 或 cert。
	Kind string
	ID   string
}

// ListPolicyTargets 按「先设备、后证书」的顺序分页。
//
// after 的形状是 kind/id，下一次请求原样带回。它只在本节点有效。
func (r *Registry) ListPolicyTargets(policy, after string, limit int) ([]PolicyTarget, string, error) {
	if _, err := r.GetPolicy(policy); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 25
	}
	if limit > 250 {
		limit = 250
	}
	phase, afterID := "thing", ""
	if after != "" {
		kind, id, ok := strings.Cut(after, "/")
		if !ok || (kind != "thing" && kind != "cert") || id == "" {
			return nil, "", fmt.Errorf("registry: 分页标记无效")
		}
		phase, afterID = kind, id
	}
	var out []PolicyTarget
	next := ""
	add := func(t PolicyTarget) bool {
		if len(out) == limit {
			last := out[len(out)-1]
			next = last.Kind + "/" + last.ID
			return false
		}
		out = append(out, t)
		return true
	}
	if phase == "thing" {
		err := store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
			name := parts[0]
			if name <= afterID {
				return true
			}
			var rec thingRecord
			if decodeJSON(value, &rec) != nil || rec.PolicyName != policy {
				return true
			}
			return add(PolicyTarget{Kind: "thing", ID: name})
		})
		if err != nil {
			return nil, "", err
		}
		if next != "" {
			return out, next, nil
		}
		phase, afterID = "cert", ""
	}
	if phase == "cert" {
		err := store.ScanPrefix(r.kv, store.PrefixCert, "", func(parts []string, value []byte) bool {
			fp := parts[0]
			if fp <= afterID {
				return true
			}
			var rec certRecord
			if decodeJSON(value, &rec) != nil || rec.PolicyName != policy {
				return true
			}
			return add(PolicyTarget{Kind: "cert", ID: fp})
		})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// GetThing 读取一台设备。
func (r *Registry) GetThing(name string) (*Thing, error) {
	rec, err := r.getThingRecordLocked(name)
	if err != nil {
		return nil, err
	}
	ver := rec.Version
	if ver < 1 {
		ver = 1
	}
	return &Thing{
		Name: rec.Name, Attributes: rec.Attributes, PolicyName: rec.PolicyName,
		CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated,
		Version: ver,
	}, nil
}

func (r *Registry) getThingRecordLocked(name string) (*thingRecord, error) {
	raw, err := r.kv.Get(store.Key(store.PrefixThing, name))
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rec thingRecord
	if derr := decodeJSON(raw, &rec); derr != nil {
		return nil, fmt.Errorf("registry: 设备记录损坏 %s: %w", name, derr)
	}
	return &rec, nil
}

// DeleteThing 删除一台设备（PRD REG-005）。
//
// 仍有证书绑定时拒绝并指出是哪几张：直接删掉会留下指向不存在设备的
// 证书，那些证书下次连接时解析不出 thingName，策略变量求值失败而被拒，
// 错误信息完全看不出根因是这次删除。
func (r *Registry) DeleteThing(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.getThingRecordLocked(name); err != nil {
		return err
	}
	bound, err := r.certsOfThingLocked(name, 3)
	if err != nil {
		return err
	}
	if len(bound) > 0 {
		return fmt.Errorf("%w: 设备 %s 仍绑定 %d 张证书（%s…）；请先解除绑定",
			ErrInUse, name, len(bound), shortFP(bound[0]))
	}
	if _, err := r.meta.Apply(ctx, meta.Command{
		Kind: meta.CmdDeleteThing, Thing: &meta.ThingCmd{Name: name},
		Records: []meta.RecordOp{deleteOp(store.Key(store.PrefixThing, name))},
	}); err != nil {
		return fmt.Errorf("registry: 删除设备热视图失败: %w", err)
	}
	return nil
}

// ListThings 分页列举设备（PRD REG-044：服务端筛选与分页）。
//
// 不提供"返回全部"的入口：百万设备下那是一次把整个冷层读进内存的操作，
// 而调用方多半只是要展示一页。
func (r *Registry) ListThings(q Query) (*ThingPage, error) {
	page := &ThingPage{}
	limit := q.limit()
	err := store.ScanPrefix(r.kv, store.PrefixThing, "", func(parts []string, value []byte) bool {
		name := parts[0]
		if name <= q.After {
			return true
		}
		if !q.matches(name) {
			return true
		}
		var rec thingRecord
		if decodeJSON(value, &rec) != nil {
			return true
		}
		if q.Policy != "" && rec.PolicyName != q.Policy {
			return true
		}
		if q.AttrName != "" && !attrMatch(rec.Attributes[q.AttrName], q.AttrValue, q.AttrPrefix) {
			return true
		}
		if len(page.Things) == limit {
			// 多扫到一条即说明还有下一页
			page.NextAfter = page.Things[len(page.Things)-1].Name
			return false
		}
		ver := rec.Version
		if ver < 1 {
			ver = 1
		}
		page.Things = append(page.Things, Thing{
			Name: rec.Name, Attributes: rec.Attributes, PolicyName: rec.PolicyName,
			CreatedAtUnixMs: rec.Created, UpdatedAtUnixMs: rec.Updated,
			Version: ver,
		})
		return true
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(page.Things, func(i, j int) bool {
		return page.Things[i].Name < page.Things[j].Name
	})
	return page, nil
}

// ThingPage 是一页设备。
type ThingPage struct {
	Things []Thing `json:"things"`
	// NextAfter 非空时表示还有下一页，作为下次请求的 After。
	NextAfter string `json:"nextAfter,omitempty"`
}

// Query 是列举的筛选条件。
type Query struct {
	// Prefix 按名字前缀筛选。
	Prefix string
	// Policy 按绑定的策略名筛选。
	Policy string
	// After 是上一页的末尾，用于翻页。
	After string
	// Limit 是每页条数，0 取默认值。
	Limit int
	// AttrName 与 AttrValue 同时非空时按属性筛选。
	// AttrPrefix 为真时按值的前缀匹配。
	AttrName   string
	AttrValue  string
	AttrPrefix bool
}

func attrMatch(got, want string, prefix bool) bool {
	if prefix {
		return len(got) >= len(want) && got[:len(want)] == want
	}
	return got == want
}

// DefaultPageLimit 与 MaxPageLimit 限制单页规模。
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 500
)

func (q Query) limit() int {
	if q.Limit <= 0 {
		return DefaultPageLimit
	}
	if q.Limit > MaxPageLimit {
		return MaxPageLimit
	}
	return q.Limit
}

func (q Query) matches(name string) bool {
	if q.Prefix == "" {
		return true
	}
	return len(name) >= len(q.Prefix) && name[:len(q.Prefix)] == q.Prefix
}
