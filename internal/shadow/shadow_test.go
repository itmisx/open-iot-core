package shadow

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/quota"
)

const testNowMs = 1_700_000_000_000
const testNowSec = testNowMs / 1000

// ---------- 测试装置 ----------

type testClock struct {
	mu sync.Mutex
	ms int64
}

func newClock() *testClock { return &testClock{ms: testNowMs} }

func (c *testClock) now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ms
}

func (c *testClock) advance(sec int64) {
	c.mu.Lock()
	c.ms += sec * 1000
	c.mu.Unlock()
}

type rig struct {
	t     *testing.T
	clk   *testClock
	store *MemStore
	svc   *Service
}

func newRig(t *testing.T, overrides map[quota.ID]int) *rig {
	t.Helper()
	clk := newClock()
	store := NewMemStore()
	svc, err := New(Options{
		Store:       store,
		Partitioner: cluster.NewLocalPartitioner("node-1", 64),
		Limiter:     quota.NewLocal(quota.Options{Now: clk.now, Overrides: overrides}),
		Now:         clk.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &rig{t: t, clk: clk, store: store, svc: svc}
}

func req(thing, shadow string, op Op) Request {
	return Request{Thing: thing, Shadow: shadow, Op: op}
}

// update 发起一次更新并断言被接受。
func (r *rig) update(thing, body string) *Outcome {
	r.t.Helper()
	out := r.svc.Update(context.Background(), req(thing, "", OpUpdate), []byte(body))
	if out.Err != nil {
		r.t.Fatalf("更新被拒: %s\n请求: %s", out.Err, body)
	}
	return out
}

// get 读取影子并返回解析后的响应。
func (r *rig) get(thing string) Object {
	r.t.Helper()
	out := r.svc.Get(context.Background(), req(thing, "", OpGet), nil)
	if out.Err != nil {
		r.t.Fatalf("读取被拒: %s", out.Err)
	}
	return mustParse(r.t, out.Response)
}

func mustParse(t *testing.T, b []byte) Object {
	t.Helper()
	o, err := Decode(b)
	if err != nil {
		t.Fatalf("无法解析响应: %v\n%s", err, b)
	}
	return o
}

// jsonEq 按 JSON 语义比较，失败时打印两侧。
func jsonEq(t *testing.T, label string, got any, wantJSON string) {
	t.Helper()
	var want any
	dec := json.NewDecoder(strings.NewReader(wantJSON))
	dec.UseNumber()
	if err := dec.Decode(&want); err != nil {
		t.Fatalf("期望值不是合法 JSON: %v", err)
	}
	if !Equal(normalize(got), want) {
		gb, _ := json.Marshal(got)
		t.Errorf("%s 不符\n实际: %s\n期望: %s", label, gb, wantJSON)
	}
}

// normalize 把任意值经一次 JSON 往返，使数字统一为 json.Number。
func normalize(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	_ = dec.Decode(&out)
	return out
}

// pick 按路径取值，便于断言嵌套字段。
func pick(t *testing.T, o Object, path ...string) any {
	t.Helper()
	var cur any = o
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("路径 %v 在 %q 处不是对象", path, p)
		}
		cur, ok = m[p]
		if !ok {
			t.Fatalf("路径 %v 在 %q 处不存在", path, p)
		}
	}
	return cur
}

// ---------- 合并规则（PRD SHDW-021 至 SHDW-023） ----------

// TestDeepMergeNotReplace 是规则 1：更新是深度合并，不是整体替换。
func TestDeepMergeNotReplace(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1,"nested":{"x":1,"y":2}}}}`)
	r.update("d1", `{"state":{"reported":{"nested":{"y":99}}}}`)

	doc := r.get("d1")
	jsonEq(t, "reported", pick(t, doc, "state", "reported"),
		`{"a":1,"nested":{"x":1,"y":99}}`)
}

// TestNullDeletesKeyAndMetadata 是规则 2：null 删除键及其元数据，任意深度。
func TestNullDeletesKeyAndMetadata(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1,"b":2,"n":{"x":1,"y":2}}}}`)
	r.update("d1", `{"state":{"reported":{"b":null,"n":{"x":null}}}}`)

	doc := r.get("d1")
	jsonEq(t, "reported", pick(t, doc, "state", "reported"), `{"a":1,"n":{"y":2}}`)
	// 元数据必须同步删除，否则文档里会留下指向已不存在字段的时间戳
	meta := pick(t, doc, "metadata", "reported")
	m, _ := meta.(map[string]any)
	if _, ok := m["b"]; ok {
		t.Error("被删除的键 b 的元数据未清除")
	}
	nested, _ := m["n"].(map[string]any)
	if _, ok := nested["x"]; ok {
		t.Error("被删除的嵌套键 n.x 的元数据未清除")
	}
	if _, ok := nested["y"]; !ok {
		t.Error("未被删除的 n.y 元数据不应消失")
	}
}

// TestNestedObjectEmptiedStaysEmptyObject 是规则 3。
//
// 这条与规则 4 的不对称是真实的 AWS 行为：嵌套的空对象是用户数据
// 的一部分，而空的顶层段只是"没有这个段"的另一种写法。
func TestNestedObjectEmptiedStaysEmptyObject(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"keep":1,"n":{"only":1}}}}`)
	r.update("d1", `{"state":{"reported":{"n":{"only":null}}}}`)

	doc := r.get("d1")
	jsonEq(t, "reported", pick(t, doc, "state", "reported"), `{"keep":1,"n":{}}`)
	// 空对象的元数据同为空对象
	jsonEq(t, "n 的元数据", pick(t, doc, "metadata", "reported", "n"), `{}`)
}

// TestTopLevelSectionDroppedWhenEmpty 是规则 4。
func TestTopLevelSectionDroppedWhenEmpty(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1},"desired":{"b":2}}}`)
	r.update("d1", `{"state":{"reported":{"a":null}}}`)

	doc := r.get("d1")
	state, _ := pick(t, doc, "state").(map[string]any)
	if _, ok := state["reported"]; ok {
		t.Errorf("空的 reported 段应整段丢弃，实际: %v", state["reported"])
	}
	if _, ok := state["desired"]; !ok {
		t.Error("desired 段不应受影响")
	}
	meta, _ := pick(t, doc, "metadata").(map[string]any)
	if _, ok := meta["reported"]; ok {
		t.Error("空的 reported 元数据段也应丢弃")
	}
}

// TestArrayReplacedWholesaleWithPerElementMetadata 是规则 5。
func TestArrayReplacedWholesaleWithPerElementMetadata(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"list":[1,2,3]}}}`)
	// 数组不逐元素合并：给一个更短的数组，结果就是那个更短的数组
	r.update("d1", `{"state":{"reported":{"list":[9]}}}`)

	doc := r.get("d1")
	jsonEq(t, "list", pick(t, doc, "state", "reported", "list"), `[9]`)
	// 元数据是逐元素的时间戳数组，长度与数组一致
	jsonEq(t, "list 的元数据", pick(t, doc, "metadata", "reported", "list"),
		`[{"timestamp":1700000000}]`)
}

// TestArrayOfObjectsMetadata 验证数组元素为对象时的元数据形状。
func TestArrayOfObjectsMetadata(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"l":[{"a":1},{"b":2}]}}}`)
	doc := r.get("d1")
	jsonEq(t, "元数据", pick(t, doc, "metadata", "reported", "l"),
		`[{"a":{"timestamp":1700000000}},{"b":{"timestamp":1700000000}}]`)
}

// TestEveryLeafInUpdateGetsTimestamp 是规则 6。
//
// 包括值没有变化的叶子：元数据记的是"这个字段最近一次被写入的时间"，
// 不是"最近一次改变的时间"。
func TestEveryLeafInUpdateGetsTimestamp(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1,"b":2}}}`)
	first := r.get("d1")
	tsA1 := pick(t, first, "metadata", "reported", "a", "timestamp")

	r.clk.advance(100)
	// a 值不变、b 改变
	r.update("d1", `{"state":{"reported":{"a":1,"b":3}}}`)
	second := r.get("d1")

	tsA2 := pick(t, second, "metadata", "reported", "a", "timestamp")
	if Equal(tsA1, tsA2) {
		t.Errorf("值未变化的叶子 a 的时间戳也应刷新：%v -> %v", tsA1, tsA2)
	}
	jsonEq(t, "a 的新时间戳", tsA2, `1700000100`)
	// 未出现在更新中的字段，时间戳不动
	r.clk.advance(100)
	r.update("d1", `{"state":{"reported":{"b":4}}}`)
	third := r.get("d1")
	jsonEq(t, "a 的时间戳不应被无关更新改变",
		pick(t, third, "metadata", "reported", "a", "timestamp"), `1700000100`)
}

// TestUpdateAcceptedMetadataIncludesNullLeaves 验证 accepted 响应的元数据
// 包含 null 叶子（规则 6），而已存储文档中它们已被删除。
func TestUpdateAcceptedMetadataIncludesNullLeaves(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1,"b":2}}}`)
	out := r.update("d1", `{"state":{"reported":{"b":null}}}`)

	acc := mustParse(t, out.Response)
	// accepted 回传的 state 就是请求中的那些段，含 null
	jsonEq(t, "accepted 的 state", pick(t, acc, "state", "reported"), `{"b":null}`)
	// 元数据里 b 带时间戳
	jsonEq(t, "accepted 的元数据", pick(t, acc, "metadata", "reported"),
		`{"b":{"timestamp":1700000000}}`)
	// 但已存储文档里 b 不存在
	doc := r.get("d1")
	jsonEq(t, "存储中的 reported", pick(t, doc, "state", "reported"), `{"a":1}`)
}

// TestObjectReplacesScalar 验证在标量上写对象时整体替换而非递归合并。
func TestObjectReplacesScalar(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"x":5}}}`)
	r.update("d1", `{"state":{"reported":{"x":{"a":1}}}}`)
	doc := r.get("d1")
	jsonEq(t, "x", pick(t, doc, "state", "reported", "x"), `{"a":1}`)
	jsonEq(t, "x 的元数据", pick(t, doc, "metadata", "reported", "x"),
		`{"a":{"timestamp":1700000000}}`)
}

// TestScalarReplacesObject 验证反方向。
func TestScalarReplacesObject(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"x":{"a":1,"b":2}}}}`)
	r.update("d1", `{"state":{"reported":{"x":7}}}`)
	doc := r.get("d1")
	jsonEq(t, "x", pick(t, doc, "state", "reported", "x"), `7`)
	jsonEq(t, "x 的元数据", pick(t, doc, "metadata", "reported", "x"),
		`{"timestamp":1700000000}`)
}

// TestNumberLiteralsPreserved 验证数字字面量不被改写。
//
// 设备上报 1.50 不应被回传成 1.5，大整数不应丢精度——影子是设备与云
// 之间逐字段比对的契约，这种"看起来等价"的改写会让对照测试出现
// 无法解释的差异。
func TestNumberLiteralsPreserved(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1.50,"b":10000000000000000001,"c":1e3}}}`)
	doc := r.get("d1")
	jsonEq(t, "数字", pick(t, doc, "state", "reported"),
		`{"a":1.50,"b":10000000000000000001,"c":1e3}`)
}

// ---------- delta（PRD SHDW-027） ----------

func TestDeltaOnlyWhenDifferent(t *testing.T) {
	r := newRig(t, nil)
	// 只有 reported：没有 desired，不应有 delta
	out := r.update("d1", `{"state":{"reported":{"a":1}}}`)
	if hasDeltaPublish(out) {
		t.Error("没有 desired 时不应发 delta")
	}

	// desired 与 reported 一致：不应有 delta
	out = r.update("d1", `{"state":{"desired":{"a":1}}}`)
	if hasDeltaPublish(out) {
		t.Error("desired 与 reported 一致时不应发 delta")
	}

	// desired 与 reported 不一致：应有 delta
	out = r.update("d1", `{"state":{"desired":{"a":2}}}`)
	if !hasDeltaPublish(out) {
		t.Fatal("存在差异时应发 delta")
	}
	msg := mustParse(t, deltaPublish(t, out).Payload)
	jsonEq(t, "delta 的 state", pick(t, msg, "state"), `{"a":2}`)
}

func TestDeltaRecursesIntoNestedObjects(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"n":{"x":1,"y":2},"same":1}}}`)
	out := r.update("d1", `{"state":{"desired":{"n":{"x":1,"y":99},"same":1}}}`)

	msg := mustParse(t, deltaPublish(t, out).Payload)
	// 只有真有差异的子树进入 delta，x 与 same 都不应出现
	jsonEq(t, "delta", pick(t, msg, "state"), `{"n":{"y":99}}`)
	// delta 的元数据只含 delta 里出现的字段
	jsonEq(t, "delta 的元数据", pick(t, msg, "metadata"),
		`{"n":{"y":{"timestamp":1700000000}}}`)
}

// TestDeltaIgnoresReportedOnlyKeys 验证 reported 有而 desired 没有的键
// 不进入 delta。
//
// 这一条常被写错：desired 没提到某字段，不代表要把它清空。
func TestDeltaIgnoresReportedOnlyKeys(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1,"extra":9}}}`)
	out := r.update("d1", `{"state":{"desired":{"a":2}}}`)
	msg := mustParse(t, deltaPublish(t, out).Payload)
	jsonEq(t, "delta", pick(t, msg, "state"), `{"a":2}`)
}

func TestDeltaIncludedInGetResponse(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1},"desired":{"a":2}}}`)
	doc := r.get("d1")
	jsonEq(t, "get 中的 delta", pick(t, doc, "state", "delta"), `{"a":2}`)
}

func TestDeltaAbsentFromGetWhenInSync(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1},"desired":{"a":1}}}`)
	doc := r.get("d1")
	state, _ := pick(t, doc, "state").(map[string]any)
	if _, ok := state["delta"]; ok {
		t.Error("同步时 get 响应不应含 delta")
	}
}

func hasDeltaPublish(out *Outcome) bool {
	for _, p := range out.Publishes {
		if strings.HasSuffix(p.Topic, "/update/delta") {
			return true
		}
	}
	return false
}

func deltaPublish(t *testing.T, out *Outcome) Publish {
	t.Helper()
	for _, p := range out.Publishes {
		if strings.HasSuffix(p.Topic, "/update/delta") {
			return p
		}
	}
	t.Fatal("没有 delta 消息")
	return Publish{}
}

func docsPublish(t *testing.T, out *Outcome) Publish {
	t.Helper()
	for _, p := range out.Publishes {
		if strings.HasSuffix(p.Topic, "/update/documents") {
			return p
		}
	}
	t.Fatal("没有 documents 消息")
	return Publish{}
}

// ---------- documents（PRD SHDW-015） ----------

func TestDocumentsMessage(t *testing.T) {
	r := newRig(t, nil)
	out := r.update("d1", `{"state":{"reported":{"a":1}}}`)
	msg := mustParse(t, docsPublish(t, out).Payload)
	// 首次创建时 previous 为 null
	if v, ok := msg["previous"]; !ok || v != nil {
		t.Errorf("首次创建时 previous 应为 null，实际 %v", v)
	}
	jsonEq(t, "current 的 reported", pick(t, msg, "current", "state", "reported"), `{"a":1}`)
	jsonEq(t, "current 的版本", pick(t, msg, "current", "version"), `1`)

	out = r.update("d1", `{"state":{"reported":{"a":2}}}`)
	msg = mustParse(t, docsPublish(t, out).Payload)
	jsonEq(t, "previous 的 reported", pick(t, msg, "previous", "state", "reported"), `{"a":1}`)
	jsonEq(t, "previous 的版本", pick(t, msg, "previous", "version"), `1`)
	jsonEq(t, "current 的 reported", pick(t, msg, "current", "state", "reported"), `{"a":2}`)
	jsonEq(t, "current 的版本", pick(t, msg, "current", "version"), `2`)
	// documents 不含 delta，delta 由单独主题承载
	cur, _ := pick(t, msg, "current", "state").(map[string]any)
	if _, ok := cur["delta"]; ok {
		t.Error("documents 消息不应含 delta")
	}
}

func TestPublishesUseQoS1(t *testing.T) {
	r := newRig(t, nil)
	out := r.update("d1", `{"state":{"reported":{"a":1},"desired":{"a":2}}}`)
	if len(out.Publishes) != 2 {
		t.Fatalf("应有 delta 与 documents 两条消息，实际 %d", len(out.Publishes))
	}
	for _, p := range out.Publishes {
		if !p.QoS1 {
			t.Errorf("%s 应以 QoS 1 投递：丢一条意味着设备停留在旧状态", p.Topic)
		}
	}
}

// ---------- 版本与冲突（PRD SHDW-025、SHDW-026、SHDW-028） ----------

func TestVersionStartsAtOneAndIncrements(t *testing.T) {
	r := newRig(t, nil)
	for i := int64(1); i <= 5; i++ {
		out := r.update("d1", `{"state":{"reported":{"a":1}}}`)
		acc := mustParse(t, out.Response)
		jsonEq(t, "版本", pick(t, acc, "version"), itoa(i))
	}
}

// TestVersionBumpsEvenWhenNothingChanged 验证版本计的是"第几次更新"。
func TestVersionBumpsEvenWhenNothingChanged(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1}}}`)
	out := r.update("d1", `{"state":{"reported":{"a":1}}}`)
	acc := mustParse(t, out.Response)
	jsonEq(t, "版本", pick(t, acc, "version"), `2`)
	// 但没有实际变化，因此不发 delta
	if hasDeltaPublish(out) {
		t.Error("状态未变化时不应发 delta")
	}
}

func TestVersionConflict(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1}}}`) // 版本 1

	out := r.svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":2}},"version":5}`))
	if out.Err == nil || out.Err.Code != 409 {
		t.Fatalf("版本不符应返回 409，实际 %v", out.Err)
	}
	// 冲突时状态不得被改动
	doc := r.get("d1")
	jsonEq(t, "a", pick(t, doc, "state", "reported", "a"), `1`)

	// 正确的版本应被接受
	out = r.svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":2}},"version":1}`))
	if out.Err != nil {
		t.Fatalf("正确版本应被接受: %s", out.Err)
	}
}

func TestVersionConflictOnMissingShadow(t *testing.T) {
	r := newRig(t, nil)
	out := r.svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}},"version":1}`))
	if out.Err == nil || out.Err.Code != 409 {
		t.Errorf("对不存在的影子带版本更新应返回 409，实际 %v", out.Err)
	}
}

// TestDeleteResetsVersion 是 PRD SHDW-028。
func TestDeleteResetsVersion(t *testing.T) {
	r := newRig(t, nil)
	for i := 0; i < 3; i++ {
		r.update("d1", `{"state":{"reported":{"a":1}}}`)
	}
	out := r.svc.Delete(context.Background(), req("d1", "", OpDelete), nil)
	if out.Err != nil {
		t.Fatalf("删除被拒: %s", out.Err)
	}
	del := mustParse(t, out.Response)
	jsonEq(t, "删除时回传的版本", pick(t, del, "version"), `3`)

	// 重新创建须从 1 开始
	out = r.update("d1", `{"state":{"reported":{"a":1}}}`)
	acc := mustParse(t, out.Response)
	jsonEq(t, "重建后的版本", pick(t, acc, "version"), `1`)
	// 旧的元数据也不应残留
	doc := r.get("d1")
	jsonEq(t, "重建后的 reported", pick(t, doc, "state", "reported"), `{"a":1}`)
}

func TestDeleteNonExistentReturns404(t *testing.T) {
	r := newRig(t, nil)
	out := r.svc.Delete(context.Background(), req("d1", "", OpDelete), nil)
	if out.Err == nil || out.Err.Code != 404 {
		t.Errorf("删除不存在的影子应返回 404，实际 %v", out.Err)
	}
}

func TestDeleteVersionConflict(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1}}}`)
	out := r.svc.Delete(context.Background(), req("d1", "", OpDelete),
		[]byte(`{"version":9}`))
	if out.Err == nil || out.Err.Code != 409 {
		t.Errorf("版本不符的删除应返回 409，实际 %v", out.Err)
	}
	if _, ok := r.store.Get("d1", ""); !ok {
		t.Error("冲突的删除不得真的删掉影子")
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
