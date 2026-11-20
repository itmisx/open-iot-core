package topic

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func collect(tr *Trie, name string) []Handle {
	var out []Handle
	tr.Match([]byte(name), func(r SubRef) bool {
		out = append(out, r.Handle)
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mustSub(t *testing.T, tr *Trie, filter string, h Handle) {
	t.Helper()
	if err := tr.Subscribe([]byte(filter), Subscription{Handle: h, QoS: 1}); err != nil {
		t.Fatalf("订阅 %q 失败: %v", filter, err)
	}
}

func TestTrieBasic(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/b/c", 1)
	mustSub(t, tr, "a/+/c", 2)
	mustSub(t, tr, "a/#", 3)
	mustSub(t, tr, "#", 4)
	mustSub(t, tr, "a/b/c", 5)

	if tr.Len() != 5 {
		t.Errorf("订阅数 = %d，期望 5", tr.Len())
	}
	got := collect(tr, "a/b/c")
	want := []Handle{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("a/b/c 命中 %v，期望 %v", got, want)
	}
	if got := collect(tr, "a/x/c"); !reflect.DeepEqual(got, []Handle{2, 3, 4}) {
		t.Errorf("a/x/c 命中 %v，期望 [2 3 4]", got)
	}
	if got := collect(tr, "z"); !reflect.DeepEqual(got, []Handle{4}) {
		t.Errorf("z 命中 %v，期望 [4]", got)
	}
	// # 匹配父层级（规范行为）
	if got := collect(tr, "a"); !reflect.DeepEqual(got, []Handle{3, 4}) {
		t.Errorf("a 命中 %v，期望 [3 4]", got)
	}
}

func TestTrieDollarRule(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "#", 1)
	mustSub(t, tr, "+/x", 2)
	mustSub(t, tr, "$aws/#", 3)
	mustSub(t, tr, "$aws/things/+/shadow/get", 4)

	// 以通配符开头的过滤器不得匹配 $ 开头的主题
	if got := collect(tr, "$aws/x"); !reflect.DeepEqual(got, []Handle{3}) {
		t.Errorf("$aws/x 命中 %v，期望 [3]", got)
	}
	if got := collect(tr, "$aws/things/dev/shadow/get"); !reflect.DeepEqual(got, []Handle{3, 4}) {
		t.Errorf("命中 %v，期望 [3 4]", got)
	}
	if got := collect(tr, "a/x"); !reflect.DeepEqual(got, []Handle{1, 2}) {
		t.Errorf("a/x 命中 %v，期望 [1 2]", got)
	}
}

func TestTrieAWSStrict(t *testing.T) {
	l := DefaultLimits()
	l.AWSStrict = true
	tr := New(l)
	mustSub(t, tr, "sport/#", 1)

	// AWS 行为：# 不匹配父层级（CMP-011）
	if got := collect(tr, "sport"); len(got) != 0 {
		t.Errorf("AWS 模式下 sport 应无命中，得到 %v", got)
	}
	if got := collect(tr, "sport/"); !reflect.DeepEqual(got, []Handle{1}) {
		t.Errorf("sport/ 命中 %v，期望 [1]", got)
	}
	if got := collect(tr, "sport/tennis"); !reflect.DeepEqual(got, []Handle{1}) {
		t.Errorf("sport/tennis 命中 %v，期望 [1]", got)
	}
}

func TestTrieResubscribeReplacesOptions(t *testing.T) {
	tr := New(DefaultLimits())
	if err := tr.Subscribe([]byte("a/b"), Subscription{Handle: 1, QoS: 0, ID: 7}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Subscribe([]byte("a/b"), Subscription{
		Handle: 1, QoS: 2, NoLocal: true, RetainAsPub: true, RetainHandling: 2, ID: 9,
	}); err != nil {
		t.Fatal(err)
	}
	if tr.Len() != 1 {
		t.Fatalf("重复订阅应替换而非新增，订阅数 = %d", tr.Len())
	}
	var got SubRef
	tr.Match([]byte("a/b"), func(r SubRef) bool { got = r; return true })
	if got.QoS != 2 || !got.NoLocal || !got.RetainAsPub || got.RetainHandling != 2 || got.ID != 9 {
		t.Errorf("选项未被替换: %+v", got)
	}
}

func TestTrieUnsubscribe(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/b/c", 1)
	mustSub(t, tr, "a/b/c", 2)
	mustSub(t, tr, "a/#", 1)

	if !tr.Unsubscribe([]byte("a/b/c"), 1) {
		t.Fatal("退订应成功")
	}
	if tr.Unsubscribe([]byte("a/b/c"), 1) {
		t.Error("重复退订应返回 false")
	}
	if tr.Unsubscribe([]byte("x/y"), 1) {
		t.Error("退订不存在的过滤器应返回 false")
	}
	if tr.Len() != 2 {
		t.Errorf("订阅数 = %d，期望 2", tr.Len())
	}
	if got := collect(tr, "a/b/c"); !reflect.DeepEqual(got, []Handle{1, 2}) {
		// handle 1 仍通过 a/# 命中
		t.Errorf("命中 %v，期望 [1 2]", got)
	}
	// 共享订阅不入本树
	if tr.Unsubscribe([]byte("$share/g/a"), 1) {
		t.Error("共享订阅的退订应返回 false")
	}
}

// TestTrieHas 覆盖 Has 的全部出口。
//
// 它是全局订阅额度的判据（见 Trie.Has），判错的后果是额度与树里的
// 订阅数永久分叉，所以四种出口都要钉住：命中、同过滤器但别的 handle、
// 过滤器不在树里、共享过滤器。
func TestTrieHas(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/b/c", 1)

	if !tr.Has([]byte("a/b/c"), 1) {
		t.Error("已订阅的 (过滤器, handle) 应报告存在")
	}
	// 同一个过滤器下别人的订阅不算自己的——算错会让这个客户端白拿一条
	// 路由表条目，而额度记在别人头上。
	if tr.Has([]byte("a/b/c"), 2) {
		t.Error("另一个 handle 不应报告存在")
	}
	// 节点存在但更深一层没有，以及节点根本不存在，都要是 false。
	if tr.Has([]byte("a/b"), 1) {
		t.Error("未订阅的过滤器不应报告存在")
	}
	if tr.Has([]byte("x/y"), 1) {
		t.Error("不在树里的过滤器不应报告存在")
	}
	// 共享订阅不入树，一律 false：由 SharedGroups 回答。
	if tr.Has([]byte("$share/g/a/b/c"), 1) {
		t.Error("共享过滤器应一律报告不存在")
	}
	// 退订之后必须翻回 false，否则重订会被当成重复而不占额度。
	if !tr.Unsubscribe([]byte("a/b/c"), 1) {
		t.Fatal("退订应成功")
	}
	if tr.Has([]byte("a/b/c"), 1) {
		t.Error("退订后仍报告存在")
	}
}

func TestTrieRemoveHandle(t *testing.T) {
	tr := New(DefaultLimits())
	for i, f := range []string{"a/b", "a/+/c", "#", "x/y/z"} {
		mustSub(t, tr, f, Handle(1))
		_ = i
	}
	mustSub(t, tr, "a/b", 2)

	if n := tr.RemoveHandle(1); n != 4 {
		t.Errorf("移除条数 = %d，期望 4", n)
	}
	if tr.Len() != 1 {
		t.Errorf("订阅数 = %d，期望 1", tr.Len())
	}
	if n := tr.RemoveHandle(1); n != 0 {
		t.Errorf("重复移除应返回 0，得到 %d", n)
	}
	if n := tr.RemoveHandle(99); n != 0 {
		t.Errorf("移除不存在的 handle 应返回 0，得到 %d", n)
	}
	if got := collect(tr, "a/b"); !reflect.DeepEqual(got, []Handle{2}) {
		t.Errorf("命中 %v，期望 [2]", got)
	}
}

// TestTriePrunesEmptyNodes 验证节点回收：全部退订后树应回到只有根节点，
// 且节点与订阅槽位进入空闲表以便复用。
func TestTriePrunesEmptyNodes(t *testing.T) {
	tr := New(DefaultLimits())
	filters := []string{"a/b/c/d", "a/b/x", "p/q", "#", "+/+/+"}
	for _, f := range filters {
		mustSub(t, tr, f, 1)
	}
	if tr.NodeCount() <= 1 {
		t.Fatalf("订阅后节点数应大于 1，得到 %d", tr.NodeCount())
	}
	if n := tr.RemoveHandle(1); n != len(filters) {
		t.Fatalf("移除条数 = %d，期望 %d", n, len(filters))
	}
	if tr.NodeCount() != 1 {
		t.Errorf("全部退订后节点数 = %d，期望 1（仅根节点）", tr.NodeCount())
	}
	if tr.Len() != 0 {
		t.Errorf("订阅数 = %d，期望 0", tr.Len())
	}
	// 复用空闲槽位：重新订阅不应让底层数组继续增长
	before := len(tr.nodes)
	for _, f := range filters {
		mustSub(t, tr, f, 2)
	}
	if len(tr.nodes) != before {
		t.Errorf("重新订阅应复用空闲节点，数组从 %d 增长到 %d", before, len(tr.nodes))
	}
}

func TestTrieRejectsSharedAndInvalid(t *testing.T) {
	tr := New(DefaultLimits())
	if err := tr.Subscribe([]byte("$share/g/a/b"), Subscription{Handle: 1}); err == nil {
		t.Error("共享订阅应被拒绝并交给 SharedGroups")
	}
	if err := tr.Subscribe([]byte("a/#/b"), Subscription{Handle: 1}); err == nil {
		t.Error("非法过滤器应被拒绝")
	}
	if err := tr.Subscribe(nil, Subscription{Handle: 1}); err == nil {
		t.Error("空过滤器应被拒绝")
	}
}

func TestTrieMatchEarlyStop(t *testing.T) {
	tr := New(DefaultLimits())
	for i := 1; i <= 5; i++ {
		mustSub(t, tr, "a/b", Handle(i))
	}
	count := 0
	tr.Match([]byte("a/b"), func(SubRef) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("回调返回 false 后应停止，实际调用 %d 次", count)
	}
}

func TestTrieMatchAppendReusesBuffer(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/b", 1)
	mustSub(t, tr, "a/#", 2)
	buf := make([]SubRef, 0, 8)
	buf = tr.MatchAppend(buf[:0], []byte("a/b"))
	if len(buf) != 2 {
		t.Errorf("命中数 = %d，期望 2", len(buf))
	}
	buf = tr.MatchAppend(buf[:0], []byte("zzz"))
	if len(buf) != 0 {
		t.Errorf("无命中时长度应为 0，得到 %d", len(buf))
	}
}

// TestTrieAgreesWithPairwiseMatch 是交叉校验：订阅树的匹配结果必须与
// 逐对 Match 的暴力结果完全一致。这覆盖了两套实现的任何分歧。
func TestTrieAgreesWithPairwiseMatch(t *testing.T) {
	filters := []string{
		"a", "a/b", "a/b/c", "a/+", "a/+/c", "a/#", "#", "+", "+/+", "+/b",
		"$aws/#", "$aws/things/+/shadow/update", "$aws/things/dev/jobs/get",
		"/", "/a", "a/", "a//b", "a/+/+/d", "x/#",
	}
	topics := []string{
		"a", "a/b", "a/b/c", "a/x/c", "a/b/c/d", "b", "", "/",
		"/a", "a/", "a//b", "a/x/y/d", "x", "x/y",
		"$aws/x", "$aws/things/dev/shadow/update", "$aws/things/dev/jobs/get",
	}
	for _, strict := range []bool{false, true} {
		l := DefaultLimits()
		l.AWSStrict = strict
		tr := New(l)
		valid := make([]string, 0, len(filters))
		for i, f := range filters {
			if err := tr.Subscribe([]byte(f), Subscription{Handle: Handle(i)}); err != nil {
				t.Fatalf("订阅 %q 失败: %v", f, err)
			}
			valid = append(valid, f)
		}
		for _, topic := range topics {
			want := []Handle{}
			for i, f := range valid {
				if Match([]byte(f), []byte(topic), l) {
					want = append(want, Handle(i))
				}
			}
			got := collect(tr, topic)
			if got == nil {
				got = []Handle{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("strict=%v 主题 %q：树得到 %v，逐对匹配得到 %v",
					strict, topic, got, want)
			}
		}
	}
}

// TestNodesArePointerFree 是契约 13 与 PRD TST-057 的静态断言：
// 订阅树的节点与订阅记录中不得出现任何含指针的字段，
// 否则 GC 扫描成本会随订阅数线性增长。
func TestNodesArePointerFree(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(node{}),
		reflect.TypeOf(sub{}),
		reflect.TypeOf(SubRef{}),
		reflect.TypeOf(Subscription{}),
	} {
		assertPointerFree(t, typ, typ.Name())
	}
}

func assertPointerFree(t *testing.T, typ reflect.Type, path string) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Map, reflect.Slice,
		reflect.String, reflect.Chan, reflect.Func, reflect.Interface:
		t.Errorf("%s 的类型为 %s，含指针，违反契约 13", path, typ.Kind())
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			assertPointerFree(t, f.Type, path+"."+f.Name)
		}
	case reflect.Array:
		assertPointerFree(t, typ.Elem(), path+"[]")
	}
}

// TestSegTable 覆盖段表的驻留与哈希冲突链。
func TestSegTable(t *testing.T) {
	var s segTable
	s.init()
	// 通配符的 id 必须固定
	if i, ok := s.find([]byte("+")); !ok || i != segPlus {
		t.Errorf("+ 的 id = %d, ok=%v，期望 %d", i, ok, segPlus)
	}
	if i, ok := s.find([]byte("#")); !ok || i != segHash {
		t.Errorf("# 的 id = %d, ok=%v，期望 %d", i, ok, segHash)
	}
	if _, ok := s.find([]byte("nope")); ok {
		t.Error("未驻留的段不应被找到")
	}
	// 驻留大量不同段，验证往返与去重
	ids := make(map[string]segIdx)
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("seg-%d", i)
		id := s.intern([]byte(k))
		if prev, dup := ids[k]; dup && prev != id {
			t.Fatalf("%q 重复驻留得到不同 id", k)
		}
		ids[k] = id
		if string(s.at(id)) != k {
			t.Fatalf("id %d 回取为 %q，期望 %q", id, s.at(id), k)
		}
	}
	for k, id := range ids {
		if got, ok := s.find([]byte(k)); !ok || got != id {
			t.Fatalf("find(%q) = (%d, %v)，期望 (%d, true)", k, got, ok, id)
		}
		if s.intern([]byte(k)) != id {
			t.Fatalf("重复 intern(%q) 应返回同一 id", k)
		}
	}
}

// TestTrieLiteralWildcardLevel 是对 fuzz 发现的缺陷的回归测试。
//
// 主题名中字面为 "+" 或 "#" 的层级会被段表解析为保留的通配符段 id，
// 曾使精确匹配分支与通配符分支指向同一节点，导致同一订阅被投递两次。
// 合法主题名不含通配符，但 Match 是公开 API，须对未校验输入保持正确。
func TestTrieLiteralWildcardLevel(t *testing.T) {
	cases := []struct {
		filters []string
		topic   string
		want    []Handle
	}{
		{[]string{"0", "#"}, "#", []Handle{1}},
		{[]string{"0", "+"}, "+", []Handle{1}},
		{[]string{"#", "+"}, "#", []Handle{0, 1}},
		{[]string{"a/#", "a/+"}, "a/#", []Handle{0, 1}},
		{[]string{"a/b"}, "a/#", nil},
		{[]string{"a/+"}, "a/+", []Handle{0}},
	}
	l := DefaultLimits()
	for _, tc := range cases {
		tr := New(l)
		for i, f := range tc.filters {
			mustSub(t, tr, f, Handle(i))
		}
		got := collect(tr, tc.topic)
		// 与逐对匹配交叉校验
		var want []Handle
		for i, f := range tc.filters {
			if Match([]byte(f), []byte(tc.topic), l) {
				want = append(want, Handle(i))
			}
		}
		if len(got) != len(want) {
			t.Errorf("过滤器 %v 主题 %q：树命中 %v，逐对匹配 %v",
				tc.filters, tc.topic, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("过滤器 %v 主题 %q：树命中 %v，逐对匹配 %v",
					tc.filters, tc.topic, got, want)
				break
			}
		}
		if tc.want != nil && len(got) != len(tc.want) {
			t.Errorf("过滤器 %v 主题 %q：命中 %v，期望 %v",
				tc.filters, tc.topic, got, tc.want)
		}
	}
}

// TestTrieUnsubscribeMidHandleList 覆盖从 handle 链表中间摘除订阅的路径。
// 链表以头插法构建，因此最早订阅的在尾部；按不同位置退订可走到
// 「头部」「中间」「尾部」三条分支。
func TestTrieUnsubscribeMidHandleList(t *testing.T) {
	filters := []string{"a/1", "a/2", "a/3", "a/4"}
	// 对每个位置各退订一次，验证链表完整性
	for pos := range filters {
		tr := New(DefaultLimits())
		for _, f := range filters {
			mustSub(t, tr, f, 1)
		}
		if !tr.Unsubscribe([]byte(filters[pos]), 1) {
			t.Fatalf("退订 %q 应成功", filters[pos])
		}
		if tr.Len() != len(filters)-1 {
			t.Fatalf("退订 %q 后订阅数 = %d，期望 %d", filters[pos], tr.Len(), len(filters)-1)
		}
		// 剩余订阅须仍可匹配，且 RemoveHandle 能把它们全部清掉
		for i, f := range filters {
			hit := len(collect(tr, f)) > 0
			if want := i != pos; hit != want {
				t.Errorf("退订 %q 后，%q 的命中状态 = %v，期望 %v",
					filters[pos], f, hit, want)
			}
		}
		if n := tr.RemoveHandle(1); n != len(filters)-1 {
			t.Errorf("RemoveHandle 清理 %d 条，期望 %d", n, len(filters)-1)
		}
		if tr.NodeCount() != 1 {
			t.Errorf("清理后节点数 = %d，期望 1", tr.NodeCount())
		}
	}
}

// TestTrieUnsubscribeNodeListPositions 覆盖同一节点上多个订阅者时，
// 从节点内双向链表的头、中、尾摘除的三条分支。
func TestTrieUnsubscribeNodeListPositions(t *testing.T) {
	for _, victim := range []Handle{1, 2, 3} {
		tr := New(DefaultLimits())
		for h := Handle(1); h <= 3; h++ {
			mustSub(t, tr, "a/b", h)
		}
		if !tr.Unsubscribe([]byte("a/b"), victim) {
			t.Fatalf("退订 handle %d 应成功", victim)
		}
		got := collect(tr, "a/b")
		if len(got) != 2 {
			t.Fatalf("退订 handle %d 后命中 %v，期望 2 条", victim, got)
		}
		for _, h := range got {
			if h == victim {
				t.Errorf("已退订的 handle %d 仍被命中", victim)
			}
		}
	}
}

func TestTrieMatchEarlyStopInHashBranch(t *testing.T) {
	// 回调在 # 分支上返回 false 时也必须立即停止
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/#", 1)
	mustSub(t, tr, "a/#", 2)
	mustSub(t, tr, "a/b", 3)
	n := 0
	tr.Match([]byte("a/b"), func(SubRef) bool { n++; return false })
	if n != 1 {
		t.Errorf("回调应只被调用一次，实际 %d 次", n)
	}
	// # 匹配父层级时的提前停止
	n = 0
	tr.Match([]byte("a"), func(SubRef) bool { n++; return false })
	if n != 1 {
		t.Errorf("父层级分支上回调应只被调用一次，实际 %d 次", n)
	}
}

func TestClassString(t *testing.T) {
	for c, want := range map[Class]string{
		ClassDeviceScoped:  "DeviceScoped",
		ClassReservedExact: "ReservedExact",
		ClassGeneral:       "General",
		Class(7):           "Unknown",
	} {
		if got := c.String(); got != want {
			t.Errorf("Class(%d).String() = %q，期望 %q", c, got, want)
		}
	}
}

func TestTrieUnsubscribeUnknownHandleOnExistingNode(t *testing.T) {
	tr := New(DefaultLimits())
	mustSub(t, tr, "a/b", 1)
	if tr.Unsubscribe([]byte("a/b"), 99) {
		t.Error("退订不存在的 handle 应返回 false")
	}
	if tr.Len() != 1 {
		t.Errorf("订阅数不应变化，得到 %d", tr.Len())
	}
	// 过滤器的部分路径存在但完整路径不存在
	if tr.Unsubscribe([]byte("a/b/c"), 1) {
		t.Error("退订更深的不存在路径应返回 false")
	}
	// 第一段就不存在
	if tr.Unsubscribe([]byte("zzz/b"), 1) {
		t.Error("退订不存在的首段应返回 false")
	}
}
