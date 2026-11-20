package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/store"
)

// 检索这一块此前只有兼容面的 HTTP 用例（awsapi 的 TestSearchAndRoleAlias），
// 注册表层一条没有。语句解析与匹配的分支都在这里，而它们错掉的方式是
// 「少返回一台设备」或「返回别人的设备」——两者在 HTTP 层都看不出来。

// newSearchRig 建一个开了检索的注册表，并预置几台设备。
func newSearchRig(t *testing.T, cfg IndexConfig) *rig {
	t.Helper()
	g := newRig(t)
	ctx := context.Background()
	if err := g.r.PutPolicy(ctx, "p1", []byte(simplePolicy)); err != nil {
		t.Fatal(err)
	}
	things := []Thing{
		{Name: "lamp-01", PolicyName: "p1", Attributes: map[string]string{"site": "sh", "line": "a"}},
		{Name: "lamp-02", PolicyName: "p1", Attributes: map[string]string{"site": "bj", "line": "a"}},
		{Name: "pump-01", PolicyName: "p1", Attributes: map[string]string{"site": "sh"}},
	}
	for _, th := range things {
		if err := g.r.PutThing(ctx, th); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.r.SetIndexing(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	return g
}

func names(hits []SearchHit) []string {
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.Thing.Name)
	}
	return out
}

// TestIndexingDefaultsOff 没写过配置时两个开关都是 OFF，检索直接拒绝。
//
// 默认开启会让一个没读过文档的部署在毫无察觉的情况下把设备属性
// 暴露到检索接口上（REG-041 要求默认关闭）。
func TestIndexingDefaultsOff(t *testing.T) {
	g := newRig(t)
	cfg, err := g.r.Indexing()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ThingMode != IndexOff || cfg.ConnMode != ConnOff {
		t.Fatalf("默认开关 = %+v，期望都是 OFF", cfg)
	}
	if _, _, err := g.r.Search("thingName:lamp-01", "", 25, SearchEnv{}); !errors.Is(err, ErrIndexOff) {
		t.Fatalf("关闭时检索 = %v，期望 ErrIndexOff", err)
	}
}

// TestSetIndexingRejectsUnknownValues 拼错的取值必须报错。
//
// 静默回落到 OFF 的后果是：运维以为打开了检索，而接口一直在拒绝；
// 静默回落到 ON 更糟，等于配置写错就把数据打开了。
func TestSetIndexingRejectsUnknownValues(t *testing.T) {
	g := newRig(t)
	if err := g.r.SetIndexing(ctx, IndexConfig{ThingMode: "REGISTRY_AND_SHADOWS", ConnMode: ConnOff}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("设备索引拼错 = %v", err)
	}
	if err := g.r.SetIndexing(ctx, IndexConfig{ThingMode: IndexRegistry, ConnMode: "ON"}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("连接索引拼错 = %v", err)
	}
	// 空字符串同样不接受：它和"没写"不是一回事，没写的那条路径走的是
	// Indexing 里的默认值。
	if err := g.r.SetIndexing(ctx, IndexConfig{}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("空取值 = %v", err)
	}
	if err := g.r.SetIndexing(ctx, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnStatus}); err != nil {
		t.Fatal(err)
	}
	cfg, err := g.r.Indexing()
	if err != nil || cfg.ThingMode != IndexRegistry || cfg.ConnMode != ConnStatus {
		t.Fatalf("读回 = %+v, %v", cfg, err)
	}
}

// TestIndexingRecordDefaultsAndCorruption 记录里缺字段按 OFF 读，整条坏了要报错。
func TestIndexingRecordDefaultsAndCorruption(t *testing.T) {
	g := newRig(t)
	// 只写 v：两个模式都缺，应当各自回落到 OFF 而不是空字符串——
	// 空字符串会让 Search 的 switch 落到"既不是 OFF 也不是 REGISTRY"，
	// 于是检索在一份旧记录上变成放行。
	if err := g.kv.Set(store.Key(store.PrefixSystem, "index"), []byte(`{"v":1}`), true); err != nil {
		t.Fatal(err)
	}
	cfg, err := g.r.Indexing()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ThingMode != IndexOff || cfg.ConnMode != ConnOff {
		t.Fatalf("缺字段时 = %+v，期望都是 OFF", cfg)
	}

	if err := g.kv.Set(store.Key(store.PrefixSystem, "index"), []byte(`{nope`), true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.Indexing(); err == nil {
		t.Error("损坏的检索配置应当报错")
	}
	if _, _, err := g.r.Search("thingName:x", "", 25, SearchEnv{}); err == nil {
		t.Error("配置损坏时检索应当报错，而不是当作关闭或开启")
	}
}

// TestParseQueryRejections 逐条断言被拒的语句。
//
// 解析放过一个不认识的字段，它就会被当成"没有这个条件"，于是
// `owner:someone-else` 这种语句会返回全部设备。
func TestParseQueryRejections(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexShadow, ConnMode: ConnStatus})
	bad := []string{
		"",
		"   ",
		"lamp-01",            // 没有冒号
		":lamp-01",           // 空字段名
		"thingName:",         // 空取值
		"attributes.:sh",     // 空属性名
		"owner:nobody",       // 不认识的字段
		"shadow.reported.:1", // 空影子路径
		"shadow.desired.:1",
		"connectivity.connected:yes", // 只接受 true/false
		"connectivity.connected:1",
	}
	for _, q := range bad {
		t.Run(fmt.Sprintf("%q", q), func(t *testing.T) {
			if _, _, err := g.r.Search(q, "", 25, SearchEnv{}); !errors.Is(err, ErrBadQuery) {
				t.Errorf("= %v，期望 ErrBadQuery", err)
			}
		})
	}
}

// TestSearchFieldsRequireTheirIndex 影子与连接条件各要各自的索引。
//
// 索引没开却照常匹配，返回的结果就取决于回调恰好有没有注入，
// 同一条语句在两次部署里给出不同答案。
func TestSearchFieldsRequireTheirIndex(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnOff})
	if _, _, err := g.r.Search("shadow.reported.mode:run", "", 25, SearchEnv{}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("影子索引未开时 = %v", err)
	}
	if _, _, err := g.r.Search("connectivity.connected:true", "", 25, SearchEnv{}); !errors.Is(err, ErrBadQuery) {
		t.Errorf("连接索引未开时 = %v", err)
	}
	// 设备字段在 REGISTRY 下照常可用
	hits, _, err := g.r.Search("thingName:lamp-01", "", 25, SearchEnv{})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(hits); len(got) != 1 || got[0] != "lamp-01" {
		t.Fatalf("= %v", got)
	}
}

// TestSearchMatching 钉住各类条件的匹配，以及多条件是 AND。
func TestSearchMatching(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexShadow, ConnMode: ConnStatus})
	env := SearchEnv{
		Reported: func(thing string) (map[string]any, bool) {
			switch thing {
			case "lamp-01":
				return map[string]any{
					"mode": "run", "interval": float64(30), "on": true,
					"nested": map[string]any{"deep": "yes"},
					"ratio":  1.5,
				}, true
			case "lamp-02":
				return map[string]any{"mode": "idle"}, true
			}
			return nil, false
		},
		Desired: func(thing string) (map[string]any, bool) {
			if thing == "lamp-01" {
				return map[string]any{"mode": "burst"}, true
			}
			return nil, false
		},
		Online: func(thing string) bool { return thing == "lamp-01" },
	}

	cases := []struct {
		query string
		want  []string
	}{
		{"thingName:lamp-01", []string{"lamp-01"}},
		{"thingName:lamp-*", []string{"lamp-01", "lamp-02"}},
		{"thingName:nope", nil},
		{"attributes.site:sh", []string{"lamp-01", "pump-01"}},
		{"attributes.site:sh attributes.line:a", []string{"lamp-01"}},
		{"attributes.site:sh thingName:pump-*", []string{"pump-01"}},
		{"attributes.missing:x", nil},
		{"connectivity.connected:true", []string{"lamp-01"}},
		{"connectivity.connected:false", []string{"lamp-02", "pump-01"}},
		// 数字与布尔按字面量比较：30 不能写成 30.0，1.5 保留小数
		{"shadow.reported.interval:30", []string{"lamp-01"}},
		{"shadow.reported.ratio:1.5", []string{"lamp-01"}},
		{"shadow.reported.on:true", []string{"lamp-01"}},
		{"shadow.reported.mode:idle", []string{"lamp-02"}},
		{"shadow.reported.nested.deep:yes", []string{"lamp-01"}},
		// 路径穿过一个标量：走不下去就是不匹配，不是 panic 也不是全命中
		{"shadow.reported.mode.deeper:yes", nil},
		{"shadow.desired.mode:burst", []string{"lamp-01"}},
		// 没有影子的设备不该因为"字段不存在"而被当成匹配
		{"shadow.reported.mode:run thingName:pump-01", nil},
	}
	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			hits, _, err := g.r.Search(c.query, "", 25, env)
			if err != nil {
				t.Fatal(err)
			}
			got := names(hits)
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("= %v，期望 %v", got, c.want)
			}
		})
	}
}

// TestSearchHitCarriesConnectedAndShadow 命中要带上连接状态与影子正文。
//
// 控制台的检索结果直接渲染这两项；缺了它们页面上就是空列。
func TestSearchHitCarriesConnectedAndShadow(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexShadow, ConnMode: ConnStatus})
	env := SearchEnv{
		Reported: func(string) (map[string]any, bool) { return map[string]any{"mode": "run"}, true },
		Desired:  func(string) (map[string]any, bool) { return map[string]any{"mode": "burst"}, true },
		Online:   func(thing string) bool { return thing == "lamp-01" },
	}
	hits, _, err := g.r.Search("thingName:lamp-01", "", 25, env)
	if err != nil || len(hits) != 1 {
		t.Fatalf("= %v, %v", hits, err)
	}
	h := hits[0]
	// 语句里没有 connectivity 条件，但开了连接索引就应当带上状态
	if h.Connected == nil || !*h.Connected {
		t.Errorf("Connected = %v，期望 true", h.Connected)
	}
	var doc map[string]map[string]any
	if err := json.Unmarshal([]byte(h.Shadow), &doc); err != nil {
		t.Fatalf("影子正文不是 JSON: %q", h.Shadow)
	}
	if doc["reported"]["mode"] != "run" || doc["desired"]["mode"] != "burst" {
		t.Errorf("影子正文 = %q", h.Shadow)
	}

	// 回调一个都不给时，两项都应当留空而不是编一个值出来
	bare, _, err := g.r.Search("thingName:lamp-01", "", 25, SearchEnv{})
	if err != nil || len(bare) != 1 {
		t.Fatalf("= %v, %v", bare, err)
	}
	if bare[0].Connected != nil || bare[0].Shadow != "" {
		t.Errorf("无回调时 = %+v", bare[0])
	}
}

// TestSearchPaging 分页的 token 必须是本页最后一台设备，limit 两端夹取。
func TestSearchPaging(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnOff})
	first, next, err := g.r.Search("thingName:*", "", 1, SearchEnv{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Thing.Name != "lamp-01" {
		t.Fatalf("第一页 = %v", names(first))
	}
	if next != "lamp-01" {
		t.Fatalf("nextToken = %q，期望 lamp-01", next)
	}

	all := names(first)
	for next != "" {
		var page []SearchHit
		page, next, err = g.r.Search("thingName:*", next, 1, SearchEnv{})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, names(page)...)
	}
	want := []string{"lamp-01", "lamp-02", "pump-01"}
	if strings.Join(all, ",") != strings.Join(want, ",") {
		t.Fatalf("翻页结果 = %v，期望 %v（既不得漏也不得重）", all, want)
	}

	if hits, _, err := g.r.Search("thingName:*", "", 0, SearchEnv{}); err != nil || len(hits) != 3 {
		t.Errorf("limit=0 时 = %v, %v", names(hits), err)
	}
	if hits, _, err := g.r.Search("thingName:*", "", 1000, SearchEnv{}); err != nil || len(hits) != 3 {
		t.Errorf("limit=1000 时 = %v, %v", names(hits), err)
	}
}

// TestSearchSkipsCorruptThingAndReportsScanError 损坏的设备记录跳过，遍历失败要传出。
//
// 跳过而不是报错，是因为一条读不出来的记录不该让整张检索结果取不到；
// 但遍历本身失败必须传出去，否则调用方拿到的是一张"看起来是空的"表。
func TestSearchSkipsCorruptThingAndReportsScanError(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnOff})
	if err := g.kv.Set(store.Key(store.PrefixThing, "zzz-broken"), []byte(`{oops`), true); err != nil {
		t.Fatal(err)
	}
	hits, _, err := g.r.Search("thingName:*", "", 25, SearchEnv{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 3 {
		t.Errorf("= %v，损坏记录应被跳过", names(hits))
	}

	failing := newIndexedErrRig(t, func(e *errKV) { e.failScan = true })
	if _, _, err := failing.Search("thingName:*", "", 25, SearchEnv{}); err == nil {
		t.Error("遍历失败应被传出")
	}
}

// newIndexedErrRig 建一个开了检索、随后注入存储错误的注册表。
//
// 顺序是故意的：配置要在正常状态下写进去，否则注入的错误会先把
// SetIndexing 打掉，测到的就不是 Search 的那条分支了。
func newIndexedErrRig(t *testing.T, mut func(*errKV)) *Registry {
	t.Helper()
	base := store.NewMemKV()
	t.Cleanup(func() { _ = base.Close() })
	ekv := &errKV{KV: base}
	ms := meta.NewLocalStore(meta.LocalOptions{
		Now: func() int64 { return testNowMs }, Cold: ekv,
		Compile: testCompile(authz.DefaultEnv()),
	})
	t.Cleanup(func() { _ = ms.Close() })
	r, err := New(Options{KV: ekv, Meta: ms, Env: authz.DefaultEnv(),
		Now: func() int64 { return testNowMs }})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetIndexing(ctx, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnOff}); err != nil {
		t.Fatal(err)
	}
	mut(ekv)
	return r
}

// TestSearchDefaultsThingVersion 旧记录没有 version 字段时按 1 读出。
//
// 0 会让兼容面的 expectedVersion 永远对不上（API-102），而设备
// 看不出自己哪里写错了。
func TestSearchDefaultsThingVersion(t *testing.T) {
	g := newSearchRig(t, IndexConfig{ThingMode: IndexRegistry, ConnMode: ConnOff})
	raw, err := g.kv.Get(store.Key(store.PrefixThing, "lamp-01"))
	if err != nil {
		t.Fatal(err)
	}
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	delete(rec, "version")
	patched, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.kv.Set(store.Key(store.PrefixThing, "lamp-01"), patched, true); err != nil {
		t.Fatal(err)
	}
	hits, _, err := g.r.Search("thingName:lamp-01", "", 25, SearchEnv{})
	if err != nil || len(hits) != 1 {
		t.Fatalf("= %v, %v", hits, err)
	}
	if hits[0].Thing.Version != 1 {
		t.Errorf("version = %d，期望 1", hits[0].Thing.Version)
	}
}
