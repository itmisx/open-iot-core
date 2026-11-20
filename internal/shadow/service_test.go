package shadow

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/quota"
)

// ---------- 主题（PRD SHDW-001、SHDW-002、SHDW-010 至 SHDW-017） ----------

func TestParseRequest(t *testing.T) {
	cases := []struct {
		topic  string
		thing  string
		shadow string
		op     Op
		ok     bool
	}{
		// 经典影子
		{"$aws/things/d1/shadow/get", "d1", "", OpGet, true},
		{"$aws/things/d1/shadow/update", "d1", "", OpUpdate, true},
		{"$aws/things/d1/shadow/delete", "d1", "", OpDelete, true},
		// 命名影子
		{"$aws/things/d1/shadow/name/cfg/get", "d1", "cfg", OpGet, true},
		{"$aws/things/d1/shadow/name/cfg/update", "d1", "cfg", OpUpdate, true},
		{"$aws/things/d1/shadow/name/cfg/delete", "d1", "cfg", OpDelete, true},
		// 设备名含允许的特殊字符
		{"$aws/things/dev-1:a.b_c/shadow/get", "dev-1:a.b_c", "", OpGet, true},

		// 响应主题不是请求：设备向 /update/accepted 发消息不应触发一次真实更新
		{"$aws/things/d1/shadow/update/accepted", "", "", OpNone, false},
		{"$aws/things/d1/shadow/update/rejected", "", "", OpNone, false},
		{"$aws/things/d1/shadow/update/delta", "", "", OpNone, false},
		{"$aws/things/d1/shadow/update/documents", "", "", OpNone, false},
		{"$aws/things/d1/shadow/get/accepted", "", "", OpNone, false},
		{"$aws/things/d1/shadow/name/cfg/update/accepted", "", "", OpNone, false},

		// 畸形
		{"", "", "", OpNone, false},
		{"$aws/things", "", "", OpNone, false},
		{"$aws/things/", "", "", OpNone, false},
		{"$aws/things/d1", "", "", OpNone, false},
		{"$aws/things/d1/shadow", "", "", OpNone, false},
		{"$aws/things//shadow/get", "", "", OpNone, false},
		{"$aws/things/d1/shadows/get", "", "", OpNone, false},
		{"$aws/things/d1/shadow/name/get", "", "", OpNone, false},
		{"$aws/things/d1/shadow/name//get", "", "", OpNone, false},
		{"$aws/things/d1/shadow/name/cfg", "", "", OpNone, false},
		{"$aws/things/d1/shadow/frobnicate", "", "", OpNone, false},
		{"other/things/d1/shadow/get", "", "", OpNone, false},
	}
	for _, c := range cases {
		t.Run(c.topic, func(t *testing.T) {
			got, ok := ParseRequest(c.topic)
			if ok != c.ok {
				t.Fatalf("ok = %v，期望 %v（解析结果 %+v）", ok, c.ok, got)
			}
			if !ok {
				return
			}
			if got.Thing != c.thing || got.Shadow != c.shadow || got.Op != c.op {
				t.Errorf("= %+v，期望 {%s %s %s}", got, c.thing, c.shadow, c.op)
			}
		})
	}
}

func TestResponseTopics(t *testing.T) {
	classic := Request{Thing: "d1", Op: OpUpdate}
	if got := classic.AcceptedTopic(); got != "$aws/things/d1/shadow/update/accepted" {
		t.Errorf("= %q", got)
	}
	if got := classic.RejectedTopic(); got != "$aws/things/d1/shadow/update/rejected" {
		t.Errorf("= %q", got)
	}
	if got := classic.DeltaTopic(); got != "$aws/things/d1/shadow/update/delta" {
		t.Errorf("= %q", got)
	}
	if got := classic.DocumentsTopic(); got != "$aws/things/d1/shadow/update/documents" {
		t.Errorf("= %q", got)
	}

	named := Request{Thing: "d1", Shadow: "cfg", Op: OpGet}
	if got := named.AcceptedTopic(); got != "$aws/things/d1/shadow/name/cfg/get/accepted" {
		t.Errorf("= %q", got)
	}
	// delta 与 documents 永远挂在 update 下，与请求的操作无关
	if got := named.DeltaTopic(); got != "$aws/things/d1/shadow/name/cfg/update/delta" {
		t.Errorf("= %q", got)
	}
}

// TestRoundTripTopics 验证解析与构造互逆。
func TestRoundTripTopics(t *testing.T) {
	for _, topic := range []string{
		"$aws/things/d1/shadow/get",
		"$aws/things/d1/shadow/update",
		"$aws/things/d1/shadow/delete",
		"$aws/things/d1/shadow/name/cfg/update",
	} {
		r, ok := ParseRequest(topic)
		if !ok {
			t.Fatalf("%q 应可解析", topic)
		}
		if got := r.Base() + "/" + r.Op.String(); got != topic {
			t.Errorf("往返后 = %q，期望 %q", got, topic)
		}
	}
}

func TestPartitionKeyIsThingName(t *testing.T) {
	// 同一 thing 的全部影子必须落在同一分区，否则
	// ListNamedShadowsForThing 要跨分区扫描
	a := Request{Thing: "d1", Shadow: ""}
	b := Request{Thing: "d1", Shadow: "cfg"}
	if a.Key() != b.Key() {
		t.Errorf("同一 thing 的分区键应相同：%q 对 %q", a.Key(), b.Key())
	}
}

func TestValidateNames(t *testing.T) {
	if err := ValidateShadowName(""); err != nil {
		t.Error("空影子名即经典影子，应合法")
	}
	for _, ok := range []string{"cfg", "a:b", "a_b", "a-b", "A1", strings.Repeat("a", 64)} {
		if err := ValidateShadowName(ok); err != nil {
			t.Errorf("%q 应合法: %v", ok, err)
		}
	}
	for _, bad := range []string{"a/b", "a b", "a.b", "a+b", strings.Repeat("a", 65), "中文"} {
		if err := ValidateShadowName(bad); err == nil {
			t.Errorf("%q 应非法", bad)
		}
	}
	for _, bad := range []string{"", "a/b", "a b", strings.Repeat("a", 129)} {
		if err := ValidateThingName(bad); err == nil {
			t.Errorf("设备名 %q 应非法", bad)
		}
	}
}

// ---------- 命名影子 ----------

func TestNamedShadowsAreIndependent(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()

	out := r.svc.Update(ctx, req("d1", "", OpUpdate), []byte(`{"state":{"reported":{"v":"classic"}}}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	out = r.svc.Update(ctx, req("d1", "cfg", OpUpdate), []byte(`{"state":{"reported":{"v":"named"}}}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}

	classic := mustParse(t, r.svc.Get(ctx, req("d1", "", OpGet), nil).Response)
	named := mustParse(t, r.svc.Get(ctx, req("d1", "cfg", OpGet), nil).Response)
	jsonEq(t, "经典影子", pick(t, classic, "state", "reported", "v"), `"classic"`)
	jsonEq(t, "命名影子", pick(t, named, "state", "reported", "v"), `"named"`)
}

func TestListNamedShadows(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	for _, n := range []string{"zeta", "alpha", "mid"} {
		out := r.svc.Update(ctx, req("d1", n, OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
		if out.Err != nil {
			t.Fatal(out.Err)
		}
	}
	// 经典影子不应出现在命名影子列表里
	out := r.svc.Update(ctx, req("d1", "", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}

	names, e := r.svc.ListNamedShadows(ctx, "d1")
	if e != nil {
		t.Fatal(e)
	}
	if strings.Join(names, ",") != "alpha,mid,zeta" {
		t.Errorf("= %v，期望按字典序且不含经典影子", names)
	}

	// 未知设备返回空列表而非错误
	empty, e := r.svc.ListNamedShadows(ctx, "nobody")
	if e != nil || len(empty) != 0 {
		t.Errorf("= %v %v，期望空列表", empty, e)
	}
	if _, e := r.svc.ListNamedShadows(ctx, "bad name"); e == nil {
		t.Error("非法设备名应报错")
	}
}

func TestDeleteOnlyAffectsTargetShadow(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	for _, n := range []string{"", "cfg"} {
		r.svc.Update(ctx, req("d1", n, OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	}
	if out := r.svc.Delete(ctx, req("d1", "cfg", OpDelete), nil); out.Err != nil {
		t.Fatal(out.Err)
	}
	if _, ok := r.store.Get("d1", ""); !ok {
		t.Error("删除命名影子不应影响经典影子")
	}
	if _, ok := r.store.Get("d1", "cfg"); ok {
		t.Error("命名影子应已被删除")
	}
}

// ---------- clientToken（PRD SHDW-024） ----------

func TestClientTokenEchoed(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()

	out := r.svc.Update(ctx, req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1},"desired":{"a":2}},"clientToken":"tok-42"}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	acc := mustParse(t, out.Response)
	jsonEq(t, "accepted 的 clientToken", pick(t, acc, "clientToken"), `"tok-42"`)
	// delta 与 documents 也要带上，SDK 据此关联请求
	for _, p := range out.Publishes {
		msg := mustParse(t, p.Payload)
		jsonEq(t, p.Topic+" 的 clientToken", pick(t, msg, "clientToken"), `"tok-42"`)
	}

	// get 与 delete 也回传
	out = r.svc.Get(ctx, req("d1", "", OpGet), []byte(`{"clientToken":"g1"}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	jsonEq(t, "get 的 clientToken", pick(t, mustParse(t, out.Response), "clientToken"), `"g1"`)

	out = r.svc.Delete(ctx, req("d1", "", OpDelete), []byte(`{"clientToken":"d1"}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	jsonEq(t, "delete 的 clientToken", pick(t, mustParse(t, out.Response), "clientToken"), `"d1"`)
}

func TestClientTokenEchoedOnRejection(t *testing.T) {
	r := newRig(t, nil)
	// 被拒时同样要回传：否则并发请求的客户端无法知道是哪一条被拒了
	out := r.svc.Get(context.Background(), req("d1", "", OpGet), []byte(`{"clientToken":"t9"}`))
	if out.Err == nil {
		t.Fatal("不存在的影子应被拒")
	}
	rej := mustParse(t, out.Response)
	jsonEq(t, "rejected 的 clientToken", pick(t, rej, "clientToken"), `"t9"`)
	jsonEq(t, "code", pick(t, rej, "code"), `404`)
}

func TestClientTokenValidation(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	long := strings.Repeat("x", MaxClientTokenLen+1)
	out := r.svc.Update(ctx, req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}},"clientToken":"`+long+`"}`))
	if out.Err == nil || out.Err.Code != 400 {
		t.Errorf("超长 clientToken 应返回 400，实际 %v", out.Err)
	}
	// 非字符串
	out = r.svc.Update(ctx, req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}},"clientToken":42}`))
	if out.Err == nil {
		t.Error("非字符串 clientToken 应被拒")
	}
}

// ---------- 请求校验与错误形状 ----------

func TestUpdateValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		code int
	}{
		{"非法 JSON", `{not json`, 400},
		{"顶层不是对象", `[1,2]`, 400},
		{"缺少 state", `{"clientToken":"a"}`, 400},
		{"state 为 null", `{"state":null}`, 400},
		{"state 不是对象", `{"state":5}`, 400},
		{"state 为空", `{"state":{}}`, 400},
		{"未知段", `{"state":{"report":{"a":1}}}`, 400},
		{"version 不是数字", `{"state":{"reported":{"a":1}},"version":"x"}`, 400},
		{"version 为负", `{"state":{"reported":{"a":1}},"version":-1}`, 400},
		{"尾随内容", `{"state":{"reported":{"a":1}}} {"x":1}`, 400},
	}
	r := newRig(t, nil)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := r.svc.Update(context.Background(), req("d1", "", OpUpdate), []byte(c.body))
			if out.Err == nil {
				t.Fatalf("应被拒: %s", c.body)
			}
			if out.Err.Code != c.code {
				t.Errorf("状态码 = %d，期望 %d（%s）", out.Err.Code, c.code, out.Err.Message)
			}
			// rejected 响应须是合法 JSON 且含 code 与 message
			rej := mustParse(t, out.Response)
			if _, ok := rej["code"]; !ok {
				t.Error("rejected 响应缺少 code")
			}
			if m, ok := rej["message"].(string); !ok || m == "" {
				t.Error("rejected 响应缺少 message")
			}
			if out.ResponseTopic != "$aws/things/d1/shadow/update/rejected" {
				t.Errorf("响应主题 = %q", out.ResponseTopic)
			}
		})
	}
}

// TestUnknownSectionRejected 单列出来说明理由。
//
// 一个把 "reported" 拼成 "report" 的设备若被静默接受，它会以为上报成功，
// 而影子里什么都没变。这种故障没有任何可观测症状。
func TestUnknownSectionRejected(t *testing.T) {
	r := newRig(t, nil)
	out := r.svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1},"report":{"b":2}}}`))
	if out.Err == nil {
		t.Fatal("未知段应被拒而不是忽略")
	}
	if _, ok := r.store.Get("d1", ""); ok {
		t.Error("被拒的请求不得写入任何状态")
	}
}

func TestGetAndDeleteRejectBadPayload(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	for _, op := range []Op{OpGet, OpDelete} {
		out := r.svc.Handle(ctx, req("d1", "", op), []byte(`{bad`))
		if out.Err == nil || out.Err.Code != 400 {
			t.Errorf("%s 对非法 JSON 应返回 400，实际 %v", op, out.Err)
		}
	}
}

func TestInvalidNamesRejected(t *testing.T) {
	r := newRig(t, nil)
	ctx := context.Background()
	out := r.svc.Update(ctx, req("bad name", "", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err == nil || out.Err.Code != 400 {
		t.Errorf("非法设备名应返回 400，实际 %v", out.Err)
	}
	out = r.svc.Update(ctx, req("d1", "bad/name", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err == nil || out.Err.Code != 400 {
		t.Errorf("非法影子名应返回 400，实际 %v", out.Err)
	}
}

func TestHandleUnknownOp(t *testing.T) {
	r := newRig(t, nil)
	out := r.svc.Handle(context.Background(), req("d1", "", OpNone), nil)
	if out.Err == nil || out.Err.Code != 500 {
		t.Errorf("未知操作应返回 500，实际 %v", out.Err)
	}
}

// ---------- 配额（PRD SHDW-030 至 SHDW-035） ----------

func TestDocSizeQuota(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowDocSize: 64})
	big := `{"state":{"reported":{"a":"` + strings.Repeat("x", 200) + `"}}}`
	out := r.svc.Update(context.Background(), req("d1", "", OpUpdate), []byte(big))
	if out.Err == nil || out.Err.Code != 413 {
		t.Fatalf("超大文档应返回 413，实际 %v", out.Err)
	}
	// 超限的请求不得被解析后才拒绝——这里只能验证它确实没写入
	if _, ok := r.store.Get("d1", ""); ok {
		t.Error("超限的请求不得写入")
	}
}

func TestDepthQuota(t *testing.T) {
	r := newRig(t, nil)
	// state 下再嵌 MaxDepth 层
	inner := `1`
	for i := 0; i < MaxDepth+1; i++ {
		inner = `{"a":` + inner + `}`
	}
	out := r.svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":`+inner+`}}`))
	if out.Err == nil || out.Err.Code != 400 {
		t.Fatalf("超深文档应返回 400，实际 %v", out.Err)
	}
	if !strings.Contains(out.Err.Message, "nesting") {
		t.Errorf("错误文案应说明是嵌套过深: %q", out.Err.Message)
	}
}

func TestRatePerThingQuota(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowRatePerThing: 2})
	ctx := context.Background()
	ok := 0
	for i := 0; i < 10; i++ {
		out := r.svc.Update(ctx, req("d1", "", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
		if out.Err == nil {
			ok++
			continue
		}
		if out.Err.Code != 429 {
			t.Fatalf("应因限速被拒，实际 %v", out.Err)
		}
	}
	if ok == 0 || ok > 4 {
		t.Errorf("通过的请求数 = %d，限速似未生效", ok)
	}
	// 另一个设备不应受影响：限速是 per-thing 的
	out := r.svc.Update(ctx, req("d2", "", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err != nil {
		t.Errorf("另一设备不应受影响: %v", out.Err)
	}
}

// TestNameValidationBeforeRateLimit 验证校验顺序。
//
// 若先查限速，一串格式错误的请求会消耗掉设备的限速额度，
// 把一个客户端 bug 变成该设备的拒绝服务。
func TestNameValidationBeforeRateLimit(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowRatePerThing: 1})
	ctx := context.Background()
	// 先打一堆非法影子名的请求
	for i := 0; i < 20; i++ {
		out := r.svc.Update(ctx, req("d1", "bad/name", OpUpdate),
			[]byte(`{"state":{"reported":{"a":1}}}`))
		if out.Err == nil || out.Err.Code != 400 {
			t.Fatalf("应因名字非法被拒，实际 %v", out.Err)
		}
	}
	// 额度不应被这些请求吃掉
	out := r.svc.Update(ctx, req("d1", "", OpUpdate), []byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err != nil {
		t.Errorf("格式错误的请求不应消耗限速额度: %v", out.Err)
	}
}

// ---------- 分区（契约 06、PRD SHDW-060） ----------

// remotePartitioner 让所有键都归属别的节点，以进入转发分支。
type remotePartitioner struct{}

func (remotePartitioner) Owner(string) cluster.NodeID { return "node-2" }
func (remotePartitioner) IsLocal(string) bool         { return false }
func (remotePartitioner) Shard(string) uint32         { return 7 }
func (remotePartitioner) Count() int                  { return 64 }

func newRemoteRig(t *testing.T, fwd func(context.Context, cluster.NodeID, Command) (*Result, error)) *Service {
	t.Helper()
	clk := newClock()
	store := NewMemStore()
	t.Cleanup(func() { _ = store.Close() })
	svc, err := New(Options{
		Store: store, Partitioner: remotePartitioner{},
		Limiter: quota.NewLocal(quota.Options{Now: clk.now}),
		Now:     clk.now, Forward: fwd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestForwardsToPartitionOwner 验证非本地分区的写入被转发。
//
// 单机下这条分支不会进入，但它必须在 v0.1 就存在（契约 06）：
// 事后插入它等于把本服务的每个写入方法的错误路径重写一遍。
func TestForwardsToPartitionOwner(t *testing.T) {
	var gotOwner cluster.NodeID
	var gotCmd Command
	svc := newRemoteRig(t, func(_ context.Context, owner cluster.NodeID, cmd Command) (*Result, error) {
		gotOwner, gotCmd = owner, cmd
		return &Result{
			Current: &Document{Reported: Object{"a": nil}, Version: 7},
			Version: 7, Changed: true,
		}, nil
	})
	out := svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err != nil {
		t.Fatalf("转发应成功: %s", out.Err)
	}
	if gotOwner != "node-2" {
		t.Errorf("转发目标 = %q", gotOwner)
	}
	if gotCmd.Kind != CmdUpdate || gotCmd.Thing != "d1" {
		t.Errorf("转发的命令 = %+v", gotCmd)
	}
	// 命令里必须带上时间戳：各副本 apply 时不得各读一次墙上时钟
	if gotCmd.NowSec != testNowSec {
		t.Errorf("命令中的时间戳 = %d，期望 %d", gotCmd.NowSec, testNowSec)
	}
	acc := mustParse(t, out.Response)
	jsonEq(t, "版本取自转发结果", pick(t, acc, "version"), `7`)
}

// TestForwardNotifiesHook 验证转发会通知追踪钩子。
//
// 去掉这次调用，跨节点写入就只剩转发本身，追踪里看不到它去了哪台机器。
func TestForwardNotifiesHook(t *testing.T) {
	var thing string
	ended := false
	svc := newRemoteRig(t, func(context.Context, cluster.NodeID, Command) (*Result, error) {
		return &Result{Current: &Document{Version: 1}, Version: 1}, nil
	})
	svc.onFwd = func(ctx context.Context, _, th string) (context.Context, func(error)) {
		thing = th
		return ctx, func(error) { ended = true }
	}
	out := svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if thing != "d1" || !ended {
		t.Fatalf("钩子 thing=%q ended=%v", thing, ended)
	}
}

func TestForwardNotConfiguredFails(t *testing.T) {
	svc := newRemoteRig(t, nil)
	out := svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err == nil || out.Err.Code != 500 {
		t.Fatalf("无法转发应返回 500，实际 %v", out.Err)
	}
	// 错误须指出是哪个节点，否则运维无从下手
	if !strings.Contains(out.Err.Message, "node-2") {
		t.Errorf("错误应指出 owner: %q", out.Err.Message)
	}
}

func TestForwardErrorPropagated(t *testing.T) {
	svc := newRemoteRig(t, func(context.Context, cluster.NodeID, Command) (*Result, error) {
		return nil, ErrVersionConflict
	})
	out := svc.Update(context.Background(), req("d1", "", OpUpdate),
		[]byte(`{"state":{"reported":{"a":1}}}`))
	if out.Err == nil || out.Err.Code != 409 {
		t.Errorf("owner 返回的冲突应原样传回，实际 %v", out.Err)
	}
}

func TestForwardGenericErrorBecomes500(t *testing.T) {
	svc := newRemoteRig(t, func(context.Context, cluster.NodeID, Command) (*Result, error) {
		return nil, context.DeadlineExceeded
	})
	out := svc.Delete(context.Background(), req("d1", "", OpDelete), nil)
	if out.Err == nil || out.Err.Code != 500 {
		t.Errorf("转发失败应返回 500，实际 %v", out.Err)
	}
}

// TestReadsAreLocal 验证读取不走转发。
//
// 影子的读取可以在本地物化视图上完成，不必打到 owner——
// 这是单写者加本地读的常规形态。
func TestReadsAreLocal(t *testing.T) {
	called := false
	svc := newRemoteRig(t, func(context.Context, cluster.NodeID, Command) (*Result, error) {
		called = true
		return nil, nil
	})
	out := svc.Get(context.Background(), req("d1", "", OpGet), nil)
	if out.Err == nil || out.Err.Code != 404 {
		t.Errorf("本地无此影子应返回 404，实际 %v", out.Err)
	}
	if called {
		t.Error("读取不应触发转发")
	}
}

// ---------- 并发（契约 06 单写者） ----------

// TestConcurrentUpdatesAreSerialized 验证同一 thing 的并发更新被串行化。
//
// 断言方式是"版本号恰好等于更新次数且无重复"：若写入没有串行化，
// 会出现两次更新拿到同一个版本号，而那意味着一次更新被静默丢弃。
func TestConcurrentUpdatesAreSerialized(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowRatePerThing: 0, quota.ShadowAPIRate: 0})
	ctx := context.Background()
	const n = 200

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := r.svc.Update(ctx, req("d1", "", OpUpdate),
				[]byte(`{"state":{"reported":{"a":1}}}`))
			if out.Err != nil {
				t.Errorf("更新被拒: %s", out.Err)
				return
			}
			acc, err := Decode(out.Response)
			if err != nil {
				t.Errorf("无法解析响应: %v", err)
				return
			}
			num, _ := acc["version"].(interface{ Int64() (int64, error) })
			v, _ := num.Int64()
			mu.Lock()
			seen[v]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for v, count := range seen {
		if count != 1 {
			t.Errorf("版本 %d 被分配了 %d 次，写入未串行化", v, count)
		}
	}
	if len(seen) != n {
		t.Errorf("得到 %d 个不同版本，期望 %d", len(seen), n)
	}
	doc, ok := r.store.Get("d1", "")
	if !ok {
		t.Fatal("影子应存在")
	}
	if doc.Version != n {
		t.Errorf("最终版本 = %d，期望 %d", doc.Version, n)
	}
}

func TestConcurrentDifferentThings(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowAPIRate: 0})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			thing := "d" + itoa(int64(i))
			for j := 0; j < 10; j++ {
				out := r.svc.Update(ctx, req(thing, "", OpUpdate),
					[]byte(`{"state":{"reported":{"a":1}}}`))
				if out.Err != nil {
					t.Errorf("%s 更新被拒: %s", thing, out.Err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 50; i++ {
		doc, ok := r.store.Get("d"+itoa(int64(i)), "")
		if !ok || doc.Version != 10 {
			t.Errorf("d%d 的版本 = %v", i, doc)
		}
	}
}

// ---------- 存储层 ----------

func TestStoreReturnsCopies(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1}}}`)

	doc, _ := r.store.Get("d1", "")
	// 改动取回的副本不得影响存储——否则调用方对响应对象的任何修改
	// 都会静默改掉已存储的影子
	doc.Reported["a"] = "tampered"
	again, _ := r.store.Get("d1", "")
	jsonEq(t, "存储中的值", again.Reported["a"], `1`)
}

func TestStoreClosedRejects(t *testing.T) {
	store := NewMemStore()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(context.Background(), Command{
		Kind: CmdUpdate, Thing: "d1", Patch: Object{},
	}); err == nil {
		t.Error("已关闭的存储应拒绝写入")
	}
	if _, ok := store.Get("d1", ""); ok {
		t.Error("已关闭的存储不应返回数据")
	}
	if names := store.List("d1"); names != nil {
		t.Errorf("已关闭的存储应返回空列表，实际 %v", names)
	}
}

func TestApplyUnknownCommandKind(t *testing.T) {
	store := NewMemStore()
	defer func() { _ = store.Close() }()
	if _, err := store.Apply(context.Background(), Command{
		Kind: CmdKind(99), Thing: "d1",
	}); err == nil {
		t.Error("未知命令类型应报错")
	}
}

func TestNewValidatesOptions(t *testing.T) {
	clk := newClock()
	store := NewMemStore()
	defer func() { _ = store.Close() }()
	part := cluster.NewLocalPartitioner("n", 64)
	lim := quota.NewLocal(quota.Options{Now: clk.now})

	cases := []struct {
		name string
		opt  Options
	}{
		{"缺少 Store", Options{Partitioner: part, Limiter: lim, Now: clk.now}},
		{"缺少 Partitioner", Options{Store: store, Limiter: lim, Now: clk.now}},
		{"缺少 Limiter", Options{Store: store, Partitioner: part, Now: clk.now}},
		{"缺少时钟", Options{Store: store, Partitioner: part, Limiter: lim}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.opt); err == nil {
				t.Error("应报错")
			}
		})
	}
}

func TestOpString(t *testing.T) {
	for op, want := range map[Op]string{
		OpNone: "none", OpGet: "get", OpUpdate: "update", OpDelete: "delete",
		Op(99): "unknown",
	} {
		if got := op.String(); got != want {
			t.Errorf("Op(%d).String() = %q，期望 %q", op, got, want)
		}
	}
}

func TestDepthFunction(t *testing.T) {
	cases := []struct {
		json string
		want int
	}{
		{`1`, 0}, {`"s"`, 0}, {`null`, 0},
		{`{}`, 1}, {`{"a":1}`, 1}, {`{"a":{"b":1}}`, 2},
		{`[]`, 1}, {`[1]`, 1}, {`[[1]]`, 2},
		{`{"a":[{"b":1}]}`, 3},
	}
	for _, c := range cases {
		var v any
		if err := decodeAny(c.json, &v); err != nil {
			t.Fatal(err)
		}
		if got := Depth(v); got != c.want {
			t.Errorf("Depth(%s) = %d，期望 %d", c.json, got, c.want)
		}
	}
}

func decodeAny(s string, out *any) error {
	obj, err := Decode([]byte(`{"v":` + s + `}`))
	if err != nil {
		return err
	}
	*out = obj["v"]
	return nil
}

func TestEqualSemantics(t *testing.T) {
	eq := func(a, b string) bool {
		var va, vb any
		_ = decodeAny(a, &va)
		_ = decodeAny(b, &vb)
		return Equal(va, vb)
	}
	// 按字面量比较：1 与 1.0 视为不同，与 AWS 回传字面量的行为一致
	if eq("1", "1.0") {
		t.Error("1 与 1.0 应视为不同（字面量语义）")
	}
	if !eq("1", "1") || !eq(`{"a":[1,2]}`, `{"a":[1,2]}`) {
		t.Error("相同字面量应相等")
	}
	if eq(`{"a":1}`, `{"a":1,"b":2}`) || eq(`[1]`, `[1,2]`) {
		t.Error("不同长度应不相等")
	}
	if eq("null", "0") || eq("true", `"true"`) {
		t.Error("类型不同应不相等")
	}
	if !isObject(map[string]any{}) || isObject(1) {
		t.Error("isObject 判断错误")
	}
}

// ---------- 剩余错误路径 ----------

func TestErrorFormatting(t *testing.T) {
	e := &Error{Code: 409, Message: "Version conflict"}
	if got := e.Error(); got != "409 Version conflict" {
		t.Errorf("= %q", got)
	}
	resp := e.Response("tok")
	if resp.Code != 409 || resp.ClientToken != "tok" {
		t.Errorf("= %+v", resp)
	}
	// 404 须带上影子名，否则多命名影子下无从排查
	if got := errNotFound("cfg").Message; !strings.Contains(got, "cfg") {
		t.Errorf("= %q，应含影子名", got)
	}
	if got := errNotFound("").Message; !strings.Contains(got, "classic") {
		t.Errorf("= %q，经典影子应有可读标识", got)
	}
}

func TestDocumentCloneNil(t *testing.T) {
	var d *Document
	if got := d.Clone(); got != nil {
		t.Errorf("nil 文档的副本应为 nil，实际 %+v", got)
	}
	if got := CloneObject(nil); got != nil {
		t.Errorf("nil 对象的副本应为 nil，实际 %v", got)
	}
}

func TestGlobalRateLimit(t *testing.T) {
	r := newRig(t, map[quota.ID]int{quota.ShadowAPIRate: 2})
	ctx := context.Background()
	limited := false
	for i := 0; i < 10; i++ {
		// 换不同设备，排除 per-thing 限速的影响
		out := r.svc.Update(ctx, req("d"+itoa(int64(i)), "", OpUpdate),
			[]byte(`{"state":{"reported":{"a":1}}}`))
		if out.Err != nil && out.Err.Code == 429 {
			limited = true
			break
		}
	}
	if !limited {
		t.Error("全局 API 速率限制未生效")
	}
	// ListNamedShadows 同样受全局限速约束
	if _, e := r.svc.ListNamedShadows(ctx, "d1"); e == nil || e.Code != 429 {
		t.Errorf("列举也应受限速约束，实际 %v", e)
	}
}

func TestEncodeRejectsUnencodable(t *testing.T) {
	// 通道无法编码为 JSON
	if _, err := Encode(map[string]any{"c": make(chan int)}); err == nil {
		t.Error("无法编码的值应报错")
	}
}

func TestEncodeDoesNotEscapeHTML(t *testing.T) {
	// 影子里的字符串是设备数据，把 < 转义成 \u003c 会让文档与设备
	// 发出的内容不一致
	b, err := Encode(map[string]any{"s": "a<b>c&d"})
	if err != nil {
		t.Fatal(err)
	}
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(string(b), escaped) {
			t.Errorf("不应转义 HTML（出现 %s）: %s", escaped, b)
		}
	}
	if !strings.Contains(string(b), "a<b>c&d") {
		t.Errorf("字符串应原样保留: %s", b)
	}
}

func TestDeleteOnRemoteForwards(t *testing.T) {
	var got Command
	svc := newRemoteRig(t, func(_ context.Context, _ cluster.NodeID, cmd Command) (*Result, error) {
		got = cmd
		return &Result{Version: 3, Changed: true}, nil
	})
	out := svc.Delete(context.Background(), req("d1", "", OpDelete), []byte(`{"version":3}`))
	if out.Err != nil {
		t.Fatalf("转发的删除应成功: %s", out.Err)
	}
	if got.Kind != CmdDelete || got.ExpectVersion != 3 {
		t.Errorf("转发的命令 = %+v", got)
	}
}

func TestGetWithEmptyPayload(t *testing.T) {
	r := newRig(t, nil)
	r.update("d1", `{"state":{"reported":{"a":1}}}`)
	// 空载荷是 get 的常规用法
	out := r.svc.Get(context.Background(), req("d1", "", OpGet), nil)
	if out.Err != nil {
		t.Fatalf("空载荷的 get 应成功: %s", out.Err)
	}
	doc := mustParse(t, out.Response)
	if _, ok := doc["clientToken"]; ok {
		t.Error("未提供 clientToken 时不应出现该字段")
	}
}

func TestDeltaMetadataMissingKeys(t *testing.T) {
	// desired 的元数据缺失时不应 panic，只是产出空元数据
	d := Object{"a": "x"}
	if got := deltaMetadata(d, nil); got != nil {
		t.Errorf("= %v，期望 nil", got)
	}
	if got := deltaMetadata(nil, Object{"a": 1}); got != nil {
		t.Errorf("= %v，期望 nil", got)
	}
	// delta 里有而元数据里没有的键被跳过
	if got := deltaMetadata(Object{"a": "x"}, Object{"b": 1}); got != nil {
		t.Errorf("= %v，期望 nil", got)
	}
}
