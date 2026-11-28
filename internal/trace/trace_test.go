package trace

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// testPatience 是测试等**真实时钟**的上限：死锁探测器，不是性能断言。
//
// 与 internal/broker 的同名值同一个理由：原先一律等 2 秒，而 CI 是 4 核共享
// runner、门禁又连着跑 `-race` 与覆盖率两种插桩，这类等待会在实现正常时超时。
// 等不到说明真的没发生，等得到就只是慢——中间没有需要被这个值拦住的情形。
const testPatience = 30 * time.Second

func TestTraceparentRejectsZeroAndBadShape(t *testing.T) {
	if _, _, ok := parseTraceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"); ok {
		t.Fatal("全零 trace id 应被拒绝")
	}
	if _, _, ok := parseTraceparent("nope"); ok {
		t.Fatal("畸形 traceparent 应被拒绝")
	}
	tid, sid, ok := parseTraceparent("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01")
	if !ok || tid != "4bf92f3577b34da6a3ce929d0e0e4736" || sid != "00f067aa0ba902b7" {
		t.Fatalf("解析 = %s %s %v", tid, sid, ok)
	}
}

// TestExportDropsSecrets 验证导出的 span 带上父子关系和状态，且不带凭证。
//
// 过滤器拿掉之后，authorization 和私钥正文会出现在收集器收到的 JSON 里。
func TestExportDropsSecrets(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			t.Errorf("路径 = %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer col.Close()

	tr := New(Options{
		Enabled: true, Endpoint: col.URL,
		Now: func() int64 { return 1_700_000_000_000 },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)

	h := tr.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sp := Start(r.Context(), "pki.issue")
		sp.Set("thing", "lamp")
		sp.Set("authorization", "secret")
		sp.Set("note", "-----BEGIN PRIVATE KEY-----\nabc")
		sp.End()
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/admin/v1/certificates/issue", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("状态 = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("traceresponse"), "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("traceresponse = %q", rec.Header().Get("traceresponse"))
	}

	deadline := time.Now().Add(testPatience)
	var all string
	for time.Now().Before(deadline) {
		mu.Lock()
		all = strings.Join(bodies, "\n")
		n := len(bodies)
		mu.Unlock()
		if n >= 2 && strings.Contains(all, "pki.issue") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(all, `"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"`) {
		t.Fatalf("没有沿用调用方的 trace id: %s", all)
	}
	if !strings.Contains(all, `"parentSpanId":"00f067aa0ba902b7"`) {
		t.Fatalf("服务端 span 没有接上 traceparent: %s", all)
	}
	if !strings.Contains(all, "pki.issue") || !strings.Contains(all, `"stringValue":"lamp"`) {
		t.Fatalf("子 span 不完整: %s", all)
	}
	if !strings.Contains(all, `"intValue":"204"`) {
		t.Fatalf("没有状态码: %s", all)
	}
	if strings.Contains(all, "secret-token") || strings.Contains(all, "PRIVATE KEY") || strings.Contains(all, `"authorization"`) {
		t.Fatalf("导出带上了凭证或私钥: %s", all)
	}
	if !strings.Contains(all, "1700000000000000000") {
		t.Fatalf("时间戳没有用注入的时钟: %s", all)
	}
}
