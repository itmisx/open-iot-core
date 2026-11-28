package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/config"
)

// TestOTelExportFromAdmin 验证管理端口上的请求会导出一条 span。
//
// 没包上中间件时，收集器收不到任何内容。令牌出现在正文里，说明请求头被抄进去了。
func TestOTelExportFromAdmin(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer col.Close()

	r := newAdminRig(t, func(c *config.Config) {
		c.OTel.Enabled = true
		c.OTel.Endpoint = col.URL
	})
	r.start()

	req, err := http.NewRequest(http.MethodGet, "http://"+r.s.AdminAddr()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}

	deadline := time.Now().Add(testPatience)
	var all string
	for time.Now().Before(deadline) {
		mu.Lock()
		all = strings.Join(bodies, "\n")
		mu.Unlock()
		if strings.Contains(all, "/healthz") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(all, "GET /healthz") || !strings.Contains(all, `"intValue":"200"`) {
		t.Fatalf("导出 = %s", all)
	}
	if strings.Contains(all, testAdminToken) {
		t.Fatalf("导出带上了管理令牌: %s", all)
	}
}
