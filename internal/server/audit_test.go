package server

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/config"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// TestAuditLogRecordsAdminWrite 验证管理面写操作进独立审计文件。
//
// 请求体里放了一段私钥形状的文字，令牌也是已知的。文件里出现其中
// 任何一个，说明审计把请求原文或凭证写进去了。去掉 emit，则等不到 PutThing。
func TestAuditLogRecordsAdminWrite(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	code, raw := r.adminReq(t, http.MethodPut, adminAPIPrefix+"/things/lamp", map[string]any{
		"attributes": map[string]string{"note": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key"},
	})
	if code != http.StatusNoContent {
		t.Fatalf("写入 = %d %s", code, raw)
	}
	body := waitAudit(t, filepath.Join(r.dd.Root(), auditLogName), `"action":"PutThing"`)
	if !strings.Contains(body, `"actor":"admin"`) || !strings.Contains(body, `"target":"lamp"`) {
		t.Fatalf("审计 = %s", body)
	}
	if strings.Contains(body, testAdminToken) || strings.Contains(body, "PRIVATE KEY") {
		t.Fatalf("审计带上了令牌或私钥: %s", body)
	}
}

// TestAuditLogSkipsFailedAWSWrite 验证兼容面只记已经生效的写操作，且不记密钥。
//
// 第二次创建同一台设备会失败。若失败也被记下来，文件里会有两行 CreateThing。
func TestAuditLogSkipsFailedAWSWrite(t *testing.T) {
	r := newTestRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	cred := r.newAWSKey("admin")

	payload := []byte(`{"attributePayload":{"attributes":{"note":"-----BEGIN PRIVATE KEY-----\nabc"}}}`)
	call := func() (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost,
			"http://"+r.s.AdminAddr()+awsapi.Prefix+"/things/bulb", strings.NewReader(string(payload)))
		if err != nil {
			t.Fatal(err)
		}
		signAWS(t, req, payload, sigv4.ServiceIoT, cred)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, raw
	}
	code, raw := call()
	if code != http.StatusOK {
		t.Fatalf("创建 = %d %s", code, raw)
	}
	code, raw = call()
	if code != http.StatusConflict {
		t.Fatalf("重复创建 = %d %s", code, raw)
	}

	body := waitAudit(t, filepath.Join(r.dd.Root(), auditLogName), `"action":"CreateThing"`)
	time.Sleep(100 * time.Millisecond)
	again, err := os.ReadFile(filepath.Join(r.dd.Root(), auditLogName))
	if err != nil {
		t.Fatal(err)
	}
	body = string(again)
	if strings.Count(body, `"action":"CreateThing"`) != 1 {
		t.Fatalf("CreateThing 出现 %d 次: %s", strings.Count(body, `"action":"CreateThing"`), body)
	}
	if !strings.Contains(body, `"actor":"ak:`+cred.id+`"`) || !strings.Contains(body, `"target":"bulb"`) {
		t.Fatalf("审计 = %s", body)
	}
	if strings.Contains(body, cred.secret) || strings.Contains(body, "PRIVATE KEY") {
		t.Fatalf("审计带上了密钥或私钥: %s", body)
	}
	var line auditLine
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.Split(body, "\n")[0])), &line); err != nil {
		t.Fatal(err)
	}
}

// TestAppendAuditIsOneJSONLine 钉住命令行追加的那一行：时间用调用方给的，
// 文件权限 600，两次调用是两行而不是互相覆盖。
func TestAppendAuditIsOneJSONLine(t *testing.T) {
	dir := t.TempDir()
	if err := AppendAudit(dir, 1_700_000_000_000, "node-a", "cert issue",
		"IssueCertificate", "abcd thing=lamp"); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, auditLogName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("权限 = %o，期望 600", fi.Mode().Perm())
	}
	if err := AppendAudit(dir, 1_700_000_000_001, "node-a", "cert revoke",
		"RevokeCertificate", "abcd"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, auditLogName))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("应有 2 行，得到 %q", body)
	}
	var line auditLine
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatal(err)
	}
	if line.At != 1_700_000_000_000 || line.Node != "node-a" || line.Actor != "cli" ||
		line.Action != "IssueCertificate" || line.Target != "abcd thing=lamp" || line.Seq != 0 {
		t.Fatalf("首行 = %+v", line)
	}
}

func waitAudit(t *testing.T, path, needle string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(b), needle) {
			return string(b)
		}
		if err == nil {
			last = string(b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("审计日志里没有 %q，内容 = %q", needle, last)
	return ""
}
