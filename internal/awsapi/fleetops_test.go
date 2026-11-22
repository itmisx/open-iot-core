package awsapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/registry"
)

// 这个文件补的是兼容面的**改与删**那几个处理函数：证书状态、角色别名的
// 读改删、自有 CA 的读改删。
//
// 它们此前只有创建路径有用例。改与删的分支没有覆盖的后果是「对不存在的
// 资源返回 200」或「参数缺失时按默认值办」这类缺陷，而 SDK 会把前者读成
// 操作成功、把后者读成配置已生效——两种都不会报错。

// TestUpdateCertificateStatus 钉住证书状态更新的参数校验与吊销原因。
//
// 缺 newStatus 必须拒绝而不是按某个默认状态办：那个默认值会把一张本想
// 吊销的证书改成启用。吊销要带原因，因为审计只记得住写进去的那一句。
func TestUpdateCertificateStatus(t *testing.T) {
	reg, srv := newHarness(t)
	fp := strings.Repeat("cd", 32)
	if err := reg.RegisterCertificate(t.Context(), registry.Certificate{
		Fingerprint: fp, PEM: "CERT",
	}); err != nil {
		t.Fatal(err)
	}

	resp, raw := do(t, srv, http.MethodPut, Prefix+"/certificates/"+fp, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("缺 newStatus = %d %s，期望 400", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPut,
		Prefix+"/certificates/"+fp+"?newStatus="+registry.StatusRevoked, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("吊销 = %d %s", resp.StatusCode, raw)
	}
	c, err := reg.GetCertificate(fp)
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != registry.StatusRevoked {
		t.Fatalf("吊销后状态 = %q", c.Status)
	}
	if c.RevokeReason == "" {
		t.Fatal("吊销必须留下原因：审计只记得住写进去的那一句")
	}

	// 对不存在的证书必须回 404 而不是 200：SDK 用它判断资源还在不在。
	resp, raw = do(t, srv, http.MethodPut,
		Prefix+"/certificates/"+strings.Repeat("ee", 32)+"?newStatus=ACTIVE", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的证书 = %d %s，期望 404", resp.StatusCode, raw)
	}
}

// TestRoleAliasLifecycle 钉住角色别名的读、改、删、列。
//
// 删一个不存在的别名要回 404：幂等删除听起来方便，但它让「我删错了名字」
// 和「它本来就没了」变成同一个结果。
func TestRoleAliasLifecycle(t *testing.T) {
	_, srv := newHarness(t)
	const arn = `{"roleArn":"arn:aws:iam::000000000000:role/ops"}`

	resp, raw := do(t, srv, http.MethodPost, Prefix+"/role-aliases/ops", arn)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("创建 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/role-aliases/ops", arn)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("重复创建 = %d %s，期望 409", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/role-aliases/ops", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"roleAlias":"ops"`) {
		t.Fatalf("读取 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/role-aliases/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("读不存在的别名 = %d %s，期望 404", resp.StatusCode, raw)
	}

	// 更新要带 roleArn：这个接口是整体替换而不是部分更新，少给 ARN 会被
	// 注册表拒绝（而不是沿用原值）。
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/role-aliases/ops",
		`{"roleArn":"arn:aws:iam::000000000000:role/ops","credentialDurationSeconds":7200}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("更新 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/role-aliases/ops", "")
	if resp.StatusCode != http.StatusOK ||
		!strings.Contains(string(raw), `"credentialDurationSeconds":7200`) {
		t.Fatalf("更新后读取 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/role-aliases/nope",
		`{"roleArn":"arn:aws:iam::000000000000:role/ops","credentialDurationSeconds":7200}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("更新不存在的别名 = %d %s，期望 404", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/role-aliases", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), "ops") {
		t.Fatalf("列举 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/role-aliases/ops", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/role-aliases/ops", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("重复删除 = %d %s，期望 404", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/role-aliases", "")
	if resp.StatusCode != http.StatusOK || strings.Contains(string(raw), `"ops"`) {
		t.Fatalf("删除后仍在列举里 = %d %s", resp.StatusCode, raw)
	}
}

// TestCARejectsUnknownAndListsEmpty 钉住自有 CA 的读删在没有 CA 时的行为。
//
// 列举必须回 200 加空列表：「这个节点还没注册过自有 CA」是正常状态，而
// 404 会让 SDK 抛异常。读与删一个不存在的 CA 则必须是 404。
func TestCARejectsUnknownAndListsEmpty(t *testing.T) {
	_, srv := newHarness(t)

	resp, raw := do(t, srv, http.MethodGet, Prefix+"/cacertificates", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("空列举 = %d %s，期望 200", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "certificates") {
		t.Fatalf("响应里没有 certificates 字段：%s", raw)
	}

	unknown := strings.Repeat("ff", 32)
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/cacertificate/"+unknown, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("读不存在的 CA = %d %s，期望 404", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/cacertificate/"+unknown, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删不存在的 CA = %d %s，期望 404", resp.StatusCode, raw)
	}
	// newStatus 在请求体里而不是查询串里。
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/cacertificate/"+unknown,
		`{"newStatus":"ACTIVE"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("改不存在的 CA = %d %s，期望 404", resp.StatusCode, raw)
	}
	// 自动注册不支持，要在动注册表之前拦住。
	resp, raw = do(t, srv, http.MethodPut, Prefix+"/cacertificate/"+unknown,
		`{"newStatus":"ACTIVE","newAutoRegistrationStatus":"ENABLE"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("自动注册 = %d %s，期望 400", resp.StatusCode, raw)
	}
}
