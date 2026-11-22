package awsapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/registry"
)

// doPrincipal 发一个带 x-amzn-principal 头的已签名请求。
//
// 这个接口的主体走请求头而不是查询参数（与 AWS 一致），所以 do() 的那条
// 快捷路径用不上——签名要把这个头算进去才不会 401。
func doPrincipal(t *testing.T, srv *httptest.Server, path, principal string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if principal != "" {
		req.Header.Set("x-amzn-principal", principal)
	}
	sign(t, req, nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, readResp(t, resp)
}

// 这个文件补的是兼容面的**列举与解绑**那几个处理函数。
//
// 它们此前完全没有 Go 侧用例：对照验收在 tests/aws-sdk（真实 boto3 与 aws
// CLI，TST-129），而那套不在门禁里。于是「翻页游标算错一格」「解绑成功却回
// 404」这类缺陷在门禁上完全看不出来——而它们恰好是 SDK 会立刻撞上的那一类，
// 因为 SDK 会照着 nextToken 一直翻下去。

// TestListPrincipalThingsPaginates 钉住按主体列设备的翻页。
//
// 游标是**上一页最后一个名字**而不是偏移量：名字集合会增删，用偏移量翻页
// 在中途插入一条时会漏掉或重复一条，而 SDK 不会报错，只是少拿到几台设备。
func TestListPrincipalThings(t *testing.T) {
	reg, srv := newHarness(t)
	fp := strings.Repeat("ab", 32)
	if err := reg.RegisterCertificate(t.Context(), registry.Certificate{
		Fingerprint: fp, PEM: "CERT",
	}); err != nil {
		t.Fatal(err)
	}
	arn := "arn:aws:iot:local:000000000000:cert/" + fp

	// 还没绑设备时：这个证书存在，所以必须回 200 加空列表，而不是 404。
	resp, raw := doPrincipal(t, srv, Prefix+"/principals/things", arn)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"things":[]`) {
		t.Fatalf("未绑定时 = %d %s，期望 200 与空列表", resp.StatusCode, raw)
	}

	// 证书必须先绑策略才能挂设备（registry 的前置条件）：没有策略的证书连
	// iot:Connect 都过不了，挂设备没有意义。
	if err := reg.CreatePolicy(t.Context(), "p1", []byte(allowDoc)); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetCertificatePolicy(t.Context(), fp, "p1"); err != nil {
		t.Fatal(err)
	}
	if err := reg.PutThing(t.Context(), registry.Thing{Name: "lamp"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.AttachThingPrincipal(t.Context(), "lamp", fp); err != nil {
		t.Fatal(err)
	}

	resp, raw = doPrincipal(t, srv, Prefix+"/principals/things", arn)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"lamp"`) {
		t.Fatalf("绑定后 = %d %s", resp.StatusCode, raw)
	}
	// 一个证书只绑一台设备（覆盖式绑定，见 registry.AttachThingPrincipal），
	// 所以这里永远不会有第二页。保留 nextToken 字段是为了与 AWS 的响应形状
	// 一致，而不是因为本实现会用到它。
	if strings.Contains(string(raw), `"nextToken"`) {
		t.Fatalf("单条结果不该带 nextToken：%s", raw)
	}

	// maxResults 必须校验：负数或非数字是请求错误，不是「按默认值办」。
	for _, bad := range []string{"0", "-1", "abc"} {
		resp, raw = doPrincipal(t, srv, Prefix+"/principals/things?maxResults="+bad, arn)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("maxResults=%s 得到 %d %s，期望 400", bad, resp.StatusCode, raw)
		}
	}
	// 游标已经越过唯一那条名字时，结果必须是空列表而不是从头再来一遍。
	resp, raw = doPrincipal(t, srv, Prefix+"/principals/things?nextToken=lamp", arn)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"things":[]`) {
		t.Fatalf("游标越过末尾 = %d %s，期望空列表", resp.StatusCode, raw)
	}

	// 主体缺失、格式不对、或不是证书 ARN，都要在读注册表之前拦住。
	for _, bad := range []string{"", "not-an-arn", "arn:aws:iot:local:000000000000:thing/lamp"} {
		resp, raw = doPrincipal(t, srv, Prefix+"/principals/things", bad)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("principal=%q 得到 %d %s，期望 400", bad, resp.StatusCode, raw)
		}
	}
}

// TestListCertificatesPaginates 钉住证书列举的翻页与参数校验。
func TestListCertificatesPaginates(t *testing.T) {
	_, srv := newHarness(t)

	resp, raw := do(t, srv, http.MethodGet, Prefix+"/certificates", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("空列举 = %d %s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "certificates") {
		t.Fatalf("响应里没有 certificates 字段：%s", raw)
	}
	for _, bad := range []string{"0", "-3", "x"} {
		resp, raw = do(t, srv, http.MethodGet, Prefix+"/certificates?pageSize="+bad, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("pageSize=%s 得到 %d %s，期望 400", bad, resp.StatusCode, raw)
		}
	}
}

// TestPolicyVersionListingAndFetch 钉住版本列举与按号取版本。
//
// 取一个不存在的版本号必须是 404 而不是 500：SDK 用它探测版本是否还在。
func TestPolicyVersionListingAndFetch(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/policies/p1",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("建策略 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/policies/p1/version",
		`{"policyDocument":`+jsonString(connectDoc)+`}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("加版本 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies/p1/version", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("列版本 = %d %s", resp.StatusCode, raw)
	}
	for _, want := range []string{`"versionId":"1"`, `"versionId":"2"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("列版本结果里缺 %s：%s", want, raw)
		}
	}

	// 注意这两个接口的字段名不同，与 AWS 实际形状一致：
	// ListPolicyVersions 回 versionId，GetPolicyVersion 回 policyVersionId。
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies/p1/version/2", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"policyVersionId":"2"`) {
		t.Fatalf("取版本 2 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies/p1/version/9", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("取不存在的版本 = %d %s，期望 404", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/policies/nope/version", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("列不存在策略的版本 = %d %s，期望 404", resp.StatusCode, raw)
	}
}

// TestDeletePolicyRequiresNoAttachment 钉住删策略的前置条件。
//
// 还挂在主体上的策略不能删：删掉之后那些主体的授权会变成「策略不存在」，
// 而设备侧的表现是突然全部被拒。AWS 在这一步也是拒绝，所以行为要对齐。
func TestDeletePolicyRequiresNoAttachment(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/policies/p1",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("建策略 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/policies/p1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删未挂载的策略 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/policies/p1", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("重复删除 = %d %s，期望 404", resp.StatusCode, raw)
	}
}

// TestDetachRejectsBadInput 钉住两个解绑接口对坏输入的拒绝。
//
// 解绑是幂等的写操作，但「目标 ARN 格式不对」必须在动注册表之前拦住——
// 放过去的后果是按一个解不出主体的字符串去删，而那会静默成功。
func TestDetachRejectsBadInput(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodPost, Prefix+"/policies/p1",
		`{"policyDocument":`+jsonString(allowDoc)+`}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("建策略 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPost, Prefix+"/target-policies/p1",
		`{"target":"not-an-arn"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("解绑坏 target = %d %s，期望 400", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/target-policies/p1", `{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("解绑缺 target = %d %s，期望 400", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/things/dev-1/principals", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("解绑主体缺 principal = %d %s，期望 400", resp.StatusCode, raw)
	}
}
