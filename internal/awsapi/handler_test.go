package awsapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/itmisx/open-iot-core/internal/authz"
	"github.com/itmisx/open-iot-core/internal/meta"
	"github.com/itmisx/open-iot-core/internal/registry"
	"github.com/itmisx/open-iot-core/internal/sigv4"
	"github.com/itmisx/open-iot-core/internal/store"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecret    = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testAmzDate   = "20150830T123600Z"
)

func testNow() time.Time {
	return time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
}

func newHarness(t *testing.T) (*registry.Registry, *httptest.Server) {
	t.Helper()
	return newHarnessIssuer(t, nil)
}

func newHarnessIssuer(t *testing.T, issuer CertIssuer) (*registry.Registry, *httptest.Server) {
	t.Helper()
	return newHarnessFull(t, issuer, nil)
}

func newHarnessFull(t *testing.T, issuer CertIssuer, fill func(*Handler)) (*registry.Registry, *httptest.Server) {
	t.Helper()
	kv := store.NewMemKV()
	ms := meta.NewLocalStore(meta.LocalOptions{
		Now: func() int64 { return 1_700_000_000_000 }, ChangeBuffer: 64,
		Cold: kv, Compile: func(name string, doc []byte) (any, error) {
			return authz.CompileJSON(name, doc, authz.DefaultEnv())
		},
	})
	reg, err := registry.New(registry.Options{
		KV: kv, Meta: ms, Env: authz.DefaultEnv(), Now: func() int64 { return 1_700_000_000_000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ms.Close()
		_ = kv.Close()
	})
	h := Handler{
		Things: reg,
		Checker: sigv4.Checker{
			Region: "us-east-1",
			Services: map[string]bool{
				sigv4.ServiceIoT: true, sigv4.ServiceIoTData: true,
			},
			Now: testNow,
			Secret: func(id string) (string, bool) {
				if id == testAccessKey {
					return testSecret, true
				}
				return "", false
			},
		},
		Partition: "aws",
		Region:    "local",
		Account:   "000000000000",
		Endpoint:  "127.0.0.1:8883",
		Issuer:    issuer,
		// 装置默认给 admin：这些用例验的是各 action 的行为，不是权限。
		// 权限由 TestRoleGate 与 TestEveryRouteDeclaresRole 单独验。
		// 默认值不能是"不判"——Role 为空就该一律拒绝（AUTH-081）。
		Role: func(string) (Role, bool) { return RoleAdmin, true },
	}
	if fill != nil {
		fill(&h)
	}
	mux := http.NewServeMux()
	h.Mount(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return reg, srv
}

func sign(t *testing.T, req *http.Request, body []byte) {
	t.Helper()
	signSvc(t, req, body, sigv4.ServiceIoT)
}

func signSvc(t *testing.T, req *http.Request, body []byte, service string) {
	t.Helper()
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Date", testAmzDate)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	headers := map[string][]string{
		"Host":                 {host},
		"X-Amz-Date":           {testAmzDate},
		"X-Amz-Content-Sha256": {payload},
	}
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if v := req.Header.Get("X-Amz-Mqtt5-User-Properties"); v != "" {
		headers["X-Amz-Mqtt5-User-Properties"] = []string{v}
		signed = append(signed, "x-amz-mqtt5-user-properties")
	}
	sort.Strings(signed)
	canon, err := sigv4.CanonicalRequestRaw(req.Method, req.URL.Path, req.URL.RawPath, req.URL.RawQuery, headers, signed, payload)
	if err != nil {
		t.Fatal(err)
	}
	sig := sigv4.Sign(testSecret, sigv4.Scope{
		AccessKeyID: testAccessKey,
		Date:        "20150830",
		Region:      "us-east-1",
		Service:     service,
	}, testAmzDate, canon)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAccessKey+"/20150830/us-east-1/"+service+"/aws4_request, "+
			"SignedHeaders="+strings.Join(signed, ";")+", Signature="+sig)
}

func do(t *testing.T, srv *httptest.Server, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sign(t, req, []byte(body))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, raw
}

type awsErr struct {
	Message string `json:"message"`
	Type    string `json:"__type"`
}

func decodeErr(t *testing.T, raw []byte) awsErr {
	t.Helper()
	var e awsErr
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatalf("错误体 %s：%v", raw, err)
	}
	return e
}

// TestAWSErrorStatuses 锁定 API-011 与 API-012。
//
// 少一个异常名，或状态码、x-amzn-ErrorType 对不上，SDK 的重试会走错分支。
func TestAWSErrorStatuses(t *testing.T) {
	want := map[string]int{
		"ResourceNotFoundException":      404,
		"ResourceAlreadyExistsException": 409,
		"InvalidRequestException":        400,
		"ThrottlingException":            429,
		"UnauthorizedException":          401,
		"AccessDeniedException":          403,
		"ServiceUnavailableException":    503,
		"VersionConflictException":       409,
		"LimitExceededException":         410,
		"InternalFailureException":       500,
	}
	if len(awsStatus) != len(want) {
		t.Fatalf("异常表有 %d 项，基线是 %d 项", len(awsStatus), len(want))
	}
	for name, status := range want {
		w := httptest.NewRecorder()
		writeAWSError(w, name, "boom")
		if w.Code != status {
			t.Errorf("%s 状态 = %d，期望 %d", name, w.Code, status)
		}
		if got := w.Header()["x-amzn-ErrorType"]; len(got) != 1 || got[0] != name {
			t.Errorf("%s 的 x-amzn-ErrorType = %v", name, got)
		}
		e := decodeErr(t, w.Body.Bytes())
		if e.Type != name || e.Message != "boom" {
			t.Errorf("%s 响应体 = %+v", name, e)
		}
	}
}

func TestSigV4Required(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := do(t, srv, http.MethodGet, Prefix+"/endpoint", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("合法签名 = %d %s", resp.StatusCode, raw)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+Prefix+"/endpoint", nil)
	if err != nil {
		t.Fatal(err)
	}
	sign(t, req, nil)
	auth := req.Header.Get("Authorization")
	req.Header.Set("Authorization", auth[:len(auth)-1]+flip(auth[len(auth)-1]))
	bad, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Body.Close()
	braw, _ := io.ReadAll(bad.Body)
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("坏签名 = %d，拿掉验签就会变成 200", bad.StatusCode)
	}
	if decodeErr(t, braw).Type != "UnauthorizedException" {
		t.Fatalf("错误类型 = %s", braw)
	}
}

func flip(c byte) string {
	if c == '0' {
		return "1"
	}
	return "0"
}

func TestThingRoundTrip(t *testing.T) {
	reg, srv := newHarness(t)
	ctx := context.Background()
	if err := reg.PutPolicy(ctx, "p1", []byte(`{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := reg.PutThing(ctx, registry.Thing{Name: "lamp", PolicyName: "p1",
		Attributes: map[string]string{"site": "sh"}}); err != nil {
		t.Fatal(err)
	}

	resp, raw := do(t, srv, http.MethodGet, Prefix+"/things/lamp", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Describe = %d %s", resp.StatusCode, raw)
	}
	var got thingJSON
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.ThingName != "lamp" || got.Version != 1 || got.Attributes["site"] != "sh" {
		t.Fatalf("Describe = %+v", got)
	}
	if got.ThingArn != "arn:aws:iot:local:000000000000:thing/lamp" {
		t.Fatalf("ARN = %s", got.ThingArn)
	}

	resp, raw = do(t, srv, http.MethodPost, Prefix+"/things/lamp", `{}`)
	if resp.StatusCode != http.StatusConflict || decodeErr(t, raw).Type != "ResourceAlreadyExistsException" {
		t.Fatalf("重复创建 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPost, Prefix+"/things/meter",
		`{"attributePayload":{"attributes":{"site":"bj"}}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Create = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPatch, Prefix+"/things/lamp",
		`{"attributePayload":{"attributes":{"line":"2"},"merge":true},"expectedVersion":1}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Update = %d %s", resp.StatusCode, raw)
	}
	th, err := reg.GetThing("lamp")
	if err != nil {
		t.Fatal(err)
	}
	if th.PolicyName != "p1" || th.Attributes["site"] != "sh" || th.Attributes["line"] != "2" || th.Version != 2 {
		t.Fatalf("合并后 = %+v，策略绑定应留着", th)
	}

	resp, raw = do(t, srv, http.MethodPatch, Prefix+"/things/lamp", `{"expectedVersion":1}`)
	if resp.StatusCode != http.StatusConflict || decodeErr(t, raw).Type != "VersionConflictException" {
		t.Fatalf("过期版本 = %d %s", resp.StatusCode, raw)
	}
	if again, _ := reg.GetThing("lamp"); again.Version != 2 {
		t.Fatalf("冲突之后版本被写成 %d", again.Version)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things/missing", "")
	if resp.StatusCode != http.StatusNotFound || decodeErr(t, raw).Type != "ResourceNotFoundException" {
		t.Fatalf("不存在 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodPost, Prefix+"/things/bad", `{"thingTypeName":"widget"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Thing Type = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodPost, Prefix+"/things/bad", `{"nope":1}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("未知字段 = %d %s", resp.StatusCode, raw)
	}
}

func TestListThingsAndEndpoint(t *testing.T) {
	_, srv := newHarness(t)
	for _, name := range []string{"a", "b", "c"} {
		resp, raw := do(t, srv, http.MethodPost, Prefix+"/things/"+name,
			fmt.Sprintf(`{"attributePayload":{"attributes":{"site":%q}}}`, name))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("创建 %s = %d %s", name, resp.StatusCode, raw)
		}
	}
	resp, raw := do(t, srv, http.MethodGet, Prefix+"/things?maxResults=2", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatal(string(raw))
	}
	var page struct {
		Things    []thingJSON `json:"things"`
		NextToken string      `json:"nextToken"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Things) != 2 || page.Things[0].ThingName != "a" || page.Things[1].ThingName != "b" || page.NextToken == "" {
		t.Fatalf("第一页 = %+v", page)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things?maxResults=2&nextToken="+page.NextToken, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatal(string(raw))
	}
	var page2 struct {
		Things    []thingJSON `json:"things"`
		NextToken string      `json:"nextToken"`
	}
	if err := json.Unmarshal(raw, &page2); err != nil {
		t.Fatal(err)
	}
	if len(page2.Things) != 1 || page2.Things[0].ThingName != "c" || page2.NextToken != "" {
		t.Fatalf("第二页 = %+v", page2)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things?attributeName=site&attributeValue=b", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatal(string(raw))
	}
	var filtered struct {
		Things []thingJSON `json:"things"`
	}
	if err := json.Unmarshal(raw, &filtered); err != nil {
		t.Fatal(err)
	}
	if len(filtered.Things) != 1 || filtered.Things[0].ThingName != "b" {
		t.Fatalf("属性筛选 = %+v", filtered.Things)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things?maxResults=251", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("maxResults = %d %s", resp.StatusCode, raw)
	}

	resp, raw = do(t, srv, http.MethodGet, Prefix+"/endpoint?endpointType=iot:Data-ATS", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatal(string(raw))
	}
	var ep struct {
		EndpointAddress string `json:"endpointAddress"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil {
		t.Fatal(err)
	}
	if ep.EndpointAddress != "127.0.0.1:8883" {
		t.Fatalf("端点 = %q，应是配置的地址而不是 ATS 域名", ep.EndpointAddress)
	}

	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/things/b?expectedVersion=2", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("错误版本删除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodDelete, Prefix+"/things/b?expectedVersion=1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = do(t, srv, http.MethodGet, Prefix+"/things/b", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除后 = %d %s", resp.StatusCode, raw)
	}
}
