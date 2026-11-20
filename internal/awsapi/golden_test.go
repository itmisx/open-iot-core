package awsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/itmisx/open-iot-core/internal/sigv4"
)

// 兼容面的响应形状是冻结契约（ARCH-056、API-004、TST-076）。
//
// 这份 golden 对不上时，测试失败，不会自己改文件。确认变更是有意的之后，
// 人在本地用 -update-golden 重写，评审看的就是那份 diff。CI 不得带这个开关：
// 带着它跑，门禁会把任何形状变化都写成新的基线，等于没有门禁。

var updateGolden = flag.Bool("update-golden", false, "重写兼容面 golden。只供人工确认，CI 不得打开")

func TestAWSCompatGolden(t *testing.T) {
	_, srv := newHarnessFull(t, nil, func(h *Handler) {
		h.Publish = func(context.Context, string, []byte, uint8, bool) error { return nil }
	})

	const doc = `{"policyDocument":"{\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"iot:*\",\"Resource\":\"*\"}]}"}`
	steps := []struct {
		name, method, path, body, service string
	}{
		{"DescribeEndpoint", "GET", "/api/aws/endpoint", "", ""},
		{"CreateThing", "POST", "/api/aws/things/lamp", `{"attributePayload":{"attributes":{"site":"a"}}}`, ""},
		{"DescribeThing", "GET", "/api/aws/things/lamp", "", ""},
		{"ListThings", "GET", "/api/aws/things", "", ""},
		{"CreateThingDuplicate", "POST", "/api/aws/things/lamp", `{}`, ""},
		{"DescribeThingMissing", "GET", "/api/aws/things/missing", "", ""},
		{"CreateThingTypeRejected", "POST", "/api/aws/things/typed", `{"thingTypeName":"sensor"}`, ""},
		{"CreatePolicy", "POST", "/api/aws/policies/allow", doc, ""},
		{"GetPolicy", "GET", "/api/aws/policies/allow", "", ""},
		{"ListPolicies", "GET", "/api/aws/policies", "", ""},
		{"CreatePolicyBad", "POST", "/api/aws/policies/bad", `{"policyDocument":"{"}`, ""},
		{"GetIndexing", "GET", "/api/aws/indexing/config", "", ""},
		{"SearchOff", "POST", "/api/aws/indices/search", `{"queryString":"thingName:lamp"}`, ""},
		{"UpdateIndexing", "PUT", "/api/aws/indexing/config", `{"thingIndexingConfiguration":{"thingIndexingMode":"REGISTRY","thingConnectivityIndexingMode":"OFF"}}`, ""},
		{"SearchThing", "POST", "/api/aws/indices/search", `{"queryString":"thingName:lamp"}`, ""},
		{"CreateRoleAlias", "POST", "/api/aws/role-aliases/ops", `{"roleArn":"arn:aws:iam::000000000000:role/ops"}`, ""},
		{"RoleAliasCredentials", "GET", "/api/aws/role-aliases/ops/credentials", "", ""},
		{"GetShadowDisabled", "GET", "/api/aws/things/lamp/shadow", "", sigv4.ServiceIoTData},
		{"Publish", "POST", "/api/aws/topics/floor/temp?qos=0", "21", sigv4.ServiceIoTData},
		{"GetRetainedDisabled", "GET", "/api/aws/retainedMessage/floor/temp", "", sigv4.ServiceIoTData},
	}

	var got []goldenSnap
	for _, s := range steps {
		var status int
		var raw []byte
		var errType string
		if s.service == sigv4.ServiceIoTData {
			r, body := doData(t, srv, s.method, s.path, s.body)
			status, raw = r.StatusCode, body
			errType = r.Header.Get("x-amzn-ErrorType")
		} else {
			r, body := do(t, srv, s.method, s.path, s.body)
			status, raw = r.StatusCode, body
			errType = r.Header.Get("x-amzn-ErrorType")
		}
		got = append(got, goldenSnap{
			Name: s.name, Status: status, ErrorType: errType, Body: canonBody(t, raw),
		})
	}

	path := filepath.Join("testdata", "awscompat.json")
	encoded := encodeGolden(t, got)
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("已重写 %s。这份 diff 要人看过才能进仓库。", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读不到 golden %s：%v。首次生成用 go test -update-golden", path, err)
	}
	if !bytes.Equal(want, encoded) {
		t.Fatalf("兼容面响应与 golden 不一致（ARCH-056）。\n确认是有意的形状变更后，在本地执行\n  go test ./internal/awsapi/ -run TestAWSCompatGolden -update-golden\n并把 diff 交给评审。CI 不得带 -update-golden。\n--- 期望 ---\n%s\n--- 实际 ---\n%s", want, encoded)
	}
}

type goldenSnap struct {
	Name      string          `json:"name"`
	Status    int             `json:"status"`
	ErrorType string          `json:"errorType,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
}

func canonBody(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("响应不是 JSON：%s", raw)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func encodeGolden(t *testing.T, snaps []goldenSnap) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(snaps); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
