package awsapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
	"github.com/itmisx/open-iot-core/internal/quota"
	"github.com/itmisx/open-iot-core/internal/shadow"
	"github.com/itmisx/open-iot-core/internal/sigv4"
)

type recordedPublish struct {
	topic   string
	payload []byte
	qos     uint8
	retain  bool
}

type memRetained struct {
	mu    sync.Mutex
	items map[string]RetainedMessage
}

func (m *memRetained) Get(topic string) (RetainedMessage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[topic]
	return v, ok
}

func (m *memRetained) List(after string, limit int) ([]RetainedMessage, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.items))
	for name := range m.items {
		names = append(names, name)
	}
	page, next := pageAfter(names, after, limit)
	out := make([]RetainedMessage, 0, len(page))
	for _, name := range page {
		out = append(out, m.items[name])
	}
	return out, next
}

func (m *memRetained) put(topic string, msg RetainedMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(msg.Payload) == 0 {
		delete(m.items, topic)
		return
	}
	m.items[topic] = msg
}

func newDataServer(t *testing.T) (*httptest.Server, *[]recordedPublish) {
	t.Helper()
	pubs := &[]recordedPublish{}
	var mu sync.Mutex
	ret := &memRetained{items: map[string]RetainedMessage{}}
	clk := func() int64 { return 1_700_000_000_000 }
	svc, err := shadow.New(shadow.Options{
		Store:       shadow.NewMemStore(),
		Partitioner: cluster.NewLocalPartitioner(cluster.NodeID("node-1"), 64),
		Limiter:     quota.NewLocal(quota.Options{Now: clk}),
		Now:         clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, srv := newHarnessFull(t, nil, func(h *Handler) {
		h.Shadows = svc
		h.Retained = ret
		h.Now = clk
		h.Publish = func(_ context.Context, topic string, payload []byte, qos uint8, retain bool) error {
			cp := append([]byte(nil), payload...)
			mu.Lock()
			*pubs = append(*pubs, recordedPublish{topic, cp, qos, retain})
			mu.Unlock()
			if retain {
				ret.put(topic, RetainedMessage{
					Topic: topic, Payload: cp, PayloadSize: len(cp),
					QoS: qos, ModifiedUnixMs: clk(),
				})
			}
			return nil
		}
	})
	return srv, pubs
}

func doData(t *testing.T, srv *httptest.Server, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	return doSvc(t, srv, method, path, body, sigv4.ServiceIoTData)
}

func doSvc(t *testing.T, srv *httptest.Server, method, path, body, service string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	signSvc(t, req, []byte(body), service)
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

func awsType(t *testing.T, raw []byte) string {
	t.Helper()
	var body struct {
		Type string `json:"__type"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("响应不是 JSON: %s", raw)
	}
	return body.Type
}

// TestDataPlanePublish 覆盖数据面 Publish 的拒绝与保留消息往返。
//
// 签成 iot、带上 MQTT 5 字段、qos 大于 1、负载超过 128 KiB，都必须在发布前失败。
// 去掉这些判断，对应用例会变成 200。扇出与保留往返也写在这里：
// 回调没被调用，或空负载没把记录清掉，断言就会失败。
func TestDataPlanePublish(t *testing.T) {
	srv, pubs := newDataServer(t)

	resp, raw := doSvc(t, srv, http.MethodPost, Prefix+"/topics/a", "hi", sigv4.ServiceIoT)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("签成 iot = %d %s，数据面只接受 iotdata", resp.StatusCode, raw)
	}
	if len(*pubs) != 0 {
		t.Fatal("签错服务名仍然发布了")
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/sensors/temp?qos=1&retain=true", "hello")
	if resp.StatusCode != http.StatusOK || len(raw) != 0 {
		t.Fatalf("发布 = %d %q，成功应是空的 200", resp.StatusCode, raw)
	}
	if len(*pubs) != 1 || (*pubs)[0].topic != "sensors/temp" || (*pubs)[0].qos != 1 || !(*pubs)[0].retain {
		t.Fatalf("发布记录 = %+v", *pubs)
	}

	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/retainedMessage/sensors/temp", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("读取保留 = %d %s", resp.StatusCode, raw)
	}
	var got struct {
		Topic    string `json:"topic"`
		Payload  string `json:"payload"`
		QoS      int    `json:"qos"`
		Modified int64  `json:"lastModifiedTime"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	plain, err := base64.StdEncoding.DecodeString(got.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "sensors/temp" || string(plain) != "hello" || got.QoS != 1 || got.Modified != 1_700_000_000_000 {
		t.Fatalf("保留消息 = %+v 正文 %q", got, plain)
	}

	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/retainedMessage?maxResults=1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("列举 = %d %s", resp.StatusCode, raw)
	}
	var page struct {
		Topics []struct {
			Topic string `json:"topic"`
		} `json:"retainedTopics"`
		Next string `json:"nextToken"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Topics) != 1 || page.Topics[0].Topic != "sensors/temp" || page.Next != "" {
		t.Fatalf("一页就够时 = %+v", page)
	}
	if strings.Contains(string(raw), `"nextToken"`) {
		t.Fatalf("没有下一页仍返回了 nextToken: %s", raw)
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/floor%2Ftemp?retain=true", "x")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("编码斜杠 = %d %s", resp.StatusCode, raw)
	}
	if (*pubs)[len(*pubs)-1].topic != "floor/temp" {
		t.Fatalf("主题 = %q，路径里的 %%2F 应还原成一级分隔", (*pubs)[len(*pubs)-1].topic)
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/a?qos=2", "x")
	if resp.StatusCode != http.StatusBadRequest || awsType(t, raw) != "InvalidRequestException" {
		t.Fatalf("qos 2 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/a?contentType=json", "x")
	if resp.StatusCode != http.StatusBadRequest || awsType(t, raw) != "InvalidRequestException" {
		t.Fatalf("contentType = %d %s", resp.StatusCode, raw)
	}

	req, err := http.NewRequest(http.MethodPost, srv.URL+Prefix+"/topics/a", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Mqtt5-User-Properties", "k=v")
	signSvc(t, req, []byte("x"), sigv4.ServiceIoTData)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || awsType(t, raw) != "InvalidRequestException" {
		t.Fatalf("MQTT 5 头 = %d %s", resp.StatusCode, raw)
	}

	big := strings.Repeat("a", maxPublishPayload+1)
	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/a", big)
	if resp.StatusCode != http.StatusRequestEntityTooLarge || awsType(t, raw) != "RequestEntityTooLargeException" {
		t.Fatalf("超限 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/topics/sensors/temp?retain=true", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("清除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/retainedMessage/sensors/temp", "")
	if resp.StatusCode != http.StatusNotFound || awsType(t, raw) != "ResourceNotFoundException" {
		t.Fatalf("清除后 = %d %s", resp.StatusCode, raw)
	}
}

// TestDataPlaneShadow 验证 REST 影子走同一套状态机，并把 delta 交出去。
//
// 更新成功却没有 delta 主题的发布，说明扇出被拿掉了。设备就收不到这次变更。
func TestDataPlaneShadow(t *testing.T) {
	srv, pubs := newDataServer(t)

	resp, raw := doData(t, srv, http.MethodGet, Prefix+"/things/lamp/shadow", "")
	if resp.StatusCode != http.StatusNotFound || awsType(t, raw) != "ResourceNotFoundException" {
		t.Fatalf("没有影子 = %d %s", resp.StatusCode, raw)
	}

	body := `{"state":{"desired":{"a":1}}}`
	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/things/lamp/shadow", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("更新 = %d %s", resp.StatusCode, raw)
	}
	var updated map[string]any
	if err := json.Unmarshal(raw, &updated); err != nil {
		t.Fatal(err)
	}
	state, _ := updated["state"].(map[string]any)
	desired, _ := state["desired"].(map[string]any)
	if desired["a"] != float64(1) {
		t.Fatalf("更新响应 = %s", raw)
	}
	sawDelta := false
	for _, p := range *pubs {
		if p.topic == "$aws/things/lamp/shadow/update/delta" {
			sawDelta = p.qos == 1 && !p.retain && len(p.payload) > 0
		}
	}
	if !sawDelta {
		t.Fatalf("没有以 QoS 1 扇出 delta: %+v", *pubs)
	}

	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/things/lamp/shadow", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"desired"`) {
		t.Fatalf("读取 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/things/lamp/shadow", `{"state":{"desired":{"a":2}},"version":9}`)
	if resp.StatusCode != http.StatusConflict || awsType(t, raw) != "ConflictException" {
		t.Fatalf("版本冲突 = %d %s", resp.StatusCode, raw)
	}

	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/things/lamp/shadow?name=cfg", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("命名影子 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodPost, Prefix+"/things/lamp/shadow?name=other", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("第二个命名影子 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/api/things/shadow/ListNamedShadowsForThing/lamp?pageSize=1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("列举 = %d %s", resp.StatusCode, raw)
	}
	var listed struct {
		Results   []string `json:"results"`
		Next      string   `json:"nextToken"`
		Timestamp int64    `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Results) != 1 || listed.Next == "" || listed.Timestamp != 1_700_000_000_000 {
		t.Fatalf("第一页 = %+v", listed)
	}
	if listed.Results[0] == "" {
		t.Fatal("经典影子不应出现在命名列表里")
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/api/things/shadow/ListNamedShadowsForThing/lamp?pageSize=1&nextToken="+listed.Next, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("第二页 = %d %s", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), `"nextToken"`) {
		t.Fatalf("最后一页仍返回 nextToken: %s", raw)
	}

	resp, raw = doData(t, srv, http.MethodDelete, Prefix+"/things/lamp/shadow", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删除 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/things/lamp/shadow", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除后 = %d %s", resp.StatusCode, raw)
	}
}

// TestDataPlaneDisabled 验证没接上 broker 或影子时返回 503，而不是空成功。
func TestDataPlaneDisabled(t *testing.T) {
	_, srv := newHarness(t)
	resp, raw := doData(t, srv, http.MethodPost, Prefix+"/topics/a", "x")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("发布 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/things/lamp/shadow", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("影子 = %d %s", resp.StatusCode, raw)
	}
	resp, raw = doData(t, srv, http.MethodGet, Prefix+"/retainedMessage", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("保留 = %d %s", resp.StatusCode, raw)
	}
}
