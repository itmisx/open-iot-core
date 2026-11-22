// 管理面交互：预置设备与策略、读服务端指标。

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ---------- 管理面 ----------

// seed 预置策略与设备。
//
// 不能省：insecure-none 只是跳过认证，授权仍然要求设备上挂着策略
// （policiesFor 从热视图按设备名取），没有策略的连接连 iot:Connect 都过不了。
// 也就是说这一轮压测里策略求值是实际发生的，只是策略本身放行一个扁平前缀。
func (r *runner) seed(names []string) error {
	if r.token == "" {
		return errors.New("预置需要管理面令牌（-token），或用 -setup=false 跳过")
	}
	policy := fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "iot:Connect", "Resource": "arn:aws:iot:*:*:client/%s-*"},
    {"Effect": "Allow", "Action": ["iot:Publish", "iot:Receive"],
     "Resource": ["arn:aws:iot:*:*:topic/%s/*", "arn:aws:iot:*:*:topic/$aws/things/*"]},
    {"Effect": "Allow", "Action": "iot:Subscribe",
     "Resource": ["arn:aws:iot:*:*:topicfilter/%s/*", "arn:aws:iot:*:*:topicfilter/$aws/things/*"]}
  ]
}`, r.prefix, r.prefix, r.prefix)
	if err := r.apiPut("/api/admin/v1/policies/"+r.prefix, policy); err != nil {
		return fmt.Errorf("建策略失败: %w", err)
	}
	const batchSize = 512
	type seedThing struct {
		Name       string `json:"name"`
		PolicyName string `json:"policyName"`
	}
	for from := 0; from < len(names); from += batchSize {
		to := min(from+batchSize, len(names))
		things := make([]seedThing, to-from)
		for i := from; i < to; i++ {
			things[i-from] = seedThing{Name: names[i], PolicyName: r.prefix}
		}
		raw, err := json.Marshal(struct {
			Things []seedThing `json:"things"`
		}{Things: things})
		if err != nil {
			return fmt.Errorf("编码设备批次失败: %w", err)
		}
		if err := r.apiRequest(http.MethodPost, "/api/admin/v1/things:batch", string(raw)); err != nil {
			return fmt.Errorf("预置设备 %d..%d 失败: %w", from, to-1, err)
		}
	}
	return nil
}

// apiPut 带重试。
//
// 重试不是为了掩盖错误，而是因为集群下的 503「不是 leader」是**暂态**的：
// 预置跑在上一轮压测刚结束之际，那时成千条连接正在断开、会话记录正在经
// 分片日志删除，一次选举抖动就足以让几条写入撞上 503。不重试的结果是整轮
// 基准在预置阶段失败，而失败的原因与被测的性能毫无关系。
//
// 只重试 5xx 与 429：4xx 是请求本身写错了，重试一百次也一样。
func (r *runner) apiPut(path, body string) error {
	return r.apiRequest(http.MethodPut, path, body)
}

func (r *runner) apiRequest(method, path, body string) error {
	const attempts = 12
	var last error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * 250 * time.Millisecond)
		}
		retryable, err := r.apiRequestOnce(method, path, body)
		if err == nil {
			return nil
		}
		last = err
		if !retryable {
			return err
		}
	}
	return fmt.Errorf("重试 %d 次仍失败: %w", attempts, last)
}

func (r *runner) apiRequestOnce(method, path, body string) (retryable bool, err error) {
	req, err := http.NewRequest(method, r.admin+path, strings.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.adminHTTP.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode >= 300 {
		retryable = resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
		return retryable, fmt.Errorf("%s %s: %s", resp.Status, path, strings.TrimSpace(string(b)))
	}
	return false, nil
}

type memorySnapshot struct {
	heapAlloc, runtimeSys, stackInuse, rss int64
	rssOK                                  bool
}

// memorySnapshot 取服务端容量相关的四种内存口径，各取多次采样里的最小值。
//
// heap_alloc 仍是对象分配回归的主口径；runtime Sys、栈与 RSS 是容量口径。
// RSS 含 Go 运行时尚未归还给系统的页，因此它的差值不能替代 heap_alloc，
// 但百万连接验收又必须报告它，否则只看堆会漏掉运行时和真实驻留开销。
//
// 取最小值是因为单次采样落在两次 GC 之间的哪个位置完全是随机的：前后两次
// 各采一个点算差值，差值可以是负数（第一版就印出过 -7207 字节/连接）。
// 最小值近似「刚回收过」的那个水位，两端用同一个近似，差值才有意义。
func (r *runner) memorySnapshot() (memorySnapshot, error) {
	const samples = 6
	var out memorySnapshot
	for i := 0; i < samples; i++ {
		if i > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		v, err := r.gauges(
			"openiot_heap_alloc_bytes",
			"openiot_runtime_sys_bytes",
			"openiot_stack_inuse_bytes",
			"openiot_process_resident_memory_bytes",
		)
		if err != nil {
			return memorySnapshot{}, err
		}
		if i == 0 || int64(v["openiot_heap_alloc_bytes"]) < out.heapAlloc {
			out.heapAlloc = int64(v["openiot_heap_alloc_bytes"])
		}
		if i == 0 || int64(v["openiot_runtime_sys_bytes"]) < out.runtimeSys {
			out.runtimeSys = int64(v["openiot_runtime_sys_bytes"])
		}
		if i == 0 || int64(v["openiot_stack_inuse_bytes"]) < out.stackInuse {
			out.stackInuse = int64(v["openiot_stack_inuse_bytes"])
		}
		if rss, ok := v["openiot_process_resident_memory_bytes"]; ok {
			if !out.rssOK || int64(rss) < out.rss {
				out.rss = int64(rss)
			}
			out.rssOK = true
		}
	}
	return out, nil
}

// gauges 用一次 scrape 取一组指标。RSS 在非 Linux 不存在，因此允许缺失；
// 其余项缺失说明压测器与服务端版本不匹配，必须明确失败。
func (r *runner) gauges(names ...string) (map[string]float64, error) {
	want := make(map[string]struct{}, len(names))
	for _, name := range names {
		want[name] = struct{}{}
	}
	req, err := http.NewRequest(http.MethodGet, r.admin+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.adminHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("读 /metrics 失败: %s（需要管理面令牌）", resp.Status)
	}
	out, err := parseGauges(resp.Body, want)
	if err != nil {
		return nil, err
	}
	for _, required := range names[:min(3, len(names))] {
		if _, ok := out[required]; !ok {
			return nil, fmt.Errorf("指标 %s 不存在", required)
		}
	}
	return out, nil
}

func parseGauges(src io.Reader, want map[string]struct{}) (map[string]float64, error) {
	out := make(map[string]float64, len(want))
	sc := bufio.NewScanner(src)
	for sc.Scan() {
		line := sc.Text()
		for name := range want {
			if !strings.HasPrefix(line, name) {
				continue
			}
			rest := line[len(name):]
			if rest != "" && rest[0] != '{' && rest[0] != ' ' {
				continue
			}
			i := strings.LastIndexByte(line, ' ')
			if i < 0 {
				continue
			}
			if f, perr := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64); perr == nil {
				out[name] = f
			}
		}
	}
	return out, sc.Err()
}

func (r *runner) gauge(name string) (float64, error) {
	req, err := http.NewRequest(http.MethodGet, r.admin+"/metrics", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.adminHTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("读 /metrics 失败: %s（需要管理面令牌）", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if rest != "" && rest[0] != '{' && rest[0] != ' ' {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
		if err != nil {
			continue
		}
		return f, nil
	}
	return 0, fmt.Errorf("指标 %s 不存在", name)
}

func newAdminHTTPClient(sourceIP net.IP) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	if sourceIP != nil {
		dialer.LocalAddr = &net.TCPAddr{IP: sourceIP}
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}
