package trace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Wrap 在 HTTP 服务外面包一层服务端 span。
//
// 只记录方法、路径和状态码。查询串不记：事件流的令牌在查询参数里。
// 请求头和正文也不记。
func (t *Tracer) Wrap(next http.Handler) http.Handler {
	if !t.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, sp := t.start(r.Context(), r.Method, kindServer, r.Header.Get("Traceparent"))
		liveSp, ok := sp.(*live)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		liveSp.Set("http.request.method", r.Method)
		liveSp.Set("url.path", r.URL.Path)
		w.Header().Set("traceresponse", "00-"+liveSp.traceID+"-"+liveSp.spanID+"-01")
		rec := &codeWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))
		name := r.Method + " " + r.URL.Path
		if r.Pattern != "" {
			name = r.Pattern
		}
		liveSp.rename(name)
		liveSp.SetInt("http.response.status_code", rec.code)
		if rec.code >= 500 {
			liveSp.SetError()
		}
		liveSp.End()
	})
}

type codeWriter struct {
	http.ResponseWriter
	code  int
	wrote bool
}

func (c *codeWriter) WriteHeader(code int) {
	if !c.wrote {
		c.code = code
		c.wrote = true
	}
	c.ResponseWriter.WriteHeader(code)
}

func (c *codeWriter) Write(b []byte) (int, error) {
	if !c.wrote {
		c.code = http.StatusOK
		c.wrote = true
	}
	return c.ResponseWriter.Write(b)
}

func (c *codeWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (c *codeWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

type finished struct {
	name     string
	kind     int
	traceID  string
	spanID   string
	parentID string
	startMs  int64
	endMs    int64
	attrs    []attr
	err      bool
}

func (t *Tracer) exportOne(sp finished) {
	body, err := json.Marshal(otlpPayload(t.service, sp))
	if err != nil {
		t.fail(err)
		return
	}
	url := t.endpoint + "/v1/traces"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.fail(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		t.fail(err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		t.fail(fmt.Errorf("trace: 收集器返回 %d", resp.StatusCode))
	}
}

func (t *Tracer) fail(err error) {
	if t.onError != nil {
		t.once.Do(func() { t.onError(err) })
	}
}

func otlpPayload(service string, sp finished) otlpExport {
	status := 1
	if sp.err {
		status = 2
	}
	out := otlpSpan{
		TraceID:           sp.traceID,
		SpanID:            sp.spanID,
		Name:              sp.name,
		Kind:              sp.kind,
		StartTimeUnixNano: strconv.FormatInt(sp.startMs*1_000_000, 10),
		EndTimeUnixNano:   strconv.FormatInt(sp.endMs*1_000_000, 10),
		Status:            otlpStatus{Code: status},
	}
	if sp.parentID != "" {
		out.ParentSpanID = sp.parentID
	}
	for _, a := range sp.attrs {
		v := otlpValue{StringValue: a.value}
		if a.isInt {
			v = otlpValue{IntValue: a.value}
		}
		out.Attributes = append(out.Attributes, otlpAttr{Key: a.key, Value: v})
	}
	return otlpExport{ResourceSpans: []otlpResourceSpan{{
		Resource: otlpResource{Attributes: []otlpAttr{{
			Key: "service.name", Value: otlpValue{StringValue: service},
		}}},
		ScopeSpans: []otlpScopeSpan{{
			Scope: otlpScope{Name: service},
			Spans: []otlpSpan{out},
		}},
	}}}
}

type otlpExport struct {
	ResourceSpans []otlpResourceSpan `json:"resourceSpans"`
}

type otlpResourceSpan struct {
	Resource   otlpResource    `json:"resource"`
	ScopeSpans []otlpScopeSpan `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpAttr `json:"attributes"`
}

type otlpScopeSpan struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpSpan struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId,omitempty"`
	Name              string     `json:"name"`
	Kind              int        `json:"kind"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	EndTimeUnixNano   string     `json:"endTimeUnixNano"`
	Attributes        []otlpAttr `json:"attributes,omitempty"`
	Status            otlpStatus `json:"status"`
}

type otlpAttr struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	StringValue string `json:"stringValue,omitempty"`
	IntValue    string `json:"intValue,omitempty"`
}

type otlpStatus struct {
	Code int `json:"code"`
}
