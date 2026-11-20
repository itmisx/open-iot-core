package awsapi

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/itmisx/open-iot-core/internal/shadow"
)

// 与 MQTT 负载配额 QTA-010 一致。更大的正文会在设备侧被拒，
// 数据面若照单全收，调用方会以为发布成功了。
const maxPublishPayload = 128 * 1024

// RetainedMessage 是一条保留消息。列举时 Payload 为空，只填 PayloadSize。
type RetainedMessage struct {
	Topic          string
	Payload        []byte
	PayloadSize    int
	QoS            uint8
	ModifiedUnixMs int64
}

// RetainedView 读取本节点的保留消息。
type RetainedView interface {
	Get(topic string) (RetainedMessage, bool)
	List(after string, limit int) ([]RetainedMessage, string)
}

// DataError 是数据面要返回的 AWS 异常。
type DataError struct {
	Type    string
	Message string
}

func (e *DataError) Error() string { return e.Message }

func (h Handler) publish(w http.ResponseWriter, r *http.Request) {
	if h.Publish == nil {
		writeDataError(w, "ServiceUnavailableException", "数据面发布未启用")
		return
	}
	if msg := rejectMQTT5(r); msg != "" {
		writeDataError(w, "InvalidRequestException", msg)
		return
	}
	qos, msg := queryQoS(r.URL.Query())
	if msg != "" {
		writeDataError(w, "InvalidRequestException", msg)
		return
	}
	retain, msg := queryBool(r.URL.Query(), "retain", false)
	if msg != "" {
		writeDataError(w, "InvalidRequestException", msg)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDataError(w, "InvalidRequestException", "请求体无法读取")
		return
	}
	if len(body) > maxPublishPayload {
		writeDataError(w, "RequestEntityTooLargeException", "负载超出上限")
		return
	}
	topic := r.PathValue("topic")
	if err := h.Publish(r.Context(), topic, body, qos, retain); err != nil {
		if de, ok := err.(*DataError); ok {
			writeDataError(w, de.Type, de.Message)
			return
		}
		writeDataError(w, "InvalidRequestException", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h Handler) getShadow(w http.ResponseWriter, r *http.Request) {
	h.shadowOp(w, r, shadow.OpGet, nil)
}

func (h Handler) updateShadow(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDataError(w, "InvalidRequestException", "请求体无法读取")
		return
	}
	h.shadowOp(w, r, shadow.OpUpdate, body)
}

func (h Handler) deleteShadow(w http.ResponseWriter, r *http.Request) {
	h.shadowOp(w, r, shadow.OpDelete, nil)
}

func (h Handler) shadowOp(w http.ResponseWriter, r *http.Request, op shadow.Op, body []byte) {
	if h.Shadows == nil {
		writeDataError(w, "ServiceUnavailableException", "影子服务未启用")
		return
	}
	req := shadow.Request{
		Thing: r.PathValue("thingName"), Shadow: r.URL.Query().Get("name"), Op: op,
	}
	var out *shadow.Outcome
	switch op {
	case shadow.OpGet:
		out = h.Shadows.Get(r.Context(), req, nil)
	case shadow.OpUpdate:
		out = h.Shadows.Update(r.Context(), req, body)
	default:
		out = h.Shadows.Delete(r.Context(), req, nil)
	}
	if out.Err != nil {
		code, msg := shadowException(out.Err)
		writeDataError(w, code, msg)
		return
	}
	if op != shadow.OpGet {
		action := "DeleteThingShadow"
		if op == shadow.OpUpdate {
			action = "UpdateThingShadow"
		}
		target := req.Thing
		if req.Shadow != "" {
			target += "/" + req.Shadow
		}
		h.note(r, action, target)
	}
	// delta 与 documents 必须从这里发出。服务层只产出消息，
	// 漏掉这一步，REST 更新就不会通知设备。
	if h.Publish != nil {
		for i := range out.Publishes {
			qos := uint8(0)
			if out.Publishes[i].QoS1 {
				qos = 1
			}
			_ = h.Publish(r.Context(), out.Publishes[i].Topic, out.Publishes[i].Payload, qos, false)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Response)
}

func (h Handler) listShadows(w http.ResponseWriter, r *http.Request) {
	if h.Shadows == nil {
		writeDataError(w, "ServiceUnavailableException", "影子服务未启用")
		return
	}
	limit, msg := limitMax(r.URL.Query(), "pageSize", 100)
	if msg != "" {
		writeDataError(w, "InvalidRequestException", msg)
		return
	}
	names, e := h.Shadows.ListNamedShadows(r.Context(), r.PathValue("thingName"))
	if e != nil {
		code, text := shadowException(e)
		writeDataError(w, code, text)
		return
	}
	page, next := pageAfter(names, r.URL.Query().Get("nextToken"), limit)
	if page == nil {
		page = []string{}
	}
	resp := map[string]any{"results": page}
	if next != "" {
		resp["nextToken"] = next
	}
	if h.Now != nil {
		resp["timestamp"] = h.Now()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) getRetained(w http.ResponseWriter, r *http.Request) {
	if h.Retained == nil {
		writeDataError(w, "ServiceUnavailableException", "保留消息未启用")
		return
	}
	msg, ok := h.Retained.Get(r.PathValue("topic"))
	if !ok {
		writeDataError(w, "ResourceNotFoundException", "没有这条保留消息")
		return
	}
	resp := map[string]any{
		"topic":   msg.Topic,
		"payload": base64.StdEncoding.EncodeToString(msg.Payload),
		"qos":     msg.QoS,
	}
	if msg.ModifiedUnixMs > 0 {
		resp["lastModifiedTime"] = msg.ModifiedUnixMs
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h Handler) listRetained(w http.ResponseWriter, r *http.Request) {
	if h.Retained == nil {
		writeDataError(w, "ServiceUnavailableException", "保留消息未启用")
		return
	}
	limit, msg := limitMax(r.URL.Query(), "maxResults", 200)
	if msg != "" {
		writeDataError(w, "InvalidRequestException", msg)
		return
	}
	items, next := h.Retained.List(r.URL.Query().Get("nextToken"), limit)
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		row := map[string]any{
			"topic": it.Topic, "payloadSize": it.PayloadSize, "qos": it.QoS,
		}
		if it.ModifiedUnixMs > 0 {
			row["lastModifiedTime"] = it.ModifiedUnixMs
		}
		out = append(out, row)
	}
	resp := map[string]any{"retainedTopics": out}
	if next != "" {
		resp["nextToken"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func shadowException(e *shadow.Error) (string, string) {
	switch e.Code {
	case http.StatusNotFound:
		return "ResourceNotFoundException", e.Message
	case http.StatusConflict:
		return "ConflictException", e.Message
	case http.StatusRequestEntityTooLarge:
		return "RequestEntityTooLargeException", e.Message
	case http.StatusTooManyRequests:
		return "ThrottlingException", e.Message
	case http.StatusInternalServerError:
		return "InternalFailureException", e.Message
	default:
		return "InvalidRequestException", e.Message
	}
}

func rejectMQTT5(r *http.Request) string {
	if r.Header.Get("x-amz-mqtt5-user-properties") != "" ||
		r.Header.Get("x-amz-mqtt5-payload-format-indicator") != "" ||
		r.Header.Get("x-amz-mqtt5-correlation-data") != "" ||
		r.URL.Query().Get("contentType") != "" ||
		r.URL.Query().Get("responseTopic") != "" ||
		r.URL.Query().Has("messageExpiry") {
		return "数据面 Publish 不接受 MQTT 5 的附加字段"
	}
	return ""
}

func queryQoS(q url.Values) (uint8, string) {
	if !q.Has("qos") {
		return 0, ""
	}
	n, err := strconv.Atoi(q.Get("qos"))
	if err != nil || n < 0 || n > 1 {
		return 0, "qos 只接受 0 或 1"
	}
	return uint8(n), ""
}

func limitMax(q url.Values, key string, max int) (int, string) {
	if !q.Has(key) {
		return 25, ""
	}
	n, err := strconv.Atoi(q.Get(key))
	if err != nil || n < 1 || n > max {
		return 0, key + " 超出允许范围"
	}
	return n, ""
}

var dataStatus = map[string]int{
	"ResourceNotFoundException":            http.StatusNotFound,
	"InvalidRequestException":              http.StatusBadRequest,
	"ThrottlingException":                  http.StatusTooManyRequests,
	"UnauthorizedException":                http.StatusUnauthorized,
	"ServiceUnavailableException":          http.StatusServiceUnavailable,
	"InternalFailureException":             http.StatusInternalServerError,
	"ConflictException":                    http.StatusConflict,
	"RequestEntityTooLargeException":       http.StatusRequestEntityTooLarge,
	"MethodNotAllowedException":            http.StatusMethodNotAllowed,
	"UnsupportedDocumentEncodingException": http.StatusUnsupportedMediaType,
}

func writeDataError(w http.ResponseWriter, code, msg string) {
	status, ok := dataStatus[code]
	if !ok {
		status = http.StatusInternalServerError
		code = "InternalFailureException"
		msg = "内部错误"
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header()["x-amzn-ErrorType"] = []string{code}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Message string `json:"message"`
		Type    string `json:"__type"`
	}{Message: msg, Type: code})
}
