package shadow

// Document 是存储中的一份影子。
//
// desired 与 reported 各自带一份同形的元数据。把元数据与状态分开存而不是
// 嵌在一起，是因为两者的生命周期一致但形状不同——合并时需要同时遍历
// 两棵树，分开存使这件事显式可见（见 merge.go）。
type Document struct {
	Desired      Object
	Reported     Object
	DesiredMeta  Object
	ReportedMeta Object
	// Version 从 1 开始，每次被接受的更新加一。
	//
	// 它是复制状态的一部分，在 apply 循环中按日志顺序确定地递增
	// （PRD SHDW-026 修订版）。不是进程内计数器，也不是时间戳。
	Version int64
	// UpdatedAtSec 是最近一次更新的 Unix 秒。
	UpdatedAtSec int64
}

// Clone 深拷贝文档。
func (d *Document) Clone() *Document {
	if d == nil {
		return nil
	}
	return &Document{
		Desired:      CloneObject(d.Desired),
		Reported:     CloneObject(d.Reported),
		DesiredMeta:  CloneObject(d.DesiredMeta),
		ReportedMeta: CloneObject(d.ReportedMeta),
		Version:      d.Version,
		UpdatedAtSec: d.UpdatedAtSec,
	}
}

// stateSection 是文档中的 state 节点。
type stateSection struct {
	Desired  Object `json:"desired,omitempty"`
	Reported Object `json:"reported,omitempty"`
	Delta    Object `json:"delta,omitempty"`
}

// metaSection 是文档中的 metadata 节点。
type metaSection struct {
	Desired  Object `json:"desired,omitempty"`
	Reported Object `json:"reported,omitempty"`
}

// GetResponse 是 /get/accepted 与 GetThingShadow 的响应。
type GetResponse struct {
	State     stateSection `json:"state"`
	Metadata  metaSection  `json:"metadata"`
	Version   int64        `json:"version"`
	Timestamp int64        `json:"timestamp"`
	// ClientToken 原样回传（PRD SHDW-024）。SDK 依赖它关联请求，
	// 丢掉它会让并发请求的响应无法对应。
	ClientToken string `json:"clientToken,omitempty"`
}

// UpdateResponse 是 /update/accepted 的响应。
//
// 只含本次请求涉及的段，不是完整文档——设备据此确认"我发的那些字段
// 被接受了"，塞进完整文档会让它误以为其他字段也刚被更新过。
type UpdateResponse struct {
	State       stateSection `json:"state"`
	Metadata    metaSection  `json:"metadata"`
	Version     int64        `json:"version"`
	Timestamp   int64        `json:"timestamp"`
	ClientToken string       `json:"clientToken,omitempty"`
}

// DeltaMessage 是 /update/delta 的消息。
//
// 注意 state 直接就是 delta 本身，不再嵌一层 "delta" 键——
// 这与 /get/accepted 中 delta 作为 state 的子键不同，是 AWS 的既有形状。
type DeltaMessage struct {
	Version     int64  `json:"version"`
	Timestamp   int64  `json:"timestamp"`
	State       Object `json:"state"`
	Metadata    Object `json:"metadata,omitempty"`
	ClientToken string `json:"clientToken,omitempty"`
}

// documentSnapshot 是 /update/documents 中的一份文档。
type documentSnapshot struct {
	State    stateSection `json:"state"`
	Metadata metaSection  `json:"metadata"`
	Version  int64        `json:"version"`
}

// DocumentsMessage 是 /update/documents 的消息（PRD SHDW-015）。
type DocumentsMessage struct {
	// Previous 在影子首次创建时为 nil。
	Previous    *documentSnapshot `json:"previous"`
	Current     *documentSnapshot `json:"current"`
	Timestamp   int64             `json:"timestamp"`
	ClientToken string            `json:"clientToken,omitempty"`
}

// DeleteResponse 是 /delete/accepted 的响应。
type DeleteResponse struct {
	Version     int64  `json:"version"`
	Timestamp   int64  `json:"timestamp"`
	ClientToken string `json:"clientToken,omitempty"`
}

// ErrorResponse 是各 /rejected 主题与 REST 错误的响应形状。
type ErrorResponse struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	ClientToken string `json:"clientToken,omitempty"`
}

// ListNamedShadowsResponse 是 ListNamedShadowsForThing 的响应。
type ListNamedShadowsResponse struct {
	Results     []string `json:"results"`
	Timestamp   int64    `json:"timestamp"`
	NextToken   string   `json:"nextToken,omitempty"`
	ClientToken string   `json:"clientToken,omitempty"`
}

// get 构造完整文档的响应。
func (d *Document) get(token string, nowSec int64) *GetResponse {
	return &GetResponse{
		State: stateSection{
			Desired:  CloneObject(d.Desired),
			Reported: CloneObject(d.Reported),
			Delta:    delta(d.Desired, d.Reported),
		},
		Metadata: metaSection{
			Desired:  CloneObject(d.DesiredMeta),
			Reported: CloneObject(d.ReportedMeta),
		},
		Version:     d.Version,
		Timestamp:   nowSec,
		ClientToken: token,
	}
}

// snapshot 构造 /update/documents 用的文档快照。
//
// 与 get 不同，它不含 delta：documents 消息的用途是让订阅方看到
// 前后两份完整文档，delta 由单独的主题承载。
func (d *Document) snapshot() *documentSnapshot {
	return &documentSnapshot{
		State: stateSection{
			Desired:  CloneObject(d.Desired),
			Reported: CloneObject(d.Reported),
		},
		Metadata: metaSection{
			Desired:  CloneObject(d.DesiredMeta),
			Reported: CloneObject(d.ReportedMeta),
		},
		Version: d.Version,
	}
}
