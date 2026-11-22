package meta

import (
	"encoding/json"
	"fmt"
)

// 命令的线格式（CLU-064）。
//
// 日志里存的是字节，复制层只搬字节；本文件是字节与 Command 之间的唯一
// 转换处。用 JSON 而不是 gob：日志要跨版本读回（备份恢复、滚动升级），
// 而 gob 把 Go 的类型名编进流里，改个字段名就读不回来了。体积不是问题——
// 元数据命令的量级是每秒几条，不是每秒几万条。
//
// 这里刻意不编码 Change 与 Revision：前者是应用的产物，后者是日志序号，
// 两者都由复制层决定，写进命令只会给出第二个真相。

// wireCmd 是 Command 的线上表示。
//
// 单独一个类型而不给 Command 加 tag，是为了让「哪些字段会被复制」
// 在一处看全。Command 上多一个字段而这里忘了带，往返测试会红
// （TestCommandWireRoundTripCoversEveryField）。
type wireCmd struct {
	V                 int          `json:"v"`
	ID                string       `json:"id,omitempty"`
	Kind              CmdKind      `json:"kind"`
	RequestedRevision Revision     `json:"requestedRevision,omitempty"`
	Cert              *wireCert    `json:"cert,omitempty"`
	Thing             *ThingCmd    `json:"thing,omitempty"`
	Things            []ThingCmd   `json:"things,omitempty"`
	Policy            *PolicyCmd   `json:"policy,omitempty"`
	AttrSlots         []string     `json:"attrSlots,omitempty"`
	Records           []wireRecord `json:"records,omitempty"`
	Shadow            []byte       `json:"shadow,omitempty"`
}

// wireCert 把指纹编成十六进制。
//
// CertFingerprint 是 [32]byte，JSON 会把它编成 32 个数字的数组——
// 又长又没法用眼睛核对，而这条日志是要被人读的（备份、排查）。
type wireCert struct {
	Fingerprint    string      `json:"fp"`
	Status         CertStatus  `json:"status"`
	PolicySet      PolicySetID `json:"policySet"`
	Thing          string      `json:"thing,omitempty"`
	CA             string      `json:"ca,omitempty"`
	NotAfterUnixMs int64       `json:"notAfter,omitempty"`
	Reason         string      `json:"reason,omitempty"`
}

type wireRecord struct {
	Key    []byte `json:"k"`
	Value  []byte `json:"v,omitempty"`
	Delete bool   `json:"del,omitempty"`
}

// wireVersion 是当前线格式版本。
//
// 读到更高的版本要明确报错而不是尽力解析：一条解不全的命令被应用之后，
// 这个节点的视图与别人的就不一样了，而没有任何报错（CLU-066）。
const wireVersion = 1

// EncodeCommand 把命令编成日志条目。
func EncodeCommand(cmd Command) ([]byte, error) {
	w := wireCmd{
		V: wireVersion, ID: cmd.ID, Kind: cmd.Kind,
		RequestedRevision: cmd.RequestedRevision,
		Thing:             cmd.Thing, Things: cmd.Things,
		Policy: cmd.Policy, AttrSlots: cmd.AttrSlots,
		Shadow: cmd.Shadow,
	}
	if cmd.Cert != nil {
		w.Cert = &wireCert{
			Fingerprint: cmd.Cert.Fingerprint.String(),
			Status:      cmd.Cert.Status, PolicySet: cmd.Cert.PolicySet,
			Thing: cmd.Cert.Thing, CA: cmd.Cert.CA,
			NotAfterUnixMs: cmd.Cert.NotAfterUnixMs, Reason: cmd.Cert.Reason,
		}
	}
	for _, op := range cmd.Records {
		w.Records = append(w.Records, wireRecord{Key: op.Key, Value: op.Value, Delete: op.Delete})
	}
	return json.Marshal(w)
}

// DecodeCommand 把日志条目解回命令。
func DecodeCommand(raw []byte) (Command, error) {
	var w wireCmd
	if err := json.Unmarshal(raw, &w); err != nil {
		return Command{}, fmt.Errorf("meta: 日志条目无法解析: %w", err)
	}
	if w.V != wireVersion {
		return Command{}, fmt.Errorf("meta: 日志条目的格式版本是 %d，本节点只认 %d", w.V, wireVersion)
	}
	cmd := Command{
		ID: w.ID, Kind: w.Kind, RequestedRevision: w.RequestedRevision,
		Thing: w.Thing, Things: w.Things, Policy: w.Policy, AttrSlots: w.AttrSlots,
		Shadow: w.Shadow,
	}
	if w.Cert != nil {
		fp, ok := ParseFingerprint(w.Cert.Fingerprint)
		if !ok {
			return Command{}, fmt.Errorf("meta: 日志条目里的证书指纹无法解析: %q", w.Cert.Fingerprint)
		}
		cmd.Cert = &CertCmd{
			Fingerprint: fp, Status: w.Cert.Status, PolicySet: w.Cert.PolicySet,
			Thing: w.Cert.Thing, CA: w.Cert.CA,
			NotAfterUnixMs: w.Cert.NotAfterUnixMs, Reason: w.Cert.Reason,
		}
	}
	for _, op := range w.Records {
		cmd.Records = append(cmd.Records, RecordOp{Key: op.Key, Value: op.Value, Delete: op.Delete})
	}
	return cmd, nil
}
