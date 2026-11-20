package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/itmisx/open-iot-core/internal/events"
)

// auditLogName 是审计日志的文件名，放在数据目录里，不进运行日志。
const auditLogName = "audit.log"

// auditLine 是审计文件里的一行。
//
// 字段就这些。策略正文、私钥、令牌和设备负载没有位置可放，
// 调用方若把它们塞进 Target，测试会在文件里搜到。
type auditLine struct {
	At     int64  `json:"at"`
	Node   string `json:"node"`
	Seq    uint64 `json:"seq"`
	Kind   string `json:"kind"`
	Actor  string `json:"actor,omitempty"`
	Remote string `json:"remote,omitempty"`
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`
	Action string `json:"action"`
	Target string `json:"target,omitempty"`
	Role   string `json:"role,omitempty"`
	Detail string `json:"detail,omitempty"`
}

func (s *Server) emitAudit(actor, remote, method, path, action, target string) {
	s.bus.Emit(&events.Event{
		Kind: events.KindAudit,
		Audit: events.AuditFields{
			Actor: actor, Remote: remote, Method: method,
			Path: path, Action: action, Target: target,
		},
	})
}

// watchAudit 把审计事件与集群变更写成独立的 JSON 行。
//
// 集群变更已经在总线上，这里只是它的一个消费者。再在成员变更处
// 打一行日志，就会和这条分叉，以后两边各记各的。
func (s *Server) watchAudit(ctx context.Context) {
	defer s.wg.Done()

	f, err := os.OpenFile(filepath.Join(s.dd.Root(), auditLogName),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.log.Error("审计日志无法打开", "错误", err.Error())
		return
	}
	defer func() { _ = f.Close() }()

	ch, cancel := s.bus.Subscribe(events.Filter{
		Kinds: events.KindAudit.Mask() | events.KindCluster.Mask(),
	}, 256)
	defer cancel()

	enc := json.NewEncoder(f)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(toAuditLine(&ev)); err != nil {
				s.log.Error("审计日志写入失败", "错误", err.Error())
				continue
			}
			_ = f.Sync()
		}
	}
}

// AppendAudit 把命令行上已经成功的写操作追加进 audit.log。
//
// 命令行和节点不是同一个进程，没有事件总线可发。这里写的一行与
// watchAudit 的字段相同，避免两套格式。调用方只在操作成功之后调用：
// 失败的签发若也记一行，审计会把没发生的事说成发生了。
// 目标只放指纹、设备名或吊销原因。私钥没有字段可放。
func AppendAudit(dir string, at int64, nodeID, path, action, target string) error {
	raw, err := json.Marshal(auditLine{
		At: at, Node: nodeID, Kind: events.KindAudit.String(),
		Actor: "cli", Method: "cli", Path: path,
		Action: action, Target: target,
	})
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	f, err := os.OpenFile(filepath.Join(dir, auditLogName),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(raw)
	serr := f.Sync()
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	if serr != nil {
		return serr
	}
	return cerr
}

func toAuditLine(ev *events.Event) auditLine {
	line := auditLine{
		At: ev.AtUnixMs, Node: string(ev.Node), Seq: ev.Seq, Kind: ev.Kind.String(),
	}
	switch ev.Kind {
	case events.KindCluster:
		line.Action = "Cluster"
		line.Target = string(ev.Cluster.Member)
		line.Role = ev.Cluster.Role
		line.Detail = ev.Cluster.Detail
	default:
		line.Actor = ev.Audit.Actor
		line.Remote = ev.Audit.Remote
		line.Method = ev.Audit.Method
		line.Path = ev.Audit.Path
		line.Action = ev.Audit.Action
		line.Target = ev.Audit.Target
	}
	return line
}
