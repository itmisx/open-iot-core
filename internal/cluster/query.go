package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Admin 面的扇出查询（契约 12、ARCH-040、ARCH-041）。
//
// 控制台的查询天然是全集群的——「列出所有在线连接」必须跨节点聚合，
// 只看本节点会显示一份**看起来完整而其实少了大半**的列表，而用户
// 无从知道自己漏看了什么。
//
// 两条设计约束来自 PRD：
//
//   - 游标必须编码每个节点各自的位置（ARCH-040）。单一 offset 下合并
//     分页无法正确推进：各节点的行数不同，按一个全局偏移翻页会跳过
//     某些节点的行、重复另一些。
//   - 单节点失败必须表达为**部分失败**而不是整体失败（ARCH-041）。
//     运维工具在故障时必须仍然可用，那正是最需要它的时刻。

// wireQuery 是扇出查询的线上表示。
//
// 行的内容以字节传输（见 wireResp.Rows 的说明）：传输层不认识任何
// 具体查询的行类型，那样它才不必随每个新查询改一次。
type wireQuery struct {
	Kind   string
	Params map[string]string
	Cursor string
	Limit  int
}

// QueryHandler 在本节点执行一次查询。
//
// rows 是已经编码的行；next 为空表示本节点读完了。
type QueryHandler func(kind string, params map[string]string, cursor string, limit int) (rows [][]byte, next string, err error)

// SetQueryHandler 注册本节点的查询执行函数。
func (g *TLSGroup) SetQueryHandler(fn QueryHandler) {
	g.queryMu.Lock()
	g.queryFn = fn
	g.queryMu.Unlock()
}

// runQuery 在本节点执行一次收到的查询。
func (g *TLSGroup) runQuery(q wireQuery) wireResp {
	g.queryMu.RLock()
	fn := g.queryFn
	g.queryMu.RUnlock()
	if fn == nil {
		return wireResp{Err: "query handler not set"}
	}
	rows, next, err := fn(q.Kind, q.Params, q.Cursor, q.Limit)
	if err != nil {
		return wireResp{Err: err.Error()}
	}
	return wireResp{OK: true, Rows: rows, NextCursor: next}
}

// askPeer 向一个对端发一次查询。
func (g *TLSGroup) askPeer(to NodeID, q wireQuery) ([][]byte, string, error) {
	resp, ok := g.roundTrip(to, wireMsg{
		Kind: wireQueryKind, From: string(g.id), Query: &q,
	})
	if !ok {
		return nil, "", fmt.Errorf("cluster: %s 不可达", to)
	}
	if resp.Err != "" {
		return nil, "", fmt.Errorf("cluster: %s 执行查询失败: %s", to, resp.Err)
	}
	return resp.Rows, resp.NextCursor, nil
}

// Query 扇出到若干节点并收集结果。
//
// 本节点也在列表里时直接走本地处理函数，不绕一圈网络。
//
// 单个节点失败体现在对应 AdminResult.Err 上，整体仍返回 nil error
// （ARCH-041）。并发发出：扇出的耗时是最慢那个节点的，而不是累加。
func (t *MeshTransport) Query(ctx context.Context, to []NodeID, q AdminQuery) ([]AdminResult, error) {
	if t.isClosed() {
		return nil, ErrClosed
	}
	if len(to) == 0 {
		return nil, nil
	}
	wq := wireQuery{Kind: q.Kind, Params: q.Params, Limit: q.Limit}
	out := make([]AdminResult, len(to))
	var wg sync.WaitGroup
	for i, node := range to {
		wg.Add(1)
		go func(i int, node NodeID) {
			defer wg.Done()
			res := AdminResult{Node: node}
			per := wq
			per.Cursor = q.Cursor[node]
			var rows [][]byte
			var next string
			var err error
			if node == t.self {
				rows, next, err = t.group.localQuery(per)
			} else {
				rows, next, err = t.group.askPeer(node, per)
			}
			if err != nil {
				res.Err = err
			} else {
				res.Items = make([]any, 0, len(rows))
				for _, raw := range rows {
					res.Items = append(res.Items, raw)
				}
				res.NextCursor = next
			}
			out[i] = res
		}(i, node)
	}
	wg.Wait()
	return out, nil
}

// localQuery 在本节点执行查询，不经网络。
func (g *TLSGroup) localQuery(q wireQuery) ([][]byte, string, error) {
	resp := g.runQuery(q)
	if resp.Err != "" {
		return nil, "", fmt.Errorf("cluster: 本节点执行查询失败: %s", resp.Err)
	}
	return resp.Rows, resp.NextCursor, nil
}

// EncodeCursor 把每节点的续读位置编成一个对调用方不透明的 token。
//
// 不能是单一 offset（ARCH-040）：各节点的行数不同，按一个全局偏移
// 翻页会跳过某些节点的行、重复另一些。
func EncodeCursor(per map[NodeID]string) (string, error) {
	if len(per) == 0 {
		return "", nil
	}
	m := make(map[string]string, len(per))
	for node, cur := range per {
		if cur != "" {
			m[string(node)] = cur
		}
	}
	if len(m) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// DecodeCursor 解开扇出游标。空串表示从头开始。
func DecodeCursor(token string) (map[NodeID]string, error) {
	if token == "" {
		return map[NodeID]string{}, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(token), &m); err != nil {
		return nil, fmt.Errorf("cluster: 分页标记无法解析: %w", err)
	}
	out := make(map[NodeID]string, len(m))
	for node, cur := range m {
		out[NodeID(node)] = cur
	}
	return out, nil
}
