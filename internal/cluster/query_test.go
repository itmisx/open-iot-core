package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Admin 扇出（契约 12、ARCH-040、ARCH-041）。
//
// 它错掉的方式是最坏的那种：控制台显示一份**看起来完整**的列表，
// 而其实只有一个节点的数据——用户无从知道自己漏看了什么。

// TestQueryFansOutAndMerges 扇出到两个节点并各自带回结果与游标。
func TestQueryFansOutAndMerges(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["a"].SetQueryHandler(func(kind string, _ map[string]string, cursor string, _ int) ([][]byte, string, error) {
		if kind != "conns" {
			return nil, "", errors.New("不认识的查询")
		}
		if cursor == "" {
			return [][]byte{[]byte("a1"), []byte("a2")}, "a2", nil
		}
		return nil, "", nil
	})
	groups["b"].SetQueryHandler(func(_ string, _ map[string]string, cursor string, _ int) ([][]byte, string, error) {
		if cursor == "" {
			return [][]byte{[]byte("b1")}, "b1", nil
		}
		return nil, "", nil
	})

	tp := NewMeshTransport(groups["a"])
	ctx := context.Background()
	res, err := tp.Query(ctx, []NodeID{"a", "b"}, AdminQuery{
		Kind: "conns", Scope: ScopeAllNodes, Limit: 10,
		Cursor: map[NodeID]string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("= %d 个结果", len(res))
	}
	per := map[NodeID]AdminResult{}
	for _, r := range res {
		if r.Err != nil {
			t.Fatalf("%s 失败: %v", r.Node, r.Err)
		}
		per[r.Node] = r
	}
	if len(per["a"].Items) != 2 || per["a"].NextCursor != "a2" {
		t.Errorf("a = %+v", per["a"])
	}
	if len(per["b"].Items) != 1 || per["b"].NextCursor != "b1" {
		t.Errorf("b = %+v", per["b"])
	}

	// 带着各自的游标再问一次：两边都读完了
	next, err := EncodeCursor(map[NodeID]string{"a": "a2", "b": "b1"})
	if err != nil {
		t.Fatal(err)
	}
	cur, err := DecodeCursor(next)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := tp.Query(ctx, []NodeID{"a", "b"}, AdminQuery{Kind: "conns", Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res2 {
		if r.Err != nil || len(r.Items) != 0 || r.NextCursor != "" {
			t.Errorf("第二页 %s = %+v", r.Node, r)
		}
	}
}

// TestQueryPartialFailure 一个节点失败不拖垮整次扇出。
//
// 运维工具在故障时必须仍然可用，那正是最需要它的时刻（ARCH-041）。
func TestQueryPartialFailure(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["a"].SetQueryHandler(func(string, map[string]string, string, int) ([][]byte, string, error) {
		return [][]byte{[]byte("a1")}, "", nil
	})
	groups["b"].SetQueryHandler(func(string, map[string]string, string, int) ([][]byte, string, error) {
		return nil, "", errors.New("这个节点坏了")
	})

	tp := NewMeshTransport(groups["a"])
	res, err := tp.Query(context.Background(), []NodeID{"a", "b"},
		AdminQuery{Kind: "conns", Cursor: map[NodeID]string{}})
	if err != nil {
		t.Fatalf("整体不该失败: %v", err)
	}
	var okCount, errCount int
	for _, r := range res {
		if r.Err != nil {
			errCount++
			if !strings.Contains(r.Err.Error(), "这个节点坏了") {
				t.Errorf("错误没带上原因: %v", r.Err)
			}
		} else {
			okCount++
		}
	}
	if okCount != 1 || errCount != 1 {
		t.Errorf("成功 %d 个、失败 %d 个，期望各一个", okCount, errCount)
	}
}

// TestQueryUnreachablePeerIsPartialFailure 对端不可达同样是部分失败。
func TestQueryUnreachablePeerIsPartialFailure(t *testing.T) {
	groups, _ := newTLSPair(t)
	groups["a"].SetQueryHandler(func(string, map[string]string, string, int) ([][]byte, string, error) {
		return [][]byte{[]byte("a1")}, "", nil
	})
	_ = groups["b"].Close() // b 没了

	tp := NewMeshTransport(groups["a"])
	res, err := tp.Query(context.Background(), []NodeID{"a", "b"},
		AdminQuery{Kind: "conns", Cursor: map[NodeID]string{}})
	if err != nil {
		t.Fatalf("整体不该失败: %v", err)
	}
	for _, r := range res {
		switch r.Node {
		case "a":
			if r.Err != nil || len(r.Items) != 1 {
				t.Errorf("a = %+v", r)
			}
		case "b":
			if r.Err == nil {
				t.Error("不可达的节点应当带 Err")
			}
		}
	}
}

// TestQueryEmptyTargets 没有目标时直接返回空。
func TestQueryEmptyTargets(t *testing.T) {
	groups, _ := newTLSPair(t)
	tp := NewMeshTransport(groups["a"])
	res, err := tp.Query(context.Background(), nil, AdminQuery{Kind: "conns"})
	if err != nil || res != nil {
		t.Errorf("= %+v, %v", res, err)
	}
}

// TestCursorRoundTrip 游标要编码每个节点各自的位置。
//
// 单一 offset 下合并分页无法正确推进（ARCH-040）：各节点行数不同，
// 按一个全局偏移翻页会跳过某些节点的行、重复另一些。
func TestCursorRoundTrip(t *testing.T) {
	// 空的编出空串：那表示"从头开始"，不该变成一个看起来有内容的 token
	if got, err := EncodeCursor(nil); err != nil || got != "" {
		t.Errorf("空游标 = %q, %v", got, err)
	}
	if got, err := EncodeCursor(map[NodeID]string{"a": "", "b": ""}); err != nil || got != "" {
		t.Errorf("全空游标 = %q, %v", got, err)
	}

	in := map[NodeID]string{"a": "lamp-9", "b": "pump-3"}
	token, err := EncodeCursor(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeCursor(token)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out["a"] != "lamp-9" || out["b"] != "pump-3" {
		t.Fatalf("往返 = %+v", out)
	}
	if _, err := DecodeCursor("这不是 JSON"); err == nil {
		t.Error("解不开的游标应当报错")
	}
	if got, err := DecodeCursor(""); err != nil || len(got) != 0 {
		t.Errorf("空串 = %+v, %v", got, err)
	}
}

// TestQueryRefusedWithoutHandler 没装处理函数时本节点也要明确失败。
func TestQueryRefusedWithoutHandler(t *testing.T) {
	groups, _ := newTLSPair(t)
	tp := NewMeshTransport(groups["a"])
	res, err := tp.Query(context.Background(), []NodeID{"a"},
		AdminQuery{Kind: "conns", Cursor: map[NodeID]string{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Err == nil {
		t.Errorf("= %+v，期望本节点带 Err", res)
	}
}
