package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/itmisx/open-iot-core/internal/cluster"
)

// TestListMembersReportsQuorum 成员接口要给出多数派所需票数。
//
// 只给名单不给这个数字，运维就得自己心算「还能掉几个」——而成员变更
// 之后它不再等于配置里的 voters（CLU-090、OBS-147）。
func TestListMembersReportsQuorum(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	code, body := r.adminReq(t, "GET", "/api/admin/v1/cluster/members", nil)
	if code != http.StatusOK {
		t.Fatalf("状态 = %d: %s", code, body)
	}
	var got struct {
		Members []memberResponse `json:"members"`
		Quorum  int              `json:"quorum"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	// 单机：只有本节点一条，多数派 1。不能报 0 条——那会让控制台
	// 显示「集群里没有节点」，而这个进程正在服务。
	if len(got.Members) != 1 || got.Quorum != 1 {
		t.Fatalf("成员 = %+v 多数派 = %d，期望 1 条与 1", got.Members, got.Quorum)
	}
	if !got.Members[0].Self {
		t.Error("单机那一条没标成 self")
	}
	if got.Members[0].ID != r.s.mf.NodeID {
		t.Errorf("成员 ID = %q，期望 %q", got.Members[0].ID, r.s.mf.NodeID)
	}
}

// TestMemberChangeNeedsToken 成员变更是管理面写操作（CLU-094）。
//
// 它把元数据写入权发给一个新节点，等级与签发证书相同；无令牌可改成员
// 等于任何人都能往这个集群里塞一个自己的节点。
func TestMemberChangeNeedsToken(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	for _, c := range []struct{ method, path string }{
		{"GET", "/api/admin/v1/cluster/members"},
		{"POST", "/api/admin/v1/cluster/members"},
		{"DELETE", "/api/admin/v1/cluster/members/d"},
	} {
		code, _ := r.adminReqToken(t, c.method, c.path, map[string]string{
			"id": "d", "addr": "127.0.0.1:1",
		}, "")
		if code != http.StatusUnauthorized {
			t.Errorf("%s %s 无令牌 = %d，期望 401", c.method, c.path, code)
		}
	}
}

// TestMemberChangeRejectedOnStandalone 单机没有复制组，改成员要明确报错。
//
// 静默成功的后果是：调用方以为加上了，而那个节点永远收不到日志。
func TestMemberChangeRejectedOnStandalone(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	code, body := r.adminReq(t, "POST", "/api/admin/v1/cluster/members",
		map[string]string{"id": "d", "addr": "127.0.0.1:7946"})
	if code == http.StatusOK {
		t.Fatalf("单机下加成员居然成功了: %s", body)
	}
	code, body = r.adminReq(t, "DELETE", "/api/admin/v1/cluster/members/d", nil)
	if code == http.StatusOK {
		t.Fatalf("单机下移除成员居然成功了: %s", body)
	}
	// 未知字段一律报错：与配置解析同一条原则（NFR-060a）。
	// 容忍的后果是把 "addres" 的拼写错当成空地址，加进来一个拨不通的成员。
	code, body = r.adminReq(t, "POST", "/api/admin/v1/cluster/members",
		`{"id":"d","addres":"127.0.0.1:7946"}`)
	if code != http.StatusBadRequest {
		t.Errorf("拼错的字段 = %d: %s，期望 400", code, body)
	}
}

// TestMemberErrorStatusCodes 失败原因要映射到能区分「做不了」与「不该做」
// 的状态码（CLU-094）。
//
// 全给 400 的后果是：控制台与脚本把「此刻不是 leader」当成请求本身有毛病
// 而不再重试，于是一次本该几百毫秒后成功的变更变成要人工介入。
func TestMemberErrorStatusCodes(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		// 此刻做不了：换个节点或等一会儿就行
		{cluster.ErrNotLeader, http.StatusServiceUnavailable},
		{cluster.ErrNoQuorum, http.StatusServiceUnavailable},
		{cluster.ErrConfInFlight, http.StatusServiceUnavailable},
		{cluster.ErrDraining, http.StatusServiceUnavailable},
		// 与当前状态冲突：重试多少次都一样
		{cluster.ErrAlreadyMember, http.StatusConflict},
		{cluster.ErrNotMember, http.StatusConflict},
		{cluster.ErrLastMember, http.StatusConflict},
		{errors.New("别的毛病"), http.StatusBadRequest},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		writeMemberError(w, c.err)
		if w.Code != c.want {
			t.Errorf("%v = %d，期望 %d", c.err, w.Code, c.want)
		}
		// 包装过一层也要认出来：调用链上会包上下文
		w = httptest.NewRecorder()
		writeMemberError(w, errors.Join(errors.New("上下文"), c.err))
		if w.Code != c.want {
			t.Errorf("包装后的 %v = %d，期望 %d（没用 errors.Is 比较）",
				c.err, w.Code, c.want)
		}
	}
}

// TestMemberChangeNeedsReplication 单机模式下成员变更要明确拒绝。
//
// 这条取代了原先的 TestConfNopKeepsRevisionContiguous:成员变更曾经要带
// 一条 Nop 占位命令，因为上层的 revision 判据是"恰好加一",而成员变更
// 占掉的日志序号会让它报「序号不连续」。换成 hashicorp/raft 之后判据
// 放宽成"严格递增"(成员变更与当选 no-op 都占序号且不进状态机),
// 占位命令随之删掉——相应的不变量现在由
// meta 的 TestPushSeamAcceptsSparseIndexes 盯着。
func TestMemberChangeNeedsReplication(t *testing.T) {
	r := newAdminRig(t, nil)
	r.start()
	if err := r.s.AddClusterMember("d", "127.0.0.1:9999", 9); err == nil {
		t.Error("单机模式下加成员应当被拒")
	}
	if err := r.s.RemoveClusterMember("d"); err == nil {
		t.Error("单机模式下移除成员应当被拒")
	}
	// 成员列表仍要给出本节点自己：控制台的集群页在单机下也要有内容。
	members := r.s.ClusterMembers()
	if len(members) != 1 || string(members[0].ID) != r.s.mf.NodeID {
		t.Errorf("单机成员列表 = %+v", members)
	}
}
