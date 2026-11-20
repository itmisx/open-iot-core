package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/config"
)

func awsRig(t *testing.T) *testRig {
	t.Helper()
	r := newAdminRig(t, func(c *config.Config) {
		c.AWSAPI.Enabled = true
		c.AWSAPI.Region = "us-east-1"
		c.AWSAPI.EndpointAddress = "127.0.0.1:8883"
	})
	r.start()
	return r
}

// awsCall 用给定的凭证打一次兼容面。
func awsCall(t *testing.T, r *testRig, c awsCred, method, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+r.s.AdminAddr()+awsapi.Prefix+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	signControlPlane(t, req, c)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// TestAWSKeyLifecycle 走一遍「发、用、停、恢复、删」。
//
// 这条是改动的核心主张：钥匙不再是一份改完要重启的文件（AUTH-086）。
// 停用与删除都必须**立刻**生效，否则「怀疑泄露了先停掉」这个动作
// 在界面上看起来成功了，而那把钥匙还在用。
func TestAWSKeyLifecycle(t *testing.T) {
	r := awsRig(t)
	id, secret, err := r.s.CreateAWSKey(context.Background(), "operator", "CI", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, awsKeyIDPrefix) || len(id) != 20 {
		t.Fatalf("id = %q，期望 AKIA 开头的 20 个字符", id)
	}
	if len(secret) != 40 {
		t.Fatalf("secret 长 %d，期望 40", len(secret))
	}
	cred := awsCred{id: id, secret: secret}

	if code, raw := awsCall(t, r, cred, "GET", "/things"); code != http.StatusOK {
		t.Fatalf("新发的钥匙 = %d %s", code, raw)
	}
	if _, err := r.s.SetAWSKeyDisabled(context.Background(), id, true); err != nil {
		t.Fatal(err)
	}
	// 停用的钥匙与不存在的钥匙对调用方是同一个 401（AUTH-075）：
	// 分开报等于把这个端点变成一个探测钥匙状态的工具。
	if code, raw := awsCall(t, r, cred, "GET", "/things"); code != http.StatusUnauthorized {
		t.Fatalf("停用后 = %d %s，期望 401", code, raw)
	}
	if _, err := r.s.SetAWSKeyDisabled(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	if code, _ := awsCall(t, r, cred, "GET", "/things"); code != http.StatusOK {
		t.Fatalf("恢复后 = %d", code)
	}
	if err := r.s.DeleteAWSKey(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if code, _ := awsCall(t, r, cred, "GET", "/things"); code != http.StatusUnauthorized {
		t.Fatalf("删除后 = %d，期望 401", code)
	}
}

// TestAWSKeyExpires 验证过期的钥匙立刻不认。
func TestAWSKeyExpires(t *testing.T) {
	r := awsRig(t)
	// 节点的时钟是墙上时钟，给一个已经过去的时刻。
	id, secret, err := r.s.CreateAWSKey(context.Background(), "admin", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if code, raw := awsCall(t, r, awsCred{id, secret}, "GET", "/things"); code != http.StatusUnauthorized {
		t.Fatalf("过期的钥匙 = %d %s，期望 401", code, raw)
	}
}

// TestAWSKeySecretIsOnDiskInClear 把"secret 明文落盘"钉成显式事实
// （AUTH-092）。
//
// 它曾经是反过来的一条（AUTH-087：扫整个数据目录，secret 的原文与
// base64 两种形态都不许出现）。静态加密去掉之后那条断言必然失败，而
// **删掉它**会让这件事变成没人知道的默认——所以反过来断言：secret
// 确实以明文躺在数据目录里。谁要把加密加回来，这条会红，并把人带到
// 这段注释和 PRD AUTH-092。
//
// 代价照旧写明：记录进 Raft 日志、快照与备份，**一份备份泄露等于全部
// 钥匙泄露**，处置是删掉重发。
//
// 同一区间里原本还有一条"没口令时发不出钥匙"（AUTH-087），随口令一起
// 删除——现在没有口令这个概念，任何节点都发得出钥匙。
func TestAWSKeySecretIsOnDiskInClear(t *testing.T) {
	r := awsRig(t)
	_, secret, err := r.s.CreateAWSKey(context.Background(), "admin", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	// 关掉节点，让还在缓冲里的东西都落盘。
	r.stop()
	// 记录是 JSON，而 JSON 把 []byte 编成 base64，所以磁盘上长的是
	// base64 的样子；原文形态也一并找，命中任意一种都算"在盘上"。
	needles := [][]byte{
		[]byte(secret),
		[]byte(base64.StdEncoding.EncodeToString([]byte(secret))),
	}
	var found []string
	err = filepath.Walk(r.dd.Root(), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		for _, needle := range needles {
			if bytes.Contains(b, needle) {
				found = append(found, path)
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("secret 没有出现在数据目录里。" +
			"若是重新加上了静态加密，请改这条用例并同步 PRD AUTH-092")
	}
}

// TestAWSKeyAdminAPI 走一遍管理面的四个端点（API-125 至 API-127）。
func TestAWSKeyAdminAPI(t *testing.T) {
	r := awsRig(t)

	code, raw := r.adminReq(t, "GET", adminAPIPrefix+"/awskeys", nil)
	// 空的时候必须是空数组而不是 null：前端对 null 要多写一个分支，
	// 漏写的表现是整页白屏。
	if code != http.StatusOK || !strings.Contains(string(raw), `[]`) {
		t.Fatalf("空列表 = %d %s", code, raw)
	}

	code, raw = r.adminReq(t, "POST", adminAPIPrefix+"/awskeys",
		`{"role":"operator","note":"CI"}`)
	if code != http.StatusOK {
		t.Fatalf("创建 = %d %s", code, raw)
	}
	var created struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
		Role   string `json:"role"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Secret == "" || created.Role != "operator" {
		t.Fatalf("创建响应 = %s", raw)
	}

	// 列表里有这把钥匙，而且**不带密钥材料**。
	code, raw = r.adminReq(t, "GET", adminAPIPrefix+"/awskeys", nil)
	if code != http.StatusOK || !strings.Contains(string(raw), created.ID) {
		t.Fatalf("列表 = %d %s", code, raw)
	}
	if strings.Contains(string(raw), created.Secret) || strings.Contains(string(raw), "sealed") {
		t.Fatalf("列表泄露了密钥材料：%s", raw)
	}

	code, raw = r.adminReq(t, "PATCH", adminAPIPrefix+"/awskeys/"+created.ID, `{"disabled":true}`)
	if code != http.StatusOK || !strings.Contains(string(raw), `"disabled": true`) {
		t.Fatalf("停用 = %d %s", code, raw)
	}
	// 不带 disabled 的 PATCH 要被挡下，而不是当成"改成 false"。
	code, raw = r.adminReq(t, "PATCH", adminAPIPrefix+"/awskeys/"+created.ID, `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("空 PATCH = %d %s，期望 400", code, raw)
	}

	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/awskeys/"+created.ID, nil)
	if code != http.StatusNoContent {
		t.Fatalf("删除 = %d", code)
	}
	code, _ = r.adminReq(t, "DELETE", adminAPIPrefix+"/awskeys/"+created.ID, nil)
	if code != http.StatusNotFound {
		t.Fatalf("删两次 = %d，期望 404", code)
	}

	// 角色名打错要在这一层就挡下来，而不是存进去变成一把什么都
	// 干不了的钥匙（那时候看不出是打错了字）。
	code, raw = r.adminReq(t, "POST", adminAPIPrefix+"/awskeys", `{"role":"root"}`)
	if code != http.StatusBadRequest || !strings.Contains(string(raw), "reader") {
		t.Fatalf("乱写角色 = %d %s", code, raw)
	}
}

// TestAWSKeyAdminOnly 验证 operator 碰不到这组端点。
//
// 列表本身不含密钥材料，但它说出了有哪些钥匙、各是什么角色——
// 那是一张攻击面地图。发钥匙更不用说：发一把 admin 的 access key
// 等于把整个控制面交出去。
func TestAWSKeyAdminOnly(t *testing.T) {
	r := awsRig(t)
	tok, err := r.s.access.PutUser("ops", roleOperator)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		method, path, body string
	}{
		{"GET", adminAPIPrefix + "/awskeys", ""},
		{"POST", adminAPIPrefix + "/awskeys", `{"role":"reader"}`},
		{"DELETE", adminAPIPrefix + "/awskeys/AKIAX", ""},
	} {
		var body any
		if c.body != "" {
			body = c.body
		}
		code, raw := r.adminReqToken(t, c.method, c.path, body, tok)
		if code != http.StatusForbidden {
			t.Fatalf("operator %s %s = %d %s，期望 403", c.method, c.path, code, raw)
		}
	}
}
