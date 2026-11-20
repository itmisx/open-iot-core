package awsapi

import (
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
)

// routeRole 是兼容面每条路由要求的最低角色（AUTH-081）。
//
// 这张表故意和挂载处的写法重复一遍。它不是为了"验证代码等于代码"，
// 而是让**改一条路由的级别**必须在两个地方都动手，并且让整张权限矩阵
// 能一眼看完——散在三百行挂载代码里的级别，谁也说不清 reader 到底
// 能碰什么。新增路由忘了配级别，下面的 TestEveryRouteDeclaresRole 会红。
var routeRole = []struct {
	method string
	path   string
	role   Role
	fn     string
}{
	{"POST", "/things/{thingName}", RoleOperator, "createThing"},
	{"GET", "/things/{thingName}", RoleReader, "describeThing"},
	{"PATCH", "/things/{thingName}", RoleOperator, "updateThing"},
	{"DELETE", "/things/{thingName}", RoleOperator, "deleteThing"},
	{"GET", "/things", RoleReader, "listThings"},
	{"GET", "/endpoint", RoleReader, "describeEndpoint"},
	{"POST", "/policies/{policyName}", RoleOperator, "createPolicy"},
	{"GET", "/policies/{policyName}", RoleReader, "getPolicy"},
	{"DELETE", "/policies/{policyName}", RoleOperator, "deletePolicy"},
	{"GET", "/policies", RoleReader, "listPolicies"},
	{"POST", "/policies/{policyName}/version", RoleOperator, "createPolicyVersion"},
	{"GET", "/policies/{policyName}/version/{policyVersionId}", RoleReader, "getPolicyVersion"},
	{"GET", "/policies/{policyName}/version", RoleReader, "listPolicyVersions"},
	{"DELETE", "/policies/{policyName}/version/{policyVersionId}", RoleOperator, "deletePolicyVersion"},
	{"PATCH", "/policies/{policyName}/version/{policyVersionId}", RoleOperator, "setDefaultPolicyVersion"},
	{"PUT", "/target-policies/{policyName}", RoleOperator, "attachPolicy"},
	{"POST", "/target-policies/{policyName}", RoleOperator, "detachPolicy"},
	{"POST", "/attached-policies/{target}", RoleReader, "listAttachedPolicies"},
	{"POST", "/policy-targets/{policyName}", RoleReader, "listTargetsForPolicy"},
	{"PUT", "/things/{thingName}/principals", RoleOperator, "attachThingPrincipal"},
	{"DELETE", "/things/{thingName}/principals", RoleOperator, "detachThingPrincipal"},
	{"GET", "/things/{thingName}/principals", RoleReader, "listThingPrincipals"},
	{"GET", "/principals/things", RoleReader, "listPrincipalThings"},
	{"POST", "/keys-and-certificate", RoleOperator, "createKeys"},
	{"POST", "/certificates", RoleOperator, "createFromCSR"},
	{"GET", "/certificates/{certificateId}", RoleReader, "describeCertificate"},
	{"PUT", "/certificates/{certificateId}", RoleOperator, "updateCertificate"},
	{"DELETE", "/certificates/{certificateId}", RoleOperator, "deleteCertificate"},
	{"GET", "/certificates", RoleReader, "listCertificates"},
	{"POST", "/topics/{topic...}", RoleOperator, "publish"},                                       // 数据面
	{"GET", "/things/{thingName}/shadow", RoleReader, "getShadow"},                                // 数据面
	{"POST", "/things/{thingName}/shadow", RoleOperator, "updateShadow"},                          // 数据面
	{"DELETE", "/things/{thingName}/shadow", RoleOperator, "deleteShadow"},                        // 数据面
	{"GET", "/api/things/shadow/ListNamedShadowsForThing/{thingName}", RoleReader, "listShadows"}, // 数据面
	{"GET", "/retainedMessage/{topic...}", RoleReader, "getRetained"},                             // 数据面
	{"GET", "/retainedMessage", RoleReader, "listRetained"},                                       // 数据面
	{"GET", "/registrationcode", RoleAdmin, "getRegistrationCode"},
	{"POST", "/cacertificate", RoleAdmin, "registerCA"},
	{"GET", "/cacertificate/{certificateId}", RoleReader, "describeCA"},
	{"PUT", "/cacertificate/{certificateId}", RoleAdmin, "updateCA"},
	{"DELETE", "/cacertificate/{certificateId}", RoleAdmin, "deleteCA"},
	{"GET", "/cacertificates", RoleReader, "listCAs"},
	{"POST", "/certificate/register", RoleOperator, "registerCert"},
	{"POST", "/certificate/register-no-ca", RoleOperator, "registerCertNoCA"},
	{"POST", "/indices/search", RoleReader, "searchIndex"},
	{"GET", "/indexing/config", RoleReader, "getIndexing"},
	{"PUT", "/indexing/config", RoleOperator, "putIndexing"},
	{"POST", "/role-aliases/{roleAlias}", RoleOperator, "createRoleAlias"},
	{"GET", "/role-aliases/{roleAlias}", RoleReader, "describeRoleAlias"},
	{"PUT", "/role-aliases/{roleAlias}", RoleOperator, "updateRoleAlias"},
	{"DELETE", "/role-aliases/{roleAlias}", RoleOperator, "deleteRoleAlias"},
	{"GET", "/role-aliases", RoleReader, "listRoleAliases"},
	{"GET", "/role-aliases/{roleAlias}/credentials", RoleAdmin, "roleAliasCredentials"},
}

// mountLine 匹配挂载处的一行。两种门（控制面 authed、数据面 data）都要
// 显式写出角色，所以这个正则覆盖得了全部路由；覆盖不了的会在下面被数出来。
var mountLine = regexp.MustCompile(
	`mux\.HandleFunc\("(\w+) "\+p\+"([^"]*)", h\.(authed|data)\((Role\w+), h\.(\w+)\)\)`)

// roleConst 把源码里的常量名映射回角色值。
var roleConst = map[string]Role{
	"RoleReader":   RoleReader,
	"RoleOperator": RoleOperator,
	"RoleAdmin":    RoleAdmin,
}

// TestEveryRouteDeclaresRole 盯住"新增路由忘了定权限级别"。
//
// 这是本仓库最典型的缺口形状：后端把 action 做完、挂上，级别随手跟了
// 旁边一行抄过来的那个，语法和类型都对，不会让任何测试变红。
// 这条测试读挂载处的源码，把实际挂上的路由与上面那张表逐条比对。
func TestEveryRouteDeclaresRole(t *testing.T) {
	var src string
	for _, name := range []string{"handler.go", "fleet.go"} {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("读 %s：%v", name, err)
		}
		src += string(b)
	}
	// 锚点对不上一律 Fatal。匹配不到就当成"没有路由"会让后面的断言
	// 全部通过，而那正是最该报警的情况。
	total := strings.Count(src, "mux.HandleFunc(")
	if total == 0 {
		t.Fatal("源码里一条 mux.HandleFunc 都没找到，锚点失效了")
	}
	ms := mountLine.FindAllStringSubmatch(src, -1)
	if len(ms) != total {
		t.Fatalf("挂了 %d 条路由，但只有 %d 条写成了带角色的形状；"+
			"剩下的那些没过这道门", total, len(ms))
	}

	want := map[string]struct {
		role Role
		fn   string
	}{}
	for _, r := range routeRole {
		want[r.method+" "+r.path] = struct {
			role Role
			fn   string
		}{r.role, r.fn}
	}
	if len(want) != len(routeRole) {
		t.Fatalf("表里有重复的方法加路径：%d 条写成了 %d 个键", len(routeRole), len(want))
	}

	seen := map[string]bool{}
	for _, m := range ms {
		key := m[1] + " " + m[2]
		seen[key] = true
		w, ok := want[key]
		if !ok {
			t.Errorf("%s 挂上了但不在 routeRole 表里：新增路由要在表里写明它要求的级别", key)
			continue
		}
		got, ok := roleConst[m[4]]
		if !ok {
			t.Errorf("%s 用了未知的角色常量 %s", key, m[4])
			continue
		}
		if got != w.role {
			t.Errorf("%s 的级别 = %s，表里是 %s", key, got, w.role)
		}
		if m[5] != w.fn {
			t.Errorf("%s 的处理函数 = %s，表里是 %s", key, m[5], w.fn)
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("routeRole 表里有 %s，但源码里没挂", key)
		}
	}
}

// TestRoleLevels 锁定三个角色的高低关系。
//
// 判定用的是级别比较：一条只要 operator 的路由，admin 也能走。
// 写反了的话，admin 会被挡在写操作外面，而那种现象很容易被当成
// "密钥配错了"去排查。
func TestRoleLevels(t *testing.T) {
	cases := []struct {
		have, need Role
		want       bool
	}{
		{RoleReader, RoleReader, true},
		{RoleReader, RoleOperator, false},
		{RoleReader, RoleAdmin, false},
		{RoleOperator, RoleReader, true},
		{RoleOperator, RoleOperator, true},
		{RoleOperator, RoleAdmin, false},
		{RoleAdmin, RoleReader, true},
		{RoleAdmin, RoleOperator, true},
		{RoleAdmin, RoleAdmin, true},
		// 拼错的角色名是拒绝，不是放行。
		{Role("Admin"), RoleReader, false},
		{Role(""), RoleReader, false},
		{Role("root"), RoleReader, false},
	}
	for _, c := range cases {
		if got := allows(c.have, c.need); got != c.want {
			t.Errorf("allows(%q, %q) = %v，期望 %v", c.have, c.need, got, c.want)
		}
	}
	for _, s := range []string{"reader", "operator", "admin"} {
		if !ValidRole(s) {
			t.Errorf("ValidRole(%q) = false", s)
		}
	}
	for _, s := range []string{"", "Reader", "root", "ADMIN"} {
		if ValidRole(s) {
			t.Errorf("ValidRole(%q) = true", s)
		}
	}
}

// TestRoleGate 验证这道门真的拦得住，而不是只在表里写了级别。
func TestRoleGate(t *testing.T) {
	var role Role
	_, srv := newHarnessFull(t, nil, func(h *Handler) {
		h.Role = func(string) (Role, bool) { return role, true }
	})

	// 先用 admin 铺一台设备，后面的读才有东西可读。
	role = RoleAdmin
	if resp, raw := do(t, srv, "POST", Prefix+"/things/lamp", ""); resp.StatusCode != 200 {
		t.Fatalf("准备设备失败：%d %s", resp.StatusCode, raw)
	}

	cases := []struct {
		name       string
		role       Role
		method     string
		path       string
		wantDenied bool
	}{
		{"reader 能读设备", RoleReader, "GET", Prefix + "/things/lamp", false},
		{"reader 不能建设备", RoleReader, "POST", Prefix + "/things/other", true},
		{"reader 不能删设备", RoleReader, "DELETE", Prefix + "/things/lamp", true},
		// POST 的读：按方法推级别就会把这条挡在 reader 外面。
		{"reader 能列已附加策略", RoleReader, "POST", Prefix + "/attached-policies/lamp", false},
		{"operator 能建设备", RoleOperator, "POST", Prefix + "/things/other", false},
		{"operator 不能注册自有 CA", RoleOperator, "POST", Prefix + "/cacertificate", true},
		// GET 的非读：按方法推级别就会把这条放给 reader。
		{"operator 不能取凭证", RoleOperator, "GET", Prefix + "/role-aliases/ra/credentials", true},
		{"admin 能注册自有 CA", RoleAdmin, "POST", Prefix + "/cacertificate", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			role = c.role
			resp, raw := do(t, srv, c.method, c.path, "")
			denied := resp.StatusCode == http.StatusForbidden
			if denied != c.wantDenied {
				t.Fatalf("状态 = %d（%s），期望拒绝 = %v", resp.StatusCode, raw, c.wantDenied)
			}
			if !denied {
				return
			}
			// 拒绝必须是 AccessDenied 而不是 Unauthorized：
			// 前者告诉调用方去换一把权限更高的钥匙，后者会让他
			// 去查密钥和时钟，而那两样都没问题。
			if e := decodeErr(t, raw); e.Type != "AccessDeniedException" {
				t.Fatalf("错误类型 = %s，期望 AccessDeniedException", e.Type)
			}
		})
	}
}

// TestRoleGateFailsClosed 验证没装配 Role 时一律拒绝。
//
// 这是整条改动的要害：漏接这根线如果退化成"不判权限"，等于什么都没加，
// 而且没有任何现象能让人发现。
func TestRoleGateFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		fill func(*Handler)
	}{
		{"没装配 Role", func(h *Handler) { h.Role = nil }},
		{"钥匙查不到角色", func(h *Handler) {
			h.Role = func(string) (Role, bool) { return RoleAdmin, false }
		}},
		{"角色名拼错", func(h *Handler) {
			h.Role = func(string) (Role, bool) { return Role("Admin"), true }
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, srv := newHarnessFull(t, nil, c.fill)
			// 连最低级别的只读路由都要被拒。
			resp, raw := do(t, srv, "GET", Prefix+"/things", "")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("状态 = %d（%s），期望 403", resp.StatusCode, raw)
			}
		})
	}
}

// TestRoleGateKeepsSignatureErrorsDistinct 验证验签失败仍是 401。
//
// 两种拒绝合成一种之后，调用方只能靠猜：401 要去查密钥和时钟，
// 403 要去换一把钥匙，修法完全不同。
func TestRoleGateKeepsSignatureErrorsDistinct(t *testing.T) {
	_, srv := newHarnessFull(t, nil, func(h *Handler) {
		h.Role = func(string) (Role, bool) { return RoleAdmin, true }
	})
	req, err := http.NewRequest("GET", srv.URL+Prefix+"/things", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iot/aws4_request, "+
		"SignedHeaders=host, Signature=deadbeef")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("签名不对时状态 = %d，期望 401", resp.StatusCode)
	}
}
