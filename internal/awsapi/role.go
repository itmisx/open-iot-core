package awsapi

// 兼容面的权限级别（AUTH-081）。
//
// 在这之前，`/api/aws` 只验签：签名对了就能建设备、签证书、吊销、改策略、
// 发布、写影子。一把只想让 CI 读设备列表的 key，发出去的其实是 root。
// AUTH-002 从一开始写的就是「access key 加 secret 加**策略绑定**」，
// 绑定这一半此前没有落地。
//
// 为什么不复用 IoT Policy（设备策略）来判这件事：设备策略回答的是
// 「这台设备能不能发这个主题」，而这里问的是「这个调用方能不能吊销证书」。
// 控制台早就为此分出了一套角色（AUTH-078、UI-024），兼容面用同一套名字，
// 因为它们回答的是同一个问题——同名不同义会让「我给他发的是 operator」
// 这句话在两个平面上指不同的权限。

// Role 是一把 access key 的权限级别。
type Role string

const (
	// RoleReader 只能读。
	RoleReader Role = "reader"
	// RoleOperator 能做日常写操作：建删设备、签发与吊销证书、改策略、
	// 发布、写影子。
	RoleOperator Role = "operator"
	// RoleAdmin 还能动信任根：注册自有 CA、发凭证。
	RoleAdmin Role = "admin"
)

// rank 把三个角色排成一条线。
//
// 判定用级别比较而不是相等：一条只要 operator 的路由，admin 当然也能走。
// 逐条列 Allow 的写法迟早会漏掉某条路由上的 admin。
func rank(r Role) (int, bool) {
	switch r {
	case RoleReader:
		return 1, true
	case RoleOperator:
		return 2, true
	case RoleAdmin:
		return 3, true
	}
	return 0, false
}

// ValidRole 报告这个字符串是不是三个角色之一。
//
// 导出是给密钥文件的校验用的：让调用方自己抄一份名字，抄错了不会编译
// 失败，只会在某一天变成「配的是 operator 却按别的判」。
func ValidRole(s string) bool {
	_, ok := rank(Role(s))
	return ok
}

// allows 报告持有 have 的调用方能不能走一条要求 need 的路由。
//
// have 不是三者之一时一律拒绝。密钥文件里拼错的角色名必须是拒绝，
// 不能退化成放行——那是这道门本身要堵的那种缺口。
func allows(have, need Role) bool {
	h, ok := rank(have)
	if !ok {
		return false
	}
	n, ok := rank(need)
	if !ok {
		return false
	}
	return h >= n
}
