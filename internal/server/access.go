package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// 控制台的角色不复用 IoT Policy（UI-024）。
//
// 设备策略回答「这台设备能不能发这个主题」。控制台回答的是另一个问题：
// 这个人能不能吊销证书、能不能抓包。混用同一份文档会让一份写给设备的 Allow
// 变成控制台的权限，或者相反。

const (
	roleAdmin    = "admin"
	roleOperator = "operator"
	roleReader   = "reader"
)

type principal struct {
	Name string
	Role string
}

type principalCtx struct{}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalCtx{}, p)
}

func principalFrom(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(principalCtx{}).(principal)
	return p, ok
}

type userRecord struct {
	Name string `json:"name"`
	Role string `json:"role"`
	Hash string `json:"hash"`
}

type userFile struct {
	V     int          `json:"v"`
	Users []userRecord `json:"users"`
}

// access 保存额外的控制台用户。
//
// 引导令牌不在这里。它仍然是角色 admin，名字 admin。
//
// 身份只有一种来源：令牌。此前还有一条 OIDC 登录会签发内存里的会话
// cookie，那条路径连同会话机制一起拿掉了（AUTH-080）——没有界面入口
// 的鉴权路径只会在升级时被忘记，而它照样能换到一个 admin 身份。
type access struct {
	mu    sync.Mutex
	dir   string
	now   func() int64
	users []userRecord
}

func newAccess(dir string, now func() int64) *access {
	a := &access{dir: dir, now: now}
	_ = a.load()
	return a
}

func (a *access) load() error {
	b, err := os.ReadFile(filepath.Join(a.dir, "users.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var f userFile
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return err
	}
	a.users = f.Users
	return nil
}

func (a *access) saveUsers() error {
	raw, err := json.Marshal(userFile{V: 1, Users: a.users})
	if err != nil {
		return err
	}
	return writeSecret(filepath.Join(a.dir, "users.json"), raw)
}

func writeSecret(path string, raw []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// UserByToken 用令牌找出用户。比较覆盖全部用户，避免用「找到就返回」泄露长度。
func (a *access) UserByToken(token string) (principal, bool) {
	sum := sha256.Sum256([]byte(token))
	got := hex.EncodeToString(sum[:])
	a.mu.Lock()
	defer a.mu.Unlock()
	var found principal
	ok := false
	for _, u := range a.users {
		if subtle.ConstantTimeCompare([]byte(u.Hash), []byte(got)) == 1 {
			found = principal{Name: u.Name, Role: u.Role}
			ok = true
		}
	}
	return found, ok
}

// PutUser 新建或替换一个用户，返回只出现一次的令牌。
func (a *access) PutUser(name, role string) (string, error) {
	if !validRole(role) || role == "" {
		return "", errors.New("角色只接受 reader、operator 或 admin")
	}
	if name == "" || name == "admin" || len(name) > 64 {
		return "", errors.New("用户名不可用")
	}
	tok, err := GenerateAdminToken()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(tok))
	rec := userRecord{Name: name, Role: role, Hash: hex.EncodeToString(sum[:])}
	a.mu.Lock()
	defer a.mu.Unlock()
	replaced := false
	for i := range a.users {
		if a.users[i].Name == name {
			a.users[i] = rec
			replaced = true
			break
		}
	}
	if !replaced {
		a.users = append(a.users, rec)
	}
	if err := a.saveUsers(); err != nil {
		return "", err
	}
	return tok, nil
}

// DeleteUser 删掉一个额外用户。引导用的 admin 不在这份名单里。
func (a *access) DeleteUser(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	next := a.users[:0]
	found := false
	for _, u := range a.users {
		if u.Name == name {
			found = true
			continue
		}
		next = append(next, u)
	}
	if !found {
		return os.ErrNotExist
	}
	a.users = next
	return a.saveUsers()
}

// Users 返回名字和角色，不含令牌哈希。
func (a *access) Users() []userRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]userRecord, len(a.users))
	for i, u := range a.users {
		out[i] = userRecord{Name: u.Name, Role: u.Role}
	}
	return out
}

func validRole(role string) bool {
	return role == roleReader || role == roleOperator || role == roleAdmin
}

// permit 决定这个角色能不能碰这条管理面路径。
//
// 抓包、用户管理与兼容面的 access key 只给 admin。只读角色只能 GET，
// 外加策略干跑（它不改任何东西）。运维可以做日常写操作。
//
// access key 连「列出来」都不给 operator：列表本身不含密钥材料，但它
// 说出了有哪些钥匙、各是什么角色、谁还没过期——那是一张攻击面地图。
// 发钥匙更不用说：发一把 admin 的 access key 等于把整个控制面交出去。
func permit(p principal, method, path string) bool {
	if strings.HasSuffix(path, "/capture") ||
		strings.Contains(path, "/users") || strings.Contains(path, "/awskeys") {
		return p.Role == roleAdmin
	}
	if p.Role == roleAdmin || p.Role == roleOperator {
		return true
	}
	if method == "GET" || method == "HEAD" {
		return true
	}
	return method == "POST" && strings.HasSuffix(path, "/authz/simulate")
}
