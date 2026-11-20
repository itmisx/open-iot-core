package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/itmisx/open-iot-core/internal/config"
)

// EnvAdminToken 是管理面鉴权令牌的环境变量名。
const EnvAdminToken = "OPENIOTD_ADMIN_TOKEN"

// AdminTokenFileName 是 data dir 下默认的令牌文件名。
const AdminTokenFileName = "admin-token"

// minAdminTokenLen 是令牌长度下限。
//
// 32 字节的 base64 约 43 个字符。设下限是为了挡住"admin"、"changeme"
// 这类令牌——一个能吊销证书的接口，其令牌被暴力猜中的代价过高。
const minAdminTokenLen = 32

// ErrNoAdminToken 表示未提供管理面令牌。
var ErrNoAdminToken = errors.New(
	"未提供管理面鉴权令牌：请设置环境变量 " + EnvAdminToken +
		"，或在配置中给出 listeners.admin.auth_token_file。" +
		"不提供「无鉴权」选项——管理面能吊销证书、踢掉连接、改写策略，" +
		"它的默认状态必须是关闭而不是敞开")

// GenerateAdminToken 生成一个管理面令牌。
func GenerateAdminToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("server: 生成管理面令牌失败: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// ResolveAdminToken 取得管理面令牌。
//
// 优先环境变量，其次令牌文件。两者都没有时返回 ErrNoAdminToken——
// 调用方据此决定不挂载 /api/admin/v1，而不是挂载一个无鉴权的版本。
func ResolveAdminToken(cfg *config.Config, dataDir string,
	lookupEnv func(string) (string, bool),
) (string, error) {
	if lookupEnv != nil {
		if v, ok := lookupEnv(EnvAdminToken); ok && strings.TrimSpace(v) != "" {
			return validateAdminToken(strings.TrimSpace(v))
		}
	}
	path := cfg.Listeners.Admin.AuthTokenFile
	if path == "" && dataDir != "" {
		// 未显式配置时看 data dir 下的默认位置：`openiotd init`
		// 会在那里生成一个，使最简部署无需任何额外配置。
		path = dataDir + string(os.PathSeparator) + AdminTokenFileName
	}
	if path == "" {
		return "", ErrNoAdminToken
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNoAdminToken
		}
		return "", fmt.Errorf("server: 读取管理面令牌文件 %s 失败: %w", path, err)
	}
	// 去掉尾部换行：几乎所有编辑器都会加一个，把换行算进令牌
	// 会导致"明明复制对了却认证失败"。
	tok := strings.TrimRight(string(data), "\r\n")
	if strings.TrimSpace(tok) == "" {
		return "", ErrNoAdminToken
	}
	return validateAdminToken(tok)
}

func validateAdminToken(tok string) (string, error) {
	if len(tok) < minAdminTokenLen {
		return "", fmt.Errorf(
			"server: 管理面令牌长度 %d 过短，至少需 %d 个字符；"+
				"可用 `openiotd admin token` 生成一个",
			len(tok), minAdminTokenLen)
	}
	return tok, nil
}

// WriteAdminTokenFile 把令牌写入文件，权限 0600。
func WriteAdminTokenFile(path, token string) error {
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return fmt.Errorf("server: 写入管理面令牌文件 %s 失败: %w", path, err)
	}
	return nil
}

// checkAdminToken 比较请求携带的令牌。
//
// 用常数时间比较：按字节提前返回的比较会把令牌的正确前缀长度
// 泄露给计时攻击，而管理面令牌一旦泄露就等同于平台的完全失守。
func checkAdminToken(want, got string) bool {
	if want == "" || got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

// bearerToken 从 Authorization 头中取出 Bearer 令牌。
func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) <= len(prefix) {
		return ""
	}
	// 方案名大小写不敏感（RFC 7235）
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
