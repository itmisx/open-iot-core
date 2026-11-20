package server

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"fmt"

	"github.com/itmisx/open-iot-core/internal/awsapi"
	"github.com/itmisx/open-iot-core/internal/registry"
)

// 兼容面 access key 的发放与查询（AUTH-086 至 AUTH-089、AUTH-092）。
//
// 钥匙存在复制的冷层里，secret **明文**存放（AUTH-092：静态加密整条
// 去掉了，连同它要求的那个 CA 口令）。SigV4 是 HMAC，验签必须拿到
// secret 的原文，所以这里存不了哈希——能做的只有加密，而加密要一个
// 口令，口令又要穿过四种部署方式送进容器。去掉之后 secret 跟着复制
// 日志走，也就会落进每个节点的磁盘、快照与备份：**一份备份泄露等于
// 全部钥匙泄露**，处置是删掉重发。这一句留在这里，是因为"存的是明文"
// 不写出来就会被当成漏实现。
//
// 此前它们是一份节点本地的 JSON，改不了也吊销不了，集群下还要人手
// 同步三份——只加在 A 上的钥匙，打到 B 的请求是 401。

const (
	// awsKeyIDPrefix 与 AWS 的 access key id 形状一致。
	//
	// SDK 不校验这个前缀，贴近真实只为省掉"这是不是填错了"这类提问：
	// 一个看起来不像 access key 的字符串，用的人会先怀疑自己。
	awsKeyIDPrefix = "AKIA"
	// awsKeyIDRandLen 是前缀之后的随机部分，base32 之后 16 个字符，
	// 连前缀共 20 个，与 AWS 一致。10 字节即 80 位，碰撞不是问题。
	awsKeyIDRandLen = 10
	// awsKeySecretLen 取 30 字节，base64 之后 40 个字符，与 AWS 一致。
	awsKeySecretLen = 30
)

// idEncoding 是不带填充的大写 base32。
//
// 不用 base64：id 要能被人念出来、抄下来、放进 URL 和日志，
// 而 base64 的 `+` `/` 两个字符在这几种场合都要再转义一次。
var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// newAWSKeyID 生成一个 access key id。
func newAWSKeyID() (string, error) {
	buf := make([]byte, awsKeyIDRandLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("生成 access key id 失败: %w", err)
	}
	return awsKeyIDPrefix + idEncoding.EncodeToString(buf), nil
}

// newAWSKeySecret 生成一个 secret。
func newAWSKeySecret() (string, error) {
	buf := make([]byte, awsKeySecretLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("生成 access key secret 失败: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

// CreateAWSKey 发一把新钥匙，返回 id 与**明文 secret**。
//
// secret 只在这一次返回里出现：没有任何接口会把它读回来（列表与详情
// 都不含密钥材料），所以调用方必须当场把它交给人，丢了只能删掉重发。
// "读不回来"是接口的约定，不是存储的加密——存储里它是明文，见本文件
// 开头。
func (s *Server) CreateAWSKey(ctx context.Context, role, note string, expires int64) (id, secret string, err error) {
	// 角色名的权威在 awsapi。在这里挡住而不是留给 registry：
	// registry 不能引用 awsapi（会成环），而让一个不认识的角色写进
	// 存储，之后的现象是"这把钥匙什么都干不了"，看不出是角色名打错了。
	if !awsapi.ValidRole(role) {
		return "", "", fmt.Errorf("角色 %q 不是 reader、operator 或 admin", role)
	}
	id, err = newAWSKeyID()
	if err != nil {
		return "", "", err
	}
	secret, err = newAWSKeySecret()
	if err != nil {
		return "", "", err
	}
	if _, err := s.reg2.PutAWSKey(ctx, id, role, note, []byte(secret), expires); err != nil {
		return "", "", err
	}
	return id, secret, nil
}

// awsSecret 按 access key id 取 secret，供验签使用。
//
// 不存在、停用、过期，对调用方是同一个"取不到"：验签那一层把它们都
// 报成"签名不正确"（AUTH-075）。分开报等于把这个接口变成一个探测钥匙
// 状态的工具。
func (s *Server) awsSecret(id string) (string, bool) {
	secret, _, err := s.reg2.KeySecret(id)
	if err != nil {
		return "", false
	}
	return string(secret), true
}

// awsRole 取这把钥匙的角色（AUTH-081）。
//
// 它和 awsSecret 查的是同一条记录，但分成两次查：验签是 sigv4 的事，
// 判权限是兼容面的事，合成一个返回值会让 sigv4 认识角色这个概念。
func (s *Server) awsRole(id string) (awsapi.Role, bool) {
	_, role, err := s.reg2.KeySecret(id)
	if err != nil {
		return "", false
	}
	return awsapi.Role(role), true
}

// ListAWSKeys 列出全部钥匙，不含密钥材料。
func (s *Server) ListAWSKeys() ([]registry.AWSKey, error) {
	return s.reg2.ListAWSKeys()
}

// SetAWSKeyDisabled 停用或恢复一把钥匙。
func (s *Server) SetAWSKeyDisabled(ctx context.Context, id string, disabled bool) (*registry.AWSKey, error) {
	return s.reg2.SetAWSKeyDisabled(ctx, id, disabled)
}

// DeleteAWSKey 删掉一把钥匙。
func (s *Server) DeleteAWSKey(ctx context.Context, id string) error {
	return s.reg2.DeleteAWSKey(ctx, id)
}
