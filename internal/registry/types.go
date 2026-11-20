// Package registry 是设备、证书与策略的控制面。
//
// 它是冷层（完整记录）与热视图（鉴权路径读取的子集）之间的唯一写入口：
// 两者必须在同一次调用里一起更新，否则会出现"冷层说这张证书已吊销、
// 热视图还放行"的窗口——而那正是吊销失效的样子。
//
// 全部写入都经 meta.Store 的命令接口（PRD REG-100），与将来的 Raft
// 实现走同一条路径，替换后端时本包无需改动。
package registry

import (
	"fmt"
	"strings"
)

// 记录的格式版本，各自独立演进。
const (
	thingRecordVersion  = 1
	certRecordVersion   = 1
	policyRecordVersion = 1
)

// Thing 是一台设备的完整记录（冷层）。
//
// 热视图只物化策略变量真正引用的属性（PRD REG-006、CLU-053），
// 这里是完整记录，供控制台与 API 读取。
type Thing struct {
	Name string `json:"name"`
	// Attributes 是设备属性。被策略变量引用的那些会进入热视图。
	Attributes map[string]string `json:"attributes,omitempty"`
	// PolicyName 是绑定的策略文档名。
	PolicyName      string `json:"policyName,omitempty"`
	CreatedAtUnixMs int64  `json:"createdAt"`
	UpdatedAtUnixMs int64  `json:"updatedAt"`
	// Version 从 1 起，每次更新加一。供兼容面的 expectedVersion 使用。
	Version int64 `json:"version,omitempty"`
}

// Certificate 是一张证书的完整记录（冷层）。
type Certificate struct {
	// Fingerprint 是证书 DER 的 SHA-256，小写十六进制（PRD PKI-102）。
	Fingerprint string `json:"fingerprint"`
	// Status 取 ACTIVE | INACTIVE | REVOKED | PENDING_ACTIVATION。
	Status string `json:"status"`
	// Thing 是绑定的设备名。
	//
	// 热视图每张证书只记一个设备：鉴权路径需要它来解析
	// ${iot:Connection.Thing.ThingName}，而该变量只能有一个值。
	// 一证多设备（PKI-083 允许但默认告警）推迟到有真实需求时再做。
	Thing      string `json:"thing,omitempty"`
	PolicyName string `json:"policyName,omitempty"`
	// Subject 与 Issuer 仅供展示与审计，不参与判定。
	Subject string `json:"subject,omitempty"`
	Issuer  string `json:"issuer,omitempty"`
	// NotAfterUnixMs 是证书有效期终点。
	//
	// 它不进热视图（CLU-053）：有效期由 TLS 握手校验，
	// 这里只为控制台的到期时间轴与预警服务。
	NotAfterUnixMs int64 `json:"notAfter,omitempty"`
	// RevokeReason 在吊销时记录，进入审计。
	RevokeReason    string `json:"revokeReason,omitempty"`
	CreatedAtUnixMs int64  `json:"createdAt"`
	UpdatedAtUnixMs int64  `json:"updatedAt"`
	// PEM 是证书正文。私钥不在这里：它只在签发的那一次响应里出现。
	PEM string `json:"pem,omitempty"`
}

// Policy 是一份策略文档的记录。
type Policy struct {
	Name string `json:"name"`
	// Document 是原始 JSON 文档。保留原文而非编译结果，
	// 使控制台能展示用户写的内容，也使升级编译器后可以重编译。
	Document        []byte `json:"document"`
	CreatedAtUnixMs int64  `json:"createdAt"`
	UpdatedAtUnixMs int64  `json:"updatedAt"`
	// DefaultVersion 是当前生效的版本号，从 "1" 起。
	DefaultVersion string `json:"defaultVersion,omitempty"`
}

// PolicyVersion 是策略的一个历史版本。只有默认版本进入热视图。
type PolicyVersion struct {
	ID              string `json:"id"`
	Document        []byte `json:"document"`
	IsDefault       bool   `json:"isDefault"`
	CreatedAtUnixMs int64  `json:"createdAt"`
}

// 证书状态常量。
const (
	StatusActive            = "ACTIVE"
	StatusInactive          = "INACTIVE"
	StatusRevoked           = "REVOKED"
	StatusPendingActivation = "PENDING_ACTIVATION"
)

// MaxThingNameLen 是设备名长度上限（PRD REG-002）。
const MaxThingNameLen = 128

// MaxPolicyNameLen 是策略名长度上限。
const MaxPolicyNameLen = 128

// 属性上限（PRD REG-003）。最多 3 个属性。
const (
	MaxAttributesUntyped = 3
	MaxAttributesBytes   = 47 * 1024
)

// ValidateThingName 校验设备名（PRD REG-002）。
func ValidateThingName(name string) error {
	if name == "" {
		return fmt.Errorf("设备名不得为空")
	}
	if len(name) > MaxThingNameLen {
		return fmt.Errorf("设备名长度 %d 超出上限 %d", len(name), MaxThingNameLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == ':':
		default:
			return fmt.Errorf("设备名含非法字符 %q；允许字母、数字、下划线、连字符与冒号",
				string(c))
		}
	}
	return nil
}

// ValidatePolicyName 校验策略名。
//
// 策略名会出现在键里，因此不得含分隔符；也不得含斜杠，
// 以免将来放进 URL 路径时产生歧义。
func ValidatePolicyName(name string) error {
	if name == "" {
		return fmt.Errorf("策略名不得为空")
	}
	if len(name) > MaxPolicyNameLen {
		return fmt.Errorf("策略名长度 %d 超出上限 %d", len(name), MaxPolicyNameLen)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-':
		default:
			return fmt.Errorf("策略名含非法字符 %q；允许字母、数字、下划线与连字符",
				string(c))
		}
	}
	return nil
}

// ValidateFingerprint 校验证书指纹。
func ValidateFingerprint(fp string) error {
	if len(fp) != 64 {
		return fmt.Errorf("指纹长度应为 64 个十六进制字符，实际 %d", len(fp))
	}
	if fp != strings.ToLower(fp) {
		return fmt.Errorf("指纹须为小写十六进制（PRD PKI-102）")
	}
	for i := 0; i < len(fp); i++ {
		c := fp[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return fmt.Errorf("指纹含非十六进制字符 %q", string(c))
		}
	}
	return nil
}

// ValidateStatus 校验证书状态。
func ValidateStatus(s string) error {
	switch s {
	case StatusActive, StatusInactive, StatusRevoked, StatusPendingActivation:
		return nil
	}
	return fmt.Errorf("未知的证书状态 %q；可取 %s、%s、%s、%s",
		s, StatusActive, StatusInactive, StatusRevoked, StatusPendingActivation)
}

// validateAttributes 校验属性数量与总大小（PRD REG-003）。
func validateAttributes(attrs map[string]string) error {
	if len(attrs) > MaxAttributesUntyped {
		return fmt.Errorf("属性数量 %d 超出上限 %d",
			len(attrs), MaxAttributesUntyped)
	}
	total := 0
	for k, v := range attrs {
		total += len(k) + len(v)
	}
	if total > MaxAttributesBytes {
		return fmt.Errorf("属性总大小 %d 字节超出上限 %d", total, MaxAttributesBytes)
	}
	return nil
}
