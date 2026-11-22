package authz

import (
	"fmt"
	"strings"
)

// varKind 是策略变量的种类。
type varKind uint8

const (
	varClientID varKind = iota
	varThingName
	varThingAttr
	varThingIsAttached
	varCertSerial
	varSubject
	varIssuer
)

// dnField 是可引用的 DN 字段。
type dnField uint8

const (
	dnCommonName dnField = iota
	dnCountry
	dnOrganization
	dnOrganizationalUnit
	dnState
	dnLocality
	dnSerialNumber
	dnQualifier
	numDNFields
)

var dnFieldNames = [numDNFields]string{
	"CommonName", "Country", "Organization", "OrganizationalUnit",
	"State", "Locality", "SerialNumber", "DistinguishedNameQualifier",
}

func parseDNField(s string) (dnField, bool) {
	for i, n := range dnFieldNames {
		if s == n {
			return dnField(i), true
		}
	}
	return 0, false
}

// varSpec 是一个已解析的变量引用。
type varSpec struct {
	kind varKind
	// field 用于 Subject 与 Issuer。
	field dnField
	// attr 用于 Attributes[name]。
	attr string
	// name 是原始变量名，写入判定轨迹供控制台展示。
	name string
}

// DN 是证书的可辨别名的可引用字段。
//
// CN、Country 与 Organization 是设备证书里最常见的三项，直接内联；
// 其余字段放进按需扩展。旧布局无论证书是否填写字段，都会在每条长连接里
// 常驻 8 个 string header（128 B），Subject + Issuer 就是 256 B。百万 mTLS
// 连接为大量空字段支付这笔固定成本没有意义。DN 复制后共享只读扩展；本包
// 不暴露扩展指针，也没有修改方法，因而不会引入跨连接可变状态。
type DN struct {
	CommonName   string
	Country      string
	Organization string
	extra        *dnExtra
}

type dnExtra struct {
	organizationalUnit string
	state              string
	locality           string
	serialNumber       string
	qualifier          string
}

// DNFields 是构造 DN 时的完整字段集。它只在 TLS 握手/管理面构造阶段使用，
// 不随长连接常驻。
type DNFields struct {
	CommonName         string
	Country            string
	Organization       string
	OrganizationalUnit string
	State              string
	Locality           string
	SerialNumber       string
	Qualifier          string
}

// NewDN 构造紧凑的只读 DN。只有至少一个少见字段非空时才分配扩展。
func NewDN(v DNFields) DN {
	d := DN{CommonName: v.CommonName, Country: v.Country, Organization: v.Organization}
	if v.OrganizationalUnit != "" || v.State != "" || v.Locality != "" ||
		v.SerialNumber != "" || v.Qualifier != "" {
		d.extra = &dnExtra{
			organizationalUnit: v.OrganizationalUnit,
			state:              v.State,
			locality:           v.Locality,
			serialNumber:       v.SerialNumber,
			qualifier:          v.Qualifier,
		}
	}
	return d
}

func (d *DN) field(f dnField) string {
	switch f {
	case dnCommonName:
		return d.CommonName
	case dnCountry:
		return d.Country
	case dnOrganization:
		return d.Organization
	case dnOrganizationalUnit:
		if d.extra != nil {
			return d.extra.organizationalUnit
		}
	case dnState:
		if d.extra != nil {
			return d.extra.state
		}
	case dnLocality:
		if d.extra != nil {
			return d.extra.locality
		}
	case dnSerialNumber:
		if d.extra != nil {
			return d.extra.serialNumber
		}
	case dnQualifier:
		if d.extra != nil {
			return d.extra.qualifier
		}
	}
	return ""
}

// CertContext 是连接所用证书的可引用字段。
type CertContext struct {
	SerialNumber string
	Subject      DN
	Issuer       DN
}

// ThingAttributeSource 是策略求值读取设备属性所需的最小热视图接口。
// Context 持有接口而非每连接闭包，避免百万连接各分配一个捕获对象。
type ThingAttributeSource interface {
	ThingAttr(thing, name string) (string, bool)
}

// Context 是一次授权判定的请求上下文。
//
// 它是求值器的全部外部输入——求值是纯函数，不查任何存储
// （PRD AUTH-026）。调用方在连接建立时从 MetaView 与 TLS 状态填充它一次，
// 之后每条消息的判定复用同一份上下文。
type Context struct {
	ClientID string
	// Thing 是连接绑定的设备名，为空表示未绑定。
	Thing string
	// ThingAttached 表示连接所用证书确实绑定了该设备。
	ThingAttached bool
	// ThingAttrs 查询设备属性。为 nil 时属性变量解析失败（从而拒绝）。
	ThingAttrs ThingAttributeSource
	// Cert 按需存在。绝大多数设备策略只引用 Thing/ClientId；用指针避免
	// 每条长期连接都为 128 B 的证书变量上下文预付空间。nil 与字段全空
	// 语义相同：证书变量解析失败并导致该资源不匹配。
	Cert *CertContext
}

// resolve 求值一个变量引用。
//
// 解析失败返回 ok 为 false，调用方必须据此拒绝，不得退化为空串匹配
// （PRD AUTH-036）：空串匹配会让 topic/${iot:Connection.Thing.ThingName}/x
// 在设备未绑定时意外匹配 topic//x，这是权限放大。
func (c *Context) resolve(v varSpec) (string, bool) {
	switch v.kind {
	case varClientID:
		if c.ClientID == "" {
			return "", false
		}
		return c.ClientID, true

	case varThingName:
		if !c.thingUsable() {
			return "", false
		}
		return c.Thing, true

	case varThingIsAttached:
		if !c.thingUsable() {
			return "", false
		}
		if c.ThingAttached {
			return "true", true
		}
		return "false", true

	case varThingAttr:
		if !c.thingUsable() || c.ThingAttrs == nil {
			return "", false
		}
		return c.ThingAttrs.ThingAttr(c.Thing, v.attr)

	case varCertSerial:
		if c.Cert == nil || c.Cert.SerialNumber == "" {
			return "", false
		}
		return c.Cert.SerialNumber, true

	case varSubject:
		if c.Cert == nil {
			return "", false
		}
		s := c.Cert.Subject.field(v.field)
		return s, s != ""

	case varIssuer:
		if c.Cert == nil {
			return "", false
		}
		s := c.Cert.Issuer.field(v.field)
		return s, s != ""

	default:
		return "", false
	}
}

// thingUsable 实现 PRD AUTH-034 的隐含约束。
//
// AWS 要求：使用 Thing 类策略变量时，连接的 clientId 必须等于 thingName。
// 这条约束很容易被漏掉，而漏掉它意味着设备 A 可以用引用了
// ${iot:Connection.Thing.ThingName} 的策略访问设备 B 的主题。
func (c *Context) thingUsable() bool {
	return c.Thing != "" && c.Thing == c.ClientID
}

// parseVar 解析形如 iot:ClientId 的变量名。
func parseVar(name string) (varSpec, error) {
	v := varSpec{name: name}
	const prefix = "iot:"
	if !strings.HasPrefix(name, prefix) {
		return v, fmt.Errorf("authz: 变量 %q 缺少 iot: 前缀", name)
	}
	rest := name[len(prefix):]

	switch {
	case rest == "ClientId":
		v.kind = varClientID
		return v, nil
	case rest == "Connection.Thing.ThingName":
		v.kind = varThingName
		return v, nil
	case rest == "Connection.Thing.IsAttached":
		v.kind = varThingIsAttached
		return v, nil
	case rest == "Certificate.SerialNumber":
		v.kind = varCertSerial
		return v, nil
	}

	if attr, ok := cutPrefixSuffix(rest, "Connection.Thing.Attributes[", "]"); ok {
		if attr == "" {
			return v, fmt.Errorf("authz: 变量 %q 的属性名为空", name)
		}
		v.kind = varThingAttr
		v.attr = attr
		return v, nil
	}
	if f, ok := cutPrefixSuffix(rest, "Certificate.Subject.", ""); ok {
		fld, ok := parseDNField(f)
		if !ok {
			return v, fmt.Errorf("authz: 不支持的证书主题字段 %q", f)
		}
		v.kind, v.field = varSubject, fld
		return v, nil
	}
	if f, ok := cutPrefixSuffix(rest, "Certificate.Issuer.", ""); ok {
		fld, ok := parseDNField(f)
		if !ok {
			return v, fmt.Errorf("authz: 不支持的证书签发者字段 %q", f)
		}
		v.kind, v.field = varIssuer, fld
		return v, nil
	}
	return v, fmt.Errorf("authz: 不支持的策略变量 %q", name)
}

func cutPrefixSuffix(s, prefix, suffix string) (string, bool) {
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	s = s[len(prefix):]
	if suffix == "" {
		return s, true
	}
	if !strings.HasSuffix(s, suffix) {
		return "", false
	}
	return s[:len(s)-len(suffix)], true
}

// SupportedVariables 返回本实现支持的全部策略变量名。
// 控制台的策略编辑器用它做补全与校验。
func SupportedVariables() []string {
	out := []string{
		"iot:ClientId",
		"iot:Connection.Thing.ThingName",
		"iot:Connection.Thing.IsAttached",
		"iot:Connection.Thing.Attributes[<name>]",
		"iot:Certificate.SerialNumber",
	}
	for _, n := range dnFieldNames {
		out = append(out, "iot:Certificate.Subject."+n)
	}
	for _, n := range dnFieldNames {
		out = append(out, "iot:Certificate.Issuer."+n)
	}
	return out
}
