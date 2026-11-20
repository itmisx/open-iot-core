package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// AWS IoT 对设备时钟的容忍是 15 分钟。更短会把只是慢了几分钟的设备
// 全部拒掉；不设上限则一张签过名的请求可以永远重放。
const DefaultSkew = 15 * time.Minute

// MaxExpires 是预签名里 X-Amz-Expires 的上限，与 S3 预签名的 7 天一致。
// 不设上限的话，一个 Expires 写成几十年的链接就永远有效。
const MaxExpires = 7 * 24 * time.Hour

const (
	// ServiceIoT 是控制面（iot）的服务名。
	ServiceIoT = "iot"
	// ServiceIoTData 是 HTTPS 数据面的服务名。
	ServiceIoTData = "iotdata"
	// ServiceDeviceGateway 是 MQTT over WebSocket 的服务名。
	ServiceDeviceGateway = "iotdevicegateway"

	algorithm  = "AWS4-HMAC-SHA256"
	terminator = "aws4_request"
	emptyHash  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var (
	// ErrMalformed 表示签名材料本身不完整，还没到验签那一步。
	ErrMalformed = errors.New("sigv4: 签名材料不完整")
	// ErrBadSignature 表示密钥不存在或签名对不上。
	//
	// 这两种对客户端必须是同一个错误。分开报等于告诉对方这个
	// access key 是否已登记。
	ErrBadSignature = errors.New("sigv4: 签名不正确")
	// ErrExpired 表示签名超出允许的时间窗口。
	ErrExpired = errors.New("sigv4: 签名超出允许的时间窗口")
	// ErrRegion 表示凭证范围里的区域不是本节点接受的那个。
	ErrRegion = errors.New("sigv4: 区域不匹配")
	// ErrService 表示凭证范围里的服务名不在允许列表里。
	ErrService = errors.New("sigv4: 服务名不匹配")
	// ErrUnsigned 表示请求里有未列入 SignedHeaders 的 x-amz-* 头。
	ErrUnsigned = errors.New("sigv4: 存在未签名的 x-amz-* 头")
)

// errMalformed 是内部解析用的哨兵，对外仍是 ErrMalformed。
var errMalformed = ErrMalformed

// Scope 是凭证范围里解析出来的身份。只有验签通过时才完整可信。
type Scope struct {
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}

// Checker 校验 SigV4。
//
// Region 与 Services 为空时拒绝一切：漏配应失败，而不是变成不验区域。
// Now 为空时用墙上时钟。签名是否过期只能和墙上时钟比，它不参与
// 消息排序（契约 08 禁止的是用墙上时钟定先后）。
type Checker struct {
	Region   string
	Services map[string]bool
	// MaxSkew 为零时用 DefaultSkew。
	MaxSkew time.Duration
	Now     func() time.Time
	// Secret 按 access key id 取密钥。找不到返回 false。
	Secret func(accessKeyID string) (string, bool)
	// rawPath 是请求的原始路径。段内的 %2F 靠它留在同一段。
	// 由 VerifyRaw 写入，Verify 保持为空。
	rawPath string
}

// VerifyRaw 与 Verify 相同，但使用原始路径的分段边界。
func (c Checker) VerifyRaw(method, path, rawPath, rawQuery string, headers map[string][]string, body []byte) (Scope, error) {
	c.rawPath = rawPath
	return c.Verify(method, path, rawQuery, headers, body)
}

// Verify 校验一个 HTTP 请求。body 为 nil 表示调用方不提供正文，
// 此时不核对正文哈希；空切片表示正文为空，必须与签进去的哈希一致。
func (c Checker) Verify(method, path, rawQuery string, headers map[string][]string, body []byte) (Scope, error) {
	if method == "" {
		return Scope{}, ErrMalformed
	}
	method = strings.ToUpper(method)
	auth, signed, querySig, qerr := c.material(rawQuery, headers)
	if qerr != nil {
		return Scope{}, qerr
	}
	scope, amzDate, payloadHash, err := c.scopeOf(auth, headers)
	if err != nil {
		return scope, err
	}
	// 没有 x-amz-content-sha256 时，规范请求里的哈希就是正文本身的哈希。
	// boto3 不送这个头。把它当成空正文的哈希，带正文的请求会全部验签失败。
	// 调用方不提供正文（nil）时仍按空正文处理，与 AWS 的 GET 示例一致。
	if payloadHash == "" {
		if body == nil {
			payloadHash = emptyHash
		} else {
			sum := sha256.Sum256(body)
			payloadHash = hex.EncodeToString(sum[:])
		}
	}
	if err := c.checkTime(amzDate, auth.expires); err != nil {
		return scope, err
	}
	if err := unsignedAmzHeaders(headers, signed); err != nil {
		return scope, err
	}
	if body != nil && payloadHash != "UNSIGNED-PAYLOAD" {
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != strings.ToLower(payloadHash) {
			return scope, ErrBadSignature
		}
	}
	canon, err := canonicalRequest(method, path, c.rawPath, rawQuery, headers, signed, payloadHash)
	if err != nil {
		return scope, ErrMalformed
	}
	secret, ok := "", false
	if c.Secret != nil {
		secret, ok = c.Secret(scope.AccessKeyID)
	}
	if !ok {
		// 仍然算一次，避免「没有这把钥匙」比「签名不对」明显更快。
		secret = "invalid-access-key-secret"
	}
	got := signature(secret, scope, amzDate, canon)
	match := hmac.Equal([]byte(got), []byte(strings.ToLower(querySig)))
	if !ok || !match {
		return scope, ErrBadSignature
	}
	return scope, nil
}

type authz struct {
	accessKeyID string
	date        string
	region      string
	service     string
	signed      []string
	signature   string
	// amzDate 是查询串形式的 X-Amz-Date。头形式的日期在 x-amz-date 头里。
	amzDate    string
	expires    time.Duration
	hasExpires bool
}

// material 从 Authorization 头或查询串取出签名材料。两者都有时以头为准。
func (c Checker) material(rawQuery string, headers map[string][]string) (authz, []string, string, error) {
	if h := headerOne(headers, "authorization"); h != "" {
		a, err := parseAuthorization(h)
		if err != nil {
			return authz{}, nil, "", err
		}
		return a, a.signed, a.signature, nil
	}
	_, params, err := canonicalQuery(rawQuery)
	if err != nil {
		return authz{}, nil, "", ErrMalformed
	}
	if params["X-Amz-Algorithm"] == "" {
		return authz{}, nil, "", ErrMalformed
	}
	if params["X-Amz-Algorithm"] != algorithm {
		return authz{}, nil, "", ErrMalformed
	}
	scope, err := parseCredential(params["X-Amz-Credential"])
	if err != nil {
		return authz{}, nil, "", err
	}
	signed := splitSigned(params["X-Amz-SignedHeaders"])
	sig := params["X-Amz-Signature"]
	if sig == "" || params["X-Amz-Date"] == "" || len(signed) == 0 {
		return authz{}, nil, "", ErrMalformed
	}
	a := authz{
		accessKeyID: scope.AccessKeyID,
		date:        scope.Date,
		region:      scope.Region,
		service:     scope.Service,
		signed:      signed,
		signature:   sig,
		amzDate:     params["X-Amz-Date"],
	}
	if exp := params["X-Amz-Expires"]; exp != "" {
		d, err := parseExpires(exp)
		if err != nil {
			return authz{}, nil, "", err
		}
		a.expires = d
		a.hasExpires = true
	}
	return a, signed, sig, nil
}

func (c Checker) scopeOf(a authz, headers map[string][]string) (Scope, string, string, error) {
	scope := Scope{
		AccessKeyID: a.accessKeyID,
		Date:        a.date,
		Region:      a.region,
		Service:     a.service,
	}
	if scope.AccessKeyID == "" || scope.Region == "" || scope.Service == "" {
		return scope, "", "", ErrMalformed
	}
	if c.Region == "" || scope.Region != c.Region {
		return scope, "", "", ErrRegion
	}
	if len(c.Services) == 0 || !c.Services[scope.Service] {
		return scope, "", "", ErrService
	}
	headerDate := headerOne(headers, "x-amz-date")
	amzDate := headerDate
	if amzDate == "" {
		amzDate = a.amzDate
	}
	if headerDate != "" && a.amzDate != "" && headerDate != a.amzDate {
		return scope, "", "", ErrMalformed
	}
	if len(amzDate) < 8 || amzDate[:8] != scope.Date {
		return scope, "", "", ErrMalformed
	}
	payload := headerOne(headers, "x-amz-content-sha256")
	return scope, amzDate, payload, nil
}

func (c Checker) checkTime(amzDate string, expires time.Duration) error {
	ts, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return ErrMalformed
	}
	now := c.now()
	if expires > 0 {
		if expires > MaxExpires {
			return ErrExpired
		}
		if now.Before(ts) || now.After(ts.Add(expires)) {
			return ErrExpired
		}
		return nil
	}
	skew := c.MaxSkew
	if skew == 0 {
		skew = DefaultSkew
	}
	d := now.Sub(ts)
	if d < 0 {
		d = -d
	}
	if d > skew {
		return ErrExpired
	}
	return nil
}

func (c Checker) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func unsignedAmzHeaders(headers map[string][]string, signed []string) error {
	have := map[string]bool{}
	for _, s := range signed {
		have[strings.ToLower(strings.TrimSpace(s))] = true
	}
	if !have["host"] {
		return ErrMalformed
	}
	for k := range headers {
		name := strings.ToLower(k)
		if strings.HasPrefix(name, "x-amz-") && !have[name] {
			return ErrUnsigned
		}
	}
	return nil
}

func parseAuthorization(h string) (authz, error) {
	h = strings.TrimSpace(h)
	algo, rest, ok := strings.Cut(h, " ")
	if !ok || algo != algorithm {
		return authz{}, ErrMalformed
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return authz{}, ErrMalformed
		}
		fields[k] = v
	}
	scope, err := parseCredential(fields["Credential"])
	if err != nil {
		return authz{}, err
	}
	signed := splitSigned(fields["SignedHeaders"])
	sig := fields["Signature"]
	if sig == "" || len(signed) == 0 {
		return authz{}, ErrMalformed
	}
	return authz{
		accessKeyID: scope.AccessKeyID,
		date:        scope.Date,
		region:      scope.Region,
		service:     scope.Service,
		signed:      signed,
		signature:   sig,
	}, nil
}

func parseCredential(cred string) (Scope, error) {
	// AKID/YYYYMMDD/region/service/aws4_request
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[0] == "" || parts[4] != terminator {
		return Scope{}, ErrMalformed
	}
	if len(parts[1]) != 8 {
		return Scope{}, ErrMalformed
	}
	return Scope{
		AccessKeyID: parts[0],
		Date:        parts[1],
		Region:      parts[2],
		Service:     parts[3],
	}, nil
}

func splitSigned(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ";")
}

func parseExpires(s string) (time.Duration, error) {
	if s == "" || s[0] == '-' || s[0] == '+' {
		return 0, ErrMalformed
	}
	var n int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrMalformed
		}
		n = n*10 + int64(s[i]-'0')
		if n > int64(MaxExpires/time.Second)+1 {
			return 0, ErrExpired
		}
	}
	if n <= 0 {
		return 0, ErrMalformed
	}
	return time.Duration(n) * time.Second, nil
}

func headerOne(headers map[string][]string, lowerName string) string {
	v, ok := headerValues(headers, lowerName)
	if !ok {
		return ""
	}
	return v
}

// Sign 计算签名的小写十六进制。canonical 是 CanonicalRequest 的返回值。
func Sign(secret string, scope Scope, amzDate, canonical string) string {
	return signature(secret, scope, amzDate, canonical)
}

func signature(secret string, scope Scope, amzDate, canonical string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), scope.Date)
	kRegion := hmacSHA256(kDate, scope.Region)
	kService := hmacSHA256(kRegion, scope.Service)
	kSigning := hmacSHA256(kService, terminator)
	sum := sha256.Sum256([]byte(canonical))
	stringToSign := algorithm + "\n" + amzDate + "\n" +
		scope.Date + "/" + scope.Region + "/" + scope.Service + "/" + terminator + "\n" +
		hex.EncodeToString(sum[:])
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write([]byte(msg))
	return m.Sum(nil)
}
