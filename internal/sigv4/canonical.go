package sigv4

import (
	"strings"
)

// 规范化规则按 AWS Signature Version 4。
//
// 空格必须编成 %20 而不是加号，百分号十六进制必须大写。用 net/url 的
// QueryEscape 会把空格编成加号，boto3 签的是 %20，两边对不上时签名
// 永远失败，而且失败看起来像密钥错了。

func isUnreserved(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~'
}

func awsEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexUpper[c>>4])
		b.WriteByte(hexUpper[c&0x0f])
	}
	return b.String()
}

const hexUpper = "0123456789ABCDEF"

type queryPair struct {
	name  string
	value string
}

// canonicalQuery 规范化查询串，并取出签名参数。
//
// X-Amz-Signature 不进入规范查询串：它是签名的结果，签的时候还不存在。
func canonicalQuery(raw string) (string, map[string]string, error) {
	params := map[string]string{}
	if raw == "" {
		return "", params, nil
	}
	var pairs []queryPair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		name, value, _ := strings.Cut(part, "=")
		decodedName, err := pctDecode(name)
		if err != nil {
			return "", nil, err
		}
		decodedValue, err := pctDecode(value)
		if err != nil {
			return "", nil, err
		}
		if decodedName == "X-Amz-Signature" {
			params[decodedName] = decodedValue
			continue
		}
		// 其余 X-Amz-* 既要进规范串，也要能被验签逻辑读到。
		if strings.HasPrefix(decodedName, "X-Amz-") {
			params[decodedName] = decodedValue
		}
		pairs = append(pairs, queryPair{
			name:  awsEncode(decodedName),
			value: awsEncode(decodedValue),
		})
	}
	sortPairs(pairs)
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.name)
		b.WriteByte('=')
		b.WriteString(p.value)
	}
	return b.String(), params, nil
}

func sortPairs(pairs []queryPair) {
	// 按编码后的名字、再按编码后的值。数量是查询参数的个数，直接插入排序。
	for i := 1; i < len(pairs); i++ {
		j := i
		for j > 0 && (pairs[j].name < pairs[j-1].name ||
			(pairs[j].name == pairs[j-1].name && pairs[j].value < pairs[j-1].value)) {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
			j--
		}
	}
}

func pctDecode(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '+':
			// application/x-www-form-urlencoded 把空格写成加号。
			// 规范形式要的是 %20，先还原成空格再编码。
			b.WriteByte(' ')
		case '%':
			if i+2 >= len(s) {
				return "", errMalformed
			}
			hi, ok1 := fromHex(s[i+1])
			lo, ok2 := fromHex(s[i+2])
			if !ok1 || !ok2 {
				return "", errMalformed
			}
			b.WriteByte(hi<<4 | lo)
			i += 2
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String(), nil
}

func fromHex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

// canonicalHeaders 按签名头名单生成规范头与 SignedHeaders 串。
//
// 名单会被排序。调用方传来的顺序不作数：规范要求按头名字母序，
// boto3 签的就是这个顺序。
func canonicalHeaders(headers map[string][]string, signed []string) (canon, signedHeader string, err error) {
	if len(signed) == 0 {
		return "", "", errMalformed
	}
	names := append([]string(nil), signed...)
	for i := range names {
		names[i] = strings.ToLower(strings.TrimSpace(names[i]))
	}
	sortStrings(names)
	var b strings.Builder
	for i, name := range names {
		if name == "" || name == "authorization" {
			return "", "", errMalformed
		}
		if i > 0 && names[i] == names[i-1] {
			return "", "", errMalformed
		}
		vals, ok := headerValues(headers, name)
		if !ok {
			return "", "", errMalformed
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(vals)
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(names, ";"), nil
}

func headerValues(headers map[string][]string, lowerName string) (string, bool) {
	var vals []string
	for k, vs := range headers {
		if strings.ToLower(k) != lowerName {
			continue
		}
		vals = append(vals, vs...)
	}
	if len(vals) == 0 {
		return "", false
	}
	for i, v := range vals {
		vals[i] = collapseSpace(v)
	}
	return strings.Join(vals, ","), true
}

func collapseSpace(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	space := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		j := i
		for j > 0 && a[j] < a[j-1] {
			a[j], a[j-1] = a[j-1], a[j]
			j--
		}
	}
}

// CanonicalRequest 是签进去的那一段文本。测试用它钉住与 AWS 示例逐字节一致。
func CanonicalRequest(method, path, rawQuery string, headers map[string][]string, signed []string, payloadHash string) (string, error) {
	return canonicalRequest(method, path, "", rawQuery, headers, signed, payloadHash)
}

// CanonicalRequestRaw 与 CanonicalRequest 相同，但按 rawPath 的分段边界编码路径。
//
// 控制面要把证书 ARN 放进路径。ARN 里的斜杠在线上是 %2F，必须留在同一段里。
func CanonicalRequestRaw(method, path, rawPath, rawQuery string, headers map[string][]string, signed []string, payloadHash string) (string, error) {
	return canonicalRequest(method, path, rawPath, rawQuery, headers, signed, payloadHash)
}

func canonicalRequest(method, path, rawPath, rawQuery string, headers map[string][]string, signed []string, payloadHash string) (string, error) {
	q, _, err := canonicalQuery(rawQuery)
	if err != nil {
		return "", err
	}
	canonHeaders, signedHeader, err := canonicalHeaders(headers, signed)
	if err != nil {
		return "", err
	}
	cp := canonicalPathRaw(path, rawPath)
	if cp == "" {
		return "", errMalformed
	}
	return method + "\n" +
		cp + "\n" +
		q + "\n" +
		canonHeaders + "\n" +
		signedHeader + "\n" +
		payloadHash, nil
}
