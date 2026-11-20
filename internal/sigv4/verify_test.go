package sigv4

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	exampleKey    = "AKIDEXAMPLE"
	exampleSecret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	// publishedIAMSignature 来自 AWS 文档里的 ListUsers 示例，不是本仓库算出来的。
	// 签名与规范请求对不上时，这道测试必须失败。
	publishedIAMSignature = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
)

func exampleChecker(now time.Time, services ...string) Checker {
	set := map[string]bool{}
	for _, s := range services {
		set[s] = true
	}
	return Checker{
		Region:   "us-east-1",
		Services: set,
		Now:      func() time.Time { return now },
		Secret: func(id string) (string, bool) {
			if id == exampleKey {
				return exampleSecret, true
			}
			return "", false
		},
	}
}

func iamHeaders() map[string][]string {
	return map[string][]string{
		// 两个空格必须被折成一个，否则和 AWS 示例的规范请求对不上。
		"Content-Type": {"application/x-www-form-urlencoded;  charset=utf-8"},
		"Host":         {"iam.amazonaws.com"},
		"X-Amz-Date":   {"20150830T123600Z"},
		"Authorization": {"AWS4-HMAC-SHA256 " +
			"Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, " +
			"SignedHeaders=content-type;host;x-amz-date, " +
			"Signature=" + publishedIAMSignature},
	}
}

func TestAuthorizationHeaderMatchesAWSExample(t *testing.T) {
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	headers := iamHeaders()
	got, err := CanonicalRequest("GET", "/", "Action=ListUsers&Version=2010-05-08",
		headers, []string{"content-type", "host", "x-amz-date"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"GET",
		"/",
		"Action=ListUsers&Version=2010-05-08",
		"content-type:application/x-www-form-urlencoded; charset=utf-8",
		"host:iam.amazonaws.com",
		"x-amz-date:20150830T123600Z",
		"",
		"content-type;host;x-amz-date",
		emptyHash,
	}, "\n")
	if got != want {
		t.Fatalf("规范请求与 AWS 示例不一致:\n%s", got)
	}
	scope, err := exampleChecker(now, "iam").Verify("GET", "/",
		"Action=ListUsers&Version=2010-05-08", headers, nil)
	if err != nil {
		t.Fatal(err)
	}
	if scope.AccessKeyID != exampleKey || scope.Service != "iam" || scope.Region != "us-east-1" {
		t.Fatalf("范围 = %+v", scope)
	}
}

func TestBadSignatureAndUnknownKeyLookTheSame(t *testing.T) {
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	headers := iamHeaders()
	headers["Authorization"] = []string{strings.Replace(
		headers["Authorization"][0], publishedIAMSignature, strings.Repeat("ab", 32), 1)}
	_, err := exampleChecker(now, "iam").Verify("GET", "/",
		"Action=ListUsers&Version=2010-05-08", headers, nil)
	if !errors.Is(err, ErrBadSignature) {
		t.Fatalf("坏签名 = %v", err)
	}

	c := exampleChecker(now, "iam")
	c.Secret = func(string) (string, bool) { return "", false }
	headers = iamHeaders()
	_, err = c.Verify("GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil)
	if !errors.Is(err, ErrBadSignature) {
		t.Fatalf("未知密钥 = %v，应与坏签名是同一个错误", err)
	}
}

func TestClockSkewIsFifteenMinutes(t *testing.T) {
	base := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	headers := iamHeaders()
	if _, err := exampleChecker(base.Add(14*time.Minute), "iam").Verify(
		"GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil); err != nil {
		t.Fatal(err)
	}
	_, err := exampleChecker(base.Add(16*time.Minute), "iam").Verify(
		"GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil)
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("超出 15 分钟 = %v", err)
	}
}

func TestRegionAndServiceMustMatch(t *testing.T) {
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	headers := iamHeaders()
	c := exampleChecker(now, "iam")
	c.Region = "eu-west-1"
	if _, err := c.Verify("GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil); !errors.Is(err, ErrRegion) {
		t.Fatalf("区域 = %v", err)
	}
	c = exampleChecker(now, "iot")
	if _, err := c.Verify("GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil); !errors.Is(err, ErrService) {
		t.Fatalf("服务名 = %v", err)
	}
	c = exampleChecker(now)
	if _, err := c.Verify("GET", "/", "Action=ListUsers&Version=2010-05-08", headers, nil); !errors.Is(err, ErrService) {
		t.Fatalf("空服务列表 = %v", err)
	}
}

func TestUnsignedAmzHeaderIsRejected(t *testing.T) {
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	headers := iamHeaders()
	headers["X-Amz-Security-Token"] = []string{"session"}
	_, err := exampleChecker(now, "iam").Verify("GET", "/",
		"Action=ListUsers&Version=2010-05-08", headers, nil)
	if !errors.Is(err, ErrUnsigned) {
		t.Fatalf("未签名的 x-amz 头 = %v", err)
	}
}

func TestQuerySignatureIgnoresParameterOrder(t *testing.T) {
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	// 参数故意不按字母序，签名放在最前。规范串必须自己排序并去掉签名。
	raw := "X-Amz-Signature=1a3461e5a46de7584b5c6aaac9d23061d4e13eee1036c22c8349806a237280bf" +
		"&X-Amz-SignedHeaders=host" +
		"&X-Amz-Date=20150830T123600Z" +
		"&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fiotdevicegateway%2Faws4_request" +
		"&X-Amz-Algorithm=AWS4-HMAC-SHA256"
	headers := map[string][]string{
		"Host": {"example.iot.us-east-1.amazonaws.com"},
	}
	scope, err := exampleChecker(now, ServiceDeviceGateway).Verify("GET", "/mqtt", raw, headers, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if scope.Service != ServiceDeviceGateway {
		t.Fatalf("服务 = %s", scope.Service)
	}

	tampered := strings.Replace(raw, "X-Amz-Date=20150830T123600Z", "X-Amz-Date=20150830T123601Z", 1)
	if _, err := exampleChecker(now, ServiceDeviceGateway).Verify("GET", "/mqtt", tampered, headers, []byte{}); !errors.Is(err, ErrBadSignature) && !errors.Is(err, ErrMalformed) {
		t.Fatalf("改过的查询串 = %v", err)
	}
}

func TestPresignExpiresIsNotSkew(t *testing.T) {
	ts := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	raw := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fiotdevicegateway%2Faws4_request" +
		"&X-Amz-Date=20150830T123600Z" +
		"&X-Amz-Expires=60" +
		"&X-Amz-SignedHeaders=host"
	headers := map[string][]string{"Host": {"example.iot.us-east-1.amazonaws.com"}}
	canon, err := CanonicalRequest("GET", "/mqtt", raw, headers, []string{"host"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	sig := signature(exampleSecret, Scope{
		AccessKeyID: exampleKey,
		Date:        "20150830",
		Region:      "us-east-1",
		Service:     ServiceDeviceGateway,
	}, "20150830T123600Z", canon)
	raw += "&X-Amz-Signature=" + sig

	if _, err := exampleChecker(ts.Add(30*time.Second), ServiceDeviceGateway).Verify(
		"GET", "/mqtt", raw, headers, nil); err != nil {
		t.Fatal(err)
	}
	// 90 秒仍在 15 分钟偏移内。只认偏移的话这里会放行。
	if _, err := exampleChecker(ts.Add(90*time.Second), ServiceDeviceGateway).Verify(
		"GET", "/mqtt", raw, headers, nil); !errors.Is(err, ErrExpired) {
		t.Fatalf("超过 Expires = %v", err)
	}
	// 早一秒也在偏移内。预签名在时刻到达之前无效。
	if _, err := exampleChecker(ts.Add(-time.Second), ServiceDeviceGateway).Verify(
		"GET", "/mqtt", raw, headers, nil); !errors.Is(err, ErrExpired) {
		t.Fatalf("Expires 开始之前 = %v", err)
	}
}

func TestPresignExpiresIsCapped(t *testing.T) {
	ts := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	raw := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fiotdevicegateway%2Faws4_request" +
		"&X-Amz-Date=20150830T123600Z" +
		"&X-Amz-Expires=604801" +
		"&X-Amz-SignedHeaders=host"
	headers := map[string][]string{"Host": {"example.iot.us-east-1.amazonaws.com"}}
	canon, err := CanonicalRequest("GET", "/mqtt", raw, headers, []string{"host"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	sig := signature(exampleSecret, Scope{
		AccessKeyID: exampleKey, Date: "20150830", Region: "us-east-1", Service: ServiceDeviceGateway,
	}, "20150830T123600Z", canon)
	_, err = exampleChecker(ts.Add(time.Second), ServiceDeviceGateway).Verify(
		"GET", "/mqtt", raw+"&X-Amz-Signature="+sig, headers, nil)
	if err == nil {
		t.Fatal("超过 7 天的 Expires 不应被接受")
	}
}

// TestCanonicalPathDoubleEncodesEscapes 钉住路径的两次编码。
//
// AWS 的 SigV4（除 S3）对路径编两遍：客户端把自己已经编码过的路径再整体编
// 一次。于是线上的 `cert%2Fab` 在签名里是 `cert%252Fab`，`demo%2Ftopic`
// 是 `demo%252Ftopic`。
//
// 这条用例原先钉的是「结果仍是 %2F」——只编一次。那个期望是推断出来的，
// 而用例自己构造规范请求再自己验，两边同一套错规则，所以永远绿。真跑起来
// 的后果是数据面的 Publish（主题整段进路径）和控制面凡把 ARN 放在路径里的
// 操作，对 boto3 与 aws-sdk-go-v2 一律 401，而 401 的字面意思是密钥或时钟
// 有问题，不指向编码。
//
// 分段边界仍按原始路径切：段内的 %2F 在原始串里不是斜杠，不该变成新的一段。
func TestCanonicalPathDoubleEncodesEscapes(t *testing.T) {
	rawPath := "/attached-policies/arn:aws:iot:local:000000000000:cert%2Fab"
	path := "/attached-policies/arn:aws:iot:local:000000000000:cert/ab"
	got, err := CanonicalRequestRaw("POST", path, rawPath, "", map[string][]string{
		"Host": {"example"},
	}, []string{"host"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	want := "POST\n/attached-policies/arn%3Aaws%3Aiot%3Alocal%3A000000000000%3Acert%252Fab\n"
	if !strings.HasPrefix(got, want) {
		t.Fatalf("规范路径 = %q", got)
	}

	// 不带原始路径时只有解码后的路径可用，那一段的斜杠于是成了分段边界。
	// 这不是"也对的另一种"：VerifyRaw 必须把 RawPath 传进来，而这条断言
	// 钉住两者确实不同——传漏了会退回到签不上的那一种。
	plain, err := CanonicalRequest("POST", path, "", map[string][]string{
		"Host": {"example"},
	}, []string{"host"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(plain, want) {
		t.Fatal("未带上原始路径时，规范路径不该与按原始分段编出来的一样")
	}
}

// TestCanonicalPathEncodesTopicSegment 数据面 Publish 的主题整段进路径。
//
// SDK 把主题当一个路径参数，斜杠在线上是 %2F：/topics/demo%2Ftopic。
// 这一条是上面那个缺陷的另一半现场——两个 SDK 都这么发，而本节点此前
// 对所有 Publish 回 401。
func TestCanonicalPathEncodesTopicSegment(t *testing.T) {
	got, err := CanonicalRequestRaw("POST", "/api/aws/topics/demo/topic",
		"/api/aws/topics/demo%2Ftopic", "", map[string][]string{
			"Host": {"example"},
		}, []string{"host"}, emptyHash)
	if err != nil {
		t.Fatal(err)
	}
	want := "POST\n/api/aws/topics/demo%252Ftopic\n"
	if !strings.HasPrefix(got, want) {
		t.Fatalf("规范路径 = %q", got)
	}
}

// TestPostBodySignedWithoutContentHashHeader 验证没有 x-amz-content-sha256
// 时，规范请求用的是正文哈希。
//
// boto3 的 CreateThing 就是这样签的。把缺省值改回空正文哈希，这条失败。
func TestPostBodySignedWithoutContentHashHeader(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	body := []byte("{}")
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	amz := "20261029T000000Z"
	headers := map[string][]string{
		"Content-Type": {"application/json"},
		"Host":         {"127.0.0.1:39091"},
		"X-Amz-Date":   {amz},
	}
	signed := []string{"content-type", "host", "x-amz-date"}
	canon, err := CanonicalRequest("POST", "/api/aws/things/lamp-boto", "", headers, signed, payload)
	if err != nil {
		t.Fatal(err)
	}
	sig := Sign(exampleSecret, Scope{
		AccessKeyID: exampleKey, Date: "20261029", Region: "us-east-1", Service: "iot",
	}, amz, canon)
	headers["Authorization"] = []string{
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261029/us-east-1/iot/aws4_request, " +
			"SignedHeaders=content-type;host;x-amz-date, Signature=" + sig,
	}
	if _, err := exampleChecker(now, "iot").Verify("POST", "/api/aws/things/lamp-boto", "", headers, body); err != nil {
		t.Fatal(err)
	}
}
