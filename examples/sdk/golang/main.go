/*
用 AWS SDK for Go v2 对一个 open-iot-core 节点跑一遍控制面与数据面。

这个文件是教程「SDK 接入 / Go」那一页的来源（UI-080）：页面上的每段代码
都由控制台按 tutorial:begin/end 锚点从这里取，不是另抄一份。另抄的那份会腐坏
而没有任何东西会发现——改了接口形状，文档里那段照样好好地印在页面上。

它是独立 module（examples/sdk/golang/go.mod），不进主 module：主 module
只有 bbolt 与 yaml/v3 两个直接依赖（LANG-019），教程的代价不该由运行时来付。

跑法见同目录的 README.md。
*/
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/smithy-go"
)

// Client 把控制面与数据面两个客户端放在一起。
//
// 两个而不是一个：控制面（建设备、签证书、改策略）的签名服务名是 iot，
// 数据面（发布、读写影子）的是 iotdata（API-110）。签错一个与签名错误
// 得到的是同一个拒绝，而那条错误不会告诉你服务名填反了。
type Client struct {
	Control *iot.Client
	Data    *iotdataplane.Client
}

// tutorial:begin client
// NewClient 把两个客户端指向自建节点。
//
// 关键是 BaseEndpoint：AWS SDK Go v2 没有 host 参数，endpoint 走
// endpoint v2 机制，而 open-iot-core 的兼容面挂在管理端口的 /api/aws 下
// （API-100）——这个路径前缀必须写进 endpoint，SDK 会把它拼到操作路径
// 前面。漏掉之后请求打到控制台首页，拿回一页 HTML，报错是
// "invalid character '<'"，不指向真因。
func NewClient(endpoint, region, keyID, secret, caFile string) (*Client, error) {
	opts := []func(*config.LoadOptions) error{
		// region 必须落在节点 aws_api.region 的允许列表里（AUTH-074），
		// 否则一律拒绝——节点不会"默认放行"一个它没配过的区域。
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(keyID, secret, "")),
	}
	// 节点用自签 CA 时要显式信任它。CA 链在数据目录的 pki/ca-chain.pem。
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s 里没有证书", caFile)
		}
		opts = append(opts, config.WithHTTPClient(
			awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
				tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
			})))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	// 每个客户端单独给 BaseEndpoint，而不是 config.WithBaseEndpoint：
	// 后者对所有服务生效，同一个 cfg 上再建 S3 客户端就会被一起指到
	// 这个节点来，而节点并不提供 S3。
	return &Client{
		Control: iot.NewFromConfig(cfg, func(o *iot.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		}),
		Data: iotdataplane.NewFromConfig(cfg, func(o *iotdataplane.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		}),
	}, nil
}

// tutorial:end client

// tutorial:begin thing
// EnsureThing 建设备与策略，并把策略附到设备上。
//
// 已存在时不算失败：教程里的脚本会被反复跑，而 ResourceAlreadyExists
// 对"确保它在"这个意图来说是成功。
func EnsureThing(ctx context.Context, c *iot.Client, thing, policy, document string) error {
	_, err := c.CreateThing(ctx, &iot.CreateThingInput{ThingName: aws.String(thing)})
	if err != nil && !alreadyExists(err) {
		return fmt.Errorf("CreateThing: %w", err)
	}
	_, err = c.CreatePolicy(ctx, &iot.CreatePolicyInput{
		PolicyName:     aws.String(policy),
		PolicyDocument: aws.String(document),
	})
	if err != nil && !alreadyExists(err) {
		return fmt.Errorf("CreatePolicy: %w", err)
	}
	return nil
}

// tutorial:end thing

// tutorial:begin thing-exists
// alreadyExists 把"已经有了"从真失败里分出来。
//
// 用 errors.As 取服务端的错误码而不是匹配错误字符串：字符串里带着
// 请求 ID，每次都不一样。
func alreadyExists(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() == "ResourceAlreadyExistsException"
	}
	return false
}

// tutorial:end thing-exists

// tutorial:begin cert
// IssueCert 签一张设备证书，附上策略，并绑到设备。
//
// 三步都要做：少了 AttachPolicy，设备连上来没有任何授权，CONNECT 直接
// 被拒；少了 AttachThingPrincipal，策略里的 ${iot:Connection.Thing.ThingName}
// 求不出值，而变量求不出值时整条语句拒绝（不是跳过）。
//
// 私钥只在这一次响应里出现，服务端不留。没存下来就只能重签一张。
func IssueCert(ctx context.Context, c *iot.Client, thing, policy, dir string) (string, error) {
	out, err := c.CreateKeysAndCertificate(ctx, &iot.CreateKeysAndCertificateInput{
		SetAsActive: true,
	})
	if err != nil {
		return "", fmt.Errorf("CreateKeysAndCertificate: %w", err)
	}
	if _, err := c.AttachPolicy(ctx, &iot.AttachPolicyInput{
		PolicyName: aws.String(policy),
		Target:     out.CertificateArn,
	}); err != nil {
		return "", fmt.Errorf("AttachPolicy: %w", err)
	}
	if _, err := c.AttachThingPrincipal(ctx, &iot.AttachThingPrincipalInput{
		ThingName: aws.String(thing),
		Principal: out.CertificateArn,
	}); err != nil {
		return "", fmt.Errorf("AttachThingPrincipal: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	files := map[string]string{
		"device.pem": aws.ToString(out.CertificatePem),
		"device.key": aws.ToString(out.KeyPair.PrivateKey),
	}
	for name, body := range files {
		// 私钥按 0600 落盘：它和证书不同，泄露等于设备被别人冒充。
		if err := os.WriteFile(dir+"/"+name, []byte(body), 0o600); err != nil {
			return "", err
		}
	}
	return aws.ToString(out.CertificateId), nil
}

// tutorial:end cert

// tutorial:begin publish
// Publish 往一个主题发一条消息（数据面）。
//
// 这里用的是 Data 客户端：它和控制面共用同一个 /api/aws 前缀，但签名
// 服务名是 iotdata。QoS 0 是 AWS HTTP 发布的唯一档——要 QoS 1 得走 MQTT。
func Publish(ctx context.Context, c *iotdataplane.Client, topic string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = c.Publish(ctx, &iotdataplane.PublishInput{
		Topic:   aws.String(topic),
		Payload: body,
	})
	return err
}

// tutorial:end publish

// tutorial:begin shadow
// SetDesired 下发一段 desired，再读回整份影子文档。
//
// 影子是深度合并的：只写要改的键，没提到的键保持原值。要删一个键得显式
// 把它写成 null——传 {} 过去什么也不会发生。
func SetDesired(ctx context.Context, c *iotdataplane.Client, thing string, desired map[string]any) ([]byte, error) {
	doc, err := json.Marshal(map[string]any{"state": map[string]any{"desired": desired}})
	if err != nil {
		return nil, err
	}
	if _, err := c.UpdateThingShadow(ctx, &iotdataplane.UpdateThingShadowInput{
		ThingName: aws.String(thing),
		Payload:   doc,
	}); err != nil {
		return nil, fmt.Errorf("UpdateThingShadow: %w", err)
	}
	got, err := c.GetThingShadow(ctx, &iotdataplane.GetThingShadowInput{
		ThingName: aws.String(thing),
	})
	if err != nil {
		return nil, fmt.Errorf("GetThingShadow: %w", err)
	}
	return got.Payload, nil
}

// tutorial:end shadow

// tutorial:begin errors
// describe 把一个失败翻译成"该去修什么"。
//
// 401 与 403 是两回事，合成一句"调用失败"之后调用方只能猜：
//
//   - 401 UnauthorizedException：密钥不对、被停用、过期，或者本机时钟
//     偏了（SigV4 带时间戳）。密钥不存在与签名错误刻意是同一个拒绝
//     （AUTH-075），免得这个接口变成探测密钥是否存在的工具。
//   - 403 AccessDeniedException：签名是对的，角色不够。换一把权限更高
//     的钥匙——reader 只读，operator 做日常写，admin 另可动信任根。
func describe(err error) string {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		if strings.Contains(err.Error(), "invalid character '<'") {
			return "endpoint 少了 /api/aws 前缀：请求打到了控制台首页，拿回的是 HTML"
		}
		return "连不上或者响应不是 JSON：" + err.Error()
	}
	switch ae.ErrorCode() {
	case "UnauthorizedException":
		return "401：密钥不对/已停用/已过期，或本机时钟偏了"
	case "AccessDeniedException":
		return "403：签名没问题，是角色不够——换一把权限更高的钥匙"
	default:
		return ae.ErrorCode() + ": " + ae.ErrorMessage()
	}
}

// tutorial:end errors

const policyDocument = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}`

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	endpoint := env("OIOT_ENDPOINT", "")
	if endpoint == "" {
		log.Fatal("要 OIOT_ENDPOINT，形如 http://127.0.0.1:9090/api/aws（前缀必须带上）")
	}
	thing := env("OIOT_THING", "lamp-go")
	policy := env("OIOT_POLICY", "lamp-go-scoped")
	outDir := env("OIOT_OUT", "./out")

	cli, err := NewClient(
		endpoint,
		env("AWS_DEFAULT_REGION", "us-east-1"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		os.Getenv("OIOT_CA_FILE"),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := EnsureThing(ctx, cli.Control, thing, policy, policyDocument); err != nil {
		log.Fatal(describe(err))
	}
	fmt.Printf("设备与策略就位：%s / %s\n", thing, policy)

	certID, err := IssueCert(ctx, cli.Control, thing, policy, outDir)
	if err != nil {
		log.Fatal(describe(err))
	}
	fmt.Printf("证书已签发：%s（证书与私钥在 %s/）\n", certID, outDir)

	// 数据面：按设备名拿到这个节点的 MQTT 接入地址，再发一条消息。
	ep, err := cli.Control.DescribeEndpoint(ctx, &iot.DescribeEndpointInput{
		EndpointType: aws.String("iot:Data-ATS"),
	})
	if err != nil {
		log.Fatal(describe(err))
	}
	fmt.Printf("MQTT 接入地址：%s\n", aws.ToString(ep.EndpointAddress))

	topic := "$aws/things/" + thing + "/shadow/update"
	if err := Publish(ctx, cli.Data, topic, map[string]any{
		"state": map[string]any{"reported": map[string]any{"online": true}},
	}); err != nil {
		log.Fatal(describe(err))
	}
	fmt.Printf("已发布到 %s\n", topic)

	doc, err := SetDesired(ctx, cli.Data, thing, map[string]any{"interval": 30})
	if err != nil {
		log.Fatal(describe(err))
	}
	fmt.Printf("影子文档：%s\n", doc)

	// 列一下这台设备上挂的策略，确认附加真的生效了。
	principals, err := cli.Control.ListThingPrincipals(ctx, &iot.ListThingPrincipalsInput{
		ThingName: aws.String(thing),
	})
	if err != nil {
		log.Fatal(describe(err))
	}
	for _, p := range principals.Principals {
		attached, err := cli.Control.ListAttachedPolicies(ctx, &iot.ListAttachedPoliciesInput{
			Target: aws.String(p),
		})
		if err != nil {
			log.Fatal(describe(err))
		}
		names := make([]string, 0, len(attached.Policies))
		for _, pol := range attached.Policies {
			names = append(names, aws.ToString(pol.PolicyName))
		}
		fmt.Printf("主体 %s 上的策略：%s\n", p, strings.Join(names, ", "))
	}
}
