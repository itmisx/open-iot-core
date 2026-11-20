/*
 * 用 AWS SDK for Java v2 对一个 open-iot-core 节点跑一遍控制面与数据面。
 *
 * 这个文件是教程「SDK 接入 / Java」那一页的来源（UI-080）：页面上的每段
 * 代码都按 tutorial:begin/end 锚点从这里取，不是另抄一份。另抄的那份会腐坏而没有任何东西
 * 会发现——改了接口形状，文档里那段照样好好地印在页面上。
 *
 * 跑法见同目录的 README.md。
 */
package io.openiotcore.example;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iot.IotClient;
import software.amazon.awssdk.services.iot.model.CreateKeysAndCertificateResponse;
import software.amazon.awssdk.services.iot.model.IotException;
import software.amazon.awssdk.services.iot.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;

public final class Main {

    private Main() {
    }

    // tutorial:begin client
    /**
     * 控制面客户端。
     *
     * 关键是 endpointOverride：endpoint 必须带上 /api/aws 前缀，兼容面挂在
     * 管理端口的这个路径下（API-100）。漏掉之后请求打到控制台首页，拿回
     * 一页 HTML，而报错是反序列化失败，不指向真因。
     *
     * region 必须落在节点 aws_api.region 的允许列表里（AUTH-074），否则
     * 一律拒绝——节点不会"默认放行"一个它没配过的区域。
     */
    static IotClient control(String endpoint, String region, String keyId, String secret) {
        return IotClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(keyId, secret)))
                .build();
    }

    /**
     * 数据面客户端（发布、读写影子）。
     *
     * 和控制面共用同一个 /api/aws 前缀，但签名服务名是 iotdata（API-110）
     * ——SDK 按客户端类型自己选，所以这里要的是**另一个客户端**，不是在
     * 控制面客户端上换个方法。
     */
    static IotDataPlaneClient data(String endpoint, String region, String keyId, String secret) {
        return IotDataPlaneClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(keyId, secret)))
                .build();
    }
    // tutorial:end client

    // tutorial:begin thing
    /** 建设备与策略。已存在时不算失败——这个程序会被反复跑。 */
    static void ensureThing(IotClient iot, String thing, String policy, String document) {
        try {
            iot.createThing(b -> b.thingName(thing));
        } catch (ResourceAlreadyExistsException ignored) {
            // 已经有了，对"确保它在"这个意图来说是成功
        }
        try {
            iot.createPolicy(b -> b.policyName(policy).policyDocument(document));
        } catch (ResourceAlreadyExistsException ignored) {
            // 同上
        }
    }
    // tutorial:end thing

    // tutorial:begin cert
    /**
     * 签一张设备证书，附策略，并绑到设备。
     *
     * 三步都要做：少了 attachPolicy，设备连上来没有授权，CONNECT 直接被拒；
     * 少了 attachThingPrincipal，策略里的 ${iot:Connection.Thing.ThingName}
     * 求不出值，而变量求不出值时整条语句拒绝（不是跳过）。
     *
     * 私钥只在这一次响应里出现，服务端不留。没存下来就只能重签一张。
     */
    static String issueCert(IotClient iot, String thing, String policy, Path dir)
            throws Exception {
        CreateKeysAndCertificateResponse issued =
                iot.createKeysAndCertificate(b -> b.setAsActive(true));
        iot.attachPolicy(b -> b.policyName(policy).target(issued.certificateArn()));
        iot.attachThingPrincipal(b -> b.thingName(thing).principal(issued.certificateArn()));

        Files.createDirectories(dir);
        Files.writeString(dir.resolve("device.pem"), issued.certificatePem());
        Path key = dir.resolve("device.key");
        Files.writeString(key, issued.keyPair().privateKey());
        // 私钥只给自己读：它和证书不同，泄露等于设备被别人冒充。
        Files.setPosixFilePermissions(key, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        return issued.certificateId();
    }
    // tutorial:end cert

    // tutorial:begin publish
    /** 往一个主题发一条消息。HTTP 发布只有 QoS 0 一档——要 QoS 1 得走 MQTT。 */
    static void publish(IotDataPlaneClient data, String topic, String json) {
        data.publish(b -> b.topic(topic).qos(0).payload(SdkBytes.fromUtf8String(json)));
    }
    // tutorial:end publish

    // tutorial:begin shadow
    /**
     * 下发一段 desired，再读回整份影子文档。
     *
     * 影子是深度合并的：只写要改的键，没提到的键保持原值。要删一个键得显式
     * 把它写成 null——传 {} 过去什么也不会发生。
     */
    static String setDesired(IotDataPlaneClient data, String thing, String desiredJson) {
        String payload = "{\"state\":{\"desired\":" + desiredJson + "}}";
        data.updateThingShadow(b -> b.thingName(thing)
                .payload(SdkBytes.fromUtf8String(payload)));
        return data.getThingShadow(b -> b.thingName(thing)).payload().asUtf8String();
    }
    // tutorial:end shadow

    // tutorial:begin errors
    /**
     * 把一个失败翻译成"该去修什么"。
     *
     * 401 与 403 是两回事，合成一句"调用失败"之后调用方只能猜：
     *
     * - UnauthorizedException（401）：密钥不对、被停用、过期，或者本机时钟
     *   偏了（SigV4 带时间戳）。密钥不存在与签名错误刻意是同一个拒绝
     *   （AUTH-075），免得这个接口变成探测密钥是否存在的工具。
     * - AccessDeniedException（403）：签名是对的，角色不够。换一把权限更高
     *   的钥匙——reader 只读，operator 做日常写，admin 另可动信任根。
     */
    static String describe(Exception err) {
        if (err instanceof IotException e && e.awsErrorDetails() != null) {
            String code = e.awsErrorDetails().errorCode();
            if ("UnauthorizedException".equals(code)) {
                return "401：密钥不对/已停用/已过期，或本机时钟偏了";
            }
            if ("AccessDeniedException".equals(code)) {
                return "403：签名没问题，是角色不够——换一把权限更高的钥匙";
            }
            return code + ": " + e.awsErrorDetails().errorMessage();
        }
        String msg = String.valueOf(err.getMessage());
        if (msg.contains("<!doctype") || msg.contains("<!DOCTYPE")) {
            return "endpoint 少了 /api/aws 前缀：请求打到了控制台首页，拿回的是 HTML";
        }
        return msg;
    }
    // tutorial:end errors

    private static final String POLICY_DOCUMENT = """
            {
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
            }
            """;

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }

    public static void main(String[] args) {
        String endpoint = env("OIOT_ENDPOINT", "");
        if (endpoint.isEmpty()) {
            System.err.println("要 OIOT_ENDPOINT，形如 http://127.0.0.1:9090/api/aws（前缀必须带上）");
            System.exit(1);
        }
        String thing = env("OIOT_THING", "lamp-java");
        String policy = env("OIOT_POLICY", "lamp-java-scoped");
        Path out = Path.of(env("OIOT_OUT", "./out"));
        String region = env("AWS_DEFAULT_REGION", "us-east-1");
        String keyId = env("AWS_ACCESS_KEY_ID", "");
        String secret = env("AWS_SECRET_ACCESS_KEY", "");

        try (IotClient iot = control(endpoint, region, keyId, secret);
                IotDataPlaneClient data = data(endpoint, region, keyId, secret)) {
            ensureThing(iot, thing, policy, POLICY_DOCUMENT);
            System.out.printf("设备与策略就位：%s / %s%n", thing, policy);

            String certId = issueCert(iot, thing, policy, out);
            System.out.printf("证书已签发：%s（证书与私钥在 %s/）%n", certId, out);

            String address = iot.describeEndpoint(b -> b.endpointType("iot:Data-ATS"))
                    .endpointAddress();
            System.out.printf("MQTT 接入地址：%s%n", address);

            String topic = "$aws/things/" + thing + "/shadow/update";
            publish(data, topic, "{\"state\":{\"reported\":{\"online\":true}}}");
            System.out.printf("已发布到 %s%n", topic);

            System.out.printf("影子文档：%s%n", setDesired(data, thing, "{\"interval\":30}"));

            List<String> principals = iot.listThingPrincipals(b -> b.thingName(thing))
                    .principals();
            for (String principal : principals) {
                String names = String.join(", ", iot.listAttachedPolicies(b -> b.target(principal))
                        .policies().stream().map(p -> p.policyName()).toList());
                System.out.printf("主体 %s 上的策略：%s%n", principal, names);
            }
        } catch (Exception err) {
            System.err.println(describe(err));
            System.exit(1);
        }
    }
}
