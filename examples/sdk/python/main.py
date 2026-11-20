#!/usr/bin/env python3
"""用 boto3 对一个 open-iot-core 节点跑一遍控制面与数据面。

这个文件是教程「SDK 接入 / Python」那一页的来源（UI-080）：页面上的每段
代码都按 tutorial:begin/end 锚点从这里取，不是另抄一份。另抄的那份会腐坏而没有任何东西会
发现——改了接口形状，文档里那段照样好好地印在页面上。

跑法见同目录的 README.md。
"""

import json
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


# tutorial:begin client
def make_clients(endpoint, region, key_id, secret, ca_file=None):
    """建控制面与数据面两个客户端。

    endpoint 必须带上 /api/aws 前缀：兼容面挂在管理端口的这个路径下
    （API-100）。漏掉之后请求打到控制台首页，拿回一页 HTML，而 botocore
    报的是 JSON 解析失败，不指向真因。

    两个客户端而不是一个：控制面（建设备、签证书、改策略）的签名服务名是
    iot，数据面（发布、读写影子）的是 iotdata（API-110）。签错一个和签名
    错误得到的是同一个拒绝，而那条错误不会说服务名填反了。
    """
    common = dict(
        endpoint_url=endpoint,
        # region 必须落在节点 aws_api.region 的允许列表里（AUTH-074），
        # 否则一律拒绝——节点不会"默认放行"一个它没配过的区域。
        region_name=region,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        # 教程里的脚本要的是立刻看到错误，而默认重试会把一个 403 变成
        # 等好几秒才出现的 403。
        config=Config(retries={"max_attempts": 1}),
    )
    # 节点用自签 CA 时要显式信任它。CA 链在数据目录的 pki/ca-chain.pem。
    if ca_file:
        common["verify"] = ca_file
    return boto3.client("iot", **common), boto3.client("iot-data", **common)


# tutorial:end client


# tutorial:begin thing
def ensure_thing(iot, thing, policy, document):
    """建设备与策略。已存在时不算失败——这个脚本会被反复跑。"""
    try:
        iot.create_thing(thingName=thing)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    try:
        iot.create_policy(policyName=policy, policyDocument=document)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise


# tutorial:end thing


# tutorial:begin cert
def issue_cert(iot, thing, policy, out_dir):
    """签一张设备证书，附策略，并绑到设备。

    三步都要做：少了 attach_policy，设备连上来没有授权，CONNECT 直接被拒；
    少了 attach_thing_principal，策略里的 ${iot:Connection.Thing.ThingName}
    求不出值，而变量求不出值时整条语句拒绝（不是跳过）。

    私钥只在这一次响应里出现，服务端不留。没存下来就只能重签一张。
    """
    issued = iot.create_keys_and_certificate(setAsActive=True)
    iot.attach_policy(policyName=policy, target=issued["certificateArn"])
    iot.attach_thing_principal(thingName=thing, principal=issued["certificateArn"])

    os.makedirs(out_dir, exist_ok=True)
    files = {
        "device.pem": issued["certificatePem"],
        "device.key": issued["keyPair"]["PrivateKey"],
    }
    for name, body in files.items():
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        # 私钥按 0600：它和证书不同，泄露等于设备被别人冒充。
        if name.endswith(".key"):
            os.chmod(path, 0o600)
    return issued["certificateId"]


# tutorial:end cert


# tutorial:begin publish
def publish(data, topic, payload):
    """往一个主题发一条消息（数据面）。

    HTTP 发布只有 QoS 0 一档——要 QoS 1 得走 MQTT。
    """
    data.publish(topic=topic, qos=0, payload=json.dumps(payload).encode())


# tutorial:end publish


# tutorial:begin shadow
def set_desired(data, thing, desired):
    """下发一段 desired，再读回整份影子文档。

    影子是深度合并的：只写要改的键，没提到的键保持原值。要删一个键得显式
    把它写成 None（JSON 的 null）——传 {} 过去什么也不会发生。
    """
    data.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"desired": desired}}).encode(),
    )
    got = data.get_thing_shadow(thingName=thing)
    return json.loads(got["payload"].read())


# tutorial:end shadow


# tutorial:begin errors
def describe(err):
    """把一个失败翻译成"该去修什么"。

    401 与 403 是两回事，合成一句"调用失败"之后调用方只能猜：

    - UnauthorizedException（401）：密钥不对、被停用、过期，或者本机时钟
      偏了（SigV4 带时间戳）。密钥不存在与签名错误刻意是同一个拒绝
      （AUTH-075），免得这个接口变成探测密钥是否存在的工具。
    - AccessDeniedException（403）：签名是对的，角色不够。换一把权限更高
      的钥匙——reader 只读，operator 做日常写，admin 另可动信任根。
    """
    if isinstance(err, ClientError):
        code = err.response["Error"]["Code"]
        if code == "UnauthorizedException":
            return "401：密钥不对/已停用/已过期，或本机时钟偏了"
        if code == "AccessDeniedException":
            return "403：签名没问题，是角色不够——换一把权限更高的钥匙"
        return f"{code}: {err.response['Error'].get('Message', '')}"
    if "while parsing" in str(err) or "<!doctype" in str(err).lower():
        return "endpoint 少了 /api/aws 前缀：请求打到了控制台首页，拿回的是 HTML"
    return str(err)


# tutorial:end errors


POLICY_DOCUMENT = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "iot:Connect",
                "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}",
            },
            {
                "Effect": "Allow",
                "Action": ["iot:Publish", "iot:Receive"],
                "Resource": "arn:aws:iot:*:*:topic/$aws/things/"
                "${iot:Connection.Thing.ThingName}/*",
            },
            {
                "Effect": "Allow",
                "Action": "iot:Subscribe",
                "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/"
                "${iot:Connection.Thing.ThingName}/*",
            },
        ],
    }
)


def main():
    endpoint = os.environ.get("OIOT_ENDPOINT")
    if not endpoint:
        sys.exit("要 OIOT_ENDPOINT，形如 http://127.0.0.1:9090/api/aws（前缀必须带上）")
    thing = os.environ.get("OIOT_THING", "lamp-py")
    policy = os.environ.get("OIOT_POLICY", "lamp-py-scoped")
    out_dir = os.environ.get("OIOT_OUT", "./out")

    iot, data = make_clients(
        endpoint,
        os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        os.environ.get("AWS_ACCESS_KEY_ID", ""),
        os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        os.environ.get("OIOT_CA_FILE"),
    )

    try:
        ensure_thing(iot, thing, policy, POLICY_DOCUMENT)
        print(f"设备与策略就位：{thing} / {policy}")

        cert_id = issue_cert(iot, thing, policy, out_dir)
        print(f"证书已签发：{cert_id}（证书与私钥在 {out_dir}/）")

        endpoint_address = iot.describe_endpoint(endpointType="iot:Data-ATS")
        print(f"MQTT 接入地址：{endpoint_address['endpointAddress']}")

        topic = f"$aws/things/{thing}/shadow/update"
        publish(data, topic, {"state": {"reported": {"online": True}}})
        print(f"已发布到 {topic}")

        doc = set_desired(data, thing, {"interval": 30})
        print(f"影子文档：{json.dumps(doc, ensure_ascii=False)}")

        for principal in iot.list_thing_principals(thingName=thing)["principals"]:
            attached = iot.list_attached_policies(target=principal)
            names = ", ".join(p["policyName"] for p in attached["policies"])
            print(f"主体 {principal} 上的策略：{names}")
    except Exception as err:  # noqa: BLE001 —— 教程要的是可读的那一句
        sys.exit(describe(err))


if __name__ == "__main__":
    main()
