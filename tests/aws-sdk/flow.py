#!/usr/bin/env python3
"""用 boto3 跑一遍注册、签发、附策略、连接、数据面、吊销。

对应的 aws CLI 子命令写在每一步上面。连接本身不是 CLI 能做的，
用签出来的证书做一次 MQTT 3.1.1 CONNECT。吊销之后同一次握手必须失败。
"""

import json
import os
import socket
import ssl
import sys

import boto3
from botocore.config import Config


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def mqtt_connect(host, port, ca, cert, key, client_id):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(ca)
    ctx.load_cert_chain(cert, key)
    raw = socket.create_connection((host, int(port)), timeout=5)
    try:
        sock = ctx.wrap_socket(raw, server_hostname="localhost")
    except ssl.SSLError as e:
        return b"", str(e)
    try:
        cid = client_id.encode()
        body = b"\x00\x04MQTT\x04\x02\x00\x1e" + len(cid).to_bytes(2, "big") + cid
        sock.sendall(bytes([0x10, len(body)]) + body)
        sock.settimeout(5)
        return sock.recv(4), ""
    except OSError as e:
        return b"", str(e)
    finally:
        sock.close()


def main():
    endpoint = os.environ["AWS_ENDPOINT"]
    thing = os.environ.get("THING", "lamp-boto")
    policy = os.environ.get("POLICY", "boto-open")
    ca = os.environ["CA_FILE"]
    mqtt_host = os.environ.get("MQTT_HOST", "127.0.0.1")
    mqtt_port = os.environ["MQTT_PORT"]
    work = os.environ["WORK"]

    # aws iot create-thing / create-policy / create-keys-and-certificate
    # attach-policy / attach-thing-principal / update-certificate
    iot = boto3.client(
        "iot",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(retries={"max_attempts": 1}),
    )

    # 数据面是另一个客户端：同一个 /api/aws 前缀，签名服务名 iotdata
    # （API-110）。此前这套测试只跑控制面，于是数据面的 Publish 对任何
    # SDK 都 401 这件事在这里完全看不见。
    data = boto3.client(
        "iot-data",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(retries={"max_attempts": 1}),
    )

    created = iot.create_thing(thingName=thing)
    if created["thingName"] != thing:
        die(f"create_thing = {created}")

    doc = (
        '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
        '"Action":"iot:*","Resource":"*"}]}'
    )
    iot.create_policy(policyName=policy, policyDocument=doc)

    issued = iot.create_keys_and_certificate(setAsActive=True)
    cert_arn = issued["certificateArn"]
    cert_id = issued["certificateId"]
    cert_path = os.path.join(work, "device.pem")
    key_path = os.path.join(work, "device.key")
    with open(cert_path, "w", encoding="utf-8") as f:
        f.write(issued["certificatePem"])
    with open(key_path, "w", encoding="utf-8") as f:
        f.write(issued["keyPair"]["PrivateKey"])
    os.chmod(key_path, 0o600)

    iot.attach_policy(policyName=policy, target=cert_arn)
    iot.attach_thing_principal(thingName=thing, principal=cert_arn)

    desc = iot.describe_thing(thingName=thing)
    if desc["thingName"] != thing:
        die(f"describe_thing = {desc}")

    ack, err = mqtt_connect(mqtt_host, mqtt_port, ca, cert_path, key_path, thing)
    if ack != b"\x20\x02\x00\x00":
        die(f"连接未被接受: {ack!r} {err}")

    # 把 ARN 放在**路径**里的操作。规范路径要对已编码的那一层再编一次
    # （AUTH-077 修订），少编一次时 ARN 里的 %2F 对不上，这里会 401 ——
    # 而 401 的字面意思是密钥或时钟有问题，不指向编码。
    # aws iot list-attached-policies --target <certArn>
    attached = iot.list_attached_policies(target=cert_arn)
    names = [p["policyName"] for p in attached["policies"]]
    if policy not in names:
        die(f"list_attached_policies = {names}")

    # 数据面：主题整段进路径，斜杠在线上是 %2F——与上面同一条编码规则。
    # aws iot-data publish --topic ... / get-thing-shadow
    data.publish(
        topic=f"$aws/things/{thing}/shadow/update",
        qos=0,
        payload=json.dumps({"state": {"reported": {"online": True}}}).encode(),
    )
    data.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"desired": {"interval": 30}}}).encode(),
    )
    shadow = json.loads(data.get_thing_shadow(thingName=thing)["payload"].read())
    if shadow["state"]["desired"]["interval"] != 30:
        die(f"get_thing_shadow = {shadow}")

    # aws iot update-certificate --new-status REVOKED
    iot.update_certificate(certificateId=cert_id, newStatus="REVOKED")
    got = iot.describe_certificate(certificateId=cert_id)
    status = got["certificateDescription"]["status"]
    if status != "REVOKED":
        die(f"吊销后状态 = {status}")

    ack, err = mqtt_connect(mqtt_host, mqtt_port, ca, cert_path, key_path, thing)
    if ack == b"\x20\x02\x00\x00":
        die("吊销后仍然连上了")
    print("boto3 flow ok")


if __name__ == "__main__":
    main()
