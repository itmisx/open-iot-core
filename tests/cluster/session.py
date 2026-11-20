"""验证持久会话与离线队列跨节点（CLU-111 至 CLU-117）。

这是 v0.3 之前最大的一处"看起来是集群、其实不是"：会话与离线队列只落在
接入它的那个节点的盘上。设备连到另一个节点就是一个新会话（订阅没了），
而节点死掉时它的离线队列跟着没。

判据分两段，中间隔着一次节点切换：

  prime   设备连到节点 a，以 clean session = 0 订阅，然后干净断开。
          接着从节点 b 的管理面发一条 QoS 1 —— 它经兴趣通告转给 a，
          由 a 入队（那次入队现在走会话分片的复制日志）。
  verify  设备连到**节点 c**。c 从来没见过这个设备，也没有它的订阅。
          CONNACK 必须报 session present = 1，并且必须收到那条排队的
          消息。两者都只能来自复制。

分两次调用而不是一个脚本跑完，是为了让 run.sh 能在中间插进"杀掉 a"
——离线队列的真正判据是"入队的那个节点死了之后消息还在"，而那一步
只有编排层能做。

只用标准库（手写最小 MQTT：CONNECT / SUBSCRIBE / PUBLISH / PUBACK /
DISCONNECT），理由同 fencing.py：不把 pip 依赖拖进这套验收。
"""

import base64
import os
import socket
import ssl
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from crossnode import drain_until_ready, send, ws_connect  # noqa: E402


def encode_remaining(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def str_field(s):
    b = s.encode()
    return len(b).to_bytes(2, "big") + b


def connect_packet(client_id, clean, keepalive=30, expiry=3600):
    """MQTT 3.1.1 的 CONNECT。

    clean=False 时 clean session 标志为 0，会话按 expiry 留存。3.1.1 没有
    会话过期属性，节点按配置的默认值处理——本项目的默认值足够长，用例
    的几秒钟不会撞上过期。
    """
    body = bytearray()
    body += str_field("MQTT")
    body.append(4)  # protocol level 4 = 3.1.1
    body.append(0x02 if clean else 0x00)
    body += keepalive.to_bytes(2, "big")
    body += str_field(client_id)
    return bytes([0x10]) + encode_remaining(len(body)) + bytes(body)


def subscribe_packet(topic_filter, qos=1, pid=1):
    body = bytearray()
    body += pid.to_bytes(2, "big")
    body += str_field(topic_filter)
    body.append(qos)
    return bytes([0x82]) + encode_remaining(len(body)) + bytes(body)


def read_packet(s, deadline):
    """读一个完整报文，返回 (首字节, 载荷)。超时返回 (None, b"")。"""
    buf = b""
    while len(buf) < 1:
        s.settimeout(max(0.2, deadline - time.time()))
        try:
            chunk = s.recv(1)
        except (socket.timeout, ssl.SSLWantReadError):
            if time.time() >= deadline:
                return None, b""
            continue
        if not chunk:
            return None, b""
        buf += chunk
    first = buf[0]
    # 剩余长度是变长编码，必须逐字节读——按固定 1 字节读在载荷超过
    # 127 字节时会把长度的第二个字节当成报文内容。
    n = 0
    mult = 1
    while True:
        s.settimeout(max(0.2, deadline - time.time()))
        b = s.recv(1)
        if not b:
            return None, b""
        n += (b[0] & 0x7F) * mult
        if not b[0] & 0x80:
            break
        mult *= 128
    body = b""
    while len(body) < n:
        s.settimeout(max(0.2, deadline - time.time()))
        chunk = s.recv(n - len(body))
        if not chunk:
            return None, b""
        body += chunk
    return first, body


def device_connect(host, port, ca, cert, key, client_id, clean):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca)
    ctx.load_cert_chain(cert, key)
    # 这里验的不是服务端证书的主机名（SAN 不一定含 127.0.0.1）。
    ctx.check_hostname = False
    raw = socket.create_connection((host, int(port)), timeout=10)
    s = ctx.wrap_socket(raw, server_hostname="localhost")
    s.sendall(connect_packet(client_id, clean))
    first, body = read_packet(s, time.time() + 10)
    if first != 0x20 or len(body) < 2:
        raise SystemExit("没有收到 CONNACK: {!r} {!r}".format(first, body))
    if body[1] != 0:
        raise SystemExit("CONNECT 被拒，原因码 {}".format(body[1]))
    return s, bool(body[0] & 0x01)


def prime(argv):
    host, port_a, ca, cert, key, client_id, topic = argv[:7]
    admin_port, admin_token, gossip_wait = argv[7:10]

    s, present = device_connect(host, port_a, ca, cert, key, client_id, clean=False)
    if present:
        # 第一次连接不该报会话已存在。报了说明上一轮的会话没清掉，
        # 后面的判据就失去意义（"收到了消息"可能是上一轮的）。
        raise SystemExit("第一次连接就报 session present——上一轮的会话没清掉")
    s.sendall(subscribe_packet(topic, qos=1))
    first, body = read_packet(s, time.time() + 10)
    if first != 0x90:
        raise SystemExit("没有收到 SUBACK: {!r}".format(first))
    if body[-1] > 2:
        raise SystemExit("订阅被拒，返回码 {}".format(body[-1]))
    print("设备已在节点 a 上建立持久会话并订阅 {}".format(topic))

    # 干净断开（DISCONNECT），不是掉线：持久会话应当留下，遗嘱不发。
    s.sendall(bytes([0xE0, 0x00]))
    s.close()

    # 等兴趣通告传到节点 b：发布端要知道 a 对这个过滤器有兴趣，
    # 才会把帧转过去。通告是周期性全量快照（CLU-075），没有回执可等。
    time.sleep(float(gossip_wait))

    pub = ws_connect(admin_port, admin_token)
    if not drain_until_ready(pub, time.time() + 10):
        raise SystemExit("发布端没有收到 ready")
    payload = "queued-{}".format(int(time.time() * 1000))
    send(pub, {
        "op": "publish",
        "topic": topic.replace("#", "cmd"),
        "payload": base64.b64encode(payload.encode()).decode(),
        "qos": 1,
    })
    # 读一下回执：管理面的测试客户端对被拒的发布会回 op=error，而
    # 不读它的话这条用例的失败现象是"c 上收不到"——那会把人引到复制
    # 层去查，而真因在发布这一步就已经发生了。
    from crossnode import recv  # 局部导入：只有这一处要它
    deadline = time.time() + 2.0
    while time.time() < deadline:
        msg = recv(pub, deadline)
        if msg is None:
            break
        if msg.get("op") == "error":
            raise SystemExit("发布被拒: {}".format(msg))
        if msg.get("op") in ("puback", "published"):
            break
    print("已从节点 b 发出一条 QoS 1（载荷 {}）".format(payload))
    # 载荷交给编排层，verify 那一段要用它判定收到的是这一条。
    with open(argv[10], "w") as f:
        f.write(payload)
    return 0


def verify(argv):
    host, port_c, ca, cert, key, client_id, payload_file = argv[:7]
    with open(payload_file) as f:
        want = f.read().strip()

    s, present = device_connect(host, port_c, ca, cert, key, client_id, clean=False)
    if not present:
        raise SystemExit(
            "节点 c 报 session present = 0：会话没有跨节点，"
            "设备在这里得到的是一个新会话（订阅全丢）")
    print("节点 c 报 session present = 1（会话经复制日志到达）")

    deadline = time.time() + 15
    while time.time() < deadline:
        first, body = read_packet(s, deadline)
        if first is None:
            break
        if first & 0xF0 != 0x30:
            continue
        tlen = int.from_bytes(body[0:2], "big")
        rest = body[2 + tlen:]
        qos = (first >> 1) & 0x03
        if qos > 0:
            pid = rest[0:2]
            rest = rest[2:]
            s.sendall(bytes([0x40, 0x02]) + pid)
        got = rest.decode(errors="replace")
        if got == want:
            print("节点 c 上补发了排队的消息 {}".format(got))
            s.close()
            return 0
        print("收到的是 {}，不是期待的 {}，继续等".format(got, want))
    raise SystemExit(
        "节点 c 没有补发那条排队的消息：离线队列仍钉在入队的那个节点上")


def main():
    if len(sys.argv) < 2:
        raise SystemExit("用法: session.py prime|verify ...")
    mode = sys.argv[1]
    if mode == "prime":
        return prime(sys.argv[2:])
    if mode == "verify":
        return verify(sys.argv[2:])
    raise SystemExit("未知的阶段 {}".format(mode))


if __name__ == "__main__":
    sys.exit(main())
