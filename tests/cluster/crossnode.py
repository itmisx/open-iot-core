"""验证一条消息能从一个节点投到另一个节点的订阅者。

用控制台的 MQTT 测试客户端（管理面 WebSocket）而不是真实设备：它的订阅
走的就是 Router.Subscribe（见 broker 的 Tap.Subscribe），投递路径与设备
完全相同，而这里不必再分发一次设备证书。

只用标准库：这套验收已经要 openssl 了，不想再把 pip 依赖拖进来。
"""

import base64
import json
import os
import socket
import sys
import time


class Conn:
    """已升级的 WebSocket 连接。

    自带缓冲：握手时的那次 recv 很可能把服务端紧跟其后发来的第一个帧
    （op=ready）一起读进来了。第一版把 \r\n\r\n 之后的字节丢掉，
    于是"收不到 ready"——而真正的原因与集群毫无关系。
    """

    def __init__(self, sock, pending=b""):
        self.sock = sock
        self.buf = bytearray(pending)

    def read(self, n, deadline):
        while len(self.buf) < n:
            remain = deadline - time.time()
            if remain <= 0:
                raise socket.timeout()
            self.sock.settimeout(remain)
            chunk = self.sock.recv(max(4096, n - len(self.buf)))
            if not chunk:
                raise SystemExit("连接被对端关闭")
            self.buf += chunk
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def sendall(self, data):
        self.sock.sendall(data)


def ws_connect(port, token):
    """握一次 WebSocket，返回已升级的连接。"""
    s = socket.create_connection(("127.0.0.1", int(port)), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET /api/admin/v1/mqtt?token={} HTTP/1.1\r\n"
        "Host: 127.0.0.1:{}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: {}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    ).format(token, port, key)
    s.sendall(req.encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            raise SystemExit("WebSocket 握手没有应答")
        buf += chunk
    status = buf.split(b"\r\n")[0].decode(errors="replace")
    if "101" not in status:
        raise SystemExit("WebSocket 握手失败: " + status)
    # 响应头之后的字节属于 WebSocket 帧，交给连接的缓冲
    return Conn(s, buf.split(b"\r\n\r\n", 1)[1])


def send(s, obj):
    """发一个掩码过的文本帧。客户端发往服务端的帧必须掩码（RFC 6455）。"""
    data = json.dumps(obj).encode()
    hdr = bytearray([0x81])
    n = len(data)
    if n < 126:
        hdr.append(0x80 | n)
    elif n < (1 << 16):
        hdr.append(0x80 | 126)
        hdr += n.to_bytes(2, "big")
    else:
        hdr.append(0x80 | 127)
        hdr += n.to_bytes(8, "big")
    mask = os.urandom(4)
    hdr += mask
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    s.sendall(bytes(hdr) + masked)


def recv(c, deadline):
    """读一个文本帧并解析成 JSON；超时返回 None。"""
    try:
        hdr = c.read(2, deadline)
        n = hdr[1] & 0x7F
        if n == 126:
            n = int.from_bytes(c.read(2, deadline), "big")
        elif n == 127:
            n = int.from_bytes(c.read(8, deadline), "big")
        body = c.read(n, deadline) if n else b""
    except socket.timeout:
        return None
    if hdr[0] & 0x0F != 1:
        return {}
    return json.loads(body)


def drain_until_ready(s, deadline):
    """服务端连上后先发一条 op=ready。"""
    while time.time() < deadline:
        msg = recv(s, deadline)
        if msg is None:
            return False
        if msg.get("op") == "ready":
            return True
    return False


def main():
    sub_port, sub_token, pub_port, pub_token = sys.argv[1:5]
    gossip_wait = float(sys.argv[5]) if len(sys.argv) > 5 else 3.0
    # 第六个参数是订阅用的过滤器。默认是普通的通配符订阅；传
    # $share/<组>/<过滤器> 就验共享订阅组的跨节点选取（MQTT-116）：
    # 订阅者只在订阅端那个节点上，而发布发生在另一个节点，
    # 发布端必须把这一帧转给订阅端而不是当成"组里没人"丢掉。
    sub_filter = sys.argv[6] if len(sys.argv) > 6 else "telemetry/#"

    sub = ws_connect(sub_port, sub_token)
    if not drain_until_ready(sub, time.time() + 10):
        raise SystemExit("订阅端没有收到 ready")
    send(sub, {"op": "subscribe", "filter": sub_filter, "qos": 0})
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = recv(sub, deadline)
        if msg is None:
            raise SystemExit("订阅没有得到应答")
        if msg.get("op") == "suback":
            break
        if msg.get("op") == "error":
            raise SystemExit("订阅被拒: " + str(msg))
    else:
        raise SystemExit("订阅没有得到 suback")

    # 等兴趣通告传到发布端那个节点。
    #
    # 通告是周期性的全量快照（CLU-075），所以这里只能等——没有"通告
    # 已送达"的回执。等不够的话，发布端算出的远端集合是空的，消息
    # 只投给它自己的本地订阅者，而这条测试会说"没收到"。
    time.sleep(gossip_wait)

    pub = ws_connect(pub_port, pub_token)
    if not drain_until_ready(pub, time.time() + 10):
        raise SystemExit("发布端没有收到 ready")
    payload = "cross-node-{}".format(int(time.time() * 1000))
    send(pub, {
        "op": "publish",
        "topic": "telemetry/site-1/temp",
        "payload": base64.b64encode(payload.encode()).decode(),
        "qos": 0,
    })

    deadline = time.time() + 15
    while time.time() < deadline:
        msg = recv(sub, deadline)
        if msg is None:
            break
        if msg.get("op") != "message":
            continue
        got = base64.b64decode(msg.get("payload", "")).decode(errors="replace")
        if got == payload:
            print("订阅者在另一个节点上收到了 {}（主题 {}）".format(got, msg.get("topic")))
            return 0
    raise SystemExit("订阅者没有收到跨节点消息（兴趣通告或 mesh 转发没生效）")


if __name__ == "__main__":
    sys.exit(main())
