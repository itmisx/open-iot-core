"""验证同一个 clientId 连到第二个节点时，第一个节点上的连接被断开。

用手写的最小 MQTT 客户端（只 CONNECT / CONNACK / PINGREQ），不引 paho：
这套验收已经要 openssl 了，不想再把 pip 依赖拖进来，而要验的东西只需要
「连上」和「被断开」两件事。

互斥是最终一致的（CLU-081）：窗口是一次 mesh 往返。所以这里给第一个
连接留了几秒去感知断开，而不是要求它立刻断。
"""

import socket
import ssl
import sys
import time


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


def mqtt_connect_packet(client_id, keepalive=10):
    """MQTT 3.1.1 的 CONNECT，clean session。"""
    payload = client_id.encode()
    body = bytearray()
    body += len(b"MQTT").to_bytes(2, "big") + b"MQTT"
    body.append(4)  # protocol level 4 = 3.1.1
    body.append(0x02)  # clean session
    body += keepalive.to_bytes(2, "big")
    body += len(payload).to_bytes(2, "big") + payload
    return bytes([0x10]) + encode_remaining(len(body)) + bytes(body)


def connect(host, port, ca_file, cert_file, key_file, client_id):
    """建 mTLS 连接并完成 MQTT CONNECT，返回套接字。"""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca_file)
    ctx.load_cert_chain(cert_file, key_file)
    # 证书的 SAN 不一定含 127.0.0.1，而这里验的不是服务端证书的主机名
    ctx.check_hostname = False
    raw = socket.create_connection((host, int(port)), timeout=10)
    s = ctx.wrap_socket(raw, server_hostname="localhost")
    s.sendall(mqtt_connect_packet(client_id))
    s.settimeout(10)
    hdr = s.recv(4)
    if len(hdr) < 4 or hdr[0] != 0x20:
        raise SystemExit("没有收到 CONNACK: {!r}".format(hdr))
    if hdr[3] != 0:
        raise SystemExit("CONNECT 被拒，原因码 {}".format(hdr[3]))
    return s


def is_closed(s, timeout):
    """等到连接被对端关掉；超时返回 False。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        s.settimeout(max(0.2, min(1.0, deadline - time.time())))
        try:
            data = s.recv(1024)
        except socket.timeout:
            continue
        except (ssl.SSLError, OSError):
            return True
        if not data:
            return True
        # 收到了别的报文（例如 DISCONNECT 0x8E），继续读直到真的关闭
    return False


def main():
    (host, port_a, port_b, ca_file, cert_file, key_file, client_id) = sys.argv[1:8]

    first = connect(host, port_a, ca_file, cert_file, key_file, client_id)
    print("已连到节点 a")

    # 让归属通告传开一轮，使这次连接在集群里是已知的
    time.sleep(1.0)

    second = connect(host, port_b, ca_file, cert_file, key_file, client_id)
    print("同一个 clientId 又连到了节点 b")

    if not is_closed(first, 10):
        raise SystemExit("节点 a 上的旧连接没有被断开——同一 clientId 在两个节点上双活")
    print("节点 a 上的旧连接已被断开（接管通知生效）")

    # 新连接必须还活着：踢错方向比不踢更坏
    try:
        second.sendall(bytes([0xC0, 0x00]))  # PINGREQ
        second.settimeout(5)
        resp = second.recv(2)
    except (ssl.SSLError, OSError) as exc:
        raise SystemExit("节点 b 上的新连接也被断开了: {}".format(exc))
    if len(resp) < 2 or resp[0] != 0xD0:
        raise SystemExit("节点 b 上的新连接没有回 PINGRESP: {!r}".format(resp))
    print("节点 b 上的新连接仍然健康")
    second.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
