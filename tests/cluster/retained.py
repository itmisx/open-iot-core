"""在一个节点上发布（或清除）一条保留消息。

读取那一头不在这里：验收用兼容面的 GetRetainedMessage 去问**另一个**
节点"你自己有没有这条记录"（见 run.sh）。判据刻意不取"订阅时收到了"
——那分不清"复制过去了"与"从发布节点转发过来了"，而这条验收要证明的
正是前者（TST-156）。

复用 crossnode.py 的 WebSocket 机器：同一个目录，python3 会把脚本所在
目录放在 sys.path[0]。

用法：retained.py <port> <token> <topic> [payload]
      省略 payload（或传空串）即清除该主题的保留消息（MQTT-124 的墓碑）。
"""

import base64
import sys
import time

from crossnode import drain_until_ready, recv, send, ws_connect


def main():
    port, token, topic = sys.argv[1:4]
    payload = sys.argv[4] if len(sys.argv) > 4 else ""

    pub = ws_connect(port, token)
    if not drain_until_ready(pub, time.time() + 10):
        raise SystemExit("发布端没有收到 ready")
    send(pub, {
        "op": "publish",
        "topic": topic,
        "payload": base64.b64encode(payload.encode()).decode(),
        "qos": 0,
        "retain": True,
    })
    # 等一条 published 回执再退出。不等的话进程可能在服务端读到那一帧
    # 之前就关掉连接，而失败的样子是"复制没生效"——与复制毫无关系。
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = recv(pub, deadline)
        if msg is None:
            break
        if msg.get("op") == "published":
            what = "清除" if payload == "" else "发布"
            print("已在端口 {} 上{} retained：{}".format(port, what, topic))
            return 0
        if msg.get("op") == "error":
            raise SystemExit("发布被拒: " + str(msg))
    raise SystemExit("没有收到 published 回执")


if __name__ == "__main__":
    sys.exit(main())
