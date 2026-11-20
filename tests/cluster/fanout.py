"""验证 Admin 扇出：两台设备分别连两个节点，从第三个节点列举要看到两条。

只看本节点的话，第三个节点上的列表是空的——而控制台不会显示
「这份列表不完整」，用户无从知道自己漏看了什么（契约 12、ARCH-041）。
"""

import json
import os
import ssl
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fencing import connect  # noqa: E402  复用那边的最小 MQTT 客户端


def admin_get(port, token, path):
    out = subprocess.run(
        ["curl", "-sS", "--fail-with-body", "-H", "Authorization: Bearer " + token,
         "http://127.0.0.1:{}{}".format(port, path)],
        capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise SystemExit("列举失败: " + out.stderr + out.stdout)
    return json.loads(out.stdout)


def main():
    host, port_a, port_b, admin_c, token_c, ca_file, work = sys.argv[1:8]

    conns = []
    for thing, port in (("fan-a", port_a), ("fan-b", port_b)):
        conns.append(connect(host, port, ca_file,
                             "{}/{}/{}.pem".format(work, thing, thing),
                             "{}/{}/{}.key".format(work, thing, thing),
                             thing))
        print("{} 已连到 {}".format(thing, port))

    try:
        # 给归属通告与连接登记一点时间
        time.sleep(1.0)
        body = admin_get(admin_c, token_c, "/api/admin/v1/connections?limit=50")
        ids = sorted(c["clientId"] for c in body.get("connections", []))
        if body.get("partial"):
            raise SystemExit("扇出报了部分失败: {}".format(body.get("unavailableNodes")))
        for want in ("fan-a", "fan-b"):
            if want not in ids:
                raise SystemExit(
                    "从 c 上列举没看到 {}（扇出没生效）：{}".format(want, ids))
        print("从 c 上列举到:", ids)

        # 按前缀筛选也要跨节点生效
        body = admin_get(admin_c, token_c,
                         "/api/admin/v1/connections?prefix=fan-a&limit=50")
        ids = sorted(c["clientId"] for c in body.get("connections", []))
        if ids != ["fan-a"]:
            raise SystemExit("前缀筛选跨节点不对: {}".format(ids))
        print("前缀筛选跨节点生效")
    finally:
        for c in conns:
            try:
                c.close()
            except (ssl.SSLError, OSError):
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
