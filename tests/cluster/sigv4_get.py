#!/usr/bin/env python3
"""对 /api/aws 发一次签过名的 GET，打印 HTTP 状态码。

只用标准库：集群验收脚本不该为了一次签名去装 boto3。签名算法本身
在 internal/sigv4 已有大量单测，这里要验的是**另一件事**——在 a 上发的
access key，打到 b 上能不能用（AUTH-086 的全部意义）。

用法：sigv4_get.py <url> <access-key-id> <secret> [region] [service]

service 省略为 iot（控制面）。数据面的路由按 iotdata 验签
（internal/sigv4 的 ServiceIoTData），服务名不对一律 401——而 401 的
字面含义是"密钥不对或时钟偏了"，不会指向服务名，所以这里要能传。
"""

import datetime
import hashlib
import hmac
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit

ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "iot"


def sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def main() -> int:
    if len(sys.argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    url, key_id, secret = sys.argv[1], sys.argv[2], sys.argv[3]
    region = sys.argv[4] if len(sys.argv) > 4 else "us-east-1"
    service = sys.argv[5] if len(sys.argv) > 5 else SERVICE

    parts = urlsplit(url)
    host = parts.netloc
    path = parts.path or "/"
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()

    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = "\n".join([
        "GET",
        path,
        parts.query,
        f"host:{host}",
        f"x-amz-content-sha256:{payload_hash}",
        f"x-amz-date:{amz_date}",
        "",
        signed_headers,
        payload_hash,
    ])
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join([
        ALGORITHM,
        amz_date,
        scope,
        hashlib.sha256(canonical.encode()).hexdigest(),
    ])
    k = sign(("AWS4" + secret).encode(), day)
    k = sign(k, region)
    k = sign(k, service)
    k = sign(k, "aws4_request")
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()

    req = urllib.request.Request(url, method="GET")
    req.add_header("X-Amz-Date", amz_date)
    req.add_header("X-Amz-Content-Sha256", payload_hash)
    req.add_header(
        "Authorization",
        f"{ALGORITHM} Credential={key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(resp.status)
    except urllib.error.HTTPError as e:
        # 状态码本身就是结果，错误体打到 stderr 供排查。
        print(e.code)
        print(e.read().decode(errors="replace"), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
