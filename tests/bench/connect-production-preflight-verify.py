#!/usr/bin/env python3
"""Verify benchmark certificate registration and pre-provisioned Thing pages."""

import json
import pathlib
import sys


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    if len(sys.argv) < 6:
        fail(
            "usage: connect-production-preflight-verify.py CERT_JSON "
            "EXPECTED_FINGERPRINT PREFIX CLIENTS THING_PAGE..."
        )
    cert_path = pathlib.Path(sys.argv[1])
    expected_fingerprint = sys.argv[2].lower()
    prefix = sys.argv[3]
    clients = int(sys.argv[4])
    pages = [pathlib.Path(value) for value in sys.argv[5:]]

    cert = json.loads(cert_path.read_text(encoding="utf-8"))
    if cert.get("fingerprint", "").lower() != expected_fingerprint:
        fail(
            f"压测证书指纹不匹配: {cert.get('fingerprint')} != "
            f"{expected_fingerprint}"
        )
    if cert.get("status") != "ACTIVE":
        fail(f"压测证书状态不是ACTIVE: {cert.get('status')}")
    if not cert.get("policyName"):
        fail("压测证书没有绑定策略；无法授权PREFIX-*的clientId")

    seen = 0
    for page_index, path in enumerate(pages):
        data = json.loads(path.read_text(encoding="utf-8"))
        things = data.get("things")
        if not isinstance(things, list):
            fail(f"{path.name}缺少things数组")
        for row in things:
            expected = f"{prefix}-c{seen:06d}"
            if seen >= clients or row.get("name") != expected:
                fail(
                    f"预置设备不连续: index={seen} got={row.get('name')} "
                    f"expected={expected}"
                )
            if not row.get("policyName"):
                fail(f"预置设备{expected}没有策略")
            seen += 1
        next_after = data.get("nextAfter", "")
        if page_index < len(pages) - 1:
            if not things or next_after != things[-1].get("name"):
                fail(f"{path.name}分页游标未指向本页末项: {next_after}")
        elif next_after:
            fail(f"最后一页仍声明nextAfter={next_after}，证据不完整")

    if seen != clients:
        fail(f"预置设备数={seen}，期望{clients}")
    print(
        json.dumps(
            {
                "passed": True,
                "certificateFingerprint": expected_fingerprint,
                "certificatePolicy": cert["policyName"],
                "prefix": prefix,
                "preprovisionedThings": seen,
                "pages": len(pages),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
