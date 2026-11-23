#!/usr/bin/env python3
"""Verify that a stopped Shadow replica rejoined and caught up safely."""

import json
import pathlib
import re
import sys


def fail(message: str) -> None:
    raise SystemExit(message)


def metric(text: str, name: str, shard: int) -> int:
    pattern = re.compile(
        rf'^{re.escape(name)}\{{[^}}]*shard="{shard}"[^}}]*\}}\s+([0-9.eE+-]+)$',
        re.MULTILINE,
    )
    values = [int(float(value)) for value in pattern.findall(text)]
    if len(values) != 1:
        fail(f"恢复证据中{name}的shard={shard}样本数不是1: {values}")
    return values[0]


def main() -> None:
    if len(sys.argv) != 8:
        fail(
            "usage: shadow-recovery-verify.py HEALTH METRICS SHADOW "
            "NODE_ID CLUSTER_ID SHARD TARGET_APPLIED"
        )
    health_path, metrics_path, shadow_path = map(pathlib.Path, sys.argv[1:4])
    node_id, cluster_id = sys.argv[4:6]
    shard, target = int(sys.argv[6]), int(sys.argv[7])
    try:
        health = json.loads(health_path.read_text(encoding="utf-8"))
        metrics = metrics_path.read_text(encoding="utf-8")
        shadow = json.loads(shadow_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"无法读取恢复证据: {exc}")

    if health.get("node_id") != node_id or health.get("cluster_id") != cluster_id:
        fail(f"恢复节点身份不匹配: {health}")
    ready = metric(metrics, "openiot_shadow_shard_ready", shard)
    applied = metric(metrics, "openiot_shadow_shard_applied_index", shard)
    if ready != 1:
        fail(f"恢复节点的shard={shard}尚未ready")
    if applied < target:
        fail(f"恢复副本尚未追平: {applied} < {target}")
    if shadow.get("version") != 2:
        fail(f"恢复节点没有读到version 2: {shadow}")
    reported = shadow.get("state", {}).get("reported", {})
    if reported.get("failover") != 2:
        fail(f"恢复节点没有读到故障后状态: {shadow}")

    print(
        json.dumps(
            {
                "passed": True,
                "nodeId": node_id,
                "clusterId": cluster_id,
                "shard": shard,
                "targetAppliedIndex": target,
                "restoredAppliedIndex": applied,
                "version": 2,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
