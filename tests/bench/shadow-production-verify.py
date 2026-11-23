#!/usr/bin/env python3
"""Verify artifacts emitted by shadow-production-linux.sh."""

import json
import math
import pathlib
import re
import sys


def samples(path: pathlib.Path, name: str) -> float:
    total = 0.0
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            try:
                total += float(line.rsplit(None, 1)[1])
            except (ValueError, IndexError):
                pass
    return total


def main() -> None:
    if len(sys.argv) != 11:
        raise SystemExit(
            "usage: shadow-production-verify.py DIR NODES SHARDS RF MIN_RATE MAX_P99 MAX_LATENCY MAX_GAP MIN_MERGE MIN_SECONDS"
        )
    root = pathlib.Path(sys.argv[1])
    nodes, shards, rf = map(int, sys.argv[2:5])
    min_rate, max_p99, max_latency, max_gap, min_merge = map(float, sys.argv[5:10])
    min_seconds = float(sys.argv[10])
    lines = [x for x in (root / "result.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    if not lines:
        raise SystemExit("result.jsonl为空")
    result = json.loads(lines[-1])
    published = int(result.get("published", 0))
    accepted = int(result.get("delivered", 0))
    errors = int(result.get("errors", 0))
    rate = float(result.get("publishPerSec", 0))
    p99 = float(result.get("latencyP99Ms", math.inf))
    max_observed = float(result.get("latencyMaxMs", math.inf))
    accepted_gap = float(result.get("shadowMaxAcceptedGapMs", math.inf))
    seconds = float(result.get("seconds", 0))
    if errors:
        raise SystemExit(f"errors={errors}, 要求0")
    if published <= 0 or published != accepted:
        raise SystemExit(f"published/accepted={published}/{accepted}, 要求逐条相等且非零")
    if rate < min_rate:
        raise SystemExit(f"rate={rate:.1f}/s < {min_rate:.1f}/s")
    if p99 > max_p99:
        raise SystemExit(f"p99={p99:.2f}ms > {max_p99:.2f}ms")
    if max_observed > max_latency:
        raise SystemExit(f"max latency={max_observed:.2f}ms > {max_latency:.2f}ms")
    if accepted_gap > max_gap:
        raise SystemExit(f"max accepted gap={accepted_gap:.2f}ms > {max_gap:.2f}ms")
    if seconds < min_seconds:
        raise SystemExit(f"measurement seconds={seconds:.2f} < {min_seconds:.2f}")

    names = {
        "logical": "openiot_shadow_leader_merge_logical_total",
        "physical": "openiot_shadow_leader_merge_physical_total",
        "proposal_errors": "openiot_shadow_proposal_errors_total",
    }
    delta = {key: 0.0 for key in names}
    leader_counts: dict[int, int] = {}
    replica_counts: dict[int, int] = {}
    for i in range(nodes):
        before = root / f"metrics.before.{i}.prom"
        after = root / f"metrics.after.{i}.prom"
        for key, name in names.items():
            delta[key] += samples(after, name) - samples(before, name)
        for line in after.read_text(encoding="utf-8").splitlines():
            match = re.search(r'shard="([0-9]+)"', line)
            if not match:
                continue
            shard = int(match.group(1))
            if line.startswith("openiot_shadow_shard_ready{"):
                replica_counts[shard] = replica_counts.get(shard, 0) + 1
            if line.startswith("openiot_shadow_shard_is_leader{") and line.endswith(" 1"):
                leader_counts[shard] = leader_counts.get(shard, 0) + 1

    if delta["proposal_errors"] != 0:
        raise SystemExit(f"shadow proposal errors delta={delta['proposal_errors']}")
    if delta["physical"] <= 0:
        raise SystemExit("没有观测到物理Raft提案")
    ratio = delta["logical"] / delta["physical"]
    if ratio < min_merge:
        raise SystemExit(f"leader merge ratio={ratio:.2f} < {min_merge:.2f}")
    if len(leader_counts) != shards or any(value != 1 for value in leader_counts.values()):
        raise SystemExit(f"shadow shard leader不完整或不唯一: {leader_counts}")
    if len(replica_counts) != shards or any(value != rf for value in replica_counts.values()):
        raise SystemExit(f"shadow replica布局不是{shards}分片/RF={rf}: {replica_counts}")

    summary = {
        "passed": True,
        "nodes": nodes,
        "shadowShards": shards,
        "shadowRF": rf,
        "published": published,
        "accepted": accepted,
        "updatesPerSec": rate,
        "p99Ms": p99,
        "maxLatencyMs": max_observed,
        "maxAcceptedGapMs": accepted_gap,
        "seconds": seconds,
        "shadowMergeLogical": int(delta["logical"]),
        "shadowMergePhysical": int(delta["physical"]),
        "shadowMergeLogicalPerPhysical": ratio,
        "proposalErrors": int(delta["proposal_errors"]),
        "leaderShards": sorted(leader_counts),
        "replicasPerShard": replica_counts,
    }
    (root / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
