#!/usr/bin/env python3
"""Verify a five-host persistent CONNECT production-gate artifact."""

import json
import math
import pathlib
import re
import sys


def samples(path: pathlib.Path, name: str, label: str = "") -> float:
    total = 0.0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not (line.startswith(name + " ") or line.startswith(name + "{")):
            continue
        if label and label not in line:
            continue
        try:
            total += float(line.rsplit(None, 1)[1])
        except (ValueError, IndexError):
            pass
    return total


def one_sample(path: pathlib.Path, name: str) -> float:
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            values.append(float(line.rsplit(None, 1)[1]))
    if len(values) != 1:
        raise SystemExit(f"{path.name}: {name}样本数={len(values)}，期望1")
    return values[0]


def main() -> None:
    if len(sys.argv) != 9:
        raise SystemExit(
            "usage: connect-production-verify.py DIR NODES SHARDS RF CLIENTS "
            "MIN_RATE MAX_P99_MS MAX_CONNECT_MS"
        )
    root = pathlib.Path(sys.argv[1])
    nodes, shards, rf, clients = map(int, sys.argv[2:6])
    min_rate, max_p99, max_connect = map(float, sys.argv[6:9])

    lines = [
        value
        for value in (root / "result.jsonl").read_text(encoding="utf-8").splitlines()
        if value.strip()
    ]
    if len(lines) != 1:
        raise SystemExit(f"result.jsonl有效行数={len(lines)}，期望恰好1")
    result = json.loads(lines[0])
    actual_clients = int(result.get("clients", 0))
    held = int(result.get("heldConns", 0))
    errors = int(result.get("errors", 0))
    resumed = int(result.get("tlsResumed", 0))
    rate = float(result.get("connectPerSec", 0))
    p99 = float(result.get("connectP99Ms", math.inf))
    connect_max = float(result.get("connectMaxMs", math.inf))
    if actual_clients != clients:
        raise SystemExit(f"clients={actual_clients}，期望{clients}")
    if errors:
        raise SystemExit(f"errors={errors}，要求0")
    if held != clients or result.get("heldVerified") is not True:
        raise SystemExit(
            f"held={held}/{clients}, heldVerified={result.get('heldVerified')}，"
            "要求全部连接通过末端PING"
        )
    if resumed != clients:
        raise SystemExit(f"TLS resumed={resumed}/{clients}，要求第二波100%恢复")
    if rate < min_rate:
        raise SystemExit(f"CONNECT rate={rate:.1f}/s < {min_rate:.1f}/s")
    if p99 > max_p99:
        raise SystemExit(f"CONNECT p99={p99:.2f}ms > {max_p99:.2f}ms")
    if connect_max > max_connect:
        raise SystemExit(f"CONNECT max={connect_max:.2f}ms > {max_connect:.2f}ms")

    leader_counts: dict[int, int] = {}
    replica_counts: dict[int, int] = {}
    applied_indexes: dict[int, list[int]] = {}
    deltas = {
        "proposalErrors": 0.0,
        "raftProposals": 0.0,
        "accepted": 0.0,
        "tlsFull": 0.0,
        "tlsResumed": 0.0,
        "tlsFailed": 0.0,
        "tlsThrottled": 0.0,
        "safeRerouteLocal": 0.0,
        "safeRerouteRemote": 0.0,
        "remoteReadClaimPiggyback": 0.0,
        "remoteReadErrors": 0.0,
        "ownershipClaimErrors": 0.0,
    }
    counter_specs = {
        "proposalErrors": ("openiot_session_proposal_errors_total", ""),
        "raftProposals": ("openiot_session_raft_proposals_total", ""),
        "accepted": ("openiot_connections_total", 'result="Accepted"'),
        "tlsFull": ("openiot_tls_handshakes_total", 'result="full"'),
        "tlsResumed": ("openiot_tls_handshakes_total", 'result="resumed"'),
        "tlsFailed": ("openiot_tls_handshakes_total", 'result="failed"'),
        "tlsThrottled": ("openiot_tls_handshakes_total", 'result="throttled"'),
        "safeRerouteLocal": (
            "openiot_session_proposal_safe_reroutes_total",
            'source="local"',
        ),
        "safeRerouteRemote": (
            "openiot_session_proposal_safe_reroutes_total",
            'source="remote"',
        ),
        "remoteReadClaimPiggyback": (
            "openiot_session_remote_read_claim_piggyback_total",
            "",
        ),
        "remoteReadErrors": ("openiot_session_remote_read_errors_total", ""),
        "ownershipClaimErrors": ("openiot_ownership_claim_errors_total", ""),
    }
    for i in range(nodes):
        before = root / f"metrics.before.{i}.prom"
        after = root / f"metrics.after.{i}.prom"
        if int(one_sample(after, "openiot_session_shard_count")) != shards:
            raise SystemExit(f"node[{i}] session_shard_count不是{shards}")
        for key, (name, label) in counter_specs.items():
            # 新的安全重路由指标必须存在；缺失说明被测节点不是当前候选版本。
            if name == "openiot_session_proposal_safe_reroutes_total" and not any(
                line.startswith(name + "{") and label in line
                for line in after.read_text(encoding="utf-8").splitlines()
            ):
                raise SystemExit(f"node[{i}]缺少{name} {label}")
            deltas[key] += samples(after, name, label) - samples(before, name, label)

        for line in after.read_text(encoding="utf-8").splitlines():
            match = re.search(r'shard="([0-9]+)"', line)
            if not match:
                continue
            shard = int(match.group(1))
            value = float(line.rsplit(None, 1)[1])
            if line.startswith("openiot_session_shard_ready{") and value == 1:
                replica_counts[shard] = replica_counts.get(shard, 0) + 1
            if line.startswith("openiot_session_shard_is_leader{") and value == 1:
                leader_counts[shard] = leader_counts.get(shard, 0) + 1
            if line.startswith("openiot_session_shard_applied_index{"):
                applied_indexes.setdefault(shard, []).append(int(value))

    if deltas["proposalErrors"] != 0:
        raise SystemExit(f"session proposal errors delta={deltas['proposalErrors']}")
    if deltas["remoteReadErrors"] != 0:
        raise SystemExit(f"session remote read errors delta={deltas['remoteReadErrors']}")
    if deltas["ownershipClaimErrors"] != 0:
        raise SystemExit(f"ownership claim errors delta={deltas['ownershipClaimErrors']}")
    if deltas["raftProposals"] <= 0:
        raise SystemExit("没有观测到持久会话Raft提案")
    if deltas["remoteReadClaimPiggyback"] <= 0:
        raise SystemExit("RF=3跨节点重连没有观测到Claim携带会话快照")
    if deltas["accepted"] != clients * 2:
        raise SystemExit(
            f"服务端Accepted delta={deltas['accepted']:.0f}，期望冷/热两波共{clients * 2}"
        )
    if deltas["tlsFull"] != clients or deltas["tlsResumed"] != clients:
        raise SystemExit(
            f"服务端TLS full/resumed delta={deltas['tlsFull']:.0f}/"
            f"{deltas['tlsResumed']:.0f}，期望{clients}/{clients}"
        )
    if deltas["tlsFailed"] != 0 or deltas["tlsThrottled"] != 0:
        raise SystemExit(
            f"服务端TLS failed/throttled delta={deltas['tlsFailed']:.0f}/"
            f"{deltas['tlsThrottled']:.0f}"
        )
    if len(leader_counts) != shards or any(value != 1 for value in leader_counts.values()):
        raise SystemExit(f"session shard leader不完整或不唯一: {leader_counts}")
    if len(replica_counts) != shards or any(value != rf for value in replica_counts.values()):
        raise SystemExit(
            f"session replica布局不是{shards}分片/RF={rf}: {replica_counts}"
        )
    if (
        len(applied_indexes) != shards
        or any(len(values) != rf for values in applied_indexes.values())
        or any(len(set(values)) != 1 for values in applied_indexes.values())
    ):
        raise SystemExit(f"session副本applied index未收敛: {applied_indexes}")

    summary = {
        "passed": True,
        "nodes": nodes,
        "sessionShards": shards,
        "sessionRF": rf,
        "clients": clients,
        "heldConns": held,
        "heldVerified": True,
        "connectPerSec": rate,
        "connectP99Ms": p99,
        "connectMaxMs": connect_max,
        "tlsResumed": resumed,
        "proposalErrors": int(deltas["proposalErrors"]),
        "raftProposals": int(deltas["raftProposals"]),
        "safeRerouteLocal": int(deltas["safeRerouteLocal"]),
        "safeRerouteRemote": int(deltas["safeRerouteRemote"]),
        "remoteReadClaimPiggyback": int(deltas["remoteReadClaimPiggyback"]),
        "remoteReadErrors": int(deltas["remoteReadErrors"]),
        "ownershipClaimErrors": int(deltas["ownershipClaimErrors"]),
        "leaderShards": sorted(leader_counts),
        "replicasPerShard": replica_counts,
        "appliedIndexPerShard": {
            str(shard): values[0] for shard, values in sorted(applied_indexes.items())
        },
    }
    (root / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
