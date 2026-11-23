#!/usr/bin/env python3
"""Verify the loaded failover result from a production shadow run."""

import json
import pathlib
import sys


def main() -> None:
    if len(sys.argv) != 7:
        raise SystemExit(
            "usage: shadow-failover-verify.py BENCH_JSON RECOVERY_MS RECOVERED_UNIX_MS "
            "MAX_RECOVERY_MS MAX_GAP_MS MIN_SUCCESS_RATE"
        )
    path = pathlib.Path(sys.argv[1])
    recovery_ms = int(sys.argv[2])
    recovered_unix_ms = int(sys.argv[3])
    max_recovery_ms = int(sys.argv[4])
    max_gap_ms = float(sys.argv[5])
    min_success_rate = float(sys.argv[6])
    data = json.loads(path.read_text(encoding="utf-8"))
    published = int(data.get("published", 0))
    accepted = int(data.get("delivered", 0))
    rejected = int(data.get("shadowRejected", 0))
    timeouts = int(data.get("shadowTimeouts", 0))
    errors = int(data.get("errors", 0))
    gap = float(data.get("shadowMaxAcceptedGapMs", 0))
    last_accepted = int(data.get("shadowLastAcceptedUnixMs", 0))
    if published <= 0 or accepted <= 0:
        raise SystemExit("故障负载没有有效published/accepted")
    if published != accepted + rejected + timeouts:
        raise SystemExit(
            f"终态不守恒: {published}!={accepted}+{rejected}+{timeouts}"
        )
    if errors != rejected + timeouts:
        raise SystemExit(
            f"错误归因不完整: errors={errors}, rejected={rejected}, timeout={timeouts}"
        )
    if last_accepted <= recovered_unix_ms:
        raise SystemExit("恢复写入之后没有新的accepted")
    if recovery_ms > max_recovery_ms:
        raise SystemExit(f"recovery={recovery_ms}ms > {max_recovery_ms}ms")
    if gap > max_gap_ms:
        raise SystemExit(f"accepted max gap={gap:.3f}ms > {max_gap_ms:.3f}ms")
    success_rate = accepted / published
    if success_rate < min_success_rate:
        raise SystemExit(
            f"success rate={success_rate:.6f} < {min_success_rate:.6f}"
        )
    summary = {
        "passed": True,
        "recoveryMs": recovery_ms,
        "published": published,
        "accepted": accepted,
        "rejected": rejected,
        "timeouts": timeouts,
        "errors": errors,
        "successRate": success_rate,
        "maxAcceptedGapMs": gap,
        "lastAcceptedUnixMs": last_accepted,
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
