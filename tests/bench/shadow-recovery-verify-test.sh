#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadow-recovery-verify-test.XXXXXX")
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

printf '%s\n' '{"node_id":"n1","cluster_id":"cluster-a"}' > "$work/health.json"
printf '%s\n' 'openiot_shadow_shard_ready{node="n1",shard="2"} 1' \
  'openiot_shadow_shard_applied_index{node="n1",shard="2"} 120' > "$work/metrics.prom"
printf '%s\n' '{"version":2,"state":{"reported":{"failover":2}}}' > "$work/shadow.json"
python3 tests/bench/shadow-recovery-verify.py "$work/health.json" "$work/metrics.prom" \
  "$work/shadow.json" n1 cluster-a 2 100 >/dev/null

expect_rejected() {
  name=$1 pattern=$2; shift 2
  if python3 tests/bench/shadow-recovery-verify.py "$@" > "$work/$name.out" 2>&1; then
    echo "$name恢复证据应被拒绝" >&2
    exit 1
  fi
  grep -q "$pattern" "$work/$name.out"
}

expect_rejected identity '身份不匹配' "$work/health.json" "$work/metrics.prom" \
  "$work/shadow.json" wrong cluster-a 2 100
expect_rejected lagging '尚未追平' "$work/health.json" "$work/metrics.prom" \
  "$work/shadow.json" n1 cluster-a 2 121
printf '%s\n' '{"version":1,"state":{"reported":{"failover":1}}}' > "$work/stale.json"
expect_rejected stale 'version 2' "$work/health.json" "$work/metrics.prom" \
  "$work/stale.json" n1 cluster-a 2 100

echo "PASS  shadow recovery verifier"
