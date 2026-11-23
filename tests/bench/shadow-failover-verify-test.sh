#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadow-failover-verify-test.XXXXXX")
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

printf '%s\n' '{"published":10000,"delivered":9950,"shadowRejected":50,"shadowTimeouts":0,"errors":50,"shadowMaxAcceptedGapMs":1500,"shadowLastAcceptedUnixMs":20000}' > "$work/good.json"
python3 tests/bench/shadow-failover-verify.py "$work/good.json" 1600 18000 5000 3000 0.99 >/dev/null

printf '%s\n' '{"published":10000,"delivered":9950,"shadowRejected":40,"shadowTimeouts":0,"errors":40,"shadowMaxAcceptedGapMs":1500,"shadowLastAcceptedUnixMs":20000}' > "$work/bad.json"
if python3 tests/bench/shadow-failover-verify.py "$work/bad.json" 1600 18000 5000 3000 0.99 \
  >"$work/bad.out" 2>&1; then
  echo "不守恒故障结果应被拒绝" >&2
  exit 1
fi
grep -q '终态不守恒' "$work/bad.out"

expect_fail() { # name json recovery recovered max-recovery max-gap min-success pattern
  name=$1; json=$2; shift 2
  printf '%s\n' "$json" > "$work/$name.json"
  pattern=${6}
  if python3 tests/bench/shadow-failover-verify.py "$work/$name.json" \
    "$1" "$2" "$3" "$4" "$5" >"$work/$name.out" 2>&1; then
    echo "$name应被拒绝" >&2
    exit 1
  fi
  grep -q "$pattern" "$work/$name.out"
}
expect_fail gap '{"published":10000,"delivered":9950,"shadowRejected":50,"shadowTimeouts":0,"errors":50,"shadowMaxAcceptedGapMs":3500,"shadowLastAcceptedUnixMs":20000}' \
  1600 18000 5000 3000 0.99 'max gap'
expect_fail stale '{"published":10000,"delivered":9950,"shadowRejected":50,"shadowTimeouts":0,"errors":50,"shadowMaxAcceptedGapMs":1500,"shadowLastAcceptedUnixMs":17000}' \
  1600 18000 5000 3000 0.99 '没有新的accepted'
expect_fail success '{"published":10000,"delivered":9800,"shadowRejected":200,"shadowTimeouts":0,"errors":200,"shadowMaxAcceptedGapMs":1500,"shadowLastAcceptedUnixMs":20000}' \
  1600 18000 5000 3000 0.99 'success rate'
echo "PASS  shadow failover verifier"
