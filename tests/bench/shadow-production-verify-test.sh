#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadow-production-verify-test.XXXXXX")
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

printf '%s\n' '{"published":10000,"delivered":10000,"errors":0,"publishPerSec":10000,"latencyP99Ms":500,"latencyMaxMs":600,"shadowMaxAcceptedGapMs":100,"seconds":60}' \
  > "$work/result.jsonl"

# 4分片、RF=3；每个分片恰好一个leader。每节点贡献5120/20的合批计数。
placements=('2 5' '2 3' '2 3 4' '3 4 5' '4 5')
leaders=(2 3 4 5 -1)
for i in 0 1 2 3 4; do
  : > "$work/metrics.before.$i.prom"
  {
    echo 'openiot_shadow_leader_merge_logical_total 5120'
    echo 'openiot_shadow_leader_merge_physical_total 20'
    echo 'openiot_shadow_proposal_errors_total 0'
    for shard in ${placements[$i]}; do
      echo "openiot_shadow_shard_ready{node=\"n$i\",shard=\"$shard\"} 1"
      value=0
      if [ "${leaders[$i]}" = "$shard" ]; then value=1; fi
      echo "openiot_shadow_shard_is_leader{node=\"n$i\",shard=\"$shard\"} $value"
    done
  } > "$work/metrics.after.$i.prom"
done

python3 tests/bench/shadow-production-verify.py "$work" 5 4 3 8000 1000 1000 1000 96 55 >/dev/null
python3 - "$work/summary.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1],encoding='utf-8'))
assert d['passed'] and d['shadowMergeLogicalPerPhysical']==256
assert d['replicasPerShard']=={'2':3,'3':3,'4':3,'5':3},d
PY

cp "$work/result.jsonl" "$work/result.good"
printf '%s\n' '{"published":10000,"delivered":10000,"errors":0,"publishPerSec":10000,"latencyP99Ms":500,"latencyMaxMs":600,"shadowMaxAcceptedGapMs":2000,"seconds":60}' > "$work/result.jsonl"
if python3 tests/bench/shadow-production-verify.py "$work" 5 4 3 8000 1000 1000 1000 96 55 \
  >"$work/gap.out" 2>&1; then
  echo "accepted长断档证据应被拒绝" >&2
  exit 1
fi
grep -q 'max accepted gap' "$work/gap.out"
mv "$work/result.good" "$work/result.jsonl"

cp "$work/result.jsonl" "$work/result.good"
printf '%s\n' '{"published":10000,"delivered":10000,"errors":0,"publishPerSec":10000,"latencyP99Ms":500,"latencyMaxMs":2000,"shadowMaxAcceptedGapMs":100,"seconds":60}' > "$work/result.jsonl"
if python3 tests/bench/shadow-production-verify.py "$work" 5 4 3 8000 1000 1000 1000 96 55 \
  >"$work/max-latency.out" 2>&1; then
  echo "单请求极端长尾证据应被拒绝" >&2
  exit 1
fi
grep -q 'max latency' "$work/max-latency.out"
mv "$work/result.good" "$work/result.jsonl"

cp "$work/metrics.after.1.prom" "$work/node1.good"
echo 'openiot_shadow_shard_is_leader{node="n1",shard="2"} 1' >> "$work/metrics.after.1.prom"
if python3 tests/bench/shadow-production-verify.py "$work" 5 4 3 8000 1000 1000 1000 96 55 \
  >"$work/duplicate.out" 2>&1; then
  echo "重复leader证据应被拒绝" >&2
  exit 1
fi
grep -q 'leader不完整或不唯一' "$work/duplicate.out"
mv "$work/node1.good" "$work/metrics.after.1.prom"

sed '/shadow_shard_ready.*shard="2"/d' "$work/metrics.after.2.prom" > "$work/node2.bad"
mv "$work/node2.bad" "$work/metrics.after.2.prom"
if python3 tests/bench/shadow-production-verify.py "$work" 5 4 3 8000 1000 1000 1000 96 55 \
  >"$work/rf.out" 2>&1; then
  echo "错误RF证据应被拒绝" >&2
  exit 1
fi
grep -q 'replica布局不是' "$work/rf.out"

echo "PASS  shadow production verifier"
