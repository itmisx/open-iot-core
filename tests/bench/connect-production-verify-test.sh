#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-connect-production-verify-test.XXXXXX")
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

mkdir -p "$work/fakebin"
printf '#!/usr/bin/env bash\necho Linux\n' > "$work/fakebin/uname"
chmod 0755 "$work/fakebin/uname"
production_env=(
  BROKERS=b1,b2,b3,b4,b5 ADMINS=a1,a2,a3,a4,a5
  TOKEN_FILES=t1,t2,t3,t4,t5 HOST_EVIDENCE_FILES=h1,h2,h3,h4,h5
  TLS_CA=ca TLS_CERT=cert TLS_KEY=key TLS_SERVER_NAME=mqtt.example
)
if (unset PREFIX; env PATH="$work/fakebin:$PATH" "${production_env[@]}" \
  tests/bench/connect-production-linux.sh) >"$work/prefix.out" 2>&1; then
  echo "未显式给PREFIX的生产门禁应失败" >&2; exit 1
fi
grep -q '请设置已预置' "$work/prefix.out"
if env PATH="$work/fakebin:$PATH" "${production_env[@]}" PREFIX=gate SETUP=1 \
  tests/bench/connect-production-linux.sh >"$work/setup.out" 2>&1; then
  echo "生产门禁SETUP=1应失败" >&2; exit 1
fi
grep -q '固定SETUP=0' "$work/setup.out"

fingerprint=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
printf '%s\n' "{\"fingerprint\":\"$fingerprint\",\"status\":\"ACTIVE\",\"policyName\":\"gate-policy\"}" \
  > "$work/certificate.json"
printf '%s\n' \
  '{"things":[{"name":"gate-c000000","policyName":"p"},{"name":"gate-c000001","policyName":"p"},{"name":"gate-c000002","policyName":"p"}],"nextAfter":"gate-c000002"}' \
  > "$work/things.0.json"
printf '%s\n' \
  '{"things":[{"name":"gate-c000003","policyName":"p"},{"name":"gate-c000004","policyName":"p"}]}' \
  > "$work/things.1.json"
python3 tests/bench/connect-production-preflight-verify.py \
  "$work/certificate.json" "$fingerprint" gate 5 \
  "$work/things.0.json" "$work/things.1.json" > "$work/preflight.json"
printf '%s\n' \
  '{"things":[{"name":"gate-c000004","policyName":"p"}]}' > "$work/things.gap.json"
if python3 tests/bench/connect-production-preflight-verify.py \
  "$work/certificate.json" "$fingerprint" gate 4 \
  "$work/things.0.json" "$work/things.gap.json" >"$work/gap-preflight.out" 2>&1; then
  echo "预置设备缺号证据应失败" >&2; exit 1
fi
grep -q '预置设备不连续' "$work/gap-preflight.out"

write_result() {
  printf '%s\n' "$1" > "$work/result.jsonl"
}
write_result '{"clients":6000,"heldConns":6000,"heldVerified":true,"errors":0,"tlsResumed":6000,"connectPerSec":3708.5,"connectP99Ms":736.1,"connectMaxMs":859.9}'

for i in 0 1 2 3 4; do
  {
    echo 'openiot_session_shard_count 8'
    echo 'openiot_session_proposal_errors_total 0'
    echo 'openiot_session_raft_proposals_total 0'
    echo 'openiot_connections_total{result="Accepted"} 0'
    echo 'openiot_tls_handshakes_total{result="full"} 0'
    echo 'openiot_tls_handshakes_total{result="resumed"} 0'
    echo 'openiot_tls_handshakes_total{result="failed"} 0'
    echo 'openiot_tls_handshakes_total{result="throttled"} 0'
    echo 'openiot_session_proposal_safe_reroutes_total{source="local"} 0'
    echo 'openiot_session_proposal_safe_reroutes_total{source="remote"} 0'
    echo 'openiot_session_remote_read_claim_piggyback_total 0'
    echo 'openiot_session_remote_read_errors_total 0'
    echo 'openiot_ownership_claim_errors_total 0'
  } > "$work/metrics.before.$i.prom"
  {
    echo 'openiot_session_shard_count 8'
    echo 'openiot_session_proposal_errors_total 0'
    echo 'openiot_session_raft_proposals_total 40'
    echo 'openiot_connections_total{result="Accepted"} 2400'
    echo 'openiot_tls_handshakes_total{result="full"} 1200'
    echo 'openiot_tls_handshakes_total{result="resumed"} 1200'
    echo 'openiot_tls_handshakes_total{result="failed"} 0'
    echo 'openiot_tls_handshakes_total{result="throttled"} 0'
    echo 'openiot_session_proposal_safe_reroutes_total{source="local"} 0'
    echo 'openiot_session_proposal_safe_reroutes_total{source="remote"} 0'
    echo 'openiot_session_remote_read_claim_piggyback_total 100'
    echo 'openiot_session_remote_read_errors_total 0'
    echo 'openiot_ownership_claim_errors_total 0'
    for shard in 16 17 18 19 20 21 22 23; do
      owner=$(( (shard - 16) % 5 ))
      for offset in 0 1 2; do
        if [ "$i" -eq $(( (owner + offset) % 5 )) ]; then
          echo "openiot_session_shard_ready{node=\"n$i\",shard=\"$shard\"} 1"
          leader=0
          if [ "$offset" -eq 0 ]; then leader=1; fi
          echo "openiot_session_shard_is_leader{node=\"n$i\",shard=\"$shard\"} $leader"
          echo "openiot_session_shard_applied_index{node=\"n$i\",shard=\"$shard\"} $((100 + shard))"
        fi
      done
    done
  } > "$work/metrics.after.$i.prom"
done

verify=(python3 tests/bench/connect-production-verify.py "$work" 5 8 3 6000 3000 1000 1000)
"${verify[@]}" >/dev/null
python3 - "$work/summary.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1],encoding='utf-8'))
assert d['passed'] and d['heldVerified'] and d['raftProposals']==200,d
assert d['remoteReadClaimPiggyback']==500 and d['remoteReadErrors']==0,d
assert len(d['leaderShards'])==8 and set(d['replicasPerShard'].values())=={3},d
PY

cp "$work/result.jsonl" "$work/result.good"
write_result '{"clients":6000,"heldConns":6000,"heldVerified":true,"errors":0,"tlsResumed":6000,"connectPerSec":3708.5,"connectP99Ms":1001,"connectMaxMs":1100}'
if "${verify[@]}" >"$work/p99.out" 2>&1; then
  echo "超过p99门槛的证据应被拒绝" >&2; exit 1
fi
grep -q 'CONNECT p99' "$work/p99.out"
mv "$work/result.good" "$work/result.jsonl"

cp "$work/result.jsonl" "$work/result.good"
write_result '{"clients":6000,"heldConns":6000,"heldVerified":true,"errors":0,"tlsResumed":6000,"connectPerSec":3708.5,"connectP99Ms":900,"connectMaxMs":1501}'
if "${verify[@]}" >"$work/max.out" 2>&1; then
  echo "超过单次最大耗时门槛的证据应被拒绝" >&2; exit 1
fi
grep -q 'CONNECT max' "$work/max.out"
mv "$work/result.good" "$work/result.jsonl"

cp "$work/result.jsonl" "$work/result.good"
write_result '{"clients":6000,"heldConns":6000,"heldVerified":true,"errors":0,"tlsResumed":5999,"connectPerSec":3708.5,"connectP99Ms":736.1,"connectMaxMs":859.9}'
if "${verify[@]}" >"$work/resume.out" 2>&1; then
  echo "TLS未全部恢复的证据应被拒绝" >&2; exit 1
fi
grep -q 'TLS resumed=5999/6000' "$work/resume.out"
mv "$work/result.good" "$work/result.jsonl"

cp "$work/metrics.after.1.prom" "$work/node1.good"
echo 'openiot_session_shard_is_leader{node="n1",shard="16"} 1' >> "$work/metrics.after.1.prom"
if "${verify[@]}" >"$work/leader.out" 2>&1; then
  echo "重复leader证据应被拒绝" >&2; exit 1
fi
grep -q 'leader不完整或不唯一' "$work/leader.out"
mv "$work/node1.good" "$work/metrics.after.1.prom"

cp "$work/metrics.after.2.prom" "$work/node2.good"
sed '/session_shard_ready.*shard="16"/d' "$work/metrics.after.2.prom" > "$work/node2.bad"
mv "$work/node2.bad" "$work/metrics.after.2.prom"
if "${verify[@]}" >"$work/rf.out" 2>&1; then
  echo "错误RF证据应被拒绝" >&2; exit 1
fi
grep -q 'replica布局不是' "$work/rf.out"
mv "$work/node2.good" "$work/metrics.after.2.prom"

# 恢复正确RF后，制造一个未追平副本；ready/leader都正常也必须拒绝。
sed 's/session_shard_applied_index{node="n1",shard="17"} 117/session_shard_applied_index{node="n1",shard="17"} 116/' \
  "$work/metrics.after.1.prom" > "$work/node1.lag"
mv "$work/node1.lag" "$work/metrics.after.1.prom"
if "${verify[@]}" >"$work/applied.out" 2>&1; then
  echo "applied index未收敛的证据应被拒绝" >&2; exit 1
fi
grep -q 'applied index未收敛' "$work/applied.out"

echo "PASS  connect production verifier"
