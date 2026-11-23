#!/usr/bin/env bash
# 对已经部署好的多机 Linux 集群做影子生产验收。与 shadow-attribution.sh
# 不同，本脚本不拉起本机进程；BROKERS/ADMINS 必须指向真实候选集群。
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ "$(uname -s)" != Linux ]; then
  echo "此验收只接受 Linux 结果，不能拿开发机loopback数字冒充生产证据。" >&2
  exit 2
fi

: "${BROKERS:?请设置五个直连MQTT入口，以逗号分隔}"
: "${ADMINS:?请设置五个直连Admin URL，以逗号分隔且与BROKERS顺序一致}"
: "${TOKEN_FILES:?请设置五个admin-token文件，以逗号分隔}"
: "${HOST_EVIDENCE_FILES:?请设置五个主机证据JSON，以逗号分隔}"
: "${TLS_CA:?请设置设备CA}"
: "${TLS_CERT:?请设置压测设备证书}"
: "${TLS_KEY:?请设置压测设备私钥}"
: "${TLS_SERVER_NAME:?请设置MQTT TLS server name}"

EXPECTED_NODES=${EXPECTED_NODES:-5}
EXPECTED_SHARDS=${EXPECTED_SHARDS:-4}
EXPECTED_RF=${EXPECTED_RF:-3}
CLIENTS=${CLIENTS:-3200}
DURATION=${DURATION:-60s}
WARMUP=${WARMUP:-5s}
MIN_RATE=${MIN_RATE:-8000}
MAX_P99_MS=${MAX_P99_MS:-1000}
MAX_LATENCY_MS=${MAX_LATENCY_MS:-1000}
MAX_ACCEPTED_GAP_MS=${MAX_ACCEPTED_GAP_MS:-1000}
MIN_MERGE_RATIO=${MIN_MERGE_RATIO:-96}
MIN_SECONDS=${MIN_SECONDS:-55}
EXPECTED_SHADOW_API_RATE=${EXPECTED_SHADOW_API_RATE:-0}
SETUP=${SETUP:-0}
PREFIX=${PREFIX:-shadow-production}
OUT_DIR=${OUT_DIR:-"docs/baseline/throughput/shadow-production-$(date -u +%Y%m%dT%H%M%SZ)"}
MQTTBENCH=${MQTTBENCH:-}
ADMIN_SOURCE_IP=${ADMIN_SOURCE_IP:-}
FAILOVER_HOOK=${FAILOVER_HOOK:-}
RESTORE_HOOK=${RESTORE_HOOK:-}
FAILOVER_CLIENTS=${FAILOVER_CLIENTS:-200}
FAILOVER_DURATION=${FAILOVER_DURATION:-20s}
FAILOVER_WARMUP=${FAILOVER_WARMUP:-2s}
FAILOVER_KILL_DELAY=${FAILOVER_KILL_DELAY:-2}
FAILOVER_SETUP=${FAILOVER_SETUP:-1}
MAX_RECOVERY_MS=${MAX_RECOVERY_MS:-5000}
MAX_ACCEPTED_GAP_MS=${MAX_ACCEPTED_GAP_MS:-3000}
MIN_FAILOVER_SUCCESS_RATE=${MIN_FAILOVER_SUCCESS_RATE:-0.99}
MAX_RESTORE_MS=${MAX_RESTORE_MS:-60000}
MAX_CATCHUP_MS=${MAX_CATCHUP_MS:-120000}

case "$SETUP" in 0|1) ;; *) echo "SETUP 只能是0或1" >&2; exit 2 ;; esac
for pair in \
  "EXPECTED_NODES:$EXPECTED_NODES" "EXPECTED_SHARDS:$EXPECTED_SHARDS" \
  "EXPECTED_RF:$EXPECTED_RF" "CLIENTS:$CLIENTS" "FAILOVER_CLIENTS:$FAILOVER_CLIENTS" \
  "MAX_RECOVERY_MS:$MAX_RECOVERY_MS" "MAX_RESTORE_MS:$MAX_RESTORE_MS" \
  "MAX_CATCHUP_MS:$MAX_CATCHUP_MS"; do
  name=${pair%%:*}; value=${pair#*:}
  case "$value" in *[!0-9]*|'') echo "$name 必须是非负整数" >&2; exit 2 ;; esac
done
case "$FAILOVER_SETUP" in 0|1) ;; *) echo "FAILOVER_SETUP只能是0或1" >&2; exit 2 ;; esac
if [ -n "$FAILOVER_HOOK" ] && [ ! -x "$FAILOVER_HOOK" ]; then
  echo "FAILOVER_HOOK必须是可执行文件；它将收到node_id、admin、broker三个参数。" >&2
  exit 2
fi
if [ -n "$FAILOVER_HOOK" ] && [ -z "$RESTORE_HOOK" ]; then
  echo "启用FAILOVER_HOOK时必须同时提供RESTORE_HOOK，生产故障门禁不能留下永久掉队副本。" >&2
  exit 2
fi
if [ -n "$RESTORE_HOOK" ] && [ -z "$FAILOVER_HOOK" ]; then
  echo "RESTORE_HOOK只能与FAILOVER_HOOK配对使用。" >&2
  exit 2
fi
if [ -n "$RESTORE_HOOK" ] && [ ! -x "$RESTORE_HOOK" ]; then
  echo "RESTORE_HOOK必须是可执行文件；它将收到node_id、admin、broker三个参数。" >&2
  exit 2
fi

IFS=',' read -r -a broker_list <<< "$BROKERS"
IFS=',' read -r -a admin_list <<< "$ADMINS"
IFS=',' read -r -a token_file_list <<< "$TOKEN_FILES"
IFS=',' read -r -a host_evidence_list <<< "$HOST_EVIDENCE_FILES"
if [ "${#broker_list[@]}" -ne "$EXPECTED_NODES" ] || \
   [ "${#admin_list[@]}" -ne "$EXPECTED_NODES" ] || \
   [ "${#token_file_list[@]}" -ne "$EXPECTED_NODES" ] || \
   [ "${#host_evidence_list[@]}" -ne "$EXPECTED_NODES" ]; then
  echo "BROKERS/ADMINS/TOKEN_FILES/HOST_EVIDENCE_FILES都必须恰好有$EXPECTED_NODES项。" >&2
  exit 2
fi
distinct_brokers=$(printf '%s\n' "${broker_list[@]}" | sort -u | wc -l | tr -d ' ')
distinct_admins=$(printf '%s\n' "${admin_list[@]}" | sort -u | wc -l | tr -d ' ')
if [ "$distinct_brokers" -ne "$EXPECTED_NODES" ] || [ "$distinct_admins" -ne "$EXPECTED_NODES" ]; then
  echo "生产验收要求$EXPECTED_NODES个不同的直连BROKERS和ADMINS，不能传重复入口或共享LB。" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadow-production.XXXXXX") || exit 1
bench_pid=""
cleanup() {
  if [ -n "$bench_pid" ]; then kill "$bench_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

if [ -z "$MQTTBENCH" ]; then
  MQTTBENCH="$work/mqttbench"
  go build -o "$MQTTBENCH" ./tools/mqttbench
fi

tokens=()
for token_file in "${token_file_list[@]}"; do
  [ -r "$token_file" ] || { echo "无法读取token文件: $token_file" >&2; exit 2; }
  tokens+=("$(tr -d '\r\n' < "$token_file")")
done

curl_source_args=()
if [ -n "$ADMIN_SOURCE_IP" ]; then curl_source_args=(--interface "$ADMIN_SOURCE_IP"); fi

node_ids=()
cluster_id=""
for i in "${!admin_list[@]}"; do
  admin=${admin_list[$i]}
  curl -sf "${curl_source_args[@]}" "$admin/healthz" > "$OUT_DIR/health.$i.json"
  node_id=$(sed -n 's/.*"node_id":[[:space:]]*"\([^"]*\)".*/\1/p' "$OUT_DIR/health.$i.json" | tail -n 1)
  cid=$(sed -n 's/.*"cluster_id":[[:space:]]*"\([^"]*\)".*/\1/p' "$OUT_DIR/health.$i.json" | tail -n 1)
  [ -n "$node_id" ] && [ -n "$cid" ] || {
    echo "$admin/healthz缺少node_id或cluster_id" >&2; exit 2;
  }
  if [ -z "$cluster_id" ]; then cluster_id=$cid; fi
  [ "$cid" = "$cluster_id" ] || {
    echo "$admin属于cluster_id=$cid，不是$cluster_id" >&2; exit 2;
  }
  node_ids+=("$node_id")
  curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
    "$admin/metrics" > "$OUT_DIR/metrics.before.$i.prom"
  curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
    "$admin/api/admin/v1/overview" > "$OUT_DIR/overview.$i.json"
done

distinct_nodes=$(printf '%s\n' "${node_ids[@]}" | sort -u | wc -l | tr -d ' ')
if [ "$distinct_nodes" -ne "$EXPECTED_NODES" ]; then
  echo "五个Admin只证明了$distinct_nodes个不同node_id，拒绝把重复入口当多机。" >&2
  exit 2
fi

for i in "${!host_evidence_list[@]}"; do
  evidence=${host_evidence_list[$i]}
  [ -r "$evidence" ] || { echo "无法读取主机证据: $evidence" >&2; exit 2; }
  cp "$evidence" "$OUT_DIR/host.$i.json"
done
copied_evidence=()
for i in "${!host_evidence_list[@]}"; do copied_evidence+=("$OUT_DIR/host.$i.json"); done
node_ids_csv=$(IFS=,; echo "${node_ids[*]}")
python3 tests/bench/shadow-host-evidence-verify.py \
  "$EXPECTED_NODES" "$node_ids_csv" "${copied_evidence[@]}" \
  > "$OUT_DIR/host-evidence-summary.json"

python3 - "$EXPECTED_NODES" "$OUT_DIR" <<'PY'
import re,sys,pathlib
expected=int(sys.argv[1]); root=pathlib.Path(sys.argv[2])
for i in range(expected):
    text=(root/f"metrics.before.{i}.prom").read_text()
    vals=[float(x) for x in re.findall(r'^openiot_quota_global_rate_members(?:\{[^}]*\})?\s+([0-9.eE+-]+)$',text,re.M)]
    if not vals or int(sum(vals)) != expected:
        raise SystemExit(f"node[{i}] quota_global_rate_members={vals}, expected {expected}")
PY

python3 - "$EXPECTED_NODES" "$EXPECTED_SHADOW_API_RATE" "$OUT_DIR" <<'PY'
import json,pathlib,sys
nodes,expected,root=int(sys.argv[1]),int(sys.argv[2]),pathlib.Path(sys.argv[3])
for i in range(nodes):
    data=json.loads((root/f"overview.{i}.json").read_text())
    rows=[q for q in data.get("quotas",[]) if q.get("name")=="shadow_api_rate"]
    if len(rows)!=1 or int(rows[0].get("limit",-1))!=expected:
        raise SystemExit(f"node[{i}] shadow_api_rate={rows}, expected limit={expected}")
PY

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "kernel=$(uname -srvm)"
  echo "go=$(go version)"
  echo "cluster_id=$cluster_id"
  echo "node_ids=$(IFS=,; echo "${node_ids[*]}")"
  echo "brokers=$BROKERS"
  echo "admins=$ADMINS"
  echo "clients=$CLIENTS duration=$DURATION warmup=$WARMUP"
  echo "expected_shards=$EXPECTED_SHARDS expected_rf=$EXPECTED_RF"
  echo "min_rate=$MIN_RATE max_p99_ms=$MAX_P99_MS max_latency_ms=$MAX_LATENCY_MS max_accepted_gap_ms=$MAX_ACCEPTED_GAP_MS min_merge_ratio=$MIN_MERGE_RATIO"
  echo "min_seconds=$MIN_SECONDS expected_shadow_api_rate=$EXPECTED_SHADOW_API_RATE"
} > "$OUT_DIR/environment.txt"

admin_token=${tokens[0]}
args=(
  -scenario shadow -broker "$BROKERS" -admin "${admin_list[0]}" -token "$admin_token"
  -clients "$CLIENTS" -duration "$DURATION" -warmup "$WARMUP"
  -prefix "$PREFIX" -label "shadow/production-n${EXPECTED_NODES}-sh${EXPECTED_SHARDS}-rf${EXPECTED_RF}"
  -setup="$SETUP" -out "$OUT_DIR/result.jsonl"
  -shadow-response-qos 1 -shadow-response-timeout 20s
  -tls-ca "$TLS_CA" -tls-cert "$TLS_CERT" -tls-key "$TLS_KEY"
  -tls-server-name "$TLS_SERVER_NAME"
)
if [ -n "$ADMIN_SOURCE_IP" ]; then args+=(-admin-source-ip "$ADMIN_SOURCE_IP"); fi
"$MQTTBENCH" "${args[@]}"

for i in "${!admin_list[@]}"; do
  curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
    "${admin_list[$i]}/metrics" > "$OUT_DIR/metrics.after.$i.prom"
done

python3 tests/bench/shadow-production-verify.py \
  "$OUT_DIR" "$EXPECTED_NODES" "$EXPECTED_SHARDS" "$EXPECTED_RF" \
  "$MIN_RATE" "$MAX_P99_MS" "$MAX_LATENCY_MS" "$MAX_ACCEPTED_GAP_MS" "$MIN_MERGE_RATIO" "$MIN_SECONDS"

if [ -n "$FAILOVER_HOOK" ]; then
  leader_i=-1
  shard=""
  for i in "${!admin_list[@]}"; do
    leader_line=$(awk '$1 ~ /^openiot_shadow_shard_is_leader/ && $NF == 1 {print; exit}' \
      "$OUT_DIR/metrics.after.$i.prom")
    if [ -n "$leader_line" ]; then
      leader_i=$i
      shard=$(printf '%s\n' "$leader_line" | sed -n 's/.*shard="\([0-9][0-9]*\)".*/\1/p')
      break
    fi
  done
  [ "$leader_i" -ge 0 ] && [ -n "$shard" ] || {
    echo "故障轮找不到真实shadow shard leader" >&2; exit 1;
  }

  # RF<N时选择不承载目标分片的Access，覆盖“先定位副本、再找leader”的完整路由。
  access_i=-1
  for i in "${!admin_list[@]}"; do
    [ "$i" -eq "$leader_i" ] && continue
    if ! grep -q "^openiot_shadow_shard_ready{.*shard=\"$shard\"" \
      "$OUT_DIR/metrics.after.$i.prom"; then
      access_i=$i
      break
    fi
  done
  [ "$access_i" -ge 0 ] || {
    echo "没有找到不承载shard=$shard的Access节点；实际布局与RF=$EXPECTED_RF不符" >&2
    exit 1
  }

  failover_seed="shadow-production-failover-$(date -u +%Y%m%dT%H%M%SZ)"
  thing=$(python3 - "$shard" "$EXPECTED_SHARDS" "$failover_seed" <<'PY'
import sys
target,count,prefix=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3]
for n in range(1000000):
    name=f"{prefix}-{n}"; h=14695981039346656037
    for b in name.encode(): h=((h^b)*1099511628211)&((1<<64)-1)
    if 2+h%count==target:
        print(name); break
else: raise SystemExit("找不到目标分片thing")
PY
  )
  access_admin=${admin_list[$access_i]}
  access_broker=${broker_list[$access_i]}
  access_token=${tokens[$access_i]}
  curl -sf "${curl_source_args[@]}" -X PUT \
    -H "Authorization: Bearer $access_token" -H 'Content-Type: application/json' -d '{}' \
    "$access_admin/api/admin/v1/things/$thing" >/dev/null
  baseline=$(curl -sf "${curl_source_args[@]}" -X PUT \
    -H "Authorization: Bearer $access_token" -H 'Content-Type: application/json' \
    -d '{"state":{"reported":{"failover":1}}}' \
    "$access_admin/api/admin/v1/things/$thing/shadow")
  printf '%s\n' "$baseline" | python3 -c \
    'import json,sys; d=json.load(sys.stdin); assert d["version"]==1,d'

  ready_file="$work/failover.ready"
  bench_json="$work/failover-load.json"
  "$MQTTBENCH" -scenario shadow -broker "$access_broker" \
    -admin "$access_admin" -token "$access_token" -clients "$FAILOVER_CLIENTS" \
    -shadow-shards "$EXPECTED_SHARDS" -shadow-shard "$shard" \
    -shadow-ready-file "$ready_file" -duration "$FAILOVER_DURATION" \
    -warmup "$FAILOVER_WARMUP" -prefix "${failover_seed}-load" \
    -label "shadow/production-failover" -setup="$FAILOVER_SETUP" -out /dev/null \
    -shadow-response-qos 1 -shadow-response-timeout 20s \
    -tls-ca "$TLS_CA" -tls-cert "$TLS_CERT" -tls-key "$TLS_KEY" \
    -tls-server-name "$TLS_SERVER_NAME" > "$bench_json" &
  bench_pid=$!
  for _ in $(seq 1 1200); do
    [ -f "$ready_file" ] && break
    kill -0 "$bench_pid" 2>/dev/null || { echo "故障负载在就绪前退出" >&2; exit 1; }
    sleep 0.1
  done
  [ -f "$ready_file" ] || { echo "故障负载120秒内未就绪" >&2; exit 1; }
  sleep "$FAILOVER_WARMUP"
  sleep "$FAILOVER_KILL_DELAY"

  started_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
  echo "调用FAILOVER_HOOK停止leader ${node_ids[$leader_i]}（shard=$shard）" >&2
  "$FAILOVER_HOOK" "${node_ids[$leader_i]}" "${admin_list[$leader_i]}" \
    "${broker_list[$leader_i]}"
  if curl -sf "${curl_source_args[@]}" "${admin_list[$leader_i]}/healthz" >/dev/null 2>&1; then
    kill "$bench_pid" 2>/dev/null || true
    echo "FAILOVER_HOOK返回后旧leader仍可访问，拒绝把leader transfer冒充进程故障" >&2
    exit 1
  fi

  recovered_ms=0
  recovered_unix_ms=0
  new_leader_i=-1
  write_body="$work/failover-write.json"
  attempts=$((MAX_RECOVERY_MS / 100 + 1))
  for _ in $(seq 1 "$attempts"); do
    new_leader=""
    for i in "${!admin_list[@]}"; do
      [ "$i" -eq "$leader_i" ] && continue
      poll="$work/failover-poll.$i.prom"
      if curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
        "${admin_list[$i]}/metrics" > "$poll"; then
        if awk -v shard="$shard" '$1 ~ /^openiot_shadow_shard_is_leader/ && \
          index($1,"shard=\"" shard "\"") && $NF == 1 {found=1} END{exit !found}' "$poll"; then
          new_leader=${node_ids[$i]}
          new_leader_i=$i
          break
        fi
      fi
    done
    if [ -n "$new_leader" ]; then
      code=$(curl -sS "${curl_source_args[@]}" -o "$write_body" -w '%{http_code}' -X PUT \
        -H "Authorization: Bearer $access_token" -H 'Content-Type: application/json' \
        -d '{"state":{"reported":{"failover":2}},"version":1}' \
        "$access_admin/api/admin/v1/things/$thing/shadow" 2>/dev/null || printf 000)
      if [ "$code" = 200 ] || [ "$code" = 409 ]; then
        recovered_unix_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
        recovered_ms=$((recovered_unix_ms - started_ms))
        break
      fi
    fi
    sleep 0.1
  done
  if [ "$recovered_ms" -le 0 ]; then
    kill "$bench_pid" 2>/dev/null || true
    echo "leader进程故障后${MAX_RECOVERY_MS}ms内未恢复版本化写入" >&2
    exit 1
  fi
  wait "$bench_pid"
  bench_pid=""

  final=$(curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer $access_token" \
    "$access_admin/api/admin/v1/things/$thing/shadow")
  printf '%s\n' "$final" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["version"]==2,d
assert d["state"]["reported"]["failover"]==2,d
'
  failover_summary=$(python3 tests/bench/shadow-failover-verify.py "$bench_json" \
    "$recovered_ms" "$recovered_unix_ms" "$MAX_RECOVERY_MS" \
    "$MAX_ACCEPTED_GAP_MS" "$MIN_FAILOVER_SUCCESS_RATE")

  # 故障切换只是半个生命周期：重新启动旧leader，并证明它恢复同一身份、
  # 重新承载目标分片且追到故障后新leader的持久化applied index。
  [ "$new_leader_i" -ge 0 ] || { echo "没有记录新leader索引" >&2; exit 1; }
  survivor_metrics="$work/failover-survivor-final.prom"
  curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$new_leader_i]}" \
    "${admin_list[$new_leader_i]}/metrics" > "$survivor_metrics"
  target_applied=$(awk -v shard="$shard" '$1 ~ /^openiot_shadow_shard_applied_index/ && \
    index($1,"shard=\"" shard "\"") {print int($NF); exit}' "$survivor_metrics")
  [ -n "$target_applied" ] && [ "$target_applied" -gt 0 ] || {
    echo "新leader缺少shard=$shard的applied index指标" >&2; exit 1;
  }

  restore_started_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
  echo "调用RESTORE_HOOK恢复旧leader ${node_ids[$leader_i]}" >&2
  "$RESTORE_HOOK" "${node_ids[$leader_i]}" "${admin_list[$leader_i]}" \
    "${broker_list[$leader_i]}"
  restore_attempts=$((MAX_RESTORE_MS / 100 + 1))
  restored_health="$work/restored-health.json"
  for _ in $(seq 1 "$restore_attempts"); do
    if curl -sf "${curl_source_args[@]}" "${admin_list[$leader_i]}/healthz" \
      > "$restored_health"; then
      break
    fi
    sleep 0.1
  done
  [ -s "$restored_health" ] || {
    echo "旧leader在${MAX_RESTORE_MS}ms内没有恢复health" >&2; exit 1;
  }
  python3 - "$restored_health" "${node_ids[$leader_i]}" "$cluster_id" <<'PY'
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
if d.get("node_id") != sys.argv[2] or d.get("cluster_id") != sys.argv[3]:
    raise SystemExit(f"恢复节点身份不匹配: {d}")
PY

  restored_metrics="$OUT_DIR/metrics.restored.$leader_i.prom"
  catchup_attempts=$((MAX_CATCHUP_MS / 100 + 1))
  restored_applied=0
  for _ in $(seq 1 "$catchup_attempts"); do
    if curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$leader_i]}" \
      "${admin_list[$leader_i]}/metrics" > "$restored_metrics"; then
      ready=$(awk -v shard="$shard" '$1 ~ /^openiot_shadow_shard_ready/ && \
        index($1,"shard=\"" shard "\"") {print int($NF); exit}' "$restored_metrics")
      restored_applied=$(awk -v shard="$shard" '$1 ~ /^openiot_shadow_shard_applied_index/ && \
        index($1,"shard=\"" shard "\"") {print int($NF); exit}' "$restored_metrics")
      if [ "${ready:-0}" -eq 1 ] && [ "${restored_applied:-0}" -ge "$target_applied" ]; then
        break
      fi
    fi
    sleep 0.1
  done
  if [ "${restored_applied:-0}" -lt "$target_applied" ]; then
    echo "恢复副本在${MAX_CATCHUP_MS}ms内未追平: $restored_applied < $target_applied" >&2
    exit 1
  fi
  restored_final="$work/restored-shadow.json"
  curl -sf "${curl_source_args[@]}" \
    -H "Authorization: Bearer ${tokens[$leader_i]}" \
    "${admin_list[$leader_i]}/api/admin/v1/things/$thing/shadow" > "$restored_final"
  python3 tests/bench/shadow-recovery-verify.py \
    "$restored_health" "$restored_metrics" "$restored_final" \
    "${node_ids[$leader_i]}" "$cluster_id" "$shard" "$target_applied" \
    > "$OUT_DIR/recovery-summary.json"
  restore_finished_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
  restore_ms=$((restore_finished_ms - restore_started_ms))

  python3 - "$failover_summary" "$OUT_DIR/failover-summary.json" \
    "${node_ids[$leader_i]}" "$new_leader" "${node_ids[$access_i]}" "$shard" \
    "$restore_ms" "$target_applied" "$restored_applied" <<'PY'
import json,sys
d=json.loads(sys.argv[1]); d.update({"oldLeader":sys.argv[3],"newLeader":sys.argv[4],
 "access":sys.argv[5],"shard":int(sys.argv[6]),"version":2,"prefixPreserved":True,
 "restored":True,"restoreMs":int(sys.argv[7]),"catchupTargetIndex":int(sys.argv[8]),
 "restoredAppliedIndex":int(sys.argv[9])})
open(sys.argv[2],"w",encoding="utf-8").write(json.dumps(d,ensure_ascii=False,indent=2)+"\n")
print(json.dumps(d,ensure_ascii=False))
PY
  cp "$bench_json" "$OUT_DIR/failover-load.json"
  echo "PASS  故障轮通过；旧leader已恢复并追平到applied index $restored_applied。" >&2
fi

echo "PASS  影子生产验收通过；证据目录: $OUT_DIR"
