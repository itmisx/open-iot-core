#!/usr/bin/env bash
# 对已经部署好的五机 Linux 集群做持久会话 CONNECT 生产验收。
# 本脚本不拉起本机进程；BROKERS/ADMINS 必须是五个候选节点的直连入口。
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ "$(uname -s)" != Linux ]; then
  echo "此验收只接受Linux结果，不能拿开发机loopback数字冒充生产证据。" >&2
  exit 2
fi

: "${BROKERS:?请设置五个直连mTLS MQTT入口，以逗号分隔}"
: "${ADMINS:?请设置五个直连Admin URL，以逗号分隔且与BROKERS顺序一致}"
: "${TOKEN_FILES:?请设置五个admin-token文件，以逗号分隔}"
: "${HOST_EVIDENCE_FILES:?请设置五个主机证据JSON，以逗号分隔}"
: "${TLS_CA:?请设置设备CA}"
: "${TLS_CERT:?请设置压测设备证书}"
: "${TLS_KEY:?请设置压测设备私钥}"
: "${TLS_SERVER_NAME:?请设置MQTT TLS server name}"

EXPECTED_NODES=${EXPECTED_NODES:-5}
EXPECTED_SHARDS=${EXPECTED_SHARDS:-8}
EXPECTED_RF=${EXPECTED_RF:-3}
CLIENTS=${CLIENTS:-6000}
CONCURRENCY=${CONCURRENCY:-1600}
MIN_RATE=${MIN_RATE:-3000}
MAX_P99_MS=${MAX_P99_MS:-1000}
MAX_CONNECT_MS=${MAX_CONNECT_MS:-1500}
HOLD=${HOLD:-3s}
RECONNECT_DELAY=${RECONNECT_DELAY:-1250ms}
SETUP=${SETUP:-0}
: "${PREFIX:?请设置已预置且由压测证书策略允许的clientId前缀}"
OUT_DIR=${OUT_DIR:-"docs/baseline/throughput/connect-production-$(date -u +%Y%m%dT%H%M%SZ)"}
MQTTBENCH=${MQTTBENCH:-}
ADMIN_SOURCE_IP=${ADMIN_SOURCE_IP:-}
SOURCE_IPS=${SOURCE_IPS:-}

if [ "$SETUP" != 0 ]; then
  echo "生产门禁固定SETUP=0：设备/策略须提前预置，容量轮不得夹带控制面写入。" >&2
  exit 2
fi
case "$PREFIX" in
  *[!A-Za-z0-9_:-]*|'') echo "PREFIX含设备名不允许的字符: $PREFIX" >&2; exit 2 ;;
esac
if [ "${#PREFIX}" -gt 119 ]; then
  echo "PREFIX过长；加上-c000000后必须不超过128字节。" >&2
  exit 2
fi
for pair in \
  "EXPECTED_NODES:$EXPECTED_NODES" "EXPECTED_SHARDS:$EXPECTED_SHARDS" \
  "EXPECTED_RF:$EXPECTED_RF" "CLIENTS:$CLIENTS" "CONCURRENCY:$CONCURRENCY"; do
  name=${pair%%:*}; value=${pair#*:}
  case "$value" in *[!0-9]*|'') echo "$name必须是非负整数" >&2; exit 2 ;; esac
done
if [ "$EXPECTED_NODES" -ne 5 ]; then
  echo "生产持久建连门禁固定要求五节点；EXPECTED_NODES=$EXPECTED_NODES" >&2
  exit 2
fi
if [ -e "$OUT_DIR/result.jsonl" ]; then
  echo "$OUT_DIR已有result.jsonl；请使用新的OUT_DIR，禁止把多轮追加成一份证据。" >&2
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
if [ "$(printf '%s\n' "${broker_list[@]}" | sort -u | wc -l | tr -d ' ')" -ne "$EXPECTED_NODES" ] || \
   [ "$(printf '%s\n' "${admin_list[@]}" | sort -u | wc -l | tr -d ' ')" -ne "$EXPECTED_NODES" ]; then
  echo "必须提供五个不同的直连BROKERS和ADMINS，不能传重复入口或共享LB。" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-connect-production.XXXXXX") || exit 1
cleanup() { rm -rf "$work"; }
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
if [ "$(printf '%s\n' "${node_ids[@]}" | sort -u | wc -l | tr -d ' ')" -ne "$EXPECTED_NODES" ]; then
  echo "五个Admin没有证明五个不同node_id: ${node_ids[*]}" >&2
  exit 2
fi

# mqttbench用同一张证书模拟一批不同clientId，因此该证书必须绑定一份直接允许
# PREFIX-* 的策略，不能依赖“证书所绑Thing名等于clientId”的变量。至少先证明
# 叶证书确实已登记、ACTIVE且有策略；具体资源范围由真实CONNECT再次验证。
command -v openssl >/dev/null || { echo "缺少openssl，无法核对压测证书指纹。" >&2; exit 2; }
cert_fingerprint=$(openssl x509 -in "$TLS_CERT" -outform DER | \
  openssl dgst -sha256 | awk '{print tolower($NF)}')
[ -n "$cert_fingerprint" ] || { echo "无法计算TLS_CERT叶证书指纹。" >&2; exit 2; }
curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[0]}" \
  "${admin_list[0]}/api/admin/v1/certificates/$cert_fingerprint" \
  > "$OUT_DIR/benchmark-certificate.json"

# 证书的通配策略可能让不存在于registry的clientId也通过认证，所以不能用
# “CONNECT成功”反推设备已预置。分页逐项核对本轮精确名称与非空策略绑定。
seen=0
after=""
page_no=0
thing_pages=()
while :; do
  page="$OUT_DIR/things-page.$page_no.json"
  thing_pages+=("$page")
  url="${admin_list[0]}/api/admin/v1/things?prefix=${PREFIX}-c&limit=500"
  if [ -n "$after" ]; then url="$url&after=$after"; fi
  curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[0]}" \
    "$url" > "$page"
  page_summary=$(python3 - "$page" <<'PY'
import json,pathlib,sys
path=pathlib.Path(sys.argv[1]); data=json.loads(path.read_text(encoding="utf-8"))
things=data.get("things",[])
print(len(things),data.get("nextAfter", ""))
PY
)
  read -r page_count next_after <<< "$page_summary"
  seen=$((seen + page_count))
  [ -n "$next_after" ] || break
  [ "$page_count" -gt 0 ] || { echo "设备分页游标未推进。" >&2; exit 2; }
  [ "$next_after" != "$after" ] || { echo "设备分页游标重复: $after" >&2; exit 2; }
  after=$next_after
  page_no=$((page_no + 1))
  [ "$page_no" -le "$CLIENTS" ] || { echo "设备分页数异常。" >&2; exit 2; }
done
python3 tests/bench/connect-production-preflight-verify.py \
  "$OUT_DIR/benchmark-certificate.json" "$cert_fingerprint" "$PREFIX" "$CLIENTS" \
  "${thing_pages[@]}" > "$OUT_DIR/preflight-summary.json"

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

# 生产容量轮必须使用空闲候选集群；否则其他设备的连接/握手会污染精确增量。
python3 - "$EXPECTED_NODES" "$OUT_DIR" <<'PY'
import pathlib,re,sys
nodes,root=int(sys.argv[1]),pathlib.Path(sys.argv[2])
for i in range(nodes):
    text=(root/f"metrics.before.{i}.prom").read_text()
    values=[float(x) for x in re.findall(r'^openiot_connections(?:\{[^}]*\})?\s+([0-9.eE+-]+)$',text,re.M)]
    if sum(values) != 0:
        raise SystemExit(f"node[{i}]开跑前已有openiot_connections={sum(values):.0f}")
PY

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "kernel=$(uname -srvm)"
  echo "go=$(go version)"
  echo "cluster_id=$cluster_id"
  echo "node_ids=$(IFS=,; echo "${node_ids[*]}")"
  echo "brokers=$BROKERS"
  echo "admins=$ADMINS"
  echo "clients=$CLIENTS concurrency=$CONCURRENCY hold=$HOLD reconnect_delay=$RECONNECT_DELAY"
  echo "prefix=$PREFIX cert_fingerprint=$cert_fingerprint setup=$SETUP"
  echo "preprovisioned_things=$seen"
  echo "expected_shards=$EXPECTED_SHARDS expected_rf=$EXPECTED_RF"
  echo "min_rate=$MIN_RATE max_p99_ms=$MAX_P99_MS max_connect_ms=$MAX_CONNECT_MS"
} > "$OUT_DIR/environment.txt"

args=(
  -scenario connect -broker "$BROKERS" -admin "${admin_list[0]}" -token "${tokens[0]}"
  -clients "$CLIENTS" -concurrency "$CONCURRENCY" -hold "$HOLD"
  -prefix "$PREFIX" -label "connect/production-persistent-mtls-reconnect-shift1-n${EXPECTED_NODES}-s${EXPECTED_SHARDS}-rf${EXPECTED_RF}"
  -setup="$SETUP" -out "$OUT_DIR/result.jsonl"
  -persistent-session -reconnect -reconnect-delay "$RECONNECT_DELAY" -reconnect-shift 1
  -verify-held -tls-ca "$TLS_CA" -tls-cert "$TLS_CERT" -tls-key "$TLS_KEY"
  -tls-server-name "$TLS_SERVER_NAME"
)
if [ -n "$ADMIN_SOURCE_IP" ]; then args+=(-admin-source-ip "$ADMIN_SOURCE_IP"); fi
if [ -n "$SOURCE_IPS" ]; then args+=(-source-ip "$SOURCE_IPS"); fi
"$MQTTBENCH" "${args[@]}"

# mqttbench返回时会关闭第二波连接；持久会话的断开还会异步Save。必须等全部
# Access/leader队列排空再冻结结束快照，否则尾部proposal error可能刚好落在采样后。
quiescent=0
for _ in $(seq 1 120); do
  pending=0
  for i in "${!admin_list[@]}"; do
    curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
      "${admin_list[$i]}/metrics" > "$OUT_DIR/metrics.after.$i.prom"
    node_pending=$(awk '
      { metric=$1; sub(/\{.*/, "", metric) }
      metric == "openiot_connections" ||
      metric == "openiot_session_proposal_queued" ||
      metric == "openiot_session_proposal_in_flight" ||
      metric == "openiot_session_proposal_in_flight_batches" ||
      metric == "openiot_session_leader_merge_queued" ||
      metric == "openiot_session_leader_merge_in_flight" { total += $NF }
      END { printf "%d", total+0 }
    ' "$OUT_DIR/metrics.after.$i.prom")
    pending=$((pending + node_pending))
  done
  if [ "$pending" -eq 0 ]; then quiescent=1; break; fi
  sleep 0.25
done
if [ "$quiescent" -ne 1 ]; then
  echo "连接关闭后30秒内session pipeline未排空，拒绝生成半截结束快照。" >&2
  exit 1
fi

# 多数派提交不等于每个RF副本已经应用。再给跟随者30秒追平，并以持久化
# applied index逐分片核对；只看ready会把“知道leader但数据仍落后”误当健康。
converged=0
for _ in $(seq 1 120); do
  for i in "${!admin_list[@]}"; do
    curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer ${tokens[$i]}" \
      "${admin_list[$i]}/metrics" > "$OUT_DIR/metrics.after.$i.prom"
  done
  if python3 - "$EXPECTED_NODES" "$EXPECTED_SHARDS" "$EXPECTED_RF" "$OUT_DIR" <<'PY'
import pathlib,re,sys
nodes,shards,rf,root=int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]),pathlib.Path(sys.argv[4])
indexes={}
for i in range(nodes):
    for line in (root/f"metrics.after.{i}.prom").read_text().splitlines():
        if not line.startswith("openiot_session_shard_applied_index{"):
            continue
        match=re.search(r'shard="([0-9]+)"',line)
        if match:
            indexes.setdefault(int(match.group(1)),[]).append(int(float(line.rsplit(None,1)[1])))
ok=(len(indexes)==shards and all(len(v)==rf and len(set(v))==1 for v in indexes.values()))
raise SystemExit(0 if ok else 1)
PY
  then
    converged=1
    break
  fi
  sleep 0.25
done
if [ "$converged" -ne 1 ]; then
  echo "session RF副本在30秒内未收敛到相同applied index。" >&2
  exit 1
fi

python3 tests/bench/connect-production-verify.py \
  "$OUT_DIR" "$EXPECTED_NODES" "$EXPECTED_SHARDS" "$EXPECTED_RF" "$CLIENTS" \
  "$MIN_RATE" "$MAX_P99_MS" "$MAX_CONNECT_MS"

echo "PASS  持久会话CONNECT五机生产验收；证据在$OUT_DIR"
