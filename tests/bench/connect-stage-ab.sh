#!/usr/bin/env bash
# 建连路径的分段归因与 clean/persistent 对照（CLU-126、CLU-129、OBS-148）。
#
# 它回答两个问题，而这两个问题此前都只能靠推断：
#
#   1. 非持久会话（压测器与绝大多数设备的默认）在集群下还提不提复制
#      日志？判据是 session_writes_total ÷ 已接受连接数，应当为 0。
#   2. 端到端的建连时间花在哪一段？判据是 connect_stage_duration_ms
#      的四个 stage，加上 session_proposal_duration_ms 的对照。
#
# 两条臂**交替跑**而不是跑完一臂再跑另一臂：同一台机器上测量顺序会
# 改结果（TST-169，集群那一栏串在单节点之后只有四分之一），交替能把
# 漂移摊到两臂上。
#
# 用法：
#   ARM=clean      NODES=3 SHARDS=8 tests/bench/connect-stage-ab.sh
#   ARM=persistent NODES=3 SHARDS=8 tests/bench/connect-stage-ab.sh
#   OUT=docs/baseline/throughput/connect-stage-ab.jsonl ... （追加原始数据）
#
# **一次调用只量一个点，用的是一个全新的集群**，与
# connect-attribution.sh 同一个协议。多轮由外层循环重复调用，而不是在
# 同一个集群里连着跑：连着跑时后面几轮量到的是前面几轮的尾巴——持久
# 会话那一臂上一轮断开时每条连接还要再提一条日志，而预置 6000 个设备
# 本身就是 6000 条元数据日志。实测同一个集群里连着跑，持久那一臂从
# 220 条/秒掉到 40 条/秒并出现 1321 次提案超时，而那不是代码的性质。
set -uo pipefail
cd "$(dirname "$0")/../.."
ARM=${ARM:-clean}
NODES=${NODES:-3}; SHARDS=${SHARDS:-8}
CONNS=${CONNS:-1500}; CONC=${CONC:-256}
MAX_VIEW_STALENESS=${MAX_VIEW_STALENESS:-10s}
SOURCE_IPS=${SOURCE_IPS:-}
ADMIN_SOURCE_IP=${ADMIN_SOURCE_IP:-}
# RF 留空保持 RF=all；例如 NODES=5 RF=3 验证选择性 session cell。
RF=${RF:-}
rf_line=""
if [ -n "$RF" ]; then rf_line="    session_replication_factor: $RF"; fi
# RTT 是共识库的 tick 间隔（CLU-132）。留空取默认的 100 毫秒。
# 调它只改提案的调度粒度，不改故障检测的时长（刻度数跟着缩放）。
RTT=${RTT:-}
rtt_line=""
if [ -n "$RTT" ]; then rtt_line="    rtt: $RTT"; fi
MQTT_TLS=${MQTT_TLS:-0}
MQTT_PROXY=${MQTT_PROXY:-0}
PROXY_IDENTITY=${PROXY_IDENTITY:-fingerprint}
case "$PROXY_IDENTITY" in der|fingerprint) ;; *) echo "PROXY_IDENTITY 只能是 der 或 fingerprint"; exit 2 ;; esac
TLS_HANDSHAKE_CONCURRENCY=${TLS_HANDSHAKE_CONCURRENCY:-0}
if [ "$MQTT_TLS" = 1 ] && [ "$MQTT_PROXY" = 1 ]; then
  echo "MQTT_TLS 与 MQTT_PROXY 只能启用一个"; exit 2
fi
# false 是容量优先的 X25519/P-256；true 恢复 Go 默认的 X25519+ML-KEM
# 混合组，用于同一装置 A/B 密钥交换成本。
POST_QUANTUM_TLS=${POST_QUANTUM_TLS:-false}
case "$POST_QUANTUM_TLS" in
  true|false) ;;
  *) echo "POST_QUANTUM_TLS 只能是 true 或 false"; exit 2 ;;
esac
RECONNECT=${RECONNECT:-0}
RECONNECT_SHIFT=${RECONNECT_SHIFT:-0}
RECONNECT_DELAY=${RECONNECT_DELAY:-1250ms}
VERIFY_HELD=${VERIFY_HELD:-0}
case "$VERIFY_HELD" in 0|1) ;; *) echo "VERIFY_HELD 只能是 0 或 1"; exit 2 ;; esac

case "$ARM" in
  clean) EXTRA="" ;;
  persistent) EXTRA="-persistent-session" ;;
  *) echo "ARM 只能是 clean 或 persistent"; exit 2 ;;
esac
BASE=${BASE:-18000}
OUT=${OUT:-/dev/null}
# 多节点会从 BASE 起每节点占用 mqtt/admin/replication 三个端口。Linux 若
# 把这些监听端口放进临时端口范围，先启动节点的 Raft 出站连接可能随机抢占
# 后启动节点的端口，表现为偶发 address already in use。
if [ "$(uname -s)" = Linux ] && [ -r /proc/sys/net/ipv4/ip_local_port_range ]; then
  read -r ephemeral_lo ephemeral_hi < /proc/sys/net/ipv4/ip_local_port_range
  listen_hi=$((BASE + (NODES - 1) * 10 + 2))
  if [ "$BASE" -le "$ephemeral_hi" ] && [ "$listen_hi" -ge "$ephemeral_lo" ]; then
    echo "BASE=$BASE 的监听区间 $BASE-$listen_hi 与 Linux 临时端口 $ephemeral_lo-$ephemeral_hi 重叠" >&2
    exit 2
  fi
fi
# 每条 TCP 流通常占一个 conntrack entry。Docker Desktop 默认上限常为
# 262144；同 namespace 压到约 26.2 万时，新连接会整批超时，而 broker 的
# Auth/Claim 指标仍为零。留 20% 给已有流、TIME_WAIT 与测量控制面。
if [ "$(uname -s)" = Linux ] && \
  [ -r /proc/sys/net/netfilter/nf_conntrack_count ] && \
  [ -r /proc/sys/net/netfilter/nf_conntrack_max ]; then
  conntrack_count=$(cat /proc/sys/net/netfilter/nf_conntrack_count)
  conntrack_max=$(cat /proc/sys/net/netfilter/nf_conntrack_max)
  conntrack_available=$((conntrack_max - conntrack_count))
  if [ "$conntrack_available" -lt 0 ]; then conntrack_available=0; fi
  conntrack_safe=$((conntrack_available * 80 / 100))
  if [ "$CONNS" -gt "$conntrack_safe" ]; then
    echo "conntrack安全余量约 $conntrack_safe，小于 CONNS=$CONNS（当前 $conntrack_count / 上限 $conntrack_max）。" >&2
    echo "请提高宿主 nf_conntrack_max、拆分网络namespace/压测机，或降低本轮连接数。" >&2
    exit 2
  fi
fi
# BSD/macOS、GNU 与 BusyBox 对 `mktemp -t` 的参数语义不同；显式模板在
# 三者都成立。不要让失败后的空 W 把临时 PKI 落到容器根目录。
# 连着跑多轮时，开跑前要先让上一轮关闭的那几千条连接退出 TIME_WAIT。
#
# 不等的后果不是「启动失败」而是**一轮废数据**：第三轮连续跑 6,000 条时实测
# 只建成 2,802 条、被拒 3,198 条，而服务端归因里只有 AuthFailed=36 与
# StoreFailed=323——其余两千多是压测端拿不到本地端口。那一轮的 260.9 条/秒
# 看上去像服务端塌了。macOS 的 net.inet.tcp.msl 默认 15 秒，即 TIME_WAIT 30 秒。
TIME_WAIT_GAP=${TIME_WAIT_GAP:-0}
if [ "${TIME_WAIT_GAP}" -gt 0 ]; then
  echo "等 ${TIME_WAIT_GAP} 秒让上一轮的端口退出 TIME_WAIT" >&2
  sleep "${TIME_WAIT_GAP}"
fi

W=$(mktemp -d "${TMPDIR:-/tmp}/openiot-stageab.XXXXXX") || exit 1
PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill -9 "$p" 2>/dev/null; done
  for p in ${PIDS[@]+"${PIDS[@]}"}; do wait "$p" 2>/dev/null || true; done
  if [ "${KEEP_WORK:-0}" = 1 ]; then echo "保留工作目录：$W" >&2; else rm -rf "$W"; fi
}
trap cleanup EXIT INT TERM
# 可把同一份预构建产物注入容器/远端 Linux，避免每台压测机各自编译出
# 不同二进制。留空保持原行为，仍从当前 worktree 构建。
if [ -n "${OPENIOTD_BIN:-}" ]; then
  cp "$OPENIOTD_BIN" "$W/openiotd" || exit 1
else
  go build -o "$W/openiotd" ./cmd/openiotd || exit 1
fi
if [ -n "${MQTTBENCH_BIN:-}" ]; then
  cp "$MQTTBENCH_BIN" "$W/mqttbench" || exit 1
else
  go build -o "$W/mqttbench" ./tools/mqttbench || exit 1
fi
chmod 0755 "$W/openiotd" "$W/mqttbench"

QUOTA='quota:
  connect_rate: 0
  handshake_rate: 0
  publish_rate_per_conn: 0
  throughput_per_conn: 0
  inbound_publish_rate: 0
  outbound_publish_rate: 0
  subscribe_rate: 0
  shadow_rate_per_thing: 0
  shadow_api_rate: 0'

ids=(a b c d e); ids=("${ids[@]:0:$NODES}")
auth_mode=insecure-none
acknowledge_insecure=true
if [ "$MQTT_TLS" = 1 ] || [ "$MQTT_PROXY" = 1 ]; then
  auth_mode=cert
  acknowledge_insecure=false
fi
pki="$W/pki"; mkdir -p "$pki"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -keyout "$pki/ca.key" -out "$pki/ca.pem" \
  -subj "/CN=stageab-ca" >/dev/null 2>&1
openssl rand -hex 32 > "$W/ticket.keys"
for id in "${ids[@]}"; do
  openssl req -newkey rsa:2048 -nodes -keyout "$pki/$id.key" -out "$pki/$id.csr" \
    -subj "/CN=$id" >/dev/null 2>&1
  printf 'subjectAltName = DNS:%s\nextendedKeyUsage = serverAuth, clientAuth\n' "$id" > "$pki/$id.ext"
  openssl x509 -req -in "$pki/$id.csr" -days 1 -CA "$pki/ca.pem" -CAkey "$pki/ca.key" \
    -CAcreateserial -extfile "$pki/$id.ext" -out "$pki/$id.pem" >/dev/null 2>&1
done
peers=""; replicas=""
for i in "${!ids[@]}"; do
  peers="${peers}
      ${ids[$i]}: \"127.0.0.1:$((BASE + i*10 + 2))\""
  replicas="${replicas}
      ${ids[$i]}: $((i+1))"
done
mqtts=(); admins=()
for i in "${!ids[@]}"; do
  id=${ids[$i]}; mqtt=$((BASE + i*10)); admin=$((BASE + i*10 + 1)); repl=$((BASE + i*10 + 2))
  mqtts+=("127.0.0.1:$mqtt"); admins+=("$admin")
  "$W/openiotd" init --data-dir "$W/$id" --node-id "$id" --log-level error >/dev/null
  mode=join; seeds="  seeds: [\"127.0.0.1:$((BASE + 2))\"]"
  if [ "$i" = 0 ]; then mode=bootstrap; seeds=""; fi
  if [ "$MQTT_TLS" = 1 ]; then
    mqtt_listeners="  mqtt_tls: {enabled: true, addr: \"127.0.0.1:$mqtt\", require_client_cert: true, client_ca_file: $W/$id/pki/ca-chain.pem, session_ticket_keys_file: $W/ticket.keys, post_quantum_key_exchange: $POST_QUANTUM_TLS}\n  mqtt_plain: {enabled: false}"
  elif [ "$MQTT_PROXY" = 1 ]; then
    mqtt_listeners="  mqtt_tls: {enabled: false}\n  mqtt_plain: {enabled: true, addr: \"127.0.0.1:$mqtt\", proxy_protocol: v2, trusted_proxies: [\"127.0.0.0/8\"], proxy_header_timeout: 3s}"
  else
    mqtt_listeners="  mqtt_tls: {enabled: false}\n  mqtt_plain: {enabled: true, addr: \"127.0.0.1:$mqtt\"}"
  fi
  cat > "$W/$id.yaml" <<YAML
node:
  id: $id
  mode: $mode
  data_dir: $W/$id
$seeds
cluster:
  voters: $NODES
  max_view_staleness: $MAX_VIEW_STALENESS
  replication:
    addr: "127.0.0.1:$repl"
    cert_file: $pki/$id.pem
    key_file: $pki/$id.key
    ca_file: $pki/ca.pem
    session_shards: $SHARDS
$rf_line
$rtt_line
    peers:$peers
    replicas:$replicas
listeners:
$(printf '%b' "$mqtt_listeners")
  admin: {enabled: true, addr: "127.0.0.1:$admin"}
auth:
  mode: $auth_mode
  acknowledge_insecure: $acknowledge_insecure
log: {level: warn, format: text}
broker:
  tls_handshake_concurrency: $TLS_HANDSHAKE_CONCURRENCY
$QUOTA
YAML
done
for id in "${ids[@]:1}"; do rm -rf "$W/$id/pki"; cp -R "$W/a/pki" "$W/$id/pki"; done
TLS_ARGS=()
if [ "$MQTT_TLS" = 1 ] || [ "$MQTT_PROXY" = 1 ]; then
  mkdir -p "$W/client"
	cert_policy=connect-bench
	cat > "$W/a/policies/$cert_policy.json" <<'JSON'
{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}
JSON
  "$W/openiotd" cert issue --data-dir "$W/a" --thing mqttbench \
    --policy "$cert_policy" --out "$W/client" >/dev/null || exit 1
  if [ "$MQTT_TLS" = 1 ]; then
    TLS_ARGS=(-tls-ca "$W/a/pki/root-ca.pem" -tls-cert "$W/client/mqttbench.pem" \
      -tls-key "$W/client/mqttbench.key" -tls-server-name localhost)
  else
    TLS_ARGS=(-proxy-v2-cert "$W/client/mqttbench.pem")
		if [ "$PROXY_IDENTITY" = fingerprint ]; then TLS_ARGS+=(-proxy-v2-fingerprint-only); fi
  fi
fi
RECONNECT_ARGS=()
reconnect_label=""
if [ "$RECONNECT" = 1 ]; then
  RECONNECT_ARGS=(-reconnect -reconnect-delay "$RECONNECT_DELAY" -reconnect-shift "$RECONNECT_SHIFT")
  reconnect_label="-reconnect"
  if [ "$RECONNECT_SHIFT" != 0 ]; then reconnect_label="${reconnect_label}-shift${RECONNECT_SHIFT}"; fi
fi
VERIFY_HELD_ARGS=()
if [ "$VERIFY_HELD" = 1 ]; then VERIFY_HELD_ARGS=(-verify-held); fi
SOURCE_ARGS=()
if [ -n "$SOURCE_IPS" ]; then SOURCE_ARGS=(-source-ip "$SOURCE_IPS"); fi
ADMIN_SOURCE_ARGS=()
if [ -n "$ADMIN_SOURCE_IP" ]; then ADMIN_SOURCE_ARGS=(-admin-source-ip "$ADMIN_SOURCE_IP"); fi
for i in "${!ids[@]}"; do
  "$W/openiotd" serve --config "$W/${ids[$i]}.yaml" >"$W/${ids[$i]}.log" 2>&1 &
  PIDS+=("$!")
done
for a in "${admins[@]}"; do
  ok=0
  for _ in $(seq 1 600); do curl -sf -o /dev/null "http://127.0.0.1:$a/readyz" && { ok=1; break; }; sleep 0.2; done
  [ "$ok" = 1 ] || { echo "节点未就绪"; tail -5 "$W"/*.log; exit 1; }
done
all=$(IFS=,; echo "${mqtts[*]}")
TOKEN=$(cat "$W/a/admin-token")
# 每个节点的管理面令牌是各自 init 出来的，不能统一用 a 的那个：
# 拿 a 的令牌去问 b 会得到 401，而 curl -sf 对 401 是静默的——于是
# 总和里少了两个节点，而少掉的那部分恰好是 leader 不在本地的那些提案。
# 第一版就是这样把 remote 读成 0 的。
TOKENS=()
for id in "${ids[@]}"; do TOKENS+=("$(cat "$W/$id/admin-token")"); done

# scrape 把全部节点的 /metrics 拼起来。各节点各自计数，归因要看总和。
scrape() {
  local ok=0 i
  for i in "${!admins[@]}"; do
    if curl -sf -H "Authorization: Bearer ${TOKENS[$i]}" \
      "http://127.0.0.1:${admins[$i]}/metrics" > "$W/m.${ids[$i]}.txt"; then
      ok=$((ok+1))
      cat "$W/m.${ids[$i]}.txt"
    fi
  done
  # 少一个节点答话，分段与提案的总和就少一份，而那会被读成"这一段很便宜"。
  # 各节点的管理面令牌是各自 init 出来的，统一用 a 的那个之前必须先确认
  # 它在别的节点上也认——元数据是复制的，所以通常认，但不能默认。
  if [ "$ok" != "${#admins[@]}" ]; then
    echo "错误：只有 $ok/${#admins[@]} 个节点返回了指标，总和不可用" >&2
    return 1
  fi
}

# msum 取一个指标在全部节点上的和。$1 指标名，$2 可选的标签子串。
msum() {
  awk -v name="$1" -v want="${2:-}" '
    {
      metric = $1
      sub(/\{.*/, "", metric)
    }
    metric == name && (want == "" || index($0, want) > 0) {
      sum += $NF
    }
    END { printf "%.3f", sum+0 }
  ' "$W/metrics.txt"
}

pending_result="$W/result.pending.jsonl"
append_pending_result() {
  local target=$1
  if [ "$OUT" != /dev/null ] && [ -s "$pending_result" ]; then
    cat "$pending_result" >> "$target"
  fi
}

printf '%-12s %10s %8s %8s %10s %10s %10s %10s %8s\n' \
  arm 'conn/s' p50ms p99ms auth_ms load_ms claim_ms write_ms writes

scrape > "$W/metrics.txt"
transport=plain
if [ "$MQTT_TLS" = 1 ]; then transport=mtls-cert-auth; fi
if [ "$MQTT_PROXY" = 1 ]; then transport="proxy-v2-$PROXY_IDENTITY"; fi
if ! json=$("$W/mqttbench" -scenario connect -broker "$all" -admin "http://127.0.0.1:${admins[0]}" \
  -token "$TOKEN" -clients "$CONNS" -concurrency "$CONC" -hold 3s -warmup 1s -duration 3s \
  -prefix "sa${ARM:0:1}" -label "stage-ab/${ARM}-${transport}${reconnect_label}-n${NODES}-s${SHARDS}-rtt${RTT:-default}" \
  -out "$pending_result" ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} \
  ${SOURCE_ARGS[@]+"${SOURCE_ARGS[@]}"} \
  ${ADMIN_SOURCE_ARGS[@]+"${ADMIN_SOURCE_ARGS[@]}"} \
  ${RECONNECT_ARGS[@]+"${RECONNECT_ARGS[@]}"} $EXTRA \
  ${VERIFY_HELD_ARGS[@]+"${VERIFY_HELD_ARGS[@]}"} \
  2> >(tee "$W/mqttbench.stderr" >&2)); then
  echo "测量失败：" >&2
  tail -20 "$W/mqttbench.stderr" >&2
  exit 1
fi
cps=$(printf '%s' "$json" | sed -n 's/.*"connectPerSec": *\([0-9.]*\).*/\1/p' | tail -1)
p50=$(printf '%s' "$json" | sed -n 's/.*"connectP50Ms": *\([0-9.]*\).*/\1/p' | tail -1)
p99=$(printf '%s' "$json" | sed -n 's/.*"connectP99Ms": *\([0-9.]*\).*/\1/p' | tail -1)
held=$(printf '%s' "$json" | sed -n 's/.*"heldConns": *\([0-9]*\).*/\1/p' | tail -1)
errs=$(printf '%s' "$json" | sed -n 's/.*"errors": *\([0-9]*\).*/\1/p' | tail -1)
resumed=$(printf '%s' "$json" | sed -n 's/.*"tlsResumed": *\([0-9]*\).*/\1/p' | tail -1)
if [ -z "$cps" ]; then
  echo "测量失败：结果缺少 connectPerSec" >&2
  printf '%s\n' "$json" | tail -20 >&2
  exit 1
fi

# mqttbench 的客户端都成功，不代表集群仍可用：资源紧张时 broker 可能在保持
# 窗口结束或连接关闭期间被 OOM killer 杀死。任何节点死亡都让吞吐与指标无效。
for i in "${!PIDS[@]}"; do
  if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
    echo "测量失败：broker节点 ${ids[$i]} 在结果采集前退出。" >&2
    tail -20 "$W/${ids[$i]}.log" >&2
    append_pending_result "${OUT}.invalid"
    exit 1
  fi
done

b_sum_auth=$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="auth"')
b_sum_load=$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="session_load"')
b_sum_claim=$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="owner_claim"')
b_sum_write=$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="session_write"')
b_writes=$(msum 'openiot_session_writes_total')
b_cnt=$(msum 'openiot_connect_stage_duration_ms_count' 'stage="auth"')
b_plocal=$(msum 'openiot_session_proposals_total' 'leader="local"')
b_premote=$(msum 'openiot_session_proposals_total' 'leader="remote"')
b_punk=$(msum 'openiot_session_proposals_total' 'leader="unknown"')
b_perr=$(msum 'openiot_session_proposal_errors_total')
b_raft=$(msum 'openiot_session_raft_proposals_total')
b_merge_access=$(msum 'openiot_session_leader_merge_access_batches_total')
b_merge_logical=$(msum 'openiot_session_leader_merge_logical_total')
b_merge_physical=$(msum 'openiot_session_leader_merge_physical_total')
b_merge_full=$(msum 'openiot_session_leader_merge_full_total')
b_merge_drained=$(msum 'openiot_session_leader_merge_drained_total')
b_merge_blocked=$(msum 'openiot_session_leader_merge_blocked_total')
b_rlogical=$(msum 'openiot_session_remote_reads_total')
b_rbatch=$(msum 'openiot_session_remote_read_batches_total')
b_rclaim=$(msum 'openiot_session_remote_read_claim_piggyback_total')
b_rerr=$(msum 'openiot_session_remote_read_errors_total')
b_pbackpressure=$(msum 'openiot_session_proposal_backpressured_total')
b_oclaims=$(msum 'openiot_ownership_remote_claims_total')
b_orpcs=$(msum 'openiot_ownership_claim_rpcs_total')
b_oerrors=$(msum 'openiot_ownership_claim_errors_total')
b_obackpressure=$(msum 'openiot_ownership_claim_backpressured_total')
b_tlsfull=$(msum 'openiot_tls_handshakes_total' 'result="full"')
b_tlsresumed=$(msum 'openiot_tls_handshakes_total' 'result="resumed"')
b_tlsfailed=$(msum 'openiot_tls_handshakes_total' 'result="failed"')
b_tlsthrottled=$(msum 'openiot_tls_handshakes_total' 'result="throttled"')
b_conn_auth_failed=$(msum 'openiot_connections_total' 'result="AuthFailed"')
b_conn_throttled=$(msum 'openiot_connections_total' 'result="Throttled"')
b_conn_view_stale=$(msum 'openiot_connections_total' 'result="ViewStale"')
b_conn_store_failed=$(msum 'openiot_connections_total' 'result="StoreFailed"')

# mqttbench 返回前会关闭全部连接。持久会话的关闭还会同步 Save 一次，若在
# 这批 Save 只完成一部分时抓指标，leader merge logical 会落在 N 到 2N
# 之间，看起来像重复提案。等连接和两级提案队列都排空，使结束快照对应一
# 个确定边界；超时不能继续产出一条貌似有效、实则只含半截清理流量的数据。
quiescent=0
for _ in $(seq 1 300); do
  if ! scrape > "$W/metrics.quiesce.txt"; then
    break
  fi
  read -r live queued inflight batches leader_queued leader_inflight <<EOF
$(awk '
  { metric = $1; sub(/\{.*/, "", metric) }
  metric == "openiot_connections" { live += $NF }
  metric == "openiot_session_proposal_queued" { queued += $NF }
  metric == "openiot_session_proposal_in_flight" { inflight += $NF }
  metric == "openiot_session_proposal_in_flight_batches" { batches += $NF }
  metric == "openiot_session_leader_merge_queued" { leader_queued += $NF }
  metric == "openiot_session_leader_merge_in_flight" { leader_inflight += $NF }
  END { printf "%d %d %d %d %d %d\n", live, queued, inflight, batches, leader_queued, leader_inflight }
' "$W/metrics.quiesce.txt")
EOF
  if [ "$live" = 0 ] && [ "$queued" = 0 ] && [ "$inflight" = 0 ] && \
    [ "$batches" = 0 ] && [ "$leader_queued" = 0 ] && [ "$leader_inflight" = 0 ]; then
    quiescent=1
    break
  fi
  sleep 0.1
done
if [ "$quiescent" != 1 ]; then
  echo "测量失败：连接关闭后30秒内session pipeline未排空（connections=${live:-?} queued=${queued:-?} in_flight=${inflight:-?} batches=${batches:-?} leader_queued=${leader_queued:-?} leader_in_flight=${leader_inflight:-?}）。" >&2
  append_pending_result "${OUT}.invalid"
  exit 1
fi

# 基线变量已从压测前的 metrics.txt 取出；现在用完全排空后的结束快照覆盖
# 文件。这样启动期的会话过期清理仍会被基线扣掉。
if ! scrape > "$W/metrics.txt"; then
  echo "测量失败：无法从全部 $NODES 个broker采集结束指标。" >&2
  append_pending_result "${OUT}.invalid"
  exit 1
fi
cnt=$(msum 'openiot_connect_stage_duration_ms_count' 'stage="auth"')
d_cnt=$(awk -v a="$cnt" -v b="$b_cnt" 'BEGIN{print a-b}')
avg() { awk -v s="$1" -v b="$2" -v n="$d_cnt" 'BEGIN{ if (n<=0) print "n/a"; else printf "%.3f", (s-b)/n }'; }
a_auth=$(avg "$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="auth"')" "$b_sum_auth")
a_load=$(avg "$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="session_load"')" "$b_sum_load")
a_claim=$(avg "$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="owner_claim"')" "$b_sum_claim")
a_write=$(avg "$(msum 'openiot_connect_stage_duration_ms_sum' 'stage="session_write"')" "$b_sum_write")
w_per=$(awk -v w="$(msum 'openiot_session_writes_total')" -v b="$b_writes" -v n="$d_cnt" \
  'BEGIN{ if (n<=0) print "n/a"; else printf "%.2f", (w-b)/n }')
plocal=$(awk -v a="$(msum 'openiot_session_proposals_total' 'leader="local"')" -v b="$b_plocal" 'BEGIN{printf "%d", a-b}')
premote=$(awk -v a="$(msum 'openiot_session_proposals_total' 'leader="remote"')" -v b="$b_premote" 'BEGIN{printf "%d", a-b}')
punk=$(awk -v a="$(msum 'openiot_session_proposals_total' 'leader="unknown"')" -v b="$b_punk" 'BEGIN{printf "%d", a-b}')
perr=$(awk -v a="$(msum 'openiot_session_proposal_errors_total')" -v b="$b_perr" 'BEGIN{printf "%d", a-b}')
raft=$(awk -v a="$(msum 'openiot_session_raft_proposals_total')" -v b="$b_raft" 'BEGIN{printf "%d", a-b}')
merge_access=$(awk -v a="$(msum 'openiot_session_leader_merge_access_batches_total')" -v b="$b_merge_access" 'BEGIN{printf "%d", a-b}')
merge_logical=$(awk -v a="$(msum 'openiot_session_leader_merge_logical_total')" -v b="$b_merge_logical" 'BEGIN{printf "%d", a-b}')
merge_physical=$(awk -v a="$(msum 'openiot_session_leader_merge_physical_total')" -v b="$b_merge_physical" 'BEGIN{printf "%d", a-b}')
merge_full=$(awk -v a="$(msum 'openiot_session_leader_merge_full_total')" -v b="$b_merge_full" 'BEGIN{printf "%d", a-b}')
merge_drained=$(awk -v a="$(msum 'openiot_session_leader_merge_drained_total')" -v b="$b_merge_drained" 'BEGIN{printf "%d", a-b}')
merge_blocked=$(awk -v a="$(msum 'openiot_session_leader_merge_blocked_total')" -v b="$b_merge_blocked" 'BEGIN{printf "%d", a-b}')
connect_writes=$(awk -v w="$(msum 'openiot_session_writes_total')" -v b="$b_writes" 'BEGIN{printf "%d", w-b}')
nonconnect_logical=$(awk -v l="$merge_logical" -v c="$connect_writes" 'BEGIN{printf "%d", l-c}')
rlogical=$(awk -v a="$(msum 'openiot_session_remote_reads_total')" -v b="$b_rlogical" 'BEGIN{printf "%d", a-b}')
rbatch=$(awk -v a="$(msum 'openiot_session_remote_read_batches_total')" -v b="$b_rbatch" 'BEGIN{printf "%d", a-b}')
rclaim=$(awk -v a="$(msum 'openiot_session_remote_read_claim_piggyback_total')" -v b="$b_rclaim" 'BEGIN{printf "%d", a-b}')
rerr=$(awk -v a="$(msum 'openiot_session_remote_read_errors_total')" -v b="$b_rerr" 'BEGIN{printf "%d", a-b}')
pbackpressure=$(awk -v a="$(msum 'openiot_session_proposal_backpressured_total')" -v b="$b_pbackpressure" 'BEGIN{printf "%d", a-b}')
oclaims=$(awk -v a="$(msum 'openiot_ownership_remote_claims_total')" -v b="$b_oclaims" 'BEGIN{printf "%d", a-b}')
orpcs=$(awk -v a="$(msum 'openiot_ownership_claim_rpcs_total')" -v b="$b_orpcs" 'BEGIN{printf "%d", a-b}')
oerrors=$(awk -v a="$(msum 'openiot_ownership_claim_errors_total')" -v b="$b_oerrors" 'BEGIN{printf "%d", a-b}')
obackpressure=$(awk -v a="$(msum 'openiot_ownership_claim_backpressured_total')" -v b="$b_obackpressure" 'BEGIN{printf "%d", a-b}')
tlsfull=$(awk -v a="$(msum 'openiot_tls_handshakes_total' 'result="full"')" -v b="$b_tlsfull" 'BEGIN{printf "%d", a-b}')
tlsresumed=$(awk -v a="$(msum 'openiot_tls_handshakes_total' 'result="resumed"')" -v b="$b_tlsresumed" 'BEGIN{printf "%d", a-b}')
tlsfailed=$(awk -v a="$(msum 'openiot_tls_handshakes_total' 'result="failed"')" -v b="$b_tlsfailed" 'BEGIN{printf "%d", a-b}')
tlsthrottled=$(awk -v a="$(msum 'openiot_tls_handshakes_total' 'result="throttled"')" -v b="$b_tlsthrottled" 'BEGIN{printf "%d", a-b}')
conn_auth_failed=$(awk -v a="$(msum 'openiot_connections_total' 'result="AuthFailed"')" -v b="$b_conn_auth_failed" 'BEGIN{printf "%d", a-b}')
conn_throttled=$(awk -v a="$(msum 'openiot_connections_total' 'result="Throttled"')" -v b="$b_conn_throttled" 'BEGIN{printf "%d", a-b}')
conn_view_stale=$(awk -v a="$(msum 'openiot_connections_total' 'result="ViewStale"')" -v b="$b_conn_view_stale" 'BEGIN{printf "%d", a-b}')
conn_store_failed=$(awk -v a="$(msum 'openiot_connections_total' 'result="StoreFailed"')" -v b="$b_conn_store_failed" 'BEGIN{printf "%d", a-b}')

printf '%-12s %10.1f %8.0f %8.0f %10s %10s %10s %10s %8s\n' \
  "$ARM" "$cps" "$p50" "$p99" "$a_auth" "$a_load" "$a_claim" "$a_write" "$w_per"
printf '  建成 %s 条，被拒 %s 条；接入提案 local=%s remote=%s unknown=%s，Raft 物理提案=%s，失败=%s（走完全程的连接 %s）\n' \
  "$held" "${errs:-0}" "$plocal" "$premote" "$punk" "$raft" "$perr" "$d_cnt"
printf '  Session proposal pipeline：背压入队=%s；当前排队=%s 在途逻辑=%s 在途批次=%s；历史峰值排队=%s 在途批次=%s；缓存请求=%s 缓存批次=%s\n' \
	"$pbackpressure" "$(msum 'openiot_session_proposal_queued')" \
  "$(msum 'openiot_session_proposal_in_flight')" \
  "$(msum 'openiot_session_proposal_in_flight_batches')" \
  "$(msum 'openiot_session_proposal_peak_queued')" \
  "$(msum 'openiot_session_proposal_peak_in_flight_batches')" \
  "$(msum 'openiot_session_proposal_cached_requests')" \
  "$(msum 'openiot_session_proposal_cached_batches')"
printf '  Session leader merge：当前排队=%s 在途Raft=%s；历史峰值排队=%s 在途Raft=%s\n' \
  "$(msum 'openiot_session_leader_merge_queued')" \
  "$(msum 'openiot_session_leader_merge_in_flight')" \
  "$(msum 'openiot_session_leader_merge_peak_queued')" \
  "$(msum 'openiot_session_leader_merge_peak_in_flight')"
if [ "$merge_physical" -gt 0 ]; then
  merge_access_ratio=$(awk -v n="$merge_access" -v p="$merge_physical" 'BEGIN{printf "%.2f", n/p}')
  merge_logical_ratio=$(awk -v n="$merge_logical" -v p="$merge_physical" 'BEGIN{printf "%.2f", n/p}')
  printf '  Session leader 合批宽度：Access批=%s 逻辑命令=%s 物理日志=%s；平均 %s Access批/日志，%s 命令/日志\n' \
    "$merge_access" "$merge_logical" "$merge_physical" "$merge_access_ratio" "$merge_logical_ratio"
  printf '    CONNECT路径写=%s，非CONNECT逻辑命令=%s（持久会话轮主要是断开Save）\n' \
    "$connect_writes" "$nonconnect_logical"
  printf '    装满64条=%s，队列快照耗尽=%s，合并约束截断=%s\n' \
    "$merge_full" "$merge_drained" "$merge_blocked"
fi
if [ "$rlogical" -gt 0 ]; then
  rratio=$(awk -v l="$rlogical" -v p="$rclaim" -v b="$rbatch" 'BEGIN{if(b==0) print "n/a"; else printf "%.2f", (l-p)/b}')
  printf '  非本地 session 读取：逻辑=%s Claim携带=%s 独立批量RPC=%s 独立合批系数=%s 错误=%s\n' \
    "$rlogical" "$rclaim" "$rbatch" "$rratio" "$rerr"
fi
if [ "$oclaims" -gt 0 ]; then
  oratio=$(awk -v l="$oclaims" -v r="$orpcs" 'BEGIN{if(r==0) print "n/a"; else printf "%.2f", l/r}')
  printf '  Ownership Claim：逻辑=%s 物理RPC=%s 合批系数=%s 背压入队=%s 错误=%s；当前排队=%s 在途逻辑=%s 在途RPC=%s\n' \
    "$oclaims" "$orpcs" "$oratio" "$obackpressure" "$oerrors" \
    "$(msum 'openiot_ownership_claim_queued')" "$(msum 'openiot_ownership_claim_in_flight')" \
    "$(msum 'openiot_ownership_claim_in_flight_rpcs')"
fi
if [ "$MQTT_TLS" = 1 ]; then
  printf '  TLS 服务端：全握手=%s 恢复=%s 失败=%s 限流=%s，历史在途峰值和=%s / 容量和=%s\n' \
    "$tlsfull" "$tlsresumed" "$tlsfailed" "$tlsthrottled" "$(msum 'openiot_tls_handshake_peak')" \
    "$(msum 'openiot_tls_handshake_capacity')"
fi
if [ "$RECONNECT" = 1 ]; then
  printf '  TLS session 恢复 %s/%s 条\n' "${resumed:-0}" "$held"
fi
if [ "${errs:-0}" -gt 0 ]; then
  printf '  拒绝归因：AuthFailed=%s Throttled=%s ViewStale=%s StoreFailed=%s\n' \
    "$conn_auth_failed" "$conn_throttled" "$conn_view_stale" "$conn_store_failed" >&2
  printf '  本轮有建连失败，原始结果：%s\n' "$json" >&2
  append_pending_result "$OUT"
  exit 1
fi
for i in "${!ids[@]}"; do
  awk -v id="${ids[$i]}" '
    index($0,"openiot_connect_stage_duration_ms_sum") == 1 && index($0,"stage=\"session_write\"") > 0 { ws = $NF }
    index($0,"openiot_connect_stage_duration_ms_count") == 1 && index($0,"stage=\"session_write\"") > 0 { wc = $NF }
    index($0,"openiot_session_proposals_total") == 1 && index($0,"leader=\"local\"") > 0 { pl = $NF }
    index($0,"openiot_session_proposals_total") == 1 && index($0,"leader=\"remote\"") > 0 { pr = $NF }
    index($0,"openiot_session_proposal_duration_ms_sum") == 1 { ps = $NF }
    index($0,"openiot_session_proposal_duration_ms_count") == 1 { pc = $NF }
    END {
      # 两个均值并排：write_ms 是 broker 看到的一次会话写入，propose_ms 是
      # 共识层看到的一次提案。两者接近就说明时间全在共识那一轮里，存储层
      # 在它之外没花什么——这是 CLU-126 往下再切一刀所需的那个对照。
      if (wc+0 > 0) printf "    节点 %s：write_ms=%.1f propose_ms=%.1f 提案 local=%d remote=%d\n", \
        id, ws/wc, (pc+0 > 0 ? ps/pc : 0), pl, pr
    }' "$W/m.${ids[$i]}.txt"
done
append_pending_result "$OUT"
