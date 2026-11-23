#!/usr/bin/env bash
# Linux 单节点 20 万长连接验收。默认是设备直连 mTLS；TRANSPORT=proxy-v2
# 时量 TLS 终结外移后的 broker 档（不包含终结器进程自身资源）。
#
# 这条脚本不替用户改 sysctl 或网卡地址；它在开跑前计算源端口容量并明确
# 拒绝不可能完成的配置。百万连接集群应在五个 broker 节点上各跑 20 万，
# 并由多个 load generator 分担 CPU。单个 generator 若要超过单源 IP 的
# 临时端口范围，用 SOURCE_IPS 提供已配置在网卡上的多个地址。
#
# 必需环境变量：
#   BROKER=10.0.0.10:8883
#   ADMIN=http://10.0.0.10:9090
#   TOKEN_FILE=/secure/admin-token
#   TLS_CA=/secure/root-ca.pem TLS_CERT=/secure/device.pem TLS_KEY=/secure/device.key
#   TLS_SERVER_NAME=mqtt.example.internal
#   SOURCE_IPS=10.1.0.11,10.1.0.12,...   # 默认端口范围按 80% 余量通常至少 9 个
#   ADMIN_SOURCE_IP=10.1.0.250                # 可选；管理采样独占源地址/端口池
#   BROKER_PID=1234                         # 可选；broker 与脚本同机时采 VmHWM
#   LOADGEN_INDEX=0                         # 多压测机依次设 0..4，隔离 clientId
#   START_AT_UNIX=1795300000                 # 可选；五台使用同一未来秒数
#   HOLD_UNTIL_UNIX=1795300600                # 可选；五台保持到同一未来秒数
#
# 默认假定设备 linux-density-c000000 ... 已预置；首次小规模试跑可 SETUP=1。
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ "$(uname -s)" != Linux ]; then
  echo "此验收只在 Linux 运行（需要 /proc/net/sockstat 与 Linux RSS）。" >&2
  exit 2
fi

: "${BROKER:?请设置 BROKER，多个地址以逗号分隔}"
: "${ADMIN:?请设置 ADMIN}"
: "${TOKEN_FILE:?请设置 TOKEN_FILE}"
TRANSPORT=${TRANSPORT:-mtls}
INGRESS_MODE=${INGRESS_MODE:-direct}
case "$INGRESS_MODE" in
  direct|shared-lb) ;;
  *) echo "INGRESS_MODE 只能是 direct 或 shared-lb。" >&2; exit 2 ;;
esac
PROXY_IDENTITY=${PROXY_IDENTITY:-der}
case "$PROXY_IDENTITY" in der|fingerprint) ;; *) echo "PROXY_IDENTITY 只能是 der 或 fingerprint。" >&2; exit 2 ;; esac
case "$TRANSPORT" in
  mtls)
    : "${TLS_CA:?TRANSPORT=mtls 时请设置 TLS_CA}"
    : "${TLS_CERT:?TRANSPORT=mtls 时请设置 TLS_CERT}"
    : "${TLS_KEY:?TRANSPORT=mtls 时请设置 TLS_KEY}"
    : "${TLS_SERVER_NAME:?TRANSPORT=mtls 时请设置 TLS_SERVER_NAME}"
    ;;
  proxy-v2)
    : "${PROXY_CERT:?TRANSPORT=proxy-v2 时请设置 PROXY_CERT（叶证书 PEM）}"
    ;;
  *) echo "TRANSPORT 只能是 mtls 或 proxy-v2。" >&2; exit 2 ;;
esac

CLIENTS=${CLIENTS:-200000}
CONC=${CONC:-4096}
HOLD=${HOLD:-60s}
PREFIX=${PREFIX:-linux-density}
LOADGEN_INDEX=${LOADGEN_INDEX:-0}
case "$LOADGEN_INDEX" in
  *[!0-9]*|'') echo "LOADGEN_INDEX 必须是非负整数。" >&2; exit 2 ;;
esac
CLIENT_OFFSET=${CLIENT_OFFSET:-$((LOADGEN_INDEX * CLIENTS))}
SETUP=${SETUP:-0}
SETUP_CONC=${SETUP_CONC:-32}
SOURCE_IPS=${SOURCE_IPS:-}
ADMIN_SOURCE_IP=${ADMIN_SOURCE_IP:-}
BROKER_PID=${BROKER_PID:-}
START_AT_UNIX=${START_AT_UNIX:-0}
HOLD_UNTIL_UNIX=${HOLD_UNTIL_UNIX:-0}
OUT_DIR=${OUT_DIR:-"docs/baseline/throughput/linux-density-$(date -u +%Y%m%dT%H%M%SZ)"}
MQTTBENCH=${MQTTBENCH:-}

mkdir -p "$OUT_DIR"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/openiot-density.XXXXXX") || exit 1
bench_pid=""
sampler_pid=""
cleanup() {
  if [ -n "$sampler_pid" ]; then kill "$sampler_pid" 2>/dev/null || true; fi
  if [ -n "$bench_pid" ]; then kill "$bench_pid" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

if [ -z "$MQTTBENCH" ]; then
  MQTTBENCH="$WORK/mqttbench"
  go build -o "$MQTTBENCH" ./tools/mqttbench
fi

if [ -n "$BROKER_PID" ] && [ ! -r "/proc/$BROKER_PID/status" ]; then
  echo "BROKER_PID=$BROKER_PID 不存在或无法读取 /proc/$BROKER_PID/status。" >&2
  exit 2
fi
case "$CLIENT_OFFSET" in
  *[!0-9]*|'') echo "CLIENT_OFFSET 必须是非负整数。" >&2; exit 2 ;;
esac
case "$START_AT_UNIX" in
  *[!0-9]*|'') echo "START_AT_UNIX 必须是非负 Unix 秒。" >&2; exit 2 ;;
esac
case "$HOLD_UNTIL_UNIX" in
  *[!0-9]*|'') echo "HOLD_UNTIL_UNIX 必须是非负 Unix 秒。" >&2; exit 2 ;;
esac
if [ "$START_AT_UNIX" -gt 0 ] && [ "$SETUP" != 0 ]; then
  echo "同步密度轮要求 SETUP=0；请先预置全部设备，避免各片在统一开始时间后才做元数据写入。" >&2
  exit 2
fi
if [ "$HOLD_UNTIL_UNIX" -gt 0 ] && [ "$HOLD_UNTIL_UNIX" -le "$START_AT_UNIX" ]; then
  echo "HOLD_UNTIL_UNIX 必须晚于 START_AT_UNIX。" >&2
  exit 2
fi

need_fds=$((CLIENTS + 8192))
soft_fds=$(ulimit -Sn)
if [ "$soft_fds" -lt "$need_fds" ]; then
  ulimit -n "$need_fds" 2>/dev/null || {
    echo "文件描述符上限 $soft_fds，小于所需 $need_fds；先提高 LimitNOFILE/ulimit。" >&2
    exit 2
  }
fi

read -r port_lo port_hi < /proc/sys/net/ipv4/ip_local_port_range
port_count=$((port_hi - port_lo + 1))
source_count=1
if [ -n "$SOURCE_IPS" ]; then
  IFS=',' read -r -a source_list <<< "$SOURCE_IPS"
  source_count=${#source_list[@]}
  for source_ip in "${source_list[@]}"; do
    case "$source_ip" in
      *:*) echo "SOURCE_IPS 的容量公式当前只支持 IPv4，不能使用 $source_ip。" >&2; exit 2 ;;
    esac
  done
fi
IFS=',' read -r -a broker_list <<< "$BROKER"
broker_count=${#broker_list[@]}
if [ "$INGRESS_MODE" = shared-lb ] && [ "$broker_count" != 1 ]; then
  echo "INGRESS_MODE=shared-lb 要求 BROKER 只有一个负载均衡入口，当前有 $broker_count 个。" >&2
  exit 2
fi
# 留 20% 给 TIME_WAIT、系统服务与分布不均；这里宁可提前拒绝，也不要跑到
# 18 万条才以 cannot assign requested address 结束。mqttbench 在 Linux 上为
# 显式源 IPv4 启用 IP_BIND_ADDRESS_NO_PORT，使同一源端口可以按完整四元组
# 复用于不同 broker；因此 broker_count 可以进入容量公式。
safe_ports=$((port_count * source_count * broker_count * 80 / 100))
if [ "$CLIENTS" -gt "$safe_ports" ]; then
  echo "源端口安全容量约 $safe_ports，小于 CLIENTS=$CLIENTS。" >&2
  echo "当前范围 $port_lo-$port_hi，源 IP $source_count 个，broker $broker_count 个；请增加 SOURCE_IPS/load generator。" >&2
  exit 2
fi

conntrack_count=unavailable
conntrack_max=unavailable
conntrack_safe=unavailable
if [ -r /proc/sys/net/netfilter/nf_conntrack_count ] && \
  [ -r /proc/sys/net/netfilter/nf_conntrack_max ]; then
  conntrack_count=$(cat /proc/sys/net/netfilter/nf_conntrack_count)
  conntrack_max=$(cat /proc/sys/net/netfilter/nf_conntrack_max)
  conntrack_available=$((conntrack_max - conntrack_count))
  if [ "$conntrack_available" -lt 0 ]; then conntrack_available=0; fi
  conntrack_safe=$((conntrack_available * 80 / 100))
  if [ "$CLIENTS" -gt "$conntrack_safe" ]; then
    echo "conntrack安全余量约 $conntrack_safe，小于 CLIENTS=$CLIENTS。" >&2
    echo "当前 $conntrack_count / 上限 $conntrack_max；请提高宿主nf_conntrack_max或拆分load generator。" >&2
    exit 2
  fi
fi

{
  echo "utc=$(date -u +%FT%TZ)"
  echo "kernel=$(uname -srvm)"
  echo "go=$(go version)"
  echo "clients=$CLIENTS concurrency=$CONC hold=$HOLD"
  echo "transport=$TRANSPORT"
  echo "ingress_mode=$INGRESS_MODE"
  echo "loadgen_index=$LOADGEN_INDEX client_offset=$CLIENT_OFFSET client_range=$CLIENT_OFFSET-$((CLIENT_OFFSET + CLIENTS - 1))"
  echo "brokers=$BROKER source_ips=${SOURCE_IPS:-default}"
  echo "admin_source_ip=${ADMIN_SOURCE_IP:-default}"
  echo "broker_pid=${BROKER_PID:-remote-or-unspecified}"
  echo "start_at_unix=$START_AT_UNIX"
  echo "hold_until_unix=$HOLD_UNTIL_UNIX"
  echo "ephemeral_ports=$port_lo-$port_hi safe_capacity=$safe_ports"
  echo "conntrack_count=$conntrack_count conntrack_max=$conntrack_max conntrack_safe_capacity=$conntrack_safe"
  if [ -n "$SOURCE_IPS" ]; then
    echo "source_port_allocation=ip_bind_address_no_port"
  else
    echo "source_port_allocation=implicit_connect_assignment"
  fi
  echo "nofile=$(ulimit -Sn)"
} > "$OUT_DIR/environment.txt"

admin_token=$(cat "$TOKEN_FILE")

args=(
  -scenario connect -broker "$BROKER" -admin "$ADMIN"
  -token "$admin_token" -clients "$CLIENTS" -concurrency "$CONC"
  -hold "$HOLD" -prefix "$PREFIX" -client-offset "$CLIENT_OFFSET"
  -hold-until-unix "$HOLD_UNTIL_UNIX"
  -verify-held
  -label "linux-density-${TRANSPORT}-n${CLIENTS}-offset${CLIENT_OFFSET}"
  -setup="$SETUP" -setup-concurrency "$SETUP_CONC"
  -out "$OUT_DIR/result.jsonl"
)
if [ "$TRANSPORT" = mtls ]; then
  args+=(-tls-ca "$TLS_CA" -tls-cert "$TLS_CERT" -tls-key "$TLS_KEY"
    -tls-server-name "$TLS_SERVER_NAME")
else
  args+=(-proxy-v2-cert "$PROXY_CERT")
  if [ "$PROXY_IDENTITY" = fingerprint ]; then args+=(-proxy-v2-fingerprint-only); fi
fi
if [ -n "$SOURCE_IPS" ]; then args+=(-source-ip "$SOURCE_IPS"); fi
if [ -n "$ADMIN_SOURCE_IP" ]; then args+=(-admin-source-ip "$ADMIN_SOURCE_IP"); fi

curl_source_args=()
if [ -n "$ADMIN_SOURCE_IP" ]; then curl_source_args=(--interface "$ADMIN_SOURCE_IP"); fi

curl -sf "${curl_source_args[@]}" "$ADMIN/healthz" > "$OUT_DIR/healthz.json"
broker_node_id=$(sed -n 's/.*"node_id":[[:space:]]*"\([^"]*\)".*/\1/p' "$OUT_DIR/healthz.json" | tail -n 1)
cluster_id=$(sed -n 's/.*"cluster_id":[[:space:]]*"\([^"]*\)".*/\1/p' "$OUT_DIR/healthz.json" | tail -n 1)
if [ -z "$broker_node_id" ] || [ -z "$cluster_id" ]; then
  echo "$ADMIN/healthz 缺少 node_id/cluster_id，不能证明五片来自同一集群的不同节点。" >&2
  exit 2
fi
printf 'broker_node_id=%s\ncluster_id=%s\n' "$broker_node_id" "$cluster_id" >> "$OUT_DIR/environment.txt"

curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer $admin_token" \
  "$ADMIN/metrics" > "$OUT_DIR/metrics.before.prom"
baseline_connections=$(awk '
  { metric=$1; sub(/\{.*/, "", metric) }
  metric == "openiot_connections" { total += $NF }
  END { printf "%d", total+0 }
' "$OUT_DIR/metrics.before.prom")
printf 'baseline_connections=%s\n' "$baseline_connections" >> "$OUT_DIR/environment.txt"

if [ "$START_AT_UNIX" -gt 0 ]; then
  now_unix=$(date +%s)
  if [ "$now_unix" -gt "$START_AT_UNIX" ]; then
    echo "已错过统一开始时间 START_AT_UNIX=$START_AT_UNIX（当前 $now_unix），拒绝产生非重叠波次。" >&2
    exit 2
  fi
  sleep $((START_AT_UNIX - now_unix))
fi

"$MQTTBENCH" "${args[@]}" > "$OUT_DIR/result.pretty.json" 2> "$OUT_DIR/mqttbench.stderr" &
bench_pid=$!
(
  while kill -0 "$bench_pid" 2>/dev/null; do
    {
      echo "timestamp $(date -u +%FT%TZ)"
      cat /proc/net/sockstat
      if [ -r /proc/net/sockstat6 ]; then cat /proc/net/sockstat6; fi
    } >> "$OUT_DIR/sockstat.log"
    {
      echo "timestamp $(date -u +%FT%TZ) pid=$bench_pid"
      # VmRSS 是当前值，VmHWM 是内核累计的进程 RSS 峰值；后者可捕获
      # TLS 冷握手期间一秒采样恰好错过的短峰。Threads 用来验证每个
      # 虚拟设备没有意外派生额外 goroutine 对应的 OS 线程风暴。
      awk '/^(VmPeak|VmSize|VmHWM|VmRSS|RssAnon|RssFile|Threads):/ {print}' \
        "/proc/$bench_pid/status" 2>/dev/null || true
    } >> "$OUT_DIR/loadgen-process.log"
    if [ -n "$BROKER_PID" ]; then
      {
        echo "timestamp $(date -u +%FT%TZ) pid=$BROKER_PID"
        awk '/^(VmPeak|VmSize|VmHWM|VmRSS|RssAnon|RssFile|Threads):/ {print}' \
          "/proc/$BROKER_PID/status" 2>/dev/null || true
      } >> "$OUT_DIR/broker-process.log"
    fi
    # 前后两个快照会漏掉 TLS 握手风暴期间的瞬时峰值。保留一条低频时间线，
    # 事后可以把连接数与 heap/runtime Sys/stack/RSS/GC 同轴对齐。这里过滤
    # 到容量指标，避免每秒复制完整的高基数业务指标。
    {
      echo "# timestamp $(date -u +%FT%TZ)"
      curl -sf "${curl_source_args[@]}" --max-time 2 -H "Authorization: Bearer $admin_token" \
        "$ADMIN/metrics" 2>/dev/null | awk '
	          /^openiot_(connections|goroutines|heap_alloc_bytes|heap_sys_bytes|heap_inuse_bytes|heap_released_bytes|heap_objects|stack_inuse_bytes|stack_sys_bytes|runtime_sys_bytes|runtime_memory_limit_bytes|gc_cycles_total|gc_pause_seconds|process_resident_memory_bytes)(\{| )/ {print}
        ' || true
    } >> "$OUT_DIR/metrics.timeline.prom"
    sleep 1
  done
) &
sampler_pid=$!

if wait "$bench_pid"; then
  rc=0
else
  rc=$?
fi
bench_pid=""
kill "$sampler_pid" 2>/dev/null || true
wait "$sampler_pid" 2>/dev/null || true
sampler_pid=""

curl -sf "${curl_source_args[@]}" -H "Authorization: Bearer $admin_token" \
  "$ADMIN/metrics" > "$OUT_DIR/metrics.after.prom"

{
  echo "# peaks captured during the connection wave"
  awk '$1 == "VmHWM:" {if ($2 > max) max=$2} END {
    printf "loadgen_vm_hwm_kib=%d\n", max
  }' "$OUT_DIR/loadgen-process.log"
  if [ -s "$OUT_DIR/broker-process.log" ]; then
    awk '$1 == "VmHWM:" {if ($2 > max) max=$2} END {
      printf "broker_vm_hwm_kib=%d\n", max
    }' "$OUT_DIR/broker-process.log"
  fi
  awk '$1 == "TCP:" {
    for (i=2; i<NF; i+=2) {
      key=$i; value=$(i+1)+0
      if (key == "inuse" && value > inuse) inuse=value
      if (key == "tw" && value > tw) tw=value
      if (key == "orphan" && value > orphan) orphan=value
      if (key == "mem" && value > mem) mem=value
    }
  } END {
    printf "loadgen_tcp_inuse_peak=%d\n", inuse
    printf "loadgen_tcp_timewait_peak=%d\n", tw
    printf "loadgen_tcp_orphan_peak=%d\n", orphan
    printf "loadgen_tcp_mem_pages_peak=%d\n", mem
  }' "$OUT_DIR/sockstat.log"
  awk '/^openiot_/ {
    name=$1
    sub(/\{.*/, "", name)
    value=$NF+0
    if (!(name in seen) || value > peak[name]) peak[name]=value
    seen[name]=1
  } END {
    for (name in peak) printf "%s_peak=%.17g\n", name, peak[name]
  }' "$OUT_DIR/metrics.timeline.prom" | sort
} > "$OUT_DIR/peaks.txt"

cat "$OUT_DIR/result.pretty.json"
if [ "$rc" != 0 ]; then
  cat "$OUT_DIR/mqttbench.stderr" >&2
  exit "$rc"
fi
echo "验收材料写入 $OUT_DIR（JSON、environment、peaks.txt、指标时间线、stderr、逐秒 sockstat、load generator RSS/HWM，以及可选 broker RSS/HWM）。"
