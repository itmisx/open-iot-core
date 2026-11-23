#!/usr/bin/env bash
# 聚合五个 connect-density-linux.sh 输出目录，证明它们真的是同一批
# 不重叠的百万并发连接，而不是五次重复 clientId 的连接尝试。
# direct 模式要求五个不同broker入口且逐片峰值覆盖held；shared-lb模式允许
# 五片共用一个生产LoadBalancer，但五个ADMIN必须落到不同节点，节点峰值总和
# 必须覆盖百万held。两种证据不能互相冒充。
set -euo pipefail

EXPECTED_TOTAL=${EXPECTED_TOTAL:-1000000}
EXPECTED_SHARDS=${EXPECTED_SHARDS:-5}
EXPECTED_PER_SHARD=${EXPECTED_PER_SHARD:-200000}
EXPECTED_TRANSPORT=${EXPECTED_TRANSPORT:-mtls}
EXPECTED_INGRESS_MODE=${EXPECTED_INGRESS_MODE:-direct}
case "$EXPECTED_INGRESS_MODE" in
  direct) expected_brokers_default=5 ;;
  shared-lb) expected_brokers_default=1 ;;
  *) echo "EXPECTED_INGRESS_MODE 只能是 direct 或 shared-lb" >&2; exit 2 ;;
esac
EXPECTED_DISTINCT_BROKERS=${EXPECTED_DISTINCT_BROKERS:-$expected_brokers_default}
if [ "$#" -lt 2 ]; then
  echo "用法: $0 <density-output-dir>...（通常传五个目录）" >&2
  exit 2
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-density-verify.XXXXXX") || exit 1
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM
rows="$work/rows.tsv"

json_number() {
  local key=$1 line=$2
  printf '%s\n' "$line" | sed -n "s/.*\"$key\":\([0-9][0-9]*\).*/\\1/p"
}

for dir in "$@"; do
  result="$dir/result.jsonl"
  [ -s "$result" ] || { echo "$dir 缺少 result.jsonl" >&2; exit 2; }
  line=$(tail -n 1 "$result")
  clients=$(json_number clients "$line")
  offset=$(json_number clientOffset "$line")
  held=$(json_number heldConns "$line")
  held_verified=$(printf '%s\n' "$line" | sed -n 's/.*"heldVerified":[[:space:]]*\([a-z][a-z]*\).*/\1/p')
  errors=$(json_number errors "$line")
  [ -n "$errors" ] || errors=0
  if [ -z "$clients" ] || [ -z "$offset" ] || [ -z "$held" ]; then
    echo "$result 缺少 clients/clientOffset/heldConns；请使用当前 mqttbench 重跑。" >&2
    exit 2
  fi
  if [ "$held_verified" != true ]; then
    echo "$result 没有 heldVerified=true；曾经建连成功不能证明保持窗口末尾仍存活。" >&2
    exit 2
  fi
  end=$((offset + clients))
  peak=0
  if [ -s "$dir/peaks.txt" ]; then
    peak=$(sed -n 's/^openiot_connections_peak=//p' "$dir/peaks.txt" | tail -n 1)
    [ -n "$peak" ] || peak=0
    peak=${peak%.*}
  fi
  environment="$dir/environment.txt"
  [ -s "$environment" ] || { echo "$dir 缺少 environment.txt" >&2; exit 2; }
  start=$(sed -n 's/^start_at_unix=//p' "$environment" | tail -n 1)
  stop=$(sed -n 's/^hold_until_unix=//p' "$environment" | tail -n 1)
  transport=$(sed -n 's/^transport=//p' "$environment" | tail -n 1)
  ingress_mode=$(sed -n 's/^ingress_mode=//p' "$environment" | tail -n 1)
  broker=$(sed -n 's/^brokers=\([^ ]*\) source_ips=.*/\1/p' "$environment" | tail -n 1)
  kernel=$(sed -n 's/^kernel=//p' "$environment" | tail -n 1)
  node_id=$(sed -n 's/^broker_node_id=//p' "$environment" | tail -n 1)
  cluster_id=$(sed -n 's/^cluster_id=//p' "$environment" | tail -n 1)
  baseline=$(sed -n 's/^baseline_connections=//p' "$environment" | tail -n 1)
  [ -n "$start" ] || start=0
  [ -n "$stop" ] || stop=0
  if [ -z "$transport" ] || [ -z "$ingress_mode" ] || [ -z "$broker" ] || [ -z "$kernel" ] || \
    [ -z "$node_id" ] || [ -z "$cluster_id" ] || [ -z "$baseline" ]; then
    echo "$environment 缺少 transport/ingress_mode/brokers/kernel/broker_node_id/cluster_id/baseline_connections；请使用当前脚本重跑。" >&2
    exit 2
  fi
  printf '%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\n' \
    "$offset" "$end" "$clients" "$held" "$errors" "$peak" "$start" "$stop" \
    "$dir" "$transport" "$broker" "$kernel" "$node_id" "$cluster_id" "$ingress_mode" "$baseline" >> "$rows"
done

sort -n -k1,1 "$rows" > "$work/sorted.tsv"
awk -F '\t' -v expected="$EXPECTED_TOTAL" -v expected_shards="$EXPECTED_SHARDS" \
  -v expected_per_shard="$EXPECTED_PER_SHARD" -v expected_transport="$EXPECTED_TRANSPORT" \
  -v expected_brokers="$EXPECTED_DISTINCT_BROKERS" \
  -v expected_ingress="$EXPECTED_INGRESS_MODE" '
  BEGIN { previous_end=-1; shared_start=-1; shared_stop=-1; bad=0 }
  {
    if ($1 < previous_end) {
      printf "clientId 区间重叠：offset %d 小于上一片结束 %d\n", $1, previous_end > "/dev/stderr"
      bad=1
    }
    previous_end=$2
    clients += $3
    held += $4
    errors += $5
    if (expected_ingress == "direct" && $6 > 0 && $6 < $4) {
      printf "%s 的服务端连接峰值 %d 小于 heldConns %d\n", $9, $6, $4 > "/dev/stderr"
      bad=1
    }
    if ($6 <= 0) {
      printf "%s 缺少服务端连接峰值，不能证明broker实际持有连接\n", $9 > "/dev/stderr"
      bad=1
    }
    server_peak += $6
    if ($3 != expected_per_shard) {
      printf "%s 的 clients=%d，不是每片要求的 %d\n", $9, $3, expected_per_shard > "/dev/stderr"
      bad=1
    }
    if ($10 != expected_transport) {
      printf "%s 的 transport=%s，不是要求的 %s\n", $9, $10, expected_transport > "/dev/stderr"
      bad=1
    }
    if ($15 != expected_ingress) {
      printf "%s 的ingress_mode=%s，不是要求的%s\n", $9, $15, expected_ingress > "/dev/stderr"
      bad=1
    }
    if ($16 != 0) {
      printf "%s 开跑前已有%d条连接，服务端峰值不是本轮独占证据\n", $9, $16 > "/dev/stderr"
      bad=1
    }
    if ($12 !~ /^Linux /) {
      printf "%s 不是 Linux 结果：%s\n", $9, $12 > "/dev/stderr"
      bad=1
    }
    brokers[$11]=1
    nodes[$13]=1
    if (shared_cluster == "") shared_cluster=$14
    if ($14 != shared_cluster) {
      printf "%s 的cluster_id=%s，与%s不一致\n", $9, $14, shared_cluster > "/dev/stderr"
      bad=1
    }
    if (shared_start < 0) { shared_start=$7; shared_stop=$8 }
    if ($7 != shared_start || $8 != shared_stop) {
      printf "%s 的同步窗口 [%d,%d] 与 [%d,%d] 不一致\n", $9, $7, $8, shared_start, shared_stop > "/dev/stderr"
      bad=1
    }
    printf "%-48s range=[%d,%d) held=%d errors=%d server_peak=%d\n", $9, $1, $2, $4, $5, $6
  }
  END {
    printf "total clients=%d held=%d errors=%d server_peak_sum=%d expected=%d ingress=%s shared_window=[%d,%d]\n", clients, held, errors, server_peak, expected, expected_ingress, shared_start, shared_stop
    for (broker in brokers) broker_count++
    for (node in nodes) node_count++
    if (shared_start <= 0 || shared_stop <= shared_start) bad=1
    if (NR != expected_shards) {
      printf "结果片数 %d，不是要求的 %d\n", NR, expected_shards > "/dev/stderr"
      bad=1
    }
    if (broker_count != expected_brokers) {
      printf "不同broker目标数 %d，不是要求的 %d\n", broker_count, expected_brokers > "/dev/stderr"
      bad=1
    }
    if (node_count != expected_shards) {
      printf "不同broker node_id数 %d，不是要求的 %d\n", node_count, expected_shards > "/dev/stderr"
      bad=1
    }
    if (server_peak < held) {
      printf "五节点服务端连接峰值之和 %d 小于 held总数 %d\n", server_peak, held > "/dev/stderr"
      bad=1
    }
    if (clients != expected || held != expected || errors != 0) bad=1
    exit bad
  }
' "$work/sorted.tsv"
