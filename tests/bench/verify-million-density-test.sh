#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

work=$(mktemp -d -t openiot-density-verify-test)
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

fixture() {
  local dir=$1 offset=$2
  mkdir -p "$dir"
  printf '{"clients":5,"clientOffset":%d,"heldConns":5,"heldVerified":true}\n' "$offset" > "$dir/result.jsonl"
  printf 'kernel=Linux test\ntransport=mtls\ningress_mode=direct\nbrokers=broker-%d:8883 source_ips=test\nbroker_node_id=node-%d\ncluster_id=cluster-1\nbaseline_connections=0\nstart_at_unix=100\nhold_until_unix=200\n' \
    "$offset" "$offset" > "$dir/environment.txt"
  printf 'openiot_connections_peak=5\n' > "$dir/peaks.txt"
}

fixture "$work/a" 0
fixture "$work/b" 5
EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null

# 只记录曾经建成的连接不能证明它们活到保持窗口末尾。
printf '{"clients":5,"clientOffset":5,"heldConns":5}\n' > "$work/b/result.jsonl"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "缺少应用层held验证的旧结果没有被拒绝" >&2
  exit 1
fi
fixture "$work/b" 5

# 第二片从 4 开始会与第一片 [0,5) 重叠，聚合器必须拒绝。
fixture "$work/b" 4
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" \
  >/dev/null 2>&1; then
  echo "重叠 clientId 区间没有被拒绝" >&2
  exit 1
fi

# 即使区间正确，不同绝对保持窗口也不能证明同时在线。
fixture "$work/b" 5
printf 'kernel=Linux test\ntransport=mtls\ningress_mode=direct\nbrokers=broker-5:8883 source_ips=test\nbroker_node_id=node-5\ncluster_id=cluster-1\nbaseline_connections=0\nstart_at_unix=101\nhold_until_unix=200\n' \
  > "$work/b/environment.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" \
  >/dev/null 2>&1; then
  echo "不一致的共同窗口没有被拒绝" >&2
  exit 1
fi

# 五个地址若实际落在同一个逻辑节点，也不是五节点集群。
fixture "$work/b" 5
sed -i.bak 's/broker_node_id=node-5/broker_node_id=node-0/' "$work/b/environment.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "重复broker node_id没有被拒绝" >&2
  exit 1
fi

# 节点还必须属于同一个集群。
fixture "$work/b" 5
sed -i.bak 's/cluster_id=cluster-1/cluster_id=cluster-2/' "$work/b/environment.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "不同cluster_id没有被拒绝" >&2
  exit 1
fi

# 代理终结档不能冒充真实 mTLS 最终验收。
fixture "$work/b" 5
sed -i.bak 's/transport=mtls/transport=proxy-v2/' "$work/b/environment.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "proxy-v2 结果冒充 mTLS 通过" >&2
  exit 1
fi

# 总数相同也必须满足固定片数与每片连接数，不能用不同拓扑替代五乘20万。
printf '{"clients":4,"clientOffset":5,"heldConns":4,"heldVerified":true}\n' > "$work/b/result.jsonl"
if EXPECTED_TOTAL=9 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 EXPECTED_DISTINCT_BROKERS=2 \
  tests/bench/verify-million-density.sh "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "错误的单片连接数没有被拒绝" >&2
  exit 1
fi

# Kubernetes生产拓扑使用一个共享LoadBalancer。两个loadgen片与两个broker
# 节点没有一一对应关系：允许某节点峰值4、另一节点6，只要五节点（本装置为
# 两节点）峰值总和覆盖全部held，且管理面确实来自不同node_id。
fixture "$work/a" 0
fixture "$work/b" 5
for dir in "$work/a" "$work/b"; do
  sed -i.bak 's/ingress_mode=direct/ingress_mode=shared-lb/' "$dir/environment.txt"
  sed -i.bak 's/brokers=broker-[0-9]*:8883/brokers=mqtt.example:8883/' "$dir/environment.txt"
done
printf 'openiot_connections_peak=4\n' > "$work/a/peaks.txt"
printf 'openiot_connections_peak=6\n' > "$work/b/peaks.txt"
EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 \
  EXPECTED_INGRESS_MODE=shared-lb tests/bench/verify-million-density.sh \
  "$work/a" "$work/b" >/dev/null

# 共享LB下仍必须由五个独立管理端覆盖全部服务端连接；峰值总和不足不能只靠
# 客户端held数字通过。
printf 'openiot_connections_peak=5\n' > "$work/b/peaks.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 \
  EXPECTED_INGRESS_MODE=shared-lb tests/bench/verify-million-density.sh \
  "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "共享LB服务端峰值总和不足没有被拒绝" >&2
  exit 1
fi

# 同一份共享LB结果不能冒充五个直连broker拓扑。
printf 'openiot_connections_peak=6\n' > "$work/b/peaks.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 \
  EXPECTED_DISTINCT_BROKERS=2 tests/bench/verify-million-density.sh \
  "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "共享LB结果冒充direct拓扑通过" >&2
  exit 1
fi

# 峰值必须属于本轮；开跑前已有连接会让绝对峰值虚增，最终验收必须使用
# 空集群，不能拿生产存量补足百万。
sed -i.bak 's/baseline_connections=0/baseline_connections=1/' "$work/a/environment.txt"
if EXPECTED_TOTAL=10 EXPECTED_SHARDS=2 EXPECTED_PER_SHARD=5 \
  EXPECTED_INGRESS_MODE=shared-lb tests/bench/verify-million-density.sh \
  "$work/a" "$work/b" >/dev/null 2>&1; then
  echo "非零连接基线没有被拒绝" >&2
  exit 1
fi
