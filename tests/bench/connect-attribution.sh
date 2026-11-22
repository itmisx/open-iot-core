#!/usr/bin/env bash
# 建连速率的归因实验：集群下每秒一百条，瓶颈在哪？
#
# 三个自变量，每次只动一个：
#   NODES=3 SHARDS=8   基准（入库的那组参数）
#   NODES=3 SHARDS=32  分片数 ×4。涨 → 每个分片在串行；不涨 → 共享瓶颈
#   NODES=1 SHARDS=8   quorum=1，没有网络往返，只剩本地 fsync
#   NODES=1 SHARDS=32  同上，再排除分片数
set -uo pipefail
cd /Users/solly/data/develop/github/open-iot-core
NODES=${NODES:-3}; SHARDS=${SHARDS:-8}
CONNS=${CONNS:-1500}; CONC=${CONC:-256}
BASE=${BASE:-44000}
W=$(mktemp -d -t openiot-connprobe)
PIDS=()
cleanup() { for p in ${PIDS[@]+"${PIDS[@]}"}; do kill -9 "$p" 2>/dev/null; done; rm -rf "$W"; }
trap cleanup EXIT INT TERM
go build -o "$W/openiotd" ./cmd/openiotd || exit 1
go build -o "$W/mqttbench" ./tools/mqttbench || exit 1

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

ids=(a b c); ids=("${ids[@]:0:$NODES}")
pki="$W/pki"; mkdir -p "$pki"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -keyout "$pki/ca.key" -out "$pki/ca.pem" \
  -subj "/CN=probe-ca" >/dev/null 2>&1
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
  cat > "$W/$id.yaml" <<YAML
node:
  id: $id
  mode: $mode
  data_dir: $W/$id
$seeds
cluster:
  voters: $NODES
  max_view_staleness: 10s
  replication:
    addr: "127.0.0.1:$repl"
    cert_file: $pki/$id.pem
    key_file: $pki/$id.key
    ca_file: $pki/ca.pem
    session_shards: $SHARDS
    peers:$peers
    replicas:$replicas
listeners:
  mqtt_tls: {enabled: false}
  mqtt_plain: {enabled: true, addr: "127.0.0.1:$mqtt"}
  admin: {enabled: true, addr: "127.0.0.1:$admin"}
auth:
  mode: insecure-none
  acknowledge_insecure: true
log: {level: warn, format: text}
$QUOTA
YAML
done
for id in "${ids[@]:1}"; do rm -rf "$W/$id/pki"; cp -R "$W/a/pki" "$W/$id/pki"; done
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
"$W/mqttbench" -scenario connect -broker "$all" -admin "http://127.0.0.1:${admins[0]}" \
  -token "$TOKEN" -clients "$CONNS" -concurrency "$CONC" -hold 3s -warmup 1s -duration 3s \
  -prefix "cp${NODES}s${SHARDS}c${CONC}" -label "probe/n${NODES}-shards${SHARDS}-conc${CONC}" \
  -out "${OUT:-/dev/null}"
