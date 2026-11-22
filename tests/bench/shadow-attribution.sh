#!/usr/bin/env bash
# 影子更新的归因：吞吐随并发涨不涨？（CLU-125 的同类，另一条路径）
#
# 机制预测：影子写入落在单个元数据组的 apply 协程上，每条至少两次 fsync
# （冷层记录与 revision）。若如此，加大并发只会加长队列、不会提高吞吐——
# 吞吐钉在 1 ÷ 服务时间上，而延迟随并发线性增长。
#
# 为什么要有并发阶梯：固定并发下这一栏分辨不出上限。按 TST-167，结果页登的
# 必须是阶梯的高点，而不是「我们用了几个客户端」。
#
#   tests/bench/shadow-attribution.sh              # 单节点
#   NODES=3 tests/bench/shadow-attribution.sh      # 三节点集群
#   OUT=/tmp/shadow.jsonl NODES=3 tests/bench/shadow-attribution.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

NODES=${NODES:-1}
LADDER=${LADDER:-1,5,10,25,50,100}
DURATION=${DURATION:-8s}
WARMUP=${WARMUP:-2s}
BASE=${BASE:-43200}
OUT=${OUT:-/dev/null}
# 影子分片数。生产清单（deploy/cluster/k8s 与 compose）都是 8，而默认 0 表示
# 影子写入仍走**单个**元数据 Raft 组——两者不是同一条路径，量出来差一个量级。
# 默认留 0 是为了能单独量出分片带来的那一截；要对标生产就显式给 8。
# 单节点不支持：standalone 写任何 cluster.replication 配置都会被配置层拒绝
# （CLU-070），所以影子分片是集群独有的能力。
SHADOW_SHARDS=${SHADOW_SHARDS:-0}
case "${NODES}" in 1|3) ;; *) echo "NODES 只支持 1 或 3" >&2; exit 2 ;; esac

W=$(mktemp -d -t openiot-shadowprobe)
PIDS=()
# cleanup 必须等端口真的释放，而不是 kill 完就走。
#
# 连着跑两轮时，上一轮的节点可能还占着端口：新一轮的预检会响亮失败（好过
# 静默压在旧节点上），但那等于白跑一轮。等到端口消失或超时再退。
cleanup() {
	for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null; done
	local waited=0
	while [ "$waited" -lt 60 ]; do
		lsof -nP -iTCP:"${BASE}" -iTCP:"$((BASE + 11))" -sTCP:LISTEN >/dev/null 2>&1 || break
		sleep 0.5
		waited=$((waited + 1))
	done
	rm -rf "$W"
}
trap cleanup EXIT INT TERM
die() { echo "FAIL  $*" >&2; exit 1; }

go build -o "$W/openiotd" ./cmd/openiotd || exit 1
go build -o "$W/mqttbench" ./tools/mqttbench || exit 1

# 与 tests/bench/run.sh 用的是同一份配额解除块。两边必须一致，否则这条探针
# 与主结果表量的不是同一件事。
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

wait_ready() { # $1=admin 端口 $2=pid $3=日志
	for _ in $(seq 1 600); do
		kill -0 "$2" 2>/dev/null || { tail -20 "$3" >&2; die "节点启动失败"; }
		curl -sf -o /dev/null "http://127.0.0.1:$1/readyz" 2>/dev/null && return 0
		sleep 0.2
	done
	tail -20 "$3" >&2
	die "节点未在 120 秒内就绪"
}

# 端口预检。
#
# 不检的后果不是「启动失败」而是**量错对象**：上一轮的节点若还占着这些端口，
# 新节点绑不上就退出，而 wait_ready 的 /readyz 探针会被那个**旧**进程应答成
# 功，于是整轮压的是旧节点，拿新 token 去访问它只会得到一个看不出原因的 401。
preflight_ports() {
	local p
	for p in "$@"; do
		if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
			die "端口 $p 已被占用（可能是上一轮没退干净的 openiotd）；换 BASE 或先清掉它"
		fi
	done
}

BROKERS=""
ADMIN=""
TOKEN=""

start_single() {
	"$W/openiotd" init --data-dir "$W/d" --node-id shdw --log-level error >/dev/null
	cat > "$W/c.yaml" <<YAML
node:
  data_dir: $W/d
listeners:
  mqtt_tls: {enabled: false}
  mqtt_plain: {enabled: true, addr: "127.0.0.1:${BASE}"}
  admin: {enabled: true, addr: "127.0.0.1:$((BASE + 1))"}
auth:
  mode: insecure-none
  acknowledge_insecure: true
log: {level: warn, format: text}
${QUOTA}
YAML
	"$W/openiotd" serve --config "$W/c.yaml" >"$W/node.log" 2>&1 &
	local pid=$!
	PIDS+=("$pid")
	# 不用 ${PIDS[-1]}：macOS 自带的 bash 3.2 不支持负数下标。
	wait_ready "$((BASE + 1))" "$pid" "$W/node.log"
	BROKERS="127.0.0.1:${BASE}"
	ADMIN="http://127.0.0.1:$((BASE + 1))"
	TOKEN=$(cat "$W/d/admin-token")
}

# 三节点与 run.sh 的 run_cluster 用同一套装配：一条独立的复制信任链，
# 证书 CommonName 等于节点 ID（CLU-069 在握手时校验）。
start_cluster() {
	local nodes=(a b c) pki="$W/replpki" id i
	mkdir -p "$pki"
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
		-keyout "$pki/ca.key" -out "$pki/ca.pem" \
		-subj "/CN=open-iot-core-shadowprobe-ca" >/dev/null 2>&1 || die "生成复制 CA 失败"
	for id in "${nodes[@]}"; do
		openssl req -newkey rsa:2048 -nodes -keyout "$pki/$id.key" -out "$pki/$id.csr" \
			-subj "/CN=$id" >/dev/null 2>&1 || die "生成 $id 的 CSR 失败"
		printf 'subjectAltName = DNS:%s\nextendedKeyUsage = serverAuth, clientAuth\n' "$id" > "$pki/$id.ext"
		openssl x509 -req -in "$pki/$id.csr" -days 1 -CA "$pki/ca.pem" -CAkey "$pki/ca.key" \
			-CAcreateserial -extfile "$pki/$id.ext" -out "$pki/$id.pem" >/dev/null 2>&1 ||
			die "签发 $id 的证书失败"
	done
	local shadow_cfg=""
	if [ "${SHADOW_SHARDS}" != 0 ]; then
		shadow_cfg="
    shadow_shards: ${SHADOW_SHARDS}"
	fi
	local peers="" replicas=""
	for i in "${!nodes[@]}"; do
		peers="${peers}
      ${nodes[$i]}: \"127.0.0.1:$((BASE + 10 + i * 10 + 2))\""
		replicas="${replicas}
      ${nodes[$i]}: $((i + 1))"
	done
	local addrs=()
	for i in "${!nodes[@]}"; do
		id="${nodes[$i]}"
		local mqtt=$((BASE + 10 + i * 10)) admin=$((BASE + 11 + i * 10)) repl=$((BASE + 12 + i * 10))
		"$W/openiotd" init --data-dir "$W/$id" --node-id "$id" --log-level error >/dev/null
		local mode=join seeds="  seeds: [\"127.0.0.1:$((BASE + 12))\"]"
		if [ "$i" = 0 ]; then mode=bootstrap; seeds=""; fi
		cat > "$W/$id.yaml" <<YAML
node:
  id: ${id}
  mode: ${mode}
  data_dir: $W/${id}
${seeds}
cluster:
  voters: 3
  max_view_staleness: 10s
  replication:
    addr: "127.0.0.1:${repl}"
    cert_file: $pki/${id}.pem
    key_file: $pki/${id}.key
    ca_file: $pki/ca.pem
    pump_interval: 100ms${shadow_cfg}
    peers:${peers}
    replicas:${replicas}
listeners:
  mqtt_tls: {enabled: false}
  mqtt_plain: {enabled: true, addr: "127.0.0.1:${mqtt}"}
  admin: {enabled: true, addr: "127.0.0.1:${admin}"}
auth:
  mode: insecure-none
  acknowledge_insecure: true
log: {level: warn, format: text}
${QUOTA}
YAML
		"$W/openiotd" serve --config "$W/$id.yaml" >"$W/$id.log" 2>&1 &
		PIDS+=("$!")
		addrs+=("127.0.0.1:${mqtt}")
	done
	for i in "${!nodes[@]}"; do
		wait_ready "$((BASE + 11 + i * 10))" "${PIDS[$i]}" "$W/${nodes[$i]}.log"
	done
	BROKERS=$(IFS=,; echo "${addrs[*]}")
	ADMIN="http://127.0.0.1:$((BASE + 11))"
	TOKEN=$(cat "$W/a/admin-token")
}

if [ "${NODES}" = 1 ] && [ "${SHADOW_SHARDS}" != 0 ]; then
	die "单节点不支持影子分片（standalone 拒绝 cluster.replication，CLU-070）"
fi
if [ "${NODES}" = 1 ]; then
	preflight_ports "${BASE}" "$((BASE + 1))"
	start_single
else
	preflight_ports "$((BASE + 10))" "$((BASE + 11))" "$((BASE + 12))" \
		"$((BASE + 20))" "$((BASE + 21))" "$((BASE + 22))" \
		"$((BASE + 30))" "$((BASE + 31))" "$((BASE + 32))"
	start_cluster
fi

echo "影子并发阶梯（${NODES} 节点，shadow_shards=${SHADOW_SHARDS}）"
for n in ${LADDER//,/ }; do
	"$W/mqttbench" -scenario shadow -broker "${BROKERS}" \
		-admin "${ADMIN}" -token "${TOKEN}" -clients "$n" \
		-duration "${DURATION}" -warmup "${WARMUP}" -prefix "shp${NODES}s${SHADOW_SHARDS}x$n" \
		-label "shadow/n${NODES}-sh${SHADOW_SHARDS}-clients-$n" -out "${OUT}" |
		python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f\"  并发 {d['clients']:4d}  {d['deliverPerSec']:7.1f} 条/秒  p50 {d['latencyP50Ms']:8.2f} ms  p99 {d['latencyP99Ms']:8.2f} ms\")" ||
		die "并发 $n 失败"
done
