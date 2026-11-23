#!/usr/bin/env bash
# 影子更新的归因：吞吐随并发涨不涨，leader把多少逻辑命令合进一条物理
# Raft日志，分片数、RF、mTLS与共识tick分别贡献多少？
#
# 旧路径落在单个元数据组且逐条同步事务；当前生产路径是独立影子分片、bbolt
# 批事务和leader二次合批。脚本同时保留两条路径，避免只看总吞吐就把存储、
# 共识、合批不足或TLS误认成同一个瓶颈。
#
# 为什么要有并发阶梯：固定并发下这一栏分辨不出上限。按 TST-167，结果页登的
# 必须是阶梯的高点，而不是「我们用了几个客户端」。
#
#   tests/bench/shadow-attribution.sh              # 单节点
#   NODES=3 tests/bench/shadow-attribution.sh      # 三节点集群
#   NODES=5 SHADOW_SHARDS=4 SHADOW_RF=3 ...        # 五节点生产拓扑
#   MQTT_TLS=1 NODES=5 SHADOW_SHARDS=4 SHADOW_RF=3 ... # 真实mTLS证书认证
#   OUT=/tmp/shadow.jsonl NODES=3 tests/bench/shadow-attribution.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

NODES=${NODES:-1}
LADDER=${LADDER:-1,5,10,25,50,100}
DURATION=${DURATION:-8s}
WARMUP=${WARMUP:-2s}
BASE=${BASE:-43200}
OUT=${OUT:-/dev/null}
# 影子分片数。生产清单（deploy/cluster/k8s 与 compose）都是 4，而默认 0 表示
# 影子写入仍走**单个**元数据 Raft 组——两者不是同一条路径，量出来差一个量级。
# 默认留 0 是为了能单独量出分片带来的那一截；要对标生产就显式给 4。
# 单节点不支持：standalone 写任何 cluster.replication 配置都会被配置层拒绝
# （CLU-070），所以影子分片是集群独有的能力。
SHADOW_SHARDS=${SHADOW_SHARDS:-0}
# 0 表示所有集群节点都是副本；五节点生产拓扑显式传3。
SHADOW_RF=${SHADOW_RF:-0}
MQTT_TLS=${MQTT_TLS:-0}
SHADOW_RESPONSE_QOS=${SHADOW_RESPONSE_QOS:-0}
SHADOW_RESPONSE_TIMEOUT=${SHADOW_RESPONSE_TIMEOUT:-5s}
RTT=${RTT:-}
FAILOVER=${FAILOVER:-0}
FAILOVER_CLIENTS=${FAILOVER_CLIENTS:-0}
FAILOVER_DURATION=${FAILOVER_DURATION:-12s}
FAILOVER_WARMUP=${FAILOVER_WARMUP:-2s}
FAILOVER_KILL_DELAY=${FAILOVER_KILL_DELAY:-2}
KEEP_WORK=${KEEP_WORK:-0}
OPENIOTD_BIN=${OPENIOTD_BIN:-}
MQTTBENCH_BIN=${MQTTBENCH_BIN:-}
case "${NODES}" in 1|3|5) ;; *) echo "NODES 只支持 1、3 或 5" >&2; exit 2 ;; esac
case "${MQTT_TLS}" in 0|1) ;; *) echo "MQTT_TLS 只支持 0 或 1" >&2; exit 2 ;; esac
case "${SHADOW_RESPONSE_QOS}" in 0|1) ;; *) echo "SHADOW_RESPONSE_QOS 只支持 0 或 1" >&2; exit 2 ;; esac
case "${FAILOVER}" in 0|1) ;; *) echo "FAILOVER 只支持 0 或 1" >&2; exit 2 ;; esac
case "${KEEP_WORK}" in 0|1) ;; *) echo "KEEP_WORK 只支持 0 或 1" >&2; exit 2 ;; esac
if [ "${SHADOW_RF}" != 0 ] && [ "${SHADOW_SHARDS}" = 0 ]; then
	echo "SHADOW_RF 非0时必须同时启用 SHADOW_SHARDS" >&2
	exit 2
fi
if [ "${FAILOVER}" = 1 ] && { [ "${NODES}" = 1 ] || [ "${SHADOW_SHARDS}" = 0 ]; }; then
	echo "FAILOVER=1 要求 NODES=3|5 且 SHADOW_SHARDS>0" >&2
	exit 2
fi
case "${FAILOVER_CLIENTS}" in *[!0-9]*|'') echo "FAILOVER_CLIENTS 必须是非负整数" >&2; exit 2 ;; esac

W=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadowprobe.XXXXXX") || exit 1
PIDS=()
PORTS=()
NODE_IDS=()
MQTT_ADDRS=()
# cleanup 必须等端口真的释放，而不是 kill 完就走。
#
# 连着跑两轮时，上一轮的节点可能还占着端口：新一轮的预检会响亮失败（好过
# 静默压在旧节点上），但那等于白跑一轮。等到端口消失或超时再退。
cleanup() {
	for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null; done
	local waited=0 p busy
	while [ "$waited" -lt 60 ]; do
		busy=0
		for p in ${PORTS[@]+"${PORTS[@]}"}; do
			if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
				busy=1
				break
			fi
		done
		[ "$busy" = 0 ] && break
		sleep 0.5
		waited=$((waited + 1))
	done
	if [ "$KEEP_WORK" = 1 ]; then
		printf '保留故障诊断目录: %s\n' "$W" >&2
	else
		rm -rf "$W"
	fi
}
trap cleanup EXIT INT TERM
die() { echo "FAIL  $*" >&2; exit 1; }

if [ -n "$OPENIOTD_BIN" ]; then
	[ -x "$OPENIOTD_BIN" ] || die "OPENIOTD_BIN不是可执行文件: $OPENIOTD_BIN"
	cp "$OPENIOTD_BIN" "$W/openiotd" || exit 1
else
	go build -o "$W/openiotd" ./cmd/openiotd || exit 1
fi
if [ -n "$MQTTBENCH_BIN" ]; then
	[ -x "$MQTTBENCH_BIN" ] || die "MQTTBENCH_BIN不是可执行文件: $MQTTBENCH_BIN"
	cp "$MQTTBENCH_BIN" "$W/mqttbench" || exit 1
else
	go build -o "$W/mqttbench" ./tools/mqttbench || exit 1
fi

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
	PORTS=("$@")
	for p in "$@"; do
		if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
			die "端口 $p 已被占用（可能是上一轮没退干净的 openiotd）；换 BASE 或先清掉它"
		fi
	done
}

BROKERS=""
ADMIN=""
TOKEN=""
TLS_ARGS=()
ADMINS=()
TOKENS=()

issue_bench_cert() { # $1=数据目录
	local data=$1 policy=shadow-bench
	mkdir -p "$W/client"
	cat > "$data/policies/$policy.json" <<'JSON'
{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}
JSON
	"$W/openiotd" cert issue --data-dir "$data" --thing mqttbench \
		--policy "$policy" --out "$W/client" >/dev/null || die "签发影子压测证书失败"
	TLS_ARGS=(-tls-ca "$data/pki/root-ca.pem" -tls-cert "$W/client/mqttbench.pem" \
		-tls-key "$W/client/mqttbench.key" -tls-server-name localhost)
}

start_single() {
	"$W/openiotd" init --data-dir "$W/d" --node-id shdw --log-level error >/dev/null
	local mqtt_listeners auth_mode=insecure-none acknowledge_insecure=true
	if [ "$MQTT_TLS" = 1 ]; then
		issue_bench_cert "$W/d"
		mqtt_listeners="  mqtt_tls: {enabled: true, addr: \"127.0.0.1:${BASE}\", require_client_cert: true, client_ca_file: $W/d/pki/ca-chain.pem}
  mqtt_plain: {enabled: false}"
		auth_mode=cert
		acknowledge_insecure=false
	else
		mqtt_listeners="  mqtt_tls: {enabled: false}
  mqtt_plain: {enabled: true, addr: \"127.0.0.1:${BASE}\"}"
	fi
	cat > "$W/c.yaml" <<YAML
node:
  data_dir: $W/d
listeners:
${mqtt_listeners}
  admin: {enabled: true, addr: "127.0.0.1:$((BASE + 1))"}
auth:
  mode: ${auth_mode}
  acknowledge_insecure: ${acknowledge_insecure}
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
	ADMINS=("${ADMIN}")
	TOKENS=("${TOKEN}")
}

# 集群与 run.sh 的 run_cluster 用同一套装配：一条独立的复制信任链，
# 证书 CommonName 等于节点 ID（CLU-069 在握手时校验）。
start_cluster() {
	local all_nodes=(a b c d e) nodes=() pki="$W/replpki" id i
	for ((i = 0; i < NODES; i++)); do
		nodes+=("${all_nodes[$i]}")
	done
	NODE_IDS=("${nodes[@]}")
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
		if [ "${SHADOW_RF}" != 0 ]; then
			shadow_cfg="${shadow_cfg}
    shadow_replication_factor: ${SHADOW_RF}"
		fi
	fi
	local rtt_cfg=""
	if [ -n "${RTT}" ]; then
		rtt_cfg="
    rtt: ${RTT}"
	fi
	local peers="" replicas=""
	for i in "${!nodes[@]}"; do
		peers="${peers}
      ${nodes[$i]}: \"127.0.0.1:$((BASE + 10 + i * 10 + 2))\""
		replicas="${replicas}
      ${nodes[$i]}: $((i + 1))"
	done
	for i in "${!nodes[@]}"; do
		id="${nodes[$i]}"
		"$W/openiotd" init --data-dir "$W/$id" --node-id "$id" --log-level error >/dev/null
	done
	local auth_mode=insecure-none acknowledge_insecure=true
	if [ "$MQTT_TLS" = 1 ]; then
		issue_bench_cert "$W/a"
		for id in "${nodes[@]:1}"; do
			rm -rf "$W/$id/pki"
			cp -R "$W/a/pki" "$W/$id/pki"
		done
		auth_mode=cert
		acknowledge_insecure=false
	fi
	local addrs=() mqtt_listeners
	for i in "${!nodes[@]}"; do
		id="${nodes[$i]}"
		local mqtt=$((BASE + 10 + i * 10)) admin=$((BASE + 11 + i * 10)) repl=$((BASE + 12 + i * 10))
		local mode=join seeds="  seeds: [\"127.0.0.1:$((BASE + 12))\"]"
		if [ "$i" = 0 ]; then mode=bootstrap; seeds=""; fi
		if [ "$MQTT_TLS" = 1 ]; then
			mqtt_listeners="  mqtt_tls: {enabled: true, addr: \"127.0.0.1:${mqtt}\", require_client_cert: true, client_ca_file: $W/$id/pki/ca-chain.pem}
  mqtt_plain: {enabled: false}"
		else
			mqtt_listeners="  mqtt_tls: {enabled: false}
  mqtt_plain: {enabled: true, addr: \"127.0.0.1:${mqtt}\"}"
		fi
		cat > "$W/$id.yaml" <<YAML
node:
  id: ${id}
  mode: ${mode}
  data_dir: $W/${id}
${seeds}
cluster:
  voters: ${NODES}
  max_view_staleness: 10s
  replication:
    addr: "127.0.0.1:${repl}"
    cert_file: $pki/${id}.pem
    key_file: $pki/${id}.key
    ca_file: $pki/ca.pem
    pump_interval: 100ms${shadow_cfg}${rtt_cfg}
    peers:${peers}
    replicas:${replicas}
listeners:
${mqtt_listeners}
  admin: {enabled: true, addr: "127.0.0.1:${admin}"}
auth:
  mode: ${auth_mode}
  acknowledge_insecure: ${acknowledge_insecure}
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
	MQTT_ADDRS=("${addrs[@]}")
	ADMIN="http://127.0.0.1:$((BASE + 11))"
	TOKEN=$(cat "$W/a/admin-token")
	ADMINS=()
	TOKENS=()
	for i in "${!nodes[@]}"; do
		ADMINS+=("http://127.0.0.1:$((BASE + 11 + i * 10))")
		TOKENS+=("$(cat "$W/${nodes[$i]}/admin-token")")
	done
}

# scrape_metrics 必须收齐全部节点。影子分片 leader 分散在各节点，只看接入节点
# 会漏掉 leader 二次合批与实际 Raft 日志数，进而把“合批不足”误诊成磁盘瓶颈。
scrape_metrics() { # $1=输出文件
	local target=$1 i ok=0 tmp
	: > "$target"
	for i in "${!ADMINS[@]}"; do
		tmp="$W/metrics-$i.txt"
		if curl -sf -H "Authorization: Bearer ${TOKENS[$i]}" \
			"${ADMINS[$i]}/metrics" > "$tmp"; then
			cat "$tmp" >> "$target"
			ok=$((ok + 1))
		fi
	done
	[ "$ok" = "${#ADMINS[@]}" ] || die "只采到 $ok/${#ADMINS[@]} 个节点的指标"
}

metric_sum() { # $1=文件 $2=指标 $3=可选标签片段
	awk -v name="$2" -v want="${3:-}" '
		{ metric=$1; sub(/\{.*/, "", metric) }
		metric == name && (want == "" || index($0, want) > 0) { sum += $NF }
		END { printf "%.0f", sum+0 }
	' "$1"
}

metric_max() { # $1=文件 $2=指标
	awk -v name="$2" '
		{ metric=$1; sub(/\{.*/, "", metric) }
		metric == name && $NF > max { max=$NF }
		END { printf "%.3f", max+0 }
	' "$1"
}

metric_delta() { # $1=指标 $2=可选标签片段
	local after before
	after=$(metric_sum "$W/metrics.after" "$1" "${2:-}")
	before=$(metric_sum "$W/metrics.before" "$1" "${2:-}")
	awk -v a="$after" -v b="$before" 'BEGIN { printf "%.0f", a-b }'
}

# 在没有设备连接断线干扰的REST路径上验证完整影子状态机：精确杀掉目标
# thing所在分片的真实leader进程，剩余多数派恢复后版本必须从1严格推进到2。
# 它量的是恢复窗口与正确性，不冒充持续负载吞吐。
failover_probe() {
	local leader_i=-1 leader_line="" shard="" i access_i=-1 thing token admin
	local access_has_replica=1 placement_start=0 j member_i
	local metrics_file="$W/failover.metrics" started_ms recovered_ms response
	local bench_pid=0 bench_json="$W/failover-load.json" ready_file="$W/failover-load.ready"
	local load_published=0 load_delivered=0 load_rejected=0 load_timeouts=0
	local load_errors=0 load_max_gap=0 load_last_ok=0
	for i in "${!NODE_IDS[@]}"; do
		curl -sf -H "Authorization: Bearer ${TOKENS[$i]}" \
			"${ADMINS[$i]}/metrics" > "$metrics_file" ||
			die "故障前无法读取节点 ${NODE_IDS[$i]} 指标"
		leader_line=$(awk '$1 ~ /^openiot_shadow_shard_is_leader/ && $NF == 1 { print; exit }' "$metrics_file")
		if [ -n "$leader_line" ]; then
			leader_i=$i
			shard=$(printf '%s\n' "$leader_line" | sed -n 's/.*shard="\([0-9][0-9]*\)".*/\1/p')
			break
		fi
	done
	[ "$leader_i" -ge 0 ] && [ -n "$shard" ] || die "没有从指标找到影子 shard leader"
	# RF<N 时优先从不承载目标 shard 的 Access 节点发压。只选“非 leader”
	# 可能仍落在 follower 上，覆盖不到生产拓扑里先定位副本、再追当前 leader
	# 的完整两跳路由。放置规则与 sessionPlacement 一致：按 replica ID 稳定
	# 排序后，从 (shard-base)%N 轮转取 RF 个连续节点。
	if [ "$SHADOW_RF" -gt 0 ] && [ "$SHADOW_RF" -lt "$NODES" ]; then
		placement_start=$(( (shard - 2) % NODES ))
		for i in "${!NODE_IDS[@]}"; do
			[ "$i" = "$leader_i" ] && continue
			access_has_replica=0
			for ((j = 0; j < SHADOW_RF; j++)); do
				member_i=$(( (placement_start + j) % NODES ))
				if [ "$i" = "$member_i" ]; then access_has_replica=1; break; fi
			done
			if [ "$access_has_replica" = 0 ]; then access_i=$i; break; fi
		done
	fi
	if [ "$access_i" -lt 0 ]; then
		access_has_replica=1
		for i in "${!NODE_IDS[@]}"; do
			if [ "$i" != "$leader_i" ]; then access_i=$i; break; fi
		done
	fi
	[ "$access_i" -ge 0 ] || die "没有可用的存活接入节点"

	thing=$(python3 -c '
import sys
target,count=int(sys.argv[1]),int(sys.argv[2])
for n in range(1000000):
 s=f"shadow-failover-{n}"; h=14695981039346656037
 for b in s.encode(): h=((h^b)*1099511628211)&((1<<64)-1)
 if 2+h%count == target: print(s); break
else: raise SystemExit("找不到目标分片thing")
' "$shard" "$SHADOW_SHARDS") || die "计算故障探针thing失败"
	token=${TOKENS[$access_i]}
	admin=${ADMINS[$access_i]}
	curl -sf -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
		-d '{}' "$admin/api/admin/v1/things/$thing" >/dev/null || die "预置故障探针thing失败"
	response=$(curl -sf -X PUT -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
		-d '{"state":{"reported":{"failover":1}}}' \
		"$admin/api/admin/v1/things/$thing/shadow") || die "故障前影子写入失败"
	printf '%s\n' "$response" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["version"]==1, d' ||
		die "故障前影子版本不是1"

	if [ "$FAILOVER_CLIENTS" -gt 0 ]; then
		"$W/mqttbench" -scenario shadow -broker "${MQTT_ADDRS[$access_i]}" \
			-admin "$admin" -token "$token" -clients "$FAILOVER_CLIENTS" \
			-shadow-shards "$SHADOW_SHARDS" -shadow-shard "$shard" \
			-shadow-ready-file "$ready_file" -duration "$FAILOVER_DURATION" \
			-warmup "$FAILOVER_WARMUP" -prefix "shadow-failover-load" \
			-label "shadow/failover-load" -out /dev/null \
			${TLS_ARGS[@]+"${TLS_ARGS[@]}"} > "$bench_json" &
		bench_pid=$!
		for _ in $(seq 1 600); do
			[ -f "$ready_file" ] && break
			kill -0 "$bench_pid" 2>/dev/null || die "故障负载在就绪前退出"
			sleep 0.1
		done
		[ -f "$ready_file" ] || die "故障负载客户端120秒内未就绪"
		sleep "$FAILOVER_WARMUP"
		sleep "$FAILOVER_KILL_DELAY"
	fi

	started_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
	kill "${PIDS[$leader_i]}" 2>/dev/null || die "停止影子leader ${NODE_IDS[$leader_i]} 失败"
	for _ in $(seq 1 100); do
		leader_line=""
		for i in "${!NODE_IDS[@]}"; do
			[ "$i" = "$leader_i" ] && continue
			if curl -sf -H "Authorization: Bearer ${TOKENS[$i]}" \
				"${ADMINS[$i]}/metrics" > "$metrics_file"; then
				leader_line=$(awk -v shard="$shard" \
					'$1 ~ /^openiot_shadow_shard_is_leader/ && index($1,"shard=\"" shard "\"") && $NF == 1 { print; exit }' \
					"$metrics_file")
			fi
			[ -n "$leader_line" ] && break
		done
		[ -n "$leader_line" ] && break
		sleep 0.1
	done
	[ -n "$leader_line" ] || die "杀掉 ${NODE_IDS[$leader_i]} 后10秒内没有新影子leader"

	# 写请求在旧leader崩溃时可能处于“已提交但应答丢失”的模糊状态，不能
	# 裸重试。这里每次都带expected version=1：状态机串行化保证最多一次能
	# 成功推进到2；409表示先前那次已经提交，随后GET仍须核对最终内容。
	local write_code=000 write_body="$W/failover-write.json"
	for _ in $(seq 1 100); do
		write_code=$(curl -sS -o "$write_body" -w '%{http_code}' -X PUT \
			-H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
			-d '{"state":{"reported":{"failover":2}},"version":1}' \
			"$admin/api/admin/v1/things/$thing/shadow" 2>/dev/null || printf 000)
		case "$write_code" in
			200|409) break ;;
		esac
		sleep 0.1
	done
	case "$write_code" in
		200|409) ;;
		*) die "leader切换后10秒内影子写入未恢复（HTTP ${write_code}）" ;;
	esac
	recovered_ms=$(python3 -c 'import time; print(time.time_ns()//1000000)')
	if [ "$write_code" = 200 ]; then
		response=$(cat "$write_body")
		printf '%s\n' "$response" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["version"]==2, d' ||
			die "leader切换后影子版本没有严格推进到2"
	fi
	response=$(curl -sf -H "Authorization: Bearer $token" \
		"$admin/api/admin/v1/things/$thing/shadow") || die "leader切换后读取影子失败"
	printf '%s\n' "$response" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["version"]==2, d
assert d["state"]["reported"]["failover"]==2, d
' || die "leader切换后影子内容或版本回退"
	if [ "$bench_pid" -gt 0 ]; then
		wait "$bench_pid" || die "故障负载压测器失败"
		read load_published load_delivered load_rejected load_timeouts load_errors load_max_gap load_last_ok <<EOF
$(python3 -c '
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
print(d.get("published",0),d.get("delivered",0),d.get("shadowRejected",0),
      d.get("shadowTimeouts",0),d.get("errors",0),
      d.get("shadowMaxAcceptedGapMs",0),d.get("shadowLastAcceptedUnixMs",0))
' "$bench_json")
EOF
		[ "$load_delivered" -gt 0 ] || die "故障负载没有accepted"
		[ "$load_published" -eq $((load_delivered + load_rejected + load_timeouts)) ] ||
			die "故障负载终态不守恒：published=$load_published accepted=$load_delivered rejected=$load_rejected timeout=$load_timeouts"
		[ "$load_errors" -eq $((load_rejected + load_timeouts)) ] ||
			die "故障负载错误归因不完整：errors=$load_errors rejected=$load_rejected timeout=$load_timeouts"
		[ "$load_last_ok" -gt "$recovered_ms" ] || die "leader恢复后没有新的accepted"
	fi
	if [ "$OUT" != /dev/null ]; then
		python3 -c '
import json,sys
out,leader,shard,access,recovery=sys.argv[1:6]
d={"scenario":"shadow/failover","nodes":int(sys.argv[6]),
   "shadowShards":int(sys.argv[7]),"shadowRF":int(sys.argv[8]),
   "mqttTLS":sys.argv[9]=="1","leader":leader,"shard":int(shard),
   "access":access,"recoveryMs":int(recovery),"version":2,
   "prefixPreserved":True,"accessHasReplica":sys.argv[18]=="1"}
if int(sys.argv[10]):
 d.update({"loadClients":int(sys.argv[10]),"published":int(sys.argv[11]),
  "delivered":int(sys.argv[12]),"rejected":int(sys.argv[13]),
  "timeouts":int(sys.argv[14]),"errors":int(sys.argv[15]),
  "maxAcceptedGapMs":float(sys.argv[16]),"lastAcceptedUnixMs":int(sys.argv[17])})
with open(out,"a",encoding="utf-8") as f: f.write(json.dumps(d,separators=(",",":"))+"\n")
' "$OUT" "${NODE_IDS[$leader_i]}" "$shard" "${NODE_IDS[$access_i]}" \
			"$((recovered_ms - started_ms))" "$NODES" "$SHADOW_SHARDS" "$SHADOW_RF" "$MQTT_TLS" \
			"$FAILOVER_CLIENTS" "$load_published" "$load_delivered" "$load_rejected" \
			"$load_timeouts" "$load_errors" "$load_max_gap" "$load_last_ok" \
			"$access_has_replica"
	fi
	printf '  故障切换  leader=%s shard=%s access=%s 恢复写入=%d ms  version=2\n' \
		"${NODE_IDS[$leader_i]}" "$shard" "${NODE_IDS[$access_i]}" "$((recovered_ms - started_ms))"
	if [ "$FAILOVER_CLIENTS" -gt 0 ]; then
		printf '    持续负载  clients=%d published=%d accepted=%d rejected=%d timeout=%d max-gap=%s ms\n' \
			"$FAILOVER_CLIENTS" "$load_published" "$load_delivered" "$load_rejected" \
			"$load_timeouts" "$load_max_gap"
	fi
}

if [ "${NODES}" = 1 ] && { [ "${SHADOW_SHARDS}" != 0 ] || [ "${SHADOW_RF}" != 0 ]; }; then
	die "单节点不支持影子分片（standalone 拒绝 cluster.replication，CLU-070）"
fi
if [ "${NODES}" = 1 ]; then
	preflight_ports "${BASE}" "$((BASE + 1))"
	start_single
else
	ports=()
	for ((i = 0; i < NODES; i++)); do
		ports+=("$((BASE + 10 + i * 10))" "$((BASE + 11 + i * 10))" "$((BASE + 12 + i * 10))")
	done
	preflight_ports "${ports[@]}"
	start_cluster
fi

topology="n${NODES}-sh${SHADOW_SHARDS}"
if [ "${SHADOW_RF}" != 0 ]; then topology="${topology}-rf${SHADOW_RF}"; fi
if [ "${MQTT_TLS}" = 1 ]; then topology="${topology}-mtls"; fi
if [ -n "${RTT}" ]; then topology="${topology}-rtt${RTT}"; fi
echo "影子并发阶梯（${NODES} 节点，shadow_shards=${SHADOW_SHARDS}，shadow_rf=${SHADOW_RF}，mtls=${MQTT_TLS}，rtt=${RTT:-default}）"
for n in ${LADDER//,/ }; do
	scrape_metrics "$W/metrics.before"
	json=$("$W/mqttbench" -scenario shadow -broker "${BROKERS}" \
		-admin "${ADMIN}" -token "${TOKEN}" -clients "$n" \
		-duration "${DURATION}" -warmup "${WARMUP}" -prefix "shp${NODES}s${SHADOW_SHARDS}r${SHADOW_RF}x$n" \
		-label "shadow/${topology}-clients-$n" -out /dev/null \
		-shadow-response-qos "$SHADOW_RESPONSE_QOS" \
		-shadow-response-timeout "$SHADOW_RESPONSE_TIMEOUT" \
		${TLS_ARGS[@]+"${TLS_ARGS[@]}"}) || die "并发 $n 失败"
	scrape_metrics "$W/metrics.after"
	plocal=$(metric_delta openiot_shadow_proposals_total 'leader="local"')
	premote=$(metric_delta openiot_shadow_proposals_total 'leader="remote"')
	punknown=$(metric_delta openiot_shadow_proposals_total 'leader="unknown"')
	perrors=$(metric_delta openiot_shadow_proposal_errors_total)
	raft=$(metric_delta openiot_shadow_raft_proposals_total)
	logical=$(metric_delta openiot_shadow_leader_merge_logical_total)
	physical=$(metric_delta openiot_shadow_leader_merge_physical_total)
	full=$(metric_delta openiot_shadow_leader_merge_full_total)
	drained=$(metric_delta openiot_shadow_leader_merge_drained_total)
	blocked=$(metric_delta openiot_shadow_leader_merge_blocked_total)
	writer_errors=$(metric_delta openiot_writer_errors_total)
	writer_timeouts=$(metric_delta openiot_writer_timeouts_total)
	writer_flushed=$(metric_delta openiot_writer_flushed_total)
	writer_pending_after=$(metric_sum "$W/metrics.after" openiot_writer_pending)
	proposal_count=$(metric_delta openiot_shadow_proposal_duration_ms_count)
	proposal_le_2500=$(metric_delta openiot_shadow_proposal_duration_ms_bucket 'le="2500"')
	proposal_over_2500=$((proposal_count - proposal_le_2500))
	merge_peak=$(metric_sum "$W/metrics.after" openiot_shadow_leader_merge_peak_queued)
	responses=$(metric_delta openiot_reserved_responses_total)
	response_no_sub=$(metric_delta openiot_reserved_response_no_subscriber_total)
	service_over_1s=$(metric_delta openiot_reserved_service_over_1s_total)
	service_max_ms=$(metric_max "$W/metrics.after" openiot_reserved_service_max_ms)
	commit_waits=$(metric_delta openiot_shadow_commit_waits_total)
	commit_wait_over_1s=$(metric_delta openiot_shadow_commit_wait_over_1s_total)
	commit_wait_max_ms=$(metric_max "$W/metrics.after" openiot_shadow_commit_wait_max_ms)
	inflight_queued=$(metric_delta openiot_outbound_inflight_queued_total)
	inflight_failed=$(metric_delta openiot_outbound_inflight_failed_total)
	enqueue_failed=$(metric_delta openiot_outbound_enqueue_failed_total)
	if [ "${SHADOW_SHARDS}" != 0 ] && { [ "$physical" -le 0 ] || [ "$raft" -le 0 ]; }; then
		die "分片影子轮没有采到物理Raft/leader合批计数，归因结果无效"
	fi
	printf '%s\n' "$json" | python3 -c '
import json,sys
d=json.load(sys.stdin)
out,plocal,premote,punknown,perrors,raft,logical,physical,full,drained,blocked,writer_errors,writer_timeouts,writer_flushed,writer_pending_after,proposal_count,proposal_over_2500,merge_peak,responses,response_no_sub,service_over_1s,service_max_ms,commit_waits,commit_wait_over_1s,commit_wait_max_ms,inflight_queued,inflight_failed,enqueue_failed=sys.argv[1:]
for key,value in {
 "shadowProposalLocal":plocal,"shadowProposalRemote":premote,
 "shadowProposalUnknown":punknown,"shadowProposalErrors":perrors,
 "shadowRaftProposals":raft,"shadowMergeLogical":logical,
 "shadowMergePhysical":physical,"shadowMergeFull":full,
 "shadowMergeDrained":drained,"shadowMergeBlocked":blocked,
 "writerErrors":writer_errors,"writerTimeouts":writer_timeouts,
 "writerFlushed":writer_flushed,"writerPendingAfter":writer_pending_after,
 "shadowProposalDurationCount":proposal_count,
 "shadowProposalDurationOver2500Ms":proposal_over_2500,
 "shadowMergePeakQueued":merge_peak,
 "reservedResponses":responses,"reservedResponseNoSubscriber":response_no_sub,
 "reservedServiceOver1s":service_over_1s,
 "shadowCommitWaits":commit_waits,
 "shadowCommitWaitOver1s":commit_wait_over_1s,
 "outboundInflightQueued":inflight_queued,
 "outboundInflightFailed":inflight_failed,
 "outboundEnqueueFailed":enqueue_failed,
}.items(): d[key]=int(value)
d["reservedServiceMaxMs"]=float(service_max_ms)
d["shadowCommitWaitMaxMs"]=float(commit_wait_max_ms)
d["shadowMergeLogicalPerPhysical"]=(d["shadowMergeLogical"]/d["shadowMergePhysical"]
 if d["shadowMergePhysical"] else 0)
problem=""
if d.get("errors",0): problem=f"影子更新出现 {d['"'"'errors'"'"']} 个错误"
elif d.get("published") != d.get("delivered"):
 problem=f"影子发送/accepted不等：{d.get('"'"'published'"'"')}/{d.get('"'"'delivered'"'"')}"
if out != "/dev/null":
 target=out+(".invalid" if problem else "")
 with open(target,"a",encoding="utf-8") as f: f.write(json.dumps(d,separators=(",",":"))+"\n")
if problem: raise SystemExit(problem)
print(f"  并发 {d['"'"'clients'"'"']:4d}  {d['"'"'deliverPerSec'"'"']:7.1f} 条/秒  "
      f"p50 {d['"'"'latencyP50Ms'"'"']:8.2f} ms  p99 {d['"'"'latencyP99Ms'"'"']:8.2f} ms")
if d["shadowMergePhysical"]:
 print(f"    Raft物理日志 {d['"'"'shadowRaftProposals'"'"']}，leader合批 "
       f"{d['"'"'shadowMergeLogical'"'"']}/{d['"'"'shadowMergePhysical'"'"']}="
       f"{d['"'"'shadowMergeLogicalPerPhysical'"'"']:.2f} 条/日志，满批 {d['"'"'shadowMergeFull'"'"']}")
' "$OUT" "$plocal" "$premote" "$punknown" "$perrors" "$raft" \
		"$logical" "$physical" "$full" "$drained" "$blocked" \
		"$writer_errors" "$writer_timeouts" "$writer_flushed" \
		"$writer_pending_after" "$proposal_count" \
		"$proposal_over_2500" "$merge_peak" "$responses" "$response_no_sub" \
		"$service_over_1s" "$service_max_ms" \
		"$commit_waits" "$commit_wait_over_1s" "$commit_wait_max_ms" \
		"$inflight_queued" "$inflight_failed" "$enqueue_failed" ||
		die "并发 $n 失败"
done
if [ "$FAILOVER" = 1 ]; then
	failover_probe
fi
