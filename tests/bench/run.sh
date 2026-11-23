#!/usr/bin/env bash
#
# 基准测试：单节点与三节点集群各跑一遍同一组场景（PRD TST-060 至 TST-064）。
#
# 为什么要有这套：README 里的吞吐与连接数一旦写上去，就必须有人能在自己
# 机器上复现（TST-064「不发布无法复现的数字」）。这个脚本连同它写出的
# env.json 就是那份可复现的东西——同一条命令、同一组参数、同一份环境记录。
#
# 它不进 make gate：门禁只依赖 Go 工具链，而这套要起进程、要 openssl（集群
# 的复制证书）、要 python3（汇总表），而且一轮要跑好几分钟（与 tests/cluster
# 同一条理由）。
#
# 用法：
#   tests/bench/run.sh                     单节点与集群都跑，结果打在终端
#   MODE=standalone tests/bench/run.sh     只跑单节点
#   RESULTS=docs/baseline/throughput tests/bench/run.sh   把原始数据写入仓库
#   CONNS=4000 DURATION=5s tests/bench/run.sh             跑快一点（数字不可比）
#
# 两件测量上的事先说清楚：
#
#   1. 压测器与被测进程在同一台机器上，它们抢同一批核。因此这里的吞吐是
#      「一台机器上端到端能跑多少」，而不是 broker 独占整机的上限——后者
#      需要第二台机器压，而那不是任何人都能复现的环境。
#   2. 连接数的天花板来自本机的临时端口范围（macOS 默认 16383 个），不是
#      服务端。单节点测 12000 条是为了留出余量；要测更多得放宽 portrange
#      或从多个源地址发起。
set -euo pipefail

cd "$(dirname "$0")/../.."

MODE=${MODE:-both}
WORK=${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/openiot-bench.XXXXXX")} || exit 1
RESULTS=${RESULTS:-}
BASE_PORT=${BASE_PORT:-42000}

# 场景规模。默认值是在 11 核笔记本上一轮约 8 分钟的那一组。
DURATION=${DURATION:-15s}
WARMUP=${WARMUP:-3s}
CONNS=${CONNS:-12000}
CONN_CONC=${CONN_CONC:-256}
# 建连那一栏的轮间离散比它自己的量级还大：同一天同一台机器、同一份代码
# 量到过 27 到 163 条/秒。所以它**默认跑多轮取中位数**，而不是把「跑几轮」
# 留给人记得——上一版入库的 cluster.jsonl 里那个单轮 70.3 就是忘了的结果，
# 结果页只好用一段话解释它为什么不算数。单轮比较两个版本至少误导过一次
# （70.3 对 100.6 看上去像改坏了，而五轮 A/B 的区间并不重叠）。
CONNECT_ROUNDS=${CONNECT_ROUNDS:-3}
# 两轮建连之间必须空出 TIME_WAIT。
#
# 一轮结束时那上万条连接同时关闭，它们占的本地端口要在 2×MSL 之后才回到
# 临时端口池（macOS 的 net.inet.tcp.msl 默认 15 秒，即 30 秒）。紧接着跑
# 下一轮会撞上端口耗尽，而现象完全不像端口问题：实测第 2 轮 12,000 条里
# 只建成 4,127 条、5,870 个错误、速率从 32,423 掉到 1,999 条/秒，看上去像
# 「服务端在第二轮垮了」。
#
# 不用 SO_REUSEADDR 一类的办法绕过去：那会掩盖这台机器真实的连接周转能力，
# 而容量规划恰好要知道它。
CONNECT_ROUND_GAP=${CONNECT_ROUND_GAP:-35}
HOLD=${HOLD:-10s}
PAIRS=${PAIRS:-100}
PAIR_RATE=${PAIR_RATE:-1000}
PAIR_RATE_OVER=${PAIR_RATE_OVER:-3000}
# 集群的连接数与跨节点速率单列。
#
# 上一份基线的跨节点零丢弃上限是每秒六万，同节点十万。当时转发
# 同步等待一次往返；CLU-139 已改为按对端异步合批，所以六万只是修改前数字，
# 新上限必须由下面的阶梯重新量出，不得直接沿用。
#
# CROSS_RATE 的默认值刻意停在四万而不是六万：它是「基础那一级」，要的是一个
# 延迟也好看的点（p50 0.47 ms），上限交给阶梯去量。
CLUSTER_CONNS=${CLUSTER_CONNS:-6000}
CROSS_RATE=${CROSS_RATE:-400}
CROSS_RATE_OVER=${CROSS_RATE_OVER:-2000}
CROSS_LADDER=${CROSS_LADDER-500,600,700,800,1000,1200,1500}
# 同节点也要一级阶梯，理由与跨节点那一级不同：同节点走的是与单节点**同一条**
# 路径（复制层不在消息热路径上），所以它必须压到与单节点同一个高度，否则结果
# 表里「单节点 15 万、集群同节点 10 万」会被读成「集群把同节点吞吐压低了」，
# 而那两个数之差只是我们压了多少。
LOCAL_LADDER=${LOCAL_LADDER-1500}

# 模式之间的冷却。MODE=both 把单节点与集群两组串着跑，而前一组刚压过十几万
# 条/秒，磁盘与页缓存都还在刷——集群那组的第一个场景是建连，它每条 CONNECT
# 要等一次 fsync 过的 raft 提交，于是**最容易被前一组的尾巴压低**。实测串着
# 跑量到 27 条/秒，隔开之后是另一个量级（见 docs/baseline/throughput.md 的
# 「建连那一栏为什么要隔开量」）。
COOLDOWN=${COOLDOWN:-60}

# 速率阶梯：**零丢弃的上限是量出来的**，不是压测器压了多少。
#
# 为什么这一档必须有：基础那一行 offered 10 万、投递 10 万、丢弃 0，它证明的
# 是「10 万以内不丢」，而不是「10 万就是上限」。两者被当成一回事是这组数最
# 容易踩的误读——README 一度只登基础那一个数，读者无从知道余量还有多少，而
# 跨节点那一栏（offered 4 万、投递 4 万）看上去就像「集群只能跑四万」。
#
# 阶梯逐级加压，直到投递跟不上 offered：于是结果页能写成「零丢弃到 X，再往上
# 投递掉到 Y」，这才是可以拿去规划容量的形状。空字符串表示不跑这一档。
#
# 三个阶梯变量用 ${VAR-默认} 而不是 ${VAR:-默认}：后者对**空值**也会取默认，
# 于是文档里写的「置空则不跑」根本做不到，调一轮快的仍要等完整阶梯。
PAIR_LADDER=${PAIR_LADDER-1500,2000,2500}
SUBS=${SUBS:-1000}
FANOUT_RATE=${FANOUT_RATE:-100}
SHADOW_CLIENTS=${SHADOW_CLIENTS:-10}
PAYLOAD=${PAYLOAD:-64}

BIN="${WORK}/openiotd"
BENCH="${WORK}/mqttbench"

# say 与 info 打到 stderr：run_standalone / run_cluster 的 stdout 是结果文件
# 的路径（主流程用 $(...) 取），进度写到 stdout 会连同路径一起被吃掉。
say()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
info() { printf '      %s\n' "$*" >&2; }
die()  { printf '\033[1;31mFAIL\033[0m  %s\n' "$*" >&2; exit 1; }

PIDS=()
cleanup() {
	for pid in ${PIDS[@]+"${PIDS[@]}"}; do
		kill "${pid}" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	if [ "${KEEP_WORK:-0}" = 1 ]; then
		echo "工作目录保留在 ${WORK}"
	else
		rm -rf "${WORK}"
	fi
}
trap cleanup EXIT INT TERM

command -v python3 >/dev/null 2>&1 || die "需要 python3（只用于最后的汇总表）"

# 二进制可以由外部注入。
#
# 两个用处：在没有 Go 工具链的机器上跑同一套场景，以及 A/B 两臂必须使用
# **逐字节相同**的产物时——各臂自己编译一次的话，差值里就混进了编译期的
# 不确定性（内联决策、符号布局），而那不是被测的东西。所以这里只复制。
if [ -n "${OPENIOTD_BIN:-}" ] && [ -n "${MQTTBENCH_BIN:-}" ]; then
	say "使用注入的二进制：${OPENIOTD_BIN} / ${MQTTBENCH_BIN}"
	cp "${OPENIOTD_BIN}" "${BIN}"
	cp "${MQTTBENCH_BIN}" "${BENCH}"
	chmod +x "${BIN}" "${BENCH}"
elif [ -n "${OPENIOTD_BIN:-}" ] || [ -n "${MQTTBENCH_BIN:-}" ]; then
	die "OPENIOTD_BIN 与 MQTTBENCH_BIN 必须同时给：只注入一个会让两个产物来自不同构建"
else
	say "构建 openiotd 与 mqttbench"
	command -v go >/dev/null 2>&1 || die "需要 Go 工具链；或用 OPENIOTD_BIN/MQTTBENCH_BIN 注入已构建的二进制"
	go build -o "${BIN}" ./cmd/openiotd
	go build -o "${BENCH}" ./tools/mqttbench
fi

# 解除与吞吐有关的配额。
#
# 默认配额是对齐 AWS 的（QTA-001），其中每连接 100 条/秒、全局 2 万条/秒
# 这两条会先于任何引擎瓶颈生效——不解除的话，量到的是限速器的设定值，
# 而那个数字早就写在 docs/prd/17-quotas.md 里，测它没有意义。
#
# connect_rate_per_client 保持默认的 1/s 不动：每个 clientId 在一轮里只连
# 一次，它不构成瓶颈，而放宽它等于放宽唯一一道防重连风暴的闸。
QUOTA_BLOCK='quota:
  connect_rate: 0
  handshake_rate: 0
  publish_rate_per_conn: 0
  throughput_per_conn: 0
  inbound_publish_rate: 0
  outbound_publish_rate: 0
  subscribe_rate: 0
  shadow_rate_per_thing: 0
  shadow_api_rate: 0'

# CHECK_ROUND_PY 判定一轮建连是不是降级轮。
#
# 压测器对「只建成一部分」不报错是对的——密度验收要的正是失败明细。但在
# 基线这条路径上，一轮降级的数据悄悄进表比没有数据更糟：它会被读成服务端
# 的能力下降，而真因多半在压测端（端口、fd、TIME_WAIT）。
CHECK_ROUND_PY='
import json, sys
path, want, label = sys.argv[1], int(sys.argv[2]), sys.argv[3]
row = {}
for line in open(path):
    d = json.loads(line)
    if d.get("scenario") == label:
        row = d
held = row.get("heldConns", 0)
errs = row.get("errors", 0)
if held < want * 0.99:
    sys.stderr.write("      %s: %d 条里只建成 %d 条（%d 个错误）；先查压测端的端口与 fd，再看服务端\n"
                     % (label, want, held, errs))
    sys.exit(1)
'

# run_connect_rounds 跑 CONNECT_ROUNDS 轮建连，每轮一行进 JSONL。
#
# 只有第一轮预置设备：后面几轮用同一批 clientId 重连，既省掉几十秒，也让
# 各轮之间唯一的变量就是机器当时的状态。每 clientId 1/s 的连接配额不受
# 影响——一轮的保持窗口就不止一秒。
#
# 中位数由汇总那一步算，原始的每一轮都留在 JSONL 里：登记中位数而丢掉分布，
# 下次就没法判断两个版本的区间重不重叠。
run_connect_rounds() { # $1=标签 $2=前缀 $3=连接数 $4=结果文件 其余=压测器参数
	local tag=$1 prefix=$2 clients=$3 out=$4
	shift 4
	local i setup
	for i in $(seq 1 "${CONNECT_ROUNDS}"); do
		setup=false
		if [ "${i}" = 1 ]; then
			setup=true
		else
			info "等 ${CONNECT_ROUND_GAP} 秒让上一轮的端口退出 TIME_WAIT"
			command sleep "${CONNECT_ROUND_GAP}"
		fi
		say "${tag}：第 ${i}/${CONNECT_ROUNDS} 轮（${clients} 条）"
		"${BENCH}" -scenario connect "$@" \
			-clients "${clients}" -concurrency "${CONN_CONC}" -hold "${HOLD}" \
			-setup=${setup} -prefix "${prefix}" -label "${tag}-round-${i}" >/dev/null ||
			die "${tag} 第 ${i} 轮失败"
		python3 -c "${CHECK_ROUND_PY}" "${out}" "${clients}" "${tag}-round-${i}" ||
			die "${tag} 第 ${i} 轮降级，本组作废"
	done
}

# ---------- 环境记录 ----------

write_env() { # $1=输出文件
	local os cpu cores mem
	os=$(uname -sr)
	cores=$( (sysctl -n hw.ncpu 2>/dev/null || nproc) | tr -d ' ')
	cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null ||
		grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- || echo unknown)
	mem=$( (sysctl -n hw.memsize 2>/dev/null ||
		awk '/MemTotal/{print $2*1024}' /proc/meminfo) | tr -d ' ')
	# 负载必须记下来。压测器与被测进程共用这台机器，所以「跑的时候机器在忙
	# 什么」是结果的一部分：同一天同一台机器上建连那一栏量到过 27 到 163
	# 条/秒，差别全在这里。不记的话，两轮对不上时无从判断是代码还是环境。
	local load
	load=$(uptime | sed -n 's/.*load averages*: *//p' | tr -d ',' | awk '{print $1}')
	python3 - "$1" "${os}" "${cpu# }" "${cores}" "${mem}" \
		"$(go version)" "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" \
		"$(ulimit -n)" "${load:-unknown}" <<'PY'
import json, sys
path, os_, cpu, cores, mem, gov, rev, nofile, load = sys.argv[1:10]
json.dump({
    "os": os_, "cpu": cpu, "cores": int(cores),
    "memoryBytes": int(mem), "go": gov, "commit": rev,
    "ulimitNofile": nofile, "loadAvg1m": load,
}, open(path, "w"), ensure_ascii=False, indent=2)
PY
	cat "$1"
}

# ---------- 单节点 ----------

wait_ready() { # $1=admin 端口 $2=pid $3=日志
	for _ in $(seq 1 600); do
		if ! kill -0 "$2" 2>/dev/null; then
			tail -20 "$3" >&2
			die "节点启动失败（$3）"
		fi
		if curl -sf -o /dev/null "http://127.0.0.1:$1/readyz" 2>/dev/null; then
			return 0
		fi
		sleep 0.2
	done
	tail -20 "$3" >&2
	die "节点未在 120 秒内就绪（$3）"
}

run_standalone() {
	local mqtt=$((BASE_PORT)) admin=$((BASE_PORT + 1))
	local data="${WORK}/standalone"
	say "单节点：初始化与启动"
	"${BIN}" init --data-dir "${data}" --node-id bench-single --log-level error >/dev/null
	cat > "${WORK}/standalone.yaml" <<YAML
node:
  data_dir: ${data}
listeners:
  mqtt_tls:
    enabled: false
  mqtt_plain:
    enabled: true
    addr: "127.0.0.1:${mqtt}"
  admin:
    enabled: true
    addr: "127.0.0.1:${admin}"
auth:
  mode: insecure-none
  acknowledge_insecure: true
log:
  level: warn
  format: text
${QUOTA_BLOCK}
YAML
	"${BIN}" serve --config "${WORK}/standalone.yaml" >"${WORK}/standalone.log" 2>&1 &
	local pid=$!
	PIDS+=("${pid}")
	wait_ready "${admin}" "${pid}" "${WORK}/standalone.log"

	local token out
	token=$(cat "${data}/admin-token")
	out="${WORK}/standalone.jsonl"
	scenarios_single "127.0.0.1:${mqtt}" \
		"http://127.0.0.1:${admin}" "${token}" "${out}"
	kill "${pid}" 2>/dev/null || true
	wait "${pid}" 2>/dev/null || true
	OUT_FILE="${out}"
}

# ---------- 三节点集群 ----------

run_cluster() {
	local nodes=(a b c)
	local pki="${WORK}/replpki"
	say "集群：生成复制链（与设备 CA 无关的一条独立信任链）"
	mkdir -p "${pki}"
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
		-keyout "${pki}/ca.key" -out "${pki}/ca.pem" \
		-subj "/CN=open-iot-core-bench-ca" >/dev/null 2>&1 || die "生成复制 CA 失败"
	for id in "${nodes[@]}"; do
		# CommonName 必须等于节点 ID（CLU-060 在握手时校验）。
		openssl req -newkey rsa:2048 -nodes \
			-keyout "${pki}/${id}.key" -out "${pki}/${id}.csr" \
			-subj "/CN=${id}" >/dev/null 2>&1 || die "生成 ${id} 的 CSR 失败"
		cat > "${pki}/${id}.ext" <<EXT
subjectAltName = DNS:${id}
extendedKeyUsage = serverAuth, clientAuth
EXT
		openssl x509 -req -in "${pki}/${id}.csr" -days 1 \
			-CA "${pki}/ca.pem" -CAkey "${pki}/ca.key" -CAcreateserial \
			-extfile "${pki}/${id}.ext" -out "${pki}/${id}.pem" >/dev/null 2>&1 ||
			die "签发 ${id} 的证书失败"
	done

	local peers="" replicas="" i
	for i in "${!nodes[@]}"; do
		peers="${peers}
      ${nodes[$i]}: \"127.0.0.1:$((BASE_PORT + 10 + i * 10 + 2))\""
		replicas="${replicas}
      ${nodes[$i]}: $((i + 1))"
	done

	say "集群：初始化三个数据目录并启动"
	local mqtt_ports=() admin_ports=()
	for i in "${!nodes[@]}"; do
		local id="${nodes[$i]}" data="${WORK}/${nodes[$i]}"
		local mqtt=$((BASE_PORT + 10 + i * 10)) admin=$((BASE_PORT + 11 + i * 10))
		local repl=$((BASE_PORT + 12 + i * 10))
		mqtt_ports+=("127.0.0.1:${mqtt}")
		admin_ports+=("${admin}")
		"${BIN}" init --data-dir "${data}" --node-id "${id}" --log-level error >/dev/null
		local mode=join seeds="  seeds: [\"127.0.0.1:$((BASE_PORT + 12))\"]"
		if [ "$i" = 0 ]; then mode=bootstrap; seeds=""; fi
		cat > "${WORK}/${id}.yaml" <<YAML
node:
  id: ${id}
  mode: ${mode}
  data_dir: ${data}
${seeds}
cluster:
  voters: 3
  max_view_staleness: 10s
  replication:
    addr: "127.0.0.1:${repl}"
    cert_file: ${pki}/${id}.pem
    key_file: ${pki}/${id}.key
    ca_file: ${pki}/ca.pem
    pump_interval: 100ms
    peers:${peers}
    replicas:${replicas}
listeners:
  mqtt_tls:
    enabled: false
  mqtt_plain:
    enabled: true
    addr: "127.0.0.1:${mqtt}"
  admin:
    enabled: true
    addr: "127.0.0.1:${admin}"
auth:
  mode: insecure-none
  acknowledge_insecure: true
log:
  level: warn
  format: text
${QUOTA_BLOCK}
YAML
	done
	# 三个节点共用同一份设备 CA：虽然这一轮走明文端点，但设备域的
	# 主题归属与会话分片仍要求它们是同一个集群。
	for id in b c; do
		rm -rf "${WORK}/${id}/pki"
		cp -R "${WORK}/a/pki" "${WORK}/${id}/pki"
	done
	local started=()
	for i in "${!nodes[@]}"; do
		"${BIN}" serve --config "${WORK}/${nodes[$i]}.yaml" >"${WORK}/${nodes[$i]}.log" 2>&1 &
		started+=("$!")
		PIDS+=("$!")
	done
	for i in "${!nodes[@]}"; do
		wait_ready "${admin_ports[$i]}" "${started[$i]}" "${WORK}/${nodes[$i]}.log"
	done

	local token out all cross
	token=$(cat "${WORK}/a/admin-token")
	out="${WORK}/cluster.jsonl"
	all="${mqtt_ports[0]},${mqtt_ports[1]},${mqtt_ports[2]}"
	# 订阅侧轮转一格：于是每一对的发布者与订阅者一定落在不同节点上，
	# 走的是 mesh 转发而不是本地投递。
	cross="${mqtt_ports[1]},${mqtt_ports[2]},${mqtt_ports[0]}"
	scenarios_cluster "${all}" "${cross}" \
		"http://127.0.0.1:${admin_ports[0]}" "${token}" "${out}"
	OUT_FILE="${out}"
}

# ---------- 场景组 ----------

# run_ladder 把 pairs 场景沿一条速率阶梯逐级重跑，用来量出**零丢弃的上限**。
#
# 它一律带 -setup=false：阶梯的每一级与基础那一级共用同一批设备和主题前缀，
# 预置只在基础那一级做一次（12000 台设备要几十秒，每级重做一遍纯属浪费）。
#
# 判据在结果里，不在这里：阶梯本身不断言，读的人看 publishPerSec 与
# deliverPerSec 何时分叉。脚本不替读者判定「上限是哪一级」，因为那取决于
# 他要的是零丢弃还是容忍百分之几。
run_ladder() { # $1=标签前缀 $2=阶梯（逗号分隔的每发布者速率） 其余=压测器参数
	local tag=$1 ladder=$2
	shift 2
	[ -n "${ladder}" ] || return 0
	local r total
	for r in ${ladder//,/ }; do
		total=$((PAIRS * r))
		say "${tag} 阶梯：${PAIRS} 对 × ${r} 条/秒（offered ${total} 条/秒）"
		"${BENCH}" -scenario pairs -pairs "${PAIRS}" -qos 0 -rate "${r}" -setup=false \
			-label "${tag}-ladder-${total}" "$@" >/dev/null ||
			die "${tag} 阶梯 ${r} 条/秒失败"
	done
}

# scenarios_single 是单节点那一组。
scenarios_single() {
	local addr=$1 admin=$2 token=$3 out=$4
	local common=(-admin "${admin}" -token "${token}" -out "${out}"
		-payload "${PAYLOAD}" -duration "${DURATION}" -warmup "${WARMUP}")

	say "single：连接并发与建连速率（${CONNS} 条 × ${CONNECT_ROUNDS} 轮）"
	info "预置 ${CONNS} 台设备要几十秒。授权仍然生效：没有策略的设备连 CONNECT 都过不了"
	run_connect_rounds "single/connect" bcs "${CONNS}" "${out}" \
		-broker "${addr}" "${common[@]}"

	say "single：一对一吞吐 QoS 0（${PAIRS} 对 × ${PAIR_RATE} 条/秒）"
	"${BENCH}" -scenario pairs -broker "${addr}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 0 -rate "${PAIR_RATE}" \
		-prefix p0s -label "single/pairs-qos0" >/dev/null || die "pairs QoS 0 失败"

	run_ladder "single/pairs-qos0" "${PAIR_LADDER}" \
		-broker "${addr}" "${common[@]}" -prefix p0s

	say "single：一对一吞吐 QoS 0 过载（${PAIRS} 对 × ${PAIR_RATE_OVER} 条/秒）"
	info "这一轮是刻意打过头的：它记的是丢弃从哪里开始，不是一个好看的数"
	info "阶梯量出的零丢弃上限是 15 万；20 万时丢 15.6%，这一级塌到 2.4 万——过载不是平滑降级"
	"${BENCH}" -scenario pairs -broker "${addr}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 0 -rate "${PAIR_RATE_OVER}" -setup=false \
		-prefix p0s -label "single/pairs-qos0-overload" >/dev/null || die "pairs QoS 0 过载失败"

	say "single：一对一吞吐 QoS 1（${PAIRS} 对 × ${PAIR_RATE} 条/秒，在途 16）"
	"${BENCH}" -scenario pairs -broker "${addr}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 1 -inflight 16 -rate "${PAIR_RATE}" \
		-prefix p1s -label "single/pairs-qos1" >/dev/null || die "pairs QoS 1 失败"

	say "single：扇出 QoS 0（1 个发布者 × ${SUBS} 个订阅者 × ${FANOUT_RATE} 条/秒）"
	"${BENCH}" -scenario fanout -broker "${addr}" "${common[@]}" \
		-subs "${SUBS}" -qos 0 -rate "${FANOUT_RATE}" \
		-prefix fos -label "single/fanout" >/dev/null || die "fanout 场景失败"

	say "single：影子更新（${SHADOW_CLIENTS} 个客户端，判据是 accepted 的到达）"
	info "这一条走复制日志，量的是控制面写入——与上面几条的消息面不在一个量级"
	"${BENCH}" -scenario shadow -broker "${addr}" "${common[@]}" \
		-clients "${SHADOW_CLIENTS}" \
		-prefix shs -label "single/shadow" >/dev/null || die "shadow 场景失败"
}

# scenarios_cluster 是三节点那一组。
#
# 它刻意同时跑「同节点」与「跨节点」两档：同一个集群里这两条路径差两个
# 数量级，只给一个数会让读者按错的那一条去规划容量。
scenarios_cluster() {
	local all=$1 cross=$2 admin=$3 token=$4 out=$5
	local common=(-admin "${admin}" -token "${token}" -out "${out}"
		-payload "${PAYLOAD}" -duration "${DURATION}" -warmup "${WARMUP}")

	say "cluster3：连接并发与建连速率（${CLUSTER_CONNS} 条 × ${CONNECT_ROUNDS} 轮，均摊到三个节点）"
	info "会话写入要过会话分区组的复制日志（CLU-111），这一条与单节点不可比"
	run_connect_rounds "cluster3/connect" bcc "${CLUSTER_CONNS}" "${out}" \
		-broker "${all}" "${common[@]}"

	# 等一下再往下跑。上一轮的几千条连接此刻正在断开，而每条断开都要经
	# 分片日志删一次会话记录；不等的话，下一轮的预置会撞上选举抖动带来的
	# 503（压测器会重试，但重试掉的那几秒会算进下一轮的预热）。
	command sleep 5

	say "cluster3：同节点一对一吞吐 QoS 0（${PAIRS} 对 × ${PAIR_RATE} 条/秒）"
	"${BENCH}" -scenario pairs -broker "${all}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 0 -rate "${PAIR_RATE}" \
		-prefix p0l -label "cluster3/pairs-qos0-local" >/dev/null || die "同节点 QoS 0 失败"

	run_ladder "cluster3/pairs-qos0-local" "${LOCAL_LADDER}" \
		-broker "${all}" "${common[@]}" -prefix p0l

	say "cluster3：跨节点一对一吞吐 QoS 0（${PAIRS} 对 × ${CROSS_RATE} 条/秒）"
	"${BENCH}" -scenario pairs -broker "${all}" -sub-broker "${cross}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 0 -rate "${CROSS_RATE}" \
		-prefix p0x -label "cluster3/pairs-qos0-cross" >/dev/null || die "跨节点 QoS 0 失败"

	run_ladder "cluster3/pairs-qos0-cross" "${CROSS_LADDER}" \
		-broker "${all}" -sub-broker "${cross}" "${common[@]}" -prefix p0x

	say "cluster3：跨节点一对一吞吐 QoS 0 过载（${PAIRS} 对 × ${CROSS_RATE_OVER} 条/秒）"
	"${BENCH}" -scenario pairs -broker "${all}" -sub-broker "${cross}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 0 -rate "${CROSS_RATE_OVER}" -setup=false \
		-prefix p0x -label "cluster3/pairs-qos0-cross-overload" >/dev/null || die "跨节点过载失败"

	say "cluster3：跨节点一对一吞吐 QoS 1（${PAIRS} 对 × ${CROSS_RATE} 条/秒，在途 16）"
	"${BENCH}" -scenario pairs -broker "${all}" -sub-broker "${cross}" "${common[@]}" \
		-pairs "${PAIRS}" -qos 1 -inflight 16 -rate "${CROSS_RATE}" \
		-prefix p1x -label "cluster3/pairs-qos1-cross" >/dev/null || die "跨节点 QoS 1 失败"

	say "cluster3：跨节点扇出 QoS 0（1 个发布者 × ${SUBS} 个订阅者 × ${FANOUT_RATE} 条/秒）"
	info "扇出的转发成本按消息算、不按订阅者算：一条消息转两次，对端各自本地扇出"
	"${BENCH}" -scenario fanout -broker "${all}" -sub-broker "${cross}" "${common[@]}" \
		-subs "${SUBS}" -qos 0 -rate "${FANOUT_RATE}" \
		-prefix foc -label "cluster3/fanout-cross" >/dev/null || die "跨节点扇出失败"

	say "cluster3：影子更新（${SHADOW_CLIENTS} 个客户端）"
	"${BENCH}" -scenario shadow -broker "${all}" "${common[@]}" \
		-clients "${SHADOW_CLIENTS}" \
		-prefix shc -label "cluster3/shadow" >/dev/null || die "shadow 场景失败"
}

# ---------- 汇总 ----------

summarize() { # $@=jsonl 文件
	python3 - "$@" <<'PY'
import json, sys

rows = []
for path in sys.argv[1:]:
    try:
        with open(path) as f:
            rows += [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        pass
if not rows:
    print("没有结果")
    raise SystemExit

def num(x, digits=0):
    return f"{x:,.{digits}f}" if x else "—"

print()
print("| 场景 | 客户端 | 发布/秒 | 投递/秒 | 丢弃 | 错误 | p50 | p99 | 建连/秒 | 每连接字节 |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
for r in rows:
    pub, dev = r.get("published", 0), r.get("delivered", 0)
    loss = "—"
    if pub and r["scenario"].find("fanout") < 0:
        pct = 100 * (1 - dev / pub)
        loss = f"{max(pct, 0):.2f}%"
    print("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
        r["scenario"], num(r.get("clients", 0)),
        num(r.get("publishPerSec", 0)), num(r.get("deliverPerSec", 0)), loss,
        num(r.get("errors", 0)),
        num(r.get("latencyP50Ms", 0), 2) + " ms" if r.get("latencyP50Ms") else "—",
        num(r.get("latencyP99Ms", 0), 2) + " ms" if r.get("latencyP99Ms") else "—",
        num(r.get("connectPerSec", 0)),
        num(r.get("bytesPerConn", 0))))
print()
# 有错误的那几级要单独点名。
#
# 「丢弃」这一列在 errors 非零时**不是吞吐**：出站队列满时 broker 按设计
# 断开慢订阅者（MQTT-035），于是那一级量到的是「订阅者死得多快」。实测
# 15 万那一级丢 65.89%，而同一行 errors=98——100 对里 98 对的订阅者已经
# 被断开。不把这个数并排显示，它看上去就是一个吞吐拐点。
#
# 判据：零丢弃上限只能在 errors=0 的那些级里取。
noisy = [r for r in rows if r.get("errors", 0) and "pairs" in r.get("scenario", "")]
if noisy:
    print("注：以下各级有连接被断开，其「投递/秒」不是吞吐——"
          "零丢弃上限只能在错误为 0 的级里取：")
    for r in noisy:
        print(f"      {r['scenario']}：{r['errors']} 个错误")
    print()

# 多轮场景（建连）另给一行中位数。
#
# 表里保留每一轮而不是只给中位数：区间才说明得了两个版本有没有差别，
# 而这一栏的轮间离散比它自己的量级还大。只登中位数等于把判据丢掉。
import statistics
families = {}
for r in rows:
    name = r["scenario"]
    if "-round-" not in name:
        continue
    families.setdefault(name.rsplit("-round-", 1)[0], []).append(r)
for name, group in families.items():
    vals = sorted(r.get("connectPerSec", 0) for r in group)
    if len(vals) < 2:
        continue
    print(f"中位数 {name}：{statistics.median(vals):,.0f} 条/秒"
          f"（{len(vals)} 轮，区间 {vals[0]:,.0f}–{vals[-1]:,.0f}）")

# 「—」在每连接字节那一栏有两种含义：这个场景不量内存，或者量了但没量出来。
# 后者必须说出来，否则一个测不出的空格看上去和「不适用」一样。
for r in rows:
    if r.get("memoryNote"):
        print(f"注：{r['scenario']} {r['memoryNote']}")
print()
PY
}

# ---------- 主流程 ----------

say "环境"
ENV_FILE="${WORK}/env.json"
write_env "${ENV_FILE}"

# 两个函数把结果文件名放进 OUT_FILE，而不是 echo 出来由 $(...) 接。
#
# 原先是 `OUTS+=("$(run_standalone | tail -1)")`，而命令替换是子 shell：
# 里面的 die 只杀得掉那个子 shell，外层照常往下走——第一轮集群预置失败时，
# 脚本把失败咽掉、继续跑完、退出码还是 0，连已经跑完的单节点数据都没落盘。
OUTS=()
collect() { # $1=模式名
	OUT_FILE=""
	"run_$1"
	[ -n "${OUT_FILE}" ] || die "$1 没有产出结果文件"
	OUTS+=("${OUT_FILE}")
	# 每跑完一组就落盘一次：后一组失败不该把前一组的数据一起带走。
	if [ -n "${RESULTS}" ]; then
		mkdir -p "${RESULTS}"
		cp "${ENV_FILE}" "${RESULTS}/env.json"
		cp "${OUT_FILE}" "${RESULTS}/$(basename "${OUT_FILE}")"
	fi
}

case "${MODE}" in
standalone) collect standalone ;;
cluster) collect cluster ;;
both)
	collect standalone
	if [ "${COOLDOWN}" -gt 0 ]; then
		say "冷却 ${COOLDOWN} 秒再跑集群那一组"
		info "不隔开的话，集群的建连那一栏量到的是「单节点那一组的尾巴」而不是集群"
		command sleep "${COOLDOWN}"
	fi
	collect cluster
	;;
*) die "MODE 只能取 standalone、cluster 或 both" ;;
esac

say "汇总"
summarize ${OUTS[@]+"${OUTS[@]}"}

if [ -n "${RESULTS}" ]; then
	say "原始数据写入 ${RESULTS}/"
	ls -1 "${RESULTS}"
fi
