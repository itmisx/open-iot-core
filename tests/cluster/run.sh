#!/usr/bin/env bash
#
# 起三个真的 openiotd 进程组成元数据集群，验收 v0.4 阶段 B 的那几条。
#
# 为什么要有这套：Go 测试里的三节点共用一个进程的内存，证明不了
# 「三个进程用 TLS 复制同一条日志」。而这里验的每一条都只有在真进程上
# 才会失败——证书的 CommonName 校验、日志落盘、杀掉一个节点之后多数派
# 还在不在、冷层有没有跟着复制。
#
# 不进 make gate：门禁只依赖 Go 工具链，而这套要 openssl 并且要起三个
# 进程等选举（与 tests/device-sdk 同一条理由）。
set -euo pipefail

cd "$(dirname "$0")/../.."

BASE_PORT=${BASE_PORT:-41000}
WORK=${WORK:-$(mktemp -d -t openiot-cluster)}
BIN="${WORK}/openiotd"


say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32mPASS\033[0m  %s\n' "$*"; }
die() { printf '\033[1;31mFAIL\033[0m  %s\n' "$*" >&2; exit 1; }

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

NODES=(a b c)

# 端口分配：每个节点三个（mqtt、admin、复制）
port_for() { # $1=节点下标 $2=偏移
	echo $((BASE_PORT + $1 * 10 + $2))
}

say "构建 openiotd"
go build -o "${BIN}" ./cmd/openiotd

say "生成复制链（与设备 CA 无关的一条独立信任链，CLU-068）"
PKI="${WORK}/replpki"
mkdir -p "${PKI}"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
	-keyout "${PKI}/ca.key" -out "${PKI}/ca.pem" \
	-subj "/CN=open-iot-core-replication-ca" >/dev/null 2>&1 ||
	die "生成复制 CA 失败"

for id in "${NODES[@]}"; do
	# CommonName 必须是节点 ID（CLU-060 在握手时校验）。
	#
	# SAN 里**故意只放节点 ID**，不放 localhost 也不放 127.0.0.1——
	# 而这个脚本拨号用的正是 127.0.0.1。节点身份按 CommonName 认、
	# 不按主机名（CLU-069、CLU-102），所以两者对不上是应该能通的。
	# 原先这里签的是 `DNS:localhost, IP:127.0.0.1`，注释还把它写成
	# 「拨号侧以 localhost 做主机名校验」——那是在把一个实现缺陷记成
	# 需求。缺陷的后果是 k8s 的复制证书（SAN 只含 Pod 的 DNS 名）
	# 一律握手失败，而本脚本因为恰好签了 localhost 一直是绿的。
	openssl req -newkey rsa:2048 -nodes \
		-keyout "${PKI}/${id}.key" -out "${PKI}/${id}.csr" \
		-subj "/CN=${id}" >/dev/null 2>&1 || die "生成 ${id} 的 CSR 失败"
	cat > "${PKI}/${id}.ext" <<EXT
subjectAltName = DNS:${id}
extendedKeyUsage = serverAuth, clientAuth
EXT
	openssl x509 -req -in "${PKI}/${id}.csr" -days 1 \
		-CA "${PKI}/ca.pem" -CAkey "${PKI}/ca.key" -CAcreateserial \
		-extfile "${PKI}/${id}.ext" \
		-out "${PKI}/${id}.pem" >/dev/null 2>&1 || die "签发 ${id} 的证书失败"
done

say "初始化三个数据目录"
PEERS=""
REPLICAS=""
for i in "${!NODES[@]}"; do
	id="${NODES[$i]}"
	repl_port=$(port_for "$i" 2)
	PEERS="${PEERS}
      ${id}: \"127.0.0.1:${repl_port}\""
	# 副本号：共识库按 uint64 认成员，而本项目的身份是字符串节点 ID
	# （CLU-002 修订）。显式写出来，不从 ID 推导——撞号的症状是日志
	# 复制错乱，而日志里看不出跟节点 ID 有关系。
	REPLICAS="${REPLICAS}
      ${id}: $((i + 1))"
done

declare -a ADMIN_PORTS
for i in "${!NODES[@]}"; do
	id="${NODES[$i]}"
	data="${WORK}/${id}"
	"${BIN}" init --data-dir "${data}" --node-id "${id}" --log-level error >/dev/null ||
		die "初始化 ${id} 失败"
	mqtt_port=$(port_for "$i" 0)
	admin_port=$(port_for "$i" 1)
	repl_port=$(port_for "$i" 2)
	ADMIN_PORTS[$i]=${admin_port}
	mode=join
	seeds="  seeds: [\"127.0.0.1:$(port_for 0 2)\"]"
	if [ "$i" = 0 ]; then
		mode=bootstrap
		seeds=""
	fi
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
    addr: "127.0.0.1:${repl_port}"
    cert_file: ${PKI}/${id}.pem
    key_file: ${PKI}/${id}.key
    ca_file: ${PKI}/ca.pem
    pump_interval: 100ms
    peers:${PEERS}
    replicas:${REPLICAS}
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${mqtt_port}"
  admin:
    enabled: true
    addr: "127.0.0.1:${admin_port}"
aws_api:
  enabled: true
  region: us-east-1
  endpoint_address: 127.0.0.1
log:
  level: warn
  format: text
YAML
done

say "三个节点共用同一份设备 CA"
# 各节点 init 时各自生成了 CA 与服务端证书，而同一台设备要能连上任意
# 节点，它们必须共用同一条链。把 a 的那份复制过去。
#
# 真实部署里这一步是「把同一份 CA 分发到各节点」（或各节点用同一个
# 外部 CA 签服务端证书），不是复制目录；这里把那件事做出来，好让围栏
# 验收能用同一张设备证书连任意节点。
for id in b c; do
	rm -rf "${WORK}/${id}/pki"
	cp -R "${WORK}/a/pki" "${WORK}/${id}/pki"
done

say "签发围栏验收用的设备证书"
cat > "${WORK}/a/policies/fence-p.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "iot:Connect",
    "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
  }]
}
JSON
for id in b c; do
	cp "${WORK}/a/policies/fence-p.json" "${WORK}/${id}/policies/fence-p.json"
done
"${BIN}" cert issue --data-dir "${WORK}/a" \
	--thing fence-dev --policy fence-p \
	--out "${WORK}/fence-dev" >/dev/null || die "签发围栏设备证书失败"

# 会话验收的设备要能订阅并接收自己的主题（围栏那张只允许 Connect）。
cat > "${WORK}/a/policies/queue-p.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Subscribe", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Subscribe", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}
JSON
for id in b c; do
	cp "${WORK}/a/policies/queue-p.json" "${WORK}/${id}/policies/queue-p.json"
done
"${BIN}" cert issue --data-dir "${WORK}/a" \
	--thing queue-dev --policy queue-p \
	--out "${WORK}/queue-dev" >/dev/null || die "签发会话设备证书失败"
# 证书登记在 a 的冷层里，而冷层随复制日志走（CLU-065），
# 所以 b、c 启动后会从日志拿到它——这正好一并验证了那条。
for id in b c; do
	rm -rf "${WORK}/${id}/certs"
done

say "启动三个节点"
for i in "${!NODES[@]}"; do
	id="${NODES[$i]}"
	"${BIN}" serve --config "${WORK}/${id}.yaml" >"${WORK}/${id}.log" 2>&1 &
	PIDS[$i]=$!
done

api() { # $1=节点下标，其余传给 curl
	local i=$1
	shift
	local token
	token=$(cat "${WORK}/${NODES[$i]}/admin-token")
	curl -sS --fail-with-body -H "Authorization: Bearer ${token}" "$@"
}

wait_ready() { # $1=节点下标
	local i=$1 port=${ADMIN_PORTS[$1]}
	for _ in $(seq 1 200); do
		if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
			cat "${WORK}/${NODES[$i]}.log" >&2
			die "节点 ${NODES[$i]} 启动失败"
		fi
		if curl -sf -o /dev/null "http://127.0.0.1:${port}/readyz"; then
			return 0
		fi
		sleep 0.1
	done
	cat "${WORK}/${NODES[$i]}.log" >&2
	die "节点 ${NODES[$i]} 未在 20 秒内就绪（集群模式下要先追平已提交日志）"
}

for i in "${!NODES[@]}"; do wait_ready "$i"; done
pass "三个节点都就绪（集群模式下就绪意味着已追平已提交日志）"

# ---------- 验收 ----------

say "在 a 上建策略与设备，到 b、c 上读"
api 0 -X PUT -H 'Content-Type: application/json' \
	--data-binary @- "http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/policies/cluster-p" >/dev/null <<'JSON' || die "建策略失败"
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "iot:Connect",
    "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
  }]
}
JSON

api 0 -X PUT -H 'Content-Type: application/json' \
	-d '{"policyName":"cluster-p","attributes":{"site":"sh"}}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/things/lamp-1" >/dev/null || die "建设备失败"

sleep 1
for i in 1 2; do
	got=$(api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/things/lamp-1")
	echo "${got}" | grep -q '"site"' ||
		die "节点 ${NODES[$i]} 读不到设备的完整记录（冷层没有跟着复制）：${got}"
	echo "${got}" | grep -q 'cluster-p' ||
		die "节点 ${NODES[$i]} 上设备的策略绑定不对：${got}"
done
pass "设备的冷层记录（含属性与策略绑定）在三个节点上一致"

for i in 1 2; do
	got=$(api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/policies/cluster-p")
	echo "${got}" | grep -q 'iot:Connect' ||
		die "节点 ${NODES[$i]} 读不到策略正文：${got}"
done
pass "策略正文在三个节点上一致（文档随日志走，各节点各自编译）"

say "从跟随者 b 写入，验证转发给 leader"
api 1 -X PUT -H 'Content-Type: application/json' \
	-d '{"policyName":"cluster-p"}' \
	"http://127.0.0.1:${ADMIN_PORTS[1]}/api/admin/v1/things/lamp-2" >/dev/null ||
	die "跟随者写入失败（Propose 应转发给 leader）"
sleep 1
api 2 "http://127.0.0.1:${ADMIN_PORTS[2]}/api/admin/v1/things/lamp-2" | grep -q lamp-2 ||
	die "跟随者写入的设备没有出现在 c 上"
pass "跟随者的写入转到 leader，三个节点都看到"

say "各节点的 revision 一致"
revs=""
for i in "${!NODES[@]}"; do
	# 解析 JSON 而不是对着它做字符串匹配：响应是缩进过的，
	# 而 sed 的单行模式抽不到跨行的字段（TST-116 记过这个反模式）。
	r=$(api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/node" |
		python3 -c 'import json,sys; print(json.load(sys.stdin).get("metaRevision",""))')
	# 抽不到就当失败：第一版把字段名写错，三个空字符串让 sort -u 得出
	# 一行，于是这条断言在「什么都没比」的情况下通过了
	case "${r}" in
	'' | *[!0-9]*) die "节点 ${NODES[$i]} 的 metaRevision 没抽到（字段名改了？）" ;;
	esac
	[ "${r}" -ge 3 ] || die "节点 ${NODES[$i]} 的 metaRevision = ${r}，至少该有 3 条命令"
	revs="${revs} ${r}"
done
uniq_count=$(echo ${revs} | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')
[ "${uniq_count}" = 1 ] || die "各节点 revision 不一致:${revs}"
pass "三个节点的 revision 相同:${revs}"

# 这一段是 AUTH-086 的全部意义：access key 以前是一份节点本地的 JSON，
# 只加在 a 上的钥匙打到 b 就是 401，运维得人手同步三份文件。
say "在 a 上发一把 access key，到 b、c 上用"
key_json=$(api 0 -X POST -H 'Content-Type: application/json' \
	-d '{"role":"reader","note":"cluster test"}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/awskeys") || die "发钥匙失败"
read_json() { python3 -c 'import json,sys; print(json.load(sys.stdin).get("'"$1"'",""))'; }
akey_id=$(printf '%s' "${key_json}" | read_json id)
akey_secret=$(printf '%s' "${key_json}" | read_json secret)
case "${akey_id}" in
AKIA*) ;;
*) die "access key id 没抽到或形状不对：${akey_id}" ;;
esac
[ -n "${akey_secret}" ] || die "secret 没抽到"

for i in "${!NODES[@]}"; do
	# 先过 a 自己这一关：签名器要是写错了，三个节点会一起红，而那
	# 看起来会像复制坏了。a 通过、b/c 不通过，才是复制的问题。
	code=$(python3 tests/cluster/sigv4_get.py \
		"http://127.0.0.1:${ADMIN_PORTS[$i]}/api/aws/things" "${akey_id}" "${akey_secret}") ||
		die "在 ${NODES[$i]} 上签名请求执行失败"
	[ "${code}" = 200 ] || die "在 ${NODES[$i]} 上用这把钥匙 = ${code}，期望 200"
done
pass "a 上发的 access key 在三个节点上都能用（secret 随同一条日志复制）"

# 停用也要跨节点生效，否则「怀疑泄露了先停掉」只停掉了收到请求的那台。
api 0 -X PATCH -H 'Content-Type: application/json' -d '{"disabled":true}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/awskeys/${akey_id}" >/dev/null ||
	die "停用失败"
# 跟随者上的生效窗口是**一次复制往返**，不是零。
#
# 这里原先写的是"立刻"，那是自写实现的一个副作用：它在 Propose 里同步
# 把日志发给每个对端、对端在同一次调用里就应用完，于是 HTTP 返回时全集群
# 都已生效。现在的跟随者是在提交之后异步 apply 的，所以要等。
# 这与 PRD 本来的承诺一致（CLU-049：全集群在 max_view_staleness 之内
# 生效），只是旧实现把那个窗口压成了零而让人以为它是零。
for i in 1 2; do
	code=""
	for _ in $(seq 1 50); do
		code=$(python3 tests/cluster/sigv4_get.py \
			"http://127.0.0.1:${ADMIN_PORTS[$i]}/api/aws/things" "${akey_id}" "${akey_secret}") ||
			die "在 ${NODES[$i]} 上签名请求执行失败"
		[ "${code}" = 401 ] && break
		sleep 0.1
	done
	[ "${code}" = 401 ] || die "在 ${NODES[$i]} 上停用 5 秒后仍然 = ${code}，期望 401"
done
pass "在 a 上停用，b、c 在一次复制往返之内也不认"

say "跨节点消息投递（阶段 D）"
# b 上订阅 telemetry/#，a 上发布一条，b 要收到。
# 用控制台的 MQTT 测试客户端：它的订阅走 Router.Subscribe，
# 投递路径与设备完全相同（见 tests/cluster/crossnode.py 的说明）。
python3 tests/cluster/crossnode.py \
	"${ADMIN_PORTS[1]}" "$(cat "${WORK}/b/admin-token")" \
	"${ADMIN_PORTS[0]}" "$(cat "${WORK}/a/admin-token")" 1.5 ||
	die "跨节点投递失败"
pass "a 上发布的消息投到了 b 上的订阅者（兴趣通告 + mesh 转发）"

say "保留消息跨节点（MQTT-122 至 MQTT-124）"
# 在 a 上发一条 retained，再到 b、c 上问"你自己有没有这条记录"。
#
# 判据取兼容面的 GetRetainedMessage（200/404）而不是"订阅时收到了"：
# 后者分不清「复制过去了」与「从 a 转发过来了」，而这条验收要证明的正是
# 前者——它也是杀掉 a 之后还能读到的前提（TST-156）。
#
# 主题刻意只用字母、数字与短横：签名脚本按单次编码构造规范路径，而 AWS
# 除 S3 之外对路径编码两遍（AUTH-077、CLAUDE.md 第 6 节第 13 条）。
# 带特殊字符的主题会在那条规则上失败，而症状是 401，完全不指向编码。
RETAINED_KEEP="sensors/cluster-keep"
RETAINED_GONE="sensors/cluster-gone"

rkey_json=$(api 0 -X POST -H 'Content-Type: application/json' \
	-d '{"role":"reader","note":"retained check"}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/awskeys") || die "发读钥匙失败"
rkey_id=$(printf '%s' "${rkey_json}" | read_json id)
rkey_secret=$(printf '%s' "${rkey_json}" | read_json secret)
[ -n "${rkey_secret}" ] || die "读钥匙的 secret 没抽到"

retained_code() { # $1=节点下标 $2=主题
	python3 tests/cluster/sigv4_get.py \
		"http://127.0.0.1:${ADMIN_PORTS[$1]}/api/aws/retainedMessage/$2" \
		"${rkey_id}" "${rkey_secret}" us-east-1 iotdata
}

for t in "${RETAINED_KEEP}" "${RETAINED_GONE}"; do
	python3 tests/cluster/retained.py \
		"${ADMIN_PORTS[0]}" "$(cat "${WORK}/a/admin-token")" "${t}" "hello-${t##*/}" ||
		die "在 a 上发 retained 失败：${t}"
done

# 先过 a 自己这一关：读不到说明发布本身没生效，而那看起来会像复制坏了。
for t in "${RETAINED_KEEP}" "${RETAINED_GONE}"; do
	code=$(retained_code 0 "${t}") || die "在 a 上读 retained 执行失败"
	[ "${code}" = 200 ] || die "在 a 上读 ${t} = ${code}，期望 200"
done

# 跨节点的生效窗口是一次广播，不是零（MQTT-125），所以这里要轮询。
for i in 1 2; do
	code=""
	for _ in $(seq 1 50); do
		code=$(retained_code "$i" "${RETAINED_KEEP}") ||
			die "在 ${NODES[$i]} 上读 retained 执行失败"
		[ "${code}" = 200 ] && break
		sleep 0.2
	done
	[ "${code}" = 200 ] ||
		die "${NODES[$i]} 上读不到 a 发的 retained（= ${code}）：广播没生效"
done
pass "a 上发的 retained 复制到了 b、c（事件广播 + 收端按 Version 合并）"

# 清除也要跨节点生效。这一条钉的是墓碑（MQTT-124）：清除只在本节点生效
# 的话，另外两个节点会继续对外提供一条已经被清除的 retained，而且不报错。
#
# 先等够一秒：retained_rate_per_topic 是每主题每秒 1 次（QTA-042，对齐
# AWS）。不等的话清除会被配额拒绝，而报错是"请求过多"——与复制毫无关系，
# 却长得很像这条用例在验的东西失败了。
sleep 1.2
python3 tests/cluster/retained.py \
	"${ADMIN_PORTS[0]}" "$(cat "${WORK}/a/admin-token")" "${RETAINED_GONE}" ||
	die "在 a 上清除 retained 失败"
for i in 0 1 2; do
	code=""
	for _ in $(seq 1 50); do
		code=$(retained_code "$i" "${RETAINED_GONE}") ||
			die "在 ${NODES[$i]} 上读 retained 执行失败"
		[ "${code}" = 404 ] && break
		sleep 0.2
	done
	[ "${code}" = 404 ] ||
		die "${NODES[$i]} 上清除后仍能读到 ${RETAINED_GONE}（= ${code}）：墓碑没传播"
done
pass "在 a 上清除，三个节点都不再提供它（墓碑随广播传播）"

say "影子跨节点一致（CLU-082）"
# 在 a 上写影子，到 b、c 上读。影子命令走同一条复制日志，
# 各节点在 apply 阶段用同一份状态机算出同样的结果。
api 0 -X PUT -H 'Content-Type: application/json' \
	-d '{"state":{"desired":{"interval":30,"mode":"run"}}}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/things/lamp-1/shadow" >/dev/null ||
	die "在 a 上写影子失败"
sleep 1
for i in 1 2; do
	got=$(api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/things/lamp-1/shadow")
	echo "${got}" | grep -q '"interval"' ||
		die "节点 ${NODES[$i]} 读不到影子（影子没有跟着复制）：${got}"
	# 版本必须一致：各节点算出不同的版本就意味着状态机不确定
	ver=$(echo "${got}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("version",""))')
	[ "${ver}" = 1 ] || die "节点 ${NODES[$i]} 上影子版本 = ${ver}，期望 1"
done
pass "影子在三个节点上一致（含版本号）"

# 冲突判定也必须是确定性的：所有节点按同样的日志前缀算出同样的冲突
code=$(curl -sS -o /dev/null -w '%{http_code}' \
	-H "Authorization: Bearer $(cat "${WORK}/a/admin-token")" \
	-X PUT -H 'Content-Type: application/json' \
	-d '{"state":{"desired":{"interval":5}},"version":99}' \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/things/lamp-1/shadow" || true)
[ "${code}" = 409 ] || die "版本不符的影子更新返回 HTTP ${code}，期望 409"
pass "版本冲突仍是 409（冲突是确定性的状态机结果，不是转发失败）"

say "Admin 扇出：列出全集群的在线连接（契约 12）"
# 两台不同的设备分别连 a 与 b，然后从 c 上列举——要看到两条。
# 只看本节点的话 c 上是空的，而控制台不会显示"这份列表不完整"。
#
# 用 Admin API 签发，不用离线的 cert issue：后者直接写本节点的冷层，
# 不经复制日志，于是 b 上根本没有这张证书（第一版就是这么失败的，
# 症状是 CONNECT 原因码 5，看起来像策略问题）。
for thing in fan-a fan-b; do
	mkdir -p "${WORK}/${thing}"
	# 先建设备：签发要求它已经存在（证书绑定的设备不能是凭空的）
	api 0 -X PUT -H 'Content-Type: application/json' \
		-d '{"policyName":"fence-p"}' \
		"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/things/${thing}" >/dev/null ||
		die "建设备 ${thing} 失败"
	api 0 -X POST -H 'Content-Type: application/json' \
		-d "{\"thing\":\"${thing}\",\"policyName\":\"fence-p\"}" \
		"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/certificates/issue" \
		> "${WORK}/${thing}/resp.json" || die "签发 ${thing} 失败"
	python3 - "${WORK}/${thing}" <<'PY2' || die "保存 ${thing} 的证书失败"
import json, sys, pathlib
d = pathlib.Path(sys.argv[1])
r = json.loads((d / "resp.json").read_text())
name = d.name
(d / (name + ".pem")).write_text(r["certificatePem"])
(d / (name + ".key")).write_text(r["privateKeyPem"])
PY2
done
# 等证书的登记随日志传到 b、c
sleep 1.5
python3 tests/cluster/fanout.py 127.0.0.1 \
	"$(port_for 0 0)" "$(port_for 1 0)" "${ADMIN_PORTS[2]}" \
	"$(cat "${WORK}/c/admin-token")" \
	"${WORK}/a/pki/ca-chain.pem" "${WORK}" || die "Admin 扇出失败"
pass "从 c 上看到了连在 a 与 b 上的两条连接"

say "跨节点的共享订阅组（MQTT-096、MQTT-116）"
# 订阅者只在 b 上，发布在 a 上。组成员通告传过去之后，a 必须把这一帧
# 转给 b，而不是当成"组里没有可投的成员"丢掉——那是此前的行为，
# 而发布者看不出任何异常：PUBACK 照常返回，因为 a 的本地订阅者
# 确实都投过了。
python3 tests/cluster/crossnode.py \
	"${ADMIN_PORTS[1]}" "$(cat "${WORK}/b/admin-token")" \
	"${ADMIN_PORTS[0]}" "$(cat "${WORK}/a/admin-token")" 2.5 \
	'$share/workers/telemetry/#' ||
	die "共享订阅组的跨节点投递失败"
pass "共享订阅组跨节点：订阅者在 b，发布在 a，消息投到了 b"

say "会话围栏：同一 clientId 连到另一个节点（CLU-081）"
python3 tests/cluster/fencing.py 127.0.0.1 \
	"$(port_for 0 0)" "$(port_for 1 0)" \
	"${WORK}/a/pki/ca-chain.pem" \
	"${WORK}/fence-dev/fence-dev.pem" "${WORK}/fence-dev/fence-dev.key" \
	fence-dev || die "会话围栏失败"
pass "a 上的旧连接被断开，b 上的新连接健康"

say "持久会话与离线队列跨节点（CLU-111 至 CLU-117）"
# 这是 v0.3 之前最大的一处"看起来是集群、其实不是"：会话与离线队列只落在
# 接入它的那个节点的盘上。判据分两段——在 a 上建持久会话并订阅、从 b 发一条
# QoS 1（经兴趣通告转给 a 入队），然后到**c** 上重连：c 从没见过这个设备，
# session present = 1 与那条补发的消息都只能来自复制。
python3 tests/cluster/session.py prime 127.0.0.1 \
	"$(port_for 0 0)" \
	"${WORK}/a/pki/ca-chain.pem" \
	"${WORK}/queue-dev/queue-dev.pem" "${WORK}/queue-dev/queue-dev.key" \
	queue-dev '$aws/things/queue-dev/#' \
	"${ADMIN_PORTS[1]}" "$(cat "${WORK}/b/admin-token")" 2.0 \
	"${WORK}/queued-payload" || die "会话跨节点的准备阶段失败"

# 入队要先经一次复制往返才会出现在 c 的盘上（CLU-116：窗口不是零）。
sleep 1.5

python3 tests/cluster/session.py verify 127.0.0.1 \
	"$(port_for 2 0)" \
	"${WORK}/a/pki/ca-chain.pem" \
	"${WORK}/queue-dev/queue-dev.pem" "${WORK}/queue-dev/queue-dev.key" \
	queue-dev "${WORK}/queued-payload" || die "会话或离线队列没有跨节点"
pass "会话与离线队列跨到了第三个节点（c 从没见过这个设备）"

say "成员变更：起第四个节点并加入（CLU-090 至 CLU-094）"
# 它的配置里只有自己：地址随成员变更复制过去，不必出现在别人的配置里。
d_mqtt=$(port_for 3 0)
d_admin=$(port_for 3 1)
d_repl=$(port_for 3 2)
openssl req -newkey rsa:2048 -nodes \
	-keyout "${PKI}/d.key" -out "${PKI}/d.csr" -subj "/CN=d" >/dev/null 2>&1 ||
	die "生成 d 的 CSR 失败"
cat > "${PKI}/d.ext" <<EXT
subjectAltName = DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth, clientAuth
EXT
openssl x509 -req -in "${PKI}/d.csr" -days 1 \
	-CA "${PKI}/ca.pem" -CAkey "${PKI}/ca.key" -CAcreateserial \
	-extfile "${PKI}/d.ext" -out "${PKI}/d.pem" >/dev/null 2>&1 ||
	die "签发 d 的证书失败"

"${BIN}" init --data-dir "${WORK}/d" --node-id d --log-level error >/dev/null ||
	die "初始化 d 失败"
# 让 api() 能查到 d 的令牌与端口
NODES[3]=d
ADMIN_PORTS[3]=${d_admin}
rm -rf "${WORK}/d/pki"
cp -R "${WORK}/a/pki" "${WORK}/d/pki"
cat > "${WORK}/d.yaml" <<YAML
node:
  id: d
  mode: join
  data_dir: ${WORK}/d
  seeds: ["127.0.0.1:$(port_for 0 2)"]
cluster:
  voters: 1
  max_view_staleness: 10s
  replication:
    addr: "127.0.0.1:${d_repl}"
    cert_file: ${PKI}/d.pem
    key_file: ${PKI}/d.key
    ca_file: ${PKI}/ca.pem
    pump_interval: 100ms
    peers:
      d: "127.0.0.1:${d_repl}"
    replicas:
      d: 4
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${d_mqtt}"
  admin:
    enabled: true
    addr: "127.0.0.1:${d_admin}"
log:
  level: warn
  format: text
YAML
"${BIN}" serve --config "${WORK}/d.yaml" >"${WORK}/d.log" 2>&1 &
PIDS[3]=$!
sleep 2

# 从 leader（a）把 d 加进来
api 0 -X POST -H 'Content-Type: application/json' \
	-d "{\"id\":\"d\",\"addr\":\"127.0.0.1:${d_repl}\",\"replicaId\":4}" \
	"http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/cluster/members" >/dev/null ||
	die "加入成员 d 失败"
# 成员变更要等它被提交并复制到新节点，**轮询而不是睡一个固定值**。
#
# 实测新节点从启动到应用那几条 ADD 大约十几秒：它要先被 leader 发现、
# 再补上整段日志。固定 sleep 写短了是偶发失败，写长了是每次都慢。
got=""
for _ in $(seq 1 60); do
	got=$(api 0 "http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/cluster/members" |
		python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["members"]), d["quorum"])')
	[ "${got}" = "4 3" ] && break
	sleep 1
done
[ "${got}" = "4 3" ] || die "a 上成员数与多数派 = ${got}，期望 4 3"
# d 自己也要看到四个成员（它的配置里只有自己，这份是复制来的）
got=""
for _ in $(seq 1 60); do
	got=$(api 3 "http://127.0.0.1:${d_admin}/api/admin/v1/cluster/members" |
		python3 -c 'import json,sys; print(len(json.load(sys.stdin)["members"]))' 2>/dev/null || echo 0)
	[ "${got}" = 4 ] && break
	sleep 1
done
[ "${got}" = 4 ] || die "d 上成员数 = ${got}，期望 4（成员随日志复制）"
pass "d 加入后四个节点的成员一致，多数派从 2 变成 3"

# d 是新成员，它要能读到之前写的全部数据（靠日志或快照追平）
got=$(api 3 "http://127.0.0.1:${d_admin}/api/admin/v1/things/lamp-1")
echo "${got}" | grep -q '"site"' || die "d 读不到既有设备（没追平）：${got}"
echo "${got}" | grep -q 'cluster-p' || die "d 上设备的策略绑定不对"
pass "新成员 d 追平了既有数据（设备、策略都在）"

# 把 d 移出去，多数派回到 2
api 0 -X DELETE "http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/cluster/members/d" >/dev/null ||
	die "移除成员 d 失败"
sleep 1
got=$(api 0 "http://127.0.0.1:${ADMIN_PORTS[0]}/api/admin/v1/cluster/members" |
	python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["members"]), d["quorum"])')
[ "${got}" = "3 2" ] || die "移除后成员数与多数派 = ${got}，期望 3 2"
pass "移出 d 之后多数派回到 2"

kill "${PIDS[3]}" 2>/dev/null || true
wait "${PIDS[3]}" 2>/dev/null || true
unset 'PIDS[3]'

# ---------------------------------------------------------------------------
# leader 故障转移（CLU-096）。
#
# 这一条是换共识库的首要理由，也是本脚本此前的盲区：原先的自写实现没有
# 选举超时，只有 bootstrap 节点在启动路径上竞选一次，跟随者永远不会超时
# 竞选。于是 leader 进程一挂，剩下的多数派选不出新 leader，写入永久 503
# ——而这个脚本从头到尾只杀过跟随者，所以那个缺陷从来没有红过。
# ---------------------------------------------------------------------------
say "杀掉 leader，剩下的多数派必须选出新 leader 并继续接受写入"

leader_of() { # $1=节点下标；打印该节点认为的 leader
	api "$1" "http://127.0.0.1:${ADMIN_PORTS[$1]}/api/admin/v1/cluster/members" |
		python3 -c 'import json,sys; print(json.load(sys.stdin).get("leader",""))'
}

index_of() { # $1=节点 ID
	for i in "${!NODES[@]}"; do
		[ "${NODES[$i]}" = "$1" ] && echo "$i" && return 0
	done
	return 1
}

old_leader=$(leader_of 0)
[ -n "${old_leader}" ] || die "成员接口没有给出当前 leader"
li=$(index_of "${old_leader}") || die "leader ${old_leader} 不在节点表里"
say "当前 leader 是 ${old_leader}（下标 ${li}）"

kill "${PIDS[$li]}" 2>/dev/null || true
wait "${PIDS[$li]}" 2>/dev/null || true
unset "PIDS[$li]"

# 找一个还活着的节点来观察与写入
alive=()
for i in "${!NODES[@]}"; do
	[ "$i" = "$li" ] && continue
	[ -n "${PIDS[$i]+x}" ] && alive+=("$i")
done
[ "${#alive[@]}" -ge 2 ] || die "杀掉 leader 之后活着的节点不足两个"
probe=${alive[0]}

# 等新 leader 选出来。判据是「活着的节点报出一个不是旧 leader 的 ID」。
new_leader=""
for _ in $(seq 1 100); do
	got=$(leader_of "${probe}" 2>/dev/null || true)
	if [ -n "${got}" ] && [ "${got}" != "${old_leader}" ]; then
		new_leader="${got}"
		break
	fi
	sleep 0.2
done
[ -n "${new_leader}" ] ||
	die "杀掉 leader 之后 20 秒内没有选出新 leader（自写实现正是卡在这里）"
pass "新 leader 是 ${new_leader}（旧的是 ${old_leader}）"

# 写入要能成功，且可以从任意一个活着的节点发起（非 leader 会转发）。
ok=""
for _ in $(seq 1 50); do
	if api "${probe}" -X PUT -H 'Content-Type: application/json' \
		-d '{"policyName":"cluster-p"}' \
		"http://127.0.0.1:${ADMIN_PORTS[$probe]}/api/admin/v1/things/lamp-failover" >/dev/null 2>&1; then
		ok=1
		break
	fi
	sleep 0.2
done
[ -n "${ok}" ] || die "选出新 leader 之后写入仍然失败"
pass "故障转移之后写入照常（从 ${NODES[$probe]} 发起，必要时转发给新 leader）"

# 两个活着的节点都要读得到它：复制没停。
for i in "${alive[@]}"; do
	for _ in $(seq 1 50); do
		api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/things/lamp-failover" >/dev/null 2>&1 && break
		sleep 0.2
	done
	api "$i" "http://127.0.0.1:${ADMIN_PORTS[$i]}/api/admin/v1/things/lamp-failover" >/dev/null ||
		die "${NODES[$i]} 读不到故障转移之后写入的设备"
done
pass "故障转移之后的写入复制到了全部存活节点"

# 保留消息也要活过 leader 之死（TST-156）。
#
# 挑一个**不是发布者**的存活节点来读：a（下标 0）是发的那台，如果它恰好
# 就是被杀的 leader，这条检查更强——那条 retained 只可能是先复制过去的。
for i in "${alive[@]}"; do
	[ "$i" = 0 ] && continue
	code=$(retained_code "$i" "${RETAINED_KEEP}") ||
		die "在 ${NODES[$i]} 上读 retained 执行失败"
	[ "${code}" = 200 ] ||
		die "杀掉 leader 之后 ${NODES[$i]} 读不到 retained（= ${code}）：它是转发来的，不是复制过去的"
	pass "杀掉 leader 之后 ${NODES[$i]} 上仍有那条 retained"
	break
done

say "再杀一个，失去多数派，写入应当失败而读取照常"
victim=${alive[1]}
kill "${PIDS[$victim]}" 2>/dev/null || true
wait "${PIDS[$victim]}" 2>/dev/null || true
unset "PIDS[$victim]"
sleep 2
code=$(curl -sS -o /dev/null -w '%{http_code}' \
	-H "Authorization: Bearer $(cat "${WORK}/${NODES[$probe]}/admin-token")" \
	-X PUT -H 'Content-Type: application/json' \
	-d '{"policyName":"cluster-p"}' \
	"http://127.0.0.1:${ADMIN_PORTS[$probe]}/api/admin/v1/things/lamp-4" || true)
# 必须是 503：请求完全合法，是这个节点此刻提交不了日志。
# 400 会让调用方以为请求写错了去改请求，而它该做的是重试或换个节点
# （API-089、CLU-073）。
[ "${code}" = 503 ] ||
	die "失去多数派之后写入返回 HTTP ${code}，期望 503"
pass "失去多数派后写入被拒（HTTP 503，不是 400）"

api "${probe}" "http://127.0.0.1:${ADMIN_PORTS[$probe]}/api/admin/v1/things/lamp-1" | grep -q '"site"' ||
	die "失去多数派之后读取也失败了（控制面读的是本地冷层，不该受影响）"
pass "失去多数派后读取照常（CLU-042 的取舍：只有写入失败）"

say "日志确实落盘了"
# 日志落在 data dir 的 raft/ 下，与冷层分开（DirRaft 的理由：冷层可以从
# 日志重建，反过来不行，所以"备份冷层"与"备份日志"不该是同一次操作）。
#
# 只验目录非空与内容，不钉具体文件名：日志的内部布局归共识库管
# （换到 dragonboat 之后是它自带的 LogDB），钉文件名等于把库的实现细节
# 写进验收，下一次升级就会报假红。
logdir="${WORK}/${NODES[$probe]}/raft"
[ -d "${logdir}" ] && [ -n "$(ls -A "${logdir}" 2>/dev/null)" ] ||
	die "没有在 data dir 下找到复制日志（${logdir}）"
# 条目正文是我们自己的命令编码，设备名应当在日志文件里以明文出现。
grep -raq 'lamp-1' "${logdir}" || die "复制日志里没有设备命令"
pass "复制日志在 ${logdir}，含已提交的命令"

echo
printf '\033[1;32m%s\033[0m\n' "集群验收全部通过"
