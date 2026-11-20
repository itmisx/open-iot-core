#!/usr/bin/env bash
#
# 起一个临时节点，用真实设备 SDK 对跑，跑完拆掉。
#
# 临时目录而不是复用 .demo/：这套测试要吊销证书、掐连接、发遗嘱，
# 在别人的演示环境里做这些事会留下一地状态。端口也与 demo 错开，
# 两边可以同时跑。
set -euo pipefail

cd "$(dirname "$0")/../.."

MQTT_PORT=${MQTT_PORT:-28883}
ALPN_PORT=${ALPN_PORT:-28443}
ADMIN_PORT=${ADMIN_PORT:-29090}
WORK=${WORK:-$(mktemp -d -t openiot-sdk-test)}
VENV="${WORK}/venv"
BIN="${WORK}/openiotd"
DATA="${WORK}/data"


say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
	[ -n "${PID:-}" ] && kill "${PID}" 2>/dev/null || true
	# 保留目录供排查的开关：失败时现场往往比结论有用
	if [ "${KEEP_WORK:-0}" = 1 ]; then
		echo "工作目录保留在 ${WORK}"
	else
		rm -rf "${WORK}"
	fi
}
trap cleanup EXIT INT TERM

command -v python3 >/dev/null 2>&1 || die "需要 python3"

# 端口先检查再动手：跑到一半才发现端口被占，失败信息会出现在一个
# 与真因无关的步骤上（demo.sh 踩过同一个坑）。
for port in "${MQTT_PORT}" "${ALPN_PORT}" "${ADMIN_PORT}"; do
	if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
		die "端口 ${port} 已被占用。换一组：MQTT_PORT=38883 ALPN_PORT=38443 ADMIN_PORT=39090 $0"
	fi
done

say "准备 Python 环境"
python3 -m venv "${VENV}"
"${VENV}/bin/pip" install -q -r tests/device-sdk/requirements.txt

say "构建 openiotd"
go build -o "${BIN}" ./cmd/openiotd

say "初始化节点"
"${BIN}" init --data-dir "${DATA}" --log-level error >/dev/null

# 两份策略：一份全开用于功能验证，一份按设备名收敛用于验证拒绝路径。
# 只有全开的那份会让"越权被拒"这类用例失去意义。
cat > "${DATA}/policies/conformance-open.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive", "iot:RetainPublish"],
      "Resource": "*" }
  ]
}
JSON
cat > "${DATA}/policies/conformance-scoped.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}" },
    { "Effect": "Allow", "Action": ["iot:Publish", "iot:Receive"],
      "Resource": ["arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*",
                   "arn:aws:iot:*:*:topic/devices/${iot:Connection.Thing.ThingName}/*"] },
    { "Effect": "Allow", "Action": "iot:Subscribe",
      "Resource": ["arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*",
                   "arn:aws:iot:*:*:topicfilter/devices/${iot:Connection.Thing.ThingName}/*"] }
  ]
}
JSON

say "签发设备证书"
"${BIN}" cert issue --data-dir "${DATA}" --thing dev-open \
	--policy conformance-open --out "${WORK}/dev-open" >/dev/null
"${BIN}" cert issue --data-dir "${DATA}" --thing dev-scoped \
	--policy conformance-scoped --out "${WORK}/dev-scoped" >/dev/null

cat > "${WORK}/node.yaml" <<YAML
node:
  data_dir: ${DATA}
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${MQTT_PORT}"
  mqtt_alpn:
    enabled: true
    addr: "127.0.0.1:${ALPN_PORT}"
  admin:
    enabled: true
    addr: "127.0.0.1:${ADMIN_PORT}"
log:
  level: warn
  format: text
YAML

say "启动节点"
"${BIN}" serve --config "${WORK}/node.yaml" > "${WORK}/serve.log" 2>&1 &
PID=$!

ready=no
for _ in $(seq 1 150); do
	if ! kill -0 "${PID}" 2>/dev/null; then
		cat "${WORK}/serve.log" >&2
		die "节点启动失败"
	fi
	if curl -sf -o /dev/null "http://127.0.0.1:${ADMIN_PORT}/readyz" 2>/dev/null; then
		ready=yes
		break
	fi
	sleep 0.1
done
[ "${ready}" = yes ] || die "节点未在 15 秒内就绪"

# 设备记录经 Admin API 建：文件化注册表只在首次启动导入一次，
# 节点已经起来之后改文件是不生效的（internal/server/import.go）。
TOKEN=$(cat "${DATA}/admin-token")
for name in dev-open dev-scoped; do
	policy=conformance-open
	[ "${name}" = dev-scoped ] && policy=conformance-scoped
	curl -sf -H "Authorization: Bearer ${TOKEN}" -X PUT \
		-H 'Content-Type: application/json' \
		-d "{\"policyName\":\"${policy}\"}" \
		"http://127.0.0.1:${ADMIN_PORT}/api/admin/v1/things/${name}" >/dev/null
done

say "对跑"
set +e
OPENIOT_WORK="${WORK}" \
OPENIOT_MQTT_PORT="${MQTT_PORT}" \
OPENIOT_ALPN_PORT="${ALPN_PORT}" \
OPENIOT_ADMIN_PORT="${ADMIN_PORT}" \
OPENIOT_TOKEN="${TOKEN}" \
WITH_KEEPALIVE="${WITH_KEEPALIVE:-0}" \
	"${VENV}/bin/python" tests/device-sdk/conformance.py
rc=$?
set -e

if [ ${rc} -ne 0 ]; then
	echo
	say "节点日志尾部（授权拒绝之类的线索在这里）"
	tail -20 "${WORK}/serve.log"
fi
exit ${rc}
