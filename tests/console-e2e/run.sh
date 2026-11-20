#!/usr/bin/env bash
#
# 在真实浏览器里把控制台点一遍。
#
# 为什么要有这套：TST-126 的 TestConsoleCriticalFlows 走的是控制台调用的
# 那几个管理面接口，它证明不了"页面能渲染出来"。窗口渲染（UI-059）、
# 在线率曲线（UI-060）、SSE 的逐类型 addEventListener、测试客户端的
# WebSocket，这些都只有在浏览器里才会失败。
#
# 不进 make gate：门禁必须只依赖 Go 工具链，而这套要 Node 与一个
# 一百多兆的 Chromium（与 tests/device-sdk 同一条理由）。定位是发版前、
# 以及改过 web/console/src 之后手动跑。
set -euo pipefail

cd "$(dirname "$0")/../.."

MQTT_PORT=${MQTT_PORT:-39283}
ADMIN_PORT=${ADMIN_PORT:-39290}
WORK=${WORK:-$(mktemp -d -t openiot-console-e2e)}
BIN="${WORK}/openiotd"
DATA="${WORK}/data"
# 截图默认跟着临时目录一起被删。想留下来看就传 SHOTS=某个目录——
# 截图的用处正是给人看，而默认那份看不到。
SHOTS=${SHOTS:-"${WORK}/screenshots"}


say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
	[ -n "${PID:-}" ] && kill "${PID}" 2>/dev/null || true
	wait "${PID:-}" 2>/dev/null || true
	if [ "${KEEP_WORK:-0}" = 1 ]; then
		echo "工作目录保留在 ${WORK}（截图在 screenshots/，节点日志在 node.log）"
	else
		rm -rf "${WORK}"
	fi
}
trap cleanup EXIT INT TERM

command -v node >/dev/null 2>&1 || die "需要 Node（这套测试驱动真实浏览器）"

# 端口先检查再动手：跑到一半才发现端口被占，失败信息会落在一个
# 与真因无关的步骤上（demo.sh 与 device-sdk 都踩过）。
for port in "${MQTT_PORT}" "${ADMIN_PORT}"; do
	if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
		die "端口 ${port} 已被占用。换一组：MQTT_PORT=49283 ADMIN_PORT=49290 $0"
	fi
done

say "准备浏览器"
if [ ! -d tests/console-e2e/node_modules ]; then
	(cd tests/console-e2e && npm install --silent)
fi
(cd tests/console-e2e && npx playwright install chromium >/dev/null)

say "构建 openiotd（控制台产物已嵌入二进制）"
go build -o "${BIN}" ./cmd/openiotd

say "初始化节点"
"${BIN}" init --data-dir "${DATA}" --log-level error >/dev/null
TOKEN=$(cat "${DATA}/admin-token")

# 策略先落成文件再签证书：cert issue 是离线命令，读的是 data dir 下的
# policies/*.json。只经 Admin API 写进去的话，签发那一步会说策略不存在。
cat > "${DATA}/policies/e2e-scoped.json" <<'JSON'
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
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}
JSON

say "签发设备证书"
for name in lamp-001 lamp-002; do
	"${BIN}" cert issue --data-dir "${DATA}" \
		--thing "${name}" --policy e2e-scoped \
		--out "${WORK}/devices/${name}" >/dev/null || die "签发 ${name} 失败"
done

cat > "${WORK}/openiotd.yaml" <<YAML
node:
  data_dir: ${DATA}
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${MQTT_PORT}"
  admin:
    enabled: true
    addr: "127.0.0.1:${ADMIN_PORT}"
# 访问控制页的下半截是兼容面的 access key（UI-075），要启用 aws_api
# 才看得到有意义的内容。
aws_api:
  enabled: true
  region: us-east-1
  endpoint_address: 127.0.0.1
log:
  level: warn
  format: text
YAML

say "启动节点"
"${BIN}" serve --config "${WORK}/openiotd.yaml" >"${WORK}/node.log" 2>&1 &
PID=$!
ready=no
for _ in $(seq 1 150); do
	if ! kill -0 ${PID} 2>/dev/null; then
		cat "${WORK}/node.log" >&2
		die "节点启动失败"
	fi
	if curl -sf -o /dev/null "http://127.0.0.1:${ADMIN_PORT}/readyz"; then
		ready=yes
		break
	fi
	sleep 0.1
done
[ "${ready}" = yes ] || die "节点未在 15 秒内就绪"

API="http://127.0.0.1:${ADMIN_PORT}/api/admin/v1"
auth=(-H "Authorization: Bearer ${TOKEN}")

say "预置数据"
# 60 台设备：服务端一页 50 条，窗口渲染要有足够的行才看得出它在工作。
for i in $(seq -f "%03g" 1 60); do
	curl -sf "${auth[@]}" -X PUT -H 'Content-Type: application/json' \
		-d "{\"policyName\":\"e2e-scoped\",\"attributes\":{\"site\":\"sh\"}}" \
		"${API}/things/lamp-${i}" >/dev/null || die "建设备 lamp-${i} 失败"
done

# 一份影子：设备详情页的影子区块要有内容
curl -sf "${auth[@]}" -X PUT -H 'Content-Type: application/json' \
	-d '{"state":{"desired":{"interval":30}}}' \
	"${API}/things/lamp-001/shadow" >/dev/null || die "写影子失败"

# 再来一个**命名**影子（SHDW-002）。控制台此前完全看不到这一类：
# 聚合页只读经典影子，listShadows 在前端是死代码。
curl -sf "${auth[@]}" -X PUT -H 'Content-Type: application/json' \
	-d '{"state":{"desired":{"channel":"stable"}}}' \
	"${API}/things/lamp-001/shadow?name=firmware" >/dev/null || die "写命名影子失败"

mkdir -p "${SHOTS}"
say "在浏览器里点一遍"
BASE="http://127.0.0.1:${ADMIN_PORT}" \
TOKEN="${TOKEN}" \
SHOTS="${SHOTS}" \
	node tests/console-e2e/console.mjs
