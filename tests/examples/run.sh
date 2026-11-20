#!/usr/bin/env bash
#
# 教程里的三份示例代码，对一个临时节点真的跑一遍（TST-144）。
#
# 为什么要有这套：教程「SDK 接入」三页上的每段代码都来自 examples/sdk/ 下的
# 真实文件（UI-080），而「真实」只有跑过才算。Go 侧的测试验证不了这件事——
# 三种 SDK 的 endpoint 拼接、签名与错误映射各是一套实现，一边对不证明另一
# 边对（Go 的 BaseEndpoint 与 botocore 的 endpoint_url 就是两套代码路径）。
#
# 不进 make gate：要 JDK、Maven、Python 与网络（各语言的 SDK 要现拉），
# 而门禁必须只依赖 Go 工具链。与 tests/aws-sdk、tests/device-sdk 同一条理由。
#
# 缺哪个语言的工具链就跳过那一份并明说，不悄悄算通过——"跳过了"和"通过了"
# 在发版前是两回事。
set -euo pipefail

cd "$(dirname "$0")/../.."

MQTT_PORT=${MQTT_PORT:-39483}
ADMIN_PORT=${ADMIN_PORT:-39490}
WORK=${WORK:-$(mktemp -d -t openiot-examples)}
BIN="${WORK}/openiotd"
DATA="${WORK}/data"

export AWS_DEFAULT_REGION=us-east-1
export OIOT_ENDPOINT="http://127.0.0.1:${ADMIN_PORT}/api/aws"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }
skip() { printf '\033[1;33mSKIP\033[0m  %s\n' "$*"; }
pass() { printf '\033[1;32mPASS\033[0m  %s\n' "$*"; }

FAILED=0

cleanup() {
	[ -n "${PID:-}" ] && kill "${PID}" 2>/dev/null || true
	wait "${PID:-}" 2>/dev/null || true
	if [ "${KEEP_WORK:-0}" = 1 ]; then
		echo "工作目录保留在 ${WORK}"
	else
		rm -rf "${WORK}"
	fi
}
trap cleanup EXIT INT TERM

for port in "${MQTT_PORT}" "${ADMIN_PORT}"; do
	if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
		die "端口 ${port} 已被占用。换一组：MQTT_PORT=49483 ADMIN_PORT=49490 $0"
	fi
done

say "构建并启动节点"
go build -o "${BIN}" ./cmd/openiotd
"${BIN}" init --data-dir "${DATA}" --log-level error >/dev/null

cat > "${WORK}/node.yaml" <<YAML
node:
  data_dir: ${DATA}
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${MQTT_PORT}"
  mqtt_alpn:
    enabled: false
  admin:
    enabled: true
    addr: "127.0.0.1:${ADMIN_PORT}"
aws_api:
  enabled: true
  region: us-east-1
  endpoint_address: 127.0.0.1
log:
  level: warn
  format: text
YAML

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
[ "${ready}" = yes ] || die "节点没有就绪"

say "发一把 operator 钥匙"
# 示例做的是日常写（建设备、签证书、发布、写影子），operator 够用。
# 刻意不用 admin：教程里写的级别如果其实不够，这套就该红。
KEY_JSON=$(curl -sf -X POST "http://127.0.0.1:${ADMIN_PORT}/api/admin/v1/awskeys" \
	-H "Authorization: Bearer $(cat "${DATA}/admin-token")" \
	-H 'Content-Type: application/json' \
	-d '{"role":"operator","note":"examples test"}') || die "发钥匙失败"
AWS_ACCESS_KEY_ID=$(printf '%s' "${KEY_JSON}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
AWS_SECRET_ACCESS_KEY=$(printf '%s' "${KEY_JSON}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret"])')
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

# 每份示例断言同样四句输出：设备与策略、证书、发布、影子。
# 三种语言的输出必须一致到这个程度，否则「同一份教程三种写法」只是口号。
check_output() {
	local name="$1" out="$2"
	for want in "设备与策略就位" "证书已签发" "已发布到" "影子文档"; do
		if ! grep -q "${want}" "${out}"; then
			printf '\033[1;31mFAIL\033[0m  %s：输出里没有「%s」\n' "${name}" "${want}"
			sed -e 's/^/        /' "${out}" >&2
			FAILED=1
			return
		fi
	done
	# 影子文档里要有刚写进去的 desired，否则"跑完了"可能只是没报错。
	if ! grep -q '"interval":30\|"interval": 30' "${out}"; then
		printf '\033[1;31mFAIL\033[0m  %s：影子里没有写进去的 desired\n' "${name}"
		sed -e 's/^/        /' "${out}" >&2
		FAILED=1
		return
	fi
	pass "${name}"
}

say "Go"
(
	cd examples/sdk/golang
	OIOT_THING=lamp-go OIOT_POLICY=lamp-go-scoped OIOT_OUT="${WORK}/go-out" \
		go run . > "${WORK}/go.out" 2>&1
) && check_output "Go" "${WORK}/go.out" || {
	printf '\033[1;31mFAIL\033[0m  Go 示例退出码非零\n'
	sed -e 's/^/        /' "${WORK}/go.out" >&2
	FAILED=1
}

say "Python"
if command -v python3 >/dev/null 2>&1; then
	VENV="${WORK}/venv"
	python3 -m venv "${VENV}"
	"${VENV}/bin/pip" install -q --upgrade pip
	if "${VENV}/bin/pip" install -q boto3; then
		(
			cd examples/sdk/python
			OIOT_THING=lamp-py OIOT_POLICY=lamp-py-scoped OIOT_OUT="${WORK}/py-out" \
				"${VENV}/bin/python" main.py > "${WORK}/py.out" 2>&1
		) && check_output "Python" "${WORK}/py.out" || {
			printf '\033[1;31mFAIL\033[0m  Python 示例退出码非零\n'
			sed -e 's/^/        /' "${WORK}/py.out" >&2
			FAILED=1
		}
	else
		skip "Python：装不上 boto3（要联网）"
	fi
else
	skip "Python：没有 python3"
fi

say "Java"
if command -v mvn >/dev/null 2>&1; then
	(
		cd examples/sdk/java
		OIOT_THING=lamp-java OIOT_POLICY=lamp-java-scoped OIOT_OUT="${WORK}/java-out" \
			mvn -q -B compile exec:java > "${WORK}/java.out" 2>&1
	) && check_output "Java" "${WORK}/java.out" || {
		printf '\033[1;31mFAIL\033[0m  Java 示例退出码非零\n'
		sed -e 's/^/        /' "${WORK}/java.out" >&2
		FAILED=1
	}
else
	skip "Java：没有 mvn（brew install maven，或 apt install maven）"
fi

echo ""
if [ "${FAILED}" = 0 ]; then
	printf '\033[1;32m示例验收通过\033[0m（跳过的那几份见上面的 SKIP）\n'
else
	die "有示例没跑通"
fi
