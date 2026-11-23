#!/usr/bin/env bash
#
# boto3 与 aws CLI 对跑本节点的控制面，再用签出的证书连一次、吊销后再连。
#
# 不进 make gate：门禁只依赖 Go 工具链。这套要装 Python 和 boto3。
set -euo pipefail

cd "$(dirname "$0")/../.."

MQTT_PORT=${MQTT_PORT:-39883}
ADMIN_PORT=${ADMIN_PORT:-39091}
WORK=${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/openiot-aws-sdk.XXXXXX")} || exit 1
VENV="${WORK}/venv"
BIN="${WORK}/openiotd"
DATA="${WORK}/data"

# 钥匙在节点起来之后才发得出来（AUTH-086），所以这两个变量在下面补上。
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT="http://127.0.0.1:${ADMIN_PORT}/api/aws"
export MQTT_PORT
export CA_FILE="${DATA}/pki/ca-chain.pem"
export WORK
export THING=lamp-boto
export POLICY=boto-open

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

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

command -v python3 >/dev/null 2>&1 || die "需要 python3"

for port in "${MQTT_PORT}" "${ADMIN_PORT}"; do
	if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
		die "端口 ${port} 已被占用"
	fi
done

say "准备 Python 环境"
python3 -m venv "${VENV}"
"${VENV}/bin/pip" install -q --upgrade pip
"${VENV}/bin/pip" install -q -r tests/aws-sdk/requirements.txt

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

# access key 现在由节点发放，经管理面（AUTH-086、API-125）。
# 引导令牌是 init 写进数据目录的那一个。
say "发两把 access key"
ADMIN_TOKEN=$(cat "${DATA}/admin-token")
issue_key() {
	curl -sf -X POST "http://127.0.0.1:${ADMIN_PORT}/api/admin/v1/awskeys" \
		-H "Authorization: Bearer ${ADMIN_TOKEN}" \
		-H 'Content-Type: application/json' \
		-d "{\"role\":\"$1\",\"note\":\"aws-sdk test\"}"
}
# 只用 python 取两个字段，不引 jq：这套测试已经有 venv，再多一个
# 系统依赖就会在别人的机器上成为"跑不起来"的理由。
read_field() { "${VENV}/bin/python" -c 'import json,sys;print(json.load(sys.stdin)["'"$1"'"])'; }

ADMIN_KEY_JSON=$(issue_key admin) || die "发 admin 钥匙失败"
AWS_ACCESS_KEY_ID=$(printf '%s' "${ADMIN_KEY_JSON}" | read_field id)
AWS_SECRET_ACCESS_KEY=$(printf '%s' "${ADMIN_KEY_JSON}" | read_field secret)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
case "${AWS_ACCESS_KEY_ID}" in
	AKIA*) ;;
	*) die "access key id 形状不对：${AWS_ACCESS_KEY_ID}" ;;
esac

READER_KEY_JSON=$(issue_key reader) || die "发 reader 钥匙失败"
READER_ID=$(printf '%s' "${READER_KEY_JSON}" | read_field id)
READER_SECRET=$(printf '%s' "${READER_KEY_JSON}" | read_field secret)

say "boto3 注册、签发、附策略、连接、吊销"
"${VENV}/bin/python" tests/aws-sdk/flow.py

say "aws CLI 读回同一台设备"
"${VENV}/bin/aws" iot describe-thing --thing-name "${THING}" --endpoint-url "${AWS_ENDPOINT}" --region us-east-1 --output json | grep -q '"thingName": "lamp-boto"'

# 这一段用真的 SDK 验权限级别。Go 侧的用例能证明服务端返回 403，
# 但证明不了 botocore 把它解成 AccessDeniedException——异常名对不上，
# 调用方的 except 分支就接不住。
say "reader 的钥匙能读不能写"
aws_reader() {
	env AWS_ACCESS_KEY_ID="${READER_ID}" AWS_SECRET_ACCESS_KEY="${READER_SECRET}" \
		"${VENV}/bin/aws" "$@" --endpoint-url "${AWS_ENDPOINT}" --region us-east-1 --output json
}
aws_reader iot describe-thing --thing-name "${THING}" | grep -q '"thingName": "lamp-boto"' \
	|| die "reader 读不到设备"
if out=$(aws_reader iot create-thing --thing-name reader-should-not-create 2>&1); then
	die "reader 居然建出了设备：${out}"
fi
printf '%s' "${out}" | grep -q 'AccessDeniedException' \
	|| die "reader 被拒了，但异常名不是 AccessDeniedException：${out}"
# 拒绝之后不能留下痕迹。
if "${VENV}/bin/aws" iot describe-thing --thing-name reader-should-not-create \
	--endpoint-url "${AWS_ENDPOINT}" --region us-east-1 >/dev/null 2>&1; then
	die "被拒的请求不该建出设备"
fi

# 停用要立刻生效：这是"钥匙不再是一份改完要重启的文件"的全部意义。
say "停用之后立刻失效"
curl -sf -X PATCH "http://127.0.0.1:${ADMIN_PORT}/api/admin/v1/awskeys/${READER_ID}" \
	-H "Authorization: Bearer ${ADMIN_TOKEN}" -H 'Content-Type: application/json' \
	-d '{"disabled":true}' >/dev/null || die "停用失败"
if out=$(aws_reader iot describe-thing --thing-name "${THING}" 2>&1); then
	die "停用之后还能读：${out}"
fi
# 停用的钥匙与不存在的钥匙必须是同一种拒绝，而不是 403。
printf '%s' "${out}" | grep -qi 'UnauthorizedException\|403\|401' \
	|| die "停用之后的错误不对：${out}"

say "通过"
