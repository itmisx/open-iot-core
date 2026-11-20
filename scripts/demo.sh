#!/usr/bin/env bash
#
# 一键启动演示环境。
#
# 它做的事：建数据目录与 CA、签几张设备证书、建几份策略与设备、
# 启动节点，并打印控制台地址与管理面令牌。
#
# 全部产物都在 DEMO_DIR（默认 .demo/）下，可随时删掉重来。它在 .gitignore 里。
set -euo pipefail

cd "$(dirname "$0")/.."

# 一个旋钮管住全部产物位置。
#
# 此前 DATA_DIR 与 BIN 各自可覆盖，而 devices/、openiotd.yaml 与
# `mkdir -p .demo` 把 .demo 写死了——于是"换一份数据再跑一个"的做法
# （DATA_DIR=.demo2/data BIN=.demo2/openiotd）会把证书和配置仍然写进
# .demo/，踩到正在运行的那一份。要并存就只改 DEMO_DIR。
DEMO_DIR=${DEMO_DIR:-.demo}
DATA_DIR=${DATA_DIR:-${DEMO_DIR}/data}
BIN=${BIN:-${DEMO_DIR}/openiotd}
DEVICES_DIR=${DEMO_DIR}/devices
CONFIG=${DEMO_DIR}/openiotd.yaml

MQTT_PORT=${MQTT_PORT:-8883}
ADMIN_PORT=${ADMIN_PORT:-9090}

# 节点就绪的等待上限（秒）。迭代次数由它算出来，不再是硬编码的 150 次
# 配一句写死的"15 秒"——那两个数要靠人同步，改一个忘改另一个，
# 报错就会说一个假的时限。
READY_TIMEOUT=${READY_TIMEOUT:-15}

DEMO_NAMES=(sensor-01 sensor-02 gateway-01)


say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m%s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

# 两条"环境已被占用"的预检都放在动手之前。
#
# 理由是一样的：跑到一半才发现被占，此时已经建了数据目录、签了证书，
# 而失败信息会出现在一个与真因无关的步骤上（有一次是"登记证书失败"）。
port_busy() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else
    # 没有 lsof 时退化为尝试连接
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && exec 3>&-
  fi
}

# 数据目录的 bbolt 锁。锁被占时 serve 会在 5 秒后超时退出，失败出现在
# "启动节点"这一步——与端口冲突的表现完全一样。此前这里没有预检，而
# 失败提示固定说"最常见的原因是端口被占用，换个端口重试"，于是人照着
# 换端口，再撞同一把锁：锁按数据目录算，与端口无关。
dir_locked() {
  local db="$1/meta/store.db"
  [ -f "${db}" ] || return 1
  if command -v lsof >/dev/null 2>&1; then
    lsof -t "${db}" >/dev/null 2>&1
  else
    # 没有 lsof 时退化为按进程名猜。不如 lsof 准（同一个 BIN 跑在
    # 不同数据目录上会误判），但比不检查好。
    pgrep -f "${BIN} serve" >/dev/null 2>&1
  fi
}

# 两处预检给的处置建议保持一致，而且都用 ${DEMO_DIR} 而非写死的 .demo：
# 覆盖了 DEMO_DIR 的人照着一句写死 .demo 的建议去做，停掉的是别的东西。
busy_hint="停掉它：
  DEMO_DIR=${DEMO_DIR} make demo-clean
或与它并存，用另一份产物：
  DEMO_DIR=${DEMO_DIR}2 MQTT_PORT=18883 ADMIN_PORT=19090 make demo"

for port in "${MQTT_PORT}" "${ADMIN_PORT}"; do
  if port_busy "${port}"; then
    die "端口 ${port} 已被占用。
${busy_hint}"
  fi
done

if dir_locked "${DATA_DIR}"; then
  die "数据目录 ${DATA_DIR} 已被另一个 openiotd 进程占用。
换端口没有用——文件锁按数据目录算，不按端口。
${busy_hint}"
fi

# 不判"二进制是不是已经最新"：go 的构建缓存已经做了这件事，
# 全缓存命中时这一步约 0.15 秒，自己写一套时间戳比较只会多一处会错的地方。
say "构建 openiotd（控制台已嵌入）"
mkdir -p "${DEMO_DIR}"
go build -o "${BIN}" ./cmd/openiotd

if [ ! -f "${DATA_DIR}/manifest.json" ]; then
  say "初始化数据目录 ${DATA_DIR}"
  "${BIN}" init --data-dir "${DATA_DIR}" --log-level error >/dev/null

  say "签发演示设备证书"
  for name in "${DEMO_NAMES[@]}"; do
    "${BIN}" cert issue --data-dir "${DATA_DIR}" \
      --thing "${name}" --policy device-scoped \
      --out "${DEVICES_DIR}/${name}" >/dev/null
  done
else
  say "复用已有数据目录 ${DATA_DIR}（删掉 ${DEMO_DIR}/ 可重来）"
fi

TOKEN=$(cat "${DATA_DIR}/admin-token")

cat > "${CONFIG}" <<YAML
node:
  data_dir: ${DATA_DIR}
listeners:
  mqtt_tls:
    enabled: true
    addr: "127.0.0.1:${MQTT_PORT}"
  admin:
    enabled: true
    addr: "127.0.0.1:${ADMIN_PORT}"
log:
  level: info
  format: text
YAML

say "启动节点"
"${BIN}" serve --config "${CONFIG}" &
PID=$!
trap 'kill ${PID} 2>/dev/null || true' EXIT INT TERM

# 等节点就绪。启动失败时必须明确报错而不是继续往下走——
# 继续走会让后面的预置步骤静默失败，最后打印一个假的"就绪"。
ready=no
for _ in $(seq 1 $(( READY_TIMEOUT * 10 ))); do
  if ! kill -0 ${PID} 2>/dev/null; then
    echo
    printf '\033[1;31m%s\033[0m\n' "节点启动失败，见上方日志"
    echo
    echo "常见原因与对应处置："
    echo "  · 端口被占用        MQTT_PORT=18883 ADMIN_PORT=19090 make demo"
    echo "  · 数据目录被占用    make demo-clean（日志里会有「文件锁超时」）"
    echo "  · 配置或证书有问题  按上方日志处理；删掉 ${DEMO_DIR}/ 可从头来一遍"
    exit 1
  fi
  if curl -sf -o /dev/null "http://127.0.0.1:${ADMIN_PORT}/readyz" 2>/dev/null; then
    ready=yes
    break
  fi
  sleep 0.1
done
[ "${ready}" = yes ] || die "
节点未在 ${READY_TIMEOUT} 秒内就绪"

API="http://127.0.0.1:${ADMIN_PORT}/api/admin/v1"

# 预置一些演示数据：空的控制台看不出效果。
# 全部经真实的 Admin API 写入，不是伪造的展示数据。
#
# 六处调用走同一个 api()，而不是各写一遍 auth 头与 Content-Type。
# 收敛掉的不只是重复：策略那一条因为要用 heredoc，此前绕开了统一的
# seed() 包装、改用行内的 `|| seed_failed=1`，于是它失败时只会汇总成
# 一句"部分预置失败"，不说是哪一条——而它恰恰是最容易失败的那条
# （策略体最长、最容易写错）。
seed_failed=0
api() {  # api <方法> <路径> [<body>|-]；body 为 - 时从 stdin 读
  local method=$1 path=$2 body=${3-}
  local args=(-sf -X "${method}" -o /dev/null
              -H "Authorization: Bearer ${TOKEN}"
              -H 'Content-Type: application/json')
  case "${body}" in
    "")  ;;
    "-") args+=(--data-binary @-) ;;
    *)   args+=(-d "${body}") ;;
  esac
  curl "${args[@]}" "${API}${path}" \
    || { seed_failed=1; printf '  预置失败: %s %s\n' "${method}" "${path}" >&2; }
}

say "预置演示数据"

# 这份策略不挂在任何设备上，它的用途是在策略调试器里演示
# "看起来写对了却仍被拒"：Deny 覆盖 Allow，以及变量 Attributes[site]
# 求值后把 topicfilter 限死在设备自己的站点上。
api PUT /policies/readonly-telemetry - <<'JSON'
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
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/telemetry/${iot:Connection.Thing.Attributes[site]}/*"
    },
    {
      "Effect": "Deny",
      "Action": "iot:Publish",
      "Resource": "arn:aws:iot:*:*:topic/telemetry/*"
    }
  ]
}
JSON

for pair in "sensor-01:shanghai" "sensor-02:beijing" "gateway-01:shanghai"; do
  name=${pair%%:*}; site=${pair##*:}
  api PUT "/things/${name}" \
    "{\"policyName\":\"device-scoped\",\"attributes\":{\"site\":\"${site}\"}}"
done

# 给一台设备写一份影子，使聚合页有内容可看
api PUT /things/sensor-01/shadow \
  '{"state":{"desired":{"interval":30,"mode":"run"}}}'

[ "${seed_failed}" = 0 ] || warn "部分演示数据预置失败，控制台仍可用"

echo
printf '\033[1;32m%s\033[0m\n' "就绪"
echo
echo "  控制台    http://127.0.0.1:${ADMIN_PORT}/"
echo "  MQTT      127.0.0.1:${MQTT_PORT}（mTLS，证书与 CA 链在 ${DEVICES_DIR}/<设备名>/）"
echo
echo "  控制台首屏要粘贴这个令牌："
printf '    \033[1;33m%s\033[0m\n' "${TOKEN}"
echo
echo "  已预置    策略 device-scoped 与 readonly-telemetry；设备 ${DEMO_NAMES[*]}；"
echo "            sensor-01 带一份 desired 影子"
echo
# 只留「一定能通的那一条」加一句为什么别的不通。
#
# 这里曾经把策略形状、两种会被拒的用法、排查去哪看、策略调试器的三个
# 示例、mosquitto 命令全列了一遍，一共 54 行——而那些内容在 README 的
# 「首次连接的常见失败原因」、docs/tutorial/zh/03-policy/03-debug.md 与
# deploy/standalone/README.md 里各有一份。终端这第三份不但把真正要用的
# 两样东西（地址与令牌）挤出了视野，而且是三份里唯一没人会想起要更新的。
echo "  策略按设备名收敛：clientId 必须等于设备名，主题限 \$aws/things/<设备名>/*。"
echo "  先试「订阅 \$aws/things/sensor-01/#」，它一定能通；随机 clientId 与 sensors/temp"
echo "  这类平坦主题一律 NotAuthorized —— 与 AWS 一致，不是故障。"
echo
echo "  原因与排查    控制台标题栏的「使用教程」，或控制台 → 策略调试器"
echo "  连真实设备    mosquitto 命令见 deploy/standalone/README.md"
echo
echo "  Ctrl-C 停止"
echo

wait ${PID}
