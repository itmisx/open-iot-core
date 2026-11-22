#!/usr/bin/env bash
#
# 生成三个 Secret，k8s 清单依赖它们：
#
#   openiotd-repl-tls      复制组的 TLS 材料，每个 Pod 一套（CN=Pod 名）
#   openiotd-device-ca     设备 CA 与 session ticket key，三个 Pod 共用
#   openiotd-admin-token   管理面令牌，三个 Pod 统一
#
# 此前还有第四个 openiotd-ca-passphrase（CA 私钥口令），随 AUTH-092
# 去掉：私钥在 device-ca 这个 Secret 里就是明文 PEM，Secret 本身只是
# base64，所以该 Secret 的访问控制与 etcd 的静态加密现在是它唯一的
# 保护——生产务必配上，见 README。
#
# 设备 CA 为什么要先在本地生成再塞进 Secret：设备证书由任一节点签发，
# 而设备可能连到任意一个 Pod——各 Pod 自己生成 CA 的话，一台设备只能
# 连上签发它的那个节点，其余节点上 mTLS 直接握手失败。
#
# 这个脚本只跑一次。重跑会换掉整条信任链，已签发的设备证书全部作废。
set -euo pipefail
cd "$(dirname "$0")"

NS=${NS:-open-iot-core}
REPLICAS=${REPLICAS:-3}
IMAGE=${OPENIOTD_IMAGE:-ghcr.io/itmisx/open-iot-core:latest}
OUT=${OUT:-./pki}
# 对外域名：设备用什么名字连，这里就要写什么。服务端证书在下面
# init 的那一次性签出，之后改这里不会重签。
EXTRA_HOSTS=${EXTRA_HOSTS:-}

if [ -d "${OUT}" ]; then
  echo "已存在 ${OUT}/，不覆盖。重新生成会换掉整条信任链。" >&2
  exit 1
fi
mkdir -p "${OUT}"

# 复制证书只签 CommonName，**不签拨号用的主机名**（CLU-103）。拨号侧
# 不校验 SAN：它走 InsecureSkipVerify，自己用复制 CA 建链再比
# CommonName（CLU-102）。所以下面的 SAN 只有 Pod 名，而复制组拨的是
# `openiotd-N.openiotd-peers:7946`——两者对不上正是要的：对得上的话，
# 一个「又按主机名校验了」的回归就不会在这里显形。
echo "==> 生成复制 CA 与每个 Pod 的证书（CommonName 必须等于 Pod 名）"
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "${OUT}/repl-ca.key" -out "${OUT}/repl-ca.pem" \
  -subj "/CN=open-iot-core replication CA" >/dev/null 2>&1

i=0
while [ "$i" -lt "${REPLICAS}" ]; do
  pod="openiotd-${i}"
  openssl req -newkey rsa:2048 -nodes \
    -keyout "${OUT}/${pod}.key" -out "${OUT}/${pod}.csr" \
    -subj "/CN=${pod}" >/dev/null 2>&1
  cat > "${OUT}/${pod}.ext" <<EXT
subjectAltName = DNS:${pod}
extendedKeyUsage = serverAuth, clientAuth
EXT
  openssl x509 -req -in "${OUT}/${pod}.csr" -days 3650 \
    -CA "${OUT}/repl-ca.pem" -CAkey "${OUT}/repl-ca.key" \
    -CAserial "${OUT}/repl-ca.srl" -CAcreateserial \
    -extfile "${OUT}/${pod}.ext" -out "${OUT}/${pod}.pem" >/dev/null 2>&1
  rm -f "${OUT}/${pod}.csr" "${OUT}/${pod}.ext"
  i=$((i + 1))
done

echo "==> 用容器跑一次 init，产出设备 CA 与管理面令牌"
# 借一个临时容器来生成，避免要求本机装 openiotd。
hosts="localhost,127.0.0.1,openiotd,openiotd.${NS}.svc.cluster.local"
i=0
while [ "$i" -lt "${REPLICAS}" ]; do
  hosts="${hosts},openiotd-${i}.openiotd-peers"
  i=$((i + 1))
done
[ -n "${EXTRA_HOSTS}" ] && hosts="${hosts},${EXTRA_HOSTS}"

# server_hosts 只能经配置文件给（没有等价的命令行开关），
# 所以这里临时写一份只含这一项的配置。
seedcfg="${OUT}/.seed-init.yaml"
{
  echo "pki:"
  echo "  enabled: true"
  echo "  server_hosts:"
  echo "${hosts}" | tr ',' '\n' | while read -r h; do
    [ -n "$h" ] && echo "    - \"$h\""
  done
} > "${seedcfg}"

docker volume rm -f openiotd-seed >/dev/null 2>&1 || true
docker run --rm -v openiotd-seed:/var/lib/openiotd \
  -v "$(cd "$(dirname "${seedcfg}")" && pwd)/$(basename "${seedcfg}"):/tmp/init.yaml:ro" \
  "${IMAGE}" init --data-dir /var/lib/openiotd --config /tmp/init.yaml >/dev/null
rm -f "${seedcfg}"
docker run --rm -v openiotd-seed:/var/lib/openiotd --entrypoint /bin/sh \
  "${IMAGE}" -c 'cd /var/lib/openiotd && tar cf - pki admin-token' \
  | tar xf - -C "${OUT}"
docker volume rm -f openiotd-seed >/dev/null

echo "==> 生成集群共享的 TLS session ticket 密钥"
openssl rand -hex 32 > "${OUT}/pki/session-ticket.keys"
chmod 600 "${OUT}/pki/session-ticket.keys"

echo "==> 创建 Secret"
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

args=(--from-file=ca.crt="${OUT}/repl-ca.pem")
i=0
while [ "$i" -lt "${REPLICAS}" ]; do
  args+=(--from-file="openiotd-${i}.crt=${OUT}/openiotd-${i}.pem")
  args+=(--from-file="openiotd-${i}.key=${OUT}/openiotd-${i}.key")
  i=$((i + 1))
done
kubectl -n "${NS}" create secret generic openiotd-repl-tls "${args[@]}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NS}" create secret generic openiotd-device-ca \
  --from-file="${OUT}/pki/root-ca.pem" \
  --from-file="${OUT}/pki/issuing-ca.pem" \
  --from-file="${OUT}/pki/issuing-ca.key" \
  --from-file="${OUT}/pki/ca-chain.pem" \
  --from-file="${OUT}/pki/server.pem" \
  --from-file="${OUT}/pki/server.key" \
  --from-file="${OUT}/pki/session-ticket.keys" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NS}" create secret generic openiotd-admin-token \
  --from-file=admin-token="${OUT}/admin-token" \
  --dry-run=client -o yaml | kubectl apply -f -

echo
echo "==> 完成。${OUT}/ 里是私钥，已在 .gitignore 中；请离线备份后删除。"
echo "    管理面令牌：$(cat "${OUT}/admin-token")"
