#!/usr/bin/env bash
#
# 生成复制组用的 TLS 材料。
#
# 这是一条**与设备 CA 无关的独立信任链**（CLU-068）：设备 CA 被攻破
# 不应当等于别人可以加入复制组。两者混用时，任何一张设备证书都能冒充
# 一个节点。
#
# 每张节点证书的 CommonName 必须等于节点 ID（CLU-069）——复制层就是靠
# 它认对端身份的。
#
# **不要给这些证书签上拨号时用的主机名**（CLU-103）。拨号侧根本不校验
# SAN：它走 InsecureSkipVerify，自己用复制 CA 建链再比 CommonName
# （CLU-102）。此前这里签的是 `DNS:localhost, IP:127.0.0.1`，而当时的
# 实现把 ServerName 钉成 localhost 并未关掉主机名校验——装置与实现
# 带着同一个假设，于是 compose 与验收脚本一路绿，而 k8s 的复制证书
# （只含 Pod 的 DNS 名）在选举阶段握手全部失败。下面只签 DNS:${id}，
# 它对校验没有作用，留着只为人工 `openssl x509 -text` 时好认。
set -euo pipefail
cd "$(dirname "$0")"

OUT=${OUT:-./pki}
NODES=${NODES:-"node-a node-b node-c"}
DAYS=${DAYS:-3650}

if [ -d "${OUT}" ]; then
  echo "已存在 ${OUT}/，不覆盖。要重新生成请先删掉它。" >&2
  exit 1
fi
mkdir -p "${OUT}"

echo "==> 生成复制 CA"
openssl req -x509 -newkey rsa:2048 -nodes -days "${DAYS}" \
  -keyout "${OUT}/ca.key" -out "${OUT}/ca.pem" \
  -subj "/CN=open-iot-core replication CA" >/dev/null 2>&1

for id in ${NODES}; do
  echo "==> 签发 ${id}（CommonName=${id}）"
  openssl req -newkey rsa:2048 -nodes \
    -keyout "${OUT}/${id}.key" -out "${OUT}/${id}.csr" \
    -subj "/CN=${id}" >/dev/null 2>&1
  cat > "${OUT}/${id}.ext" <<EXT
subjectAltName = DNS:${id}
extendedKeyUsage = serverAuth, clientAuth
EXT
  openssl x509 -req -in "${OUT}/${id}.csr" -days "${DAYS}" \
    -CA "${OUT}/ca.pem" -CAkey "${OUT}/ca.key" \
    -CAserial "${OUT}/ca.srl" -CAcreateserial \
    -extfile "${OUT}/${id}.ext" -out "${OUT}/${id}.pem" >/dev/null 2>&1
  rm -f "${OUT}/${id}.csr" "${OUT}/${id}.ext"
done

# 私钥给容器里的 uid 10001 读。
chmod 644 "${OUT}"/*.pem
chmod 644 "${OUT}"/*.key
echo "==> 完成，见 ${OUT}/"
