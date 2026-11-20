# 三节点 · docker compose

在一台机器上起三个容器。用来**看集群怎么复制、怎么失效**，不是高可用部署
——三个容器在同一台机器上，机器一挂全没。生产看 [`../k8s/`](../k8s/)。

共通的事（多数派算术、能力与边界表、加减节点、两条铁律、备份与恢复）在
[`../README.md`](../README.md)。

## 起步

```bash
cd deploy/cluster/compose

./gen-certs.sh                # 复制链的 TLS 材料，只跑一次（需要本机有 openssl）

docker compose up -d          # init 先跑完，再拉起三个节点
docker compose ps             # 等三个都 healthy
```

宿主端口：`8883/9090`（node-a）、`8884/9091`（node-b）、`8885/9092`（node-c）。
`8883` 很容易已被本机的 `make demo` 占着，换一组即可：

```bash
MQTT_PORT_A=18883 ADMIN_PORT_A=19090 docker compose up -d
```

用本地构建的镜像：`export OPENIOTD_IMAGE=open-iot-core:dev`。

## 确认它真的是一个集群

```bash
TOKEN=$(docker compose exec -T node-a cat /var/lib/openiotd/admin-token)

# 三个节点都该报 3 个成员、quorum 2、同一个 leader
for p in 9090 9091 9092; do
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:$p/api/admin/v1/cluster/members"; echo
done

# 在 node-a 上写，在 node-b 上读
curl -sf -H "Authorization: Bearer $TOKEN" -X PUT \
  -H 'Content-Type: application/json' -d '{"attributes":{"site":"compose"}}' \
  http://127.0.0.1:9090/api/admin/v1/things/lamp-1
curl -s -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:9091/api/admin/v1/things
```

**写完立刻读跟随者可能 404。** 跟随者在提交之后异步应用，窗口是一次复制
往返（通常毫秒级）。要读自己刚写的，读 leader，或者重试一次。

## 看它怎么失效

```bash
docker compose stop node-c
# 写入照常：三个成员的多数派是 2，还剩两个
curl -s -o /dev/null -w '掉一个后写入 %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN" -X PUT \
  -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:9090/api/admin/v1/things/lamp-2

docker compose stop node-b
# 只剩一个，失去多数派：写入 503，读取仍然 200
curl -s -o /dev/null -w '失去多数派后写入 %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN" -X PUT \
  -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:9090/api/admin/v1/things/lamp-3
curl -s -o /dev/null -w '失去多数派后读取 %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:9090/api/admin/v1/things

docker compose start node-b node-c
```

杀 leader 也值得试一次：从 `members` 的 `leader` 字段查出是谁，
`docker compose stop` 掉它，剩下两个应当在几秒内选出新 leader，
之后从任一存活节点写入照常（非 leader 会转发）。

## 清理

```bash
docker compose down -v   # -v 连数据卷一起删，CA 也没了
rm -rf pki
```

## 照搬到生产之前

- 三个数据卷里的 CA 私钥与 access key secret 都是明文（AUTH-092），
  生产请在卷那一层加密；
- 三个容器在同一台机器上，没有故障域隔离；
- 管理面端口直接映射出来，生产要放在反向代理后面并限制来源；
- 没有资源限制、日志收集与备份。
