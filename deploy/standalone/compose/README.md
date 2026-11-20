# 单节点 · docker compose

共通的事（`server_hosts`、备份、升级、签设备证书）在
[`../README.md`](../README.md)。这里只有命令。

**不用自己先跑 `init`**：清单里设了 `OPENIOTD_AUTO_INIT=1`，容器在 `serve`
之前自己跑一次 `init --if-needed`（已做过的部分跳过），所以下面这一条命令
对空卷和已有数据都成立，反复执行也没事。

这份清单因此只有一个服务（以前是「一次性 init 服务 + `depends_on`」两个，
行为与现在等价）。那个开关的代价、以及为什么换掉它不等于放宽 CLU-055，
在 [`../../README.md`](../../README.md)。

```bash
cd deploy/standalone/compose

docker compose up -d          # 容器内先 init，再 serve
docker compose ps             # 等 openiotd 变成 healthy
```

控制台在 <http://127.0.0.1:9090/>，首屏要粘贴令牌：

```bash
docker compose exec openiotd openiotd admin token
```

端口被占就换一组（`8883` 很容易已被本机的 `make demo` 占着）：

```bash
MQTT_PORT=18883 ADMIN_PORT=19090 docker compose up -d
```

用本地构建的镜像：`export OPENIOTD_IMAGE=open-iot-core:dev`。

```bash
docker compose logs -f openiotd
docker compose down           # 停掉，数据卷保留
docker compose down -v        # 连数据卷一起删 —— CA 也没了
```
