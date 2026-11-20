# 单节点

一个进程一个节点，元数据走一副本 raft 组（没有选举可做）。节点掉了就停服
——要高可用看 [`../cluster/`](../cluster/)。

两个子目录是**同一个部署**，区别只在谁来记住那串挂载和端口参数：

| | 什么时候用 |
| --- | --- |
| [`compose/`](compose/) | 默认选它。`up -d` 一条命令，容器自己把 init 补上 |
| [`docker/`](docker/) | 已有的脚本或编排里不想引入 compose 时。同样是一条 `docker run`（`OPENIOTD_AUTO_INIT=1`），生产上把那个变量去掉 |

配置是共用的一份 [`openiotd.yaml`](openiotd.yaml)，改它对两种方式同时生效。
它与 `examples/openiotd.yaml` 的差别只在容器相关的三处：管理面监听从回环
改成 `0.0.0.0`（设备侧那条本来就是）、数据目录指向卷、管理面显式确认对外暴露。

先读 [`../README.md`](../README.md) 的「开始之前定一件事」——
`pki.server_hosts` 事后改要重新初始化。那一节也写了 CA 口令为什么没有了，
以及由此而来的「数据目录就是明文密钥库」这个结论。

## 签一张设备证书

```bash
# compose 用 `docker compose exec openiotd`，裸 docker 用 `docker exec openiotd`。
docker compose exec openiotd \
  openiotd cert issue --thing sensor-01 --policy device-scoped \
  --out /var/lib/openiotd/devices/sensor-01

docker compose cp openiotd:/var/lib/openiotd/devices/sensor-01 ./sensor-01
```

输出目录里就是设备要的三个文件——`sensor-01.pem`、`sensor-01.key`
与 `ca-chain.pem`（CA 链 `cert issue` 会一并写出，不用另外取）：

```bash
mosquitto_sub -h 127.0.0.1 -p 8883 -i sensor-01 \
  --cert ./sensor-01/sensor-01.pem \
  --key  ./sensor-01/sensor-01.key \
  --cafile ./sensor-01/ca-chain.pem \
  -t '$aws/things/sensor-01/#' -v
```

`clientId` 必须等于设备名，主题必须在 `$aws/things/<设备名>/` 下面——
起步策略按设备名收敛，别的形状一律 `NotAuthorized`。被拒时去控制台的
策略调试器查，不用真连设备。

## 升级

```bash
# compose
docker compose pull && docker compose up -d
# 裸 docker：pull、stop、rm，再重跑一次 serve 那条命令
```

数据卷不动。删了等于换掉 CA，全部设备证书作废。

## 备份

单机要备份的就是那一个卷（共通的那半——里面有什么、为什么它的密级等于
整套系统的密级——在 [`../README.md`](../README.md)）：

```bash
docker run --rm -v openiotd_data:/data -v "$PWD:/backup" alpine \
  tar czf /backup/openiotd-$(date +%F).tar.gz -C /data .
```

卷名随 compose 项目名（`name: openiotd`）前缀，`docker volume ls` 能看到;
裸 docker 那边是自己建的 `openiotd-data`。

里面含**明文的** CA 私钥与管理面令牌，按密级保管；如果按上面那条
`cert issue --out /var/lib/openiotd/devices/...` 签过证书，**设备私钥也在
这个卷里**，于是也在每一份备份里。恢复不需要口令，把卷放回去即可——也就是
说拿到这份 tar 的人就拿到了签发能力，备份文件本身务必自己加密。

## 别把 9090 直接挂公网

两种方式都只把端口绑在宿主的 `127.0.0.1` 上。管理面能吊销证书、踢连接、
改策略，要对外提供请放在反向代理后面，并至少做到：限制来源 IP、走 HTTPS、
不要把令牌写进 URL。

设备侧的 8883 本身是双向 TLS，可以直接对外——但先确认
`pki.server_hosts` 里有设备用来连接的那个名字。
