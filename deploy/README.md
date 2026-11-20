# 部署

```
standalone/   单节点。掉了就停服
  docker/       裸 docker 命令
  compose/      docker compose（同一个部署，少记几个参数）
cluster/      三节点，元数据经 raft 多数派复制
  compose/      一台机器上三个容器 —— 验集群行为用，不是高可用
  k8s/          跨机三节点 —— 真正的高可用
Dockerfile    镜像构建，四种方式共用
```

| 要什么 | 用哪个 |
| --- | --- |
| 试用、边缘站点、小规模生产 | [`standalone/compose/`](standalone/compose/) |
| 嵌进已有的脚本或编排 | [`standalone/docker/`](standalone/docker/) |
| 想在本机看集群怎么复制、怎么失效 | [`cluster/compose/`](cluster/compose/) |
| 生产高可用 | [`cluster/k8s/`](cluster/k8s/) |

## 开始之前定一件事

它**第一次就要定对**，事后改要重新初始化，而重新初始化等于换掉整条
信任链——已签发的设备证书全部作废。其余配置随时能改。

> 以前这里是两件事，第一件是 CA 私钥口令（`OPENIOTD_CA_PASSPHRASE`）。
> **口令已整条去掉**：CA 私钥与兼容面 access key 的 secret 都以明文落在
> 数据目录里，保护只有 0700 的目录权限与 0600 的文件权限。换的是部署与
> 运维的简单——四种部署方式不必再各想一遍"口令怎么送进容器"，也不会再
> 因为送错一次而起不来。代价是**数据目录及其任何一份备份泄露，等于泄露
> 整条信任链的签发能力与全部 access key**，而那时吊销单张设备证书无济于
> 事。要静态加密请在**卷那一层**做（dm-crypt、云盘加密、k8s 的 etcd
> 静态加密），不要指望应用层。旧配置里的 `pki.passphrase_file` 现在会被
> 配置层拒绝，环境变量则直接被忽略。

### 设备用什么名字连进来（`pki.server_hosts`）

它决定 broker 服务端证书的 SAN，`init` 时签一次，之后改配置**不会重签**。
漏写的表现是设备握手时报主机名不匹配，而服务端只看到一次失败的握手
（而且目前握手失败没有指标也没有日志，见根 README 的「已知边界」）。
域名、k8s Service 名、NodePort 的主机名——设备会用到的都写进去。

## 镜像

`ghcr.io/itmisx/open-iot-core`，打 tag 时由
[`release.yml`](../.github/workflows/release.yml) 构建推送（linux/amd64 与
linux/arm64，非 root 运行）。`v1.2.3` 产出 `1.2.3`、`1.2` 与 `latest`；
带后缀的预发布（`v1.2.3-rc1`）不动 `latest`。生产钉具体版本，别用 `latest`。

本地构建：

```bash
docker build -f deploy/Dockerfile -t open-iot-core:dev .
export OPENIOTD_IMAGE=open-iot-core:dev    # 三份 docker 清单都认这个变量
```

k8s 那份**不认**：`30-statefulset.yaml` 里镜像是写死的（`prepare.sh` 只在
它那个临时 init 容器上认 `OPENIOTD_IMAGE`）。要换就改清单，或者
`kubectl set image`——见 [`cluster/k8s/README.md`](cluster/k8s/README.md) 的「升级」。

## 通用约定

下面几条对四种方式都成立，例外写在各条里。

### 数据目录必须是持久卷

里面有 CA 私钥、raft 日志、元数据与审计日志。容器一删就等于已签发的设备
证书全部作废，没有找回的办法。

镜像里数据目录是 `/var/lib/openiotd`，`OPENIOTD_DATA_DIR` 已经指好，所有
子命令都认它——因此「运维命令」那一节的命令都不必带 `--data-dir`。

### `init` 与 `serve` 是两步，不会自动合并

`serve` 遇到未初始化的目录会直接报错（CLU-055）：一个会自动建空集群的
`serve`，在数据目录写错时的症状是「所有设备认证失败」，而真因完全看不出来。

但**多数情况下你不必自己去跑那一步**：四份清单都替你记住了它，并一律带上
`--if-needed`（已做过的部分跳过），所以 `docker compose up -d` 或
`kubectl apply` 一条命令就位，反复执行也没事。两种记法：

| 清单 | 谁跑 init |
| --- | --- |
| [`standalone/compose/`](standalone/compose/) | 容器自己（`OPENIOTD_AUTO_INIT=1`，下一节），所以只有一个服务 |
| [`cluster/compose/`](cluster/compose/)、[`cluster/k8s/`](cluster/k8s/) | 一次性 init 服务 / initContainer。那一步还要把设备 CA 分发给其余节点，不只是建目录 |
| [`standalone/docker/`](standalone/docker/) | 容器自己（同一个开关）。生产上去掉它，改为手工先跑一次 init |

### 让容器自己补上 init（`OPENIOTD_AUTO_INIT`）

**这一条只适用于单节点**：集群的两份清单不要用它，理由见
[`cluster/README.md`](cluster/README.md) 的铁律 2——那里它会让每个节点各建
一套设备 CA。

镜像的入口脚本留了一个**默认关闭**的开关（NFR-105）：

```bash
-e OPENIOTD_AUTO_INIT=1
```

设上它，`serve` 之前会先跑一次 `init --if-needed`，于是单节点的快速开始是
一条命令。它只对 `serve` 子命令生效（`config print` 这类只读命令不会因此
建出数据目录），且只认 `1` 与 `true`。`standalone/compose/` 的清单里就设着
它，所以那份只有一个服务。

`serve` 本身的脾气没有变——自动初始化只在镜像入口这一层，所以它既不是
配置键（`openiotd config print` 与 `config env` 都不会提到它），也不会在
用二进制直接跑时起作用。

**代价要先接受**：打开之后，**卷挂错路径不再报错**，而是静默初始化出一个
空节点，于是原有设备全部认证失败——正是上一节那个「真因完全看不出来」的
症状。

这件事要算清楚，别照「有 init 服务就安全」的直觉办事：一个**无条件跑
`init --if-needed` 的前置服务**在这一点上与这个开关完全等价，两者都是编排
在替你初始化（CLU-055 禁止的是 `serve` 自己去做）。所以 `standalone/compose/`
从两个服务收成一个并没有变危险，真正的分界在**谁**掌握数据卷：单节点清单里
卷就写在同一份文件里，而裸 `docker run` 的 `-v` 由敲命令的人现写——那条路径
上生产还是用两步，让「目录没初始化」停在第一条命令上。

剩下两件小事：init 的输出会混进节点日志（而不是单独一份 `logs init`），
init 失败时容器按 restart 策略重试（而不是一次性服务那样干净地失败一次）。

### 管理面绑非回环地址要显式确认

要在配置里写 `acknowledge_public_exposure: true`，否则配置校验直接拒绝启动。
容器里没有「回环」可言，端口一映射就是对外的。

这个端点能吊销证书、踢连接、改策略——请放在反向代理或 NetworkPolicy 后面。
没有等价的命令行开关，这是刻意的：做成一个随手可加的 flag 就等于没有这道门。

### 探活用 `/readyz`，不是 `/healthz`

两个都不鉴权。`/healthz` 只回答「进程还活着吗」，监听还没起来时它也返回 ok；
`/readyz` 回答「能接流量吗」。做 readiness 探针用后者。

### 运维命令

`docker exec` / `docker compose exec` / `kubectl exec` 里都一样：

```bash
openiotd admin token                      # 打印管理面令牌
openiotd cert issue --thing sensor-01 --policy device-scoped \
  --out /var/lib/openiotd/devices/sensor-01
openiotd config print                     # 四层叠加后的生效配置
```

### 容量旁的两组旋钮

默认值按单节点 10 到 50 万连接规划（PRD LANG-009、NFR-001）。超出这个
区间的做法是加节点，不是提高单节点密度——每连接约 40 KB 内存是硬成本。

要调的话有两处，都可以留空（留空即取默认值）：

```yaml
# 配额：名字取自 docs/prd/17-quotas.md，0 表示取消该项限制。
# 名字打错会被拒绝启动，并在错误里列出全部可用名字。
quota:
  connections: 500000        # 并发连接上限（默认 500000）
  connect_rate: 3000         # 每秒建连上限（默认 3000）
  subscriptions: 500000      # 全局订阅总数上限（默认 500000）

# 接入层运行参数。
broker:
  writer_workers: 0          # 0 = 按 CPU 核数自适应（GOMAXPROCS × 2，下限 8）
  outbound_buffer: 64        # 每连接出站队列深度，满了即判为慢客户端并断开
  write_timeout: 5s          # 单次刷写上限，不要配到秒级以下
```

调高 `connections` 之前先确认两件事：文件描述符上限（每连接一个）与
内存（连接数乘以约 40 KB，再留 GC 余量；发布制品请设 `GOMEMLIMIT`）。

## 四处踩进去不报错的地方

这四条的共同形状是：操作看起来成功了，而实际没有生效，且**节点与命令都不
报错**——所以只能写下来。

### 命令行签出的证书要重启节点才生效

注册表在 v0.1 是文件化的临时实现，节点启动时把它导入控制面一次。免重启就走
管理面 API 或控制台的「签发证书」。

集群下也是这个机制：在哪个节点上签的，就要重启那个节点——导入会随同一条
复制日志到达其余节点。

### 吊销不要用命令行

`openiotd cert revoke` 改的是文件化注册表，而导入只认「控制面里还没有这个
指纹」——证书一旦进了控制面（节点起过一次就进了），这条命令**连重启也不会
生效**，而它和节点都不会报错。

吊销一律走管理面 `PUT /api/admin/v1/certificates/<指纹>/status` 或控制台的
证书页，那条路径本节点立即生效，其余节点一次复制往返后生效。

### `admin token --rotate` 是按节点的，而且要重启才生效

令牌只在启动时解析一次。单机上轮换完重启即可。

集群下三个节点各有一份（两份 compose 与 k8s 的初始化都刻意把它统一成同一
个），在一个节点上轮换只会让三个节点的令牌不一致；而 k8s 下 initContainer
每次起 Pod 都从 Secret 复制回去，轮换会被静默回滚。集群要换令牌：改 Secret
（k8s）或三个数据目录都换，再滚动重启。

### 备份就是备份数据目录

里面有 **明文的** CA 私钥与 access key secret、管理面令牌、raft 日志与
元数据；「运维命令」那条 `cert issue --out` 还会把**设备私钥**也写进去。
没有任何一层应用加密，所以这份备份的密级就等于整套系统的密级：按最高
密级保管，传输与静态都自己加密。恢复不需要任何口令——把目录放回去即可，
这既是它简单的地方，也是它危险的地方。

具体命令分别在 [`standalone/README.md`](standalone/README.md) 与
[`cluster/README.md`](cluster/README.md)——两者要备的东西数量不同。
