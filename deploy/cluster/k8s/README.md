# 三节点 · Kubernetes

跨机高可用。三个 Pod 由 StatefulSet 管理，各自一份持久卷与稳定的 DNS 名。

共通的事（多数派算术、能力与边界表、加减节点、两条铁律、备份与恢复）在
[`../README.md`](../README.md)。

> **验证程度（2026-10-31）**：这套清单在 **Docker Desktop 的单节点
> Kubernetes（v1.34.1）上实跑过**，下面这些已经验证：`prepare.sh` 生成
> 证书与四个 Secret、`kubectl apply -f .` 的顺序、PVC 绑定与 fsGroup
> 权限、initContainer 的 init 与设备 CA 分发、`subPathExpr` 让每个 Pod
> 取到自己那张证书（CommonName 等于 Pod 名）、按序号算 bootstrap/join、
> 三节点经 headless Service 选出同一个 leader（`quorum: 2`、成员视图
> 一致）、跟随者写入转发（204）与三节点读回同一个 `metaRevision`、
> 以及**强杀 leader 后约 8 秒选出新 leader 且写入照常**。
>
> **没有验证的是跨故障域那一半**：单节点上 `requiredDuringScheduling`
> 的反亲和让另外两个 Pod 永远 Pending（这是清单的设计意图，不是缺陷），
> 所以那次验证是把反亲和摘掉跑的。真实多节点上的调度分散、
> `kubectl drain` 与 PDB 的配合、滚动升级，仍**请先在测试集群上过一遍**。
>
> 那一轮实跑压出了一个真缺陷：复制层拨号时把对端证书按 `localhost`
> 做主机名校验，而 k8s 的复制证书只含 Pod 的 DNS 名，于是选举阶段
> TLS 握手全部失败、三个 Pod 一致地报「对端不可达」。已修（CLU-102）。

## 部署

```bash
cd deploy/cluster/k8s

# 设备用什么域名连进来就写进来 —— 服务端证书的 SAN 在这一步定死
export EXTRA_HOSTS='mqtt.example.com'

./prepare.sh                  # 生成证书与 Secret，只跑一次。需要本机有 openssl、docker 与 kubectl
kubectl apply -f .
kubectl -n open-iot-core rollout status statefulset/openiotd
```

`prepare.sh` 建三个 Secret：

| Secret | 内容 | 为什么 |
| --- | --- | --- |
| `openiotd-repl-tls` | 复制 CA + 每个 Pod 一套证书 | CN 必须等于 Pod 名，Pod 靠 `subPathExpr` 取自己那张 |
| `openiotd-device-ca` | 设备 CA 与服务端证书 | 三个 Pod 必须共用 |
| `openiotd-admin-token` | 管理面令牌 | 统一，否则三个 Pod 三个令牌 |

此前还有第四个 `openiotd-ca-passphrase`，随 AUTH-092 去掉了。于是
`openiotd-device-ca` 里的签发 CA 私钥是**明文 PEM**，PVC 里的 access key
secret 同样是明文。k8s 的 Secret 默认只是 base64，**不是加密**，所以这两处
的保护现在全靠：Secret 的 RBAC、PVC 所在存储的加密，以及
[etcd 的静态加密](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
——生产务必配上第三项，它不是默认开的。要更强的隔离就换外部 KMS 或 CSI
secret driver 接管 `openiotd-device-ca`。

## 在单节点集群上试（Docker Desktop、minikube、kind）

清单默认要求三个 Pod 落在**不同节点**上（`requiredDuringScheduling` 反亲和）。
单节点集群上只有一个 Pod 能调度，另外两个永远 Pending，而 `openiotd-0`
作为 bootstrap 等不到多数派，会每 15 秒退出一次、进入 CrashLoopBackOff。
这是设计意图：三个副本挤在一台机器上，那台机器一挂就直接失去多数派。

只是想在本机看看的话，把反亲和摘掉（**仅限本机，不要带进生产**）：

```bash
kubectl -n open-iot-core patch statefulset openiotd --type=json \
  -p '[{"op":"remove","path":"/spec/template/spec/affinity"}]'
kubectl -n open-iot-core delete pod --all     # 三个都要重建
```

必须三个 Pod 一起删：反亲和是**对称**的，只要还有一个旧 Pod 带着规则
活着，新 Pod 就仍然调度不上去，而事件里报的是「didn't satisfy existing
pods anti-affinity rules」——说的是别人的规则，不是自己的。

想在本机验集群行为，更合适的是 [`../compose/`](../compose/)。

## 本机实测到哪一步

2026-11-20 在 Docker Desktop 的单节点 k8s（v1.34.1）上跑过两轮（摘掉
反亲和）。**第二轮推翻了第一轮的一个结论**，两轮都记在这里。

验证通过的：

- 三个 Pod 冷启动 13 秒全部 Ready，选出 leader，`quorum: 2`、成员视图一致。
- 在一个 Pod 上建策略与设备，另外两个 Pod 读得到（元数据随日志复制）。
- **删掉任意一个 Pod，它在 45 到 61 秒内以「重启次数 0」回到 Ready**
  （连删两个不同的 Pod，各验一次）。
- **持久会话与离线队列跨 Pod**：设备在 Pod 0 上以 clean session = 0 订阅
  并干净断开，从 Pod 1 的管理面发一条 QoS 1，再到 **Pod 2** 上重连——
  session present = 1 且收到那条排队的消息。Pod 2 从没见过这个设备，
  两件事都只能来自复制（CLU-111 至 CLU-117）。
- 上面那条的加强版：**在重连之前把入队的那个 Pod（Pod 0）删掉**，Pod 2
  上照样补发出来。这才是「离线队列不再钉在节点上」的判据。

**第一轮那句「被删的 Pod 重建后追平了它不在期间写入的数据」是错的**，
第二轮反复压它时稳定地复现为 CrashLoopBackOff。两个真缺陷：

- **CLU-119：自定义 raft 传输层缺写超时。** dragonboat 对每个对端只持有
  一条连接，写入阻塞住那条连接的发送协程就永远不返回，于是 leader 从此
  不再跟那个跟随者说话，而且既不报错也不重拨。现象极具误导性：重建的
  Pod 出站正常（对端收得到它的拉票），入站一条都没有，而 leader 那边
  关于这个对端的日志**完全是空的**——没有拨号、没有错误、没有重试。
  容器重启而 Pod IP 不变时最容易撞上（旧套接字半开，写入先被内核缓冲
  接下来，不报错）。三进程验收碰不到它：那里杀掉的节点不会被自动拉起。
- **CLU-118：启动路径上「等不到就退出」本身会阻断收敛。** 进程一退，
  端口随之关闭，对端的拨号从「超时」变成「连接被拒绝」、断路器一直
  开着，于是更等不到；而每轮重启还把本节点的任期推高一截，对端又以
  「leader 尚在」为由丢弃它的拉票。最终是**永久分裂**——它既选不上又
  跟不上（任期比 leader 高，拒收心跳），而任期是落盘的，那个 PVC 从此
  废掉，只能清掉重建。现在会话分片的就绪改成就绪探针的一个分项
  （`shards`），元数据组的等待上限放宽到两分钟并打进度日志。

**跨故障域的那一半仍未验证**：单节点上三个 Pod 挤在一台机器，摘掉反亲和
才起得来，所以"一台机器挂掉仍可写"这件事本机验不了。

### 本机验证的两个坑

- **摘反亲和之后要删两轮 Pod。** 规则是对称的：只要还有一个带着旧规则的
  Pod 活着，新 Pod 就调度不上去，而事件里报的是「didn't satisfy existing
  pods anti-affinity rules」——说的是别人的规则，不是自己的。
- **想换成本地构建的镜像要改清单。** `30-statefulset.yaml` 里镜像是写死的
  （`prepare.sh` 只在它那个临时 init 容器上认 `OPENIOTD_IMAGE`）。

## 确认集群成形

```bash
kubectl -n open-iot-core port-forward svc/openiotd 9090:9090 &
TOKEN=$(kubectl -n open-iot-core get secret openiotd-admin-token \
  -o jsonpath='{.data.admin-token}' | base64 -d)

curl -s -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:9090/api/admin/v1/cluster/members
```

应当看到 3 个成员、`quorum: 2`、一个 `leader`。

## 为什么是这些选择

| 选择 | 理由 |
| --- | --- |
| StatefulSet 而非 Deployment | 节点 ID 写在数据目录里、复制证书的 CN 必须等于它、复制组按固定 DNS 名拨号。三样都要求身份稳定 |
| `replicas: 3` 且不配 HPA | 节点数随负载跳动意味着多数派跟着跳；而四节点与三节点的容错数相同 |
| `podManagementPolicy: Parallel` | 选举要多数派同时在场。默认的逐个启动会卡在第一个 Pod 等 ready |
| headless Service + `publishNotReadyAddresses` | 节点要点对点连接，不能走 ClusterIP 的负载均衡；选举期间还没 ready 但必须能互相找到 |
| `requiredDuringScheduling` 反亲和 | 三个 Pod 落在同一台机器上时，这台机器一挂就直接失去多数派 |
| PDB `maxUnavailable: 1` | 一次 `kubectl drain` 可能带走两个 Pod，那就失去多数派了 |
| readiness `/readyz`、liveness `/healthz` | 拿 `/healthz` 做 readiness 会把监听还没起来的 Pod 放进 Service |

主容器虽然要用 shell 按 Pod 序号设置 bootstrap/join，最终仍显式执行镜像的
`docker-entrypoint.sh`。Kubernetes 的`command`会覆盖镜像ENTRYPOINT；若直接
`exec openiotd`，入口脚本基于cgroup设置`GOMEMLIMIT`的保护会被绕过，大连接节点
会直接撞到OOM killer。对应接线有Go门禁，不能改回直接启动二进制。

## 扩容

**不要只改 `replicas`。** 顺序见 [`../README.md`](../README.md) 的「加减节点」，
k8s 这边具体是：

```bash
# 1) 给新 Pod 签一张 CN=openiotd-3 的复制证书并更新 Secret
#    prepare.sh 拒绝覆盖已有的 pki/，这一张目前要手工补签
# 2) 在 ConfigMap 的 cluster.replication.replicas 里给它一个副本号
#    （接着往后排，这里是 4），然后扩容
kubectl -n open-iot-core scale statefulset/openiotd --replicas=4
# 3) 加进复制组。replicaId 必须与上一步配的那个相同——共识层按它认成员，
#    而 leader 无从知道新 Pod 配了几号（见 ../README.md 的铁律 3）。
curl -sf -H "Authorization: Bearer $TOKEN" -X POST \
  -H 'Content-Type: application/json' \
  -d '{"id":"openiotd-3","addr":"openiotd-3.openiotd-peers:7946","replicaId":4}' \
  http://127.0.0.1:9090/api/admin/v1/cluster/members
```

缩容反过来：先 `DELETE .../cluster/members/openiotd-3`，再 `scale`。

## 升级

```bash
kubectl -n open-iot-core set image statefulset/openiotd \
  openiotd=ghcr.io/itmisx/open-iot-core:1.2.3
```

默认滚动更新，配合 PDB 一次只动一个 Pod。更新期间写入可用（三节点掉一个
仍有多数派），但同一 clientId 的跨节点互斥是最终一致的——Pod 轮换时设备
重连到别的节点会有一次 mesh 往返的窗口。

## 还没有的

- **没有 Ingress 示例。** 管理面不建议配 Ingress（它能吊销证书、踢连接、
  改策略）；设备侧 8883 是 mTLS，要对外请用 LoadBalancer 或 NodePort，
  并确认服务端证书的 SAN 里有那个域名（也就是 `prepare.sh` 的
  `EXTRA_HOSTS`，不是 ConfigMap 里的 `server_hosts`）。
- **基础三节点示例没有 resources requests/limits。** 它用于功能试跑，不能直接
  承载百万连接。五节点容量部署使用[`../k8s-million/`](../k8s-million/) overlay：
  每Pod请求8 CPU/32GiB、限制40GiB、100GiB卷，并提供只暴露8883且
  `externalTrafficPolicy: Local`的LoadBalancer。最终仍须按目标机器密度轮校准。
- **镜像写死成 `latest`。** 配 `imagePullPolicy: IfNotPresent` 既不会自动
  更新，又让「这三个 Pod 跑的是哪一版」取决于各节点碰巧缓存了什么。
  生产钉版本，见上面的「升级」。
- **没有 NetworkPolicy。** 生产里至少要限制谁能访问 9090 与 7946。
- **没有 Helm chart。** 清单只有五个文件，先把它们读懂比套一层模板有用。
- **没有 ServiceMonitor。** 指标在 `/metrics`，需要管理面令牌，
  Prometheus 侧配 `bearer_token_file`。
