# 五节点百万连接容量 overlay

这套 overlay 对应最终验收拓扑：五个固定 openiotd 节点，每节点20万真实mTLS
长连接，session与shadow均为8分片、RF=3。它不是“装上就保证百万”的营销清单；
宿主、负载均衡器、压测机和证书预置仍须通过下面的硬检查。

## 节点前提

五个Pod必须落在五台不同Linux宿主；基础清单的required pod anti-affinity会执行
这一点。每台宿主至少提供8个可调度CPU、40GiB可用内存和100GiB持久卷。overlay
请求8 CPU/32GiB并把容器内存上限设为40GiB；镜像入口据此自动设置约30GiB的
`GOMEMLIMIT`，给socket、mmap和page cache保留约10GiB。CPU不设limit，避免CFS
quota在冷启动风暴时把TLS握手节流成周期性长尾。

每台broker和load generator开跑前至少确认：

```bash
ulimit -Sn                         # >= 208192
cat /proc/sys/fs/file-max          # 建议 >= 1048576
cat /proc/sys/net/netfilter/nf_conntrack_max  # 建议 >= 524288
cat /proc/sys/net/core/somaxconn   # 冷启动风暴建议 >= 65535
```

这些是节点级设置，不能由非特权Pod可靠修改。用节点镜像、systemd-sysctl或受控的
运维DaemonSet配置；不要给broker容器增加`privileged`或`CAP_SYS_ADMIN`。最终值以
`tests/bench/connect-density-linux.sh`的预检和现场时间线为准。

## 部署

先为五个Pod生成复制证书和共享设备CA，再应用overlay：

```bash
cd deploy/cluster/k8s
REPLICAS=5 EXTRA_HOSTS=mqtt.example.internal ./prepare.sh
kubectl apply -k ../k8s-million
```

`openiotd-mqtt`只暴露8883，管理面9090仍仅在原ClusterIP Service内。MQTT入口使用
`externalTrafficPolicy: Local`，避免kube-proxy再把长连接跨节点转发。云厂商支持
IP target/Pod直连模式时应按其文档给该Service增加annotation；否则节点conntrack
仍须覆盖本机20万连接及安全余量。

首次部署必须看到5个不同`node_id`、同一`cluster_id`且全部ready。然后在五台独立
load generator上使用相同`START_AT_UNIX`/`HOLD_UNTIL_UNIX`、不同
`LOADGEN_INDEX=0..4`运行`tests/bench/connect-density-linux.sh`。五台的`BROKER`
都填同一个`openiotd-mqtt` LoadBalancer地址并设置`INGRESS_MODE=shared-lb`；
`ADMIN`则分别指向openiotd-0..4的内部管理端（VPN、堡垒机或各自port-forward均可），
从而采到五个不同节点的连接峰值而不把9090暴露到公网。最后执行：

```bash
EXPECTED_INGRESS_MODE=shared-lb \
  tests/bench/verify-million-density.sh /results/density-{0,1,2,3,4}
```

只有聚合器得到1,000,000 held、zero errors、五个不同broker节点、五节点服务端
连接峰值之和至少一百万和共同保持窗口，才算百万真实mTLS并发验收。PROXY v2
核心密度轮不能替代它。该验收使用独立空集群；任一节点开跑前已有MQTT连接都会
被聚合器拒绝，不能拿生产存量补足服务端峰值。

overlay显式设置`auth.mode: cert`。`require_client_cert`只保证TLS握手验证证书链；
若MQTT认证使用`insecure-none`，吊销、Thing绑定与策略检查都会被绕过，这种结果
只能算TLS transport探针，不能作为本节的真实mTLS验收材料。
