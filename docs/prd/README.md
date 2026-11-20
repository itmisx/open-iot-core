# open-iot-core 需求基线

本目录是 `open-iot-core` 的需求基线。实现不得绕过这里定义的接口契约与行为边界；
与本目录冲突的代码视为缺陷，而非既成事实。

架构推导过程与竞品分析见 `../blueprint.html`。本目录是其规范化结果，二者冲突时以本目录为准。

## 文档索引

| 文档 | 内容 | 前置依赖 |
| --- | --- | --- |
| [00-product.md](00-product.md) | 产品定位、目标用户、竞争格局、非目标、许可策略 | — |
| [01-language-runtime.md](01-language-runtime.md) | 语言与运行时选型，Go 的硬性设计约束，结论反转判据 | 00 |
| [02-architecture.md](02-architecture.md) | 四个数据平面、13 条 Day-1 接口契约、包结构、CI 强制规则 | 01 |
| [03-cluster.md](03-cluster.md) | 内嵌 Raft 拓扑、节点生命周期、元数据后端、单机到集群演进 | 02 |
| [04-compatibility.md](04-compatibility.md) | AWS IoT Core 兼容性矩阵，含刻意偏离项 | 00 |
| [05-mqtt-broker.md](05-mqtt-broker.md) | 协议、会话、路由、QoS、保留消息、共享订阅 | 02 04 |
| [06-transport.md](06-transport.md) | 端点、端口、TLS、ALPN、WebSocket、接入限速 | 04 |
| [07-identity-authz.md](07-identity-authz.md) | 认证方式、IoT Policy 求值、策略变量、SigV4 | 02 |
| [08-pki.md](08-pki.md) | CA、签发、吊销、密钥后端 | 03 07 |
| [10-shadow.md](10-shadow.md) | Device Shadow 完整语义 | 03 |
| [12-registry.md](12-registry.md) | Thing、检索索引 | 03 |
| [14-apis.md](14-apis.md) | AWS 兼容面与 Admin 面 | 04 02 |
| [15-console.md](15-console.md) | 管理后台 | 14 16 |
| [16-observability.md](16-observability.md) | 事件总线、指标、追踪、审计日志 | 02 |
| [17-quotas.md](17-quotas.md) | 配额清单与作用域划分 | 02 04 |
| [18-nonfunctional.md](18-nonfunctional.md) | 性能、容量、安全、可移植性、运维 | 01 03 |
| [19-testing.md](19-testing.md) | 测试策略与验收门槛 | 全部 |
| [20-roadmap.md](20-roadmap.md) | 里程碑与完成定义 | 全部 |

进度对账见 [TODO.md](../TODO.md)。它记录「做到哪了」,
不定义「要做什么」——不是本基线的一部分,与本目录冲突时以本目录为准。

## 需求编号

每条需求有稳定 ID，格式 `<域>-<三位序号>`。ID 一经分配不得重用，
需求废弃时保留 ID 并标记状态为 `废弃`，不得删除行。

新编号取该域已出现的最大序号加 1，不得按小节内的空号回填。
按小节回填会与其他节已经用掉的号撞车，同一个 ID 就会指向两条需求。

下列整章已删除，前缀停用，不得再分配：`PROV`、`JOBS`、`RULE`。
各章里已删掉的编号同样停用，包括 Thing Type、Thing Group、标签、JITP、JITR、CRL、OCSP、WASM 授权器与 WASM sink。

| 域 | 文档 | 域 | 文档 |
| --- | --- | --- | --- |
| `PROD` | 00 | `REG` | 12 |
| `LANG` | 01 | `API` | 14 |
| `ARCH` | 02 | `UI` | 15 |
| `CLU` | 03 | `OBS` | 16 |
| `CMP` | 04 | `QTA` | 17 |
| `MQTT` | 05 | `NFR` | 18 |
| `NET` | 06 | `TST` | 19 |
| `AUTH` | 07 | `RMAP` | 20 |
| `PKI` | 08 | | |
| `SHDW` | 10 | | |

## 状态取值

| 状态 | 含义 |
| --- | --- |
| `v0.1` | 首个版本必须交付。缺任一条不得发布 v0.1。 |
| `v0.2` `v0.3` `v0.4` | 路线图内，按里程碑交付。 |
| `超集` | 刻意超出 AWS IoT Core 的行为。必须可通过配置退化。 |
| `偏离` | 与 AWS 行为刻意不同。必须在用户文档显著位置说明。 |
| `不做` | 明确排除。必须出现在 README 首屏的能力边界声明中。 |
| `待议` | 尚未决策。不得进入实现。 |
| `废弃` | 曾定义后撤销。保留以防 ID 重用。 |

## 术语

| 术语 | 含义 |
| --- | --- |
| node | 运行 `openiotd` 的实例。`node_id` 持久化于 data dir，重启不变。 |
| voter | 参与 Raft 投票与日志复制的节点。 |
| learner | 接收日志并构建本地视图，但不参与投票的节点。 |
| meta group | 强一致复制 Thing、证书、策略、成员的单个 Raft 组。 |
| shard | Shadow、retained 状态所在的固定哈希分区组。 |
| epoch | clientId 所有权的单调围栏令牌。更小的 epoch 不得修改或投递会话。 |
| ready | 节点元数据视图已追平当前提交 revision，可以接收设备流量。 |
| principal | 认证主体。可以是证书指纹或 SigV4 访问密钥。 |
| 兼容面 | `/api/aws/*`，契约冻结的 AWS 兼容 API。 |
| Admin 面 | `/api/admin/v1/*`，自有的运维 API，可自由演进。 |
