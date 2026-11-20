# 00 产品定位

## 1. 一句话定位

一个 Go 实现、Apache-2.0 许可、可集群的 MQTT broker 与设备管理平面，
线路与 API 层面兼容 AWS IoT Core，设备注册、证书签发与吊销、影子全部内建，
不需要回调服务。交付物是单个静态二进制。

## 2. 需求

| ID | 需求 | 状态 |
| --- | --- | --- |
| PROD-001 | 交付物为单个静态二进制，含 broker、控制面、管理后台、内嵌共识。默认配置零外部依赖。 | v0.1 |
| PROD-002 | 官方 AWS IoT Device SDK 示例不修改代码即可连接并完成 pubsub、shadow 场景。 | v0.1 |
| PROD-003 | `aws` CLI 与 `boto3` 指向本服务后可完成注册、签发、附策略、连接、吊销全流程。 | v0.1 |
| PROD-004 | 认证、鉴权、注册、影子在进程内完成，不依赖 webhook 或外部函数。 | v0.1 |
| PROD-006 | 单机与集群使用同一套代码路径。单机是副本数为一的集群。 | v0.1 |
| PROD-007 | 能力边界声明（做什么、不做什么、哪里与 AWS 刻意不同）出现在 README 首屏。 | v0.1 |
| PROD-008 | 许可为 Apache-2.0。不采用 BSL、SSPL、ELv2 或其他 source-available 许可。 | v0.1 |
| PROD-009 | 交叉编译支持 linux/amd64、linux/arm64、darwin/arm64，且不依赖 CGO。 | v0.1 |

## 3. 目标用户

| 画像 | 诉求 | 本项目提供 |
| --- | --- | --- |
| 从 AWS IoT Core 迁出者 | 已有设备固件基于 AWS Device SDK，不想改固件 | 线路与 API 兼容，固件零改动 |
| 数据不出境/内网部署者 | 合规或网络隔离要求自建 | 单二进制自建，无云依赖 |
| EMQX 集群许可受限者 | EMQX 5.9 起集群需商业授权 | Apache-2.0 且可集群 |
| 自建 IoT 平台团队 | 不想为影子、证书管理各写一个服务 | 全部内建 |
| 边缘网关部署者 | 需要在受限硬件上跑同一套制品 | 同一二进制，单节点模式 |

## 4. 竞争格局

截至 2026 年，"Apache-2.0 许可、可集群、生产级 MQTT broker" 是一个空位。

| 项目 | 许可 | 集群 | 平台能力 | 评估 |
| --- | --- | --- | --- | --- |
| EMQX ≥5.9 | BSL 1.1 | 需商业授权 | broker + hook | 附加使用许可仅允许单节点免费生产使用。v5.8 及更早仍为 Apache-2.0 但已冻结。 |
| Mosquitto | EPL/EDL | 无 | 无 | 单节点轻量 broker。 |
| VerneMQ | Apache-2.0 | 有 | 无 | 许可与集群都符合，但维护活跃度长期偏低。 |
| HiveMQ CE | Apache-2.0 | 企业版 | 无 | 与 EMQX 同一分层模式。 |
| NanoMQ | MIT | 弱 | 无 | 边缘定位，极轻量。 |
| RabbitMQ MQTT | MPL-2.0 | 有 | 无 | 通用消息中间件，非 IoT 形状，单连接开销大。 |
| rmqtt | Apache-2.0 | 有 | 无 | Rust 实现，集群支持 Raft 或广播两种模式。 |
| Mochi | MIT | 无 | 无 | Go 实现，单节点。 |
| ThingsBoard | Apache-2.0 | 有 | 有 | 平台能力完整但自成体系，不兼容 AWS API，依赖 JVM + Postgres + Kafka + Redis。 |
| Magistrala | Apache-2.0 | 有 | 有 | Go 微服务架构，不兼容 AWS API。 |

本项目同时占据两个位置：开源可集群 broker，以及开源的 AWS IoT Core 兼容平台。
后者目前没有竞品。

## 5. 相对 EMQX 的诚实定位

| 维度 | 评估 |
| --- | --- |
| 单节点连接密度 | 劣势。EMQX 公布 23 节点 1 亿连接（约 4.3M/节点，接近空闲）。本项目规划 10–50 万/节点，TLS 终结外移可提升。 |
| 单节点吞吐 | 大致相当。EMQX 在热路径有长期优化积累，不宣称领先。 |
| 尾延迟 | 劣势。BEAM 的每进程堆与抢占式调度是运行时特性。可通过工程手段接近，非免费获得。 |
| broker 内核正确性 | 劣势，且需 2–3 年加真实生产用户才能接近。缓解手段见 19-testing.md 的确定性模拟测试。 |
| 集群架构 | 潜在优势。EMQX 的全局订阅路由表在百万级 filter 下有天花板；本项目按 thingName 哈希路由设备域主题，仅复制通配符订阅。 |
| 部署与运维 | 优势。单静态二进制对 OTP release；无 EPMD 与分布式 cookie。 |
| 协议广度 | 劣势且不追。EMQX 有 CoAP/LwM2M/MQTT-SN/STOMP 等网关。AWS IoT Core 本身仅 MQTT 与 HTTPS，相对兼容目标不构成缺口。 |
| 平台能力 | 优势。影子、注册、证书生命周期、AWS API 兼容在 EMQX 中均需自建。 |
| 贡献者可达性 | 优势。Erlang 代码库的贡献者池极小。 |

## 6. 非目标

| ID | 非目标 | 理由 |
| --- | --- | --- |
| PROD-010 | 不做 Greengrass 等边缘运行时 | 独立产品量级。本项目单二进制本身即可作为边缘节点。 |
| PROD-011 | 不做 IoT Analytics / Events / SiteWise / TwinMaker / FleetWise | 各为独立产品。分析由用户自有系统承担。 |
| PROD-012 | 不做 LoRaWAN / Sidewalk 等无线网络服务 | 需 LNS 实现与运营商对接。 |
| PROD-013 | 不做 Device Defender 的 ML 异常检测 | 指标接收端见 16-observability.md，检测交由用户系统。 |
| PROD-014 | 不做 Secure Tunneling | 需独立隧道中继服务。 |
| PROD-015 | 不做 Cognito、完整 IAM、STS | 仅兼容 SigV4 验签，身份体系为本地 IAM-lite。 |
| PROD-016 | 不做 MQTT 以外的设备协议网关 | 与兼容目标无关，且会严重放大范围。 |
| PROD-017 | 不追求单节点连接密度对标 EMQX | 目标用户的实际规模由集群横向扩满足。 |

## 7. 许可与法律边界

| ID | 要求 | 状态 |
| --- | --- | --- |
| PROD-018 | 代码许可 Apache-2.0。贡献者协议采用 DCO，不用 CLA。 | v0.1 |
| PROD-019 | 不复制 AWS 文档原文。API 字段语义可实现，描述文字须自行撰写。 | v0.1 |
| PROD-020 | 可使用 botocore 的 service model（Apache-2.0）作为 API 形状来源，须保留其许可声明。 | v0.1 |
| PROD-021 | 对外表述使用 "AWS IoT Core–compatible API"。不得暗示 AWS 背书或隶属关系。 | v0.1 |
| PROD-022 | 不使用 AWS 商标作为项目名、域名或 logo 元素。 | v0.1 |
