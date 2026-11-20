# 04 兼容性边界

## 1. 原则

| ID | 原则 | 状态 |
| --- | --- | --- |
| CMP-001 | 兼容性声明须公开且精确。含糊的兼容性声明比缺少功能更损伤可信度。 | v0.1 |
| CMP-002 | AWS 自身偏离 OASIS MQTT 规范处，默认遵循规范，并提供 `aws_strict` 配置退化为 AWS 行为。 | v0.1 |
| CMP-003 | 超出 AWS 的行为须可通过配置关闭，以保证严格兼容场景下的行为一致。 | v0.1 |
| CMP-004 | 兼容基线为 AWS IoT Core 文档，核对日期须记录在本文件并随核对更新。 | v0.1 |

兼容基线核对日期：2026-10-26。
完整的逐项矩阵（约 200 条）见 `../blueprint.html`，本文件为规范性摘要与决策记录。

## 2. 刻意偏离 AWS 的决策

这些是最容易让移植方踩坑的差异，须在用户文档显著位置列出。

| ID | 项 | AWS 行为 | 本项目行为 | 状态 |
| --- | --- | --- | --- | --- |
| CMP-010 | QoS 2 | 不支持。无 PUBREC/PUBREL/PUBCOMP。请求 QoS 2 时不回 PUBACK/SUBACK | 完整实现。`aws_strict` 下退化为 AWS 行为，含静默不回 ACK | 超集 |
| CMP-011 | `sensor/#` 是否匹配 `sensor` | 不匹配。违反 MQTT 规范对 `#` 匹配父层级的要求 | 按规范匹配。`aws_strict` 下关闭 | 偏离 |
| CMP-012 | 通配符订阅接收 retained | 不支持，须精确匹配主题 | 按规范投递。`aws_strict` 下关闭 | 偏离 |
| CMP-013 | DUP 标志 | 不支持。重投可能换 packet ID 且不置 DUP | 按规范置 DUP 并保持 packet ID | 偏离 |
| CMP-014 | MQTT 5 server redirect 0x9C/0x9D | 不支持 | 支持。集群再均衡与滚动升级的唯一优雅手段，协议支持属 Day-1 | 超集 |
| CMP-015 | AUTH 包与增强认证 | 不支持 | 支持 SCRAM-SHA-256 | 超集 v0.3 |
| CMP-016 | 吊销后断开在用连接 | 不保证立即断开 | 立即断开，上界为 `max_view_staleness` | 超集 |
| CMP-017 | 消息顺序保证 | 不保证消息与 ACK 顺序 | 同一发布者到同一主题单节点内保序；跨节点保批内有序。保证边界须明确声明 | 超集 |
| CMP-018 | 共享订阅用于 MQTT 3.1.1 | 支持，属 AWS 扩展 | 同样支持 | 对齐 |
| CMP-019 | 策略干跑与解释 | 无。只能经日志事后排查 | 提供判定结果、命中语句下标、策略变量求值全过程 | 超集 |
| CMP-020 | Shadow 版本历史 | 不保留 | 保留可配置窗口的版本链，支持 diff 与回放 | 超集 v0.3 |

## 3. 范围分级摘要

| 域 | v0.1 | 路线图 | 超集 | 不做 |
| --- | --- | --- | --- | --- |
| MQTT 协议（05） | 3.1.1 与 5.0 全功能、QoS 0/1、retained、LWT、会话过期、全部 MQTT5 属性、共享订阅 | 共享订阅排队 v0.4 | QoS 2、AUTH、server redirect、DUP、规范化通配符 | MQTT over QUIC |
| 传输（06） | 8883 mTLS、443 ALPN `x-amzn-mqtt-ca`、443 WSS SigV4、SNI、TLS 1.2/1.3 | HTTPS 发布 v0.2、自定义认证 v0.3、自定义域名 v0.3 | 握手限速与重连退避 | VPC 端点 |
| 认证授权（07） | X.509、IoT Policy 全部策略变量、SigV4 | 角色别名 v0.3 | 策略调试器 | Cognito、完整 IAM、STS |
| PKI（08） | 内置 CA 签发、CSR 签发、证书状态机、吊销即时断连 | 注册自有 CA v0.2、轮换 v0.3 | PKCS#11/KMS 密钥后端、到期预警、吊销影响预览 | 跨账户证书转移 |
| Shadow（10） | 经典与命名影子全语义 | — | 版本历史与回放 v0.3 | — |
| Registry（12） | Thing CRUD、principal 关联 | 检索索引 v0.3 | — | Billing Group |
| API（14） | SigV4 验签、错误形状、分页、约 40 个 action、数据面全部 | CA 注册 v0.2、检索与角色别名 v0.3 | Admin 面 | CloudTrail、Resource Groups |
| 事件（16） | 连接与订阅生命周期 | 注册表事件 v0.2 | Prometheus、OTel、结构化审计、设备时间线 | CloudWatch 集成 |

## 4. 明确排除

须出现在 README 首屏的能力边界声明中。

| ID | 排除项 | 替代 |
| --- | --- | --- |
| CMP-030 | Greengrass | 本项目单二进制可作为边缘节点部署 |
| CMP-031 | IoT Analytics / Events / SiteWise / TwinMaker / FleetWise | 各为独立产品，由用户自有系统承担 |
| CMP-032 | IoT Wireless（LoRaWAN / Sidewalk） | 无 |
| CMP-033 | Device Defender 的 ML 异常检测 | 指标接收端见 16-observability.md，检测交由用户系统 |
| CMP-034 | Secure Tunneling | `$aws/things/{t}/tunnels/notify` 仅保留占位不报错 |
| CMP-035 | Device Location | 无 |
| CMP-036 | Device Advisor / Fleet Hub | Fleet Hub 能力由本项目控制台覆盖 |
| CMP-037 | SiteWise 资产模型主题 | 无 |
| CMP-038 | Cognito / 完整 IAM / STS | SigV4 验签兼容，身份体系为本地 IAM-lite |
| CMP-039 | AWS 生态规则 action（lambda、dynamodb、kinesis、firehose、sns、sqs、step-functions、timestream、opensearch、cloudwatch、iot-analytics、iot-events、iot-sitewise、salesforce、location） | 不提供这些 action |
| CMP-040 | Commands（`$aws/commands/...`） | 不实现 |

## 5. 不追求的兼容性

| ID | 说明 |
| --- | --- |
| CMP-042 | `DescribeEndpoint` 返回本地配置端点而非 ATS 域名。`endpointType` 参数接受但语义本地化。 |
| CMP-043 | 配额默认值对齐 AWS 以保证行为可预期，但全部可配。自建场景无理由继承云厂商的商业分层。 |
