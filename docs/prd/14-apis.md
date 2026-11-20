# 14 API

## 1. 双平面

| ID | 平面 | 路径 | 契约 | 状态 |
| --- | --- | --- | --- | --- |
| API-001 | AWS 兼容面 | `/api/aws/*` | 冻结。不得为满足控制台需求修改响应形状 | v0.1 |
| API-002 | Admin 面 | `/api/admin/v1/*` | 自有，可自由演进 | v0.1 |

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-003 | 控制台只使用 Admin 面。符合 ARCH-039。 | v0.1 |
| API-004 | 兼容面响应结构变更须经 golden diff 人工确认。符合 ARCH-056。 | v0.1 |

## 2. 兼容面通用要求

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-010 | SigV4 请求验签：canonical request 构造、时钟偏移容忍、区域与服务名校验。 | v0.1 |
| API-011 | 错误形状对齐：`ResourceNotFoundException`、`ResourceAlreadyExistsException`、`InvalidRequestException`、`ThrottlingException`、`UnauthorizedException`、`ServiceUnavailableException`、`VersionConflictException`、`LimitExceededException`、`InternalFailureException`。 | v0.1 |
| API-012 | 错误须含正确的 HTTP 状态码与 `x-amzn-ErrorType` 头。SDK 的重试逻辑依赖这些。 | v0.1 |
| API-013 | 分页使用 `nextToken`。集群下 token 须编码各节点位置。符合 ARCH-040。 | v0.1 |
| API-014 | 可使用 botocore service model（Apache-2.0）作为形状来源并保留许可声明。 | v0.1 |

## 3. 控制面 action（v0.1 范围）

覆盖官方 SDK 与 CLI 实际使用的全部路径。

| ID | 分组 | action |
| --- | --- | --- |
| API-020 | Thing | `CreateThing`、`DescribeThing`、`UpdateThing`、`DeleteThing`、`ListThings` |
| API-021 | Principal 关联 | `AttachThingPrincipal`、`DetachThingPrincipal`、`ListThingPrincipals`、`ListPrincipalThings` |
| API-022 | 证书 | `CreateKeysAndCertificate`、`CreateCertificateFromCsr`、`DescribeCertificate`、`UpdateCertificate`、`DeleteCertificate`、`ListCertificates` |
| API-023 | 策略 | `CreatePolicy`、`CreatePolicyVersion`、`GetPolicy`、`GetPolicyVersion`、`ListPolicyVersions`、`SetDefaultPolicyVersion`、`DeletePolicy`、`DeletePolicyVersion`、`ListPolicies` |
| API-024 | 策略附加 | `AttachPolicy`、`DetachPolicy`、`ListAttachedPolicies`、`ListTargetsForPolicy` |
| API-025 | 端点 | `DescribeEndpoint` |

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-026 | `DescribeEndpoint` 返回本地配置端点而非 ATS 域名。`endpointType` 接受但语义本地化。 | v0.1 |

## 4. 控制面 action（路线图）

| ID | 分组 | 状态 |
| --- | --- | --- |
| API-030 | CA 证书：`RegisterCACertificate`、`DescribeCACertificate`、`UpdateCACertificate`、`DeleteCACertificate`、`ListCACertificates`、`RegisterCertificate`、`RegisterCertificateWithoutCA` | v0.2 |
| API-036 | 检索：`SearchIndex`、`GetIndexingConfiguration`、`UpdateIndexingConfiguration` | v0.3 |
| API-037 | 角色别名：`CreateRoleAlias` 等 | v0.3 |

## 5. 数据面

| ID | action | 状态 |
| --- | --- | --- |
| API-040 | `Publish` | v0.1 |
| API-041 | `GetThingShadow`、`UpdateThingShadow`、`DeleteThingShadow`、`ListNamedShadowsForThing` | v0.1 |
| API-042 | `GetRetainedMessage`、`ListRetainedMessages` | v0.1 |

## 6. Admin 面

| ID | 能力 | 状态 |
| --- | --- | --- |
| API-050 | 连接：列举（服务端筛选与分页）、详情、踢出 | v0.1（跨节点聚合待 v0.4） |
| API-051 | 事件流：SSE，支持按类型、clientId、thing、节点筛选 | v0.1 |
| API-052 | 主题抓包：WebSocket，带采样与速率上限 | v0.2 |
| API-053 | 策略干跑与历史判定回放 | v0.1 |
| API-054 | 设备时间线聚合查询 | v0.1 |
| API-055 | 集群：status、members、promote、drain、remove、transfer leadership、backup、restore、join token | v0.4 |
| API-056 | 证书：树形视图、到期清单、吊销影响预览、批量操作 | v0.2 |
| API-058 | RBAC：角色与用户（OIDC 配置随 AUTH-080 废弃） | v0.1 |
| API-059 | 指标摘要：供控制台总览页使用的聚合端点 | v0.1 |

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-060 | 扇出须支持部分失败，响应含 `partial=true` 与未响应节点清单。符合 ARCH-041。 | v0.1 |
| API-061 | Admin 面所有写操作须写审计事件，含操作者身份与请求摘要。 | v0.1 |
| API-062 | Admin 面须可整体禁用（边缘部署场景）。 | v0.1 |

## 7. 不做

| ID | 项 |
| --- | --- |
| API-070 | CloudTrail、Resource Groups、标签计费 API。审计走本地结构化审计日志。 |
| API-071 | 全部 Greengrass、IoT Analytics、IoT Wireless、Device Advisor API。 |

## 8. Admin 面的鉴权（实现期补充）

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-080 | `/api/admin/v1` **一律要求** Bearer 令牌，不提供「无鉴权」选项。该端点能吊销证书、踢掉连接、改写策略，其默认状态必须是关闭而不是敞开。 | v0.1 |
| API-081 | 未配置令牌时整个 `/api/admin/v1` **不挂载**，而不是挂载一个返回 401 的版本。未挂载时请求得到 404，攻击者无法从响应里确认这里有一个管理面。 | v0.1 |
| API-082 | `/healthz` 与 `/readyz` 始终可用且不需要令牌：探针需要它们，而它们不泄露任何信息。 | v0.1 |
| API-083 | 令牌比较须为常数时间。按字节提前返回的比较会把令牌的正确前缀长度泄露给计时攻击，而管理面令牌泄露等同于平台完全失守。 | v0.1 |
| API-084 | 鉴权失败不区分「没带令牌」与「令牌不对」。区分它们等于告诉攻击者哪一步走对了。 | v0.1 |
| API-085 | 令牌长度下限 32 字符。设下限是为了挡住 `admin`、`changeme` 这类取值。 | v0.1 |
| API-086 | 管理端点绑到非回环地址须 `acknowledge_public_exposure` 显式确认——即便已配令牌。把管理面暴露到网络上须是一个有意识的决定（与 `auth.acknowledge_insecure` 同一模式）。 | v0.1 |
| API-087 | `openiotd init` 自动生成令牌并写入 data dir（权限 600），同时打印出 `curl` 用法。安全不该以难用为代价——要求用户手工设令牌，多数人会设成 `admin`。`openiotd admin token --rotate` 可轮换。 | v0.1 |
| API-088 | 请求体中的未知字段一律报错，与配置解析同一原则（NFR-060a）。一个把 `policyName` 拼成 `policy` 的请求若被静默接受，调用方会以为绑定成功了，而设备连不上。 | v0.1 |
| API-089 | 「状态不允许」与「请求非法」须用不同状态码区分：引用未解除导致的删除失败返回 409 并在消息中列出引用者，而非 400。 | v0.1 |
| API-090 | 影子端点须经 `shadow.Service` 产出响应，形状与 MQTT 的 `/get/accepted`、REST 的 `GetThingShadow` 完全一致（SHDW-044）。直接序列化内部结构会把实现语言的字段名暴露成 API 契约，控制台也就无法复用同一套解析。 | v0.1 |

### 已落地的端点

```
GET    /api/admin/v1/node
GET    /api/admin/v1/policies            PUT/GET/DELETE /policies/{name}
GET    /api/admin/v1/things              PUT/GET/DELETE /things/{name}
GET    /api/admin/v1/things/{name}/shadow
GET    /api/admin/v1/certificates        GET/DELETE /certificates/{fp}
PUT    /api/admin/v1/certificates/{fp}/status      ← PKI-030 的 HTTP 入口
GET    /api/admin/v1/connections         GET/DELETE /connections/{clientId}
```

本轮新增：

```
POST   /api/admin/v1/authz/simulate            ← ★ 策略干跑（API-053、UI-010）
GET    /api/admin/v1/events                    ← SSE 事件流（API-051、UI-003）
GET    /api/admin/v1/overview                  ← 指标摘要（API-059、UI-020）
GET    /api/admin/v1/mqtt                     ← 测试客户端 WebSocket（API-114、UI-013）
GET    /api/admin/v1/capture                  ← 主题抓包 WebSocket（API-118、UI-055）
GET    /api/admin/v1/things/{name}/timeline   ← 设备时间线（API-121、UI-057）
GET    /api/admin/v1/session                  DELETE /session
GET    /api/admin/v1/users                    PUT/DELETE /users/{name}
PUT    /api/admin/v1/things/{name}/shadow      ← 影子写入，触发 delta（SHDW-045）
DELETE /api/admin/v1/things/{name}/shadow
GET    /api/admin/v1/things/{name}/shadows
```

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-091 | 事件流走 SSE 而非 WebSocket：事件是单向的，SSE 走普通 HTTP、自带断线重连、能被反向代理透明转发。需要双向的通道另走 WebSocket：测试客户端见 API-114，主题抓包仍是 API-052。 | v0.1 |
| API-092 | SSE 须声明 `X-Accel-Buffering: no` 并定期发送注释行心跳。代理默认缓冲响应会把事件攒住；代理与负载均衡常在 60 秒无数据时断开连接。 | v0.1 |
| API-093 | SSE 与控制台测试客户端允许令牌经查询参数传递，因为浏览器的 `EventSource` 和 `WebSocket` 都不支持自定义请求头。这是有意识的权衡：查询参数会进访问日志，而管理端点默认只绑回环地址。彻底避免它需要基于 Cookie 的会话层，随 RBAC（API-058）一并落地。其余端点走请求头。 | v0.1 |
| API-094 | 策略干跑须回传每个变量的求值结果与"如何放行"的建议。前者在判定时捕获、事后无法重建（AUTH-035）；后者是调试器区别于"打印判定结果"的地方。`ExplicitDeny` 与 `ImplicitDeny` 的建议须不同——前者加 Allow 无效，必须改那条 Deny。 | v0.1 |
| API-095 | 经 Admin 面的影子写入须真的把 delta 与 documents 发出去。只改存储不发通知会让控制台的"下发配置"变成一个假动作。 | v0.1 |
| API-114 | 控制台 MQTT 测试客户端是 `GET /api/admin/v1/mqtt` 的 WebSocket，不走设备的 `/mqtt`。握手之后双方用文本帧交换 JSON：客户端发 `subscribe`、`unsubscribe`、`publish`，服务端回 `ready`、`suback`、`unsuback`、`published`、`message`、`error`。负载与 `correlationData` 用标准 base64。一条连接最多 32 个过滤器，队列 64 条，满了就丢并计数。它不占全局订阅额度，也不按 IoT 策略放行：操作者已经过管理面令牌。QoS 接受 0、1、2。成功的订阅、退订和发布各记一行审计，目标只含过滤器或主题。失败的命令不记。 | v0.1 |

跨节点扇出（API-060）仍在后续里程碑。主题抓包、时间线与 RBAC 的实现期决定见第 12 节。

## 9. 兼容面的第一刀（实现期补充）

这一刀覆盖 Thing 的五个 action 和 `DescribeEndpoint`。证书、策略与
Principal 见第 10 节。

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-100 | 兼容面挂在管理端口的 `/api/aws`，默认不挂载。未挂载时请求得到 404。鉴权是 SigV4，服务名 `iot`，与管理面的 Bearer 令牌无关。boto3 的 `endpoint_url` 要包含这个前缀。 | v0.1 |
| API-101 | 本切片的 `nextToken` 是本节点名字序的上一页末尾，对调用方不透明。集群下把各节点位置编进 token（API-013）还没做，这枚 token 只在本节点有效。 | v0.1 |
| API-102 | `CreateThing` 在设备已存在时返回 `ResourceAlreadyExistsException`。更新走 `UpdateThing`。`expectedVersion` 省略则不检查；给出且与当前版本不同则返回 `VersionConflictException` 并且不写入。版本从 1 起，每次更新加一。 | v0.1 |
| API-103 | `thingTypeName`、`billingGroupName`、`removeThingType` 这些 v0.2 才有的字段，出现非空值就拒绝，而不是丢掉。丢掉会让调用方以为类型已经建上。 | v0.1 |
| API-104 | 控制面签名失败一律是 `UnauthorizedException` 与 401。密钥不存在、签名不对、过期、区域或服务名不对，响应相同。 | v0.1 |

## 10. 证书、策略与 Principal（实现期补充）

路径与 botocore `iot` `2015-05-28` 的 `requestUri` 一致，挂在 `/api/aws` 下。
`ListPolicies`、`ListCertificates` 用 `marker` / `pageSize` / `nextMarker`。
Principal 列举用 `nextToken` / `maxResults`。`x-amzn-principal` 携带证书 ARN。

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-105 | 省略 `pageSize` 或 `maxResults` 时每页 25 条，合法范围 1 到 250。`nextMarker` 与 `nextToken` 是本节点上一页的末尾，空页不返回这个字段。`isAscendingOrder=false` 拒绝：静默改成升序会让调用方按降序翻页时漏掉或重复记录。 | v0.1 |
| API-106 | 每份策略最多五个版本。`CreatePolicy` 建立版本 `1` 并设为默认。`CreatePolicyVersion` 在 `setAsDefault=false` 时只保存文档，热视图仍用原来的默认版。默认版本不能删除。管理面的 `PutPolicy` 改写默认版正文，不另占版本号，否则几次保存就会把五个名额用完。 | v0.1 |
| API-107 | 每张证书、每台设备各附加一份策略。已经附了另一份时 `AttachPolicy` 返回 `LimitExceededException`。卸下未附加的策略返回 `ResourceNotFoundException`。非空 `tags` 与 `recursive=true` 拒绝：标签和设备组继承都还没有，丢掉这些字段会让调用方以为已经生效。 | v0.1 |
| API-108 | 一张证书只绑定一台设备。`AttachThingPrincipal` 覆盖原绑定。显式 `thingPrincipalType=NON_EXCLUSIVE_THING` 拒绝；不传该参数时按覆盖语义执行，因为 SDK 默认不发送这个参数。`ListPrincipalThings` 因此最多返回一台设备。 | v0.1 |
| API-109 | `CreateKeysAndCertificate` 与 `CreateCertificateFromCsr` 使用内嵌 CA。未启用 PKI 时返回 `ServiceUnavailableException`。私钥只出现在签发响应里，登记内容只有证书正文。`certificateId` 是证书 DER 的 SHA-256。密钥对的通用名由本节点生成，因为这个 action 不接收主题；CSR 的主题只在签发策略声明采纳 CSR 时使用。证书可以先登记再 `AttachPolicy`。活动证书，或仍关联设备、策略的证书，删除时须带 `forceDelete=true`。 | v0.1 |

## 11. 数据面（实现期补充）

路径与 botocore `iot-data` `2015-05-28` 的 `requestUri` 一致，挂在 `/api/aws` 下。签名服务名是 `iotdata`。`ListNamedShadowsForThing` 的 URI 本身以 `/api/` 开头，因此完整路径是 `/api/aws/api/things/shadow/ListNamedShadowsForThing/{thingName}`。

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-110 | 数据面与控制面共用 `/api/aws`，但签名服务名必须是 `iotdata`。签成 `iot` 与签名错误一样拒绝。这一层只校验签名，不按 IoT 策略判断这个密钥能不能发布或读写影子。 | v0.1 |
| API-111 | 影子读写走 `shadow.Service`，成功响应是文档本身。`Update` 与 `Delete` 之后把服务层产出的 delta 与 documents 发出去。影子的 404、409、413、429、500 分别映射到 `ResourceNotFoundException`、`ConflictException`、`RequestEntityTooLargeException`、`ThrottlingException`、`InternalFailureException`。 | v0.1 |
| API-112 | `Publish` 的正文是原始负载。`qos` 省略为 0，只接受 0 或 1。`retain` 省略为 false。空负载加 retain 清除该主题的保留消息。负载上限 128 KiB。MQTT 5 的 user properties、payload format、correlation data、contentType、responseTopic、messageExpiry 出现就拒绝。 | v0.1 |
| API-113 | `GetRetainedMessage` 的 payload 是 base64。`lastModifiedTime` 取写入时注入的时钟，没有写入时间则省略。列举的 `nextToken` 是本节点上一页的末尾主题名——**不必跨节点扇出**（契约 12 那套机制这里用不上）：每个节点都保存全量 retained（MQTT-122、MQTT-126），所以问任意一个节点就是全集群的视图，游标在各节点上也指同一个位置。代价是那份视图的新鲜度是一次广播窗口（MQTT-125）。命名影子 `pageSize` 上限 100，保留消息 `maxResults` 上限 200，省略时都是 25。空的 `nextToken` 不出现在响应里。 | v0.1 |

## 12. CA、检索、别名、抓包与登录（实现期补充）

自有 CA、检索和角色别名挂在 `/api/aws`，签名服务名是 `iot`。抓包、用户和时间线挂在管理面。自动注册相关字段一律拒绝：那条开通路径已经从产品里拿掉，悄悄忽略会让调用方以为配上了。

| ID | 需求 | 状态 |
| --- | --- | --- |
| API-115 | `GetRegistrationCode` 返回本节点稳定的注册码。`RegisterCACertificate` 要求验证证书由该 CA 签发，且使用者通用名等于注册码。`setAsActive` 为假时状态是 `INACTIVE`。`allowAutoRegistration`、`registrationConfig`、`newAutoRegistrationStatus`、`removeAutoRegistration` 返回 `InvalidRequestException`，文案是「不支持自动注册」。`RegisterCertificate` 要求 CA 已登记且为 `ACTIVE`。`RegisterCertificateWithoutCA` 不接收 CA 正文。两条注册都先按证书注册速率判定，再解析证书。 | v0.1 |
| API-116 | 检索默认关闭。关闭时 `SearchIndex` 返回 `InvalidRequestException`，设备列举仍按名前缀扫描。语句是空白分隔的 AND，字段只接受 `thingName`（可用 `*` 结尾表示前缀）、`attributes.<名>`、`connectivity.connected`、`shadow.reported.<路径>`、`shadow.desired.<路径>`。对应索引没开时拒绝该字段。每页默认 25，最多 100，`nextToken` 是上一页最后的设备名。影子直接读存储，不走会消耗影子接口速率的读取。 | v0.1 |
| API-117 | 角色别名是名字到 `roleArn` 的对照。名字 1 到 128，字符集 `[A-Za-z0-9_:-@]`。`roleArn` 以 `arn:` 开头，长度 8 到 2048。`credentialDurationSeconds` 省略时为 3600，否则须在 900 到 43200。创建已存在的名字返回 `ResourceAlreadyExistsException`。`GET .../credentials` 在别名存在时返回 `ServiceUnavailableException`，文案是「本节点不签发云临时凭证」，响应里没有访问密钥。 | v0.1 |
| API-118 | 主题抓包是 `GET /api/admin/v1/capture` 的 WebSocket，只给 `admin`。它不进入路由表，因此不占全局订阅额度，也不加入共享订阅组。客户端发 `watch` 与 `unwatch`，服务端回 `watching`、`unwatched`、`message`、`error`。一条连接最多 8 条普通过滤器，不接受 `$share`。默认每秒 50 条，上限 200。队列 64 条，满了就丢并计数。负载用标准 base64。没有设备订阅者时，经过本节点的发布仍然可见。 | v0.1 |
| API-119 | 管理面角色是 `reader`、`operator`、`admin`，不复用 IoT Policy。`reader` 只能 GET 和 HEAD，外加 `POST /authz/simulate`。`operator` 可以做日常写操作。`/users`、`/oidc` 的配置和 `/capture` 只给 `admin`。不够格时 HTTP 403，文案是「当前角色不能做这件事」。引导令牌是 `admin`，操作者名字是 `admin`。额外用户放在数据目录的 `users.json`，模式 0600，只存令牌的 SHA-256。比较覆盖全部用户。登录会话是内存里的 cookie `oiot_session`，HttpOnly、SameSite=Lax，明文管理端口不标 Secure，8 小时过期，重启即失效。任何已登录角色都可以 `DELETE /session`。 | v0.1 |
| API-120 | ~~OIDC 的四个端点（`GET /oidc`、`/oidc/start`、`/oidc/callback`、`PUT /oidc`）与 `oidc.json`。~~ 整条路径随 AUTH-080 移除，端点现在 404。`DELETE /session` 一并下线：它销的是 OIDC 签发的会话 cookie，而令牌登录没有服务端会话。 | 废弃（AUTH-080） |
| API-121 | `GET /things/{name}/timeline` 从事件日志取该设备的事件，新的在前。`limit` 默认 50，最大 200。`beforeSeq` 只返回更早的序号。没有事件时 `events` 是空数组。 | v0.1 |
| API-122 | `GET /connections/{clientId}` 带上该连接的订阅（过滤器、共享组名、QoS）和出站在途（报文标识、主题、QoS、阶段）。没有时是空数组。在途不含负载和属性。 | v0.1 |
| API-123 | `GET /overview` 的 `online` 是在线连接采样，按时间从旧到新，每项 `t`（毫秒）和 `n`（连接数）。每次读取追加一点，最多 120 点，空时是数组。进程内缓冲，重启即空。 | v0.1 |
| API-124 | 兼容面的异常表加 `AccessDeniedException`（403），用于权限不足（AUTH-083）。API-011 的九个异常名一个不少，这是第十个。`x-amzn-ErrorType` 与 `__type` 同样要带——botocore 按它把响应解成对应的异常类，名字对不上调用方的 `except` 分支就接不住。 | v0.1 |
| API-125 | `GET /awskeys` 列出兼容面的 access key，`POST /awskeys` 发一把（请求体 `role`、`note`、`expires`，响应里的 `secret` 只出现这一次）。列表**不得**带任何密钥材料：把列表直接序列化成响应是最自然的写法，所以存储层要让密文不在那个结构里，而不是靠每个调用点记得去掉它。 | v0.1 |
| API-126 | `PATCH /awskeys/{id}` 只改停用状态，请求体必须显式给出 `disabled`。缺字段要报 400 而不是当成 `false`：一次漏字段的请求会把刚停掉的钥匙重新放出来。`DELETE /awskeys/{id}` 删掉一把，删不存在的是 404。两者都写审计（动作名 `EnableAWSKey`、`DisableAWSKey`、`DeleteAWSKey`，目标是 id），审计里不得出现密钥材料。 | v0.1 |
| API-127 | 这组端点只给 `admin`。列表本身不含密钥材料，但它说出了有哪些钥匙、各是什么角色、谁还没过期——那是一张攻击面地图；而发一把 `admin` 的 access key 等于把整个控制面交出去，不该由 `operator` 做。（AUTH-092 之前这里还有一句「口令缺失导致发不出钥匙时返回 503」，口令去掉之后任何节点都发得出钥匙，那条分支与它的 503 一并移除。） | v0.1 |
| API-128 | `GET /api/admin/v1/cluster/members` 的响应须给出当前 `leader`（节点 ID）。原先它只列成员与多数派票数，看不出"现在谁在接写入"——而那是故障时第一个要知道的事，也是运维判断"写入失败是因为没有 leader 还是因为没有多数派"的唯一依据。单机返回本节点自己。这个字段随选举变化，不是配置；集群验收的杀 leader 用例（TST-147）也靠它查出要杀谁。 | v0.1 |
| API-129 | `GET /overview` 的 `quotas` 须返回全部生效配额，不得过滤统计值全为零的定义。配额从启动时就已存在；按命中情况过滤会把“尚未统计”错误呈现为“没有这项配置”。`allowed`、`rejected` 与 `current` 保留数值零，由控制台区分尚未统计与统计后的零值。 | v0.1 |

