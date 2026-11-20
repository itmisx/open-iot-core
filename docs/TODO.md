# 实现进度与待办

本文件是 `prd/` 到代码的**对账表**。它记录「做到哪了」，不定义
「要做什么」——**不是需求基线的一部分**。每一项都指回具体需求 ID;
状态只反映**已验证的事实**,不反映意图。

与需求基线冲突时，以 `prd/` 为准，并修正本文件。放在基线之外是
刻意的：进度会每天变，需求不该。

## 状态约定

| 记法 | 含义 |
| --- | --- |
| `[x]` | 已实现**且**有测试覆盖，门禁绿灯 |
| `[~]` | 部分实现。必须在条目里写明差什么，不得笼统标为完成 |
| `[ ]` | 未开始 |

`[~]` 不是 `[x]` 的近似。一项只要还差任何可观察的行为，就是 `[~]`——
把它记成 `[x]` 会让这张表失去唯一的用途。

## 核对方式

状态不靠记忆填写。每次更新须对着代码确认，确认手段记在条目的「依据」里:
存在性(文件/符号)、行为(测试名)、或端到端验证(实际请求与输出)。

最近一次全量核对:2026-10-27。核对脚本思路见本文件末。
最近一次真实设备验证:2026-10-27(AWS IoT Device SDK for Python)。
最近一次 `make gate` 全绿:2026-10-29。**当天它先红了三次**:
`internal/broker/will.go` 没 gofmt、订阅树的基线比较违反 ARCH-052
(见第 4 节 TST-130)、整体覆盖率 84.7% 低于门槛。
前两条是机械问题，第三条说明「全部 `[x]`」与「门禁绿灯」在这张表里
已经脱钩了一段时间——`[x]` 的定义里就写着门禁绿灯。

非门禁套件的最近一次实跑(都在 2026-10-29):
设备 SDK 18/18(`tests/device-sdk`)、控制面 SDK 对照(`tests/aws-sdk`)、
浏览器 37/37(`tests/console-e2e`)、三进程集群验收(`tests/cluster`)、
教程示例(`tests/examples`，Java 那份待实跑)。

---

## 1. v0.1 可用内核

目标(RMAP-005、RMAP-006):**能跑真实设备、能签证书、能吊销、能看状态**。

### 1.1 入口与节点

- [x] `openiotd` 可执行入口,YAML 配置加等价 flags — `internal/config`(覆盖率 95.7%)
- [x] data dir 与 manifest,原子写加 fsync — `internal/node/manifest.go`
- [x] 节点身份,bootstrap 冲突检测(CLU-031) — `internal/node/identity.go`
- [x] `/healthz` 与 `/readyz`,分组件就绪度 — `internal/node/readiness.go`
- [x] 优雅关闭，关闭顺序有测试钉住 — `TestShutdownIsPrompt`
- [x] 后台协程由 shutdown 自身叫停，不依赖外部 ctx(NFR-065b) — `Server.bgCtx`

### 1.2 协议与传输

- [x] MQTT 3.1.1 与 5.0 — `internal/mqtt`(覆盖率 98.0%)
- [x] 主题校验、匹配、无指针订阅树 — `internal/mqtt/topic`
- [x] 8883 mTLS — `internal/broker/listener.go`,`mtls_test.go` 走真实 TLS
- [x] 443 ALPN `x-amzn-mqtt-ca`
- [x] QoS 0/1/2
- [x] 保留消息，版本计数器跨重启不回退(MQTT-109) — `Router.SeedVersion`
- [x] 共享订阅 `$share/`
- [x] **遗嘱(MQTT-060、MQTT-063、MQTT-064)** — 原先只在 MQTT 5 的
      DISCONNECT reason 0x04 时发出，连接异常断开与保活超时都不发,
      而那正是遗嘱存在的理由。已在 `conn.close` 统一补发
      (`flushWill`),判据是「遗嘱是否已了结」而不是原因码——对端掐断
      走的 `closeGraceful` 与正常断开拿到的原因码相同。门禁
      `TestWillPublishedOnAbruptDisconnect`(已验证去掉修复即失败);
      真机三条路径实测：掐断、保活超时、0x04 都发，正常 DISCONNECT 不发
- [x] **Will Delay Interval(MQTT-061、MQTT-065…067)** — 原先
      `Will.DelaySeconds` 解析后无人读取。现在延迟取
      `min(延迟间隔, 会话过期间隔)`,这个取小值让「会话先结束」那一半
      不需要过期轮询器；窗口内重连即取消(`cancelWill`,挂在抢占所有权
      之后);0x04 同样走延迟。定时器经 `Options.AfterFunc` 注入,
      测试用假时钟驱动——冻结时钟下真实定时器只能靠 sleep 验,
      而「延迟 3600 秒」根本没法验。门禁三条
      (`TestWillDelay*`,均已验证去掉对应实现即失败),真机两条
- [x] **会话在线期间以运行时为权威(MQTT-105…107)** — 接管接过被抢占
      连接内存里的订阅集而不回存储读；离线队列不重放；不续上会话时清掉
      路由表里的残留订阅。详见第 4 节
- [x] MQTT 5 server redirect(`UseAnotherServer` 加 `ServerReference`)
      — WebSocket 连接时长到期且配置了 `redirect_to` 时发送 `0x9C` 并带上 Server Reference（NET-102，`TestWebSocketLifetimeRedirects`）。没有目标时只关连接。集群再均衡时的重定向仍随阶段 B

### 1.3 安全与鉴权

- [x] X.509 认证 — `internal/broker/auth.go`
- [x] IoT Policy 求值含全部策略变量 — `internal/authz`(覆盖率 97.8%)
- [x] 显式 Deny 优先，扫完全部语句才能放行(AUTH-021)
- [x] 变量求值失败即拒绝，不退化为空串匹配(AUTH-036)
- [x] 不支持的策略字段显式拒绝，不静默忽略
- [x] 多种拒绝原因对外同码，防探测(AUTH-067)
- [x] 起步策略含设备私有业务分支，并有测试断言放行范围(AUTH-068…071、AUTH-064、AUTH-065)
- [x] **遗嘱主题过发布授权(MQTT-068、MQTT-069)** — 遗嘱主题此前从不判
      授权，设备可以把遗嘱设到别的设备的命令主题上让服务端代发。旧行为下
      这个缺口够不着(遗嘱只在客户端主动要求时才发),补上「掉线也发」
      之后才真正打开，两件事一起做的。越权遗嘱丢弃而不拒连(与 AWS 一致),
      但产出带完整轨迹的拒绝事件，不静默。判定在连接终止时做——延迟发布
      时主体已经不在。门禁 `TestWillTopicRequiresPublishPermission`,
      真机一条(含「自己名下的仍正常发出」的反向确认)

### 1.4 PKI

- [x] 内嵌 CA 签发与 CSR 签发 — `internal/pki`
- [x] 证书状态机 — `Registry.SetCertificateStatus`
- [x] 吊销即时断连，节点不重启(PKI-030) — `TST-110`
- [x] 根 CA 私钥不落盘；设备私钥只在签发响应里出现一次(PKI-010)
- [x] ~~文件密钥库强制口令，无明文存储选项~~ — **已撤销**（AUTH-092）：
      口令整条去掉，私钥以 PEM 落盘，保护只有 0600
- [x] **私钥 PEM 编码改为设备 SDK 吃得下的格式(PKI-055…057)** —
      原先一律 PKCS#8,AWS IoT Device SDK(awscrt)在 Apple 平台上
      读不了 EC 私钥，报 `AWS_IO_FILE_VALIDATION_FAILURE`,错误信息
      完全指不到密钥编码。现在 ECDSA 发 SEC1、RSA 发 PKCS#1、
      Ed25519 仍 PKCS#8;读取侧三种都收，旧私钥不失效
      (`TestParsePrivateKeyPEMStillAcceptsPKCS8`)。门禁断言 PEM 块
      类型本身(`TestPrivateKeyPEMFormatIsSDKReadable`)——只断言
      「能解析回来」钉不住，那正是原来漏掉它的原因

### 1.5 平台功能

- [x] Device Shadow 经典与命名 — `internal/shadow`(覆盖率 97.0%)
- [x] 逐叶子 metadata 时间戳，六条合并规则
- [x] 版本号单调,409 冲突
- [x] `ApplyCommand` 与 MQTT 路径共用同一份语义
- [x] 经 REST 改 desired 同样触发 delta 与 documents(SHDW-045)

### 1.6 注册表

- [x] Thing CRUD — `internal/registry`(覆盖率 93.8%)。
      2026-10-29 之前是 60.5%:ca.go、search.go、alias.go、principal.go
      这批代码进来时没带包内测试，而 92.9% 那个数字是它们进来之前的。
      补齐的是四块：角色别名、自有 CA 的生命周期、策略版本与设备版本
      （API-102、API-106）、策略附加与 `ListPolicyTargets`（API-107）。
      检索（search.go）与 principal.go 此前包内 0%,现在分别是 99.3% 与 100%
- [x] 证书与设备绑定(签发时),`CertificatesOfThing` 反查
- [x] 单写路径，热视图先于冷层
- [x] 属性槽位投影 — `projectAttrs`、`SyncAttrSlots`
- [x] **principal 关联操作**(REG-060 至 REG-065) —
      `PUT/DELETE /things/{name}/principals/{fp}`、
      `GET /things/{name}/principals`、`GET /certificates/{fp}/things`
- [x] **改绑或解绑须断开活连接**(PKI-109) — 这是本项之前不存在的
      安全语义，已实测复现过缺口：改绑后旧连接仍以旧设备的身份订阅成功。
      `Change` 增加 `PrevThing`,`RequiresDisconnect` 覆盖该转换
- [x] 幂等绑定不得断连 — 否则重复执行的配置脚本会周期性踢掉全网设备
- [x] 断连判定的真值表测试(13 个分支) — `TestRequiresDisconnectTruthTable`

### 1.7 持久化

- [x] 冷层键值存储 — `internal/store`
- [x] 会话落盘与重启恢复 — `internal/store/session.go`
- [x] 保留消息落盘与重启恢复 — `internal/store/retained.go`
- [x] 影子落盘，写入 `sync=true`
- [x] 重启后影子一致且版本继续递增 — `internal/server/server_test.go:1396`
- [x] **修正存储选型的记述** — `01-language-runtime.md` 的 LANG-021 修订
      本来就记着这件事；补齐的是其他章节(roadmap 2.1 节、ARCH-065、
      12-registry、03-cluster)与两处代码注释。`RMAP-023` 与 `LANG-014`
      的「锁定 Pebble」**不改** —— 它们说的是 dragonboat 的 Raft 日志
      后端，与冷层选型无关，只补了一句把两者分清
- [x] **ARCH-065 的隔离规则曾对实际在用的引擎不生效** — 规则只拦
      `github.com/cockroachdb/`(Pebble),不拦 `go.etcd.io/bbolt`。
      已实测复现：在 `internal/registry` 直接 import bbolt,门禁照常通过。
      两条分开写(理由不同，合并后违反报告说不清违反了哪条)
- [x] 新增「import 规则表过期检测」—
      `TestEveryDirectDependencyIsAccountedFor` 要求每个直接依赖要么被
      隔离、要么在豁免名单里写明理由;
      `TestImportRuleDenyPrefixesAreEffective` 要求每条 deny 前缀真的
      能拦住东西(防拼错导致静默失效)。两者都已验证能抓到原 bug

### 1.8 Admin API

- [x] 24 个端点:node、policies、things、certificates、connections、shadow、
      authz/simulate、events、overview — `internal/server/adminapi.go`
- [x] 一律要求 Bearer 令牌；无令牌则整个前缀不挂载(404 而非 401)
- [x] 常量时间比较，最短 32 字符
- [x] 非回环绑定须 `acknowledge_public_exposure`
- [x] **证书签发端点**(PKI-104 至 PKI-108、PKI-045 至 PKI-046) — 两条路径分成两个端点:
      `POST /certificates/issue`(服务端生成密钥，私钥一次性返回)与
      `POST /certificates/sign-csr`(设备自持私钥，推荐路径)
- [x] 签发即时生效，不需重启 — `TestIssueCertificateUsableImmediately`
- [x] CSR 路径永不返回私钥 — `TestSignCSRNeverReturnsPrivateKey`
- [x] 非回环绑定时拒绝经明文 HTTP 导出私钥，并指向 CSR 路径 —
      `TestIssueRefusesPlaintextKeyExportOffLoopback`
- [x] 审计记指纹，日志不含私钥 — `TestIssueDoesNotLogPrivateKey`

### 1.9 控制台

- [x] Node 模块构建,React 加 Vite,`go:embed` 嵌入 — `web/console`
- [x] 总览、设备、设备详情、证书、实时连接、策略、调试器、事件流
- [x] 证书签发入口(UI-050、UI-052、UI-053、UI-083、UI-084) — 证书页与
      设备详情页都可进入;私钥面板提供复制与下载，关闭按钮在未取走前
      明说"私钥将丢失"
- [x] 控制台只保留服务端生成密钥对这一条路径(UI-083，**废弃 UI-051**)
      — CSR 签发不进控制台:浏览器里没法生成并保管设备私钥，表单只能
      承担"把 CSR 贴进来"这后一半。端点 `POST /certificates/sign-csr`
      不变(PKI-011、PKI-105)，仍可经 API 与命令行使用;`api.ts` 的
      `signCSR` 一并移除(留着会被 TST-136 判为无人调用)。表单说明文案
      预先写出非回环部署下的两条出路(UI-084、PKI-045)
- [x] 可写(超出 PRD 的「单页只读」)
- [x] 令牌存 `sessionStorage`(UI-041)
- [x] 中英双语
- [x] SSE 逐类型 `addEventListener`,并有门禁测试钉住(OBS-120、OBS-121)
- [x] 事件流回填最近 200 条，带 `backlog` 标记与 `sync` 分界(OBS-110…115)
- [x] 事件流空状态说清「通道是通的/确实没数据/怎么造一条」(UI-030…033)
- [x] **其余五个页面的空状态**(设备、证书、连接、策略、概览)按 UI-030
      补齐 — 共用 `Empty` / `EmptyFiltered` 组件(`web/console/src/ui.tsx`);
      带筛选的三页区分「真的没有数据」与「被筛选滤空」(UI-032);
      「怎么造出一条来」按依赖顺序给(策略→证书→连接);通用文案键
      `common.empty` 已删除，门禁 `TestConsoleEmptyStatesAreActionable`
      禁止它回来
- [x] 虚拟滚动(UI-004、UI-059) — 设备、证书、连接三张表用 `useWindow` 只渲染视口内的行。没有新的前端依赖。服务端仍然每页 50 条。
      2026-10-29 在真实 Chromium 里验过:60 台设备、服务端一页 50 条时,
      DOM 里是 25 行数据加一个撑高行（`tests/console-e2e/`,TST-131）

### 1.10 可观测性

- [x] 单一事件源，四个消费者(契约 11) — `internal/events`
- [x] 连接与订阅生命周期事件
- [x] 采样，拒绝与错误不受采样影响 — `mustKeep`
- [x] 被拒连接写日志，字段取齐(OBS-100…103)
- [x] 被拒授权写日志，含策略变量求值结果(OBS-104…106)
- [x] 日志限速，压制数定期汇总
- [x] 日志不含私钥、密钥、完整签名、设备载荷(NFR-041、NFR-048)
- [x] **Prometheus `/metrics` 端点**(OBS-020 至 OBS-029) — 零依赖
      自实现文本暴露格式 `internal/metrics`;计数器由总线在 `Emit`
      采样之前累加 `internal/events/metrics.go`;采集端
      `internal/server/metrics.go`。覆盖连接、消息、路由、授权、
      元数据、集群、PKI、配额、运行时九族
- [x] 指标须与采样和丢弃解耦 — `TestMetricsUnaffectedBySampling`、
      `TestMetricsUnaffectedByDrops`
- [x] 指标维度全为有界枚举，上界由名字数组长度推出 —
      `TestEnumBoundsMatchNames`、`TestMetricDimensionsCoverAllEnumValues`
- [x] 被拒的订阅也产出事件、日志与指标(OBS-130) — 之前非法过滤器
      导致的订阅失败在事件、日志、指标三处全不可见
- [x] `/metrics` 不被控制台通配路由吞掉(TST-120)

### 1.11 CI 门禁

- [x] ARCH-052 `internal/mqtt` 不 import 其他 internal 包
- [x] ARCH-053 `SessionState` 字段类型白名单
- [x] ARCH-054 `MetaView` 签名不含 ctx 或 error
- [x] ARCH-055 核心包禁止 `time.Now()`
- [x] ARCH-058 订阅树无指针字段
- [x] 覆盖率门槛：整体 85%,`mqtt` 与 `authz` 95%
- [x] race、交叉编译三平台、短时 fuzz
- [x] 投递路径的并发用例三条(TST-123) — 两发布者同一订阅者、投递中断开、
      关机与投递重叠。`internal/broker/concurrency_test.go`
- [x] ARCH-056 `api/awscompat` 响应 golden diff — `TestAWSCompatGolden` 对照 `internal/awsapi/testdata/awscompat.json`。重写只走 `-update-golden`，CI 不得打开（ARCH-076）
- [x] ARCH-057 单机测试跑一副本 Raft 组 — `LocalStore.Apply` 先 `Append` 再改视图。`TestSingleReplicaCommitsBeforeApply`（ARCH-077）。多节点复制仍未做

---

## 2. v0.1.5 AWS 兼容面

RMAP-007:兼容性是卖点，但其价值依赖于内核**先被真实设备验证过**。
先做兼容面等于在未验证的内核上叠加冻结契约(NFR-072)。

- [x] 策略调试器(提前做了,UI-010) — `POST /authz/simulate`,含变量求值
      轨迹与建议文案
- [x] 中英双语
- [x] **SigV4 验签** — 算法已落地（`internal/sigv4`，AUTH-072 至 AUTH-075），并已用于 `/mqtt` 的 WebSocket 握手和 `/api/aws`。控制面服务名是 `iot`，数据面是 `iotdata`。自有 CA 注册、检索索引和角色别名已接入（API-115 至 API-117，PKI-113，REG-066）
- [x] **443 WSS** — `/mqtt`、子协议 `mqtt`、查询串 SigV4（服务名 `iotdevicegateway`）、帧按字节流重组。连接时长默认 24 小时（NET-102）：配置了 `redirect_to` 才发 MQTT 5 的 `0x9C`。access key 绑到设备策略（AUTH-076）。与 `mqtt_alpn` 是两条监听器，默认都写 443，不能同时绑在同一地址。控制台测试客户端走管理面 WebSocket（UI-013、API-114），不走这条设备通道
- [x] **控制面约 40 个 action**,`aws` CLI 与 `boto3` 可直接使用
      — PRD v0.1 的 action 已挂上。对照脚本是 `tests/aws-sdk/run.sh`（TST-129），不进 `make gate`。已删除的 Job、预配、规则、Thing Type、标签不再接
- [x] **OTel** — 控制面请求、签发和跨节点影子转发导出为 OTLP/HTTP JSON（OBS-140 至 OBS-142）。默认关闭。消息数据面按 OBS-042 不追踪
- [x] 独立的结构化审计事件族 — 管理面与 `/api/aws` 已生效的写操作经事件总线写入数据目录的 `audit.log`（OBS-137 至 OBS-139）。命令行 `cert issue` 与 `cert revoke` 成功后追加同一份文件（OBS-143）
- [x] 控制台 MQTT 测试客户端(UI-013、UI-054、API-114) — 管理面 WebSocket，进程内订阅，QoS 0/1/2，浏览器里解码 JSON、定长 CBOR 与 protobuf 线格式
- [x] 控制台主题抓包(API-052、API-118、UI-055) — 管理面 WebSocket，不进路由表，不占订阅额度
- [x] RBAC(UI-024、API-058、API-119、AUTH-078) — reader、operator、admin。引导令牌仍是 admin。会话 cookie `oiot_session`
- [x] ~~OIDC(API-120)~~ — **已移除**（AUTH-080）。登录只剩管理面令牌一种
- [x] 设备历史时间线(UI-011、API-121、OBS-145) — 事件写入 `events.jsonl`，时间线按设备读取。影子成功更新和删除记一条 Meta
- [x] SDK 对照套件 — 设备接入层在 `tests/device-sdk/`。控制面 `boto3` 与 `aws` CLI 在 `tests/aws-sdk/`（TST-129）。两者都不进 `make gate`。
      2026-10-29 两套都实跑过：device-sdk **18/18 通过**（含默认跳过的
      「保活超时」那条，`WITH_KEEPALIVE=1` 下 44 秒后真的收到遗嘱）；
      aws-sdk 的 boto3 流程与 `aws` CLI 读回同一台设备都通过
- [x] 性能与内存基线公开数据 — `docs/baseline/memory.json`：热视图每设备 188 字节、订阅树每订阅 185 字节、每连接 13096 字节。高于记录 10% 即失败（TST-127）。耗时不入库

---

## 3. 集群(阶段 A–E)

ARCH-001:**单机不是集群之前的版本，而是节点数为一的集群。**
13 条 Day-1 契约已全部落地，所以这部分是填充实现，不是重构。

- [x] 13 条 Day-1 接口契约
- [x] epoch 栅栏 `(raft_term << 32) | leader_local_seq` — `cluster.MakeEpoch`
- [x] `SessionOwnership`、`Partitioner`、`Transport` 接口
- [x] 单机实现:`LocalOwnership`、`LocalPartitioner`、`LoopbackTransport`
- [x] 陈旧视图对新连接失败闭合、但不断开已有连接(CLU-023…025)
- [x] **一副本 Raft** — 单机元数据写入走 `cluster.SingleReplica`，投票者 1 个，追加即提交（ARCH-057、ARCH-077）。没有第三方 Raft 库
- [x] 阶段 A 节点基础 — 模式、data dir、manifest、node identity、readiness。`TestIdentifyGeneratesStableIdentity`、`TestReadyzReports503WhenNotReady`
- [x] 进程内三节点日志复制与 quorum（CLU-058）— 多数派提交后各节点已提交序列一致；丢掉多数派时写入拒绝，已提交的仍可读
- [x] 进程内快照、leader 转发与视图应用（CLU-037、CLU-059）— 空白节点靠快照追平；跟随者的写入转到 leader；三个视图的 revision 与设备集合一致
- [x] 阶段 B 真实多进程（CLU-060）— 提交前落盘；三个进程用 TLS 互连，证书 CommonName 必须是节点 ID；只剩一个节点时写入失败。`openiotd serve` 仍是单机，**不能把单机进程说成高可用**
- [x] 阶段 C 分区与围栏（CLU-061）— 只有 leader 发放 epoch；任期升高后旧 epoch 失效；分区稳定；版本只增
- [x] 阶段 D 消息 mesh（CLU-062、ARCH-017）— 精确订阅不进兴趣表；发送队列有上限，满了就拒绝
- [x] 阶段 E 运维（CLU-063）— 排空后拒绝新写入；日志可以备份再恢复

---

## 4. 测试体系

- [x] 单元与集成测试，整体覆盖率 85.3%（2026-10-29 实测,`make cover-check`）。
      此前记的 91.3% 是旧数字：兼容面与集群那批新代码进来之后，整体覆盖率
      一路落到 **84.7%**,门禁的 85% 实际上是红的，而这张表记着 91.3%。
      补了 `internal/registry` 两处的测试才回到线上：角色别名此前只有
      兼容面的 HTTP 用例(`awsapi` 的 `TestSearchAndRoleAlias` 与 golden),
      注册表层一条没有——校验、分页 token、损坏记录、存储失败全在
      `alias_test.go` 里补齐，分页那条已验证把 token 写成本页第一个即转红;
      自有 CA 此前只测了登记时的两处证明(`TestRegisterCARequiresProof`、
      `TestCASubjectLimit`),状态改写、删除、列举分页与外部证书登记的
      五条拒绝理由在 `ca_lifecycle_test.go` 里补齐。
      覆盖率这类数字记成"上次看到的值"等于没记：它只会在真的掉下去时骗人
- [x] 真实 mTLS 验收(TST-090…092):证书**有效期**无法用注入时钟测试,
      **吊销**可以
- [x] 属性测试与 3 个 fuzz 目标(shadow 合并)
- [x] 零拷贝别名陷阱已钉住(TST-107)
- [x] 字符串匹配缩进 JSON 的反模式已清除，改为解析后断言(TST-116)
- [x] **真实设备验证(RMAP-007 的前置条件)** — 2026-10-27 用
      AWS IoT Device SDK for Python(awsiotsdk 1.31)与 paho 2.1
      对跑真实节点。通过:mTLS 8883 接入、clientId 与证书绑定、
      QoS 0/1/2 往返、越权订阅被 SUBACK 拒、clientId 不符被拒连、
      影子 get/update/delta(走 SDK 的 `iotshadow`,真实
      `$aws/things/<name>/shadow/*` 契约)、吊销即时断连(0.0 秒)、
      ALPN `x-amzn-mqtt-ca` 端点、MQTT 5 客户端、保留消息、
      持久会话与离线补投。发现两个缺陷，见 1.2 的遗嘱与 1.4 的私钥编码
- [x] **确定性模拟测试(TST-003、TST-017、TST-031、TST-033…038)** —
      `internal/broker/simulation_test.go`。种子驱动一串随机操作去打
      真实 broker(真实会话存储、路由表、配额、元数据视图，只有时钟与
      定时器是注入的),每一步断言一组不变量。固定种子集进门禁,
      随机种子扫描走 `make sim`(夜间 CI 有独立 job)
- [x] **TST-030 的网络与磁盘可注入** — `Isolate` 丢掉一个节点；`SetDiskFail` 让落盘失败时已提交序列不前进。
      协程调度在 Go 里要可重放等于换掉整个运行时，按 TST-035 不做
- [x] **外部协程不得代连接做拆解(竞态)** — `KickClient`、`kickByCert`、
      驱逐通知、接管、关机这五处此前在**调用方的协程**上跑完整的
      `close`,而它会持久化会话、发遗嘱、碰编码缓冲——这三样按设计
      归连接自己的读协程独占。竞态检测器实证:`KickClient` 的 `persist`
      与连接自己的 `flushQueued` 撞在同一个 `State.Queued` 上。
      改为 `conn.evict`:只发 DISCONNECT(编码到局部缓冲)并关套接字,
      读循环醒来后带着登记的原因码走正常拆解。对客户端时序无变化。
      这条是既有缺陷，由模拟在 `-race` 下压出来的
- [x] **模拟已找出并修掉四个缺陷** —
      ① 读缓冲污染:`packet.Decode` 零拷贝，而读循环在返回报文**之前**
      就把缓冲压实，一次读取收到多个报文时后一个会盖掉前一个的主题/
      过滤器/负载。客户端订阅 `pipe/x`、服务端给它订上别的主题并回
      Granted,两边都不报错 — `TestPipelinedPacketsAreNotCorrupted`;
      ② 重复订阅同一过滤器每次都占一份配额，而路由表与会话是就地替换 —
      `TestDuplicateSubscribeDoesNotLeakQuota`;
      ③ 全局订阅配额只在显式 UNSUBSCRIBE 时归还，「订阅完直接断开」
      这条最常见的路径每次都永久少一份 —
      `TestSubscriptionQuotaReleasedWhenSessionEnds`;
      ④ 退订时两份配额共用一个释放条件，而每连接额度属于会话、全局额度
      属于路由表，接管与 clean start 之后两者会不一致。
      ②③④ 耗尽后果相同：全局上限 50 万，到顶则**整个 broker** 不能再
      新增订阅，且只能重启恢复
- [x] **订阅状态的权威已收口(MQTT-105…107、QTA-106…108)** — 原记为
      「接管会让持久会话的订阅回滚到上次落盘时的样子」，查下去是同一份
      逻辑状态散在三处(会话运行时、会话存储、路由表)而没有一处被定为
      权威，各条路径各挑了一个顺手的读。一共三处分叉，方向不同:

      ① **接管回存储读**(原记录的那条)。订阅与退订只改内存，落盘发生在
      断开时；被抢占的连接因为索引已指向新连接而不会再落盘。改为接管时
      接过被抢占连接运行时里的订阅集(`conn.adoptLive`)。原复现
      `-sim.seeds=1593049638 -sim.ops=3000` 第 399 步，已验证修复后转绿。
      门禁 `TestTakeoverKeepsLiveSubscriptionState`(两个方向各一条子用例,
      均已验证去掉修复即失败，其中退订方向直接表现为**设备收到它已经
      退订的主题**)

      ② **会话过期不清路由表**。过期只是让恢复落空，没有人摘路由表里的
      订阅，于是新会话继承了上一代的订阅——**会收到自己从未订阅过的主题**,
      不只是记账问题。改为「没有续上任何会话就清掉该 clientId 的残留
      订阅并归还额度」,clean start 并入同一条规则。门禁
      `TestExpiredSessionLeavesNoRouterOrphans`(已验证)

      ③ **全局订阅额度的占用侧问了会话**。归还侧本来就问路由表
      (`removed`),占用侧两份额度却共用会话的判定，于是会话与路由表一旦
      不一致，同一条订阅就多占一份全局额度而路由表只是就地替换。改为
      两份额度各问各的权威(QTA-106),为此给路由表加了
      `Router.Has`/`Trie.Has`/`SharedGroups.Has`。顺带修掉一条潜伏的:
      **重启后重放订阅一概不占全局额度**,而那时路由表是空的，重放是真的
      往里新加(QTA-107)。门禁 `TestReplayAfterRestartTakesSubscriptionQuota`
      (已验证),另有 `TestTrieHas`、`TestRouterHas` 覆盖判定本身

      ③ 只在随机种子深扫才触发(前提是 ① 或 ② 先把两者弄得不一致)。
      验证:`-sim.random=80 -sim.ops=3000` 全绿，另有 6 轮 2 并发 × 60 种子
      (共 360 次种子运行)无分叉
- [x] **需求 ID 唯一(TST-125)** — 实现期补充按小节空号编号，与正文撞出
      23 个重复 ID。正文保号，补充改到该域最大号之后(MQTT-108…110、
      PKI-104…112、AUTH-066…071、CLU-055…057)。`ARCH-062`/`ARCH-065`
      是同一条规则写了两遍，并成正文一行。门禁
      `TestRequirementIDsAreUnique`
- [x] **投递路径的三条并发缺陷(ARCH-073…075)** — 查 ① 的过程中顺手压的,
      与记账无关，但同属「外部协程不得代连接做事」这条已经修过一次的纪律
      (`conn.evict` 那次)。三处都**不需要罕见时序**,两处会 panic
      而全仓库没有 `recover`:

      ① 投递借用订阅者的编码缓冲。投递跑在**发布者**的读协程上，而编码
      缓冲归订阅者自己的读协程独占。两个发布者同时投给同一个订阅者时,
      两次 `EncodeTo` 写进同一块缓冲，拷出来的是两个报文的混合——订阅者
      收到畸形报文后断连。拓扑上这是 IoT 里最普通的情形。改为
      `conn.sendForeign` 自带缓冲(分配次数没增加：入队本来就要求一份副本)

      ② 出站队列的「是否已解绑」判定在锁外，与入队分处两次加锁。那道缝里
      订阅者自己的协程会把队列通道关掉，于是发布者协程的那次发送是**向已
      关闭的通道发送**——panic。一次完全正常的"客户端在别人正给它投消息时
      断开"就能让 broker 整个退出。改为判定与入队同一临界区(锁的次数
      没有增加)

      ③ 写出池用 `close(就绪队列)` 停工，而入队发生在任意协程上(含工作
      协程 flush 的尾部，它离 Close 最近)。关机时刚好有报文入队同样是向
      已关闭通道发送。改为独立的 `done` 通道停工

      三条各有门禁用例，且都已验证**逐条**去掉修复即在 `-race` 下报红:
      `TestConcurrentDeliveryDoesNotShareEncodeBuffer`(同时不靠检测器,
      断言主题与负载必须自洽)、`TestDeliveryToClosingConnectionDoesNotPanic`、
      `TestShutdownDuringDeliveryDoesNotPanic`。确定性模拟压不到这三条——
      它的操作是串行的(TST-123)
- [x] **设备 SDK 对照套件入库** — `tests/device-sdk/`,`./run.sh` 自建
      临时节点跑 18 项后拆掉。不进 `make gate`:门禁必须只依赖 Go 工具链
      (`go install` 不该要求先装 Python 与一套 C 扩展),定位是发版前与
      改动接入层之后手动跑，以及 CI 里单独的可选 job
- [x] 夜间长时 fuzz 与语料库入库 — 每天 02:00（CST）跑 `make fuzz-long`。失败语料从报文、主题、策略、影子四个包的 `testdata/fuzz` 下载后入库（TST-128）
- [x] e2e(登录、设备列表、策略调试、MQTT 客户端) — `TestConsoleCriticalFlows` 走这四条管理面调用（TST-126）
- [x] **浏览器里真的点过了（TST-131）** — `tests/console-e2e/run.sh`，
      18 项全绿：登录闸（含坏令牌被拒）、总览、设备页的窗口渲染
      （服务端一页 50 条，DOM 里 25 行数据 + 撑高行）、前缀筛选、设备详情
      的影子、证书页、连接页的空状态与在线率曲线、策略页、调试器的放行与
      拒绝两条路径（含变量求值与「如何放行」）、事件流的 SSE、测试客户端
      订阅后收到自己发布的那条、抓包页、访问控制页、中英切换，以及
      整轮无浏览器报错。不进 `make gate`（要 Node 与 Chromium），
      命令是 `make console-e2e`。
      第一版 18 条里挂了 12 条，全是测试自己的问题:Chromium 默认 `en-US`
      而 `detectLang` 读 `navigator.language`,断言在英文界面上找中文文案。
      脚本现在固定 `locale: 'zh-CN'`,英文那一半用切换按钮验。
      产品侧没查出缺陷——曾以为 `arn:aws:iot:*:*:*/devices/...` 该放行,
      查下去是 AUTH-023 刻意不许资源类型带通配（否则一条放行 `topic/` 的
      语句会连带放行 `topicfilter/`）
- [x] 性能回归门禁(超 10% 阻断合并) — 比的是 `docs/baseline/memory.json` 里的字节，不是耗时。`TestViewMemoryBudget`、`TestConnMemoryBudget`、`TestTrieBytesPerSubscriptionBaseline`（TST-127）。竞态检测下不做 10% 比较。
      订阅树那条原先放在 `internal/mqtt/topic`，而那次比较要导入 `internal/baseline`，
      违反 ARCH-052（协议层须可独立 fuzz 与开源）——`make contract` 报的就是它。
      现已拆成两条：量级断言（400 字节/订阅）留在 `TestTrieScaleSanity`，
      10% 比较移到 `internal/router`（TST-130）。已验证把基线调低即转红

---

## 5. 建议的推进顺序

排序依据是「让内核真正可交付」，不是实现难度。

| 次序 | 项 | 理由 |
| --- | --- | --- |
| ~~1~~ | ~~Prometheus `/metrics`~~ | **已完成** |
| ~~2~~ | ~~证书签发 API 加控制台入口~~ | **已完成** |
| ~~3~~ | ~~修存储选型的记述~~ | **已完成**,并修掉一条失效的门禁规则 |
| ~~4~~ | ~~principal 关联操作~~ | **已完成**,并补上「改绑须断连」这条缺失的安全语义 |
| ~~5~~ | ~~其余五页空状态~~ | **已完成**,并加了门禁防止标准再被绕过 |
| ~~6~~ | ~~真实设备验证~~ | **已完成**(2026-10-27),见第 4 节。它是 RMAP-007 给兼容面定的前置条件，此前未被记录为条目 |
| ~~7~~ | ~~修遗嘱(MQTT-060)~~ | **已完成**,并把「除 DISCONNECT 外一律发出」写进基线(MQTT-064) |
| ~~8~~ | ~~修私钥 PEM 编码~~ | **已完成**(PKI-055…057),门禁改为断言 PEM 块类型本身 |
| ~~9~~ | ~~Will Delay Interval(MQTT-061)~~ | **已完成**,定时器经 `Options.AfterFunc` 注入；取小值的设计让会话过期那一半不必引入轮询器 |
| ~~10~~ | ~~确定性模拟测试~~ | **已完成**,并当场找出四个缺陷(含一个数据污染) |
| ~~11~~ | ~~收掉模拟剩下的那处记账分叉~~ | **已完成**。那一处查下去是三处，根因是同一份状态散在三处而没有权威(MQTT-105…107、QTA-106…108);过程中另压出三条投递路径的并发缺陷，两条会 panic(ARCH-073…075) |
| ~~12~~ | ~~QoS 重传与在途额度~~ | **已完成**。续上会话按原 packet ID、带 DUP 重投并继续占额度；重启后补占差额；会话丢掉时连额度一起放；接管接运行时里的在途表；窗口满则排队（MQTT-111 至 MQTT-115） |
| ~~13~~ | ~~兼容面(第 2 节)~~ | **已完成**。WebSocket、`/api/aws`、自有 CA、检索、角色别名、审计、OTLP、控制台 MQTT、抓包、RBAC、时间线、golden、连接详情、内存基线、虚拟滚动（UI-059）、在线率曲线（UI-060）、一副本 Raft（ARCH-057）、进程内 quorum（CLU-058）、快照与 leader 转发（CLU-059），以及三进程 TLS、分区围栏、有界 mesh、排空和备份（CLU-060 至 CLU-063）都已接上 |
| ~~14~~ | ~~补回门禁与三套非门禁套件~~ | **已完成**(2026-10-29)。门禁先红了三次（gofmt、ARCH-052、覆盖率 84.7%）；`internal/registry` 从 60.5% 补到 93.8%；device-sdk 18/18（含保活超时那条）、aws-sdk、新增的浏览器 e2e 18/18 都实跑过。详见第 2、4 节 |
| ~~15~~ | ~~使用教程（UI-076 至 UI-082、TST-142 至 TST-146）~~ | **已完成**（2026-10-30）。入口在标题栏（2026-10-31 由侧栏顶部挪过来，UI-076 修订），三章九页，中英两棵树；门禁六项、浏览器 e2e 36/36。Java 示例未实跑（本机无 JDK）。顺带压出一个真缺陷：规范路径少编一层，数据面 `Publish` 与把 ARN 放在路径里的控制面操作对任何 SDK 都 401（AUTH-077 修订、TST-145） |

## 6. v0.4 阶段 B：元数据复制（2026-10-29 接到 `openiotd serve` 上）

选的是「冷层纳入复制日志」那条路，不是「读写转发给 leader」。依据是
PRD 本来就那么规定的：CLU-021 与 CLU-053 把热视图定义成热路径读取的
子集、冷层是完整记录；CLU-059 要求空白节点靠快照追平（若冷层不复制，
追平之后 `GetThing` 仍读不到）；ARCH-057 要求单机走同一条路径。

- [x] 命令的线格式（CLU-064） — `internal/meta/wire.go`。策略传**文档**
      而不是编译产物：后者是宿主语言里的一棵匹配器，序列化之后在另一个
      节点上还原不出来，各节点拿同一份文档各自编译。带格式版本号，读到
      更高的版本明确报错。`TestCommandWireRoundTripCoversEveryField`
      用反射逐字段比，Command 上加字段而线格式忘了带就会红
- [x] 冷层随命令走同一条日志（CLU-065） — `meta.RecordOp`，控制面不再
      自己写 KV。计数器的推进（策略集编号，REG-112）也在里面：各节点
      各自读-改-写会把同一个编号分配两次，而编号正被证书与设备记录引用
- [x] `meta.ReplicatedStore`（CLU-067） — 包住同一个 `LocalStore`：
      命令先过共识，再由 `ApplyCommitted` 作用到状态机。单机那条路径
      不是旁路，两者应用命令的代码完全相同
- [x] `serve` 的集群模式 — `node.mode` 取 bootstrap/join 时建 TLS 复制组
      （`cluster.replication` 配置段），起追平协程，就绪度要求先追平
      （CLU-023：没追上就放设备进来，等于让它们用一份可能漏掉吊销的视图
      通过鉴权）。standalone 写了复制配置直接报错（CLU-070）
- [x] 三进程验收 — `tests/cluster/run.sh`（`make cluster-test`）：
      冷层与策略正文在三节点一致、跟随者写入转发给 leader、三节点
      `metaRevision` 相同、掉一个节点写入照常、再掉一个失去多数派则
      写入 503 而读取照常、日志落在 data dir 的 `raft/` 下
- [x] 实测抓出的四个缺陷（都只在真多节点上显形）：
      ① 启动时的文件化注册表导入发生在选举之前，第一次写入撞上
      「不是 leader」（CLU-074：集群下只由 bootstrap 节点导入，且等选举完成）；
      ② 跟随者写入在 Propose 成功后立刻查本地，而那条日志还要等 leader
      推回来——返回 500 而数据其实写成功了（CLU-071）；
      ③ `registry` 有一张「策略名到编号」的内存映射，它只在本节点自己
      写入时更新，于是在跟随者上建设备会说「策略不存在」而那份策略就在
      冷层里（CLU-072，与 MQTT-105…107 同一类：同一份事实散在几处而
      没有一处是权威。已删掉那张映射）；
      ④ 失去多数派时写入返回 400，而请求完全合法（CLU-073，改为 503）
- [x] `/node` 暴露 `metaRevision`（OBS-146） — 三个节点的这个数字不一致
      就说明有人没追上

### 阶段 D：消息面跨节点（2026-10-29 同日接上）

- [x] mesh 通道（CLU-076） — 复用复制组那条 TLS 连接，加一种消息类型
      承载消息帧、兴趣通告与设备归属。不另开端口、不另发证书：
      节点之间只有一条互信通道
- [x] 兴趣通告（CLU-075、ARCH-017） — 本节点的**通配符**订阅周期性以
      全量快照通告给其他节点。精确订阅不传播（百万条设备订阅不进全局表，
      这是相对通用 broker 的主要性能杠杆）。引用计数而不是集合：
      两个客户端订阅同一过滤器时，一个退订不该把兴趣从集群里撤掉
- [x] 跨节点投递（CLU-062、CLU-077） — `broker.route` 的远端分支接上
      `cluster.Transport`；收到的帧只投本地订阅者，不再转发（否则两个
      互相有兴趣的节点之间会无限弹）。帧格式版本不认识就丢弃并计数
- [x] 设备归属（CLU-075、CLU-080） — 设备域主题的路由靠通告来的归属表；
      本节点有就判本地，谁都没有就**不转发**（随便挑一个节点只会让消息
      在那边被丢掉）
- [x] 控制台也算在内（CLU-078） — 测试客户端与抓包的订阅走的就是
      `Router.Subscribe`，自然参与通告。不参与的话多节点下控制台只能
      看到本节点的流量
- [x] 三进程验收 — `tests/cluster/run.sh` 里新增一条：b 上订阅
      `telemetry/#`，a 上发布，b 收到（`tests/cluster/crossnode.py`，
      只用标准库的 WebSocket 客户端）

### 会话围栏与影子（2026-10-29 同日收尾）

- [x] **会话围栏**（CLU-081） — 节点接管一个 clientId 之后广播通知，
      收到的节点断开本地同名连接。选的是广播而不是「epoch 经 raft 发放」：
      后者让每个设备连接都等一次共识，而设备上下线是高频事件——
      把它放进日志会被连接流量撑爆（这也是阶段 B 时否掉「所有权进日志」
      的同一条理由）。所以互斥是**最终一致的**，窗口是一次 mesh 往返。
      通知在连接路径上只入队不阻塞，队列满就丢并记一条。
      验收：同一 clientId 先连 a 再连 b，a 上的旧连接被断、b 上的新连接
      仍健康（`tests/cluster/fencing.py`，手写的最小 MQTT 客户端，
      只用标准库）。已验证把广播摘掉即红（"同一 clientId 在两个节点上双活"）
- [x] **影子跨节点一致**（CLU-082 至 CLU-085） — 影子命令随**同一条**
      复制日志走，各节点在 apply 阶段用同一份 `shadow.ApplyCommand`
      算出同样的结果。没选分区转发：分区要有副本就得每分区一个 raft 组，
      而单副本分区在归属节点永久故障时直接丢影子，违反 SHDW-070。
      409 冲突是确定性的状态机结果，经幂等键交回发起方，不算 apply 失败；
      真正的存储故障仍让 apply 失败。线格式保住数字字面量
      （`1.50` 不能变成 `1.5`）。
      验收：a 上写影子，b、c 读到同样的内容**与同样的版本号**，
      而版本不符的更新仍返回 409

- [x] **Admin 扇出查询**（CLU-086 至 CLU-089，契约 12） — 「列出在线连接」
      跨节点聚合：并发发出（耗时是最慢那个节点的，不是累加），本节点
      直接走本地处理函数不绕网络，游标编码**每个节点各自的位置**
      （ARCH-040——单一 offset 下各节点行数不同，按全局偏移翻页会跳过
      某些节点的行、重复另一些），单节点失败标成 `partial` 加失败节点
      清单而整体仍成功（ARCH-041）。
      验收：两台设备分别连 a 与 b，从 **c** 上列举看到两条，前缀筛选
      也跨节点生效。已验证把扇出摘掉即红（c 上列出来是空的）

### 阶段 E：成员变更（2026-10-29 同日接上）

- [x] 成员列表进 raft 状态（CLU-090） — 变更作为日志条目复制，**提交之后
      才改多数派的分母**；条目带地址，所以新节点不必出现在别人的配置里。
      配置里的 `peers`/`voters` 退化为「初始成员」，只在日志为空时使用
- [x] 一次只允许一个变更在途（CLU-091） — 单步变更并发时新旧配置的多数派
      可能不相交；失败路径须撤掉在途标记，否则这个 leader 以后再也提不了
      变更而没有任何报错
- [x] 成员随 raft 状态落盘、随快照传播（CLU-092、CLU-093） — 只靠重放日志
      会在变更被压缩进快照后把成员退回初始配置；空白节点若按自己配置里的
      成员算多数派会选自己当 leader
- [x] Admin API：`GET/POST/DELETE /api/admin/v1/cluster/members`（CLU-094、
      OBS-147） — 要令牌、写审计；503 给「此刻做不了」（不是 leader、上一次
      还没提交、没有多数派），409 给「已是成员 / 不是成员 / 移除最后一个」
- [x] 逐字段盯住条目的三条复制路径（CLU-095） — `TestEntryCloneCoversEveryField`
      用反射比对内存克隆、线格式与落盘往返。**这是实测犯过的错**：加了成员
      字段之后三处全忘了带，表现是跟随者永远看不到成员变更，继续按旧成员
      算多数派而没有任何报错
      验收：`tests/cluster/run.sh` 里起第四个节点 d（配置里只写自己）并加入，
      断言四个节点都看到 4 个成员、多数派从 2 变成 3、d 追平了既有设备与
      策略绑定，移出后多数派回到 2

### 控制台外壳改版（2026-10-29）

- [x] 标题栏（UI-063） — 横跨整宽，左边品牌与节点就绪，右边角色、语言、
      主题、退出。全局动作原先在侧栏底部，而侧栏窄屏下会变成横向滚动条，
      那组控件会被滚出可视区。标题栏**不**重复当前页标题：那个 h1 归各
      视图自己，一页两个同名标题会被读屏软件念两遍
- [x] 登录页（UI-062、UI-066） — 补上语言与主题开关（此前第一屏根本
      没有，浏览器语言判错就只能先用读不懂的语言登进去）；改成真 form；
      登录页只有令牌一条路（OIDC 随后整条移除，见下）
- [x] 导航分组加图标（UI-064） — 十项分四组（资源 / 授权 / 运行时 / 诊断），
      判据是生命周期不是对象类型：设备证书是重启还在的资源，连接随进程
      消失。图标是手写内联 SVG，不引图标库
- [x] 状态不再只靠颜色（UI-061） — 就绪带文字，当前导航项除底色外另加
      一道竖条。原先那个 9px 圆点带 aria-hidden，颜色是它携带的全部信息
- [x] 窄屏藏文字的按钮补 aria-label（UI-065） — display:none 的文字不在
      无障碍树里，只剩图标的按钮就没有名字了。语言按钮的"EN/中文"不藏：
      地球图标说不出它会切到哪一种语言
- [x] 顺带修了 `Button` 不转发 `aria-label`（UI-065） — TypeScript 不检查
      带连字符的 JSX 属性，调用方写了而包装组件没转发时类型检查照样通过，
      标签静默消失
- [x] 门禁补三项（TST-132 至 TST-134） — 中英词典逐键比对加未定义键检查、
      控制台运行时依赖锁死两个、外壳约定读 TSX 源码钉住。七条断言都确认过
      破坏实现会红
      验收：`make console-e2e` 22 项（新增登录页切语言、标题栏身份与状态、
      侧栏分组与图标、退出回到登录闸），深色与 420px 窄屏各看过一遍

### 登录收敛与访问控制页（2026-10-29）

- [x] 移除 OIDC 登录（AUTH-080，废弃 AUTH-062、API-120） — 删掉
      `internal/server/oidc.go`（279 行）、四个 `/oidc` 端点、`oidc.json`，
      以及**只有 OIDC 会签发**的内存会话 cookie：`OpenSession` 全仓库只有
      一个调用方就是 OIDC 回调，留着就是一个谁也填不进去的会话表。
      `DELETE /session`、`identify` 的 cookie 分支、前端的 `'cookie'` 特判
      一并下线。退出登录现在就是客户端忘掉令牌——令牌登录没有服务端会话
      可销毁。理由是身份来源要唯一：两条入口意味着两套过期语义、两处
      吊销点，而 cookie 那条在界面上没有任何痕迹，最容易在升级时被忘记
- [x] 访问控制页改成用户列表（UI-067） — 表格给名称、角色、删除，
      「新建用户」在右上角，点了才弹对话框。原先是一张常驻的六行表单
      贴在列表下面，而这一页多数时候只有零到几个用户
- [x] 令牌只在对话框里出现一次（UI-067） — 原先是页面上一条横幅，
      会被下一次操作顶掉；而令牌服务端只存哈希，顶掉就只能删号重建。
      现在建成后对话框换成第二幕，关闭是用户自己的动作
- [x] 删除要确认（UI-067） — 原先点一下就删了。确认框说清后果（令牌
      立刻失效、正在用它的脚本开始收到 401），当前登录的那一行另给一句
- [x] 补齐空状态标准的两处漏网（UI-030） — 总览的配额与设备时间线此前
      只有标题和说明，没说"怎么不空"。门禁原先按页写死要 `cmd`，现在
      改成「`cmd` 或 `action` 至少有一个」，这两页随即红了
      验收：`make console-e2e` 24 项（新增空状态→对话框建用户→令牌只显示
      一次→列表出现、删除先取消再确认、`/oidc` 已 404）

### 策略调试器收窄（2026-10-29）

- [x] 只按策略判定（UI-068） — 去掉「按证书指纹 / 按设备 / 按策略」三个
      切换按钮。三者问的是同一件事，前两种只是先把主体解析成它绑定的
      策略；要反查用的是哪条，设备页与证书页上直接写着。策略下拉默认
      选中第一条，页面一进来就能用。接口仍接受三种主体，窄的是界面
- [x] 三句写死的中文搬进 i18n（TST-135） — `hint="64 位小写十六进制"`
      这类直接写在 TSX 里，英文界面上原样显示中文。逐键比对查不出来：
      它压根没进词典。补了门禁检查 JSX 属性里的汉字，确认过会红

### 部署清单与发布流水线（2026-10-29）

- [x] 容器镜像与发布流水线（NFR-100、NFR-101） — `deploy/Dockerfile`
      两阶段静态编译进 alpine，18.9 MB，非 root，控制台已嵌入所以构建不需
      要 Node。`.github/workflows/release.yml` 在打 tag 时先跑 `make gate`
      再推 ghcr.io 的 amd64/arm64 镜像，并附各平台二进制；推完跑一次
      init→serve→/readyz 的冒烟
- [x] 三份部署清单（NFR-102） — 单机 Docker、三节点 compose、三节点
      Kubernetes，各带 README 说清提供与**不提供**什么
- [x] 集群共用设备 CA（NFR-103） — compose 的 init 服务与 k8s 的
      initContainer 都做了这一步
- [x] 修 `OPENIOTD_DATA_DIR` 对 cert/admin 子命令不生效（NFR-104） —
      回归测试 `TestDataDirEnvAppliesToEverySubcommand`，两条路径都确认
      过破坏实现会红
      验收：镜像构建、单机 init→serve→/readyz→签证书→控制台 200、
      compose 三节点（成员一致、跨节点读写、掉一个仍可写、失去多数派
      写 503 读 200）都在本机真跑过
- [~] k8s 清单**未在真实集群上验证** — 环境里没有 kubectl。已验证的是
      YAML 可解析、`prepare.sh` 的证书与 init 部分真跑过、同一套配置与
      证书约定在 compose 三节点上跑通。k8s 特有的 `subPathExpr` 取证书、
      按 Pod 序号算 bootstrap/join、PVC 权限三处要在测试集群上过一遍

### 影子页与命名影子（2026-10-29）

- [x] 独立影子页 `#/shadow/<设备名>`（UI-069） — 设备列表操作列加「影子」
      直达。此前影子藏在"点进设备再往下翻"之后，列表上看不出有这个功能
- [x] 命名影子终于有入口（UI-069，SHDW-002、SHDW-043） — 服务端四个端点
      一直支持 `?name=`，`api.ts` 的 `listShadows` 也早写好了，**但没有任何
      视图调用**：一台用命名影子的设备在控制台上只看得到经典那一个。
      现在有命名影子时出选择器，切换后读写删都作用在选中的那一个上
- [x] `ShadowPanel` 抽成共用组件（`views/shadow.tsx`） — 设备详情页与影子页
      共用。下发 desired 是真写操作，两份实现迟早有一边漏掉版本冲突或删除确认
- [x] 补门禁：`api.ts` 导出的方法必须有人调用（TST-136） — 与 TST-132
      方向相反，拦的是"后端做完、前端忘了接"。用这次的 `listShadows`
      和一个新造的未接方法各验了一遍会红
- [x] 顺带清掉 OIDC 移除后残留的死 CSS（`.gate-oidc`、`.gate-or`），
      并把 `a.btn` 的下划线统一去掉——此前只给 OIDC 那一处单独修过
      验收：`make console-e2e` 26 项，新增「列表操作列直达影子页」与
      「切到命名影子看到它自己的 desired（且不残留经典影子的内容）」

### 默认策略收敛与抓包并档（2026-10-29）

- [x] 起步策略只放行 `$aws/things/<设备名>/*`（AUTH-069 修订） — 去掉了
      原来那条 `devices/<设备名>/*` 业务分支。非影子的子主题在
      `HandleReserved` 返回 ok=false 之后落回普通路由，所以业务发布与
      订阅照样成功，不会卡在"连上了但发什么都被拒"。门禁里那条
      `TestSamplePolicyGrantsWhatItClaims` 当场变红，改完同时钉住
      "旧的 devices 分支不再放行"
- [x] 全仓库的举例换成 `$aws` 形状 — 可粘贴的连接命令、测试客户端的
      过滤器与主题默认值、空状态示例、demo 打印、Docker README。
      原先的 `floor/#`、`floor/temp` 按默认策略本来就会被拒
- [x] 两档的说明文案收短，并修掉文案里的 markdown 星号（TST-137） —
      `**之前**` 这类直接塞进 `<p>`，界面上原样显示两对星号。同一轮里
      犯了三次，补了门禁拦它
- [x] 主题抓包并入测试客户端的「旁观」模式（UI-070，UI-055 修订） —
      删掉 `views/capture.tsx` 与那一页导航。两页看起来几乎一样，而
      差别在"要不要进路由表"；并成一个开关之后这个差别才说得清楚
      验收：`make console-e2e` 27 项。新增的那条直接证明旁观的价值——
      用 Admin API 改影子触发一条**没有任何订阅者**的发布，旁观捕获到了。
      把旁观模式偷偷换回订阅套接字，这条确实变红（前两次破坏因为没编译
      通过、产物没更新而假绿，产物指纹核对之后才确认）

### 使用说明抽屉与动作图标（2026-10-30）

- [x] 影子页加「使用说明」，右侧抽屉（UI-071） — 主题表、更新/成功/失败
      三段报文样例、六条规则。主题按**当前设备与当前选中的影子**渲染，
      切到命名影子时主题跟着换成 `shadow/name/<名字>/…`
- [x] 新增共用的 `Drawer` 组件 — Esc 关闭、点遮罩关闭、打开时焦点移进去
      （否则 Tab 还在后面的页面上走）。只滑不淡入：淡入期间面板半透明，
      底下的按钮会透上来，两层文字叠在一起谁也读不清——第一版就是这样，
      截图里看出来的
- [x] 策略页也加「使用说明」（UI-072） — 文档骨架、五个动作与资源类型、
      策略变量、六条坑。重点讲清 Subscribe 判 topicfilter 而 Receive 判
      topic：只放行订阅不放行接收，设备订得上却收不到
- [x] 动作按钮统一配图标（UI-073） — 做成 `Button` 的 `icon` 属性，
      对照表集中一处，同一个动词全站同一个图标。共 24 处按钮
      验收：`make console-e2e` 29 项，新增「影子说明按当前设备给出主题、
      不留占位符、Esc 可关」「切到命名影子后说明里的主题也跟着换」
      「策略说明列出五个动作与资源类型」

      写策略说明时第四次把 `**强调**` 写进文案，被上一轮刚加的 TST-137
      当场拦下——这条门禁加对了

- [x] 表单行按底边对齐（UI-074） — MQTT 测试客户端的订阅行与发布行上
      「QoS 下拉 + 按钮」是居中对齐的，而 `Field` 比按钮高出一个标签，
      按钮的中线因此落在下拉框上方约 11px。改成共用的 `.row-controls`
      按底边对齐，复选框那一格撑到与控件等高
      验收：`make console-e2e` 29 项全过；截图量过像素——下拉框与按钮
      的上下沿都落在 741/772 这两行上

### 兼容面 access key 的角色（2026-10-30）

- [x] 控制面 key 带角色，`/api/aws` 按角色判权限（AUTH-081 至 AUTH-083）
      — 此前 `/api/aws` 只验签，签名对了就能建设备、签证书、吊销、改
      策略、发布、写影子。一把只想让 CI 读设备列表的钥匙，发出去的其实
      是 root。AUTH-002 从一开始写的就是「access key 加 secret 加**策略
      绑定**」，绑定这一半此前没落地
- [x] 级别写在挂载处，53 条路由逐条定级（AUTH-082） — 不能按 HTTP 方法
      推：`ListAttachedPolicies`、`ListTargetsForPolicy`、`SearchIndex`
      是 POST 的读，`GET /role-aliases/{n}/credentials` 是 GET 的发凭证。
      后者今天只回 503，级别照样给 admin——按语义定，不按当前实现定
- [x] 密钥文件分成两种形状（AUTH-084） — 设备钥匙 `{id,secret,thing}`，
      控制面钥匙 `{id,secret,role}`，互斥且各自必填，两份文件对调会在
      启动时报错。此前控制面钥匙也得编一个没有作用的设备名
- [x] 403 `AccessDeniedException` 进异常表（API-124） — 与验签失败的 401
      分开。两者的修法不同：换钥匙 vs 查密钥和时钟
      验收：`internal/awsapi` 的权限矩阵与三条拦截用例（TST-138）；
      真节点上的 reader 用例（TST-139）；`tests/aws-sdk/run.sh` 用真的
      `aws` CLI 验 botocore 把 403 解成 `AccessDeniedException`，并确认
      被拒的请求没建出设备

      三处都先把实现改坏确认会红：去掉权限判定（8 条红）、把一条路由
      调低一级（矩阵红）、新增一条没登记的路由（矩阵红）。接线那一行
      删掉之后，真节点用例红在「reader 读设备列表 = 403」——fail-closed
      的方向是对的

### 兼容面 access key 的发放与吊销（2026-10-30）

- [x] 钥匙搬进复制的元数据冷层（AUTH-086） — 随同一条已提交日志走
      （CLU-065），`aws_api.keys_file` 去掉；旧配置里还写着会在启动时
      被拒并指出改法。此前每个节点各一份文件，只加在 A 上的钥匙打到 B
      是 401
- [x] ~~secret 加密落盘（AUTH-087、AUTH-088）~~ — **已撤销**（AUTH-092，
      2026-11-18）：`internal/secretbox` 整个包删除，secret 明文进冷层。
      连同 CA 口令（`OPENIOTD_CA_PASSPHRASE`、`pki.passphrase_file`）一起
      去掉，换的是部署与运维的简单；代价是一份备份泄露等于全部钥匙泄露，
      处置是删掉重发。`registry` 的字段也由 `sealed` 改名 `secret`——
      存明文却叫 sealed 是一句谎话
- [x] 停用、过期、删除（AUTH-089） — 三者加不存在对调用方是同一个 401。
      停用与删除都留着：怀疑泄露时要的是「立刻停掉**并且**留下痕迹」，
      删掉之后审计里的 `ak:AKIA…` 再也对不上人
- [x] 管理面四个端点（API-125 至 API-127） — 只给 admin。列表不带密钥
      材料；PATCH 缺 `disabled` 报 400 而不是当成 false（漏字段会把刚
      停掉的钥匙重新放出来）
- [x] 三类主体的边界写进基线（AUTH-090、AUTH-091） — 文档性需求：控制台
      用户（管理面令牌，作用域 `/api/admin/v1/*`，角色约束管理面操作）、
      控制面 access key（SigV4 调 `/api/aws`，角色约束 action 集合）、
      设备（证书或设备 WS 钥匙，授权走 IoT Policy 求值）互不相通，角色
      同名只是级别语义复用。此前只在 UI-075 的界面描述里顺口提了一句，
      而误解的代价是发错凭据，要等 401/403 才暴露。行为未变，
      已由 AUTH-081 至 AUTH-089、UI-068 的现有验收覆盖
- [x] 控制台「访问控制」页加一块（UI-075） — 与用户那块同形：带标题的
      卡片、各自的工具条、创建走两幕对话框、secret 只显示一次。页头不
      再放动作按钮——一组按钮悬在两块之上，分不清刷新的是哪张表
      验收：registry 的 CRUD/停用/过期/重开；真节点上的生命周期与
      明文扫盘（TST-140，随 AUTH-092 反转为 TST-153：正面断言 secret
      确实在盘上）；`tests/cluster/run.sh`
      新增「a 上发的钥匙 b、c 能用，a 上停用 b、c 立刻不认」（TST-141）；
      `tests/aws-sdk/run.sh` 改成经管理面发钥匙，真 `aws` CLI 验 reader
      被拒与停用后失效；`make console-e2e` 31 项

      明文扫盘那条第一版是假绿的：只找 secret 原文，而记录是 JSON、
      []byte 编成 base64，把封装改成直接存明文之后它照样通过。两种形态
      都找之后才真的会红

- [ ] **命令行没有 `apikey` 子命令** — 钥匙在复制的元数据里，只有正在
      跑的那个节点写得动（bbolt 文件被它独占）；离线的 CLI 要么写坏
      要么得自己接一遍 Raft 客户端。引导入口是 `openiotd admin token`
      加管理面，够用。真要加的话应当是一个打 Admin API 的 HTTP 客户端，
      而不是第二条写路径
- [ ] **设备的 WebSocket 密钥仍是节点本地文件**（AUTH-076） — 它在连接
      路径上，而那条路径不许有磁盘 IO（AUTH-010），搬进来要先在热视图
      里给它一个位置。改完要重启、集群下每节点一份这两条限制还在

### 换共识库：自写单组 Raft → hashicorp/raft（2026-10-31）

阶段 B 至 E 的复制组是仓库自己写的（`internal/cluster/memraft.go`，2431 行
实现加 1947 行测试）。审计它时按顺序发现七条缺陷，前四条是算法要求的组成
部分而不是"还没做的功能"：

1. **没有自动选举与故障转移**。`Campaign()` 全仓库只有两个调用点，都在
   `startReplication` 里且带 `Mode == bootstrap` 前提；没有选举超时，跟随者
   永远不会竞选。leader 进程一挂，剩下的多数派选不出新 leader，写入永久 503。
   而 `tests/cluster/run.sh` 只杀过跟随者——这个缺陷从未让门禁变红。
2. **当选时按副本计数提交前任期的日志**（论文 Figure 8 明确禁止）。
   `Campaign` 直接 `replicate(term, lastIndex)`，而那条日志可能来自前任期；
   正确做法是先提交一条本任期的 no-op。今天难触发，因为第 1 条让多节点
   几乎不会轮流当选——两个缺陷互相遮住了对方。
3. **候选人自投票不落盘**。崩溃重启后带着旧 term 回来，可以在同一任期里
   再投一票，而那是"同任期两个 leader"的入口。
4. **每次写入重写整份状态**：term、votedFor、快照、全量日志、成员列表一起
   JSON 序列化再落盘，而生产代码从不调用压缩——调了也没用，`Compact()` 只是
   把历史从 `log` 字段搬到同一个文件的 `snap` 字段。这个设计下日志无法截断。
5. `Applied()` 深拷贝**全部历史命令**，而 `runPump` 每 200 毫秒调它一次。
6. 启动时 `registry.Restore` 经**写入路径**重建热视图：集群下一个冷层里已有
   一百万条记录的节点重启，会把这一百万条重新提进日志（命令不带 ID，幂等
   窗口认不出它们是同一件事），在跟随者上还要逐条转发给 leader。
7. `writeAtomicPath` 改名之后没有 fsync 目录。

- [x] 选型（LANG-014、CLU-002 修订） — 定 `hashicorp/raft v1.7.3`。不选
      dragonboat 的判据是成本与收益的时间差：它的不可替代之处（多组调度、
      共享传输、合并心跳）要到 v0.4 的分区组才兑现，而代价在引入当天就要付
      （Pebble → `cockroachdb/errors` → sentry，实测 37 个符号进二进制；
      26 个模块、+15 MB；单一维护者）。钉 v1.7.3 而不是 v1.8.0：后者声明
      `go 1.25`，会把 LANG-001 的 1.24 底线顶走
- [x] **不用 `raft-boltdb`**（LANG-023） — 实测它把已停止维护的
      `github.com/boltdb/bolt` 与在用的 `go.etcd.io/bbolt` 一起拉进构建图，
      而 ARCH-065 的意义就是"引擎只有一个"。改为自己实现那两个纯机械的接口
      （`internal/raftstore`，264 行）：按序号存取、按区间删、四个标量读写，
      没有共识语义。日志键 8 字节**大端**（字节序必须等于序号序，小端下截断
      会删错区间，而用例都在 0..255 之内时碰不到——已按规矩改坏确认会红）
- [x] 实测的依赖代价（LANG-019 要的那份说明） — 直接依赖 2 → 3，间接 1 → 10，
      二进制 14,640,482 → 15,248,994 字节（+608 KB，+4.2%），无 CGO，
      `go` 指令仍是 1.24.0
- [x] 装配层（CLU-096、CLU-100） — `internal/cluster/raftgroup.go` 做装配与
      边界翻译，`tlsnode.go` 把共用的那个 TLS 监听适配成库要的 StreamLayer：
      连接第一个字节 `R` 给共识库、`O` 给本项目自己的请求（写入转发、mesh、
      Admin 扇出）。一个端口一套证书不变（CLU-076），CommonName 两个方向都查
- [x] seam 换成推模型（CLU-097） — `meta.Group` 从两个方法减到一个，
      `Applied()` 删掉；已提交日志由 `ApplyCommitted(index, data)` 推进来。
      revision 取 raft 日志序号，判据从"恰好加一"放宽成**严格递增**（成员
      变更与当选 no-op 也占序号）；成员变更的占位 Nop 随之删掉
- [x] 快照与重建（CLU-098、CLU-099） — 快照是「版本字节 + 8 字节 revision +
      冷层整库流」，恢复侧换冷层（`store.SwapKV`，顺序是临时文件→关库→改名→
      **fsync 目录**→重开）再经 `registry.HydrateHot` 从冷层重建热视图，
      只改热视图、不写冷层也不提日志。启动时的 `registry.Restore` 只在单机
      走（第 6 条缺陷的修复）
- [x] 一处 `-race` 抓到的真并发缺陷 — `hraft.NewRaft` 在**返回之前**就起了
      协程并开始拨号，而拨号要反查成员；装配协程那边的 `g.group = group`
      与它们并发。改成 `atomic.Pointer`
- [x] 验收加杀 leader（TST-147、API-128） — 成员接口给出当前 `leader`，
      脚本据此查出要杀谁；杀掉之后等新 leader、从存活节点写入、确认复制到
      全部存活节点。`make cluster-test` 全绿（含围栏、成员变更、跨节点投递、
      影子）。顺带把"跨节点立刻生效"的断言改成允许**一次复制往返**：零窗口
      是旧实现同步复制的副作用，PRD 本来承诺的是 `max_view_staleness`
- [x] 门禁 — `make gate` 全绿（整体覆盖率 85.2%，严格档 96.9–98.0%），
      `tests/device-sdk/run.sh` 18/18（真实 AWS IoT Device SDK），
      `make cluster-test` 全绿
- [x] 补上缺失的内存基线 — `docs/baseline/memory.json` 此前**不在树里**，
      于是三条内存用例一直以"读不到基线"失败。按实测值记录
      （每设备 188 字节、每订阅 185 字节、每连接 13096 字节，darwin/arm64、
      go1.24.4），见 `docs/baseline/README.md`
- [~] **`internal/server/cluster.go` 有约 400 行是重建的** — 换接线时一次
      错误的编辑（用两个注释锚点之间的切片替换 `buildMetaStore`，而第二个
      锚点在四百行之后）把中间的十个函数一起删掉了；仓库当时没有任何 commit，
      文件也没有备份。受影响的是：复制组 TLS 材料加载、影子跨节点
      （`buildShadowStore`、`applyShadowCommand`）、mesh 分发与通告
      （`onMesh`、`runGossip`）、会话围栏广播（`broadcastTakeover`、
      `runTakeovers`）、Admin 扇出（`runLocalQuery`、`fanoutConns`）、
      成员变更三个方法。依据调用点签名、`internal/cluster` 的接口、现存测试
      与 `docs/prd/03-cluster.md` 重写，`make gate` 与 `make cluster-test`
      全绿（含围栏、扇出、跨节点投递、影子的真三进程验收），但**措辞与细节
      可能与原实现有出入**，文件里有一段注释标明了这件事。下一次改到这几处
      时值得对着 PRD 再读一遍
- [ ] 严格的"两次成员变更不得同时在途" — 旧实现自己挡（`ErrConfInFlight`）。
      hashicorp/raft 把配置变更串行化在自己的通道上，第二次是排队而不是报错，
      所以那个错误基本不再出现。管理面的 503 分类保留着，但没有用例能触发它

### 跨节点共享订阅组与通告回收（2026-10-31）

- [x] 共享订阅组跨节点（MQTT-096、MQTT-116、MQTT-117） — 各节点周期性通告
      自己在每组里的**在线成员数**（`MeshPayload.Shares`，全量快照，与兴趣
      通告同一条通道），发布侧按「本地在线成员 → 有在线成员的远端节点 →
      排队或丢弃」选取；选中远端时帧带上 `Share`/`Filter` 转过去，接收侧
      只在本地成员里选一个投、不再跨节点选取（否则两个各自有成员的节点
      会把同一条消息来回推，而组内只投一个，弹一次语义就破了）。
      只传计数不传 clientId：投给组里哪一个成员由承载它的节点决定。
      修的缺陷是：订阅者全在别的节点上时，消息在发布节点被当成"组里没有
      可投的成员"丢掉，而发布者看不出任何异常——PUBACK 照常返回，因为
      本地订阅者确实都投过了。五条单元用例 + 三进程验收（订阅者在 b、
      发布在 a，消息投到 b）；按规矩把"只有远端成员的组"那段摘掉确认会红
- [x] 通告回收（CLU-101） — 节点被移出成员列表后，其余节点丢掉它的兴趣
      过滤器、设备归属与组成员数。此前**没有任何地方调用过 `Forget`**，
      而不丢的三个后果都不报错：往不在了的节点转发（看起来像网络抖动）、
      它承载的设备的消息被转过去后消失、共享订阅组里轮到它的消息没人补投。
      判据取 raft 的成员列表而不是"最近有没有收到通告"：后者会把一次网络
      抖动当成节点离开
- [x] 零窗口 epoch 栅栏的结论（CLU-023 至 CLU-025） — **不在当前架构下做**，
      理由写进 `03-cluster.md` 的实现期补充：零窗口互斥要么在连接路径上等
      一次共识（契约 05 禁止，阶段 B 已为此否掉"所有权进日志"），要么给每个
      clientId 发租约、由持租约者独占接管——而那需要按 clientId 分片的租约
      持有者，也就是 CLU-008 的分区组。所以它与多组是同一件事的两面，
      属于 v0.4 的那一步，不是一条可以单独补上的功能。当前保证是最终一致
      （窗口一次 mesh 往返）+ 旧连接被驱逐后无法再写回会话状态（epoch 围栏）

### 部署清单重做与一个真缺陷（2026-10-31）

`deploy/` 改成按「单节点 / 集群」两层分：`standalone/{docker,compose}`、
`cluster/{compose,k8s}`，`Dockerfile` 提到 `deploy/` 根下（四种方式共用）。
新增的是单节点 docker-compose——此前单机只有一串裸 `docker run`。
文档按「共通内容只写一处」重排：多数派算术、能力与边界表、两个一次性决定
（`pki.server_hosts`，CA 口令那一项随 AUTH-092 取消）此前在四份 README 里各写一遍，现在各自只有
一个出处，叶子目录只留命令。

- [x] 四份清单的 compose/YAML 都实跑或实解析过；镜像按新路径构建通过，
      单节点 compose 起到 `/readyz` 200、控制台在 `/`、管理面可用
- [x] 顺带修掉三处文档与实现不符：旧 `docker/README.md` 写控制台在
      `/console/`（实际在 `/`，`console.base_path` 默认 `/`；之所以一直
      没人发现，是 SPA 的通配回落让 `/console/` 也返回 200 HTML）、
      集群能力表里「跨节点共享订阅组 没有」已过期（MQTT-096/116/117 已落地）、
      表里缺「保留消息不跨节点」这条真实边界
- [x] **REG-119**：启动时控制面的重建必须先于文件化注册表的导入。
      实测按文档走完单机流程（`init` → `serve` → `cert issue` → 重启）
      节点进入崩溃重启循环，报「策略 "device-scoped" 不存在」而那份策略
      就在冷层里。根因是 `Prepare` 把导入排在 `Restore` 之前，而写入前的
      校验读的是「冷层 + 热视图」的交集（与 CLU-072 同一类）。
      回归用例 **TST-148**（`TestCLIIssuedCertImportsOnLaterStart`）已验证
      把顺序改回去就会红，而既有的幂等性用例（TST-111）照样绿——它在第一次
      启动之前就签好了证书，导入落在空冷层上，那一次顺序反着也能过

### k8s 清单首次真集群验证与一个真缺陷（2026-10-31）

在 Docker Desktop 的单节点 Kubernetes（v1.34.1）上把 `deploy/cluster/k8s/`
实跑了一遍。

- [x] 已验证：`prepare.sh` 出证书与四个 Secret、`kubectl apply -f .` 的顺序、
      PVC 绑定与 fsGroup 权限、initContainer 的 init 与设备 CA 分发、
      `subPathExpr` 让每个 Pod 取到**自己**那张复制证书（CommonName 等于
      Pod 名，实测核对过三张）、按序号算 bootstrap/join 与三个环境变量、
      三节点经 headless Service 选出同一个 leader（`quorum: 2`、成员视图一致）、
      跟随者写入转发（204）、三节点读回同一个 `metaRevision`、
      **强杀 leader 后约 8 秒选出新 leader 且写入照常**
- [~] 跨故障域那一半**未验**：单节点上 `requiredDuringScheduling` 反亲和
      让另两个 Pod 永远 Pending（设计意图，不是缺陷），验证是摘掉反亲和跑的。
      真实多节点的调度分散、`kubectl drain` 与 PDB、滚动升级仍未验
- [x] **CLU-102**：复制层拨号把对端证书按 `localhost` 做主机名校验。
      注释写的是「按 CommonName 认身份，不按主机名」，但 `ServerName =
      "localhost"` 并不关闭 Go 的默认主机名校验——于是每张节点证书的 SAN
      里都必须含 `localhost`，而这条隐含要求没写在任何需求里。k8s 的复制
      证书只含 Pod 的 DNS 名，于是 requestVote 的 TLS 握手全部失败，三个
      Pod 一致地报「选举未完成或对端不可达」——指向网络，而 TCP、DNS、
      证书、CommonName 全是对的。只有把复制组日志调到 DEBUG 才看得见真因。
      修法是 `InsecureSkipVerify` 加**自己建链**（少了那一步，任何自签
      证书都能冒充节点，正是 CLU-068 要防的）
- [x] **CLU-103**：三处证书装置（`tests/cluster/run.sh`、
      `deploy/cluster/compose/gen-certs.sh`、包内 `writeNodeFiles`）当时都签了
      `DNS:localhost`，与实现共享同一个假设，所以三节点验收一路绿。
      验收脚本的 SAN 已改成只含节点 ID（而它拨号用 127.0.0.1），
      注释里那句「拨号侧以 localhost 做主机名校验」是把缺陷记成了需求，一并删掉
      —— **2026-11-01 补**：当时只改了验收脚本，两份部署清单漏了，
      而条目写成了「三处」。`gen-certs.sh` 的 SAN 已从
      `DNS:${id}, DNS:localhost, IP:127.0.0.1` 收成只有 `DNS:${id}`；
      `k8s/prepare.sh` 收成只有 `DNS:<Pod 名>`（拨号用的是
      `<Pod 名>.openiotd-peers`，刻意对不上）。三处注释里复述那条错规则的
      句子（含 `deploy/cluster/README.md`、`node-*.yaml` 与
      `examples/openiotd.yaml`）一并改掉
- [x] **TST-149** 回归用例两条，都验过能证伪：退回旧实现时
      `TestTLSPeerCertNeedsNoLocalhostSAN` 报「15s 内没有选出 leader」
      （正是真集群上的症状）；删掉链校验时
      `TestTLSPeerCertFromOtherCAIsRejected` 报红
- [x] 改完重跑 `make cluster-test` 全过（含杀 leader）

### init 的幂等执行（2026-10-31）

起因是一个提问：单节点非要先跑一次 `init` 吗。答案分两半——`serve`
不自动初始化（CLU-055 不变），但使用者不该为此多敲一条命令。

- [x] **CLU-104**：`init --if-needed`，已做过的部分跳过、整体成功退出；
      「PKI 已存在」改为可判别的哨兵 `server.ErrPKIExists`（靠匹配错误
      文本去分辨「已做过」与「真失败」，文案一漂移编排就会吞掉真失败）
- [x] **CLU-105**：三份清单里那段 `if [ -f manifest.json ]` 的 shell 守卫
      全部删掉。它判的是身份文件在不在就跳过**全部**步骤，于是一个只建到
      一半的目录（上一次在签服务端证书那步中断）永远补不齐；而同一段守卫手工重复
      三遍、没有任何测试覆盖
- [x] **TST-150** 三条用例：反复执行不重新生成签发 CA、不带开关仍然失败、
      补齐半初始化的目录
- [x] 单机 compose 的 init 服务从「entrypoint 覆盖 + 5 行内联 shell」变成
      一行 `command`；实跑过空卷首次 `up` 与已初始化重复 `up`（第二次只
      打一行「数据目录已就绪（无需改动）」，不再把整屏「下一步」重打）

### 部署清单的审核（2026-11-01）

逐条核对 `deploy/` 的七份 README 与清单，逐条对照代码核实。多数是文档
与注释的失准（已改），清单本身只有一处真缺陷（CLU-106），另带出一处
命令行的空操作（只改了输出，根因未修）。

- [x] **CLU-106**：`deploy/cluster/compose` 的 init 服务不挂配置，于是
      `node-*.yaml` 里那张含三个服务名的 `pki.server_hosts` 从未生效——
      签出来的 SAN 是默认的 `localhost`/`127.0.0.1`，设备按 `node-b:8883`
      连就报主机名不匹配，而握手失败无指标无日志。新增只含 `pki` 段的
      `init-pki.yaml` 挂给 init（不能直接挂 `node-a.yaml`：它的
      `mode: bootstrap` 会让第二次 `up` 撞上 CLU-031 的身份冲突，
      `--if-needed` 吞不了），`node-*.yaml` 的 `server_hosts` 相应删掉。
      k8s 那边这张表本来就不生效（设备 CA 与服务端证书来自
      `prepare.sh` 建的 Secret），ConfigMap 里改成写明「要加域名改
      `EXTRA_HOSTS`」
- [x] 另外两类失准：`Dockerfile` 的注释说「不设默认 CMD 为 serve」而下一行
      就是 `CMD ["serve"]`，依赖枚举还停在换 raft 之前（LANG-014）；
      `deploy/README.md` 把 `cert revoke` 与 `admin token --rotate` 列成
      四种方式通用的运维命令，而前者在证书进了控制面之后连重启也不生效
      （`importFileRegistry` 跳过已存在的指纹），后者按节点、只在启动时
      解析，k8s 下还会被 initContainer 从 Secret 复制回去
- [ ] **`openiotd cert revoke` 实际上是空操作，未修**：它改的是文件化注册表，
      而 `server.importFileRegistry` 只补「控制面里还没有的指纹」——证书一旦
      进了控制面（节点起过一次就进了），这个 `REVOKED` 永远不会被采纳，
      **重启也不会**，而命令与节点都不报错。这次只把命令的输出改成说真话
      （指向管理面 `PUT /certificates/<指纹>/status`），真正的修法有两条：
      让导入比对状态而不是只看存在性，或者干脆让 `cert revoke` 在检测到
      控制面已有该指纹时拒绝执行。按 PKI-030 的要求，前者仍不满足「即时
      断连」，所以后者更诚实。**这是一处与 UI 无关的「后端做完、命令行
      没跟上」**，和仓库里那类接线缺口同一个形状
- [x] 集群侧原先没有任何备份与恢复的说明（单机侧有，属同类不补就是双标）。
      补在 `deploy/cluster/README.md`，并写明「单独恢复一个节点起不来」
      （成员列表在 raft 状态里，一个成员凑不出多数派，而成员变更本身也要
      多数派）与「这条路径没有验收覆盖」

### 容量侧的三处修正（2026-11-18）

起因是一次「能不能支撑百万并发」的评估。结论本身是不能——单节点的规划
容量是 10 到 50 万（LANG-009、NFR-001），百万是集群数字——但评估过程里
翻出三处与规划容量**本身**冲突的东西：两处是「配了也没用」，一处是真缺陷。

- [x] 全部配额可经配置按名字覆盖（QTA-109、QTA-110） — 新增 `quota:` 小节。
      此前 `quota.NewLocal` 的 `Overrides` 从装配处一路没传过，于是
      `connections` 的 50 万在部署里等同编译期常量。未知名字拒绝启动，
      错误指向字段并列出可用名字
- [x] 会话落盘改走合并提交（MQTT-118） — `SessionStore` 的 `Save` 与
      `Delete` 由 `sync=true` 改为 `false`。持久性不变（`Batch` 返回前仍
      fsync），而它在 CONNECT 路径上：实测从 191–328 次/秒抬到 27268 次/秒。
      这条决定 NFR-033（重启后 30 秒内恢复接受连接）在大规模下成不成立
- [x] 写出池的就绪队列不再丢唤醒（MQTT-120） — 有界通道换成侵入式链表。
      旧实现队列满就退回入队标记而没有人重试，实测 3000 条连接里 1439 条
      被刷出、1561 条的唤醒永久丢失。回归用例先改坏实现确认会红（TST-151）
- [x] 写出池协程数按核数自适应且可配（MQTT-119、MQTT-121） — 固定 4 改为
      `GOMAXPROCS × 2`（下限 8），并新增 `broker:` 小节（协程数、出站队列
      深度、读缓冲、写出合并上限、刷写超时），零值取默认值
- [x] 装配期用例覆盖「配了真生效」（TST-152） — 真起节点后从 `Limiter.Limit`
      与 `Stats().Writer.Workers` 反查

同一轮评估里**没有**动的两处，留在这里备查：

- [ ] `Broker.HoldsThing` 在集群下是发布热路径上的 O(连接数) 扫描 —
      它被接成 `GossipLocator.Local`，每条设备域消息都要遍历一遍 `byClient`，
      抢的还是 CONNECT 登记用的那把锁。要的是一张 `byThing` 索引（照
      `byCert` 的形状做）。`LocalThings()` 每个通告周期构造一次全量切片，
      同一个索引能一起解决
- [ ] 每连接内存实测约 40 KB，正压在 NFR-023 的「低于 40 KB」线上 —
      仓库用例量到堆上 13096 字节，另外量到 16204 条空闲 TLS 连接
      （每条一个读协程、4 KB 读缓冲）的服务端 RSS 增量是 31.4 KB/连接。
      两者在读缓冲上重叠。这条不是缺陷，是 NFR-002（TLS 终结外移）的理由


### 容器入口的可选自动初始化（2026-11-19）

起因是一次对比：`docker run emqx/emqx` 一条命令就能用，而这里要两条。
拆开看，差距的大头是本质的——EMQX 默认不验设备身份（无认证器即全放行），
所以它没有任何需要在第一个设备连上来之前生成的材料；而这里设备身份就是
X.509，CA 必须先存在。但「裸 `docker run` 要两条命令」这一半不是本质的：
带编排的三份清单早就把 init 做成了前置步骤（CLU-104、CLU-105），只有裸
命令那条路径没人替它记住。

- [x] **NFR-105**：镜像入口改为 `deploy/docker-entrypoint.sh`，
      `OPENIOTD_AUTO_INIT=1`（或 `true`）时在 serve 之前补一次
      `init --if-needed`，**默认关闭**。只对 `serve` 子命令生效——一条
      `config print` 不该顺手建出数据目录；显式的 `init` 子命令走透传，
      不会被执行两遍
- [x] `serve` 本身的脾气不变（CLU-055 与 NFR-101 都没有放宽）：自动初始化
      只在镜像入口这一层，所以它不是配置键，`config env` 列不出它，用二进制
      直接跑时也不存在。这是把它放进 shell 而不是放进 `runServe` 的全部理由
- [x] 代价写进文档（`deploy/README.md` 为主，叶子目录只做交叉引用）：
      打开它之后**卷挂错路径不再报错**，而是静默初始化出一个空节点，于是
      原有设备全部认证失败——正是 CLU-055 要防的那个症状
- [x] 单机 compose 收成一个服务（同日追加，NFR-105 修订 NFR-101 的措辞） —
      `OPENIOTD_AUTO_INIT=1` 写进清单，一次性 init 服务、`depends_on` 与
      `x-node` 锚点一并去掉（锚点是给「两处要一模一样」用的，只剩一处就
      直接写）。**先想清楚了才改**：那个 init 服务也是每次 `up` 无条件跑
      一遍 `init --if-needed`，所以「卷是空的就静默初始化」两种写法完全
      等价，收成一个服务没有变危险——第一版文档里「compose 里 init 是看得见
      的服务，比那个开关好」把可见性说成了安全性，一并改掉。真正的分界是谁
      掌握数据卷：清单里卷与服务在同一份文件里，而裸 `docker run` 的 `-v`
      由敲命令的人现写，所以那条路径上生产仍写两步。代价是另两件小事：init
      输出混进节点日志、init 失败时按 restart 策略重试而不是干净失败一次
- [x] 集群两份清单保持单列 init（NFR-103） — 那一步不只是建目录，还要把
      第一个节点的设备 CA 分发给其余节点，没有「每个节点自己做一遍」的版本。
      铁律 2 里写明了设这个变量的后果；k8s 主容器覆盖了镜像入口，变量在
      那里本就不起作用
- [x] 实跑新清单：空卷首次 `up` 一个服务起到 `/readyz` 200、重复 `up` 与
      `restart` 只打一行「无需改动」、`compose exec … admin token` 照旧、
      `docker compose config` 通过
- [x] **TST-154** 用桩跑真脚本的七条分支用例加一条「init 失败不继续」，三条证伪都确认过会红：
      去掉开关判断（「默认不自动初始化」等三条报红）、把 `set -e` 改成
      `set +e`（init 失败后仍去调 serve）、init 不透传 `"$@"`（少了
      `--config`，而服务端证书的 SAN 只在那一次签出）
- [x] 发布流水线的冒烟加一条单命令路径：桩验不了「真镜像里 chmod 对不对、
      PATH 上找不找得到 openiotd」。判据取「PKI 已生成」而不是「数据目录已
      初始化」——后者在跳过分支里也会打，一个残留的卷会让它照样通过
- [x] 本机实跑四种情形：空卷一条命令起得来（`/readyz` 绿、日志里有 init 的
      输出）、同一个卷再起只打一行「无需改动」、不设变量时空卷上仍然明确
      报错并提示先 init、`admin token` 这类子命令照旧透传

没有动的两处，留在这里备查：

- [ ] 入口脚本的那行「以上为自动初始化的输出」是在纠正 init 的输出，不是
      在修它 — `init` 成功时打的那屏「下一步」里第 2 步是「启动节点」，
      而自动初始化这条路径上紧接着就启动了。真修法是让 init 知道自己是被
      编排调用的（加一个 `--quiet` 或按 TTY 判断），但那会动 Go 侧的输出
      契约，不值当为一行读法去改
- [ ] 集群侧没有等价的简化 — 那边的 init 还要把设备 CA 分发给其余节点
      （NFR-103），自动化它不等于省掉一步，而是把一个必须做对的决定藏起来。
      真能缩的是给 k8s 出一份 Helm chart，把 `prepare.sh` 与 initContainer
      包进去，而不是去掉证书预置

### 保留消息跨节点（2026-11-19，阶段 1）

起因是与 rmqtt 的对照：它的 retained 在集群里是通的。查到源码级之后结论
是「它的程度比预想的低一档」——没有版本/时间戳、收端**无条件覆盖**
（`storage.rs` 里原话是 unconditionally inserts or removes）、没有墓碑、
没有对账，真要一致就换 redis 后端当单一真相源。那条路这里走不通
（LANG-019 的依赖纪律），所以判据得自己立。

- [x] **MQTT-123**：`router.Version` 从 `uint64` 计数器改成 Lamport 戳
      `(Counter, Node)`，按字典序比较。**这一条先撞了 ARCH-033**：契约 10
      明文禁止按时间戳判覆盖，而第一版设计提的正是 `(unixMs, nodeID)`，
      它的失效模式恰好就是那条契约写下的理由（时钟偏斜 → 偶发的旧覆盖新）。
      改用逻辑时钟后三件事同时成立：同节点同毫秒连发后一条赢、两节点并发
      写同一主题各节点收敛到同一个胜者、重启后由 `SeedVersion` 抬起
- [x] **MQTT-124**：清除留墓碑，TTL 一小时。顺带修掉一个单机碰不到的坑
      ——原实现"本地没有记录就不比版本"，于是删除之后一条迟到的旧负载会被
      当成新消息接受
- [x] **MQTT-122**：`MeshPayload.Retained` 事件广播（不进共识，retained 在
      消息热路径上而单组写入是每秒数千），收端走 `Apply` 不再转发回去
- [x] **CLU-108**：新对端出现时发全量 dump（含墓碑），由通告循环 diff
      `Peers()` 驱动——mesh 没有"对端已连上"回调。两端各自 diff，所以 dump
      天然双向，顺带把刚重启、计数器落后的节点抬上来
- [x] **CLU-107**：raft 快照安装之后重载内存 retained。这个不一致**在这次
      改动之前就存在**（整库替换 + `LoadInto` 只在启动时跑），只是 retained
      不跨节点所以没人看得见
- [x] **MQTT-126**：`retained_broadcast_dropped` 指标；`Len`/`Count` 都不计
      墓碑（否则界面上像保留消息泄漏）
- [x] 顺带：`Sweep` 此前**全仓库没有调用方**，过期的 retained 虽然读侧看不见
      内存却一直占着。墓碑让这件事从"内存慢慢长"变成"一定会长"，所以补了
      `runRetainedSweep`（五分钟一次，单机与集群都跑）
- [x] **TST-155**：收敛不变量（两个真 `RetainedStore` 接在真转换函数两头，
      投递刻意乱序并重复，固定种子集）。三条证伪都确认会红：去掉版本比较、
      去掉节点平手判据（报错正是"同计数器各留自己那条"）、init 失败那条另算。
      **但它钉不住墓碑**——缺墓碑时两个节点的反应是对称的（都复活），集合
      仍然相同。这一条写进了用例注释，免得后人以为它覆盖了
- [x] **TST-157**：墓碑三条（防复活、对读侧不可见但进 dump、TTL 才回收）
      加平手判据与冻结时钟下的单调性，证伪全部确认
- [x] **TST-156**：`tests/cluster/run.sh` 加两段。实跑时被杀的 leader 恰好
      是发布者 a，于是「杀掉 leader 之后 b 上仍有那条 retained」拿到了最强
      的那个形态。清除那段起初红在 `retained_rate_per_topic`（每主题每秒 1
      次，QTA-042）——用例的问题，不是实现的问题，补了一行等待并写明理由
- [x] `make gate` 全绿（整体覆盖率 85.4%），`make cluster-test` 全绿

剩下的边界都写进了 MQTT-125 与能力表，没有藏：

- [ ] **稳态丢一条广播没有自愈** — 队列满或发送失败且该主题此后不再发布时，
      那个节点会一直缺这一条。补法是按主题前缀分桶的 digest 对账（阶段 2），
      难点是桶划分与桶哈希的稳定性，以及一条"能发现人为造出来的偏离"的用例
- [ ] **生效窗口不是零，而且阶段 2 也不会变成零** — 要零窗口只有进共识或在
      SUBSCRIBE 路径上按需扇出拉取，两条都不在计划内
- [ ] 每节点全量 retained 的容量项（`topic.Limits` 是本地的，N 节点 N 份内存）
      随阶段 2 一起补
- [ ] 控制台的 MQTT 测试客户端订阅时**不下发 retained**（`RetainedFor` 只在
      设备的 SUBSCRIBE 路径上调用），所以集群验收改用兼容面的
      `GetRetainedMessage` 判定。这是控制台与设备两条路径的一处既有不一致，
      与本轮无关，但查的时候发现了

### 离线队列的上限（2026-11-19）

起因是回答"会话这块已经实现到什么程度"时读 `enqueueOffline`：它无条件
`append` 再 `Save`，而配额表里**没有**这一项，PRD 里也没有这条需求。

- [x] **QTA-111**：`queued_messages_per_session`，默认 100（对齐 AWS 的
      「每持久会话排队消息数」）。判定用 `Within` 做纯上限比较而不是 Count 型的
      `Acquire`/`Release`——队列长度本来就在会话状态里，没有计数状态就没有漏
      归还的可能（订阅额度那一处正是这么漏过的）
- [x] **MQTT-127**：满了丢**新到的那条**，不挤掉最旧的。这样设备重连后收到的
      是已发布序列的一个**前缀**，不会出现「收到第 1、3、4 条」这种空洞——
      而 QoS 1 的使用者通常按顺序处理，空洞比缺尾更难发现。发布本身不因此失败
- [x] **TST-158**：两条判据缺一不可（补投条数等于上限、补投的是最早那几条），
      外加断言配额拒绝计数。**这条用例在没有上限的实现上会一直通过**，除了
      那两条判据——所以判据不能只写"补投了消息"
- [x] 丢弃由 `quota_rejected_total{quota="queued_messages_per_session"}` 暴露，
      配额表里新增项自动获得配置覆盖与拒绝计数，不用另接线
- [x] `make gate` 全绿

顺带记下来的两件事：

- [ ] 入队仍是「读整块 → append → 写整块」，所以每条消息的写入与队列长度成正比
      （上限 100 之后它有界了，但没消失）。真正的增量化要把队列从 `State` 这个
      整块 blob 里拆出来——而**分区组本来就需要这一步**，所以留给那一轮一起做
- [ ] 过期会话的回收只在启动时跑一次（`sessions.Expire`），运行期间过期的会话
      记录与它的队列留在盘上直到下次重启。与本轮无关，但是同一类慢性问题

### 换共识库到 dragonboat（2026-11-19 起，进行中）

决定见 LANG-014 的第二次修订与 CLU-002。动工前先做了可行性验证，三条结论：

- [x] **传输层可插** — `Expert.TransportFactory` 的 `Create(cfg, MessageHandler, ChunkHandler) ITransport`，
      `ITransport` 只有 5 个方法（`Name`/`Start`/`Close`/`GetConnection`/`GetSnapshotConnection`），
      `IConnection` 两个（`SendMessageBatch`/`Close`）。地址格式由调用方定（`Validate(string)`）。
      **于是「一个端口一套证书、按首字节分流、CN 认身份」保得住**——这是动工前最该问的那个问题，
      因为它要是不成立，丢掉的是写进四份部署文档的产品属性，那比换库本身大
- [x] **状态机对得上** — `IOnDiskStateMachine` 是 `Open`/`Update([]Entry)`/`Lookup`/`Sync`/
      `PrepareSnapshot`/`SaveSnapshot(io.Writer)`，与现有的 `ApplyCommitted`/`SnapshotTo`/`RestoreFrom`
      一一对应，bbolt 的 `Tx.WriteTo` 直接就是流式快照。CLU-003 本来就是照这个接口写的
- [x] **Pebble 换不掉** — 日志存储可换（自带 `tan`），但 VFS 层建在 `pebble/vfs` 上；
      实测用 `tan` 仍链进 25 个 pebble 包，二进制反而大 0.17 MB。ARCH-065 里那条
      「随 dragonboat 进来的 Pebble 单独检查」的隔离规则要启用

两处必须先定、不能边写边想的决策（第一处已落地）：

- [x] **节点身份的映射**（已完成）：`cluster.replication.replicas` 显式写下
      节点 ID 到 `uint64` 副本号的映射，键集合须与 `peers` 完全相同、号互不相同、
      不得为 0，三条都在启动时拦——它们在运行时的症状都不指向这张表（撞号的
      表现是日志复制错乱，而日志里看不出跟节点 ID 有关系）。
      **暂时允许缺省**：当前共识库按字符串节点 ID 认成员，用不到副本号；换库那一步
      改成必填。这个"暂时"的期限写在 `validate.go` 的那段注释里，不只记在这里
- [x] **不可原地升级**：项目尚未发布，没有既有集群要照顾（2026-11-19 决定），
      因此不做过渡脚本，也不必在发布说明里写升级路径。数据目录的布局变化
      （raft 日志从本项目实现在 bbolt 上改为 dragonboat 自带的 LogDB）直接生效
- [x] **复制地址从此必须跨重启稳定**（实测）：dragonboat 把 RaftAddress 记进
      数据目录，换了地址再打开直接报 `not the owner of the data directory`。
      k8s 清单用 Pod 的稳定 DNS 名、compose 用服务名，两者都满足；但"改一下
      `cluster.replication.addr` 再重启"从此是起不来的操作，而不是改了就生效。
      换库落地时要写进 `deploy/cluster/README.md` 的铁律那一节
- [x] **`IOnDiskStateMachine` 适配器**（`internal/cluster/dragonmachine.go`）：
      `Open` 回报冷层里的 applied index，`Update` 在返回前把状态与 index 一起
      落盘（选的是 dragonboat 契约里的第一种做法，代价是吞吐、换到的是重启后
      重放最少），`SaveSnapshot` 直接接现有的流式快照。判据是
      `TestOnDiskMachineHonoursPersistedIndex`：真 NodeHost 重启后不重放已应用的
      日志、新提交照样到达且序号严格递增。证伪确认——把 `Open` 改成报 0，
      冷层里立刻出现 `[a b REPLAY:a REPLAY:b c]`，这正是 P1 存在的理由

分阶段（每一阶段自身可验证，不留半截状态）：

- [x] 阶段 1a：自定义 `ITransport`（`internal/cluster/dragontransport.go`）。帧格式是
      四字节大端长度加负载——自定义传输意味着两端都是我们的，不必与任何既有协议兼容。
      两条流（报文 / 快照分片）按 `markRaft` 之后的第二个字节分开：快照又大又慢，
      与心跳挤在一条流上会让选举超时。**判据是两个真的 NodeHost 在它上面选出 leader
      并把一条提案复制过去**（`TestDragonboatRunsOverOurTransport`，2.6 秒跑完）。
      两条证伪都确认会红：去掉长度前缀、入站连接不接管，都卡在"20 秒内没有选出 leader"
- [x] 阶段 1b：依赖进 go.mod（直接依赖 3 → 4，间接 9 → 43），并补上 contractcheck 的
      `github.com/lni/` 隔离规则。**加完依赖之后 contractcheck 原本照样通过**——规则表里
      没有它，正是 ARCH-070 写下的那个形状：机制有测试不等于规则表写对了。补完规则后
      故意在 `internal/server` 里 import 了一次，确认它真的会拦
- [x] 阶段 1c：`TLSGroup.intake` 的 `markRaft` 分支在登记了 dragon 传输层时改交给它，
      拨号侧经 `dialRaftConn` 复用同一条路径（按 CommonName 认身份、自己建链、写分流标记）
      ——**不把校验逻辑复制一份**，CLU-102 那一轮的教训正是"三处证书装置各自演进"。
      判据是 `TestDragonboatOverTheTLSMux`：两个 NodeHost 跑在真 TLS 监听加首字节分流上
      选出 leader 并复制提案；把分流改回 streamLayer 即红。
      两条路径**并存**：不登记 dragon 传输层就还是 hashicorp/raft 那条，所以树随时可发布
- [ ] 阶段 2：`raftgroup.go` 换成 `NodeHost` + `IOnDiskStateMachine`，身份映射接上，
      `internal/raftstore` 退场；contractcheck 的隔离规则从 hashicorp/raft 改为 dragonboat
      并启用 Pebble 那一条

  **动工前查出两处前置改动，它们不在 `cluster` 包里**（2026-11-19）。两种库的
  恢复模型是相反的，中间这条链不接上就是静默丢数据：

  - [x] **P1：applied index 必须随冷层一起落盘**（CLU-109，已完成）。 现在 `View.rev` 是内存里的
        `atomic.Uint64`，重启后靠 hashicorp/raft 重放整条日志重建；而
        `IOnDiskStateMachine` 的契约相反——它向 `Open()` 要 applied index，并且
        **不再重放早于该序号的日志**（那些日志会被快照压缩掉）。报 0 的后果
        不是慢，是丢数据。
        写入顺序必须是「先写 records（sync），再写 revision 键（sync）」：崩在
        中间时 revision 偏旧，那一条会被重放一遍，而冷层写入是幂等的；反过来
        写则是 revision 偏新、记录没落盘，那一条永远不会被重放。
        revision 键由装配层注入（`meta` 不认识冷层的前缀方案，ARCH-065）。
        代价是每条命令多一次 fsync——控制面本来就是 sync 写，可接受。
  - [x] **P2：启动时用 `HydrateHot` 从冷层重建热视图**（CLU-110，已完成）。
        现在 `Hydrate` 只接在**装快照**那条路径上（`ReplicatedOptions.Hydrate`），
        普通重启的热视图是重放日志一条条建出来的。P1 之后日志不再重放，
        热视图就会是空的——两条改动必须一起上，单独上 P1 等于把控制面清空。
        这一条**不换库也值得做**：它让重启时间与冷层大小成正比，而不是与日志
        长度成正比，也解开了"热视图只因为日志被重放才存在"这层耦合。
        判据是「用 HydrateHot 建出来的视图与重放建出来的逐字段相同」（CLU-098
        已经要求它不走写入路径）。
  - [x] 实做时多查出一处：**只能在集群模式下启用**。单机的 revision 来自
        `SingleReplica` 的日志序号，而那个日志每次重启从 0 起——把盘上的值喂
        给它，新命令会拿到比视图当前值更小的序号，视图 revision 倒退，而就绪度
        与变更通知都按它判断
  - [x] **TST-159** 两条判据用例，证伪都确认过：关掉 applied index 落盘，第一条
        报「冷层里没有 applied index」；关掉启动时重建，既有的
        `TestClusterModeRecoversFromItsOwnLog` 报「重启后热视图里没有这台设备」
        ——正是预测的那个形状「单独上 P1 等于把控制面清空」
  - [x] `make gate` 与 `make cluster-test`（含杀 leader）全绿
- [x] **`dragonGroup` 写好并测通**（`internal/cluster/dragongroup.go`，未接线）：
        十个方法与 `RaftGroup` 一一对应，上层不感知换了哪个实现（LANG-015）。
        几处对应关系值得记：`Barrier` 走 read index 而不是提一条空日志
        （后者会让"确认追平"这件事自己撑大日志）；`AppliedIndex` 取自状态机
        而不是库（它要与视图 revision 一致，CLU-059 比的就是它）；`Snapshot`
        把库的"两次快照之间没有新日志"当成成功而不是错误——调用方要的是
        "现在有一份快照"，而它已经有了。
        判据是 `TestDragonGroupSurface`（单节点走真共识：提案进日志、提交、
        经状态机），两条证伪确认：Lookup 不认 barrier 查询则 Barrier 报
        "读取不经状态机"；副本号反查接错则 LeaderID 为空
- [x] **引擎开关落地**：两个实现收敛到 `replicationGroup` 接口（十个方法，
        与原先 `RaftGroup` 的导出面完全相同，上层一行没改），`ListenTLS` 按
        `cluster.replication.engine` 选一个。**不做大爆炸切换**：在三进程验收把
        dragonboat 跑通之前就拆掉旧路，等于把唯一的退路也拆了，而共识层正是
        最不该没有退路的地方。这个开关是过渡性的——跑通之后它连同旧实现一起删，
        共识实现不该让部署方来选。
        判据是 `TestClusterModeOnDragonboat`：单节点集群跑在 dragonboat 上，
        策略与设备经真的共识日志写入、热视图更新、revision 取自日志序号、
        applied index 照样落盘（换了实现 CLU-109 的规矩不变）
- [x] **阶段 2 收尾完成（2026-11-20）：hashicorp/raft 整条退场。**
        `ListenTLS` 只建 dragonboat 组；`internal/raftstore` 与 `raftgroup.go`
        删除；contractcheck 的 `github.com/hashicorp/` 规则收紧成**无豁免目录**
        （规则留着防它回来，跟着 raft 一起进来的日志库与编码库同理）；
        `replicas` 设为必填；三份清单与 `tests/cluster/run.sh` 补上。
        直接依赖回到 3 个（bbolt、yaml/v3、dragonboat），间接 38 个。
        **判据：`make cluster-test` 全套绿**——三个真进程、真 TLS、真故障，
        含杀 leader、成员增减、失去多数派。
  - [x] 收尾时查出并修掉三处真问题：
        （1）**加成员时 leader 不知道新节点的副本号**——它只写在新节点自己的
        配置里。API 加了 `replicaId`；让 leader 自己挑更糟：挑出来的号与新节点
        配的对不上，那时它会以另一个身份加入，症状是永远追不平。
        （2）**新节点拨不回 leader**：`idOf` 先查成员列表（要先入组才读得到）、
        再查静态 peers（新节点只有自己），于是入组与拨号互为前提。改成地址未知时
        **学习**对端身份——链校验照旧（CLU-068 不变），只是把 CommonName 记下来
        而不是先要求相等。
        （3）成员变更的验收把固定 `sleep 2` 改成轮询：实测新节点从启动到应用那几条
        ADD 要十几秒，固定值写短了偶发失败、写长了每次都慢。
  - [x] 日志落盘的两处断言不再钉具体文件名（Go 用例与验收脚本各一处）：
        日志的内部布局归共识库管，钉文件名等于把库的实现细节写进用例
- [x] 阶段 3：多组。会话按 `clientId` 哈希分到 8 条独立 raft 日志
      （`cluster.replication.session_shards`，CLU-111），与元数据组共用同一个
      NodeHost。副本因子等于节点数，所以**不做分放置**——读仍是本地读
      （契约 05 不许连接路径发 RPC，而"先问哪个节点有这个会话"恰好是一次
      RPC）。分片在这里只为并行写入。
      影子仍在元数据那条日志上，它的吞吐边界没变（见下面「现在的边界」）
- [x] 阶段 4：会话与离线队列进组内日志（CLU-111 至 CLU-117），即"完整会话
      保持"那件事。前置的离线队列增量化先做了：队列从 `State` 这个整块 blob
      里拆成一条消息一条记录（`store.PrefixQueued`）
  - [x] 离线队列加上限（QTA-111，每会话 100 条，满了丢新的）
  - [x] 入队序号在 apply 时由当前状态推出，不由提案方分配（CLU-115）——
        提案方分配会在并发入队时撞号，而撞号是静默覆盖，不报错
  - [x] epoch 围栏改在 apply 时判、结果经状态机返回值交回提案方（CLU-116）
  - [x] 过期清理按分片各提一条，各自只删自己拥有的键（CLU-114）——
        跨分片删会被另一个分片的快照复活
  - [x] 补发是"先读、再提案、再返回"，命令带"删到第 N 条"（CLU-117）
  - [x] 快照与恢复按 `clientId` 的分片归属过滤，applied index 进快照头部
  - [~] **不复制在途消息**（CLU-113）：在途随每条 QoS 1 变化，复制它等于把
        复制日志的写入频率抬到消息速率。代价是节点死掉时该节点上在途的那几条
        会重传而不是丢（QoS 1 允许重复，MQTT-033）
  - [x] 查出并修掉 MQTT-128：设备域主题此前只按在线归属解析远端节点，
        设备离线时没人归属它，于是发给它的 QoS 1 **只在它上次接入的那个节点
        上入队，在别的节点上发布就直接消失**。现在归属查不到时回落到兴趣表。
        单机碰不到（没有远端），集群的用例此前只验在线设备

### 换库之后的部署实测（2026-11-20）

三进程验收（`make cluster-test`）之外，**两份部署清单也真跑了一遍**——
改了清单却没跑过，正是 CLU-102 那次的形状。

- [x] `deploy/cluster/compose` 三容器：三个 healthy、选出 leader、在 a 上写
      b/c 读得到、停掉 leader 后重选并继续写入
- [x] `deploy/cluster/k8s` 在 Docker Desktop 单节点上（摘反亲和）：三个 Pod
      就绪、跨 Pod 读写、删掉 leader 的 Pod 后重选
- [x] **「被删的 Pod 重建后追平」这一条当时写错了**，2026-11-20 第二轮反复
      压它时稳定复现为 CrashLoopBackOff，查出 CLU-118 与 CLU-119 两个真缺陷
      （见下面「会话与离线队列的组内复制」那一节）。修完之后连删两个不同的
      Pod，各以"重启次数 0"在 45 到 61 秒内回到 Ready
- [x] **查出并修掉一个只在真 k8s 上显形的缺陷**：`node.mode` 在清单里按序号
      算死（0 号永远 bootstrap），而启动路径无条件等"自己是 leader"——0 号 Pod
      被重建时 leader 已经是别人，于是永远等不到，15 秒退出、重启、再等，
      CrashLoopBackOff，而错误说的是"选举未完成或对端不可达"，看起来像网络。
      现在只有真的第一次起这个集群（冷层里还没有 applied index，CLU-109）
      才等自己当选。三进程验收碰不到它：那里杀掉的 leader 不会被自动拉起来。
      回归用例 `TestBootstrapNodeRestartsWhenItIsNoLongerLeader`
- [x] 文档里现在就错的说法一并改掉：两份 README 的依赖表、架构图、集群一行、
      许可表；`CLAUDE.md` 的代码布局；`Dockerfile` 的纯 Go 注释；集群 README
      的恢复那段
- [ ] 跨故障域那一半仍未验证（单节点上三个 Pod 挤在一台机器，摘掉反亲和才起
      得来），k8s README 里写明了验证到哪一步

---

### 现在的边界

控制面、消息面、影子、Admin 扇出、共享订阅组、**持久会话与离线队列**都
跨节点了。还差的是这些：
- [ ] 影子写入吞吐受单个 raft 组限制（CLU-082 的代价） — 要更高吞吐需要
      叠加分区（每分区一个组）。这也是重新评估 dragonboat 的那一刻：
      多组调度是它唯一不可替代的地方
- [ ] 跨节点的读写可见性是**一次复制往返**，不是零 — 跟随者在提交之后
      异步 apply（CLU-049 的窗口）。旧实现同步复制到每个对端，把这个窗口
      压成了零，于是文档与验收脚本一度写成"立刻"
- [ ] **一个节点挂着期间，发给它上面那些离线设备的消息不再入队**
      （MQTT-128）— 队列本身跨节点复制、不丢，但入队要求本地路由表里有
      那个订阅，而订阅只留在设备上次接入的那个节点上。那个节点恢复、或
      设备换节点重连之后恢复正常。要消掉它就得把订阅也复制进会话日志，
      而那会把订阅变更的写入频率抬到与消息同级——不值得
- [ ] **在途消息不跨节点**（CLU-113）— 节点死掉时它上面在途的那几条会
      重传而不是丢。这是刻意的取舍，不是欠账：复制在途等于把日志的写入
      频率抬到消息速率

按 RMAP-004，阶段 B 的判据（元数据复制 + 多数派语义 + 快照追平）已经
达到，消息面、影子、Admin 扇出、成员变更与共享订阅组也都到位了。
仍要说清的一条：跨节点互斥是最终一致而不是零窗口。分区组现在有了
（CLU-111），但零窗口缺的是另一半——它要求 CONNECT **等一次共识**，而
契约 05 禁止连接路径等网络。把会话写入放进分片日志没有改变这一点：读
仍是本地读，于是生效窗口仍是一次复制往返（CLU-116）。

---

### 不在 v0.1 范围内的（避免被当成欠账）

- **吞吐与延迟不作为合并门禁**是 TST-127 的刻意决定（耗时随机器变化，
  写进基线就是发布无法复现的数字），不是缺的门禁。
- PRD 里标 v0.2 及以后的需求（Job、预配、规则引擎、Thing Type、标签、
  注册表事件等）不属于 v0.1。这里不再写具体条数：每次加需求都要改一遍，
  而改漏了就是一个假的数字——要数用本文件末尾的核对脚本。

需求编号已去重(TST-125):正文表保号，实现期补充改到该域最大号之后。
`ARCH-062` 与 `ARCH-065` 是同一条规则写了两遍，并成正文一行。

---

## 7. 使用教程（UI-076 至 UI-082，TST-142 至 TST-146）

入口在标题栏的全局动作组里（UI-076 修订），三章：SDK 接入（概览 +
Java/Go/Python 各一页）、设备影子（主题与一次完整交互、合并语义与版本）、
访问策略（策略文档与动作、变量、排查）。正文是 `docs/tutorial/{zh,en}/`
下的多级目录 markdown。

- [x] 内容源与渲染（UI-078、UI-079） — markdown 由 vite 以 `?raw` 取入，
      控制台自己解析一个受限子集（`web/console/src/guide.ts`，约 200 行，
      不引解析器依赖）。目录与文件名的数字前缀定顺序、首个 `#` 定标题、
      章标题与摘要来自 `_chapter.md`，不另立清单。中英两棵树，正文不进
      `i18n.ts`（词典装不下长文），骨架文案仍进。子集之外的语法渲染成
      一条带路径的红色错误，不静默丢弃

      原计划是 Go 工具在构建期编译成结构化数据、另行入库、门禁逐字节
      比对。那一层去掉了：它多出第二份入库产物，而用户看到的是前端
      dist，比对中间产物证明不了 dist 是新的
- [x] 教程页与路由（UI-076、UI-077） — `#/guide` 是目录，
      `#/guide/<章>/<页>` 直达；入口在**标题栏**的全局动作组里（UI-076
      修订，2026-10-31 从侧栏顶部挪过来，`navgroup.start` 一并废弃），
      带图标与 `aria-current`，全站只有这一处；三个角色
      都可见、不按角色裁剪章节；左侧章节树、右侧页内目录、底部上下页；
      代码块带来源文件名与复制按钮
- [x] 真实示例代码（UI-080） — `examples/sdk/{golang,python,java}`，
      markdown 按 `tutorial:begin/end` 锚点注入。Go 那份是独立 module，
      主 module 仍只有 bbolt 与 yaml/v3 两个直接依赖
- [x] 与两个「使用说明」抽屉的分工（UI-081） — 影子与策略的抽屉各加一条
      链到教程对应页，重叠事实的权威在教程
- [x] 命名与语体（UI-082，2026-10-31） — 功能名由「系统教程」改为
      **「使用教程」**：它讲的是如何使用本系统，而「系统」在本仓库已指
      节点自身。两棵树的全部正文与章页标题改写为书面技术语体，章页标题
      一律取名词性短语（「先读这一页」→「通用前置条件」，「调试一条被拒的
      请求」→「排查被拒绝的请求」，「跑一遍」→「运行示例」）——标题同时
      是侧边章节树与页内目录的菜单项，而菜单项是用来扫视定位的。控制台
      里教程相关的文案（入口名、副标题、空页与解析失败提示）同步改写，
      中英两份语体一致。英文树同轮改写，以免两棵树各自口语化的方式不同
      放大成语气不一致
- [x] 门禁与验收（TST-142、TST-143、TST-146） — `internal/server/console_guide_test.go`
      七项进 `make gate`（锚点、两棵树路径集合、代码块逐字相同、markdown
      与 TSX 里的教程链接都指向真实页、语法在子集内、页标题出现在已提交
      的产物里、语体的穷举名单与标题形状）；`make console-e2e` 36/36，其中五项是教程（顶部入口、
      深链接与 9 个代码块、复制按钮、中英各渲染一遍、抽屉链过去）

      六项都先把对应的东西改坏确认会红：锚点改名、删掉英文一页、改标题
      不重建产物、塞一个四级标题、markdown 与 TSX 的链接各指向不存在的页
- [~] 英文树里的代码块带中文注释 — 代码在两棵树里逐字相同（UI-079：代码
      不翻译），而示例源文件的注释按仓库风格是中文（CLAUDE.md 第 4 条）。
      于是英文读者看到的是英文正文加中文注释的代码。三种解法都有代价：
      翻译代码块就违反「逐字相同」那条（也就失去了「代码只有一份」的
      保证）；按语言剥掉注释等于把示例里最值钱的部分（为什么这么写）
      扔掉；把示例的注释改成英文则与全仓库的注释风格不一致。先按现状
      记着，等有英文读者反馈再定
- [~] 三语言示例的实跑（TST-144，`make examples-test`） — Go 与 Python
      对一个临时节点真跑通（建设备、签证书、取接入地址、发布、写影子再
      读回、列出挂着的策略，全程用一把 **operator** 钥匙）。**Java 那份
      没有实跑过**：这台机器上没有 JDK/Maven，脚本按「跳过并明说」处理。
      发版前要在有 JDK 的机器上跑一次 `make examples-test`

### 顺带压出来的一个真缺陷（AUTH-077 修订、TST-145）

教程的 Go 与 Python 示例第一次跑就在数据面 `Publish` 上 401，而控制面
全是通的。查下去不是示例的问题：**规范路径少编了一层**。AWS 的 SigV4
（除 S3）对路径编两遍，线上的 `demo%2Ftopic` 在签名里是 `demo%252Ftopic`，
两个官方 SDK 各自都这么做；本节点只编一次，于是这两类请求对任何 SDK
都 401：

- 数据面的 `Publish`（主题整段进路径）
- 控制面凡把 ARN 放在路径里的（`ListAttachedPolicies`、`ListTargetsForPolicy`）

- [x] 修 `internal/sigv4` 的规范路径（AUTH-077 修订） — 不再解码段内的
      百分号转义，直接按原始分段再编一次。分段边界仍按原始路径切
- [x] 真 SDK 覆盖（TST-145） — `tests/aws-sdk/flow.py` 加上数据面的
      `publish`、`update_thing_shadow`、`get_thing_shadow` 与
      `list_attached_policies`。先把修复改回去确认它会红（会，红在
      `ListAttachedPolicies` 上）

它能一直活着的原因值得记下来：包内用例自己构造规范请求再自己验，两边
用同一套错规则，于是永远绿；而 PRD 原文写的也是那套错规则。**凡是签名
要靠对端复现的规则，判据只能是真 SDK 跑通**——自对自的用例在这类规则上
没有证明力。

---

## 核对脚本

在**仓库根**执行(路径相对根目录，不是本文件所在目录)。

```bash
# 抽取全部需求 ID 与目标版本
python3 - <<'PY'
import pathlib, re
for f in sorted(pathlib.Path('docs/prd').glob('*.md')):
    for line in f.read_text().split('\n'):
        m = re.match(r'\|\s*([A-Z]{3,4}-\d{3}[a-z]?)\s*\|(.*)', line)
        if m:
            cells = [c.strip() for c in m.group(2).split('|')]
            print(m.group(1), cells[-2] if len(cells) >= 2 else '')
PY

# 模块存在性
for d in sigv4 ws; do
  [ -d "internal/$d" ] || echo "internal/$d 缺失"
done

# 门禁
make gate
```

---

## 会话与离线队列的组内复制（2026-11-20）

需求在 PRD `CLU-111` 至 `CLU-119`、`MQTT-128`、`TST-160` 至 `TST-162`。
这一节是实现与验证的对账。

### 做了什么

- [x] 离线队列从 `session.State` 这个整块 blob 里拆出来：一条消息一条记录
      （`store.PrefixQueued`，键是 `Q<clientID>\x00<8 字节大端序号>`），
      入队变成一次**追加**。整块写的代价有两处，都在发布路径上：N 条消息
      累计 O(N²)，集群下还要把上百 KB 的会话提进复制日志
- [x] `SessionStore.Save` 的 nil 约定：`st.Queued` 为 `nil` 表示"这次不碰
      队列"，空切片表示"队列应当是空的"。少了这个区别，任何一个只想改订阅的
      Save 都会把离线队列静默清掉——而那条路径（Load 改字段再 Save）在
      broker 里到处都是
- [x] 离线队列加上限（QTA-111）：每会话 100 条，满了丢**新的**（MQTT-127），
      于是设备收到的是已发布序列的一个前缀，顺序不被打乱
- [x] `internal/cluster/shards.go`：会话分区组。`ShardOf` 用 FNV-1a（纯函数，
      不是 Go 的 map 哈希——带随机种子的哈希会让不同节点把同一个会话算到
      不同分片，表现是"会话时有时无"）；分片号从 16 起，与元数据组（1）错开
- [x] `internal/store/sessionrepl.go`：会话命令的编解码、分片状态机、
      `ReplicatedSessionStore`（写走提案、读走本地）
- [x] `internal/server/sessionswitch.go`：后置切换点。broker 的参数在 New 时
      就定了，而分片要等复制组起来之后才有（它建在那个 NodeHost 上）
- [x] `cluster.replication.session_shards` 配置项（默认 8，上限 64）

### 验证

- [x] 包内用例覆盖命令语义：围栏在 apply 时判、take 只删读到的那一批、
      过期只清本分片、快照按分片切且带 applied index、未知操作码致命
- [x] **六条新不变量逐条证伪过**（§6 第 10 条的规矩）：把实现改坏，确认
      用例真的报红。包括"快照不按分片过滤"与"恢复时不先删本分片既有键"
      ——后者的表现是"已经删掉的会话在落后过的节点上又活了"
- [x] `TestRemoteFrameReachesOfflineQueue`：远端来的帧落到离线持久会话上
      要入队。`DeliverRemote` 是与本地发布分开的一段代码，少一步就全无声息
- [x] `make cluster-test` 新增跨三个节点的那一段（TST-160）：设备在 a 上建
      持久会话、从 b 发一条 QoS 1、到 **c** 上重连，session present = 1 且
      收到那条排队的消息。c 从没见过这个设备，两件事都只能来自复制
- [x] **k8s 上实跑**（Docker Desktop 单节点，摘反亲和）：同一条判据过了，
      而且加强版也过了——**在重连之前把入队的那个 Pod 删掉**，第三个 Pod
      上照样补发出来。这才是"离线队列不再钉在节点上"的判据

### 查出的三个缺陷

- [x] **MQTT-128**：设备域主题（`$aws/things/<设备>/...`）此前只按**在线
      归属**解析远端节点，归属查不到就判"无处可投"。设备离线时没有任何节点
      归属它，于是**发给离线设备的 QoS 1 只在它上次接入的那个节点上入队，
      在别的节点上发布就直接消失**，而没有任何位置报错。
      发现过程值得记：会话与队列明明已经复制到三个节点，而"在 b 上发、到 c
      上收"仍然收不到——排查方向被"离线队列没跨节点"带偏了一轮，直到把三个
      节点的冷层 dump 出来，看见**盘上根本没有入队记录**，才确定那一帧在 b
      上就没出去。单机碰不到它（没有远端），集群的用例此前只验在线设备
- [x] **CLU-119**：自定义 raft 传输层缺写超时。dragonboat 对每个对端只持有
      一条连接，写入阻塞住那条连接的发送协程就永远不返回，于是 leader **从此
      不再跟那个跟随者说话，而且既不报错也不重拨**。现象极具误导性：重建的
      Pod 出站正常（对端收得到它的拉票），入站一条都没有，而 leader 那边
      关于这个对端的日志**完全是空的**——没有拨号、没有错误、没有重试，
      三个方向都不指向传输层。dragonboat 自带的 TCP 传输层正是为此逐帧设
      超时，换成自定义传输时这件事不会被编译器或测试提醒
- [x] **CLU-118**：启动路径上"等不到就退出"**本身会阻断收敛**。第一版把
      分片就绪做成 15 秒硬等，于是重建一个 Pod 变成死锁：进程一退，端口随之
      关闭，对端的拨号从"超时"变成"连接被拒绝"、断路器一直开着，更等不到；
      而每轮重启还把本节点的任期推高一截，对端又以"leader 尚在"为由丢弃它的
      拉票。最终是**永久分裂**——既选不上又跟不上（任期比 leader 高，拒收
      心跳），而任期是落盘的，那个 PVC 从此废掉。
      判据取自实测日志：`[00001:00003] t520 dropped Heartbeat from n00002,
      term 159, term not matched`。现在分片就绪是 `/readyz` 的一个分项
      （`shards`），元数据组的等待上限放宽到两分钟并打进度日志，启动期的
      会话过期清理容忍 `ErrNotLeader`

### 这一轮暴露的方法论问题

- [x] `TestOfflineQueueIsNotRedelivered` **证伪时没有报红**——把"从盘上取走"
      改回"只从内存取"，它照样绿。原因是 `session.State.Clone()` 把 `Queued`
      做成非 nil 的空切片，而断开时的 `persist` 用的正是克隆，于是那次对账
      把盘上记录删了，两种实现在这条路径上结果相同。真正只有"盘上是权威"
      才覆盖的是**进程在补发之后、断开落盘之前死掉**，而用例鼓不动进程崩溃。
      已在用例的注释里写明它能证明和不能证明什么，而不是留着一条看起来
      在守但其实不守的用例
- [x] k8s 的验收判据从"冷启动能起来"改成"**删掉一个 Pod 后它以重启次数 0
      回到 Ready**"（TST-162）。CLU-118 与 CLU-119 都只在"一个节点重启、
      其余节点仍在"这个场景下显形，而三进程验收碰不到——那里杀掉的节点不会
      被自动拉起来。上一轮 k8s 实测写下的"被删的 Pod 重建后追平"是错的，
      已在 `deploy/cluster/k8s/README.md` 里纠正并说明错在哪

## 总览列出全部生效配额（2026-11-20）

- [x] `GET /overview` 返回全部配额定义（API-129），不再把统计值全为零的
      项目过滤掉。配额从启动时就已存在，命中只改变统计值；旧行为会让控制台
      把“尚未统计”误呈现成“系统没有这项配置”
- [x] 配额表默认显示全部项目（UI-085）。尚未产生统计的放行、拒绝与当前值
      显示 `—`，检查过且结果为零才显示 `0`；限值始终显示，异常项仍按拒绝数
      优先排列
- [x] UI-020 的中文名称由 AWS 术语直译“舰队总览”收成“运行概览”：
      页面同时展示节点、事件与配额，并不只是设备集合
- [x] 验收：总览接口用例断言返回 `quota.QuotaCount()` 项；`make console`、
      `make console-check`、`make console-e2e` 与 `make gate` 全部通过
