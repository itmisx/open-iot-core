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
- [x] 性能与内存基线公开数据 — `docs/baseline/memory.json`：热视图每设备 188 字节、订阅树每订阅 185 字节、每已建立连接 934 字节。高于记录 10% 即失败（TST-127）。耗时不进门禁，但自 2026-11-20 起**有公开基线**：`docs/baseline/throughput.md` 加同目录下的原始数据，由 `make bench-e2e` 一条命令产出（TST-163 至 TST-165）

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
      在那边被丢掉）。百万规模下已从周期性全量名字列表改为 epoch + 连续版本
      增量；新节点、重启、断档和发送失败通过按需精确快照自愈
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
      （每设备 188 字节、每订阅 185 字节、每初始连接 5416 字节，darwin/arm64、
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

- [x] `Broker.HoldsThing` 在集群下是发布热路径上的 O(连接数) 扫描 —
      它被接成 `GossipLocator.Local`，每条设备域消息都要遍历一遍 `byClient`，
      抢的还是 CONNECT 登记用的那把锁。要的是一张 `byThing` 索引（照
      `byCert` 的形状做）。已改为计数索引，同 clientId 接管和旧连接延迟清理
      也有回归覆盖；`LocalThings()` 同时不再扫描 `byClient` 和构建去重 map。
      后续又把 client / thing / cert 三类索引都拆成 256 分片（MQTT-129），
      无关设备的建连与断连不再争抢同一把 broker 全局锁
- [x] 每连接内存实测约 40 KB，正压在 NFR-023 的「低于 40 KB」线上 —
      仓库用例的初始堆基线已从 13096 字节降到 5416 字节：读缓冲从
      4 KiB 降到 1 KiB，编码/写出缓冲改成首次使用时分配。另外旧基线量到 16204 条空闲 TLS 连接
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
- [x] 影子写入原来只有单节点141/s、集群39/s并非Raft固有代价（CLU-127）。
      单节点apply现由bbolt合并并发持久事务；集群影子拆为4个Raft分片，逐请求
      ReadIndex改为等待本地状态机提交，时间线日志改为有界批量同步。2026-11-22
      实测单节点高点11,661/s；影子leader合批解除会话stale位图带来的64条误限，
      五节点4分片RF=3、真实设备mTLS的两轮60秒长稳达到15,896--16,165/s，
      p99 279--356ms，959,710与975,177次发布均与accepted逐条相等。五台独立Linux、
      独立磁盘和真进程带负载故障切换仍是外部生产验收，不把同机loopback数字
      冒充最终容量。共识正确性门禁已真实停止shard leader副本，三轮在
      0.924--1.097秒恢复写入，存活副本的提交前缀与applied index均未回退；
      三节点真进程mTLS探针进一步直接杀leader，1.230秒恢复且version严格到2。
      200客户端持续更新单一目标shard的mTLS故障轮在1.318秒恢复，accepted最大
      断档1.228秒，65,926次请求中65,581 accepted、201 rejected、144 timeout；
      相比修复前11.068秒断档和150,038 rejected，已消除旧leader路由重试风暴与
      优雅停机10秒连接黑洞。五节点RF=3又以不承载目标分片的Access节点
      验证了完整两跳路由：按分片单飞、一次性mTLS并行leader自证探测与
      3秒共享重探消除陈旧连接和惊群，200客户端最终轮1.499秒恢复、
      accepted最大断档1.356秒，36,161=35,961 accepted+200 rejected+0 timeout，
      成功率99.45%。剩余模糊请求须由设备携带version安全重试
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
- [x] **跨节点投递仍比同节点低一档**（阶梯量出零丢弃到 6 万条/秒，同节点 10 万；
      规划按 5 万，7 万时丢弃仅 0.34% 而 p50 已跳到 155 ms）
      — 剩下的这一档不是握手（那一处已修，见下面「节点间长连接」一节），
      而是形状：mesh 转发**同步**发生在发布者自己的读协程上，一条消息一次
      问答，上限是「并发度 ÷ 一次往返」。要再上一个台阶得让转发按对端攒批
      （`Transport.Forward` 本来就收一批帧），代价是「投出去了几个节点」从
      同步结果变成乐观结果——消息面是至少一次语义。已按 CLU-139 完成：
      每对端独立的有界字节/帧队列，200 微秒合批，发送失败保留队首并退避重试。
      尚需重跑阶梯基准，不沿用修改前的 6 万条/秒宣称新上限
- [ ] **集群下的建连速率是每秒一百条上下**（单节点 7840 条/秒）— **这 78 倍是
      一处已定位的实现瓶颈，不是共识的固有代价**（CLU-125）：apply 侧逐条提交
      bbolt 事务，可合并的并发度等于分片数（8）而不是并发连接数。归因见
      `docs/baseline/throughput.md`「建连那一栏为什么只有每秒一百条」与
      `tests/bench/connect-attribution.sh`。与连接池那处修复无关（接池前后 92
      与 88，重测 101）。受影响的是「同时重连多少台」，不是「能挂多少台」；
      修掉之前批量重连要靠 `connect_rate` 配额削峰

按 RMAP-004，阶段 B 的判据（元数据复制 + 多数派语义 + 快照追平）已经
达到，消息面、影子、Admin 扇出、成员变更与共享订阅组也都到位了。
仍要说清的一条：跨节点互斥是最终一致而不是零窗口。分区组现在有了
（CLU-111），但零窗口缺的是另一半——它要求 CONNECT **等一次共识**，而
契约 05 禁止连接路径等网络。把会话写入放进分片日志没有改变这一点：读
仍是本地读，于是生效窗口仍是一次复制往返（CLU-116）。

---

### 不在 v0.1 范围内的（避免被当成欠账）

- **吞吐与延迟不作为合并门禁**是 TST-127 的刻意决定（耗时随机器变化，
  进门禁等于让一次 CI 的机器调度决定能不能合并），不是缺的门禁。
  **公开基线自 2026-11-20 起有了**：`docs/baseline/throughput.md`，
  脚本与原始数据一并入库（TST-060、TST-163）。
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

## 端到端基准（2026-11-20）

起因是与 rmqtt 这类项目对照时的一个事实：README 里一个性能数字都没有，而
PRD 的 TST-060 至 TST-064 早就要求「脚本、硬件规格、原始数据公开入库」。

### 做了什么

- [x] 自带压测器 — `tools/mqttbench`（TST-061 的那一半），只依赖标准库与
      `internal/mqtt/packet`。六个场景：建连与每连接内存、一对一 QoS 0、
      一对一 QoS 1、扇出、影子更新（判据是 `accepted` 的到达而不是 PUBACK）、
      以及刻意过载的那一轮。`-broker` 与 `-sub-broker` 各接受地址列表并轮转，
      跨节点那几档就是把订阅侧轮转一格做出来的
- [x] 一条命令跑完 — `make bench-e2e`（`tests/bench/run.sh`）：起单节点、
      再起三节点集群（自己签复制链），同一组场景各跑一遍，结果、环境记录
      （CPU / 核数 / 内存 / OS / Go / 提交号）与逐轮原始数据一并落盘。
      不进门禁：要起进程、要 openssl 与 python3，一轮约八分钟
- [x] 结果入库 — `docs/baseline/throughput.md` 与 `docs/baseline/throughput/`
      下的 `standalone.jsonl`、`cluster.jsonl`、`env.json`。报告里的每个数字
      都能在 JSONL 里找到出处
- [x] 两份 README 加「基准测试」一节，并把「吞吐与延迟没有公开基线」那条
      边界改写成「不作为门禁，但有公开基线；跨机器 / 含 TLS 握手 / 十万级
      连接三种场景没测过」
- [x] 覆盖率门槛的分母收成「随二进制发布的包」（TST-166）— 压测器有 700
      多条语句而没有行覆盖，算进分母会把整体从 85.2% 压到 82.2%，而产品
      代码一行没改。它配的是另一类用例：`tools/mqttbench/main_test.go`
      盯住算错了也没有现象的那几处（分位数、亚毫秒不塌成 0、清零要连
      直方图一起清）。按本仓库的规矩先改坏确认会红——把桶的基准从微秒
      换成毫秒，两条立刻失败（p50 印出 0.00005 ms）
- [x] PRD 补 TST-163 至 TST-166：结果页必须写出三个前提（明文端点、授权仍
      生效、配额已解除）、同节点与跨节点必须分开记、吞吐必须与丢弃率一起报、
      覆盖率分母只算发布包

### 数字（M3 Pro 11 核 / 36 GiB / macOS 26.3 / go1.24.4）

**以下是这一轮（修复前）的现场记录，不是当前的能力**——跨节点那个两千条正是
下面「节点间长连接」一节修掉的那处缺陷；当前入库的数见本节之后的「按阶梯重测
基线」。

单节点：QoS 0 与 QoS 1 各 10 万条/秒零丢弃（p99 分别 0.51 ms 与 20 ms）、
扇出 10 万条/秒、建连 8519 条/秒（12000 条并存，每连接 15323 字节）、
影子 174 条/秒。集群：同节点 10 万条/秒，跨节点 2000 条/秒，建连 91 条/秒，
影子 39 条/秒。

### 顺带查出的三件事

- [x] **跨节点转发每条消息一次 TLS 握手**（违反 CLU-038）— 当天修掉了，见
      下面「节点间长连接」一节。这是这一轮最有价值的产出：它在 Go 测试、
      三进程验收与 k8s 实跑里都不会显形，因为那几套验的是「通不通」而不是
      「多快」
- [x] **过载不是平滑降级** — offered 从 20 万抬到 30 万条/秒，投递从 20 万
      塌到 2.5 万。QoS 0 在出站队列满时按设计丢弃，而丢弃又让发布者继续加压。
      这条写进了结果页：它正是默认配额存在的理由
- [x] **压测器自己的三个坑**，都记在代码注释里：发布者不设写超时会在服务端
      被打满时永久卡住（与 CLU-119 同一形状）；堆内存前后各采一个点算差值
      会得到负数（第一版印出过 -7207 字节/连接），改成取多次采样的最小值；
      扁平主题的精确订阅跨节点根本不投（ARCH-017），跨节点那几档必须用设备域
      主题或通配符订阅——第一版用 `bench/p/<i>`，集群那一轮收到 0 条

## 节点间长连接（2026-11-20，CLU-038 的实现补齐）

基准测出来的那处缺陷：`TLSGroup.roundTrip` 每次调用现拨一条 TCP、做完整 TLS
握手、发一条 gob、读应答、关闭，而 `Broker.forward` 是一条消息调一次。PRD
CLU-038 要的是长连接，所以这是缺陷不是取舍。

- [x] 按对端的空闲连接池 — `internal/cluster/peerpool.go`。两侧的 gob 编解码器
      都建在请求循环之外（类型定义每条流只发一次，每请求新建一对会让对端解出
      「重复的类型定义」）；服务端 `handleOwn` 从「一条连接一个请求」改成循环
- [x] 容量上限取 256 而不是 8（CLU-121）— 它是「缓存多少条空闲连接」，不是并发
      上限，取不到照旧现拨。取 8 时实测只从 2000 升到 5000 条/秒：100 个发布者
      里 92 个每条消息仍在现拨。mesh 转发是同步发生在发布者读协程上的，并发度
      是设备数量级
- [x] 只有幂等的请求重试（CLU-122）— mesh 帧与扇出查询换新连接重试一次；
      **写入转发不重试**，因为一次失败既可能是「请求没到」也可能是「到了、
      应用了，应答丢了」，自动重发第二种就是把命令应用两遍
- [x] 两侧都有超时（CLU-123）：客户端一次问答设读写绝对超时（上限 = applyTO +
      dialTO，要容得下对端等一次共识提交），服务端对空闲连接设上限（对端进程
      消失时这条连接没有任何事件）
- [x] 按建立时的地址判废，成员被移出时连带丢掉它的空闲连接（CLU-124）
- [x] 回归用例 `internal/cluster/peerpool_test.go` — 判据是**服务端接了几条
      连接**：50 次问答必须只用 1 条。已验证改回「每次现拨」立刻变红
      （「50 次问答用掉 50 条连接」）。另三条：坏连接要换新连接重试、写入转发
      不重试且下一次自愈、池子按地址与空闲时限判废并有容量上限
- [x] 验收：`make gate` 全绿、`make cluster-test` 全绿（含杀 leader、成员变更、
      会话与离线队列跨三个节点）、`make bench-e2e` 重跑并更新基线

### 修完的数字（同机同参数前后对比）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 跨节点 QoS 0（零丢弃上限） | 2,000 条/秒，p50 5.11 ms | **40,000 条/秒**，p50 0.44 ms |
| 跨节点 QoS 1 | 2,000 条/秒，p99 86.5 ms | **40,002 条/秒**，p99 53.1 ms |
| 跨节点扇出（1 → 1000） | 94,427 条/秒，p50 412 ms | **99,997 条/秒**，p50 2.23 ms |
| 跨节点过载（offered 8 万+） | 投递 2,341 条/秒，p50 14.5 秒 | 投递 59,023 条/秒，p50 2.9 秒 |
| 集群建连 | 92 条/秒 | 88 条/秒（不受影响；瓶颈是 apply 侧逐条提交，CLU-125） |
| 集群影子 | 39 条/秒 | 38 条/秒（同上） |

这张表的「之后」一列仍是**同一个 offered 速率下的对照**（压 4 万、投递 4 万），
它证明的是修复有效，不是上限。上限由下一节的阶梯量出。

## 按阶梯重测基线（2026-11-20 晚）

起因是一个提问：README 的集群那一栏写着「压 4 万 → 投递 4 万」，读起来像
「集群只能跑四万」。查下来 README 没写错（4 万是压力值，2000 是缺陷期的历史
值，三处都标着），但**整张表登的都是「我们压了多少」而不是上限**——而读者会
拿它当选型依据。

### 做了什么

- [x] 压测脚本加速率阶梯（TST-167）— `PAIR_LADDER`（单节点 15/20/25 万）、
      `CROSS_LADDER`（跨节点 5/6/7/8 万）、`LOCAL_LADDER`（同节点 15 万）。
      阶梯带 `-setup=false` 与基础级共用设备；每一级都进 JSONL。脚本**不自己
      判定上限**：它只记每级的 `publishPerSec` 与 `deliverPerSec`
- [x] 同节点也加阶梯 — 它与单节点走同一条路径，不压到同一个高度的话，
      「单节点 15 万、同节点 10 万」会被读成「集群把同节点吞吐压低了」
- [x] 两组之间插入冷却（TST-169，`COOLDOWN` 默认 60 秒）— 见下面第 2 条
- [x] 两份 README 与 `deploy/cluster/README.md` 按「零丢弃上限」重写，并显式
      区分两类数字：标「零丢弃上限」的是阶梯量出的上限，写「压 X → 投递 X」
      的是**下界**（QoS 1 与扇出两行仍是下界，没有为它们做阶梯）
- [x] PRD 补 TST-167 至 TST-169，并修订 TST-164（原文那句「相差两个数量级」
      引的是缺陷期的数）与 CLU-121（补上阶梯量出的六万）
- [x] 对账：`docs/TODO.md` 本节之前那段「数字」小节已标明是修复前的现场记录
      ——提问者看到的 2000 很可能就在那里，而它当时没有标注

### 查出的三件事

- [x] **原基线的「20 万条/秒仍零丢弃」是错的** — 阶梯实测 20 万丢 15.58%，
      零丢弃上限是 15 万。那句话写在 `run.sh` 的提示与结果页里，来源是另一组
      参数下的一次观察，没有入库的数据支撑
- [x] **`MODE=both` 会系统性压低集群的建连那一栏**（TST-169）— 同一台机器同
      一天：紧接单节点那组之后 27 条/秒，机器空闲时单独跑 101 条/秒，差 3.7
      倍而代码一行没变。集群那组的第一个场景就是建连，而它每条 CONNECT 等一次
      fsync 过的 raft 提交，最吃前一组留下的磁盘尾巴。入库的这版因此是分两次
      调用产出的
- [x] **QoS 1 那行的「丢弃 0.81%」没有复现**（TST-168）— 复测 0.00%，再用
      四倍窗口复核：60 秒、600 万条，发布数与投递数**精确相等**
      （`docs/baseline/throughput/qos1-window.jsonl`）。一个标着丢弃的 QoS 1
      行会被读成「至少一次语义没做到」，所以它必须有判据而不是解释

### 当时入库的数字（2026-11-20 晚，M3 Pro 11 核 / 36 GiB / macOS 26.3 / go1.24.4）

**这一段是当时的值，不是当前基线。** 当前值见
`docs/baseline/throughput.md` 的「结果」一节（最近一次重跑 2026-11-22）。

单节点：QoS 0 零丢弃到 **15 万条/秒**（p99 0.97 ms，20 万时丢 15.6%）、
QoS 1 压 10 万零丢弃（p50 14.9 ms）、扇出 10 万条/秒、建连 7840 条/秒
（12000 条并存，每连接 14311 字节）、影子 141 条/秒。
集群：同节点零丢弃到 **10 万条/秒**（15 万时丢 2.0%），跨节点零丢弃到
**6 万条/秒**（7 万时丢 0.34% 而 p50 跳到 155 ms，所以规划按 5 万），
跨节点扇出 10 万条/秒，建连 101 条/秒（单轮；后来按三轮中位数重登为 119），
影子 41 条/秒。

这一段里有两个数后来被判定**不可引用**：20 万那一级的「丢 15.6%」与集群
15 万那一级的「丢 2.0%」都没有记连接错误数，而错误非零时那一级量到的是
订阅者被断开的速度，不是吞吐（TST-221）。

## 会话 apply 的合并提交（CLU-125，未实施）

起因是另一个提问：集群建连每秒一百条，而单节点 7840 条。原先这一栏被归因为
「fsync 过的 raft 提交」，也就是当成了共识的固有代价——**那个归因是错的**。

- [x] 归因实验入库 — `tests/bench/connect-attribution.sh` 与
      `docs/baseline/throughput/connect-attribution.jsonl`。每次只动一个自变量：
      1 节点集群分片 8→32 吞吐 187→312 条/秒（涨 1.7 倍，说明可合并的并发度
      就是分片数），3 节点同样改动 99→103（不涨：一条日志三个节点各 apply 一次，
      本机三进程共用一块盘，已到 fsync 上限；312÷3≈104 与 3 节点实测吻合）
- [x] 机制在代码里是明确的 — `cluster.metaMachine.Update(ents)` 逐条调 apply，
      `store.SessionStore.ApplySessionCmd` 每条走一次 `kv.Batch(ops, false)` →
      `bbolt.DB.Batch`，而 `Batch` 只合并**并发**调用（默认等 10 ms 或攒到
      1000 条）。每个分片只有一个 apply 协程、循环里串行，于是合并只发生在不同
      分片之间。单节点那 7840 条/秒走同一个 `Batch`，但调用方是几百个并发的
      CONNECT 协程——MQTT-119 抬的就是这个合并因子
- [x] 三处错误归因已改（两份 README 与结果页）：原文说瓶颈在 raft 提交，
      现在写明是 apply 侧逐条提交，并标注修法未实施
- [x] 先量清修完的天花板 — `internal/cluster/shardceiling_test.go`
      （`TestShardProposeCeiling`，设 `OPENIOT_SHARD_CEILING=1` 才跑，不进门禁）：
      同一条路径、同样 8 分片与 256 并发、同样单副本，只把状态机换成不落盘的
      内存实现，实测 **13,772 条/秒**对现状 187 条/秒。**73 倍的余量在 apply
      这一侧，不在共识那一侧**——「修 apply 只是把瓶颈让给 raft 的 fsync」这个
      反驳可以排除了，所以这一条值得修
- [x] **实施合并提交**（2026-11-21）— `store.KV` 加 `InTx(sync, fn)`：一个写事务
      里边读边写（事务内的读看得见自己未提交的写）。三个实现都补齐：bbolt 走
      `db.Update`/`db.Batch`，内存实现用「覆盖层加整批落地」，SwapKV 转发。
      `SessionStore.ApplySessionBatch` 按批应用，`cluster.ShardOptions.ApplyBatch`
      取代逐条的 `Apply`（元数据组仍走逐条）
- [x] 回归用例 `internal/store/sessionbatch_test.go` — 七条，钉的都是「合批之后
      要靠实现主动维持、失手了也不报错」的那几条：批内后一条看得见前一条
      （CLU-115）、判定值逐条回填（CLU-116）、applied index 取最后一条、一条出错
      整批回滚、整批与逐条**等价**（同一串命令落出同一份键空间）、`InTx` 读得到
      自己的写入且升序、`InTx` 出错回滚。后两条内存与 bbolt 各跑一遍
- [x] 按规矩先把实现改坏确认会红 — 三组都验过：applied index 取第一条 → 红在
      「报小了的那几条重启后会被重放」；判定值取最后一条 → 红在「围栏判定没有
      逐条回填」；批内改回从 KV 读 → **卡死**（`panic: test timed out`），因为
      内存实现在 InTx 期间持写锁而 RWMutex 不可重入。这个坑记进了 `InTx` 的注释
- [x] 验收：`make gate` 全绿（整体覆盖率 85.4%）、`make cluster-test` 全绿
      （含杀 leader、成员变更、会话与离线队列跨三个节点）

### 效果：apply 侧 15.7 倍，端到端 1.18 倍

受控的那一层（离散很小，一轮即可）：

| 层次 | 改前 | 改后 |
| --- | --- | --- |
| 存储层 apply（8 协程、真 bbolt、17 条一批） | 374 条/秒 | **5,889 条/秒** |
| 同层裸写上限（单事务、不读不编解码） | — | 6,384 条/秒 |

端到端那一层**必须给分布**：这一栏的轮间离散很大，第一次只跑一轮就得出了
「+24%」，而官方那组参数（6000 条）同一次改动前后是 100.6 → 70.3，方向相反。
所以做了 A/B——3 节点、1500 条、并发 256，两臂各 5 轮：

| 臂 | 各轮（条/秒） | 中位数 |
| --- | --- | --- |
| 逐条 apply（改前） | 106.2 / 106.6 / 106.7 / 106.9 / 109.7 | 106.7 |
| 整批 apply（改后） | 119.6 / 124.8 / 125.7 / 140.3 / 163.0 | **125.7** |

两组区间不重叠（改后最小 119.6 > 改前最大 109.7），**效应是真的：中位数
+18%**。副产品：逐条那一臂紧在 106 到 110，说明历史上那些 27、70、88 的离散
来自环境与规模（6000 条对 1500 条），不是这条代码路径。

**apply 这一层到位了**（5,889 对裸写 6,384，差 8%），批也是大批（dragonboat
交给 `Update` 的批平均 16.8 条、最大 108 条，由 `TestShardProposeCeiling`
顺带量到）。

**但动手前我预期的是「千条量级」——那个预期错了**，它把当时最明显的瓶颈当成了
唯一的瓶颈。现在 apply 比端到端的数高 47 倍，剩下的那一段在上游、尚未定位，
记为 CLU-126：不要把 125 当成这条路径的上限，也不要据此说 CLU-125 没用。

- [x] 这一轮自己的教训也记下来：**性能结论要么给分布，要么不给。** 建连这一栏
      第一次的「+24%」是单轮对单轮，而同一天这一栏量到过 27 到 163。判据改成
      「两臂各 N 轮、区间不重叠」之后才站得住

- [x] **CLU-126：定位完了（2026-11-21）。** 待查那三条的结论，按当时的顺序：

      1. **一次 CONNECT 提几条日志**——非持久会话提了两到三条，而它一条
         都不该提（CLU-128）。这一条就是那 25 至 48 倍的主要部分：改完
         非持久会话那条路径从 119 条/秒到 26,028 条/秒（3 轮中位数，约
         219 倍），判据是 `session_writes_total` 除以已接受连接数等于 0。
      2. **分片 leader 不在本节点时提案怎么到 leader**——dragonboat 把
         跟随者上的提案**转发**给 leader（`raft.handleFollowerPropose` 改投
         leader），所以能成功但要多一跳；实测跟随者上的提案是 leader 上的
         3 到 4 倍（1,487 / 1,243 毫秒对 320 毫秒），而副本因子等于节点数时
         三分之二的提案都是这一类（local 1,000 对 remote 2,000）
      3. **会话与元数据共用同一个 bbolt**——确实共用（一个 `store.db`），
         但它在 apply 侧，而 `session_proposal_duration_ms` 与
         `connect_stage_duration_ms{stage="session_write"}` 几乎相等
         （346.9 对 345.6），说明时间全在 `SyncPropose` 里面，存储层在共识
         之外没花什么。**这条假设不成立**

- [x] **RF=3 session cell 已完成新集群启用闭环（CLU-140/141）。** 配置项
      `session_replication_factor`、选择性副本启动、非副本全局 readiness、
      远程读/写/Barrier/ownership、不可变布局清单和 leader 二级合批均已接通。
      五节点、8 分片、6,000 条持久连接、2,048 并发实测 2,944.5 CONNECT/s、
      p99 1,156 ms、零拒绝；16 分片同机反降到 2,287.1/s，所以保留默认 8。
      仍未做的是**已有 RF=all 集群在线迁移**与 RF 模式下在线增删成员；两者在
      管理面被明确拒绝，不会静默重算放置

- [x] **真实 mTLS CONNECT 与驻留内存已经单独量出（CLU-142）。** 压测器支持
      CA、客户端证书、私钥和服务端名称，并为每条连接做完整握手；五节点 RF=3
      的 1,500 条 clean 为 3,710.1/s，persistent 为 1,396.0/s，均零拒绝。
      单节点密度轮 12,000 条全部建成、零拒绝，为 3,606.7/s、p99 2.06 秒，
      每条真实 TLS 长连接堆增量 27,913 B（仍低于 40 KiB 规划预算）。
      百万连接均摊五节点仅这项约 5.20 GiB/节点；新增 runtime/栈口径后容量规划
      从 32 GiB/节点起步，
      仍须在目标 Linux 内核和真实多机上做 20 万连接/节点验收；本机数据不冒充
      百万连接已验收。CONNECT/CONNACK 已加 10 秒 deadline，服务端卡住时压测
      会失败而不是永久等待

- [x] **跨节点 TLS session 恢复与重连风暴已接通（CLU-143）。** MQTT TLS
      listener 可选加载共享 ticket 密钥环，第一把签发、后续把只解密旧票据，
      支持滚动轮换。压测器为每个 clientId 使用独立 session cache，并可把第二波
      整体移到下一 broker。单节点 6,000 条冷/恢复为 5,806.7 / 6,948.4 CONNECT/s，
      恢复率 6,000/6,000；五节点强制换节点为 7,510.9/s、恢复率 1,500/1,500，
      均零拒绝。安全回归确认恢复后仍重新查元数据：两波之间吊销证书，第二波
      `DidResume=true` 但 MQTT CONNECT 被拒，ticket 不缓存授权结论

- [ ] **Linux 百万连接验收入口已完成，实机验收待执行（CLU-144）。** `mqttbench`
      支持按 clientId 稳定轮转多个 `-source-ip`，突破单源 IP 临时端口上限；
      `tests/bench/connect-density-linux.sh` 默认验 20 万/节点，开跑前校验 nofile 与
      端口安全容量，产出环境、JSON 和逐秒 sockstat。服务端/JSON 已补 runtime Sys、
      stack in-use 与 Linux 当前 RSS，最终不再只看 HeapAlloc。当前缺五台目标 Linux
      主机、已配置源地址和百万设备预置数据，**不得标成已验收**

- [x] **TLS 握手栈不再随长连接常驻（CLU-145）。** listener 过去在同一个
      goroutine 里完成 TLS 握手并继续跑 MQTT read loop；6,000 条实测栈增量
      19,808 B/连接。握手成功后交给新 goroutine，短命握手 goroutine 退出归还
      大栈；复测降到 8,814 B/连接（-55.5%），runtime Sys 50,087→43,150 B/连接，
      按 20 万连接/节点约省 2.05 GiB 栈。listener Close 的 WaitGroup 仍覆盖交接后
      goroutine，broker 全回归通过

- [x] **百万设备预置已从逐台日志改成有界批量命令（CLU-146）。** 管理 API、
      registry、元数据线格式、三副本 apply 与 bbolt batch 已贯通；每批最多
      512 台且最多 2 MiB，任一非法输入整批不提交。`mqttbench` 每批串行调用
      `things:batch`，百万台的元数据日志理论数量从 1,000,000 降到 1,954。
      Apple M3 Pro 本地 bbolt 512 台三次中位数为逐台 1.783 s、批量 9.97 ms
      （约 179 倍）。这解决的是验收前置数据，不代表百万连接已实机验收

- [ ] **RF=3 非本地 session read 已合批，待真实多机 A/B（CLU-147）。** 过去
      ownership Claim 已按 shard 合批，但紧接着的会话 Load 仍是一连接一 RPC；
      现在用 200 微秒窗口、每批最多 256 条，并暴露 logical/batch/error 指标。
      包内 128 条并发读验证为 1 次物理调用，关闭与错误路径有门禁；当前沙箱
      禁止绑定回环端口，无法执行四节点传输用例，更不能把机制用例写成多机成绩

- [x] **按 clientId 的速率桶不再永久保留（CLU-148）。** 百万唯一 clientId
      只连一次时，旧实现仍为每个设备永久留一个 token bucket；微基准为
      121 B/subject，即约 115 MiB/百万台，还不含 map 峰值不缩的问题。现在每
      100 ms 回收一个 shard 中已经补满的桶，删多留少时重建 map；20 万桶全量
      扫描三次中位数 7.21 ms，生产单次只扫 1/64。未满不删、满额等价、增量
      扫描和并发 Allow 都有测试，指标可直接观察当前桶数与累计回收数

- [x] **设备归属索引去掉永久双表（CLU-149）。** 远端只保留 `thing -> node`
      权威表，快照替换与 Forget 才扫描；消息 Locate 仍为一次 O(1) 查表。10 万
      设备首次快照五轮由约 14.0 MB / 10.1 ms 降到约 5.25 MB / 5.9 ms，分配约
      降 62.5%。本地百万上线风暴产生的 dirty map 在大批 drain 后也会换表，避免
      `clear` 后哈希桶与在线 counts 双份常驻

- [ ] **Claim 已可携带非本地会话快照，待真实 RF=3 A/B（CLU-150）。** 修复了
      `sessionSwitch.Load` 强制读本地、绕过 CLU-147 的生产接线错误；非副本节点
      正常建连由 Claim+Load 两轮串行 RPC 折成一轮，结果按 clientId+epoch 一次性
      消费，未携带时仍走可换副本读。远程读取失败不再被当成“不存在”并覆盖旧
      会话，而是释放 ownership 后拒绝 CONNECT。新增 piggyback 指标；当前沙箱
      禁止 bind，不能把包内机制测试冒充多机性能成绩

- [x] **空闲连接初始读缓冲由 1 KiB 降为 256 B（CLU-151）。** 解析器仍按
      2 倍增长到 packet-size 上限，大报文跨缓冲测试通过。20,000 条连接堆基线
      五轮均由 5,416 降到约 4,648 B/连接，少 768 B；按 20 万连接约省
      146 MiB/节点。该数只含 Go 堆对象，不替代 Linux RSS/stack/sockstat 验收

- [x] **默认出站通道由 64 槽降为 32 槽（CLU-152）。** 仍可通过
      `broker.outbound_buffer` 按工作负载加深；20,000 条连接五轮均从中间基线
      4,648 降到约 3,368 B/连接，再少 1,280 B，按 20 万连接约省 244 MiB/节点。
      3,000 连接并发唤醒全刷出、FIFO/去重和 Runtime race 门禁通过；真实突发
      丢弃/断连率仍纳入 Linux 百万连接验收，不把对象基准冒充端到端成绩

- [x] **ownership 热表使用紧凑节点编号并可缩表（CLU-153）。** shard 内同一
      NodeID 只驻留一次，每条 owner 由字符串头改为编号；20 万条五轮中位数从
      约 20.6 MB 降到 18.5 MB，约减 10%，耗时基本不变。大量 Release 后删除
      多于剩余时重建 map。批量 Claim wire 格式保持不变，已撤回会破坏新旧节点
      滚动兼容的“批次级 Node 字段”实验

- [x] **在线证书索引不再为每台设备分配一张单元素 map（CLU-155）。** 唯一
      指纹直接内联连接指针，同证书第二连接才升级集合、删回一条再折叠。20 万
      唯一证书五轮中位数由约 50.99 MB / 400,513 allocs / 20.1 ms 降到
      14.69 MB / 513 allocs / 5.16 ms；按 20 万连接约少 34.6 MiB/节点与
      40 万个 GC 对象。吊销一证书驱逐多连接的语义保持不变；client/thing/cert
      三类在线索引在大量下线后都会换表，释放 Go map 不会自动缩掉的峰值桶

- [x] **策略属性查询去掉每连接闭包（CLU-156）。** CONNECT 直接把稳定热视图
      接到策略上下文，复用已有 thing 字段；一百万次绑定三轮由约
      9.7--10.2 ns / 16 B / 1 alloc 降到 1.09--1.16 ns / 0 B / 0 alloc，
      避免百万设备冷连时制造一百万个捕获对象

- [x] **认证输入不再随长连接永久常驻（CLU-157）。** 证书 DN、序列号、指纹
      与 AccessKeyID 在 Authenticator 返回后解除引用，只保留 Remote/TLS/ALPN/
      生命周期等运行字段。20,000 连接五轮由 3,368 稳定降至 3,112 B/连接，
      少 256 B；按 20 万连接约省 48.8 MiB/节点

- [x] **出站队列改为一个内联槽、按突发延迟分配（CLU-158）。** 典型设备发完
      CONNACK 后不再为 32 个从未使用的 channel 槽常驻内存；第二条并发积压才
      分配完整有界环。20,000 连接五轮由 3,112 稳定降至 1,688 B/连接，少
      1,424 B，按 20 万连接约省 271.6 MiB/节点。单消息入队/出队还由约
      24.9--26.1 ns 降至 14.3--15.0 ns，FIFO、回绕、队满与 race 门禁通过

- [ ] **CONNECT 后再做一次 goroutine 栈交接（CLU-159），机制已完成、待 Linux
      A/B。** Listener 的 TLS/WebSocket/明文路径在认证、会话恢复和 ownership
      完成后让建连 goroutine 退出，稳态读循环从最小栈开始；公开同步 `Serve`
      不变。net.Pipe 门禁已证明交接与连接计数闭环，但每连接 stack-in-use 的
      实际下降只能由 `connect-density-linux.sh` 复测后登记

- [x] **共享 NodeHost 支持多个独立 shard group（CLU-160）。** shard base、
      持久布局文件与 TLS 直达路由均按组隔离；同一端口可同时承载 session 与
      shadow，区间重叠启动即拒绝，关闭一组不会摘掉另一组。会话历史区间 16+
      与 `session-layout.json` 保持不变

- [x] **影子写入可显式切到 8 条独立 Raft 日志（CLU-161）。** 新配置
      `cluster.replication.shadow_shards` 缺省 0 保持滚动升级兼容，示例新集群
      设为 8。每批一个事务，文档与最终 applied index 原子提交，结果只在提交
      后交付；快照按 thingName 过滤并拒绝夹带别组键。现已支持显式 RF=3：
      本地副本直接提案，非副本写只发一次不可模糊重试，Get/List 可在副本间
      回退且读前 Barrier；RPC 解码保持 `json.Number`

- [x] **影子同设备互斥由永久 map 改为固定条带（CLU-162）。** 旧实现每个
      写过影子的 thing 永久保存字符串键和 mutex；20 万唯一设备五轮约
      15.58--15.60 MB、201k allocations、13.6--15.9 ms。256 条 FNV-1a
      条带为 464 B、6 allocations、0.97--1.19 ms；按百万设备避免约 74 MiB
      常驻索引和百万级 GC 对象，同 thing 串行语义与 race 门禁保持不变

- [x] **首次 clientId 的限速桶不再单独分配小对象（CLU-163）。** bucket
      直接内联在分片 map 值中，20 万唯一 subject 五轮由约 112--119 ns、
      120--121 B、1 alloc/op 变为 104--113 ns、124--126 B、0 alloc/op。
      每项多约 4--6 B，换取百万冷连少约一百万个 GC 对象；满桶增量回收与
      backing map 换表逻辑保持不变

- [x] **会话同 clientId 互斥也改为固定条带（CLU-164）。** 1024 条带约
      8 KiB 固定常驻，替代写过会话后永不回收的 mutex map。20 万唯一
      clientId 五轮旧表约 15.58--15.60 MB、201k allocations、13.8--14.6 ms，
      新索引循环 464 B/op、6 allocations、1.09--1.19 ms；按百万持久设备
      避免约 74 MiB 索引与百万级 GC 对象，整批 apply/epoch 围栏 race 全通过

- [x] **TLS 认证短命载荷清零复用（CLU-165）。** 证书 DN、序列号、指纹与
      AccessKeyID 仍不进入长连接对象，但不再每次冷连单独分配；认证返回或
      早退 cleanup 时清零后放回 pool。10 万次生命周期五轮由 1,504 B、
      3 allocs、201--229 ns 降到 1,153 B、2 allocs、190--209 ns；敏感字符串
      不会被 pool 延长引用

- [x] **metadata 快照不再夹带 shard keyspace（CLU-166）。** 新流式格式排除
      Session/Queued，以及启用影子分片后的 Shadow 与各自 applied-index；恢复
      在一个事务里只替换 metadata 键，保留本节点 RF=3 数据。旧 bbolt 整库
      快照仍可读取，但其中 shard 键会被过滤，避免升级后反向覆盖

- [x] **百万键快照消除逐记录头分配并传播写失败（CLU-167）。** 10 万 metadata
      +10 万 session 的过滤快照输出 3,000,024 B，三轮约 14.1--15.6 ms；复用
      8 字节记录头后 allocations 从约 102k 降到 2,042。Session shard 快照此前
      会静默吞掉 writer 错误，现已修复并加回滚/故障门禁

- [x] **Shadow RF=3 历史全量副本安全裁剪（CLU-168）。** 启动已在
      metadata Barrier 之后；分片组成功后，在阻止影子读写的切换锁内，
      用 bbolt cursor 删除本节点非放置分片的影子。删除与布局指纹 marker
      同事务，失败全回滚、重启可重跑，完成后不再重扫百万键。20 万键
      保留/删除各 10 万约 44.9--46.4 ms；metadata 快照保留 marker 与
      剩余本地影子，不会把已删数据带回

- [x] **RF=3 二级/三级副本真正就地读写（CLU-169）。** `IsLocal`
      从“是首选 owner”改为“本节点承载任一副本”，避免本地副本还多跳
      一次 RPC，也避免历史数据裁剪错删合法的非首选副本。远程影子读现单列
      逻辑数、物理 RPC、回退次数与最终错误；写只发一次并单列错误

- [x] **默认读缓冲内联且放宽内核 TCP keepalive（CLU-170）。** 256 B
      初始读缓冲放入 `conn` 本体，TLS 认证连接生命周期由 2 allocations
      降到 1，B/op 保持 1,152；百万连接少一百万个 GC 对象。Go 接收 TCP
      连接的 15s 内核保活默认改为容量化 5m（可配）；百万空闲连接的
      理论探测频率由约 66,667/s 降至 3,333/s，MQTT keepalive 仍负责主要死链路检测

- [x] **会话提案合批请求容器复用（CLU-171）。** Access 侧与 shard leader
      二次合批过去每条逻辑写都分配 request、buffered channel 并复制一遍
	  已编码命令。现在每个组用最多 4096 个的有界 free-list 复用容器/
	  channel（不依赖会被 GC 整池清掉的 `sync.Pool`），同步等待期间
      直接引用调用方命令字节，完成前不返回所以不存在生命周期窗口；归还前
	  清空 clientId/data，并受 256 并发合批 race 门禁覆盖。Access 侧 64 逻辑写
	  稳态已为 0 alloc/op，约 28 B/逻辑写是那条物理批命令的连续输出

- [x] **shard leader 会话二次合并去掉完整状态反复解码（CLU-172）。**
	  用 canonical JSON 前缀快速读 op/clientId、零拷贝切分外层 batch，非标准/
	  转义 clientId 仍回退标准解码；用定长数组和 FNV 去重。64 个带 8 条订阅
	  的真实会话合并由约 1.56--1.70 ms、312,970 B、3,141 allocations
	  降至 421--447 us、98,415 B、2 allocations；剩余主体是必须交给 Raft 的
	  连续合并命令

- [x] **Ownership Claim 风暴有界背压与零分配选批（CLU-173）。**
	  每个 session shard 最多 8 个在途 Claim RPC，同 clientId 必须等前批完成；
	  请求 free-list 最多 4096，请求指针与 wire item 工作区最多与 8 个在途
	  批次一一对应。256 条选批由约 14.4--15.6 us、2,305 B、1 alloc 降到
	  14.4--15.7 us、稳态 0 alloc。模糊写失败仍不换副本，仅全批明确
	  `ErrNotLeader` 才能回退

- [x] **TLS 重连风暴改为 listener 有界 admission（CLU-174 / MQTT-131）。**
	  TLS 1.3 mTLS 全握手的无网络机制基准约 0.76--0.80 ms、160 KB、
	  1,513 allocations/条；并发度 1→2→4 时吞吐快速上升，512--1024 已进入
	  约 134--143 us/op 的平台。现在每个 TLS/WSS listener 默认限制为
	  `GOMAXPROCS×64`（最小 256、最大 4096），饱和后停止交付新 TLS 工作集，
	  让 listen backlog 背压。新增全/恢复/失败计数、在途/峰值/容量指标；
	  原先只定义却未调用的 `handshake_rate` 也已接到 TLS 前，被限流连接
	  不消耗握手工作槽。可控 listener race 门禁证明上限、限流顺序与
	  StopAccepting 不死锁

- [x] **Session proposal 从每 shard 串行等待改为有界 pipeline（CLU-175）。**
	  Access 侧过去提交一批后必须等整轮 Raft 返回才派下一批，8 shard 因而
	  最多只有 8 个物理提案在途；现在每 shard 最多 8 批并行交给 Dragonboat，
	  让其合并 WAL/传输。相同 clientId 仍严格等前批完成，小尾批最多等 2 ms，
	  request 缓存上限 4096、批工作区上限 8；queued/in-flight/peak/saturation
	  指标已进入建连 A/B。无网络门禁连续证明能到 8、绝不出现第 9 批且同设备
	  不乱序。五节点 RF=3 真实 mTLS 持久重连由 1,113.5 提升到 1,362.0
	  CONNECT/s，p99 794→581 ms，物理 Raft 提案 275→176。leader 也开 8 路
	  的受控实验反降到 603.6/s、713 个物理提案，已撤回；保留每 shard 串行
	  leader 合并以维持 Dragonboat/LogDB 的有效批次

- [x] **Access session 批收紧为 16 条，leader 仍合到 64（CLU-176）。**
	  新增 leader 宽度与截断原因计数后，6,000 条高压档确认大 Access
	  批作为不可拆的 stale 位图返回单位，使 64 位日志平均只装
	  49.16 条。分块后两轮平均宽度 53.13/53.40，物理日志
	  406→377/378，吞吐 1,707.1→1,848.5/1,832.2 CONNECT/s，p99
	  2,787→2,273/2,627 ms；零拒绝、零提案错误。同 clientId 跨批
	  串行、epoch fencing 和模糊写失败不重试均未改变。8 条分块实验
	  为 1,840.8/s、2,658 ms、372 条日志，但 Access 批升到 3,833、当前
	  排队 1,762 且在途批次逼近 320 上限，没有额外吞吐收益，已撤回
	  。又尝试保留队列顺序的 leader best-fit：满 64 条日志约从 95
	  升到 211--223，物理日志降到 364--369，但四轮中两轮出现 1/8 条
	  连接等 CONNACK 超过 10 秒；紧接着回切前缀合批即 6,000/6,000
	  零错误。百万连接目标不接受用极端长尾换平均日志宽度，已整体撤回。
	  `mqttbench` 结果现会记录 `firstError`，A/B 脚本遇到任何建连失败直接失败，
	  禁止再把部分成功轮当成性能收益
	  后续 6,000 条档又撤回三项无稳定收益的实验：Claim leader 提示命中约
	  4,700 次但 RPC 与吞吐未改善；Access 在途最小批强制 16 条降到
	  1,458.4/s；leader 有序双槽异步首轮 1,882.0/s，复测却有 1 条 CONNACK
	  超时、仅 580.8/s，且日志由串行对照 383 增到 415/439。当前仍保留
	  200 微秒 Claim tick、Access 最小批 8、leader 每 shard 串行前缀合批

- [x] **百万连接压测器去掉每设备 32 KiB 常驻缓冲（TST-201）。**
	  `mqttbench` 旧客户端每条连接永久持有 16 KiB reader + 16 KiB writer，
	  百万设备仅压测端就约 30.5 GiB，可能先于 broker OOM。现改为 256 B
	  内联流缓冲、按需增长，编码结果只保留到 flush 且随后释放。机制基准
	  五轮由 32,944 B / 5 alloc 降到 352 B / 1 alloc；4 KiB 报文的碎片读取、
	  粘包、增长、完整写与释放均通过 race。Linux 验收同时记录 load generator
	  的 runtime Sys、当前 RSS、逐秒 VmRSS 与内核累计 VmHWM

- [x] **重连压测 ticket cache 去掉百万个独立 LRU（TST-202）。**
	  仍按 clientId 隔离 TLS session，但改为 64 个固定锁分片直接存
	  `(clientId, serverKey) -> ClientSessionState`，不再为每设备创建 mutex、
	  map 与 LRU 节点。10 万设备五轮由约 53.4 MB / 838k allocations /
	  32--47 ms 降到 12.22 MB / 1.4k allocations / 5.2--6.6 ms；同设备复用、
	  跨设备/服务端隔离、删除与 1024 设备并发均通过 race

- [x] **证书 DN 常见字段内联、稀有字段按需分配（TST-203）。** Subject 与
	  Issuer 原来无论是否填写都各自常驻 8 个 string header；现在常见的 CN、
	  Country、Organization 保持零分配，其余字段仅出现时分配扩展。全部 8 个
	  Subject/Issuer 策略变量继续逐项覆盖。20,000 连接基线五轮由 1688 B/连接
	  稳定降到 1559--1560 B/连接；TLS 连接认证生命周期由 1152 B 降到 1024 B，
	  仍为 1 次分配，百万连接按 Go 堆约减少 122 MiB。尝试删除重复 Thing 字段
	  虽把结构体从 920 B 缩到 896 B，但实际仍落在 1024 B allocator 档且无收益，
	  已撤回，避免用结构体表面尺寸冒充容量改进

- [x] **CONNECT 分段耗时不再随长连接常驻（CLU-177）。** 四个耗时和三个
	  结果标记只在 CONNECT 结果事件发出前有用，改为池化 sidecar，发出后或
	  异常 cleanup 时清零回收。`conn` 主对象由 1024 B 分配档进入 896 B 档；
	  20,000 条连接五轮从 1558--1559 稳定降到 1430--1431 B/连接，百万连接
	  约少 122 MiB Go 堆。认证生命周期保持 1 alloc，五轮为 896 B/op、
	  143.7--145.6 ns/op；事件字段完整性、重复 cleanup 和 race 均有门禁

- [x] **空出站队列状态整体按需分配（CLU-178）。** Runtime 只常驻队列容量
	  与一个 sidecar 指针；第一条消息取 sidecar，第二条积压才取完整 ring，
	  完全排空后清引用并归还两者。20,000 条连接五轮从 1430--1431 降到
	  1350--1351 B/连接，百万连接再少约 76 MiB。单消息入队+出队五轮为
	  16.51--16.85 ns/op、0 alloc，仍快于旧 channel 的 21.32--25.10 ns/op；
	  FIFO、回绕、背压、关闭竞态和 sidecar 释放均通过 race

- [x] **证书策略上下文只由引用证书变量的连接保留（CLU-179）。** 编译策略
	  预计算是否引用证书序列号、Subject 或 Issuer；CONNECT 鉴权完成后，普通
	  Thing/ClientId 策略释放 128 B `CertContext`，连接事件需要的 CN 暂存在
	  CONNECT sidecar。把 `Context.Cert` 改为按需指针并重排 `Principal` 后，
	  `conn` 从 888 B 缩到 760 B，实际分配档从 896 B 降到 768 B；20,000 条
	  连接五轮由 1350--1351 降到 1222--1223 B/连接，百万连接约再少
	  122 MiB。证书变量发布授权、序列号授权、事件 CN 与 race 回归均通过

- [x] **长期连接只保留稳态传输事实（CLU-180）。** 握手耗时移入 CONNECT
	  sidecar，事件发出后回收；三个标准 ALPN 编成紧凑枚举，非标准值才侧挂；
	  WSS 的最大寿命和重定向地址只由 WSS 连接分配 extras。clientId 以
	  `AuthzCtx.ClientID` 为唯一副本，`ver` 与持久标记填入原有对齐空洞。
	  `conn` 实际分配档从 768 B 降到 704 B，20,000 条连接五轮由
	  1222--1223 降到 1158--1159 B/连接，百万连接再少约 61 MiB。
	  标准/自定义 ALPN、认证信息往返、WSS 到期重定向、管理快照与 race 均覆盖

- [x] **leader 会话合并只校验一次 JSON，并允许同设备顺序合入（CLU-181）。**
	  Access envelope 已完整校验，最终编码不得再次扫描最多 1 MiB 的子命令；
	  相邻批里的同 clientId 保留队列前缀顺序，在同一状态机事务内逐条应用并按
	  原位置拆 stale 位。64 条、每条 8 个订阅的微基准由 412--484 us 降到
	  212--223 us，保持 2 alloc、约 98.4 KB。五节点最终两轮 6,000/6,000、
	  零错误；backlog 免等待和旧写取消未形成稳定收益，均已撤回

- [x] **在线连接进入 Go 1.24 的 640 B 分配档（CLU-182）。** 保留公开
	  `Principal.Thing` 兼容自定义认证器，连接内部只留 `AuthzCtx.Thing`；读协程
	  直接编码到出站所有权缓冲，WSS 寿命取消函数移入已有 extras，broker 级
	  `max_write_batch` 不再逐连接复制。`conn` 由 688 B 压到 632 B，计入 8 B
	  malloc header 后由 704 B/op 降到 640 B/op；20,000 条五轮为 1095 B/连接，
	  百万连接约再少 61 MiB

- [x] **writer 单帧直接写队列所有权缓冲（MQTT-133）。** CONNACK、PINGRESP
	  与普通 ACK 不再先复制到每连接 `wbuf`；只有确实积压第二帧才合并，批量
	  系统调用语义不变。单帧五轮 65.5--77.7 ns/op、0 B/op、0 alloc；五节点
	  6,000 条持久 mTLS 为 6,000/6,000、零错误、1,644.2/s、p99 3,054 ms，
	  与相邻轮方向为正但不超出本机漂移，不宣称端到端百分比

- [x] **在线连接进入 Go 1.24 的 576 B 分配档（CLU-184）。** 多帧 writer
	  缓冲与 QoS 重试状态按需侧挂；CONNECT 分段、认证输入和稀有传输扩展共用
	  sidecar；远端地址紧凑保存，IPv6 zone 按需保留；keepalive 用有界 32 位
	  毫秒值。`conn` 568 B、实际 576 B/op，20,000 条五轮 1030--1031 B/连接，
	  百万连接相对 CLU-182 再少约 61 MiB。五节点 6,000 条持久 mTLS 全成、
	  零错误，1,703.6/s、p99 2,499 ms

- [x] **内联读缓冲 192 B，使连接进入 512 B 分配档（CLU-185）。** 常见控制
	  报文仍一次容纳，大报文继续增长到原 packet_size 上限。`conn` 504 B、
	  实际 512 B/op，20,000 条五轮约 966 B/连接，百万连接相对 CLU-184 再少
	  约 61 MiB。五节点 6,000 条持久 mTLS 全成、零错误，1,907.2/s、p99
	  2,309 ms

- [x] **连接进入 480 B 分配档（CLU-186）。** 读游标用有界32位；多帧合并
	  缓冲由 writer 池按 flush 借还；packet ID 计数器移入 QoS sidecar；写失败
	  复用关闭状态位。20,000 条五轮 934--935 B/连接，百万连接再少约
	  30.5 MiB；五节点 6,000 条持久 mTLS 全成、零错误，1,687.3/s、p99
	  2,707 ms

- [x] **可信 TLS 终结外移入口真正落地（CLU-187 / NET-017）。** 裸明文端点
	  不再被当作“已支持外移”：显式 `proxy_protocol: v2` 后，原始 TCP peer
	  必须命中非空 CIDR allowlist，header 缺失、畸形、超长、截断、未知 family、
	  重复/缺失证书 TLV 均 fail closed。私有 `0xE0` TLV 携带叶证书 DER，broker
	  自行计算指纹并完整恢复证书策略上下文；每次 CONNECT 仍按最新热视图复核
	  登记、吊销、Thing 与策略，标准 SSL verified/CN 位不能冒充身份。6000 条
	  单节点 clean 代理档全部建成、零拒绝：16,101.3 CONNECT/s、p99 233 ms、
	  Heap 增量 3,617 B/连接、runtime Sys 增量 4,555 B/连接、stack 2,135 B/连接。
	  后续全新进程轮出现 1,503--2,902/s 与约 2 秒调度长尾，证明 16,101/s 是
	  快轮而非稳定上限，不再宣称建连倍数；最差密度轮仍为 direct mTLS 的约
	  4.9x Heap、4.4x runtime Sys。该数字只算 broker，不包含外部终结器，
	  不能冒充端到端 mTLS 验收。
	  本轮 race 还抓到 CONNECT sidecar 回收与 takeover/吊销读取 `redirectTo` 的
	  竞态；稀有传输扩展已迁入原有原子 QoS sidecar，`conn` 仍为 480 B、20,000
	  条仍为 935 B/连接，定向 race 与全包回归均通过

- [x] **PROXY v2 增加终结器友好的紧凑指纹身份（CLU-188）。** `0xE1` TLV
	  接受 32 B 原始或 64 位十六进制 SHA-256，避免每次 CONNECT 传 DER 并做
	  `x509.ParseCertificate`；与 DER 同时出现时必须匹配。只有指纹时证书 DN 与
	  serial 变量为空，引用它们的策略按既有语义拒绝，Thing/ClientId 策略正常。
	  两轮 6000/6000、零拒绝；本机长尾漂移压过吞吐差异，不宣称提速，但最差
	  Heap 增量仅 717 B/连接，且该格式可由通用终结器直接构造。纯身份投影五轮：
	  DER+x509 为 1,358--1,362 ns、2,400 B、25 alloc；指纹为 38.5--40.6 ns、
	  0 B、0 alloc，确定性消除了约 35 倍 CPU 与全部短命分配

- [x] **Linux 突发建连复核并撤回 SO_REUSEPORT 实验（CLU-189）。** 在同一个
	  Linux aarch64 network namespace 内用 6,000 条 PROXY 指纹连接、2,048
	  并发交替 A/B；单 listen queue 两轮为 41,946.9/41,010.8 CONNECT/s、
	  p99 86/84 ms，8 个 reuseport queue 反而只有 35,465.3/30,957.5、p99
	  105/102 ms。首个冷轮曾给出相反方向，交替复测才排除热机噪声。多 socket
	  pump、配置项和平台分支已撤回，不把“高级手段”本身当优化。单 socket 的
	  1 对 8 acceptor 交替轮同样仅有约 3--4% 且分位方向不一致，旋钮也已撤回。
	  保留压测器注入同一份预构建 Linux 二进制以及跨 BSD/GNU/BusyBox 的
	  mktemp 修复。

- [x] **真实 Linux mTLS 冷连与 ticket 恢复基线（TST-207）。** Docker Linux
	  aarch64、4 vCPU、6.10.14 linuxkit、broker/loadgen 同 namespace，6,000 条、
	  2,048 并发：冷全握手为 5,098.1/s、p99 510 ms；共享 ticket 的第二波为
	  9,923.6/s、p99 297 ms、6,000/6,000 恢复、零拒绝，且仍重新执行 MQTT
	  认证/授权。握手 admission 扫 64/128/256/512 的吞吐为 5,243.5/
	  5,338.4/5,393.9/5,205.6，当前默认 256 已在拐点，512 还把 p99 拉到
	  688 ms，故不调默认。压测器原先两波只等 250 ms，会撞上生产默认的
	  `connect_rate_per_client=1/s`；MQTT 3.1.1 又把 0x9F 映成 0x88，曾造成
	  1,164--1,825 条伪“服务不可用”。现默认显式等待 1,250 ms，不放松服务端
	  防风暴配额，并在失败时打印 AuthFailed/Throttled/ViewStale/StoreFailed
	  归因。原始结果在 `connect-linux-docker.jsonl`；Docker Desktop 仍不是
	  五台独立 Linux，不能替代百万并发最终验收。相同环境扩大到 20,000 条
	  全握手仍 20,000/20,000、零拒绝，3,801.9/s、p99 935 ms；服务端 RSS、
	  runtime Sys、栈增量分别为 25,296/25,293/4,110 B/连接。按 RSS 机械外推
	  20 万条约 4.71 GiB 进程增量，但不含内核 socket，仍只作进入多机验收的
	  可行性判据。

- [x] **Linux密度探针推进到10万直连与20万broker核心（TST-208）。** 多源IP已接入
	  `connect-stage-ab.sh`。真实mTLS冷连的50,000与100,000条轮均全部建成、零拒绝；
	  10万轮为2,033.5/s、p99 2.19s，服务端RSS 22,973 B/连接。15万同VM轮因
	  broker+TLS loadgen占到约6 GiB并换页，只建成142,698、失败7,302；MQTT四类
	  拒绝均为0，失败行保留用于证明该环境边界。可信PROXY v2指纹档用8源IP成功
	  持有200,000/200,000、零拒绝，broker RSS仅5,220 B/连接；它证明broker核心
	  单节点密度，不包含终结器成本。首轮20万还暴露管理HTTP端口被设备socket
	  饿死；新增`-admin-source-ip`及密度脚本`ADMIN_SOURCE_IP`，mqttbench和curl
	  时间线均绑定独立源地址。功能测试与race通过。

- [x] **Linux多broker压测端口按四元组复用（TST-209）。** 五节点RF=3、4源IP的
	  20万轮原先稳定停在112,928条（4×28,232个临时端口），因为
	  `Dialer.LocalAddr`先执行`bind(sourceIP, 0)`并独占端口。mqttbench现对Linux
	  显式源IPv4设置`IP_BIND_ADDRESS_NO_PORT`，失败直接返回；真实socket门禁读回
	  选项值。相同5节点、RF=3、512并发复测为200,000/200,000 held、零错误，
	  38,478.8 CONNECT/s、p99 39.0 ms；159,990个Ownership Claim合成49,194次RPC，
	  全部零错误。该代理轮证明横向路径与端口容量公式，不替代真实mTLS及五台
	  独立Linux的最终百万验收。

- [x] **百万建连过程计数与conntrack预检（TST-210）。** 五节点RF=3代理档继续
	  抬到35万时，新增的5秒进度精确显示前10秒零错误建成261,854条，随后512个
	  在途连接整批停住并按10秒超时；现场`nf_conntrack_count=max=262144`，而
	  socket为524,690（同namespace每条流两端各一条），证明是Docker宿主状态跟踪
	  硬上限，不是broker认证/Ownership拒绝。mqttbench现报告started/completed/
	  held/errors/in-flight/速率；stage-ab实时透传并保存stderr；Linux两套密度脚本
	  开跑前按conntrack剩余量的80%预检，避免再把平台上限误报成broker边界。
	  后续20万快轮还抓到客户端已全部成功后两个broker被环境OOM杀死，而旧脚本
	  只警告“3/5节点有指标”并退出0；现已把全部broker存活和全节点结束指标改为
	  硬门禁。该轮40,032/s原始行保留作失败证据，不计入有效性能结果。

- [x] **五节点Kubernetes容量overlay与GOMEMLIMIT接线（TST-211）。** 原
	  StatefulSet用`command`直接`exec openiotd`，绕开镜像ENTRYPOINT，也绕开
	  cgroup内存上限推导`GOMEMLIMIT=75%`的保护；已改为显式经过入口脚本并加
	  Go门禁。新增`deploy/cluster/k8s-million`：固定5成员、session/shadow均8分片
	  RF=3，每Pod请求8 CPU/32GiB、40GiB内存上限、100GiB卷，MQTT单独使用只暴露
	  8883且`externalTrafficPolicy: Local`的LoadBalancer。kubectl kustomize实渲染
	  通过，配置/资源/入口均有静态门禁；真实能力仍只认五机mTLS密度结果。

- [x] **持久建连结束快照等待完整排空（TST-212）。** 6,000 次 CONNECT 的
	  `session_writes_total` 是 6,000，但旧脚本曾在客户端关闭后立即抓到
	  9,667 个 leader logical command；这不是重复提案，而是 6,000 条断开
	  Save 当时只完成了 3,667 条。`connect-stage-ab.sh` 现在等待连接数、Access
	  queued/in-flight 与 leader queued/in-flight 全部归零，30 秒未排空或少一
	  节点即整轮无效，并分别打印 CONNECT 与非 CONNECT 写。五节点 RF=3、8
	  shards、PROXY 指纹的 600 条机制轮精确闭合为 600+600=1,200；Linux 4 vCPU
	  三轮 6,000 条均精确闭合为 12,000、zero errors，CONNECT/s 为 8,514.5 /
	  10,232.2 / 9,888.7，中位 9,888.7，p99 中位 275 ms。它修正的是观测边界，
	  与旧 9,648.3/s 同档，不宣称热路径提速。

- [x] **百万冷连负载机复用只读TLS与拨号配置（TST-213）。** 固定
	  ServerName的冷连不再逐连接`tls.Config.Clone`；恢复轮仍克隆并安装按
	  clientId隔离的ticket cache，动态ServerName也不污染共享配置。默认路径与
	  每个显式源IP各预构建一份并发安全`net.Dialer`，Linux端口延迟绑定Control
	  回调仍随对应源IP保留。Apple M3 Pro三轮机制基准：TLS选择由54.4--59.5 ns、
	  480 B、1 alloc降到1.88--1.90 ns、0 B、0 alloc；源IP dialer由34.4--36.3 ns、
	  192 B、2 alloc降到3.71--3.72 ns、0 B、0 alloc。百万冷连少约641 MiB累计
	  分配和300万个GC对象。Linux五节点4源IP 6,000条真实socket全部成功、零错误；
	  mTLS复用版三轮中位3,914.0/s，而严格旧clone对照3,922.8/s，同档，故只登记
	  负载机GC减压、不宣称broker吞吐提升。1,000台跨节点ticket轮恢复
	  1,000/1,000，认证、Thing/策略与ownership路径仍完整执行。

- [x] **共享LoadBalancer百万验收与Kubernetes拓扑对齐（TST-214）。** 容量
	  overlay只暴露一个`openiotd-mqtt` Service，但旧聚合器硬要求五个不同MQTT
	  地址，并把每个20万loadgen片与一个节点峰值逐片对应；按README运行会被自己
	  的门禁拒绝，即使放宽地址数也会因LB散列而随机误判。密度材料现显式记录
	  `direct`/`shared-lb`：前者仍要求五地址并逐片核对；后者要求同一LB、五个
	  不同内部ADMIN/node_id、同一cluster_id，并以五节点`openiot_connections`
	  峰值之和覆盖一百万held。聚合器机制门禁证明4+6节点峰值可覆盖两个5连接片，
	  4+5会失败，共享LB材料也不能冒充direct；任一节点开跑前连接基线非零同样
	  失败，防止生产存量补足绝对峰值。Kubernetes README已改为可实际执行的
	  shared-lb命令，9090仍不加入公网Service。

- [x] **保持窗口末端逐连接应用层存活验证（TST-215）。** 旧`heldConns`只统计
	  曾收到CONNACK的连接；broker若在保持期内关闭socket，而压测器从不读取，最终
	  仍会把它算作held，服务端历史峰值也无法补上这个事实缺口。密度轮现在于共同
	  绝对终点后，对每条连接完成MQTT PINGREQ/PINGRESP，失败连接计入errors并移出
	  held，JSON写入`heldVerified=true`；五片聚合器拒绝旧材料。`net.Pipe`门禁以
	  8条正常连接和1条断连证明计数、关闭及清理语义；Linux五节点6,000条PROXY
	  指纹真实socket全部通过、零错误，约19,936.8 CONNECT/s；同一4 vCPU Linux
	  容器中的五节点RF=3完整证书认证mTLS复核也达到6,000/6,000、零错误、
	  3,548.1/s，末端PING在51 ms内全部往返。PING扫描每5秒另报
	  `wave=held-verify`进度，20万条
	  验收的最后阶段不再静默。该机制补强证据语义，不替代五台独立Linux的真实
	  mTLS百万终验。

- [x] **mTLS压测不再绕过MQTT证书认证（TST-216）。** 审计发现
	  `connect-stage-ab.sh`的direct mTLS虽然在握手层要求并验证客户端证书，MQTT
	  CONNECT却仍配置`insecure-none`；旧行只能量密码学和socket，不包含证书状态、
	  吊销、Thing与策略热视图。脚本现对direct mTLS与PROXY均显式使用`auth.mode:
	  cert`和通配压测策略，场景标为`mtls-cert-auth`；百万Kubernetes overlay也
	  显式锁定cert模式并加源码门禁，不再依赖默认值。修正后的Linux五节点RF=3、
	  6,000条为3,548.1 CONNECT/s、p99 1,159 ms、zero errors，6,000次末端PING
	  全部成功。跨节点ticket轮也恢复1,000/1,000、5,692.2/s、zero errors且
	  held PING全过，仍经过cert认证。原始文件中的旧`...-mtls-...`两行保留作
	  transport-only证据，不能与新行混用。

- [x] **TLS 密钥交换提供容量优先与后量子两档（NET-018）。** Go 1.24 默认
	  客户端与服务端共同支持时选择 X25519+ML-KEM；百万设备冷连时这笔成本
	  会同时落到 CPU、报文与 GC。设备 TLS/WSS 端点默认显式使用 X25519/P-256，
	  `post_quantum_key_exchange: true` 恢复 Go 默认混合组。Apple M3 Pro 串行
	  mTLS 五轮：混合组 755.8--759.1 us、约 160 KB/握手；容量档
	  673.3--717.0 us、约 147 KB/握手，约快 10%。三轮 6,000 真实 mTLS
	  容量档全部零错误，中位数 1,500.6 CONNECT/s；轮间 1,495.8--2,895.8
	  再次证明端到端须多轮且不能用本机单臂宣称 10% 收益

- [x] **百万连接多压测机使用不重叠 clientId 分片（TST-204）。** `mqttbench`
	  新增 `-client-offset` 并写入结果 JSON；Linux 密度脚本用
	  `LOADGEN_INDEX*CLIENTS` 自动生成编号起点。五台各 20 万时 index 0..4 正好
	  覆盖 0..999999。此前五台都用默认 `linux-density-c000000...` 会互相接管，
	  即使累计发起一百万次 CONNECT，也只能保留二十万并发连接

- [x] **百万连接五片使用共同绝对保持窗口（TST-205）。** 密度脚本新增
	  `START_AT_UNIX` / `HOLD_UNTIL_UNIX`，同步轮强制提前预置；mqttbench 在
	  建连完成后保持到共同终点，错过终点即失败。`verify-million-density.sh`
	  每片另存管理端`/healthz`；聚合校验固定五片、每片20万、五个不同`node_id`、
	  同一`cluster_id`、真实mTLS、Linux内核、窗口、区间、held总数、错误数和
	  服务端连接峰值。直连模式要求五个broker地址并逐片核对峰值；共享LB模式
	  要求一个入口、五个管理节点并核对峰值总和，避免把五次先后发生的20万、
	  地址别名指向同一节点、不同集群、不同分片形状或PROXY核心密度结果冒充验收

- [x] **官方容器按 cgroup 自动设置 Go soft memory limit（TST-206）。** 用户
	  未提供 `GOMEMLIMIT` 时，入口脚本取 cgroup/物理内存较小者的 75%，给
	  socket、page cache、mmap 与非 Go 内存保留 25%；显式数值和 `off` 均优先。
	  真实 shell 门禁覆盖自动、覆盖和关闭三条分支

- [x] **CLU-132：共识库的 tick 间隔接成了配置项，而 A/B 的结论是不要调小。**
      100 毫秒（默认）254.8 / 297.4 条/秒、20 毫秒 253.0 / 241.6、**5 毫秒
      塌到 60.8 / 81.8 并且几百条连接被拒**（tick 循环自己排队，心跳在墙上
      时间里仍然迟到，换届反复发生）。旋钮留着是为了这个结论可复现

- [x] 这一轮翻出来的三处缺陷都已修或已记：顶掉旧持久记录的那次删除从来
      没生效过（CLU-129，门禁固定种子集五个里红四个）、持久会话落盘失败
      仍回成功的 CONNACK（CLU-130，一次 6,000 条的压测里真的出现了 1,321
      条）、epoch 跨节点撞号（CLU-131，已加平手判据与指标，真正的修法要等
      ownership 服务）

## 影子更新的吞吐（CLU-127/154/190，2026-11-22，端到端组提交已实施）

结果表里是单节点 141 条/秒、集群 39 条/秒（阶梯量到的单节点高点是 187，见下）。
机制与 CLU-125 同类但更重，**而且它不走会话分片，所以 CLU-125 没有改到它**：

- `store.ShadowStore.Apply` 落盘用 `kv.Set(key, enc, true)`——`sync=true` 走
  `bbolt.DB.Update`，**连并发合并都没有**（`db.Batch` 才合并）
- `meta.LocalStore` 每条命令还要 `persistRevision(rev)`，也是 `true`
- 两次 fsync 都串在**单个**元数据组的 apply 协程里，每个节点各来一遍
- 那一处注释给的理由是「控制面的写入是人工操作的结果，丢失一次吊销的后果远
  大于一次 fsync」——对吊销与建设备成立，**对影子不成立**：影子是设备流量

- [x] 判定性验证完成（`tests/bench/shadow-attribution.sh`，数据
      `docs/baseline/throughput/shadow-sweep.jsonl`）— 单节点扫并发
      1/5/10/25/50/100：吞吐 132 / 157 / 161 / 165 / 187 / 185 条/秒，
      p50 7.5 / 31 / 61 / 148 / 253 / 526 ms。**并发涨 100 倍吞吐只涨 42%，
      延迟线性增长**，正是「某一级被串行化」的形状；并发 1 时服务时间 7.6 ms，
      恰好是两次 fsync 的量级。机制确认
- [x] 按 CLU-125 的同一招改元数据组的 apply：整批进一个事务，顺带把
      「冷层记录」与「applied index」这两次 fsync 合成一次（现在是分两次写、
      靠顺序保证崩溃窗口无害；进同一个事务则窗口消失）。注意元数据的 apply
      还会改**内存热视图**，而事务回滚不会回滚内存——不过任何 apply 错误本来
      就是致命的（节点会被停掉），所以这一点不构成障碍，但要写明白。另外它
      牵着 CLU-023 的陈旧度判定，范围比会话那处大。现已由 Dragonboat 的
      `Update(ents)` 直通一个同步事务，影子、冷记录与最终 applied index 同时
      提交；Change 与 pending 结果只在提交后交付。64 条真实 bbolt 机制基准
      三轮由逐条约 341--365 ms 降到整批 5.38--6.79 ms（约 50--68 倍）
- [x] 重跑 `tests/bench/shadow-attribution.sh` 并更新结果页。单节点阶梯由并发1
      的53.6/s升到并发400的11,661/s；三节点4分片并发800三轮为
      5,130 / 4,974 / 5,019/s。原始JSONL已入
      `docs/baseline/throughput/shadow-groupcommit-n1.jsonl` 与
      `shadow-prod-n3-sh4.jsonl`。完整 `make bench-e2e` 仍应在下一次发布基线时
      重跑；本轮影子专项和全仓测试已经完成
- [x] **结果表改登阶梯与多轮中位数**（TST-167）：单节点登11,661/s；三节点
      4分片登三轮中位5,019/s，不再引用固定10客户端的141/s或不稳定的旧区间

- [x] **压测器按职责拆文件，接线判定前移（TST-217）。** `tools/mqttbench/main.go`
	  原先 1,875 行一个文件，现拆为 `main`（旗标与结果）、`scenario`、`client`、
	  `stats`、`admin`、`tlscache`、`util`，没有行为变化。真正的修复是那条
	  「测量窗口内收到 0 条就报错」的判定：它同时承担了发现接线错误（扁平主题的
	  精确订阅不跨节点传播，ARCH-017）和刻意过载两件事，于是 `make bench-e2e`
	  在当前机器上**跑不完**——单节点 offered 30 万那一档投递确实会塌到 0，而
	  那正是这一档要记录的东西。现在接线在加压之前用一条带标记的探针判定
	  （跨节点要重试，兴趣与归属靠 mesh 周期通告），通过后记 `wiringVerified`；
	  窗口里的 0 条还原成「全丢」，而「一条都没发出去」仍然失败。`drain` 忽略
	  探针负载——按时间戳解它会得到一个五十多年的延迟样本，表现只是一个离群点。
	  门禁：`TestDrainIgnoresWiringProbe`、
	  `TestMeasureAllowsZeroDeliveryOnlyAfterWiringProbe`、
	  `TestMeasureRejectsSilentPublisher`

- [x] **建连那一栏改为多轮中位数，轮间空出 TIME_WAIT（TST-218）。** 「必须多轮
	  取中位数」此前只写在文档里，靠人记得；入库的 `cluster.jsonl` 里那个单轮
	  70.3 就是忘了的结果，结果页只好用一段话解释它为什么不算数。现在
	  `CONNECT_ROUNDS` 默认 3，每轮进 JSONL，中位数与区间由汇总给出。实装时
	  撞到一处本机限制：第 2 轮 12,000 条里只建成 4,127 条、5,870 个错误、速率
	  从 32,423 掉到 1,999——上一轮关闭的连接还占着临时端口（macOS
	  `net.inet.tcp.msl=15000`，即 TIME_WAIT 30 秒）。轮间因此默认空出 35 秒，
	  并对建成数低于九成九的轮整组作废。压测器本身仍如实记录部分失败：密度
	  验收要的正是失败明细，这条阈值只属于基线那条路径

- [x] **每连接内存测不出时不再给数（TST-219）。** 六次采样取最小值仍可能让
	  前后差为负——1,500 条时汇总表里印出过 −145 字节/连接。现在增量非正就
	  省略每连接那几项并给出说明；不夹到 0，因为那会让「测不出来」和「几乎
	  不占内存」长得一模一样。绝对水位照常记录

- [x] **阶梯可置空，环境记录加负载（TST-220）。** 三个阶梯变量此前写的是
	  `${VAR:-默认}`，而 `:-` 对空值也取默认，于是 README 里「置空则不跑」
	  根本做不到。`env.json` 现在记 1 分钟平均负载：压测器与被测进程共用一台
	  机器，两轮对不上时这是第一个要看的字段

- [x] **阶梯的丢弃列必须与错误列并排读（TST-221）。** 排查「同一档基础级
	  两轮分别丢 0.00% 与 10.83%，而 p50 都是 0.09 ms」时查出来的：出站队列
	  满时 broker 按设计断开慢订阅者（MQTT-035），于是高档位量到的是订阅者
	  死得多快，不是吞吐。单节点 15 万那一级丢 65.89%、同一行 `errors=98`
	  ——100 对里 98 对的订阅者已经被断开。汇总表此前**不显示错误数**，那一行
	  看上去就是一个吞吐拐点。现在错误列与丢弃列并排，有错误的级单独点名，
	  并写明零丢弃上限只能在 `errors=0` 的级里取

- [x] **定速发布者显式错开相位（TST-222）。** 一百个每秒千条的发布者同时
	  起跑会在同一毫秒边界齐发，服务端看到的是每周期一百条的突发。旧实现里
	  这件事是偶然成立的（建连与订阅串行，第 i 个发布者天然晚几毫秒）；
	  TST-217 把全部建连提前到统一探完接线之后，那层偶然性就没了，所以按
	  序号显式分配周期内相位

- [x] **影子吞吐改按并发阶梯登记，并补齐三节点那条曲线（TST-223）。**
	  此前只有单节点阶梯，结论「影子被串行化在 apply 那一级、加并发没用」被
	  当成了整条路径的结论。补测三节点同一条阶梯后形状**相反**：单节点并发
	  1→100 恒在 111–119 条/秒（113.4 / 118.5 / 119.1 / 114.0 / 114.1 /
	  110.9），而三节点从 46.6 一路涨到 672.4，并发 100 时是单节点的 6.1 倍且
	  仍未到顶。历史上公开的 39 条/秒对应三节点并发 1 到 5 之间那一段（本轮
	  同位置 46.6 与 91.1），不是上限——阶梯高点是它的 17 倍。
	  **单节点没有跟着涨**是一处待归因的缺口：它走 `cluster.SingleReplica`，
	  CLU-127 的整批 apply 在上面没有体现；并发为 1 时服务时间 8.73 ms，倒数
	  正好是 114 条/秒，说明并发请求之间没有任何有效合批。
	  `shadow-attribution.sh` 同时修掉三处：硬编码仓库路径改为按 `$0` 定位、
	  macOS 自带 bash 3.2 不支持的负数数组下标、以及**缺少端口预检**——上一轮
	  残留的节点仍占端口时，新节点绑不上即退出而 `/readyz` 被旧进程应答成功，
	  整轮静默压在旧节点上，症状只是一个无线索的 401

- [x] **走共识那两栏改按并发阶梯登记（TST-224）。** 排查「集群持久建连只有
	  1,183 条/秒，是不是框架问题」时发现：那个数是按并发 256 量的，而该并发
	  根本没把这条路径压饱和。同参数（3 节点、8 分片、6,000 条）阶梯为
	  256→1,155.7、512→1,922.1、2048→**3,151.8**，差 2.7 倍；并发 2048 下三轮
	  确认为 2,976.4 / 3,227.0 / 3,269.5（中位 **3,227**，离散 ±5%）。影子同样
	  随并发上升。两栏的发布值现在都把并发写进场景。阶梯里并发 1024 那一级只
	  建成 3,697/6,000、p99 3.47 秒，是降级级而不是拐点——高并发下尾部不稳是
	  真实约束，与仓库里「偶现一条 CONNACK 超过 10 秒」同形状

- [x] **影子专项已在单机三进程环境压出稳定容量（TST-223 续）。** 真正缺口是
	  standalone `ShadowStore.Apply` 绕过 `meta.LocalStore` 组提交，仍然每更新一次
	  同步事务；改用bbolt合并事务后并发50由113.2升到2,129.8/s（18.8倍），
	  并发400为11,661/s。事件时间线原来每个Meta事件另做一次`f.Sync()`，改成
	  100ms/256条有界批同步后，三节点2分片峰值由约2,974升到3,921/s。
	  分片A/B进一步得到4分片三轮稳定约5,000/s，覆盖了当时默认
	  `shadow_api_rate=4,000/s`；8分片虽达到4,010/s但p99为1.09秒，证明同盘上
	  Raft组越多不一定越快。随后解除影子被会话`uint64`结果位图误限的64条leader
	  合批上限，改为影子专用256条；故障路由完成后五节点4分片RF=3、真实mTLS
	  同机两轮60秒长稳达到15,896--16,165/s，约96万次发布均与accepted逐条相等；
	  默认全局配额为8,000/s，按保守轮保留约50%本机长稳余量；聚合型全局速率
	  已按Raft成员数本地等分，五节点不再放大为40,000/s。五台独立Linux磁盘仍是
	  最终外部生产验收项

- [x] **影子生产准入真正按全集群8,000/s分摊（ARCH-032第一阶段）。**
	  `ShadowAPIRate`这类subject为空的聚合型全局速率按当前Raft成员数等分；
	  五节点每节点1,600/s，成员增加时旧桶余额立即钳制，后台每秒刷新成员数，
	  热路径只读原子分母，实际分母由`openiot_quota_global_rate_members`暴露。
	  控制台仍展示全集群8,000而非本地份额。剩余的周期
	  再均衡用于回收流量偏斜下的闲置额度，不影响防止N倍过载这一安全边界；
	  每clientId类全局配额也不能用整数除法解决，仍需一致哈希或协调者

- [x] **补齐真实五机影子生产验收入口（TST-229）。** 新增
	  `shadow-production-linux.sh`直接压已部署集群，不再由本机脚本伪装多机；
	  它核对五个不同node_id、同一cluster_id、4分片/RF=3、唯一leader、全局
	  配额分母、60秒吞吐/p99/零错误/逐条accepted及合批宽度，并保留五节点
	  前后metrics。容量轮要求逐节点overview确认`shadow_api_rate=0`，结束后恢复
	  8,000/s。独立验证器已有接受完整证据、拒绝重复leader与错误RF的假材料门禁，
	  `make bench-verify-test`执行。门禁还强制归档五份主机证据，要求machine-id
	  与数据盘/云卷ID分别唯一，并用假证据覆盖重复主机、重复磁盘与节点错配；
	  不同卷ID仍不能代替底层存储阵列故障域核查。可选的配对
	  `FAILOVER_HOOK`/`RESTORE_HOOK`会进一步从非副本Access
	  持续发压并停止真实leader，门禁5秒恢复、3秒accepted断档、99%成功率、
	  版本1→2与终态守恒，并恢复旧节点、核对身份、分片ready、持久化applied
	  index追平及从恢复节点读到version 2；它不擅自SSH（TST-230）。真实五机结果和
	  独立五机长稳与存储故障域证据仍待外部环境

- [x] **两个归因脚本补齐轮间资源释放（TST-225）。** `connect-stage-ab.sh` 加
	  `TIME_WAIT_GAP`（连续第三轮曾只建成 2,802/6,000、量到 260.9 条/秒，真因
	  是压测端拿不到本地端口而不是服务端）；`shadow-attribution.sh` 的 cleanup
	  改为等端口消失再退出，并加端口预检、`NODES=1|3`、`SHADOW_SHARDS` 与
	  `LADDER` 参数，同时修掉硬编码仓库路径与 macOS bash 3.2 不支持的负数数组
	  下标

- [x] **影子生产门禁补齐可靠响应通道（TST-231）。** 服务端accepted/rejected
	  本来按QoS 1发布，但旧压测器只以QoS 0订阅；高负载下响应可在出站队列被
	  丢弃，形成“Raft提交成功、proposal errors为0、客户端却歧义超时”的假写入
	  瓶颈。`mqttbench`新增独立`-shadow-response-qos`，对所有QoS 1响应（包括迟到
	  token）逐条PUBACK，并在JSON记录口径；生产容量与故障轮固定QoS 1，历史归因
	  默认QoS 0保持可比。Linux同容器五进程的QoS 0严格长稳基线为14,753.3/s、
	  p99 50.58ms、885,744条零错误；24K/s过载档因338--1,034个响应超时已明确
	  标为无效容量证据。进一步分段计数发现压测器的响应期限参数误接到普通
	  QoS ACK循环，影子仍硬编码5秒；修正后歧义超时立即停止该thing写流，生产
	  期限20秒覆盖10秒apply加5秒拨号预算。非leader本地副本也改为直接取leader
	  状态机结果，不再等接入follower追平；读路径已有Barrier。一容器五进程
	  Linux安全长稳档现为800并发、**24,892.1/s**、p99 55.77ms、最大188.57ms、
	  accepted最大断档142.94ms、1,495,660条逐条相等，较旧14,753.3/s严格档提高
	  约68.7%。1,600档虽零错误24,276.4/s，却有9.76秒全局断档；1,200档虽
	  26,987.5/s且合批144.22，却有10.04秒单请求尾部。生产验证器因此同时新增
	  1秒最大断档与1秒单请求最大延迟门槛，并把合批下限128调为96，避免为批次
	  数字把系统推入长尾。阶段指标进一步证明1,200并发复现轮的10秒尾部来自
	  Raft proposal/WAL：服务>1秒11次，proposal>2.5秒5次，而193,677次
	  `WaitCommitted`没有一次超过1秒、最大276.62ms，writer/入队也全为0；真实
	  五台独立Linux/磁盘仍待外部验收

- [x] **补齐集群接线与兼容面的拒绝分支用例，门禁覆盖率回到门槛之上
	  （TST-226）。** 整体覆盖率此前 83.44%，低于 85% 的门槛——缺口集中在这
	  一轮新增最多的两个包（`internal/server` 70.0%、`internal/cluster`
	  79.3%）与兼容面（`internal/awsapi` 66.8%）。补的全是**拒绝分支**：
	  所有权关闭后的四个入口、未启动的组做成员变更、分片号越界、布局不一致、
	  放置表的六种无效输入、兼容面列举与解绑的参数校验、证书状态与角色别名
	  与自有 CA 的读改删、WebSocket 与控制台两处帧解析的十余种畸形帧、
	  PROXY v2 的十六条拒绝、会话 save 的队列三态与 epoch 围栏、expire 的
	  分片归属过滤。后续影子故障路由、恢复观测与全局配额代码一度令整体覆盖率
	  回落到84.4%；补上非副本Access的leader冷解析/缓存失效真集群用例，以及
	  Shadow Raft写入后ready/leader/applied-index指标用例后，当前为**85.3%**。
	  严格档mqtt/authz/shadow分别为98.0%/97.9%/95.6%，`make gate`各阶段全绿。
	  过程中查出两处与覆盖率无关的问题：`forwardSessionRead` 与
	  `forwardOwnershipClaim` 是合批落地后的**死代码**（接收端仍保留，滚动
	  升级时旧节点还会发），以及自己写的两节点用例一度让
	  `TestMultiProcessRaft` 在全量插桩下变红——四套 NodeHost 同处一个测试
	  二进制，已合并为共用一个集群

- [x] **分片布局文件的 rename 之后补上目录 fsync（TST-227）。** 评审
	  `ensureShardLayout` 时发现：它做了临时文件 fsync 与原子 rename，但没有
	  同步目录项，而同仓库 `internal/node/datadir.go` 与 `internal/store/swap.go`
	  的原子写都做了这一步。那个文件是分片放置的不可变锚点，丢了它下次启动
	  会把改过的配置当成首次布局静默接受

- [x] **单机元数据写入改为组提交（CLU-190）。** 排查「单节点影子吞吐不随并发
	  上升」时定位到：`LocalStore.Apply` 全程持 `s.mu`，锁里包着一次同步落盘
	  事务，并发调用一条也合不了批。先量再修（CLU-125 的教训）：探针
	  `TestLocalApplyRate` 给出并发 Apply **160** 条/秒对整批 **2,628** 条/秒
	  ——16.4 倍的余量都在这一层。改为组提交之后 apply 层 160 → **1,318**
	  条/秒（8.2 倍，64 条命令合成 2 个事务），端到端单节点影子
	  115 → **160** 条/秒（+39%），并发 1 的服务时间 8.73 → 5.91 ms。
	  改完这一层时端到端**仍然不随并发上升**（并发 1 到 100 都是 154 到 166），
	  当时记作 `[~]` 并留了「上游还有一处串行、尚未定位」——与 CLU-126 那一轮
	  同一形状（apply 不再是瓶颈，剩下的在上游）。那处已于 2026-11-23 定位，
	  见本条末尾的补记，因此改记 `[x]`。
	  实装中撞到两处必须写下来的约束：**固定合批窗口会让节点起不来**（启动期
	  `Registry.Restore` 逐条串行调 Apply，200 微秒窗口乘以上万条记录直接吃光
	  就绪预算），以及**无条件开写事务会死锁**——`Restore` 在打开的 bbolt 读
	  游标里调 Apply，而写事务扩文件时要拿 `mmaplock`，那把锁要求没有打开的
	  读事务，实测 worker 卡在 bbolt 的 mmap 上。所以只有带记录或带影子的命令
	  才进合批，且这条路径不持久化 revision（与旧语义一致）

	  **2026-11-23 补记：上游那处串行已经定位并修掉**（影子写只在本节点就是
	  shard leader 时才本地 Apply，否则整条转发给 leader，见
	  `internal/server/cluster.go`）。旧路径只判断「有本地副本」，于是接入落在
	  follower 时先把提案转给 leader，leader 提交之后还要同步等**这个
	  follower** 的本地状态机追平——那一步把并发压在了单副本的 apply 速度上。
	  单节点影子阶梯因此从「并发 1 到 100 恒在 154–166 条/秒」变成线性扩展：
	  并发 1 为 61.8、50 为 2,787.6、200 为 10,136.9、400 为 **12,489.9**
	  条/秒（p50 26.8 ms、p99 58.6 ms、零错误、零 proposal 错误）。
	  CLU-190 的组提交是这条路径的前提，两处合起来才有这个结果

- [x] **本轮评审的四处修正（TST-232 至 TST-234）。** 读新改动时查出的，都不改
	  功能：
	  - `internal/broker/pubsub.go` 与 `internal/shadow/replicated.go` 新增的
	    两处 `time.Now` 缺少契约豁免注释（保留主题的服务耗时、`WaitCommitted`
	    的本地副本追平耗时，都属契约列出的「超时」用途），`make contract` 红
	  - `TestShadowLoopAcknowledgesQoS1Response` 有竞争：先发应答再关 `stop`
	    时两条分支的断言不同（TST-232），改为先关 `stop`——被测协程此刻阻塞在
	    `recv()` 上，关它不会打断，而处理完应答回到外层 select 必然看得见；
	    等待上限 2 秒 → 5 秒
	  - `TestMultiProcessRaft` 用「恰好多了一条」断言失去多数派后的转发结果，
	    而歧义写可能二次提交（命令不带 ID，CLU-124），改为「杀节点前记下条数、
	    之后断言未变化」（TST-233）
	  - `make cover-check` 的整体那一跑把 `go test` 输出丢进 `/dev/null`，任何
	    包失败都只剩一行 `Error 1`（TST-234），改为落临时文件并在失败时打印
	    `FAIL`/`panic` 行。另外 `internal/store/shadow.go` 顶上那段注释还写着
	    已经不存在的 per-call sync 标志，按现在的落盘路径重写；`make clean` 与
	    `.gitignore` 补上 `mqttbench` 与 `*.test`

- [x] **影子阶梯重测并更新三处文档（2026-11-23）。** 上一轮登的两个「阶梯高点」
	  都不是高点：单节点停在并发 400、三节点 4 分片停在 800，而两边那时都还在
	  涨。本轮各自向上压到回落为止，并按 TST-218 复测三轮：
	  - 单节点并发 800 为 **14,314.2 条/秒**（三轮 14,314.2 / 14,489.9 /
	    13,670.5，离散 ±3%），p50 53.1 ms、p99 95.4 ms、零错误；并发 1600 吞吐
	    不变而 p50 翻到 110 ms，是排队不是增产，所以高点在 800
	  - 三节点 4 分片并发 3200 为 **15,282.3 条/秒**（两个零错误轮取低值，另一轮
	    15,537.2；第 2 轮 14,415.1 带 367 次 proposal 错误，按 TST-221 整轮
	    作废），p50 198.3 ms、p99 293.0 ms，合批 218–227 条逻辑写／条物理日志；
	    并发 6400 降到 14,371 且 p50 翻到 412 ms
	  - **未启用 `shadow_shards` 的三节点另登一档**：高点在并发 50 的 337 条/秒，
	    此后随并发**下降**（并发 400 为 219、p50 1.4 秒）——单条元数据日志饱和
	    后纯排队。这一档此前没有登过，而默认值就是 0，漏了它读者会以为集群
	    开箱即是上面那一档
	  跟改的文档：`README.md` 与 `README.en.md` 的影子行与读表须知、
	  `docs/baseline/throughput.md`（两张结果表、百万设备余量表、新增一节
	  「影子：单节点 1.43 万/秒，三节点 4 分片 1.53 万/秒」、CLU-127/154 那一节
	  里已过期的三条推论）、`CLAUDE.md` 第 6 节第 19 条、
	  `deploy/cluster/README.md` 的能力表与分片数选择。原始 JSONL 入
	  `docs/baseline/throughput/shadow-leaderroute-{n1,n3-sh4,n3-unsharded}.jsonl`。
	  `deploy/cluster/README.md` 里 4 分片对 8 分片那段 A/B 是合批上限 64 时期
	  的，8 分片未在新代码下重测，所以那段改成「结论是 4 优于 8，而不是那两个
	  绝对值」

