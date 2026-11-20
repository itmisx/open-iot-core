# 02 架构契约

## 1. 原则

| ID | 原则 | 含义 |
| --- | --- | --- |
| ARCH-001 | 集群是接口，单机是实现 | 所有跨节点关注点在 v0.1 以接口形式落地，单机实现填充。单机不是集群之前的版本，而是节点数为一的集群。 |
| ARCH-002 | 连接路径不许有网络调用 | 认证、鉴权、配额判定仅查本节点内存物化视图。元数据经复制日志更新视图。 |
| ARCH-003 | 声明式取代回调 | 策略在进程内求值，不引入网络回调。 |

## 2. 四个数据平面

| 平面 | 状态 | 一致性 | 单机实现 | 集群实现 | 失败语义 |
| --- | --- | --- | --- | --- | --- |
| 元数据 | Thing、证书与状态、策略 | 强一致写；每节点本地物化读视图 | 一副本 Raft 组 | meta Raft 组，3 或 5 voter 加其余 learner | 丢 quorum 时写拒绝，读继续 |
| 会话与路由 | clientId 归属、订阅兴趣、共享订阅组成员 | 归属须 fencing；兴趣最终一致 | 内存 map 加本地订阅树 | 分区 leader 内存租约加兴趣传播 | 可短暂重复或漏投；绝不允许同 clientId 双活 |
| 消息 | 节点间 PUBLISH 转发 | 无 | 进程内直投 | mTLS 长连接 mesh，批量加背压 | QoS 0 丢弃，QoS 1 由发送侧重试 |
| 持久队列 | 离线 QoS 1 队列、retained、共享订阅积压 | 可插拔 | 本地 WAL | 离线队列经会话分区组复制（CLU-111）；retained 经 mesh 广播最终一致（MQTT-122）；共享订阅积压仍未实现 | 离线队列不丢；retained 的窗口是一次广播；共享订阅积压见 MQTT-095 |

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-004 | 四个平面的存储与一致性机制独立选型，不得合并为单一"集群模块"。 | v0.1 |
| ARCH-005 | 持久队列的失败语义须在 README 首屏声明——它决定使用者敢不敢把 QoS 1 当成「到了就不会丢」。**修订说明（2026-11-20）**：原文是「默认持久队列后端为 pinned-to-node，节点故障时离线队列丢失」，这一半已随 CLU-111 作废：离线队列经会话分区组的 raft 日志复制，副本因子等于节点数，节点故障时队列不丢。要声明的改成另外三条——（1）**一个节点挂着期间，发给它上面那些离线设备的消息不再入队**（MQTT-128：入队要求本地路由表里有那个订阅，而订阅只留在设备上次接入的那个节点上）；（2）在途消息不复制（CLU-113），节点死掉时那几条重传而不是丢；（3）retained 的跨节点同步是最终一致的，窗口是一次广播（MQTT-122）。三条都不是「以后会补」，是取舍：复制在途或订阅会把日志写入频率抬到与消息同级。 | v0.1 |

## 3. Day-1 接口契约

判据：凡会改变函数签名或数据结构形状的，必须在 v0.1 落地；凡只换实现体的，可延后。

### 契约 01 会话归属与抢占围栏

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-010 | `Epoch` 为 `uint64`，对每个 clientId 单调递增且跨节点全局有效。 | v0.1 |
| ARCH-011 | 所有改变会话状态的操作须携带并校验 epoch，包括单机下的本地操作。epoch 不匹配返回 `ErrStaleEpoch`。 | v0.1 |
| ARCH-012 | `SessionOwnership` 接口提供 `Claim`、`Release`、`Lookup`、`Evictions`。`Claim` 须保证旧持有者收到驱逐通知或因租约过期失效。 | v0.1 |

```go
type Epoch uint64

type SessionOwnership interface {
    Claim(ctx context.Context, clientID string, node NodeID) (Epoch, error)
    Release(ctx context.Context, clientID string, e Epoch) error
    Lookup(ctx context.Context, clientID string) (NodeID, Epoch, bool)
    Evictions() <-chan Eviction
}
```

延后代价：会话层全部方法签名需追加 epoch 参数，调用点遍布协议状态机、投递路径、持久化，等于会话模块全量重写。

### 契约 02 路由返回本地与远端两份目标

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-013 | `Route` 从 v0.1 起返回 `(local []SubRef, remote []NodeID)` 两份结果。单机下 `remote` 恒为 nil。 | v0.1 |
| ARCH-014 | 投递器从 v0.1 起含本地投递与远端转发两条分支。单机下远端分支不进入但必须存在。 | v0.1 |
| ARCH-015 | 共享订阅走独立的 `RouteShared`，因为选取订阅者的逻辑是全局的。 | v0.1 |

```go
type Router interface {
    Subscribe(sub Subscription) error
    Unsubscribe(clientID, filter string) error
    Route(topic string) (local []SubRef, remote []NodeID)
    RouteShared(topic string) []SharedGroupTarget
}
```

延后代价：投递路径、retained 下发、遗嘱、共享订阅、影子与作业回包全部需改，且这些路径通常已假设目标为本地指针。

### 契约 03 主题分类：设备域与通配符域

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-016 | 主题在路由前分类。`ClassDeviceScoped` 的目标节点经一次哈希分区的归属查询定位，不进入全局订阅表。 | v0.1 |
| ARCH-017 | 仅通配符订阅需要跨节点兴趣传播。精确订阅不得进入全局复制。 | v0.1 |
| ARCH-059 | 设备域主题的路由经 `SessionLocator` 接口完成，单机实现恒返回本节点。 | v0.1 |

```go
type TopicClass uint8

const (
    ClassDeviceScoped TopicClass = iota // $aws/things/{thing}/...
    ClassReservedExact
    ClassGeneral
)

func Classify(topic string) (TopicClass, ScopeKey)
```

理由：设备只订阅自己的主题，后端应用订阅通配符且连接数是数十而非数百万。
全局复制整棵订阅树在百万级不同 filter 下不可扩展。

**实现时发现的机制修正**：早先的表述是「归属节点由 scope key 直接哈希算出」，
这不准确。thingName 的哈希给出的是*所有权记录*所在的分区 leader，
而会话本身在设备实际连上的那个节点，二者通常不是同一个。

因此设备域主题的路由是「一次哈希分区的归属查询」，而非「零查询」。
杠杆依然成立：数百万条精确订阅不进入全局订阅表，只有后端应用的通配符订阅
需要跨节点传播，而那是数十条而非数百万条；归属查询本身在 CONNECT 时就要做，
远比全局复制订阅树便宜。该查询经 `SessionLocator` 接口完成（ARCH-059）。

### 契约 04 会话状态与会话运行时分离

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-018 | `SessionState` 为纯数据，可序列化。禁止含 `net.Conn`、`chan`、`func`、`sync.*`、`unsafe.Pointer`。 | v0.1 |
| ARCH-019 | `SessionRuntime` 持有连接与 goroutine，不可序列化，生命周期短于 `SessionState`。 | v0.1 |
| ARCH-020 | CI 须以自定义分析器强制 ARCH-018 的字段类型白名单。 | v0.1 |

延后代价：会话结构体是全系统引用最密集的类型。拆分需改协议状态机、投递、保活、流控、持久化、指标采集，并极易引入并发竞态。本条若省略，等于重写 broker。

### 契约 05 元数据：复制日志加本地全量物化视图

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-021 | `MetaStore.View()` 返回的 `MetaView` 的所有方法签名不得含 `context.Context` 或 `error`。该签名即"本地内存、不阻塞、不失败"的契约。 | v0.1 |
| ARCH-022 | CI 须以自定义分析器强制 ARCH-021。 | v0.1 |
| ARCH-023 | 元数据写入经 `Apply(ctx, cmd)` 走复制日志。读取不得经过写入路径。 | v0.1 |
| ARCH-024 | 策略在 apply 阶段一次性编译为匹配器，不得在消息路径上解析策略文档。 | v0.1 |

```go
type MetaStore interface {
    Apply(ctx context.Context, cmd Command) error
    View() *MetaView
}

func (v *MetaView) Thing(name string) (*Thing, bool)
func (v *MetaView) CertStatus(fp CertFingerprint) (CertStatus, bool)
func (v *MetaView) PoliciesFor(p Principal) []*CompiledPolicySet
func (v *MetaView) Revision() uint64
func (v *MetaView) LastConfirmedAt() time.Time
```

延后代价：若写成 `GetThing(ctx) (*Thing, error)` 并在 CONNECT 调用，集群化时每次连接一发 RPC，握手吞吐崩塌；届时被迫重写认证路径并补缓存与失效机制，而缓存失效的正确性问题比一开始做物化视图更难。

### 契约 06 分区单写者

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-025 | Shadow 的写入须经 `Partitioner` 判定归属，本地则直接处理，否则转发 owner。单机下转发分支不进入但必须存在。 | v0.1 |
| ARCH-026 | 分区数在集群创建时确定且之后不可变更。默认 64。 | v0.1 |

```go
type Partitioner interface {
    Owner(key string) NodeID
    IsLocal(key string) bool
    Count() int
}
```

### 契约 07 节点间传输

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-027 | `Transport.Forward` 从 v0.1 起为批量接口并返回 error 以支持背压与重试决策。 | v0.1 |
| ARCH-028 | `Transport.Query` 提供 Admin 扇出能力，供契约 12 使用。 | v0.1 |

```go
type Transport interface {
    Forward(ctx context.Context, to []NodeID, batch []*Message) error
    OnReceive(fn func(from NodeID, batch []*Message))
    Query(ctx context.Context, to []NodeID, q AdminQuery) ([]AdminResult, error)
}
```

### 契约 08 禁用墙上时钟做排序与版本

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-029 | 核心包禁止直接调用 `time.Now()`，须经注入时钟。CI 以 `forbidigo` 强制，例外须注释豁免并说明用途。 | v0.1 |
| ARCH-030 | 时钟仅用于超时、TTL、过期、展示。禁止用时间比较作为正确性依据。 | v0.1 |

各类顺序的来源：

| 对象 | 顺序来源 |
| --- | --- |
| Shadow 版本 | 分区单写者的单调计数器，由 Raft log index 派生 |
| 消息序号 | `(NodeID, LocalSeq)` 二元组 |
| 元数据版本 | 复制日志 index |
| 会话代次 | `Epoch`（契约 01） |
| 证书状态版本 | 元数据 revision |

### 契约 09 配额作用域显式

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-031 | 每个配额在 v0.1 即确定走 `AllowLocal` 或 `AllowGlobal`，不得含糊。清单见 17-quotas.md。 | v0.1 |
| ARCH-032 | `AllowGlobal` 在集群下为近似实现（本地额度分摊加周期再均衡），允许短时超限。 | v0.4 |
| ARCH-060 | 配额以编号而非字符串标识。单一复合键会迫使热路径拼接字符串，违反 LANG-007。 | v0.1 |
| ARCH-061 | 作用域用错视为编码错误：直接拒绝并计数，使其在压测与指标中立刻可见，而非悄悄放宽 N 倍。 | v0.1 |
| ARCH-069 | 策略编译器须在编译期给出该策略引用的设备属性名清单（`AttrRefs`）。没有它，控制面只能把全部属性塞进热视图——百万设备下是几十 MB 的无用内存，而绝大多数属性从不被策略引用（CLU-053）。在鉴权路径上反复扫描策略同样不可接受：属性引用是策略的静态性质，不随请求变化。 | v0.1 |
| ARCH-066 | 会话的持久化须保持契约 01 的 epoch 围栏：`Save` 拒绝更低 epoch，`Delete` 要求 epoch 相等。围栏判定是"读、比较、写"三步，须对同一 clientId 原子——两个并发写入若交错，被接管的旧会话会覆盖掉新会话的状态。 | v0.1 |
| ARCH-067 | 会话状态整体序列化，不逐字段抄写。`State` 是契约 04 规定的纯数据结构（由 `contractcheck` 保证无指针无接口），因此可整体编码；逐字段抄写会在新增字段时静默丢数据，表现为"重启后某个属性没了"。 | v0.1 |
| ARCH-068 | 损坏记录的处置按数据性质分开：会话记录允许被覆盖（状态可由新连接重建，留着坏记录会让该 clientId 永久无法连接），影子记录须拒绝写入并报错（影子是设备数据，覆盖等于丢数据）。 | v0.1 |
| ARCH-065 | 本地存储引擎只允许出现在 `internal/store` 内，上层只见本项目定义的 `KV` 接口。覆盖冷层的 bbolt（LANG-021）与 v0.4 随 dragonboat 进来做 Raft 日志后端的 Pebble（LANG-014），两条分别检查。它是最大的依赖（构建图 26 个模块、体积 +15 MB），且其类型一旦渗进业务代码的函数签名，替换就不再是模块级改动。 | v0.1 |
| ARCH-063 | broker 经 `ReservedService` 接口处理 `$aws/` 保留主题，不直接依赖 Shadow 等平台包。服务只产出"待投递的消息"，由 broker 经正常路由投递，因此订阅匹配、`iot:Receive` 复查、QoS 与离线排队自动适用，平台服务不必各自重复实现。 | v0.1 |
| ARCH-064 | 平台自己发布的消息没有来源连接。路由路径须处理发布方为空的情形——No Local 对这类消息不适用。遗嘱消息、Shadow 响应、经 REST 触发的通知都走这条路径。 | v0.1 |
| ARCH-062 | 第三方 YAML 库只允许出现在 `internal/config` 内，与 LANG-015 对共识库的约束同一模式：外部库关在实现体里，上层只见本项目的类型。配置结构体本身不带库的 tag 语义以外的任何耦合。 | v0.1 |

```go
// 配额以编号标识（ARCH-060）：subject 是作用对象（如 clientId），
// 节点级配额传空串。两者都不参与字符串拼接，判定路径零分配。
type Limiter interface {
    AllowLocal(id ID, subject string, n int) bool
    AllowGlobal(ctx context.Context, id ID, subject string, n int) bool

    // 计数型配额须成对调用。
    Acquire(id ID, subject string, n int) bool
    Release(id ID, subject string, n int)

    // 大小型配额直接比较。
    Within(id ID, v int) bool
}
```

### 契约 10 Retained 与共享订阅组是全局状态

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-033 | Retained 的覆盖判定依据 `Version`，不依据时间戳。集群下 `Version` 的具体形状见 MQTT-123：**Lamport 逻辑时钟加节点 ID 的平手判据**，既不是墙上时钟（那正是本条禁止的），也不是 Raft 日志序号（retained 不进共识，见 MQTT-122）。此前本条旁边的代码注释写的是「集群下来自分区的 Raft log index」，那是分区组那一步的设计，分区组没有实现而 retained 先接上了跨节点，所以以 MQTT-123 为准。 | v0.1 |
| ARCH-034 | 共享订阅组成员与选取逻辑经 `SharedGroups` 接口，不得直接操作本地结构。 | v0.1 |

```go
type RetainedStore interface {
    Set(topic string, m *Message, v Version) error
    Delete(topic string, v Version) error
    Match(filter string) []*Message
}

type SharedGroups interface {
    Join(group, filter, clientID string, node NodeID, persistent bool) error
    Leave(group, filter, clientID string) error
    Pick(group, filter string) (SubRef, NodeID, bool)
    Members(group, filter string) []Member
}
```

### 契约 11 结构化事件总线

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-035 | 所有可观测事实以结构化事件产出，单一事件源服务四个消费者：`$aws/events` 保留主题、控制台实时流、审计日志、指标聚合。禁止为各消费者分别埋点。 | v0.1 |
| ARCH-036 | `ConnEvent` 须含断连原因码与可读原因、证书指纹与主题、认证方式、协议版本、TLS 版本、ALPN、握手耗时。 | v0.1 |
| ARCH-037 | `AuthzEvent` 须含判定结果、命中策略名与语句下标、以及全部策略变量的求值结果。 | v0.1 |

```go
type ConnEvent struct {
    ClientID    string
    Epoch       Epoch
    Thing       string
    Result      ConnResult // Accepted / AuthFailed / Throttled / Evicted / ...
    ReasonCode  byte
    ReasonText  string
    CertFP      CertFingerprint
    CertSubject string
    AuthMethod  AuthMethod
    ProtoVer    byte
    RemoteAddr  netip.AddrPort
    TLSVersion  uint16
    ALPN        string
    HandshakeMS float64
}

type AuthzEvent struct {
    ClientID   string
    Action     Action
    Resource   string
    Decision   Decision // Allow / ExplicitDeny / ImplicitDeny
    PolicyName string
    StatementI int
    Vars       map[string]string
}
```

延后代价：埋点位置分布在协议状态机、TLS 握手回调、策略求值器内部，均为热路径与深层函数。且策略变量求值的中间结果在未捕获时无法事后重建，补做等于重写策略求值器。
`AuthzEvent.Vars` 是 15-console.md 的策略调试器能够存在的唯一前提。

### 契约 12 双 API 平面与 Admin 扇出

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-038 | `/api/aws/*` 为兼容面，契约冻结。不得为满足控制台需求修改其响应形状。 | v0.1 |
| ARCH-039 | `/api/admin/v1/*` 为自有面，支持 REST 与 SSE/WebSocket，独立 RBAC。控制台只使用该面。 | v0.1 |
| ARCH-040 | Admin 列表接口的分页游标须编码每个节点的读取位置，不得为单一 offset。 | v0.1 |
| ARCH-041 | Admin 扇出须支持部分失败：超时节点在响应中标注，不得使整个请求失败。 | v0.1 |

```go
type AdminAPI interface {
    ListConnections(ctx context.Context, f ConnFilter, cur Cursor) (*Page[Conn], error)
    StreamEvents(ctx context.Context, f EventFilter) (<-chan Event, error)
    KickClient(ctx context.Context, clientID string, reason byte) error
}
```

### 契约 13 无指针索引化订阅树

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-042 | 订阅树节点存于 slice，节点间以 `int32` 索引互指，不使用指针。节点回收经 freelist。 | v0.1 |
| ARCH-043 | 订阅树的 GC 扫描成本须与订阅数无关。基准测试须覆盖百万级订阅下的 GC 暂停与扫描时间。 | v0.1 |

```go
type trieNode struct {
    firstChild int32 // -1 表示无
    nextSibling int32
    segment    int32 // 段字符串的 intern 表索引
    subs       int32 // 订阅链表头，-1 表示无
}

type Trie struct {
    nodes    []trieNode
    freelist []int32
    interned *segmentTable
}
```

理由与延后代价：这是 Go 实现下收益最大的单项优化，使 GC 扫描从遍历百万指针变为跳过一个 slice。
订阅树的数据结构事后更换会牵动整个路由层。详见 01-language-runtime.md 的 LANG-004。

## 4. 可安全延后的部分

以下仅更换实现体，不改签名或数据结构形状，延后成本接近于零。
在 v0.1 投入这些是主要的浪费来源。

| ID | 可延后项 |
| --- | --- |
| ARCH-044 | 共识库具体选型。 |
| ARCH-045 | 节点发现与成员管理机制（静态列表、gossip、Kubernetes、DNS SRV）。 |
| ARCH-046 | 持久会话的复制后端（v0.4）。v0.1 的本地持久化已落地。 |
| ARCH-047 | 兴趣传播的具体编码（全量 filter 列表、bloom、cuckoo 摘要）。 |
| ARCH-048 | 跨节点共享订阅的负载均衡算法。 |
| ARCH-049 | 再均衡策略。注意 MQTT 5 server redirect 的协议支持本身属 Day-1。 |
| ARCH-050 | 多可用区与跨集群桥接。 |
| ARCH-051 | 控制台的图表与交互细节。 |

## 5. 包结构

```
cmd/
  openiotd/              单一静态二进制：broker、控制面、控制台、内嵌共识
  openiotctl/            本地运维 CLI
internal/
  mqtt/                  纯协议。零业务依赖，可独立 fuzz
    packet/              3.1.1 与 5.0 编解码，零分配热路径
    reason/              reason code 表
    topic/              校验、无指针 trie、通配符匹配、TopicClass 分类
  session/
    state.go             SessionState 纯数据（契约 04）
    runtime.go           SessionRuntime 持连接与 goroutine
    epoch.go             Epoch 校验（契约 01）
  router/
    local.go interest.go route.go shared.go retained.go
  cluster/
    ownership.go partition.go transport.go
    raft/                dragonboat 封装：meta 组、shard 组、成员变更
    discovery/
  meta/
    view.go log.go model/
  authn/                 mtls sigv4
  authz/
    policy.go vars.go
  pki/                   ca issue revoke keystore/{file,pkcs11,kms}
  shadow/
  events/                bus.go aws.go audit.go
  quota/
  api/
    awscompat/           /api/aws/*，契约冻结
    admin/               /api/admin/v1/*，含 fanout.go
  console/embed.go
  obs/                   metrics tracing logging
  clock/
web/                     控制台前端源码
test/
  conformance/ interop/ fuzz/ cluster/ sim/
pkg/mqttcodec/           可对外开放的编解码库
```

## 6. CI 强制规则

契约写下来不等于守得住。以下规则机械检查，是契约不腐化的唯一可靠手段。

| ID | 规则 | 守护 | 实现 | 状态 |
| --- | --- | --- | --- | --- |
| ARCH-052 | `internal/mqtt/` 不得 import 任何其他 internal 包 | 协议纯度 | `contractcheck` | 已实现 |
| ARCH-053 | `SessionState` 字段类型白名单 | 契约 04 | `contractcheck` | 已实现 |
| ARCH-054 | `MetaView` 方法签名不得含 `ctx` 或 `error` | 契约 05 | `contractcheck` | 已实现 |
| ARCH-055 | 核心包禁止直接调用 `time.Now()` | 契约 08 | `contractcheck`，豁免注释 `//contract:allow-time-now` | 已实现 |
| ARCH-056 | `api/awscompat` 响应结构变更须 golden diff 人工确认 | 契约 12 | golden 文件加 CI 门禁 | 已实现 |
| ARCH-057 | 单机测试须跑集群代码路径（一副本 Raft 组），禁止旁路实现 | ARCH-001 | 默认测试配置 | 已实现 |
| ARCH-058 | 订阅树禁止出现指针类型字段 | 契约 13 | `contractcheck` | 已实现 |

YAML 库与存储引擎的隔离（ARCH-062、ARCH-065）同样由 `contractcheck` 检查。
需求在上文，本表不再单列，避免同一个 ID 出现两次。

五条可静态判定的规则由 `tools/contractcheck` 实现。它只依赖标准库——
这些规则在语法层面即可判定，不需要类型信息，因此不引入
`golang.org/x/tools`（见 LANG-019 的依赖纪律）。

规则表在 `tools/contractcheck/main.go` 中集中登记，并有两类测试保障：
每条规则各有故意违反的样例验证其确实触发（一个永远通过的检查器
比没有检查更糟），以及规则表引用的类型被重命名或删除时报出
「规则表已过期」，防止契约随重构静默失效。

## 门禁规则自身的检验（实现期补充）

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-070 | import 隔离规则须有「规则表过期检测」：每个直接第三方依赖要么被某条规则隔离，要么在豁免名单里**写明理由**。机制有测试不等于规则表写对了——冷层由 Pebble 改为 bbolt 之后，ARCH-065 守着一个项目不再使用的库，而真正在用的 bbolt 可被任意包直接 import，门禁照常通过。 | v0.1 |
| ARCH-071 | 每条 deny 前缀须有测试证明它真的能拦住东西。一个拼错的前缀（少一个斜杠、域名写错）会让规则静默失效：它仍在规则表里，门禁仍通过，而约束已经不存在。 | v0.1 |
| ARCH-072 | 多个引擎的隔离规则分开写，不合并成一个前缀列表。它们的理由不同（bbolt 是当前冷层，Pebble 是 v0.4 的 Raft 日志后端），合并后违反报告说不清到底违反了哪一条约束。 | v0.1 |

### 为什么这一类测试值得写

本项目用 `contractcheck` 把设计约束变成机器可查的规则，其价值取决于规则
本身是对的。一条失效的规则比没有规则更糟：它让人以为约束还在，于是不再
人工检查。

这与 OBS-121（服务端事件类型与控制台监听须钉在一起）是同一类——**凡是
靠人工维护一张表来保证正确性的地方，那张表就需要自己的测试。**

## 外部协程的边界（实现期补充）

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-073 | 连接的编码缓冲、读缓冲与会话状态归该连接的**读协程独占**。投递路径跑在发布者的读协程上，必须自带编码缓冲，不得借用订阅者的——两个发布者同时投给同一个订阅者时，两次编码写进同一块缓冲，拷出来的是两个报文的混合，订阅者收到畸形报文后断连。分配次数没有增加：入队本来就要求一份副本。 | v0.1 |
| ARCH-074 | 出站队列的「是否已解绑」判定与入队必须在**同一个临界区**内。分处两次加锁之间的那道缝会让投递协程向已关闭的通道发送而 panic，而本项目没有任何 `recover`——一次完全正常的"客户端在别人正给它投消息时断开"就能让进程退出。 | v0.1 |
| ARCH-075 | 不得以 `close(工作队列)` 作为工作池的停工信号。入队发生在任意协程上，`closing` 标志的判定与随后的发送之间同样有缝，关机时刚好有报文入队就是向已关闭通道发送。停工须走独立的 `done` 通道，使关机后的入队最多变成一个没人取的元素。 | v0.1 |

### 为什么这三条是一条

它们是同一条纪律的三个落点：**跨协程共享的东西只有两种形态是安全的——
要么全程在锁内，要么由所有者独占而别人只能发信号。**介于两者之间的"检查
一下再动手"在并发下一律是缝。

本项目此前已经为同一条纪律修过一次：`KickClient`、驱逐、接管、关机四处
曾在调用方的协程上跑完整的 `conn.close`，而那会持久化会话、发遗嘱、碰
编码缓冲。修法是 `conn.evict`——外部协程只发 DISCONNECT 并关套接字，
剩下的由连接自己的读循环醒来后处理。ARCH-073 至 ARCH-075 是同一条纪律
在投递路径、出站队列与工作池上的三处遗漏，其中两处会 panic。

## 一副本 Raft 组（实现期补充）

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-077 | 单机元数据写入先追加一副本组的日志（`cluster.SingleReplica`），再改热视图。投票者数量是 1，任期从 1 开始，追加即提交。校验失败的命令不进日志。相同命令 ID 的重复提交不追加第二条。**修订说明（2026-10-31）**：原文末句是"多节点的选举、复制和再均衡不在这一组里，完成阶段 B 之前不得宣称高可用"。阶段 B 被标为完成之后这句话的保护一度失效：自写实现并没有自动选举，leader 故障后剩余多数派选不出新 leader（CLU-096 记下了这件事）。换成 hashicorp/raft 之后选举与故障转移由库提供，并由 `tests/cluster/run.sh` 的杀 leader 用例守着（TST-147）。单机这一组不变：它没有选举可做，也因此不含上述那类缺陷。 | v0.1 |

教训是：当一条纪律在一个地方被违反时，**要把同类的全部位置一起查一遍**，
而不是只修报错的那一处（这与 UI-030 把空状态标准补齐到全部六个页面
是同一回事）。

## 兼容面 golden（实现期补充）

响应按字段比对，不按字节。键的顺序和缩进变了不算形状变了；多一个字段、少一个字段、换了错误类型，才算。

| ID | 需求 | 状态 |
| --- | --- | --- |
| ARCH-076 | 兼容面的状态码、`x-amzn-ErrorType` 和 JSON 正文钉在 `internal/awsapi/testdata/awscompat.json`。对不上就失败，测试不会自己改文件。重写只通过 `-update-golden`，并且 CI 不得打开这个开关：打开之后任何形状变化都会变成新的基线，门禁就没有了。 | v0.1 |

