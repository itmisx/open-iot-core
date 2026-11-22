# 端到端基准测试

`make bench-e2e`（或 `tests/bench/run.sh`）起真的 `openiotd` 进程，用本项目
自带的压测器 `tools/mqttbench` 按真 MQTT 连接压一遍：单节点一组场景，三节点
集群一组。入库的结果在 [`docs/baseline/throughput.md`](../../docs/baseline/throughput.md)，
原始数据与环境记录在 `docs/baseline/throughput/`。

它不在 `make gate` 里：门禁只依赖 Go 工具链，而这套要起进程、要 `openssl`
（集群的复制证书）与 `python3`（汇总表），两组跑完二十分钟上下。

## 两种规模，一台开发机

`run.sh` 起的是**裸二进制进程**，不经容器、不经编排：

| `MODE` | 拓扑 | 它能回答 | 它**不能**回答 |
| --- | --- | --- | --- |
| `standalone` | 一个 `openiotd` 进程独占整台机器 | 单节点的消息面上限、建连速率、每连接堆增量 | 复制层的代价 |
| `cluster` | 同一台机器上三个进程组成复制集群 | 同节点与跨节点的差距、会话走复制日志的建连代价、影子写入 | 跨机 RTT、三块独立的盘、容错 |

压测器与被测进程**在同一台机器上**，抢同一批核。所以这组数是「一台机器端到端
能跑多少」，不是 broker 独占整机的上限。

**测量时机比参数更影响结果。** 同一天同一台机器、同一份代码，建连那一栏量到过
27 到 163 条/秒：差别来自机器当时在忙什么。所以

- 跑之前关掉吃 CPU 的东西（容器运行时、IDE 的语言服务、会议软件）；`run.sh`
  会把 `loadavg` 记进 `env.json`，事后对不上的两轮先看这一项；
- 建连那一栏默认跑 `CONNECT_ROUNDS=3` 轮取**中位数**，单轮不作数；
- `MODE=both` 在两组之间插一轮冷却（`COOLDOWN`，默认 60 秒）。入库的那份仍然
  分两次调用产出，理由见本文末。

**容量结论不要跨规模搬运。** 三个进程共享一台机器的 CPU、一块盘和同一个回环
网卡；它们量出的「三节点合计 N」不等于三台机器能跑 3N，反过来也不成立。

需要五台独立 Linux 才成立的那一档（百万并发终验，TST-201/204/205）由
`connect-density-linux.sh` 与 `verify-million-density.sh` 承担，脚本就绪但
**本仓库没有这个环境，未验收**。

## 跑一遍

```bash
make bench-e2e                                      # 单节点与集群都跑，结果打在终端
MODE=standalone make bench-e2e                      # 只跑单节点
RESULTS=docs/baseline/throughput make bench-e2e     # 顺手把原始数据写进仓库
CONNS=2000 DURATION=5s make bench-e2e               # 跑快一点（这组数与入库的那份不可比）
```

百万连接 Linux 密度脚本默认验收直连 mTLS。要对比 TLS 终结外移后的 broker
资源档，使用同一脚本但显式指定 `TRANSPORT=proxy-v2 PROXY_CERT=...`，并让
`BROKER` 指向开启 `mqtt_plain.proxy_protocol: v2` 的受信端点。该结果只包含
broker，不包含终结器；两档必须分别保存，不能把 proxy-v2 数字标成端到端 mTLS。
默认传完整 DER；`PROXY_IDENTITY=fingerprint` 使用紧凑 SHA-256 TLV，适合
HAProxy 类终结器，但引用证书 DN/序列号变量的策略需要保留 DER 档。

可覆盖的旋钮（默认值是在 11 核笔记本上单节点约 5 分钟、集群约 15 分钟的那一组）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `MODE` | `both` | `standalone`、`cluster` 或 `both` |
| `DURATION` / `WARMUP` | `15s` / `3s` | 测量窗口与预热窗口 |
| `CONNS` / `CLUSTER_CONNS` | `12000` / `6000` | 连接场景的连接数 |
| `CONN_CONC` | `256` | 建连并发度 |
| `CONNECT_ROUNDS` | `3` | 建连场景跑几轮；中位数由汇总算，每一轮都进 JSONL |
| `PAIRS` / `PAIR_RATE` | `100` / `1000` | 一对一收发的对数与每个发布者的速率 |
| `PAIR_LADDER` | `1500,2000,2500` | 单节点的**速率阶梯**（每发布者速率，逗号分隔），置空则不跑 |
| `CROSS_RATE` | `400` | 跨节点那几档的每发布者速率 |
| `CROSS_LADDER` | `500,600,700,800,1000,1200,1500` | 跨节点的速率阶梯 |
| `LOCAL_LADDER` | `1500` | 集群**同节点**的速率阶梯 |
| `COOLDOWN` | `60` | `MODE=both` 时两组之间的冷却秒数 |
| `SUBS` / `FANOUT_RATE` | `1000` / `100` | 扇出的订阅者数与发布速率 |
| `SHADOW_CLIENTS` | `10` | 影子更新的并发客户端数 |
| `RESULTS` | 空 | 指向一个目录则把 `env.json` 与各轮的 JSONL 复制进去 |
| `OPENIOTD_BIN` / `MQTTBENCH_BIN` | 空 | 注入已构建的二进制而不是现编。两个必须同时给——A/B 的两臂要用逐字节相同的产物 |
| `KEEP_WORK` | `0` | 置 1 保留数据目录与节点日志，便于事后查 |

## 各个场景量什么

| 场景 | 量的是 | 判据 |
| --- | --- | --- |
| `connect` | 建连速率与每连接内存 | CONNACK 的到达，默认 3 轮取中位数；内存取服务端 `heap_alloc` 多次采样的最小值之差，增量低于 GC 噪声时不给数 |
| `pairs-qos0` | 转发一条消息的成本 | 订阅者收到的条数；延迟取负载前 8 字节的时间戳 |
| `pairs-qos0-ladder-<offered>` | 零丢弃的上限 | 逐级加压，直到 `deliverPerSec` 跟不上 `publishPerSec` |
| `pairs-qos0-overload` | 丢弃从哪里开始 | 刻意打过头，记的是丢弃率而不是吞吐 |
| `pairs-qos1` | QoS 1 的额外成本 | 同上，外加 PUBACK 计数 |
| `fanout` | 复制一条消息的成本 | 一个发布者、N 个订阅者，投递总数 |
| `shadow` | 控制面写入（经复制日志） | `shadow/update/accepted` 的到达，而不是 PUBACK |

集群那一组把 `pairs` 跑两遍：发布者与订阅者在**同一节点**，以及在**不同
节点**。这两条路径此前差一档（实测 10 万对 6 万），按对端异步合批（CLU-139）
之后已经接近（15 万对 12 万）；两档仍然分开跑，因为「接近」是量出来的结论，
不是可以假设的前提。

## 速率阶梯：为什么每一档都要再压几级（TST-167）

基础那一级证明的是「X 以内不丢」，**不是「X 就是上限」**——而读者只会拿它当
上限用。所以每一档后面跟一条阶梯：同一组客户端按递增的 offered 速率逐级重跑
（`-setup=false`，与基础级共用设备），直到投递跟不上 offered。结果页于是能写
成「零丢弃到 X，再往上一级掉到 Y」。

阶梯**不自己判定上限**：它只把每一级的 `publishPerSec`、`deliverPerSec` 与
`errors` 写进 JSONL，由读的人决定要的是零丢弃还是容忍百分之几。判读时有三件事：

- **先看错误列，再看丢弃列。** `errors` 非零时「丢弃」根本不是吞吐：出站队列满
  时 broker 按设计断开慢订阅者（MQTT-035），于是那一级量到的是「订阅者死得
  多快」。实测单节点 15 万那一级丢 65.89%，而同一行 `errors=98`——100 对里 98
  对的订阅者已经被断开，剩下的消息当然到不了。**零丢弃上限只能在 `errors=0`
  的那些级里取**；汇总表会把有错误的级单独点名。
- **丢弃出现之前，延迟先走。** 队列积压先体现在 p50 上，而丢弃率还没动。只看
  丢弃率会把「已经堵住」读成「还有余量」。
- **压测器自己会先饱和。** 某一级的 `publishPerSec` 明显低于 offered 时，到顶
  的是负载产生侧，那一级量的不再是 broker。

最后一条也解释了为什么这台机器上的高档位不稳：订阅者是和 broker 抢同一批核的
进程，一次几十毫秒的调度停顿就足以填满它的出站队列并招来断开。同一档基础级在
背靠背的两轮里量到过 0.00% 与 10.83%，**而两轮的 p50 都是 0.09 ms**——低延迟配
高丢弃就是这个形状，不是 broker 到顶了。

## 两组之间必须隔开（TST-169）

`MODE=both` 把单节点与集群串着跑，而集群那组的第一个场景是建连，它每条 CONNECT
要等一次 fsync 过的 raft 提交——**整组里最容易被前一组的尾巴压低的就是它**。
同一台机器上实测：紧接单节点那组之后是 27 条/秒，机器空闲时单独跑是 101 条/秒，
差 3.7 倍而代码一行没变。因此脚本在两组之间插了 `COOLDOWN`（默认 60 秒）；要
发布的那份仍建议分两次调用（`MODE=standalone` 与 `MODE=cluster`）。

## 读数之前先知道的四件事

1. **压测器与被测进程在同一台机器上**，抢同一批核。所以这里的数是「一台
   机器端到端能跑多少」，不是 broker 独占整机的上限。要后者得用第二台机器
   压，而那不是任何人都能复现的环境——而 TST-064 要求的恰恰是可复现。
2. **走明文端点（`listeners.mqtt_plain`）与 `auth.mode: insecure-none`**，
   因此不含 TLS 握手与证书校验的开销。认证跳过了，**授权没有**：策略仍然
   逐条求值，设备也仍然要在注册表里——没有策略的设备连 CONNECT 都过不了，
   所以每一轮开头都要预置设备（12000 台约一分钟）。
3. **与吞吐有关的配额被解除了**。默认配额对齐 AWS（每连接 100 条/秒、全局
   2 万条/秒），不解除的话量到的是限速器的设定值，而那个数早就写在
   `docs/prd/17-quotas.md` 里。`connect_rate_per_client` 保持默认的 1/s
   不动：每个 clientId 一轮只连一次，它不构成瓶颈。
4. **连接数的天花板来自本机的临时端口范围**（macOS 默认 16383 个），不是
   服务端。默认测 12000 条是为了留余量；要压更多得放宽 `net.inet.ip.portrange`
   或从多个源地址发起。**端口的回收也要算进来**：一轮结束时关闭的上万条连接
   要等 2×MSL（macOS 默认 30 秒）才回到端口池，所以两轮建连之间空着
   `CONNECT_ROUND_GAP`。省掉它的那一版第 2 轮只建成 4,127/12,000，而现象
   看上去像服务端垮了。

## 两个归因探针（不在 make bench-e2e 里）

它们回答的不是「跑多少」，而是「为什么是这个数」。两个都要起真进程、落真盘，
所以单独调用：

| 脚本 | 回答的问题 | 旋钮 |
| --- | --- | --- |
| `connect-attribution.sh` | 建连慢在哪一层？ | `NODES`（1 或 3）、`SHARDS`、`CONNS`、`CONC`、`BASE`、`OUT` |
| `shadow-attribution.sh` | 影子的吞吐是被并发限住还是被串行化限住？ | `OUT` |

两条判读规则，都是踩过才写下来的：

- **建连那一栏必须多轮取中位数。** 它的轮间离散比量级还大（同一天量到过 27 到
  163），单轮对单轮比较过一次，结论是反的。
- **吞吐随并发平、延迟随并发线性涨 = 某一级被串行化。** 影子那一栏就是这个
  形状（并发 1 到 100，吞吐 132 到 185，p50 7.5 到 526 ms）。

另有两个 Go 侧的探针，设环境变量才跑、不进门禁：
`internal/cluster` 的 `TestShardProposeCeiling`（`OPENIOT_SHARD_CEILING=1`，
量 apply 不落盘时的提交上限，顺带报 dragonboat 交给状态机的批大小）与
`internal/store` 的 `TestSessionApplyRate`（`OPENIOT_APPLY_RATE=1`，量存储层
逐条对整批的 apply 吞吐）。

## 压测器单独用

`tools/mqttbench` 可以脱离这个脚本跑，对着任何一个开了明文端点的节点：

```bash
go run ./tools/mqttbench -scenario pairs \
  -broker 127.0.0.1:1883 -admin http://127.0.0.1:9090 \
  -token "$(cat data/admin-token)" \
  -pairs 200 -qos 1 -duration 20s
```

`-broker` 与 `-sub-broker` 都接受逗号分隔的地址列表并轮转使用，跨节点那几
档就是靠把订阅侧的列表轮转一格做出来的。结果打在 stdout（JSON），`-out`
追加写入一个 JSONL 文件。

## connect-stage-ab.sh：建连路径的分段归因（CLU-126、TST-170）

把两条代价差两个数量级的建连路径分开量，并把 CONNECT 的四段耗时与提案侧
的三项指标一起打出来：

```bash
ARM=clean      NODES=3 SHARDS=8 tests/bench/connect-stage-ab.sh
ARM=persistent NODES=3 SHARDS=8 tests/bench/connect-stage-ab.sh
```

脚本默认从当前 worktree 编译，也可用 `OPENIOTD_BIN` / `MQTTBENCH_BIN` 注入
同一份预构建产物。`Dockerfile.linux-bench` 只提供 bash/curl/openssl 运行环境，
适合把 Linux 静态二进制只读挂进去，在同一个 Linux network namespace 内排除
宿主端口转发；它不是五台 Linux 验收环境。TLS 恢复轮可用 `RECONNECT=1`，默认
等待 1,250 ms 再发第二波，避免把每 clientId 1/s 的生产限流误判成恢复失败。
`RECONNECT_DELAY` 可显式覆盖，`TLS_HANDSHAKE_CONCURRENCY` 用于 admission A/B。
超过单源临时端口容量时，用逗号分隔的 `SOURCE_IPS` 按 clientId 稳定轮转源地址。
Linux 上压测器会为这些显式源 IPv4 启用 `IP_BIND_ADDRESS_NO_PORT`，让内核到
`connect(2)` 时才按完整四元组选端口；因此同一源端口可安全复用于不同 broker
目的地址。设置该 socket option 失败会直接令连接失败，不会静默退回到提前
`bind(sourceIP, 0)`。非 Linux 没有这项保证，容量不得乘以 broker 地址数。
默认拨号与每个显式源IP的 `net.Dialer` 在整轮内并发复用；固定ServerName的
冷TLS连接也共享构造后不再修改的 `tls.Config`。重连仍为每个clientId克隆配置
并安装隔离ticket cache，动态ServerName同样使用拨号私有clone。这样减少的是
负载机短命对象，不改变broker协议，也不能把机制分配下降直接写成CONNECT/s收益。
最终密度脚本的端口预检当前只接受IPv4 `SOURCE_IPS`；IPv6的临时地址、端口范围
和复用语义需单独取证，不能套用IPv4公式。
`MQTT_TLS=1`不只在握手层设置`require_client_cert`，还会把MQTT认证显式设为
`cert`并附加压测策略；场景名为`mtls-cert-auth`。旧脚本产生的`mtls`场景使用了
`insecure-none`，只代表TLS transport成本，不得引用为吊销、Thing和策略完整路径。
超大轮同时设置不在设备池里的 `ADMIN_SOURCE_IP`，避免设备连接耗尽后管理指标
采样无法新建HTTP连接；该地址必须已配置在压测机上。
本地集群监听端口默认从18000开始；Linux会拒绝与
`/proc/sys/net/ipv4/ip_local_port_range`重叠的`BASE`，避免Raft出站连接随机抢占
尚未启动节点的监听端口。

**一次调用只量一个点，用一个全新集群**（与 `connect-attribution.sh` 同一个
协议）。多轮由外层循环重复调用——在同一个集群里连着跑时，持久那一臂从 220
条/秒掉到 40 条/秒并出现 1,321 次提案超时，因为上一轮断开时每条连接还要再
提一条日志，而预置设备本身就是同样多条元数据日志。

输出里的四段（`auth` / `session_load` / `owner_claim` / `session_write`）是
按节点求和后的均值；逐节点那几行另外给出 `write_ms` 与 `propose_ms` 的对照，
两者接近就说明时间全在共识那一轮里。`writes` 那一列是「一次建连发了几次
逻辑会话写」，非持久会话必须是 0.00。CLU-134 之后多个逻辑写会合成
一条 Raft 日志，实际日志数看 `session_proposals_total`，不再与 `writes` 相等。
压测器返回前会关闭全部连接；持久会话还会为每条断开连接再 Save 一次。脚本
因此会等连接数、Access proposal queue 与 leader merge queue 全部归零后才抓
结束快照，并分别打印 CONNECT 路径写与非 CONNECT 逻辑命令。若30秒仍未排空，
本轮直接失败并只写入 `.invalid`，不能把 N 到 2N 之间的半截清理流量解释成
重复提案或合批宽度。
高 CONNECT 并发度不会再放大预置流量：`mqttbench` 按 512 台调用批接口，
每批只提交一条元数据日志并串行推进；`-setup-concurrency` 只为旧脚本保留，
不再制造并发元数据提案。RF 小于节点数时，输出还会给出非本地 session read
的逻辑数、批量 RPC 数与合批系数，用于验证 CLU-147，而不是从 CONNECT/s 猜测。
输出还包含 session proposal pipeline 的饱和入队累计值、当前 queued/in-flight
以及历史峰值。压测结束时当前值通常已回到零，所以判断共识背压要看历史在途
批次是否达到上限、历史排队是否增长，并与 `session_write` / 物理 Raft 提案对照。
最终客户端JSON不是单独的成功判据：脚本还要求全部broker进程仍存活，并且开始、
结束两次都能从所有节点抓到指标。少一个节点即整轮失败，避免“OOM杀掉broker后
客户端刚好已完成”被登记成高吞吐成功结果。结果先写临时文件，门禁全部通过后
才追加到`OUT`；客户端有明确错误的容量边界仍进入`OUT`并返回失败，broker死亡或
指标不完整的环境无效行则写到`OUT.invalid`，避免被自动基线读取为有效成绩。

## connect-density-linux.sh：20 万 mTLS 连接/节点

最终百万连接验收在五个 Linux broker 节点各运行 20 万连接。脚本预检 nofile、
临时端口范围、源 IP 与 broker 地址的安全容量，保存前后 Prometheus 指标、逐秒
sockstat、broker 容量指标时间线，以及 load generator 的 VmRSS/VmHWM。broker 与
脚本同机时可传 `BROKER_PID`，脚本会另外保存 broker 的 VmRSS/VmHWM；远端 broker
应在其主机独立采集进程与 socket 峰值，不能把 load generator 主机的 sockstat
误认成服务端数据。`mqttbench` 的 JSON 也报告发生器
HeapAlloc、runtime Sys、Linux RSS 与每连接值；这些字段用于确认发生器没有先于
broker 饱和，不能拿来替代服务端 RSS。压测客户端默认只常驻 192 B 内联接收区，
写缓冲在每次 flush 后释放，因此不再为百万设备额外需要约 30.5 GiB 用户态缓冲。
`peaks.txt` 会汇总上述时间线的最大值；其中 broker VmHWM 只在显式传入同机
`BROKER_PID` 时存在，而 `loadgen_tcp_*` 始终属于压测机。
脚本还会读取`nf_conntrack_count/max`并保留20%余量。conntrack满时，新TCP流会
整批超时，broker认证/Ownership指标可能完全看不到请求；这属于宿主网络容量，
不能归因为broker。最终每台load generator和broker宿主都应按20万流加系统余量
配置conntrack，或在明确不经过状态跟踪的网络路径上用现场证据说明该限制不适用。
大规模轮建议再配置一个不属于 `SOURCE_IPS` 设备池的 `ADMIN_SOURCE_IP`；
`mqttbench` 与脚本里的 curl 时间线都会绑定它，防止20万设备连接占满临时端口后
恰好无法读取最终指标。该地址必须真实配置在压测机网卡/loopback 上。

五台 load generator 并行时必须给它们设置不同的 `LOADGEN_INDEX=0..4`。脚本
按 `CLIENT_OFFSET=LOADGEN_INDEX*CLIENTS` 生成区间；在默认 20 万规模下分别是
`000000..199999`、`200000..399999`，直到 `800000..999999`。五台仍使用同一个
`PREFIX`，以便共用预置策略。也可显式传 `CLIENT_OFFSET` 覆盖计算值。若五台都
使用默认 offset 0，它们会用相同 clientId 互相接管，表面上发起了一百万次连接，
实际永远只能同时保留二十万条，不能算百万连接验收。

五台还必须使用相同的 `START_AT_UNIX` 与 `HOLD_UNTIL_UNIX`，且时钟先经 NTP
同步。例如统一约定两分钟后开始、十二分钟后结束，把同一对整数分发到五台，
并保持 `SETUP=0`（设备须提前预置）。每片若在统一结束时间之后才完成建连会
直接失败。全部结束后运行：

```bash
tests/bench/verify-million-density.sh /results/density-{0,1,2,3,4}
```

每片还会保存对应管理端`/healthz`。聚合器固定检查五份结果、每份20万、五个不同
`node_id`、同一`cluster_id`、真实`mtls`传输、Linux内核、编号区间无重叠、
开始/结束窗口一致、`heldConns`总和恰好一百万且错误为零。入口有两种合法形状：

这里的`heldConns`不是历史成功数。脚本强制启用`-verify-held`，在共同绝对保持
终点后让每条连接完成一次PINGREQ/PINGRESP；失败会计入errors并从held移除，JSON
必须带`heldVerified=true`。因此稍后成功响应的仍是建连时的同一条TCP/MQTT连接，
可证明它在共同终点没有提前消失。扫描期间stderr每5秒输出
`wave=held-verify`进度；聚合器会拒绝没有该验证字段的旧结果。

- `INGRESS_MODE=direct`（默认）：五台load generator各直连不同broker地址，
  `ADMIN`对应同一节点；聚合器要求五个不同broker目标，且每节点峰值不低于该片held。
- `INGRESS_MODE=shared-lb`：五台都连接同一个生产LoadBalancer，但每台的`ADMIN`
  必须指向五个不同broker节点。聚合时使用
  `EXPECTED_INGRESS_MODE=shared-lb tests/bench/verify-million-density.sh ...`；此时不把
  loadgen片与broker节点错误配对，而要求五节点连接峰值之和不低于一百万。

共享LB模式下，管理面只需由压测网访问，仍不得随公网LoadBalancer暴露。没有共同
绝对窗口的五份独立成功结果只能证明五次20万；只有客户端held而没有五节点服务端
峰值也不能通过。五个节点开跑前的`openiot_connections`还必须全为0，防止生产
存量连接把绝对峰值补足到一百万。PROXY v2核心密度结果不能冒充真实mTLS验收。
`EXPECTED_*`覆盖项只供聚合器自身的小型测试使用。
