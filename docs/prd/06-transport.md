# 06 传输与端点

## 1. 端点矩阵

官方 SDK 能否不改一行跑通，主要由本节决定。ALPN 与 SNI 须精确对齐。

| ID | 端点 | 认证 | ALPN | 状态 |
| --- | --- | --- | --- | --- |
| NET-001 | 8883 MQTT over TLS | X.509 客户端证书 | — | v0.1 |
| NET-002 | 443 MQTT over TLS | X.509 客户端证书 | `x-amzn-mqtt-ca` | v0.1 |
| NET-003 | 443 MQTT over WebSocket Secure，路径 `/mqtt` | SigV4 | — | v0.1 |
| NET-004 | 443 MQTT over TLS | 自定义认证 | `mqtt` | v0.3 |
| NET-005 | 443 MQTT over WebSocket Secure | 自定义认证 | — | v0.3 |
| NET-006 | 8443 HTTPS 发布，路径 `/topics/{topic}` | X.509 客户端证书 | — | v0.2 |
| NET-007 | 443 HTTPS 发布 | SigV4 或自定义认证 | `x-amzn-http-ca` 时为 X.509 | v0.2 |

NET-002 为 SDK 在受限网络下的默认路径，缺失即视为不兼容。

## 2. TLS

| ID | 需求 | 状态 |
| --- | --- | --- |
| NET-010 | 支持 TLS 1.2 与 1.3。默认最低 1.2，可配置为仅 1.3。 | v0.1 |
| NET-011 | 默认证书与密钥套件优先 ECDSA P-256。握手是 CPU 墙，不得默认 RSA-2048。 | v0.1 |
| NET-012 | 默认启用会话票据（TLS 1.2）与 PSK 恢复（TLS 1.3）。 | v0.1 |
| NET-013 | 缺少 SNI 的连接默认拒绝，与 AWS 一致。提供宽松开关，自建环境常无 SNI。 | v0.1 |
| NET-014 | SNI 作为多租户与自定义域名的分流依据。 | v0.3 |
| NET-015 | 客户端证书链校验须支持多个受信 CA。 | v0.2 |
| NET-016 | 证书状态校验查本地热视图，不得发起网络请求。符合 ARCH-002。 | v0.1 |
| NET-017 | 支持 TLS 终结外移部署：`mqtt_plain.proxy_protocol: v2` 后仅接受原始 TCP peer 命中 `trusted_proxies` 的 PROXY v2 连接，不得在缺 header 时回退。私有 TLV `0xE0` 携带版本、TLS 版本、ALPN 与叶证书 DER，broker 自行计算 SHA-256；紧凑 `0xE1` 可携带 32 B 原始或 64 位十六进制 SHA-256，便于 HAProxy 一类终结器直接导出摘要。两者均在每次 CONNECT 按最新热视图复核登记、吊销、Thing 和策略；仅 E1 时证书 DN/序列号变量为空并 fail closed。标准 SSL verified/CN TLV 不足以构成身份。该端点须位于设备不可直达的私网/loopback/Unix socket 边界；CIDR allowlist 不是公网链路加密。 | v0.2 |
| NET-018 | 设备 TLS 监听器默认使用 X25519/P-256 的容量优先密钥交换；须可按端点显式启用 Go 默认的 X25519+ML-KEM 混合组。两档都不得改变证书链校验、ticket 恢复后的注册表/吊销/策略复核或最低 TLS 版本。机制基准须同时公布 CPU 与分配差异，最终仍以真实设备兼容性与 Linux 重连风暴压测验收。 | v0.3 |

NET-017 对应 LANG-009，是提升单节点连接密度的主要手段。

## 3. WebSocket

| ID | 需求 | 状态 |
| --- | --- | --- |
| NET-020 | 子协议为 `mqtt`。握手须校验 `Sec-WebSocket-Protocol`。 | v0.1 |
| NET-021 | SigV4 签名经 query string 传递，须校验签名、时间戳偏移、区域与服务名。 | v0.1 |
| NET-022 | 单帧与分片帧均须支持。MQTT 报文可跨 WebSocket 帧边界。 | v0.1 |
| NET-023 | WSS 与 SigV4 连接最长时长默认 24 小时，可配。到期前使用 server redirect 优雅迁移而非硬断。 | v0.1 |

## 4. 连接管理

| ID | 需求 | 状态 |
| --- | --- | --- |
| NET-030 | 支持 IPv4 与 IPv6。 | v0.1 |
| NET-031 | 证书与自定义认证的连接无硬性时长上限。 | v0.1 |
| NET-032 | 每连接最多一个读 goroutine，写出走共享 writer 池。符合 LANG-005。 | v0.1 |
| NET-033 | 单节点容量按 10–50 万连接规划。容量基准须公开可复现。 | v0.1 |

## 5. 接入限速

大规模断网恢复时的重连风暴是此类系统最常见的雪崩原因，不得依赖客户端自觉。

| ID | 需求 | 状态 |
| --- | --- | --- |
| NET-040 | 每 clientId 连接速率固定 1/秒，与 AWS 一致。 | v0.1 |
| NET-041 | 全局连接速率默认 3000/秒，走 `AllowGlobal`，可配。 | v0.1 |
| NET-042 | TLS 握手须在进入密码学前独立执行 `handshake_rate` 限速，防止握手 CPU 耗尽导致已有连接饥饿。被限流的 TCP 连接立即关闭，不得先完成 TLS 再判定；否则不发 MQTT CONNECT 的对端可绕过该配额。限速与 MQTT-131 的并发 admission 同时生效：前者约束每秒总量，后者约束瞬时工作集。 | v0.1 |
| NET-043 | 服务端在超出接入速率时须返回带退避提示的拒绝，而非静默丢弃连接。MQTT 5 使用对应 reason code。 | v0.1 |
| NET-044 | 握手队列须有界。队列满时立即拒绝而非排队等待超时。 | v0.1 |
| NET-045 | 腾空节点时对 MQTT 3 客户端的断开须限速进行，避免一次性断开引发反向风暴。 | v0.4 |

## 6. 不做

| ID | 项 | 理由 |
| --- | --- | --- |
| NET-050 | VPC 端点 | AWS 网络设施概念，自建场景无对应物。 |
| NET-051 | 为设备侧提供 QUIC 接入 | 见 MQTT-100。 |

## 不认证模式的两道确认（实现期补充）

| ID | 需求 | 状态 |
| --- | --- | --- |
| NET-100 | `auth.mode: insecure-none` 须 `auth.acknowledge_insecure: true` 显式确认才生效。 | v0.1 |
| NET-101 | 该模式下 MQTT 端点绑定在非回环地址时须拒绝启动，并在错误中指出具体地址。 | v0.1 |
| NET-102 | WebSocket 连接时长从握手成功起算，默认 24 小时，用注入时钟。配置了 `redirect_to` 时，到期对 MQTT 5 发送 DISCONNECT `0x9C` 并带上 Server Reference。没有目标时只关闭连接：`0x9C` 表示改连另一台服务器，没有地址可填就不发这个原因码。它与 443 上的 ALPN 客户端证书监听器分开，不能绑在同一个套接字上。 | v0.1 |

两道闸对应两种不同量级的风险，因此不合并成一个开关：一是"根本不认证"，
二是"不认证且监听在任何人都能访问的地址上"。后者是把一个可任意收发的 broker
放到网络上。空主机名与 `0.0.0.0`、`::` 都视为对外暴露；无法在校验阶段解析的
主机名同样保守地视为对外暴露。
