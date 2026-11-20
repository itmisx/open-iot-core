# 内存基线（TST-127、CLU-021）

这三个数字是门禁比对的对象：实测高于记录的 110%（或低于一半）即失败。
耗时**不在**这里——它随机器、负载与竞态检测器变化，写进基线就是发布
无法复现的数字（TST-127 的刻意决定）。

| 键 | 含义 | 记录值 | 来源用例 |
| --- | --- | --- | --- |
| `hotViewBytesPerDevice` | 百万设备热视图里每台设备占的字节 | 188 | `internal/meta` 的 `TestViewMemoryBudget` |
| `trieBytesPerSubscription` | 百万订阅的订阅树里每条订阅占的字节 | 185 | `internal/router` 的 `TestTrieBytesPerSubscriptionBaseline` |
| `connBytes` | 一条连接在堆上的字节 | 13096 | `internal/broker` 的 `TestConnMemoryBudget` |

**记录于 2026-10-31**，darwin/arm64、go1.24.4。这三个数都是对应用例在本机
实测打印出来的值——此前仓库里没有这份文件，于是那三条用例一直以
「读不到基线」失败，而失败信息里正好印着该记的数。

改大这些数字要说明结构上为什么变大。低于一半同样失败：那通常是对象被
回收了、测量没有量到该量的结构，而不是优化成功。
