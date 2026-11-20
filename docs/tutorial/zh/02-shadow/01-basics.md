# 影子主题与一次完整交互

影子不是一个接口，而是一组**主题**。设备侧全部经 MQTT 收发，云侧既可使用 MQTT，也可调用数据面的 HTTP 接口。

## 单台设备的主题清单

将 `lamp-01` 替换为实际设备名。经典影子的主题前缀为 `$aws/things/<设备名>/shadow`，命名影子在其后增加 `name/<影子名>` 一段。

| 用途 | 主题 |
| --- | --- |
| 请求当前文档 | `$aws/things/lamp-01/shadow/get` |
| 请求成功 | `$aws/things/lamp-01/shadow/get/accepted` |
| 请求失败 | `$aws/things/lamp-01/shadow/get/rejected` |
| 提交更新 | `$aws/things/lamp-01/shadow/update` |
| 更新成功 | `$aws/things/lamp-01/shadow/update/accepted` |
| 更新失败 | `$aws/things/lamp-01/shadow/update/rejected` |
| 接收差集 | `$aws/things/lamp-01/shadow/update/delta` |
| 更新前后的完整文档 | `$aws/things/lamp-01/shadow/update/documents` |
| 删除整份文档 | `$aws/things/lamp-01/shadow/delete` |

请求类主题（`get`、`update`、`delete`）由**请求方**发布；以 `accepted`、`rejected`、`delta`、`documents` 结尾的主题由节点发布，须预先订阅才能接收。

## 设备侧的最小交互序列

设备启动阶段通常执行三步：订阅应答主题、请求一次全量文档、随后等待 delta。

```text
SUBSCRIBE  $aws/things/lamp-01/shadow/get/accepted
SUBSCRIBE  $aws/things/lamp-01/shadow/update/delta
PUBLISH    $aws/things/lamp-01/shadow/get          {}
```

`get` 的负载可为空对象 `{}`。收到 `get/accepted` 后，设备按 `state.desired` 调整自身，并将实际生效的取值上报：

```json
{
  "state": {
    "reported": { "interval": 30, "mode": "auto" }
  }
}
```

上报环节不可省略。若设备仅调整自身而不上报，云端的 `delta` 将持续存在——delta 的定义是 `desired` 与 `reported` 的差集，而 `reported` 仍为旧值。

## 订阅成功但无法接收：优先核查策略

这是「连接正常但没有数据」最常见的一种形态，且其成因不在影子：`iot:Subscribe` 针对 `topicfilter/` 判定，`iot:Receive` 针对 `topic/` 判定。仅放行订阅而未放行接收时，SUBACK 返回成功，但消息无法送达。详见[访问策略](#/guide/policy/basics)。

## 命名影子

单台设备可持有多份影子，各自独立计算版本号，便于将固件、配置与运行态分开管理，互不影响：

| 用途 | 主题前缀 |
| --- | --- |
| 经典影子 | `$aws/things/lamp-01/shadow` |
| 命名影子 `firmware` | `$aws/things/lamp-01/shadow/name/firmware` |
| 列举全部命名影子 | `$aws/things/lamp-01/shadow/list` |

控制台的影子页是命名影子的唯一入口：设备详情页上的影子区块仅读取经典影子，使用命名影子的设备在该处无法完整查看。

## 控制台中的入口

设备列表的操作列可直达 `#/shadow/<设备名>`。该页并列展示 `desired`、`reported` 与 `delta` 三栏，支持就地下发 desired；右侧「使用说明」抽屉中的主题串按当前设备与当前影子渲染，照其复制可避免拼写错误。
