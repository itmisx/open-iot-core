# 策略文档、动作与资源

一份策略由若干条语句组成。每条语句声明三项内容：许可或拒绝、适用的动作、以及作用的资源。

## 起步策略

下述策略即证书签发时默认附加的那一份。`${iot:Connection.Thing.ThingName}` 须原样保留——它在判定时按当前连接求值。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "iot:Connect",
      "Resource": "arn:aws:iot:*:*:client/${iot:Connection.Thing.ThingName}"
    },
    {
      "Effect": "Allow",
      "Action": ["iot:Publish", "iot:Receive"],
      "Resource": "arn:aws:iot:*:*:topic/$aws/things/${iot:Connection.Thing.ThingName}/*"
    },
    {
      "Effect": "Allow",
      "Action": "iot:Subscribe",
      "Resource": "arn:aws:iot:*:*:topicfilter/$aws/things/${iot:Connection.Thing.ThingName}/*"
    }
  ]
}
```

其放行范围仅为设备自身的 `$aws/things/<设备名>/` 子树：包括影子、作业，以及自行挂载在该前缀下的业务主题。需要启用其他前缀时，按相同形状追加语句即可。

## 五类动作及其资源类型

| 动作 | 资源类型 | 判定时机 |
| --- | --- | --- |
| `iot:Connect` | `client/` | CONNECT 报文，资源为 clientId |
| `iot:Publish` | `topic/` | 每条 PUBLISH，按具体主题判定 |
| `iot:Subscribe` | `topicfilter/` | SUBSCRIBE，资源为过滤器本身，允许含通配符 |
| `iot:Receive` | `topic/` | 每次投递，按具体主题逐条判定 |
| `iot:RetainPublish` | `topic/` | 发布保留消息，与 `iot:Publish` 分别判定 |

资源类型不可使用通配符：`arn:aws:iot:*:*:*/…` 不会放行任何操作，必须明确写出 `client/`、`topic/` 或 `topicfilter/`。

## Subscribe 与 Receive 分别判定

这是「连接成功但没有数据」最常见的一种成因：

- `iot:Subscribe` 判定的是提交的**过滤器**，位于 `topicfilter/` 之下。
- `iot:Receive` 判定的是实际投递的**具体主题**，位于 `topic/` 之下。

仅放行 Subscribe 而未放行 Receive 时，SUBACK 返回成功，但消息无法送达，设备侧观察到的状态是「订阅成功」。相反情形（仅放行 Receive）下 SUBACK 直接失败，排查难度低得多。

## 两类通配符语义不同

| 使用位置 | 符号 | 是否跨斜杠 |
| --- | --- | --- |
| 策略资源（IAM 风格） | `*`、`?` | 跨斜杠。`topic/a/*` 可匹配 `a/b/c` |
| MQTT 过滤器 | `+`、`#` | `+` 仅匹配一层，`#` 匹配其后全部层级 |

将 MQTT 的 `+` 写入策略资源不会触发错误，但永远无法匹配，而外部现象是「该条策略似乎未生效」。

## Deny 优先，未匹配即拒绝

显式 `Deny` 优先于任何 `Allow`。无任何语句匹配时判为**隐式拒绝**：策略是白名单，未声明即不许可。

收紧权限时不应采用「追加一条 Deny 予以关闭」的思路，而应先定位是哪条 Allow 放开了该操作；否则多份策略叠加后的最终结果将无人能够准确判断。

## clientId 通常须与设备名一致

起步策略中 `iot:Connect` 的资源为 `client/${iot:Connection.Thing.ThingName}`，因此 clientId 必须等于设备名。图形化 MQTT 客户端默认使用随机 clientId，连接将被拒绝；由于该拒绝发生在 CONNECT 阶段，现象类似于证书问题。
