# 策略变量

策略变量使同一份策略可下发至全部设备，而每台设备只能访问自身的资源。变量在判定时按**当前连接**求值。

## 可用变量

| 变量 | 求值结果 |
| --- | --- |
| `${iot:ClientId}` | 本条连接 CONNECT 报文中的 clientId |
| `${iot:Connection.Thing.ThingName}` | 证书所绑定的设备名 |
| `${iot:Connection.Thing.IsAttached}` | 证书是否已绑定设备 |
| `${iot:Connection.Thing.Attributes[name]}` | 设备的指定属性值 |
| `${iot:Certificate.SerialNumber}` | 客户端证书序列号 |
| `${iot:Certificate.Subject.CommonName}` | 证书主题中的 CN（Issuer 同形） |

一份策略即可覆盖上万台设备：`topic/$aws/things/${iot:Connection.Thing.ThingName}/*` 对每台设备求出不同取值，彼此无法访问对方的主题。

## 变量无法求值时整条语句判为拒绝

该规则是刻意设计，也是本页在安全上最关键的一条：变量无法求值时**不会**退化为空串。

空串会使 `topic/${iot:Connection.Thing.ThingName}/x` 变为 `topic//x`，再配合资源中跨斜杠的 `*`，一条本应仅放行自身主题的语句可能放行全部设备的主题。

因此，证书未绑定设备（未执行 `AttachThingPrincipal`）时，含设备变量的语句**一条都不放行**。其现象为「证书无误、策略已附加，但所有操作均被拒绝」，此时应首先核查证书与设备的绑定关系。

## 基于设备属性细化授权

设备属性可用于策略，按产线、站点或型号划分权限：

```json
{
  "Effect": "Allow",
  "Action": "iot:Publish",
  "Resource": "arn:aws:iot:*:*:topic/sites/${iot:Connection.Thing.Attributes[site]}/*"
}
```

属性变更后，后续判定立即采用新值。这也意味着修改属性等同于一次**权限变更**——其形式不像权限操作，但效果相同。

## 应使用变量而非枚举设备名

将上万台设备的名称写入单条 `Resource` 列表可以工作，但该文档将超出大小限制，且每新增一台设备都需修改策略并重新下发。变量是同一语义的常量规模写法。
