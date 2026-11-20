# 通用前置条件

三种语言的接入代码仅相差数行，但以下六项约定对三种语言完全一致，也是实际接入过程中的主要失败原因。它们在本页集中说明，语言专属的三页不再重复。

## endpoint 须包含 `/api/aws` 前缀

兼容面挂载于**管理端口**的 `/api/aws` 路径下，并非独立端口。因此 endpoint 的形式为 `http://127.0.0.1:9090/api/aws`，该前缀是 endpoint 的组成部分。

| 面 | 职责 | 签名服务名 |
| --- | --- | --- |
| 控制面 | 设备的注册与删除、证书的签发与吊销、策略维护 | `iot` |
| 数据面 | 消息发布、影子读写 | `iotdata` |
| 设备 WebSocket | 设备以 SigV4 自行连接 `/mqtt` | `iotdevicegateway` |

控制面与数据面共用同一 `/api/aws` 前缀，但**签名服务名不同**，因此在 SDK 中对应两个独立客户端。若将数据面调用按 `iot` 签名，服务端返回的拒绝与签名错误完全一致，该响应不会指出服务名有误。

遗漏前缀时的现象不指向真因：请求落到控制台首页并返回一页 HTML，SDK 报告响应不是 JSON——Go SDK 的原始信息为 `invalid character '<'`。出现尖括号即表示前缀缺失。

## 签发访问凭据

访问凭据不来自配置文件，而由节点签发（保存在复制的元数据中，集群内各节点均予承认）。两种签发途径：

- 控制台「访问控制」页下方的卡片，仅 `admin` 可见。secret 仅在创建成功的对话框中显示一次。
- 管理面接口，以引导令牌调用：

```bash
curl -s -X POST http://127.0.0.1:9090/api/admin/v1/awskeys \
  -H "Authorization: Bearer $(cat data/admin-token)" \
  -H 'Content-Type: application/json' \
  -d '{"role":"operator","note":"ci","expires":"2027-01-01T00:00:00Z"}'
```

响应中的 `id` 与 `secret` 分别对应 `AWS_ACCESS_KEY_ID` 与 `AWS_SECRET_ACCESS_KEY`。secret 无法二次获取：它在服务端以加密形式保存，仅在验签时解密。

> 控制台用户与 access key 是两类互不相通的主体。控制台用户持管理面令牌，作用域为控制台与 `/api/admin/v1`；access key 供 SDK 使用，作用域为 `/api/aws` 可调用的 action 集合。两侧的角色同名同义，但凭据不可互换：以管理面令牌调用 `/api/aws` 返回 401，以 access key 调用管理面同样返回 401。

## 角色决定可调用的 action 集合

| 角色 | 权限范围 |
| --- | --- |
| `reader` | 只读：列举设备、读取策略、读取影子、检索 |
| `operator` | 常规写入：注册与删除设备、签发与吊销证书、维护策略、发布消息、写入影子 |
| `admin` | 另含信任根操作：注册与删除自有 CA、签发凭证 |

所需级别标注在每条路由的挂载处，**不按 HTTP 方法推断**：`SearchIndex` 为 POST 而语义是读取，`GET /role-aliases/{n}/credentials` 为 GET 而语义是签发凭证。

## 401 与 403 是两类不同的失败

| 状态码 | 错误码 | 处置方式 |
| --- | --- | --- |
| 401 | `UnauthorizedException` | 密钥有误、已停用、已过期，或本机时钟偏差过大（SigV4 带时间戳） |
| 403 | `AccessDeniedException` | 签名有效但角色级别不足，需改用权限更高的凭据 |

密钥不存在与签名错误被刻意合并为同一个 401：否则该接口将成为探测「某个 id 是否存在」的手段。停用与过期出于同一理由并入这一响应。

## region 须在允许列表内

节点配置项 `aws_api.region` 是**允许列表**而非默认值。SDK 侧的 region 不在列表内时请求被拒绝，不会被改写为配置中的区域。在 CI 中将 region 与 endpoint 一并声明，可消除「更换机器后出现 401」这一类问题。

## 自签 CA 须显式信任

启用 HTTPS 时，节点的服务端证书由自有 CA 签发。CA 链位于数据目录的 `pki/ca-chain.pem`，三种 SDK 均提供传入该链的入口，详见各语言页。

## 后续章节

- [Java](#/guide/sdk/java)
- [Go](#/guide/sdk/golang)
- [Python](#/guide/sdk/python)

上述三页的代码均取自仓库中真实可编译的示例工程（`examples/sdk/`），而非另行抄录的副本。
