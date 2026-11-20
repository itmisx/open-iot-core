# Go

AWS SDK for Go v2。前置条件见[通用前置条件](#/guide/sdk/overview)，本页仅说明 Go 特有的部分。

## 依赖

```bash
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
go get github.com/aws/aws-sdk-go-v2/service/iot
go get github.com/aws/aws-sdk-go-v2/service/iotdataplane
```

## 指向自建节点

Go v2 不提供 host 参数，endpoint 经 `BaseEndpoint` 传入。路径前缀 `/api/aws` 包含在该值中，SDK 以 `JoinPath` 将操作路径拼接于其后。

<!-- include: examples/sdk/golang/main.go#client -->

三种写法的取舍：

- 为每个客户端单独设置 `o.BaseEndpoint`——推荐。由同一 `cfg` 构造的其他客户端（例如 S3）不会被一并重定向。
- `config.WithBaseEndpoint(endpoint)`——对**所有**服务生效，仅接入本节点时足够。
- 环境变量 `AWS_ENDPOINT_URL_IOT`（按服务）或 `AWS_ENDPOINT_URL`（全局）——无需修改代码。

`config.WithEndpointResolverWithOptions` 已被 SDK 标记为废弃，不应继续使用。

## 注册设备与创建策略

<!-- include: examples/sdk/golang/main.go#thing -->

判定「资源已存在」须经 `errors.As` 取错误码，不得匹配错误字符串：

<!-- include: examples/sdk/golang/main.go#thing-exists -->

## 签发证书并完成绑定

<!-- include: examples/sdk/golang/main.go#cert -->

三步中遗漏任意一步的现象均为「设备无法连接」，但成因不同：

- 遗漏 `AttachPolicy`：证书有效但未获得任何授权，CONNECT 被拒绝。
- 遗漏 `AttachThingPrincipal`：策略中的 `${iot:Connection.Thing.ThingName}` 无法求值，而变量无法求值时整条语句判为**拒绝**，并非跳过。

## 发布消息

<!-- include: examples/sdk/golang/main.go#publish -->

## 读写设备影子

<!-- include: examples/sdk/golang/main.go#shadow -->

影子的语义另设一章，见[设备影子](#/guide/shadow/basics)。

## 错误码与处置方式

<!-- include: examples/sdk/golang/main.go#errors -->

## 运行示例

```bash
cd examples/sdk/golang
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  go run .
```
