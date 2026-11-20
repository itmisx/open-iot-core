# Java

AWS SDK for Java v2。前置条件见[通用前置条件](#/guide/sdk/overview)，本页仅说明 Java 特有的部分。

## 依赖

控制面与数据面是两个 artifact，须同时引入。版本由 BOM 统一管理。

<!-- include: examples/sdk/java/pom.xml#deps -->

## 指向自建节点

Java 侧的入口是 `endpointOverride`，参数类型为 `URI`。前缀 `/api/aws` 写入该 URI 的路径部分，SDK 将操作路径拼接于其后。

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#client -->

两个客户端须分别构造，而不是在同一客户端上切换方法：签名服务名由客户端类型决定（`iot` 与 `iotdata`），不是可传入的参数。

## 注册设备与创建策略

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#thing -->

`createThing` 的 lambda 形式（`b -> b.thingName(thing)`）是 v2 的惯用写法，可省去一行 `XxxRequest.builder()...build()`。

## 签发证书并完成绑定

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#cert -->

三步中遗漏任意一步的现象均为「设备无法连接」，但成因不同：

- 遗漏 `attachPolicy`：证书有效但未获得任何授权，CONNECT 被拒绝。
- 遗漏 `attachThingPrincipal`：策略中的 `${iot:Connection.Thing.ThingName}` 无法求值，而变量无法求值时整条语句判为**拒绝**，并非跳过。

## 发布消息

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#publish -->

## 读写设备影子

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#shadow -->

影子的语义另设一章，见[设备影子](#/guide/shadow/basics)。

## 错误码与处置方式

<!-- include: examples/sdk/java/src/main/java/io/openiotcore/example/Main.java#errors -->

## 运行示例

```bash
cd examples/sdk/java
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  mvn -q compile exec:java
```
