# Python

boto3。前置条件见[通用前置条件](#/guide/sdk/overview)，本页仅说明 Python 特有的部分。

## 依赖

```bash
python3 -m venv venv
venv/bin/pip install boto3
```

## 指向自建节点

boto3 的入口是 `endpoint_url`，前缀 `/api/aws` 包含在该值中。控制面的服务名为 `iot`，数据面为 `iot-data`（botocore 的服务名，其签名服务名为 `iotdata`）。

<!-- include: examples/sdk/python/main.py#client -->

自签 CA 经 `verify` 传入 CA 链路径。设置 `verify=False` 同样可以连通，但这等于关闭证书校验——中间人替换证书后亦可正常连接，而外部现象与一切正常时完全相同。

`aws` CLI 的对应参数是 `--endpoint-url`：

```bash
aws iot describe-thing --thing-name lamp-01 \
  --endpoint-url http://127.0.0.1:9090/api/aws \
  --region us-east-1
```

## 注册设备与创建策略

<!-- include: examples/sdk/python/main.py#thing -->

判定「资源已存在」须读取 `e.response["Error"]["Code"]`，不得匹配异常字符串：该字符串中包含请求 ID。

## 签发证书并完成绑定

<!-- include: examples/sdk/python/main.py#cert -->

三步中遗漏任意一步的现象均为「设备无法连接」，但成因不同：

- 遗漏 `attach_policy`：证书有效但未获得任何授权，CONNECT 被拒绝。
- 遗漏 `attach_thing_principal`：策略中的 `${iot:Connection.Thing.ThingName}` 无法求值，而变量无法求值时整条语句判为**拒绝**，并非跳过。

## 发布消息

<!-- include: examples/sdk/python/main.py#publish -->

## 读写设备影子

<!-- include: examples/sdk/python/main.py#shadow -->

须注意 `get_thing_shadow` 返回的 `payload` 是**流**，只能 `.read()` 一次，读取后即为空。如需多次输出，应先将内容保存。

影子的语义另设一章，见[设备影子](#/guide/shadow/basics)。

## 错误码与处置方式

<!-- include: examples/sdk/python/main.py#errors -->

## 运行示例

```bash
cd examples/sdk/python
OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... \
  venv/bin/python main.py
```
