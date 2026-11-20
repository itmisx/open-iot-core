# 控制面 SDK 对照

用 boto3 对一个临时节点跑：创建设备、创建策略、签发证书、附策略、附到设备、MQTT 连接、吊销、再次连接被拒。然后用 `aws iot describe-thing` 读回同一台设备。

不进 `make gate`。门禁只依赖 Go 工具链。

```bash
./tests/aws-sdk/run.sh
```

失败时加 `KEEP_WORK=1` 可以留下临时目录。
