# SDK 接入

使用 AWS 官方 SDK 直接接入自建节点：仅需替换 endpoint，`boto3`、`aws` CLI 以及 Go 与 Java 的 SDK 均可沿用原有代码。本章自访问凭据的签发开始，依次说明设备注册、证书签发、消息发布与设备影子的读写。
