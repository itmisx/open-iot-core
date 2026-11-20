# Go 示例

AWS SDK for Go v2。独立 module：主 module 只有 `bbolt` 与 `yaml/v3` 两个
直接依赖（LANG-019），AWS SDK 不能进去。

```bash
export OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=...
go run .
```

教程对应页：控制台里的 `#/guide/sdk/golang`。`main.go` 里的
`tutorial:begin` / `tutorial:end` 标记就是那一页引用的锚点，别随手删。
