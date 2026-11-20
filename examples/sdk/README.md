# 教程里的 SDK 示例

这三份是教程「SDK 接入」那一章的**来源**，不是另抄的一份：页面上的每段
代码都由 `docs/tutorial/*/01-sdk/*.md` 里的 `include` 锚点从这里取
（UI-080）。改了这里的函数签名，教程页上那段也跟着变；改坏了锚点，门禁会
红（TST-142）。

```
golang/   独立 go module（主 module 只有 bbolt 与 yaml/v3 两个直接依赖）
python/   boto3
java/     Maven + AWS SDK for Java v2
```

三份做同一件事，输出也一样：建设备与策略、签证书并绑好、取接入地址、
发一条消息、写一次影子再读回来、列出设备上挂的策略。

## 跑

先起一个节点并发一把 `operator` 的 access key（控制台「访问控制」页，或
`POST /api/admin/v1/awskeys`），然后：

```bash
export OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws   # 前缀必须带上
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=...

cd golang && go run .
cd python && python3 -m venv venv && venv/bin/pip install boto3 && venv/bin/python main.py
cd java   && mvn -q compile exec:java
```

一键跑全部（会自己起一个临时节点、自己发钥匙）：

```bash
make examples-test
```

它不在 `make gate` 里：门禁只依赖 Go 工具链，而这套要 Python、JDK 与网络。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OIOT_ENDPOINT` | 无，必填 | 兼容面地址，要带 `/api/aws` 前缀 |
| `OIOT_THING` | `lamp-go` / `lamp-py` / `lamp-java` | 设备名 |
| `OIOT_POLICY` | `<设备名>-scoped` | 策略名 |
| `OIOT_OUT` | `./out` | 证书与私钥落盘的目录 |
| `OIOT_CA_FILE` | 空 | 自签 CA 链，走 HTTPS 时要（Java 那份用 JVM 的信任库，见下） |

Java 那份没有读 `OIOT_CA_FILE`：JVM 的信任库是进程级的，用
`keytool -importcert` 把 CA 导进去，或者起节点时用 HTTP 的管理面。
这与 Go、Python 两份按请求装 RootCAs 不同——不是遗漏，是 JVM 的模型不同。
