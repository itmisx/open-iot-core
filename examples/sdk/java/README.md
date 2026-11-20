# Java 示例

AWS SDK for Java v2，Maven 工程，需要 JDK 17。

```bash
export OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=...
mvn -q compile exec:java
```

教程对应页：控制台里的 `#/guide/sdk/java`。`Main.java` 与 `pom.xml` 里的
`tutorial:begin` / `tutorial:end` 标记就是那一页引用的锚点，别随手删。

自签 CA：这一份没有按请求装信任库，JVM 的信任库是进程级的——用
`keytool -importcert -cafile` 把节点的 `pki/ca-chain.pem` 导进去，
或者先用 HTTP 的管理面跑通。
