# 控制台浏览器端到端检查

在真实 Chromium 里把控制台点一遍。

```bash
bash tests/console-e2e/run.sh              # 跑一遍，跑完拆掉
KEEP_WORK=1 bash tests/console-e2e/run.sh  # 保留工作目录与截图
MQTT_PORT=49283 ADMIN_PORT=49290 bash tests/console-e2e/run.sh
```

脚本自己建临时数据目录、签证书、起节点、预置 60 台设备，跑完删掉。
当前 37 项全绿（**改过 `web/console/src/` 或 `docs/tutorial/` 之后要先跑
`make console`**，否则它点的还是旧产物）。
`make console-e2e` 是同一条命令。

## 为什么它不在 `make gate` 里

门禁必须只依赖 Go 工具链（与 `tests/device-sdk`、`tests/aws-sdk` 同一条
理由）。这套要 Node 和一个一百多兆的 Chromium。定位是**改过
`web/console/src/` 之后**与发版前手动跑，以及 CI 里单独的可选 job。

## 它验什么，不验什么

Go 侧的 `TestConsoleCriticalFlows`（TST-126）走的是控制台调用的那几个
管理面接口。它证明不了页面能渲染出来——这套补的正是这一段：

- 窗口渲染（UI-059）：服务端一页 50 条，DOM 里只能有二十几行数据行，
  其余高度由 `aria-hidden` 的撑高行顶起
- 在线率曲线与空状态（UI-060、UI-030）
- 事件流的 SSE 真的连上并分发
- MQTT 测试客户端的 WebSocket 往返：订阅后能收到自己发布的那条
- 中英切换后 `documentElement.lang` 与导航文案都变，且没有中文残留
- 使用教程（UI-076 至 UI-082）：标题栏里的入口（点亮状态随路由变）、
  `#/guide/<章>/<页>` 深链接、
  `include` 注入的代码块、复制按钮、中英两棵树各渲染一遍、使用说明抽屉
  链到教程对应页。这一页是纯渲染的——markdown 的八种块级节点里任何一种
  写错，表现是白屏或少一段，而两者在 Go 测试里完全不可见
- **整轮运行里浏览器一条错误都不许有**。React 的渲染错误与未捕获的
  Promise 拒绝在 Go 测试里完全不可见，而它们会让页面白屏

接口的响应形状不在这里验（那是 Go 侧的事），避免同一件事两处维护。

## 两个坑

1. **locale 必须固定。** `detectLang` 读 `navigator.language`，Chromium
   默认 `en-US`。第一版没固定，于是所有中文断言都在英文界面上找不存在的
   文案——18 条里挂了 12 条，而产品完全正常。脚本现在固定 `zh-CN`，
   英文那一半用侧栏的切换按钮验。

2. **预置策略要落成文件再签证书。** `cert issue` 是离线命令，读 data dir
   下的 `policies/*.json`；只经 Admin API 写进 KV 的话，签发会说策略不存在。

另外：策略里的资源类型不接受通配（`arn:aws:iot:*:*:*/devices/...` 会被
判成拒绝），这是 AUTH-023 的保证，不是缺陷。要放行发布就写
`topic/`，订阅写 `topicfilter/`。
