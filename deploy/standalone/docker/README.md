# 单节点 · 裸 docker 命令

共通的事（`server_hosts`、备份、升级、签设备证书）在
[`../README.md`](../README.md)。这里只有命令。

多数情况下 [`../compose/`](../compose/) 更省事——它是同一个部署，
只是把下面这些参数记在文件里。

## 起步

`OPENIOTD_AUTO_INIT=1` 让容器在 `serve` 之前自己补一次
`init --if-needed`（NFR-105），所以只有这一条命令：

```bash
cd deploy/standalone

docker run -d --name openiotd --restart unless-stopped \
  -e OPENIOTD_AUTO_INIT=1 \
  -v openiotd-data:/var/lib/openiotd \
  -v "$PWD/openiotd.yaml:/etc/openiotd/openiotd.yaml:ro" \
  -p 127.0.0.1:8883:8883 \
  -p 127.0.0.1:9090:9090 \
  ghcr.io/itmisx/open-iot-core:latest \
  serve --config /etc/openiotd/openiotd.yaml

curl -sf http://127.0.0.1:9090/readyz && echo ok
docker logs openiotd | head -30      # 首次启动的 init 输出在这里，含管理面令牌
```

它从 `deploy/standalone/` 跑，因为 `$PWD/openiotd.yaml` 指的是上一层那份
共用配置（两种单机方式共用）。反复执行也没事：`--if-needed` 是**按步骤**
跳过，已有的 CA 不会被重建（重建等于换掉整条信任链），而一个只建到一半的
目录会被补齐。

控制台在 <http://127.0.0.1:9090/>，令牌：

```bash
docker exec openiotd openiotd admin token
```

## 生产上去掉那个变量

把 `-e OPENIOTD_AUTO_INIT=1` 删掉，改为先手工跑一次
`… "$IMAGE" init --config /etc/openiotd/openiotd.yaml --if-needed`
（挂载与上面那条相同，换掉最后的子命令），再起 `serve`。

多这一步换到的是：`-v` 的路径打错时，第一条命令就报「目录未初始化」并停住。
带着那个变量的容器不会报错，而是当场初始化出一个空节点——于是原有设备全部
认证失败，真因完全看不出来（CLU-055）。裸命令这条路径上尤其值得在意，因为
挂载是现敲的，不像清单里写死在文件中。
