# open-iot-core 容器镜像。
#
# 两阶段：用官方 Go 镜像静态编译，再把单个二进制放进 alpine。
#
# 构建阶段不需要 Node：控制台产物是提交进仓库的（UI-039），
# go:embed 直接把它打进二进制。镜像里因此也没有静态文件要挂。
#
# 运行镜像选 alpine 而不是 distroless/scratch，是为了留一个 shell。
# 日常运维要在容器里跑 `openiotd admin token`、`openiotd cert issue`、
# 翻 data dir；集群初始化还要把设备 CA 从第一个节点复制到其余节点。
# 没有 shell 的镜像省下约 6 MB，代价是这些事都得另起一个容器挂同一个卷。
# 想要更小的镜像，把最后一个 FROM 换成 gcr.io/distroless/static:nonroot
# 即可——二进制是静态的，不依赖 libc。

FROM golang:1.24-alpine AS build

WORKDIR /src

# 先只拷依赖清单：go.mod/go.sum 不变时这一层命中缓存，
# 改一行业务代码不必重新下载依赖。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 得到纯静态二进制（三个直接依赖 bbolt、yaml/v3 与
# dragonboat 都是纯 Go——它的默认日志后端 Pebble 也是，LANG-014
# 第二次修订）。-trimpath 让构建路径不进
# 二进制，便于复现构建。
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/openiotd ./cmd/openiotd

FROM alpine:3.21

# ca-certificates 是给 OTLP 导出用的（OBS-140）：节点自己只在开启
# otel 时才发出站 HTTPS。其余功能不联网。
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -g 10001 -S openiot \
 && adduser -u 10001 -S -G openiot -h /var/lib/openiotd openiot \
 && mkdir -p /var/lib/openiotd \
 && chown -R openiot:openiot /var/lib/openiotd

COPY --from=build /out/openiotd /usr/local/bin/openiotd
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
# 显式 chmod 而不是依赖 git 里的权限位：源文件的 exec 位一旦丢掉，
# 症状是容器起不来（permission denied），而这一层代价约等于零。
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

# 数据目录必须是卷：里面有 CA 私钥、raft 日志、元数据与审计日志。
# 它不在卷上就意味着容器一删，签过的证书全部作废。
VOLUME ["/var/lib/openiotd"]
ENV OPENIOTD_DATA_DIR=/var/lib/openiotd

# 8883 设备 mTLS，9090 管理面与控制台，7946 集群复制（仅集群模式）
EXPOSE 8883 9090 7946

USER 10001:10001

# 默认 CMD 是 serve，但第一次仍必须跑一次 init：serve 遇到未初始化的
# 数据目录会明确报错（CLU-055），不会自动建集群。自动初始化会让一个
# 打错的 data_dir 静默建出一个空集群，症状是"所有设备认证失败"而真因
# 完全看不出来。
#
# 入口脚本给这一步留了一个**默认关闭**的开关：`OPENIOTD_AUTO_INIT=1`
# 时，serve 之前补一次 `init --if-needed`，于是单节点的快速开始收成
# 一条命令（NFR-105）。它是镜像这一层的变量，不是配置键——`openiotd
# config env` 列不出它。打开它就等于接受上面那个症状，所以带编排的
# 清单一律不设它；理由与边界见入口脚本顶部与 README。
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["serve"]
