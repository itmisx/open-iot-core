#!/bin/sh
# 容器入口。
#
# 它只做一件事：在**显式要求**时，于 serve 之前补一次
# `init --if-needed`。其余情形原样把参数交给二进制，所以
# `docker run <镜像> cert issue ...` 这类用法不受影响。
#
# 为什么不把这个能力做进 serve：CLU-055 禁止的正是 serve 自动初始化，
# 理由是 data_dir 打错时会静默新建一个空集群，症状是「所有设备认证
# 失败」而真因完全看不出来。那条理由没有变，所以 serve 的脾气也不变
# ——自动初始化只存在于容器入口这一层，而且要显式打开。
#
# 为什么还是提供它：单节点的快速开始本来要两条命令，而第一条除了
# "先建一次 CA" 没有别的决定可做（`pki.server_hosts` 是唯一的决定，
# 它在配置文件里）。把它收成一条命令，省掉的是初次试用时最容易卡住
# 的一步。
#
# 打开它的代价，务必知道：**卷挂错路径不再报错**，而是静默初始化出
# 一个空节点，于是原有设备全部认证失败——也就是 CLU-055 要防的那个
# 症状。所以它默认关闭，三份带编排的清单（两份 compose 与 k8s）一律
# 不设它：那几份清单里 init 是一个看得见的步骤，更该保持看得见。
set -e

# Go 不会自动把容器 memory limit 当成 runtime soft memory limit。没有
# GOMEMLIMIT 时，大连接堆在 cgroup OOM 之前仍可能保留过多 span/栈/缓存页；
# OOM killer 不给进程留下降载或写日志的机会。用户显式设置的值（包括 off）
# 始终优先；否则取 cgroup 与物理内存较小者的 75%，给内核 socket、mmap、
# bbolt/LogDB page cache 和非 Go 内存留四分之一。
meminfo_path=${OPENIOTD_MEMINFO_PATH:-/proc/meminfo}
cgroup_v2_path=${OPENIOTD_CGROUP_V2_PATH:-/sys/fs/cgroup/memory.max}
cgroup_v1_path=${OPENIOTD_CGROUP_V1_PATH:-/sys/fs/cgroup/memory/memory.limit_in_bytes}
if [ -z "${GOMEMLIMIT:-}" ] && [ -r "$meminfo_path" ]; then
  host_kib=0
  while read -r key value _; do
    if [ "$key" = "MemTotal:" ]; then host_kib=$value; break; fi
  done < "$meminfo_path"
  host_bytes=$((host_kib * 1024))
  effective_bytes=$host_bytes
  cgroup_bytes=""
  if [ -r "$cgroup_v2_path" ]; then
    IFS= read -r cgroup_bytes < "$cgroup_v2_path" || cgroup_bytes=""
  elif [ -r "$cgroup_v1_path" ]; then
    IFS= read -r cgroup_bytes < "$cgroup_v1_path" || cgroup_bytes=""
  fi
  case "$cgroup_bytes" in
    ''|max|*[!0-9]*) ;;
    *)
      if [ "$cgroup_bytes" -gt 0 ] && [ "$cgroup_bytes" -lt "$effective_bytes" ]; then
        effective_bytes=$cgroup_bytes
      fi
      ;;
  esac
  if [ "$effective_bytes" -ge 67108864 ]; then
    go_memory_limit=$((effective_bytes / 4 * 3))
    GOMEMLIMIT="${go_memory_limit}B"
    export GOMEMLIMIT
    echo "openiotd: GOMEMLIMIT 未设置，按可用内存的 75% 自动设为 $GOMEMLIMIT" >&2
  fi
fi

# 只认 "1" 与 "true"。
#
# 不用「非空即为真」那种写法：`OPENIOTD_AUTO_INIT=0` 与
# `OPENIOTD_AUTO_INIT=false` 在那种写法下都是打开的，而写下这两个值
# 的人意思恰好相反。
auto=no
case "${OPENIOTD_AUTO_INIT:-}" in
  1 | true) auto=yes ;;
esac

# 仅限 serve 子命令。
#
# 不加这个判断，`docker run <镜像> config print` 也会先建一遍数据目录
# ——一条只读命令不该有副作用。显式的 `init` 子命令同样走下面的透传，
# 不会被执行两遍。
if [ "$auto" = yes ] && [ "$1" = "serve" ]; then
  shift
  # 把 serve 的参数原样交给 init。
  #
  # 两个子命令共用同一套配置 flag（都是 config.NewFlags），所以
  # `--config` / `--data-dir` 这些不必在这里解析，更不必在这里重复一份
  # 默认值——重复一份就会与 Go 侧各自漂移。
  #
  # --if-needed 是按步骤跳过而不是按身份文件跳过（CLU-104），因此一个
  # 只建到一半的目录（上次在签服务端证书那步中断那种）会被补齐。
  openiotd init --if-needed "$@"
  # init 第一次成功时会打一屏「下一步」，其中第 2 步是「启动节点」。
  # 这里紧接着就要启动，所以加一行把那段读法纠正过来——否则首次启动
  # 的日志末尾看起来像是在等人手工再跑一次 serve。
  echo "（OPENIOTD_AUTO_INIT=1：以上为自动初始化的输出，接着启动节点）"
  set -- serve "$@"
fi

# 按 PATH 找，不写绝对路径：镜像里 /usr/local/bin 在 PATH 上，而这样
# 入口脚本就不必知道二进制装在哪，测试里也能用桩替换它。
exec openiotd "$@"
