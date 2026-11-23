#!/usr/bin/env bash
# 在每个broker主机本地运行，采集影子生产验收所需的主机/数据盘身份。
set -euo pipefail
if [ "$(uname -s)" != Linux ]; then
  echo "只支持Linux主机" >&2
  exit 2
fi
: "${NODE_ID:?请设置本机openiot node_id}"
: "${DATA_DIR:?请设置本机openiot数据目录}"
[ -d "$DATA_DIR" ] || { echo "DATA_DIR不是目录: $DATA_DIR" >&2; exit 2; }
command -v findmnt >/dev/null || { echo "缺少findmnt" >&2; exit 2; }
command -v lsblk >/dev/null || { echo "缺少lsblk" >&2; exit 2; }

machine_id=$(tr -d '\r\n' < /etc/machine-id)
[ -n "$machine_id" ] || { echo "/etc/machine-id为空" >&2; exit 2; }
data_dir=$(realpath "$DATA_DIR")
source_dev=$(findmnt -n -o SOURCE --target "$data_dir" | head -n 1)
fs_type=$(findmnt -n -o FSTYPE --target "$data_dir" | head -n 1)
[ -n "$source_dev" ] && [ -n "$fs_type" ] || { echo "无法解析数据目录挂载" >&2; exit 2; }

disk_id=${DISK_ID:-}
block_device=""
rotational=""
model=""
size_bytes=""
case "$source_dev" in
  /dev/*)
    parent=$(lsblk -n -o PKNAME "$source_dev" | head -n 1 | tr -d ' ')
    if [ -n "$parent" ]; then block_device="/dev/$parent"; else block_device="$source_dev"; fi
    if [ -z "$disk_id" ]; then disk_id=$(lsblk -dn -o SERIAL "$block_device" | xargs); fi
    rotational=$(lsblk -dn -o ROTA "$block_device" | xargs)
    model=$(lsblk -dn -o MODEL "$block_device" | xargs)
    size_bytes=$(lsblk -bdn -o SIZE "$block_device" | xargs)
    ;;
  *)
    # overlay、网络卷或设备映射通常拿不到可靠serial，必须由云卷/存储系统提供ID。
    ;;
esac
[ -n "$disk_id" ] || {
  echo "无法从块设备取得唯一磁盘ID；请显式设置DISK_ID为云卷/物理盘唯一ID。" >&2
  exit 2
}

python3 - "$NODE_ID" "$machine_id" "$disk_id" "$data_dir" "$source_dev" "$fs_type" \
  "$block_device" "$rotational" "$model" "$size_bytes" "$(uname -srvm)" <<'PY'
import json,sys
keys=("nodeId","machineId","diskId","dataDir","source","fsType","blockDevice",
      "rotational","model","sizeBytes","kernel")
print(json.dumps(dict(zip(keys,sys.argv[1:])),ensure_ascii=False,separators=(",",":")))
PY
