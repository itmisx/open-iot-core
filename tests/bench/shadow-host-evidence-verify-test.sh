#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

work=$(mktemp -d "${TMPDIR:-/tmp}/openiot-shadow-host-evidence-verify-test.XXXXXX")
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM

for i in 0 1 2 3 4; do
  printf '{"nodeId":"n%s","machineId":"machine-%s","diskId":"disk-%s","source":"/dev/disk%s","fsType":"xfs"}\n' \
    "$i" "$i" "$i" "$i" > "$work/$i.json"
done
files=("$work/0.json" "$work/1.json" "$work/2.json" "$work/3.json" "$work/4.json")
python3 tests/bench/shadow-host-evidence-verify.py 5 n0,n1,n2,n3,n4 "${files[@]}" \
  >/dev/null

expect_rejected() {
  name=$1 pattern=$2
  if python3 tests/bench/shadow-host-evidence-verify.py 5 n0,n1,n2,n3,n4 \
    "${files[@]}" > "$work/$name.out" 2>&1; then
    echo "$name证据应被拒绝" >&2
    exit 1
  fi
  grep -q "$pattern" "$work/$name.out"
}

cp "$work/4.json" "$work/4.good"
sed 's/machine-4/machine-3/' "$work/4.good" > "$work/4.json"
expect_rejected duplicate-machine '不同machine-id'
cp "$work/4.good" "$work/4.json"

sed 's/disk-4/disk-3/' "$work/4.good" > "$work/4.json"
expect_rejected duplicate-disk '不同磁盘/云卷ID'
cp "$work/4.good" "$work/4.json"

sed 's/"n4"/"wrong-node"/' "$work/4.good" > "$work/4.json"
expect_rejected wrong-node 'nodeId顺序不匹配'

echo "PASS  shadow host evidence verifier"
