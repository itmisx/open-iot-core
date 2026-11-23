#!/usr/bin/env python3
"""Validate host and data-volume identity evidence for the Shadow gate."""

import json
import pathlib
import sys


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    if len(sys.argv) < 4:
        fail(
            "usage: shadow-host-evidence-verify.py EXPECTED_NODES "
            "EXPECTED_NODE_IDS_CSV EVIDENCE..."
        )

    expected_nodes = int(sys.argv[1])
    expected_ids = sys.argv[2].split(",")
    paths = [pathlib.Path(value) for value in sys.argv[3:]]
    if len(expected_ids) != expected_nodes or len(paths) != expected_nodes:
        fail(
            f"需要{expected_nodes}个node ID和证据文件，实际为"
            f"{len(expected_ids)}和{len(paths)}"
        )

    rows = []
    for path in paths:
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            fail(f"无法读取主机证据{path}: {exc}")
        if not isinstance(row, dict):
            fail(f"主机证据必须是JSON对象: {path}")
        rows.append(row)

    actual_ids = [row.get("nodeId") for row in rows]
    if actual_ids != expected_ids:
        fail(f"主机证据nodeId顺序不匹配: {actual_ids} != {expected_ids}")

    machines = [row.get("machineId", "") for row in rows]
    disks = [row.get("diskId", "") for row in rows]
    if any(not value for value in machines) or len(set(machines)) != expected_nodes:
        fail(f"不是{expected_nodes}个不同machine-id: {machines}")
    if any(not value for value in disks) or len(set(disks)) != expected_nodes:
        fail(f"不是{expected_nodes}个不同磁盘/云卷ID: {disks}")
    if any(not row.get("source") or not row.get("fsType") for row in rows):
        fail("主机证据缺少数据目录挂载源或文件系统类型")

    print(
        json.dumps(
            {
                "passed": True,
                "nodes": expected_nodes,
                "nodeIds": actual_ids,
                "machineIds": machines,
                "diskIds": disks,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
