# -*- coding: utf-8 -*-
"""_coverage_scan2.py — 数据完整性体检 (决定哪些数据集可以接进字段层/MCP)

用户需求: **有完整数据的接上暴露给外部, 没有完整数据的不暴露**。
所以先量: 每个数据集的 行数 / 覆盖标的数 / 时间范围 / 有无 data_available_at /
PIT 滞后是否干净。据此分三类: 可接 / 不完整 / 硬屏蔽。
"""
from __future__ import annotations

import os
import sys

import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "data", "l2", "certified")


def scan(ds: str) -> dict:
    root = os.path.join(ROOT, ds)
    tot, nfiles, cols, ent = 0, 0, [], set()
    tcol = None
    for dirpath, _, files in os.walk(root):
        for fn in sorted(files):
            if not fn.endswith(".parquet"):
                continue
            nfiles += 1
            path = os.path.join(dirpath, fn)
            try:
                md = pq.ParquetFile(path)
                tot += md.metadata.num_rows
                if not cols:
                    cols = md.schema_arrow.names
                    tcol = next((c for c in cols
                                 if c.endswith("_utc") or c in ("date", "time")), None)
                # 目录名当实体名 (venue/instrument/token/asset)
                rel = os.path.relpath(dirpath, root)
                ent.add(rel.split(os.sep)[-1] if rel != "." else "all")
            except Exception:
                pass
    return {"ds": ds, "rows": tot, "files": nfiles, "cols": cols,
            "tcol": tcol, "entities": len(ent)}


print(f"{'数据集':32s} {'行数':>13s} {'文件':>5s} {'实体':>5s} {'avail':>6s} "
      f"{'时间列':22s}")
print("-" * 100)
info = {}
for ds in sorted(os.listdir(ROOT)):
    if not os.path.isdir(os.path.join(ROOT, ds)):
        continue
    r = scan(ds)
    info[ds] = r
    has_av = "YES" if "data_available_at" in r["cols"] else "--"
    print(f"{ds:32s} {r['rows']:13,d} {r['files']:5d} {r['entities']:5d} "
          f"{has_av:>6s} {str(r['tcol']):22s}")
