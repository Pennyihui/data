"""扫描 data/ 下所有 parquet, 找出损坏/被截断的文件 (magic bytes 检查 + footer 读取)。"""
import os
import sys

import pyarrow.parquet as pq

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

bad = []
n = 0
for dirpath, _dirnames, filenames in os.walk(ROOT):
    for fn in filenames:
        if not fn.endswith(".parquet"):
            continue
        p = os.path.join(dirpath, fn)
        n += 1
        try:
            pf = pq.ParquetFile(p)
            _ = pf.metadata.num_rows
        except Exception as e:  # noqa: BLE001
            bad.append((p, os.path.getsize(p), f"{type(e).__name__}: {str(e)[:120]}"))

print(f"scanned={n} bad={len(bad)}")
for p, sz, err in bad:
    print(f"  BAD {sz:>12,}  {p}")
    print(f"      {err}")
sys.exit(0)
