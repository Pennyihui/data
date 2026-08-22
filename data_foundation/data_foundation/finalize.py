# -*- coding: utf-8 -*-
"""
finalize.py — manifest 汇总
===========================
certified 数据集 manifest 原本按数据集单份、由各 stage 写入时互相覆盖
(最后写入的 venue 统计覆盖全局)。本模块从 certified parquet 重新统计
每个数据集的真实全局行数/覆盖范围/suspect 数, 重写 manifest.json。

用法: python -m data_foundation.finalize
"""
from __future__ import annotations

import glob
import json
import os
from datetime import datetime, timezone

import pyarrow.parquet as pq

from .config import CERTIFIED_DIR


def scan_dataset(dataset_dir: str) -> dict | None:
    """统计某数据集目录下所有 data.parquet (跨 venue)。

    只读必要列 (行数取 parquet 元数据), 避免 token_transfer 级大表全量载入。
    """
    files = glob.glob(os.path.join(dataset_dir, "**", "data.parquet"), recursive=True)
    if not files:
        return None
    row_count = dup = gap = suspect = 0
    cov_start = cov_end = None
    for f in files:
        pf = pq.ParquetFile(f)
        row_count += pf.metadata.num_rows
        sch = pf.schema_arrow
        names = set(sch.names)
        if "is_suspect" in names:
            suspect += int(pf.read(columns=["is_suspect"])
                           .column("is_suspect").to_pandas().sum())
        if "is_gap" in names:
            gap += int(pf.read(columns=["is_gap"])
                       .column("is_gap").to_pandas().sum())
        # 时间列: 优先已知主键时间列, 其次首个 datetime 列, 最后退回首列
        prefer = ("open_time_utc", "timestamp_utc", "funding_time_utc",
                  "date_utc", "time_utc", "fetched_at", "block_timestamp_utc",
                  "snapshot_utc", "data_available_at")
        time_col = next((c for c in prefer if c in names), None)
        if time_col is None:
            dt_cols = [n for n in sch.names
                       if str(sch.field(n).type).startswith("timestamp")]
            time_col = dt_cols[0] if dt_cols else sch.names[0]
        s = pf.read(columns=[time_col]).column(time_col).to_pandas().dropna()
        if len(s):
            v_min, v_max = s.min(), s.max()
            if cov_start is None or v_min < cov_start:
                cov_start = v_min
            if cov_end is None or v_max > cov_end:
                cov_end = v_max
    return {"row_count": row_count, "duplicate_count": dup, "gap_count": gap,
            "suspect_count": suspect,
            "coverage_start": str(cov_start), "coverage_end": str(cov_end)}


def finalize_all(verbose=True) -> list[str]:
    fixed = []
    for mf in sorted(glob.glob(os.path.join(CERTIFIED_DIR, "**", "manifest.json"),
                               recursive=True)):
        ds_dir = os.path.dirname(mf)
        with open(mf, encoding="utf-8") as f:
            m = json.load(f)
        stats = scan_dataset(ds_dir)
        if not stats:
            continue
        m["row_count"] = stats["row_count"]
        m["duplicate_count"] = stats["duplicate_count"]
        m["gap_count"] = stats["gap_count"]
        m["suspect_count"] = stats["suspect_count"]
        m["coverage_start"] = stats["coverage_start"]
        m["coverage_end"] = stats["coverage_end"]
        m["certification_status"] = "certified"
        m["certified_at"] = datetime.now(timezone.utc).isoformat()
        m["finalized_at"] = datetime.now(timezone.utc).isoformat()
        m["note_agg"] = "stats recomputed across all venues by finalize.py"
        with open(mf, "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=2, default=str)
        fixed.append(os.path.basename(ds_dir))
        if verbose:
            print(f"{os.path.basename(ds_dir):<34} rows={stats['row_count']:>9} "
                  f"suspect={stats['suspect_count']:>6} "
                  f"{str(stats['coverage_start'])[:10]}~{str(stats['coverage_end'])[:10]}")
    return fixed


if __name__ == "__main__":
    fixed = finalize_all()
    print(f"\n已汇总 {len(fixed)} 个数据集 manifest")
