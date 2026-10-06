# -*- coding: utf-8 -*-
"""_fix_fng_pit.py — 修补 sentiment_fng 的 PIT 可用时间 (数据底座数据修正)

问题: certified sentiment_fng 的 data_available_at 整列为 NaT (3,121 行)。
语义依据: alternative.me 的恐惧贪婪指数**当日值次日才定稿**
("Yesterday's value is final" —— 当日值还会变), 所以
    data_available_at = date_utc + 1 天 (次日 00:00 UTC)
这是保守且符合真实发布时间的填法: 用 2022-01-01 的情绪值, 最早只能在
2022-01-02 00:00 之后。

不填的后果: 该数据集无法通过字段层的 PIT 三列校验, 等于不可用。
"""
from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.config import CERTIFIED_DIR, QUALITY_RULE_VERSION  # noqa: E402
from data_foundation.manifest import (certify_manifest, empty_manifest,  # noqa: E402
                                      load_manifest, write_manifest)

ROOT = os.path.join(CERTIFIED_DIR, "sentiment_fng")


def main() -> None:
    files = []
    for dirpath, _, names in os.walk(ROOT):
        files += [os.path.join(dirpath, n) for n in names if n.endswith(".parquet")]
    assert files, "fng parquet 不存在"
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    n_nat = int(df["data_available_at"].isna().sum())
    print(f"修补前: {len(df)} 行, data_available_at 空 {n_nat}")
    df["data_available_at"] = (pd.to_datetime(df["date_utc"], utc=True)
                               + pd.Timedelta(days=1)).astype("datetime64[us, UTC]")
    for p in files:
        from data_foundation.atomic import atomic_write_parquet
        atomic_write_parquet(df, p)
    # 更新 manifest
    mf = load_manifest(ROOT)
    mf = certify_manifest(
        mf if mf.get("dataset") == "sentiment_fng"
        else empty_manifest("sentiment_fng", "1.0"),
        coverage_start=str(pd.to_datetime(df["date_utc"], utc=True).min()),
        coverage_end=str(pd.to_datetime(df["date_utc"], utc=True).max()),
        row_count=int(len(df)),
        duplicate_count=int(df["date_utc"].duplicated().sum()),
        gap_count=0,
        suspect_count=int(df["is_suspect"].sum()),
        quality_rule_version=QUALITY_RULE_VERSION)
    mf["notes"] = list(mf.get("notes") or []) + [
        "2026-10-05: data_available_at 由 NaT 修补为 date_utc+1d "
        "(fng 当日值次日定稿, 保守 PIT 语义)"]
    write_manifest(ROOT, mf)
    print(f"修补后: data_available_at 空 {int(df['data_available_at'].isna().sum())}"
          f" | 样例: {df['data_available_at'].iloc[0]}")
    print("manifest 已更新 (含修补记录)")


if __name__ == "__main__":
    main()