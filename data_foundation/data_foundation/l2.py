# -*- coding: utf-8 -*-
"""
l2.py — L2 认证层
=================
质量规则 (quality_rule_version 记录, 异常只标记不修改):
  1. high >= max(open, close); high >= low
  2. low  <= min(open, close)
  3. volume_base >= 0, volume_quote >= 0, trade_count >= 0
  4. open_time_utc 唯一 (主键)
  5. 周期边界正确 (1h/1d/1w 对齐)
  6. 时间不超过可用时间 (close_time <= now + 容忍)
  7. 缺口检测 (与前一根间隔 > 1 周期 => is_gap)
  8. 价格非负、有限
输出:
  l2/certified/{dataset}/{venue_id}/{market_type}/{instrument_id}/interval={interval}/date=.../
  l2/certified/{dataset}/manifest.json
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import CERTIFIED_DIR, QUALITY_RULE_VERSION
from .manifest import certify_manifest, empty_manifest, write_manifest

PERIOD = {"1h": "h", "1d": "D", "1w": "W-MON"}


def certify_candles(df: pd.DataFrame) -> pd.DataFrame:
    """执行质量规则, 标记 is_suspect/is_gap + quality_reason。不修改原值。"""
    df = df.copy()
    reasons = {}
    ok = pd.Series(True, index=df.index)

    def mark(mask, reason):
        nonlocal ok
        m = mask.fillna(True)
        ok &= ~m
        for idx in df.index[m]:
            reasons.setdefault(idx, []).append(reason)

    # 规则 1-2: 高低价一致性
    mark(df["high"] < df[["open", "close"]].max(axis=1), "high<max(open,close)")
    mark(df["low"] > df[["open", "close"]].min(axis=1), "low>min(open,close)")
    mark(df["high"] < df["low"], "high<low")
    # 规则 3: 非负
    mark(df["volume_base"] < 0, "volume_base<0")
    mark(df["volume_quote"] < 0, "volume_quote<0")
    mark(df["trade_count"] < 0, "trade_count<0")
    # 规则 8: 价格有限
    mark(~np.isfinite(df[["open", "high", "low", "close"]]).all(axis=1), "price_not_finite")
    mark(df["close"] <= 0, "close<=0")
    # 规则 4: open_time 唯一 (主键)
    dup = df["open_time_utc"].duplicated(keep=False)
    mark(dup, "open_time_duplicated")
    # 规则 5: 周期边界 (1h 对齐到整点, 1d 到 00:00)
    if df["bar_interval"].iloc[0] == "1h":
        mark(df["open_time_utc"].dt.minute != 0, "bar_not_aligned")
    elif df["bar_interval"].iloc[0] == "1d":
        mark(df["open_time_utc"].dt.time != pd.Timestamp("00:00:00").time(), "bar_not_aligned")
    # 规则 6: 未超过可用时间
    now = pd.Timestamp.now(tz="UTC")
    mark(df["close_time_utc"] > now + pd.Timedelta(hours=1), "close_in_future")
    # 规则 7: 缺口
    prev = df["open_time_utc"].shift(1)
    gap = (df["open_time_utc"] - prev) > pd.Timedelta(hours=2) if \
        df["bar_interval"].iloc[0] == "1h" else pd.Series(False, index=df.index)
    df["is_gap"] = gap.fillna(False)

    df["is_suspect"] = ~ok
    df["quality_reason"] = df.index.map(
        lambda i: ";".join(reasons.get(i, [])))
    return df


def write_certified(df: pd.DataFrame, dataset: str, venue_id: str, market_type: str,
                    instrument: str, interval: str) -> tuple[str, dict]:
    """写 certified 快照 + 返回数据集目录与统计。"""
    root = os.path.join(CERTIFIED_DIR, dataset, venue_id, market_type, instrument,
                        f"interval={interval}")
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    df["date"] = df["open_time_utc"].dt.strftime("%Y-%m-%d")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    stats = {
        "row_count": int(len(df)),
        "duplicate_count": int(df["open_time_utc"].duplicated().sum()),
        "gap_count": int(df["is_gap"].sum()),
        "suspect_count": int(df["is_suspect"].sum()),
        "coverage_start": str(df["open_time_utc"].min()),
        "coverage_end": str(df["open_time_utc"].max()),
    }
    return root, stats


def certify_derivatives(df: pd.DataFrame, time_col: str,
                        core_numeric_cols: list[str] | None = None) -> pd.DataFrame:
    """衍生品认证: 时间唯一/核心数值列有限/非负/不超可用时间。只标记不修改。

    core_numeric_cols: 该数据集的核心数值列, 只对这些列做有限性检查
    (辅助列如 mark_price_at_funding 早期可能缺失, 不视为异常)。
    """
    df = df.copy()
    core = core_numeric_cols or [c for c in df.select_dtypes(include=[np.number]).columns
                                 if c != time_col]
    reasons = {}
    ok = pd.Series(True, index=df.index)

    def mark(mask, reason):
        nonlocal ok
        m = mask.fillna(True)
        ok &= ~m
        for idx in df.index[m]:
            reasons.setdefault(idx, []).append(reason)

    mark(df[time_col].duplicated(keep=False), f"{time_col}_duplicated")
    for c in core:
        mark(~np.isfinite(df[c]), f"{c}_not_finite")
    if "open_interest_contracts" in df.columns:
        mark(df["open_interest_contracts"] < 0, "oi_contracts<0")
    if "open_interest_notional" in df.columns:
        mark(df["open_interest_notional"] < 0, "oi_notional<0")
    now = pd.Timestamp.now(tz="UTC")
    mark(pd.to_datetime(df[time_col], utc=True) > now + pd.Timedelta(hours=1),
         "time_in_future")
    df["is_suspect"] = ~ok
    df["quality_reason"] = df.index.map(lambda i: ";".join(reasons.get(i, [])))
    return df


def write_certified_derivatives(df: pd.DataFrame, dataset: str, venue_id: str,
                                instrument: str, time_col: str) -> str:
    root = os.path.join(CERTIFIED_DIR, dataset, venue_id, instrument)
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    df["date"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    return root


def build_dataset_manifest(dataset: str, venue_id: str, market_type: str,
                           instrument: str, interval: str, stats: dict,
                           source_batches: list, aggregation_rules: dict | None) -> dict:
    """生成并保存 L2 manifest。"""
    manifest = empty_manifest(dataset, "1.0.0")
    manifest["source_batches"] = source_batches
    manifest["aggregation_rules"] = aggregation_rules
    manifest["timestamp_unit"] = "us"
    manifest["timezone"] = "UTC"
    certify_manifest(manifest, stats["coverage_start"], stats["coverage_end"],
                     stats["row_count"], stats["duplicate_count"],
                     stats["gap_count"], stats["suspect_count"],
                     QUALITY_RULE_VERSION)
    dataset_dir = os.path.join(CERTIFIED_DIR, dataset)
    os.makedirs(dataset_dir, exist_ok=True)
    write_manifest(dataset_dir, manifest)
    return manifest
