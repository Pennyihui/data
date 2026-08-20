# -*- coding: utf-8 -*-
"""
reader.py — L2 Certified 读取 API (L3 研究默认读取层)
======================================================
只读 certified 快照, 统一返回 UTC 时间列。

示例:
  from data_foundation.reader import load_candles
  df = load_candles("binance", "BTC-USDT", "1h")
  df = load_candles("binance", "BTC-USDT", "1h", as_of="2026-08-01")
"""
from __future__ import annotations

import os

import pandas as pd
import pyarrow.parquet as pq

from .config import CERTIFIED_DIR


def _dataset_root(dataset: str, venue: str, instrument: str, interval: str | None) -> str:
    parts = [CERTIFIED_DIR, dataset, venue, "spot", instrument]
    if interval:
        parts.append(f"interval={interval}")
    return os.path.join(*parts)


def load_candles(venue: str, instrument: str, interval: str = "1h",
                 as_of=None, cols: list[str] | None = None) -> pd.DataFrame:
    """读取 certified market_candle。as_of 做 PIT 过滤 (data_available_at <= as_of)。"""
    root = _dataset_root(f"market_candle_spot_{interval}", venue, instrument, interval)
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True)
    df = df.sort_values("open_time_utc").reset_index(drop=True)
    if as_of is not None:
        df = df[df["data_available_at"] <= pd.Timestamp(as_of, tz="UTC")]
    if cols:
        df = df[[c for c in cols if c in df.columns]]
    return df


def load_derivatives(venue: str, instrument: str, dataset: str) -> pd.DataFrame:
    """读取 certified 衍生品数据集 (funding / open_interest / mark_price / ratio)。"""
    root = os.path.join(CERTIFIED_DIR, dataset, venue, instrument)
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True)
    return df.sort_values(df.columns[0]).reset_index(drop=True)


def load_instruments(venue: str = "binance") -> pd.DataFrame:
    p = os.path.join(CERTIFIED_DIR, "instrument", venue, "instruments.parquet")
    if not os.path.exists(p):
        p = os.path.join(os.path.dirname(CERTIFIED_DIR), "..", "l1", "instrument", venue,
                         "instruments.parquet")
    df = pd.read_parquet(p)
    for c in df.columns:
        if "time" in c:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


def load_manifest(dataset: str) -> dict:
    import json
    p = os.path.join(CERTIFIED_DIR, dataset, "manifest.json")
    with open(p, encoding="utf-8") as f:
        return json.load(f)
