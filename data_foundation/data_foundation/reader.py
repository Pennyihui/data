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
                 as_of=None, cols: list[str] | None = None,
                 market_type: str = "spot") -> pd.DataFrame:
    """读取 certified market_candle。as_of 做 PIT 过滤 (data_available_at <= as_of)。"""
    ds = f"market_candle_{market_type}_{interval}"
    root = os.path.join(CERTIFIED_DIR, ds, venue, market_type, instrument,
                        f"interval={interval}")
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


def load_instruments(venue_id: str | None = None, market_type: str | None = None,
                     as_of=None) -> pd.DataFrame:
    """读取 certified PIT instrument 元数据 (跨 venue 合并)。

    as_of 语义: 保留 data_available_at <= as_of 的每 (venue_id, symbol)
    最后一版快照 (取 max data_available_at); as_of=None 取最新版本。
    venue_id / market_type 可过滤; 返回 INSTRUMENT_COLUMNS 列。
    """
    from .schema import INSTRUMENT_COLUMNS
    root = os.path.join(CERTIFIED_DIR, "instrument")
    if venue_id:
        venues = [venue_id]
    elif os.path.isdir(root):
        venues = sorted(d for d in os.listdir(root)
                        if os.path.isdir(os.path.join(root, d)))
    else:
        venues = []
    frames = []
    for v in venues:
        d = os.path.join(root, v, "all")
        if not os.path.isdir(d):
            continue
        frames.append(pq.read_table(d).to_pandas())
    if not frames:
        return pd.DataFrame(columns=[c for c, _ in INSTRUMENT_COLUMNS])
    df = pd.concat(frames, ignore_index=True)
    for c in df.columns:
        if "time" in c or c == "data_available_at" or c.endswith("_utc"):
            df[c] = pd.to_datetime(df[c], utc=True)
    if market_type:
        df = df[df["market_type"] == market_type]
    if as_of is not None:
        df = df[df["data_available_at"] <= pd.Timestamp(as_of, tz="UTC")]
    # Binance 现货/永续 symbol 字符串相同, 去重键必须含 market_type
    df = df.sort_values("data_available_at").drop_duplicates(
        subset=["venue_id", "symbol", "market_type"], keep="last").reset_index(drop=True)
    cols = [c for c, _ in INSTRUMENT_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def load_manifest(dataset: str) -> dict:
    import json
    p = os.path.join(CERTIFIED_DIR, dataset, "manifest.json")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def load_asset_master(asset: str | None = None,
                      venue_id: str | None = None) -> pd.DataFrame:
    """读取 certified asset_master/master 最新全量快照。

    asset / venue_id 可选过滤 (精确匹配); 返回 ASSET_MASTER_COLUMNS 列
    (外加认证附加列 is_suspect/quality_reason/date)。
    """
    from .schema import ASSET_MASTER_COLUMNS
    root = os.path.join(CERTIFIED_DIR, "asset_master", "master", "all")
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at" or c.endswith("_utc"):
            df[c] = pd.to_datetime(df[c], utc=True)
    if asset is not None:
        df = df[df["asset"] == asset]
    if venue_id is not None:
        df = df[df["venue_id"] == venue_id]
    cols = [c for c, _ in ASSET_MASTER_COLUMNS]
    return df[[c for c in cols if c in df.columns]].reset_index(drop=True)
