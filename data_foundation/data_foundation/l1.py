# -*- coding: utf-8 -*-
"""
l1.py — L1 标准化层
===================
- 从 L0 原始 CSV 读取, 统一列名/UTC/微秒时间戳/类型 (schema.py)
- 输出 Parquet: l1/{dataset}/{venue_id}/{market_type}/{instrument_id}/interval={interval}/date=YYYY-MM-DD/
- 1d/1w 由 1h 派生 (聚合规则写入 manifest)
"""
from __future__ import annotations

import glob
import os
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import L1_DIR, RAW_DIR, DERIVED_INTERVALS
from .l0 import list_raw_batches
from .schema import DATASETS

# Binance 源 K 线 12 列 -> L1 market_candle 映射
KLINE_MAP = {
    "Open Time": "open_time_utc",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume_base",
    "Close Time": "close_time_utc",
    "Quote Asset Volume": "volume_quote",
    "Number of Trades": "trade_count",
    "Taker Buy Base Asset Volume": "taker_buy_volume_base",
    "Taker Buy Quote Asset Volume": "taker_buy_volume_quote",
    "Ignore": None,
}
NUMERIC = ["open", "high", "low", "close", "volume_base", "volume_quote",
           "taker_buy_volume_base", "taker_buy_volume_quote"]
CANDLE_COLS = [c for c, _ in DATASETS["market_candle_spot_1h"]]


def instrument_id(symbol: str) -> str:
    """BTCUSDT -> BTC-USDT。"""
    m = re.match(r"^(.+?)(USDT|USDC|USD1|FDUSD|BUSD|USD)$", symbol)
    return f"{m.group(1)}-{m.group(2)}" if m else symbol


def load_raw_batches(venue_id: str, dataset: str, symbol: str) -> list[str]:
    """返回该 symbol 的全部 L0 批次文件路径 (按 ingest 排序)。"""
    out = []
    for meta in list_raw_batches(venue_id, dataset):
        if meta.get("source", {}).get("symbol") != symbol:
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue_id, dataset, f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append(os.path.join(d, f))
    return out


def normalize_klines(df: pd.DataFrame, venue_id: str, market_type: str,
                     symbol: str, interval: str = "1h") -> pd.DataFrame:
    """原始 K 线 -> L1 market_candle。"""
    df = df.rename(columns={k: v for k, v in KLINE_MAP.items() if v})
    df["open_time_utc"] = pd.to_datetime(df["open_time_utc"], utc=True, errors="coerce")
    df["close_time_utc"] = pd.to_datetime(df["close_time_utc"], utc=True, errors="coerce")
    for c in NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["trade_count"] = (pd.to_numeric(df.get("trade_count"), errors="coerce")
                         .fillna(0).astype("int64"))
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["market_type"] = market_type
    df["bar_interval"] = interval
    df["is_closed"] = True
    df["is_gap"] = False
    df["is_suspect"] = False
    df["quality_reason"] = ""
    df["data_available_at"] = df["close_time_utc"]
    df = df.drop_duplicates("open_time_utc", keep="first")
    df = df.sort_values("open_time_utc").reset_index(drop=True)
    return df[[c for c in CANDLE_COLS if c in df.columns]]


def derive_aggregates(df_1h: pd.DataFrame, interval: str) -> pd.DataFrame:
    """1h -> 1d/1w: open=first, high=max, low=min, close=last, 量求和, 其余末值。"""
    rule = {"open": "first", "high": "max", "low": "min", "close": "last",
            "volume_base": "sum", "volume_quote": "sum", "trade_count": "sum",
            "taker_buy_volume_base": "sum", "taker_buy_volume_quote": "sum"}
    freq = DERIVED_INTERVALS[interval]
    g = df_1h.set_index("open_time_utc")
    agg = g.resample(freq).agg(rule).dropna(subset=["close"]).reset_index()
    step = pd.Timedelta(days=1) if interval == "1d" else pd.Timedelta(days=7)
    agg["close_time_utc"] = agg["open_time_utc"] + step - pd.Timedelta(seconds=1)
    agg["bar_interval"] = interval
    agg["data_available_at"] = agg["close_time_utc"]
    agg["is_gap"] = False
    agg["is_suspect"] = False
    agg["quality_reason"] = ""
    agg["is_closed"] = True
    for c in ["venue_id", "instrument_id", "symbol", "market_type", "source_batch_id"]:
        agg[c] = df_1h[c].iloc[0] if c in df_1h.columns else ""
    return agg[[c for c in CANDLE_COLS if c in agg.columns]]


def write_parquet(df: pd.DataFrame, dataset: str, venue_id: str, market_type: str,
                  instrument: str, interval: str) -> str:
    """写 Parquet (时间列 timestamp[us, UTC]; 单文件/交易对/周期, date 为普通列)。

    设计原则: 分区不要切得过细 —— 每 (dataset, venue, instrument, interval)
    一个文件, 由 pyarrow 列统计支撑快速过滤, 研究读取更快。
    """
    root = os.path.join(L1_DIR, dataset, venue_id, market_type, instrument,
                        f"interval={interval}")
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    df["date"] = df["open_time_utc"].dt.strftime("%Y-%m-%d")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, os.path.join(root, "data.parquet"), compression="snappy")
    return root
