# -*- coding: utf-8 -*-
"""derivatives.py — L1: 衍生品原始 CSV -> 统一 schema Parquet。"""
from __future__ import annotations

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import L1_DIR
from .l1 import instrument_id, load_raw_batches
from .schema import (DERIVATIVES_FUNDING_COLUMNS, DERIVATIVES_INDEX_COLUMNS,
                     DERIVATIVES_MARK_COLUMNS, DERIVATIVES_OI_COLUMNS,
                     DERIVATIVES_RATIO_COLUMNS)


def write_derivatives_parquet(df: pd.DataFrame, dataset: str, venue_id: str,
                              instrument: str, time_col: str) -> str:
    """衍生品数据集按 date 分区写 Parquet。"""
    root = os.path.join(L1_DIR, dataset, venue_id, instrument)
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    df["date"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    return root


def _concat_raw(venue_id: str, dataset: str, symbol: str) -> pd.DataFrame:
    frames = []
    for p in load_raw_batches(venue_id, dataset, symbol):
        frames.append(pd.read_csv(p))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def normalize_funding(venue_id: str, symbol: str) -> pd.DataFrame:
    df = _concat_raw(venue_id, "derivatives_funding", symbol)
    if df.empty:
        return df
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["funding_time_utc"] = pd.to_datetime(df["funding_time"], utc=True)
    df["funding_rate"] = pd.to_numeric(df.get("funding_rate"), errors="coerce")
    df["mark_price_at_funding"] = pd.to_numeric(df.get("mark_price"), errors="coerce")
    df["data_available_at"] = df["funding_time_utc"]
    df["source_batch_id"] = "binance_funding_v1"
    df = df.drop_duplicates("funding_time_utc", keep="first").sort_values("funding_time_utc")
    cols = [c for c, _ in DERIVATIVES_FUNDING_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def normalize_open_interest(venue_id: str, symbol: str) -> pd.DataFrame:
    df = _concat_raw(venue_id, "derivatives_open_interest", symbol)
    if df.empty:
        return df
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["timestamp_utc"] = pd.to_datetime(df["time"], utc=True)
    df["open_interest_contracts"] = pd.to_numeric(df.get("sumOpenInterest"), errors="coerce")
    df["open_interest_notional"] = pd.to_numeric(df.get("sumOpenInterestValue"), errors="coerce")
    df["data_available_at"] = df["timestamp_utc"]
    df["source_batch_id"] = "binance_oi_v1"
    df = df.drop_duplicates("timestamp_utc", keep="first").sort_values("timestamp_utc")
    cols = [c for c, _ in DERIVATIVES_OI_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def normalize_mark_price(venue_id: str, symbol: str) -> pd.DataFrame:
    df = _concat_raw(venue_id, "derivatives_mark_price", symbol)
    if df.empty:
        return df
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["open_time_utc"] = pd.to_datetime(df["open_time"], utc=True)
    for a, b in [("open", "mark_open"), ("high", "mark_high"),
                 ("low", "mark_low"), ("close", "mark_close")]:
        df[b] = pd.to_numeric(df.get(a), errors="coerce")
    df["data_available_at"] = df["open_time_utc"] + pd.Timedelta(hours=1)
    df["source_batch_id"] = "binance_mark_v1"
    df = df.drop_duplicates("open_time_utc", keep="first").sort_values("open_time_utc")
    cols = [c for c, _ in DERIVATIVES_MARK_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def normalize_index_price(venue_id: str, symbol: str) -> pd.DataFrame:
    """指数价 K 线 (1h) -> derivatives_index_price (index_* 列)。与 normalize_mark_price 同构。"""
    df = _concat_raw(venue_id, "derivatives_index_price", symbol)
    if df.empty:
        return df
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["open_time_utc"] = pd.to_datetime(df["open_time"], utc=True)
    for a, b in [("open", "index_open"), ("high", "index_high"),
                 ("low", "index_low"), ("close", "index_close")]:
        df[b] = pd.to_numeric(df.get(a), errors="coerce")
    df["data_available_at"] = df["open_time_utc"] + pd.Timedelta(hours=1)
    df["source_batch_id"] = "binance_index_v1"
    df = df.drop_duplicates("open_time_utc", keep="first").sort_values("open_time_utc")
    cols = [c for c, _ in DERIVATIVES_INDEX_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def normalize_ratio(venue_id: str, symbol: str, metric: str) -> pd.DataFrame:
    df = _concat_raw(venue_id, f"derivatives_ratio_{metric}", symbol)
    if df.empty:
        return df
    df["venue_id"] = venue_id
    df["instrument_id"] = instrument_id(symbol)
    df["symbol"] = symbol
    df["timestamp_utc"] = pd.to_datetime(df["time"], utc=True)
    df["metric"] = metric
    if metric == "taker":
        # takerlongshortRatio 字段不同: buySellRatio/sellVol/buyVol
        # (buyVol=主动买量, sellVol=主动卖量)
        df["long_account"] = pd.to_numeric(df.get("buyVol"), errors="coerce")
        df["long_short_ratio"] = pd.to_numeric(df.get("buySellRatio"), errors="coerce")
        df["short_account"] = pd.to_numeric(df.get("sellVol"), errors="coerce")
    else:
        df["long_account"] = pd.to_numeric(df.get("longAccount"), errors="coerce")
        df["long_short_ratio"] = pd.to_numeric(df.get("longShortRatio"), errors="coerce")
        df["short_account"] = pd.to_numeric(df.get("shortAccount"), errors="coerce")
    df["data_available_at"] = df["timestamp_utc"]
    df["source_batch_id"] = f"binance_{metric}_v1"
    df = df.drop_duplicates(["timestamp_utc", "metric"], keep="first").sort_values("timestamp_utc")
    cols = [c for c, _ in DERIVATIVES_RATIO_COLUMNS]
    return df[[c for c in cols if c in df.columns]]
