# -*- coding: utf-8 -*-
"""l1_coinbase.py — Coinbase L1 标准化 (第三交易所跨所验证)。"""
from __future__ import annotations

import glob
import json
import os

import pandas as pd

from .config import L1_DIR, RAW_DIR
from .l0 import list_raw_batches
from .l1 import CANDLE_COLS


def raw_files(venue: str, dataset: str, product: str) -> list[str]:
    out = []
    for meta in list_raw_batches(venue, dataset):
        if meta.get("source", {}).get("product") != product:
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append(os.path.join(d, f))
    return out


def normalize_coinbase_candles(venue: str, product: str, interval: str = "1h") -> pd.DataFrame:
    """Coinbase [time,low,high,open,close,volume] -> market_candle。"""
    frames = []
    for p in raw_files(venue, "spot_klines_1h", product):
        df = pd.read_csv(p)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={"time": "open_time_utc", "low": "low", "high": "high",
                            "open": "open", "close": "close", "volume": "volume_base"})
    df["open_time_utc"] = pd.to_datetime(pd.to_numeric(df["open_time_utc"]),
                                         unit="s", utc=True)
    for c in ["open", "high", "low", "close", "volume_base"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["close_time_utc"] = df["open_time_utc"] + pd.Timedelta(hours=1) - pd.Timedelta(seconds=1)
    base = product.split("-")[0]
    df["venue_id"] = venue
    df["instrument_id"] = product          # 如 BTC-USD
    df["symbol"] = product
    df["market_type"] = "spot"
    df["bar_interval"] = interval
    df["volume_quote"] = float("nan")      # Coinbase candle 无计价量
    df["volume_contracts"] = float("nan")
    df["trade_count"] = 0
    df["taker_buy_volume_base"] = float("nan")
    df["taker_buy_volume_quote"] = float("nan")
    df["is_closed"] = True                 # Coinbase REST 只返回已收盘 bar
    df["is_gap"] = False
    df["is_suspect"] = False
    df["quality_reason"] = ""
    df["data_available_at"] = df["close_time_utc"]
    df["source_batch_id"] = f"{base}USD_cb_v1"
    df = df.drop_duplicates("open_time_utc", keep="first").sort_values("open_time_utc")
    cols = [c for c in CANDLE_COLS if c in df.columns]
    return df[cols]


def normalize_coinbase_instruments() -> pd.DataFrame:
    """products -> L1 instrument 表 (仅 -USD 现货)。"""
    frames = []
    for meta in list_raw_batches("coinbase", "exchange_metadata"):
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, "coinbase", "exchange_metadata",
                         f"ingest_date={ingest}")
        for f in glob.glob(os.path.join(d, f"{meta['batch_id']}.*")):
            if f.endswith(".meta.json"):
                continue
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            for p in data:
                pid = p.get("id", "")
                if not pid.endswith("-USD"):
                    continue
                frames.append({
                    "venue_id": "coinbase",
                    "symbol": pid,
                    "instrument_id": pid,
                    "base_asset": p.get("base_currency"),
                    "quote_asset": p.get("quote_currency"),
                    "market_type": "spot",
                    "contract_type": "spot",
                    "contract_size": 1.0,
                    "tick_size": pd.to_numeric(p.get("quote_increment"), errors="coerce"),
                    "lot_size": pd.to_numeric(p.get("base_increment"), errors="coerce"),
                    "min_order_size": pd.to_numeric(p.get("min_market_funds"), errors="coerce"),
                    "price_precision": None,
                    "quantity_precision": None,
                    "listing_time": None,   # Coinbase REST 无上市时间
                    "delisting_time": None,
                    "status": "listed" if p.get("status") == "online" else p.get("status"),
                    "settlement_asset": p.get("quote_currency"),
                    "underlying_asset": p.get("base_currency"),
                    "data_available_at": pd.Timestamp.now(tz="UTC"),
                    "source_batch_id": meta["batch_id"],
                })
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames)
