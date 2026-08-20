# -*- coding: utf-8 -*-
"""
l1_okx.py — OKX L1 标准化 (跨交易所 schema 验证)
=================================================
- spot/swap K线: 数组 [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm] -> market_candle
    * confirm: 0=未收盘, 1=已收盘 -> is_closed (跨所语义验证)
    * swap 的 vol 是"合约张数" -> 存 volume_contracts (不与其他成交量混用)
    * spot 的 vol 是基础币量 -> volume_base
- funding: realizedRate 单独存 realized_rate
- mark/index K线: 独立数据集 (mark_* / index_*)
- OI: 当前快照 (contracts + notional)
"""
from __future__ import annotations

import glob
import json
import os

import pandas as pd

from .config import L1_DIR, RAW_DIR
from .l0 import list_raw_batches
from .l1 import CANDLE_COLS
from .l1 import write_parquet  # noqa: F401  (复用)

# OKX 源字段 -> L1 字段 (K线数组下标)
OKX_CANDLE_IDX = {"ts": 0, "open": 1, "high": 2, "low": 3, "close": 4,
                  "vol": 5, "volCcy": 6, "volCcyQuote": 7, "confirm": 8}


def raw_files(venue: str, dataset: str, symbol: str) -> list[str]:
    out = []
    for meta in list_raw_batches(venue, dataset):
        if meta.get("source", {}).get("instId", "").startswith(
                symbol.replace("USDT", "")) or \
           meta.get("source", {}).get("symbol") == symbol:
            ingest = meta["ingested_at"][:10]
            d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
            for f in sorted(os.listdir(d)):
                if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                    out.append(os.path.join(d, f))
    return out


def _load_csv(paths: list[str]) -> pd.DataFrame:
    return pd.concat([pd.read_csv(p) for p in paths], ignore_index=True) if paths \
        else pd.DataFrame()


def normalize_okx_candles(venue: str, symbol: str, market_type: str,
                          interval: str = "1h") -> pd.DataFrame:
    """spot/perpetual 1H -> market_candle (跨所 schema 一致)。"""
    dataset = "spot_klines_1h" if market_type == "spot" else "perpetual_klines_1h"
    df = _load_csv(raw_files(venue, dataset, symbol))
    if df.empty:
        return df
    df = df.rename(columns={"ts": "open_time_utc", "open": "open", "high": "high",
                            "low": "low", "close": "close", "vol": "vol_contracts",
                            "volCcy": "volume_base", "volCcyQuote": "volume_quote",
                            "confirm": "confirm"})
    df["open_time_utc"] = pd.to_datetime(pd.to_numeric(df["open_time_utc"]),
                                         unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume_base", "volume_quote",
              "vol_contracts"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["close_time_utc"] = df["open_time_utc"] + pd.Timedelta(hours=1) - pd.Timedelta(milliseconds=1)
    df["trade_count"] = 0  # OKX K线不提供成交笔数
    df["taker_buy_volume_base"] = float("nan")
    df["taker_buy_volume_quote"] = float("nan")
    df["is_closed"] = pd.to_numeric(df["confirm"], errors="coerce").eq(1)
    df = df.drop(columns=["confirm"], errors="ignore")
    df["venue_id"] = venue
    df["instrument_id"] = f"{symbol[:-4]}-USDT" if market_type == "spot" \
        else f"{symbol[:-4]}-USDT-SWAP"
    df["symbol"] = symbol if market_type == "spot" else f"{symbol[:-4]}-USDT-SWAP"
    df["market_type"] = market_type
    df["bar_interval"] = interval
    df["is_gap"] = False
    df["is_suspect"] = False
    df["quality_reason"] = ""
    df["data_available_at"] = df["close_time_utc"].where(df["is_closed"],
                                                         df["open_time_utc"] + pd.Timedelta(hours=1))
    df["volume_contracts"] = df["vol_contracts"]
    df = df.drop(columns=["vol_contracts"], errors="ignore")
    df = df.drop_duplicates("open_time_utc", keep="first").sort_values("open_time_utc")
    cols = [c for c in CANDLE_COLS if c in df.columns]
    return df[cols]


def normalize_okx_funding(venue: str, symbol: str) -> pd.DataFrame:
    df = _load_csv(raw_files(venue, "derivatives_funding", symbol))
    if df.empty:
        return df
    df["funding_time_utc"] = pd.to_datetime(pd.to_numeric(df["fundingTime"]),
                                            unit="ms", utc=True)
    df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df["realized_rate"] = pd.to_numeric(df["realizedRate"], errors="coerce")
    df["venue_id"] = venue
    df["instrument_id"] = f"{symbol[:-4]}-USDT-SWAP"
    df["symbol"] = f"{symbol[:-4]}-USDT-SWAP"
    df["data_available_at"] = df["funding_time_utc"]
    df["source_batch_id"] = "okx_funding_v1"
    df = df.drop_duplicates("funding_time_utc", keep="first").sort_values("funding_time_utc")
    cols = ["venue_id", "instrument_id", "symbol", "funding_time_utc", "funding_rate",
            "mark_price_at_funding", "realized_rate", "data_available_at",
            "source_batch_id"]
    df["mark_price_at_funding"] = float("nan")
    return df[[c for c in cols if c in df.columns]]


def normalize_okx_mark_index(venue: str, symbol: str, kind: str) -> pd.DataFrame:
    """kind: mark -> derivatives_mark_price (mark_*), index -> derivatives_index_price (index_*)。"""
    dataset = "derivatives_mark_price" if kind == "mark" else "derivatives_index_price"
    df = _load_csv(raw_files(venue, dataset, symbol))
    if df.empty:
        return df
    df["open_time_utc"] = pd.to_datetime(pd.to_numeric(df["ts"]), unit="ms", utc=True)
    p = "mark" if kind == "mark" else "index"
    for a in ["open", "high", "low", "close"]:
        df[f"{p}_{a}"] = pd.to_numeric(df[a], errors="coerce")
    df["venue_id"] = venue
    df["instrument_id"] = f"{symbol[:-4]}-USDT-SWAP" if kind == "mark" \
        else f"{symbol[:-4]}-USDT"
    df["symbol"] = df["instrument_id"]
    df["data_available_at"] = df["open_time_utc"] + pd.Timedelta(hours=1)
    df["source_batch_id"] = f"okx_{p}_v1"
    df = df.drop_duplicates("open_time_utc", keep="first").sort_values("open_time_utc")
    cols = ["venue_id", "instrument_id", "symbol", "open_time_utc",
            f"{p}_open", f"{p}_high", f"{p}_low", f"{p}_close",
            "data_available_at", "source_batch_id"]
    return df[[c for c in cols if c in df.columns]]


def normalize_okx_oi(venue: str, symbol: str) -> pd.DataFrame:
    frames = []
    for meta in list_raw_batches(venue, "derivatives_open_interest"):
        if not meta.get("source", {}).get("instId", "").startswith(
                f"{symbol[:-4]}-USDT-SWAP"):
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, "derivatives_open_interest",
                         f"ingest_date={ingest}")
        for f in glob.glob(os.path.join(d, f"{meta['batch_id']}.*")):
            if f.endswith(".meta.json"):
                continue
            with open(f, encoding="utf-8") as fh:
                oi = json.load(fh)
            for row in oi:
                frames.append({
                    "venue_id": venue,
                    "instrument_id": f"{symbol[:-4]}-USDT-SWAP",
                    "symbol": f"{symbol[:-4]}-USDT-SWAP",
                    "timestamp_utc": pd.to_datetime(pd.to_numeric(row.get("ts", 0)),
                                                    unit="ms", utc=True),
                    "open_interest_contracts": pd.to_numeric(row.get("oi"), errors="coerce"),
                    "open_interest_notional": pd.to_numeric(row.get("oiUsd"), errors="coerce"),
                    "data_available_at": pd.Timestamp.now(tz="UTC"),
                    "source_batch_id": "okx_oi_snapshot_v1",
                })
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames)


def normalize_okx_instruments() -> pd.DataFrame:
    """OKX instruments -> L1 instrument 表 (仅 -USDT / -USDT-SWAP)。"""
    frames = []
    for meta in list_raw_batches("okx", "exchange_metadata"):
        itype = meta["batch_id"].split("_")[1]
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, "okx", "exchange_metadata", f"ingest_date={ingest}")
        for f in glob.glob(os.path.join(d, f"{meta['batch_id']}.*")):
            if f.endswith(".meta.json"):
                continue
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            for s in data:
                inst = s.get("instId", "")
                if not inst.endswith(("-USDT", "-USDT-SWAP")):
                    continue
                is_swap = inst.endswith("-SWAP")
                frames.append({
                    "venue_id": "okx",
                    "symbol": inst,
                    "instrument_id": inst,
                    "base_asset": (inst[:-9] if is_swap else inst[:-5]),
                    "quote_asset": "USDT",
                    "market_type": "perpetual" if is_swap else "spot",
                    "contract_type": "perpetual" if is_swap else "spot",
                    "contract_size": pd.to_numeric(s.get("ctVal", 1), errors="coerce")
                    if is_swap else 1.0,
                    "tick_size": pd.to_numeric(s.get("tickSz"), errors="coerce"),
                    "lot_size": pd.to_numeric(s.get("lotSz"), errors="coerce"),
                    "min_order_size": pd.to_numeric(s.get("minSz"), errors="coerce"),
                    "price_precision": None,
                    "quantity_precision": None,
                    "listing_time": pd.to_datetime(pd.to_numeric(s.get("listTime", 0)),
                                                   unit="ms", utc=True),
                    "delisting_time": None,
                    "status": "listed" if s.get("state") == "live" else s.get("state"),
                    "settlement_asset": s.get("settleCcy") or "USDT",
                    "underlying_asset": s.get("uly") or (inst[:-9] if is_swap else inst[:-5]),
                    "data_available_at": pd.Timestamp.now(tz="UTC"),
                    "source_batch_id": meta["batch_id"],
                })
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames)
