# -*- coding: utf-8 -*-
"""
l1_deribit.py — Deribit L1 标准化
==================================
- dvol_15m: 原始 OHLC 数组 [ts, open, high, low, close] ->
    venue_id, currency, timestamp_utc, dvol (=close/100, 百分数->小数)
- options_chain_snapshot: instrument_name 解析
    (BTC-22AUG26-77000-P -> expiry_utc=2026-08-22 08:00 UTC(交割时刻),
     strike=77000, cp=P); mark_iv/100; bid_iv/ask_iv 该端点不提供 -> NaN
- index_price: get_index_price 快照 (参考数据, 供 underlying 对照)
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd

from .config import RAW_DIR
from .derivatives import write_derivatives_parquet  # noqa: F401  (复用)
from .l0 import list_raw_batches

_MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
           "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
# 到期日格式: 1-2 位日 + 3 位月 + 2 位年, 如 "22AUG26" / "4SEP26"
_EXPIRY_RE = re.compile(r"^(\d{1,2})([A-Z]{3})(\d{2})$")

DVOL_COLS = ["venue_id", "currency", "timestamp_utc", "dvol",
             "data_available_at", "source_batch_id"]
CHAIN_COLS = ["venue_id", "currency", "instrument_name", "expiry_utc", "strike",
              "cp", "mark_iv", "bid_iv", "ask_iv", "open_interest", "volume",
              "underlying_price", "snapshot_utc", "data_available_at",
              "source_batch_id"]


def _raw_batches(venue: str, dataset: str, batch_prefix: str):
    """返回该数据集匹配前缀的所有 (文件路径, meta) 对。"""
    out = []
    for meta in list_raw_batches(venue, dataset):
        if not meta["batch_id"].startswith(batch_prefix):
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append((os.path.join(d, f), meta))
    return out


def parse_expiry(s: str) -> datetime:
    """'22AUG26' / '4SEP26' -> 2026-08-22 08:00 UTC (Deribit 期权到期交割时刻)。"""
    m = _EXPIRY_RE.match(s.strip().upper())
    if not m:
        raise ValueError(f"无法解析到期日: {s!r}")
    day, mon, yy = int(m.group(1)), _MONTHS[m.group(2)], int(m.group(3))
    return datetime(2000 + yy, mon, day, 8, 0, tzinfo=timezone.utc)


def parse_instrument(name: str):
    """'BTC-22AUG26-77000-P' -> (currency, expiry_dt, strike, cp)。"""
    parts = name.split("-")
    if len(parts) != 4:
        return None
    currency, expiry_s, strike_s, cp = parts
    try:
        return currency, parse_expiry(expiry_s), float(strike_s), cp.upper()
    except Exception:  # noqa: BLE001
        return None


def normalize_dvol() -> pd.DataFrame:
    frames = []
    for p, meta in _raw_batches("deribit", "dvol_15m", "dvol_"):
        with open(p, encoding="utf-8") as f:
            rows = json.load(f)
        if not rows:
            continue
        currency = (meta.get("source", {}).get("currency")
                    or meta["batch_id"].split("_")[1].upper())
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
        df["currency"] = currency
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["timestamp_utc"] = pd.to_datetime(pd.to_numeric(out["ts"]),
                                          unit="ms", utc=True)
    out["dvol"] = pd.to_numeric(out["close"], errors="coerce") / 100.0
    out["venue_id"] = "deribit"
    out["data_available_at"] = out["timestamp_utc"]
    out["source_batch_id"] = "deribit_dvol_v1"
    out = out.drop_duplicates(["currency", "timestamp_utc"], keep="last")
    out = out.sort_values("timestamp_utc").reset_index(drop=True)
    return out[[c for c in DVOL_COLS if c in out.columns]]


def normalize_chain() -> pd.DataFrame:
    frames = []
    for p, meta in _raw_batches("deribit", "options_chain", "chain_"):
        with open(p, encoding="utf-8") as f:
            rows = json.load(f)
        if not rows:
            continue
        snap = pd.to_datetime(meta.get("source", {}).get("fetched_at")
                              or meta.get("ingested_at"), utc=True)
        recs, skipped = [], 0
        for r in rows:
            parsed = parse_instrument(r.get("instrument_name", ""))
            if parsed is None:
                skipped += 1
                continue
            cur, expiry_dt, strike, cp = parsed
            recs.append({
                "venue_id": "deribit",
                "currency": cur,
                "instrument_name": r["instrument_name"],
                "expiry_utc": expiry_dt,
                "strike": strike,
                "cp": cp,
                "mark_iv": pd.to_numeric(r.get("mark_iv"), errors="coerce") / 100.0,
                "bid_iv": float("nan"),   # get_book_summary_by_currency 不提供
                "ask_iv": float("nan"),
                "open_interest": pd.to_numeric(r.get("open_interest"), errors="coerce"),
                "volume": pd.to_numeric(r.get("volume"), errors="coerce"),
                "underlying_price": pd.to_numeric(r.get("underlying_price"),
                                                  errors="coerce"),
                "snapshot_utc": snap,
                "data_available_at": snap,
                "source_batch_id": meta["batch_id"],
            })
        if skipped:
            print(f"  [warn] chain 解析失败 {skipped} 行 (批次 {meta['batch_id']})",
                  flush=True)
        frames.append(pd.DataFrame(recs))
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["instrument_name", "snapshot_utc"], keep="last")
    return out[[c for c in CHAIN_COLS if c in out.columns]]


def normalize_index_price() -> pd.DataFrame:
    frames = []
    for p, meta in _raw_batches("deribit", "index_price", "index_"):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        idx = meta.get("source", {}).get("index_name", "btc_usd")
        currency = idx.split("_")[0].upper()
        fetched = pd.to_datetime(meta.get("source", {}).get("fetched_at")
                                 or meta.get("ingested_at"), utc=True)
        frames.append(pd.DataFrame([{
            "venue_id": "deribit",
            "currency": currency,
            "timestamp_utc": fetched,
            "index_price": pd.to_numeric(j.get("index_price"), errors="coerce"),
            "estimated_delivery_price": pd.to_numeric(
                j.get("estimated_delivery_price"), errors="coerce"),
            "data_available_at": fetched,
            "source_batch_id": meta["batch_id"],
        }]))
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(["currency", "timestamp_utc"], keep="last")
