# -*- coding: utf-8 -*-
"""l1_stablecoins.py — 稳定币数据 L1 标准化。"""
from __future__ import annotations

import glob
import json
import os

import pandas as pd

from .config import L1_DIR, RAW_DIR
from .l0 import list_raw_batches

SUPPLY_COLS = ["venue_id", "token", "date_utc", "circulating_supply", "rank",
               "source_batch_id"]
FLOW_COLS = ["venue_id", "date_utc", "metric", "value_usd", "source_batch_id"]
PEG_COLS = ["venue_id", "token", "time_utc", "price", "peg_deviation",
            "data_available_at", "source_batch_id"]


def _raw_paths(venue: str, dataset: str, batch_prefix: str) -> list[str]:
    out = []
    for meta in list_raw_batches(venue, dataset):
        if not meta["batch_id"].startswith(batch_prefix):
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append(os.path.join(d, f))
    return out


def normalize_supply(tokens=None) -> pd.DataFrame:
    """CMC 流通量历史 -> stablecoin_supply。"""
    tokens = tokens or ["USDT", "USDC", "DAI"]
    frames = []
    for p in _raw_paths("cmc", "stablecoin_supply", "cmc_supply"):
        df = pd.read_csv(p)
        df["venue_id"] = "cmc"
        df["date_utc"] = pd.to_datetime(df["date"], utc=True)
        df["circulating_supply"] = pd.to_numeric(df["circulating_supply"], errors="coerce")
        df["rank"] = pd.to_numeric(df.get("rank"), errors="coerce")
        df["source_batch_id"] = "cmc_supply_v1"
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out[out["symbol"].isin(tokens)].rename(columns={"symbol": "token"})
    out = out.drop_duplicates(["token", "date_utc"], keep="last").sort_values("date_utc")
    return out[[c for c in SUPPLY_COLS if c in out.columns]]


def normalize_flows() -> pd.DataFrame:
    """Ercin 稳定币流向 -> stablecoin_exchange_flows。"""
    frames = []
    for p in _raw_paths("ercin", "stablecoin_flows", "stablecoin_"):
        name = os.path.basename(p).split(".")[0]
        if name.endswith("_v1"):
            name = name[:-3]
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        arr = d.get("data", []) if isinstance(d, dict) else d
        df = pd.DataFrame(arr)
        if df.empty:
            continue
        df["venue_id"] = "ercin"
        df["date_utc"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="ms", utc=True)
        df["value_usd"] = pd.to_numeric(df["value"], errors="coerce")
        df["metric"] = name.replace("stablecoin_", "")
        df["source_batch_id"] = f"{name}_v1"
        frames.append(df[["venue_id", "date_utc", "metric", "value_usd",
                          "source_batch_id"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["metric", "date_utc"], keep="last").sort_values("date_utc")
    return out


def normalize_peg(tokens=("USDC", "DAI")) -> pd.DataFrame:
    """Binance USDCUSDT/DAIUSDT 1h K线 -> stablecoin_peg (peg_deviation = price - 1)。"""
    frames = []
    for tok in tokens:
        for p in _raw_paths("binance", "stablecoin_peg_klines", f"{tok}USDT_peg"):
            df = pd.read_csv(p)
            df["venue_id"] = "binance"
            df["token"] = tok
            df["time_utc"] = pd.to_datetime(df["Open Time"], utc=True)
            df["price"] = pd.to_numeric(df["Close"], errors="coerce")
            df["peg_deviation"] = df["price"] - 1.0
            df["data_available_at"] = df["time_utc"] + pd.Timedelta(hours=1)
            df["source_batch_id"] = f"{tok}USDT_peg_v1"
            frames.append(df[["venue_id", "token", "time_utc", "price",
                              "peg_deviation", "data_available_at",
                              "source_batch_id"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(["token", "time_utc"], keep="first").sort_values("time_utc")
    return out


def write_stablecoin_parquet(df: pd.DataFrame, dataset: str, venue: str,
                             time_col: str) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = os.path.join(L1_DIR, dataset, venue)
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    df["date"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    return root
