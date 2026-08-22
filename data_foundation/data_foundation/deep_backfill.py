# -*- coding: utf-8 -*-
"""
deep_backfill.py — 深回填编排 (子代理 B)
========================================
1. Coinbase 现货 1h 深回填到上市日 (BTC/ETH/SOL/XRP):
   L0: ingest_coinbase.ingest_coinbase_deep -> raw/coinbase/spot_klines_1h
       batch {SYM}_deep_v2_{year} (按年份分段, 不动 v1/每日批次)
   L1: 重建 market_candle_spot_1h/coinbase (全部批次合并去重) + 1d/1w 派生
   L2: certify + build_dataset_manifest (market_candle_spot_1h/1d/1w)
2. Deribit DVOL 多年回填 (BTC/ETH):
   L0: ingest_deribit.ingest_deribit_dvol_deep -> batch dvol_{cur}_deep_v2
   L1: 重建 dvol_15m (v1 + deep 全部批次, 语义: dvol=close/100, interval=1H)
   L2: certify + build_dataset_manifest (dvol_15m)

阶段 (--stage):
  cb-l0    仅 Coinbase L0 深回填
  dvol-l0  仅 Deribit DVOL L0 深回填
  rebuild  L1 重建 + L2 认证 + manifest (依赖 L0 批次已就绪)
  all     依次执行全部
不调用 finalize_all。
用法:
  python -m data_foundation.deep_backfill --stage all
  python deep_backfill.py --stage rebuild
"""
from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import pandas as pd
import pyarrow.parquet as pq

from data_foundation.config import L1_DIR, RAW_DIR
from data_foundation.l0 import list_raw_batches
from data_foundation.l1 import CANDLE_COLS, derive_aggregates, write_parquet
from data_foundation.l2 import (build_dataset_manifest, certify_candles,
                                certify_derivatives, write_certified,
                                write_certified_derivatives)

CB_ASSETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD",
             "XRP": "XRP-USD"}


# ---------------------------------------------------------------------------
# L0
# ---------------------------------------------------------------------------
def stage_cb_l0(symbols=("BTC", "ETH", "SOL", "XRP")):
    from data_foundation.ingest_coinbase import ingest_coinbase_deep
    print("== Coinbase L0 深回填 (到上市日) ==", flush=True)
    ingest_coinbase_deep(symbols=symbols)


def stage_dvol_l0(days=2500, currencies=("BTC", "ETH")):
    from data_foundation.ingest_deribit import ingest_deribit_dvol_deep
    print(f"== Deribit DVOL L0 深回填 (days={days}) ==", flush=True)
    ingest_deribit_dvol_deep(days=days, currencies=currencies)


# ---------------------------------------------------------------------------
# Coinbase L1/L2
# ---------------------------------------------------------------------------
def _cb_raw_batches(product: str):
    """该产品全部 L0 批次 (文件路径, meta) 对, 按 ingest 排序。"""
    out = []
    for meta in list_raw_batches("coinbase", "spot_klines_1h"):
        if meta.get("source", {}).get("product") != product:
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, "coinbase", "spot_klines_1h",
                         f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append((os.path.join(d, f), meta))
    return out


def normalize_cb_candles(product: str) -> pd.DataFrame:
    """Coinbase [time,low,high,open,close,volume] -> market_candle (全部批次)。

    与 l1_coinbase.normalize_coinbase_candles 同构, 差异: source_batch_id
    逐批次取自 meta.batch_id (深回填批次各自可追溯)。
    """
    frames = []
    for p, meta in _cb_raw_batches(product):
        df = pd.read_csv(p)
        df["_bid"] = meta["batch_id"]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={"time": "open_time_utc", "low": "low",
                            "high": "high", "open": "open",
                            "close": "close", "volume": "volume_base"})
    df["open_time_utc"] = pd.to_datetime(pd.to_numeric(df["open_time_utc"]),
                                         unit="s", utc=True)
    for c in ["open", "high", "low", "close", "volume_base"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["close_time_utc"] = df["open_time_utc"] + pd.Timedelta(hours=1) \
        - pd.Timedelta(seconds=1)
    df["venue_id"] = "coinbase"
    df["instrument_id"] = product
    df["symbol"] = product
    df["market_type"] = "spot"
    df["bar_interval"] = "1h"
    df["volume_quote"] = float("nan")
    df["volume_contracts"] = float("nan")
    df["trade_count"] = 0
    df["taker_buy_volume_base"] = float("nan")
    df["taker_buy_volume_quote"] = float("nan")
    df["is_closed"] = True
    df["is_gap"] = False
    df["is_suspect"] = False
    df["quality_reason"] = ""
    df["data_available_at"] = df["close_time_utc"]
    df["source_batch_id"] = df.pop("_bid")
    df = df.drop_duplicates("open_time_utc", keep="first") \
        .sort_values("open_time_utc").reset_index(drop=True)
    cols = [c for c in CANDLE_COLS if c in df.columns]
    return df[cols]


def rebuild_coinbase(symbols=("BTC", "ETH", "SOL", "XRP")):
    print("== Coinbase L1 重建 (1h + 1d/1w 派生) ==", flush=True)
    for a in symbols:
        product = CB_ASSETS[a]
        df = normalize_cb_candles(product)
        if df.empty:
            print(f"  [warn] coinbase {product}: L0 批次为空, 跳过", flush=True)
            continue
        write_parquet(df, "market_candle_spot_1h", "coinbase", "spot",
                      product, "1h")
        for iv in ["1d", "1w"]:
            agg = derive_aggregates(df, iv)
            write_parquet(agg, f"market_candle_spot_{iv}", "coinbase", "spot",
                          product, iv)
        print(f"  coinbase {product}: 1h {len(df)} 行 "
              f"({df['open_time_utc'].min()} ~ {df['open_time_utc'].max()})",
              flush=True)

    print("-- Coinbase L2 认证 --", flush=True)
    accum = {}

    def acc(ds, s):
        x = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                                  "gap_count": 0, "suspect_count": 0,
                                  "coverage_start": None, "coverage_end": None})
        x["row_count"] += s["row_count"]
        x["duplicate_count"] += s["duplicate_count"]
        x["gap_count"] += s["gap_count"]
        x["suspect_count"] += s["suspect_count"]
        if x["coverage_start"] is None or s["coverage_start"] < x["coverage_start"]:
            x["coverage_start"] = s["coverage_start"]
        if x["coverage_end"] is None or s["coverage_end"] > x["coverage_end"]:
            x["coverage_end"] = s["coverage_end"]

    src_batches = []
    for a in symbols:
        product = CB_ASSETS[a]
        src_batches += [meta["batch_id"]
                        for _, meta in _cb_raw_batches(product)]
        for iv in ["1h", "1d", "1w"]:
            ds = f"market_candle_spot_{iv}"
            root = os.path.join(L1_DIR, ds, "coinbase", "spot", product,
                                f"interval={iv}")
            if not os.path.isdir(root):
                continue
            df = pq.read_table(root).to_pandas()
            if df.empty:
                continue
            df = certify_candles(df)
            _, stats = write_certified(df, ds, "coinbase", "spot", product, iv)
            acc(ds, stats)
            print(f"  certified coinbase {product} {iv}: {stats['row_count']} 行",
                  flush=True)
    rules = {
        "note": "Coinbase REST candles 深回填至上市日 (deep_v2 按年份分段批次"
                " + v1 年度批次 + run_daily 每日批次); 1d/1w 由 1h resample 派生"
                " (open=first/high=max/low=min/close=last, 量求和); "
                "无 confirm 字段(全为已收盘 bar); 停牌期缺口标记 is_gap",
        "method": "resample from 1h (1d/1w)", "version": "1.0"}
    for ds, s in accum.items():
        build_dataset_manifest(ds, "coinbase", "spot", "*", "*", s,
                               src_batches, rules)
        print(f"  manifest {ds}: {s['row_count']} 行, coverage "
              f"{s['coverage_start']} ~ {s['coverage_end']}", flush=True)


# ---------------------------------------------------------------------------
# Deribit DVOL L1/L2
# ---------------------------------------------------------------------------
def rebuild_dvol():
    from data_foundation.derivatives import write_derivatives_parquet
    from data_foundation.l1_deribit import normalize_dvol
    print("== Deribit DVOL L1 重建 ==", flush=True)
    dvol = normalize_dvol()
    if dvol.empty:
        raise RuntimeError("dvol L1 为空: 检查 L0 批次")
    write_derivatives_parquet(dvol, "dvol_15m", "deribit", "all",
                              "timestamp_utc")
    print(f"  dvol_15m: {len(dvol)} 行, {dvol['currency'].nunique()} 币, "
          f"{dvol['timestamp_utc'].min()} ~ {dvol['timestamp_utc'].max()}",
          flush=True)

    print("-- Deribit DVOL L2 认证 --", flush=True)
    dvol_c = certify_derivatives(dvol, "timestamp_utc",
                                 core_numeric_cols=["dvol"],
                                 key_cols=["currency", "timestamp_utc"])
    write_certified_derivatives(dvol_c, "dvol_15m", "deribit", "all",
                                "timestamp_utc")
    stats = {"row_count": int(len(dvol_c)),
             "duplicate_count": int(dvol_c[["currency", "timestamp_utc"]]
                                    .duplicated().sum()),
             "gap_count": 0,
             "suspect_count": int(dvol_c["is_suspect"].sum()),
             "coverage_start": str(dvol_c["timestamp_utc"].min()),
             "coverage_end": str(dvol_c["timestamp_utc"].max())}
    src = []
    for meta in list_raw_batches("deribit", "dvol_15m"):
        if meta["batch_id"].startswith("dvol_"):
            src.append(meta["batch_id"])
    rules = {
        "note": "DVOL 波动率指数 (BTC/ETH); 官方 get_volatility_index_data "
                "resolution 枚举仅 [1,60,3600,43200,'1D'] 秒 (无 15M), "
                "当前取 3600s=1H; dvol=close/100 (百分数→小数); "
                "深回填: days=2500 试探至 DVOL 最早可用 (~2021-03), "
                "deep_v2 批次 + v1 批次合并去重 (currency, timestamp_utc)"}
    build_dataset_manifest("dvol_15m", "deribit", "volatility", "all", "*",
                           stats, src, rules)
    print(f"  manifest dvol_15m: {stats['row_count']} 行, suspect="
          f"{stats['suspect_count']}, coverage {stats['coverage_start']} ~ "
          f"{stats['coverage_end']}", flush=True)


def verify_all(symbols=("BTC", "ETH", "SOL", "XRP")):
    print("-- 验证 --", flush=True)
    for a in symbols:
        product = CB_ASSETS[a]
        root = os.path.join(L1_DIR, "market_candle_spot_1h", "coinbase",
                            "spot", product, "interval=1h")
        if not os.path.isdir(root):
            print(f"  [warn] {product} L1 缺失", flush=True)
            continue
        df = pq.read_table(root).to_pandas()
        print(f"  [verify] coinbase {product}: {len(df)} 行, "
              f"{df['open_time_utc'].min()} ~ {df['open_time_utc'].max()}",
              flush=True)
    dvol = pq.read_table(os.path.join(L1_DIR, "dvol_15m", "deribit",
                                      "all", "data.parquet")).to_pandas()
    for cur, g in dvol.groupby("currency"):
        print(f"  [verify] dvol {cur}: {len(g)} 行, "
              f"{g['timestamp_utc'].min()} ~ {g['timestamp_utc'].max()}",
              flush=True)


def main():
    ap = argparse.ArgumentParser(description="深回填编排 (子代理 B)")
    ap.add_argument("--stage", default="all",
                    choices=["all", "cb-l0", "dvol-l0", "rebuild"])
    ap.add_argument("--symbols", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--days", type=int, default=2500,
                    help="DVOL 深回填天数 (默认 2500 试探至最早)")
    args = ap.parse_args()
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",")
                    if s.strip())
    if args.stage in ("all", "cb-l0"):
        stage_cb_l0(symbols)
    if args.stage in ("all", "dvol-l0"):
        stage_dvol_l0(days=args.days)
    if args.stage in ("all", "rebuild"):
        rebuild_coinbase(symbols)
        rebuild_dvol()
    verify_all(symbols)
    print("深回填完成", flush=True)


if __name__ == "__main__":
    main()
