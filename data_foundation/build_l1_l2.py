# -*- coding: utf-8 -*-
"""
build_l1_l2.py — 从 L0 raw (bybit/bitget funding CSV + bybit OI CSV + mark 缓存)
构建 L1 parquet / L2 certified / manifest。
数据集:
  derivatives_funding   venue=bybit|bitget, instrument={BASE}-USDT (每 instrument 一个 certified 文件)
  derivatives_oi_cross  venue=bybit, instrument="all" (venue 级单文件)
用法: python build_l1_l2.py
"""
import os
import sys
import json
import csv
from datetime import datetime, timezone

import pandas as pd

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation.config import MVP_ASSETS, CERTIFIED_DIR
from data_foundation.l0 import list_raw_batches
from data_foundation.l1_onchain import write_onchain_parquet
from data_foundation.l2 import (certify_derivatives, write_certified_derivatives,
                                build_dataset_manifest)
from data_foundation import finalize

CACHE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\_scratch_subA\cache"

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def _now():
    return pd.Timestamp.now(tz="UTC")

def load_raw_csv(venue, dataset, batch_id):
    """返回 L0 raw CSV 的 DataFrame (原始字符串列)。"""
    for meta in list_raw_batches(venue, dataset):
        if meta.get("batch_id") == batch_id:
            src = meta.get("source_path")
            if src and os.path.exists(src):
                return pd.read_csv(src)
    return None

# ---------------------------------------------------------------------------
# 1) funding L1+L2
# ---------------------------------------------------------------------------
def build_funding(venue):
    frames = []
    for a in MVP_ASSETS:
        sym = f"{a}USDT"
        df = load_raw_csv(venue, "derivatives_funding", f"{sym}_v1")
        if df is None or df.empty:
            log(f"  [warn] {venue} {sym}: 无 raw, 跳过")
            continue
        df = df.rename(columns={"funding_time": "funding_time_utc"})
        df["funding_time_utc"] = pd.to_datetime(df["funding_time_utc"], utc=True, errors="coerce")
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
        df["mark_price_at_funding"] = pd.to_numeric(df.get("mark_price"), errors="coerce")
        df["venue_id"] = venue
        df["instrument_id"] = f"{a}-USDT"
        df["symbol"] = sym
        df["data_available_at"] = df["funding_time_utc"]
        df["source_batch_id"] = f"{venue}_{sym}_v1"
        df = (df.dropna(subset=["funding_time_utc"])
                .drop_duplicates(subset=["instrument_id", "funding_time_utc"], keep="first")
                .sort_values("funding_time_utc"))
        frames.append(df[[c for c in [
            "venue_id", "instrument_id", "symbol", "funding_time_utc",
            "funding_rate", "mark_price_at_funding", "data_available_at",
            "source_batch_id"] if c in df.columns]])
    if not frames:
        log(f"  {venue}: 无 funding 数据")
        return None
    all_df = pd.concat(frames, ignore_index=True).sort_values(
        ["instrument_id", "funding_time_utc"]).reset_index(drop=True)
    l1_root = write_onchain_parquet(all_df, "derivatives_funding", venue, "funding_time_utc")
    log(f"  {venue} funding L1 -> {l1_root} ({len(all_df)} 行)")
    # L2 per instrument
    for inst, g in all_df.groupby("instrument_id"):
        g = g.sort_values("funding_time_utc").reset_index(drop=True)
        cert = certify_derivatives(g, "funding_time_utc",
                                   core_numeric_cols=["funding_rate"],
                                   key_cols=["funding_time_utc"])
        root = write_certified_derivatives(cert, "derivatives_funding", venue,
                                           inst, "funding_time_utc")
        log(f"    L2 {venue}/{inst}: {len(cert)} 行 ({cert['funding_time_utc'].min()} -> "
            f"{cert['funding_time_utc'].max()})")
    return all_df

# ---------------------------------------------------------------------------
# 2) OI cross L1+L2 (bybit; usd = contracts x mark_close)
# ---------------------------------------------------------------------------
def build_oi_cross(venue):
    frames = []
    for a in MVP_ASSETS:
        sym = f"{a}USDT"
        df = load_raw_csv(venue, "derivatives_oi_cross", f"{sym}_oi_v1")
        if df is None or df.empty:
            log(f"  [warn] {venue} {sym}: 无 OI raw, 跳过")
            continue
        df["timestamp_utc"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["open_interest_contracts"] = pd.to_numeric(df["open_interest"], errors="coerce")
        df = df.dropna(subset=["timestamp_utc", "open_interest_contracts"])
        df["ts_ms"] = (df["timestamp_utc"].astype("int64") // 10**6).astype("int64")
        # mark close join
        mk_path = os.path.join(CACHE, f"mark_{sym}.csv")
        close_map = {}
        if os.path.exists(mk_path):
            with open(mk_path, encoding="utf-8") as f:
                rd = csv.reader(f)
                next(rd, None)
                for r in rd:
                    if r:
                        close_map[int(r[0])] = float(r[1])
        df["_close"] = df["ts_ms"].map(close_map)
        df["open_interest_usd"] = df["open_interest_contracts"] * df["_close"]
        df["venue_id"] = venue
        df["asset"] = a
        df["data_available_at"] = df["timestamp_utc"]
        df["source_batch_id"] = f"{venue}_{sym}_oi_v1"
        df = (df.drop_duplicates(subset=["asset", "timestamp_utc"], keep="last")
                .sort_values("timestamp_utc"))
        frames.append(df[[c for c in [
            "venue_id", "asset", "timestamp_utc", "open_interest_contracts",
            "open_interest_usd", "data_available_at", "source_batch_id"] if c in df.columns]])
        usd_fill = df["open_interest_usd"].notna().mean()
        log(f"  {venue} {a}: {len(df)} 行 OI, usd 填充率 {usd_fill:.1%}")
    if not frames:
        log(f"  {venue}: 无 OI 数据")
        return None
    all_df = pd.concat(frames, ignore_index=True).sort_values(
        ["asset", "timestamp_utc"]).reset_index(drop=True)
    l1_root = write_onchain_parquet(all_df, "derivatives_oi_cross", venue, "timestamp_utc")
    log(f"  {venue} oi_cross L1 -> {l1_root} ({len(all_df)} 行)")
    # 核心数值只查 open_interest_contracts; open_interest_usd 是衍生折算列
    # (早期 mark kline 历史不足时为 NaN, 属预期稀疏, 不标记 suspect)
    core = ["open_interest_contracts"]
    cert = certify_derivatives(all_df, "timestamp_utc", core_numeric_cols=core,
                               key_cols=["asset", "timestamp_utc"])
    root = write_certified_derivatives(cert, "derivatives_oi_cross", venue,
                                       "all", "timestamp_utc")
    usd_fill = all_df["open_interest_usd"].notna().mean()
    log(f"  {venue} oi_cross L2 -> {root} ({len(cert)} 行, "
        f"suspect={int(cert['is_suspect'].sum())}, usd 填充率 {usd_fill:.1%})")
    return all_df

# ---------------------------------------------------------------------------
# 3) manifest
# ---------------------------------------------------------------------------
def merge_manifest(dataset, new_batches, agg_rules, note_extra=""):
    existing = {}
    mf = os.path.join(CERTIFIED_DIR, dataset, "manifest.json")
    if os.path.exists(mf):
        with open(mf, encoding="utf-8") as f:
            existing = json.load(f)
    stats = finalize.scan_dataset(os.path.join(CERTIFIED_DIR, dataset))
    if stats is None:
        log(f"  {dataset}: 无 certified 数据, 跳过 manifest")
        return None
    src_batches = list(existing.get("source_batches") or [])
    for b in new_batches:
        if b not in src_batches:
            src_batches.append(b)
    agg = existing.get("aggregation_rules") or agg_rules
    if note_extra:
        if isinstance(agg, dict):
            agg = dict(agg)
            agg["note"] = (agg.get("note", "") + " | " + note_extra).strip(" |")
        else:
            agg = {"note": note_extra}
    man = build_dataset_manifest(dataset, venue_id="all", market_type="",
                                 instrument="", interval="", stats=stats,
                                 source_batches=src_batches, aggregation_rules=agg)
    log(f"  {dataset} manifest: rows={stats['row_count']} "
        f"{str(stats['coverage_start'])[:10]} ~ {str(stats['coverage_end'])[:10]} "
        f"batches={len(src_batches)}")
    return man

# ---------------------------------------------------------------------------
def main():
    funding_batches = []
    for venue in ("bybit", "bitget"):
        df = build_funding(venue)
        if df is not None:
            funding_batches += sorted(df["source_batch_id"].unique().tolist())
    build_oi_cross("bybit")

    merge_manifest("derivatives_funding", funding_batches,
                   {"interval": "8h",
                    "note": "multi-venue funding history: binance/okx (既有) + bybit/bitget (本轮)"},
                   note_extra="bybit: /v5/market/funding/history endTime 游标全历史; "
                              "bitget: v2 history-fund-rate 仅最近 ~100 条 (~33 天, 公开 API 上限)")
    oi_batches = [f"bybit_{s}_oi_v1" for s in [f"{a}USDT" for a in MVP_ASSETS]]
    merge_manifest("derivatives_oi_cross", oi_batches,
                   {"interval": "1h",
                    "note": "venue 级 OI 历史: bybit (cursor 全量); "
                            "open_interest_usd=contracts x mark_close(1h) 折算, "
                            "早期 (mark kline 历史不足) 为 NaN"})
    log("done")

if __name__ == "__main__":
    main()
