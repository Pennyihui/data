# -*- coding: utf-8 -*-
"""
ingest_long_history.py — 四类免费长史回填 (L0 -> L1 -> L2)   [子代理 F]
========================================================================
1. btc_network_daily (blockchain.info charts, 13 slugs, sampled=false)
   * L0: raw/blockchain_info/btc_network_daily (batch btc_network_v1, json)
   * L1: l1/btc_network_daily/blockchain_info/data.parquet (长格式)
   * L2: l2/certified/btc_network_daily/blockchain_info/all/data.parquet
   聚合约定: 日频序列直接取点; market-cap 取每日最后值(收盘口径);
   mempool-size 取每日均值; 丢弃抓取当日(不完整)的日值。
2. macro_daily (Yahoo chart period1/period2, 延长至 2005-01-01)
   * L0: raw/yahoo/macro_daily/macro_{SERIES}_max_v2.json
   * 与现有 l1/macro_daily/yahoo/data.parquet 合并去重 [series,date_utc] keep last
3. cm_asset_daily (Coin Metrics 社区 GitHub 归档 csv, usdt/usdc/dai/eth/btc)
   * L0: raw/coinmetrics/cm_asset_daily/{asset}_v1.csv.gz
   * L1: l1/cm_asset_daily/coinmetrics/data.parquet (长格式 melt)

用法:
  python ingest_long_history.py btc     # 仅 BTC 网络日频
  python ingest_long_history.py macro   # 仅宏观延长
  python ingest_long_history.py cm      # 仅 Coin Metrics
  python ingest_long_history.py all     # 全部
  python ingest_long_history.py build   # 由已有 raw 重建 L1/L2 (幂等)
  python ingest_long_history.py verify  # 打印覆盖验证
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from data_foundation import netpath
from data_foundation.config import RAW_DIR
from data_foundation.l0 import list_raw_batches, write_raw_file
from data_foundation.l1_onchain import write_onchain_parquet
from data_foundation.l2 import (build_dataset_manifest, certify_derivatives,
                                write_certified_derivatives)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
TIMESTAMP_UNIT = "us"
BCI_BASE = "https://api.blockchain.info/charts"
BCI_SLUGS = ["hash-rate", "difficulty", "miners-revenue", "n-unique-addresses",
             "n-transactions", "n-transactions-total", "mempool-size",
             "median-confirmation-time", "market-cap",
             "estimated-transaction-volume-usd", "cost-per-transaction",
             "transaction-fees", "estimated-transaction-volume"]
# 日内高频 slug -> 日聚合规则 (其余 slug 已为日频单点, 用 last 兜底去重)
BCI_AGG = {"market-cap": "last", "mempool-size": "mean"}

YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
# 与 ingest_fng_macro 一致: US10Y=^TNX 原始收盘即百分比收益率 (系数 1.0)
YAHOO_MAP = {
    "DXY": ("DX-Y.NYB", 1.0),
    "SPX": ("^GSPC", 1.0),
    "NDX": ("^NDX", 1.0),
    "VIX": ("^VIX", 1.0),
    "GOLD": ("GC=F", 1.0),
    "US10Y": ("^TNX", 1.0),
}
MACRO_PERIOD1 = 1104537600          # 2005-01-01 00:00:00 UTC

CM_BASE = "https://raw.githubusercontent.com/coinmetrics/data/master/csv"
CM_ASSETS = ["usdt", "usdc", "dai", "eth", "btc"]

SCHEMA_COLS = ["data_available_at", "source_batch_id"]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_norm() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").normalize()


def _save_tmp(name: str, content: str | bytes) -> str:
    tmp = os.path.join(RAW_DIR, "_tmp", name)
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    with open(tmp, mode, encoding=None if isinstance(content, bytes) else "utf-8") as f:
        f.write(content)
    return tmp


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def _raw_files(venue: str, dataset: str, batch_prefix: str) -> list[tuple[dict, str]]:
    """返回 (meta, 文件路径) 列表, batch_id 以 batch_prefix 开头。"""
    out = []
    for m in list_raw_batches(venue, dataset):
        if not m["batch_id"].startswith(batch_prefix):
            continue
        ingest = m["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.startswith(m["batch_id"]) and not f.endswith(".meta.json"):
                out.append((m, os.path.join(d, f)))
    return out


def _read_json(path: str):
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _all_batch_ids(venue: str, dataset: str) -> list[str]:
    return sorted({m["batch_id"] for m in list_raw_batches(venue, dataset)})


# ===========================================================================
# 1. BTC 网络日频 (blockchain.info)
# ===========================================================================
def ingest_btc_network() -> str | None:
    """13 slug 全历史 (sampled=false) -> L0 raw/blockchain_info/btc_network_daily。"""
    bid = "btc_network_v1"
    if _already("blockchain_info", "btc_network_daily", bid):
        print(f"  [btc] 批次 {bid} 已存在, 跳过")
        return None
    charts: dict[str, dict] = {}
    fails = []
    for slug in BCI_SLUGS:
        try:
            j = netpath.fetch_json(f"{BCI_BASE}/{slug}",
                                   {"timespan": "all", "format": "json",
                                    "sampled": "false"},
                                   timeout=90, retries=8)
            if not (j.get("values")):
                raise RuntimeError("空 values")
            charts[slug] = j
            n = len(j["values"])
            print(f"  [btc] {slug}: {n} 点")
        except Exception as e:  # noqa: BLE001
            fails.append(f"{slug}:{str(e)[:60]}")
        time.sleep(0.3)
    if fails or len(charts) != len(BCI_SLUGS):
        print(f"  [btc] 部分失败 ({len(charts)}/{len(BCI_SLUGS)}), 不写批次: {fails}")
        return None
    payload = {"fetched_at": _now_iso(), "api": BCI_BASE,
               "params": {"timespan": "all", "format": "json", "sampled": "false"},
               "charts": charts}
    tmp = _save_tmp("btc_network_all.json", json.dumps(payload, ensure_ascii=False))
    dst = write_raw_file(
        tmp, "blockchain_info", "btc_network_daily", bid,
        source={"api": BCI_BASE, "params": {"timespan": "all", "format": "json",
                                            "sampled": "false"},
                "fetched_at": payload["fetched_at"],
                "slugs": BCI_SLUGS, "rows": sum(len(c["values"]) for c in charts.values())},
        timestamp_unit=TIMESTAMP_UNIT, ext="json")
    os.remove(tmp)
    print(f"  [btc] {bid}: 13 slugs -> {dst}")
    return dst


def normalize_btc_network() -> pd.DataFrame:
    frames = []
    for meta, p in _raw_files("blockchain_info", "btc_network_daily", "btc_network_v1"):
        j = _read_json(p)
        fetched = pd.to_datetime(j.get("fetched_at"), utc=True, errors="coerce")
        for slug, cj in (j.get("charts") or {}).items():
            vals = cj.get("values") or []
            if not vals:
                continue
            d = pd.DataFrame(vals)
            d = d.rename(columns={"x": "x", "y": "value"})
            d["date_utc"] = pd.to_datetime(d["x"], unit="s", utc=True).dt.normalize()
            d["value"] = pd.to_numeric(d["value"], errors="coerce")
            d = d.dropna(subset=["value"])
            if d.empty:
                continue
            rule = BCI_AGG.get(slug, "last")
            g = d.groupby("date_utc")["value"].agg(rule).reset_index()
            g["metric"] = slug
            frames.append(g)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # 丢弃抓取当日 (不完整) 的日值
    df = df[df["date_utc"] < _today_norm()]
    df = df.drop_duplicates(["metric", "date_utc"], keep="last")
    df = df.sort_values(["metric", "date_utc"]).reset_index(drop=True)
    # 兼容多批次: data_available_at/source_batch_id 从最后读到的批次取
    df["data_available_at"] = fetched
    df["source_batch_id"] = "btc_network_v1"
    df["value"] = df["value"].astype("float64")
    df["date_utc"] = df["date_utc"].astype("datetime64[us, UTC]")
    df["data_available_at"] = pd.to_datetime(df["data_available_at"], utc=True,
                                             errors="coerce").astype("datetime64[us, UTC]")
    return df[["metric", "date_utc", "value"] + SCHEMA_COLS]


def build_btc_network() -> dict:
    df = normalize_btc_network()
    if df.empty:
        print("  [btc] 无原始数据, 跳过")
        return {}
    write_onchain_parquet(df, "btc_network_daily", "blockchain_info", "date_utc")
    per = df.groupby("metric")["date_utc"].agg(["count", "min", "max"])
    print(f"  [btc] L1: {len(df)} 行, {df['metric'].nunique()} metrics "
          f"({df['date_utc'].min().date()} ~ {df['date_utc'].max().date()})")
    cdf = certify_derivatives(df, "date_utc", core_numeric_cols=["value"],
                              key_cols=["metric", "date_utc"])
    write_certified_derivatives(cdf, "btc_network_daily", "blockchain_info",
                                "all", "date_utc")
    stats = {"row_count": len(cdf),
             "duplicate_count": int(cdf[["metric", "date_utc"]].duplicated().sum()),
             "gap_count": 0, "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
    batches = _all_batch_ids("blockchain_info", "btc_network_daily")
    build_dataset_manifest(
        "btc_network_daily", "*", "*", "*", "*", stats, batches,
        {"note": "blockchain.info charts sampled=false; 日频序列直接取点, "
                 "market-cap 取每日最后值(收盘口径), mempool-size 取每日均值; "
                 "丢弃抓取当日不完整日值; 日频序列滞后约 6 天; 主键 (metric, date_utc)"})
    print(f"  [btc] L2: certified {len(cdf)} 行, manifest 已更新 "
          f"(source_batches={batches})")
    return stats


# ===========================================================================
# 2. 宏观日频延长 (Yahoo period1/period2)
# ===========================================================================
def ingest_macro_max() -> list[str]:
    """6 序列 2005-01-01 起全历史 -> raw/yahoo/macro_daily/macro_{SERIES}_max_v2.json。"""
    p2 = int(datetime.now(timezone.utc).timestamp())
    written = []
    for series, (sym, mult) in YAHOO_MAP.items():
        bid = f"macro_{series}_max_v2"
        if _already("yahoo", "macro_daily", bid):
            print(f"  [macro] 批次 {bid} 已存在, 跳过")
            continue
        try:
            j = netpath.fetch_json(
                f"{YAHOO_BASE}/{sym}",
                params={"period1": MACRO_PERIOD1, "period2": p2,
                        "interval": "1d", "events": "history"},
                timeout=60, retries=8)
            res = (j.get("chart") or {}).get("result") or []
            if not res:
                print(f"  [macro] {series} ({sym}): 空响应, 跳过")
                continue
            r = res[0]
            n = len(r.get("timestamp") or [])
            j["fetched_at"] = _now_iso()
            tmp = _save_tmp(f"macro_{series}_max_v2.json",
                            json.dumps(j, ensure_ascii=False))
            dst = write_raw_file(
                tmp, "yahoo", "macro_daily", bid,
                source={"api": f"{YAHOO_BASE}/{sym}",
                        "params": {"period1": MACRO_PERIOD1, "period2": p2,
                                   "interval": "1d", "events": "history"},
                        "fetched_at": j["fetched_at"], "rows": n,
                        "series": series, "symbol": sym,
                        "note": "long-history extension"},
                timestamp_unit=TIMESTAMP_UNIT, ext="json")
            os.remove(tmp)
            written.append(dst)
            print(f"  [macro] {bid}: {n} 根 -> {dst}")
        except Exception as e:  # noqa: BLE001
            print(f"  [macro] {series} ({sym}) 失败: {str(e)[:100]}")
        time.sleep(0.3)
    return written


def normalize_macro_v2() -> pd.DataFrame:
    """v2 批次 (chart 结构, period1/period2) -> macro_daily 标准表。"""
    frames = []
    for meta, p in _raw_files("yahoo", "macro_daily", "macro_"):
        if not meta["batch_id"].endswith("_max_v2"):
            continue
        j = _read_json(p)
        fetched = pd.to_datetime(j.get("fetched_at"), utc=True, errors="coerce")
        res = (j.get("chart") or {}).get("result") or []
        if not res:
            continue
        r = res[0]
        meta0 = r.get("meta") or {}
        sym = meta0.get("symbol", "")
        series = next((s for s, (y, _) in YAHOO_MAP.items() if y == sym), None)
        if series is None:
            print(f"  [macro] 无法识别 symbol {sym}, 跳过 {p}")
            continue
        mult = YAHOO_MAP[series][1]
        ts = r.get("timestamp") or []
        close = (r.get("indicators") or {}).get("quote", [{}])[0].get("close") or []
        for t, c in zip(ts, close):
            if c is None:
                continue
            frames.append({
                "series": series,
                "date_utc": pd.to_datetime(int(t), unit="s", utc=True).normalize(),
                "close": float(c) * mult,
                "data_available_at": fetched,
                "source_batch_id": f"macro_{series}_max_v2",
            })
    if not frames:
        return pd.DataFrame()
    df = pd.DataFrame(frames)
    df["data_available_at"] = pd.to_datetime(df["data_available_at"], utc=True,
                                             errors="coerce")
    df = df.drop_duplicates(["series", "date_utc"], keep="last")
    df = df.sort_values(["series", "date_utc"]).reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["date_utc"] = df["date_utc"].astype("datetime64[us, UTC]")
    df["data_available_at"] = df["data_available_at"].astype("datetime64[us, UTC]")
    return df[["series", "date_utc", "close"] + SCHEMA_COLS]


def build_macro() -> dict:
    """v2 全史 + 现有 L1 合并去重 [series,date_utc] keep last -> L1/L2。"""
    from data_foundation.config import L1_DIR
    new = normalize_macro_v2()
    if new.empty:
        print("  [macro] 无 v2 原始数据, 跳过")
        return {}
    l1_path = os.path.join(L1_DIR, "macro_daily", "yahoo", "data.parquet")
    old = pd.DataFrame()
    if os.path.exists(l1_path):
        old = pd.read_parquet(l1_path, columns=["series", "date_utc", "close",
                                                "data_available_at",
                                                "source_batch_id"])
    before_rows = len(old)
    before_start = str(old["date_utc"].min()) if not old.empty else "N/A"
    merged = pd.concat([old, new], ignore_index=True)
    merged = merged.drop_duplicates(["series", "date_utc"], keep="last")
    merged = merged.sort_values(["series", "date_utc"]).reset_index(drop=True)
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["date_utc"] = pd.to_datetime(merged["date_utc"], utc=True).astype(
        "datetime64[us, UTC]")
    merged["data_available_at"] = pd.to_datetime(
        merged["data_available_at"], utc=True, errors="coerce").astype(
        "datetime64[us, UTC]")
    write_onchain_parquet(merged, "macro_daily", "yahoo", "date_utc")
    print(f"  [macro] L1: {before_rows} -> {len(merged)} 行, "
          f"起点 {before_start} -> {merged['date_utc'].min()}")
    cdf = certify_derivatives(merged, "date_utc", core_numeric_cols=["close"],
                              key_cols=["series", "date_utc"])
    write_certified_derivatives(cdf, "macro_daily", "yahoo", "all", "date_utc")
    stats = {"row_count": len(cdf),
             "duplicate_count": int(cdf[["series", "date_utc"]].duplicated().sum()),
             "gap_count": 0, "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
    batches = _all_batch_ids("yahoo", "macro_daily")
    build_dataset_manifest(
        "macro_daily", "*", "*", "*", "*", stats, batches,
        {"note": "Yahoo Finance chart 日频收盘; period1=2005-01-01 起全史 "
                 "(macro_*_max_v2) 与近 5 年 (macro_*_v1) 合并去重 [series,date_utc] "
                 "keep last; 仅交易日; US10Y=^TNX 原始值即百分比收益率 (系数 1.0, 未÷10); "
                 "主键 (series, date_utc)"})
    print(f"  [macro] L2: certified {len(cdf)} 行, manifest 已更新 "
          f"(source_batches={batches})")
    return stats


# ===========================================================================
# 3. Coin Metrics 社区归档 (cm_asset_daily)
# ===========================================================================
def ingest_cm() -> list[str]:
    """5 资产社区 CSV -> raw/coinmetrics/cm_asset_daily/{asset}_v1.csv.gz。"""
    written = []
    for asset in CM_ASSETS:
        bid = f"{asset}_v1"
        if _already("coinmetrics", "cm_asset_daily", bid):
            print(f"  [cm] 批次 {bid} 已存在, 跳过")
            continue
        try:
            url = f"{CM_BASE}/{asset}.csv"
            txt = netpath.fetch_text(url, timeout=120, retries=6)
            n_rows = txt.count("\n")
            fetched = _now_iso()
            tmp_csv = _save_tmp(f"{asset}_v1.csv", txt)
            gz_tmp = os.path.join(RAW_DIR, "_tmp", f"{asset}_v1.csv.gz")
            with open(tmp_csv, "rb") as fin, gzip.open(gz_tmp, "wb") as fout:
                shutil.copyfileobj(fin, fout)
            dst = write_raw_file(
                gz_tmp, "coinmetrics", "cm_asset_daily", bid,
                source={"api": url, "fetched_at": fetched, "rows": n_rows,
                        "asset": asset, "note": "community archive csv (raw)"},
                timestamp_unit=TIMESTAMP_UNIT, ext="csv.gz")
            os.remove(tmp_csv)
            os.remove(gz_tmp)
            written.append(dst)
            print(f"  [cm] {bid}: {n_rows} 行 -> {dst}")
        except Exception as e:  # noqa: BLE001
            print(f"  [cm] {asset} 失败: {str(e)[:100]}")
        time.sleep(0.3)
    return written


def normalize_cm() -> pd.DataFrame:
    """全部 {asset}_v1.csv.gz -> cm_asset_daily 长格式 (跳过全空列, 数值转 float64)。"""
    frames = []
    for meta, p in _raw_files("coinmetrics", "cm_asset_daily", ""):
        asset = meta["batch_id"].removesuffix("_v1")
        fetched = pd.to_datetime((meta.get("source") or {}).get("fetched_at"),
                                 utc=True, errors="coerce")
        raw = pd.read_csv(p, compression="gzip")
        if "time" not in raw.columns:
            print(f"  [cm] {p} 缺 time 列, 跳过")
            continue
        keep = ["time"]
        for c in raw.columns:
            if c == "time":
                continue
            num = pd.to_numeric(raw[c], errors="coerce")
            if num.notna().any():
                keep.append(c)
        wide = raw[keep].copy()
        long = wide.melt(id_vars="time", var_name="metric", value_name="value")
        long["value"] = pd.to_numeric(long["value"], errors="coerce")
        long = long.dropna(subset=["value"])
        long["date_utc"] = pd.to_datetime(long["time"], utc=True).dt.normalize()
        long["asset"] = asset
        long["data_available_at"] = fetched
        long["source_batch_id"] = f"{asset}_v1"
        frames.append(long[["asset", "metric", "date_utc", "value"] + SCHEMA_COLS])
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(["asset", "metric", "date_utc"], keep="last")
    df = df.sort_values(["asset", "metric", "date_utc"]).reset_index(drop=True)
    df["value"] = df["value"].astype("float64")
    df["date_utc"] = df["date_utc"].astype("datetime64[us, UTC]")
    df["data_available_at"] = pd.to_datetime(df["data_available_at"], utc=True,
                                             errors="coerce").astype(
        "datetime64[us, UTC]")
    return df


def build_cm() -> dict:
    df = normalize_cm()
    if df.empty:
        print("  [cm] 无原始数据, 跳过")
        return {}
    write_onchain_parquet(df, "cm_asset_daily", "coinmetrics", "date_utc")
    per = df.groupby("asset").agg(rows=("date_utc", "count"),
                                  start=("date_utc", "min"),
                                  end=("date_utc", "max"),
                                  metrics=("metric", "nunique"))
    print(f"  [cm] L1: {len(df)} 行, {df['asset'].nunique()} assets "
          f"({df['date_utc'].min().date()} ~ {df['date_utc'].max().date()})")
    cdf = certify_derivatives(df, "date_utc", core_numeric_cols=["value"],
                              key_cols=["asset", "metric", "date_utc"])
    write_certified_derivatives(cdf, "cm_asset_daily", "coinmetrics", "all",
                                "date_utc")
    stats = {"row_count": len(cdf),
             "duplicate_count": int(cdf[["asset", "metric", "date_utc"]]
                                    .duplicated().sum()),
             "gap_count": 0, "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
    batches = _all_batch_ids("coinmetrics", "cm_asset_daily")
    build_dataset_manifest(
        "cm_asset_daily", "*", "*", "*", "*", stats, batches,
        {"note": "Coin Metrics 社区 GitHub 归档 (master/csv), 滞后约 3 个月 "
                 "(至 2026-05-24); 近期数据由其他源覆盖; 长格式 melt 保留全部有值列; "
                 "主键 (asset, metric, date_utc)"})
    print(f"  [cm] L2: certified {len(cdf)} 行, manifest 已更新 "
          f"(source_batches={batches})")
    return stats


# ===========================================================================
# 验证
# ===========================================================================
def verify():
    from data_foundation.config import CERTIFIED_DIR, L1_DIR

    print("\n========== 验证 ==========")

    p = os.path.join(L1_DIR, "btc_network_daily", "blockchain_info", "data.parquet")
    if os.path.exists(p):
        df = pd.read_parquet(p)
        print(f"\n[btc_network_daily] L1 共 {len(df)} 行, "
              f"{df['date_utc'].min().date()} ~ {df['date_utc'].max().date()}")
        g = df.groupby("metric")["date_utc"].agg(["count", "min", "max"])
        for metric, row in g.iterrows():
            print(f"  {metric:<30} {int(row['count']):>6}  {row['min'].date()} -> {row['max'].date()}")
        cp = os.path.join(CERTIFIED_DIR, "btc_network_daily", "blockchain_info",
                          "all", "data.parquet")
        if os.path.exists(cp):
            print(f"  certified 行数: {len(pd.read_parquet(cp))}")
    else:
        print("\n[btc_network_daily] L1 不存在!")

    p = os.path.join(L1_DIR, "macro_daily", "yahoo", "data.parquet")
    if os.path.exists(p):
        df = pd.read_parquet(p)
        print(f"\n[macro_daily] L1 共 {len(df)} 行 (延长前 7535 行 / 起点 2021-08-23), "
              f"现 {df['date_utc'].min().date()} ~ {df['date_utc'].max().date()}")
        g = df.groupby("series")["date_utc"].agg(["count", "min", "max"])
        for series, row in g.iterrows():
            print(f"  {series:<6} {int(row['count']):>6}  {row['min'].date()} -> {row['max'].date()}")
        cp = os.path.join(CERTIFIED_DIR, "macro_daily", "yahoo", "all",
                          "data.parquet")
        if os.path.exists(cp):
            print(f"  certified 行数: {len(pd.read_parquet(cp))}")
    else:
        print("\n[macro_daily] L1 不存在!")

    p = os.path.join(L1_DIR, "cm_asset_daily", "coinmetrics", "data.parquet")
    if os.path.exists(p):
        df = pd.read_parquet(p)
        print(f"\n[cm_asset_daily] L1 共 {len(df)} 行, "
              f"{df['date_utc'].min().date()} ~ {df['date_utc'].max().date()}")
        for asset, g in df.groupby("asset"):
            mets = sorted(g["metric"].unique())
            print(f"  {asset}: {len(g)} 行, {g['date_utc'].min().date()} ~ "
                  f"{g['date_utc'].max().date()}, {len(mets)} 指标")
            print(f"    指标: {', '.join(mets)}")
        cp = os.path.join(CERTIFIED_DIR, "cm_asset_daily", "coinmetrics", "all",
                          "data.parquet")
        if os.path.exists(cp):
            print(f"  certified 行数: {len(pd.read_parquet(cp))}")
    else:
        print("\n[cm_asset_daily] L1 不存在!")

    print("\n========== 原始批次 ==========")
    for venue, ds in [("blockchain_info", "btc_network_daily"),
                      ("yahoo", "macro_daily"),
                      ("coinmetrics", "cm_asset_daily")]:
        bids = _all_batch_ids(venue, ds)
        print(f"  {venue}/{ds}: {bids}")


# ===========================================================================
# CLI
# ===========================================================================
def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "btc":
        ingest_btc_network()
        build_btc_network()
    elif mode == "macro":
        ingest_macro_max()
        build_macro()
    elif mode == "cm":
        ingest_cm()
        build_cm()
    elif mode == "all":
        ingest_btc_network()
        build_btc_network()
        ingest_macro_max()
        build_macro()
        ingest_cm()
        build_cm()
    elif mode == "build":
        build_btc_network()
        build_macro()
        build_cm()
    elif mode == "verify":
        verify()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
