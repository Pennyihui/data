# -*- coding: utf-8 -*-
"""
ingest_fng_macro.py — FNG 情绪 + 宏观日频序列 三层底座 (L0 -> L1 -> L2)
======================================================================
- sentiment_fng: alternative.me Fear & Greed Index, 2018-02 至今全历史
  * L0: raw/alternative_me/sentiment_fng (batch fng_v1 / fng_daily_YYYYMMDD)
  * L1: l1/sentiment_fng/fng/data.parquet
  * L2: l2/certified/sentiment_fng/fng/all/data.parquet + manifest
- macro_daily: Yahoo Finance chart API 近 5 年日频收盘
  * L0: raw/yahoo/macro_daily (batch macro_{SERIES}_v1 / macro_daily_YYYYMMDD)
  * L1: l1/macro_daily/yahoo/data.parquet
  * L2: l2/certified/macro_daily/yahoo/all/data.parquet + manifest
- 每日增量函数: ingest_fng_daily() / ingest_macro_daily() (幂等, 按日去重)

用法:
  python ingest_fng_macro.py backfill          # 全历史 FNG + 宏观 (首次)
  python ingest_fng_macro.py fng               # 仅 FNG 全历史
  python ingest_fng_macro.py macro             # 仅宏观全历史
  python ingest_fng_macro.py fng_daily         # FNG 每日增量
  python ingest_fng_macro.py macro_daily       # 宏观每日增量
  python ingest_fng_macro.py build             # 由已有 raw 重建 L1/L2 (幂等)
"""
from __future__ import annotations

import json
import os
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

FNG_URL = "https://api.alternative.me/fng/"
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
# 宏观序列 -> (Yahoo symbol, close 换算系数)
# 注: 任务书写 "US10Y→^TNX 值÷10 转百分比收益率", 但实测本环境 Yahoo ^TNX 原始
# 收盘即为百分比收益率 (如 4.738=4.738%), 与 data_new/additional/macro_daily.csv
# (us10y=4.641~4.988) 一致; 真实 Yahoo 的 ^TNX 才是收益率×10。故 US10Y 系数=1.0,
# 直接存百分比收益率 (偏离任务书字面 ÷10, 详见交付报告)。
MACRO_MAP = {
    "DXY": ("DX-Y.NYB", 1.0),
    "SPX": ("^GSPC", 1.0),
    "NDX": ("^NDX", 1.0),
    "VIX": ("^VIX", 1.0),
    "GOLD": ("GC=F", 1.0),
    "US10Y": ("^TNX", 1.0),
}
TIMESTAMP_UNIT = "us"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utcnow() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


# ---------------------------------------------------------------------------
# L0 原始层
# ---------------------------------------------------------------------------
def _save_tmp(name: str, content: str) -> str:
    tmp = os.path.join(RAW_DIR, "_tmp", name)
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    return tmp


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def _raw_file_paths(venue: str, dataset: str, batch_prefix: str) -> list[str]:
    """返回该 dataset 下 batch_id 以 batch_prefix 开头的全部原始文件路径。"""
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
                out.append(os.path.join(d, f))
    return out


def _read_json(path: str):
    if path.endswith(".gz"):
        import gzip
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _all_batch_ids(venue: str, dataset: str) -> list[str]:
    return sorted({m["batch_id"] for m in list_raw_batches(venue, dataset)})


# ===========================================================================
# 1. FNG 恐惧贪婪指数
# ===========================================================================
def fetch_fng(limit: int = 0) -> dict:
    """alternative.me FNG。limit=0 表示全历史。"""
    return netpath.fetch_json(FNG_URL, params={"limit": limit},
                              retries=8, timeout=30)


def ingest_fng_backfill() -> str | None:
    """FNG 全历史 -> L0 raw/alternative_me/sentiment_fng (batch fng_v1)。"""
    bid = "fng_v1"
    if _already("alternative_me", "sentiment_fng", bid):
        print(f"  [fng] 批次 {bid} 已存在, 跳过")
        return None
    j = fetch_fng(0)
    data = j.get("data") or []
    tmp = _save_tmp("fng_all.json", json.dumps(j, ensure_ascii=False))
    dst = write_raw_file(
        tmp, "alternative_me", "sentiment_fng", bid,
        source={"api": FNG_URL, "params": {"limit": 0},
                "fetched_at": _now_iso(), "rows": len(data)},
        timestamp_unit=TIMESTAMP_UNIT, ext="json")
    os.remove(tmp)
    print(f"  [fng] {bid}: {len(data)} 天 -> {dst}")
    return dst


def ingest_fng_daily() -> str | None:
    """FNG 每日增量: 拉最近 3 天, 只写 certified 里没有的日期 (batch fng_daily_YYYYMMDD)。"""
    from data_foundation.config import CERTIFIED_DIR
    have = set()
    cert_path = os.path.join(CERTIFIED_DIR, "sentiment_fng", "fng", "all",
                             "data.parquet")
    if os.path.exists(cert_path):
        try:
            have = set(pd.read_parquet(cert_path,
                                       columns=["date_utc"])["date_utc"]
                       .dt.strftime("%Y-%m-%d"))
        except Exception as e:  # noqa: BLE001
            print(f"  [fng] 读 certified 失败: {e}")
    j = fetch_fng(3)
    data = j.get("data") or []
    rows = []
    for item in data:
        d = pd.to_datetime(int(item["timestamp"]), unit="s", utc=True).normalize()
        if d.strftime("%Y-%m-%d") not in have:
            rows.append({"timestamp": item["timestamp"],
                         "value": int(item["value"]),
                         "value_classification": item["value_classification"]})
    if not rows:
        print("  [fng] 无新增日期, 跳过")
        return None
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    bid = f"fng_daily_{day}"
    if _already("alternative_me", "sentiment_fng", bid):
        print(f"  [fng] 批次 {bid} 已存在, 跳过")
        return None
    payload = {"data": rows, "fetched_at": _now_iso(),
               "note": "daily incremental"}
    tmp = _save_tmp(f"fng_daily_{day}.json", json.dumps(payload, ensure_ascii=False))
    dst = write_raw_file(
        tmp, "alternative_me", "sentiment_fng", bid,
        source={"api": FNG_URL, "params": {"limit": 3},
                "fetched_at": _now_iso(), "rows": len(rows)},
        timestamp_unit=TIMESTAMP_UNIT, ext="json")
    os.remove(tmp)
    print(f"  [fng] {bid}: 新增 {len(rows)} 天 -> {dst}")
    return dst


def _normalize_fng() -> pd.DataFrame:
    """全部 raw 批次 -> sentiment_fng 标准表 (按 date_utc 去重)。"""
    frames = []
    for p in _raw_file_paths("alternative_me", "sentiment_fng", ""):
        j = _read_json(p)
        data = j.get("data") or []
        if not data:
            continue
        fetched = pd.to_datetime(j.get("fetched_at"), utc=True,
                                 errors="coerce")
        for item in data:
            frames.append({
                "date_utc": pd.to_datetime(int(item["timestamp"]), unit="s",
                                           utc=True).normalize(),
                "value": int(item["value"]),
                "classification": item["value_classification"],
                "data_available_at": fetched,
                "source_batch_id": "fng_v1",
            })
    if not frames:
        return pd.DataFrame()
    df = pd.DataFrame(frames)
    df["data_available_at"] = pd.to_datetime(df["data_available_at"], utc=True,
                                             errors="coerce")
    df = df.drop_duplicates("date_utc", keep="last")   # 增量批次优先
    df = df.sort_values("date_utc").reset_index(drop=True)
    df["value"] = df["value"].astype("int64")
    df["date_utc"] = df["date_utc"].astype("datetime64[us, UTC]")
    df["data_available_at"] = df["data_available_at"].astype("datetime64[us, UTC]")
    return df[["date_utc", "value", "classification",
               "data_available_at", "source_batch_id"]]


def build_fng() -> dict:
    """raw -> L1 -> L2 certified + manifest (幂等重建)。"""
    from data_foundation.config import CERTIFIED_DIR
    df = _normalize_fng()
    if df.empty:
        print("  [fng] 无原始数据, 跳过")
        return {}
    write_onchain_parquet(df, "sentiment_fng", "fng", "date_utc")
    print(f"  [fng] L1: {len(df)} 行 ({df['date_utc'].min().date()} ~ "
          f"{df['date_utc'].max().date()})")
    cdf = certify_derivatives(df, "date_utc", core_numeric_cols=["value"],
                              key_cols=["date_utc"])
    write_certified_derivatives(cdf, "sentiment_fng", "fng", "all", "date_utc")
    stats = {"row_count": len(cdf), "duplicate_count": 0, "gap_count": 0,
             "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
    batches = _all_batch_ids("alternative_me", "sentiment_fng")
    manifest = build_dataset_manifest(
        "sentiment_fng", "*", "*", "*", "*", stats, batches,
        {"note": "alternative.me Fear & Greed Index 日频; "
                 "value=0..100, classification 文本; 主键 date_utc"})
    print(f"  [fng] L2: certified {len(cdf)} 行, manifest 已更新 "
          f"(source_batches={batches})")
    return stats


# ===========================================================================
# 2. 宏观日频 (Yahoo Finance chart)
# ===========================================================================
def fetch_macro_series(series: str, range_: str = "5y") -> list[dict] | None:
    """单序列 Yahoo chart 日频 -> [{date_utc, close}, ...] (UTC 日)。失败返回 None。"""
    sym, mult = MACRO_MAP[series]
    try:
        j = netpath.fetch_json(
            f"{YAHOO_BASE}/{sym}",
            params={"range": range_, "interval": "1d", "events": "history"},
            retries=8, timeout=30)
        res = (j.get("chart") or {}).get("result") or []
        if not res:
            return None
        r = res[0]
        ts = r.get("timestamp") or []
        close = (r.get("indicators") or {}).get("quote", [{}])[0].get("close") or []
        if not ts or not close:
            return None
        rows = []
        for t, c in zip(ts, close):
            if c is None:
                continue
            rows.append({"date_utc": pd.to_datetime(int(t), unit="s",
                                                    utc=True).normalize(),
                         "close": float(c) * mult})
        return rows
    except Exception as e:  # noqa: BLE001
        print(f"  [macro] {series} ({sym}) 失败: {str(e)[:120]}")
        return None


def ingest_macro_backfill() -> list[str]:
    """全部宏观序列近 5 年 -> L0 raw/yahoo/macro_daily (batch macro_{SERIES}_v1)。"""
    written = []
    for series in MACRO_MAP:
        bid = f"macro_{series}_v1"
        if _already("yahoo", "macro_daily", bid):
            print(f"  [macro] 批次 {bid} 已存在, 跳过")
            continue
        sym, _ = MACRO_MAP[series]
        try:
            j = netpath.fetch_json(
                f"{YAHOO_BASE}/{sym}",
                params={"range": "5y", "interval": "1d", "events": "history"},
                retries=8, timeout=30)
            res = (j.get("chart") or {}).get("result") or []
            if not res:
                print(f"  [macro] {series}: 空响应, 跳过")
                continue
            r = res[0]
            n = len(r.get("timestamp") or [])
            j["fetched_at"] = _now_iso()
            tmp = _save_tmp(f"macro_{series}_v1.json",
                            json.dumps(j, ensure_ascii=False))
            dst = write_raw_file(
                tmp, "yahoo", "macro_daily", bid,
                source={"api": f"{YAHOO_BASE}/{sym}",
                        "params": {"range": "5y", "interval": "1d",
                                   "events": "history"},
                        "fetched_at": _now_iso(), "rows": n,
                        "series": series, "symbol": sym},
                timestamp_unit=TIMESTAMP_UNIT, ext="json")
            os.remove(tmp)
            written.append(dst)
            print(f"  [macro] {bid}: {n} 行 -> {dst}")
        except Exception as e:  # noqa: BLE001
            print(f"  [macro] {series} ({sym}) 失败: {str(e)[:120]}, 跳过")
        time.sleep(0.3)
    return written


def ingest_macro_daily() -> str | None:
    """宏观每日增量: 拉最近 5 天, 只写 certified 里没有的日期 (batch macro_daily_YYYYMMDD)。"""
    from data_foundation.config import CERTIFIED_DIR
    have = set()
    cert_path = os.path.join(CERTIFIED_DIR, "macro_daily", "yahoo", "all",
                             "data.parquet")
    if os.path.exists(cert_path):
        try:
            have = set(pd.read_parquet(cert_path,
                                       columns=["date_utc"])["date_utc"]
                       .dt.strftime("%Y-%m-%d"))
        except Exception as e:  # noqa: BLE001
            print(f"  [macro] 读 certified 失败: {e}")
    rows = []
    for series in MACRO_MAP:
        recs = fetch_macro_series(series, range_="5d")
        if not recs:
            continue
        for r in recs:
            if r["date_utc"].strftime("%Y-%m-%d") not in have:
                rows.append({"series": series, "date_utc": r["date_utc"],
                             "close": r["close"]})
        time.sleep(0.3)
    if not rows:
        print("  [macro] 无新增日期, 跳过")
        return None
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    bid = f"macro_daily_{day}"
    if _already("yahoo", "macro_daily", bid):
        print(f"  [macro] 批次 {bid} 已存在, 跳过")
        return None
    payload = {"rows": [{"series": r["series"],
                         "date_utc": r["date_utc"].strftime("%Y-%m-%d"),
                         "close": r["close"]} for r in rows],
               "fetched_at": _now_iso(), "note": "daily incremental"}
    tmp = _save_tmp(f"macro_daily_{day}.json",
                    json.dumps(payload, ensure_ascii=False))
    dst = write_raw_file(
        tmp, "yahoo", "macro_daily", bid,
        source={"api": "Yahoo chart (daily incremental)",
                "fetched_at": _now_iso(), "rows": len(rows)},
        timestamp_unit=TIMESTAMP_UNIT, ext="json")
    os.remove(tmp)
    print(f"  [macro] {bid}: 新增 {len(rows)} 行 -> {dst}")
    return dst


def _normalize_macro() -> pd.DataFrame:
    """全部 raw 批次 -> macro_daily 标准表 (按 series+date_utc 去重)。"""
    frames = []
    for p in _raw_file_paths("yahoo", "macro_daily", ""):
        j = _read_json(p)
        fetched = pd.to_datetime(j.get("fetched_at"), utc=True, errors="coerce")
        if "rows" in j:                       # daily incremental 批次
            series_map = dict(MACRO_MAP)
            for r in j["rows"]:
                s = r["series"]
                mult = series_map.get(s, (None, 1.0))[1]
                frames.append({
                    "series": s,
                    "date_utc": pd.to_datetime(r["date_utc"], utc=True).normalize(),
                    "close": float(r["close"]) * mult,
                    "data_available_at": fetched,
                    "source_batch_id": "macro_daily",
                })
            continue
        res = (j.get("chart") or {}).get("result") or []
        if not res:
            continue
        r = res[0]
        meta = r.get("meta") or {}
        sym = meta.get("symbol", "")
        series = next((s for s, (y, _) in MACRO_MAP.items() if y == sym), None)
        if series is None:
            print(f"  [macro] 无法识别 symbol {sym}, 跳过 {p}")
            continue
        mult = MACRO_MAP[series][1]
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
                "source_batch_id": f"macro_{series}_v1",
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
    return df[["series", "date_utc", "close", "data_available_at",
               "source_batch_id"]]


def build_macro() -> dict:
    """raw -> L1 -> L2 certified + manifest (幂等重建)。"""
    df = _normalize_macro()
    if df.empty:
        print("  [macro] 无原始数据, 跳过")
        return {}
    write_onchain_parquet(df, "macro_daily", "yahoo", "date_utc")
    print(f"  [macro] L1: {len(df)} 行, {df['series'].nunique()} 序列 "
          f"({df['date_utc'].min().date()} ~ {df['date_utc'].max().date()})")
    cdf = certify_derivatives(df, "date_utc", core_numeric_cols=["close"],
                              key_cols=["series", "date_utc"])
    write_certified_derivatives(cdf, "macro_daily", "yahoo", "all", "date_utc")
    stats = {"row_count": len(cdf), "duplicate_count": 0, "gap_count": 0,
             "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
    batches = _all_batch_ids("yahoo", "macro_daily")
    manifest = build_dataset_manifest(
        "macro_daily", "*", "*", "*", "*", stats, batches,
        {"note": "Yahoo Finance chart 日频收盘近 5 年; series=DXY|SPX|NDX|"
                 "VIX|GOLD|US10Y; US10Y=^TNX 原始值即百分比收益率(实测, 未÷10); "
                 "主键 (series, date_utc)"})
    print(f"  [macro] L2: certified {len(cdf)} 行, manifest 已更新 "
          f"(source_batches={batches})")
    return stats


# ===========================================================================
# CLI
# ===========================================================================
def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if mode == "fng":
        ingest_fng_backfill()
        build_fng()
    elif mode == "macro":
        ingest_macro_backfill()
        build_macro()
    elif mode in ("backfill", "all"):
        ingest_fng_backfill()
        build_fng()
        ingest_macro_backfill()
        build_macro()
    elif mode == "fng_daily":
        ingest_fng_daily()
        build_fng()
    elif mode == "macro_daily":
        ingest_macro_daily()
        build_macro()
    elif mode == "build":
        build_fng()
        build_macro()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
