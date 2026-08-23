# -*- coding: utf-8 -*-
"""
_J_backfill.py — 子代理 J: 补齐 CMC 流动性门槛币种的 1h K 线
============================================================
目标: _J_targets.csv (38 个, 全部 active per listing_universe; LUNAUSDT 手动补)
路径:
  active  : Binance REST /api/v3/klines (经 netpath -> data-api.binance.vision 直连)
            游标分页 startTime -> live, 写 raw {SYM}_expand_v2.csv.gz
  delisted: REST 400/无效 symbol 时兜底 Vision 月度归档, 写 {SYM}_delisted_v2.csv.gz
            (G 模式: 路径含 /1h/ 段; 时间戳 2025 起 us 16 位, 按 |v|>=1e14 判断;
             zip 内存解压不落盘)
之后: L1 normalize_klines + write_parquet; L2 certify_candles + write_certified;
      merge manifest (合并式, 保留既有 venue/batch)。
幂等: L2 certified 覆盖完整 (active: 最新 bar 在近 2 天内) 即跳过;
      L0 gz 已存在且完整则复用不重新下载。
红线: 不改 schema/run_daily/reader/finalize; 不删文件; 全程 UTC; 不调 finalize_all。
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation import netpath
from data_foundation.config import RAW_DIR, CERTIFIED_DIR
from data_foundation import l1 as l1m
from data_foundation.l2 import (certify_candles, write_certified,
                                build_dataset_manifest)
from data_foundation.manifest import load_manifest
from data_foundation.finalize import scan_dataset

TARGETS_CSV = "_J_targets.csv"
DATASET = "market_candle_spot_1h"
VENUE = "binance"
MARKET = "spot"
INTERVAL = "1h"
DISK_GUARD_GB = 4.5
SLEEP = 0.12

# Binance REST / Vision 月度 klines 12 列 (REST 数组顺序 / Vision CSV 无表头)
KLINES12 = ["Open Time", "Open", "High", "Low", "Close", "Volume",
            "Close Time", "Quote Asset Volume", "Number of Trades",
            "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
            "Ignore"]

RAW_DATASET_DIR = os.path.join(RAW_DIR, VENUE, "spot_klines_1h")


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 时间戳解析 (统一: REST 恒 ms; Vision 2025 前 ms / 2025 起 us, 按值判断)
# ---------------------------------------------------------------------------
def _parse_epoch(col: pd.Series) -> pd.Series:
    v = pd.to_numeric(col, errors="coerce")
    out = pd.Series(pd.NaT, index=v.index, dtype="datetime64[ns, UTC]")
    ms = v.abs() < 1e14
    if ms.any():
        out[ms] = pd.to_datetime(v[ms], unit="ms", utc=True, errors="coerce")
    if (~ms).any():
        out[~ms] = pd.to_datetime(v[~ms], unit="us", utc=True, errors="coerce")
    return out


# ---------------------------------------------------------------------------
# 1) 抓取: REST 分页到 live
# ---------------------------------------------------------------------------
def fetch_rest_full(sym: str, first_period: str) -> pd.DataFrame:
    """REST /api/v3/klines 游标分页 (startTime 从 first_period 月首到 live)。"""
    start_ms = int(pd.Timestamp(f"{first_period}-01", tz="UTC").timestamp() * 1000)
    frames = []
    cur = start_ms
    while True:
        r = netpath.request("GET", "https://api.binance.com/api/v3/klines",
                            params={"symbol": sym, "interval": "1h",
                                    "startTime": cur, "limit": 1000},
                            timeout=30, retries=8)
        j = r.json()
        if not isinstance(j, list) or len(j) == 0:
            break
        frames.append(pd.DataFrame(j, columns=KLINES12))
        cur = int(j[-1][0]) + 3600_000
        if len(j) < 1000:
            break
        time.sleep(SLEEP)
    if not frames:
        return pd.DataFrame(columns=KLINES12)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# 2) 抓取: Vision 月度归档 (delisted 兜底, G 模式)
# ---------------------------------------------------------------------------
def download_month(sym: str, ym: str, timeout: int = 60) -> bytes | None:
    url = (f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1h/"
           f"{sym}-1h-{ym}.zip")
    try:
        r = netpath.request("GET", url, timeout=timeout, retries=3)
        return r.content
    except Exception as e:  # noqa: BLE001
        if "404" in str(e):
            return None
        raise


def fetch_vision_months(sym: str, first_period: str, last_period: str,
                        sleep: float = 0.15) -> pd.DataFrame:
    months = pd.period_range(first_period, last_period, freq="M").strftime("%Y-%m").tolist()
    frames = []
    for ym in months:
        z = download_month(sym, ym)
        if z is None:
            continue
        with zipfile.ZipFile(io.BytesIO(z)) as zf:
            name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            raw = zf.read(name)
        frames.append(pd.read_csv(io.BytesIO(raw), header=None, names=KLINES12))
        time.sleep(sleep)
    if not frames:
        return pd.DataFrame(columns=KLINES12)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# 3) L0 写入 (gz + meta)
# ---------------------------------------------------------------------------
def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_l0(sym: str, df: pd.DataFrame, batch_id: str, source: dict,
             timestamp_unit: str) -> str:
    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    os.makedirs(RAW_DATASET_DIR, exist_ok=True)
    tmp = gz_path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False, header=True, lineterminator="\n")
    os.replace(tmp, gz_path)
    meta = {
        "batch_id": batch_id,
        "source": source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "timestamp_unit": timestamp_unit,
        "timezone": "UTC",
        "checksum_sha256": _sha256(gz_path),
        "file_size_bytes": os.path.getsize(gz_path),
        "row_count": int(len(df)),
        "immutable": True,
    }
    with open(gz_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return gz_path


def load_l0_raw(batch_id: str) -> pd.DataFrame:
    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        return pd.read_csv(f)


# ---------------------------------------------------------------------------
# 4) L1 + L2 (单 symbol, 与 G 同模式)
# ---------------------------------------------------------------------------
def build_l1_l2(sym: str, raw: pd.DataFrame, batch_id: str) -> tuple[str, dict]:
    inst = l1m.instrument_id(sym)
    df = raw.copy()
    for c in ("Open Time", "Close Time"):
        df[c] = _parse_epoch(df[c])
    df = l1m.normalize_klines(df, VENUE, MARKET, sym, INTERVAL)
    df["source_batch_id"] = batch_id
    l1_root = l1m.write_parquet(df, DATASET, VENUE, MARKET, inst, INTERVAL)
    cdf = certify_candles(df)
    root, stats = write_certified(cdf, DATASET, VENUE, MARKET, inst, INTERVAL)
    log(f"  {sym}: L1 -> {os.path.relpath(l1_root, HERE)} | L2 rows={stats['row_count']} "
        f"suspect={stats['suspect_count']} gap={stats['gap_count']} dup={stats['duplicate_count']} "
        f"{str(cdf['open_time_utc'].min())[:10]} ~ {str(cdf['open_time_utc'].max())[:16]}")
    return root, stats


# ---------------------------------------------------------------------------
# 5) 幂等检查
# ---------------------------------------------------------------------------
def l2_complete(sym: str, status: str, last_period: str) -> bool:
    """active: 最新 bar 近 2 天内; delisted: 覆盖到 last_period 月份。"""
    inst = l1m.instrument_id(sym)
    p = os.path.join(CERTIFIED_DIR, DATASET, VENUE, MARKET, inst,
                     f"interval={INTERVAL}", "data.parquet")
    if not os.path.isfile(p):
        return False
    import pyarrow.parquet as pq
    t = pq.read_table(p, columns=["open_time_utc"])
    mx = t.column("open_time_utc").to_pandas().max()
    if pd.isna(mx):
        return False
    mx = pd.Timestamp(mx)
    if mx.tzinfo is None:
        mx = mx.tz_localize("UTC")
    else:
        mx = mx.tz_convert("UTC")
    if status == "delisted":
        return str(mx)[:7] == last_period
    now = pd.Timestamp.now(tz="UTC")
    return mx >= now - pd.Timedelta(days=2)


# ---------------------------------------------------------------------------
# 6) manifest (合并式)
# ---------------------------------------------------------------------------
def merge_manifest(new_batches: list[str], agg_rules: dict) -> dict:
    ds_dir = os.path.join(CERTIFIED_DIR, DATASET)
    existing = load_manifest(ds_dir) if os.path.exists(
        os.path.join(ds_dir, "manifest.json")) else {}
    src = list(existing.get("source_batches") or [])
    for b in new_batches:
        if b not in src:
            src.append(b)
    stats = scan_dataset(ds_dir)
    agg = dict(agg_rules)
    if isinstance(existing.get("aggregation_rules"), dict):
        prev = existing["aggregation_rules"].get("note", "")
        if prev:
            agg["note"] = prev + " | " + agg.get("note", "")
    man = build_dataset_manifest(DATASET, VENUE, MARKET, "*", "*", stats,
                                 src, agg)
    log(f"manifest: rows={stats['row_count']} suspect={stats['suspect_count']} "
        f"{str(stats['coverage_start'])[:10]} ~ {str(stats['coverage_end'])[:10]} "
        f"batches={len(src)}")
    return man


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def process_symbol(row: dict) -> dict:
    sym = row["symbol"]
    status = row["status"]
    first_period = row["first_period"]
    last_period = row["last_period"]

    if l2_complete(sym, status, last_period):
        log(f"{sym}: L1/L2 覆盖完整, 跳过")
        return {"symbol": sym, "status": "skipped"}

    if status == "delisted":
        # Vision 归档主路径
        batch_id = f"{sym}_delisted_v2"
        gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
        if os.path.isfile(gz_path):
            log(f"{sym}: 复用既有 L0 (delisted)")
            raw = load_l0_raw(batch_id)
        else:
            raw = fetch_vision_months(sym, first_period, last_period)
            if len(raw) == 0:
                raise RuntimeError(f"{sym}: Vision 无任何月份数据")
            write_l0(sym, raw, batch_id,
                     {"api": "binance-vision monthly klines",
                      "url_template": ("https://data.binance.vision/data/spot/monthly/klines/"
                                       "{SYM}/1h/{SYM}-1h-{YYYY-MM}.zip"),
                      "symbol": sym, "interval": "1h", "market_type": "spot",
                      "first_period": first_period, "last_period": last_period,
                      "fetched_at": datetime.now(timezone.utc).isoformat()},
                     "ms|us (per value; Binance 2025 起改用 us)")
            log(f"{sym}: L0 delisted -> {os.path.relpath(gz_path, HERE)} ({len(raw)} 行)")
    else:
        # active: REST 主路径; REST 无效 symbol 时兜底 Vision 归档
        batch_id = f"{sym}_expand_v2"
        gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
        if os.path.isfile(gz_path):
            log(f"{sym}: 复用既有 L0 (expand)")
            raw = load_l0_raw(batch_id)
        else:
            try:
                raw = fetch_rest_full(sym, first_period)
                via = "rest"
            except Exception as e:  # noqa: BLE001
                if "400" not in str(e) and "symbol" not in str(e).lower():
                    raise
                log(f"{sym}: REST 无效 ({str(e)[:60]}), 兜底 Vision 归档")
                raw = fetch_vision_months(sym, first_period, last_period)
                via = "vision_fallback"
            if len(raw) == 0:
                raise RuntimeError(f"{sym}: 无任何数据 (REST+Vision 均为空)")
            write_l0(sym, raw, batch_id,
                     {"api": "https://api.binance.com/api/v3/klines (netpath->data-api.binance.vision)",
                      "symbol": sym, "interval": "1h", "market_type": "spot",
                      "first_period": first_period, "last_period": last_period,
                      "fetch_mode": via,
                      "fetched_at": datetime.now(timezone.utc).isoformat()},
                     "ms")
            log(f"{sym}: L0 expand -> {os.path.relpath(gz_path, HERE)} ({len(raw)} 行)")

    _root, stats = build_l1_l2(sym, raw, batch_id)
    stats["symbol"] = sym
    stats["status"] = "ok"
    stats["batch_id"] = batch_id
    return stats


def main() -> None:
    t0 = time.time()
    targets = pd.read_csv(TARGETS_CSV).to_dict("records")
    log(f"目标 {len(targets)} 个: active={sum(1 for r in targets if r['status']=='active')}, "
        f"delisted={sum(1 for r in targets if r['status']=='delisted')}")

    results, failures = [], []
    new_batches = []
    for row in targets:
        free_gb = shutil.disk_usage(os.path.dirname(HERE)).free / 1e9
        if free_gb < DISK_GUARD_GB:
            log(f"磁盘守卫: 剩余 {free_gb:.2f}GB < {DISK_GUARD_GB}GB, 停止")
            failures.append({"symbol": row["symbol"], "error": "disk_guard"})
            break
        try:
            st = process_symbol(row)
            results.append(st)
            if st.get("status") in ("ok", "skipped") and st.get("batch_id"):
                new_batches.append(st["batch_id"])
        except Exception as e:  # noqa: BLE001
            log(f"{row['symbol']}: FAILED {str(e)[:200]}")
            failures.append({"symbol": row["symbol"], "error": str(e)[:300]})

    if new_batches:
        merge_manifest(new_batches, {"note": "CMC 流动性门槛 (历史日成交额>=1M USD) 币种 K 线扩容 "
                                            "(子代理 J, expand_v2 REST 分页 / delisted_v2 Vision 归档)",
                                     "interval": "1h"})

    ok = len([r for r in results if r.get("status") == "ok"])
    sk = len([r for r in results if r.get("status") == "skipped"])
    log(f"== 完成: {ok} ok, {sk} skipped, {len(failures)} failed, "
        f"耗时 {time.time()-t0:.0f}s ==")
    for f in failures:
        log(f"  FAIL {f['symbol']}: {f['error']}")
    with open("_J_backfill_report.json", "w", encoding="utf-8") as f:
        json.dump({"targets": targets, "results": results, "failures": failures},
                  f, ensure_ascii=False, indent=2, default=str)


if __name__ == "__main__":
    main()
