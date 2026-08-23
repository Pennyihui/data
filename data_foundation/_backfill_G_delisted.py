# -*- coding: utf-8 -*-
"""
_backfill_G_delisted.py — 已下架交易对 1h K 线回填 (消幸存者偏差)
================================================================
任务 (数据流水线子代理 G):
  1. 选标的: listing_universe delisted 集合 ∩ quote=USDT 现货对,
     按历史最大日成交额排序取 top 30 (排名源可用时);
     排名源覆盖不足时按 CMC 日成交额排名按 base asset 兜底匹配。
  2. 抓取: data.binance.vision 月度 1h klines zip (内存解压, 不落盘)。
  3. L0: raw/binance/spot_klines_1h/{SYM}_delisted_v1.csv.gz + meta。
  4. L1: l1.normalize_klines + write_parquet (仅 1h)。
  5. L2: certify_candles + write_certified + build_dataset_manifest (合并式)。
  6. 幂等: L1 覆盖完整即跳过; 每 symbol try/except; 磁盘守卫 <4.5GB 即停。

注意 (2026-08 实测): Binance Vision 月度 1h klines 自 2025 年起时间戳
从毫秒 (13 位) 改为微秒 (16 位); 解析按值判断单位 (|v|>=1e14 -> us)。

红线: 不改 schema/run_daily/reader/finalize 等代理文件; 不删已有文件;
     不调 finalize_all; 全程 UTC。
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

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
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

UNIVERSE = r"data/l1/listing_universe/binance_vision/data.parquet"
RANK_WIDE = r"../data_new/additional/spot_daily_volume_wide.csv"
RANK_CMC = r"../data_new/additional/cmc_daily_volume_ranking.csv"
DATASET = "market_candle_spot_1h"
VENUE = "binance"
MARKET = "spot"
INTERVAL = "1h"
DISK_GUARD_GB = 4.5

# Binance Vision 月度 klines CSV: 无表头, 12 列 (与 REST 标准列名对应)
VISION_HEADER = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                 "Close Time", "Quote Asset Volume", "Number of Trades",
                 "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
                 "Ignore"]

RAW_DATASET_DIR = os.path.join(RAW_DIR, VENUE, "spot_klines_1h")


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 1) 选标的
# ---------------------------------------------------------------------------
def select_targets(top: int = 30) -> tuple[list[dict], dict]:
    """delisted USDT 现货对, 按历史最大日成交额排序。

    主源 spot_daily_volume_wide.csv (Binance 现货, 日期×symbol×成交额);
    主源未覆盖的 delisted 对, 用 CMC 日成交额排名按 base asset 兜底。
    返回 (排名列表, 口径说明)。
    """
    uni = pd.read_parquet(UNIVERSE)
    delu = uni[(uni["status"] == "delisted")
               & (uni["market_type"] == "spot")
               & (uni["symbol"].str.endswith("USDT"))].copy()
    delu["base"] = delu["symbol"].str.replace(r"USDT$", "", regex=True)

    wide = pd.read_csv(RANK_WIDE)
    w_cols = [c for c in wide.columns if c != "date"]
    w_max = wide[w_cols].max()

    cmc = pd.read_csv(RANK_CMC)
    cmc_max = cmc.groupby("symbol")["volume"].max()

    rows = []
    for _, r in delu.iterrows():
        vol, src = None, None
        if r["symbol"] in w_max.index and pd.notna(w_max[r["symbol"]]):
            vol, src = float(w_max[r["symbol"]]), "binance_spot_wide"
        elif r["base"] in cmc_max.index:
            vol, src = float(cmc_max[r["base"]]), "cmc_base_asset"
        if vol is not None and vol > 0:
            rows.append({"symbol": r["symbol"], "first_period": r["first_period"],
                         "last_period": r["last_period"],
                         "max_daily_volume": vol, "vol_source": src})
    cand = sorted(rows, key=lambda x: -x["max_daily_volume"])[:top]
    note = ("排名口径=历史最大日成交额: 主源 Binance 现货 spot_daily_volume_wide.csv "
            "(2019+, 覆盖 delisted USDT 对有限); 主源未覆盖对按 CMC 日成交额排名 "
            "(cmc_daily_volume_ranking.csv) base asset 兜底; "
            f"可排名 delisted USDT 对共 {len(rows)} 个, 取 top {min(top, len(rows))}。")
    return cand, {"ranked_total": len(rows), "note": note}


# ---------------------------------------------------------------------------
# 2) 抓取 + L0
# ---------------------------------------------------------------------------
def download_month(sym: str, ym: str, timeout: int = 60) -> bytes | None:
    """下载单月 zip -> 返回 zip 字节; 404 返回 None; 其他异常抛出。"""
    url = (f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1h/"
           f"{sym}-1h-{ym}.zip")
    try:
        r = netpath.request("GET", url, timeout=timeout, retries=3)
        return r.content
    except Exception as e:  # noqa: BLE001
        if "404" in str(e):
            return None
        raise


def fetch_symbol_months(sym: str, first_period: str, last_period: str,
                        sleep: float = 0.15) -> tuple[list[pd.DataFrame], list[str]]:
    """逐月下载, 内存解压读 CSV, 返回 (frames, ok_months)。"""
    months = pd.period_range(first_period, last_period, freq="M").strftime("%Y-%m").tolist()
    frames, ok_months = [], []
    for ym in months:
        z = download_month(sym, ym)
        if z is None:
            log(f"  {sym} {ym}: 404 跳过")
            continue
        with zipfile.ZipFile(io.BytesIO(z)) as zf:
            name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            raw = zf.read(name)
        df = pd.read_csv(io.BytesIO(raw), header=None, names=VISION_HEADER)
        frames.append(df)
        ok_months.append(ym)
        time.sleep(sleep)
    return frames, ok_months


def _parse_epoch(col: pd.Series) -> pd.Series:
    """Binance Vision 时间戳: 2025 前 13 位 ms, 2025 起 16 位 us。按值判断。"""
    v = pd.to_numeric(col, errors="coerce")
    out = pd.Series(pd.NaT, index=v.index, dtype="datetime64[ns, UTC]")
    ms = v.abs() < 1e14
    if ms.any():
        out[ms] = pd.to_datetime(v[ms], unit="ms", utc=True, errors="coerce")
    if (~ms).any():
        out[~ms] = pd.to_datetime(v[~ms], unit="us", utc=True, errors="coerce")
    return out


def write_l0(sym: str, frames: list[pd.DataFrame], ok_months: list[str],
             first_period: str, last_period: str) -> str:
    """合并全史 -> gz 批次 + meta (zip 不落盘, 只写最终 gz)。"""
    merged = pd.concat(frames, ignore_index=True)
    batch_id = f"{sym}_delisted_v1"
    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    os.makedirs(RAW_DATASET_DIR, exist_ok=True)
    tmp = gz_path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        merged.to_csv(f, index=False, header=True, lineterminator="\n")
    os.replace(tmp, gz_path)
    meta = {
        "batch_id": batch_id,
        "source": {
            "api": "binance-vision monthly klines",
            "url_template": ("https://data.binance.vision/data/spot/monthly/klines/"
                             "{SYM}/1h/{SYM}-1h-{YYYY-MM}.zip"),
            "symbol": sym,
            "interval": "1h",
            "market_type": "spot",
            "months": ok_months,
            "first_period": first_period,
            "last_period": last_period,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "timestamp_unit": "ms|us (per value; Binance 2025 起改用 us)",
        "timezone": "UTC",
        "checksum_sha256": _sha256(gz_path),
        "file_size_bytes": os.path.getsize(gz_path),
        "row_count": int(len(merged)),
        "immutable": True,
    }
    with open(gz_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return gz_path


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fix_l0_meta(sym: str, gz_path: str) -> None:
    """修正早期 L0 meta 的 timestamp_unit (ms|us 混合, 2025 起 vision 用 us)。"""
    mp = gz_path + ".meta.json"
    if not os.path.isfile(mp):
        return
    with open(mp, encoding="utf-8") as f:
        meta = json.load(f)
    if meta.get("timestamp_unit") == "ms":
        meta["timestamp_unit"] = "ms|us (per value; Binance 2025 起改用 us)"
        with open(mp, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2, default=str)


def load_l0_raw(sym: str) -> pd.DataFrame:
    """从既有 L0 gz 读回原始 12 列 DataFrame (不重新下载)。"""
    gz_path = os.path.join(RAW_DATASET_DIR, f"{sym}_delisted_v1.csv.gz")
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        return pd.read_csv(f)


# ---------------------------------------------------------------------------
# 3) L1 + L2 (单 symbol)
# ---------------------------------------------------------------------------
def build_l1_l2(sym: str, raw: pd.DataFrame) -> tuple[str, dict]:
    """raw 12 列 -> L1 parquet -> L2 certified。返回 (cert_root, stats)。"""
    inst = l1m.instrument_id(sym)
    df = raw.copy()
    for c in ("Open Time", "Close Time"):
        df[c] = _parse_epoch(df[c])
    df = l1m.normalize_klines(df, VENUE, MARKET, sym, INTERVAL)
    df["source_batch_id"] = f"{sym}_delisted_v1"
    l1_root = l1m.write_parquet(df, DATASET, VENUE, MARKET, inst, INTERVAL)
    log(f"{sym}: L1 -> {os.path.relpath(l1_root, HERE)} ({len(df)} 行, "
        f"{df['open_time_utc'].min()} ~ {df['open_time_utc'].max()})")
    cdf = certify_candles(df)
    root, stats = write_certified(cdf, DATASET, VENUE, MARKET, inst, INTERVAL)
    log(f"{sym}: L2 -> {os.path.relpath(root, HERE)} "
        f"rows={stats['row_count']} suspect={stats['suspect_count']} "
        f"gap={stats['gap_count']} dup={stats['duplicate_count']}")
    return root, stats


def l1_complete(sym: str, last_period: str) -> bool:
    """幂等检查: 既有 L1 覆盖是否已达 last_period 所在月份。

    用年月比对 (而非月末), 兼容月中下架 (如 DAIUSDT last=2020-08, 实际
    数据止于 2020-08-12; XMRUSDT last=2024-02, 止于 2024-02-20)。
    """
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
    return str(mx)[:7] == last_period


def process_symbol(sym: str, first_period: str, last_period: str) -> dict:
    """下载(或复用 L0) -> L0 -> L1 -> L2, 返回该 symbol 的统计。"""
    gz_path = os.path.join(RAW_DATASET_DIR, f"{sym}_delisted_v1.csv.gz")
    if l1_complete(sym, last_period):
        log(f"{sym}: L1/L2 覆盖完整, 跳过")
        return {"symbol": sym, "status": "skipped"}
    if os.path.isfile(gz_path):
        log(f"{sym}: 复用既有 L0 (不重新下载)")
        _fix_l0_meta(sym, gz_path)
        raw = load_l0_raw(sym)
        _root, stats = build_l1_l2(sym, raw)
        stats["symbol"] = sym
        stats["status"] = "ok"
        return stats
    frames, ok_months = fetch_symbol_months(sym, first_period, last_period)
    if not frames:
        raise RuntimeError(f"{sym}: 无任何月份数据 (first={first_period} last={last_period})")
    gz = write_l0(sym, frames, ok_months, first_period, last_period)
    log(f"{sym}: L0 -> {os.path.relpath(gz, HERE)} ({len(ok_months)} 个月, "
        f"{sum(len(f) for f in frames)} 行)")
    raw = pd.concat(frames, ignore_index=True)
    _root, stats = build_l1_l2(sym, raw)
    stats["symbol"] = sym
    stats["status"] = "ok"
    return stats


# ---------------------------------------------------------------------------
# 4) manifest (合并式, 保留既有 venue/batch 信息)
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
def main() -> None:
    t0 = time.time()
    targets, rank_note = select_targets(top=30)
    log(f"选中 {len(targets)} 个 delisted USDT 对 (可排名总数 {rank_note['ranked_total']})")
    for t in targets[:10]:
        log(f"  {t['symbol']:<12} max_vol={t['max_daily_volume']:.3e} "
            f"src={t['vol_source']:<16} {t['first_period']}~{t['last_period']}")

    results, failures = [], []
    new_batches = []
    for t in targets:
        free_gb = shutil.disk_usage(os.path.dirname(HERE)).free / 1e9
        if free_gb < DISK_GUARD_GB:
            log(f"磁盘守卫: 剩余 {free_gb:.2f}GB < {DISK_GUARD_GB}GB, 停止后续 symbol")
            failures.append({"symbol": t["symbol"], "error": "disk_guard"})
            break
        try:
            st = process_symbol(t["symbol"], t["first_period"], t["last_period"])
            results.append(st)
            if st.get("status") in ("ok", "skipped"):
                new_batches.append(f"{t['symbol']}_delisted_v1")
        except Exception as e:  # noqa: BLE001
            log(f"{t['symbol']}: FAILED {str(e)[:200]}")
            failures.append({"symbol": t["symbol"], "error": str(e)[:300]})

    agg_rules = {"note": rank_note["note"] + " | 仅 1h (1d/1w 派生本次不做)",
                 "interval": "1h"}
    if new_batches:
        merge_manifest(new_batches, agg_rules)

    log(f"== 完成: {len([r for r in results if r.get('status')=='ok'])} ok, "
        f"{len([r for r in results if r.get('status')=='skipped'])} skipped, "
        f"{len(failures)} failed, 耗时 {time.time()-t0:.0f}s ==")
    for f in failures:
        log(f"  FAIL {f['symbol']}: {f['error']}")
    with open("_G_delisted_report.json", "w", encoding="utf-8") as f:
        json.dump({"targets": targets, "results": results, "failures": failures,
                   "rank_note": rank_note}, f, ensure_ascii=False, indent=2,
                  default=str)


if __name__ == "__main__":
    main()
