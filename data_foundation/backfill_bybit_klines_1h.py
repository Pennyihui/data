# -*- coding: utf-8 -*-
"""
backfill_bybit_klines_1h.py — Bybit 永续 1h K线全历史回填 (15 MVP 币)
=====================================================================
- L0: raw/bybit/perpetual_klines_1h/{SYM}_v1.csv.gz  (Binance 12 列兼容格式,
      Open Time=ISO ms, Close Time=Open+3599999ms, Volume=base, Quote Asset
      Volume=turnover, trades/taker/ignore 空; meta 记录 api/interval/游标参数)
- L1: l1/market_candle_perpetual_1h/bybit/perpetual/{INST}/interval=1h/data.parquet
      (venue_id=bybit, market_type=perpetual, instrument=BASE-USDT)
- L2: l2/certified/market_candle_perpetual_1h/bybit/perpetual/{INST}/interval=1h
      + 数据集级 manifest (合并既有 source_batches, stats 跨 venue 重扫)
- 幂等: L0 batch / L1 parquet / L2 certified 已存在则跳过
- 每 symbol try/except; 磁盘守卫 < 4.2GB; 全程 UTC
用法: python backfill_bybit_klines_1h.py [SYM...]   (默认全部 MVP)
"""
import argparse
import csv
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation import netpath
from data_foundation.config import MVP_ASSETS, CERTIFIED_DIR
from data_foundation.l0 import write_raw_file, list_raw_batches
from data_foundation.l1 import normalize_klines, write_parquet, instrument_id
from data_foundation.l2 import certify_candles, write_certified, build_dataset_manifest
from data_foundation import finalize

BASE = "https://api.bybit.com/v5/market"
KLINE_API = f"{BASE}/kline"
START_MS = 1577836800000          # 2020-01-01 00:00 UTC (全历史起点, 早于上市则空页)
HOUR_MS = 3600000
MAX_PAGES = 600
SLEEP = 0.1
DISK_BUDGET = 1.0e9              # 本轮新增量软上限 1GB (远小于实际需求)
BYBIT_CAP = 4.5e9                # bybit klines 三层足迹硬上限 (红线 <4.5GB)

DS = "market_candle_perpetual_1h"
HDR12 = ["Open Time", "Open", "High", "Low", "Close", "Volume",
         "Close Time", "Quote Asset Volume", "Number of Trades",
         "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def now_ms():
    return int(time.time() * 1000)


def _iso(ms):
    d = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return f"{d:%Y-%m-%dT%H:%M:%S}.{int(ms) % 1000:03d}"


def fetch_kline_history(sym):
    """start 游标向前推进 (start=页内 newest+1h) 抓全历史。
    返回 [(start_ms, 7 元组), ...] 升序去重; 到 live 尾部或无进展即停。
    """
    rows, seen, start, pages = [], set(), START_MS, 0
    last_newest = None
    while pages < MAX_PAGES:
        r = netpath.fetch_json(KLINE_API, params={
            "category": "linear", "symbol": sym, "interval": "60",
            "limit": 1000, "start": start}, retries=8, timeout=30)
        if r.get("retCode") != 0:
            raise RuntimeError(f"{sym} kline retCode={r.get('retCode')} {r.get('retMsg')}")
        lst = (r.get("result") or {}).get("list") or []
        if not lst:
            break
        added = 0
        for row in lst:
            ts = int(row[0])
            if ts not in seen:
                seen.add(ts)
                rows.append((ts, row))
                added += 1
        page_newest = max(int(x[0]) for x in lst)
        pages += 1
        if added == 0 or (last_newest is not None and page_newest <= last_newest):
            break                                   # 无进展
        if page_newest >= now_ms() - HOUR_MS:
            break                                   # 已到 live 尾部
        last_newest = page_newest
        start = page_newest + HOUR_MS
        time.sleep(SLEEP)
    rows.sort(key=lambda t: t[0])
    return [t[1] for t in rows]


def write_l0(sym, api_rows):
    """写 gz CSV -> L0 raw/bybit/perpetual_klines_1h/{sym}_v1.csv.gz"""
    tmp = os.path.join(HERE, "_tmp", f"bybit_{sym}_klines_1h.csv.gz")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(HDR12)
        for row in api_rows:
            ts = int(row[0])
            close_ts = ts + HOUR_MS - 1
            w.writerow([_iso(ts),
                        f"{float(row[1]):.12g}", f"{float(row[2]):.12g}",
                        f"{float(row[3]):.12g}", f"{float(row[4]):.12g}",
                        f"{float(row[5]):.12g}", _iso(close_ts),
                        f"{float(row[6]):.12g}", "", "", "", ""])
    dst = write_raw_file(
        tmp, "bybit", "perpetual_klines_1h", f"{sym}_v1",
        {"api": KLINE_API, "category": "linear", "symbol": sym,
         "interval": "60", "limit": 1000, "start": START_MS,
         "pagination": "start cursor walk (oldest+1h), full history",
         "fetched_at": datetime.now(timezone.utc).isoformat()},
        timestamp_unit="ms", ext="csv.gz")
    return dst


def build_l1(sym, raw_dst):
    inst = instrument_id(sym)
    df = pd.read_csv(raw_dst, compression="gzip")
    norm = normalize_klines(df, "bybit", "perpetual", sym, "1h")
    norm["source_batch_id"] = f"bybit_{sym}_v1"
    root = write_parquet(norm, DS, "bybit", "perpetual", inst, "1h")
    return inst, norm


def build_l2(sym, inst, norm):
    cert = certify_candles(norm)
    root, stats = write_certified(cert, DS, "bybit", "perpetual", inst, "1h")
    return root, stats, cert


def dir_size(path):
    tot = 0
    for dp, _, fns in os.walk(path):
        for fn in fns:
            try:
                tot += os.path.getsize(os.path.join(dp, fn))
            except OSError:
                pass
    return tot


def load_certified(venue, inst):
    p = os.path.join(CERTIFIED_DIR, DS, venue, "perpetual", inst,
                     "interval=1h", "data.parquet")
    if not os.path.exists(p):
        return None
    return pd.read_parquet(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("syms", nargs="*")
    args = ap.parse_args()
    syms = [s.upper() for s in args.syms] if args.syms else [f"{a}USDT" for a in MVP_ASSETS]

    netpath.probe(timeout=8, ref_urls={"*": "https://api.bybit.com/v5/market/time"})

    results = {}       # sym -> dict
    new_batches = []   # bybit L0 batch_ids actually written
    data_root = os.path.join(HERE, "data")
    base_size = dir_size(data_root)

    def bybit_footprint():
        return sum(dir_size(os.path.join(data_root, p)) for p in [
            os.path.join("raw", "bybit", "perpetual_klines_1h"),
            os.path.join("l1", DS, "bybit"),
            os.path.join("l2", "certified", DS, "bybit"),
        ])

    for sym in syms:
        results[sym] = {"status": "pending"}
        try:
            grown = dir_size(data_root) - base_size
            if grown > DISK_BUDGET:
                log(f"[guard] 本轮新增 {grown/1e9:.2f}GB 超预算, 中止剩余抓取")
                results[sym] = {"status": "disk_guard"}
                break
            if bybit_footprint() > BYBIT_CAP:
                log(f"[guard] bybit klines 足迹超 {BYBIT_CAP/1e9:.1f}GB, 中止")
                results[sym] = {"status": "disk_guard"}
                break
            inst = instrument_id(sym)
            l1_path = os.path.join(HERE, "data", "l1", DS, "bybit", "perpetual",
                                   inst, "interval=1h", "data.parquet")
            l2_path = os.path.join(CERTIFIED_DIR, DS, "bybit", "perpetual", inst,
                                   "interval=1h", "data.parquet")
            existing_batches = {m.get("batch_id")
                                for m in list_raw_batches("bybit", "perpetual_klines_1h")}
            bid = f"{sym}_v1"
            # ---- L0 ----
            if bid in existing_batches and os.path.exists(l1_path) and os.path.exists(l2_path):
                log(f"{sym}: L0/L1/L2 均已存在, 跳过")
                raw_dst = None
                results[sym] = {"status": "skipped_exists", "batch": bid}
            else:
                t0 = time.time()
                if bid in existing_batches:
                    # 找已有 raw 文件 (幂等: batch 存在则复用)
                    raw_dst = None
                    for meta in list_raw_batches("bybit", "perpetual_klines_1h"):
                        if meta.get("batch_id") == bid:
                            d = os.path.join(HERE, "data", "raw", "bybit",
                                             "perpetual_klines_1h",
                                             f"ingest_date={meta['ingested_at'][:10]}")
                            for fn in sorted(os.listdir(d)):
                                if fn.startswith(bid) and not fn.endswith(".meta.json"):
                                    raw_dst = os.path.join(d, fn)
                                    break
                            break
                    log(f"{sym}: L0 batch 已存在, 复用 {os.path.basename(raw_dst)}")
                else:
                    rows = fetch_kline_history(sym)
                    if not rows:
                        log(f"[warn] {sym}: kline 历史为空 (可能未上市/已下架), 跳过")
                        results[sym] = {"status": "empty"}
                        continue
                    raw_dst = write_l0(sym, rows)
                    new_batches.append(bid)
                    d0, d1 = int(rows[0][0]), int(rows[-1][0])
                    log(f"{sym}: L0 {len(rows)} 条 {_iso(d0)[:16]} -> {_iso(d1)[:16]} "
                        f"({time.time()-t0:.0f}s) -> {os.path.basename(raw_dst)}")
                # ---- L1 ----
                if os.path.exists(l1_path):
                    log(f"{sym}: L1 已存在, 跳过")
                else:
                    inst2, norm = build_l1(sym, raw_dst)
                    log(f"{sym}: L1 -> l1/{DS}/bybit/perpetual/{inst2}/interval=1h "
                        f"({len(norm)} 行)")
                # ---- L2 ----
                if os.path.exists(l2_path):
                    log(f"{sym}: L2 已存在, 跳过")
                    cert = None
                else:
                    # 从 L1 parquet 重建 L2 (保证与 L1 一致)
                    if not os.path.exists(l1_path):
                        _, norm = build_l1(sym, raw_dst)
                    l1_df = pd.read_parquet(l1_path)
                    cert = certify_candles(l1_df)
                    root, stats = write_certified(cert, DS, "bybit", "perpetual", inst, "1h")
                    log(f"{sym}: L2 -> {root} ({stats['row_count']} 行, "
                        f"suspect={stats['suspect_count']})")
                # 汇总 (行数/起止/suspect 从 certified 读, 与交付一致)
                cert_df = load_certified("bybit", inst)
                if cert_df is None or cert_df.empty:
                    results[sym] = {"status": "no_certified"}
                    continue
                results[sym] = {
                    "status": "ok", "batch": bid, "inst": inst,
                    "rows": int(len(cert_df)),
                    "start": str(cert_df["open_time_utc"].min()),
                    "end": str(cert_df["open_time_utc"].max()),
                    "suspect": int(cert_df["is_suspect"].sum()),
                    "gap": int(cert_df["is_gap"].sum()),
                }
        except Exception as e:  # noqa: BLE001
            log(f"[ERR] {sym}: {str(e)[:300]}")
            results[sym] = {"status": f"fail: {str(e)[:150]}"}

    # ---- manifest (数据集级, 合并既有批次, stats 跨 venue 重扫) ----
    try:
        ds_dir = os.path.join(CERTIFIED_DIR, DS)
        existing = {}
        mf = os.path.join(ds_dir, "manifest.json")
        if os.path.exists(mf):
            with open(mf, encoding="utf-8") as f:
                existing = json.load(f)
        src_batches = list(existing.get("source_batches") or [])
        for b in new_batches:
            if b not in src_batches:
                src_batches.append(b)
        agg = dict(existing.get("aggregation_rules") or {})
        note = ("Bybit v5 kline interval=60 全历史; turnover 映射 volume_quote; "
                "taker 列空")
        if note not in (agg.get("note") or ""):
            agg["note"] = " | ".join(x for x in [agg.get("note"), note] if x)
        stats = finalize.scan_dataset(ds_dir)
        if stats:
            man = build_dataset_manifest(DS, "bybit", "perpetual", "*", "*",
                                         stats, src_batches, agg)
            log(f"manifest: rows={stats['row_count']} suspect={stats['suspect_count']} "
                f"{str(stats['coverage_start'])[:10]}~{str(stats['coverage_end'])[:10]} "
                f"batches={len(src_batches)}")
    except Exception as e:  # noqa: BLE001
        log(f"[ERR] manifest: {str(e)[:300]}")

    # ---- 验证报告 ----
    log("=" * 70)
    log("每 symbol 行数/起止日期 (bybit certified):")
    for sym, res in results.items():
        if res.get("status") == "ok":
            log(f"  {sym:9s} rows={res['rows']:>6} {res['start'][:16]} -> {res['end'][:16]} "
                f"suspect={res['suspect']} gap={res['gap']}")
        else:
            log(f"  {sym:9s} {res.get('status')}")
    total_suspect = sum(r.get("suspect", 0) for r in results.values())
    total_rows = sum(r.get("rows", 0) for r in results.values())
    log(f"汇总: {total_rows} 行, suspect={total_suspect}")
    # 与 Binance 永续日收盘相关系数 (前 5)
    corr_rows = []
    for sym, res in results.items():
        if res.get("status") != "ok":
            continue
        inst = res["inst"]
        try:
            b = load_certified("binance", inst)
            if b is None or b.empty:
                continue
            y = load_certified("bybit", inst)
            def daily_close(d):
                d = d.copy()
                d["date"] = d["open_time_utc"].dt.floor("1D")
                return d.groupby("date")["close"].last()
            s = pd.concat([daily_close(b).rename("bin"), daily_close(y).rename("byb")],
                          axis=1).dropna()
            if len(s) >= 30:
                corr_rows.append((sym, s["bin"].corr(s["byb"]), len(s)))
        except Exception as e:  # noqa: BLE001
            log(f"  corr {sym} 失败: {str(e)[:120]}")
    corr_rows.sort(key=lambda t: -t[1])
    log("与 Binance 永续日收盘相关系数 (重叠期, top5):")
    for sym, c, n in corr_rows[:5]:
        log(f"  {sym:9s} corr={c:.6f}  (n={n} 天)")
    log(f"数据目录大小: {dir_size(data_root)/1e9:.2f} GB")
    os.makedirs(os.path.join(HERE, "_tmp"), exist_ok=True)
    with open(os.path.join(HERE, "_tmp", "bybit_kline_results.json"), "w",
              encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    log("done")


if __name__ == "__main__":
    main()
