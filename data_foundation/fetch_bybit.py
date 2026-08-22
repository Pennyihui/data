# -*- coding: utf-8 -*-
"""
fetch_bybit.py — Bybit 全历史抓取 (L0 raw):
  1) funding/history: category=linear, endTime 游标回退 -> 全历史资金费
  2) open-interest:   category=linear, intervalTime=1h, cursor 游标 -> 可回溯深度探测
  3) mark-price-kline: 与 OI 同跨度 1h 收盘价, 用于 OI USD 折算 (缓存到 scratch)
写 L0 raw: raw/bybit/derivatives_funding / derivatives_oi_cross
幂等: 已存在的 batch_id 跳过。
用法: python fetch_bybit.py [SYM...]  (默认全部 MVP)
"""
import os
import sys
import json
import time
import csv
import argparse
from datetime import datetime, timezone

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation import netpath
from data_foundation.config import MVP_ASSETS
from data_foundation.l0 import write_raw_file, list_raw_batches

CACHE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\_scratch_subA\cache"
os.makedirs(CACHE, exist_ok=True)
SUMMARY = os.path.join(CACHE, "bybit_fetch_summary.json")

BASE = "https://api.bybit.com/v5/market"
SYMS = [f"{a}USDT" for a in MVP_ASSETS]

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def j(url, params=None):
    return netpath.fetch_json(url, params=params, retries=8, timeout=30)

def _dt3(ms):
    ms = int(ms)
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{d:%Y-%m-%d %H:%M:%S}.{ms % 1000:03d}"

def now_ms():
    return int(time.time() * 1000)

# ---------------------------------------------------------------------------
def fetch_funding(sym):
    """funding/history endTime 回退 -> 全历史。返回 [(funding_time_ms, rate), ...] 升序。"""
    rows, end, pages = [], now_ms(), 0
    seen = set()
    while pages < 120:
        r = j(f"{BASE}/funding/history",
              {"category": "linear", "symbol": sym, "limit": 200, "endTime": end})
        if r.get("retCode") != 0:
            raise RuntimeError(f"{sym} funding retCode={r.get('retCode')} {r.get('retMsg')}")
        lst = (r.get("result") or {}).get("list") or []
        if not lst:
            break
        page_newest = int(lst[0]["fundingRateTimestamp"])
        if page_newest in seen and len(rows) > 0:
            break  # 无进展
        for item in lst:
            ts = int(item["fundingRateTimestamp"])
            if ts not in seen:
                seen.add(ts)
                rows.append((ts, float(item["fundingRate"])))
        oldest = min(int(x["fundingRateTimestamp"]) for x in lst)
        pages += 1
        if oldest <= 1546300800000:  # 2019-01-01
            break
        end = oldest - 1
        time.sleep(0.25)
    rows.sort()
    return rows

def write_funding_raw(sym, rows):
    tmp = os.path.join(CACHE, f"bybit_{sym}_funding.csv")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["funding_time", "funding_rate", "mark_price"])
        for ts, rate in rows:
            w.writerow([_dt3(ts), f"{rate:.10g}", ""])
    dst = write_raw_file(tmp, "bybit", "derivatives_funding", f"{sym}_v1",
                         {"api": f"{BASE}/funding/history", "symbol": sym,
                          "category": "linear", "limit": 200,
                          "pagination": "endTime cursor walk, full history",
                          "fetched_at": datetime.now(timezone.utc).isoformat()},
                         timestamp_unit="ms")
    return dst

# ---------------------------------------------------------------------------
def fetch_oi(sym, max_pages=100):
    """open-interest cursor 回退。返回 [(ts_ms, oi_contracts), ...] 升序去重。"""
    rows, cur, pages = [], None, 0
    seen = set()
    while pages < max_pages:
        params = {"category": "linear", "symbol": sym, "intervalTime": "1h", "limit": 200}
        if cur:
            params["cursor"] = cur
        r = j(f"{BASE}/open-interest", params)
        if r.get("retCode") != 0:
            raise RuntimeError(f"{sym} oi retCode={r.get('retCode')} {r.get('retMsg')}")
        res = r.get("result") or {}
        lst = res.get("list") or []
        if not lst:
            break
        added = 0
        for item in lst:
            ts = int(item["timestamp"])
            if ts not in seen:
                seen.add(ts)
                rows.append((ts, float(item["openInterest"])))
                added += 1
        pages += 1
        nxt = res.get("nextPageCursor")
        if not nxt or nxt == "0" or added == 0:
            break
        cur = nxt
        time.sleep(0.25)
    rows.sort()
    return rows

def write_oi_raw(sym, rows):
    tmp = os.path.join(CACHE, f"bybit_{sym}_oi.csv")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open_interest", "open_interest_value"])
        for ts, oi in rows:
            w.writerow([_dt3(ts), f"{oi:.8f}", ""])
    dst = write_raw_file(tmp, "bybit", "derivatives_oi_cross", f"{sym}_oi_v1",
                         {"api": f"{BASE}/open-interest", "symbol": sym,
                          "category": "linear", "intervalTime": "1h", "limit": 200,
                          "pagination": "cursor walk",
                          "fetched_at": datetime.now(timezone.utc).isoformat()},
                         timestamp_unit="ms")
    return dst

# ---------------------------------------------------------------------------
def fetch_mark_klines(sym, start_ms, end_ms):
    """mark-price-kline 1h, [start,end] 窗口回退。返回 dict ts_ms -> close。"""
    out = {}
    end = end_ms
    pages = 0
    while pages < 80 and end > start_ms:
        r = j(f"{BASE}/mark-price-kline",
              {"category": "linear", "symbol": sym, "interval": "60",
               "limit": 1000, "start": start_ms, "end": end})
        if r.get("retCode") != 0:
            raise RuntimeError(f"{sym} mark kline retCode={r.get('retCode')} {r.get('retMsg')}")
        lst = (r.get("result") or {}).get("list") or []
        if not lst:
            break
        for row in lst:
            out[int(row[0])] = float(row[4])  # close
        oldest = min(int(x[0]) for x in lst)
        if oldest <= start_ms:
            break
        end = oldest - 1
        pages += 1
        time.sleep(0.25)
    return out

# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("syms", nargs="*")
    ap.add_argument("--max-oi-pages", type=int, default=400)
    args = ap.parse_args()
    syms = [s.upper() for s in args.syms] if args.syms else SYMS

    netpath.probe(timeout=8, ref_urls={"*": "https://api.bybit.com/v5/market/time"})

    summary = {}
    if os.path.exists(SUMMARY):
        try:
            with open(SUMMARY, encoding="utf-8") as f:
                summary = json.load(f)
        except Exception:
            summary = {}

    for sym in syms:
        log(f"===== {sym} =====")
        entry = summary.setdefault(sym, {})
        # 1) funding
        existing = {m.get("batch_id") for m in list_raw_batches("bybit", "derivatives_funding")}
        bid = f"{sym}_v1"
        if bid in existing:
            log(f"  funding batch {bid} 已存在, 跳过")
            entry["funding"] = "skipped(exists)"
        else:
            try:
                t0 = time.time()
                rows = fetch_funding(sym)
                if not rows:
                    log(f"  [warn] {sym} funding 空 (可能是新上市/已下架)")
                    entry["funding"] = "empty"
                else:
                    dst = write_funding_raw(sym, rows)
                    d0 = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
                    d1 = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
                    log(f"  funding: {len(rows)} 条 {d0:%Y-%m-%d} -> {d1:%Y-%m-%d} ({time.time()-t0:.0f}s) -> {dst}")
                    entry["funding"] = {"rows": len(rows), "start": str(d0), "end": str(d1)}
            except Exception as e:  # noqa: BLE001
                log(f"  [ERR] {sym} funding 失败: {str(e)[:200]}")
                entry["funding"] = f"fail: {str(e)[:120]}"
        # 2) OI
        existing = {m.get("batch_id") for m in list_raw_batches("bybit", "derivatives_oi_cross")}
        bid = f"{sym}_oi_v1"
        if bid in existing:
            log(f"  oi batch {bid} 已存在, 跳过")
            entry["oi"] = "skipped(exists)"
        else:
            try:
                t0 = time.time()
                rows = fetch_oi(sym, max_pages=args.max_oi_pages)
                if not rows:
                    log(f"  [warn] {sym} OI 历史为空 (深度 0)")
                    entry["oi"] = "empty"
                else:
                    dst = write_oi_raw(sym, rows)
                    d0 = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
                    d1 = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
                    log(f"  oi: {len(rows)} 条 {d0:%Y-%m-%d} -> {d1:%Y-%m-%d} (深度 {(rows[-1][0]-rows[0][0])/86400000:.0f} 天, {time.time()-t0:.0f}s) -> {dst}")
                    entry["oi"] = {"rows": len(rows), "start": str(d0), "end": str(d1),
                                   "depth_days": round((rows[-1][0] - rows[0][0]) / 86400000)}
            except Exception as e:  # noqa: BLE001
                log(f"  [ERR] {sym} OI 失败: {str(e)[:200]}")
                entry["oi"] = f"fail: {str(e)[:120]}"
        with open(SUMMARY, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    log("done")

if __name__ == "__main__":
    main()
