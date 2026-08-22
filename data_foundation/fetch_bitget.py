# -*- coding: utf-8 -*-
"""
fetch_bitget.py — Bitget 资金费 (探测结论: v2 history-fund-rate 仅返回最近 ~100 条/币,
分页参数 startTime/endTime/idLessThan 均被忽略, v1 已下线) + OI 快照探测。
写 L0 raw: raw/bitget/derivatives_funding (batch {SYM}_v1)
幂等: 已存在的 batch_id 跳过。
用法: python fetch_bitget.py [SYM...]
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
SUMMARY = os.path.join(CACHE, "bitget_fetch_summary.json")

BASE = "https://api.bitget.com/api/v2/mix/market"
SYMS = [f"{a}USDT" for a in MVP_ASSETS]

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def j(url, params=None):
    return netpath.fetch_json(url, params=params, retries=8, timeout=30)

def _dt3(ms):
    ms = int(ms)
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{d:%Y-%m-%d %H:%M:%S}.{ms % 1000:03d}"

def fetch_funding(sym):
    """单页最多 100 条 (约 33 天, 8h 间隔)。"""
    r = j(f"{BASE}/history-fund-rate",
          {"symbol": sym, "productType": "USDT-FUTURES", "pageSize": 100})
    if r.get("code") != "00000":
        raise RuntimeError(f"{sym} funding code={r.get('code')} msg={r.get('msg')}")
    data = r.get("data") or []
    rows = []
    for item in data:
        rows.append((int(item["fundingTime"]), float(item["fundingRate"])))
    rows.sort()
    return rows

def write_funding_raw(sym, rows):
    tmp = os.path.join(CACHE, f"bitget_{sym}_funding.csv")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["funding_time", "funding_rate", "mark_price"])
        for ts, rate in rows:
            w.writerow([_dt3(ts), f"{rate:.10g}", ""])
    dst = write_raw_file(tmp, "bitget", "derivatives_funding", f"{sym}_v1",
                         {"api": f"{BASE}/history-fund-rate", "symbol": sym,
                          "productType": "USDT-FUTURES", "pageSize": 100,
                          "note": "Bitget v2 公开 API 仅返回最近 ~100 条资金费 "
                                  "(~33 天); startTime/endTime/idLessThan 分页参数实测被忽略, "
                                  "v1 API 已下线; 全历史需付费数据源",
                          "fetched_at": datetime.now(timezone.utc).isoformat()},
                         timestamp_unit="ms")
    return dst

def probe_oi(sym):
    """OI 历史探测: 返回 (has_history, current_oi 或错误信息)。"""
    try:
        r = j(f"{BASE}/open-interest", {"symbol": sym, "productType": "USDT-FUTURES"})
        return {"ok": r.get("code") == "00000", "response": r.get("data")}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:150]}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("syms", nargs="*")
    args = ap.parse_args()
    syms = [s.upper() for s in args.syms] if args.syms else SYMS

    netpath.probe(timeout=8, ref_urls={"*": "https://api.bitget.com/api/v2/public/time"})

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
        existing = {m.get("batch_id") for m in list_raw_batches("bitget", "derivatives_funding")}
        bid = f"{sym}_v1"
        if bid in existing:
            log(f"  funding batch {bid} 已存在, 跳过")
            entry["funding"] = "skipped(exists)"
        else:
            try:
                rows = fetch_funding(sym)
                if not rows:
                    log(f"  [warn] {sym} funding 空")
                    entry["funding"] = "empty"
                else:
                    dst = write_funding_raw(sym, rows)
                    d0 = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
                    d1 = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
                    log(f"  funding: {len(rows)} 条 {d0:%Y-%m-%d} -> {d1:%Y-%m-%d} -> {dst}")
                    entry["funding"] = {"rows": len(rows), "start": str(d0), "end": str(d1)}
            except Exception as e:  # noqa: BLE001
                log(f"  [ERR] {sym} funding 失败: {str(e)[:200]}")
                entry["funding"] = f"fail: {str(e)[:120]}"
        # OI 探测 (仅一次, 记录结论)
        oi = probe_oi(sym)
        entry["oi_probe"] = "has_snapshot_only" if oi.get("ok") else f"fail: {oi.get('error', '')[:100]}"
        log(f"  oi probe: {entry['oi_probe']}")
        with open(SUMMARY, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        time.sleep(0.3)
    log("done")

if __name__ == "__main__":
    main()
