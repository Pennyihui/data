# -*- coding: utf-8 -*-
"""Verification for delisted backfill (subagent G)."""
import json
import os
import sys
sys.path.insert(0, r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation")
import pandas as pd
import pyarrow.parquet as pq

BASE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation\data"
SYMS = ["DAIUSDT", "XMRUSDT", "LRCUSDT", "NFPUSDT", "TONUSDT",
        "HFTUSDT", "VANRYUSDT", "UTKUSDT", "LITUSDT"]

uni = pd.read_parquet(os.path.join(BASE, "l1/listing_universe/binance_vision/data.parquet"))
uni_map = {(r.symbol, r.status): r for _, r in uni.iterrows()}

print("== L1/L2 逐对核对 ==")
rows = []
for s in SYMS:
    inst = s[:-4] + "-USDT"
    p1 = os.path.join(BASE, f"l1/market_candle_spot_1h/binance/spot/{inst}/interval=1h/data.parquet")
    p2 = os.path.join(BASE, f"l2/certified/market_candle_spot_1h/binance/spot/{inst}/interval=1h/data.parquet")
    t1 = pq.read_table(p1, columns=["open_time_utc", "is_suspect", "is_gap"])
    d = t1.to_pandas()
    t2 = pq.read_table(p2)
    u = uni_map.get((s, "delisted"))
    fp, lp = (u.first_period, u.last_period) if u is not None else ("?", "?")
    cov_first, cov_last = str(d["open_time_utc"].min())[:10], str(d["open_time_utc"].max())[:10]
    fp_ok = cov_first[:7] == fp
    lp_ok = cov_last[:7] == lp
    print(f"{s:<10} L1={t1.num_rows:>6} L2={t2.num_rows:>6} suspect={int(d['is_suspect'].sum())} "
          f"gap={int(d['is_gap'].sum())} cov={cov_first}~{cov_last} "
          f"uni={fp}~{lp} fpOK={fp_ok} lpOK={lp_ok}")
    rows.append({"symbol": s, "l1_rows": t1.num_rows, "l2_rows": t2.num_rows,
                 "suspect": int(d["is_suspect"].sum()), "gap": int(d["is_gap"].sum()),
                 "cov_first": cov_first, "cov_last": cov_last,
                 "uni_first": fp, "uni_last": lp, "fp_ok": fp_ok, "lp_ok": lp_ok})

print("\n== L0 raw ==")
for s in SYMS:
    gz = os.path.join(BASE, f"raw/binance/spot_klines_1h/{s}_delisted_v1.csv.gz")
    mp = gz + ".meta.json"
    if os.path.isfile(gz) and os.path.isfile(mp):
        with open(mp, encoding="utf-8") as f:
            m = json.load(f)
        print(f"{s:<10} gz={os.path.getsize(gz):>9}B months={len(m['source']['months'])} "
              f"rows={m['row_count']} tu={m['timestamp_unit'][:20]}")
    else:
        print(f"{s:<10} MISSING")

print("\n== manifest ==")
with open(os.path.join(BASE, "l2/certified/market_candle_spot_1h/manifest.json"), encoding="utf-8") as f:
    man = json.load(f)
print("batches:", man["source_batches"])
print("coverage:", man["coverage_start"], "~", man["coverage_end"])
print("rows:", man["row_count"], "suspect:", man["suspect_count"],
      "gap:", man["gap_count"], "dup:", man["duplicate_count"])
print("note:", man["aggregation_rules"]["note"][:120], "...")

print("\n== 磁盘 ==")
import shutil
print("D: free GB:", round(shutil.disk_usage(BASE).free / 1e9, 2))

with open("_G_verify.json", "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
