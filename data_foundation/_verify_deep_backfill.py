# -*- coding: utf-8 -*-
"""临时验证脚本 (子代理 B): 深回填结果核对, 运行后可删除。"""
import os

import pandas as pd
import pyarrow.parquet as pq

C = "data/l2/certified"
L1 = "data/l1"

print("=== certified market_candle_spot_1h (coinbase) ===")
for sym in ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]:
    p = os.path.join(C, "market_candle_spot_1h", "coinbase", "spot", sym,
                     "interval=1h", "data.parquet")
    df = pq.read_table(p).to_pandas()
    batches = sorted(df["source_batch_id"].unique())
    print(f"{sym}: rows={len(df)} suspect={int(df['is_suspect'].sum())} "
          f"gaps={int(df['is_gap'].sum())} "
          f"src_batches={len(batches)} (e.g. {batches[:2]}...{batches[-2:]})")

print()
print("=== certified 1d/1w (coinbase) suspect check ===")
for iv in ["1d", "1w"]:
    ds = f"market_candle_spot_{iv}"
    tot = 0
    for sym in ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]:
        p = os.path.join(C, ds, "coinbase", "spot", sym, f"interval={iv}",
                         "data.parquet")
        df = pq.read_table(p).to_pandas()
        tot += len(df)
        sus = int(df["is_suspect"].sum())
        print(f"  {sym} {iv}: rows={len(df)} suspect={sus}",
              end="")
        if sus:
            print(" reasons:", df[df["is_suspect"]]["quality_reason"]
                  .value_counts().to_dict(), end="")
        print()
    print(f"  total {ds}: {tot}")

print()
print("=== certified dvol_15m ===")
df = pq.read_table(os.path.join(C, "dvol_15m", "deribit", "all",
                                "data.parquet")).to_pandas()
print(f"rows={len(df)} suspect={int(df['is_suspect'].sum())}")
for cur, g in df.groupby("currency"):
    print(f"  {cur}: rows={len(g)} "
          f"{g['timestamp_utc'].min()} ~ {g['timestamp_utc'].max()} "
          f"dvol=[{g['dvol'].min():.4f},{g['dvol'].max():.4f}]")

print()
print("=== L1 文件存在性 ===")
for iv in ["1h", "1d", "1w"]:
    ds = f"market_candle_spot_{iv}"
    for sym in ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]:
        p = os.path.join(L1, ds, "coinbase", "spot", sym, f"interval={iv}",
                         "data.parquet")
        print(f"  {'OK ' if os.path.exists(p) else 'MISS'} {ds}/{sym}")
p = os.path.join(L1, "dvol_15m", "deribit", "all", "data.parquet")
print(f"  {'OK ' if os.path.exists(p) else 'MISS'} dvol_15m/deribit/all")

print()
print("=== manifests ===")
import json
for ds in ["market_candle_spot_1h", "market_candle_spot_1d",
           "market_candle_spot_1w", "dvol_15m"]:
    p = os.path.join(C, ds, "manifest.json")
    m = json.load(open(p, encoding="utf-8"))
    print(f"  {ds}: status={m['certification_status']} rows={m['row_count']} "
          f"coverage={m['coverage_start']} ~ {m['coverage_end']} "
          f"src_batches={len(m['source_batches'])}")
