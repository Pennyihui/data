# -*- coding: utf-8 -*-
"""验证链上扩展: 各链各币行数 / 聚合表 / solana_snapshot / manifest。"""
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_foundation.config import CERTIFIED_DIR, L1_DIR, RAW_DIR  # noqa: E402

print("=" * 70)
print("1) raw 批次清单 (meta)")
for venue, dataset in [("ethereum", "erc20_transfer_logs"),
                       ("arbitrum", "erc20_transfer_logs"),
                       ("solana", "solana_snapshot")]:
    root = os.path.join(RAW_DIR, venue, dataset)
    if not os.path.isdir(root):
        print(f"  {venue}/{dataset}: 无 raw 目录")
        continue
    for ingest in sorted(os.listdir(root)):
        d = os.path.join(root, ingest)
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.endswith(".meta.json"):
                with open(os.path.join(d, f), encoding="utf-8") as fh:
                    m = json.load(fh)
                print(f"  {venue}/{dataset}/{ingest}/{f.split('.meta')[0]}: "
                      f"rows? bytes={m['file_size_bytes']} "
                      f"src={str(m['source'])[:70]}")

print("=" * 70)
print("2) token_transfer L1/certified 各链各币行数")
for venue in ("ethereum", "arbitrum"):
    p = os.path.join(CERTIFIED_DIR, "token_transfer", venue, "all", "data.parquet")
    if not os.path.exists(p):
        p = os.path.join(L1_DIR, "token_transfer", venue, "data.parquet")
    if not os.path.exists(p):
        print(f"  {venue}: 无数据")
        continue
    df = pd.read_parquet(p)
    g = df.groupby(["chain_id", "token"]).size()
    for (c, t), n in g.items():
        print(f"  {c} {t}: {n} 行")
    print(f"  时间范围: {df['block_timestamp_utc'].min()} ~ {df['block_timestamp_utc'].max()}")

print("=" * 70)
print("3) onchain_daily_aggregate (L2 certified)")
for venue in ("ethereum", "arbitrum"):
    p = os.path.join(CERTIFIED_DIR, "onchain_daily_aggregate", venue, "all", "data.parquet")
    if not os.path.exists(p):
        p = os.path.join(L1_DIR, "onchain_daily_aggregate", venue, "data.parquet")
    if not os.path.exists(p):
        print(f"  {venue}: 无数据")
        continue
    df = pd.read_parquet(p)
    print(f"  [{venue}]")
    print(df[["chain_id", "token", "date_utc", "transfer_count", "volume_token",
              "large_transfer_count", "mint_count", "burn_count"]].to_string(index=False))

print("=" * 70)
print("4) solana_snapshot")
p = os.path.join(CERTIFIED_DIR, "solana_snapshot", "solana", "all", "data.parquet")
if not os.path.exists(p):
    p = os.path.join(L1_DIR, "solana_snapshot", "solana", "data.parquet")
if os.path.exists(p):
    df = pd.read_parquet(p)
    print(df.to_string(index=False))
    print(f"  is_suspect: {int(df['is_suspect'].sum()) if 'is_suspect' in df.columns else 'n/a'}")
else:
    print("  solana_snapshot 无数据!")

print("=" * 70)
print("5) L2 manifests")
for ds in ("token_transfer", "onchain_daily_aggregate", "solana_snapshot"):
    p = os.path.join(CERTIFIED_DIR, ds, "manifest.json")
    if not os.path.exists(p):
        print(f"  {ds}: 无 manifest")
        continue
    with open(p, encoding="utf-8") as fh:
        m = json.load(fh)
    print(f"  {ds}: status={m.get('certification_status')} "
          f"rows={m.get('row_count')} suspect={m.get('suspect_count')} "
          f"start={m.get('coverage_start')} end={m.get('coverage_end')}")

print("=" * 70)
print("6) gitignore 检查 (raw 大文件不入库)")
for venue, dataset in [("ethereum", "erc20_transfer_logs"), ("arbitrum", "erc20_transfer_logs")]:
    root = os.path.join(RAW_DIR, venue, dataset)
    if os.path.isdir(root):
        total = sum(os.path.getsize(os.path.join(r, f))
                    for r, _, fs in os.walk(root) for f in fs)
        print(f"  {venue}/{dataset}: {total/1e6:.1f} MB (应已 gitignore)")
