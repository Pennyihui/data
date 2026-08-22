# -*- coding: utf-8 -*-
"""
_vision_verify_final.py — 回填收尾验证 (子代理 E)
==================================================
打印:
  1. 每 symbol 五份数据集覆盖起止/行数 (读 L1)
  2. 每数据集跨 symbol 汇总 (certified 读, 行数/去重/可疑/起止)
  3. 与旧 21 天 API 数据衔接处的重叠冲突 (state.overlap)
  4. 缺失日清单 (state.missing_days) + L0 批次统计
  5. certified manifest 检查
"""
from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import pandas as pd
import pyarrow.parquet as pq

from data_foundation.config import L1_DIR, MVP_ASSETS, CERTIFIED_DIR
from data_foundation.l0 import list_raw_batches
from data_foundation.l1 import instrument_id

DATASETS = ["derivatives_open_interest", "derivatives_ratio_glsr",
            "derivatives_ratio_tlsr_acct", "derivatives_ratio_tlsr_pos",
            "derivatives_ratio_taker"]
ALL_SYMS = [f"{a}USDT" for a in MVP_ASSETS]


def load_l1(ds, sym):
    inst = instrument_id(sym)
    p = os.path.join(L1_DIR, ds, "binance", inst, "data.parquet")
    if not os.path.exists(p):
        return None
    df = pq.read_table(p).to_pandas()
    for c in ["timestamp_utc", "data_available_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


def load_cert(ds, sym):
    inst = instrument_id(sym)
    p = os.path.join(CERTIFIED_DIR, ds, "binance", inst, "data.parquet")
    if not os.path.exists(p):
        return None
    return pq.read_table(p).to_pandas()


def main():
    state = {}
    if os.path.exists(os.path.join(_HERE, "_vision_state.json")):
        state = json.load(open(os.path.join(_HERE, "_vision_state.json"),
                               encoding="utf-8"))

    print("=" * 100)
    print("1) 每 symbol 覆盖 (L1, 1h)")
    print("=" * 100)
    for sym in ALL_SYMS:
        df = load_l1("derivatives_open_interest", sym)
        if df is None:
            print(f"  {sym}: L1 OI 缺失!")
            continue
        src = df["source_batch_id"].value_counts().to_dict()
        print(f"  {sym}: OI {len(df):>7} 行  "
              f"{df['timestamp_utc'].min()} ~ {df['timestamp_utc'].max()}  "
              f"src={src}")

    print()
    print("=" * 100)
    print("2) 每数据集汇总 (certified, 跨 15 symbol)")
    print("=" * 100)
    for ds in DATASETS:
        total = 0
        dups = 0
        suspects = 0
        start = end = None
        per_sym = {}
        for sym in ALL_SYMS:
            df = load_cert(ds, sym)
            if df is None:
                per_sym[sym] = "MISS"
                continue
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
            total += len(df)
            keys = ["timestamp_utc", "metric"] if "metric" in df.columns \
                else ["timestamp_utc"]
            dups += int(df[keys].duplicated().sum())
            suspects += int(df["is_suspect"].sum())
            s0, s1 = df["timestamp_utc"].min(), df["timestamp_utc"].max()
            start = s0 if start is None or s0 < start else start
            end = s1 if end is None or s1 > end else end
            per_sym[sym] = f"{len(df)}"
        print(f"  {ds}: total={total} dup={dups} suspect={suspects}")
        print(f"        coverage {start} ~ {end}")
        print(f"        per-sym: {per_sym}")

    print()
    print("=" * 100)
    print("3) 衔接处重叠冲突 (Vision 历史 vs 旧 21 天 API 行, 保留现有行)")
    print("=" * 100)
    ov = state.get("overlap", {})
    if not ov:
        print("  (state 无 overlap 记录 — 尚未 rebuild 或未完成)")
    for sym, rep in sorted(ov.items()):
        print(f"  {sym}: {rep}")

    print()
    print("=" * 100)
    print("4) L0 批次 + 缺失日")
    print("=" * 100)
    metas = list_raw_batches("binance", "futures_metrics")
    by_sym = {}
    for m in metas:
        s = m["source"].get("symbol", "?")
        by_sym.setdefault(s, []).append(m)
    print(f"  月度批次总数: {len(metas)}")
    for s in sorted(by_sym):
        ms = sorted(m["batch_id"].split("_")[-1] for m in by_sym[s])
        missing = sum(len(m["source"].get("missing_days", []))
                      for m in by_sym[s])
        print(f"  {s}: {len(ms)} 个月度批次 "
              f"{ms[0]}..{ms[-1]} 缺失日合计 {missing}")
    md = state.get("missing_days", [])
    print(f"  state.missing_days: {len(md)} 条")
    for x in md[:15]:
        print(f"    {x}")

    print()
    print("=" * 100)
    print("5) certified manifest")
    print("=" * 100)
    for ds in DATASETS:
        p = os.path.join(CERTIFIED_DIR, ds, "manifest.json")
        if not os.path.exists(p):
            print(f"  {ds}: manifest 缺失!")
            continue
        m = json.load(open(p, encoding="utf-8"))
        agg = m.get("aggregation_rules", {})
        depth = agg.get("depth", "?")
        print(f"  {ds}: rows={m['row_count']} suspect={m['suspect_count']} "
              f"coverage={m['coverage_start']} ~ {m['coverage_end']}")
        print(f"        depth: {depth}")
        print(f"        src_batches: {len(m['source_batches'])} 条 (前2): "
              f"{m['source_batches'][:2]}")


if __name__ == "__main__":
    main()
