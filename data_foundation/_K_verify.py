# -*- coding: utf-8 -*-
"""
_K_verify.py — 子代理 K 收尾验证
================================
在 backfill 与 universe_builder 重建之后运行:
  1. certified spot instrument 总数 (应 = 62 + 本次成功数)
  2. universe_membership 三层 distinct symbol 数 (research/backtest/tradeable)
  3. 抽 3 个死币打印生命周期与行数
  4. certified market_candle_spot_1h 全体 suspect 总数 + universe_membership suspect
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

CERT = os.path.join("data", "l2", "certified")
SPOT_ROOT = os.path.join(CERT, "market_candle_spot_1h", "binance", "spot")
UNIVERSE = os.path.join("data", "l1", "listing_universe", "binance_vision",
                        "data.parquet")


def main() -> None:
    # --- 1) certified spot instrument 总数 + suspect 汇总 ---
    insts = sorted(os.listdir(SPOT_ROOT)) if os.path.isdir(SPOT_ROOT) else []
    print(f"certified spot instrument 总数: {len(insts)}")

    tot_rows = tot_suspect = tot_gap = 0
    dead_samples = []
    for inst in insts:
        p = os.path.join(SPOT_ROOT, inst, "interval=1h", "data.parquet")
        if not os.path.isfile(p):
            continue
        d = pd.read_parquet(p, columns=["symbol", "open_time_utc", "is_suspect",
                                        "is_gap", "source_batch_id"])
        tot_rows += len(d)
        tot_suspect += int(d["is_suspect"].sum())
        tot_gap += int(d["is_gap"].sum())
        dead_samples.append({
            "inst": inst,
            "symbol": d["symbol"].iloc[0],
            "rows": len(d),
            "first": str(d["open_time_utc"].min())[:10],
            "last": str(d["open_time_utc"].max())[:10],
            "suspect": int(d["is_suspect"].sum()),
            "batch": d["source_batch_id"].iloc[0],
        })
    print(f"  spot 全体行数: {tot_rows:,}  suspect: {tot_suspect:,}  gap: {tot_gap:,}")

    # --- 2) universe_membership 三层 distinct symbol ---
    um = pd.read_parquet(os.path.join(CERT, "universe_membership", "builder",
                                      "all", "data.parquet"))
    um["date_utc"] = pd.to_datetime(um["date_utc"], utc=True)
    print(f"\nuniverse_membership: {len(um):,} 行, "
          f"{um['date_utc'].min().date()} ~ {um['date_utc'].max().date()} "
          f"({um['date_utc'].dt.date.nunique()} 日)")
    for layer, col in (("research", "layer_research"), ("backtest", "layer_backtest"),
                       ("tradeable", "layer_tradeable")):
        n = int(um.loc[um[col] == True, "symbol"].nunique())  # noqa: E712
        dailymax = int(um[um[col] == True].groupby(um["date_utc"].dt.date)["symbol"].nunique().max())  # noqa: E712
        print(f"  {layer:<10}: distinct symbol={n:>5}  单日最多={dailymax:>5}")
    um_suspect = int(um["is_suspect"].sum()) if "is_suspect" in um.columns else -1
    print(f"  universe_membership suspect: {um_suspect}")

    # --- 3) 抽 3 个死币: 生命周期 + 行数 ---
    uni = pd.read_parquet(UNIVERSE)
    uni_d = uni.set_index("symbol")
    print("\n死币抽样 (delisted, 有 K 线):")
    pick = [s for s in dead_samples
            if uni_d.loc[s["symbol"], "status"] == "delisted"][:3]
    if len(pick) < 3:
        pick += dead_samples[: 3 - len(pick)]
    for s in pick:
        u = uni_d.loc[s["symbol"]]
        print(f"  {s['symbol']:<14} universe: {u['first_period']}~{u['last_period']} "
              f"({u['status']}) | K线: {s['first']} ~ {s['last']} "
              f"rows={s['rows']:,} suspect={s['suspect']} batch={s['batch']}")

    # --- 4) 空清单 (如有) ---
    if os.path.isfile("_K_empty_list.json"):
        emp = json.load(open("_K_empty_list.json", encoding="utf-8"))
        print(f"\n空清单 (Vision 无数据): {len(emp)} 个 -> {[e['symbol'] for e in emp[:20]]}")

    # --- 5) report 对照 ---
    if os.path.isfile("_K_backfill_report.json"):
        rp = json.load(open("_K_backfill_report.json", encoding="utf-8"))
        print(f"\n对照 report: ok={rp['ok']} skipped={rp['skipped']} "
              f"empty={rp['empty']} failed={rp['failed']} "
              f"| certified 应 = 62 + {rp['ok']} = {62 + rp['ok']} (实际 {len(insts)})")


if __name__ == "__main__":
    main()
