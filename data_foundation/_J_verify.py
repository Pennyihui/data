# -*- coding: utf-8 -*-
"""
_J_verify.py — 子代理 J 收尾验证 (重建快照后运行)
==================================================
打印:
  1. 预筛口径统计 (active/delisted 分布, 成功/失败清单) — 从 _J_backfill_report.json
  2. 重建后 universe_membership 各层 distinct symbol 数
  3. 按年 tradeable distinct 数 vs CMC 基准 (31/44/56/76)
  4. LUNAUSDT 生命周期 (入库 + 2022-05 崩盘 + 是否 research/backtest/tradeable)
  5. certified market_candle suspect=0 抽查 + universe suspect=0
  6. load_universe 冒烟
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

from data_foundation.reader import load_universe, load_candles

# CMC 基准: 历史任意日成交额 >=1M USD 的币级数量 (全所口径)
CMC_BENCH = {2019: 31, 2021: 44, 2023: 56, 2026: 76}


def main() -> None:
    # 1) 抓取报告
    if os.path.exists("_J_backfill_report.json"):
        rep = json.load(open("_J_backfill_report.json", encoding="utf-8"))
        targets = rep["targets"]
        res = rep["results"]
        fails = rep["failures"]
        ok = [r for r in res if r.get("status") == "ok"]
        sk = [r for r in res if r.get("status") == "skipped"]
        print("=" * 78)
        print("1) 抓取报告 (子代理 J)")
        print("=" * 78)
        print(f"预筛目标: {len(targets)}  (active={sum(1 for t in targets if t['status']=='active')}, "
              f"delisted={sum(1 for t in targets if t['status']=='delisted')})")
        print(f"成功 {len(ok)} / 跳过(幂等) {len(sk)} / 失败 {len(fails)}")
        if fails:
            print("失败清单:")
            for f in fails:
                print(f"  {f['symbol']}: {f['error'][:120]}")
        print("成功清单:", ", ".join(sorted(r["symbol"] for r in ok)))
        print("跳过清单:", ", ".join(sorted(r["symbol"] for r in sk)))
    else:
        print("未找到 _J_backfill_report.json")

    # 2) 各层 distinct symbol
    print()
    print("=" * 78)
    print("2) universe_membership 各层 distinct symbol (重建后)")
    print("=" * 78)
    full = load_universe(layer="research")
    if full.empty:
        print("  快照为空! 请先重建")
        return
    for layer, col in [("research", "layer_research"), ("backtest", "layer_backtest"),
                       ("tradeable", "layer_tradeable")]:
        df = load_universe(layer=layer)
        n = df["symbol"].nunique()
        tot = len(df)
        print(f"  {layer:<10} distinct={n:>4}  成员日={tot:>9,}")
    print(f"  日期范围: {full['date_utc'].min().date()} ~ {full['date_utc'].max().date()}")

    # 3) 按年 tradeable distinct vs CMC 基准
    print()
    print("=" * 78)
    print("3) 按年 tradeable distinct symbol 数 vs CMC 币级基准 (全所量, 仅对照)")
    print("=" * 78)
    tr = load_universe(layer="tradeable")
    tr["year"] = tr["date_utc"].dt.year
    per_year = tr.groupby("year")["symbol"].nunique()
    # 累计口径: 该年及以前曾 tradeable 过的 distinct
    years = sorted(per_year.index)
    cum = {}
    seen: set = set()
    for y in years:
        seen |= set(tr[tr["year"] == y]["symbol"])
        cum[y] = len(seen)
    for y in sorted(set(list(per_year.index) + list(CMC_BENCH.keys()))):
        py = f"{per_year.get(y, 0):>3}" if y in per_year.index else "  -"
        cy = f"{cum.get(y, 0):>3}" if y in cum else "  -"
        cb = CMC_BENCH.get(y, "-")
        print(f"  {y}: 当年tradeable={py}  累计={cy}   CMC基准累计={cb}")
    max_d = full["date_utc"].max().date()
    print(f"  (tradeable distinct 应 70±; 截至 {max_d} 累计 {len(seen)})")

    # 4) LUNAUSDT 生命周期
    print()
    print("=" * 78)
    print("4) LUNAUSDT 生命周期")
    print("=" * 78)
    lu = load_universe(layer="research", base_asset="LUNA")
    if lu.empty:
        print("  LUNA 不在 universe 中!")
    else:
        print(f"  LUNA research 成员行: {len(lu)}  "
              f"({lu['date_utc'].min().date()} ~ {lu['date_utc'].max().date()})")
        for s in ["2020-09-01", "2021-06-01", "2022-04-01", "2022-05-15", "2022-06-15",
                  "2023-06-01", str(max_d)]:
            row = lu[lu["date_utc"] == pd.Timestamp(s, tz="UTC").normalize()]
            if row.empty:
                print(f"  {s}: (非 research 成员)")
                continue
            r = row.iloc[0]
            print(f"  {s}: R={int(r['layer_research'])} B={int(r['layer_backtest'])} "
                  f"T={int(r['layer_tradeable'])}  "
                  f"avg_vol30d={r['avg_volume_30d_usd']:.3e}  "
                  f"gap={r['gap_ratio_30d']:.3f}")
    try:
        candles = load_candles("binance", "LUNA-USDT", "1h")
        print(f"  LUNA K线: {len(candles)} 行 "
              f"({pd.Timestamp(candles['open_time_utc'].min()).date()} ~ "
              f"{pd.Timestamp(candles['open_time_utc'].max()).date()})")
        may = candles[(candles["open_time_utc"] >= "2022-05-01")
                      & (candles["open_time_utc"] < "2022-06-01")]
        if len(may):
            print(f"  2022-05 崩盘段: {len(may)} 根, close 最低 "
                  f"{may['close'].min():.6f} (LUNA 归零)")
    except Exception as e:  # noqa: BLE001
        print(f"  LUNA K线读取失败: {e}")

    # 5) certified 质量
    print()
    print("=" * 78)
    print("5) certified 质量抽查 (K线 suspect / universe suspect)")
    print("=" * 78)
    import pyarrow.parquet as pq
    import glob as _glob
    suspect_total = 0
    n_inst = 0
    for p in _glob.glob(r"data/l2/certified/market_candle_spot_1h/binance/spot/*/interval=1h/data.parquet"):
        t = pq.read_table(p, columns=["is_suspect"])
        s = int(t.column("is_suspect").to_pandas().sum())
        suspect_total += s
        n_inst += 1
    print(f"  K线 certified: {n_inst} 个 instrument, suspect 合计 {suspect_total}")
    up = pq.read_table(r"data/l2/certified/universe_membership/builder/all/data.parquet",
                       columns=["is_suspect"])
    print(f"  universe certified suspect 合计: {int(up.column('is_suspect').to_pandas().sum())}")

    # 6) load_universe 冒烟
    print()
    print("=" * 78)
    print("6) load_universe 冒烟")
    print("=" * 78)
    t = load_universe(as_of=str(max_d), layer="tradeable")
    print(f"  load_universe(as_of={max_d}, layer='tradeable'): {len(t)} 个 symbol")
    btc = load_universe(layer="research", base_asset="BTC")
    print(f"  load_universe(layer='research', base_asset='BTC'): {len(btc)} 行")
    b = load_universe(as_of=str(max_d), layer="backtest", base_asset="ETH")
    print(f"  load_universe(as_of={max_d}, layer='backtest', base_asset='ETH'): {len(b)} 行")
    print()
    print("验证完成")


if __name__ == "__main__":
    main()
