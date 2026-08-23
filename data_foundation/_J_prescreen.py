# -*- coding: utf-8 -*-
"""
_J_prescreen.py — 子代理 J 预筛: CMC 历史日成交额 >= $1M 的币 -> Binance USDT 现货对
============================================================================
口径:
  - 源: ../data_new/additional/cmc_daily_volume_ranking.csv (币级全所量, CMC)
  - 阈值: 历史上任意一天 volume >= 1e6
  - 映射: {COIN}USDT -> listing_universe (binance_vision)
  - 排除: 已有 K 线的 24 个 (15 MVP + 9 G 回填下架)
  - 手动补: LUNAUSDT (2022 归零, CMC 排名文件已无记录, 任务指定核心样本)
输出:
  - 打印预筛统计与目标清单
  - 写 _J_targets.csv (symbol, status, first_period, last_period, cmc_max_vol, source)
"""
from __future__ import annotations

import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

CMC_CSV = r"../data_new/additional/cmc_daily_volume_ranking.csv"
UNIVERSE = r"data/l1/listing_universe/binance_vision/data.parquet"

HAVE_KLINES = [
    # 15 MVP (现役)
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT",
    "AAVEUSDT", "ARBUSDT", "POLUSDT",
    # 9 下架 (G 已回填)
    "DAIUSDT", "XMRUSDT", "LRCUSDT", "NFPUSDT", "TONUSDT", "HFTUSDT",
    "VANRYUSDT", "UTKUSDT", "LITUSDT",
]

# 任务指定核心样本 (CMC 排名文件已无记录, 但 2022-05 崩盘前成交额巨大)
MANUAL_ADD = ["LUNAUSDT"]


def main() -> None:
    cmc = pd.read_csv(CMC_CSV)
    lu = pd.read_parquet(UNIVERSE)
    lu_idx = lu.set_index("symbol")

    mx = cmc.groupby("symbol")["volume"].max()
    cand = sorted(mx[mx >= 1e6].index.tolist())
    print(f"CMC 历史任意日 >= $1M 的币: {len(cand)}")

    rows = []
    no_pair = []
    for c in cand:
        s = c + "USDT"
        if s in lu_idx.index:
            r = lu_idx.loc[s]
            rows.append({
                "symbol": s, "status": r["status"], "first_period": r["first_period"],
                "last_period": r["last_period"], "cmc_max_vol": float(mx[c]),
                "source": "cmc",
            })
        else:
            no_pair.append(s)
    for s in MANUAL_ADD:
        if s in lu_idx.index and s not in HAVE_KLINES:
            r = lu_idx.loc[s]
            rows.append({
                "symbol": s, "status": r["status"], "first_period": r["first_period"],
                "last_period": r["last_period"], "cmc_max_vol": float("nan"),
                "source": "manual_luna",
            })

    df = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
    targets = df[~df["symbol"].isin(HAVE_KLINES)].copy()

    print(f"映射到 Binance USDT 对: {len(rows)} (无 Binance 现货 USDT 对: {len(no_pair)})")
    print(f"排除已有 K 线 24 个后目标: {len(targets)}")
    print(f"  目标 active: {(targets['status']=='active').sum()}  delisted: {(targets['status']=='delisted').sum()}")
    if no_pair:
        print("无 Binance USDT 对(过选, 忽略):", ", ".join(sorted(no_pair)))

    print("\n=== 目标清单 ===")
    for _, r in targets.iterrows():
        print(f"  {r['symbol']:<18} {r['status']:<9} {r['first_period']} ~ "
              f"{r['last_period']}  cmc_max={r['cmc_max_vol']:.3e}")

    targets.to_csv("_J_targets.csv", index=False)
    print(f"\n已写 _J_targets.csv ({len(targets)} 行)")


if __name__ == "__main__":
    main()
