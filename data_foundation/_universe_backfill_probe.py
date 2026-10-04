"""验证 universe_builder 在 2017 年能否正常产出 (listing/K线/CMC 数据覆盖检查)。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd  # noqa: E402

from data_foundation import universe_builder as ub  # noqa: E402

# 抽 2017 年 3 个日期测试
for d in ["2017-07-15", "2017-09-01", "2017-12-01"]:
    df = ub.build_universe(d)
    if df.empty:
        print(f"{d}: 空 (无 research 成员)")
        continue
    r = int(df["layer_research"].sum())
    b = int(df["layer_backtest"].sum())
    t = int(df["layer_tradeable"].sum())
    mcap_known = int(df["market_cap_usd"].notna().sum())
    kline = int(df["avg_volume_30d_usd"].notna().sum())
    print(f"{d}: research={r} backtest={b} tradeable={t} | "
          f"有K线={kline} 有市值={mcap_known}")
    # 抽几个 symbol 看生命周期
    top = df.nlargest(3, "age_days")[["symbol", "age_days", "first_trade_date"]]
    for _, row in top.iterrows():
        print(f"    {row['symbol']:<14} age={int(row['age_days']):>4}d "
              f"first={str(row['first_trade_date'])[:10]}")
