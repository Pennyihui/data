# -*- coding: utf-8 -*-
"""
build_oi_cross_section.py — 跨币种 OI(未平仓合约) 截面特征
============================================================

基于 multi_symbol/ 下各币的 open_interest_1h.csv(最近~500小时),
按 1h 对齐, 生成市场级杠杆分布特征。

输出: data_new/additional/open_interest_cross_section_1h.csv
列说明:
  time                   1h 时刻(UTC)
  n_symbols              参与统计的币数
  total_oi_value         全市场未平仓合约总价值(USD)
  mean / median / std    分布
  q25 / q75              分位
  btc_oi_value           BTC 未平仓价值
  btc_share              BTC 占总 OI 比例
  btc_pctile             BTC 在截面中的百分位
  total_oi_chg_24h_pct   总 OI 24h 变化(%)
"""

import os

import pandas as pd

D = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 "data_new", "additional", "multi_symbol")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "data_new", "additional", "open_interest_cross_section_1h.csv")


def main():
    series = {}
    for sym in sorted(os.listdir(D)):
        p = os.path.join(D, sym, "open_interest_1h.csv")
        if not os.path.exists(p):
            continue
        df = pd.read_csv(p, parse_dates=["time"])
        s = df.set_index("time")["sumOpenInterestValue"].dropna()
        s = s[~s.index.duplicated(keep="last")]
        series[sym] = s
    print(f"加载 {len(series)} 个币的 OI")

    wide = pd.DataFrame(series).sort_index()
    print(f"OI 宽表: {wide.shape[0]} 时刻 x {wide.shape[1]} 币 "
          f"({wide.index.min()} ~ {wide.index.max()})")

    out = pd.DataFrame(index=wide.index)
    out["n_symbols"] = wide.count(axis=1)
    out["total_oi_value"] = wide.sum(axis=1)
    out["mean"] = wide.mean(axis=1)
    out["median"] = wide.median(axis=1)
    out["std"] = wide.std(axis=1)
    out["q25"] = wide.quantile(0.25, axis=1)
    out["q75"] = wide.quantile(0.75, axis=1)
    if "BTCUSDT" in wide.columns:
        out["btc_oi_value"] = wide["BTCUSDT"]
        out["btc_share"] = wide["BTCUSDT"] / out["total_oi_value"]
        out["btc_pctile"] = wide.le(wide["BTCUSDT"], axis=0).mean(axis=1)
    out["total_oi_chg_24h_pct"] = out["total_oi_value"].pct_change(24) * 100

    out = out.reset_index()
    out = out.rename(columns={out.columns[0]: "time"})
    out.to_csv(OUT, index=False, encoding="utf-8-sig", float_format="%.8f")
    print(f"OI 截面已保存: {OUT} ({out.shape[0]} 行 x {out.shape[1]} 列)")
    print("\n最近 3 行样本:")
    print(out.tail(3).to_string(index=False))


if __name__ == "__main__":
    main()
