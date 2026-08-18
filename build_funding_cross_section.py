# -*- coding: utf-8 -*-
"""
build_funding_cross_section.py — 跨币种资金费率截面特征
========================================================

基于 multi_symbol/ 下各币的资金费率历史, 按 8h 结算时刻对齐, 生成市场级
"拥挤度截面"特征: 每个时刻所有币种资金费率的分布统计 + BTC/ETH 在分布中的位置。

输出: data_new/additional/funding_cross_section_8h.csv
列说明:
  time                    8h 结算时刻(UTC)
  n_symbols               参与统计的币数
  mean / median / std     资金费率分布
  q05/q25/q75/q95         分位数
  min / max / range       极值
  pct_long_crowded        费率>+0.01% 的币占比(多头拥挤, 多头付费)
  pct_short_crowded       费率<-0.01% 的币占比(空头拥挤)
  mean_abs                平均绝对费率(整体拥挤度)
  skew                    分布偏度(>0 右偏=极端多头拥挤)
  btc_funding / eth_funding  参考币费率
  btc_pctile / eth_pctile    参考币在截面中的百分位(0-1, 越接近1越拥挤)
  btc_z / eth_z              参考币相对截面的 z 分数
"""

import os

import numpy as np
import pandas as pd

D = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 "data_new", "additional", "multi_symbol")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "data_new", "additional", "funding_cross_section_8h.csv")


def load_all_funding():
    """读取每个币的 funding_rate.csv, 返回 {symbol: Series(funding_rate, index=8h时刻)}"""
    series = {}
    for sym in sorted(os.listdir(D)):
        p = os.path.join(D, sym, "funding_rate.csv")
        if not os.path.exists(p):
            continue
        df = pd.read_csv(p, parse_dates=["funding_time"])
        s = df.set_index("funding_time")["funding_rate"].dropna()
        s.index = s.index.floor("8h")  # 对齐到 8h 结算桶
        s = s[~s.index.duplicated(keep="last")]
        series[sym] = s
    return series


def build():
    series = load_all_funding()
    print(f"加载 {len(series)} 个币的资金费率")
    wide = pd.DataFrame(series).sort_index()
    print(f"截面宽表: {wide.shape[0]} 个时刻 x {wide.shape[1]} 个币 "
          f"({wide.index.min()} ~ {wide.index.max()})")

    q = wide.quantile([0.05, 0.25, 0.5, 0.75, 0.95], axis=1).T
    out = pd.DataFrame(index=wide.index)
    out["n_symbols"] = wide.count(axis=1)
    out["mean"] = wide.mean(axis=1)
    out["median"] = q[0.5]
    out["std"] = wide.std(axis=1)
    out["q05"] = q[0.05]
    out["q25"] = q[0.25]
    out["q75"] = q[0.75]
    out["q95"] = q[0.95]
    out["min"] = wide.min(axis=1)
    out["max"] = wide.max(axis=1)
    out["range"] = out["max"] - out["min"]
    out["pct_long_crowded"] = (wide > 0.0001).mean(axis=1)
    out["pct_short_crowded"] = (wide < -0.0001).mean(axis=1)
    out["mean_abs"] = wide.abs().mean(axis=1)
    out["skew"] = wide.skew(axis=1)

    for sym in ("BTCUSDT", "ETHUSDT"):
        if sym in wide.columns:
            col = sym[:-4].lower()
            out[f"{col}_funding"] = wide[sym]
            out[f"{col}_pctile"] = wide.le(wide[sym], axis=0).mean(axis=1)
            std = wide.std(axis=1).replace(0, np.nan)
            out[f"{col}_z"] = (wide[sym] - wide.mean(axis=1)) / std

    out = out.reset_index()
    out = out.rename(columns={out.columns[0]: "time"})
    out.to_csv(OUT, index=False, encoding="utf-8-sig", float_format="%.8f")
    print(f"截面特征已保存: {OUT} ({out.shape[0]} 行 x {out.shape[1]} 列)")
    return out


if __name__ == "__main__":
    out = build()
    print("\n最近 5 个时刻样本:")
    show = ["time", "n_symbols", "median", "mean_abs", "pct_long_crowded",
            "pct_short_crowded", "max", "min", "btc_funding", "btc_pctile", "btc_z"]
    print(out.tail(5)[show].to_string(index=False))
    print("\n全历史描述统计:")
    print(out[["median", "mean_abs", "pct_long_crowded", "pct_short_crowded", "skew"]]
          .describe().to_string())
