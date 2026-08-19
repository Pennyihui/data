# -*- coding: utf-8 -*-
"""
build_multi_coin_features.py — 多币 1h 特征表
===============================================

对 data_new/klines_1h/ 的 Top N 币 1h K线:
  1. 复用 calculate_indicator_v2.Hourly1HIndicators 计算 200+ 技术指标 (1h_ 前缀)
  2. 合并 multi_symbol/{SYM}/ 的衍生品数据:
     资金费率(8h->1h ffill, 全历史) / OI / 多空比 / 大户持仓比 / 主动买卖 / 标记价(近21天)
  3. 输出 features_1h/{SYM}.csv (timestamp 列 + 全部特征)

防未来约定: 辅助列对齐到 1h 桶后仅向前填充(ffill), 不使用未来值。
用法: python build_multi_coin_features.py [--limit 100] [--out-dir data_new/features_1h]
"""

import argparse
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from calculate_indicator_v2 import Hourly1HIndicators  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
KLINE_DIR = os.path.join(ROOT, "data_new", "klines_1h")
MULTI_DIR = os.path.join(ROOT, "data_new", "additional", "multi_symbol")

COL_MAP = {
    "Open Time": "timestamp", "Open": "Open", "High": "High", "Low": "Low",
    "Close": "Close", "Volume": "Volume", "Close Time": "close_timestamp",
    "Quote Asset Volume": "quote_volume", "Number of Trades": "trades",
    "Taker Buy Base Asset Volume": "taker_buy_volume",
    "Taker Buy Quote Asset Volume": "taker_buy_quote_volume",
}
KEEP = ["Open", "High", "Low", "Close", "Volume", "quote_volume", "trades",
        "taker_buy_volume", "taker_buy_quote_volume"]


def load_1h(path):
    df = pd.read_csv(path).rename(columns=COL_MAP)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df[KEEP] = df[KEEP].astype(float)  # 强制 float64, 避免 talib "input array type is not double"
    return df[KEEP]


def merge_derivatives(base, sym):
    """合并衍生品数据到 1h 主表。返回 (base, 合并的列名列表)。"""
    d = os.path.join(MULTI_DIR, sym)
    if not os.path.isdir(d):
        return base, []
    merged_cols = []

    def join_file(fname, tcol, freq, prefix, cols):
        nonlocal base
        p = os.path.join(d, fname)
        if not os.path.exists(p):
            return
        df = pd.read_csv(p, parse_dates=[tcol]).set_index(tcol)
        df = df[cols]
        df = df.rename(columns={c: f"{prefix}_{c}" for c in df.columns})
        base = base.join(df.resample(freq).last(), how="left")
        merged_cols.extend(df.columns)

    join_file("funding_rate.csv", "funding_time", "1h", "funding",
              ["funding_rate", "mark_price"])
    join_file("open_interest_1h.csv", "time", "1h", "oi",
              ["sumOpenInterest", "sumOpenInterestValue"])
    join_file("global_ls_account_ratio_1h.csv", "time", "1h", "glsr",
              ["longAccount", "longShortRatio", "shortAccount"])
    join_file("top_trader_ls_account_ratio_1h.csv", "time", "1h", "tlsr_acct",
              ["longAccount", "longShortRatio", "shortAccount"])
    join_file("top_trader_ls_position_ratio_1h.csv", "time", "1h", "tlsr_pos",
              ["longAccount", "longShortRatio", "shortAccount"])
    join_file("taker_buy_sell_ratio_1h.csv", "time", "1h", "taker",
              ["buySellRatio", "sellVol", "buyVol"])
    join_file("mark_price_klines_1h.csv", "open_time", "1h", "mark",
              ["open", "high", "low", "close"])

    # 辅助列在 base 时间轴上向前填充(不用未来值)
    base_cols = set(KEEP)
    for c in base.columns:
        if c not in base_cols:
            base[c] = base[c].ffill()
    return base, merged_cols


def main():
    ap = argparse.ArgumentParser(description="多币 1h 特征表构建")
    ap.add_argument("--limit", type=int, default=100, help="处理币数上限")
    ap.add_argument("--out-dir", default="data_new/features_1h")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    symbols = sorted(f[:-4] for f in os.listdir(KLINE_DIR) if f.endswith(".csv"))
    symbols = symbols[: args.limit]
    print(f"目标: {len(symbols)} 个币 -> {out_dir}", flush=True)

    indicator = Hourly1HIndicators()
    t0 = time.time()
    ok, skipped = 0, 0
    for i, sym in enumerate(symbols, 1):
        out_p = os.path.join(out_dir, f"{sym}.csv")
        if os.path.exists(out_p) and os.path.getsize(out_p) > 0:
            skipped += 1
            continue
        try:
            df = load_1h(os.path.join(KLINE_DIR, f"{sym}.csv"))
            feat = indicator.calculate(df)
            feat, _ = merge_derivatives(feat, sym)
            feat = feat.reset_index().rename(columns={"timestamp": "timestamp"})
            feat.to_csv(out_p, index=False, encoding="utf-8-sig",
                        float_format="%.8f")
            ok += 1
            print(f"[{i}/{len(symbols)}] {sym}: {len(feat)} 行 x {feat.shape[1]} 列 "
                  f"({feat.timestamp.min()} ~ {feat.timestamp.max()})", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[{i}/{len(symbols)}] {sym} 失败: {str(e)[:100]}", flush=True)
        if i % 10 == 0:
            print(f"  进度 {i}/{len(symbols)}, 已用 {(time.time()-t0)/60:.1f} 分钟", flush=True)

    print(f"\n完成: 成功 {ok}, 跳过 {skipped}, 耗时 {(time.time()-t0)/60:.1f} 分钟")
    print(f"输出目录: {out_dir}")


if __name__ == "__main__":
    main()
