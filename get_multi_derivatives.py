# -*- coding: utf-8 -*-
"""
get_multi_derivatives.py — 多币种衍生品数据获取
=================================================

对 Binance U本位永续 Top N 交易对, 逐个获取与 BTC 相同的一套衍生品数据:
  资金费率全历史 / OI / 全市场多空账户比 / 大户多空账户比 / 大户多空持仓比 /
  主动买卖量比 / 标记价K线 / 溢价快照

用法:
    python get_multi_derivatives.py [--top 100] [--out-dir data_new/additional/multi_symbol]

特性:
  - 断点续传: 已完整存在的币自动跳过, 重跑只补缺失/失败的币
  - 每个币输出到 {out_dir}/{SYMBOL}/ 子目录
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd

from get_additional_data import (fetch_funding_rate_history,
                                 fetch_futures_data_series,
                                 fetch_mark_price_klines,
                                 fetch_premium_index)

SERIES = [
    ("openInterestHist", "open_interest_1h.csv", "未平仓合约(1h)"),
    ("globalLongShortAccountRatio", "global_ls_account_ratio_1h.csv", "全市场多空账户比(1h)"),
    ("topLongShortAccountRatio", "top_trader_ls_account_ratio_1h.csv", "大户多空账户比(1h)"),
    ("topLongShortPositionRatio", "top_trader_ls_position_ratio_1h.csv", "大户多空持仓比(1h)"),
    ("takerlongshortRatio", "taker_buy_sell_ratio_1h.csv", "主动买卖量比(1h)"),
]

FILES_PER_SYMBOL = 1 + len(SERIES) + 2  # funding + 5 series + mark + premium


def symbol_complete(out_dir, sym):
    """该币 8 个文件是否已全部存在且非空。"""
    d = os.path.join(out_dir, sym)
    if not os.path.isdir(d):
        return False
    targets = ["funding_rate.csv"] + [f for _, f, _ in SERIES] + \
              ["mark_price_klines_1h.csv", "premium_index_snapshot.csv"]
    for t in targets:
        p = os.path.join(d, t)
        if not os.path.exists(p) or os.path.getsize(p) == 0:
            return False
    return True


def save(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", float_format="%.8f")


def fetch_one_symbol(sym, out_dir, ratio_period="1h"):
    """拉取单个币的全部衍生品数据, 返回 (成功文件数, 失败列表)。"""
    d = os.path.join(out_dir, sym)
    os.makedirs(d, exist_ok=True)
    ok, fail = 0, []

    def do(fname, fn):
        nonlocal ok
        try:
            df = fn()
            if df is None or df.empty:
                fail.append(f"{fname}(空)")
                return
            save(df, os.path.join(d, fname))
            ok += 1
        except Exception as e:  # noqa: BLE001
            fail.append(f"{fname}:{str(e)[:60]}")

    do("funding_rate.csv", lambda: fetch_funding_rate_history(sym))
    for kind, fname, _ in SERIES:
        do(fname, lambda k=kind: fetch_futures_data_series(k, sym, period=ratio_period))
    do("mark_price_klines_1h.csv", lambda: fetch_mark_price_klines(sym, "1h"))
    do("premium_index_snapshot.csv", lambda: fetch_premium_index(sym))
    return ok, fail


def main():
    ap = argparse.ArgumentParser(description="多币种衍生品数据获取")
    ap.add_argument("--top", type=int, default=100, help="取永续排行的前 N 个交易对")
    ap.add_argument("--out-dir", default="data_new/additional/multi_symbol")
    ap.add_argument("--ratio-period", default="1h")
    ap.add_argument("--ranking", default="data_new/additional/binance_futures_pair_ranking.csv",
                    help="永续排行 CSV 路径(用于确定 Top N)")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    rank = pd.read_csv(args.ranking)
    symbols = rank["symbol"].head(args.top).tolist()
    print(f"目标: Top {len(symbols)} 永续交易对 -> {out_dir}")

    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(),
                "ratio_period": args.ratio_period, "symbols": {}}
    skipped = 0
    t0 = time.time()
    for i, sym in enumerate(symbols, 1):
        if symbol_complete(out_dir, sym):
            skipped += 1
            continue
        print(f"[{i}/{len(symbols)}] {sym} ...", flush=True)
        ok, fail = fetch_one_symbol(sym, out_dir, args.ratio_period)
        manifest["symbols"][sym] = {"files_ok": ok, "failures": fail}
        if fail:
            print(f"    -> 成功 {ok}/8, 失败: {fail}", flush=True)
        time.sleep(0.3)

    with open(os.path.join(out_dir, "multi_symbol_manifest.json"), "w",
              encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"\n完成: {len(symbols)} 个币, 跳过已存在 {skipped} 个, 耗时 {time.time()-t0:.0f}s")
    print(f"输出目录: {out_dir}")


if __name__ == "__main__":
    main()
