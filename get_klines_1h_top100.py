# -*- coding: utf-8 -*-
"""
get_klines_1h_top100.py — Top100 永续币 1h K线全历史
=====================================================

基于 binance_futures_pair_ranking.csv 的 Top N 永续交易对,
抓取每个币的 1h K线全历史(上市日至今), 输出到 data_new/klines_1h/{SYMBOL}.csv。

- 格式与 GitHub 仓库已有 *_1h.csv 完全一致(12列, Open Time 为 UTC 字符串)
- 现货优先, 现货无此交易对时用合约 K线兜底
- 断点续传: 已存在且非空的文件自动跳过

用法: python get_klines_1h_top100.py [--top 100] [--out-dir data_new/klines_1h]
"""

import argparse
import os
import time

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (data-pipeline)"}
SPOT = "https://api.binance.com/api/v3/klines"
FAPI = "https://fapi.binance.com/fapi/v1/klines"
COLS = ["Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]


def _get_json(url, params, retries=8, timeout=25):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            if r.status_code == 429:
                time.sleep(5)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(1.5 * (i + 1), 12))
    raise RuntimeError(str(last)[:150])


def fetch_1h(symbol):
    """返回 (df, 来源)。现货优先, 合约兜底。"""
    end_ms = int(time.time() * 1000)
    rows, cursor, src = [], 0, None
    for url, name in [(SPOT, "spot"), (FAPI, "futures")]:
        try:
            rows, cursor = [], 0
            while True:
                data = _get_json(url, {"symbol": symbol, "interval": "1h",
                                       "startTime": cursor, "limit": 1000})
                if not data:
                    break
                rows.extend(data)
                cursor = data[-1][0] + 1
                time.sleep(0.08)
            src = name
            break
        except Exception:  # noqa: BLE001
            continue  # 现货失败 -> 试合约; 合约失败 -> 抛给上层
    if src is None:
        raise RuntimeError("spot 与 futures 均获取失败")
    df = pd.DataFrame(rows, columns=COLS)
    df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")
    df["Ignore"] = df["Ignore"].astype("int64")
    return df, src


def main():
    ap = argparse.ArgumentParser(description="Top N 永续币 1h K线全历史")
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--out-dir", default="data_new/klines_1h")
    ap.add_argument("--ranking", default="data_new/additional/binance_futures_pair_ranking.csv")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    rank = pd.read_csv(args.ranking)
    symbols = rank["symbol"].head(args.top).tolist()
    print(f"目标: Top {len(symbols)} 永续币 1h 全历史 -> {out_dir}", flush=True)

    t0 = time.time()
    ok, skipped, failed = 0, 0, []
    for i, sym in enumerate(symbols, 1):
        out_p = os.path.join(out_dir, f"{sym}.csv")
        if os.path.exists(out_p) and os.path.getsize(out_p) > 0:
            skipped += 1
            continue
        try:
            df, src = fetch_1h(sym)
            df.to_csv(out_p, index=False, encoding="utf-8-sig",
                      float_format="%.8f", date_format="%Y-%m-%d %H:%M:%S")
            ok += 1
            print(f"[{i}/{len(symbols)}] {sym} ({src}): {len(df)} 行 "
                  f"{df['Open Time'].min()} ~ {df['Open Time'].max()}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed.append(sym)
            print(f"[{i}/{len(symbols)}] {sym} 失败: {str(e)[:80]}", flush=True)
        time.sleep(0.15)

    print(f"\n完成: 成功 {ok}, 跳过 {skipped}, 失败 {len(failed)} {failed[:10]}"
          f", 耗时 {(time.time()-t0)/60:.1f} 分钟")
    print(f"输出目录: {out_dir}")


if __name__ == "__main__":
    main()
