# -*- coding: utf-8 -*-
"""
build_4h_research_table.py — 4h 研究宽表（截面特征并入）
=========================================================

把 BTCUSDT 1h K线(2019-09 起) 重采样为 4h, 并入:
  1. 跨币种资金费率截面 (funding_cross_section_8h.csv, 8h->4h)
  2. 跨币种 OI 截面 (open_interest_cross_section_1h.csv, 1h->4h)
  3. 恐惧贪婪指数 (日->4h)
  4. 宏观 (日->4h)

防未来约定: 所有辅助列先对齐到 4h 桶起点, 再在表内向前填充——
每个 4h 行只使用"该时刻已发生"的信息 (资金费率 08:00 结算值属于
[08:00,12:00) 桶, 时刻 08:00 已知, 无未来泄漏)。

输出: data_new/additional/merged_4h_research_crosssection.csv
"""

import os
import time

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (data-pipeline)"}
BASE = "https://api.binance.com"
A = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_new", "additional")
OUT = os.path.join(A, "merged_4h_research_crosssection.csv")

KLINE_COLS = {"open", "high", "low", "close", "volume", "quote_volume",
              "trades", "taker_buy_base", "taker_buy_quote"}


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


def fetch_btc_1h(start="2019-09-01"):
    """BTCUSDT 1h K线全量(分页)。"""
    end_ms = int(time.time() * 1000)
    start_ms = int(pd.to_datetime(start).timestamp() * 1000)
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _get_json(f"{BASE}/api/v3/klines",
                         {"symbol": "BTCUSDT", "interval": "1h",
                          "startTime": cursor, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        cursor = data[-1][0] + 1
        time.sleep(0.1)
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"]
    df = pd.DataFrame(rows, columns=cols)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    for c in KLINE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.set_index("time").sort_index()


def main():
    print("== 1. 拉取 BTCUSDT 1h K线 (2019-09 起) ==")
    k1 = fetch_btc_1h()
    print(f"   {len(k1)} 根 1h K线 ({k1.index.min()} ~ {k1.index.max()})")

    print("== 2. 重采样为 4h ==")
    k4 = k1.resample("4h").agg({"open": "first", "high": "max", "low": "min",
                                "close": "last", "volume": "sum",
                                "quote_volume": "sum", "trades": "sum",
                                "taker_buy_base": "sum",
                                "taker_buy_quote": "sum"}).dropna(subset=["close"])
    print(f"   {len(k4)} 根 4h K线")

    def join_resampled(path, tcol, freq="4h", rename=None):
        df = pd.read_csv(os.path.join(A, path), parse_dates=[tcol])
        df = df.set_index(tcol)
        if rename:
            df = df.rename(columns=rename)
        return k4.join(df.resample(freq).last(), how="left")

    OI_RENAME = {"n_symbols": "oi_n_symbols", "total_oi_value": "oi_total_value",
                 "mean": "oi_mean", "median": "oi_median", "std": "oi_std",
                 "q25": "oi_q25", "q75": "oi_q75", "btc_oi_value": "oi_btc_value",
                 "btc_share": "oi_btc_share", "btc_pctile": "oi_btc_pctile",
                 "total_oi_chg_24h_pct": "oi_total_chg_24h_pct"}

    print("== 3. 并入截面与日频数据 ==")
    k4 = join_resampled("funding_cross_section_8h.csv", "time")
    print("   + funding_cross_section (8h)")
    k4 = join_resampled("open_interest_cross_section_1h.csv", "time", rename=OI_RENAME)
    print("   + open_interest_cross_section (1h, oi_ 前缀)")
    k4 = join_resampled("fear_greed_index.csv", "date")
    print("   + fear_greed_index (日)")
    k4 = join_resampled("macro_daily.csv", "date")
    print("   + macro_daily (日)")

    # 统一向前填充辅助列(不含K线列), 每个时刻只用已知信息
    for c in k4.columns:
        if c not in KLINE_COLS:
            k4[c] = k4[c].ffill()

    k4 = k4.reset_index()
    k4 = k4.rename(columns={k4.columns[0]: "time"})
    k4.to_csv(OUT, index=False, encoding="utf-8-sig", float_format="%.8f")
    print(f"\n4h 研究宽表已保存: {OUT}")
    print(f"   {k4.shape[0]} 行 x {k4.shape[1]} 列 "
          f"({k4.time.min()} ~ {k4.time.max()})")
    print(f"   辅助列缺失率: {k4.isna().mean().sort_values(ascending=False).head(5).to_dict()}")
    print("\n最近 3 行关键列:")
    show = [c for c in ["time", "close", "median", "mean_abs", "pct_long_crowded",
                        "btc_pctile", "btc_z", "oi_total_value", "oi_btc_share",
                        "fng_value", "dxy"] if c in k4.columns]
    print(k4.tail(3)[show].to_string(index=False))


if __name__ == "__main__":
    main()
