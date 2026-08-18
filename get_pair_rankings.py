# -*- coding: utf-8 -*-
"""
get_pair_rankings.py — 交易对排行数据获取
==========================================

生成"市场列表排行"数据（类似 CoinMarketCap 的交易所市场列表页）：

  1. Binance 现货交易对排行  binance_spot_pair_ranking.csv
     —— 官方 /api/v3/ticker/24hr, 3684 个交易对, 按 24h 成交额排序
  2. Binance U本位合约交易对排行  binance_futures_pair_ranking.csv
     —— 官方 /fapi/v1/ticker/24hr (742 个) + /fapi/v1/premiumIndex (资金费率/标记价快照)
  3. 全局加密货币市值排行  coingecko_market_cap_ranking.csv
     —— CoinGecko 免费接口, 按市值排序（含价格/24h涨跌/成交额）

说明：CMC 页面把各计价币的成交额统一折算成 USD；本脚本对 Binance 现货
也做了近似 USD 折算（用同批 ticker 中的 BTCUSDT/ETHUSDT 等价格换算 BTC/ETH
计价对），并保留原始 quoteVolume 列便于核对。

用法:
    python get_pair_rankings.py [--top 20] [--out-dir data_new/additional]
                                [--no-coingecko]
"""

import argparse
import os
import time

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (data-pipeline)"}
SPOT_BASE = "https://api.binance.com"
FAPI_BASE = "https://fapi.binance.com"
CG_BASE = "https://api.coingecko.com/api/v3"

# 视作 ~1 USD 的计价币（直接使用 quoteVolume 作为美元额）
STABLES = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "EUR", "TRY"}


def _get_json(url, params=None, retries=8, timeout=25):
    """带重试的 GET（本机代理间歇不稳定, 见 get_additional_data.py 说明）。"""
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            if r.status_code == 429:
                time.sleep(5)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError) as e:
            last_err = e
            time.sleep(min(1.5 * (i + 1), 12))
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5)
    raise RuntimeError(f"GET {url} 失败: {last_err}")


def fetch_spot_ranking():
    """Binance 现货 24h ticker -> 按 24h 成交额排序。"""
    data = _get_json(f"{SPOT_BASE}/api/v3/ticker/24hr")
    df = pd.DataFrame(data)
    df = df[~df["symbol"].str.contains(r"_|DOWN|UP", regex=True)]  # 剔除杠杆/双币
    df["base"] = df["symbol"].str[:-4]
    df["quote"] = df["symbol"].str[-4:]
    for c in ["lastPrice", "priceChangePercent", "highPrice", "lowPrice",
              "volume", "quoteVolume", "weightedAvgPrice", "count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 近似 USD 折算: 用同一批数据里的 XUSDT 价格给 BTC/ETH 等计价对换算
    price_of = {row.symbol[:-4]: row.lastPrice
                for row in df.itertuples() if row.symbol.endswith("USDT")}
    def usd_vol(r):
        if r.quote in STABLES:
            return r.quoteVolume
        p = price_of.get(r.quote)
        return r.quoteVolume * p if p and p > 0 else float("nan")
    df["usd_volume_approx"] = df.apply(usd_vol, axis=1)

    df = df.sort_values("usd_volume_approx", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    cols = ["rank", "symbol", "base", "quote", "lastPrice", "priceChangePercent",
            "highPrice", "lowPrice", "volume", "quoteVolume", "usd_volume_approx",
            "weightedAvgPrice", "count"]
    return df[cols].rename(columns={
        "lastPrice": "price", "priceChangePercent": "change_24h_pct",
        "highPrice": "high_24h", "lowPrice": "low_24h",
        "volume": "volume_24h", "quoteVolume": "quote_volume_24h",
        "usd_volume_approx": "usd_volume_24h_approx",
        "weightedAvgPrice": "vwap_24h", "count": "trades_24h"})


def fetch_futures_ranking():
    """Binance U本位合约 24h ticker + premiumIndex(资金费率/标记价) -> 排序。"""
    data = _get_json(f"{FAPI_BASE}/fapi/v1/ticker/24hr")
    df = pd.DataFrame(data)
    df["base"] = df["symbol"].str[:-4]
    df["quote"] = df["symbol"].str[-4:]
    for c in ["lastPrice", "priceChangePercent", "highPrice", "lowPrice",
              "volume", "quoteVolume", "count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 资金费率/标记价快照（一次调用返回全部交易对）
    try:
        pi = _get_json(f"{FAPI_BASE}/fapi/v1/premiumIndex")
        pi_df = pd.DataFrame(pi)[["symbol", "markPrice", "lastFundingRate", "nextFundingTime"]]
        pi_df["lastFundingRate"] = pd.to_numeric(pi_df["lastFundingRate"], errors="coerce")
        df = df.merge(pi_df, on="symbol", how="left")
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] premiumIndex 获取失败: {e}")

    df = df.sort_values("quoteVolume", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    cols = ["rank", "symbol", "base", "quote", "lastPrice", "priceChangePercent",
            "highPrice", "lowPrice", "volume", "quoteVolume", "markPrice",
            "lastFundingRate", "count"]
    return df[cols].rename(columns={
        "lastPrice": "price", "priceChangePercent": "change_24h_pct",
        "highPrice": "high_24h", "lowPrice": "low_24h",
        "volume": "volume_24h", "quoteVolume": "quote_volume_24h",
        "count": "trades_24h"})


def fetch_coingecko_ranking(top=100):
    """CoinGecko 全局市值排行（免费）。"""
    out, page = [], 1
    while len(out) < top:
        data = _get_json(f"{CG_BASE}/coins/markets",
                         {"vs_currency": "usd", "order": "market_cap_desc",
                          "per_page": 250, "page": page})
        if not data:
            break
        out.extend(data)
        page += 1
        if page > 5:
            break
    df = pd.DataFrame(out).head(top)
    cols = ["market_cap_rank", "id", "symbol", "name", "current_price",
            "market_cap", "total_volume", "price_change_percentage_24h",
            "high_24h", "low_24h", "ath", "ath_change_percentage",
            "circulating_supply", "total_supply", "max_supply"]
    df = df[cols].rename(columns={
        "market_cap_rank": "rank", "current_price": "price_usd",
        "total_volume": "volume_24h_usd",
        "price_change_percentage_24h": "change_24h_pct",
        "circulating_supply": "circ_supply"})
    return df


# ---------------------------------------------------------------------------
# 历史排名重建（免费 API 无历史排名快照, 用历史成交额/市值按日重建）
# ---------------------------------------------------------------------------
def fetch_daily_klines(symbol, days=90, start_date=None, fapi=False):
    """单交易对日 K 线成交额（计价币）。

    start_date 给定时从该日期拉取（分页直到最新）；否则拉最近 days 天。
    Binance 现货最早 2017-08-17, U本位合约最早 2019-09-08。
    """
    path = f"{FAPI_BASE}/fapi/v1/klines" if fapi else f"{SPOT_BASE}/api/v3/klines"
    end_ms = int(time.time() * 1000)
    if start_date:
        start_ms = int(pd.to_datetime(start_date).timestamp() * 1000)
    else:
        start_ms = end_ms - days * 24 * 3600 * 1000
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _get_json(path, {"symbol": symbol, "interval": "1d",
                                "startTime": cursor, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        cursor = data[-1][0] + 1
        time.sleep(0.05)
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "t1", "t2", "t3"]
    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.normalize()
    df["quote_volume"] = pd.to_numeric(df["quote_volume"], errors="coerce")
    return df[["date", "quote_volume"]].drop_duplicates("date")


def build_pair_history(symbols, days, start_date, fapi, out_dir, label):
    """按日重建交易对成交额排名（支持长历史回溯）。输出:
       {label}_daily_volume_wide.csv   日期 x 交易对 成交额矩阵
       {label}_daily_ranking.csv       日期, 交易对, 成交额, 当日排名, 较前日排名变化
    """
    frames, failed = [], []
    for sym in symbols:
        try:
            k = fetch_daily_klines(sym, days=days, start_date=start_date, fapi=fapi)
            k = k.rename(columns={"quote_volume": sym}).set_index("date")
            frames.append(k[sym])
            time.sleep(0.05)
        except Exception as e:  # noqa: BLE001
            failed.append(sym)
            print(f"  [skip] {sym}: {str(e)[:80]}")
    if failed:
        print(f"  [{label}] 失败 {len(failed)} 个: {failed[:10]}...")

    wide = pd.concat(frames, axis=1).sort_index()
    wide.to_csv(os.path.join(out_dir, f"{label}_daily_volume_wide.csv"),
                encoding="utf-8-sig", float_format="%.8f")

    long = wide.stack().reset_index()
    long.columns = ["date", "symbol", "volume"]
    long = long[long["volume"].notna()]
    long["rank"] = long.groupby("date")["volume"].rank(ascending=False,
                                                       method="first").astype(int)
    long["rank_change"] = long.groupby("symbol")["rank"].diff().fillna(0).astype(int)
    long = long.sort_values(["date", "rank"]).reset_index(drop=True)
    long.to_csv(os.path.join(out_dir, f"{label}_daily_ranking.csv"),
                index=False, encoding="utf-8-sig", float_format="%.8f")
    print(f"  [{label}] {len(wide.columns)} 个交易对 x {len(wide)} 天 "
          f"({wide.index.min().date()} ~ {wide.index.max().date()}) -> saved")
    return wide, long


def fetch_coingecko_daily(coin_id, days=90, start_date=None):
    """单币种日频 价格/市值/成交额。

    免 key 最多 365 天; 设置环境变量 COINGECKO_API_KEY (免费 demo key) 后
    可用 days=max 取全历史 (如 BTC 自 2013)。
    """
    key = os.environ.get("COINGECKO_API_KEY", "").strip()
    if key:
        params = {"vs_currency": "usd", "days": "max", "interval": "daily"}
    else:
        params = {"vs_currency": "usd", "days": min(days, 365), "interval": "daily"}
    j = _get_json(f"{CG_BASE}/coins/{coin_id}/market_chart", params)
    out = {}
    for key2, col in [("prices", "price_usd"), ("market_caps", "market_cap"),
                      ("total_volumes", "volume_usd")]:
        arr = pd.DataFrame(j.get(key2, []), columns=["ts", col])
        arr["date"] = pd.to_datetime(arr["ts"], unit="ms").dt.normalize()
        out[col] = arr.groupby("date")[col].last()
    df = pd.DataFrame(out).sort_index()
    if start_date:
        df = df[df.index >= pd.to_datetime(start_date)]
    df = df[~df.index.duplicated(keep="last")]
    return df


def build_coingecko_history(ids, days, start_date, out_dir):
    """按日重建币种市值/成交额排名。输出:
       coingecko_daily_marketcap_ranking.csv  日期, id, symbol, 市值, 市值排名
       coingecko_daily_volume_ranking.csv     日期, id, symbol, 成交额, 成交额排名
    """
    frames = {}  # coin_id -> DataFrame[price_usd, market_cap, volume_usd]
    for cid in ids:
        try:
            frames[cid] = fetch_coingecko_daily(cid, days, start_date)
            time.sleep(3.5)  # CoinGecko 免费限流 ~10-30 次/分
        except Exception as e:  # noqa: BLE001
            print(f"  [skip] {cid}: {str(e)[:80]}")
            time.sleep(2)
    if not frames:
        print("  [coingecko] 无任何币种数据")
        return None, None

    mc = pd.DataFrame({cid: frames[cid]["market_cap"] for cid in frames}).sort_index()
    vol = pd.DataFrame({cid: frames[cid]["volume_usd"] for cid in frames}).sort_index()
    try:
        clist = _get_json(f"{CG_BASE}/coins/list")
        id2sym = {c["id"]: c["symbol"] for c in clist}
    except Exception:  # noqa: BLE001
        id2sym = {c: c for c in mc.columns}

    for wide, metric, fname in [(mc, "market_cap", "coingecko_daily_marketcap_ranking"),
                                (vol, "volume_usd", "coingecko_daily_volume_ranking")]:
        long = wide.stack().reset_index()
        long.columns = ["date", "id", metric]
        long["symbol"] = long["id"].map(id2sym)
        long = long[long[metric].notna()]
        long["rank"] = long.groupby("date")[metric].rank(ascending=False,
                                                         method="first").astype(int)
        long = long.sort_values(["date", "rank"]).reset_index(drop=True)
        long.to_csv(os.path.join(out_dir, f"{fname}.csv"),
                    index=False, encoding="utf-8-sig", float_format="%.8f")
        print(f"  [coingecko] {fname}: {len(long)} 行 "
              f"({long.date.min().date()} ~ {long.date.max().date()})")
    return mc, vol


def run_history(out_dir, top, days, start_date, no_coingecko):
    """基于当前排行 Top N 重建历史每日排名（支持长历史回溯）。"""
    spot_rank = pd.read_csv(os.path.join(out_dir, "binance_spot_pair_ranking.csv"))
    fut_rank = pd.read_csv(os.path.join(out_dir, "binance_futures_pair_ranking.csv"))
    spot_syms = spot_rank["symbol"].head(top).tolist()
    fut_syms = fut_rank["symbol"].head(top).tolist()
    label_start = start_date or f"近{days}天"
    print(f"== 历史排名重建 ({label_start} 至今, Top {top}) ==")
    print("-- 现货交易对按日成交额排名 --")
    build_pair_history(spot_syms, days, start_date, fapi=False,
                       out_dir=out_dir, label="spot")
    print("-- 合约交易对按日成交额排名 (最早 2019-09) --")
    fut_start = start_date
    if fut_start and pd.to_datetime(fut_start) < pd.to_datetime("2019-09-08"):
        fut_start = "2019-09-08"
    build_pair_history(fut_syms, days, fut_start, fapi=True,
                       out_dir=out_dir, label="futures")
    if not no_coingecko:
        key = os.environ.get("COINGECKO_API_KEY", "").strip()
        if start_date and not key:
            print("  [coingecko] 提示: 未设置 COINGECKO_API_KEY, 币种历史最多 365 天;"
                  " 注册免费 key (coingecko.com) 后可回溯 2013 至今")
        print("-- 币种按日市值/成交额排名 --")
        cg = pd.read_csv(os.path.join(out_dir, "coingecko_market_cap_ranking.csv"))
        ids = cg["id"].head(min(top, 50)).tolist()
        build_coingecko_history(ids, days, start_date, out_dir)


def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", float_format="%.8f")
    return df.shape


def main():
    ap = argparse.ArgumentParser(description="交易对排行数据获取")
    ap.add_argument("--top", type=int, default=20, help="终端展示前 N 名")
    ap.add_argument("--out-dir", default="data_new/additional")
    ap.add_argument("--no-coingecko", action="store_true")
    ap.add_argument("--history", action="store_true",
                    help="额外重建历史每日排名（Top N 交易对/币种按日成交额/市值）")
    ap.add_argument("--history-days", type=int, default=90)
    ap.add_argument("--history-start", type=str, default=None,
                    help="历史回溯起始日期 YYYY-MM-DD（如 2017-01-01）; 不填则回溯 history-days 天")
    ap.add_argument("--history-top", type=int, default=100,
                    help="参与历史排名重建的交易对/币种数量上限")
    args = ap.parse_args()
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    print("== Binance 现货交易对排行 ==")
    spot = fetch_spot_ranking()
    save_csv(spot, os.path.join(out_dir, "binance_spot_pair_ranking.csv"))
    print(f"  共 {len(spot)} 个交易对 -> saved")
    print(spot.head(args.top)[["rank", "symbol", "price", "change_24h_pct",
                               "quote_volume_24h", "usd_volume_24h_approx"]]
          .to_string(index=False))

    print("\n== Binance U本位合约交易对排行 ==")
    fut = fetch_futures_ranking()
    save_csv(fut, os.path.join(out_dir, "binance_futures_pair_ranking.csv"))
    print(f"  共 {len(fut)} 个交易对 -> saved")
    print(fut.head(args.top)[["rank", "symbol", "price", "change_24h_pct",
                              "quote_volume_24h", "lastFundingRate"]]
          .to_string(index=False))

    if not args.no_coingecko:
        print("\n== CoinGecko 全局市值排行 ==")
        cg = fetch_coingecko_ranking(top=100)
        save_csv(cg, os.path.join(out_dir, "coingecko_market_cap_ranking.csv"))
        print(f"  共 {len(cg)} 个币 -> saved")
        print(cg.head(args.top)[["rank", "symbol", "name", "price_usd",
                                 "market_cap", "change_24h_pct"]]
              .to_string(index=False))

    if args.history:
        run_history(out_dir, top=args.history_top, days=args.history_days,
                    start_date=args.history_start, no_coingecko=args.no_coingecko)

    print(f"\n完成。输出目录: {out_dir}")


if __name__ == "__main__":
    main()
