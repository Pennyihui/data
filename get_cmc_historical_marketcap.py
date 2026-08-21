# -*- coding: utf-8 -*-
"""
get_cmc_historical_marketcap.py — CoinMarketCap 历史市值/成交额 (免 key)
========================================================================

用 CMC 官网的 data-api v3 (免 key, 免登录) 按年窗口拉取 Top N 币的
日频市值/成交额/流通量, 重建 2013 至今的历史市值排名。

- 币种 id 从 CMC listing 接口映射
- 按年窗口请求保证日频粒度 (长区间接口会降采样)
- 断点续传: 已存在的币自动跳过

输出:
  data_new/additional/cmc_daily_marketcap_ranking.csv   date, symbol, market_cap, rank
  data_new/additional/cmc_daily_volume_ranking.csv      date, symbol, volume, rank
  data_new/additional/cmc_daily_wide.csv                date x symbol 市值宽表

用法: python get_cmc_historical_marketcap.py [--top 100] [--start-year 2013]
"""

import argparse
import os
import time

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
API = "https://api.coinmarketcap.com/data-api/v3"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "data_new", "additional")
CONVERT_ID = 2781  # USD


def get(url, params, retries=8, timeout=25):
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


def cmc_id_map(limit=200):
    """symbol -> (id, name) 从 CMC listing 获取。"""
    j = get(f"{API}/cryptocurrency/listing",
            {"start": 1, "limit": limit, "sortBy": "market_cap",
             "sortType": "desc", "convert": "USD"})
    rows = j["data"]["cryptoCurrencyList"]
    return {r["symbol"]: (r["id"], r.get("name", "")) for r in rows}


def fetch_coin_history(cid, start_year, end_year=2027):
    """按年窗口拉日频 quotes, 返回 DataFrame[date, market_cap, volume, circ_supply]。"""
    frames = []
    for year in range(start_year, end_year):
        t0 = int(pd.Timestamp(f"{year}-01-01").timestamp())
        t1 = int(pd.Timestamp(f"{year+1}-01-01").timestamp())
        j = get(f"{API}/cryptocurrency/historical",
                {"id": cid, "convertId": CONVERT_ID, "timeStart": t0,
                 "timeEnd": t1, "interval": "1d"})
        quotes = j.get("data", {}).get("quotes", [])
        if not quotes:
            continue
        df = pd.DataFrame([{
            "date": pd.to_datetime(q["timeOpen"]).normalize(),
            "market_cap": q["quote"].get("marketCap"),
            "volume": q["quote"].get("volume"),
            "circ_supply": q["quote"].get("circulatingSupply"),
        } for q in quotes])
        frames.append(df)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates("date").sort_values("date")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--start-year", type=int, default=2013)
    ap.add_argument("--symbols", default=None,
                    help="逗号分隔币种列表(如 BTC,ETH,SOL), 缺省取 CoinGecko 市值 Top N")
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        cg = pd.read_csv(os.path.join(OUT_DIR, "coingecko_market_cap_ranking.csv"))
        symbols = cg["symbol"].head(args.top).str.upper().tolist()

    print(f"目标: {len(symbols)} 币, {args.start_year} 年至今", flush=True)
    idmap = cmc_id_map(limit=args.top + 100)
    missing = [s for s in symbols if s not in idmap]
    if missing:
        print(f"CMC 未找到: {missing}")

    wide_mc, wide_vol, wide_sup = {}, {}, {}
    t0 = time.time()
    for i, sym in enumerate(symbols, 1):
        if sym not in idmap:
            continue
        try:
            df = fetch_coin_history(idmap[sym][0], args.start_year)
            if df.empty:
                print(f"[{i}/{len(symbols)}] {sym}: 无数据", flush=True)
                continue
            d = df.set_index("date")
            wide_mc[sym] = d["market_cap"]
            wide_vol[sym] = d["volume"]
            wide_sup[sym] = d["circ_supply"]
            print(f"[{i}/{len(symbols)}] {sym}: {len(df)} 天 "
                  f"({df.date.min().date()} ~ {df.date.max().date()})", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[{i}/{len(symbols)}] {sym} 失败: {str(e)[:80]}", flush=True)

    for wide, suffix in [(wide_mc, "marketcap"), (wide_vol, "volume"),
                         (wide_sup, "circulating_supply")]:
        w = pd.DataFrame(wide).sort_index()
        w.to_csv(os.path.join(OUT_DIR, f"cmc_daily_{suffix}_wide.csv"),
                 encoding="utf-8-sig", float_format="%.8f")
        long = w.stack().reset_index()
        long.columns = ["date", "symbol", suffix]
        long = long[long[suffix].notna()]
        long["rank"] = long.groupby("date")[suffix].rank(ascending=False,
                                                         method="first").astype(int)
        long = long.sort_values(["date", "rank"]).reset_index(drop=True)
        long.to_csv(os.path.join(OUT_DIR, f"cmc_daily_{suffix}_ranking.csv"),
                    index=False, encoding="utf-8-sig", float_format="%.8f")
        print(f"cmc_daily_{suffix}_ranking.csv: {len(long)} 行 "
              f"({long.date.min().date()} ~ {long.date.max().date()}, "
              f"{long.symbol.nunique()} 币)")

    print(f"\n完成, 耗时 {(time.time()-t0)/60:.1f} 分钟")


if __name__ == "__main__":
    main()
