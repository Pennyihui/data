# -*- coding: utf-8 -*-
"""
get_additional_data.py — 扩展数据获取模块
=========================================

现有 get_data.py 只拉取 Binance 现货 K 线（OHLCV 基础数据）。
本模块在基础数据之上，补充 4 大类可免费获取的数据：

  A. Binance 合约衍生品数据 (fapi, 公开接口, 无需 API key)
      1. 资金费率历史      funding rate        (8h 一次, 2019-09 至今全历史)
      2. 未平仓合约历史    open interest       (1h, 最近约 500 小时)
      3. 全市场多空账户比  global long/short   (1h)
      4. 大户多空持仓比    top trader position (1h)
      5. 主动买卖量比      taker buy/sell      (1h)
      6. 标记价格 K 线     mark price klines   (1h)
      7. 当前溢价/标记价   premium index       (快照)

  B. 市场情绪数据 (无 key)
      8. 恐惧贪婪指数      Fear & Greed Index  (alternative.me, 日频, 2018-02 至今)

  C. 比特币链上数据 (blockchain.info charts, 无 key)
      9. 算力 / 难度 / 矿工收入 / 活跃地址 / 交易数 / 内存池 / 区块大小 / 市值 等

  D. 宏观数据 (Yahoo Finance chart API, 无 key)
      10. 美元指数 DXY / 标普500 / 纳斯达克 / VIX / 黄金 / 10Y美债收益率

  E. 合并上下文宽表
      把上述数据按 1h 时间戳对齐到 BTCUSDT 1h K 线上, 生成 btc_context_1h.csv,
      可直接作为 produce_csv.py 的补充输入。

可选（需免费注册 API key, 无 key 时自动跳过）:
  F. FRED 宏观数据 (https://fred.stlouisfed.org/docs/api/api_key.html)
     通过环境变量 FRED_API_KEY 提供 key 后可用。

用法:
    python get_additional_data.py [--symbol BTCUSDT] [--out-dir data_new/additional]
                                  [--ratio-period 1h] [--no-macro] [--no-onchain]

输出目录默认: Data_pipeline/data_new/additional/
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# 常量与全局配置
# ---------------------------------------------------------------------------
SPOT_BASE = "https://api.binance.com"
FAPI_BASE = "https://fapi.binance.com"
FNG_BASE = "https://api.alternative.me"
ONCHAIN_BASE = "https://api.blockchain.info/charts"
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"

UA = {"User-Agent": "Mozilla/5.0 (data-pipeline)"}

# blockchain.info 可用图表（键: 图表 slug, 值: 中文说明）
ONCHAIN_CHARTS = {
    "hash-rate": "比特币全网算力 (EH/s)",
    "difficulty": "挖矿难度",
    "miners-revenue": "矿工总收入 (USD)",
    "n-unique-addresses": "活跃地址数",
    "n-transactions": "每日交易笔数",
    "n-transactions-total": "累计交易笔数",
    "mempool-size": "内存池大小 (bytes)",
    "median-confirmation-time": "中位确认时间 (分钟)",
    "market-cap": "流通市值 (USD)",
    "estimated-transaction-volume-usd": "链上转账总额 (USD)",
    "cost-per-transaction": "单笔交易成本 (USD)",
    "transaction-fees": "交易手续费 (BTC)",
}

# Yahoo Finance 宏观符号（键: 列名, 值: (Yahoo符号, 中文说明)）
MACRO_SYMBOLS = {
    "dxy": ("DX-Y.NYB", "美元指数 DXY"),
    "sp500": ("^GSPC", "标普500指数"),
    "nasdaq": ("^IXIC", "纳斯达克综合指数"),
    "vix": ("^VIX", "VIX 恐慌指数"),
    "gold": ("GC=F", "黄金期货 (USD/oz)"),
    "us10y": ("^TNX", "美国10年期国债收益率"),
}

# FRED 可选宏观序列
FRED_SERIES = {
    "fred_cpi": "CPIAUCSL",          # 美国 CPI
    "fred_m2": "M2SL",               # 美国 M2 货币供应
    "fred_ffr": "DFF",               # 联邦基金有效利率
    "fred_dxy": "DTWEXBGS",          # 美元广义指数
}


def _session():
    s = requests.Session()
    s.headers.update(UA)
    return s


def _get_json(url, params=None, retries=8, timeout=20):
    """带重试的 GET 请求, 返回解析后的 JSON。

    本机需经本地代理(127.0.0.1:7897)访问外网, 代理间歇性不稳定
    (SSL EOF / 读超时 / RemoteDisconnected), 因此对连接类错误做
    多次快速退避重试; 缩短单次超时以快速失败并尽早重试。
    """
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            if r.status_code == 429:  # 限流, 退避重试
                time.sleep(5)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError) as e:
            last_err = e
            time.sleep(min(1.5 * (i + 1), 12))  # 1.5,3,4.5,...,12s 封顶
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5)
    raise RuntimeError(f"GET {url} 失败: {last_err}")


def _to_ts(dt_str):
    """'YYYY-MM-DD' -> 毫秒时间戳"""
    return int(pd.to_datetime(dt_str).timestamp() * 1000)


# ---------------------------------------------------------------------------
# A. Binance 合约衍生品数据
# ---------------------------------------------------------------------------
def fetch_funding_rate_history(symbol="BTCUSDT", start="2019-09-01", end=None):
    """资金费率历史（8h 一次）。返回 DataFrame: funding_time, funding_rate, mark_price。"""
    end = end or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_ts, end_ts = _to_ts(start), _to_ts(end)
    all_rows, cursor = [], start_ts
    while cursor < end_ts:
        data = _get_json(f"{FAPI_BASE}/fapi/v1/fundingRate",
                         {"symbol": symbol, "startTime": cursor, "limit": 1000})
        if not data:
            break
        all_rows.extend(data)
        cursor = data[-1]["fundingTime"] + 1
        time.sleep(0.15)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["funding_time"] = pd.to_datetime(df["fundingTime"], unit="ms")
    # 早期记录 fundingRate/markPrice 可能为空字符串, 统一转数值并置 NaN
    df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df["mark_price"] = pd.to_numeric(df["markPrice"], errors="coerce")
    return df[["funding_time", "funding_rate", "mark_price"]].drop_duplicates("funding_time")


def fetch_futures_data_series(kind, symbol="BTCUSDT", period="1h", limit=500):
    """通用拉取 Binance 合约统计序列 (futures/data)。

    kind: openInterestHist | globalLongShortAccountRatio
         | topLongShortAccountRatio | topLongShortPositionRatio | takerlongshortRatio
    """
    urls = {
        "openInterestHist": f"{FAPI_BASE}/futures/data/openInterestHist",
        "globalLongShortAccountRatio": f"{FAPI_BASE}/futures/data/globalLongShortAccountRatio",
        "topLongShortAccountRatio": f"{FAPI_BASE}/futures/data/topLongShortAccountRatio",
        "topLongShortPositionRatio": f"{FAPI_BASE}/futures/data/topLongShortPositionRatio",
        "takerlongshortRatio": f"{FAPI_BASE}/futures/data/takerlongshortRatio",
    }
    data = _get_json(urls[kind], {"symbol": symbol, "period": period, "limit": limit})
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.drop(columns=["symbol", "timestamp"], errors="ignore")
    return df


def fetch_mark_price_klines(symbol="BTCUSDT", interval="1h", limit=500):
    """标记价格 K 线（避免异常插针的稳健价格序列）。"""
    data = _get_json(f"{FAPI_BASE}/fapi/v1/markPriceKlines",
                     {"symbol": symbol, "interval": interval, "limit": limit})
    cols = ["open_time", "open", "high", "low", "close", "ignore1", "close_time",
            "ignore2", "trades", "ignore3", "ignore4", "ignore5"]
    df = pd.DataFrame(data, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    for c in ["open", "high", "low", "close"]:
        df[c] = df[c].astype(float)
    return df[["open_time", "open", "high", "low", "close"]]


def fetch_premium_index(symbol="BTCUSDT"):
    """当前标记价/指数价/下一期资金费率快照。"""
    d = _get_json(f"{FAPI_BASE}/fapi/v1/premiumIndex", {"symbol": symbol})
    return pd.DataFrame([{
        "time": pd.to_datetime(d["time"], unit="ms"),
        "symbol": d["symbol"],
        "mark_price": float(d["markPrice"]),
        "index_price": float(d["indexPrice"]),
        "estimated_settle_price": float(d["estimatedSettlePrice"]),
        "last_funding_rate": float(d.get("lastFundingRate") or 0),
        "interest_rate": float(d.get("interestRate") or 0),
        "next_funding_time": pd.to_datetime(d.get("nextFundingTime"), unit="ms"),
    }])


# ---------------------------------------------------------------------------
# B. 情绪数据
# ---------------------------------------------------------------------------
def fetch_fear_greed_index(limit=0):
    """恐惧贪婪指数全历史 (alternative.me, 日频, 2018-02 至今)。limit=0 表示全部。"""
    params = {"limit": limit} if limit else {"limit": 0}
    data = _get_json(f"{FNG_BASE}/fng/", params)
    rows = []
    for item in data["data"]:
        rows.append({
            "date": pd.to_datetime(int(item["timestamp"]), unit="s").normalize(),
            "fng_value": int(item["value"]),
            "fng_classification": item["value_classification"],
        })
    df = pd.DataFrame(rows).drop_duplicates("date").sort_values("date")
    return df


# ---------------------------------------------------------------------------
# C. 链上数据
# ---------------------------------------------------------------------------
def fetch_onchain_daily(charts=None):
    """拉取 blockchain.info 多个图表并合并为日频宽表。"""
    charts = charts or ONCHAIN_CHARTS
    frames = []
    for slug, desc in charts.items():
        try:
            j = _get_json(f"{ONCHAIN_BASE}/{slug}",
                          {"timespan": "all", "format": "json", "sampled": "false"})
            s = pd.Series({pd.to_datetime(p["x"], unit="s").normalize(): p["y"]
                           for p in j["values"]}, name=slug)
            s = s[~s.index.duplicated(keep="last")]
            frames.append(s)
            print(f"  [onchain] {slug} ({desc}) -> {len(s)} 天")
            time.sleep(0.2)
        except Exception as e:  # noqa: BLE001
            print(f"  [onchain] {slug} 失败: {e}")
    df = pd.concat(frames, axis=1).sort_index()
    df.index.name = "date"
    return df.reset_index()


# ---------------------------------------------------------------------------
# D. 宏观数据
# ---------------------------------------------------------------------------
def fetch_yahoo_series(symbol, interval="1d", range_="5y"):
    """Yahoo Finance chart API (免 key)。返回 (日期index, close Series)。"""
    for base in ("https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"):
        try:
            url = f"{base}/v8/finance/chart/{symbol}"
            r = requests.get(url, params={"range": range_, "interval": interval},
                             timeout=20, headers=UA)
            j = r.json()
            res = j["chart"]["result"][0]
            ts = res.get("timestamp") or []
            close = res["indicators"]["quote"][0].get("close") or []
            s = pd.Series(close, index=pd.to_datetime(ts, unit="s").normalize()).dropna()
            return s[~s.index.duplicated(keep="last")]
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"Yahoo {symbol} 失败: {last_err}")


def fetch_macro_daily(symbols=None):
    """拉取多个宏观序列并合并为日频宽表。"""
    symbols = symbols or MACRO_SYMBOLS
    frames = []
    for col, (sym, desc) in symbols.items():
        try:
            s = fetch_yahoo_series(sym)
            s.name = col
            frames.append(s)
            print(f"  [macro] {col} ({desc}) -> {len(s)} 天")
        except Exception as e:  # noqa: BLE001
            print(f"  [macro] {col} ({sym}) 失败: {e}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, axis=1).sort_index()
    df.index.name = "date"
    return df.reset_index()


# ---------------------------------------------------------------------------
# F. FRED 宏观（可选, 需 FRED_API_KEY）
# ---------------------------------------------------------------------------
def fetch_fred_series(series_id, api_key):
    """FRED 单序列全历史（需免费 key）。返回 (日期index, value Series)。"""
    url = "https://api.stlouisfed.org/fred/series/observations"
    r = requests.get(url, params={"series_id": series_id, "api_key": api_key,
                                  "file_type": "json"}, timeout=30)
    r.raise_for_status()
    rows = [(o["date"], o["value"]) for o in r.json()["observations"]
            if o["value"] not in (".", "")]
    s = pd.Series({pd.to_datetime(d): float(v) for d, v in rows})
    return s[~s.index.duplicated(keep="last")]


def fetch_fred_optional():
    """若环境变量 FRED_API_KEY 存在, 拉取 FRED 宏观序列。"""
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        print("  [fred] 未设置 FRED_API_KEY, 跳过 (免费注册: "
              "https://fred.stlouisfed.org/docs/api/api_key.html)")
        return pd.DataFrame()
    frames = []
    for col, sid in FRED_SERIES.items():
        try:
            s = fetch_fred_series(sid, api_key)
            s.name = col
            frames.append(s)
            print(f"  [fred] {col} ({sid}) -> {len(s)} 天")
        except Exception as e:  # noqa: BLE001
            print(f"  [fred] {col} ({sid}) 失败: {e}")
    return pd.concat(frames, axis=1).sort_index() if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# E. 合并 1h 上下文宽表
# ---------------------------------------------------------------------------
def build_context_table(symbol, ratio_period, out_dir, days_klines=60):
    """把 K线 + 资金费率 + OI + 多空比 + 主动买卖比 + FNG + 宏观 对齐到 1h。"""
    print("  合并 1h 上下文宽表 ...")
    # 1. BTCUSDT 1h K线
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days_klines * 24 * 3600 * 1000
    klines, cursor = [], start_ms
    while cursor < end_ms:
        data = _get_json(f"{SPOT_BASE}/api/v3/klines",
                         {"symbol": symbol, "interval": "1h", "startTime": cursor, "limit": 1000})
        if not data:
            break
        klines.extend(data)
        cursor = data[-1][0] + 1
        time.sleep(0.1)
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"]
    base = pd.DataFrame(klines, columns=cols)
    base["open_time"] = pd.to_datetime(base["open_time"], unit="ms")
    base["close"] = base["close"].astype(float)
    base = base.set_index("open_time").sort_index()
    base = base[["open", "high", "low", "close", "volume", "quote_volume",
                 "trades", "taker_buy_base", "taker_buy_quote"]]

    # 2. 资金费率 (8h -> 1h 桶, 最后统一在 base 时间轴 ffill)
    try:
        fr = fetch_funding_rate_history(symbol)
        if not fr.empty:
            fr = fr.set_index("funding_time")[["funding_rate", "mark_price"]].resample("1h").last()
            base = base.join(fr, how="left")
    except Exception as e:  # noqa: BLE001
        print(f"  [context] 资金费率合并失败: {e}")

    # 3. 合约统计序列 (1h, 直接按时间戳合并)
    for kind, prefix in [("openInterestHist", "oi"),
                         ("globalLongShortAccountRatio", "glsr"),
                         ("topLongShortAccountRatio", "tlsr_acct"),
                         ("topLongShortPositionRatio", "tlsr_pos"),
                         ("takerlongshortRatio", "taker")]:
        try:
            df = fetch_futures_data_series(kind, symbol, period=ratio_period)
            if df.empty:
                continue
            df = df.set_index("time")
            df = df.rename(columns={c: f"{prefix}_{c}" for c in df.columns})
            base = base.join(df.resample("1h").last(), how="left")
        except Exception as e:  # noqa: BLE001
            print(f"  [context] {kind} 合并失败: {e}")

    # 4. 恐惧贪婪指数 (日 -> 1h 桶)
    try:
        fng = fetch_fear_greed_index().set_index("date")[["fng_value", "fng_classification"]]
        base = base.join(fng.resample("1h").last(), how="left")
    except Exception as e:  # noqa: BLE001
        print(f"  [context] fng 合并失败: {e}")

    # 5. 宏观 (日 -> 1h 桶)
    try:
        macro_path = os.path.join(out_dir, "macro_daily.csv")
        if os.path.exists(macro_path):
            macro = pd.read_csv(macro_path, parse_dates=["date"]).set_index("date")
            base = base.join(macro.resample("1h").last(), how="left")
    except Exception as e:  # noqa: BLE001
        print(f"  [context] macro 合并失败: {e}")

    # 6. 统一在 base 时间轴上向前填充所有辅助列:
    #    资金费率/多空比等在最近一次值之后的小时也应携带最近已结算值,
    #    日频的 FNG/宏观在当天内向前填充; K线列保持原样。
    kline_cols = {"open", "high", "low", "close", "volume", "quote_volume",
                  "trades", "taker_buy_base", "taker_buy_quote"}
    for c in base.columns:
        if c not in kline_cols:
            base[c] = base[c].ffill()

    base = base.reset_index()
    base.columns = ["time"] + list(base.columns[1:])
    base = base.reset_index(drop=True)  # 丢弃 RangeIndex, time 为普通列
    return base


# ---------------------------------------------------------------------------
# 保存与清单
# ---------------------------------------------------------------------------
def save_csv(df, path, index_label=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # 统一 index=False: 时间戳一律作为普通列保存, 避免 RangeIndex 与时间列重名
    df.to_csv(path, index=False, encoding="utf-8-sig", float_format="%.8f")
    return df.shape


def main():
    ap = argparse.ArgumentParser(description="扩展数据获取: 合约/情绪/链上/宏观")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--out-dir", default="data_new/additional")
    ap.add_argument("--ratio-period", default="1h", choices=["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"])
    ap.add_argument("--no-macro", action="store_true", help="跳过宏观数据")
    ap.add_argument("--no-onchain", action="store_true", help="跳过链上数据")
    ap.add_argument("--no-context", action="store_true", help="跳过 1h 上下文合并表")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    sym = args.symbol
    manifest = {"symbol": sym, "generated_at": datetime.now(timezone.utc).isoformat(),
                "files": {}}

    def record(name, df, note, index_col=None):
        path = os.path.join(out_dir, name)
        rows, cols = save_csv(df, path, index_label=index_col)
        manifest["files"][name] = {"rows": rows, "cols": cols, "note": note}
        print(f"  saved {name}: {rows} 行 x {cols} 列")

    print(f"== A. Binance 合约衍生品数据 (symbol={sym}) ==")
    try:
        fr = fetch_funding_rate_history(sym)
        record(f"funding_rate_{sym.lower()}.csv", fr, "资金费率历史(8h, 2019-至今)", "funding_time")
    except Exception as e:
        print(f"  funding rate 失败: {e}")

    for kind, fname, note in [
        ("openInterestHist", "open_interest_1h.csv", "未平仓合约(1h)"),
        ("globalLongShortAccountRatio", "global_ls_account_ratio_1h.csv", "全市场多空账户比(1h)"),
        ("topLongShortAccountRatio", "top_trader_ls_account_ratio_1h.csv", "大户多空账户比(1h)"),
        ("topLongShortPositionRatio", "top_trader_ls_position_ratio_1h.csv", "大户多空持仓比(1h)"),
        ("takerlongshortRatio", "taker_buy_sell_ratio_1h.csv", "主动买卖量比(1h)"),
    ]:
        try:
            df = fetch_futures_data_series(kind, sym, period=args.ratio_period)
            record(fname, df, note, "time")
        except Exception as e:
            print(f"  {kind} 失败: {e}")

    try:
        mk = fetch_mark_price_klines(sym, "1h")
        record("mark_price_klines_1h.csv", mk, "标记价格K线(1h, 抗插针)", "open_time")
    except Exception as e:
        print(f"  mark price 失败: {e}")

    try:
        pi = fetch_premium_index(sym)
        record("premium_index_snapshot.csv", pi, "当前标记价/指数价/下期资金费率快照")
    except Exception as e:
        print(f"  premium index 失败: {e}")

    print("== B. 情绪数据 ==")
    try:
        fng = fetch_fear_greed_index()
        record("fear_greed_index.csv", fng, "恐惧贪婪指数(日, 2018-02 至今)", "date")
    except Exception as e:
        print(f"  fng 失败: {e}")

    if not args.no_onchain:
        print("== C. 链上数据 ==")
        try:
            oc = fetch_onchain_daily()
            record("onchain_btc_daily.csv", oc, "链上日频宽表 (blockchain.info)", "date")
        except Exception as e:
            print(f"  onchain 失败: {e}")

    if not args.no_macro:
        print("== D. 宏观数据 ==")
        try:
            macro = fetch_macro_daily()
            if not macro.empty:
                record("macro_daily.csv", macro, "宏观日频宽表 (Yahoo Finance)", "date")
        except Exception as e:
            print(f"  macro 失败: {e}")
        try:
            fred = fetch_fred_optional()
            if not fred.empty:
                record("fred_daily.csv", fred, "FRED 宏观日频 (可选)", "date")
        except Exception as e:
            print(f"  fred 失败: {e}")

    if not args.no_context:
        print("== E. 1h 上下文宽表 ==")
        try:
            ctx = build_context_table(sym, args.ratio_period, out_dir)
            record(f"btc_context_1h_{sym.lower()}.csv", ctx, "K线+资金费率+OI+多空比+FNG+宏观 对齐1h", "time")
        except Exception as e:
            print(f"  context 失败: {e}")

    manifest_path = os.path.join(out_dir, "additional_data_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"\n完成。数据目录: {out_dir}")
    print(f"清单: {manifest_path}")


if __name__ == "__main__":
    main()
