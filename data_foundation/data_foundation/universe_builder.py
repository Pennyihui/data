# -*- coding: utf-8 -*-
"""
universe_builder.py — 三层交易宇宙构建器 (研究 ⊇ 回测 ⊇ 交易) + 每日成员快照
============================================================================

输入 (全部本地, 不联网):
  - L1 listing_universe/binance_vision (Binance Vision 月度归档枚举, 3695 对)
    (缺失时回退 L2 certified/listing_universe)
  - L1 asset_master/master (symbol -> base_asset/quote_asset 跨所映射, 7226 行)
    (缺失时回退 L2 certified/asset_master)
  - L2 certified/market_candle_spot_1h/binance/spot/{INST}/interval=1h
    (动态扫描该目录下全部 instrument: 15 MVP + G 回填 9 下架 + J 扩容 38
     CMC 流动性门槛币; 缺失时回退 L1) —— 流动性/缺口率来源
  - CMC 市值史: <project_root>/../data_new/additional/cmc_daily_marketcap_ranking.csv
    (按 base_asset 匹配填 market_cap_usd; 匹配不到为 NaN, 市值门槛不否决)

输出:
  - L1: data/l1/universe_membership/builder/data.parquet
  - L2: data/l2/certified/universe_membership/builder/all/data.parquet + manifest
        (aggregation_rules 完整写入 DEFAULT_RULES + 三层定义)

三层语义:
  layer_research   : as_of ∈ [first_trade_date, last_trade_date], 由 first/last_period
                     转月首/月末近似; status=active 视为无上界 (仍在交易);
                     与 K 线有无无关 —— PIT 真实存在即可
  layer_backtest   : research + age_days>=min_age_days + 有K线数据 +
                     gap_ratio_30d<=max_gap_ratio_30d + 近30日有数据天数>=min_history_days
  layer_tradeable  : backtest + median_volume_30d_usd>=min_median_volume_30d_usd +
                     (市值已知时) market_cap_usd>=min_market_cap_usd

性能: 全部有 K 线 instrument 的日频成交额/缺口序列 + CMC 市值 + 上市宇宙一次性装载为
模块级缓存 (_UniverseData), 日循环内只做内存切片, 不反复读 parquet。

用法:
  python -m data_foundation.universe_builder                  # 全量快照 + 验证
  python -m data_foundation.universe_builder --daily          # 只补缺失日期 (增量)
  python -m data_foundation.universe_builder --verify-only    # 只打印验证
  python -m data_foundation.universe_builder --start 2021-01-01 --end 2021-12-31
  python -m data_foundation.universe_builder --quote ""       # 不过滤 quote (全部 3695 对)
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

from .config import CERTIFIED_DIR, L1_DIR, PROJECT_ROOT
from .l1_onchain import write_onchain_parquet
from .l2 import build_dataset_manifest, certify_derivatives, write_certified_derivatives
from .schema import UNIVERSE_MEMBERSHIP_COLUMNS

# ---------------------------------------------------------------------------
# 默认规则 (可覆盖; 完整写入 manifest aggregation_rules)
# ---------------------------------------------------------------------------
DEFAULT_RULES = {
    "quote": "USDT",                      # 报价资产过滤; None = 不过滤
    "min_age_days": 90,                   # 回测层: 最低上市年龄
    "min_median_volume_30d_usd": 1_000_000,   # 交易层: 近30日中位日成交额门槛
    "min_market_cap_usd": 50_000_000,          # 交易层: 市值门槛 (未知则不否决)
    "max_gap_ratio_30d": 0.05,                 # 回测层: 近30日缺口率上限
    "min_history_days_for_volume": 30,         # 回测层: 近30日窗口内有数据天数下限
}

LAYER_DEFINITIONS = {
    "layer_research": "as_of ∈ [first_trade_date, last_trade_date], 由 first/last_period "
                      "转月首/月末近似; status=active 视为无上界; 与 K 线有无无关 (PIT 真实存在即可)",
    "layer_backtest": "research 且 age_days >= min_age_days 且 有 K 线数据且 "
                      "gap_ratio_30d <= max_gap_ratio_30d 且 近30日有数据天数 "
                      ">= min_history_days_for_volume",
    "layer_tradeable": "backtest 且 median_volume_30d_usd >= min_median_volume_30d_usd 且 "
                       "(market_cap_usd 已知时 >= min_market_cap_usd)",
}

# 近30日滚动窗口: 取 as_of 之前的 30 个完整自然日 (bars open_time_utc < date_utc, PIT 安全)
_VOLUME_WINDOW_DAYS = 30
_SOURCE_BATCH = "universe_membership_v1"
_MCAP_CSV = os.path.join(PROJECT_ROOT, "..", "data_new", "additional",
                         "cmc_daily_marketcap_ranking.csv")

# 兜底 symbol 拆分用的报价后缀 (asset_master 覆盖不到的极少数对)
_QUOTE_SUFFIXES = ["USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP", "DAI",
                   "AEUR", "BIDR", "IDRT", "EUR", "TRY", "GBP", "AUD", "BRL",
                   "RUB", "UAH", "NGN", "PLN", "RON", "ARS", "USD"]


def _parse_symbol(symbol: str) -> tuple[str, str]:
    """兜底: BTCUSDT -> (BTC, USDT); 无法识别 -> (symbol, "")。"""
    for q in sorted(_QUOTE_SUFFIXES, key=len, reverse=True):
        if symbol.endswith(q) and len(symbol) > len(q):
            return symbol[: -len(q)], q
    return symbol, ""


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[c for c, _ in UNIVERSE_MEMBERSHIP_COLUMNS])


def _as_utc(x) -> pd.Timestamp:
    """任意 str/Timestamp (naive 或 tz-aware) -> UTC 归一化 Timestamp。"""
    t = pd.Timestamp(x)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return t.normalize()


# ---------------------------------------------------------------------------
# 一次性数据装载 (模块级缓存)
# ---------------------------------------------------------------------------
class _UniverseData:
    """全部静态输入的一次性装载: 上市宇宙 + asset 映射 + 24 instrument 日频序列 + CMC 市值。"""

    def __init__(self):
        self.listing = self._load_listing()
        asset_map, quote_map = self._load_asset_map()
        syms = self.listing["symbol"]
        self.listing["base_asset"] = syms.map(
            lambda s: asset_map.get(s) or _parse_symbol(s)[0])
        self.listing["quote_asset"] = syms.map(
            lambda s: quote_map.get(s) or _parse_symbol(s)[1])
        self.candle_daily, self.candle_batches = self._load_candle_daily()
        self.cmc_pivot, self.cmc_index = self._load_cmc()

    # -- 上市宇宙 (L1, 回退 L2) ------------------------------------------------
    @staticmethod
    def _load_listing() -> pd.DataFrame:
        p1 = os.path.join(L1_DIR, "listing_universe", "binance_vision", "data.parquet")
        p2 = os.path.join(CERTIFIED_DIR, "listing_universe", "binance_vision",
                          "all", "data.parquet")
        p = p1 if os.path.exists(p1) else p2
        df = pd.read_parquet(p)
        if "data_available_at" in df.columns:
            df["data_available_at"] = pd.to_datetime(df["data_available_at"], utc=True)
        return df

    # -- asset_master 映射 (L1, 回退 L2) --------------------------------------
    @staticmethod
    def _load_asset_map() -> tuple[dict, dict]:
        p1 = os.path.join(L1_DIR, "asset_master", "master", "data.parquet")
        p2 = os.path.join(CERTIFIED_DIR, "asset_master", "master", "all", "data.parquet")
        p = p1 if os.path.exists(p1) else p2
        am = pd.read_parquet(p)
        sub = am[(am["venue_id"] == "binance") & (am["market_type"] == "spot")]
        asset_map = dict(zip(sub["symbol"], sub["asset"]))
        quote_map = dict(zip(sub["symbol"], sub["quote_asset"].fillna("")))
        return asset_map, quote_map

    # -- 全部有 K 线 instrument 的日频序列: {symbol: DataFrame(date, daily_vol, has_gap)} --
    @staticmethod
    def _load_candle_daily() -> tuple[dict, list]:
        roots = [
            os.path.join(CERTIFIED_DIR, "market_candle_spot_1h", "binance", "spot"),
            os.path.join(L1_DIR, "market_candle_spot_1h", "binance", "spot"),
        ]
        out: dict[str, pd.DataFrame] = {}
        batches: list[str] = []
        for root in roots:
            if not os.path.isdir(root):
                continue
            for inst in sorted(os.listdir(root)):
                p = os.path.join(root, inst, "interval=1h", "data.parquet")
                if not os.path.exists(p):
                    continue
                d = pd.read_parquet(p, columns=["symbol", "open_time_utc",
                                                "volume_quote", "is_gap",
                                                "source_batch_id"])
                sym = d["symbol"].iloc[0]
                if sym in out:  # L2 已装载则跳过 L1
                    continue
                d["open_time_utc"] = pd.to_datetime(d["open_time_utc"], utc=True)
                d["date"] = d["open_time_utc"].dt.normalize()
                g = (d.groupby("date")
                      .agg(daily_vol=("volume_quote", "sum"),
                           has_gap=("is_gap", "max"))
                      .sort_index())
                g["daily_vol"] = pd.to_numeric(g["daily_vol"], errors="coerce")
                out[sym] = g
                b = d["source_batch_id"].iloc[0]
                if isinstance(b, str) and b:
                    batches.append(b)
            if out:
                break
        return out, sorted(set(batches))

    # -- CMC 日频市值: 宽表 (date × symbol, ffill) -----------------------------
    @staticmethod
    def _load_cmc() -> tuple[pd.DataFrame, pd.DatetimeIndex]:
        if not os.path.exists(_MCAP_CSV):
            return pd.DataFrame(), pd.DatetimeIndex([])
        df = pd.read_csv(_MCAP_CSV, usecols=["date", "symbol", "marketcap"])
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize()
        pivot = (df.pivot_table(index="date", columns="symbol",
                                values="marketcap", aggfunc="first")
                 .sort_index().ffill())
        return pivot, pivot.index

    # -- 某日的市值行 (dict: CMC symbol -> marketcap, 取 date <= as_of 最近值) --
    def mcap_row(self, as_of: pd.Timestamp) -> dict:
        if len(self.cmc_pivot) == 0:
            return {}
        idx = self.cmc_index.searchsorted(as_of, side="right") - 1
        if idx < 0:
            return {}
        return self.cmc_pivot.iloc[idx].to_dict()


_DATA: _UniverseData | None = None


def _get_data() -> _UniverseData:
    global _DATA
    if _DATA is None:
        _DATA = _UniverseData()
    return _DATA


def reset_cache():
    """清空模块级缓存 (测试/热重载用)。"""
    global _DATA
    _DATA = None


# ---------------------------------------------------------------------------
# 核心: 单日宇宙
# ---------------------------------------------------------------------------
def build_universe(as_of, rules: dict | None = None) -> pd.DataFrame:
    """构建 as_of 当日三层宇宙成员快照 (research 成员行, 全 schema 列)。

    as_of: str | Timestamp, 归一化为 UTC 日。
    rules: 覆盖 DEFAULT_RULES 的字典 (逐键覆盖)。
    """
    rules = {**DEFAULT_RULES, **(rules or {})}
    as_of = _as_utc(as_of)
    d = _get_data()

    lu = d.listing
    quote = rules.get("quote")
    if quote:
        lu = lu[lu["quote_asset"] == quote]
    if lu.empty:
        return _empty_frame()

    # first/last_period -> 月首/月末近似 (PIT 真实存在窗口)
    first = pd.to_datetime(lu["first_period"] + "-01", utc=True).dt.normalize()
    last = (pd.to_datetime(lu["last_period"] + "-01", utc=True)
            + pd.offsets.MonthEnd(0)).dt.normalize()
    alive = (as_of >= first) & ((lu["status"] == "active") | (as_of <= last))
    out = lu[alive].copy()
    if out.empty:
        return _empty_frame()

    out["date_utc"] = as_of
    out["first_trade_date"] = first[alive].to_numpy()
    out["last_trade_date"] = last[alive].to_numpy()
    out["age_days"] = ((as_of - out["first_trade_date"]).dt.days).astype("int64")
    out["venue_id"] = "binance"
    out["market_type"] = "spot"

    # 流动性/缺口 (仅对有 K 线的 symbol; 其余保持 NaN)
    out["avg_volume_30d_usd"] = np.nan
    out["median_volume_30d_usd"] = np.nan
    out["gap_ratio_30d"] = np.nan
    out["_hist_days"] = 0
    syms = set(out["symbol"])
    window_start = as_of - pd.Timedelta(days=_VOLUME_WINDOW_DAYS)
    for sym, g in d.candle_daily.items():
        if sym not in syms:
            continue
        idx = g.index
        lo = idx.searchsorted(window_start, side="left")
        hi = idx.searchsorted(as_of, side="left")
        w = g.iloc[lo:hi]
        n = len(w)
        if n == 0:
            continue
        m = out["symbol"] == sym
        out.loc[m, "avg_volume_30d_usd"] = w["daily_vol"].mean()
        out.loc[m, "median_volume_30d_usd"] = w["daily_vol"].median()
        out.loc[m, "gap_ratio_30d"] = w["has_gap"].sum() / n
        out.loc[m, "_hist_days"] = int(n)

    # 市值 (CMC 按 base_asset 匹配; 匹配不到为 NaN)
    out["market_cap_usd"] = out["base_asset"].map(d.mcap_row(as_of))

    # 三层
    age_ok = out["age_days"] >= rules["min_age_days"]
    has_data = out["_hist_days"] >= 1
    gap_ok = out["gap_ratio_30d"].fillna(np.inf) <= rules["max_gap_ratio_30d"]
    hist_ok = out["_hist_days"] >= rules["min_history_days_for_volume"]
    layer_backtest = age_ok & has_data & gap_ok & hist_ok
    vol_ok = out["median_volume_30d_usd"].fillna(-np.inf) >= rules["min_median_volume_30d_usd"]
    mcap_known = out["market_cap_usd"].notna()
    mcap_ok = (~mcap_known) | (out["market_cap_usd"] >= rules["min_market_cap_usd"])
    layer_tradeable = layer_backtest & vol_ok & mcap_ok
    out["layer_research"] = True
    out["layer_backtest"] = layer_backtest
    out["layer_tradeable"] = layer_tradeable
    out["data_available_at"] = as_of
    out["source_batch_id"] = _SOURCE_BATCH
    out = out.drop(columns=["_hist_days"])

    cols = [c for c, _ in UNIVERSE_MEMBERSHIP_COLUMNS]
    out = out[cols]
    for c in ("date_utc", "first_trade_date", "last_trade_date", "data_available_at"):
        out[c] = pd.to_datetime(out[c], utc=True).astype("datetime64[us, UTC]")
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 写入: L1 + L2 + manifest
# ---------------------------------------------------------------------------
def _write_all(df: pd.DataFrame, rules: dict) -> tuple[str, str]:
    df = df.copy()
    for c in ("date_utc", "first_trade_date", "last_trade_date", "data_available_at"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True).astype("datetime64[us, UTC]")
    df = df.sort_values(["date_utc", "symbol"]).reset_index(drop=True)

    l1_root = write_onchain_parquet(df, "universe_membership", "builder", "date_utc")

    # core_numeric_cols=[] 语义 = 明确不做数值列检查 (l2.py):
    # research 层大部分 symbol 无 K 线, avg_volume_30d_usd 等为 NaN 是合法状态,
    # 不视为 suspect; 仅检查主键唯一与时间不超可用时间。
    cdf = certify_derivatives(df, "date_utc", core_numeric_cols=[],
                              key_cols=["symbol", "date_utc"])
    l2_root = write_certified_derivatives(cdf, "universe_membership", "builder",
                                          "all", "date_utc")
    stats = {
        "row_count": int(len(cdf)),
        "duplicate_count": int(cdf.duplicated(["symbol", "date_utc"]).sum()),
        "gap_count": 0,
        "suspect_count": int(cdf["is_suspect"].sum()),
        "coverage_start": str(cdf["date_utc"].min()),
        "coverage_end": str(cdf["date_utc"].max()),
    }
    source_batches = ["universe_v1", _SOURCE_BATCH] + list(_get_data().candle_batches)
    aggregation_rules = {
        "rules": rules,
        "layer_definitions": LAYER_DEFINITIONS,
        "volume_window": (f"trailing {_VOLUME_WINDOW_DAYS} 个完整自然日 (bars open_time_utc "
                          f"< date_utc, PIT), 1h volume_quote 按日聚合求和; avg/median 为窗口内"
                          f"有数据日的均值/中位数; gap_ratio_30d = 窗口内有缺口日/有数据日; "
                          f"无 K 线 symbol 三列为 NaN 且 backtest/tradeable=False"),
        "first_last_period": ("first_period -> 当月首日, last_period -> 当月末日 (月末近似); "
                              "status=active 视为无上界 (仍在上架, 存储的 last_trade_date 仅记录"
                              "归档末月, research 判定对 active 不做上界约束)"),
        "market_cap": ("CMC 日频市值 (data_new/additional/cmc_daily_marketcap_ranking.csv), "
                       "按 base_asset 匹配, 取 date <= date_utc 的最近值; 匹配不到为 NaN, "
                       "市值门槛不否决 (仅市值已知时生效)"),
        "note": ("research 成员 = as_of 当日 PIT 真实存在的 symbol (仅存成员行, 非成员无行); "
                 "2026-08 扩容: K 线覆盖 CMC 流动性门槛币种 (历史日成交额>=1M USD) "
                 "(子代理 J, expand_v2 REST 分页 + delisted_v2 Vision 归档 + LUNAUSDT 手动补), "
                 "tradeable/backtest 层基于真实 K 线流动性证据; "
                 "仍无 K 线的 research 成员 avg_volume_30d_usd=NaN -> certified 仅查主键唯一"
                 "与时间边界 (core_numeric_cols=[]), NaN 为合法状态不标 suspect"),
    }
    build_dataset_manifest("universe_membership", "builder", "*", "*", "*",
                           stats, source_batches, aggregation_rules)
    return l1_root, l2_root


# ---------------------------------------------------------------------------
# 全量快照
# ---------------------------------------------------------------------------
def stage_snapshot(start="2020-01-01", end=None, rules: dict | None = None,
                   verbose: bool = True) -> dict:
    """逐日调用 build_universe, 合并全部日期, 一次写入 L1/L2 + manifest。"""
    rules = {**DEFAULT_RULES, **(rules or {})}
    start = _as_utc(start)
    end = _as_utc(end) if end is not None else pd.Timestamp.now(tz="UTC").normalize()
    days = pd.date_range(start, end, freq="D")

    frames = []
    t0 = pd.Timestamp.now(tz="UTC")
    for i, day in enumerate(days, 1):
        frames.append(build_universe(day, rules))
        if verbose and (i % 100 == 0 or i == len(days)):
            print(f"  [snapshot] {i}/{len(days)} 日 "
                  f"({(pd.Timestamp.now(tz='UTC') - t0).total_seconds():.0f}s)",
                  flush=True)
    df = pd.concat(frames, ignore_index=True) if frames else _empty_frame()
    df = df.drop_duplicates(["symbol", "date_utc"], keep="last")
    _write_all(df, rules)

    res = {
        "total_rows": int(len(df)),
        "days": int(len(days)),
        "start": str(start.date()),
        "end": str(end.date()),
        "quote": rules.get("quote"),
    }
    if verbose:
        print(f"  [snapshot] 完成: {res['total_rows']:,} 行 / {res['days']} 日")
    return res


# ---------------------------------------------------------------------------
# 幂等增量: 只补缺失日期
# ---------------------------------------------------------------------------
def run_daily_entry(start="2020-01-01", end=None, rules: dict | None = None,
                    verbose: bool = True) -> dict:
    """增量: 查 L1 universe_membership 已有最大 date_utc, 只构建缺失日期, 合并重写。

    不改 run_daily.py (由主代理负责注册本函数)。
    """
    rules = {**DEFAULT_RULES, **(rules or {})}
    end = _as_utc(end) if end is not None else pd.Timestamp.now(tz="UTC").normalize()
    start = _as_utc(start)

    p = os.path.join(L1_DIR, "universe_membership", "builder", "data.parquet")
    existing = None
    max_d = None
    if os.path.exists(p):
        existing = pd.read_parquet(p)
        if "date_utc" in existing.columns:
            existing["date_utc"] = pd.to_datetime(existing["date_utc"], utc=True)
        if len(existing):
            max_d = existing["date_utc"].max().normalize()

    if max_d is None:
        missing = pd.date_range(start, end, freq="D")
    else:
        missing = pd.date_range(max_d + pd.Timedelta(days=1), end, freq="D")

    if len(missing) == 0:
        res = {"added_days": 0, "total_rows": int(len(existing)) if existing is not None else 0,
               "start": str(start.date()), "end": str(end.date()), "quote": rules.get("quote"),
               "note": "无缺失日期, 未重写"}
        if verbose:
            print(f"  [daily] 无缺失日期 (已有至 {max_d.date()}), 跳过")
        return res

    new = pd.concat([build_universe(d, rules) for d in missing], ignore_index=True)
    df = pd.concat([existing, new], ignore_index=True) if existing is not None else new
    df = df.drop_duplicates(["symbol", "date_utc"], keep="last")
    _write_all(df, rules)

    res = {"added_days": int(len(missing)), "total_rows": int(len(df)),
           "start": str(start.date()), "end": str(end.date()), "quote": rules.get("quote")}
    if verbose:
        print(f"  [daily] 补 {len(missing)} 日, 合并后 {len(df):,} 行")
    return res


# ---------------------------------------------------------------------------
# 收尾验证
# ---------------------------------------------------------------------------
def verify_outputs(verbose: bool = True) -> dict:
    """从 L2 certified 读取快照, 打印任务要求的验证项。"""
    from .reader import load_universe

    print("=" * 80)
    print("universe_membership 快照验证 (读 L2 certified)")
    print("=" * 80)
    full = load_universe(layer="research")
    if full.empty:
        print("  快照为空 — 请先运行 stage_snapshot 或 run_daily_entry")
        return {"total_rows": 0}
    max_date = full["date_utc"].max().normalize()
    print(f"快照总行数        : {len(full):,}")
    print(f"日期范围          : {full['date_utc'].min().date()} ~ {max_date.date()} "
          f"({full['date_utc'].nunique()} 日)")

    # 按年抽样三层成员数
    samples = ["2020-06-01", "2022-06-01", "2024-06-01", str(max_date.date())]
    print("\n按年抽样三层成员数:")
    sample_stats = {}
    for s in samples:
        day = full[full["date_utc"] == pd.Timestamp(s, tz="UTC").normalize()]
        r = int(day["layer_research"].sum())
        b = int(day["layer_backtest"].sum())
        t = int(day["layer_tradeable"].sum())
        sample_stats[s] = {"research": r, "backtest": b, "tradeable": t}
        print(f"  {s}: research={r:>4}  backtest={b:>4}  tradeable={t:>4}")

    # 2021-06-01 tradeable top10 by market cap
    day = full[full["date_utc"] == pd.Timestamp("2021-06-01", tz="UTC")]
    t10 = (day[day["layer_tradeable"]]
           .sort_values("market_cap_usd", ascending=False, na_position="last")
           .head(10))
    print("\n2021-06-01 tradeable 层 top10 (按市值):")
    print(t10[["symbol", "median_volume_30d_usd", "market_cap_usd"]]
          .to_string(index=False, float_format=lambda x: f"{x:,.0f}"))

    # XMRUSDT 生命周期示例
    x = full[full["symbol"] == "XMRUSDT"]
    print("\nXMRUSDT (2019-03 上市, 2024-02 下架) 生命周期各层变化:")
    for s in ["2019-04-15", "2019-08-01", "2021-06-01", "2024-01-15",
              "2024-02-25", "2024-03-05"]:
        row = x[x["date_utc"] == pd.Timestamp(s, tz="UTC")]
        if row.empty:
            print(f"  {s}: (非 research 成员 — 未上市或已下架, 无行)")
            continue
        r = row.iloc[0]
        print(f"  {s}: age={int(r['age_days']):>4}d  avg_vol={r['avg_volume_30d_usd']:>14,.0f}"
              f"  gap={r['gap_ratio_30d']:.3f}  mcap={r['market_cap_usd']:>13,.0f}  "
              f"R={int(r['layer_research'])} B={int(r['layer_backtest'])} "
              f"T={int(r['layer_tradeable'])}")

    # load_universe 冒烟
    print("\nload_universe 冒烟测试:")
    t = load_universe(as_of=str(max_date.date()), layer="tradeable")
    print(f"  load_universe(as_of={max_date.date()}, layer='tradeable'): "
          f"{len(t)} 个 symbol")
    btc = load_universe(layer="research", base_asset="BTC")
    print(f"  load_universe(layer='research', base_asset='BTC'): {len(btc)} 行 "
          f"({btc['date_utc'].nunique()} 日, symbols={sorted(btc['symbol'].unique())})")
    b = load_universe(as_of=str(max_date.date()), layer="backtest", base_asset="ETH")
    print(f"  load_universe(as_of={max_date.date()}, layer='backtest', "
          f"base_asset='ETH'): {len(b)} 行")

    # 默认参数三层数量级对比
    print("\n默认参数三层数量级对比 (日均成员数 / 累计成员日):")
    g = full.groupby(full["date_utc"].dt.normalize())
    summary = {}
    for layer, col in (("research", "layer_research"), ("backtest", "layer_backtest"),
                       ("tradeable", "layer_tradeable")):
        if layer == "research":
            avg = float(g.size().mean())          # 所有行都是 research 成员
            tot = int(len(full))
        else:
            avg = float(g[col].sum().mean())
            tot = int(full[col].sum())
        summary[layer] = {"daily_avg": avg, "total": tot}
        print(f"  {layer:<9}: 日均 {avg:>8,.0f}  |  累计成员日 {tot:>10,}")
    return {"total_rows": int(len(full)), "max_date": str(max_date.date()),
            "samples": sample_stats, "layer_summary": summary}


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(description="三层交易宇宙构建器")
    ap.add_argument("--start", default="2020-01-01", help="快照起始日 (默认 2020-01-01)")
    ap.add_argument("--end", default=None, help="快照结束日 (默认今天)")
    ap.add_argument("--quote", default=None,
                    help="覆盖 rules.quote (默认 USDT; 传空串表示不过滤 quote)")
    ap.add_argument("--daily", action="store_true", help="增量补缺 (run_daily_entry)")
    ap.add_argument("--verify-only", action="store_true", help="只打印验证, 不构建")
    args = ap.parse_args(argv)

    rules = dict(DEFAULT_RULES)
    if args.quote is not None:
        rules["quote"] = None if args.quote == "" else args.quote

    if args.verify_only:
        verify_outputs()
        return
    if args.daily:
        res = run_daily_entry(start=args.start, end=args.end, rules=rules)
    else:
        res = stage_snapshot(start=args.start, end=args.end, rules=rules)
    print(f"\n构建结果: {res}")
    verify_outputs()


if __name__ == "__main__":
    main()
