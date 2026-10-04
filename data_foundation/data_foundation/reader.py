# -*- coding: utf-8 -*-
"""
reader.py — L2 Certified 读取 API (L3 研究默认读取层)
======================================================
只读 certified 快照, 统一返回 UTC 时间列。

示例:
  from data_foundation.reader import load_candles
  df = load_candles("binance", "BTC-USDT", "1h")
  df = load_candles("binance", "BTC-USDT", "1h", as_of="2026-08-01")
  uni = load_universe(as_of="2026-08-22", layer="tradeable")   # 三层宇宙成员
"""
from __future__ import annotations

import os

import pandas as pd
import pyarrow.parquet as pq

from .config import CERTIFIED_DIR


def _as_utc(ts) -> pd.Timestamp:
    """任意 str / naive / tz-aware 时间戳 -> UTC aware (不重复传 tz= 造成冲突)。"""
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _dataset_root(dataset: str, venue: str, instrument: str, interval: str | None) -> str:
    parts = [CERTIFIED_DIR, dataset, venue, "spot", instrument]
    if interval:
        parts.append(f"interval={interval}")
    return os.path.join(*parts)


def load_candles(venue: str, instrument: str, interval: str = "1h",
                 as_of=None, cols: list[str] | None = None,
                 market_type: str = "spot", scope=None) -> pd.DataFrame:
    """读取 certified market_candle。as_of 做 PIT 过滤 (data_available_at <= as_of)。

    scope: 可选的 PoolScope (见 pool_registry.py)。传入后 as_of 被硬钳制到该池
    终点 —— 时间墙在 API 层强制, Agent 无法越界读取池外数据。
    """
    from .pool_registry import factor_available
    ds = f"market_candle_{market_type}_{interval}"
    lower = None
    if scope is not None:
        as_of = scope.clamp(as_of)
        lower = scope.pool.start_ts      # 池内取数: 下界 = 池起点
    if not factor_available(ds, as_of):
        raise ValueError(f"因子 {ds!r} 在 {as_of} 不可用 (因子屏蔽, 见 pool_registry.FACTOR_AVAILABILITY)")
    root = os.path.join(CERTIFIED_DIR, ds, venue, market_type, instrument,
                        f"interval={interval}")
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True)
    df = df.sort_values("open_time_utc").reset_index(drop=True)
    if as_of is not None:
        df = df[df["data_available_at"] <= _as_utc(as_of)]
    if lower is not None:
        # 池内取数: 剔除池起点之前的历史 (避免拿池外更早的数据)
        df = df[df["open_time_utc"] >= lower]
    if cols:
        df = df[[c for c in cols if c in df.columns]]
    return df


def load_derivatives(venue: str, instrument: str, dataset: str,
                     as_of=None, scope=None) -> pd.DataFrame:
    """读取 certified 衍生品数据集 (funding / open_interest / mark_price / ratio)。

    scope: 可选 PoolScope; 传入后 as_of 被钳制到该池终点 + 因子屏蔽生效。
    """
    from .pool_registry import clamp_factor_as_of
    lower = None
    if scope is not None:
        as_of = scope.clamp(as_of)
        lower = scope.pool.start_ts
    clamp_factor_as_of(dataset, as_of)
    root = os.path.join(CERTIFIED_DIR, dataset, venue, instrument)
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True)
    tcol = df.columns[0]
    df = df.sort_values(tcol).reset_index(drop=True)
    if as_of is not None and "data_available_at" in df.columns:
        df = df[df["data_available_at"] <= _as_utc(as_of)]
    if lower is not None:
        df = df[df[tcol] >= lower]
    return df


def load_instruments(venue_id: str | None = None, market_type: str | None = None,
                     as_of=None) -> pd.DataFrame:
    """读取 certified PIT instrument 元数据 (跨 venue 合并)。

    as_of 语义: 保留 data_available_at <= as_of 的每 (venue_id, symbol)
    最后一版快照 (取 max data_available_at); as_of=None 取最新版本。
    venue_id / market_type 可过滤; 返回 INSTRUMENT_COLUMNS 列。
    """
    from .schema import INSTRUMENT_COLUMNS
    root = os.path.join(CERTIFIED_DIR, "instrument")
    if venue_id:
        venues = [venue_id]
    elif os.path.isdir(root):
        venues = sorted(d for d in os.listdir(root)
                        if os.path.isdir(os.path.join(root, d)))
    else:
        venues = []
    frames = []
    for v in venues:
        d = os.path.join(root, v, "all")
        if not os.path.isdir(d):
            continue
        frames.append(pq.read_table(d).to_pandas())
    if not frames:
        return pd.DataFrame(columns=[c for c, _ in INSTRUMENT_COLUMNS])
    df = pd.concat(frames, ignore_index=True)
    for c in df.columns:
        if "time" in c or c == "data_available_at" or c.endswith("_utc"):
            df[c] = pd.to_datetime(df[c], utc=True)
    if market_type:
        df = df[df["market_type"] == market_type]
    if as_of is not None:
        df = df[df["data_available_at"] <= _as_utc(as_of)]
    # Binance 现货/永续 symbol 字符串相同, 去重键必须含 market_type
    df = df.sort_values("data_available_at").drop_duplicates(
        subset=["venue_id", "symbol", "market_type"], keep="last").reset_index(drop=True)
    cols = [c for c, _ in INSTRUMENT_COLUMNS]
    return df[[c for c in cols if c in df.columns]]


def load_universe(as_of=None, layer: str = "tradeable",
                  base_asset: str | None = None, scope=None) -> pd.DataFrame:
    """读取 certified universe_membership (三层交易宇宙) 成员快照。

    layer ∈ {"research", "backtest", "tradeable"}: 返回通过该层的成员行
    (schema 全部列, 外加 certified 附加列 is_suspect/quality_reason/date)。
    as_of: str | Timestamp, 归一化为 UTC 日过滤 date_utc; None = 全部日期。
    base_asset: 可选, 精确过滤统一基础资产 (如 "BTC")。
    scope: 可选 PoolScope; 传入后 as_of 被钳制到该池终点 (宇宙门控)。
    """
    from .schema import UNIVERSE_MEMBERSHIP_COLUMNS
    valid = ("research", "backtest", "tradeable")
    if layer not in valid:
        raise ValueError(f"layer 必须是 {valid} 之一, 收到: {layer!r}")
    if scope is not None and as_of is None:
        as_of = scope.end          # 默认取池末日做门控
    if scope is not None:
        as_of = scope.clamp(as_of)
    root = os.path.join(CERTIFIED_DIR, "universe_membership", "builder", "all")
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "date_utc" or c.endswith("_utc") \
                or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True)
    df = df[df[f"layer_{layer}"].fillna(False)]
    if as_of is not None:
        day = _as_utc(as_of).normalize()
        hi = day + pd.Timedelta(days=1)
        df = df[(df["date_utc"] >= day) & (df["date_utc"] < hi)]
    if base_asset is not None:
        df = df[df["base_asset"] == base_asset]
    cols = [c for c, _ in UNIVERSE_MEMBERSHIP_COLUMNS]
    return df[[c for c in cols if c in df.columns]].sort_values(
        ["date_utc", "symbol"]).reset_index(drop=True)


def load_manifest(dataset: str) -> dict:
    import json
    p = os.path.join(CERTIFIED_DIR, dataset, "manifest.json")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def load_asset_master(asset: str | None = None,
                      venue_id: str | None = None) -> pd.DataFrame:
    """读取 certified asset_master/master 最新全量快照。

    asset / venue_id 可选过滤 (精确匹配); 返回 ASSET_MASTER_COLUMNS 列
    (外加认证附加列 is_suspect/quality_reason/date)。
    """
    from .schema import ASSET_MASTER_COLUMNS
    root = os.path.join(CERTIFIED_DIR, "asset_master", "master", "all")
    if not os.path.isdir(root):
        raise FileNotFoundError(root)
    df = pq.read_table(root).to_pandas()
    for c in df.columns:
        if "time" in c or c == "data_available_at" or c.endswith("_utc"):
            df[c] = pd.to_datetime(df[c], utc=True)
    if asset is not None:
        df = df[df["asset"] == asset]
    if venue_id is not None:
        df = df[df["venue_id"] == venue_id]
    cols = [c for c, _ in ASSET_MASTER_COLUMNS]
    return df[[c for c in cols if c in df.columns]].reset_index(drop=True)
