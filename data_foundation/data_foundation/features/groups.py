# -*- coding: utf-8 -*-
"""groups.py — 分组维度 (设计文档 3.2: 加密的分组维度, 替换 A 股的行业)

设计文档决策 2: 分组分类**接现成数据源, 不手工维护全表**。本模块给出四个
可直接从数据底座 PIT 构造的维度 (不需要外部 API):

    market_cap_tier   市值分层 (大/中/小盘, 每日截面分位)
    listing_age_tier  上市时长分层 (新/中/老)
    venue             主交易所 (来自宇宙快照)
    quote             计价资产 (来自宇宙快照)

**PIT 关键**: 分组标签必须**逐日取当日宇宙快照**, 不能用窗口末日的市值给整段
历史打标签 —— 那是前视 (用"今天谁是大盘股"去划分 2019 年的组, 等于偷看了未来
的市值排名)。所以 build_group_labels 按 date_utc 逐日算, 再广播到 1h 网格。

(板块 sector / 公链 chain 需要 CoinGecko categories, 属外部数据源, 见待办;
本模块先把不依赖外部的四个维度做扎实。)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..pool_registry import PoolScope
from .. import reader

__all__ = ["GROUP_DIMENSIONS", "build_group_labels", "list_group_dimensions",
           "describe_group"]

#: 引擎/特征库可用的分组维度
GROUP_DIMENSIONS = ("market_cap_tier", "listing_age_tier", "venue", "quote")

#: 需要外部数据源 (CoinGecko categories) 才能做的维度 —— 显式登记为"未就绪",
#: 免得表达式里写了却静默拿不到标签
PENDING_DIMENSIONS = ("sector", "chain")

_CACHE: dict = {}


def _utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def list_group_dimensions(ready_only: bool = True) -> list[str]:
    return list(GROUP_DIMENSIONS) if ready_only else \
        list(GROUP_DIMENSIONS) + list(PENDING_DIMENSIONS)


def describe_group(dim: str) -> dict:
    """维度说明 (MCP/特征库展示用)。"""
    meta = {
        "market_cap_tier": {
            "desc": "市值分层: 每日截面按 market_cap_usd 三分位划 large/mid/small",
            "source": "universe_membership.market_cap_usd (PIT 逐日)",
            "use": "大盘/小盘的截面相对强弱 (加密没有行业, 规模是最稳的分组轴)"},
        "listing_age_tier": {
            "desc": "上市时长分层: age_days <180 / 180-730 / >730 天",
            "source": "universe_membership.age_days (PIT 逐日)",
            "use": "新币与老币的定价差异 (新币投机性强、老币机构持仓多)"},
        "venue": {
            "desc": "主交易所 (宇宙快照里该资产的 venue_id)",
            "source": "universe_membership.venue_id",
            "use": "跨所流动性/定价差异"},
        "quote": {
            "desc": "计价资产 (USDT/USDC/其他)",
            "source": "universe_membership.quote_asset",
            "use": "计价市场结构差异"},
        "sector": {"desc": "板块 (DeFi/L2/Meme/AI...)", "source": "CoinGecko categories",
                   "use": "板块轮动", "ready": False},
        "chain": {"desc": "所属公链", "source": "asset_master.chain_id",
                  "use": "公链生态轮动", "ready": False},
    }[dim]
    return {"dimension": dim, **meta,
            "ready": dim in GROUP_DIMENSIONS}


def _tier_labels(vals: pd.Series, edges_desc: list[tuple[float, str]],
                 default: str = "unknown") -> pd.Series:
    """按数值大小打分层标签。edges_desc = [(阈值, 标签), ...] 从大到小匹配。"""
    out = pd.Series(default, index=vals.index, dtype=object)
    remaining = vals.notna()
    for thr, lab in edges_desc:
        sel = remaining & (vals >= thr)
        out[sel] = lab
        remaining &= ~sel
    return out


def build_group_labels(dim: str, uni: pd.DataFrame, index: pd.MultiIndex) -> pd.Series:
    """把 (date, base_asset) 维度的日频标签广播到面板 (base_asset, time) 索引。

    uni   : 宇宙快照 (含 date_utc / base_asset / market_cap_usd / age_days /
            venue_id / quote_asset), 需覆盖面板时间范围的所有交易日
    index : 面板 MultiIndex (base_asset, time)
    返回   : 与 index 对齐的标签 Series (未知 -> "unknown")
    """
    if dim not in GROUP_DIMENSIONS:
        hint = " (该维度需要外部数据源, 尚未就绪)" if dim in PENDING_DIMENSIONS else ""
        raise KeyError(f"未知分组维度 {dim!r}{hint}; 可用: {list(GROUP_DIMENSIONS)}")
    if uni.empty:
        return pd.Series("unknown", index=index, dtype=object)

    days = pd.DatetimeIndex(index.get_level_values("time")).normalize()
    keys = pd.MultiIndex.from_arrays(
        [index.get_level_values("base_asset"), days], names=["base_asset", "date_utc"])

    u = uni.copy()
    u["date_utc"] = pd.to_datetime(u["date_utc"], utc=True).dt.normalize()
    u = u.drop_duplicates(subset=["date_utc", "base_asset"], keep="last")

    if dim == "market_cap_tier":
        # 每日截面三分位 (逐日算 —— PIT 正确: 2019 年的大盘股是按 2019 年的市值排的)
        u = u.dropna(subset=["market_cap_usd"])
        if u.empty:
            return pd.Series("unknown", index=index, dtype=object)
        q = u.groupby("date_utc")["market_cap_usd"].transform(
            lambda s: s.rank(pct=True))
        u["label"] = np.select(
            [q >= 2 / 3, q >= 1 / 3], ["large", "mid"], default="small")
    elif dim == "listing_age_tier":
        age = u["age_days"]
        u["label"] = np.select([age < 180, age < 730], ["new", "young"],
                               default="seasoned")
    elif dim == "venue":
        u["label"] = u["venue_id"].astype(object)
    elif dim == "quote":
        u["label"] = u["quote_asset"].astype(object)
    else:                                              # pragma: no cover
        raise KeyError(dim)

    labels = u.set_index(["base_asset", "date_utc"])["label"]
    out = labels.reindex(keys).to_numpy()
    return pd.Series(out, index=index, dtype=object).fillna("unknown")


def load_groups(scope: PoolScope, dims, index: pd.MultiIndex) -> dict:
    """按面板索引构造分组标签 (带缓存 —— 宇宙快照读取较贵)。

    dims  : 维度名列表
    index : 面板 MultiIndex (base_asset, time)
    返回   : {维度名: 与 index 对齐的标签 Series}
    """
    if isinstance(dims, str):
        dims = [dims]
    dims = [d for d in dict.fromkeys(dims)]
    need = [d for d in dims if d not in GROUP_DIMENSIONS]
    if need:
        hint = [d for d in need if d in PENDING_DIMENSIONS]
        raise KeyError(
            f"分组维度 {need} 不可用" +
            (f"; {hint} 需要外部数据源 (CoinGecko categories / asset_master.chain_id), "
             f"尚未接入" if hint else ""))

    times = index.get_level_values("time")
    lo, hi = _utc(times.min()), _utc(times.max())
    out: dict[str, pd.Series] = {}
    uni_cache: pd.DataFrame | None = None
    for d in dims:
        key = (d, str(lo.date()), str(hi.date()))
        if key in _CACHE:
            out[d] = _CACHE[key].reindex(index)
            continue
        if uni_cache is None:
            # 一次读整段宇宙 (按日行存), 再按窗口过滤 —— 不要逐日 load_universe
            # (那会把 830k 行的表读 365 次)
            allu = reader.load_universe(as_of=None, layer=scope.layer)
            if allu.empty:
                raise ValueError("宇宙快照为空 —— 无法构造分组维度")
            days = pd.to_datetime(allu["date_utc"], utc=True).dt.normalize()
            uni_cache = allu[(days >= lo.normalize()) & (days <= hi.normalize())].copy()
            uni_cache["date_utc"] = days[(days >= lo.normalize())
                                         & (days <= hi.normalize())].to_numpy()
        s = build_group_labels(d, uni_cache, index)
        _CACHE[key] = s
        out[d] = s
    return out


if __name__ == "__main__":  # pragma: no cover
    s = PoolScope("oof")
    idx = pd.MultiIndex.from_product(
        [["BTC", "ETH"], pd.date_range("2022-01-01", periods=24, freq="h", tz="UTC")],
        names=["base_asset", "time"])
    g = load_groups(s, ["market_cap_tier", "listing_age_tier"], idx)
    for k, v in g.items():
        print(f"{k}: {v.value_counts().to_dict()}")