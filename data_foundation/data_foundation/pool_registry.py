# -*- coding: utf-8 -*-
"""pool_registry.py — 研究池注册表与时间墙强制

设计文档: docs/research-pools-timewall-design.md (v1.0 定稿)

核心机制
--------
时间墙不能靠"约定", 必须靠 API 强制。本模块是那道墙:

  1. 池边界    三个池 (OOF 开发 / 反馈 / OOS) + 两条隔离带 (purge+embargo)
  2. as_of 钳制  clamp(as_of, pool.end) —— 参数只能往前调, 不能往后调
  3. 因子屏蔽   因子早于 availability_start 时不可用 (防幸存时段偏差)
  4. 宇宙门控   instrument 必须属于 universe(pool 末日, layer)

Agent/研究员传入 as_of="2026-10-01" 但会话绑定在池1 时, 返回的最大日期仍是
池1 的结束日。这是唯一靠得住的防线 —— 对 Agent 尤其重要, 因为它不知道有墙,
只会看到工具返回什么。

四个池
------
    oof    开发池    2018-01-01 ~ 2023-12-31
    gap1   隔离带1   2024-01-01 ~ 2024-01-14 (purge + embargo, 14 天)
    valid  反馈池    2024-01-15 ~ 2025-06-30
    gap2   隔离带2   2025-07-01 ~ 2025-07-14
    oos    OOS 池    2025-07-15 ~ 2026-09-30  (冻结)
    rolling_oos 滚动OOS 2026-10-01 ~ (每季度开启)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import pandas as pd

from .config import CERTIFIED_DIR

# ---------------------------------------------------------------------------
# 因子可用起点 (实测 2026-10-03, 决定因子屏蔽)
# 语义: 因子在其 availability_start 之前不可用 —— 否则会得到"某因子只在最近
# N 天有效"的假象 (幸存时段偏差)。决策问题 2: 池1 不后移, 靠因子屏蔽解决。
# ---------------------------------------------------------------------------
FACTOR_AVAILABILITY = {
    # 价格
    "market_candle_spot_1h": "2015-07-20",
    "market_candle_spot_4h": "2017-08-17",
    "market_candle_perpetual_1h": "2019-09-08",
    "market_candle_perpetual_4h": "2019-09-08",
    # 衍生品
    "derivatives_funding": "2019-09-10",
    "derivatives_mark_price": "2019-12-23",
    "derivatives_index_price": "2019-12-23",
    "basis_1h": "2019-12-16",
    "derivatives_open_interest": "2020-09-01",     # 池1 前 8 个月屏蔽
    "derivatives_ratio_glsr": "2020-09-01",        # 池1 前 8 个月屏蔽
    "derivatives_ratio_tlsr_acct": "2020-09-01",
    "derivatives_ratio_tlsr_pos": "2020-09-01",
    "derivatives_ratio_taker": "2020-09-01",
    "derivatives_oi_cross": "2020-07-20",
    # 宏观/情绪
    "macro_daily": "2005-01-03",
    "sentiment_fng": "2018-02-01",
    # 稳定币
    "stablecoin_supply": "2015-02-25",
    "stablecoin_peg": "2018-12-15",
    "stablecoin_flows": "2022-11-27",
    "stablecoin_mint_burn": "2015-02-25",
    # 链上 (仅滚动 OOS 可用, 决策问题 4)
    "onchain_daily_aggregate": "2026-08-01",
    "token_transfer": "2026-08-01",
    "dex_volume": "2014-02-17",
    "btc_blocks": "2026-08-19",
    "btc_network_daily": "2009-01-03",
    "cm_asset_daily": "2009-01-03",
    # 元数据
    "instrument": "2026-08-20",
    "asset_master": "2026-08-20",
    "universe_membership": "2017-08-01",   # P0 已回填
}

# 默认因子集 (主研究线, 含衍生品; 已剔除修订污染源)
DEFAULT_FACTOR_SET = [
    "market_candle_spot_1h",
    "market_candle_perpetual_1h",
    "derivatives_funding",
    "derivatives_mark_price",
    "derivatives_index_price",
    "derivatives_open_interest",
    "derivatives_ratio_glsr",
    # 注意: macro_daily / stablecoin_supply 已移出默认集 ——
    #   macro_daily  = revision_contaminated (泄露路径 #6)
    #   stablecoin_supply = 缺 data_available_at, 无法 PIT
    # 若要用宏观, 先做"仅保留近期窗口"或改为每日快照自积累 (见文档 6.1)
]


def _u(s: str) -> pd.Timestamp:
    """字符串/时间戳 -> UTC 归一化日"""
    t = pd.Timestamp(s)
    return (t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")).normalize()


# ---------------------------------------------------------------------------
# 修订污染源 (泄露路径 #6, 2026-10-04 审计 _audit_revision_leak.py 结果)
# ---------------------------------------------------------------------------
# 这些数据集的历史是"事后抓的全量数据", data_available_at 指向数年之后,
# 内容可能已被上游修正 —— 用它们回测历史 = 用了事后才知道的信息。
# 审计证据 (lag = data_available_at - 事件时间, 单位天):
#   cm_asset_daily    lag 中位 2110d, 早期 4110d, 近期 1585d (漂移 +2525d)
#   macro_daily       lag 中位 3937d, 早期 5917d, 近期 1945d (漂移 +3972d)
#   btc_network_daily lag 中位 3069d, 早期 4756d, 近期 1614d (漂移 +3142d)
# 对比: 核心价格因子 (spot 1h / funding / mark) lag <= 0.04d, PIT 干净。
REVISION_CONTAMINATED = {
    "macro_daily",
    "cm_asset_daily",
    "btc_network_daily",
}

# 无 data_available_at 列 -> 无法做 PIT 过滤, 等同不可用于历史回测
NO_PIT_COLUMN = {
    "stablecoin_supply",
    "stablecoin_flows",
    "dex_volume",
}


@dataclass(frozen=True)
class Pool:
    """一个研究池 (或隔离带)。"""
    pool_id: str
    start: str
    end: str | None          # None = 开放 (滚动 OOS)
    kind: str                # dev | valid | oos | gap | rolling_oos
    name: str
    factors: tuple[str, ...] = tuple(DEFAULT_FACTOR_SET)
    desc: str = ""

    @property
    def start_ts(self) -> pd.Timestamp:
        return _u(self.start)

    @property
    def end_ts(self) -> pd.Timestamp | None:
        return None if self.end is None else _u(self.end)

    def clamp(self, as_of=None) -> pd.Timestamp | None:
        """核心: 把 as_of 双向钳制到本池范围内。

        语义: 查询**必须**落在本池 [start, end] 内。
        - as_of=None       -> 返回 None (不钳制, 交给下游 PIT 过滤)
        - as_of 晚于池终点  -> 返回池终点 (防"往后"看未来 —— 泄漏的主要方向)
        - as_of 早于池起点  -> 返回池起点 (保证查询始终在本池内, 不越界取更早的历史)
        """
        if as_of is None:
            return None
        a = _u(as_of)
        start = self.start_ts
        end = self.end_ts
        if a < start:
            return start          # 往前越界 -> 钳到起点
        if end is not None and a > end:
            return end            # 往后越界 -> 钳到终点
        return a

    def contains(self, t) -> bool:
        ts = _u(t)
        if ts < self.start_ts:
            return False
        end = self.end_ts
        return True if end is None else ts <= end


# ---------------------------------------------------------------------------
# 池注册表 (定稿 v1.0, 决策记录见设计文档第 12 节)
# ---------------------------------------------------------------------------
POOLS: dict[str, Pool] = {
    "oof": Pool(
        pool_id="oof", start="2018-01-01", end="2023-12-31", kind="dev",
        name="开发池 (OOF)",
        desc="训练/调参/试错。覆盖 2018 熊、2019-2020、2021 牛、2022 熊、2023 震荡 (5 个周期)。",
    ),
    "gap1": Pool(
        pool_id="gap1", start="2024-01-01", end="2024-01-14", kind="gap",
        name="隔离带 1 (purge+embargo)",
        desc="14 天, 两边都不能用。",
    ),
    "valid": Pool(
        pool_id="valid", start="2024-01-15", end="2025-06-30", kind="valid",
        name="反馈池",
        desc="输出指标做反馈, 用于验证定型与少量选择。覆盖 2024 减半牛 + 2025。",
    ),
    "gap2": Pool(
        pool_id="gap2", start="2025-07-01", end="2025-07-14", kind="gap",
        name="隔离带 2 (purge+embargo)",
        desc="14 天, 两边都不能用。",
    ),
    "oos": Pool(
        pool_id="oos", start="2025-07-15", end="2026-09-30", kind="oos",
        name="OOS 池 (冻结)",
        desc="进交易前最后闸门。Agent 盲 (读不到分数), 研究人员经人审通道读结果做 go/no-go。",
    ),
    "rolling_oos": Pool(
        pool_id="rolling_oos", start="2026-10-01", end=None, kind="rolling_oos",
        name="滚动 OOS",
        desc="每季度开启新窗口。链上因子 (2026-08 起) 的首个可验证窗口在这里。",
    ),
}

# 池内可用的因子集 (gap 不用于研究, 因子集留空)
RESEARCH_POOLS = ["oof", "valid", "oos", "rolling_oos"]


# ---------------------------------------------------------------------------
# 因子可用性
# ---------------------------------------------------------------------------
def factor_available(dataset: str, as_of=None) -> bool:
    """因子在 as_of 时刻是否可用 (早于 availability_start 则屏蔽)。"""
    start = FACTOR_AVAILABILITY.get(dataset)
    if start is None:
        return True          # 未登记的因子不屏蔽 (默认放行)
    if as_of is None:
        return True
    return _u(as_of) >= _u(start)


def clamp_factor_as_of(dataset: str, as_of=None):
    """返回因子在 as_of 是否可用; 不可用时抛错 (显式而非静默返回空)。"""
    if dataset in REVISION_CONTAMINATED:
        raise ValueError(
            f"因子 {dataset!r} 已被标记为 revision_contaminated: 其历史为事后抓取的全量"
            f"数据, data_available_at 指向数年之后, 回测历史会引入修订泄漏"
            f"(泄露路径 #6, 见 _audit_revision_leak.py)。请改用近期窗口, 或从现在起"
            f"每日快照自积累。"
        )
    if dataset in NO_PIT_COLUMN:
        raise ValueError(
            f"因子 {dataset!r} 缺少 data_available_at 列, 无法做 PIT 过滤, "
            f"不可用于点时回测。"
        )
    if not factor_available(dataset, as_of):
        start = FACTOR_AVAILABILITY.get(dataset, "?")
        raise ValueError(
            f"因子 {dataset!r} 在 {as_of} 不可用 (可用起点 {start}); "
            f"这是防止幸存时段偏差的因子屏蔽机制。"
        )
    return as_of


# ---------------------------------------------------------------------------
# 宇宙门控
# ---------------------------------------------------------------------------
_UNI_CACHE: dict = {}


def universe_symbols(as_of, layer: str = "research") -> set[str]:
    """取 as_of 当日 (钳制到该日) 属于 universe 层的 symbol 集合。"""
    as_of = _u(as_of)
    key = (as_of, layer)
    if key in _UNI_CACHE:
        return _UNI_CACHE[key]
    from .reader import load_universe
    df = load_universe(as_of=as_of, layer=layer)
    syms = set(df["symbol"]) if not df.empty else set()
    _UNI_CACHE[key] = syms
    return syms


# ---------------------------------------------------------------------------
# 会话作用域 (Agent/研究员在池内工作)
# ---------------------------------------------------------------------------
class PoolScope:
    """池作用域: 绑定一个池, 把所有查询钳制在该池内。Agent/研究员各持一个。"""

    def __init__(self, pool_id: str = "oof", layer: str = "research"):
        if pool_id not in POOLS:
            raise KeyError(f"未知池 {pool_id!r}, 可用: {sorted(POOLS)}")
        self.pool = POOLS[pool_id]
        if self.pool.kind == "gap":
            raise ValueError(f"隔离带 {pool_id!r} 不能用于研究")
        self.layer = layer
        self._uni: set[str] | None = None

    @property
    def pool_id(self) -> str:
        return self.pool.pool_id

    @property
    def end(self) -> pd.Timestamp | None:
        return self.pool.end_ts

    def clamp(self, as_of=None):
        return self.pool.clamp(as_of)

    def factor_available(self, dataset: str, as_of=None) -> bool:
        a = self.clamp(as_of) if as_of is not None else self.end
        # 因子屏蔽看的是"实际请求的时刻"; 未指定 as_of 时按池末日判定
        return factor_available(dataset, a)

    def require_factor(self, dataset: str, as_of=None):
        a = self.clamp(as_of) if as_of is not None else self.end
        return clamp_factor_as_of(dataset, a)

    def universe(self, as_of=None) -> set[str]:
        """本池末日 (或指定日) 的宇宙成员。"""
        a = self.clamp(as_of) if as_of is not None else self.end
        if a is None:
            raise ValueError(f"池 {self.pool_id} 为开放池, 需显式指定 as_of 取宇宙")
        if self._uni is None:
            self._uni = universe_symbols(a, self.layer)
        return self._uni

    def filter_instruments(self, instruments, as_of=None) -> list[str]:
        """把 instrument 列表按宇宙门控过滤 (不在本池宇宙内的剔除)。"""
        uni = self.universe(as_of)
        return [i for i in instruments if i in uni]

    def __repr__(self):
        e = str(self.pool.end_ts.date()) if self.pool.end_ts else "open"
        return f"<PoolScope {self.pool_id} ({self.pool.name}) {self.pool.start}~{e} " \
               f"layer={self.layer}>"


# ---------------------------------------------------------------------------
# 便捷访问
# ---------------------------------------------------------------------------
def get_pool(pool_id: str) -> Pool:
    if pool_id not in POOLS:
        raise KeyError(f"未知池 {pool_id!r}, 可用: {sorted(POOLS)}")
    return POOLS[pool_id]


def list_pools() -> list[dict]:
    """池注册表摘要 (给 MCP list_pools 工具用)。"""
    out = []
    for pid in POOLS:
        p = POOLS[pid]
        out.append({
            "pool_id": pid, "name": p.name, "kind": p.kind,
            "start": str(p.start_ts.date()),
            "end": str(p.end_ts.date()) if p.end_ts else None,
            "usable": pid in RESEARCH_POOLS,
            "n_factors": len(p.factors), "desc": p.desc,
        })
    return out


if __name__ == "__main__":
    import json
    for p in list_pools():
        print(json.dumps(p, ensure_ascii=False))
    # 自检: 钳制
    s = PoolScope("oof")
    print("\n[自检] 池1 钳制:")
    print("  as_of=2026-10-01 ->", s.clamp("2026-10-01"), "(应被钳到 2023-12-31)")
    print("  as_of=2021-06-01 ->", s.clamp("2021-06-01"), "(池内, 不变)")
    print("  因子 funding @2021-06:", s.factor_available("derivatives_funding", "2021-06-01"))
    print("  因子 funding @2018-06:", s.factor_available("derivatives_funding", "2018-06-01"))