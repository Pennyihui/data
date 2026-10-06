# -*- coding: utf-8 -*-
"""specs.py — F4 特征定义数据结构

设计文档: docs/feature-foundation-design.md 第 4 节 (v0.5)

FeatureSpec 是"特征"的声明式定义 —— **库是实体集合**（表达式本身）, 注册表只是
索引 (设计文档 4.0)。字段/算子链/血缘依赖由注册时编译一次并缓存, 表达式变更
即升版 (旧版保留, 保证历史研究可复现)。
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field

import pandas as pd

from . import dsl
from . import operators as op

__all__ = ["FeatureSpec", "Lookback", "BAR_HOURS"]

BAR_HOURS = 1          # 面板基础粒度 = 1h bar

#: 低频字段: 表达式里的"窗口/bar 数"是**该周期自己的 bar**, 折算成 1h 面板
#: 需要的行数要乘以它的周期长度 (月线第 3 根 = 3 个月 ≈ 2208 行 1h, 不是 3 行)。
#: 不折算会导致预热不足 -> 月线特征空值 (实测 ret_3m 在 3 个月窗口里取不到)。
_FIELD_BAR_HOURS: dict = {
    "c4_": 4, "pc4_": 4,          # 4h K线
    "cd_": 24, "cw_": 168,         # 日线 / 周线
    "cm_": 730, "pcm_": 730,       # 月线 (按 30.4 天/月 近似)
}


@dataclass(frozen=True)
class Lookback:
    """表达式需要的 trailing 历史长度 (bar 数) —— 用于自动预热。"""

    bars: int
    reason: str

    @property
    def hours(self) -> pd.Timedelta:
        return pd.Timedelta(hours=int(self.bars) * BAR_HOURS)


@dataclass
class FeatureSpec:
    """一个特征的完整定义 (表达式是实体, 下面是派生元数据)。"""

    name: str
    expr: str
    category: str                       # price / derivatives / liquidity / quality / ...
    desc: str = ""
    version: str = "1.0"
    tags: tuple[str, ...] = ()
    source: str = ""                    # library 模块 (注册时自动填)
    status: str = "active"              # active / deprecated
    # ---- 派生元数据 (compile_feature_spec 填充) ----
    fields: tuple[str, ...] = ()        # 直接依赖的**字段**
    features: tuple[str, ...] = ()      # 直接依赖的**其他特征**
    operators: tuple[str, ...] = ()
    groups: tuple[str, ...] = ()        # 用到的分组标签
    depth: int = 1
    lookback: Lookback | None = None
    _compiled: object = None            # dsl.CompiledExpr (缓存)

    # -- 编译 ---------------------------------------------------------------
    def compile(self, known_fields: set[str], known_features: set[str],
                known_groups: set[str] | None = None):
        """编译表达式 (注册时或计算时); 失败抛 dsl.ExprError。

        known_groups: 可用的分组维度名 (market_cap_tier 等) —— 它们既不是字段
        也不是特征, 但在表达式里合法, 所以必须并进白名单。
        """
        kg = set(known_groups or ())
        c = dsl.compile_expr(self.expr, known_fields | known_features | kg)
        self.fields = tuple(f for f in c.fields
                            if f in known_fields and f not in known_features)
        self.features = tuple(f for f in c.fields if f in known_features)
        self.groups = tuple(g for g in c.fields if g in kg)
        self.operators = c.operators
        self.depth = c.depth()
        self.lookback = compute_lookback(c)
        self._compiled = c
        return c

    @property
    def compiled(self):
        if self._compiled is None:
            raise ValueError(
                f"特征 {self.name!r} 尚未编译 —— 先调 registry.validate_all() "
                f"或由 FeatureEngine 计算时编译")
        return self._compiled

    def lineage(self) -> list[str]:
        return self.compiled.lineage()

    def to_dict(self) -> dict:
        return {
            "name": self.name, "expr": self.expr, "category": self.category,
            "desc": self.desc, "version": self.version, "tags": list(self.tags),
            "source": self.source, "status": self.status,
            "fields": list(self.fields), "features": list(self.features),
            "operators": list(self.operators), "groups": list(self.groups),
            "depth": self.depth,
            "lookback_bars": (self.lookback.bars if self.lookback else None),
            "lineage": self.lineage() if self._compiled is not None else [],
        }

    def __repr__(self) -> str:
        return (f"<FeatureSpec {self.name} v{self.version} "
                f"[{self.category}] {self.expr}>")


# ---------------------------------------------------------------------------
# 预热推导: 表达式需要的 trailing 历史 (bar 数)
# ---------------------------------------------------------------------------
def _node_lookback(node: dsl.Node, family_fields: set[str]) -> tuple[int, str]:
    """节点需要的 trailing bar 数 (取所有子节点最大值)。"""
    if node.kind == "field":
        return 0, "field"
    kids = [a for a in node.args if isinstance(a, dsl.Node)]
    child_max, reason = 0, ""
    for k in kids:
        b, r = _node_lookback(k, family_fields)
        if b > child_max:
            child_max, reason = b, r
    if node.kind == "call":
        fname = node.op_name
        fam = op.ALL_OPERATORS[fname].family
        win = 1
        arg_i = 1
        if fam == "ts":
            if fname in ("ts_backfill", "ts_ewma"):
                return child_max, f"{fname}(unbounded)"
            if fname in ("ts_delay", "ts_delta", "ts_pct_change"):
                win = (node.args[1] if len(node.args) > 1
                       and isinstance(node.args[1], int) else 1) + 1
                reason = fname
            else:
                win = (node.args[1] if len(node.args) > 1
                       and isinstance(node.args[1], int)
                       else dsl._default_arg(fname, 1))
                reason = fname
        elif fam == "pp":
            if fname in ("pp_diff", "pp_pct_change"):
                win = (node.args[1] if len(node.args) > 1
                       and isinstance(node.args[1], int) else 1) + 1
                reason = fname
            elif fname in ("pp_frac_diff", "pp_detrend"):
                idx = 2 if fname == "pp_frac_diff" else 1
                if len(node.args) > idx and isinstance(node.args[idx], int):
                    win = int(node.args[idx])
                elif len(node.args) > 1 and isinstance(node.args[1], int) \
                        and fname == "pp_frac_diff":
                    win = int(node.args[1])
                else:
                    win = int(dsl._default_arg(fname, idx))
                reason = fname
            elif fname == "pp_savgol":
                win = int(node.args[1]) if len(node.args) > 1 \
                    and isinstance(node.args[1], int) else 7
                reason = fname
            elif fname == "pp_boxcox":
                # by='ts' 时窗口参数在第 3 个位置; by='time'(默认) 不需回看
                args = node.args[1:]
                by = next((a for a in args if isinstance(a, str)), "time")
                if by in ("ts", "trailing", "window"):
                    win = next((int(a) for a in args
                                if isinstance(a, int)), 20)
                    reason = fname
                else:
                    win = 1
                    reason = fname
            elif fname == "pp_ema":
                return child_max, "pp_ema(unbounded)"
            else:
                win = 1
                reason = fname
        else:
            win, reason = 1, fname
        return max(child_max, int(win)), reason
    # neg / binop
    return child_max, reason or node.kind


def compute_lookback(c: dsl.CompiledExpr) -> Lookback:
    """整个表达式的预热长度 = 各分支最大值。

    低频字段 (日线/周线/月线): 表达式里的窗口数是**该周期自己的 bar 数**,
    但面板是 1h 网格, 所以需要额外预热"若干个低频周期"才能让窗口填满。折算
    用**下限**而非精确倍数: 低频字段的历史本来就从更早开始, 多读几周就够;
    按精确倍数 (月线 ×730) 会把预热撑到一年, 面板加载从秒级掉到分钟级。
    """
    bars, reason = _node_lookback(c.root, set())
    max_period = 1
    for f in c.fields:
        for pref, hours in _FIELD_BAR_HOURS.items():
            if f.startswith(pref) and hours > max_period:
                max_period = hours
                reason = f"{f}({hours}h/条)"
    if max_period > 1:
        # 至少多读 3 个该周期 (封顶 90 天, 够任何合理窗口填满又不拖慢)
        extra = min(max_period * 3, 24 * 90)
        bars = max(bars, extra)
    return Lookback(bars=max(bars, 1), reason=reason)