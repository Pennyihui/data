# -*- coding: utf-8 -*-
"""definitions.py — 标签定义 (同一目标下的多种标签)

设计: docs/label-system-design.md §5

成交时点口径 (决策 L1, 全文档最关键的约定)
--------------------------------------------
    信号于 t 收盘            -> close[t] 已知
    进场成交价 = open[t+1]    <- 与回测引擎 "T+1 开盘成交" 完全一致
    离场成交价 = open[t+H+1]  <- 同样是下一根开盘
    ret_H(t) = open[t+H+1] / open[t+1] - 1 - c_rt

不用 close-to-close: 那等于假设看到收盘价的瞬间还能按收盘价成交 (回测引擎明确
禁止); 不用 open[t+1]→close[t+H]: 离场价不可成交。开-开是引擎唯一能逐位兑现的口径。

成本 (决策 L2)
-------------
    净收益 = 毛收益 - c_rt,  c_rt = 2*(taker_fee + slippage)  默认 20bps
    成本参数随标签规格走 (改成本 -> 新指纹 -> 新版本)。

PIT 语义
--------
    label_available_at = bar[t+H+1] 的收盘 (保守取收盘, 不提前声明)。
    标签是**未来函数**, 这是它的定义而非缺陷 —— 因此它被物理隔离在特征体系外
    (见 __init__.py 的隔离说明与 _test_label_isolation.py)。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .objectives import Objective, CostParams, REGISTRY as OBJ_REGISTRY, get_objective

__all__ = [
    "LabelSpec", "LabelResult", "LABELS", "register_label", "get_label",
    "list_labels", "compute_labels", "KINDS",
]

#: 支持的标签种类
KINDS = ("ret", "sign", "quantile", "excess", "vol_scaled")


# ---------------------------------------------------------------------------
# 标签规格
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LabelSpec:
    name: str
    objective: str                   # 归属目标
    kind: str                        # ret | sign | quantile | excess | vol_scaled
    horizon_bars: int                # H (决策格上的 bar 数)
    cost: CostParams | None = None   # None -> 取目标对象的成本参数
    n_quantiles: int = 5             # quantile 标签的分位数
    vol_window: int = 20             # vol_scaled 的 trailing 窗口 (只用过去)
    version: str = "v1"
    research_only: bool = False
    winsor: float | None = None      # 截面缩尾分位 (None=不缩尾; 0.01=1%/99%)
    desc: str = ""

    def __post_init__(self):
        if self.kind not in KINDS:
            raise ValueError(f"未知标签种类 {self.kind!r}; 可用: {KINDS}")
        if not re.fullmatch(r"[a-z0-9_]+", self.name):
            raise ValueError(f"标签名只允许 [a-z0-9_], 收到 {self.name!r}")
        if int(self.horizon_bars) < 1:
            raise ValueError("horizon_bars 必须 >= 1")
        if self.cost is None:
            object.__setattr__(self, "cost", get_objective(self.objective).cost)

    def fingerprint(self) -> str:
        return (f"{self.name}|{self.version}|obj={self.objective}|kind={self.kind}"
                f"|H={self.horizon_bars}|q={self.n_quantiles}|vw={self.vol_window}"
                f"|winsor={self.winsor}|{self.cost.fingerprint()}")


# dataclasses 辅助
LABELS: dict[str, LabelSpec] = {}


def register_label(spec: LabelSpec, *, replace: bool = False) -> LabelSpec:
    """注册标签并挂到目标下 (一个目标多标签)。"""
    get_objective(spec.objective)                      # 目标必须已注册
    if spec.name in LABELS and not replace:
        raise KeyError(f"标签 {spec.name!r} 已注册 (要覆盖请 replace=True)")
    LABELS[spec.name] = spec
    obj = get_objective(spec.objective)
    if spec.name not in obj.labels:
        object.__setattr__(obj, "labels", obj.labels + (spec.name,))
    return spec


def get_label(name: str) -> LabelSpec:
    if name not in LABELS:
        raise KeyError(f"未知标签 {name!r}, 已注册: {sorted(LABELS)}")
    return LABELS[name]


def list_labels(objective: str | None = None) -> list[dict]:
    out = []
    for spec in LABELS.values():
        if objective and spec.objective != objective:
            continue
        out.append({"name": spec.name, "objective": spec.objective,
                    "kind": spec.kind, "horizon_bars": spec.horizon_bars,
                    "version": spec.version, "research_only": spec.research_only,
                    "round_trip_cost": spec.cost.round_trip,
                    "desc": spec.desc})
    return out


# ---------------------------------------------------------------------------
# 计算结果
# ---------------------------------------------------------------------------
@dataclass
class LabelResult:
    """标签计算结果: 值 + label_available_at + 规格。"""

    name: str
    values: pd.Series               # MultiIndex (base_asset, time) 的决策时刻标签
    available_at: pd.Series         # 同索引: 标签何时可知 (未来函数!)
    spec: LabelSpec
    horizon_bars: int
    #: 标签窗口: 进场 open[t+1] -> 离场 open[t+H+1] (bar 偏移, 供切分器验证)
    entry_offset: int = 1
    exit_offset: int = 0            # 构造时填 t+H+1

    def __repr__(self):
        return (f"<LabelResult {self.name} {len(self.values):,} 行 "
                f"H={self.horizon_bars} 非空={int(self.values.notna().sum()):,}>")


# ---------------------------------------------------------------------------
# 核心计算
# ---------------------------------------------------------------------------
def _panel_index_ok(prices: pd.DataFrame) -> None:
    if not isinstance(prices.index, pd.MultiIndex) or \
            prices.index.names != ["base_asset", "time"]:
        raise ValueError(
            "prices 必须是 MultiIndex(base_asset, time) 面板 "
            f"(当前: {prices.index.names})")
    for col in ("open", "close"):
        if col not in prices.columns:
            raise ValueError(f"prices 缺少列 {col!r}; 需要 open/close")


def _trailing_vol(prices: pd.DataFrame, window: int) -> pd.Series:
    """过去 window 根 bar 的已实现波动 (trailing, **只用 t 及之前**的收益)。"""
    close = prices["close"]
    ret = close.groupby(level="base_asset", sort=False).pct_change()
    # 逐资产 rolling 后按原索引拼回 (groupby.rolling 会产生三级索引, 容易错位)
    vol = ret.groupby(level="base_asset", sort=False).rolling(
        window, min_periods=max(2, window // 2)).std().reset_index(
            level=0, drop=True)
    vol = vol.reindex(close.index)
    return vol


def compute_labels(spec: LabelSpec | str,
                   prices: pd.DataFrame,
                   *,
                   grid_days: float = 1.0) -> LabelResult:
    """在 1D (或任意决策格) 价格面板上计算标签。

    prices : MultiIndex(base_asset, time) x [open, close] 的**认证**价格面板,
             time 为该 bar 的 open_time。必须已按 (asset, time) 排序。

    返回的 available_at 是 bar[t+H+1] 的收盘时刻 (保守: 开盘价更早可知但不提前声明)。
    """
    if isinstance(spec, str):
        spec = get_label(spec)
    _panel_index_ok(prices)
    H = int(spec.horizon_bars)
    px = prices.sort_index()
    g = px.groupby(level="base_asset", sort=False)

    entry = g["open"].shift(-1)                 # open[t+1]  进场
    exit_ = g["open"].shift(-(H + 1))           # open[t+H+1] 离场
    with np.errstate(divide="ignore", invalid="ignore"):
        gross = exit_ / entry - 1.0
    gross = gross.where(np.isfinite(gross))
    c_rt = spec.cost.round_trip
    net = gross - c_rt

    # -- 截面缩尾 (可选): crypto 收益分布重尾, 单期 +243 倍的样本会主导所有
    #    统计量 (实测 gbdt 的 IC 缩尾后从 0.0705 掉到 0.0161)。按**决策日截面**
    #    分位缩尾 —— 只用同一时刻的横截面分布, 不引入任何未来信息。
    if spec.winsor:
        q = float(spec.winsor)
        by_time = net.groupby(level="time")
        lo = by_time.transform(lambda s: s.quantile(q))
        hi = by_time.transform(lambda s: s.quantile(1.0 - q))
        net = net.clip(lower=lo, upper=hi)

    if spec.kind == "ret":
        values = net
    elif spec.kind == "sign":
        values = net.where(net.notna()).gt(0).astype(float)
        values = net.where(net.isna(), values)   # 缺失保持 NaN
    elif spec.kind in ("quantile", "excess"):
        by_time = net.groupby(level="time")
        if spec.kind == "excess":
            values = net - by_time.transform("median")
        else:
            pct = net.groupby(level="time").rank(pct=True, method="average")
            k = int(spec.n_quantiles)
            values = np.ceil(pct * k).clip(1, k)
            values = net.where(net.isna(), values)
    elif spec.kind == "vol_scaled":
        vol = _trailing_vol(px, int(spec.vol_window))
        with np.errstate(divide="ignore", invalid="ignore"):
            values = net / vol
        values = values.where(np.isfinite(values))
    else:                                          # pragma: no cover - 已在构造校验
        raise ValueError(spec.kind)

    # label_available_at = 离场 bar 的收盘 = open_time(离场) + 格长 - 1s
    times = pd.Series(px.index.get_level_values("time"), index=px.index)
    exit_open = times.groupby(level="base_asset", sort=False).shift(-(H + 1))
    dur = pd.Timedelta(days=float(grid_days))
    available_at = (exit_open + dur - pd.Timedelta(seconds=1)).where(
        exit_open.notna())

    return LabelResult(name=spec.name, values=values, available_at=available_at,
                       spec=spec, horizon_bars=H, entry_offset=1,
                       exit_offset=H + 1)


# ---------------------------------------------------------------------------
# 首批标签 (目标 trend_10d, H=10 根日 bar) —— 设计文档 §5 表
# ---------------------------------------------------------------------------
for _name, _kind, _wins, _desc in [
    ("ret_10d", "ret", None, "开-开净收益 (连续值), 回归目标"),
    ("ret_10d_w", "ret", 0.01, "缩尾版净收益 (截面 1%/99%) —— 重尾市场的稳健口径"),
    ("sign_10d", "sign", None, "净收益符号 (二分类)"),
    ("sign_10d_w", "sign", 0.01, "缩尾后净收益符号"),
    ("quantile_10d", "quantile", None, "决策日截面 5 分位 (多分类/排序)"),
    ("excess_10d", "excess", None, "净收益 - 截面中位数 (中性化回归)"),
    ("excess_10d_w", "excess", 0.01, "缩尾后截面超额 (中性化稳健回归)"),
    ("vol_scaled_10d", "vol_scaled", None,
     "净收益 / 过去20日已实现波动 (异方差稳健)"),
    ("vol_scaled_10d_w", "vol_scaled", 0.01, "缩尾 + 波动缩放"),
]:
    register_label(LabelSpec(name=_name, objective="trend_10d", kind=_kind,
                             horizon_bars=10, winsor=_wins, desc=_desc))


if __name__ == "__main__":  # pragma: no cover
    import json
    print(json.dumps(list_labels(), ensure_ascii=False, indent=2))
    from .objectives import list_objectives
    print(json.dumps(list_objectives(), ensure_ascii=False, indent=2))