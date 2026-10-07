# -*- coding: utf-8 -*-
"""objectives.py — 目标注册表 (阶段1: 目标 → 标签 的第一类对象)

设计: docs/label-system-design.md §4

一个目标 (Objective) = 研究意图 + 决策节拍 + horizon + 成本参数, 下面挂多个标签。
"预测未来 10 天走势" = 一个目标 (trend_10d), 而不是五个互不相干的手写标签。

风格与 pool_registry 一致: 纯注册表 + 帮助函数, 不用装饰器魔法。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

__all__ = [
    "CostParams", "Objective", "REGISTRY", "register_objective",
    "get_objective", "list_objectives", "parse_horizon_days",
    "grid_days", "TREND_10D",
]


# ---------------------------------------------------------------------------
# 成本参数 (与 backtest.CostModel / 标签扣费同源, 单一数字来源)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CostParams:
    """标签成本参数 (单边)。默认值与 backtest.execution_engine.CostModel 一致。"""

    taker_fee: float = 0.0005      # 吃单手续费 (单边)
    slippage_bps: float = 5.0       # 单边滑点 (基点)

    @property
    def slippage(self) -> float:
        return self.slippage_bps / 1e4

    @property
    def round_trip(self) -> float:
        """一次完整开平仓的成本 (双边)。标签默认扣这个。"""
        return 2.0 * (self.taker_fee + self.slippage)

    def fingerprint(self) -> str:
        return f"taker={self.taker_fee:.6f},slip_bps={self.slippage_bps:.3f}"


# ---------------------------------------------------------------------------
# 目标
# ---------------------------------------------------------------------------
_GRID_DAYS = {"1M": 30, "1W": 7, "1D": 1, "4h": 1.0 / 6, "1h": 1.0 / 24}


def grid_days(grid: str) -> float:
    """决策节拍 -> 天数 (1M/1W 取名义长度, 用于时点对齐; 标签用 bar 数对齐更精确)。"""
    g = str(grid).strip()
    if g not in _GRID_DAYS:
        raise ValueError(f"未知决策节拍 {grid!r}; 可用: {sorted(_GRID_DAYS)}")
    return _GRID_DAYS[g]


def parse_horizon_days(horizon: str) -> float:
    """"10D" -> 10.0 天。只支持 D/W (月长不固定, 按名义 30 天处理)。"""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([DWMdwm])\s*", str(horizon))
    if not m:
        raise ValueError(f"horizon 格式应为 '10D' / '2W' / '3M', 收到 {horizon!r}")
    n, unit = float(m.group(1)), m.group(2).lower()
    return n * {"d": 1.0, "w": 7.0, "m": 30.0}[unit]


@dataclass(frozen=True)
class Objective:
    """研究目标: 意图 + 节拍 + horizon + 成本 + 其下的标签集合。"""

    name: str
    horizon: str                     # "10D"
    decision_grid: str               # "1D" —— 决策节拍
    cost: CostParams = field(default_factory=CostParams)
    labels: tuple[str, ...] = ()     # 由 labels.definitions 注册时回填
    universe_layer: str = "research"
    desc: str = ""

    # -- 派生量 --------------------------------------------------------------
    @property
    def horizon_days(self) -> float:
        return parse_horizon_days(self.horizon)

    @property
    def grid_days(self) -> float:
        return grid_days(self.decision_grid)

    @property
    def horizon_bars(self) -> int:
        """horizon 在决策节拍上折算的 bar 数 (至少 1)。"""
        return max(1, int(round(self.horizon_days / self.grid_days)))

    def fingerprint(self) -> str:
        return (f"{self.name}|h={self.horizon}|grid={self.decision_grid}"
                f"|{self.cost.fingerprint()}")


REGISTRY: dict[str, Objective] = {}


def register_objective(obj: Objective, *, replace: bool = False) -> Objective:
    if obj.name in REGISTRY and not replace:
        raise KeyError(f"目标 {obj.name!r} 已注册 (要覆盖请 replace=True)")
    if not re.fullmatch(r"[a-z0-9_]+", obj.name):
        raise ValueError(f"目标名只允许 [a-z0-9_], 收到 {obj.name!r}")
    REGISTRY[obj.name] = obj
    return obj


def get_objective(name: str) -> Objective:
    if name not in REGISTRY:
        raise KeyError(f"未知目标 {name!r}, 已注册: {sorted(REGISTRY)}")
    return REGISTRY[name]


def list_objectives() -> list[dict]:
    return [{"name": o.name, "horizon": o.horizon,
             "decision_grid": o.decision_grid,
             "horizon_bars": o.horizon_bars,
             "round_trip_cost": o.cost.round_trip,
             "labels": list(o.labels), "desc": o.desc}
            for o in REGISTRY.values()]


# ---------------------------------------------------------------------------
# 首个目标: 预测未来 10 天走势 (设计文档 §4)
# ---------------------------------------------------------------------------
TREND_10D = register_objective(Objective(
    name="trend_10d",
    horizon="10D",
    decision_grid="1D",
    cost=CostParams(taker_fee=0.0005, slippage_bps=5.0),
    labels=(),                        # 由 labels.definitions 注册时回填
    universe_layer="research",
    desc="预测未来 10 天走势。1D 节拍决策, H=10 根日 bar, 开-开口径, 扣双边成本。",
))


if __name__ == "__main__":  # pragma: no cover
    import json
    for row in list_objectives():
        print(json.dumps(row, ensure_ascii=False))
    o = get_objective("trend_10d")
    print(f"\n[自检] horizon_bars={o.horizon_bars} "
          f"round_trip={o.cost.round_trip:.4f} (20bps)")
    print("[自检] parse_horizon_days('2W') =", parse_horizon_days("2W"))