# -*- coding: utf-8 -*-
"""signals.py — 面板信号适配器: 把特征面板变成策略可用的信号源

设计: docs/backtest-engine-design.md (v0.1)

Strategy 只订阅 BarEvent (只含价格与资产集合), 不认识面板/特征。要让策略用
特征做决策, 需要一个**只读查找器**: 给定 BarEvent 的 (ts, assets), 返回对应
的信号向量。

这个适配器同时是**防泄漏的第二道闸**: 它按 ts 精确查表, 不做任何前视填充 ——
查不到就返回 NaN (策略须自行决定如何处理), 绝不用"最近一个可用值"顶替。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .events import BarEvent

__all__ = ["PanelSignal"]


class PanelSignal:
    """特征面板 → 按 BarEvent 查询的信号。

    panel.values: MultiIndex (base_asset, time) 的特征面板
    用法:
        sig = PanelSignal(bundle.values["mom_zscore_24h"])
        strat = TopNStrategy(lambda bar: sig.get(bar), top_n=5)
    """

    def __init__(self, series: pd.Series, fill: float | None = None):
        if not isinstance(series, pd.Series):
            raise TypeError(f"需要 Series, 收到 {type(series).__name__}")
        # 只保留索引是 (base_asset, time) 的部分
        if isinstance(series.index, pd.MultiIndex):
            self._wide = series.unstack(level="time")
        else:
            self._wide = series.to_frame().T
        self.fill = fill
        self._cache: dict = {}

    def get(self, bar: BarEvent) -> np.ndarray:
        """返回与 bar.assets 一一对应的信号向量 (缺失 -> NaN 或 fill)。

        查表用 bar.bar_time (面板网格时间 = open_time); 若为空则退回 bar.ts。
        精确查找, **不前视填充**: 查不到就是查不到 —— 宁可信号缺失, 也不能拿
        "最近一个可用值"顶替 (那是隐式的未来信息)。
        """
        key = bar.bar_time if bar.bar_time is not None else bar.ts
        if key in self._cache:
            return self._cache[key]
        try:
            col = self._wide[key] if key in self._wide.columns else pd.Series(dtype=float)
        except KeyError:
            col = pd.Series(dtype=float)
        vals = col.reindex(bar.assets).to_numpy(dtype=float)
        if self.fill is not None:
            vals = np.where(np.isfinite(vals), vals, self.fill)
        self._cache[key] = vals
        return vals

    def __call__(self, bar: BarEvent) -> np.ndarray:
        return self.get(bar)