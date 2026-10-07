# -*- coding: utf-8 -*-
"""signal_adapter.py — 模型分数 → 回测信号 + 仓位映射

设计: docs/supervised-learning-protocol-design.md §7

补上回测引擎设计 §9 明确留给模型协议层的缺口: "不做优化器 (仓位映射先固定,
权重优化属模型协议)"。这里实现两种映射 (决策 M2):

  top_n        分数前 N 等权 —— 与已有 TopNStrategy 等价, 用作**对照基准**
  rank_linear  截面排名中心化线性权重 —— 多空零成本 / 纯多

原则4: **模型与仓位映射解耦**。模型只输出分数面板 (越大越看好); 仓位映射是
独立适配层; **回测引擎不动** —— 复用 PanelSignal (精确查表、不前视填充) 与
RiskEngine (权重上限/总杠杆)。

用法:
    sig = scores_to_panel_signal(score_panel)          # MultiIndex(asset, time)
    strat = ScoreWeightedStrategy(sig, mode="rank_linear", long_short=True)
    engine = BacktestEngine(data, strat)               # 引擎照常
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..backtest.events import BarEvent, OrderIntent
from ..backtest.signals import PanelSignal
from ..backtest.strategy import Strategy

__all__ = ["scores_to_panel_signal", "ScoreWeightedStrategy"]


def scores_to_panel_signal(scores: pd.Series) -> PanelSignal:
    """MultiIndex(asset, time) 的分数面板 -> PanelSignal。

    分数语义统一"越大越看好" (决策 M7)。NaN **不填充** —— PanelSignal 查不到
    就是查不到 (那是回测侧的第二道防泄漏闸)。
    """
    if not isinstance(scores, pd.Series):
        raise TypeError(f"需要 Series, 收到 {type(scores).__name__}")
    return PanelSignal(scores, fill=None)


class ScoreWeightedStrategy(Strategy):
    """按分数做仓位映射的策略 (模型协议层)。

    mode="top_n"       : 分数前 top_n 等权 (纯多) 或 前半多/后半空 (多空)
    mode="rank_linear" : 截面排名中心化线性权重 w ∝ (rank − 中位 rank)
                         多空对称、零净敞口; long_short=False 时只取正半 (纯多)
    权重最后交给 RiskEngine 修正 (max_weight / max_gross), 适配层不重复实现约束。
    """

    def __init__(self, signal_fn, mode: str = "rank_linear", top_n: int = 5,
                 long_short: bool = False, rebalance_every: int = 1,
                 name: str = "score_w", fill_missing: float | None = None):
        super().__init__(name)
        if mode not in ("top_n", "rank_linear"):
            raise ValueError("mode ∈ {top_n, rank_linear}")
        self.signal_fn = signal_fn
        self.mode = mode
        self.top_n = int(top_n)
        self.long_short = bool(long_short)
        self.rebalance_every = max(1, int(rebalance_every))
        self.fill_missing = fill_missing
        self._count = 0

    # -- 分数 -> 权重 --
    def _weights_top_n(self, sig: np.ndarray) -> np.ndarray:
        n = len(sig)
        valid = np.where(np.isfinite(sig), sig, -np.inf)
        k = min(self.top_n, n)
        order = np.argsort(-valid, kind="stable")[:k]
        w = np.zeros(n)
        if self.long_short and k >= 2:
            half = k // 2
            w[order[:half]] = 0.5 / max(half, 1) * 2 * 0.5   # 多头一半权重
            w[order[half:k]] = -0.5 / max(k - half, 1) * 2 * 0.5
            # 归一: 多头总权重 = 空头总权重
            w[w > 0] = 0.5 / max(half, 1)
            w[w < 0] = -0.5 / max(k - half, 1)
        else:
            w[order] = 1.0 / k
        return w

    def _weights_rank_linear(self, sig: np.ndarray) -> np.ndarray:
        finite = np.isfinite(sig)
        n = len(sig)
        w = np.zeros(n)
        m = int(finite.sum())
        if m == 0:
            return w
        # 截面排名 (仅有限值), 中心化到 [-0.5, 0.5]
        vals = sig[finite]
        order = np.argsort(vals, kind="stable")
        ranks = np.empty(m, dtype=float)
        ranks[order] = np.arange(m, dtype=float)
        center = (m - 1) / 2.0
        centered = (ranks - center) / max(m, 1)      # 约在 [-0.5, 0.5]
        if self.long_short:
            w[finite] = centered * 2.0              # 多空对称, 净敞口≈0
        else:
            w[finite] = np.clip(centered * 2.0, 0.0, None)   # 纯多: 只取上半
        gross = np.abs(w).sum()
        if gross > 0:
            w = w / gross                          # 归一到总杠杆 1
        return w

    # -- Strategy 契约 --
    def on_bar(self, bar: BarEvent) -> OrderIntent | None:
        self._count += 1
        if (self._count - 1) % self.rebalance_every != 0:
            return None
        sig = np.asarray(self.signal_fn(bar), dtype=float)
        if self.fill_missing is not None:
            sig = np.where(np.isfinite(sig), sig, self.fill_missing)
        else:
            sig = np.where(np.isfinite(sig), sig, np.nan)
        if self.mode == "top_n":
            w = self._weights_top_n(sig)
        else:
            w = self._weights_rank_linear(sig)
        if not np.isfinite(w).any() or np.abs(w).sum() < 1e-12:
            return None
        return OrderIntent(ts=bar.ts, target_weights=w,
                           reason=f"{self.mode}:{self.name}")

    def __repr__(self):
        return (f"<ScoreWeightedStrategy mode={self.mode} top_n={self.top_n} "
                f"long_short={self.long_short}>")