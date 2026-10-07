# -*- coding: utf-8 -*-
"""strategy.py — Strategy 基类 + RiskEngine

设计: docs/backtest-engine-design.md (v0.1) §3/§4

Strategy 契约 (回测实盘同路径的核心):
    - 只订阅 BarEvent, 物理上**看不到**下一根 bar 的价格
    - 只输出 OrderIntent (目标权重), 不管成交、不管成本
    - 不知道自己在回测还是实盘

RiskEngine: 在 Strategy 之后、ExecutionEngine 之前修正目标仓位 (约束)。
"""
from __future__ import annotations

import numpy as np

from .events import BarEvent, OrderIntent

__all__ = ["Strategy", "RiskEngine", "TopNStrategy"]


class Strategy:
    """策略基类。子类实现 on_bar -> OrderIntent。

    防泄漏是**结构性**的, 不是纪律性的:
      * Strategy 收到的 BarEvent 里**没有下一根 bar 的开盘价** —— 未来价格根本
        不在它能碰到的对象上 (成交价由引擎从自己的数据引用取出, 作为参数传给
        ExecutionEngine)。
      * 事件流按 ts 严格排序, 策略收到的永远只有当前及之前的 bar。
    """

    def __init__(self, name: str = "strategy"):
        self.name = name
        self.equity = None              # 由引擎回填 (可选, 仅用于展示)

    def on_start(self, engine) -> None:
        """回测开始。engine 引用可用于读取只读的市场元信息 (如历史窗口)。"""

    def on_bar(self, bar: BarEvent) -> OrderIntent | None:
        """处理一根 bar, 返回目标仓位 (None = 不调仓)。子类必须实现。"""
        raise NotImplementedError

    # -- 防泄漏辅助 -------------------------------------------------------
    @staticmethod
    def assert_no_future(bar: BarEvent, idx: int) -> None:
        """断言 idx 指向的是当前 bar (不是未来)。子类若从别处取数据可调用。"""
        if idx >= len(bar.assets):
            raise IndexError("资产下标越界")


class RiskEngine:
    """目标仓位约束 (在策略之后, 执行之前修正)。

    默认约束:
      * 单资产权重上限 (max_weight)
      * 总杠杆上限 (gross ≤ max_gross)
      * 只允许在当期可交易资产 (universe) 上持仓
    """

    def __init__(self, max_weight: float = 0.1, max_gross: float = 1.0):
        self.max_weight = float(max_weight)
        self.max_gross = float(max_gross)

    def apply(self, intent: OrderIntent, bar: BarEvent) -> OrderIntent:
        w = np.asarray(intent.target_weights, dtype=float).copy()
        w = np.where(np.isfinite(w), w, 0.0)
        # 单资产上限 (对称: 多空都限幅)
        w = np.clip(w, -self.max_weight, self.max_weight)
        # 总杠杆
        gross = float(np.abs(w).sum())
        if gross > self.max_gross > 0:
            w *= self.max_gross / gross
        return OrderIntent(ts=intent.ts, target_weights=w,
                           reason=intent.reason + "|risk")


class TopNStrategy(Strategy):
    """示例策略: 按信号取前 N 名等权, 周期再平衡。

    signal_fn: (BarEvent, 历史 bar 列表) -> np.ndarray, 越大越看好。
    这个类同时是**端到端测试的基准**: 净值可手工核对。
    """

    def __init__(self, signal_fn, top_n: int = 5, rebalance_every: int = 1,
                 long_only: bool = True, name: str = "topn"):
        super().__init__(name)
        self.signal_fn = signal_fn
        self.top_n = int(top_n)
        self.rebalance_every = int(rebalance_every)
        self.long_only = long_only
        self._count = 0
        self._last_w = np.zeros(0)

    def on_bar(self, bar: BarEvent) -> OrderIntent | None:
        self._count += 1
        if (self._count - 1) % self.rebalance_every != 0:
            return None                      # 调仓周期外: 不动
        sig = np.asarray(self.signal_fn(bar), dtype=float)
        n = len(bar.assets)
        sig = np.where(np.isfinite(sig), sig, -np.inf)
        k = min(self.top_n, n)
        idx = np.argsort(-sig, kind="stable")[:k]
        w = np.zeros(n)
        if self.long_only:
            w[idx] = 1.0 / k
        else:
            w[idx[: k // 2]] = 1.0 / k
            w[idx[k // 2: k]] = -1.0 / k
        self._last_w = w
        return OrderIntent(ts=bar.ts, target_weights=w, reason="topn")