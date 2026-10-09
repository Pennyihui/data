# -*- coding: utf-8 -*-
"""engine.py — BacktestEngine: 事件循环 (把四个组件串成一条流)

设计: docs/backtest-engine-design.md (v0.1) §3

事件循环 (每根 bar):
    BarEvent(T) ──→ Strategy ──→ OrderIntent(T) ──→ RiskEngine ──→ (挂起)
                                                                        ↓
    FillEvent(T+1 开盘成交, 扣成本) ──→ Portfolio 结算权益
    BarEvent(T+1) ──→ Strategy ...

时序保证 (结构性防泄漏):
    - 策略在 T 收到的 BarEvent 只有 T 及之前的数据
    - 成交只发生在下一根 bar 的开盘价 (ExecutionEngine 保证)
    - 每根 bar 结算: 用上一期权重 × 本期 close/prev_close 收益, 再扣成本
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .events import BarEvent, EventBus, LeakageError, OrderIntent
from .data_engine import BacktestData, DataEngine
from .execution_engine import CostModel, ExecutionEngine, Portfolio
from .strategy import RiskEngine, Strategy

__all__ = ["BacktestEngine", "BacktestResult"]


def _align_to(w: np.ndarray, exec_engine, assets) -> np.ndarray:
    """把持仓权重从"上次记录的资产顺序"重排到**本期**资产顺序。

    逐日 PIT 宇宙下资产集合会变 (币上市/退市): 成交后的持仓向量是按**成交
    那一刻**的资产顺序存的, 而 Portfolio.on_bar 要的是本期顺序。不对齐就会
    维度不匹配崩溃, 或者把权重安到错的资产上 (更危险 —— 静默算错收益)。
    """
    w = np.asarray(w, dtype=float)
    prev = exec_engine.current_asset_order
    if prev is None or tuple(prev) == tuple(assets) or w.size == 0:
        return w
    prev_idx = {a: i for i, a in enumerate(prev)}
    out = np.zeros(len(assets))
    for j, a in enumerate(assets):
        i = prev_idx.get(a)
        if i is not None and i < w.size:
            out[j] = w[i]
    return out


class BacktestResult:
    def __init__(self, portfolio: Portfolio, bars: int):
        self.portfolio = portfolio
        self.bars = bars

    @property
    def metrics(self) -> dict:
        return self.portfolio.metrics()

    @property
    def equity_curve(self) -> pd.DataFrame:
        return self.portfolio.equity_frame()

    def summary(self) -> str:
        m = self.metrics
        if "final_equity" not in m:
            return f"回测结束 ({self.bars} bars), 数据不足"
        return (f"bars={self.bars} | 权益 {m['init_equity']:.0f} -> {m['final_equity']:.0f}"
                f" | 总收益 {m['total_return']:.2%} | Sharpe {m['sharpe']:.2f}"
                f" | 最大回撤 {m['max_drawdown']:.2%} | 总成本 {m['total_cost']:.0f}"
                f" | 总换手 {m['total_turnover']:.2f}")


class BacktestEngine:
    """bar 级流式回测引擎。

    usage:
        eng = BacktestEngine(data, strategy, risk=..., cost=...)
        result = eng.run()
    """

    def __init__(self, data: BacktestData, strategy: Strategy,
                 risk: RiskEngine | None = None,
                 cost: CostModel | None = None,
                 init_equity: float = 100_000.0):
        self.data = data
        self.strategy = strategy
        self.risk = risk or RiskEngine()
        self.cost = cost or CostModel()
        self.init_equity = float(init_equity)

    def run(self) -> BacktestResult:
        bus = EventBus()
        data_engine = DataEngine(self.data, bus)
        # 先把所有 bar 推进总线 (DataEngine 一次性产出, 但严格有序)
        data_engine.stream()

        exec_engine = ExecutionEngine(self.cost)
        portfolio = Portfolio(self.init_equity)
        self.strategy.on_start(self)

        # 事件循环: 逐 bar 取 -> 策略 -> 风险 -> 执行(下一根) -> 结算
        pending_w = np.zeros(self.data.n_asset)   # 上一期成交后的实际权重
        n_bars = 0
        while True:
            ev = bus.pop()
            if ev is None:
                break
            bar: BarEvent = ev
            n_bars += 1

            # 1) 策略基于当前 bar 出目标仓位
            intent = self.strategy.on_bar(bar)
            # 2) 风险修正
            if intent is not None:
                intent = self.risk.apply(intent, bar)
            # 3) 执行: 把上一期的目标拿到本 bar 开盘成交 (此时 exec 已挂起)
            # 3) 执行: 把上一期的目标拿到**下一根 bar 的开盘价**成交。
            #    next_open 由引擎从自己的数据引用取 —— 不经过 BarEvent, 所以
            #    策略拿不到未来价格 (结构性防泄漏)。
            next_open = None
            if 0 <= bar.bar_index < self.data.n_time - 1:
                idx = [self.data.asset_index[a] for a in bar.assets]
                next_open = self.data.open[bar.bar_index + 1, idx]
            fill = exec_engine.on_bar(bar, next_open)
            if fill is not None:
                portfolio.on_fill(fill)
                # 成交后的实际持仓按**本期资产顺序**重排 (逐日 PIT 宇宙下
                # 资产集合会变; 上一次记录的顺序可能与本期不同)
                pending_w = exec_engine.current_weights
            # 4) 挂起本期的目标, 留给下一根 bar 成交
            if intent is not None:
                exec_engine.on_intent(intent, len(bar.assets),
                                       asset_order=bar.assets)
            # 5) 结算权益: 用"上一期权重" x "本期收益", 扣本期成本
            portfolio.on_bar(bar, _align_to(pending_w, exec_engine,
                                            bar.assets))

        return BacktestResult(portfolio, n_bars)