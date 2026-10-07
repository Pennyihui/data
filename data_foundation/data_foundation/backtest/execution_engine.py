# -*- coding: utf-8 -*-
"""execution_engine.py — ExecutionEngine: 目标仓位 → 下一根 bar 成交 (含成本)

设计: docs/backtest-engine-design.md (v0.1) §5/§6

这是**防泄漏的核心**: 策略在第 T 根 bar 收盘时输出目标仓位, 成交发生在
**第 T+1 根 bar 的开盘价**。Strategy 无法用 T 的收盘价成交 —— 那个价格要到
T 收盘才存在, 而信号也是那时才算出来的。

成本: 换手 × (手续费 + 滑点), 换手独立可查 (因子门槛的指标之一)。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .events import BarEvent, FillEvent, OrderIntent

__all__ = ["CostModel", "ExecutionEngine"]


@dataclass(frozen=True)
class CostModel:
    """成本模型 (费率 + 滑点, 都是单边)。"""

    taker_fee: float = 0.0005      # 吃单手续费 (假设 T+1 开盘吃单)
    slippage_bps: float = 5.0       # 单边滑点 (基点)
    #: 滑点方向: 买入向上、卖出向下 (永远不利于你)
    slippage_always_adverse: bool = True

    @property
    def slippage(self) -> float:
        return self.slippage_bps / 1e4

    def fill_price(self, open_price: np.ndarray, delta_w: np.ndarray) -> np.ndarray:
        """按仓位变化方向施加滑点: 买 (Δw>0) 价格上滑, 卖 (Δw<0) 下滑。"""
        sign = np.sign(delta_w)
        if not self.slippage_always_adverse:
            sign = np.ones_like(sign)
        return open_price * (1.0 + sign * self.slippage)


class ExecutionEngine:
    """把目标仓位落到下一根 bar 的成交。

    状态: current_weights (当前实际持仓)。收到 T 的 OrderIntent 后挂起, 等
    收到 T+1 的 BarEvent 时用它的 next_open 成交。
    """

    def __init__(self, cost: CostModel | None = None):
        self.cost = cost or CostModel()
        self.current_weights = np.zeros(0)
        self.fills: list[FillEvent] = []
        self._pending: OrderIntent | None = None

    def on_intent(self, intent: OrderIntent, n_asset: int) -> None:
        """Strategy 输出目标仓位 -> 挂起到下一根 bar 成交。"""
        intent.validate(n_asset)
        if self.current_weights.size == 0:
            self.current_weights = np.zeros(n_asset)
        if self.current_weights.shape != (n_asset,):
            raise ValueError(
                f"持仓维度 {self.current_weights.shape} 与 OrderIntent "
                f"({n_asset},) 不一致 —— 资产集合中途变了? 每期资产集合应固定")
        self._pending = intent

    def on_bar(self, bar: BarEvent, next_open: np.ndarray | None = None) -> FillEvent | None:
        """收到新 bar -> 执行上一期挂起的目标仓位。

        next_open 由 BacktestEngine 显式传入 (下一根 bar 的开盘价)。**它不在
        BarEvent 上** —— 策略拿到的事件里没有未来价格, 所以策略无法用它成交。
        最后一根 bar 没有下一根开盘价 -> 传 None -> 不成交 (仓位保持)。
        """
        if self._pending is None:
            return None
        intent = self._pending
        self._pending = None
        n = len(bar.assets)
        if next_open is None:
            # 最后一根 bar: 无未来开盘价 -> 不成交 (挂着的仓位保持)
            return None
        target = np.asarray(intent.target_weights, dtype=float)
        delta = target - self.current_weights
        # 极小的变动忽略 (避免浮点噪声产生虚假换手)
        delta[np.abs(delta) < 1e-12] = 0.0
        if not delta.any():
            return None
        price = self.cost.fill_price(np.asarray(next_open, dtype=float), delta)
        # 成本: 换手 × 手续费; 滑点已内含在成交价里 (不再重复扣)
        turnover = float(np.abs(delta).sum())
        cost = turnover * self.cost.taker_fee
        self.current_weights = target
        fill = FillEvent(ts=bar.ts, filled_weights=delta, price=price,
                         cost=cost, turnover=turnover)
        self.fills.append(fill)
        return fill


class Portfolio:
    """权益曲线与绩效 (由 BarEvent + FillEvent 驱动)。

    用**权重法**而不是逐笔现金记账: 权益逐期复利

        equity_t = equity_{t-1} * (1 + Σ w_{t-1} * r_t) - cost_t

    其中 r_t 是从上一根 bar 收盘到本根 bar 收盘的收益率, w_{t-1} 是上一期
    成交后的持仓权重。权重法的好处: 数值干净、可手工核对 (测试里就是这么验的),
    且与"目标权重"接口天然一致 —— 不用维护现金余额和股数两套账。
    """

    def __init__(self, init_equity: float = 100_000.0):
        self.init_equity = float(init_equity)
        self.equity = float(init_equity)
        self.times: list[pd.Timestamp] = []
        self.equities: list[float] = []
        self.period_returns: list[float] = []
        self.total_cost = 0.0
        self.total_turnover = 0.0
        self._prev_close: np.ndarray | None = None
        self._prev_assets: tuple[str, ...] | None = None
        self._weights = np.zeros(0)        # 当前持仓权重 (本 bar 收盘时持有)
        self._fills_pending: list[FillEvent] = []   # 成交先挂起, 下根 bar 结算

    # -- 逐 bar 推进 ---------------------------------------------------------
    def on_bar(self, bar: BarEvent, weights_now: np.ndarray) -> None:
        """结算本 bar 的损益。

        权重用**上一根 bar 收盘时定下的** (即 w_{t-1}), 收益率用 close/prev_close
        —— 持仓承担的是持仓期间的价格变动, 与本 bar 何时成交无关 (成交在开盘)。
        """
        w_hold = self._weights
        if self._prev_close is not None and self._prev_assets == bar.assets \
                and w_hold.size == len(bar.assets):
            with np.errstate(divide="ignore", invalid="ignore"):
                r = bar.close / self._prev_close - 1.0
            r = np.where(np.isfinite(r), r, 0.0)
            pnl = float(np.dot(w_hold, r)) if w_hold.size else 0.0
        else:
            pnl = 0.0
        cost = 0.0
        if self._fills_pending:
            cost = sum(f.cost for f in self._fills_pending)
            self.total_turnover += sum(f.turnover for f in self._fills_pending)
            self.total_cost += cost
            self._fills_pending = []
        self.equity *= (1.0 + pnl)
        self.equity -= cost
        self.period_returns.append(pnl)
        self.times.append(pd.Timestamp(bar.ts))
        self.equities.append(self.equity)
        self._weights = np.asarray(weights_now, dtype=float)
        self._prev_close = np.array(bar.close, dtype=float, copy=True)
        self._prev_assets = bar.assets

    def on_fill(self, fill: FillEvent) -> None:
        """成交先挂起, 在下一根 bar 结算时扣 (成交发生在 bar 开盘, 成本当期计入)。"""
        self._fills_pending.append(fill)

    # -- 绩效 ---------------------------------------------------------------
    def metrics(self, periods_per_year: float = 365 * 24) -> dict:
        if len(self.equities) < 2:
            return {"n_points": len(self.equities),
                    "init_equity": self.init_equity}
        eq = pd.Series(self.equities, index=pd.DatetimeIndex(self.times))
        rets = pd.Series(self.period_returns)
        total = float(self.equity / self.init_equity - 1.0)
        n_years = len(rets) / periods_per_year
        cagr = float((self.equity / self.init_equity) ** (1 / n_years) - 1) \
            if n_years > 0 and self.equity > 0 else float("nan")
        vol = float(rets.std(ddof=1) * np.sqrt(periods_per_year))
        sharpe = float(cagr / vol) if vol and np.isfinite(vol) and vol > 0 \
            else float("nan")
        dd = eq / eq.cummax() - 1.0
        return {
            "init_equity": self.init_equity,
            "final_equity": float(self.equity),
            "total_return": total,
            "cagr": cagr,
            "vol_annual": vol,
            "sharpe": sharpe,
            "max_drawdown": float(dd.min()),
            "total_cost": self.total_cost,
            "total_turnover": self.total_turnover,
            "avg_turnover_per_period": float(self.total_turnover / len(rets)),
            "n_points": len(eq),
        }

    def equity_frame(self) -> pd.DataFrame:
        return pd.DataFrame({"equity": self.equities,
                             "period_return": self.period_returns},
                            index=pd.DatetimeIndex(self.times))