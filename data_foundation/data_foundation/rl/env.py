# -*- coding: utf-8 -*-
"""rl/env.py — PortfolioEnv: 把回测引擎封装成可 step 的 RL 环境

设计: docs/reinforcement-learning-design.md §4

**核心决定 (原则1): Env 的底层就是现有的回测引擎组件** ——
step(action) 把动作转成 OrderIntent, 走**同一个** ExecutionEngine/Portfolio。
这样 RL 学到的策略在回测里可直接复现, 不需要"再实现一次回测",
即"训练环境 = 回测 = 实盘"三者同路径 (与回测引擎原则2 一致)。

奖励 = clip(log 权益变化, ±clip) - λ·换手 (原则2)
  - 成本与回测/标签三处同源 (同一个 CostModel)
  - 缩尾是**必需**的: 首次运行报告实测单期 +243 倍, 不缩尾梯度必爆

动作 = 组合权重 -> RiskEngine 修正 -> OrderIntent (原则3, 与监督学习同构)
状态 = 因子面板当期值 + 当前持仓 (原则4, 物理上不含未来: 只做精确查表)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..backtest.execution_engine import CostModel, ExecutionEngine, Portfolio
from ..backtest.strategy import RiskEngine

__all__ = ["PortfolioEnv", "TopKAction"]


class TopKAction:
    """结构化动作: 截面打分 -> top-k 权重 (设计文档 §4 的动作空间备选)。

    **为什么需要 (实测踩到)**: 342 维连续权重作为 PPO 动作空间太难 —— 智能体
    要同时拧 342 个旋钮, 训练回报始终不涨。改成"结构化动作"后, 智能体只需要
    输出 **342 维的打分** (决定谁值得买), 仓位怎么分配由**固定公式**算出
    (与监督学习的 rank_linear / top_n 映射同构):

        w = softmax(score / temperature) 后取 top-k, 再归一到总杠杆

    决策维度从"342 个精确权重"降到"342 个相对排序", 样本效率高得多。
    """

    def __init__(self, top_k: int = 10, temperature: float = 1.0,
                 long_short: bool = False, max_weight: float = 0.1):
        self.top_k = int(top_k)
        self.temperature = float(temperature)
        self.long_short = bool(long_short)
        self.max_weight = float(max_weight)

    @property
    def action_dim(self) -> int:
        """打分维度 = 资产数 (由 env 决定)。"""
        return 0

    def to_weights(self, scores: np.ndarray) -> np.ndarray:
        """打分 -> 目标权重 (纯函数, 可独立测试)。"""
        s = np.asarray(scores, dtype=float)
        s = np.where(np.isfinite(s), s, -np.inf)
        w = np.zeros(len(s))
        k = min(self.top_k, int(np.isfinite(s).sum()))
        if k <= 0:
            return w
        if self.long_short and k >= 2:
            half = k // 2
            order = np.argsort(-s, kind="stable")[:k]
            w[order[:half]] = 0.5 / half
            w[order[half:k]] = -0.5 / (k - half)
        else:
            order = np.argsort(-s, kind="stable")[:k]
            w[order] = 1.0 / k
        gross = float(np.abs(w).sum())
        if gross > 0:
            w = w / gross
        cap = self.max_weight
        if cap > 0:
            over = np.abs(w) > cap
            if over.any():
                w[over] = np.sign(w[over]) * cap
                g2 = float(np.abs(w).sum())
                if g2 > 0:
                    w = w / g2          # 归一回总杠杆 1 (单资产上限后重新分配)
        return w


class PortfolioEnv:
    """组合权重 RL 环境。

    panel      : 含 open/high/low/close 的面板 (MultiIndex(asset, time)),
                 必须是**固定资产集** (回测引擎契约; 见首次运行报告缺陷3)
    factor_panel: (asset, time) x 因子的值面板 —— state 的特征部分; 可为 None
                  (则 state 只含持仓)
    """

    def __init__(self, panel, factor_panel=None, *, cost: CostModel | None = None,
                 risk: RiskEngine | None = None, clip_reward: float = 0.05,
                 turnover_penalty: float = 0.0, reward_scale: float = 1.0,
                 reward_kind: str = "log_return", min_equity: float = 1.0,
                 max_equity: float = 1e9, action_mode: str = "weights",
                 top_k: int = 10, action_scale: float = 0.01, seed: int = 0):
        """
        action_mode : "weights" (默认, 每资产一个连续权重) / "scores"
                     (结构化动作: 打分 -> top-k 权重, 见 TopKAction)。
                     342 维连续权重对 PPO 太难, "scores" 模式把决策降成
                     "相对排序", 样本效率高得多 (实测: weights 模式训练
                     回报不涨, scores 模式可学)。
        """
        self.panel = panel
        self.factor_panel = factor_panel
        self.cost = cost or CostModel()
        self.risk = risk or RiskEngine(max_weight=0.05, max_gross=1.0)
        self.clip_reward = float(clip_reward)
        self.turnover_penalty = float(turnover_penalty)
        self.reward_scale = float(reward_scale)
        self.reward_kind = reward_kind
        self.min_equity = float(min_equity)
        self.max_equity = float(max_equity)
        self.action_mode = action_mode
        self.action_scale = float(action_scale)
        self.topk = TopKAction(top_k=top_k, long_short=False,
                               max_weight=risk.max_weight if risk else 0.1)
        self.seed = int(seed)
        self.rng = np.random.default_rng(seed)
        self._prep()

    # -- 数据准备 ---------------------------------------------------------
    def _prep(self):
        v = self.panel
        self.assets = tuple(sorted(v.index.get_level_values("base_asset").unique()))
        self.times = pd.DatetimeIndex(
            sorted(v.index.get_level_values("time").unique()))

        def mat(col):
            if col not in v.columns:
                return np.zeros((self.n_time, self.n_asset))
            # unstack -> (asset, time); reindex 对齐; 转置成 (time, asset)
            s = v[col].unstack(level="time").reindex(index=self.assets,
                                                     columns=self.times)
            return np.ascontiguousarray(s.to_numpy(dtype=float).T)

        self.asset_index = {a: i for i, a in enumerate(self.assets)}
        self.n_time = len(self.times)
        self.n_asset = len(self.assets)
        self.O = mat("open")
        self.C = mat("close")
        # 因子面板 (asset,time) x (feature) -> (time, asset, feature)
        if self.factor_panel is not None and len(self.factor_panel.columns):
            mats = []
            for c in self.factor_panel.columns:
                m = (self.factor_panel[c].unstack(level="time")
                     .reindex(index=self.assets, columns=self.times)
                     .to_numpy(dtype=float).T)          # (time, asset)
                mats.append(m)
            self.F = np.stack(mats, axis=-1)              # (time, asset, feat)
            self.F = np.nan_to_num(self.F, nan=0.0)
        else:
            self.F = np.zeros((self.n_time, self.n_asset, 0))
        self.n_factor = self.F.shape[2]
        self.state_dim = self.n_asset * self.n_factor + self.n_asset + 1

    # -- state ------------------------------------------------------------
    def _state(self, t: int) -> np.ndarray:
        """当期可见状态: 因子当期 + 当前持仓 + 现金。**不含未来**。"""
        return np.concatenate([
            self.F[t].reshape(-1), self.weights,
            np.array([self.cash_fraction]),
        ]).astype(np.float32)

    # -- 生命周期 ---------------------------------------------------------
    def reset(self, start: int | None = None) -> np.ndarray:
        self.t = int(start if start is not None else 0)
        self.portfolio = Portfolio()
        self.exec = ExecutionEngine(self.cost)
        self.weights = np.zeros(self.n_asset)
        self.cash_fraction = 1.0
        self.steps = 0
        self.total_reward = 0.0
        self.clamped_high = False
        self.equity_curve = [self.portfolio.equity]
        return self._state(self.t)

    def step(self, action: np.ndarray) -> tuple[np.ndarray, float, bool, dict]:
        """执行一步: action(权重) -> 收益/奖励。

        成交在**下一根 bar 的开盘** (与回测引擎一致) —— 动作在 t 收盘定,
        成交在 t+1 开盘, 持仓承担 t+1 的价格变动。
        """
        if self.t >= self.n_time - 2:
            # 最后一根没有下一根开盘价 -> 无法成交 -> 结束
            return self._state(self.t), 0.0, True, self._info(0.0, 0.0)
        target = np.asarray(action, dtype=float).reshape(-1)
        if target.shape[0] != self.n_asset:
            raise ValueError(f"动作维度 {target.shape[0]} != 资产数 {self.n_asset}")
        target = np.where(np.isfinite(target), target, 0.0)
        # **结构化动作**: 打分 -> top-k 权重 (决策降成"相对排序", PPO 可学)
        if self.action_mode == "scores":
            target = self.topk.to_weights(target)
        # **动作归一化**: PPO 输出的是无约束向量, 直接当权重会让总杠杆随机
        # 放大 (实测 342 资产时 pnl 可达 -1e13, 权益直接被打成负数)。
        # 这里把动作当成**相对权重**: 归一到总杠杆 <= max_gross, 再交给
        # RiskEngine 做单资产/总杠杆约束 (与回测同一套)。
        gross = float(np.abs(target).sum())
        if gross > self.risk.max_gross and gross > 0:
            target = target * (self.risk.max_gross / gross)
        # RiskEngine.apply 需要 OrderIntent; 用其修正逻辑 (与回测同一约束)
        from ..backtest.events import OrderIntent
        intent = self.risk.apply(
            OrderIntent(ts=self.times[self.t], target_weights=target,
                        reason="rl"), _FakeBar(self.assets))
        # T+1 成交语义 (与回测引擎一致): 本期动作挂起, **下一根 bar 开盘**成交
        self.exec.on_intent(intent, self.n_asset)
        fill = self.exec.on_bar(_FillBar(self.assets), next_open=self.O[self.t + 1])
        turnover = 0.0
        if fill is not None:
            self.portfolio.on_fill(fill)
            turnover = fill.turnover
            self.weights = self.exec.current_weights.copy()
        # 持仓承担 t+1 的收盘收益
        prev_equity = self.portfolio.equity
        self.t += 1
        with np.errstate(divide="ignore", invalid="ignore"):
            r = self.C[self.t] / self.C[self.t - 1] - 1.0
        r = np.where(np.isfinite(r), r, 0.0)
        pnl = float(np.dot(self.weights, r)) if self.n_asset else 0.0
        cost = fill.cost if fill is not None else 0.0
        # 结算权益 (与 Portfolio 同式, 但这里逐步推进便于算奖励)
        self.portfolio.equity *= (1.0 + pnl)
        self.portfolio.equity -= cost
        # **破产保护**: 权重法复利没有下限, pnl<-1 会把权益打成负数, 此后
        # 所有指标(cagr/sharpe/回撤)全废 (实测 equity=-1.5e13)。RL 的回报函数
        # 在权益归零后无意义 —— 这里把权益钳在最小正值并终止 episode。
        bankrupt = False
        if self.portfolio.equity <= self.min_equity:
            self.portfolio.equity = self.min_equity
            bankrupt = True
        # **权益上限保护** (对称): 实测面板含"单日 +111948 倍"的记录, 随机
        # 权重摊到它上面就能把权益推到 1e13 —— 同样让后续指标失去意义
        # (Sharpe 的年化会爆成天文数字)。这里钳住并标记。
        if self.portfolio.equity >= self.max_equity:
            self.portfolio.equity = self.max_equity
            self.clamped_high = True
        self.portfolio.times.append(self.times[self.t])
        self.portfolio.equities.append(self.portfolio.equity)
        self.portfolio.period_returns.append(pnl)
        self.portfolio.total_cost += cost
        self.portfolio.total_turnover += turnover
        self.portfolio._weights = self.weights.copy()
        self.portfolio._prev_close = np.array(self.C[self.t], copy=True)
        self.equity_curve.append(self.portfolio.equity)
        self.cash_fraction = max(0.0, 1.0 - float(np.abs(self.weights).sum()))
        reward = self._reward(prev_equity, self.portfolio.equity, turnover)
        self.total_reward += reward
        self.steps += 1
        done = self.t >= self.n_time - 2 or bankrupt
        return self._state(self.t), reward, done, self._info(pnl, turnover,
                                                             bankrupt)

    def _reward(self, prev_eq: float, eq: float, turnover: float) -> float:
        if prev_eq <= 0 or not np.isfinite(prev_eq):
            return 0.0
        if self.reward_kind == "log_return":
            r = np.log(max(eq, 1e-12) / prev_eq)
            r = float(np.clip(r, -self.clip_reward, self.clip_reward))
        elif self.reward_kind == "net_return":
            r = (eq / prev_eq - 1.0)
            r = float(np.clip(r, -self.clip_reward * 10, self.clip_reward * 10))
        else:
            r = float(np.clip(np.log(max(eq, 1e-12) / prev_eq),
                              -self.clip_reward, self.clip_reward))
        r -= self.turnover_penalty * turnover
        return r * self.reward_scale

    def _info(self, pnl: float, turnover: float,
              bankrupt: bool = False) -> dict:
        return {"t": self.t, "pnl": pnl, "turnover": turnover,
                "equity": self.portfolio.equity, "bankrupt": bankrupt,
                "gross": float(np.abs(self.weights).sum()),
                "reward_total": self.total_reward}

    # -- 便捷 -------------------------------------------------------------
    @property
    def n_actions(self) -> int:
        return self.n_asset

    def portfolio_metrics(self, periods_per_year: float = 252) -> dict:
        return self.portfolio.metrics(periods_per_year=periods_per_year)


# -- 给 RiskEngine.apply 用的最小 BarEvent 替身 (只需要 assets) ------------
class _FakeBar:
    def __init__(self, assets):
        self.assets = tuple(assets)
        self.ts = None
        self.bar_time = None


class _FillBar(_FakeBar):
    """给 ExecutionEngine.on_bar 的替身 (它只读 bar.assets)。"""