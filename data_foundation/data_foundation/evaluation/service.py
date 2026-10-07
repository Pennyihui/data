# -*- coding: utf-8 -*-
"""service.py — evaluate_candidate(): 开发池自评与 valid/oos 服务评的**同一入口**

设计: docs/evaluation-protocol-design.md §1 原则1 (单一代码路径)

  "开发池自评与 valid/oos 服务评调用同一个 evaluate_candidate() —— 服务只是
   换执行环境, 不是换评价逻辑。"

所以本模块只接收**已算好的三样东西** (分数面板 / 标签面板 / 回测绩效) 和一个
试验次数, 组装三层指标并过 schema 门。取数、训练、回测由调用方 (各自的池路径)
完成, 但**评价逻辑只有这一份**。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .metrics import (METRICS_VERSION, build_metrics, prediction_metrics,
                      trading_metrics, validate_metrics, walkforward_stability)
from .significance import significance_metrics, go_no_go
from ..labels.objectives import CostParams

__all__ = ["evaluate_candidate"]


def evaluate_candidate(scores: pd.Series, labels: pd.Series, *,
                       portfolio: dict | None = None,
                       period_returns: pd.Series | None = None,
                       periods_per_year: float = 365 * 24,
                       n_trials: int = 1,
                       cost: CostParams | None = None,
                       fold_metrics: list[dict] | None = None,
                       extra: dict | None = None,
                       strict: bool = True) -> dict:
    """三层指标一次算齐 (开发池自评 = 服务端评估 = 同一个函数)。

    scores / labels : MultiIndex(asset, time) 面板 (预测分数 × 标签)
    portfolio       : 回测引擎 Portfolio.metrics() 的返回 (交易层输入)
    period_returns  : 逐 bar 损益 (胜率/盈亏比 + 显著性层的 T/偏度/峰度)
    n_trials        : 全局试验次数 N (含开发池) —— DSR 用; N 失真则 DSR 失真
    fold_metrics    : 各 walk-forward 折的指标 -> 跨折稳定性
    """
    cost = cost or CostParams()
    out: dict = {"metrics_version": METRICS_VERSION}

    # -- 预测层 --
    try:
        pred = prediction_metrics(scores, labels, cost=cost)
    except Exception as e:                       # 样本不足等 -> 记 nan 不崩
        pred = {"error": f"{type(e).__name__}: {e}"}

    # -- 交易层 --
    if portfolio is not None:
        trd = trading_metrics(portfolio, period_returns=period_returns,
                              periods_per_year=periods_per_year)
    else:
        trd = {}

    # -- 显著性层 --
    sig: dict = {}
    if period_returns is not None and len(period_returns) >= 3:
        sig = significance_metrics(period_returns, n_trials=int(n_trials),
                                   sharpe=trd.get("sharpe"),
                                   periods_per_year=periods_per_year)
    stability = walkforward_stability(fold_metrics) if fold_metrics else None

    metrics = build_metrics(pred, trd, sig, stability=stability, extra=extra)
    metrics["decision"] = go_no_go(metrics)      # 人审闸 (决策 E5)
    validate_metrics(metrics, strict=strict)      # schema 门
    return metrics


if __name__ == "__main__":  # pragma: no cover
    idx = pd.MultiIndex.from_product(
        [["A", "B", "C", "D", "E"],
         pd.date_range("2021-01-01", periods=40, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    rng = np.random.default_rng(11)
    y = pd.Series(rng.normal(0.005, 0.05, len(idx)), index=idx)
    s = y + rng.normal(0, 0.02, len(idx))
    pr = pd.Series(rng.normal(0.0002, 0.01, 1000))
    m = evaluate_candidate(
        s, y,
        portfolio={"cagr": 0.25, "vol_annual": 0.15, "sharpe": 1.7,
                   "max_drawdown": -0.15, "total_cost": 200.0,
                   "total_turnover": 8.0, "n_points": 1000},
        period_returns=pr, n_trials=20)
    print("ic_mean =", round(m["prediction"]["ic_mean"], 4))
    print("sharpe  =", round(m["trading"]["sharpe"], 4))
    print("psr/dsr =", round(m["significance"]["psr"], 4),
          "/", round(m["significance"]["dsr"], 4))
    print("decision=", m["decision"]["passed"], m["decision"]["checks"])