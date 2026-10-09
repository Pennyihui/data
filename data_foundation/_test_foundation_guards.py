# -*- coding: utf-8 -*-
"""_test_foundation_guards.py — 底座防护的回归测试 (#1 破产保护 / #2 可变资产集)

背景: 两轮真实研究踩到的两个底座缺陷 (docs/research-chain-run02-*.md):
  #1 Portfolio 权重法复利无下限 -> 权益变负 -> 指标全废 (实测回撤 -10186%)
  #2 DataEngine 派发可变资产集 vs ExecutionEngine 固定长度权重 -> 维度不匹配崩
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.backtest.data_engine import BacktestData
from data_foundation.backtest.engine import BacktestEngine
from data_foundation.backtest.events import BarEvent, OrderIntent
from data_foundation.backtest.execution_engine import (CostModel,
                                                       ExecutionEngine,
                                                       Portfolio)
from data_foundation.backtest.strategy import RiskEngine, Strategy

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


def bar(ts, assets, close, open_=None, avail=None):
    o = np.asarray(open_ if open_ is not None else close, dtype=float)
    c = np.asarray(close, dtype=float)
    n = len(assets)
    return BarEvent(ts=ts, available_at=avail or ts, assets=assets,
                    open=o, high=c * 1.01, low=c * 0.99, close=c,
                    volume=np.ones(n), asset_index={a: i for i, a in enumerate(assets)},
                    bar_time=ts, bar_index=0)


# ---------------------------------------------------------------------------
# #1 破产保护
# ---------------------------------------------------------------------------
def test_bankrupt_clamped_not_negative():
    """权益跌破下限 -> 钳住并标记 bankrupt, 指标不炸。"""
    p = Portfolio(100_000.0, min_equity=1.0)
    assets = ("A",)
    # 价格连续腰斩 17 次: 100000 * 0.5^17 ≈ 0.76 -> 必然跌破 min_equity
    px = 100.0
    for i in range(18):
        ts = pd.Timestamp("2021-01-01", tz="UTC") + pd.Timedelta(days=i)
        p.on_bar(bar(ts, assets, np.array([px])), np.array([1.0]))
        px *= 0.5
    assert p.equity > 0, f"权益不得为负, 实际 {p.equity}"
    assert p.bankrupt, "应标记 bankrupt"
    m = p.metrics(periods_per_year=252)
    assert m["bankrupt"] is True
    assert m["trustworthy"] is False, "破产结果必须标记为不可信"


def test_max_equity_clamped():
    """极端暴涨 -> 上限钳制 + clamped_high 标记。"""
    p = Portfolio(100_000.0, max_equity_mult=10.0)
    assets = ("A",)
    for i, px in enumerate([100.0, 1000.0, 10000.0]):
        ts = pd.Timestamp("2021-01-01", tz="UTC") + pd.Timedelta(days=i)
        p.on_bar(bar(ts, assets, np.array([px])), np.array([1.0]))
    assert p.clamped_high, "应标记 clamped_high"
    assert p.equity <= 100_000.0 * 10.0 + 1e-6, \
        f"权益应被钳在 10x, 实际 {p.equity}"


def test_normal_path_unaffected():
    """正常回测不受影响 (没有破产/上限时 trustworthy=True)。"""
    p = Portfolio(100_000.0)
    assets = ("A",)
    for i, px in enumerate([100.0, 101.0, 102.5, 103.0]):
        ts = pd.Timestamp("2021-01-01", tz="UTC") + pd.Timedelta(days=i)
        p.on_bar(bar(ts, assets, np.array([px])), np.array([1.0]))
    m = p.metrics(periods_per_year=252)
    assert m["trustworthy"] is True and not m["bankrupt"]
    assert m["final_equity"] > 100_000.0, "上涨行情权益应增加"


# ---------------------------------------------------------------------------
# #2 可变资产集
# ---------------------------------------------------------------------------
def test_execution_realigns_weights():
    """资产集合变化时, 上一期权重按资产对齐 (退场权重置 0)。"""
    ex = ExecutionEngine(CostModel())
    prev = ("A", "B", "C")
    intent = OrderIntent(ts=1, target_weights=np.array([0.5, 0.3, 0.2]),
                         reason="t")
    ex.on_intent(intent, 3, asset_order=prev)
    # 下一期 B 退场, 新增 D
    new = ("A", "C", "D")
    b = bar(pd.Timestamp("2021-01-02", tz="UTC"), new, np.array([10.0, 10.0, 10.0]))
    fill = ex.on_bar(b, np.array([10.0, 10.0, 10.0]))
    assert fill is not None, "应成交"
    w = ex.current_weights
    assert w.shape == (3,), f"权重维度应跟随新资产集 {w.shape}"
    assert abs(w[1] - 0.2) < 1e-12, f"C 的权重应保持 {w[1]} (B 退场不应影响它)"
    assert abs(w[2]) < 1e-12, "新上市 D 应初始权重 0"


def test_portfolio_handles_asset_change():
    """Portfolio 在资产集合变化时按资产对齐计算盈亏, 不崩。"""
    p = Portfolio(100_000.0)
    t0 = pd.Timestamp("2021-01-01", tz="UTC")
    p.on_bar(bar(t0, ("A", "B"), np.array([100.0, 100.0])), np.array([0.5, 0.5]))
    # 第二期 B 退场 (只剩 A), A 涨 10%
    t1 = t0 + pd.Timedelta(days=1)
    p.on_bar(bar(t1, ("A",), np.array([110.0])), np.array([1.0]))
    m = p.metrics(periods_per_year=252)
    assert np.isfinite(m["total_return"]), "指标应有限"
    assert m["total_return"] > 0, "A 涨 10% 应有正收益"


def test_backtest_runs_with_changing_universe():
    """端到端: 逐日 PIT 宇宙变化的资产集能跑完回测 (原来会维度不匹配崩)。"""
    times = pd.date_range("2021-01-01", periods=12, freq="D", tz="UTC")
    assets = ("A", "B", "C", "D")
    n_t, n_a = len(times), len(assets)
    close = np.tile(np.array([100.0, 50.0, 25.0, 10.0]), (n_t, 1))
    universe = np.ones((n_t, n_a), dtype=bool)
    universe[6:, 1] = False      # B 在第 7 天退市
    universe[3:, 2] = False      # C 在第 4 天退市
    universe[9:, 3] = False      # D 在第 10 天退市
    close[~universe] = np.nan
    data = BacktestData(
        open=np.nan_to_num(close), high=close, low=close, close=close,
        volume=np.ones((n_t, n_a)),
        available_at=pd.DatetimeIndex(times + pd.Timedelta(days=1) -
                                      pd.Timedelta(seconds=1)),
        times=pd.DatetimeIndex(times), assets=assets, universe=universe,
        asset_index={a: i for i, a in enumerate(assets)})

    class Const(Strategy):
        def on_bar(self, b):
            return OrderIntent(ts=b.ts,
                               target_weights=np.ones(len(b.assets)) /
                               len(b.assets), reason="eq")

    res = BacktestEngine(data, Const(), risk=RiskEngine(max_weight=1.0),
                         cost=CostModel(), ).run()
    assert res is not None
    m = res.portfolio.metrics(periods_per_year=252)
    assert np.isfinite(m.get("final_equity", float("nan"))), "权益应有限"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"底座防护测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)