# -*- coding: utf-8 -*-
"""_test_evaluation.py — 评价协议单测 (三层指标 / PSR-DSR / schema / 单一路径)

设计: docs/evaluation-protocol-design.md §10 验收标准
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.evaluation import (METRICS_VERSION, evaluate_candidate,
                                        ic_series, prediction_metrics,
                                        rank_ic_series, trading_metrics,
                                        validate_metrics, walkforward_stability)
from data_foundation.evaluation import significance as sg
from data_foundation.labels.objectives import CostParams

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# 1. 预测层 —— 手算对照
# ---------------------------------------------------------------------------
def _panel(seed=42, n_assets=6, n_days=5):
    idx = pd.MultiIndex.from_product(
        [[f"A{i}USDT" for i in range(n_assets)],
         pd.date_range("2021-01-01", periods=n_days, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    rng = np.random.default_rng(seed)
    y = pd.Series(rng.normal(0.01, 0.03, len(idx)), index=idx)
    s = pd.Series(y.to_numpy() + rng.normal(0, 0.005, len(idx)), index=idx)
    return s, y


def test_ic_handcheck():
    s, y = _panel(seed=1, n_assets=6, n_days=2)
    ic = ic_series(s, y)
    # 手算第一个决策日的截面 Pearson
    t0 = pd.Timestamp("2021-01-01", tz="UTC")
    sub_s = s.xs(t0, level="time")
    sub_y = y.xs(t0, level="time")
    expect = float(np.corrcoef(sub_s.to_numpy(), sub_y.to_numpy())[0, 1])
    actual = float(ic.iloc[0])
    assert abs(actual - expect) < 1e-10, \
        f"IC 手算 {expect:.12f} vs 实现 {actual:.12f}"
    assert -1.0 <= actual <= 1.0


def test_rank_ic_handcheck():
    s, y = _panel(seed=1, n_assets=6, n_days=1)
    ric = rank_ic_series(s, y)
    sub_s = s.xs(pd.Timestamp("2021-01-01", tz="UTC"), level="time")
    sub_y = y.xs(pd.Timestamp("2021-01-01", tz="UTC"), level="time")
    expect = float(sub_s.corr(sub_y, method="spearman"))
    assert abs(float(ric.iloc[0]) - expect) < 1e-10


def test_icir_formula():
    s, y = _panel(seed=2, n_assets=6, n_days=30)
    pm = prediction_metrics(s, y)
    ic = ic_series(s, y).dropna()
    if len(ic) > 1 and ic.std(ddof=1) > 0:
        expect_icir = ic.mean() / ic.std(ddof=1)
        assert abs(pm["icir"] - expect_icir) < 1e-10, \
            f"ICIR 应为 mean/std: {pm['icir']} vs {expect_icir}"


def test_perfect_prediction():
    """完美预测 -> IC = 1, hit_rate = 1。"""
    idx = pd.MultiIndex.from_product(
        [["A", "B", "C", "D", "E"],
         pd.date_range("2021-01-01", periods=3, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    rng = np.random.default_rng(9)
    y = pd.Series(rng.normal(0.01, 0.05, len(idx)), index=idx)
    s = y.copy()   # 完全相同
    pm = prediction_metrics(s, y)
    assert abs(pm["ic_mean"] - 1.0) < 1e-9, f"完美预测 IC 应=1, 得 {pm['ic_mean']}"
    assert abs(pm["rank_ic_mean"] - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# 2. 分位组合 + 成本对照 (原则3)
# ---------------------------------------------------------------------------
def test_quantile_cost_contrast():
    s, y = _panel(seed=3, n_assets=10, n_days=8)
    c = CostParams(taker_fee=0.0005, slippage_bps=5.0)
    pm = prediction_metrics(s, y, cost=c, n_quantiles=5)
    # 成本对照: 单边 top 组零成本收益 - 净收益 = c_rt
    c_rt = c.round_trip
    if pm["top_group_return_net"] == pm["top_group_return_net"]:
        assert abs(pm["cost_eaten_by_top_group"] - c_rt) < 1e-12, \
            "单边成本应恰为 c_rt"
        assert abs((pm["top_group_return_zero_cost"]
                    - pm["top_group_return_net"]) - c_rt) < 1e-12
    # 多空价差对均一成本不敏感 (两边同扣)
    assert abs(pm["quantile_spread_zero_cost"] - pm["quantile_spread"]) < 1e-12, \
        "多空价差对均一成本应不敏感"


def test_quantile_monotonicity():
    # 分数与标签严格同向 -> 分位单调性应接近 1
    idx = pd.MultiIndex.from_product(
        [[f"A{i}" for i in range(20)],
         pd.date_range("2021-01-01", periods=3, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    base = np.arange(20, dtype=float)
    s = pd.Series(np.tile(base, 3) + np.tile(base, 3) * 0, index=idx)
    y = pd.Series(np.tile(base * 0.01, 3) + np.tile(base * 0.01, 3) * 0, index=idx)
    pm = prediction_metrics(s, y, n_quantiles=5)
    if pm["quantile_monotonicity"] == pm["quantile_monotonicity"]:
        assert pm["quantile_monotonicity"] > 0.99, \
            f"严格同向时分位单调性应≈1, 得 {pm['quantile_monotonicity']}"


# ---------------------------------------------------------------------------
# 3. 交易层
# ---------------------------------------------------------------------------
def test_trading_metrics():
    pf = {"cagr": 0.3, "vol_annual": 0.15, "sharpe": 2.0, "max_drawdown": -0.2,
          "total_cost": 500.0, "total_turnover": 10.0, "n_points": 8760}
    pr = pd.Series([0.01, -0.005, 0.02, -0.01, 0.03])
    tm = trading_metrics(pf, period_returns=pr, periods_per_year=365 * 24)
    assert abs(tm["ann_return_net"] - 0.3) < 1e-12
    assert abs(tm["calmar"] - 0.3 / 0.2) < 1e-9, f"Calmar={tm['calmar']}"
    assert abs(tm["win_rate"] - 0.6) < 1e-12, "3涨2跌 -> 胜率 0.6"
    gains = 0.01 + 0.02 + 0.03
    losses = 0.005 + 0.01
    assert abs(tm["profit_factor"] - gains / losses) < 1e-9
    # 年化换手 = 总换手 / 年数
    assert tm["annualized_turnover"] > 0


# ---------------------------------------------------------------------------
# 4. 显著性层 (PSR / DSR)
# ---------------------------------------------------------------------------
def test_norm_inverse():
    assert abs(sg._norm_ppf(0.975) - 1.959964) < 1e-5
    assert abs(sg._norm_ppf(0.95) - 1.644854) < 1e-5
    assert abs(sg._norm_ppf(0.5)) < 1e-9


def test_dsr_monotone_in_n():
    """固定 Sharpe, N 增大 -> 阈值升 -> DSR 降 (多重检验修正的核心性质)。"""
    n_obs = 2000
    for sr in (0.06, 0.10):
        dsrs = []
        for nt in (1, 100, 10000):
            thr = sg.expected_max_sr(nt, n_obs=n_obs)
            dsrs.append(sg.probabilistic_sharpe_ratio(sr, benchmark=thr,
                                                      n_obs=n_obs))
        assert dsrs[0] >= dsrs[1] >= dsrs[2], \
            f"DSR 应随 N 不增: {dsrs}"
        assert dsrs[0] > dsrs[2], f"DSR 应严格下降: {dsrs}"


def test_expected_max_sr_zero_at_n1():
    assert abs(sg.expected_max_sr(1, n_obs=1000)) < 1e-12, \
        "N=1 时期望最大 Sharpe 应为 0 (无多重检验)"


def test_significance_from_returns():
    rng = np.random.default_rng(21)
    r = pd.Series(rng.normal(0.0002, 0.01, 1000))
    sm = sg.significance_metrics(r, n_trials=10, periods_per_year=365 * 24)
    assert 0.0 <= sm["psr"] <= 1.0
    assert 0.0 <= sm["dsr"] <= 1.0
    assert sm["n_obs"] == 1000 and sm["n_trials"] == 10
    assert sm["kurtosis"] > 0, "峰度应转成非 excess"


def test_go_no_go():
    strong = sg.go_no_go({"significance": {"psr": 0.99, "dsr": 0.97},
                          "trading": {"ann_return_net": 0.2}})
    weak = sg.go_no_go({"significance": {"psr": 0.90, "dsr": 0.60},
                        "trading": {"ann_return_net": 0.05}})
    negative = sg.go_no_go({"significance": {"psr": 0.99, "dsr": 0.97},
                            "trading": {"ann_return_net": -0.1}})
    assert strong["passed"] and not weak["passed"] and not negative["passed"]


# ---------------------------------------------------------------------------
# 5. schema 门
# ---------------------------------------------------------------------------
def test_validate_metrics_schema():
    s, y = _panel(seed=4, n_assets=6, n_days=10)
    pr = pd.Series(np.random.default_rng(5).normal(0.0002, 0.01, 300))
    m = evaluate_candidate(
        s, y,
        portfolio={"cagr": 0.25, "vol_annual": 0.15, "sharpe": 1.7,
                   "max_drawdown": -0.15, "total_cost": 100.0,
                   "total_turnover": 5.0, "n_points": 300},
        period_returns=pr, n_trials=10, strict=False)
    assert m["metrics_version"] == METRICS_VERSION
    validate_metrics(m, strict=True)   # 合法 metrics 应通过
    # 注入非法字段
    bad = {**m, "prediction": {**m["prediction"], "made_up_metric": 1.0}}
    assert any("白名单外" in p for p in validate_metrics(bad, strict=False)), \
        "白名单外字段应被检出"
    # 缺顶层
    missing = {k: v for k, v in m.items() if k != "trading"}
    assert any("缺少顶层" in p for p in validate_metrics(missing, strict=False))
    # 越界
    oob = {**m, "prediction": {**m["prediction"], "ic_mean": 1.5}}
    assert any("越界" in p for p in validate_metrics(oob, strict=False))


# ---------------------------------------------------------------------------
# 6. 单一代码路径 + 跨折稳定性
# ---------------------------------------------------------------------------
def test_single_code_path_deterministic():
    """dev 自评 = 同一函数再跑一次 (原则1: 同输入逐位一致)。"""
    s, y = _panel(seed=6, n_assets=6, n_days=10)
    pr = pd.Series(np.random.default_rng(7).normal(0.0002, 0.01, 200))
    pf = {"cagr": 0.2, "vol_annual": 0.1, "sharpe": 2.0, "max_drawdown": -0.1,
          "total_cost": 50.0, "total_turnover": 4.0, "n_points": 200}
    m1 = evaluate_candidate(s, y, portfolio=pf, period_returns=pr, n_trials=5)
    m2 = evaluate_candidate(s, y, portfolio=pf, period_returns=pr, n_trials=5)
    assert m1["prediction"]["ic_mean"] == m2["prediction"]["ic_mean"], \
        "同输入两次 ic_mean 应逐位一致"
    assert m1["trading"]["sharpe"] == m2["trading"]["sharpe"]
    assert m1["significance"]["dsr"] == m2["significance"]["dsr"]


def test_walkforward_stability():
    folds = [{"sharpe": 1.0}, {"sharpe": 2.0}, {"sharpe": 1.5}]
    st = walkforward_stability(folds)
    assert st["n_folds"] == 3
    assert abs(st["worst_fold_sharpe"] - 1.0) < 1e-12
    assert st["fold_sharpe_std"] > 0, "不同折 Sharpe 应有标准差"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"evaluation 测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)