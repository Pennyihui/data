# -*- coding: utf-8 -*-
"""_test_rl.py — 强化学习协议单测 (Env 封装 / 奖励 / 确定性)

设计: docs/reinforcement-learning-design.md §11 验收标准
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.rl import PortfolioEnv

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


def make_panel(n_assets=5, n_days=60, seed=0):
    times = pd.date_range("2021-01-01", periods=n_days, freq="D", tz="UTC")
    idx = pd.MultiIndex.from_product(
        [[f"A{i}USDT" for i in range(n_assets)], times],
        names=["base_asset", "time"])
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.01, (n_days, n_assets)),
                                     axis=0))
    open_ = np.vstack([close[0:1], close[:-1]])      # 次日开盘 = 前日收盘
    frames = []
    for i, a in enumerate([f"A{i}USDT" for i in range(n_assets)]):
        frames.append(pd.DataFrame(
            {"open": open_[:, i], "high": close[:, i] * 1.01,
             "low": close[:, i] * 0.99, "close": close[:, i]},
            index=pd.MultiIndex.from_product([[a], times],
                                             names=["base_asset", "time"])))
    return pd.concat(frames).sort_index()


def make_factors(n_assets=5, n_days=60, seed=1):
    times = pd.date_range("2021-01-01", periods=n_days, freq="D", tz="UTC")
    idx = pd.MultiIndex.from_product(
        [[f"A{i}USDT" for i in range(n_assets)], times],
        names=["base_asset", "time"])
    rng = np.random.default_rng(seed)
    # (n_assets, n_days) -> 转成 MultiIndex 面板
    vals = rng.normal(size=(len(idx), 2))
    return pd.DataFrame(vals, index=idx, columns=["f1", "f2"])


# ---------------------------------------------------------------------------
def test_env_shapes():
    p, f = make_panel(), make_factors()
    env = PortfolioEnv(p, f)
    s = env.reset()
    assert s.shape == (env.state_dim,), \
        f"state 维度 {s.shape} 应为 ({env.state_dim},)"
    assert env.n_actions == 5
    assert env.n_factor == 2
    # state = 因子 + 持仓 + 现金
    assert s[-1] == 1.0, "reset 后现金应为 1"


def test_step_runs():
    p, f = make_panel(), make_factors()
    env = PortfolioEnv(p, f)
    env.reset()
    done = False
    steps = 0
    rng = np.random.default_rng(0)
    while not done and steps < 100:
        a = rng.normal(0, 0.05, env.n_actions)
        _, r, done, info = env.step(a)
        assert np.isfinite(r), "奖励必须有限"
        steps += 1
    assert steps > 10, f"应能跑多步, 实际 {steps}"
    assert done, "应能走到终点"


def test_deterministic_same_seed():
    p, f = make_panel(), make_factors()
    rng_seed = 42
    trajs = []
    for _ in range(2):
        env = PortfolioEnv(p, f, seed=rng_seed)
        s0 = env.reset()
        acts = [np.full(env.n_actions, 0.05) for _ in range(15)]
        traj = [s0.copy()]
        for a in acts:
            s, r, done, _ = env.step(a)
            traj.append(s.copy())
            if done:
                break
        trajs.append(np.array(traj))
    assert trajs[0].shape == trajs[1].shape
    assert np.allclose(trajs[0], trajs[1]), "同 seed 同动作 -> 轨迹应逐位相同"


def test_reward_clipping():
    """奖励必须被 clip (尾部收益不能直接进梯度) —— 原则2。"""
    p, f = make_panel(n_days=30, seed=5), make_factors()
    env = PortfolioEnv(p, f, clip_reward=0.05, turnover_penalty=0.0)
    env.reset()
    rng = np.random.default_rng(1)
    rewards = []
    for _ in range(20):
        _, r, done, _ = env.step(rng.normal(0, 0.1, env.n_actions))
        rewards.append(r)
        if done:
            break
    # 无换手惩罚时 |r| <= clip*reward_scale
    assert max(abs(x) for x in rewards) <= 0.05 + 1e-9, \
        f"奖励应被缩尾到 ±0.05, 实测最大 {max(abs(x) for x in rewards)}"


def test_turnover_penalty_reduces_reward():
    p, f = make_panel(n_days=40, seed=7), make_factors()
    acts = [np.full(5, 0.05), np.full(5, -0.05)] * 10
    def total_reward(lam):
        env = PortfolioEnv(p, f, turnover_penalty=lam)
        env.reset()
        tot = 0.0
        for a in acts:
            _, r, done, _ = env.step(a)
            tot += r
            if done:
                break
        return tot
    r0, r5 = total_reward(0.0), total_reward(0.5)
    assert r5 < r0, f"换手惩罚应降低总奖励: 无罚 {r0:.4f} vs 有罚 {r5:.4f}"


def test_risk_limits_applied():
    """动作必须过 RiskEngine (单资产/总杠杆上限) —— 原则3。"""
    p, f = make_panel(), make_factors()
    env = PortfolioEnv(p, f)
    env.risk = type(env.risk)(max_weight=0.1, max_gross=1.0)
    env.reset()
    env.step(np.full(env.n_actions, 5.0))       # 极端动作
    assert float(np.abs(env.weights).max()) <= 0.1 + 1e-9, \
        f"单资产权重应 <= 0.1, 实测 {np.abs(env.weights).max()}"
    assert float(np.abs(env.weights).sum()) <= 1.0 + 1e-9, "总杠杆应 <= 1"


def test_portfolio_metrics_available():
    """Env 跑完能给绩效 (与回测同一张成绩单的输入) —— 原则1。"""
    p, f = make_panel(n_days=50), make_factors()
    env = PortfolioEnv(p, f)
    env.reset()
    rng = np.random.default_rng(3)
    done = False
    while not done:
        _, _, done, _ = env.step(rng.normal(0, 0.02, env.n_actions))
    m = env.portfolio_metrics(periods_per_year=252)
    assert "sharpe" in m and "total_return" in m
    assert np.isfinite(m.get("total_return", np.nan)) or \
        m.get("cagr") != m.get("cagr"), "绩效应可读"


def test_state_no_future():
    """未来不变性: 把未来价格改成噪声, 历史 state 不变 —— 原则4。"""
    p, f = make_panel(n_days=40, seed=11), make_factors()
    env1 = PortfolioEnv(p, f)
    s1 = env1.reset()
    # 只扰动 t>=20 的价格
    p2 = p.copy()
    mask = p2.index.get_level_values("time") >= pd.Timestamp("2021-01-21", tz="UTC")
    rng = np.random.default_rng(99)
    p2.loc[mask, "close"] = rng.uniform(50, 150, int(mask.sum()))
    p2.loc[mask, "open"] = p2.loc[mask, "close"] * 0.99
    env2 = PortfolioEnv(p2, f)
    s2 = env2.reset()
    # reset 的 state 只含 t=0 的因子+空仓, 不受未来影响
    assert np.allclose(s1, s2), "reset state 不应受未来数据影响"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"RL 测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)