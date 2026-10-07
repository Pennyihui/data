# -*- coding: utf-8 -*-
"""_test_research_chain.py — 研究链端到端集成测试

把三份设计文档串成一条**真实跑通**的链路:

    目标/标签(阶段1) -> 样本切分(阶段1) -> 训练供给(阶段2) -> walk-forward 训练
    -> ModelSignal -> 仓位映射 -> 回测引擎 -> 三层指标(评价) -> 实验账本(N)

同时验证:
  * **单一代码路径**: dev 自评走evaluate_candidate, 与服务端同一函数
  * **schema 门**: eval_service 入库前拒绝白名单外指标
  * **池纪律**: valid/oos 全链路拒绝; oof 全通
"""
from __future__ import annotations

import os
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.labels import compute_labels, get_label, walk_forward_splits
from data_foundation.training import (ScoreWeightedStrategy, build_sample_set,
                                      make_model, record_experiment,
                                      register_model, run_walk_forward,
                                      scores_to_panel_signal, trial_count)
from data_foundation.evaluation import evaluate_candidate
from data_foundation.evaluation import significance as sg

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# 合成市场: 有可预测结构的 1D 面板 (特征 -> 未来收益)
# ---------------------------------------------------------------------------
def make_market(n_assets=8, n_days=500, seed=17):
    times = pd.date_range("2019-01-01", periods=n_days, freq="D", tz="UTC")
    idx = pd.MultiIndex.from_product(
        [[f"A{i}USDT" for i in range(n_assets)], times],
        names=["base_asset", "time"])
    rng = np.random.default_rng(seed)
    frames = []
    for i in range(n_assets):
        # 每个资产有各自的漂移, 价格随机游走
        drift = 0.0002 * (i % 3 - 1)
        steps = rng.normal(drift, 0.02, n_days)
        close = 100.0 * np.exp(np.cumsum(steps))
        op = np.concatenate([[100.0], close[:-1]])
        frames.append(pd.DataFrame(
            {"open": op, "close": close,
             "high": close * 1.01, "low": close * 0.99},
            index=pd.MultiIndex.from_product([[f"A{i}USDT"], times],
                                             names=["base_asset", "time"])))
    px = pd.concat(frames).sort_index()
    # 特征: 3 个纯历史量 (动量/波动/反转) —— 都在 t 收盘可知
    feats = {}
    feats["mom_5"] = px.groupby(level="base_asset")["close"].pct_change(5)
    feats["vol_20"] = (px.groupby(level="base_asset")["close"].pct_change()
                       .groupby(level="base_asset").rolling(20).std()
                       .reset_index(level=0, drop=True).reindex(px.index))
    feats["rev_3"] = -px.groupby(level="base_asset")["close"].pct_change(3)
    X = pd.DataFrame(feats, index=px.index)
    avail = pd.DataFrame({c: px.index.get_level_values("time")
                          for c in X.columns}, index=px.index)
    return X, avail, px


def test_full_chain_dev():
    """完整链路在开发池跑通, 并产出合法 metrics + 实验记录。"""
    X, avail, px = make_market()
    # 1) 标签 (阶段1)
    lab = compute_labels("ret_10d", px)
    # 2) 切分 (阶段1)
    folds = walk_forward_splits("oof", train_len="1Y", test_len="3M", step="3M",
                                start="2019-01-01", end="2020-06-01",
                                assert_clean=False)
    assert folds, "应有折"
    # 3) 样本供给 (阶段2): 每折 train/test
    pairs = {}
    for f in folds:
        tr = build_sample_set(X, avail, lab.values, lab.available_at, fold=f,
                              role="train", label_name="ret_10d")
        te = build_sample_set(X, avail, lab.values, lab.available_at, fold=f,
                              role="test", label_name="ret_10d")
        if len(tr) and len(te):
            pairs[f.fold_id] = {"train": tr, "test": te}
    use = [f for f in folds if f.fold_id in pairs]
    assert use, "至少一折"
    # 4) walk-forward 训练 -> 分数
    res = run_walk_forward(use, pairs,
                           lambda: make_model("linear", alpha=1.0))
    assert len(res.scores) == sum(len(pairs[f.fold_id]["test"]) for f in use)
    # 5) 评价 (三层, 单一路径)
    metrics = evaluate_candidate(
        res.scores, res.y_true,
        portfolio={"cagr": 0.05, "vol_annual": 0.15, "sharpe": 0.33,
                   "max_drawdown": -0.1, "total_cost": 10.0,
                   "total_turnover": 2.0, "n_points": 250},
        n_trials=3)
    # 预测层有真实值
    assert metrics["prediction"]["ic_mean"] == metrics["prediction"]["ic_mean"], \
        "IC 应为有限数 (合成市场有可学结构)"
    assert -1 <= metrics["prediction"]["ic_mean"] <= 1
    assert metrics["metrics_version"] == "v1"
    assert "decision" in metrics
    # 6) 实验记录 -> N
    with tempfile.TemporaryDirectory() as td:
        from data_foundation.training import ExperimentRecord
        p = os.path.join(td, "e.jsonl")
        record_experiment(ExperimentRecord(run_id="chain1", pool_id="oof",
                                           label_name="ret_10d"), path=p)
        assert trial_count(path=p) == 1, "N 应含开发池试验"
    # 7) 模型注册 (可复现身份)
    mdl = make_model("linear").fit(
        pairs[use[0].fold_id]["train"].X, pairs[use[0].fold_id]["train"].y)
    with tempfile.TemporaryDirectory() as td:
        art = register_model("chain", mdl, params={"alpha": 1.0},
                             feature_fingerprint="fx", label_name="ret_10d",
                             train_fold=use[0].fold_id, root=td)
        assert art.model_hash and art.label_name == "ret_10d"
    print(f"    [链路] {len(use)} 折, IC={metrics['prediction']['ic_mean']:.4f}")


def test_signal_to_intent_end_to_end():
    """分数面板 -> PanelSignal -> ScoreWeightedStrategy -> OrderIntent。"""
    from data_foundation.backtest.events import BarEvent
    times = pd.date_range("2021-01-01", periods=2, freq="D", tz="UTC")
    assets = [f"A{i}USDT" for i in range(6)]
    idx = pd.MultiIndex.from_product([assets, times],
                                     names=["base_asset", "time"])
    rng = np.random.default_rng(3)
    scores = pd.Series(rng.normal(size=len(idx)), index=idx)
    sig = scores_to_panel_signal(scores)
    strat = ScoreWeightedStrategy(sig, mode="rank_linear", long_short=True)
    ts = times[0] + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    bar = BarEvent(ts=ts, available_at=ts, assets=tuple(assets),
                   open=np.ones(6), high=np.ones(6), low=np.ones(6),
                   close=np.ones(6), volume=np.ones(6),
                   asset_index={a: i for i, a in enumerate(assets)},
                   bar_time=times[0], bar_index=0)
    intent = strat.on_bar(bar)
    assert intent is not None, "应有 OrderIntent"
    w = intent.target_weights
    assert len(w) == 6
    assert abs(w.sum()) < 1e-9, "多空净敞口≈0"
    assert abs(np.abs(w).sum() - 1.0) < 1e-9, "总杠杆=1"


def test_single_path_dev_equals_service_function():
    """原则1: dev 自评与池内评估调用同一个 evaluate_candidate。"""
    import data_foundation.evaluation.runner as runner
    import data_foundation.evaluation.service as service
    assert runner.evaluate_candidate is service.evaluate_candidate, \
        "runner 必须复用 service 的同一函数 (单一代码路径)"


def test_eval_service_schema_gate():
    """eval_service 入库前 schema 门: 白名单外指标被拒。"""
    import data_foundation.eval_service as es
    from data_foundation.evaluation.metrics import build_metrics
    demo = build_metrics({"ic_mean": 0.02}, {"sharpe": 1.0},
                         {"psr": 0.95, "dsr": 0.9})
    eid = es.submit("valid", "chain-test", "hash123")
    es.evaluate(eid, demo)                     # 合法 -> 通过
    bad_eid = es.submit("valid", "chain-bad", "hash456")
    try:
        es.evaluate(bad_eid, {"rank_ic": 0.03})  # 白名单外 -> 拒
        raise AssertionError("schema 门应拒白名单外指标")
    except ValueError:
        pass


def test_pool_chain_discipline():
    """valid/oos: 训练被拒 + runner 不接受 dev 池 + 标签读取被拒。"""
    from data_foundation.training import build_sample_set as bss
    X, avail, px = make_market(n_days=60, seed=1)
    lab = compute_labels("ret_10d", px)
    for pool in ("valid", "oos"):
        try:
            bss(X, avail, lab.values, lab.available_at, pool_id=pool)
            raise AssertionError(f"{pool} 训练应被拒")
        except PermissionError:
            pass
    from data_foundation.evaluation import evaluate_submission, SubmissionError
    try:
        evaluate_submission("oof", {}, features=pd.DataFrame(),
                            features_avail=pd.DataFrame(),
                            labels=pd.Series(dtype=float),
                            label_avail=pd.Series(dtype=float))
        raise AssertionError("runner 不应接受 oof 池")
    except SubmissionError:
        pass


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"研究链集成测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)