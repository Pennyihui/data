# -*- coding: utf-8 -*-
"""_test_training.py — 监督学习协议单测

设计: docs/supervised-learning-protocol-design.md §15 验收标准

核心:
  * 断言 A1/A2/A3 (特征前视 / 标签提前可知 / 标签伸进测试期) 都能拦下
  * 有状态预处理: predict 不得重拟合 (保存参数 vs 重拟合数值不同)
  * 可复现: 同 seed 同指纹 -> 同 model_hash; 同输入 -> 逐位相同预测
  * walk-forward: 训练只用 train 折, 预测只落在 test 折
  * 三池: valid/oos 训练被拒
  * 端到端: 分数 -> PanelSignal -> ScoreWeightedStrategy -> OrderIntent
"""
from __future__ import annotations

import os
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.training import (ExperimentRecord, SampleBuildError,
                                      ScoreWeightedStrategy, build_sample_set,
                                      list_models, load_model, make_model,
                                      record_experiment, register_model,
                                      run_walk_forward, scores_to_panel_signal,
                                      trial_count)
from data_foundation.labels.splitter import Fold, walk_forward_splits
from data_foundation.backtest.events import BarEvent

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# 合成面板
# ---------------------------------------------------------------------------
def make_panels(n_assets=4, n_days=120, seed=0):
    idx = pd.MultiIndex.from_product(
        [[f"A{i}USDT" for i in range(n_assets)],
         pd.date_range("2021-01-01", periods=n_days, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    rng = np.random.default_rng(seed)
    X = pd.DataFrame(rng.normal(size=(len(idx), 3)), index=idx,
                     columns=["f1", "f2", "f3"])
    avail = pd.DataFrame(
        {c: idx.get_level_values("time") for c in X.columns}, index=idx)
    y = pd.Series(rng.normal(0.01, 0.05, len(idx)), index=idx)
    yavail = pd.Series(idx.get_level_values("time") + pd.Timedelta(days=11),
                       index=idx)
    return X, avail, y, yavail


def _fold():
    return Fold(pool_id="oof",
                train_start=pd.Timestamp("2021-01-01", tz="UTC"),
                train_end=pd.Timestamp("2021-03-01", tz="UTC"),
                test_start=pd.Timestamp("2021-03-22", tz="UTC"),
                test_end=pd.Timestamp("2021-04-30", tz="UTC"),
                horizon_days=10, embargo_days=10)


# ---------------------------------------------------------------------------
# 1. 断言 A1/A2/A3 (泄漏拦截)
# ---------------------------------------------------------------------------
def test_assert_a1_forward_feature_rejected():
    X, avail, y, yavail = make_panels()
    bad = avail + pd.Timedelta(days=2)      # 特征可用时间晚于决策时刻
    try:
        build_sample_set(X, bad, y, yavail, fold=_fold())
        raise AssertionError("A1 应拦下特征前视")
    except SampleBuildError as e:
        assert "A1" in str(e), f"应报 A1: {e}"


def test_assert_a2_label_early_rejected():
    X, avail, y, yavail = make_panels()
    bad = yavail - pd.Timedelta(days=30)    # 标签"提前可知"
    try:
        build_sample_set(X, avail, y, bad, fold=_fold())
        raise AssertionError("A2 应拦下标签提前可知")
    except SampleBuildError as e:
        assert "A2" in str(e), f"应报 A2: {e}"


def test_assert_a3_label_reaches_test_period_rejected():
    X, avail, y, yavail = make_panels()
    f = _fold()
    # 只把**训练窗内**的行改成"标签晚于测试起点", 让它撞 A3 而非 A2
    late = yavail.copy()
    in_train = (late.index.get_level_values("time") >= f.train_start) & \
               (late.index.get_level_values("time") <= f.train_end)
    late[in_train] = f.test_start + pd.Timedelta(days=5)
    try:
        build_sample_set(X, avail, y, late, fold=f, role="train")
        raise AssertionError("A3 应拦下标签伸进测试期")
    except SampleBuildError as e:
        assert "A3" in str(e), f"应报 A3: {e}"
    # 同一批数据在 test 角色下合法 (测试期标签本就晚于 test_start)
    ss = build_sample_set(X, avail, y, late, fold=f, role="test")
    assert len(ss) > 0


def test_clean_sample_set_builds():
    X, avail, y, yavail = make_panels()
    ss = build_sample_set(X, avail, y, yavail, fold=_fold(),
                          feature_fingerprint="fp", label_fingerprint="lp",
                          label_name="ret_10d")
    assert len(ss) > 0 and ss.X.shape[1] == 3
    assert ss.meta.pool_id == "oof" and ss.meta.role == "train"
    # 训练样本决策时刻都在 train 窗口内
    tt = ss.X.index.get_level_values("time")
    assert tt.min() >= pd.Timestamp("2021-01-01", tz="UTC")
    assert tt.max() <= pd.Timestamp("2021-03-01", tz="UTC")


def test_pool_discipline_valid_rejected():
    X, avail, y, yavail = make_panels()
    for pool in ("valid", "oos"):
        try:
            build_sample_set(X, avail, y, yavail, pool_id=pool)
            raise AssertionError(f"{pool} 池训练应被拒")
        except PermissionError:
            pass


def test_nan_rows_dropped():
    X, avail, y, yavail = make_panels()
    X = X.copy()
    X.iloc[0, 0] = np.nan
    ss = build_sample_set(X, avail, y, yavail)
    assert ss.meta.n_dropped_nan >= 1, "含 NaN 的样本应被丢弃"
    assert not ss.X.isna().any().any(), "样本集不应残留 NaN"


# ---------------------------------------------------------------------------
# 2. 模型: 预处理随模型 / 可复现 / 序列化
# ---------------------------------------------------------------------------
def test_preprocessor_travels_with_model():
    rng = np.random.default_rng(1)
    X = pd.DataFrame(rng.normal(10, 3, (300, 3)), columns=list("abc"))
    y = pd.Series(X["a"] * 0.5 + rng.normal(0, 0.2, 300))
    m = make_model("linear", alpha=1.0)
    m.fit(X, y)
    # 保存的均值来自**训练数据**
    assert abs(m.pre.mean_["a"] - X["a"].mean()) < 1e-10
    # 换一批偏移很大的数据 predict, 预处理参数不变 (未重拟合)
    X2 = X + 1000.0
    before = m.pre.mean_.copy()
    _ = m.predict(X2)
    pd.testing.assert_series_equal(m.pre.mean_, before), \
        "predict 不得重拟合预处理参数"
    # 真正重拟合会给出不同的标准化结果 (证明测试有意义)
    m2 = make_model("linear", alpha=1.0)
    m2.fit(X2, y)
    assert abs(m2.pre.mean_["a"] - X2["a"].mean()) < 1e-10
    assert abs(m.pre.mean_["a"] - m2.pre.mean_["a"]) > 1, \
        "若两者相等, 说明测试构造无效"


def test_reproducible_same_hash():
    X, avail, y, yavail = make_panels(seed=5)
    ss = build_sample_set(X, avail, y, yavail)
    m1 = make_model("linear", alpha=2.0, seed=7)
    m1.fit(ss.X, ss.y)
    m2 = make_model("linear", alpha=2.0, seed=7)
    m2.fit(ss.X, ss.y)
    p1, p2 = m1.predict(ss.X), m2.predict(ss.X)
    assert np.allclose(p1, p2), "同 seed 同数据 -> 预测应一致"
    with tempfile.TemporaryDirectory() as td:
        a1 = register_model("m", m1, params={"alpha": 2.0, "seed": 7},
                            feature_fingerprint="fp", root=td)
        a2 = register_model("m", m2, params={"alpha": 2.0, "seed": 7},
                            feature_fingerprint="fp", root=td)
        assert a1.model_hash == a2.model_hash, "同模型应同 hash (可复现)"
        assert a2.version == 1, "同 hash 复用原版本 (不可变)"
        # 反序列化后预测一致
        m3, art = load_model("m", a1.model_hash, root=td)
        assert np.allclose(m1.predict(ss.X), m3.predict(ss.X)), \
            "load_model 后预测应逐位一致"


def test_registry_fingerprint_gate():
    X, avail, y, yavail = make_panels(seed=6)
    ss = build_sample_set(X, avail, y, yavail)
    m = make_model("linear").fit(ss.X, ss.y)
    with tempfile.TemporaryDirectory() as td:
        art = register_model("m", m, params={}, feature_fingerprint="fpA", root=td)
        load_model("m", art.model_hash, root=td,
                   verify_feature_fingerprint="fpA")     # 一致 -> 通过
        try:
            load_model("m", art.model_hash, root=td,
                       verify_feature_fingerprint="fpB")
            raise AssertionError("指纹不符应拒绝应用")
        except ValueError as e:
            assert "指纹" in str(e)


def test_gbdt_and_baseline_fit():
    X, avail, y, yavail = make_panels(seed=8)
    ss = build_sample_set(X, avail, y, yavail)
    for fam in ("linear", "gbdt", "baseline"):
        m = make_model(fam)
        m.fit(ss.X, ss.y)
        p = m.predict(ss.X)
        assert p.shape[0] == len(ss.X) and np.isfinite(p).all(), \
            f"{fam} 预测应为有限值"
    # 分类标签
    yc = (ss.y > 0).astype(float)
    for fam in ("linear", "gbdt"):
        params = {"task": "classification"} if fam == "linear" else \
            {"task": "classification"}
        m = make_model(fam, **params)
        m.fit(ss.X, yc)
        p = m.predict(ss.X)
        assert ((p >= 0) & (p <= 1)).all(), "分类预测应为概率"


# ---------------------------------------------------------------------------
# 3. walk-forward: 训练/预测分离
# ---------------------------------------------------------------------------
def test_walkforward_train_test_separation():
    X, avail, y, yavail = make_panels(seed=9)
    f = _fold()
    ss_tr = build_sample_set(X, avail, y, yavail, fold=f, role="train")
    ss_te = build_sample_set(X, avail, y, yavail, fold=f, role="test")
    pair = {f.fold_id: {"train": ss_tr, "test": ss_te}}
    res = run_walk_forward([f], pair, lambda: make_model("linear"))
    assert len(res.scores) == len(ss_te), "预测点数应等于 test 样本数"
    assert res.folds[0].train_rows == len(ss_tr)
    # 预测的时间都在 test 窗内
    pt = res.scores.index.get_level_values("time")
    assert pt.min() >= f.test_start and pt.max() <= f.test_end, \
        "预测必须落在 test 折内"


def test_walkforward_real_folds():
    # 400 天数据, 3 个折 (train 90d, test 30d, step 30d) —— oof 池从 2018 起,
    # 我们把范围显式限定在合成数据覆盖的 2021 段内。
    X, avail, y, yavail = make_panels(n_days=400, seed=10)
    folds = walk_forward_splits("oof", train_len="90D", test_len="30D",
                                step="30D", horizon="10D", embargo="10D",
                                start="2021-01-01", end="2021-07-01",
                                assert_clean=False)
    pairs = {}
    for f in folds:
        tr = build_sample_set(X, avail, y, yavail, fold=f, role="train")
        te = build_sample_set(X, avail, y, yavail, fold=f, role="test")
        if len(tr) and len(te):
            pairs[f.fold_id] = {"train": tr, "test": te}
    use = [f for f in folds if f.fold_id in pairs]
    assert len(use) >= 2, f"合成数据下至少应有两折可用, 实际 {len(use)}"
    res = run_walk_forward(use, pairs, lambda: make_model("linear"))
    assert len(res.fold_ids) == len(use)
    # 预测时间严格落在各折 test 窗内
    for fr in res.folds:
        f = next(x for x in use if x.fold_id == fr.fold_id)
        pt = fr.scores.index.get_level_values("time")
        assert pt.min() >= f.test_start and pt.max() <= f.test_end


# ---------------------------------------------------------------------------
# 4. ModelSignal + 仓位映射
# ---------------------------------------------------------------------------
def _bar(n=5, time="2021-01-01"):
    """构造一根 BarEvent (与 events.BarEvent 的真实字段一致)。"""
    ts = pd.Timestamp(time, tz="UTC") + pd.Timedelta(days=1) - \
        pd.Timedelta(seconds=1)                 # 决策时刻 = 收盘
    assets = tuple(f"A{i}USDT" for i in range(n))
    return BarEvent(
        ts=ts, available_at=ts, assets=assets,
        open=np.arange(n, dtype=float) + 1.0,
        high=np.arange(n, dtype=float) + 1.0,
        low=np.arange(n, dtype=float) + 1.0,
        close=np.arange(n, dtype=float) + 1.0,
        volume=np.ones(n),
        asset_index={a: i for i, a in enumerate(assets)},
        bar_time=pd.Timestamp(time, tz="UTC"), bar_index=0)


def test_scores_to_signal():
    idx = pd.MultiIndex.from_product(
        [["A0USDT", "A1USDT"],
         [pd.Timestamp("2021-01-01", tz="UTC")]],
        names=["base_asset", "time"])
    s = pd.Series([0.1, 0.9], index=idx)
    sig = scores_to_panel_signal(s)
    bar = _bar(2)
    vals = sig.get(bar)
    assert len(vals) == 2 and vals[1] == 0.9


def test_score_weighted_topn():
    sig = lambda bar: np.array([3.0, 2.0, 1.0, 0.0])
    st = ScoreWeightedStrategy(sig, mode="top_n", top_n=2, long_short=False)
    w = st.on_bar(_bar(4)).target_weights
    assert abs(w[0] - 0.5) < 1e-12 and abs(w[1] - 0.5) < 1e-12
    assert abs(w[2]) < 1e-12 and abs(w.sum() - 1.0) < 1e-12


def test_score_weighted_rank_linear():
    sig = lambda bar: np.array([3.0, 2.0, 1.0, 0.0])
    st = ScoreWeightedStrategy(sig, mode="rank_linear", long_short=True)
    w = st.on_bar(_bar(4)).target_weights
    # 多空对称: 净敞口≈0, 总杠杆 1
    assert abs(w.sum()) < 1e-9, f"多空应净敞口≈0, 实际 {w.sum():.6f}"
    assert abs(np.abs(w).sum() - 1.0) < 1e-9
    # 分数最高的权重最大 (正)
    assert w[0] > 0 and w[-1] < 0
    # 纯多: 只有上半
    st2 = ScoreWeightedStrategy(sig, mode="rank_linear", long_short=False)
    w2 = st2.on_bar(_bar(4)).target_weights
    assert (w2 >= -1e-12).all(), "纯多不应有空头"


def test_rebalance_every():
    calls = {"n": 0}

    def sig(bar):
        calls["n"] += 1
        return np.arange(len(bar.assets), dtype=float)

    st = ScoreWeightedStrategy(sig, mode="top_n", top_n=2, rebalance_every=3)
    got = [st.on_bar(_bar(4)) for _ in range(6)]
    n_intents = sum(1 for g in got if g is not None)
    assert n_intents == 2, f"每 3 根调仓一次 -> 6 根应有 2 次, 实际 {n_intents}"


# ---------------------------------------------------------------------------
# 5. 实验追踪 (试验计数 N)
# ---------------------------------------------------------------------------
def test_trial_count_includes_oof():
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "e.jsonl")
        record_experiment(ExperimentRecord(run_id="r1", pool_id="oof"), path=p)
        record_experiment(ExperimentRecord(run_id="r1", pool_id="valid"), path=p)
        record_experiment(ExperimentRecord(run_id="r2", pool_id="oos"), path=p)
        record_experiment(ExperimentRecord(run_id="r3", pool_id="oof"), path=p)
        assert trial_count(path=p) == 3, "去重 run_id = 3"
        assert trial_count(pool_ids=("oof",), path=p) == 2, "oof 计入 N (决策E3)"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"training 测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)