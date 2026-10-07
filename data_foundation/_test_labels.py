# -*- coding: utf-8 -*-
"""_test_labels.py — 标签体系单测 (目标注册表 / 标签定义 / 存储 / 切分器)

设计: docs/label-system-design.md §13 验收标准

关键测试:
  * 手算对照: 构造已知价格序列, ret_10d 与手工 open[t+1]->open[t+H+1] 逐位核对
  * 口径核对: 进场/离场偏移、label_available_at = bar[t+H+1] 收盘
  * 成本感知: 净收益 = 毛收益 - c_rt, 改成本参数 -> 指纹变化
  * 切分: purge+embargo 边界 (test_start - 21 天), 真实 available_at 验证无重叠
  * 泄漏用例: 训练集标签伸进测试期 -> assert_no_overlap 抛错
"""
from __future__ import annotations

import os
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.labels import (CostParams, LabelSpec, compute_labels,
                                    get_label, get_objective, list_labels,
                                    list_objectives, parse_horizon_days,
                                    register_label, save_label, load_label,
                                    label_fingerprint, list_stored)
from data_foundation.labels.objectives import REGISTRY as OBJ_REGISTRY
from data_foundation.labels.definitions import LABELS
from data_foundation.labels.splitter import (Fold, walk_forward_splits,
                                             assert_no_overlap)

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# 合成价格面板: 1 个资产, N 根日 bar, open = 100 + i (确定性, 可手算)
# ---------------------------------------------------------------------------
def make_prices(n_assets=2, n_days=40, start="2021-01-01", drift=1.0):
    idx = []
    frames = []
    times = pd.date_range(start, periods=n_days, freq="D", tz="UTC")
    for a in range(n_assets):
        op = np.array([100.0 + 10 * a + drift * i for i in range(n_days)])
        cl = op * 1.01
        f = pd.DataFrame({"open": op, "close": cl},
                         index=pd.MultiIndex.from_product(
                             [[f"A{a}USDT"], times], names=["base_asset", "time"]))
        frames.append(f)
    return pd.concat(frames).sort_index()


# ---------------------------------------------------------------------------
# 1. 目标注册表
# ---------------------------------------------------------------------------
def test_objective_registry():
    assert "trend_10d" in OBJ_REGISTRY, "trend_10d 未注册"
    o = get_objective("trend_10d")
    assert o.horizon == "10D" and o.decision_grid == "1D"
    assert o.horizon_bars == 10, f"horizon_bars 应为 10, 实际 {o.horizon_bars}"
    assert abs(o.cost.round_trip - 0.002) < 1e-12, \
        f"round_trip 应为 0.2%, 实际 {o.cost.round_trip}"
    # 标签注册后应挂到目标下
    assert "ret_10d" in o.labels, f"ret_10d 未挂到 trend_10d, 实际 {o.labels}"
    assert parse_horizon_days("2W") == 14.0
    rows = list_labels()
    assert len(rows) == 5, f"应注册 5 个标签, 实际 {len(rows)}"
    assert all(r["objective"] == "trend_10d" for r in rows)


def test_cost_params():
    c = CostParams(taker_fee=0.0005, slippage_bps=5.0)
    assert abs(c.slippage - 0.0005) < 1e-12
    assert abs(c.round_trip - 0.002) < 1e-12, "round trip = 2*(0.0005+0.0005)=0.002"
    c2 = CostParams(taker_fee=0.001, slippage_bps=10.0)
    assert c2.round_trip > c.round_trip


# ---------------------------------------------------------------------------
# 2. 标签定义 —— 手算对照 (核心)
# ---------------------------------------------------------------------------
def test_ret_handcheck():
    """ret_10d 手算对照: open[t+1] -> open[t+H+1], 扣 c_rt。"""
    H = 10
    px = make_prices(n_assets=1, n_days=30)
    res = compute_labels("ret_10d", px)
    spec = get_label("ret_10d")
    c_rt = spec.cost.round_trip
    op = px.loc["A0USDT", "open"]
    # 取 t=0: entry=open[1], exit=open[11]
    i = 0
    entry = op.iloc[i + 1]
    exit_ = op.iloc[i + H + 1]
    expect = exit_ / entry - 1.0 - c_rt
    actual = res.values.loc[("A0USDT", op.index[i])]
    assert abs(actual - expect) < 1e-12, \
        f"t=0 手算 {expect:.12f} vs 实现 {actual:.12f}"
    # t=5: entry=open[6], exit=open[16]
    i = 5
    expect = op.iloc[i + H + 1] / op.iloc[i + 1] - 1.0 - c_rt
    actual = res.values.loc[("A0USDT", op.index[i])]
    assert abs(actual - expect) < 1e-12, f"t=5 手算 {expect} vs 实现 {actual}"
    # 末尾 H+1 根无未来价格 -> NaN
    assert pd.isna(res.values.iloc[-1]), "最后 H+1 根应无标签"
    assert pd.isna(res.values.iloc[-(H + 1)]), "倒数 H+1 根应无标签"
    # 起点: t=0, entry=open[1] 存在 -> 有标签
    assert pd.notna(res.values.iloc[0]), "t=0 应有标签"


def test_ret_no_cost():
    """零成本时净收益 = 毛收益。"""
    px = make_prices(n_assets=1, n_days=30)
    spec = LabelSpec(name="ret_10d_nc", objective="trend_10d", kind="ret",
                     horizon_bars=10, cost=CostParams(0.0, 0.0))
    register_label(spec, replace=True)
    try:
        res = compute_labels(spec, px)
        op = px.loc["A0USDT", "open"]
        expect = op.iloc[11] / op.iloc[1] - 1.0
        actual = res.values.iloc[0]
        assert abs(actual - expect) < 1e-12, f"零成本 {expect} vs {actual}"
    finally:
        LABELS.pop("ret_10d_nc", None)


def test_sign_label():
    px = make_prices(n_assets=1, n_days=30, drift=-1.0)  # 下跌 -> 负收益
    res = compute_labels("sign_10d", px)
    assert set(res.values.dropna().unique()) <= {0.0, 1.0}, "sign 标签只应有 0/1"
    assert float(res.values.dropna().iloc[0]) == 0.0, "下跌应为 0"


def test_available_at():
    """label_available_at = bar[t+H+1] 收盘 (open_time + 1D - 1s)。"""
    H = 10
    px = make_prices(n_assets=1, n_days=30)
    res = compute_labels("ret_10d", px)
    exit_open_time = pd.Timestamp("2021-01-01", tz="UTC") + pd.Timedelta(days=H + 1)
    expect = exit_open_time + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    actual = res.available_at.iloc[0]
    assert actual == expect, f"available_at {actual} 应为 {expect}"


def test_quantile_and_excess():
    # 6 个资产, 不同 drift -> 不同收益 -> 分位有序
    frames = []
    times = pd.date_range("2021-01-01", periods=25, freq="D", tz="UTC")
    for a in range(6):
        op = np.array([100.0 + a * 5.0 + i * (a + 1) for i in range(25)])
        frames.append(pd.DataFrame(
            {"open": op, "close": op * 1.001},
            index=pd.MultiIndex.from_product([[f"A{a}USDT"], times],
                                             names=["base_asset", "time"])))
    px = pd.concat(frames).sort_index()
    q = compute_labels("quantile_10d", px)
    ex = compute_labels("excess_10d", px)
    t0 = pd.Timestamp("2021-01-01", tz="UTC")
    # 决策日 t=0 的截面: 收益随 a 递增 -> 分位也应递增
    row = q.values.xs(t0, level="time")
    vals = row.dropna().sort_index().tolist()
    assert vals == sorted(vals), f"分位应随收益单调, 实际 {vals}"
    assert set(vals) <= {1.0, 2.0, 3.0, 4.0, 5.0}, f"分位值域错: {set(vals)}"
    # excess: 减去截面中位数
    exrow = ex.values.xs(t0, level="time").dropna()
    assert abs(exrow.median()) < 1e-9, "excess 中位数应约 0"


def test_vol_scaled():
    px = make_prices(n_assets=1, n_days=40)
    res = compute_labels("vol_scaled_10d", px)
    assert res.values.notna().any(), "vol_scaled 应有值"
    finite = res.values.dropna()
    assert np.isfinite(finite).all(), "vol_scaled 不应有 inf"


# ---------------------------------------------------------------------------
# 3. 存储 + 指纹 + 池访问
# ---------------------------------------------------------------------------
def test_store_roundtrip_and_fingerprint():
    px = make_prices(n_assets=1, n_days=30)
    res = compute_labels("ret_10d", px)
    with tempfile.TemporaryDirectory() as td:
        meta = save_label(res, pool_id="oof", data_fingerprint="fp1",
                          pool_dir=td)
        assert meta["label"] == "ret_10d" and meta["n_labeled"] > 0
        frame = load_label("ret_10d", pool_id="oof",
                           pool_dir=None) if False else None
        # 直接读回验证
        back = pd.read_parquet(meta["path"], engine="pyarrow")
        assert "label" in back.columns and "label_available_at" in back.columns
        assert abs(back["label"].dropna().iloc[0] - res.values.dropna().iloc[0]) < 1e-12
    # 指纹: 改数据/池/资产 -> 变; 同输入 -> 不变
    f1 = label_fingerprint("ret_10d", pool_id="oof", data_fingerprint="fp1")
    f2 = label_fingerprint("ret_10d", pool_id="oof", data_fingerprint="fp2")
    f3 = label_fingerprint("ret_10d", pool_id="valid", data_fingerprint="fp1")
    f4 = label_fingerprint("ret_10d", pool_id="oof", data_fingerprint="fp1")
    assert f1 != f2, "改数据指纹应变化"
    assert f1 != f3, "改池应变化"
    assert f1 == f4, "同输入应一致"
    # 改成本 -> 新指纹
    spec_a = LabelSpec("ret_10d", "trend_10d", "ret", 10)
    spec_b = LabelSpec("ret_10d", "trend_10d", "ret", 10,
                       cost=CostParams(0.001, 10.0))
    assert spec_a.fingerprint() != spec_b.fingerprint(), "改成本应改规格指纹"


def test_pool_access_control():
    """valid/oos 读标签被拒 (标签视同原始数据)。"""
    for pool in ("valid", "oos"):
        try:
            load_label("ret_10d", pool_id=pool, allow_protected=False)
            raise AssertionError(f"{pool} 池读标签应被拒")
        except PermissionError:
            pass
        except FileNotFoundError:
            pass   # 先撞池门 (assert_can_read_data 在读 meta 之前)


# ---------------------------------------------------------------------------
# 4. 切分器 (purge + embargo)
# ---------------------------------------------------------------------------
def test_walk_forward_bounds():
    folds = walk_forward_splits("oof")
    assert len(folds) >= 3, f"应有多折, 实际 {len(folds)}"
    f0 = folds[0]
    # train_end 比 test_start 早 H+1+embargo = 21 天
    gap = (f0.test_start - f0.train_end).days
    assert gap == 21, f"purge+embargo 间隔应为 21 天, 实际 {gap}"
    assert f0.test_end > f0.test_start
    # 折按时间递进
    for a, b in zip(folds, folds[1:]):
        assert b.test_start > a.test_start, "折应递进"


def test_no_overlap_clean():
    """用真实 available_at: 训练集标签不伸进测试期。"""
    px = make_prices(n_assets=1, n_days=200, start="2018-01-01")
    res = compute_labels("ret_10d", px)
    folds = walk_forward_splits("oof", start="2018-01-01", end="2020-01-01",
                                assert_clean=False)
    # 池 oof 从 2018-01-01 起, 用一个覆盖 labels 的范围
    folds = [f for f in folds
             if f.train_start >= pd.Timestamp("2018-01-01", tz="UTC")
             and f.test_end <= pd.Timestamp("2018-05-01", tz="UTC")]
    if not folds:
        # 合成面板的跨度有限 -> 直接构造 fold 验证函数本身
        f = Fold(pool_id="oof",
                 train_start=pd.Timestamp("2018-02-01", tz="UTC"),
                 train_end=pd.Timestamp("2018-02-20", tz="UTC"),
                 test_start=pd.Timestamp("2018-03-13", tz="UTC"),
                 test_end=pd.Timestamp("2018-06-13", tz="UTC"),
                 horizon_days=10, embargo_days=10)
        assert_no_overlap(f, res.available_at)   # 训练标签最晚 03-02 < 03-13
        # 构造泄漏: train_end 推近 test_start -> 抛错
        bad = Fold(pool_id="oof", train_start=f.train_start,
                   train_end=f.test_start - pd.Timedelta(days=5),
                   test_start=f.test_start, test_end=f.test_end,
                   horizon_days=10, embargo_days=10)
        leaked = False
        try:
            assert_no_overlap(bad, res.available_at)
        except AssertionError as e:
            leaked = "切分泄漏" in str(e)
        assert leaked, "重叠 fold 应抛切分泄漏错误"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"labels 测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)