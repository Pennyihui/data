# -*- coding: utf-8 -*-
"""dataset.py — 训练数据供给 (X⋈y 全链唯一对齐处)

设计: docs/supervised-learning-protocol-design.md §4

原则1: **X 与 y 只在这里对齐**, 对齐处带双断言。其它模块拿不到"已对齐样本" ——
要么原始 X (特征面板), 要么原始 y (标签面板)。

双断言 (泄漏重灾区的执行保证)
----------------------------
  A1: X 的 data_available_at ≤ 决策时刻 t   (特征在决策时已知)
  A2: y 的 label_available_at > t 且 ≤ 训练窗口末 (标签是未来函数; 且其
      可知时刻必须落在训练期内 —— 否则训练集见过"训练期之外"的未来)

NaN 策略 (决策 M6): 含 NaN 的特征样本**丢弃** (填充属有状态预处理, 随模型
携带, 不在供给层做)。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..labels.splitter import Fold
from ..pool_registry import assert_can_read_data

__all__ = ["SampleMeta", "SampleSet", "build_sample_set", "SampleBuildError"]


class SampleBuildError(AssertionError):
    """训练样本构建失败 (泄漏 / 口径不一致 / 池不允许)。"""


@dataclass(frozen=True)
class SampleMeta:
    feature_fingerprint: str = ""
    label_fingerprint: str = ""
    label_name: str = ""
    pool_id: str = "oof"
    fold_id: str = ""
    role: str = "train"
    n_rows: int = 0
    n_features: int = 0
    n_dropped_nan: int = 0
    n_dropped_pool: int = 0


@dataclass(frozen=True)
class SampleSet:
    X: pd.DataFrame                  # (asset, t) x 特征
    y: pd.Series                     # (asset, t) -> 标签
    meta: SampleMeta
    fingerprint: str = ""
    decision_times: pd.Series = field(default=None, repr=False)

    def __len__(self):
        return len(self.X)

    def __repr__(self):
        return (f"<SampleSet {len(self.X):,} 行 x {self.X.shape[1]} 特征 "
                f"label={self.meta.label_name} pool={self.meta.pool_id}>")

    def split_by_time(self, times, train_mask) -> tuple["SampleSet", "SampleSet"]:
        return (SampleSet(self.X.loc[train_mask], self.y.loc[train_mask],
                          self.meta, self.fingerprint),
                SampleSet(self.X.loc[~train_mask], self.y.loc[~train_mask],
                          self.meta, self.fingerprint))


def _sha1(*parts) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def build_sample_set(features: pd.DataFrame, features_avail: pd.DataFrame,
                     labels: pd.Series, label_avail: pd.Series, *,
                     label_name: str = "", pool_id: str = "oof",
                     feature_fingerprint: str = "", label_fingerprint: str = "",
                     fold: Fold | None = None, role: str = "train",
                     dropna: bool = True,
                     allow_protected: bool = False) -> SampleSet:
    """构建训练样本集 (唯一的 X⋈y 对齐处)。

    features / features_avail : MultiIndex(asset, t) 的特征值与可用时间面板
    labels / label_avail     : MultiIndex(asset, t) 的标签值与可知时间
    fold                      : walk-forward 折
    role                      : "train" (受断言 A3 约束: 标签须在训练窗内定稿)
                               / "test"  (标签只在评估时用, 可晚于 test_end)
    pool_id                   : 训练池 (valid/oos 拒绝 —— 训练只发生在 oof)
    """
    if role not in ("train", "test"):
        raise ValueError("role ∈ {train, test}")
    # -- 池纪律 (原则5: 训练只发生在 oof) --
    if not allow_protected:
        assert_can_read_data(pool_id, "训练数据")

    names = list(features.index.names)
    if names[:2] != ["base_asset", "time"]:
        raise SampleBuildError(f"面板索引应为 (base_asset, time), 实际 {names}")
    for nm, obj in (("features", features), ("features_avail", features_avail),
                    ("labels", labels), ("label_avail", label_avail)):
        if not isinstance(obj.index, pd.MultiIndex):
            raise SampleBuildError(f"{nm} 必须是 MultiIndex 面板")
        if not obj.index.equals(features.index):
            raise SampleBuildError(f"{nm} 索引与 features 不一致 —— "
                                   f"X 与 y 必须同一决策格")
    if not features.index.is_monotonic_increasing:
        features = features.sort_index()
        features_avail = features_avail.sort_index()
        labels = labels.sort_index()
        label_avail = label_avail.sort_index()

    idx = features.index
    decision_t = pd.Series(idx.get_level_values("time"), index=idx)
    if fold is not None:
        train_mask, test_mask = fold.mask(pd.Series(decision_t.to_numpy(),
                                                    index=idx))
        mask = train_mask if role == "train" else test_mask
    else:
        mask = pd.Series(True, index=idx)
        train_mask = mask

    # ---- 断言 A1: 特征可用时间 ≤ 决策时刻 (avail 是多列面板 -> 逐列比较) ----
    dec = decision_t.reindex(features_avail.index)
    a1_any = pd.Series(False, index=features_avail.index)
    for _c in features_avail.columns:
        col = features_avail[_c]
        a1_any |= col.notna() & (col > dec)
    if bool(a1_any.any()):
        r = features_avail.loc[a1_any].iloc[:, 0].iloc[0]
        t_r = dec.loc[a1_any].iloc[0]
        raise SampleBuildError(
            f"断言A1 失败(特征前视泄漏): {int(a1_any.sum())} 行的 "
            f"data_available_at 晚于决策时刻 (例: avail={r} > t={t_r}) —— "
            f"特征侧 PIT 被绕过")
    # ---- 断言 A2: 标签可知时刻 > 决策时刻 (标签是未来函数) ----
    dec_l = decision_t.reindex(label_avail.index)
    a2_bad = label_avail.notna() & (label_avail <= dec_l)
    if bool(a2_bad.any()):
        raise SampleBuildError(
            f"断言A2 失败(标签提前可知): {int(a2_bad.sum())} 行的 label_available_at "
            f"不晚于决策时刻 —— 标签不应在决策时已知")
    # ---- 断言 A3: **训练**样本的标签须在测试期开始前定稿 ----
    # 这是"训练集没见过测试期未来"的执行保证: 训练样本的 label_available_at
    # 必须 <= test_start。fold 的 purge+embargo 已保证 train_end 留出 H+1+E 的
    # 余量, 但**在这里再验一次** —— 因为样本可能被下游传错 fold。
    # test 角色的标签只在评估时消费 (它是测试期的已实现结果, 天然晚于
    # test_start), 因此 A3 只对训练集生效。
    if fold is not None and role == "train":
        late = (label_avail.notna() & (label_avail > fold.test_start)
                & mask.reindex(label_avail.index, fill_value=False))
        if bool(late.any()):
            raise SampleBuildError(
                f"断言A3 失败(标签伸进测试期): {int(late.sum())} 个训练样本的标签在 "
                f"测试起点 {fold.test_start.date()} 之后才可知 —— 训练集见过测试期"
                f"的未来收益 (fold 的 purge+embargo 不足或传错 fold)")

    y = labels.where(mask)
    la = label_avail.where(mask)
    n_pool_drop = int(len(idx) - int(mask.sum()))
    if dropna:
        keep = y.notna() & np.isfinite(y.to_numpy(dtype=float)) & \
            ~pd.isna(la) & features.notna().all(axis=1)
    else:
        keep = y.notna()
    n_nan_drop = int(keep.size - keep.sum())
    X, ys, las = features[keep], y[keep], la[keep]
    meta = SampleMeta(feature_fingerprint=feature_fingerprint,
                      label_fingerprint=label_fingerprint,
                      label_name=label_name or "", pool_id=pool_id,
                      fold_id=fold.fold_id if fold is not None else "",
                      role=role, n_rows=int(len(X)),
                      n_features=int(X.shape[1]),
                      n_dropped_nan=n_nan_drop, n_dropped_pool=n_pool_drop)
    fp = _sha1(meta.feature_fingerprint, meta.label_fingerprint, pool_id,
               meta.fold_id, role, X.shape[1], meta.n_rows)
    return SampleSet(X=X, y=ys, meta=meta, fingerprint=fp,
                     decision_times=pd.Series(
                         X.index.get_level_values("time"), index=X.index))


if __name__ == "__main__":  # pragma: no cover
    import json
    idx = pd.MultiIndex.from_product(
        [["A", "B"], pd.date_range("2021-01-01", periods=5, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    f = pd.DataFrame({"x": np.arange(len(idx), dtype=float)}, index=idx)
    fa = pd.DataFrame({"x": idx.get_level_values("time")}, index=idx)
    y = pd.Series(np.arange(len(idx), dtype=float), index=idx)
    la = pd.Series(idx.get_level_values("time") + pd.Timedelta(days=1),
                   index=idx)
    ss = build_sample_set(f, fa, y, la, label_name="ret_x", pool_id="oof")
    print(json.dumps({"rows": ss.meta.n_rows, "fp": ss.fingerprint[:8]},
                     ensure_ascii=False))
    # 断言A1: 让特征可用时间晚于决策时刻
    bad = fa + pd.Timedelta(days=2)
    try:
        build_sample_set(f, bad, y, la)
    except SampleBuildError as e:
        print("[自检] A1 捕获:", str(e)[:50])