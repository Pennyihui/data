# -*- coding: utf-8 -*-
"""splitter.py — 样本切分器 (purge + embargo)

设计: docs/label-system-design.md §7

切分泄漏 (结构性风险 #2)
------------------------
10 天 horizon 下, 相邻 train/test 的**标签窗口**是重叠的: t 时刻的标签要用到
t+11 才定稿。若 train 里有 t = test_start 前 5 天, 它的标签落在测试期内 ——
训练集因此"见过"验证期的未来收益。不做 purge + embargo 的交叉验证全部虚高。

规则 (决策 L5: embargo 默认 = horizon)
------------------------------------
    purge:   训练样本必须 t + H + 1 ≤ test_start      (标签窗口不进测试期)
    embargo: 再加 E 根 (默认 E = H)  ->  t ≤ test_start − H − 1 − E

    日格 H=10, E=10 -> train 最晚 test_start − 21 天
    (设计文档例: test_start=2023-10-01 -> train 最晚 2023-09-10)

与池隔离带的关系: gap1/gap2 = 14 天 ≥ H = 10 天, 池级边界自洽; 本切分器只管
**池内** walk-forward。

纪律: 本模块是 fold 掩码的**唯一产地** —— 所有实验代码消费它的 mask, 禁止手写
切分 (与"时间墙不能靠约定"同一条纪律)。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from ..pool_registry import PoolScope, get_pool
from .objectives import parse_horizon_days

__all__ = ["Fold", "walk_forward_splits", "assert_no_overlap", "splitter_fingerprint"]


def _ts(x) -> pd.Timestamp:
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _len_to_timedelta(s: str) -> pd.Timedelta:
    """'4Y'/'6M'/'3D'/'24h' -> Timedelta (年 365 天、月 30 天, 与 horizon 同约定)。"""
    s = str(s).strip()
    unit = s[-1].lower()
    n = float(s[:-1]) if len(s) > 1 else float(s)
    days = {"y": 365.0, "m": 30.0, "d": 1.0, "h": 1.0 / 24}.get(unit)
    if days is None:
        raise ValueError(f"长度格式应为 '4Y'/'6M'/'30D'/'720h', 收到 {s!r}")
    return pd.Timedelta(days=n * days)


@dataclass(frozen=True)
class Fold:
    """一个 walk-forward 折。train_end 已含 purge + embargo 裁剪。"""

    pool_id: str
    train_start: pd.Timestamp
    train_end: pd.Timestamp            # 已裁剪 (purge + embargo)
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    horizon_days: int = 10
    embargo_days: int = 10
    label: str = "trend_10d"

    @property
    def fold_id(self) -> str:
        return (f"{self.label}:{self.train_start.date()}~{self.test_end.date()}")

    def mask(self, decision_times) -> pd.Series:
        """决策时刻序列 -> (train_mask, test_mask) 两个布尔 Series。

        decision_times 可以是: Series / Index (含 MultiIndex, 取 time 层) / 时间列表。
        """
        if isinstance(decision_times, pd.Series):
            t = decision_times          # 保留 tz (不用 .values, 它会丢 tz)
        else:
            idx = pd.Index(decision_times)
            if isinstance(idx, pd.MultiIndex):
                if "time" not in (idx.names or []):
                    raise ValueError("MultiIndex 必须含 'time' 层")
                idx = idx.get_level_values("time")
            t = pd.Series(idx, index=np.arange(len(idx)))
        train = (t >= self.train_start) & (t <= self.train_end)
        test = (t >= self.test_start) & (t <= self.test_end)
        return train, test

    def to_dict(self) -> dict:
        return {"pool_id": self.pool_id, "fold_id": self.fold_id,
                "train_start": str(self.train_start.date()),
                "train_end": str(self.train_end.date()),
                "test_start": str(self.test_start.date()),
                "test_end": str(self.test_end.date()),
                "horizon_days": self.horizon_days,
                "embargo_days": self.embargo_days}


def walk_forward_splits(pool_id: str = "oof", *, train_len: str = "3Y",
                        test_len: str = "6M", step: str = "6M",
                        horizon: str = "10D", embargo: str | None = None,
                        label: str = "trend_10d", start=None, end=None,
                        label_available_at: pd.Series | None = None,
                        assert_clean: bool = True) -> list[Fold]:
    """池内 walk-forward 折序列。

    horizon : 标签 horizon ("10D"), 决定 purge 幅度
    embargo : 默认 = horizon (决策 L5)
    label_available_at : 若给出, 每折跑 assert_no_overlap (用**真实**可用时间)
    """
    pool = get_pool(pool_id)
    if pool.kind == "gap":
        raise ValueError(f"隔离带 {pool_id!r} 不能用于研究")
    H = int(round(parse_horizon_days(horizon)))
    E = H if embargo is None else int(round(parse_horizon_days(embargo)))
    t0 = _ts(start) if start is not None else pool.start_ts
    t1 = _ts(end) if end is not None else (pool.end_ts or _ts("2026-10-01"))
    dl_train, dl_test, dl_step = (_len_to_timedelta(x)
                                  for x in (train_len, test_len, step))
    folds: list[Fold] = []
    cursor = t0
    one_day = pd.Timedelta(days=1)
    while True:
        test_start = cursor + dl_train
        # 测试窗**闭区间恰好 test_len 天**。若写成 test_start + dl_test, 当
        # step == test_len 时相邻两折会共享边界日 -> 同一个决策格被预测两次
        # (下游 unstack/reshape 直接崩, 且评估会被重复计数)。
        test_end = test_start + dl_test - one_day
        train_end = test_start - pd.Timedelta(days=H + 1 + E)
        if train_end < t0 or test_start >= t1:
            break
        f = Fold(pool_id=pool_id, train_start=max(t0, cursor),
                 train_end=train_end, test_start=test_start,
                 test_end=min(test_end, t1), horizon_days=H, embargo_days=E,
                 label=label)
        folds.append(f)
        if test_end >= t1:
            break
        cursor = cursor + dl_step
    if assert_clean and label_available_at is not None:
        for f in folds:
            assert_no_overlap(f, label_available_at)
    return folds


def assert_no_overlap(fold: Fold, label_available_at: pd.Series,
                      ) -> None:
    """用**真实** label_available_at 验证: 训练样本的标签何时可知。

    训练集里最晚的那个标签的可知时刻必须 <= test_start (purge+embargo 生效)。
    否则训练集见过测试期的未来收益 —— 这是切分泄漏, 抛错。
    """
    if not isinstance(label_available_at, pd.Series):
        raise TypeError("label_available_at 必须是 Series")
    times = pd.Series(label_available_at.index.get_level_values("time"),
                      index=label_available_at.index)
    train_mask, _ = fold.mask(times)
    if not bool(train_mask.any()):
        return
    avail = label_available_at[train_mask]
    latest = avail.max()
    if pd.isna(latest):
        return
    if latest > fold.test_start:
        raise AssertionError(
            f"切分泄漏 ({fold.fold_id}): 训练集最晚标签的可知时刻 {latest} "
            f"晚于测试起点 {fold.test_start} —— 训练样本的标签窗口伸进了测试期 "
            f"(purge={fold.horizon_days}d, embargo={fold.embargo_days}d 未生效?)")


def splitter_fingerprint(fold_plan: list[Fold]) -> str:
    import hashlib
    h = hashlib.sha1()
    for f in fold_plan:
        h.update(f.fold_id.encode("utf-8"))
    return h.hexdigest()


if __name__ == "__main__":  # pragma: no cover
    folds = walk_forward_splits("oof")
    for f in folds:
        print(f.to_dict())
    if folds:
        span = (folds[-1].test_start - folds[0].test_start).days
        print(f"[自检] {len(folds)} 折, 测试期跨度 {span} 天, "
              f"train_end 比 test_start 早 "
              f"{(folds[0].test_start - folds[0].train_end).days} 天 "
              f"(= H+1+embargo = 21)")