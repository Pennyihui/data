# -*- coding: utf-8 -*-
"""walkforward.py — walk-forward 训练框架 (消费标签体系 splitter 的 folds)

设计: docs/supervised-learning-protocol-design.md §8

纪律 (代码级):
  1. 每折: fit(train fold 样本) -> predict(test fold) -> 记录分数面板
  2. 模型在 test fold 只用对应 train fold 训练 (fold 结构由样本供给层的
     掩码 + 断言物理隔离 —— 作弊"偷看 test"的模型拿不到 test 样本)
  3. 不滚动学习 test (用 test 数据在线更新 = 泄漏)
  4. 汇总 = 各折分数拼接 -> 一次喂给回测 + 三层指标
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from ..labels.splitter import Fold
from .dataset import SampleSet

__all__ = ["FoldResult", "WalkForwardResult", "run_walk_forward"]


@dataclass
class FoldResult:
    fold_id: str
    train_rows: int
    test_rows: int
    scores: pd.Series            # **test 折**的分数面板 (asset, t)
    train_span: str = ""

    def to_dict(self) -> dict:
        return {"fold_id": self.fold_id, "train_rows": self.train_rows,
                "test_rows": self.test_rows, "train_span": self.train_span}


@dataclass
class WalkForwardResult:
    folds: list[FoldResult]
    scores: pd.Series            # 各折 test 分数拼接 (评估用)
    y_true: pd.Series            # 对应标签 (评估用)
    fold_ids: list[str]

    def __repr__(self):
        return (f"<WalkForwardResult {len(self.folds)} 折 "
                f"{len(self.scores):,} 评分点>")


def run_walk_forward(folds: list[Fold], sample_sets: dict[str, dict],
                     model_factory) -> WalkForwardResult:
    """逐折训练+预测。

    sample_sets[f.fold_id] = {"train": SampleSet, "test": SampleSet}
        train 角色受断言 A3 约束 (标签须在训练窗内定稿); test 角色用于预测/评估。

    model_factory: 无参可调用 -> 返回一个**未训练**的模型。
    """
    if not folds:
        raise ValueError("folds 为空")
    fold_results: list[FoldResult] = []
    all_scores, all_y, fold_ids = [], [], []
    for f in folds:
        if f.fold_id not in sample_sets:
            raise KeyError(f"缺少折 {f.fold_id} 的样本集 "
                           f"(可用: {sorted(sample_sets)})")
        pair = sample_sets[f.fold_id]
        if not isinstance(pair, dict) or "train" not in pair or "test" not in pair:
            raise ValueError(f"折 {f.fold_id} 的样本集必须是 "
                             f"{{'train': SampleSet, 'test': SampleSet}}")
        tr, te = pair["train"], pair["test"]
        if len(tr) == 0:
            raise ValueError(f"折 {f.fold_id} 训练集为空 —— 检查 purge 幅度/池范围")
        model = model_factory()
        model.fit(tr.X, tr.y)                       # 只在 train 折上训练
        pred = pd.Series(model.predict(te.X),
                         index=te.X.index, name="score")
        fold_results.append(FoldResult(
            fold_id=f.fold_id, train_rows=len(tr.X), test_rows=len(te.X),
            scores=pred,
            train_span=f"{f.train_start.date()}~{f.train_end.date()}"))
        all_scores.append(pred)
        all_y.append(te.y)
        fold_ids.append(f.fold_id)
    return WalkForwardResult(
        folds=fold_results,
        scores=pd.concat(all_scores) if all_scores else pd.Series(dtype=float),
        y_true=pd.concat(all_y) if all_y else pd.Series(dtype=float),
        fold_ids=fold_ids)


if __name__ == "__main__":  # pragma: no cover
    print(__doc__.split("\n")[0])