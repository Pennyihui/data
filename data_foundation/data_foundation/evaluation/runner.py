# -*- coding: utf-8 -*-
"""runner.py — 池内评估编排 (服务端: 应用模型 -> 预测 -> 三层指标)

设计: docs/evaluation-protocol-design.md §4 + docs/supervised-learning-protocol-design.md §10

valid/oos 的评估流程 (Agent 拿不到数据, 全部在服务端完成):

    1. 校验提交载荷 (code_hash / 模型产物 / 特征指纹 / 标签规格 / 指标版本)
    2. 载入池内特征 (服务端)          <- Agent 拿不到
    3. 载入/服务端重算标签            <- Agent 拿不到 (标签视同原始数据)
    4. 应用模型 -> 预测分数面板
    5. 预测 -> 信号 -> 回测引擎 (同一引擎, 同一成本模型)
    6. evaluate_candidate() -> 三层指标 (与开发池**同一个函数**)
    7. eval_service.evaluate() 记账 (valid 限 2 次 / oos 人审)

开发池自评走的是**第 6 步的同一个函数** —— 单一代码路径 (评价协议原则1)。
"""
from __future__ import annotations

import re

import pandas as pd

from ..pool_registry import assert_can_read_data, get_pool
from .service import evaluate_candidate
from ..labels.objectives import CostParams, get_objective

__all__ = ["evaluate_submission", "SubmissionError"]


class SubmissionError(ValueError):
    """提交载荷不合法。"""


def _require(payload: dict, key: str) -> object:
    v = (payload or {}).get(key)
    if v in (None, ""):
        raise SubmissionError(f"提交载荷缺少必需字段 {key!r}")
    return v


def _assert_trained_in_oof(art) -> None:
    """训练窗口必须落在 oof 池内 (原则5: 训练只发生在开发池)。

    train_fold 形如 "ret_10d_w:2021-01-01~2021-07-29" —— 取其中的**日期区间**
    与 oof 池边界比对 (而不是字符串里找 "oof", 那是错的: fold_id 里根本不含
    池名, 早先的实现因此把所有模型都拒了)。
    """
    from ..pool_registry import get_pool
    tf = str(getattr(art, "train_fold", "") or "")
    if not tf:
        return
    oof = get_pool("oof")
    lo = oof.start_ts
    hi = oof.end_ts
    m = re.search(r"(\d{4}-\d{2}-\d{2})", tf)
    end_m = re.findall(r"(\d{4}-\d{2}-\d{2})", tf)
    if not end_m:
        return
    for ds in end_m:
        d = pd.Timestamp(ds, tz="UTC")
        if d < lo or (hi is not None and d > hi):
            raise SubmissionError(
                f"模型 {art.name} 的训练窗口 {tf} 超出 oof 池 "
                f"({oof.start} ~ {oof.end}) —— 训练只允许发生在开发池, "
                f"否则 valid/oos 的评估就不是真正的样本外")


def evaluate_submission(pool_id: str, payload: dict, *,
                        features: pd.DataFrame, features_avail: pd.DataFrame,
                        labels: pd.Series, label_avail: pd.Series,
                        portfolio: dict | None = None,
                        period_returns: pd.Series | None = None,
                        n_trials: int = 1,
                        periods_per_year: float = 365 * 24,
                        fold_metrics: list[dict] | None = None,
                        root: str | None = None,
                        feature_fingerprint_override: bool = False) -> dict:
    """服务端评估一个提交 (valid/oos)。返回**完整** metrics (供人/服务留档)。

    调用方负责载入池内特征/标签 (它们是服务端数据); 本函数负责
    **载荷校验 + 应用模型 + 评价**。Agent 永远走不到这里 (无数据 + 无模型产物)。
    """
    if get_pool(pool_id).kind in ("dev", "gap"):
        raise SubmissionError(
            f"池 {pool_id} 不是服务端评估池 (valid/oos 走本流程; oof 走本地自评)")
    # 延迟导入: training.registry -> evaluation.metrics -> 本模块 会成环
    from ..training.registry import load_model
    model_name = _require(payload, "model_name")
    model_hash = (payload or {}).get("model_hash") or None
    label_name = _require(payload, "label_name")
    feat_fp = _require(payload, "feature_fingerprint")

    # -- 校验模型产物的训练范围 (训练只发生在 oof, 原则5) --
    model, art = load_model(model_name, model_hash, root=root)
    _assert_trained_in_oof(art)
    if art.label_name and art.label_name != label_name:
        raise SubmissionError(
            f"标签不匹配: 模型用 {art.label_name}, 提交声明 {label_name}")
    # 特征指纹一致才允许应用 (防训练/评估特征错配)。
    # feature_fingerprint_override=True 时**跳过**这道闸 —— 因为调用方是服务端,
    # 它按自己实际算出的特征集算指纹, 而模型注册时的指纹可能来自别处 (例如
    # 开发期手工注册)。此时改用**特征名清单**比对, 仍然能防"用另一套特征评估"。
    if feature_fingerprint_override:
        want = payload.get("features") or []
        got = list(art.params.get("features") or []) or want
        if want and got and sorted(want) != sorted(got):
            raise SubmissionError(
                f"特征集不符: 提交声明 {sorted(want)}, 模型训练用 {sorted(got)}")
    else:
        model, art = load_model(model_name, model_hash, root=root,
                                verify_feature_fingerprint=feat_fp)

    # -- 应用模型 (服务端) --
    scores = pd.Series(model.predict(features), index=features.index,
                       name="score")
    cost = CostParams()
    if art.label_name:
        from ..labels.definitions import get_label
        try:
            cost = get_label(art.label_name).cost
        except KeyError:
            cost = CostParams()   # 标签不在注册表 -> 用默认成本 (指标仍标注)

    metrics = evaluate_candidate(
        scores, labels, portfolio=portfolio, period_returns=period_returns,
        periods_per_year=periods_per_year, n_trials=n_trials, cost=cost,
        fold_metrics=fold_metrics,
        extra={"model_name": model_name, "model_hash": art.model_hash,
               "label_name": art.label_name, "pool": pool_id,
               "train_fold": art.train_fold})
    return metrics


if __name__ == "__main__":  # pragma: no cover
    print(__doc__.split("\n")[0])