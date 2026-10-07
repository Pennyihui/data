# -*- coding: utf-8 -*-
"""evaluation/ — 评价方法 (三层指标电池 + 显著性 + 单一执行路径)

设计: docs/evaluation-protocol-design.md (v0.1)

现状 (设计文档 §0): eval_service 之前只有通道 (谁能评/谁能看/看几次), 评价算法
由调用方注入 —— 等于没有评价方法。本包补齐算法本体:

  metrics.py       三层指标电池 (预测/交易/显著性) + schema 校验
  significance.py  PSR / DSR (多重检验修正, Bailey & López de Prado 2014)
  service.py       evaluate_candidate(): dev 自评与 valid/oos 服务评的**同一入口**
  runner.py        池内评估编排 (服务端: 校验载荷 -> 应用模型 -> 三层指标)
"""
from .metrics import (METRICS_VERSION, build_metrics, ic_series,
                      prediction_metrics, rank_ic_series, trading_metrics,
                      validate_metrics, walkforward_stability)
from .service import evaluate_candidate
from .runner import SubmissionError, evaluate_submission
from .significance import (deflated_sharpe_ratio, expected_max_sr,
                           go_no_go, probabilistic_sharpe_ratio,
                           significance_metrics)

__all__ = [
    "METRICS_VERSION", "prediction_metrics", "trading_metrics",
    "walkforward_stability", "build_metrics", "validate_metrics",
    "ic_series", "rank_ic_series",
    "probabilistic_sharpe_ratio", "expected_max_sr", "deflated_sharpe_ratio",
    "significance_metrics", "go_no_go",
    "evaluate_candidate", "evaluate_submission", "SubmissionError",
]