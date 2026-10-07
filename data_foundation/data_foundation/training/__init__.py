# -*- coding: utf-8 -*-
"""training/ — 监督学习协议 (训练供给 → 模型注册 → 信号适配 → 实验追踪)

设计: docs/supervised-learning-protocol-design.md (v0.1)

六条原则 (设计文档 §2):
  P1 X⋈y 只在 dataset.build_sample_set 一处对齐, 双断言 (A1 特征前视 / A2 标签
     提前可知 / A3 标签伸进测试期)
  P2 有状态预处理 (标准化/填充) 参数随模型序列化; predict 禁重拟合
  P3 模型是第一类对象 (registry.ModelArtifact + 指纹 + 不可变版本)
  P4 模型与仓位映射解耦 (分数面板 -> signal_adapter); 回测引擎不动
  P5 训练只发生在 oof (build_sample_set 对 valid/oos 直接 PermissionError)
  P6 每次训练/评估留痕 (experiments), 全局试验计数 N 供给 DSR
"""
from .dataset import SampleMeta, SampleSet, SampleBuildError, build_sample_set
from .experiments import (ExperimentRecord, list_experiments, record_experiment,
                          trial_count)
from .models import (BaselineModel, BaseModel, GBDTModel, LinearModel,
                     MODEL_FAMILIES, ModelNotFitted, Standardizer, make_model)
from .registry import (ModelArtifact, describe_model, list_models, load_model,
                       register_model)
from .signal_adapter import ScoreWeightedStrategy, scores_to_panel_signal
from .walkforward import FoldResult, WalkForwardResult, run_walk_forward

__all__ = [
    "SampleSet", "SampleMeta", "SampleBuildError", "build_sample_set",
    "Standardizer", "BaseModel", "LinearModel", "GBDTModel", "BaselineModel",
    "make_model", "MODEL_FAMILIES", "ModelNotFitted",
    "ModelArtifact", "register_model", "load_model", "list_models",
    "describe_model",
    "scores_to_panel_signal", "ScoreWeightedStrategy",
    "FoldResult", "WalkForwardResult", "run_walk_forward",
    "ExperimentRecord", "record_experiment", "list_experiments", "trial_count",
]