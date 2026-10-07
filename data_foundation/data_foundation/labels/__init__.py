# -*- coding: utf-8 -*-
"""labels/ — 目标 → 标签 → 样本切分 (研究链第二环)

设计: docs/label-system-design.md (v0.1)

**标签是未来函数** —— t 时刻的标签用 t 之后的价格定义。这是它的定义, 不是缺陷。
因此标签与特征之间是**物理隔离**, 而不是纪律性的:

  1. 命名空间: 特征表达式 DSL 的字段白名单只由 features/fields.py 生成,
     标签名不在其中 -> 引用标签的特征表达式编译失败 (ExprError)。
  2. 存储:     标签写在 data/labels/, 特征引擎的路径里根本没有它。
  3. 字段源:   fields.py 无任何前视字段, 且由隔离测试机械扫描保持。
  4. 池访问:   valid/oos 视同原始数据 -> Agent 读标签被拒 (PermissionError)。

反过来说: 标签**故意不适用**未来不变性测试 —— 它的"未来依赖"是规范的一部分。
防标签泄漏靠的是上面四道闸, 不是靠"标签算得对"。
"""
from .definitions import (LABELS, LabelResult, LabelSpec, compute_labels,
                          get_label, list_labels, register_label, KINDS)
from .objectives import (CostParams, Objective, REGISTRY, get_objective,
                         list_objectives, parse_horizon_days, register_objective)
from .splitter import Fold, assert_no_overlap, walk_forward_splits
from .store import (load_label, label_fingerprint, list_stored, load_meta,
                    save_label)

__all__ = [
    "CostParams", "Objective", "REGISTRY", "register_objective",
    "get_objective", "list_objectives", "parse_horizon_days",
    "LabelSpec", "LabelResult", "LABELS", "register_label", "get_label",
    "list_labels", "compute_labels", "KINDS",
    "Fold", "walk_forward_splits", "assert_no_overlap",
    "save_label", "load_label", "load_meta", "label_fingerprint",
    "list_stored",
]