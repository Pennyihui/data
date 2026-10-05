# -*- coding: utf-8 -*-
"""features — 特征底座 (Feature Foundation)

设计文档: docs/feature-foundation-design.md (v0.4)

特征线 F1-F7 (先行):
    F1 算子库   operators.py      ts_*/cs_*/group_*/pp_*  ✅
    F2 字段+PIT  fields.py        reader 取数 + data_available_at 推导 + PoolScope
    F3 DSL验证  dsl.py            expr_codegen 选型验证
    F4 特征库   specs.py          FeatureSpec + registry + catalog
    F5 血缘     lineage.py        DAG 直接父节点 + 深度<=5
    F6 首批特征 library/          50-100 个
    F7 MCP      (mcp_server 扩展)

核心契约 (全层共享):
    - 面板数据: 行 = (instrument, time), 列 = 字段值
    - 每个特征值带 data_available_at (PIT 知识时间)
    - 特征在时刻 t 的 data_available_at = 输入中最大的 data_available_at
    - 受 PoolScope (时间墙) 约束, as_of 双向钳制

用法 (F1 算子库)::

    from data_foundation.features import ALL_OPERATORS, ts_mean, cs_rank
    from data_foundation.features.operators import describe_operator, audit_pit_source
"""
from .operators import (
    ALL_OPERATORS,
    CS_OPERATORS,
    FAMILY_REGISTRIES,
    GROUP_OPERATORS,
    INSTRUMENT_LEVEL,
    PP_OPERATORS,
    TIME_LEVEL,
    TS_OPERATORS,
    OperatorSpec,
    audit_pit_source,
    describe_operator,
    operator_names,
    referenced_operators,
    unknown_operators,
)

# 算子函数直接提升到包命名空间 (ts_mean / cs_rank / group_rank / pp_log ...)
from .operators import *  # noqa: F401,F403
from .operators import __all__ as _operator_all  # noqa: F401

__all__ = [n for n in _operator_all] + [
    "ALL_OPERATORS", "TS_OPERATORS", "CS_OPERATORS", "GROUP_OPERATORS",
    "PP_OPERATORS", "FAMILY_REGISTRIES", "OperatorSpec",
    "describe_operator", "operator_names", "referenced_operators",
    "unknown_operators", "audit_pit_source",
    "INSTRUMENT_LEVEL", "TIME_LEVEL",
]
