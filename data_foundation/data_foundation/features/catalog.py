# -*- coding: utf-8 -*-
"""catalog.py — F4 可浏览目录 (多路索引 + 去重)

设计文档: docs/feature-foundation-design.md 4.5

多路索引: 按类别 / 按数据来源 / 按算子 / 按标签 / 全文检索; 去重检查:
同一表达式 (规范化后) 或同一血缘链的特征是重复登记 —— 库要当"活水"而不是
垃圾场 (决策 7), 重复定义会让多重检验负担白白翻倍。
"""
from __future__ import annotations

import ast
import hashlib
from collections import defaultdict

from . import dsl, registry
from .specs import FeatureSpec

__all__ = [
    "by_category", "by_input", "by_operator", "by_tag", "search",
    "find_duplicates", "canonical_key", "coverage_report", "index_stats",
]


def _field_names() -> set[str]:
    from ..fields import FIELD_REGISTRY
    return set(FIELD_REGISTRY)


def by_category(category: str) -> list[FeatureSpec]:
    return registry.list_features(category=category)


def by_input(dataset_or_field: str) -> list[FeatureSpec]:
    """用了某个字段/数据源的特征 (血缘第一跳即可判定)。"""
    registry.validate_all()
    needle = dataset_or_field
    out = []
    for s in registry.list_features():
        if needle in s.fields or any(needle in f for f in s.features):
            out.append(s)
    return out


def by_operator(op_name: str) -> list[FeatureSpec]:
    """用了某个算子的特征 (算子治理: 某族算子的依赖面)。"""
    registry.validate_all()
    return [s for s in registry.list_features() if op_name in s.operators]


def by_tag(tag: str) -> list[FeatureSpec]:
    return [s for s in registry.list_features() if tag in s.tags]


def search(keyword: str) -> list[FeatureSpec]:
    """全文检索: 名字 / 描述 / 标签 / 表达式 / 依赖字段。"""
    registry.validate_all()
    k = keyword.lower()
    out = []
    for s in registry.list_features():
        blob = " ".join([s.name, s.desc, s.expr, s.category, *s.tags,
                         *s.fields, *s.features]).lower()
        if k in blob:
            out.append(s)
    return out


def canonical_key(spec: FeatureSpec) -> str:
    """规范化去重键: 表达式编译后按结构 (非字面) 哈希。"""
    registry.validate_all()
    c = spec.compiled
    return hashlib.sha1(_struct(c.root).encode("utf-8")).hexdigest()[:16]


def _struct(node) -> str:
    """节点的结构表示。叶子字段**含字段名** (否则不同字段的单字段 raw 特征会
    被误判成重复), 数字字面量统一整数/浮点但保留数值 (否则 ts_mean(x,24) 与
    ts_mean(x,7) 会被误判)。"""
    if node.kind == "field":
        return f"field:{node.code}"
    kids = [_struct(a) for a in node.args if isinstance(a, ast.AST)]
    lits = [_lit(a) for a in node.args if not isinstance(a, ast.AST)]
    return f"{node.kind}:{node.op_name or node.binop or ''}[{','.join(kids + lits)}]"


def _lit(a) -> str:
    """字面量表示: 整数/浮点统一 (24 与 24.0 同一表达式), 但**不同窗口值不同**
    —— 否则 ts_mean(x,24) 与 ts_mean(x,7) 会被误判成重复特征。"""
    if isinstance(a, bool):
        return "True" if a else "False"
    if isinstance(a, (int, float)):
        return f"num:{int(a)}" if float(a).is_integer() else f"num:{a}"
    return str(a)


def find_duplicates() -> list[tuple[str, ...]]:
    """返回重复组 (同一规范化表达式/血缘链的两个以上特征名)。

    编译失败的特征 (表达式非法) 无法比较结构, 跳过 —— 它们由 validate_all 报错。
    """
    registry.validate_all()
    buckets: dict[str, list[str]] = defaultdict(list)
    for s in registry.list_features():
        if s._compiled is None:
            continue
        buckets[canonical_key(s)].append(s.name)
    return [tuple(sorted(names)) for names in buckets.values() if len(names) > 1]


def coverage_report() -> dict:
    """库的结构体检 (决策 7: 规模上限 500/1000, 每加 100 个跑体检)。"""
    registry.validate_all()
    specs = registry.list_features()
    cats: dict[str, int] = defaultdict(int)
    ops: dict[str, int] = defaultdict(int)
    fields_used: set[str] = set()
    depths: list[int] = []
    for s in specs:
        cats[s.category] += 1
        for o in s.operators:
            ops[o] += 1
        fields_used.update(s.fields)
        depths.append(s.depth)
    from ..fields import FIELD_REGISTRY
    unused = sorted(set(FIELD_REGISTRY) - fields_used)
    return {
        "n_features": len(specs),
        "by_category": dict(sorted(cats.items())),
        "n_fields_total": len(FIELD_REGISTRY),
        "n_fields_used": len(fields_used),
        "unused_fields": unused,
        "operator_hist": dict(sorted(ops.items(), key=lambda kv: -kv[1])),
        "max_depth": max(depths) if depths else 0,
        "mean_depth": round(sum(depths) / len(depths), 2) if depths else 0.0,
        "duplicates": find_duplicates(),
    }


def index_stats() -> dict:
    """索引规模 (MCP list_features 用)。"""
    n = registry.registry_size()
    return {"n_features": n,
            "soft_cap": 500, "hard_cap": 1000,
            "at_soft_cap": n >= 500, "at_hard_cap": n >= 1000}