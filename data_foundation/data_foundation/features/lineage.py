# -*- coding: utf-8 -*-
"""lineage.py — F5 血缘系统 (特征级 DAG + 审计 + 版本)

设计文档: docs/feature-foundation-design.md 第 9 节 (v0.5)

**血缘的四个用途** (设计文档 9): 泄漏审计 / 相关性判断 / 失效归因 / 可复现。

存储决策 (9): **DAG 只存直接父节点** (每条边指向它的直接依赖特征/字段), 完整
链路用时遍历 —— 每节点冗余全链路会 O(N²); 深度上限 **5 层** (Alpha101 实践:
有效因子嵌套通常 ≤3-4 层), 超限在 validate/audit 时报错。

两层血缘不要混淆:
  * **表达式级血缘** (表达式里用了哪些算子/字段) —— 由 dsl.CompiledExpr 给出
  * **特征级血缘** (特征依赖哪些特征) —— 本模块, 用于"组合相关性判断"与"改一处
    要重算哪些"的影响面分析
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field

from . import registry

__all__ = ["LineageGraph", "MAX_DEPTH", "build_graph", "trace", "impact",
           "audit", "describe_node"]

MAX_DEPTH = 5          # 决策 9: 血缘深度上限 (超过难解释/易过拟合)


@dataclass
class LineageGraph:
    """特征级 DAG。edges[name] = 该特征直接依赖的特征名集合; parents 反向。"""

    edges: dict[str, tuple[str, ...]]          # 特征 -> 直接依赖的特征
    leaves: dict[str, tuple[str, ...]]         # 特征 -> 直接依赖的字段
    versions: dict[str, str]

    def parents_of(self, name: str) -> tuple[str, ...]:
        return self.edges.get(name, ())

    def children_of(self, name: str) -> list[str]:
        return [n for n, deps in self.edges.items() if name in deps]

    def depth_of(self, name: str) -> int:
        """特征级嵌套深度 (依赖其它特征才算一层; 直接吃字段 = 1)。"""
        memo: dict[str, int] = {}

        def d(n: str, stack=()) -> int:
            if n in memo:
                return memo[n]
            if n in stack:
                raise ValueError(f"血缘存在环: {' -> '.join(stack + (n,))}")
            deps = self.edges.get(n, ())
            memo[n] = 1 + max([d(x, stack + (n,)) for x in deps], default=0)
            return memo[n]

        return d(name)


def build_graph(names=None) -> LineageGraph:
    """由注册表构建血缘 DAG (只覆盖请求到的特征及其递归依赖)。"""
    registry.load_library()
    registry.validate_all(strict=False)
    names = list(names or registry.feature_names())
    edges, leaves, versions = {}, {}, {}
    seen = set()

    def visit(n):
        if n in seen:
            return
        seen.add(n)
        spec = registry.get_feature(n)
        versions[n] = spec.version
        deps = tuple(spec.features)
        edges[n] = deps
        leaves[n] = tuple(f for f in spec.fields if f not in deps)
        for d in deps:
            visit(d)

    for n in names:
        visit(n)
    return LineageGraph(edges=edges, leaves=leaves, versions=versions)


def trace(name: str, graph: LineageGraph | None = None) -> list[str]:
    """完整血缘链 (从叶子特征/字段到目标特征), 按拓扑展开。

    返回 ["ret_1h", "mom_zscore_24h", ...] —— 叶子字段用 "field:close" 标注。
    """
    graph = graph or build_graph([name])
    if name not in graph.edges:
        raise KeyError(f"未知特征 {name!r}")
    out: list[str] = []

    def walk(n, stack=()):
        if n in stack:
            raise ValueError(f"血缘成环: {' -> '.join(stack + (n,))}")
        deps = graph.edges.get(n, ())
        if not deps:
            leaves = graph.leaves.get(n, ())
            for lf in leaves:
                out.append(f"field:{lf}")
            out.append(n)
            return
        for d in deps:
            walk(d, stack + (n,))
        out.append(n)

    walk(name)
    return out


def impact(name: str, graph: LineageGraph | None = None) -> list[str]:
    """影响面: 直接/间接依赖 name 的全部特征 (改一处要重算哪些)。"""
    graph = graph or build_graph()
    direct = graph.children_of(name)
    out, stack = set(direct), list(direct)
    while stack:
        cur = stack.pop()
        for ch in graph.children_of(cur):
            if ch not in out:
                out.add(ch)
                stack.append(ch)
    return sorted(out)


def audit(graph: LineageGraph | None = None, max_depth: int = MAX_DEPTH) -> dict:
    """血缘体检 (泄漏审计 / 深度 / 影响面 / 版本)。"""
    graph = graph or build_graph()
    depths = {n: graph.depth_of(n) for n in graph.edges}
    too_deep = {n: d for n, d in depths.items() if d > max_depth}
    roots = [n for n, deps in graph.edges.items() if not deps]
    leaves = sorted({lf for ls in graph.leaves.values() for lf in ls})
    # 共享子结构: 同一父特征被 >=3 个子特征复用 (CSE 的潜力点, 也是相关性风险面)
    fanout: dict[str, int] = {}
    for n, deps in graph.edges.items():
        for d in deps:
            fanout[d] = fanout.get(d, 0) + 1
    hot = {k: v for k, v in fanout.items() if v >= 3}
    return {
        "n_features": len(graph.edges),
        "max_depth": max(depths.values()) if depths else 0,
        "mean_depth": round(sum(depths.values()) / len(depths), 2) if depths else 0.0,
        "too_deep": too_deep,
        "root_features": sorted(roots),
        "n_leaf_fields": len(leaves),
        "leaf_fields": leaves,
        "hot_parents": hot,                       # 被 >=3 个特征复用的父特征
        "versions": graph.versions,
        "max_impact": max((len(impact(n, graph)) for n in graph.edges), default=0),
    }


def describe_node(name: str, graph: LineageGraph | None = None) -> dict:
    """单节点血缘画像 (MCP describe_feature 用)。"""
    graph = graph or build_graph([name])
    spec = registry.get_feature(name)
    return {
        "name": name,
        "version": spec.version,
        "expr": spec.expr,
        "depth": graph.depth_of(name),
        "parents": list(graph.parents_of(name)),
        "fields": list(graph.leaves.get(name, ())),
        "children": graph.children_of(name),
        "impacted_by_change": impact(name, graph),
        "lineage": trace(name, graph),
    }


if __name__ == "__main__":  # pragma: no cover
    import json
    g = build_graph()
    rep = audit(g)
    print("血缘体检:")
    for k, v in rep.items():
        if k in ("versions", "root_features", "leaf_fields"):
            continue
        print(f"  {k}: {v}")
    print("\n示例 trace(mom_zscore_24h):", trace("mom_zscore_24h", g))
    print("示例 impact(ret_1h):", impact("ret_1h", g))