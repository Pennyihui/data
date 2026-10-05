# -*- coding: utf-8 -*-
"""registry.py — F4 特征注册表 (库 != 注册表)

设计文档: docs/feature-foundation-design.md 4.0 / 4.5

**注册表是索引, 特征库是实体集合** —— 注册表只负责:
  1. 登记 (装饰器), 2. 编译校验, 3. 按名字取, 4. 供 catalog 多路检索。
实体 (表达式) 在 library/*.py 里; 这里不复制一份表达式。

命名约束 (防止表达式歧义):
  * 特征名不得与任何**字段**或分组标签同名 (否则表达式里 "close" 指谁?);
  * 特征名只允许 [a-z][a-z0-9_]* (表达式是白名单解析, 名字要能当标识符)。
"""
from __future__ import annotations

import re
from dataclasses import replace

from . import dsl
from .specs import FeatureSpec
from ..fields import FIELD_REGISTRY

__all__ = [
    "feature", "register", "list_features", "get_feature", "feature_names",
    "registry_size", "validate_all", "load_library", "RESERVED_NAMES",
]

_FEATURES: dict[str, FeatureSpec] = {}
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

#: 保留字: 字段名与算子名不能被特征占用
RESERVED_NAMES = set(FIELD_REGISTRY) | set(dsl.op.ALL_OPERATORS)


def _validate_name(name: str) -> None:
    if not _NAME_RE.match(name):
        raise ValueError(
            f"特征名 {name!r} 不合法: 只允许小写字母开头的 [a-z0-9_] "
            f"(表达式是白名单 AST, 名字要能直接当标识符)")
    if name in RESERVED_NAMES:
        kind = "字段" if name in FIELD_REGISTRY else "算子"
        raise ValueError(
            f"特征名 {name!r} 与{kind}同名 —— 表达式里无法区分指的是哪个")


def register(spec: FeatureSpec) -> FeatureSpec:
    """登记一个特征 (通常由 @feature 自动调用)。"""
    _validate_name(spec.name)
    old = _FEATURES.get(spec.name)
    if old is not None:
        if old.expr == spec.expr and old.version == spec.version:
            return old                       # 重复 import (幂等)
        if old.expr != spec.expr:
            # 表达式变更即升版: 保留旧版以保证历史研究可复现
            new_version = _bump(old.version)
            spec = replace(spec, version=new_version)
    _FEATURES[spec.name] = spec
    return spec


def _bump(version: str) -> str:
    try:
        major, minor = version.split(".", 1)
        return f"{major}.{int(minor) + 1}"
    except (ValueError, IndexError):
        return f"{version}.1"


def feature(name: str, expr: str, category: str = "", desc: str = "",
            tags: tuple[str, ...] = (), version: str = "1.0",
            source: str = "", status: str = "active") -> FeatureSpec:
    """声明一个特征 (设计文档 4.2)。

    用法::

        @feature(name="funding_rank_7d",
                 expr="group_rank(ts_rank(funding_rate, 168), sector)",
                 category="derivatives", desc="资金费率在自身168h历史的排名, 再在板块内排名")
        def funding_rank_7d():
            ...
    """
    spec = FeatureSpec(name=name, expr=expr, category=category or "misc",
                       desc=desc, tags=tuple(tags), version=version,
                       source=source, status=status)
    return register(spec)


def list_features(category: str | None = None, status: str = "active") -> list[FeatureSpec]:
    out = [s for s in _FEATURES.values() if s.status == status]
    if category:
        out = [s for s in out if s.category == category]
    return sorted(out, key=lambda s: s.name)


def feature_names() -> list[str]:
    return sorted(_FEATURES)


def registry_size() -> int:
    return len(_FEATURES)


def get_feature(name: str) -> FeatureSpec:
    if name not in _FEATURES:
        raise KeyError(f"未知特征 {name!r} (库里有 {len(_FEATURES)} 个; "
                       f"用 list_features() 查看)")
    return _FEATURES[name]


def validate_all(strict: bool = False) -> dict[str, str]:
    """编译校验全部特征 (表达式字段/算子/依赖/分组维度是否存在)。

    返回 {name: ''} 表示全部通过; strict=True 时把错误合并抛出。
    """
    from .groups import GROUP_DIMENSIONS
    known_fields = set(FIELD_REGISTRY)
    errors: dict[str, str] = {}
    for name, spec in _FEATURES.items():
        try:
            # 分组维度算进"已知标签": 表达式里的 sector/market_cap_tier 等
            # 既不属字段也不属特征, 需要单独声明可用性
            spec.compile(known_fields, set(_FEATURES) - {name},
                         known_groups=set(GROUP_DIMENSIONS))
            errors[name] = ""
        except dsl.ExprError as exc:
            errors[name] = str(exc)
    bad = {k: v for k, v in errors.items() if v}
    if bad and strict:
        raise dsl.ExprError(
            f"{len(bad)} 个特征表达式非法: " +
            "; ".join(f"{k}: {v}" for k, v in list(bad.items())[:5]))
    return bad


_LIBRARY_MODULES = (
    "price", "derivatives", "liquidity", "quality", "cross_asset", "onchain",
    "intrabar", "regime", "group_features", "robust",
)


def load_library(modules=None) -> int:
    """导入特征库模块 (触发 @feature 注册)。返回库中特征总数。

    用绝对导入 (不依赖 __package__), 这样 `python -m` 与普通 import 行为一致。
    """
    import importlib
    import data_foundation.features as _pkg
    for m in (modules or _LIBRARY_MODULES):
        try:
            importlib.import_module(f"data_foundation.features.library.{m}")
        except ModuleNotFoundError as exc:
            if f"library.{m}" in str(exc):
                continue                       # 该库文件尚未创建
            raise
    return len(_FEATURES)


if __name__ == "__main__":  # pragma: no cover
    # 直接 import 真实模块 (避免 `python -m` 把 __main__ 和 registry 当两个
    # 模块实例, 导致 @feature 注册到空的 _FEATURES)
    from data_foundation.features import registry as _r
    n = _r.load_library()
    print(f"特征库: {n} 个特征")
    for s in _r.list_features():
        print(f"  {s.name:24s} [{s.category:12s}] v{s.version}  {s.expr}")