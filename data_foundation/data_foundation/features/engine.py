# -*- coding: utf-8 -*-
"""engine.py — F4 特征计算引擎 (时间墙 + 依赖解析 + PIT + 血缘)

设计文档: docs/feature-foundation-design.md 第 7、8 节 (v0.5)

FeatureEngine 是 Agent/研究员拿特征的**唯一入口** (设计文档 8):

    scope = PoolScope("oof")
    eng  = FeatureEngine(scope, start="2022-01-01", end="2023-12-31")
    out  = eng.compute(["funding_rank_7d", "mom_zscore_24h"])

引擎自动完成 (不让使用者手写, 手写必错):
  * 时间墙: 窗口与池求交集 + 因子屏蔽 + 宇宙门控 (经 fields.load_panel)
  * 预热: 按被请求特征的 lookback 自动向前多读 (池起点处的滚动窗口也要有历史)
  * 依赖解析: 特征可引用其它特征, 按拓扑序一次算好, 共享中间结果 (CSE)
  * PIT: 每个特征值带 data_available_at, 逐节点传播 + assert_no_leakage
  * 血缘: 每个特征返回完整链路与深度 (供 describe_feature / F5 审计)
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field

import pandas as pd

from ..fields import FIELD_REGISTRY, Panel, load_panel
from ..pool_registry import PoolScope
from . import dsl, groups as groups_mod, registry
from .cache import FeatureCache
from .specs import FeatureSpec

__all__ = ["FeatureEngine", "FeatureBundle", "GROUP_DIMENSIONS"]

#: 全库稳定预热 (进程内缓存一次)
_STABLE_WARMUP: str | None = None

#: 分组维度 (设计文档 3.2; 由 features/groups.py 从 PIT 宇宙快照构造,
#: 不需要手工维护 —— 决策 2)
GROUP_DIMENSIONS = groups_mod.GROUP_DIMENSIONS


@dataclass
class FeatureBundle:
    """一批特征的计算结果。

    values: (index=base_asset,time) x (feature) 的值面板
    avail : 同形状的 data_available_at 面板
    panel : 底层字段面板 (审计/复用)
    specs : 名字 -> FeatureSpec
    results: 名字 -> dsl.FeatureResult (含血缘/输入可用时间)
    """

    values: pd.DataFrame
    avail: pd.DataFrame
    panel: Panel
    specs: dict[str, FeatureSpec]
    results: dict[str, dsl.FeatureResult]

    def __repr__(self) -> str:
        return (f"<FeatureBundle {self.values.shape[0]:,} 行 x "
                f"{len(self.specs)} 特征 [{self.panel.scope.pool_id}]>")


class FeatureEngine:
    """特征计算引擎 (绑定一个 PoolScope)。"""

    def __init__(self, scope: PoolScope | str = "oof", *,
                 start=None, end=None, as_of=None, assets=None,
                 warmup: str | pd.Timedelta | None = None,
                 layer: str | None = None, groups: dict | None = None,
                 cache: str | bool = "auto", cache_root: str | None = None):
        self.scope = scope if isinstance(scope, PoolScope) else PoolScope(
            scope, layer=layer or "research")
        self.start, self.end, self.as_of = start, end, as_of
        self.assets = list(assets) if assets is not None else None
        # 预热: 默认取**全库最大 lookback** 而不是"本次请求的最大" —— 后者会让
        # 同一特征在不同请求组合下得到不同的 warmup, 进而缓存键不同、命中率归零,
        # 而且同一窗口起点的特征值会因历史多少而不同 (不可复现)。取全库最大值
        # 让预热与请求无关: 稳定 + 可复现, 代价是多读一点历史。
        self.warmup = warmup if warmup is not None else self._stable_warmup()
        self.groups = dict(groups or {})
        self._panel: Panel | None = None
        registry.load_library()
        # 落盘缓存 (决策 3): cache='auto' 读+写, 'read' 只读, 'write' 只写,
        # 'off'/False 关闭。缓存键含 特征版本 + 输入数据指纹 + 池 + 窗口,
        # 上游数据一重建指纹即变 -> 旧缓存自动失效 (防"用过期数据做研究")。
        self._cache_mode = ("off" if cache is False else
                            "auto" if cache is True else str(cache))
        self.cache = FeatureCache(root=cache_root,
                                  enabled=self._cache_mode != "off")

    # -- 面板 (按需装载, 含自动预热) --------------------------------------
    @staticmethod
    def _stable_warmup() -> str:
        """全库最大预热 (小时) —— 进程内算一次并缓存, 保证同一引擎生命周期内稳定。"""
        global _STABLE_WARMUP
        if _STABLE_WARMUP is None:
            registry.validate_all(strict=False)
            bars = max((s.lookback.bars if s.lookback else 0)
                       for s in registry.list_features())
            _STABLE_WARMUP = f"{int(bars)}h"
        return _STABLE_WARMUP

    def panel(self, field_names, warmup_bars: int = 0) -> Panel:
        wm = self.warmup or self._stable_warmup()
        return load_panel(self.scope, field_names, assets=self.assets,
                          start=self.start, end=self.end, as_of=self.as_of,
                          warmup=wm if wm is not None else "0h")

    # -- 依赖解析 ---------------------------------------------------------
    def _resolve(self, names: list[str]) -> list[str]:
        """返回按依赖拓扑序排列的特征名 (特征引用特征)。

        先编译每个特征 (填充 fields/features 依赖), 依赖解析基于编译结果 ——
        否则 registry 刚导入时的 spec 还没有 fields 信息。
        """
        from ..fields import FIELD_REGISTRY
        for n in names:
            if n not in registry._FEATURES:
                raise KeyError(f"未知特征 {n!r}")
        # 编译所有特征一次 (拿到 fields/features 依赖)
        registry.validate_all(strict=False)
        order, state = [], {}

        def visit(n: str, stack: tuple[str, ...] = ()):
            if state.get(n) == "done":
                return
            if n in stack:
                raise ValueError(
                    f"特征循环依赖: {' -> '.join(stack + (n,))}")
            if state.get(n) == "visiting":
                return
            state[n] = "visiting"
            spec = registry.get_feature(n)
            for dep in spec.features:
                visit(dep, stack + (n,))
            state[n] = "done"
            order.append(n)

        for n in names:
            visit(n)
        return order

    # -- 主入口 -----------------------------------------------------------
    def compute(self, names, panel: Panel | None = None,
                check: bool = True) -> FeatureBundle:
        """批量计算特征 (PIT + 泄漏自检 + 血缘)。

        names        : 特征名列表 (可依赖其它特征, 自动拓扑展开)
        panel        : 可选, 传入已装载的面板 (避免重复读盘)
        check        : 是否跑 assert_no_leakage (生产必须 True)
        """
        order = self._resolve(list(names))
        wanted = {n: registry.get_feature(n) for n in order}
        # 需要的字段 = 所有特征的字段依赖 (不含特征依赖)
        field_need = sorted({f for s in wanted.values() for f in s.fields})
        unknown = [f for f in field_need if f not in FIELD_REGISTRY]
        if unknown:
            raise KeyError(f"特征引用了未知字段: {unknown}")
        lookback = max((s.lookback.bars if s.lookback else 0)
                       for s in wanted.values())
        if panel is None:
            panel = self.panel(field_need, warmup_bars=lookback)
        values = panel.values
        avail = panel.avail

        # 分组维度: 表达式用到哪些维度就自动构造哪些 (PIT 逐日标签), 调用方
        # 还可以用 self.groups 覆盖/补充 (例如外部数据源的板块分类)
        used_dims = {g for s in wanted.values() for g in s.groups}
        dims_needed = sorted(used_dims - set(self.groups))
        groups = dict(self.groups)
        if dims_needed:
            groups.update(groups_mod.load_groups(self.scope, dims_needed,
                                                 values.index))

        known = set(values.columns) | set(groups) | set(GROUP_DIMENSIONS)
        results: dict[str, dsl.FeatureResult] = {}
        specs: dict[str, FeatureSpec] = {}
        out_v: dict[str, pd.Series] = {}
        out_a: dict[str, pd.Series] = {}
        # 中间结果攒起来批量 concat 进面板: 逐个 insert 会让 DataFrame 碎片化
        # (167 列时 pandas 直接告警)。但特征可以引用特征, 所以**下游有依赖时**
        # 必须先把挂起的列并进去 —— 于是只在"下一站有依赖"那一刻 flush。
        pend_v: dict[str, pd.Series] = {}
        pend_a: dict[str, pd.Series] = {}

        def flush():
            nonlocal values, avail
            if pend_v:
                values = pd.concat([values, pd.DataFrame(pend_v)], axis=1)
                avail = pd.concat([avail, pd.DataFrame(pend_a)], axis=1)
                pend_v.clear()
                pend_a.clear()

        for n in order:
            spec = wanted[n]
            if spec.features:                     # 下游引用特征 -> 先并入
                flush()
            # 缓存查询 (auto/read 模式)。命中则直接复用值+可用时间, 不重算。
            ck = None
            hit = None
            if self._cache_mode in ("auto", "read"):
                ck = self.cache.make_key(spec, panel, self.scope,
                                         assets=self.assets)
                hit = self.cache.lookup(ck)
            if hit is not None:
                cv, ca, _meta = hit
                cv = cv.reindex(values.index)
                ca = ca.reindex(values.index)
                # 缓存命中也要走一遍泄漏自检: 缓存文件可能来自旧的引擎版本,
                # 这里的自检保证它与当前引擎的 PIT 规则一致
                if check:
                    from ..fields import assert_no_leakage
                    assert_no_leakage(ca, {n: ca}, name=n + "(cached)",
                                       feature_values=cv)
                res = dsl.FeatureResult(name=n, values=cv, avail=ca,
                                        input_avail=ca, expr=spec.expr,
                                        compiled=spec.compiled)
            else:
                c = dsl.compile_expr(spec.expr, known)
                res = dsl.evaluate(c, values, avail, groups=groups, name=n,
                                   group_cols=set(GROUP_DIMENSIONS), check=check)
                if self._cache_mode in ("auto", "write"):
                    self.cache.store(ck, res.values, res.avail)
            pend_v[n] = res.values
            pend_a[n] = res.avail
            known.add(n)
            results[n] = res
            specs[n] = spec
            out_v[n] = res.values
            out_a[n] = res.avail
        vdf = pd.DataFrame(out_v).reindex(values.index)
        adf = pd.DataFrame(out_a).reindex(values.index)
        return FeatureBundle(values=vdf, avail=adf, panel=panel, specs=specs,
                             results=results)

    # -- 单特征审计 --------------------------------------------------------
    def describe(self, name: str) -> dict:
        """特征定义 + 血缘 (MCP describe_feature 用)。"""
        registry.validate_all(strict=False)
        spec = registry.get_feature(name)
        d = spec.to_dict()
        d["pool"] = self.scope.pool_id
        d["available_rule"] = (
            "data_available_at = 所用输入可用时间的最大值 (逐算子按取数窗口传播)")
        d["lineage_depth"] = spec.depth
        d["lookback_hours"] = spec.lookback.hours.total_seconds() / 3600 \
            if spec.lookback else 0
        return d


if __name__ == "__main__":  # pragma: no cover
    scope = PoolScope("oof")
    eng = FeatureEngine(scope, start="2022-01-01", end="2022-01-31")
    n = registry.load_library()
    print(f"特征库 {n} 个; 引擎就绪 [{eng.scope.pool_id}]")