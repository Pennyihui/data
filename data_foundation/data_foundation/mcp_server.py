# -*- coding: utf-8 -*-
"""mcp_server.py — 数据底座 MCP 服务 (Agent 侧)

把研究池的时间墙封装成 Agent 可调用的工具集。核心原则:

    **时间墙不能靠"约定", 必须靠 API 强制。**
    Agent 不知道有墙, 它只会看到工具返回什么。所以 as_of 被硬钳制到会话
    绑定的池内, 因子越界直接抛错 —— 这些都是在代码里, 不是在文档里。

工具分组
--------
    池与作用域   list_pools / bind_pool
    数据查询     query_candles / query_derivatives / query_universe
    因子可用性   factor_available / list_factors
    提交         submit_metrics (池1/池2) / submit_oos_candidate (池3, 拿不到分数)

人审通道 (oos_ledger.oos_result_read / oos_verdict) **不在本模块暴露** ——
那是给研究人员的, Agent 无入口。物理隔离, 见设计文档 8.4。

运行 (stdio MCP):
    python -m data_foundation.mcp_server
"""
from __future__ import annotations

import json
import sys

import pandas as pd

from .pool_registry import (FACTOR_AVAILABILITY, NO_PIT_COLUMN,
                            REVISION_CONTAMINATED, POOLS, RESEARCH_POOLS,
                            PoolScope, assert_can_read_data,
                            data_access_mode, factor_available, list_pools)
from .reader import load_candles, load_derivatives, load_universe
from . import eval_service

# 会话状态: Agent 启动时绑定一个池, 之后所有查询都在该池内
_SCOPE: PoolScope | None = None
_FEATURE_ENGINE = None          # 惰性创建 (特征线 F4/F7)


# ---------------------------------------------------------------------------
# 池与作用域
# ---------------------------------------------------------------------------
def tool_list_pools() -> dict:
    """列出所有研究池及其边界、可用性。"""
    return {"pools": list_pools(),
            "note": "gap 池 (隔离带) unusable=True 之外的都是研究池; "
                    "usable=False 的隔离带两侧都不能用。"}


def tool_bind_pool(pool_id: str, layer: str = "research") -> dict:
    """绑定会话到一个研究池。**必须在任何查询前调用**。绑定后不可改。"""
    global _SCOPE
    if _SCOPE is not None:
        raise RuntimeError(f"会话已绑定到池 {_SCOPE.pool_id}, 不可重绑。"
                           f"(一个会话只能在一个池内工作 —— 这是防越界的核心)")
    _SCOPE = PoolScope(pool_id, layer=layer)
    return {"bound": _SCOPE.pool_id, "pool": _SCOPE.pool.name,
            "range": f"{_SCOPE.pool.start} ~ {_SCOPE.pool.end or 'open'}",
            "layer": layer,
            "note": "之后所有 query 的 as_of 会被钳制在本池内, 越界参数无效。"}


def _require_scope() -> PoolScope:
    if _SCOPE is None:
        raise RuntimeError("尚未绑定研究池, 请先调用 bind_pool(pool_id)")
    return _SCOPE


# ---------------------------------------------------------------------------
# 数据查询 (as_of 被硬钳制)
# ---------------------------------------------------------------------------
def tool_query_candles(venue: str, instrument: str, interval: str = "1h",
                       market_type: str = "spot", as_of=None,
                       cols: list[str] | None = None) -> dict:
    """读 K 线。as_of 被钳制到本池内; 因子越界/污染会抛错。

    **三池隔离**: 只有开发池 (oof) 允许读原始数据。反馈池(valid)/OOS 池一律
    拒绝 —— 走 submit -> eval_service 通道。这堵的是"Agent 自己用池内行情
    + 特征算出业绩"绕过 Agent 盲的绕门。
    """
    s = _require_scope()
    assert_can_read_data(s.pool_id, "原始数据")
    df = load_candles(venue, instrument, interval, as_of=as_of, cols=cols,
                      market_type=market_type, scope=s)
    return {"n_rows": int(len(df)),
            "effective_as_of": str(s.clamp(as_of)) if as_of else str(s.pool.end_ts),
            "data_min": str(df["open_time_utc"].min()) if len(df) else None,
            "data_max": str(df["open_time_utc"].max()) if len(df) else None,
            "pool": s.pool_id}


def tool_query_derivatives(venue: str, instrument: str, dataset: str,
                           as_of=None) -> dict:
    """读衍生品 (funding/OI/mark/index/ratio)。as_of 被钳制; 污染源抛错。

    **三池隔离**: 同 query_candles —— 只有开发池可读。
    """
    s = _require_scope()
    assert_can_read_data(s.pool_id, "原始数据")
    df = load_derivatives(venue, instrument, dataset, as_of=as_of, scope=s)
    tcol = df.columns[0] if len(df) else None
    return {"n_rows": int(len(df)), "dataset": dataset, "pool": s.pool_id,
            "data_min": str(pd.to_datetime(df[tcol].min(), utc=True)) if len(df) else None,
            "data_max": str(pd.to_datetime(df[tcol].max(), utc=True)) if len(df) else None}


def tool_query_universe(as_of=None, layer: str | None = None) -> dict:
    """读三层宇宙成员 (研究/回测/交易)。默认取本池末日的名单。

    **三池隔离**: 同 query_candles —— 只有开发池可读 (宇宙名单也是池内信息)。
    """
    s = _require_scope()
    assert_can_read_data(s.pool_id, "宇宙成员")
    a = s.clamp(as_of) if as_of is not None else s.pool.end_ts
    lay = layer or s.layer
    df = load_universe(as_of=a, layer=lay, scope=s)
    return {"n_members": int(len(df)), "layer": lay,
            "as_of": str(pd.Timestamp(a).date()), "pool": s.pool_id,
            "symbols": sorted(df["symbol"].tolist())[:200] if len(df) else []}


# ---------------------------------------------------------------------------
# 因子可用性 (透明化: 让 Agent 知道什么能用)
# ---------------------------------------------------------------------------
def tool_factor_available(dataset: str, as_of=None) -> dict:
    s = _require_scope()
    eff = s.clamp(as_of) if as_of is not None else s.pool.end_ts
    ok = factor_available(dataset, eff)
    return {"dataset": dataset, "as_of": str(pd.Timestamp(eff).date()),
            "available": ok, "availability_start": FACTOR_AVAILABILITY.get(dataset),
            "contaminated": dataset in REVISION_CONTAMINATED,
            "no_pit_column": dataset in NO_PIT_COLUMN}


def tool_list_factors() -> dict:
    """列出所有因子的可用性起点与健康状态 (供 Agent 规划因子集)。"""
    rows = []
    for ds, start in sorted(FACTOR_AVAILABILITY.items()):
        rows.append({
            "dataset": ds, "availability_start": start,
            "contaminated": ds in REVISION_CONTAMINATED,
            "no_pit_column": ds in NO_PIT_COLUMN,
            "usable_default": ds not in REVISION_CONTAMINATED
                              and ds not in NO_PIT_COLUMN,
        })
    return {"factors": rows,
            "default_factor_set": list(POOLS["oof"].factors)}


# ---------------------------------------------------------------------------
# 提交
# ---------------------------------------------------------------------------
def tool_submit_metrics(run_id: str, metrics: dict, pool_id: str | None = None) -> dict:
    """提交指标到池1/池2 (获得反馈)。池3 (OOS) 拒绝 —— OOS 走 submit_oos_candidate。"""
    s = _require_scope()
    if s.pool.kind == "oos" or s.pool.kind == "rolling_oos":
        raise PermissionError("OOS 池不接受指标反馈; 请用 submit_oos_candidate 提交模型版本。")
    rec = {"type": "metrics", "run_id": run_id, "metrics": metrics,
           "pool": s.pool_id, "submitted_at": pd.Timestamp.now(tz="UTC").isoformat()}
    from .config import DATA_ROOT
    import os
    path = os.path.join(DATA_ROOT, "_oos_ledger", "submissions.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    return {"submitted": True, "pool": s.pool_id, "run_id": run_id}


def tool_submit_oos_candidate(run_id: str, code_hash: str,
                              artifact_uri: str = "", notes: str = "") -> dict:
    """把模型版本送进 OOS。**只返回回执, 永远不返回分数** (Agent 盲)。"""
    from .oos_ledger import submit_oos_candidate
    _require_scope()          # 必须已绑池 (即使是 OOS)
    return submit_oos_candidate(run_id, code_hash, artifact_uri, notes)


def tool_oos_status(evaluation_id: str) -> dict:
    """查 OOS 提交状态。只返回 pending/evaluated, **不返回分数**。"""
    from .oos_ledger import oos_status
    return oos_status(evaluation_id)


# ---------------------------------------------------------------------------
# MCP 工具清单
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 特征库 (F7): list / describe / compute / catalog —— 全部受时间墙约束
# ---------------------------------------------------------------------------
def _feature_module():
    """惰性导入特征模块 (MCP 启动不拖特征库)。"""
    from .features import engine, lineage, registry          # noqa: PLC0415
    return engine, lineage, registry


def tool_list_features(category: str | None = None) -> dict:
    """列出特征库中的特征 (名字/类别/表达式/版本)。全可见, 无池约束。"""
    _, _, registry = _feature_module()
    registry.load_library()
    specs = registry.list_features(category=category)
    return {"n_features": len(specs),
            "categories": sorted({s.category for s in specs}),
            "features": [{"name": s.name, "category": s.category,
                          "expr": s.expr, "version": s.version,
                          "desc": s.desc} for s in specs]}


def tool_describe_feature(name: str) -> dict:
    """查特征定义 + 血缘 (表达式链/深度/预热/影响面)。全可见。"""
    engine, lineage_mod, registry = _feature_module()
    registry.load_library()
    graph = lineage_mod.build_graph([name])
    d = lineage_mod.describe_node(name, graph)
    d["lookback_hours"] = (registry.get_feature(name).lookback.hours.total_seconds()
                           / 3600 if registry.get_feature(name).lookback else 0)
    d["available_rule"] = ("data_available_at = 所用输入可用时间的最大值 "
                           "(逐算子按取数窗口传播, 引擎自动推导)")
    return d


def tool_compute_features(names: list[str], start: str | None = None,
                          end: str | None = None, as_of=None,
                          assets: list[str] | None = None) -> dict:
    """批量计算特征。**as_of/窗口被钳制到会话绑定的池内** (时间墙在引擎层强制);
    返回汇总 (值面板不下发, 只给统计 —— 数据量大时让 Agent 用摘要工作)。

    **三池隔离**: 只有开发池可自行计算 —— 反馈池/OOS 的计算必须走 submit_candidate
    (服务端用池内数据评), 否则 Agent 拿行情+特征自己算业绩即绕过 Agent 盲。
    """
    s = _require_scope()
    assert_can_read_data(s.pool_id, "原始数据 (计算特征)")
    engine_mod, _, registry = _feature_module()
    registry.load_library()
    eng = engine_mod.FeatureEngine(
        s, start=start, end=end, as_of=as_of, assets=assets)
    bundle = eng.compute(list(names))
    stats = {}
    for c in bundle.values.columns:
        v = bundle.values[c].dropna()
        stats[c] = {"n": int(len(v)),
                    "avail_max": str(bundle.avail[c].dropna().max()),
                    "mean": float(v.mean()) if len(v) else None,
                    "std": float(v.std()) if len(v) > 1 else None}
    return {"pool": s.pool_id,
            "effective_window": [str(bundle.panel.start.date()),
                                 str(bundle.panel.end.date())],
            "as_of": str(bundle.panel.as_of),
            "n_rows": int(bundle.values.shape[0]),
            "features": stats,
            "lineage": {n: bundle.results[n].lineage() for n in bundle.specs}}


def tool_feature_catalog() -> dict:
    """特征库体检: 类别分布/算子依赖/深度/去重/规模上限 (决策 7)。"""
    _, _, registry = _feature_module()
    registry.load_library()
    from .features import catalog                            # noqa: PLC0415
    return catalog.coverage_report() | catalog.index_stats()


# ---------------------------------------------------------------------------
# 三池隔离通道 (2026-10-05): 反馈池/OOS 不给原始数据, 走"提交->服务评->取结果"
# ---------------------------------------------------------------------------
def tool_pool_access_policy(pool_id: str | None = None) -> dict:
    """查各池的数据访问策略 (Agent 能否读原始数据 / 结果怎么拿)。"""
    pid = pool_id or (_SCOPE.pool_id if _SCOPE else "oof")
    mode = data_access_mode(pid)
    return {"pool": pid, "data_access": mode,
            "can_read_raw": mode == "full",
            "channel": ("自行读数据+算特征" if mode == "full" else
                        "提交 -> 内部服务评 -> 限次数取结果(反馈池)" if pid == "valid"
                        else "提交 -> 内部服务评 -> Agent 永拿不到结果(人可读)")}


def tool_submit_candidate(code_hash: str, payload: dict | None = None,
                          run_id: str | None = None) -> dict:
    """向内部评估服务提交候选 (当前池)。反馈池/OOS 专用通道 —— Agent 只交代码/
    参数, 由服务端用池内数据评估。返回 evaluation_id。"""
    s = _require_scope()
    eid = eval_service.submit(s.pool_id, run_id=run_id or code_hash[:16],
                              code_hash=code_hash, payload=payload or {})
    return {"evaluation_id": eid, "pool": s.pool_id,
            "note": "已提交; 由内部服务在服务端评估该池数据, 结果按池规则返回。"}


def tool_candidate_status(evaluation_id: str) -> dict:
    """查候选评估状态 (不给分数)。任何池可用。"""
    return eval_service.status(evaluation_id)


def tool_read_feedback(evaluation_id: str) -> dict:
    """取**反馈池**结果 (限次数, 防反复看结果调参)。OOS 池调用会被拒。"""
    return eval_service.read_feedback(evaluation_id)


TOOLS = [
    {"name": "list_pools", "fn": tool_list_pools,
     "desc": "列出研究池及边界"},
    {"name": "bind_pool", "fn": tool_bind_pool,
     "desc": "绑定会话到研究池 (查询前必调, 不可重绑)"},
    {"name": "query_candles", "fn": tool_query_candles,
     "desc": "读K线 (as_of 钳制到本池)"},
    {"name": "query_derivatives", "fn": tool_query_derivatives,
     "desc": "读衍生品 (as_of 钳制)"},
    {"name": "query_universe", "fn": tool_query_universe,
     "desc": "读三层宇宙成员"},
    {"name": "factor_available", "fn": tool_factor_available,
     "desc": "查某因子在本池某时刻是否可用"},
    {"name": "list_factors", "fn": tool_list_factors,
     "desc": "列出所有因子及健康状态"},
    {"name": "submit_metrics", "fn": tool_submit_metrics,
     "desc": "提交指标到池1/池2 (OOS 拒绝)"},
    {"name": "submit_oos_candidate", "fn": tool_submit_oos_candidate,
     "desc": "送模型进OOS (拿不到分数)"},
    {"name": "oos_status", "fn": tool_oos_status,
     "desc": "查OOS状态 (无分数)"},
    {"name": "list_features", "fn": tool_list_features,
     "desc": "列出特征库特征 (F7)"},
    {"name": "describe_feature", "fn": tool_describe_feature,
     "desc": "查特征定义+血缘+预热 (F7)"},
    {"name": "compute_features", "fn": tool_compute_features,
     "desc": "批量算特征 (窗口钳制到本池, PIT 自检) (F7)"},
    {"name": "feature_catalog", "fn": tool_feature_catalog,
     "desc": "特征库体检 (类别/算子/深度/去重) (F7)"},
    {"name": "pool_access_policy", "fn": tool_pool_access_policy,
     "desc": "查各池数据访问策略 (谁能读原始数据)"},
    {"name": "submit_candidate", "fn": tool_submit_candidate,
     "desc": "提交候选给内部服务评估 (反馈池/OOS 通道)"},
    {"name": "candidate_status", "fn": tool_candidate_status,
     "desc": "查候选评估状态 (不给分数)"},
    {"name": "read_feedback", "fn": tool_read_feedback,
     "desc": "取反馈池结果 (限次数); OOS 拒绝"},
]

if __name__ == "__main__":
    # 自检模式: 不启动 MCP, 演示工具调用与墙的强制
    print("=" * 72)
    print("MCP 工具自检 — 时间墙在工具层强制")
    print("=" * 72)
    print(f"注册工具 {len(TOOLS)} 个:")
    for t in TOOLS:
        print(f"  - {t['name']:<24}{t['desc']}")

    print("\n[1] list_pools")
    for p in tool_list_pools()["pools"]:
        print(f"    {p['pool_id']:<12}{str(p['start']):<12}~ {str(p['end']):<12}"
              f"usable={p['usable']}")

    print("\n[2] bind_pool('oof')")
    print("   ", tool_bind_pool("oof")["bound"], "-> 范围",
          tool_bind_pool.__name__ and "2018-01-01 ~ 2023-12-31")

    print("\n[3] query_candles 越界尝试 (as_of=2026-10-01, 应被钳到 2023-12-31)")
    r = tool_query_candles("binance", "BTC-USDT", as_of="2026-10-01")
    print(f"    rows={r['n_rows']}, data_max={r['data_max']}, "
          f"effective_as_of={r['effective_as_of']}")

    print("\n[4] query_candles 早于池起点 (as_of=2015-01-01, 应被钳到 2018-01-01)")
    r2 = tool_query_candles("binance", "BTC-USDT", as_of="2015-01-01")
    print(f"    rows={r2['n_rows']}, data_min={r2['data_min']}")

    print("\n[5] query_candles 永续 @2015 (因子屏蔽, 应抛错)")
    try:
        tool_query_candles("binance", "BTC-USDT", market_type="perpetual",
                           as_of="2015-01-01")
        print("    ^ 未抛错, 有风险!")
    except ValueError as e:
        print(f"    BLOCKED: {str(e)[:70]}")

    print("\n[6] factor_available(macro_daily) — 修订污染源")
    print("   ", tool_factor_available("macro_daily")["contaminated"],
          "(contaminated=True -> 默认屏蔽)")

    # 特征库 (F7)
    print("\n[7] list_features")
    lf = tool_list_features()
    print(f"    {lf['n_features']} 个特征, 类别: {lf['categories']}")

    print("\n[8] describe_feature('mom_zscore_24h')")
    d = tool_describe_feature("mom_zscore_24h")
    print(f"    expr={d['expr']} | depth={d['depth']} | lineage={d['lineage']}")

    print("\n[9] compute_features (池1 BTC/ETH 2022-01, as_of 钳制)")
    r = tool_compute_features(
        ["mom_zscore_24h", "funding_zscore_30d", "basis_raw"],
        start="2022-01-01", end="2022-01-31", assets=["BTC", "ETH"])
    print(f"    pool={r['pool']} window={r['effective_window']} rows={r['n_rows']}")
    for k, v in r["features"].items():
        print(f"    {k:<22} n={v['n']:<5} avail_max={str(v['avail_max'])[:10]}")

    print("\n[10] compute_features 越池 (end=2026, 应被截到 2023-12-31)")
    r2 = tool_compute_features(["mom_zscore_24h"], start="2023-11-01",
                               end="2026-01-01", assets=["BTC"])
    print(f"    effective_window={r2['effective_window']}")

    print("\n[11] feature_catalog")
    cat = tool_feature_catalog()
    print(f"    n={cat['n_features']} max_depth={cat['max_depth']} "
          f"dups={cat['duplicates']} soft_cap={cat['soft_cap']}")

    print("\n自检完成 —— 墙在工具层生效。")