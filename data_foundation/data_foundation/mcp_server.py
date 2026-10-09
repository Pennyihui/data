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

import numpy as np
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


# ---------------------------------------------------------------------------
# 标签体系 (2026-10-07 阶段1): 目标/标签注册表 + 切分器 + oof 计算
# ---------------------------------------------------------------------------
def tool_list_objectives() -> dict:
    """列出研究目标 (每个目标下挂多个标签)。"""
    from .labels import list_objectives                          # noqa: PLC0415
    return {"objectives": list_objectives()}


def tool_list_labels(objective: str | None = None) -> dict:
    """列出标签 (可按目标过滤)。同一目标可有多标签。"""
    from .labels import list_labels                              # noqa: PLC0415
    return {"labels": list_labels(objective)}


def tool_describe_label(name: str) -> dict:
    """看一个标签的完整规格: 口径/成本/horizon/指纹。"""
    from .labels import get_label                               # noqa: PLC0415
    spec = get_label(name)
    return {"name": spec.name, "objective": spec.objective,
            "kind": spec.kind, "horizon_bars": spec.horizon_bars,
            "version": spec.version, "research_only": spec.research_only,
            "cost": {"taker_fee": spec.cost.taker_fee,
                     "slippage_bps": spec.cost.slippage_bps,
                     "round_trip": spec.cost.round_trip},
            "fingerprint": spec.fingerprint(), "desc": spec.desc,
            "available_at_rule": "bar[t+H+1] 收盘 (标签是未来函数)"}


def tool_walkforward_plan(pool_id: str | None = None, train_len: str = "3Y",
                          test_len: str = "6M", step: str = "6M",
                          horizon: str = "10D",
                          embargo: str | None = None) -> dict:
    """看池内 walk-forward 切分计划 (purge+embargo 已含在 train_end 里)。"""
    from .labels import walk_forward_splits                    # noqa: PLC0415
    pid = pool_id or (_SCOPE.pool_id if _SCOPE else "oof")
    folds = walk_forward_splits(pid, train_len=train_len,
                                test_len=test_len, step=step,
                                horizon=horizon, embargo=embargo)
    return {"pool": pid, "n_folds": len(folds),
            "horizon_days": folds[0].horizon_days if folds else None,
            "embargo_days": folds[0].embargo_days if folds else None,
            "folds": [f.to_dict() for f in folds]}


def tool_label_fingerprint(name: str, pool_id: str | None = None,
                           data_fingerprint: str = "") -> dict:
    """算标签的落盘指纹 (规格+数据+池 合成) —— 提交载荷需要它。"""
    from .labels import label_fingerprint                      # noqa: PLC0415
    pid = pool_id or (_SCOPE.pool_id if _SCOPE else "oof")
    return {"label": name, "pool": pid,
            "fingerprint": label_fingerprint(name, pool_id=pid,
                                             data_fingerprint=data_fingerprint)}


# ---------------------------------------------------------------------------
# 评价协议 (2026-10-07): 试验计数 N + 显著性自检 (dev 侧)
# ---------------------------------------------------------------------------
def tool_trial_count(pool_ids: list[str] | None = None) -> dict:
    """全局试验计数 N (含开发池) —— DSR 的输入。N 失真则 DSR 失真。"""
    from .training.experiments import trial_count               # noqa: PLC0415
    n = trial_count(tuple(pool_ids) if pool_ids else None)
    return {"n_trials": n, "note": "去重 run_id; 开发池试验同样计入"}


def tool_significance_check(period_returns: list[float], n_trials: int = 1,
                            sharpe: float | None = None,
                            periods_per_year: float = 365 * 24) -> dict:
    """对一条逐期收益序列算 PSR/DSR (多重检验修正)。"""
    from .evaluation.significance import significance_metrics  # noqa: PLC0415
    import pandas as pd                                          # noqa: PLC0415
    return significance_metrics(pd.Series(period_returns),
                                n_trials=n_trials, sharpe=sharpe,
                                periods_per_year=periods_per_year)


# ---------------------------------------------------------------------------
# 研究链算子 (2026-10-07): 价格面板 -> 标签 -> 样本 -> 训练 -> 回测 -> 评价
#
# 这一组把 labels/ training/ evaluation/ 三个包挂成**服务工具**。设计文档:
#   docs/label-system-design.md           (标签/切分)
#   docs/supervised-learning-protocol-design.md (训练/注册/信号)
#   docs/evaluation-protocol-design.md    (三层指标)
#
# 池纪律沿用既有不变式:
#   * 标签是未来函数 -> **视同原始数据**, valid/oos 一律拒 (原则5)
#   * 训练只发生在 oof -> valid/oos 的 build/train 直接 PermissionError
#   * valid/oos 的评价由**服务端** evaluate_submission 执行, Agent 只提交
# ---------------------------------------------------------------------------
_LABEL_CACHE: dict = {}          # label_name -> LabelResult (会话内缓存)
_SAMPLES_CACHE: dict = {}        # run_id -> {fold_id: {"train","test"}}
_SCORES_CACHE: dict = {}         # run_id -> 预测分数面板 (回测/评价用)
_RESULTS_CACHE: dict = {}        # run_id -> BacktestResult
_RL_POLICY: dict = {}            # model_id -> {"policy","env","run_id"}


def _require_dev_pool(what: str) -> PoolScope:
    """训练/标签类操作只允许在开发池 (valid/oos 由服务端做)。"""
    s = _require_scope()
    if s.pool_id != "oof":
        raise PermissionError(
            f"{what} 只在开发池 (oof) 可做; 当前会话绑定的是 {s.pool_id}。"
            f"反馈池/OOS 必须走『提交 -> 内部服务评 -> (限次数/永不)取结果』通道。")
    return s


def _panel_engine(s: PoolScope, start=None, end=None, assets=None):
    """建特征引擎并**先装载特征库** (底座 bug: FeatureEngine.__init__ 里
    _stable_warmup() 先于 load_library(), 新进程会 ValueError)。这里是绕过,
    不是修改底座 —— 底座要不要修由人决定。"""
    engine_mod, _, registry = _feature_module()
    registry.load_library()
    return engine_mod.FeatureEngine(s, start=start, end=end, assets=assets)


def tool_load_price_panel(start: str | None = None, end: str | None = None,
                          assets: list[str] | None = None,
                          interval: str = "1d") -> dict:
    """载入**价格面板** (标签的输入): 认证层 OHLCV, MultiIndex(asset, time)。

    与 compute_features 的区别: 那个返回**特征**面板, 这个返回**原始价格**
    面板 —— 标签 (未来收益) 必须从价格算, 它不是特征。
    interval 目前支持 1d (标签决策格)。返回面板统计 + 缓存句柄。
    """
    s = _require_scope()
    assert_can_read_data(s.pool_id, "原始数据 (价格面板)")
    field_map = {"1d": ("cd_open", "cd_high", "cd_low", "cd_close",
                        "cd_volume_quote"),
                 "1h": ("open", "high", "low", "close", "volume_quote")}
    if interval not in field_map:
        raise ValueError(f"interval 目前支持 {sorted(field_map)}")
    names = list(field_map[interval])
    eng = _panel_engine(s, start=start, end=end, assets=assets)
    panel = eng.panel(names)
    v = panel.values.dropna(how="all")
    if v.empty:
        raise ValueError("价格面板为空 —— 检查窗口/资产/字段可用性")
    key = f"{interval}|{start}|{end}|{len(assets or [])}"
    _LABEL_CACHE[f"__panel__{key}"] = panel
    tt = v.index.get_level_values("time")
    return {"pool": s.pool_id, "interval": interval, "panel_key": key,
            "n_rows": int(v.shape[0]),
            "n_assets": int(v.index.get_level_values("base_asset").nunique()),
            "time_range": [str(tt.min().date()), str(tt.max().date())],
            "columns": list(v.columns),
            "excluded": panel.excluded[:5],
            "note": "面板已缓存; compute_labels 用 panel_key 取用"}


def _get_panel(panel_key: str):
    p = _LABEL_CACHE.get(f"__panel__{panel_key}")
    if p is None:
        raise KeyError(f"panel_key {panel_key!r} 不在会话缓存里; "
                       f"请先调 load_price_panel。可用: "
                       f"{[k.replace('__panel__', '') for k in _LABEL_CACHE if k.startswith('__panel__')]}")
    return p


def tool_compute_labels(panel_key: str, name: str = "ret_10d",
                        save: bool = True) -> dict:
    """在价格面板上算标签 (阶段1)。返回标签分布统计 + 可用时间语义。

    标签是**未来函数** (t 的标签用到 t+H+1 的价格) —— 因此 valid/oos 拒算
    (标签视同原始数据, 拿到标签≈拿到未来收益)。
    """
    s = _require_dev_pool("算标签")
    from .labels import compute_labels, get_label, save_label   # noqa: PLC0415
    panel = _get_panel(panel_key)
    spec = get_label(name)
    v = panel.values
    px = pd.DataFrame({"open": v["cd_open"] if "cd_open" in v else v["open"],
                       "close": v["cd_close"] if "cd_close" in v else v["close"]})
    px = px.dropna().sort_index()
    grid = 1.0 if spec.horizon_bars and "1d" in panel_key else 1.0
    res = compute_labels(spec, px, grid_days=grid)
    _LABEL_CACHE[name] = res
    out = {"label": name, "objective": spec.objective, "kind": spec.kind,
           "horizon_bars": res.horizon_bars,
           "cost_round_trip": spec.cost.round_trip,
           "n_rows": int(len(res.values)),
           "n_labeled": int(res.values.notna().sum()),
           "mean": float(res.values.mean()),
           "std": float(res.values.std()),
           "pos_rate": float((res.values.dropna() > 0).mean()),
           "available_at_rule": "bar[t+H+1] 收盘 (未来函数)",
           "label_key": name}
    if save:
        meta = save_label(res, pool_id=s.pool_id,
                          assets=f"{px.index.get_level_values(0).nunique()}")
        out["fingerprint"] = meta["fingerprint"]
        out["saved"] = True
    else:
        out["saved"] = False
    return out


def tool_build_training_samples(features: list[str], label_name: str = "ret_10d",
                                panel_key: str | None = None,
                                train_len: str = "3Y", test_len: str = "6M",
                                step: str = "6M", embargo: str | None = None,
                                start: str | None = None,
                                end: str | None = None,
                                run_id: str | None = None) -> dict:
    """构建训练样本 (阶段2 的唯一对齐处, 带双断言)。按 walk-forward 折产出。

    X 来自**特征引擎** (features 必须是特征库里的特征名); 面板用 panel_key 定位
    (价格面板与标签共用同一坐标系)。返回 run_id (后续 train_model /
    run_backtest / evaluate_candidate 都用它)。
    """
    s = _require_dev_pool("构建训练样本")
    from .labels import walk_forward_splits                        # noqa: PLC0415
    from .training import build_sample_set                          # noqa: PLC0415
    rid = run_id or f"run-{label_name}-{len(_SAMPLES_CACHE) + 1}"
    lab = _LABEL_CACHE.get(label_name)
    if lab is None:
        raise KeyError(f"标签 {label_name!r} 未算; 请先调 compute_labels")
    if not features:
        raise ValueError("features 不能为空 (X 来自特征引擎)")
    eng = _panel_engine(s, start=start, end=end)
    try:
        bundle = eng.compute(list(features))
    except KeyError as e:
        raise KeyError(
            f"特征名不在特征库里: {e}。X 必须是**已注册特征**(不是价格字段);"
            f"先用 list_features 看有哪些, 或 describe_feature 看口径。"
            f"若只是想要价格/成交量这类原始字段, 用 load_price_panel。") from e
    X, avail = bundle.values, bundle.avail
    # 对齐: 标签面板与特征面板的资产/时间覆盖天然不同 (价格面板带预热、特征面板
    # 按请求窗口裁剪)。供给层要求**严格同索引** (那是防错配的硬检查, 保留),
    # 因此在这里显式取交集 —— 交集外一律不参与训练, 不做任何填充。
    common = X.index.intersection(lab.values.index)
    if len(common) == 0:
        raise ValueError(
            "标签与特征没有共同决策格: 标签面板与特征面板的资产/时间不重叠。"
            f"标签 {len(lab.values):,} 行 vs 特征 {len(X):,} 行 —— "
            f"检查窗口与资产设置是否一致。")
    X, avail = X.loc[common], avail.loc[common]
    y_aligned, ya_aligned = lab.values.loc[common], lab.available_at.loc[common]
    folds = walk_forward_splits(s.pool_id, train_len=train_len,
                                test_len=test_len, step=step,
                                label=label_name, embargo=embargo,
                                start=start, end=end)
    pairs, skipped = {}, []
    for f in folds:
        try:
            tr = build_sample_set(X, avail, y_aligned, ya_aligned,
                                  fold=f, role="train", label_name=label_name,
                                  pool_id=s.pool_id)
            te = build_sample_set(X, avail, y_aligned, ya_aligned,
                                  fold=f, role="test", label_name=label_name,
                                  pool_id=s.pool_id)
        except Exception as e:                       # noqa: BLE001
            skipped.append({"fold": f.fold_id, "reason": f"{type(e).__name__}: {e}"})
            continue
        if len(tr) and len(te):
            pairs[f.fold_id] = {"train": tr, "test": te}
        else:
            skipped.append({"fold": f.fold_id,
                            "reason": f"empty (train={len(tr)} test={len(te)})"})
    if not pairs:
        raise ValueError(f"没有可用折: {skipped[:3]}")
    _SAMPLES_CACHE[rid] = pairs
    return {"run_id": rid, "pool": s.pool_id, "n_features": X.shape[1],
            "n_common_grid": int(len(common)),
            "n_folds": len(pairs), "skipped": skipped[:5],
            "folds": [{"fold_id": k, "train_rows": len(v["train"]),
                       "test_rows": len(v["test"]),
                       "fingerprint": v["train"].fingerprint[:12]}
                      for k, v in pairs.items()]}


def tool_train_model(run_id: str, family: str = "linear",
                     label_name: str = "ret_10d", task: str = "regression",
                     register: bool = True, params: dict | None = None,
                     model_id: str | None = None) -> dict:
    """walk-forward 训练 (每折只用 train 折训练), 可选注册到模型注册表。

    family: linear / gbdt / baseline。超参走 params 字典 (alpha / n_estimators ...),
    不做 **kwargs 展开 —— 避免与工具参数名撞车。
    task: regression (拟合标签值) / classification (拟合涨跌符号)。
    model_id: 同一份样本 (run_id) 上可训练多个模型, 各自缓存分数/结果。
             默认与 run_id 相同 (会覆盖上一次的分数缓存)。
    """
    s = _require_dev_pool("训练模型")
    from .training import (make_model, register_model)            # noqa: PLC0415
    # task 归一化: 容忍 reg/regi/regression 与 cls/class/classification 简写
    # (否则 "cls" 与 "classification" 不匹配 -> 分类目标函数静默失效, 实测
    #  ridge 与 logreg 给出逐位相同的结果)
    _t = str(task).lower()
    task = ("classification" if _t in ("cls", "class", "classification")
            else "regression")
    pairs = _SAMPLES_CACHE.get(run_id)
    if pairs is None:
        raise KeyError(f"run_id {run_id!r} 无样本; 请先调 build_training_samples")
    params = dict(params or {})
    mid = model_id or run_id
    # task 若是 classification 且家族支持 -> 透传给模型构造器
    # (否则 linear 的 logreg 会退化成 ridge, 分类目标函数形同没设)
    if task == "classification" and family in ("linear", "gbdt", "nn"):
        params.setdefault("task", "classification")

    def factory():
        return make_model(family, **params)

    # train/test 分离已在 build_training_samples 完成 (供给层双断言保证)
    res = _run_walkforward_pairs(list(pairs.values()), factory, task)
    _SCORES_CACHE[mid] = res.scores
    # **每次训练都记实验账本** (原则6): 否则 trial_count 恒为 0, DSR 的 N
    # 完全失真 —— 多重检验修正形同虚设。RL 训练尤其依赖它。
    from .training import ExperimentRecord, record_experiment  # noqa: PLC0415
    record_experiment(ExperimentRecord(
        run_id=mid, code_hash="", model_family=family,
        model_params=dict(params), label_name=label_name,
        fold_plan=f"{len(res.fold_ids)} folds", pool_id=s.pool_id,
        metrics={"n_folds": len(res.fold_ids), "n_scores": int(len(res.scores))},
        note=f"task={task}"))
    out = {"run_id": run_id, "model_id": mid, "family": family, "task": task,
           "n_folds": len(res.fold_ids), "n_scores": int(len(res.scores)),
           "folds": [f.to_dict() for f in res.folds]}
    if register:
        first = pairs[res.fold_ids[0]]["train"]
        y_first = first.y
        if task == "classification":
            y_first = y_first.where(y_first.notna()).gt(0).astype(float)
        m = make_model(family, **params)
        m.fit(first.X, y_first)
        art = register_model(mid, m, params=params,
                             label_name=label_name,
                             train_fold=res.fold_ids[0])
        out["model_hash"] = art.model_hash
        out["version"] = art.version
    return out


def _run_walkforward_pairs(pairs_list, factory, task):
    """对已切好的 {train,test} 折列表跑 walk-forward (模型协议层同一语义)。"""
    import pandas as pd
    from .training.walkforward import FoldResult, WalkForwardResult
    fold_results, all_s, all_y, ids = [], [], [], []
    for pair in pairs_list:
        tr, te = pair["train"], pair["test"]
        if len(tr) == 0 or len(te) == 0:
            continue
        model = factory()
        if task == "classification":
            y_tr = tr.y.where(tr.y.notna()).gt(0).astype(float)
        else:
            y_tr = tr.y
        model.fit(tr.X, y_tr)
        pred = pd.Series(model.predict(te.X), index=te.X.index, name="score")
        fid = tr.meta.fold_id or f"fold{len(fold_results)}"
        fold_results.append(FoldResult(fold_id=fid, train_rows=len(tr.X),
                                        test_rows=len(te.X), scores=pred,
                                        train_span=tr.meta.fold_id))
        all_s.append(pred)
        all_y.append(te.y)
        ids.append(fid)
    return WalkForwardResult(fold_results,
                             pd.concat(all_s) if all_s else pd.Series(dtype=float),
                             pd.concat(all_y) if all_y else pd.Series(dtype=float),
                             ids)


def tool_predict_with_model(run_id: str, model_name: str | None = None,
                            model_hash: str | None = None,
                            features: list[str] | None = None,
                            start: str | None = None,
                            end: str | None = None) -> dict:
    """用注册模型在**当前池**的特征面板上预测 -> 分数面板摘要。"""
    s = _require_scope()
    assert_can_read_data(s.pool_id, "原始数据 (预测)")
    from .training import load_model                             # noqa: PLC0415
    name = model_name or run_id
    model, art = load_model(name, model_hash)
    eng = _panel_engine(s, start=start, end=end)
    names = features or [n for n in art.params.get("features", [])] or None
    if not names:
        raise ValueError("请给 features (或模型注册时带特征名)")
    bundle = eng.compute(list(names))
    pred = pd.Series(model.predict(bundle.values),
                     index=bundle.values.index, name="score")
    _SCORES_CACHE.setdefault(run_id, pred)
    return {"run_id": run_id, "model": name, "model_hash": art.model_hash,
            "n_scores": int(len(pred)),
            "score_mean": float(pred.mean()), "score_std": float(pred.std()),
            "time_range": [str(pred.index.get_level_values("time").min().date()),
                           str(pred.index.get_level_values("time").max().date())]}


def tool_run_backtest(run_id: str, mode: str = "rank_linear", top_n: int = 5,
                      long_short: bool = False, rebalance_every: int = 5,
                      max_weight: float = 0.05, max_gross: float = 1.0,
                      panel_key: str | None = None,
                      max_abs_daily_return: float = 0.5,
                      start: str | None = None, end: str | None = None) -> dict:
    """分数面板 -> PanelSignal -> 仓位映射 -> 回测引擎 (同一引擎/成本)。

    mode: rank_linear (排名线性权重) / top_n (前N等权, 对照基准)。
    max_abs_daily_return: 资产筛选 —— 剔除窗口内单日涨跌幅超过该阈值的资产。
        加密市场存在暴涨/暴跌近千倍的标的, 而回测的权重法权益是
        `equity *= (1 + Σw·r)` 的**无破产保护**复利: 一次 r=+999 就能让权益
        乘 100 倍, 一次 pnl<-1 直接把权益打成负数 (之后 cagr/sharpe 全废)。
        这是实测踩到的真问题, 不是数值噪声 —— 默认剔除极端日收益资产。
    """
    s = _require_dev_pool("回测")
    from .backtest.data_engine import build_data                  # noqa: PLC0415
    from .backtest.engine import BacktestEngine                   # noqa: PLC0415
    from .backtest.execution_engine import CostModel              # noqa: PLC0415
    from .backtest.strategy import RiskEngine                     # noqa: PLC0415
    from .training import ScoreWeightedStrategy, scores_to_panel_signal  # noqa
    scores = _SCORES_CACHE.get(run_id)
    if scores is None:
        raise KeyError(f"run_id {run_id!r} 无预测分数; 请先 train_model")
    panel = _get_panel(panel_key) if panel_key else None
    if panel is None:
        # 没有指定就现取会话里的第一个价格面板
        keys = [k.replace("__panel__", "") for k in _LABEL_CACHE
                if k.startswith("__panel__")]
        if not keys:
            raise KeyError("需先 load_price_panel (回测要价格面板)")
        panel = _get_panel(keys[0])
    # build_data 约定列名为 open/high/low/close/volume_quote; 价格面板里是
    # 认证字段名 (cd_open/...) -> 这里做**列名映射**, 不改底座代码。
    from .fields import Panel as _FieldsPanel                    # noqa: PLC0415
    ren = {"cd_open": "open", "cd_high": "high", "cd_low": "low",
           "cd_close": "close", "cd_volume_quote": "volume_quote"}
    v2 = panel.values.rename(columns={k: v2 for k, v2 in ren.items()})
    a2 = panel.avail.rename(columns={k: v2 for k, v2 in ren.items()})
    bp = _FieldsPanel(values=v2, avail=a2, scope=panel.scope,
                      as_of=panel.as_of, start=panel.start, end=panel.end,
                      warmup=panel.warmup, provenance=panel.provenance,
                      excluded=panel.excluded)
    data = build_data(bp)
    # 只在分数有值的时间上交易: 用分数面板的时间索引过滤
    st = scores.index.get_level_values("time")
    keep = data.times.isin(sorted(set(st)))
    if int(keep.sum()) < 2:
        raise ValueError("分数与价格时间不重叠 —— 检查 run_backtest 的窗口")
    idx = np.where(keep)[0]
    # **可变资产集** (2026-10-07 修复): 引擎现在支持逐日 PIT 宇宙的可变资产集合,
    # 因此**不再**强制"窗口内全程有数据"的固定资产集 —— 那样会把晚上市/早退场
    # 的币整体剔除, 削弱 PIT 语义 (它们本该"上市那天进宇宙、退市那天退场")。
    # 只保留**极端日收益资产**的剔除 (那个是数值病态防护, 与 PIT 无关)。
    from .backtest.data_engine import BacktestData                # noqa: PLC0415
    close_sel = data.close[idx]
    rets = np.abs(np.nan_to_num(close_sel[1:] / close_sel[:-1] - 1.0,
                                 nan=0.0, posinf=0.0, neginf=0.0))
    max_ret = rets.max(axis=0) if rets.size else np.zeros(len(data.assets))
    tradable = [a for a, mr in zip(data.assets, max_ret)
                if np.isfinite(mr) and mr <= float(max_abs_daily_return)]
    n_dropped_extreme = len(data.assets) - len(tradable)
    cols = np.array([data.asset_index[a] for a in tradable])
    uni = np.isfinite(data.close[np.ix_(idx, cols)])     # 逐日 PIT 宇宙
    d2 = BacktestData(
        open=data.open[np.ix_(idx, cols)],
        high=data.high[np.ix_(idx, cols)],
        low=data.low[np.ix_(idx, cols)],
        close=data.close[np.ix_(idx, cols)],
        volume=data.volume[np.ix_(idx, cols)],
        available_at=data.available_at[idx], times=data.times[idx],
        assets=tuple(tradable), universe=uni,
        asset_index={a: i for i, a in enumerate(tradable)})
    sig = scores_to_panel_signal(scores)
    strat = ScoreWeightedStrategy(sig, mode=mode, top_n=top_n,
                                  long_short=long_short,
                                  rebalance_every=rebalance_every)
    eng = BacktestEngine(d2, strat, risk=RiskEngine(max_weight=max_weight,
                                                    max_gross=max_gross),
                         cost=CostModel())
    result = eng.run()
    _RESULTS_CACHE[run_id] = result
    m = result.portfolio.metrics(periods_per_year=252)
    # 数值病态自检: 无破产保护的权重法权益遇到极端收益就会爆 —— 如实标记,
    # 不让病态数字流进指标层 (schema 门会因 max_drawdown 越界而拒绝入库)。
    finite = np.isfinite(float(m.get("total_return", np.nan)))
    pathological = (not finite) or abs(float(m.get("total_return", 0.0))) > 100.0
    out = {"run_id": run_id, "mode": mode, "long_short": long_short,
           "rebalance_every": rebalance_every,
           "n_assets": len(full_assets),
           "n_dropped_extreme": n_dropped_extreme,
           "max_abs_daily_return_filter": float(max_abs_daily_return),
           "n_bars": int(m.get("n_points", 0)),
           "time_range": [str(d2.times[0].date()), str(d2.times[-1].date())],
           "pathological_equity": pathological,
           "metrics": {k: (float(v) if isinstance(v, (int, float)) else v)
                       for k, v in m.items()}}
    if pathological:
        out["warning"] = (
            "权益数值病态 (无破产保护的权重法复利遇到极端日收益): "
            f"total_return={m.get('total_return')}, mdd={m.get('max_drawdown')}. "
            f"已剔除 {n_dropped_extreme} 个极端日收益资产; 若仍异常, 需收紧 "
            f"max_abs_daily_return 或复核数据质量。")
    return out


def tool_evaluate_candidate(run_id: str, label_name: str = "ret_10d",
                            n_trials: int | None = None,
                            with_backtest: bool = True,
                            note: str = "") -> dict:
    """三层评价 (唯一代码路径): 预测层 + 交易层 + 显著性层 (PSR/DSR)。

    n_trials 默认取全局试验计数 N (含开发池) —— **N 失真则 DSR 失真**。
    """
    s = _require_dev_pool("评价")
    from .evaluation import evaluate_candidate                    # noqa: PLC0415
    from .training import trial_count                             # noqa: PLC0415
    scores = _SCORES_CACHE.get(run_id)
    if scores is None:
        raise KeyError(f"run_id {run_id!r} 无预测分数; 请先 train_model")
    lab = _LABEL_CACHE[label_name]
    # 对齐到共同决策格 (分数与标签的覆盖可能不同), 并**拒绝**重复索引:
    # 重复说明某一折的预测被写了两遍 —— 那是真问题, 不能静默去重。
    common = scores.index.intersection(lab.values.index)
    if scores.index.has_duplicates:
        dup = scores.index[scores.index.duplicated()][:3].tolist()
        raise ValueError(f"预测分数面板有重复决策格 (前 3 个: {dup}) —— "
                         f"多半是折之间测试窗重叠, 检查 build_training_samples 的 "
                         f"train_len/test_len/step 参数。")
    if len(common) == 0:
        raise ValueError("预测分数与标签没有共同决策格")
    sc = scores.loc[common]
    yy = lab.values.loc[common]
    N = n_trials if n_trials is not None else max(1, trial_count())
    pf = None
    pr = None
    if with_backtest:
        res = _RESULTS_CACHE.get(run_id)
        if res is not None:
            pf = res.portfolio.metrics(periods_per_year=252)
            pr = pd.Series(res.portfolio.period_returns)
    metrics = evaluate_candidate(sc, yy, portfolio=pf,
                                period_returns=pr, periods_per_year=252,
                                n_trials=N)
    metrics["extra"] = {"run_id": run_id, "pool": s.pool_id,
                        "label_name": label_name, "note": note}
    return metrics


def tool_list_models() -> dict:
    """列出已注册模型 (模型是第一类对象: 指纹 + 训练窗口 + 标签)。"""
    from .training import list_models as _lm                      # noqa: PLC0415
    return {"models": [{k: m.get(k) for k in
                        ("name", "family", "version", "model_hash",
                         "label_name", "train_fold", "created_at")}
                       for m in _lm()]}


# ---------------------------------------------------------------------------
# 强化学习 (2026-10-07): Env 复用回测引擎, 训练只发生在开发池
# 设计: docs/reinforcement-learning-design.md
# ---------------------------------------------------------------------------
def _rl_env(s: PoolScope, run_id: str, *, clip_reward: float = 0.05,
            turnover_penalty: float = 0.002, reward_kind: str = "log_return",
            max_weight: float = 0.05, start: str | None = None,
            end: str | None = None, action_mode: str = "weights",
            top_k: int = 10, assets: list[str] | None = None):
    """按某个 run_id 的训练窗口构造 RL 环境 (同一段历史, 与监督学习可比)。"""
    from .rl import PortfolioEnv                                   # noqa: PLC0415
    from .backtest.execution_engine import CostModel               # noqa: PLC0415
    from .backtest.strategy import RiskEngine                      # noqa: PLC0415
    pairs = _SAMPLES_CACHE.get(run_id)
    if not pairs:
        raise KeyError(f"run_id {run_id!r} 无样本; 请先调 build_training_samples")
    keys = [k.replace("__panel__", "") for k in _LABEL_CACHE
            if k.startswith("__panel__")]
    if not keys:
        raise KeyError("需先 load_price_panel (Env 要价格面板)")
    panel = _get_panel(keys[0])
    v = panel.values
    ren = {"cd_open": "open", "cd_high": "high", "cd_low": "low",
           "cd_close": "close", "cd_volume_quote": "volume_quote"}
    px = v.rename(columns=ren).dropna(subset=["open", "close"])
    if assets:
        px = px[px.index.get_level_values("base_asset").isin(set(assets))]
        if len(px) == 0:
            raise ValueError(f"指定的资产在池内没有价格数据: {assets[:5]}")
    # 因子面板: 复用会话缓存的分数面板不可行(那是预测), 这里取最简状态
    # (只含持仓+现金) —— 因子状态留给后续按需扩展。
    return PortfolioEnv(px, None, cost=CostModel(),
                        risk=RiskEngine(max_weight=max_weight, max_gross=1.0),
                        clip_reward=clip_reward,
                        turnover_penalty=turnover_penalty,
                        reward_kind=reward_kind,
                        action_mode=action_mode, top_k=top_k)


def tool_train_rl(run_id: str, *, hidden: int = 64, lr: float = 3e-4,
                  gamma: float = 0.99, clip: float = 0.2, epochs: int = 4,
                  n_steps: int = 256, batch: int = 64, seed: int = 0,
                  clip_reward: float = 0.05, turnover_penalty: float = 0.002,
                  action_mode: str = "scores", top_k: int = 10,
                  assets: list[str] | None = None,
                  model_id: str | None = None) -> dict:
    """强化学习训练 (PPO)。环境底层复用回测引擎, 训练只发生在开发池。

    action_mode: "scores" (默认, **结构化动作**: 打分 -> top-k 权重, PPO 可学)
                 / "weights" (每资产一个连续权重, 342 维时 PPO 学不动 ——
                 实测训练回报不涨; 保留作对照)。
    assets: 限定资产子集 (先用 50 个币验证 RL 能否学习, 再扩全市场)。
    奖励 = 缩尾的成本后收益 − λ·换手 (成本与回测/标签同源)。
    """
    s = _require_dev_pool("RL 训练")
    from .rl import train_ppo                                       # noqa: PLC0415
    env = _rl_env(s, run_id, clip_reward=clip_reward,
                  turnover_penalty=turnover_penalty,
                  action_mode=action_mode, top_k=top_k, assets=assets)
    pol, hist = train_ppo(env, hidden=hidden, lr=lr, gamma=gamma, clip=clip,
                          epochs=epochs, n_steps=n_steps, batch=batch,
                          seed=seed)
    mid = model_id or f"rl-{run_id}"
    _RL_POLICY[mid] = {"policy": pol, "env": env, "run_id": run_id}
    rewards = [h["reward"] for h in hist]
    # **RL 训练计入试验次数 N** (设计原则6): 超参搜索是 N 的主要来源,
    # 不记就等于多重检验修正失效
    from .training import ExperimentRecord, record_experiment   # noqa: PLC0415
    record_experiment(ExperimentRecord(
        run_id=mid, code_hash="", model_family="rl_ppo",
        model_params={"hidden": hidden, "lr": lr, "gamma": gamma,
                      "action_mode": action_mode, "top_k": top_k,
                      "turnover_penalty": turnover_penalty},
        pool_id=s.pool_id,
        metrics={"train_rewards": rewards},
        note=f"RL epochs={epochs} n_steps={n_steps}"))
    return {"model_id": mid, "run_id": run_id, "algorithm": "ppo",
            "action_mode": action_mode, "top_k": top_k,
            "state_dim": env.state_dim, "n_actions": env.n_actions,
            "n_assets": env.n_asset, "n_time": env.n_time,
            "epochs": epochs, "train_rewards": [round(r, 6) for r in rewards],
            "reward_improved": (len(rewards) > 1 and rewards[-1] > rewards[0]),
            "bankrupt": env.portfolio.bankrupt,
            "note": "环境=回测引擎组件; 成本与标签/回测同源"}


def tool_eval_rl_policy(model_id: str, *, mode: str = "greedy",
                        metrics: bool = True) -> dict:
    """用训练好的 RL 策略跑一段并给出与监督学习同格式的成绩单。"""
    s = _require_dev_pool("RL 评估")
    import torch                                                # noqa: PLC0415
    rec = _RL_POLICY.get(model_id)
    if rec is None:
        raise KeyError(f"model_id {model_id!r} 未训练; 请先调 train_rl")
    pol, env = rec["policy"], rec["env"]
    env.reset()
    done = False
    while not done:
        with torch.no_grad():
            ot = torch.tensor(env._state(env.t), dtype=torch.float32).unsqueeze(0)
            a = (pol.actor(pol.net(ot)).squeeze(0).numpy()
                 * getattr(pol, "action_scale", 1.0))
        _, _, done, _ = env.step(a)
    m = env.portfolio_metrics(periods_per_year=252)
    out = {"model_id": model_id, "policy": "greedy",
           "reward_total": env.total_reward,
           "action_mode": getattr(env, "action_mode", "weights"),
           "n_bars": int(m.get("n_points", 0)),
           "bankrupt": bool(getattr(env.portfolio, "bankrupt", False)),
           "clamped_high": bool(getattr(env.portfolio, "clamped_high", False)),
           "metrics": {k: (float(v) if isinstance(v, (int, float)) else v)
                       for k, v in m.items()}}
    return out


def tool_rl_env_info(run_id: str) -> dict:
    """看 RL 环境规格 (state 维度/资产数/时间跨度) —— 研究前先体检。"""
    s = _require_dev_pool("RL 环境")
    env = _rl_env(s, run_id)
    return {"run_id": run_id, "state_dim": env.state_dim,
            "n_factor": env.n_factor, "n_actions": env.n_actions,
            "n_assets": env.n_asset, "n_time": env.n_time,
            "action_mode": getattr(env, "action_mode", "weights"),
            "clip_reward": env.clip_reward,
            "turnover_penalty": env.turnover_penalty,
            "reward_kind": env.reward_kind}


# ---------------------------------------------------------------------------
# 服务端评估通道 (valid/oos): 三池纪律"提交 -> 服务端评 -> 取结果"的最后一段
# 设计: docs/evaluation-protocol-design.md §4 / supervised-learning-... §10
# 之前 evaluate_submission 实现了但**没挂工具** —— 反馈池/OOS 的流程在"提交
# 之后"断掉 (银行后台建好了结算系统, 营业厅没开窗口)。这里补上这一段。
# ---------------------------------------------------------------------------
def _server_n_trials() -> int:
    from .training.experiments import trial_count            # noqa: PLC0415
    return max(1, trial_count())


def tool_evaluate_submission(evaluation_id: str) -> dict:
    """服务端执行一次已提交候选的评估 (valid/oos)。

    流程: 读提交载荷 -> 服务端取池内特征/标签 -> 应用已注册模型 -> 预测 ->
    三层指标 -> 写回 eval_service。**只有本工具能评 valid/oos** (Agent 无池内
    数据, 物理上无法自评 —— 这是三池隔离的服务端那一半)。
    """
    from .evaluation.runner import evaluate_submission        # noqa: PLC0415
    _es = eval_service                                     # 模块顶部已导入
    subs = [r for r in _es._read_all(_es.SUBMISSIONS)
            if r["evaluation_id"] == evaluation_id]
    if not subs:
        raise KeyError(f"未知 evaluation_id: {evaluation_id}")
    rec = subs[-1]
    pool = rec["pool"]
    if pool not in ("valid", "oos", "rolling_oos"):
        raise ValueError(f"池 {pool} 不走服务端评估 (开发池自行 compute+evaluate)")
    payload = dict(rec.get("payload") or {})
    s = PoolScope(pool)
    from .labels import compute_labels, get_label             # noqa: PLC0415
    keys = [k.replace("__panel__", "") for k in _LABEL_CACHE
            if k.startswith("__panel__")]
    label_name = payload.get("label_name") or "ret_10d"
    spec = get_label(label_name)
    payload["label_name"] = label_name
    # **标签必须从被评估的那个池取数** —— 用会话里缓存的别的池面板会与池内
    # 特征的时间完全不重叠 (oof 是 2018-2023, valid 是 2024-2025), 对齐后为空。
    panel = _get_panel_from_pool(s)
    v = panel.values
    ocol = "cd_open" if "cd_open" in v else "open"
    ccol = "cd_close" if "cd_close" in v else "close"
    px = pd.DataFrame({"open": v[ocol], "close": v[ccol]}).dropna().sort_index()
    lab = compute_labels(spec, px)
    names = payload.get("features") or []
    if not names:
        raise ValueError("提交载荷必须带 features (服务端据此自行取特征)")
    eng = _panel_engine(s)
    bundle = eng.compute(list(names))
    # **特征指纹由服务端按实际算出的特征集算**, 而不是相信提交方声明 ——
    # 否则 Agent 声明一个指纹就能绕过"训练/评估特征错配"这道闸。
    from .features.cache import data_fingerprint as _dfp      # noqa: PLC0415
    payload["feature_fingerprint"] = _dfp(sorted(names))
    metrics = evaluate_submission(
        pool, payload, features=bundle.values, features_avail=bundle.avail,
        labels=lab.values, label_avail=lab.available_at,
        n_trials=_server_n_trials(), root=None,
        feature_fingerprint_override=True)
    _es.evaluate(evaluation_id, metrics, evaluated_by="eval_service",
                 notes=f"server-side {pool}")
    return {"evaluation_id": evaluation_id, "pool": pool,
            "status": "evaluated", "metrics": metrics,
            "note": "valid 限 2 次取结果; oos Agent 永不返回 (人审)"}


def tool_server_evaluate_pool(pool_id: str, model_name: str,
                              label_name: str, features: list[str],
                              model_hash: str | None = None) -> dict:
    """服务端直接评估一个已注册模型 (不经 eval_service 提交记录)。"""
    from .evaluation.runner import evaluate_submission        # noqa: PLC0415
    s = _require_scope()
    if pool_id not in ("valid", "oos", "rolling_oos"):
        raise ValueError("服务端评估只适用于 valid/oos/rolling_oos")
    from .labels import compute_labels, get_label             # noqa: PLC0415
    keys = [k.replace("__panel__", "") for k in _LABEL_CACHE
            if k.startswith("__panel__")]
    panel = _get_panel(keys[0]) if keys else _get_panel_from_pool(s)
    spec = get_label(label_name)
    v = panel.values
    ocol = "cd_open" if "cd_open" in v else "open"
    ccol = "cd_close" if "cd_close" in v else "close"
    px = pd.DataFrame({"open": v[ocol], "close": v[ccol]}).dropna().sort_index()
    lab = compute_labels(spec, px)
    eng = _panel_engine(s)
    bundle = eng.compute(list(features))
    payload = {"model_name": model_name, "model_hash": model_hash,
               "label_name": label_name, "feature_fingerprint": ""}
    metrics = evaluate_submission(
        pool_id, payload, features=bundle.values, features_avail=bundle.avail,
        labels=lab.values, label_avail=lab.available_at,
        n_trials=_server_n_trials())
    return {"pool": pool_id, "model": model_name, "metrics": metrics}


def _get_panel_from_pool(s: PoolScope):
    """服务端直接从**池内**取价格面板 (不走会话缓存)。

    这是服务端评估的取数入口: valid/oos 的面板只能由服务端自己读, Agent
    碰不到 (三池隔离的服务端那一半)。
    """
    names = ["cd_open", "cd_close", "cd_high", "cd_low", "cd_volume_quote"]
    eng = _panel_engine(s)
    return eng.panel(names)


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
    {"name": "list_objectives", "fn": tool_list_objectives,
     "desc": "列出研究目标 (目标下挂多个标签)"},
    {"name": "list_labels", "fn": tool_list_labels,
     "desc": "列出标签 (同一目标可有多标签)"},
    {"name": "describe_label", "fn": tool_describe_label,
     "desc": "看标签规格 (口径/成本/horizon/指纹)"},
    {"name": "walkforward_plan", "fn": tool_walkforward_plan,
     "desc": "池内 walk-forward 切分计划 (含 purge+embargo)"},
    {"name": "label_fingerprint", "fn": tool_label_fingerprint,
     "desc": "算标签落盘指纹 (提交载荷用)"},
    {"name": "trial_count", "fn": tool_trial_count,
     "desc": "全局试验计数 N (含开发池; DSR 输入)"},
    {"name": "significance_check", "fn": tool_significance_check,
     "desc": "对逐期收益算 PSR/DSR (多重检验修正)"},
    # -- 研究链算子 (标签 -> 样本 -> 训练 -> 回测 -> 评价) --
    {"name": "load_price_panel", "fn": tool_load_price_panel,
     "desc": "载入价格面板 (标签的输入; 区别于 compute_features 返回的特征)"},
    {"name": "compute_labels", "fn": tool_compute_labels,
     "desc": "算标签 (未来函数; 仅开发池, 标签视同原始数据)"},
    {"name": "build_training_samples", "fn": tool_build_training_samples,
     "desc": "构建训练样本 (唯一对齐处+双断言; 按 walk-forward 折)"},
    {"name": "train_model", "fn": tool_train_model,
     "desc": "walk-forward 训练 (linear/gbdt/baseline), 可注册"},
    {"name": "predict_with_model", "fn": tool_predict_with_model,
     "desc": "用注册模型在当前池特征上预测"},
    {"name": "run_backtest", "fn": tool_run_backtest,
     "desc": "分数->信号->仓位映射->回测引擎 (同一成本模型)"},
    {"name": "evaluate_candidate", "fn": tool_evaluate_candidate,
     "desc": "三层评价 (预测/交易/显著性 PSR-DSR), 唯一代码路径"},
    {"name": "list_models", "fn": tool_list_models,
     "desc": "列出已注册模型 (指纹+训练窗口+标签)"},
    # -- 强化学习 (Env 复用回测引擎) --
    {"name": "rl_env_info", "fn": tool_rl_env_info,
     "desc": "RL 环境体检 (state 维度/动作空间/时间跨度)"},
    {"name": "train_rl", "fn": tool_train_rl,
     "desc": "PPO 强化学习训练 (Env=回测引擎; 成本后缩尾奖励)"},
    {"name": "eval_rl_policy", "fn": tool_eval_rl_policy,
     "desc": "用 RL 策略跑一段并给出与监督学习同格式的成绩单"},
    # -- 服务端评估通道 (valid/oos 三池纪律的最后一公里) --
    {"name": "evaluate_submission", "fn": tool_evaluate_submission,
     "desc": "服务端评估已提交的候选 (valid/oos; Agent 无池内数据只能走这条)"},
    {"name": "server_evaluate_pool", "fn": tool_server_evaluate_pool,
     "desc": "服务端直接评估已注册模型 (valid/oos)"},
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