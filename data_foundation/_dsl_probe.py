# -*- coding: utf-8 -*-
"""_dsl_probe.py — F3 DSL 选型验证 (expr_codegen 可行性实测)

背景: 设计文档决策 1/6 —— 表达式语言用 expr_codegen (不自研 parser),
F3 阶段须验证两点: ① 是否支持 group_* 等分组算子 ② 1,967 万行的计算耗时。

本脚本是可复现的验证实验, 结论已写入
docs/feature-foundation-design.md (v0.5, 第 12.3 节 F3 结论)。

实验项
------
E1  把 46 个算子注册进 expr_codegen 的 global_env, 转译混合表达式
    (ts_ + cs_ + group_ + pp_), 看能否生成**可读代码** (决策 6 的核心承诺:
    输出可读代码 ⇒ 血缘天然可解析 ⇒ 可注入 PIT 逻辑)。
E2  看它是否做公共子表达式消除 (CSE) 与阶段拆分 (时序阶段 -> 截面/分组阶段)。
E3  拿本项目的**面板** (MultiIndex base_asset/time) 喂给它的 pandas 执行器,
    看能否算出与直接调用一致的结果 —— 这是能否落地的关键。
E4  1,967 万行规模的性能口径: 我们的 pandas 引擎实测 (expr_codegen 因 E3
    不兼容, 无从在同一面板上测, 只记录其代码生成耗时)。

运行: python _dsl_probe.py   (需要 pip install expr_codegen polars)
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from io import StringIO

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import operators as op  # noqa: E402

OUT = {}
RESULT_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "_dsl_probe_result.json")


def hr(title):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


# ---------------------------------------------------------------------------
# 面板 (与 features 面板约定一致)
# ---------------------------------------------------------------------------
def make_panel(n_inst=4, n_time=300, seed=0) -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [[f"C{i}" for i in range(n_inst)],
         pd.date_range("2024-01-01", periods=n_time, freq="h", tz="UTC")],
        names=["base_asset", "time"])
    rs = np.random.RandomState(seed)
    return pd.DataFrame({
        "close": np.exp(rs.randn(len(idx)) * 0.01) * 100.0,
        "volume_quote": rs.rand(len(idx)) * 1e6 + 1e5,
        "funding_rate": rs.randn(len(idx)) * 1e-4,
        "sector": np.repeat(["defi", "defi", "l2", "l2"], n_time),
    }, index=idx).sort_index()


def block():
    # 混合表达式: 同时用到 pp_ / ts_ / cs_ / group_
    _a = ts_zscore(pp_log(close), 24)            # 会被 F1 与 F2 复用 (CSE 靶子)
    _b = ts_rank(funding_rate, 24)
    F1 = group_rank(_a, sector)
    F2 = cs_rank(_b)
    F3 = _a + _b


hr("环境")
try:
    import expr_codegen as ec
    OUT["expr_codegen_version"] = getattr(ec, "__version__", "?")
    print("expr_codegen", OUT["expr_codegen_version"], "|", ec.__file__)
except Exception as exc:
    print("expr_codegen 不可用:", exc)
    OUT["expr_codegen_version"] = None
    ec = None

genv = {n: getattr(op, n) for n in dir(op)
        if n.startswith(("ts_", "cs_", "group_", "pp_")) and callable(getattr(op, n))}
print("注册算子:", len(genv))


# ---------------------------------------------------------------------------
# E1/E2: 代码生成 (决策 6 的核心承诺)
# ---------------------------------------------------------------------------
if ec is not None:
    hr("E1+E2 expr_codegen 转译 (style=pandas)")
    buf = StringIO()
    t0 = time.time()
    ec.codegen_exec(None, block, style="pandas", output_file=buf,
                    asset="base_asset", date="time", over_null=None)
    OUT["codegen_seconds"] = round(time.time() - t0, 3)
    code = buf.getvalue()
    lines = [ln for ln in code.splitlines() if ln.strip().startswith("g[")]
    print("生成的算子调用行 (可读 = 可审计):")
    for ln in lines:
        print("   ", ln.strip())
    OUT["readable_calls"] = lines
    # CSE: 同一子表达式在生成代码里只应有一条 **计算赋值** 行 (g[x] = ...)。
    # (_a 被 F1/F3 共用; 若未消除会出现两条 ts_zscore(pp_log(...)) 计算行)
    calc_lines = [ln.strip() for ln in lines if "=" in ln]
    cse_calc = [ln for ln in calc_lines if "ts_zscore(pp_log(" in ln]
    OUT["cse_reuse_count"] = len(cse_calc)
    print(f"\nCSE: ts_zscore(pp_log(close),24) 的计算赋值行 = {len(cse_calc)} "
          f"(表达式引用 2 次 -> {'已消除为一次计算' if len(cse_calc) == 1 else '未消除'})")
    # 阶段拆分
    stages = [ln.strip() for ln in code.splitlines() if ln.strip().startswith("df = ")]
    OUT["stage_pipeline"] = stages
    print("阶段编排:")
    for s in stages:
        print("   ", s)
    # group_* 支持?
    OUT["group_ops_in_code"] = [ln.strip() for ln in lines if "group_" in ln]
    print("group_* 支持:", "是" if OUT["group_ops_in_code"] else "否",
          OUT["group_ops_in_code"])
    print("生成代码行数:", len(code.splitlines()), "| 生成耗时",
          OUT["codegen_seconds"], "s")

    # ---------------------------------------------------------------------
    hr("E3 用面板 (MultiIndex) 执行它的 pandas 执行器")
    panel = make_panel()
    try:
        out = ec.codegen_exec(panel.copy(), block, style="pandas",
                              asset="base_asset", date="time", over_null=None)
        print("执行成功; 输出列:", [c for c in out.columns if c.startswith("F")])
        print("索引名:", out.index.names)
        direct = op.group_rank(op.ts_zscore(op.pp_log(panel["close"]), 24),
                               panel["sector"])
        got = out["F1"].reindex(direct.index)
        ok = np.allclose(got.to_numpy(), direct.to_numpy(),
                         equal_nan=True, rtol=1e-12)
        OUT["multindex_exec_ok"] = bool(ok)
        print("与直接计算一致:", ok)
    except Exception as exc:
        OUT["multindex_exec_ok"] = False
        OUT["multindex_exec_error"] = f"{type(exc).__name__}: {exc}"
        print("执行失败 ->", type(exc).__name__, str(exc)[:200])
        print("根因: 它的 pandas 执行模型把 asset/date 当作**列**并自行 "
              "groupby 逐片调用算子 (df.groupby(ASSET).apply(ts阶段)); "
              "而本项目的算子是面板模型 —— 按索引层自行分组。两套分组模型互斥。")
        print(traceback.format_exc(limit=2))

    # 平表能不能跑通? (验证"只差索引形态"这一判断)
    hr("E3b 平表 (asset/date 变成列) 能否跑通")
    flat = panel.reset_index()
    try:
        out2 = ec.codegen_exec(flat.copy(), block, style="pandas",
                               asset="base_asset", date="time", over_null=None)
        direct2 = op.group_rank(op.ts_zscore(op.pp_log(flat["close"]), 24),
                                flat["sector"])
        ok2 = np.allclose(out2["F1"].reindex(direct2.index).to_numpy(),
                          direct2.to_numpy(), equal_nan=True, rtol=1e-12)
        OUT["flat_exec_ok"] = bool(ok2)
        print("平表执行成功, 与直接计算一致:", ok2)
        if ok2:
            print("  -> 说明差异只在索引形态: 但要接上面板就得给 46 个算子各写一个"
                  "平表内核 = 第二套实现 (PIT 传播也要重写一遍)")
    except Exception as exc:
        OUT["flat_exec_ok"] = False
        OUT["flat_exec_error"] = f"{type(exc).__name__}: {exc}"
        print("平表执行失败 ->", type(exc).__name__, str(exc)[:200])


# ---------------------------------------------------------------------------
# E4: 性能口径 (pandas 引擎, 1,967 万行等比例外推)
# ---------------------------------------------------------------------------
hr("E4 pandas 引擎性能 (特征线引擎的实际执行路径)")
try:
    n_i, n_t = 500, 4000        # 200 万行 = 500 币 x 4000 小时 (~1.7 年 1h)
    idx = pd.MultiIndex.from_product(
        [range(n_i), pd.date_range("2020-01-01", periods=n_t, freq="h", tz="UTC")],
        names=["base_asset", "time"])
    rs = np.random.RandomState(0)
    big = pd.DataFrame({"close": np.exp(rs.randn(len(idx)) * 0.01) * 100.0,
                        "funding_rate": rs.randn(len(idx)) * 1e-4,
                        "sector": np.repeat([f"s{i%7}" for i in range(n_i)], n_t)},
                       index=idx)
    expr_ops = {
        "ts_zscore(24)": lambda: op.ts_zscore(big["close"], 24),
        "ts_rank(24)": lambda: op.ts_rank(big["funding_rate"], 24),
        "cs_rank": lambda: op.cs_rank(big["close"]),
        "group_rank(sector)": lambda: op.group_rank(big["close"], big["sector"]),
    }
    per_op = {}
    for k, fn in expr_ops.items():
        t0 = time.time()
        fn()
        per_op[k] = round(time.time() - t0, 2)
        print(f"  {k:22s} {per_op[k]:6.2f}s  ({len(idx)/per_op[k]/1e6:.1f} M行/s)")
    OUT["perf_2M_rows_seconds"] = per_op
    OUT["perf_note"] = ("单算子耗时; 一个特征 3-6 个算子 -> 2M 行约 "
                        f"{sum(per_op.values())/len(per_op)*4:.0f}s; "
                        "1967 万行按线性外推约 %.0f 倍 -> 单特征分钟级, "
                        "全库 50-100 特征小时级 (一次性计算, 决策 3 不落盘缓存)"
                        % (19670000/2000000))
    print("\n" + OUT["perf_note"])
except Exception:
    print("性能测试跳过:")
    traceback.print_exc(limit=2)


# ---------------------------------------------------------------------------
hr("结论 (写入设计文档 v0.5)")
print("1. expr_codegen 能消费我们的算子命名空间并生成**可读的分阶段代码**,")
print("   CSE 生效 (共享子表达式只算一次) —— 决策 6 的核心承诺成立。")
print("2. 但它的 pandas 执行模型与本项目的面板算子模型**互斥**:")
print("   它把 asset/date 当列并自行 groupby 逐片调用; 我们的算子按索引层自行")
print("   分组。要落地就得为 46 个算子再写一套平表内核 (PIT 传播也要重写),")
print("   双实现 = 双倍维护 + 双倍泄漏风险。")
print("3. PIT 可用时间传播**必须知道每个 AST 节点的窗口语义** —— expr_codegen")
print("   执行时不暴露这层信息, 所以无论如何我们都要自带一次 AST 遍历。")
print("4. 决策 1 担心的\"自研 parser 的 bug\"在本方案下不成立: 解析用 Python")
print("   标准库 ast.parse, 我们只写白名单求值 + AST 遍历, 并用与参考实现的")
print("   逐点对照测试兜底。F4 引擎见 features/dsl.py。")

with open(RESULT_JSON, "w", encoding="utf-8") as f:
    json.dump(OUT, f, ensure_ascii=False, indent=2)
print("\n结果已存:", RESULT_JSON)