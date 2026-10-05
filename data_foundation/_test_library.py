# -*- coding: utf-8 -*-
"""_test_library.py — F4 特征库验证 (注册表 + catalog + 引擎)

覆盖:
  1. 注册: 命名约束 (不撞字段/算子/大写)、重复登记幂等、表达式变更升版
  2. 编译校验: 全库表达式合法; 未知字段/算子被拒
  3. catalog 多路索引: 类别 / 输入 / 算子 / 标签 / 全文; 去重能抓真重复
  4. 规模体检: 覆盖报告 + 上限监控 (决策 7)
  5. 引擎: 依赖拓扑序、循环依赖拒绝、预热自动推导、PIT + 泄漏自检
  6. 真实数据: 池1 批量算 8+ 特征, 可用时间 <= as_of, 血缘完整
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import catalog, dsl, engine, registry  # noqa: E402
from data_foundation.features.specs import FeatureSpec, compute_lookback  # noqa: E402
from data_foundation.pool_registry import PoolScope  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


def raises(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return False
    except Exception:
        return True


print("=" * 74)
print("F4 特征库验证")
print("=" * 74)

n = registry.load_library()
print(f"特征库载入: {n} 个特征")

# --- 1. 注册 ---------------------------------------------------------------
print("\n1) 注册与命名")
check("库非空", n > 20, f"{n} 个")
check("特征名不与字段/算子冲突",
      not (set(registry.feature_names()) & registry.RESERVED_NAMES))
check("特征名合法 (小写标识符)",
      all(registry._NAME_RE.match(x) for x in registry.feature_names()))
# 撞字段名被拒
from data_foundation.features.registry import FeatureSpec as FS  # noqa: E402
check("撞字段名被拒", raises(registry.register, FS("close", "x", "price")))
check("撞算子名被拒", raises(registry.register, FS("ts_mean", "x", "price")))
check("大写名被拒", raises(registry.register, FS("Bad_Name", "x", "price")))
# 重复登记幂等
before = registry.registry_size()
s1 = registry.get_feature("mom_zscore_24h")
registry.register(FS("mom_zscore_24h", s1.expr, s1.category, s1.desc))
check("重复登记幂等 (不新增)", registry.registry_size() == before)
# 表达式变更升版
s = registry.get_feature("mom_zscore_24h")
registry.register(FS("mom_zscore_24h", "ts_rank(ret_1h, 336)", s.category,
                     "changed"))
s_new = registry.get_feature("mom_zscore_24h")
check("表达式变更自动升版", s_new.version != s.version,
      f"{s.version} -> {s_new.version}")
registry.register(FS("mom_zscore_24h", s.expr, s.category, s.desc))   # 还原

# --- 2. 编译校验 -----------------------------------------------------------
print("\n2) 全库表达式校验")
errs = registry.validate_all()
check("全部表达式合法", not errs, str(errs) if errs else f"{n} 个全过")
check("catalog 按类别可检索", len(catalog.by_category("price")) > 5)
check("catalog 按算子可检索", len(catalog.by_operator("ts_zscore")) > 3)
check("catalog 全文检索", len(catalog.search("资金")) >= 3)
check("catalog 按输入检索", len(catalog.by_input("close")) >= 5)

# --- 3. 去重 ---------------------------------------------------------------
print("\n3) 去重 (活水不是垃圾场)")
dups = catalog.coverage_report()["duplicates"]
check("现库无重复 (真实去重)", not dups, str(dups))
# 造真重复 (含 24 vs 24.0 视为同一)
registry.register(FS("dup_a", "ts_mean(volume_quote, 24)", "liquidity"))
registry.register(FS("dup_b", "ts_mean(volume_quote, 24)", "liquidity"))
registry.register(FS("dup_c", "ts_mean(volume_quote, 24.0)", "liquidity"))
d = catalog.find_duplicates()
dup_group = [g for g in d if "dup_a" in g]
check("真重复被抓到 (24==24.0, 且并入已有的同表达式特征)",
      bool(dup_group) and {"dup_a", "dup_b", "dup_c"} <= set(dup_group[0]),
      str(d))
# 不同窗口不应算重复
registry.register(FS("keep_24", "ts_mean(volume_quote, 24)", "liquidity"))
registry.register(FS("keep_7", "ts_mean(volume_quote, 7)", "liquidity"))
d2 = catalog.find_duplicates()
check("不同窗口不算重复", not any("keep_24" in g and "keep_7" in g for g in d2))
for dup in ("dup_a", "dup_b", "dup_c", "keep_24", "keep_7"):
    registry._FEATURES.pop(dup, None)

# --- 4. 规模体检 -----------------------------------------------------------
print("\n4) 规模体检 (决策 7: 软上限500/硬1000)")
rep = catalog.coverage_report()
check("体检含类别/算子/深度", all(k in rep for k in
      ("by_category", "operator_hist", "max_depth", "duplicates")))
check("库规模在软上限内", rep["n_features"] < 500, f"{rep['n_features']}/500")
check("index_stats 上限监控", catalog.index_stats()["soft_cap"] == 500)
# 预热推导
sp = registry.get_feature("mom_zscore_24h")
sp.compile(set(), set()) if False else None
reg2 = registry.validate_all()
spec = registry.get_feature("funding_zscore_30d")
check("lookback 推导非零", spec.lookback and spec.lookback.bars >= 90,
      f"{spec.lookback.bars if spec.lookback else '?'} bars")
check("lookback 小时换算", spec.lookback.hours.total_seconds() / 3600 == spec.lookback.bars)

# --- 5. 引擎 ---------------------------------------------------------------
print("\n5) 引擎 (依赖/循环/预热)")
eng = engine.FeatureEngine("oof")
order = eng._resolve(["basis_zscore_7d"])       # 依赖 ret_1h? basis 不依赖; 用带依赖的
order = eng._resolve(["mom_zscore_24h"])
check("特征依赖特征时拓扑展开", "ret_1h" in order and
      order.index("ret_1h") < order.index("mom_zscore_24h"), str(order))
# 循环依赖
registry._FEATURES["cyc_a"] = FS("cyc_a", "ts_mean(cyc_b, 5)", "price")
registry._FEATURES["cyc_b"] = FS("cyc_b", "ts_mean(cyc_a, 5)", "price")
check("循环依赖被拒", raises(eng._resolve, ["cyc_a"]))
registry._FEATURES.pop("cyc_a", None)
registry._FEATURES.pop("cyc_b", None)
check("未知特征被拒", raises(eng.compute, ["不存在的特征"]))

# --- 6. 真实数据端到端 ------------------------------------------------------
print("\n6) 真实数据 (池1 OOF, BTC/ETH)")
scope = PoolScope("oof")
eng = engine.FeatureEngine(scope, start="2022-01-01", end="2022-01-31",
                           assets=["BTC", "ETH"])
names = ["mom_zscore_24h", "funding_zscore_30d", "basis_raw", "gap_rate_24h",
         "amihud_illiq", "oi_change_24h", "mark_premium", "volume_zscore_24h"]
bundle = eng.compute(names)
check("批量算出 >=8 特征", len(bundle.specs) >= 8, f"{len(bundle.specs)} 个")
check("值面板有内容", bundle.values.notna().sum().sum() > 0)
check("可用时间 <= as_of (时间墙)",
      bool(all(bundle.avail[c].dropna().max() <= bundle.panel.as_of
               for c in bundle.avail.columns)))
check("泄漏自检已在引擎内跑过 (compute 不抛)", True)
check("血缘完整 (每个特征有 lineage)",
      all(len(b.lineage()) >= 2 for b in bundle.results.values()))
# 特征依赖链
mz = bundle.results["mom_zscore_24h"]
check("特征依赖血缘 (mom->ret_1h)", "ret_1h" in " ".join(mz.lineage()),
      str(mz.lineage()))
# 预热: 池起点之前的行被读进来 (lookback>0)
times = bundle.panel.values.index.get_level_values("time")
check("预热行存在 (早于 start)", bool(times.min() < bundle.panel.start),
      f"最早 {times.min()}")
# 数据可用时间单调不减 (同一特征)
fa = bundle.avail["mom_zscore_24h"].dropna()
mono = fa.groupby(level="base_asset").apply(lambda s: s.is_monotonic_increasing)
check("可用时间序列单调不减 (PIT 合理)", bool(mono.all()))

print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)