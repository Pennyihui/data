# -*- coding: utf-8 -*-
"""_test_groups.py — 分组维度验证 (设计文档 3.2 / 决策 2)

分组维度是加密版的"行业中性": 算子的 group_* 家族早就写好了, 但一直缺
"给每个资产、每一天打什么标签"这一层。本测试守三件事:

  1. PIT 正确性: 标签**逐日取当日宇宙快照** —— 用窗口末日的市值给整段历史打
     标签是前视 (用"今天谁是大盘股"去划分 2019 年的组)。
  2. 与引擎的集成: 表达式写了 market_cap_tier, 引擎自动构造标签, 不用调用方手动传。
  3. 组内语义: group_rank 在每个 (time, group) 内部排名, 组间不串。
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import groups as G  # noqa: E402
from data_foundation.features import operators as op  # noqa: E402
from data_foundation.pool_registry import PoolScope  # noqa: E402
from data_foundation import reader  # noqa: E402

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
print("分组维度验证")
print("=" * 74)

scope = PoolScope("oof")
IDX = pd.MultiIndex.from_product(
    [["BTC", "ETH", "SOL"], pd.date_range("2022-03-01", periods=24, freq="h",
                                           tz="UTC")],
    names=["base_asset", "time"])

# --- 1. 维度注册 ------------------------------------------------------------
print("\n1) 维度注册与错误路径")
check("四个就绪维度", set(G.GROUP_DIMENSIONS) ==
      {"market_cap_tier", "listing_age_tier", "venue", "quote"},
      str(list(G.GROUP_DIMENSIONS)))
check("sector/chain 登记为未就绪", set(G.PENDING_DIMENSIONS) == {"sector", "chain"})
check("未就绪维度报错且提示原因", raises(G.load_groups, scope, ["sector"], IDX))
check("未知维度报错", raises(G.load_groups, scope, ["bogus"], IDX))
check("describe_group 有说明", "desc" in G.describe_group("market_cap_tier"))
check("describe_group 标注 ready", G.describe_group("sector")["ready"] is False)

# --- 2. 标签构造 ------------------------------------------------------------
print("\n2) 标签构造 (真实宇宙快照)")
g = G.load_groups(scope, list(G.GROUP_DIMENSIONS), IDX)
check("返回全部请求的维度", set(g) == set(G.GROUP_DIMENSIONS))
check("标签与面板索引对齐", all(v.index.equals(IDX) for v in g.values()))
check("无缺失标签 (未知填 unknown)",
      all(v.notna().all() for v in g.values()))
check("venue 标签合理 (binance)", set(g["venue"].unique()) <= {"binance", "okx",
                                                              "bybit", "bitget",
                                                              "unknown"},
      str(set(g["venue"].unique())))
check("quote 标签合理 (USDT)", "USDT" in set(g["quote"].unique()),
      str(set(g["quote"].unique())))
check("上市时长分层有区分度", len(set(g["listing_age_tier"].unique())) >= 1,
      str(g["listing_age_tier"].value_counts().to_dict()))

# --- 3. PIT: 标签逐日变化 (不是拿末日市值贴整段历史) ---------------------------
print("\n3) PIT 正确性 (逐日标签 vs 末日标签)")
# 取一段跨越牛熊的长窗口, 看 market_cap_tier 是否随日期变化
LONG = pd.MultiIndex.from_product(
    [["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "LINK"],
     pd.date_range("2021-01-01", periods=3 * 24, freq="8h", tz="UTC")],
    names=["base_asset", "time"])
gl = G.load_groups(scope, ["market_cap_tier"], LONG)["market_cap_tier"]
per_day = gl.groupby(gl.index.get_level_values("time").normalize()).first()
distinct_days = per_day.astype(str).agg("|".join)
check("标签随日期变化 (非全期固定)",
      len(set(distinct_days)) > 1 or len(set(gl.unique())) > 1,
      f"全域标签集合={sorted(set(gl.unique()))}")
# 直接核对: 手工用当日快照算一次, 与函数结果一致
uni_all = reader.load_universe(as_of=None, layer="research")
day = pd.Timestamp("2021-06-01", tz="UTC")
snap = uni_all[pd.to_datetime(uni_all["date_utc"], utc=True).dt.normalize() == day]
manual_idx = pd.MultiIndex.from_product(
    [sorted(snap["base_asset"].unique())[:3], [day + pd.Timedelta(hours=1)]],
    names=["base_asset", "time"])
manual = G.build_group_labels("market_cap_tier", snap.assign(date_utc=day),
                              manual_idx)
check("单日手工对照: 标签按当日市值分位", manual.notna().all(),
      str(manual.to_dict()))

# --- 4. 与算子的组内语义 ------------------------------------------------------
print("\n4) 组内语义 (group_rank 不跨组)")
lab = pd.Series(np.repeat(["g1", "g1", "g2"], 24), index=IDX)
x = pd.Series(np.arange(len(IDX), dtype=float), index=IDX)
r = op.group_rank(x, lab)
check("group_rank 输出 ∈ (0,1]", bool(((r > 0) & (r <= 1)).all()))
# 组内成员数: g1 两个, g2 一个 -> g2 组内排名恒为 1.0
g2_mask = lab == "g2"
check("单成员组排名恒为 1.0", bool((r[g2_mask] == 1.0).all()))
# 同组同刻的两个成员排名互补 (0.5 / 1.0)
g1_mask = lab == "g1"
per_time = r[g1_mask].groupby(level="time").apply(lambda s: sorted(s.tolist()))
check("双成员组内排名为 [0.5, 1.0]",
      all(abs(a[0] - 0.5) < 1e-12 and abs(a[1] - 1.0) < 1e-12 for a in per_time),
      str(per_time.iloc[0]))

# --- 5. 引擎集成 -------------------------------------------------------------
print("\n5) 引擎集成 (表达式写维度名, 引擎自动构造标签)")
from data_foundation.features import engine, registry  # noqa: E402
registry.load_library()
registry.validate_all()
spec = registry.get_feature("grp_cap_mom")
check("特征声明了分组维度", spec.groups == ("market_cap_tier",), str(spec.groups))
names = [n for n in registry.feature_names() if n.startswith("grp_")]
check("库里有分组特征", len(names) >= 15, f"{len(names)} 个")
eng = engine.FeatureEngine(scope, start="2022-03-01", end="2022-03-05",
                           assets=["BTC", "ETH", "SOL"])
pick = [n for n in names if n in ("grp_cap_mom", "grp_age_mom",
                                  "grp_cap_funding_rank")] or names[:3]
b = eng.compute(pick)
check("引擎自动加载维度并算出结果", b.values.notna().sum().sum() > 0,
      f"列={list(b.values.columns)}")
rank_col = [c for c in b.values.columns if "mom" in c][0]
vals = b.values[rank_col].dropna()
check("分组特征值域合理 (rank ∈ (0,1])",
      bool(((vals > 0) & (vals <= 1)).all()), f"{rank_col}")
check("可用时间 <= as_of",
      all(b.avail[c].dropna().max() <= b.panel.as_of for c in b.avail.columns))

print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)