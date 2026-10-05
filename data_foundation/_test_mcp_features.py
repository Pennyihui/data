# -*- coding: utf-8 -*-
"""_test_mcp_features.py — F7 MCP 特征工具验证

核心: 时间墙必须在**工具层**继续生效 (Agent 只看得到工具返回什么)。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import data_foundation.mcp_server as mcp  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


print("=" * 72)
print("F7 MCP 特征工具验证")
print("=" * 72)

check("注册了 4 个特征工具",
      {"list_features", "describe_feature", "compute_features",
       "feature_catalog"} <= {t["name"] for t in mcp.TOOLS},
      str([t["name"] for t in mcp.TOOLS][-4:]))

lf = mcp.tool_list_features()
check("list_features 列出全部特征", lf["n_features"] >= 50, str(lf["n_features"]))
check("list_features 按类别过滤",
      len(mcp.tool_list_features(category="derivatives")["features"]) >= 10)

d = mcp.tool_describe_feature("mom_zscore_24h")
check("describe_feature 含血缘", "ret_1h" in d["lineage"], str(d["lineage"]))
check("describe_feature 含预热/规则", d["lookback_hours"] >= 168
      and "data_available_at" in d["available_rule"])
try:
    mcp.tool_describe_feature("不存在")
    check("describe 未知特征报错", False)
except KeyError:
    check("describe 未知特征报错", True)

# 未绑池 -> compute 拒绝 (Agent 必须先声明自己在哪个池)
try:
    mcp.tool_compute_features(["mom_zscore_24h"])
    check("未绑池 compute 拒绝", False)
except RuntimeError:
    check("未绑池 compute 拒绝", True)

mcp.tool_bind_pool("oof")
r = mcp.tool_compute_features(["mom_zscore_24h", "funding_zscore_30d", "basis_raw"],
                              start="2022-01-01", end="2022-01-31",
                              assets=["BTC", "ETH"])
check("compute_features 返回统计", all(v["n"] > 0 for v in r["features"].values()))
check("血缘随结果返回", all(len(v) >= 2 for v in r["lineage"].values()))
check("effective_window 在池内", r["effective_window"][0] == "2022-01-01"
      and r["effective_window"][1] == "2022-01-31")

# 越池: end 被截断 (区间与池求交集), 绝不给出池外数据
r2 = mcp.tool_compute_features(["mom_zscore_24h"], start="2023-11-01",
                               end="2026-01-01", assets=["BTC"])
check("越池请求被截到池终点", r2["effective_window"][1] == "2023-12-31",
      str(r2["effective_window"]))

# 完全在池外 -> 拒绝
try:
    mcp.tool_compute_features(["mom_zscore_24h"], start="2025-01-01",
                              end="2025-12-31", assets=["BTC"])
    check("完全池外请求拒绝", False)
except ValueError:
    check("完全池外请求拒绝", True)

cat = mcp.tool_feature_catalog()
check("feature_catalog 体检完整", cat["n_features"] >= 50 and cat["max_depth"] <= 5
      and not cat["duplicates"],
      f"n={cat['n_features']} depth={cat['max_depth']} dups={cat['duplicates']}")

print("\n" + "=" * 72)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 72)
sys.exit(1 if FAIL else 0)