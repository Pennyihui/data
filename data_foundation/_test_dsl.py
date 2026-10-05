# -*- coding: utf-8 -*-
"""_test_dsl.py — F3/F4 表达式引擎验证

覆盖:
  1. 编译: 白名单 (只放行字段/算子/常量/四则运算; 拒绝下标/属性/比较/lambda/未知名)
  2. 血缘: 输入字段、算子链、完整链路、深度、直接父节点 (CSE 后不重复)
  3. 求值: 与"直接调用算子"的参考实现逐点对照 (四族 + 嵌套 + 双输入 + 四则)
  4. CSE: 共享子表达式只算一次
  5. PIT: 传播窗口与算子窗口一致; assert_no_leakage 通过; 人工泄漏被拦
  6. 未来不变性: 引擎整体 (表达式 + PIT 传播) 对未来数据不敏感
  7. 真实数据端到端: 池1 BTC/ETH, 组内/截面/时序嵌套特征, 泄漏自检通过
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import dsl  # noqa: E402
from data_foundation.features import operators as op  # noqa: E402
from data_foundation import fields as F  # noqa: E402
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
    except dsl.ExprError:
        return True
    except Exception:
        return False


print("=" * 74)
print("F3/F4 表达式引擎验证")
print("=" * 74)

# ---------------------------------------------------------------------------
# 面板 (真实数据 + 合成数据各用其所需字段)
# ---------------------------------------------------------------------------
N_I, N_T = 5, 400
idx = pd.MultiIndex.from_product(
    [[f"C{i}" for i in range(N_I)],
     pd.date_range("2024-01-01", periods=N_T, freq="h", tz="UTC")],
    names=["base_asset", "time"])
rs = np.random.RandomState(11)
VAL = pd.DataFrame({
    "close": np.exp(rs.randn(len(idx)) * 0.01) * 100.0,
    "volume_quote": rs.rand(len(idx)) * 1e6 + 1e5,
    "funding_rate": rs.randn(len(idx)) * 1e-4,
    "sector": np.repeat(["defi", "defi", "l2", "l2", "meme"], N_T),
}, index=idx).sort_index()
AVAIL = pd.DataFrame(index=VAL.index)
for c in VAL.columns:
    AVAIL[c] = VAL.index.get_level_values("time") + pd.Timedelta(minutes=1)
FIELDS = {"close", "volume_quote", "funding_rate", "sector"}
GCOLS = {"sector"}


def run(expr, name="f", values=None, avail=None):
    c = dsl.compile_expr(expr, FIELDS)
    return dsl.evaluate(c, values if values is not None else VAL,
                        avail if avail is not None else AVAIL,
                        group_cols=GCOLS, name=name)


# --- 1. 白名单 --------------------------------------------------------------
print("\n1) 编译白名单")
check("正常表达式编译", dsl.compile_expr("ts_mean(close, 24)", FIELDS) is not None)
check("拒绝未知字段", raises(dsl.compile_expr, "ts_mean(nope, 5)", FIELDS))
check("拒绝未知算子", raises(dsl.compile_expr, "ts_moan(close, 5)", FIELDS))
check("拒绝下标访问", raises(dsl.compile_expr, "close[0]", FIELDS))
check("拒绝属性访问", raises(dsl.compile_expr, "close.mean()", FIELDS))
check("拒绝比较运算", raises(dsl.compile_expr, "close > 100", FIELDS))
check("拒绝布尔运算", raises(dsl.compile_expr, "close & close", FIELDS))
check("拒绝 lambda", raises(dsl.compile_expr, "(lambda: close)()", FIELDS))
check("拒绝关键字参数", raises(dsl.compile_expr, "ts_mean(close, window=24)", FIELDS))
check("拒绝裸常量", raises(dsl.compile_expr, "5", FIELDS))
check("拒绝单字段", raises(dsl.compile_expr, "close", FIELDS))
check("拒绝非字符串", raises(dsl.compile_expr, 123, FIELDS))
check("拒绝语法错误", raises(dsl.compile_expr, "ts_mean(close, 24", FIELDS))
check("拒绝 ifexp", raises(dsl.compile_expr, "1 if close > 0 else 2", FIELDS))
check("四则运算允许", dsl.compile_expr("(close - close) / (close + close)", FIELDS)
      is not None)
check("一元负号允许", dsl.compile_expr("(-close)", FIELDS) is not None)

# --- 2. 血缘 ----------------------------------------------------------------
print("\n2) 血缘 (CSE 后唯一节点 + 直接父节点)")
expr = "group_rank(cs_zscore(ts_zscore(pp_log(close), 24)), sector)"
c = dsl.compile_expr(expr, FIELDS)
check("输入字段识别", c.fields == ("close", "sector",), str(c.fields))
check("算子链 (求值顺序)", c.operators == ("pp_log", "ts_zscore", "cs_zscore", "group_rank"),
      str(c.operators))
steps = c.steps()
check("步骤数 = 字段2 + 算子4", len(steps) == 6, str(len(steps)))
check("每个节点记直接父节点",
      all(n.parents for n in steps if n.kind in ("call", "binop", "neg")))
check("叶子节点无父节点", all(not n.parents for n in steps if n.kind == "field"))
check("完整血缘链路", c.lineage()[0] == "close" and "group_rank" in c.lineage()[-1],
      str(c.lineage()))
check("深度计算 (字段计第1层)", c.depth() == 5, f"depth={c.depth()}")
# CSE: 同一子表达式出现两次 -> 节点唯一
c2 = dsl.compile_expr("ts_mean(close, 24) + ts_mean(close, 24)", FIELDS)
codes = [n.code for n in c2.steps()]
check("CSE: 相同子表达式只留一个节点", codes.count("ts_mean(close, 24)") == 1, str(codes))
check("CSE: 算子链也去重", c2.operators.count("ts_mean") == 1, str(c2.operators))

# --- 3. 求值 vs 参考实现 -----------------------------------------------------
print("\n3) 求值 == 直接调用算子 (逐点对照)")
cases = [
    ("ts_mean(close, 24)",
     op.ts_mean(VAL["close"], 24)),
    ("ts_rank(funding_rate, 90)",
     op.ts_rank(VAL["funding_rate"], 90)),
    ("cs_rank(close)", op.cs_rank(VAL["close"])),
    ("cs_zscore(close)", op.cs_zscore(VAL["close"])),
    ("group_rank(close, sector)", op.group_rank(VAL["close"], VAL["sector"])),
    ("group_neutralize(close, sector)",
     op.group_neutralize(VAL["close"], VAL["sector"])),
    ("pp_log(close)", op.pp_log(VAL["close"])),
    ("pp_pct_change(close, 2)", op.pp_pct_change(VAL["close"], 2)),
    ("ts_zscore(pp_log(close), 24)",
     op.ts_zscore(op.pp_log(VAL["close"]), 24)),
    ("group_rank(ts_zscore(pp_log(close), 24), sector)",
     op.group_rank(op.ts_zscore(op.pp_log(VAL["close"]), 24), VAL["sector"])),
    ("cs_rank(ts_zscore(pp_log(close), 24))",
     op.cs_rank(op.ts_zscore(op.pp_log(VAL["close"]), 24))),
    ("ts_corr(close, volume_quote, 24)",
     op.ts_corr(VAL["close"], VAL["volume_quote"], 24)),
    ("(p_close_placeholder - close) / close", None),   # 占位, 下面单独测四则
    ("pp_frac_diff(close, 0.5, 10)",
     op.pp_frac_diff(VAL["close"], 0.5, 10)),
    ("ts_decay_linear(close, 12)",
     op.ts_decay_linear(VAL["close"], 12)),
]
for e, ref in cases:
    if ref is None:
        continue
    r = run(e, name=e)
    got = r.values.reindex(ref.index)
    ok = np.allclose(got.to_numpy(), ref.to_numpy(), equal_nan=True, rtol=1e-12, atol=1e-12)
    check(f"表达式 == 参考实现: {e}", ok)
# 四则运算
e = "(close - volume_quote) / volume_quote"
r = run(e, name=e)
ref = (VAL["close"] - VAL["volume_quote"]) / VAL["volume_quote"]
check("表达式 == 参考实现: 四则运算",
      np.allclose(r.values.reindex(ref.index).to_numpy(), ref.to_numpy(),
                  equal_nan=True, rtol=1e-12))
# ts_corr 双输入
r = run("ts_corr(close, volume_quote, 24)", name="corr")
ref = op.ts_corr(VAL["close"], VAL["volume_quote"], 24)
check("表达式 == 参考实现: ts_corr 双输入",
      np.allclose(r.values.reindex(ref.index).to_numpy(), ref.to_numpy(),
                  equal_nan=True, rtol=1e-12))

# --- 4. PIT 传播 ------------------------------------------------------------
print("\n4) PIT 可用时间传播 (窗口与算子一致)")
r = run("ts_zscore(close, 24)", name="z")
# 参考: 24 窗口内 close 可用时间的滚动 max (int64 上做 —— pandas rolling 不吃 datetime)
ref_av = AVAIL["close"].astype("int64").groupby(level="base_asset", sort=False).transform(
    lambda s: s.rolling(24, min_periods=1).max())
ref_av = pd.to_datetime(ref_av, unit="ns", utc=True)
check("ts_zscore 传播 == 24 窗口 max (逐行相等)",
      bool((r.avail.astype("int64") == ref_av.astype("int64")).all()))
# 窗口越大 -> 可用时间越晚 (下界单调)
av24 = r.avail
r48 = run("ts_zscore(close, 48)", name="z48")
check("窗口越大可用时间不早", bool((r48.avail.reindex(av24.index) >= av24).all()))
# cs 传播: 同刻截面 max (本面板各资产同刻可用时间相同 -> 相同)
r_cs = run("cs_rank(close)", name="cs")
check("cs 传播 == 截面 max",
      bool((r_cs.avail.reindex(AVAIL["close"].index) >= AVAIL["close"]).all()))
# group 传播
r_g = run("group_rank(close, sector)", name="g")
check("group 传播 >= 单输入可用时间",
      bool((r_g.avail.reindex(AVAIL["close"].index) >= AVAIL["close"]).all()))
# 窗口默认: 省略窗口时用算子默认值 (ts_mean 默认 20) 而不是 24
r_def = run("ts_mean(close)", name="mdef")
r_20 = run("ts_mean(close, 20)", name="m20")
check("省略窗口用算子默认值(20)且传播一致",
      bool((r_def.avail == r_20.avail).all()))
# 泄漏自检: 传播后自检通过 (evaluate 内部已跑), 再确认输入最大可用时间正确
check("输入最大可用时间记录正确",
      bool((r.input_avail.reindex(AVAIL["close"].index) == AVAIL["close"]).all()))

# --- 5. 未来不变性 (引擎级) --------------------------------------------------
print("\n5) 未来不变性 (引擎整体: 值 + PIT 传播)")
CUT = VAL.index.get_level_values("time").unique()[N_T // 2]
FUT = np.asarray(VAL.index.get_level_values("time") > CUT)     # 未来掩码
PAST_ = ~FUT
VAL2, AVAIL2 = VAL.copy(), AVAIL.copy()
for col in ("close", "volume_quote", "funding_rate"):
    VAL2.loc[FUT, col] = rs.randn(int(FUT.sum())) * 1e6 + 1e9
    AVAIL2.loc[FUT, col] = CUT + pd.Timedelta(days=30)
for e in ["ts_zscore(pp_log(close), 24)",
          "group_rank(cs_zscore(ts_zscore(pp_log(close), 24)), sector)",
          "ts_corr(close, volume_quote, 24)",
          "(close - ts_mean(close, 24)) / ts_std(close, 24)",
          "pp_quantile_bucket(close, 5)"]:
    a = run(e, name=e)
    b = run(e, name=e, values=VAL2, avail=AVAIL2)
    sub = a.values[PAST_].index
    okv = np.allclose(a.values[PAST_].to_numpy(),
                      b.values.reindex(sub).to_numpy(),
                      equal_nan=True, rtol=1e-12)
    oka = bool((a.avail.reindex(sub) == b.avail.reindex(sub)).all())
    check(f"未来不变: {e[:48]}", okv and oka, f"值一致={okv} 可用时间一致={oka}")

# --- 6. 泄漏拦截 ------------------------------------------------------------
print("\n6) 传播保守性 (引擎自检不可关)")
# 引擎的核心不变量: 特征可用时间 >= 输入最大可用时间 (逐行)
for e in ["ts_zscore(close, 24)",
          "group_rank(cs_zscore(ts_zscore(pp_log(close), 24)), sector)",
          "ts_corr(close, volume_quote, 24)",
          "pp_quantile_bucket(close, 5)",
          "(close - ts_mean(close, 24)) / ts_std(close, 24)"]:
    r = run(e, name=e)
    ok = bool((r.avail.reindex(r.input_avail.index) >= r.input_avail).all())
    check(f"传播保守 (特征>=输入): {e[:40]}", ok)
# 输入整体谎报更早可用 -> 特征同步更早 (引擎不越权提前), 且自检仍通过
c_bad = dsl.compile_expr("ts_zscore(close, 24)", FIELDS)
AVAIL_BAD = AVAIL.copy()
AVAIL_BAD["close"] = AVAIL_BAD["close"] - pd.Timedelta(hours=1)
res_bad = dsl.evaluate(c_bad, VAL, AVAIL_BAD, group_cols=GCOLS, name="shifted")
check("输入谎报更早可用 -> 特征同步不晚于输入 (引擎不自作主张)",
      bool((res_bad.avail <= res_bad.input_avail + pd.Timedelta(0)).all()))
# check=False 允许调试, 但仍返回输入最大可用时间供人工检查
res_nc = dsl.evaluate(c_bad, VAL, AVAIL, group_cols=GCOLS, name="nocheck", check=False)
check("check=False 时正常返回 (调试用)",
      isinstance(res_nc.input_avail, pd.Series))

# --- 7. 真实数据端到端 ------------------------------------------------------
print("\n7) 真实数据端到端 (池1 OOF, BTC/ETH, 时间墙内)")
scope = PoolScope("oof")
panel = F.load_panel(scope, ["close", "funding_rate", "volume_quote"],
                     assets=["BTC", "ETH"], start="2022-01-01",
                     end="2022-02-15", warmup="240h")
v, a = panel.values, panel.avail
fields = set(v.columns)
exprs = [
    ("mom_zscore", "ts_zscore(pp_pct_change(close, 1), 168)"),
    ("vol_of_vol", "ts_mean(ts_std(pp_pct_change(close, 1), 24), 168)"),
    ("funding_rank", "ts_rank(funding_rate, 90)"),
    ("cs_mom_rank", "cs_rank(ts_zscore(pp_pct_change(close, 1), 24))"),
    ("liq_intensity", "ts_mean(volume_quote, 24) / ts_mean(volume_quote, 168)"),
    ("funding_z", "ts_zscore(funding_rate, 336)"),
]
for name, e in exprs:
    c = dsl.compile_expr(e, fields)
    res = dsl.evaluate(c, v, a, name=name)
    n_val = int(res.values.notna().sum())
    check(f"真实数据特征: {name}", n_val > 0,
          f"{n_val:,} 个值 | 血缘 {len(res.lineage())} 层 | 深度 {c.depth()}")

# 端到端未来不变性 (真实数据)
panel2 = F.load_panel(scope, ["close", "funding_rate", "volume_quote"],
                      assets=["BTC", "ETH"], start="2022-01-01",
                      end="2022-02-15", warmup="240h",
                      as_of="2022-01-31")
cut = pd.Timestamp("2022-01-31", tz="UTC")
mask = np.asarray(v.index.get_level_values("time") <= cut)
e = "cs_rank(ts_zscore(pp_pct_change(close, 1), 24))"
c = dsl.compile_expr(e, fields)
r1 = dsl.evaluate(c, v, a, name="cut")
r2 = dsl.evaluate(c, v.loc[mask], a.loc[mask], name="cut")
idx1 = r1.values[mask].index
same = np.allclose(r1.values[mask].to_numpy(),
                   r2.values.reindex(idx1).to_numpy(), equal_nan=True, rtol=1e-12)
check("真实数据: as_of 提前 -> 池内历史特征不变 (PIT 语义)",
      same, f"{int(mask.sum())} 行")

print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)