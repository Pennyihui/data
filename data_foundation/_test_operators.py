# -*- coding: utf-8 -*-
"""_test_operators.py — F1 算子库验证

核心是**未来不变性 (future-invariance)**: 把 t 之后的数据换成任意噪声,
t 时刻及之前的输出必须逐位不变。任何居中窗口 / 反向填充 / 全样本估计 /
跨 instrument 串时间, 都会让这条性质失败 —— 这是比"人工审查看代码"强得多的
PIT 证明。

覆盖:
  1. 注册表完整性 (族/前缀/文档/元数据自洽)
  2. 未来不变性 —— 逐算子, 含空值面板
  3. instrument 隔离 (改 A 不影响 B)
  4. 与独立参考实现逐点对照 (pandas / numpy)
  5. 入参校验与错误路径 (负 lag / 非法窗口 / DataFrame / 乱序 / 不对齐)
  6. 静态 PIT 审计 (源码里没有 center=True / shift(- / bfill)
"""
from __future__ import annotations

import inspect
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import operators as op  # noqa: E402

PASS, FAIL = [], []


def check(name: str, cond, detail: str = "") -> bool:
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


def section(title: str) -> None:
    print(f"\n{title}")


def try_raises(fn, *args, **kw) -> bool:
    """函数是否按预期抛异常 (而不是静默返回垃圾值)。"""
    try:
        fn(*args, **kw)
        return False
    except Exception:  # noqa: BLE001
        return True


def params_of(func) -> list[str]:
    return [p for p in inspect.signature(func).parameters if p not in ("name",)]


# ---------------------------------------------------------------------------
# 测试面板
# ---------------------------------------------------------------------------
N_INST, N_TIME = 5, 60
RS = np.random.RandomState(20261005)
INSTS = [f"C{i}-USDT" for i in range(N_INST)]
SECTORS = ["defi", "defi", "l2", "l2", "meme"]
TIMES = pd.date_range("2023-01-01", periods=N_TIME, freq="h", tz="UTC")
IDX = pd.MultiIndex.from_product([INSTS, TIMES], names=["instrument", "time"])

X = pd.Series(np.exp(np.cumsum(RS.randn(len(IDX)) * 0.01)) * 100.0, index=IDX, name="x")
Y = pd.Series(np.sin(np.arange(len(IDX)) / 7.0) + RS.randn(len(IDX)) * 0.1, index=IDX, name="y")
G = pd.Series(np.repeat(SECTORS, N_TIME), index=IDX, name="sector")
FLAG = pd.Series((RS.rand(len(IDX)) > 0.9), index=IDX, name="is_suspect")

# 含空值的版本 (空值传播是另一个维度的考验)
X_NA = X.copy()
X_NA.iloc[[3, 17, 44, 100, 250]] = np.nan

CUT = TIMES[35]          # 未来不变性的切割点
FUTURE = IDX.get_level_values("time") > CUT
PAST = ~FUTURE


def _noise_panel(s: pd.Series) -> pd.Series:
    """把 t> CUT 的值换成噪声, 其余原样。bool 质量列换成随机布尔 (保持 dtype)。"""
    out = s.copy()
    n_future = int(FUTURE.sum())
    if out.dtype == bool:
        out[FUTURE] = RS.rand(n_future) > 0.5
    else:
        out[FUTURE] = RS.randn(n_future) * 10_000 + 1e6
    return out


def _same(a: pd.Series, b: pd.Series, where: np.ndarray) -> tuple[bool, int]:
    """NaN 视作相等, 逐位比较 (先按标签对齐, 允许行序不同)。"""
    if not a.index.equals(b.index):
        if len(a) != len(b) or not a.index.sort_values().equals(b.index.sort_values()):
            return False, -1
        b = b.reindex(a.index)
    av = a.to_numpy(dtype=float)[where]
    bv = b.to_numpy(dtype=float)[where]
    both_nan = np.isnan(av) & np.isnan(bv)
    diff = ~np.isclose(av, bv, rtol=1e-12, atol=1e-12, equal_nan=True)
    return bool(not diff.any()), int((diff & ~both_nan).sum())


# ---------------------------------------------------------------------------
# 1. 注册表
# ---------------------------------------------------------------------------
print("=" * 74)
print("F1 算子库验证 — %d 个算子" % len(op.ALL_OPERATORS))
print("=" * 74)

section("1) 注册表完整性")
check("四族注册表 + ALL_OPERATORS 数量一致",
      len(op.ALL_OPERATORS) == len(op.TS_OPERATORS) + len(op.CS_OPERATORS)
      + len(op.GROUP_OPERATORS) + len(op.PP_OPERATORS))
check("无重名覆盖",
      sum(len(d) for d in op.FAMILY_REGISTRIES.values()) == len(op.ALL_OPERATORS))
bad_prefix = [n for n, s in op.ALL_OPERATORS.items() if not n.startswith(s.family + "_")]
check("算子名以族前缀开头", not bad_prefix, str(bad_prefix))
no_doc = [n for n, s in op.ALL_OPERATORS.items() if not s.doc]
check("每个算子都有一行文档 (MCP 可展示)", not no_doc, str(no_doc))
bad_sig = [n for n, s in op.ALL_OPERATORS.items()
           if not params_of(s.func) or params_of(s.func)[0] != "x"]
check("每个算子第一个参数是数据 x", not bad_sig, str(bad_sig))
check("族名合法", set(op.FAMILY_REGISTRIES) == {"ts", "cs", "group", "pp"})
check("算子清单覆盖设计文档要求",
      {"ts_mean", "ts_std", "ts_rank", "ts_zscore", "ts_corr", "ts_decay_linear",
       "cs_rank", "cs_zscore", "cs_normalize", "cs_winsorize",
       "group_rank", "group_neutralize", "group_zscore",
       "pp_log", "pp_diff", "pp_frac_diff", "pp_quantile_bucket"} <= set(op.ALL_OPERATORS))

section("2) 表达式算子名扫描 (lint 级)")
check("referenced_operators 认出 ts_/group_ 算子",
      op.referenced_operators("group_rank(ts_rank(funding_rate, 168), sector)")
      == {"group_rank", "ts_rank"})
check("unknown_operators 抓出拼写错误",
      "ts_rnak" in op.unknown_operators("ts_rnak(close, 5)"))

# ---------------------------------------------------------------------------
# 3. 未来不变性 —— 本文件的核心
# ---------------------------------------------------------------------------
section("3) 未来不变性 (把 t 之后的数据换成噪声, t 及之前的输出必须不变)")


def _cases(x: pd.Series, g: pd.Series, flag: pd.Series, y: pd.Series) -> dict:
    """全部算子的调用样例 (参数固定, 便于逐算子跑未来不变性)。"""
    return {
        "ts_delay": lambda: op.ts_delay(x, 3),
        "ts_backfill": lambda: op.ts_backfill(x),
        "ts_mean": lambda: op.ts_mean(x, 8),
        "ts_std": lambda: op.ts_std(x, 8),
        "ts_sum": lambda: op.ts_sum(x, 8),
        "ts_min": lambda: op.ts_min(x, 8),
        "ts_max": lambda: op.ts_max(x, 8),
        "ts_median": lambda: op.ts_median(x, 8),
        "ts_quantile": lambda: op.ts_quantile(x, 8, 0.8),
        "ts_delta": lambda: op.ts_delta(x, 3),
        "ts_pct_change": lambda: op.ts_pct_change(x, 3),
        "ts_rank": lambda: op.ts_rank(x, 12),
        "ts_zscore": lambda: op.ts_zscore(x, 12),
        "ts_skew": lambda: op.ts_skew(x, 12),
        "ts_kurt": lambda: op.ts_kurt(x, 12),
        "ts_ewma": lambda: op.ts_ewma(x, span=10),
        "ts_decay_linear": lambda: op.ts_decay_linear(x, 10),
        "ts_corr": lambda: op.ts_corr(x, y, 10),
        "cs_rank": lambda: op.cs_rank(x),
        "cs_zscore": lambda: op.cs_zscore(x),
        "cs_winsorize": lambda: op.cs_winsorize(x, 2),
        "cs_normalize": lambda: op.cs_normalize(x),
        "cs_scale": lambda: op.cs_scale(x),
        "group_rank": lambda: op.group_rank(x, g),
        "group_zscore": lambda: op.group_zscore(x, g),
        "group_neutralize": lambda: op.group_neutralize(x, g),
        "group_mean": lambda: op.group_mean(x, g),
        "group_std": lambda: op.group_std(x, g),
        "group_size": lambda: op.group_size(x, g),
        "pp_log": lambda: op.pp_log(x),
        "pp_sqrt": lambda: op.pp_sqrt(x),
        "pp_abs": lambda: op.pp_abs(x),
        "pp_sign": lambda: op.pp_sign(x),
        "pp_power": lambda: op.pp_power(x, 0.5),
        "pp_clip": lambda: op.pp_clip(x, 90, 110),
        "pp_diff": lambda: op.pp_diff(x, 2),
        "pp_pct_change": lambda: op.pp_pct_change(x, 2),
        "pp_frac_diff": lambda: op.pp_frac_diff(x, 0.5, 8),
        "pp_ema": lambda: op.pp_ema(x, span=10),
        "pp_zscore": lambda: op.pp_zscore(x),
        "pp_minmax": lambda: op.pp_minmax(x),
        "pp_robust": lambda: op.pp_robust(x),
        "pp_quantile_bucket(cs)": lambda: op.pp_quantile_bucket(x, 5),
        "pp_quantile_bucket(ts)": lambda: op.pp_quantile_bucket(x, 5, by="ts", window=20),
        "pp_detrend": lambda: op.pp_detrend(x, 20),
        "pp_is_missing": lambda: op.pp_is_missing(x),
        "pp_is_outlier(thr)": lambda: op.pp_is_outlier(x, threshold=1.0),
        "pp_is_outlier(bool)": lambda: op.pp_is_outlier(flag),
    }


for label, panel in (("完整面板", (X, G, FLAG, Y)), ("含空值面板", (X_NA, G, FLAG, Y))):
    base_cases = _cases(*panel)
    noise_cases = _cases(_noise_panel(panel[0]), panel[1],
                         _noise_panel(panel[2]), _noise_panel(panel[3]))
    sub_pass = 0
    for name in base_cases:
        try:
            a, b = base_cases[name](), noise_cases[name]()
            same, ndiff = _same(a, b, PAST)
            check(f"{label}: {name}", same, f"不一致 {ndiff} 行")
            sub_pass += same
        except Exception as exc:  # noqa: BLE001
            check(f"{label}: {name}", False, f"异常 {type(exc).__name__}: {exc}")
    print(f"  -> {label}: {sub_pass}/{len(base_cases)} 个算子满足未来不变性")

# ---------------------------------------------------------------------------
# 4. instrument 隔离
# ---------------------------------------------------------------------------
section("4) instrument 隔离 (改一个币的数据, 其他币的输出必须不变)")
for name, fn in [("ts_mean", lambda s: op.ts_mean(s, 8)),
                 ("ts_rank", lambda s: op.ts_rank(s, 12)),
                 ("ts_zscore", lambda s: op.ts_zscore(s, 12)),
                 ("ts_decay_linear", lambda s: op.ts_decay_linear(s, 10)),
                 ("pp_frac_diff", lambda s: op.pp_frac_diff(s, 0.5, 8)),
                 ("pp_detrend", lambda s: op.pp_detrend(s, 20))]:
    base = fn(X)
    other = INSTS[0]
    mask = X.index.get_level_values("instrument") == other
    x2 = X.copy()
    x2[mask] = x2[mask] * 7.0 + 1000.0
    after = fn(x2)
    not_other = ~mask
    same, ndiff = _same(base, after, not_other)
    check(f"{name} 未串到其他 instrument", same, f"不一致 {ndiff} 行")

# ---------------------------------------------------------------------------
# 5. 与参考实现对照
# ---------------------------------------------------------------------------
section("5) 与独立参考实现逐点对照")
one = X.loc[INSTS[0]]

# ts_mean vs pandas
ref = one.rolling(8, min_periods=1).mean()
check("ts_mean == pandas rolling.mean",
      np.allclose(op.ts_mean(one, 8).to_numpy(), ref.to_numpy(), equal_nan=True))
# ts_rank vs 手工平均秩 (与 pandas rank(pct=True) 同口径)
w = 12
expect = [np.nan] * len(one)
arr = one.to_numpy()
for t in range(len(one)):
    lo = max(0, t - w + 1)
    v = arr[lo:t + 1]
    v = v[~np.isnan(v)]
    if v.size and not np.isnan(arr[t]):
        expect[t] = ((v < arr[t]).sum() + 0.5 * ((v == arr[t]).sum() + 1)) / v.size
got = op.ts_rank(one, w).to_numpy()
check("ts_rank == 手工平均秩口径", np.allclose(got, expect, atol=1e-12, equal_nan=True))
# cs_rank == pandas groupby rank(pct)
check("cs_rank == groupby(level=time).rank(pct=True)",
      np.allclose(op.cs_rank(X).to_numpy(),
                  X.groupby(level="time").rank(pct=True).to_numpy(), equal_nan=True))
# cs_zscore == 手工
t0 = TIMES[10]
sec = X.loc[(slice(None), t0)]
check("cs_zscore == (x-mean)/std (截面)",
      np.allclose(op.cs_zscore(X).loc[(slice(None), t0)].to_numpy(),
                  ((sec - sec.mean()) / sec.std()).to_numpy(), equal_nan=True))
# ts_delta / ts_pct_change == shift
check("ts_delta == x - shift(lag)",
      np.allclose(op.ts_delta(one, 3).to_numpy(),
                  (one - one.shift(3)).to_numpy(), equal_nan=True))
check("ts_pct_change == x/shift-1",
      np.allclose(op.ts_pct_change(one, 3).to_numpy(),
                  (one / one.shift(3) - 1).to_numpy(), equal_nan=True))
# ts_decay_linear 权重 (默认: 最新值权重最大 d..1; reverse: 1..d)
w = 5
norm = w * (w + 1) / 2
d = op.ts_decay_linear(one, w)
manual = sum((w - k) / norm * one.shift(k) for k in range(w))
check("ts_decay_linear == 权重 d..1 (最新最大)",
      np.allclose(d.to_numpy(), manual.to_numpy(), equal_nan=True))
d_rev = op.ts_decay_linear(one, w, reverse=True)
manual_rev = sum((k + 1) / norm * one.shift(k) for k in range(w))
check("ts_decay_linear(reverse=True) == 权重 1..d (最旧最大)",
      np.allclose(d_rev.to_numpy(), manual_rev.to_numpy(), equal_nan=True))
# ts_corr vs np.corrcoef
t = 40
lo = t - 9
a_w = one.to_numpy()[lo:t + 1]
b_w = Y.loc[INSTS[0]].to_numpy()[lo:t + 1]
check("ts_corr == np.corrcoef (窗口)",
      np.isclose(op.ts_corr(one, Y.loc[INSTS[0]], 10).to_numpy()[t], np.corrcoef(a_w, b_w)[0, 1]))
# pp_frac_diff 系数 (1-B)^-d
w, d_ = 6, 0.5
coef = [1.0]
for j in range(1, w):
    coef.append(coef[-1] * (d_ + j - 1) / j)
manual = sum(c * one.shift(j) for j, c in enumerate(coef))
check("pp_frac_diff == (1-B)^-d 系数 [1, .5, .375, .3125, .2734, .2461]",
      np.allclose(op.pp_frac_diff(one, d_, w).to_numpy(), manual.to_numpy(),
                  equal_nan=True),
      str([round(c, 4) for c in coef]))
# ts_skew / ts_kurt vs scipy (无偏口径)
from scipy.stats import kurtosis, skew  # noqa: E402
sk_got = op.ts_skew(one, 10, min_periods=10).to_numpy()
ku_got = op.ts_kurt(one, 10, min_periods=10).to_numpy()
sk_ref, ku_ref = [], []
arr = one.to_numpy()
for t in range(9, len(one)):
    win = arr[t - 9:t + 1]
    sk_ref.append(skew(win, bias=False))
    ku_ref.append(kurtosis(win, fisher=True, bias=False))
check("ts_skew == scipy.skew(bias=False)",
      np.allclose(sk_got[9:], sk_ref, atol=1e-10, equal_nan=True))
check("ts_kurt == scipy.kurtosis(fisher, bias=False)",
      np.allclose(ku_got[9:], ku_ref, atol=1e-10, equal_nan=True))
# cs_normalize(sum_abs)
tot = op.cs_normalize(X).loc[(slice(None), t0)].abs().sum()
check("cs_normalize(sum_abs) 截面绝对值和 = 1", np.isclose(tot, 1.0))
# group_neutralize 组均值和为 0 (用算子自己求组均值, 避免混合类型 groupby 陷阱)
resid = op.group_neutralize(X, G)
gm = op.group_mean(resid, G)
check("group_neutralize(full=True) 组均值 = 0",
      bool(np.allclose(gm.dropna().to_numpy(), 0.0, atol=1e-8)))
# group_rank 落在 (0,1]
check("group_rank ∈ (0,1]", bool(((op.group_rank(X, G) > 0) &
                                 (op.group_rank(X, G) <= 1)).all()))
# cs_normalize 三种口径
mn = op.cs_normalize(X, "minmax")
check("cs_normalize(minmax) ∈ [0,1]",
      bool((mn >= -1e-12).all() and (mn <= 1 + 1e-12).all()))
check("cs_normalize(未知口径) 报错", try_raises(op.cs_normalize, X, "bogus"))

# ---------------------------------------------------------------------------
# 6. 排序无关性 + 入参校验
# ---------------------------------------------------------------------------
section("6) 排序无关性 / 错误路径")


def _canon(s: pd.Series) -> pd.Series:
    """统一成 (instrument, time) 元组并排序 —— 跨行序比较用
    ((inst,time) 与 (time,inst) 是不同的元组标签, 不能直接 reindex)。"""
    out = pd.Series(s.to_numpy(), index=pd.MultiIndex.from_arrays(
        [s.index.get_level_values("instrument"), s.index.get_level_values("time")],
        names=["instrument", "time"]))
    return out.sort_index()


# 因法重排: 时间优先 (time, instrument) —— 组内时间仍递增, 结果必须一致
Xt = X.swaplevel().sort_index()         # (time, instrument) 排列
Gt = G.swaplevel().sort_index()
Yt = Y.swaplevel().sort_index()
ONES = np.ones(len(X), bool)
same_p, _ = _same(_canon(op.ts_mean(X, 8)), _canon(op.ts_mean(Xt, 8)), ONES)
check("时间优先排序(ts_mean) 结果一致", same_p)
same_p2, _ = _same(_canon(op.group_rank(X, G)), _canon(op.group_rank(Xt, Gt)), ONES)
check("时间优先排序(group_rank) 结果一致", same_p2)
same_p3, _ = _same(_canon(op.ts_corr(X, Y, 10)), _canon(op.ts_corr(Xt, Yt, 10)), ONES)
check("时间优先排序(ts_corr) 结果一致", same_p3)

section("7) 入参校验")
expect_raises = [
    ("负 lag (取未来)", lambda: op.ts_delta(X, -1)),
    ("window=0", lambda: op.ts_mean(X, 0)),
    ("window 非整数", lambda: op.ts_mean(X, 2.5)),
    ("min_periods > window", lambda: op.ts_mean(X, 5, min_periods=9)),
    ("DataFrame 入参", lambda: op.ts_mean(pd.DataFrame({"a": X, "b": Y}), 5)),
    ("x/y 索引不一致", lambda: op.ts_corr(X, Y.iloc[:-5], 5)),
    ("x/y 长度不一致", lambda: op.ts_corr(X, Y.iloc[:-5].reset_index(drop=True), 5)),
    ("group 标签传字符串", lambda: op.group_rank(X, "sector")),
    ("group 标签长度不符", lambda: op.group_rank(X, np.arange(3))),
    ("全样本估计分布 (PIT 铁律2)", lambda: op.pp_zscore(X, by="series")),
    ("pp_is_outlier 无参照", lambda: op.pp_is_outlier(X)),
    ("pp_frac_diff 缺 window", lambda: op.pp_frac_diff(X, 0.5)),
    ("frac_diff d 越界", lambda: op.pp_frac_diff(X, 3.0, 5)),
    ("面板索引重复", lambda: op.ts_mean(pd.concat([X, X.iloc[[0]]]), 5)),
    ("instrument 内时间乱序", lambda: op.ts_mean(
        X.iloc[np.r_[5:10, 0:5, 10:len(X)]], 5)),
    ("未知算子名", lambda: op.describe_operator("ts_不存在")),
    ("未知族名", lambda: op.operator_names("xx")),
    ("ts_ewma 同时给 span+alpha", lambda: op.ts_ewma(X, span=5, alpha=0.3)),
]
for label, fn in expect_raises:
    check(f"拒绝: {label}", try_raises(fn))

section("8) 输出卫生 (无 inf / 索引保持)")
inf_ops = []
for name, fn in [(k, v) for k, v in _cases(X, G, FLAG, Y).items()]:
    s = fn()
    if not s.index.equals(X.index):
        inf_ops.append(f"{name}(索引被破坏)")
    v = s.to_numpy(dtype=float)
    if np.isinf(v).any():
        inf_ops.append(f"{name}(出现 inf)")
check("所有算子返回同索引且无 inf", not inf_ops, str(inf_ops))

section("9) 静态 PIT 审计")
viol = op.audit_pit_source()
check("源码无 center=True / shift(- / bfill / interpolate", not viol, str(viol))
demo = pd.Series(np.linspace(-3, 3, 12), index=pd.date_range("2024-01-01", periods=12, tz="UTC"))
check("平面索引(单序列)也能算 ts_*", op.ts_mean(demo, 3).notna().sum() == 12)
check("平面索引 ts_rank 可用", op.ts_rank(demo, 5).notna().sum() == 12)

# ---------------------------------------------------------------------------
print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    print("失败项:")
    for f in FAIL:
        print("   -", f)
print("=" * 74)
sys.exit(1 if FAIL else 0)
