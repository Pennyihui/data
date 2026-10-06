# -*- coding: utf-8 -*-
"""_test_fields.py — F2 字段层 + PIT 引擎验证 (真实数据)

覆盖:
  1. 字段注册表: 名字唯一 / 无污染源 / 三列齐全
  2. 时间墙: 池钳制 / 隔离带拒绝 / 污染字段拒绝 / 宇宙门控 / 开放池必须给 end
  3. 真实面板: BTC 现货 close + 永续 funding_rate, 池1 (OOF) 内装载
  4. PIT 可用性: close 的可用时间 = bar 收盘时刻; funding = 结算时刻
  5. 可用时间传播: point/ts/cs/ expanding, 与手工 rolling max 对照
  6. 泄漏自检: 正常特征通过; 人为把可用时间提前 -> 必须报错
  7. 预热窗口: warmup 行真实存在于面板 (池起点之前的历史)
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import operators as op  # noqa: E402
from data_foundation import fields as F  # noqa: E402
from data_foundation.pool_registry import PoolScope  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


def try_raises(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return False
    except Exception:
        return True


print("=" * 74)
print("F2 字段层 + PIT 引擎验证")
print("=" * 74)

# --- 1. 注册表 -------------------------------------------------------------
print("\n1) 字段注册表")
check("字段数量 >= 30", len(F.FIELD_REGISTRY) >= 30, str(len(F.FIELD_REGISTRY)))
check("名字唯一", len(F.FIELD_REGISTRY) == len(F._FIELDS))
contaminated = [n for n, f in F.FIELD_REGISTRY.items()
                if f.dataset in ("macro_daily", "cm_asset_daily", "btc_network_daily",
                                 "stablecoin_supply", "stablecoin_flows", "dex_volume")]
check("注册表不含修订污染/无PIT列数据集的字段", not contaminated, str(contaminated))
check("未知字段报错", try_raises(F.get_field, "不存在"))
fams = {f["family"] for f in F.list_fields()}
check("family 分类合理", fams == {"price", "derivatives", "sentiment"}, str(fams))

# --- 2. 时间墙 -------------------------------------------------------------
print("\n2) 时间墙 (API 层强制)")
check("隔离带拒绝", try_raises(PoolScope, "gap1"))

scope = PoolScope("oof")
# 造一个指向污染数据集的字段并尝试装载 -> 必须报错 (字段层第二道闸)
bad_spec = F.FieldSpec("macro_close", "macro_daily", "close", "date_utc", "spot")
F.FIELD_REGISTRY["macro_close"] = bad_spec
try:
    err = try_raises(F.load_panel, scope, ["macro_close"], assets=["BTC"],
                     start="2021-06-01", end="2021-06-30")
finally:
    del F.FIELD_REGISTRY["macro_close"]
check("污染数据集字段在字段层被硬屏蔽", err)
check("开放池不给 end 报错", try_raises(
    F.load_panel, PoolScope("rolling_oos"), ["close"], assets=["BTC"]))
check("宇宙门控 (不在宇宙的资产) 报错", try_raises(
    F.load_panel, scope, ["close"], assets=["NOT_A_COIN"],
    start="2022-01-01", end="2022-01-31"))

# --- 3. 真实面板 -----------------------------------------------------------
print("\n3) 真实面板装载 (池1 OOF, BTC + ETH)")
panel = F.load_panel(scope, ["close", "funding_rate"], assets=["BTC", "ETH"],
                     start="2022-01-01", end="2022-01-31", warmup="240h")
print(" ", panel)
v = panel.values
a = panel.avail
check("面板非空", len(v) > 0, f"{len(v):,} 行")
check("索引层级命名 (base_asset, time)", list(v.index.names) == ["base_asset", "time"])
check("列 = 字段", set(v.columns) == {"close", "funding_rate"})
check("可用时间面板同形状", a.shape == v.shape)
check("值面板无全空列", v.notna().any().all())
check("时间墙: as_of 被钳制在池内", panel.as_of <= scope.pool.end_ts)
check("时间墙: 面板时间不晚于 end", v.index.get_level_values("time").max() <= panel.end)
check("时间墙: 可用时间不晚于 as_of", a.stack().dropna().max() <= panel.as_of)
check("宇宙内资产", set(v.index.get_level_values("base_asset")) <= {"BTC", "ETH"})
check("provenance 记录了 venue", all(len(t) == 2 for t in panel.provenance.values()),
      str(list(panel.provenance.items())[:2]))
check("资产内时间严格递增",
      all(g.index.is_monotonic_increasing
          for _, g in v["close"].groupby(level="base_asset")))

# --- 4. PIT 可用性 ----------------------------------------------------------
print("\n4) PIT 可用性 (真实数据)")
ac = a["close"].dropna()
af = a["funding_rate"].dropna()
lag_h = (ac - ac.index.get_level_values("time")).dt.total_seconds() / 3600
check("close 可用时间 == bar 收盘 (lag ∈ [0, 1h])",
      bool(((lag_h >= 0) & (lag_h <= 1.0)).all()), f"lag 范围 [{lag_h.min():.3f}, {lag_h.max():.3f}]h")
lag_f = (af - af.index.get_level_values("time")).dt.total_seconds() / 3600
check("funding 可用时间 == 结算时刻 (lag ∈ [0, 0.02h])",
      bool(((lag_f >= 0) & (lag_f <= 0.02)).all()),
      f"lag 范围 [{lag_f.min():.4f}, {lag_f.max():.4f}]h")

# --- 5. 可用时间传播 --------------------------------------------------------
print("\n5) 可用时间传播 (PIT 引擎核心规则)")
close = v["close"].dropna()
avail_close = a["close"].reindex(close.index)
# point
p = F.propagate_availability("point", avail_close)
check("point 传播 == 原值", p.equals(avail_close))
# ts: 与手工 shift+maximum 对照 (全程 int64, 避免 rolling 把 int64 升成 float 丢精度)
got = F.propagate_availability("ts", avail_close, window=24)
sec = avail_close.astype("int64")
manual = sec.copy()
for k in range(1, 24):
    manual = np.maximum(manual, sec.groupby(level="base_asset", sort=False).shift(k))
cmp_ok = True
for asset, g in got.groupby(level="base_asset"):
    d = (g.astype("int64") - manual.loc[g.index]).abs().max()
    cmp_ok &= bool(d == 0)            # int64 精确, 不许有 1ns 之差
check("ts 传播 == 窗口内 max (手工对照, 逐位相等)", cmp_ok)
# 窗口内 max >= 当前行可用时间 (单调上界)
check("ts 传播 >= 当前行可用时间", bool((got >= avail_close).all()))
# cs (跳过 funding 独有的 NaT 行 —— 那些行 close 没有数据, 比较无意义)
z = op.cs_rank(v["close"])
az = F.propagate_availability("cs", a["close"])
both = az.notna() & a["close"].notna()
check("cs 传播 >= 当前行可用时间 (非 NaT 行)", bool((az[both] >= a["close"][both]).all()))
cs_same = a["close"].groupby(level="time").transform("max")
check("cs 传播 == groupby(time).transform(max)", bool((az[both] == cs_same[both]).all()))
# expanding
ew = op.ts_ewma(v["close"], span=20)
ae = F.propagate_availability("ts_unbounded", a["close"])
ok_rows = ae.notna() & got.reindex(ae.index).notna()
check("expanding 传播 >= 任意窗口内 max (非 NaT 行)",
      bool((ae[ok_rows] >= got.reindex(ae.index)[ok_rows]).all()))
# 未知 kind
check("未知传播类型报错", try_raises(F.propagate_availability, "bogus", a["close"]))
check("kind='ts' 缺 window 报错", try_raises(F.propagate_availability, "ts", a["close"]))

# --- 6. 端到端特征 + 泄漏自检 ------------------------------------------------
print("\n6) 端到端: 特征计算 + 泄漏自检 (真实数据)")
feat = op.ts_zscore(close, 168)
feat_avail = F.propagate_availability("ts", avail_close, window=168)
in_max = F.assert_no_leakage(feat_avail, {"close": avail_close},
                             name="ts_zscore(close,168)", feature_values=feat)
check("ts_zscore(close,168) 通过泄漏自检", True,
      f"特征可用时间 >= 输入 max (最大滞后 {(feat_avail - in_max).max()})")
check("返回了输入最大可用时间", isinstance(in_max, pd.Series))

# 资金费率特征: funding 是 8h 网格, 先 ffill 到 bar 语义缺失 -> 直接在自身网格上算
fr = v["funding_rate"].dropna()
avail_fr = a["funding_rate"].reindex(fr.index)
fr_rank = op.ts_rank(fr, 90)
fr_avail = F.propagate_availability("ts", avail_fr, window=90)
F.assert_no_leakage(fr_avail, {"funding_rate": avail_fr},
                    name="ts_rank(funding_rate,90)", feature_values=fr_rank)
check("ts_rank(funding_rate,90) 通过泄漏自检", True, f"{len(fr_rank):,} 行")

# 截面特征
z_cs = op.cs_rank(v["close"])
z_avail = F.propagate_availability("cs", a["close"])
F.assert_no_leakage(z_avail, {"close": a["close"]}, name="cs_rank(close)",
                    feature_values=z_cs)
check("cs_rank(close) 通过泄漏自检", True)

# 人为泄漏: 把特征可用时间提前 2 小时 -> 必须报错
shifted = feat_avail - pd.Timedelta(hours=2)
check("可用时间提前 2h -> 泄漏自检报错",
      try_raises(F.assert_no_leakage, shifted, {"close": avail_close},
                 "leaky", feat))
# 人为: 两个输入里其中一个某行缺失 (NaT) -> 该输入该行被跳过, 取另一个, 不误报
a1 = avail_close
a2 = avail_close + pd.Timedelta(hours=1)
two_feat = a2                                  # 特征可用时间 = max(a1, a2) = a2
mid = a1.index[50]
a1_nat = a1.copy()
a1_nat.loc[mid] = pd.NaT                       # 只打掉一个输入
try:
    F.assert_no_leakage(two_feat, {"x": a1_nat, "y": a2}, "partial_nat")
    ok_skip, detail = True, "按行 skipna, 不误报"
except AssertionError as exc:
    ok_skip, detail = False, str(exc)[:140]
check("两输入其一 NaT -> 按行跳过不误报", ok_skip, detail)
# 同一行两输入皆 NaT 而特征有值 -> ghost 规则必须报错
vals = pd.Series(np.arange(len(a1), dtype=float), index=a1.index)
both_nat = {"x": a1.copy(), "y": a2.copy()}
both_nat["x"].loc[mid] = pd.NaT
both_nat["y"].loc[mid] = pd.NaT
# 同一行两输入皆缺失但特征有值 -> **合法** (窗口算子用窗口里更早的 bar),
# 只要传播出的可用时间仍保守; 只有"全部输入整体无时间"才是绕过引擎。
vals = pd.Series(np.arange(len(a1), dtype=float), index=a1.index)
both_nat = {"x": a1.copy(), "y": a2.copy()}
both_nat["x"].loc[mid] = pd.NaT
both_nat["y"].loc[mid] = pd.NaT
try:
    F.assert_no_leakage(two_feat, both_nat, "window_ok", vals)
    ok_win, det_win = True, "窗口内更早数据可用 -> 允许"
except AssertionError as exc:
    ok_win, det_win = False, str(exc)[:120]
check("同排两输入皆 NaT 但窗口内有值 -> 允许 (窗口语义)", ok_win, det_win)
# 全部输入的可用时间整体为空 -> 必须报错 (真的绕过了引擎)
empty = {"x": pd.Series(pd.NaT, index=a1.index, dtype="datetime64[ns, UTC]")}
check("全部输入无可用时间 -> 泄漏自检报错",
      try_raises(F.assert_no_leakage, two_feat, empty, "no_time", vals))
# 全输入无数据但特征有值 -> 必须报错
all_nat = pd.Series(pd.NaT, index=avail_close.index, dtype="datetime64[ns, UTC]")
check("全输入 NaT 而特征有值 -> 泄漏自检报错",
      try_raises(F.assert_no_leakage, feat_avail, {"close": all_nat},
                 "ghost", feat))
check("特征有值但可用时间为 NaT -> 报错",
      try_raises(F.assert_no_leakage,
                 pd.Series(pd.NaT, index=feat_avail.index,
                           dtype="datetime64[ns, UTC]"),
                 {"close": avail_close}, "noavail", feat))
check("input_avails 为空 -> 报错", try_raises(F.assert_no_leakage, feat_avail, {}))

# --- 7. 预热窗口 ------------------------------------------------------------
print("\n7) 预热窗口 (池边界是研究边界不是数据边界)")
p2 = F.load_panel(scope, ["close"], assets=["BTC"], start="2018-01-05",
                  end="2018-01-10", warmup="240h")
times = p2.values.index.get_level_values("time")
check("warmup 行真实存在 (面板含池起点之前的数据)",
      bool(times.min() < p2.scope.pool.start_ts),
      f"最早 {times.min()}")
check("研究窗口不越池 (end 钳制)", times.max() <= p2.end)
p3 = F.load_panel(scope, ["close"], assets=["BTC"], start="2018-01-05",
                  end="2018-01-10", warmup=pd.Timedelta(0))
t3 = p3.values.index.get_level_values("time")
check("warmup=0 时面板从池起点/窗口起点开始", bool(t3.min() >= p3.start),
      f"最早 {t3.min()}")

# --- 8. 窗口钳制 (区间语义 = 与池求交集) ------------------------------------
print("\n8) 窗口钳制 (区间与池求交集; 点时间才是 clamp)")
# 完全落在池之后 -> 交集为空 -> 显式报错 (不静默返回 1 天窗口)
check("窗口完全在池后 -> 报无交集", try_raises(
    F.load_panel, scope, ["close"], assets=["BTC"],
    start="2026-09-01", end="2026-12-31", as_of="2026-12-31"))
# 完全落在池之前 -> 同样报无交集
check("窗口完全在池前 -> 报无交集", try_raises(
    F.load_panel, scope, ["close"], assets=["BTC"],
    start="2015-01-01", end="2016-12-31"))
# 跨越池右边界 -> 求交集: end 被截到池终点
p4 = F.load_panel(scope, ["close"], assets=["BTC"], start="2023-06-01",
                  end="2025-01-01", as_of="2025-01-01")
check("跨越池右边界 -> end 截到池终点", p4.end == scope.pool.end_ts,
      str(p4.end.date()))
check("跨越池右边界 -> start 保持请求值", p4.start == pd.Timestamp("2023-06-01", tz="UTC"),
      str(p4.start.date()))
t4 = p4.values.index.get_level_values("time")
check("面板时间不越池终点", t4.max() <= scope.pool.end_ts, str(t4.max()))
check("面板可用时间不越池终点 (as_of 钳制生效)",
      p4.avail.stack().dropna().max() <= scope.pool.end_ts,
      str(p4.avail.stack().dropna().max()))
check("as_of 钳到 min(请求, 池终点)", p4.as_of == scope.pool.end_ts, str(p4.as_of))
# 池起点处的 warmup: 池起点是研究边界, warmup 读更早历史合法 (不是别的池)
p5 = F.load_panel(scope, ["close"], assets=["BTC"], start="2018-01-02",
                  end="2018-01-20", warmup="48h")
check("warmup 允许早于池起点", bool(
    p5.values.index.get_level_values("time").min() < scope.pool.start_ts),
    str(p5.values.index.get_level_values("time").min().date()))

# ---------------------------------------------------------------------------
print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    print("失败项:")
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)
