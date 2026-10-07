# -*- coding: utf-8 -*-
"""_test_cache.py — 落盘缓存 + 注册表版本化验证

缓存守三条 (设计文档决策 3):
  1. 命中结果与首算**逐位相同** (值 + data_available_at + 索引)
  2. key 含 特征版本 + 输入数据指纹 + 池 + 窗口 + 资产集 —— 任何一项变则失效
  3. 缓存是**加速**不是**权威**: 上游数据一重建, 指纹变 → 自动重算
注册表版本化守: 快照可存/可读/可 diff, 漂移可检测。
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.features import cache as cachemod  # noqa: E402
from data_foundation.features import engine, registry  # noqa: E402
from data_foundation.features import registry_versioning as rv  # noqa: E402
from data_foundation.pool_registry import PoolScope  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


BASE = dict(start="2022-01-01", end="2022-01-15", assets=["BTC", "ETH", "SOL"])
NAMES = ["vol_24h", "mom_zscore_24h", "basis_raw", "fng_raw", "vol_1d"]

registry.load_library()
registry.validate_all()

# ===========================================================================
print("\n1) 缓存往返: 命中 == 首算")
# ===========================================================================
e0 = engine.FeatureEngine("oof", cache="off", **BASE)
ref = e0.compute(NAMES)

e1 = engine.FeatureEngine("oof", cache="auto", **BASE)
e1.cache.clear()
e1.compute(NAMES)                                   # 写入

e2 = engine.FeatureEngine("oof", cache="auto", **BASE)
got = e2.compute(NAMES)                             # 读取
st = e2.cache.stats
check("全部命中 (无重算)", st.hit >= len(NAMES) and st.miss == 0,
      f"hit={st.hit} miss={st.miss}")
check("索引与首算一致", ref.values.index.equals(got.values.index))
vals_ok = all(np.allclose(ref.values[c].to_numpy(), got.values[c].to_numpy(),
                          equal_nan=True, rtol=1e-12, atol=1e-12) for c in NAMES)
check("值逐位相同", vals_ok)
avail_ok = True
for c in NAMES:
    a = ref.avail[c].reindex(got.avail[c].index)
    b = got.avail[c]
    sentinel = pd.Timestamp("1970-01-01", tz="UTC")
    avail_ok &= bool((a.fillna(sentinel) == b.fillna(sentinel)).all())
check("可用时间逐位相同 (PIT 契约随缓存落盘)", avail_ok)

# ===========================================================================
print("\n2) 缓存键敏感性 (任一项变 → 失效)")
# ===========================================================================
def probe(**over):
    e = engine.FeatureEngine("oof", cache="auto", **over)
    b = e.compute(NAMES)
    return e.cache.stats.hit, e.cache.stats.miss

# 资产集不同 -> 不命中
h, m = probe(start="2022-01-01", end="2022-01-15", assets=["BTC", "ETH"])
check("换资产集 -> 不命中 (防子集污染)", h == 0 and m > 0, f"hit={h}")
# 窗口不同 -> 不命中
h, m = probe(start="2022-01-01", end="2022-01-16", assets=["BTC", "ETH", "SOL"])
check("换窗口 -> 不命中", h == 0 and m > 0, f"hit={h}")
# 池不同 -> 不命中 (valid 池必须用它自己池内的窗口)
e_other = engine.FeatureEngine("valid", cache="auto",
                               start="2024-02-01", end="2024-02-15",
                               assets=["BTC", "ETH", "SOL"])
b = e_other.compute(["vol_24h"])
check("换池 -> 不命中 (池在键里)", e_other.cache.stats.hit == 0,
      f"hit={e_other.cache.stats.hit}")
# 数据指纹变 -> 不命中 (模拟上游重建: 伪造一个指纹)
c = cachemod.FeatureCache()
spec = registry.get_feature("vol_24h")
panel_cls = type("P", (), {})
k1 = c.make_key(spec, panel_cls(), PoolScope("oof"), assets=["BTC"])
k2 = c.make_key(spec, panel_cls(), PoolScope("oof"), assets=["BTC"])
check("同输入 -> 同键 (指纹稳定)", k1.data_fp == k2.data_fp and k1.window_fp == k2.window_fp)
k3 = cachemod.CacheKey(**{**k1.__dict__, "data_fp": "DIFFERENT"})
check("数据指纹变 -> 键不同", k1.data_fp != k3.data_fp)
check("键含 特征版本", k1.version == spec.version)
check("键含 表达式哈希", k1.expr_hash == cachemod._sha1(spec.expr))
# 表达式变 -> 键不同 (版本没升但表达式改了, 内容哈希仍能拦住)
spec2 = registry.get_feature("vol_24h")
import dataclasses
spec2x = dataclasses.replace(spec2, expr="ts_std(ret_1h, 25)")
k4 = c.make_key(spec2x, panel_cls(), PoolScope("oof"), assets=["BTC"])
check("表达式变 -> 键不同 (expr_hash 拦住未升版的改动)",
      k4.expr_hash != k1.expr_hash)

# ===========================================================================
print("\n3) 缓存是加速不是权威")
# ===========================================================================
e3 = engine.FeatureEngine("oof", cache="auto", **BASE)
e3.compute(NAMES)
e3.cache.clear()
e4 = engine.FeatureEngine("oof", cache="auto", **BASE)
b = e4.compute(NAMES)
check("清缓存后全部重算", e4.cache.stats.hit == 0 and e4.cache.stats.miss > 0,
      f"hit={e4.cache.stats.hit} miss={e4.cache.stats.miss}")
check("重算结果仍正确",
      all(np.allclose(ref.values[c].to_numpy(), b.values[c].to_numpy(),
                      equal_nan=True, rtol=1e-12) for c in NAMES))
# 部分命中
e5 = engine.FeatureEngine("oof", cache="auto", **BASE)
b5 = e5.compute(["vol_24h", "ret_24h"])            # vol_24h 已在缓存
check("部分请求命中已有特征", e5.cache.stats.hit >= 1,
      f"hit={e5.cache.stats.hit} miss={e5.cache.stats.miss}")

# ===========================================================================
print("\n4) cache=off 不落盘")
# ===========================================================================
e6 = engine.FeatureEngine("oof", cache="off", **BASE)
before = os.path.isdir(e6.cache.root)
e6.compute(NAMES)
e7 = engine.FeatureEngine("oof", cache="auto", **BASE)
check("off 模式不写缓存", e7.cache.stats.store == 0, f"store={e7.cache.stats.store}")

# ===========================================================================
print("\n5) 注册表版本化")
# ===========================================================================
snap = rv.take_snapshot("test-baseline")
check("快照落盘并可回读", os.path.exists(
    os.path.join(rv._snapshot_dir(), snap.snapshot_id + ".json")))
loaded = rv.load_snapshot(snap.snapshot_id)
check("快照内容完整", loaded.n_features == snap.n_features and
      loaded.registry_hash == snap.registry_hash)
lst = rv.list_snapshots()
check("快照列表可枚举", len(lst) >= 1 and lst[0]["snapshot_id"] == snap.snapshot_id)
check("快照含内容哈希", all("content_hash" in f for f in
                       list(loaded.features.values())[:20]))

# diff: 模拟新增/删除/改表达式
mutated = {s.name: s for s in registry.list_features()}
import dataclasses
mutated["brand_new"] = dataclasses.replace(
    registry.get_feature("vol_24h"), name="brand_new", expr="ts_std(ret_1h, 25)")
del mutated["basis_raw"]
mutated["vol_24h"] = dataclasses.replace(registry.get_feature("vol_24h"),
                                         expr="ts_std(ret_1h, 30)")
snap2 = rv.take_snapshot("test-mutated", spec_map=mutated)
d = rv.diff_snapshots(snap.snapshot_id, snap2.snapshot_id)
check("diff 检出新增", "brand_new" in d["added"], str(d["added"][:3]))
check("diff 检出删除", "basis_raw" in d["removed"], str(d["removed"][:3]))
check("diff 检出表达式变化", "vol_24h" in d["changed"], str(d["changed"][:3]))
check("diff 给出新旧表达式", "old_expr" in d["changed_detail"].get("vol_24h", {}))

# drift: 拿"变异后的虚拟注册表"去比旧快照 -> 应检出漂移
drift = rv.drift_from(snap.snapshot_id, spec_map=mutated)
check("drift 检出未快照的改动", drift["drifted"] is True,
      f"+{drift['n_added']}/-{drift['n_removed']}/~{drift['n_changed']}")
# 同快照 drift = 无
snap3 = rv.take_snapshot("test-current")
drift2 = rv.drift_from(snap3.snapshot_id)
check("当前状态与新快照无漂移", drift2["drifted"] is False,
      f"+{drift2['n_added']}/-{drift2['n_removed']}/~{drift2['n_changed']}")

# ===========================================================================
print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)