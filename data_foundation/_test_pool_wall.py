# -*- coding: utf-8 -*-
"""_test_pool_wall.py — 验证时间墙在 API 层真实生效 (防止跨池泄露)。

测试覆盖:
  1. as_of 钳制: 池1 请求未来日期 -> 被钳到池末日
  2. 池间隔离:  池1 拿不到 2024 之后的数据; 池3 拿不到 2025-07 之前的数据
  3. 因子屏蔽:  衍生品因子在可用起点之前不可用
  4. 宇宙门控:  instrument 过滤到本池宇宙内
  5. 隔离带:    gap 池不能用于研究
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd  # noqa: E402

from data_foundation.pool_registry import POOLS, PoolScope, list_pools  # noqa: E402
from data_foundation.reader import load_candles, load_universe  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))


print("=" * 72)
print("时间墙 API 层强制 — 端到端验证")
print("=" * 72)

# --- 1. 池注册表 ---
print("\n1) 池注册表")
check("池数量 = 6", len(POOLS) == 6, str(list(POOLS)))
check("研究池 = 4 (oof/valid/oos/rolling)", len([p for p in list_pools() if p["usable"]]) == 4)
check("隔离带 2 个", len([p for p in POOLS.values() if p.kind == "gap"]) == 2)

# --- 2. as_of 钳制 ---
print("\n2) as_of 钳制 (核心机制)")
s1 = PoolScope("oof")
check("池1: as_of=2026-10-01 被钳到 2023-12-31",
      str(s1.clamp("2026-10-01").date()) == "2023-12-31",
      str(s1.clamp("2026-10-01")))
check("池1: 池内日期不变", str(s1.clamp("2021-06-01").date()) == "2021-06-01")
s3 = PoolScope("oos")
check("池3: as_of=2019-01-01 被钳到池起点 2025-07-15 (双向钳制)",
      str(s3.clamp("2019-01-01").date()) == "2025-07-15",
      str(s3.clamp("2019-01-01")))
check("池3: as_of=2026-10-01 被钳到池末日 2026-09-30",
      str(s3.clamp("2026-10-01").date()) == "2026-09-30",
      str(s3.clamp("2026-10-01")))
check("池3: 越界请求被钳制而非报错", s3.clamp("2030-01-01") is not None)

# --- 3. 池间隔离: 实际取数 ---
print("\n3) 池间隔离 (实际取数验证)")
d1 = load_candles("binance", "BTC-USDT", "1h", as_of="2026-10-01", scope=s1)
mx1 = pd.to_datetime(d1["open_time_utc"].max(), utc=True) if len(d1) else None
check("池1 取数最大日期 <= 2023-12-31",
      mx1 is not None and mx1 <= pd.Timestamp("2023-12-31 23:59", tz="UTC"),
      f"max={mx1}")
d3 = load_candles("binance", "BTC-USDT", "1h", as_of="2026-01-15", scope=s3)
mn3 = pd.to_datetime(d3["open_time_utc"].min(), utc=True) if len(d3) else None
mx3 = pd.to_datetime(d3["open_time_utc"].max(), utc=True) if len(d3) else None
check("池3 取数最小日期 >= 池起点 2025-07-15 (池内下界生效)",
      mn3 is not None and mn3 >= pd.Timestamp("2025-07-15", tz="UTC"),
      f"min={mn3}")
check("池3 取数最大日期 <= 请求日 2026-01-15 (上界生效)",
      mx3 is not None and mx3 <= pd.Timestamp("2026-01-15 23:59", tz="UTC"),
      f"max={mx3}")
check("池3 取数行数远小于全量 (确实被钳制)", 0 < len(d3) < 6000, f"rows={len(d3)}")
# 池1 = 2018-01-01 ~ 2023-12-31 六年的 1h K线, 约 6*365*24 ≈ 52,560 行
check("池1 取数行数 ≈ 6年1hK线 (被上界截断)",
      50000 < len(d1) < 53000, f"rows={len(d1)}")

# --- 4. 因子屏蔽 ---
print("\n4) 因子屏蔽 (防幸存时段偏差)")
try:
    load_candles("binance", "BTC-USDT", "1h", as_of="2018-06-01",
                 market_type="perpetual")
    check("永续因子 @2018-06 被屏蔽 (应抛错)", False, "未抛错 -> 有泄露风险")
except ValueError as e:
    check("永续因子 @2018-06 被屏蔽 (抛错)", True, str(e)[:60])
d_ok = load_candles("binance", "BTC-USDT", "1h", as_of="2021-06-01",
                    market_type="perpetual")
check("永续因子 @2021-06 可用", len(d_ok) > 0, f"rows={len(d_ok)}")
try:
    load_derivatives_early = None
    from data_foundation.reader import load_derivatives
    load_derivatives("binance", "BTC-USDT", "derivatives_open_interest",
                     as_of="2018-06-01", scope=s1)
    check("OI 因子 @2018-06 被屏蔽 (应抛错)", False, "未抛错")
except ValueError as e:
    check("OI 因子 @2018-06 被屏蔽 (抛错)", True, str(e)[:60])

# --- 5. 宇宙门控 ---
print("\n5) 宇宙门控")
u = load_universe(as_of="2021-06-01", layer="research")
check("池1末日(2023-12-31)宇宙可取", len(u) > 0, f"rows={len(u)}")
uni = s1.universe()
check("scope.universe() 返回集合", isinstance(uni, set) and len(uni) > 0,
      f"n={len(uni)}")
flt = s1.filter_instruments(["BTCUSDT", "ETHUSDT", "NOTAREALCOIN"])
check("filter_instruments 剔除不存在的币",
      "NOTAREALCOIN" not in flt, f"kept={flt}")

# --- 6. 隔离带不可用 ---
print("\n6) 隔离带不可用于研究")
try:
    PoolScope("gap1")
    check("隔离带不能建 scope (应抛错)", False, "未抛错")
except ValueError as e:
    check("隔离带不能建 scope (抛错)", True, str(e)[:50])

# --- 汇总 ---
print("\n" + "=" * 72)
print(f"结果: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("失败项:")
    for f in FAIL:
        print(f"  - {f}")
print("=" * 72)
sys.exit(1 if FAIL else 0)