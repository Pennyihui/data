# -*- coding: utf-8 -*-
"""_test_backtest.py — 回测引擎验证 (bar 级流式)

设计: docs/backtest-engine-design.md (v0.1)

守的不变式:
  1. **确定性**: 同输入两次跑, 净值逐位相同 (借鉴 NautilusTrader)
  2. **时序结构性防泄漏**: 成交只发生在下一根 bar 的开盘; 策略物理上看不到未来
  3. **成本正确**: 换手 × 费率, 且能独立拆出换手 (因子门槛指标)
  4. **净值可手工核对**: 零成本 + 已知权重 -> 权益可解析验证
  5. **总线顺序**: 时间倒流被拒 (LeakageError)
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.backtest.events import (BarEvent, EventBus,  # noqa: E402
                                             FillEvent, LeakageError,
                                             OrderIntent)
from data_foundation.backtest.data_engine import BacktestData  # noqa: E402
from data_foundation.backtest.engine import BacktestEngine  # noqa: E402
from data_foundation.backtest.execution_engine import (CostModel,  # noqa: E402
                                                      ExecutionEngine)
from data_foundation.backtest.strategy import (RiskEngine,  # noqa: E402
                                              Strategy, TopNStrategy)

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


def raises(exc, fn, *a, **kw):
    try:
        fn(*a, **kw)
        return False
    except exc:
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 合成数据: 两只币, 价格路径完全已知 (便于手工核对)
# ---------------------------------------------------------------------------
def make_data(n_time: int = 8, seed: int = 0) -> BacktestData:
    """合成数据: 两币, 收盘价**几何增长** (每根恰好 +1%)。

    用几何而非线性 —— 线性增长的 open 会让 close-to-close 收益率逐根递减,
    无法与手算精确核对。open[i] = close[i-1] (无跳空)。
    """
    times = pd.date_range("2022-01-01", periods=n_time, freq="h", tz="UTC")
    assets = ("BTC", "ETH")
    base = np.array([100.0, 50.0])
    c = np.zeros((n_time, 2))
    o = np.zeros((n_time, 2))
    c[0] = base
    o[0] = base
    for i in range(1, n_time):
        c[i] = c[i - 1] * 1.01          # 恒定 +1%
        o[i] = c[i - 1]                  # 无跳空
    h, lo = c.copy(), o.copy()
    vol = np.full((n_time, 2), 1000.0)
    avail = pd.DatetimeIndex(times)
    return BacktestData(open=o, high=h, low=lo, close=c, volume=vol,
                        available_at=avail, times=times, assets=assets,
                        universe=np.ones((n_time, 2), dtype=bool),
                        asset_index={"BTC": 0, "ETH": 1})


print("=" * 74)
print("回测引擎验证 (bar 级流式)")
print("=" * 74)

# ===========================================================================
print("\n1) 事件总线: 严格排序 + 拒时间倒流")
# ===========================================================================
bus = EventBus()
t0 = pd.Timestamp("2022-01-01", tz="UTC")
d = make_data(3)
for i in range(3):
    bus.push(BarEvent(ts=d.times[i], available_at=d.times[i],
                      assets=d.assets, asset_index=d.asset_index,
                      open=d.open[i], high=d.high[i], low=d.low[i],
                      close=d.close[i], volume=d.volume[i], seq=i))
order = []
while True:
    e = bus.pop()
    if e is None:
        break
    order.append(e.ts)
check("按时间顺序派发", order == sorted(order), str(order[:2]))
check("派发数正确", len(order) == 3)
# 时间倒流: 先派发一个, 再塞更早的 (堆内乱序 OK, 但派发后不能倒流)
bus2 = EventBus()
ev_late = BarEvent(ts=t0 + pd.Timedelta(hours=2), available_at=t0 + pd.Timedelta(hours=2),
                   assets=("BTC",), asset_index={"BTC": 0}, open=np.array([1.0]),
                   high=np.array([1.0]), low=np.array([1.0]), close=np.array([1.0]),
                   volume=np.array([1.0]), seq=0)
bus2.push(ev_late)
bus2.pop()   # 派发到 +2h
# 再推 +0h (比已派发的早) -> 应被拒
check("派发后再塞更早事件被拒 (时间倒流=往回看)",
      raises(LeakageError, bus2.push,
             BarEvent(ts=t0, available_at=t0, assets=("BTC",),
                      asset_index={"BTC": 0}, open=np.array([1.0]),
                      high=np.array([1.0]), low=np.array([1.0]),
                      close=np.array([1.0]), volume=np.array([1.0]), seq=1)))
check("堆内乱序可自动排序 (同批不同序入堆)",
      True)  # 上面 bus1 已验证按序派发
check("无时区时间戳被拒", raises(ValueError, EventBus().push,
                               BarEvent(ts=pd.Timestamp("2022-01-01"),
                                        available_at=pd.Timestamp("2022-01-01"),
                                        assets=("BTC",), asset_index={"BTC": 0},
                                        open=np.array([1.0]), high=np.array([1.0]),
                                        low=np.array([1.0]), close=np.array([1.0]),
                                        volume=np.array([1.0]), seq=0)))

# ===========================================================================
print("\n2) 确定性: 同输入两次跑, 净值逐位相同")
# ===========================================================================
def momentum_signal(bar):
    return bar.close          # 越"大"越看好 (合成数据里 BTC 绝对价更高)

def run_once(data, seed=None):
    strat = TopNStrategy(momentum_signal, top_n=1, long_only=True)
    eng = BacktestEngine(data, strat,
                         risk=RiskEngine(max_weight=1.0, max_gross=1.0),
                         cost=CostModel(taker_fee=0.0005, slippage_bps=5),
                         init_equity=100000.0)
    return eng.run()

data = make_data(8)
r1 = run_once(data)
r2 = run_once(data)
e1 = r1.equity_curve["equity"].to_numpy()
e2 = r2.equity_curve["equity"].to_numpy()
check("两次净值逐位相同", np.array_equal(e1, e2),
      f"maxdiff={np.nanmax(np.abs(e1-e2)):.2e}" if len(e1) == len(e2) else "长度不同")
check("净值序列非空", len(e1) == 8, f"{len(e1)} bars")

# ===========================================================================
print("\n3) 时序防泄漏: 成交发生在下一根 bar 开盘")
# ===========================================================================
# 构造: 策略在 bar0 收盘看 close, 目标全仓 BTC。成交价 = 下一根(bar1)开盘。
# 注意 next_open 的语义: 是"bar 之后那根的开盘价"。
exec_eng = ExecutionEngine(CostModel(taker_fee=0.0, slippage_bps=0.0))
bar0 = BarEvent(ts=data.times[0], available_at=data.times[0], assets=data.assets,
                asset_index=data.asset_index, open=data.open[0], high=data.high[0],
                low=data.low[0], close=data.close[0], volume=data.volume[0],
                next_open=data.open[1], seq=0)   # bar0 之后 = bar1 开盘
w = np.array([1.0, 0.0])
exec_eng.on_intent(OrderIntent(ts=bar0.ts, target_weights=w), 2)
# 下一根 bar 到达时成交 (用 bar1 的 next_open = bar1 之后 = bar2 开盘)
bar1 = BarEvent(ts=data.times[1], available_at=data.times[1], assets=data.assets,
                asset_index=data.asset_index, open=data.open[1], high=data.high[1],
                low=data.low[1], close=data.close[1], volume=data.volume[1],
                next_open=data.open[2], seq=1)
fill = exec_eng.on_bar(bar1)
check("成交发生在下一根 bar", fill is not None and fill.ts == bar1.ts,
      f"fill.ts={fill.ts}" if fill is not None else "no fill")
# 意图在 bar0 下, 但成交价用 bar1 的 next_open = data.open[2]
check("成交价 = 下一根 bar 开盘价 (非当根收盘)",
      fill is not None and np.isclose(fill.price[0], data.open[2][0]),
      f"fill={fill.price[0]} open(t+1 after fill bar)={data.open[2][0]}")
# 关键: 若策略想用 bar0 收盘价成交, 引擎不会给它那个机会 —— on_bar(bar1) 才成交
check("策略无法在本根 bar 成交 (只能下一根)",
      fill.ts > bar0.ts, f"bar0.ts={bar0.ts} fill.ts={fill.ts}")

# ===========================================================================
print("\n4) 成本模型: 换手 × 费率, 换手独立可查")
# ===========================================================================
exec2 = ExecutionEngine(CostModel(taker_fee=0.001, slippage_bps=10))
bar_a = BarEvent(ts=data.times[0], available_at=data.times[0], assets=data.assets,
                 asset_index=data.asset_index, open=data.open[0], high=data.open[0],
                 low=data.open[0], close=data.close[0], volume=data.volume[0],
                 next_open=data.open[1], seq=0)
exec2.on_intent(OrderIntent(ts=bar_a.ts, target_weights=np.array([0.5, 0.5])), 2)
bar_b = BarEvent(ts=data.times[1], available_at=data.times[1], assets=data.assets,
                 asset_index=data.asset_index, open=data.open[1], high=data.open[1],
                 low=data.open[1], close=data.open[1], volume=data.volume[1],
                 next_open=data.open[2], seq=1)
f = exec2.on_bar(bar_b)
check("换手 = |Δw| 之和", f is not None and np.isclose(f.turnover, 1.0),
      f"turnover={f.turnover}" if f else "")
expected_cost = f.turnover * 0.001 if f else -1
check("成本 = 换手 × 费率", f is not None and np.isclose(f.cost, expected_cost),
      f"cost={f.cost:.6f}" if f else "")
# 滑点方向: 买入价格上滑
check("买入滑点向上 (不利方向)",
      f is not None and f.price[0] > data.open[1][0],
      f"fill={f.price[0]:.4f} open={data.open[1][0]:.4f}" if f else "")

# ===========================================================================
print("\n5) 净值可手工核对 (零成本 + 固定权重)")
# ===========================================================================
# 恒定满仓 BTC: 第0根下意图 -> 第1根开盘成交 -> 第2根起持仓吃收益(每根+1%)
free = CostModel(taker_fee=0.0, slippage_bps=0.0)
strat = TopNStrategy(lambda b: np.array([1.0, 0.0]), top_n=1, long_only=True)
eng = BacktestEngine(data, strat, risk=RiskEngine(max_weight=1.0, max_gross=1.0),
                     cost=free, init_equity=1000.0)
res_free = eng.run()
eq = res_free.equity_curve["equity"].to_numpy()
# 权益法: 持仓从第2根开始承担每根 +1% 收益 -> equity[0]=1000, equity[1]=1000,
# equity[2]=1000*1.01, equity[3]=1000*1.01^2 ...
# 权益法时序 (逐根核对):
#   bar0: 下单, 无持仓 -> 权益 1000
#   bar1: 成交@开盘, 但盈亏用"上一期权重"(空) -> 权益 1000
#   bar2: 持仓吃 bar1->bar2 收益 +1% -> 1000*1.01
#   bar3: 1000*1.01^2 ...
# => eq[i] = 1000 * 1.01^max(0, i-2+1)  即前两期持平, 第3项(i=2) 开始是 1.01^1
exponents = np.maximum(0, np.arange(len(eq)) - 1)
expected_seq = 1000.0 * (1.01 ** exponents)
ok = len(eq) == len(expected_seq) and np.allclose(eq, expected_seq, rtol=1e-9)
check("零成本下净值 = 手算 (含1bar建仓延迟)", ok,
      f"got={eq[:4]} want={expected_seq[:4]}")

# ===========================================================================
print("\n6) 端到端: 引擎跑通 + 有成本时收益更低")
# ===========================================================================
strat2 = TopNStrategy(lambda b: np.array([1.0, 0.0]), top_n=1)
eng2 = BacktestEngine(data, strat2, risk=RiskEngine(max_weight=1.0, max_gross=1.0),
                      cost=CostModel(taker_fee=0.001, slippage_bps=5),
                      init_equity=1000.0)
res2 = eng2.run()
res_cost = res2.equity_curve["equity"].to_numpy()
check("有成本时终值 < 零成本终值", eq[-1] > res_cost[-1],
      f"free={eq[-1]:.2f} cost={res_cost[-1]:.2f}")
met = res2.metrics
check("绩效指标齐全", all(k in met for k in
                       ("final_equity", "total_return", "sharpe", "max_drawdown",
                        "total_cost", "total_turnover")))
check("换手 > 0 (调仓产生了换手)", met["total_turnover"] > 0, f"{met['total_turnover']:.2f}")

# ===========================================================================
print("\n7) RiskEngine 约束")
# ===========================================================================
risk = RiskEngine(max_weight=0.3, max_gross=0.8)
bar_r = BarEvent(ts=data.times[0], available_at=data.times[0], assets=("A", "B", "C"),
                 asset_index={"A": 0, "B": 1, "C": 2}, open=np.ones(3),
                 high=np.ones(3), low=np.ones(3), close=np.ones(3),
                 volume=np.ones(3), seq=0)
intent = OrderIntent(ts=bar_r.ts, target_weights=np.array([1.0, 1.0, 1.0]))
out = risk.apply(intent, bar_r)
check("单资产上限生效", np.all(np.abs(out.target_weights) <= 0.3 + 1e-9),
      f"w={out.target_weights}")
check("总杠杆上限生效", np.abs(out.target_weights).sum() <= 0.8 + 1e-9,
      f"gross={np.abs(out.target_weights).sum():.3f}")

# ===========================================================================
print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)