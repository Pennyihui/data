# -*- coding: utf-8 -*-
"""events.py — 回测事件模型与事件总线（流式回测引擎核心）

设计: docs/backtest-engine-design.md (v0.1)
借鉴: NautilusTrader 的事件驱动 / 确定性 / 回测实盘同路径

这里定的是**地基**: 事件不可变、总线按 (ts, seq) 严格排序、派发只给过去。
防泄漏不是靠"记得别用未来数据"的纪律, 而是靠"物理上喂不到"—— 引擎是这个
纪律的结构性载体。

关键不变式:
  1. 事件 frozen (不可变): 派发后无法被篡改, 避免"策略偷偷改了 bar"
  2. 总线严格按 (ts, seq) 排序: 同一输入 -> 逐位相同输出 (确定性)
  3. Strategy 只能收到 ts <= 当前决策时点的事件: 未来数据在总线上就排不到前面去
"""
from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

__all__ = ["BarEvent", "OrderIntent", "FillEvent", "EventBus",
           "LeakageError", "TS_ONLY"]


class LeakageError(RuntimeError):
    """引擎在结构上发现了未来信息的使用 (试图用尚未到达的数据)。"""


#: 序列号计数器 (同一 ts 内的事件按产生顺序排列, 保证确定性)
_SEQ = itertools.count()


def _next_seq() -> int:
    return next(_SEQ)


@dataclass(frozen=True)
class BarEvent:
    """一根 bar 已收盘、其数据在 data_available_at 时点变为可见。

    这是 Strategy 能看到的**唯一**数据入口。引擎保证只派发"其
    data_available_at <= 决策时点"的 bar —— 未来的 bar 不会进入总线。

    字段:
      ts          决策时刻 = 该 bar 的可见时刻 (收盘)
      available_at data_available_at (何时才知道这根 bar)
      assets      该时刻可交易的资产 (当日 PIT 宇宙成员, 无幸存者偏差)
      open/high/low/close  该 bar 的价格
      bar_time    面板网格时间 (open_time) —— 信号查表用
      bar_index   在 BacktestData 里的行号 —— 仅引擎用来定位下一根

    **刻意不含 next_open (下一根 bar 的开盘价)**: 它是"未来价格", 挂在策略
    能拿到的事件上就等于留了泄漏口。成交价由 BacktestEngine 从自己的数据引用
    里取出, 作为参数显式传给 ExecutionEngine —— 策略拿不到, 因为它根本不在
    策略看得见的对象上。
    """

    ts: pd.Timestamp              # **决策时刻** = 该 bar 的收盘/可见时刻
    available_at: pd.Timestamp    # data_available_at (应与 ts 相等)
    assets: tuple[str, ...]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    asset_index: dict[str, int]
    bar_time: pd.Timestamp | None = None      # 面板网格时间 (open_time, 信号查表用)
    bar_index: int = -1      # 在 BacktestData 里的行号 (引擎内部定位下一根)
    seq: int = field(default_factory=_next_seq, compare=False)

    @property
    def n_assets(self) -> int:
        return len(self.assets)

    def price(self, name: str, which: str = "close") -> np.ndarray:
        arr = {"open": self.open, "high": self.high,
               "low": self.low, "close": self.close}.get(which)
        if arr is None:
            raise KeyError(which)
        return arr

    def ret(self) -> np.ndarray:
        """本 bar 收益率 (close/open - 1), NaN 安全。"""
        with np.errstate(divide="ignore", invalid="ignore"):
            r = self.close / self.open - 1.0
        return np.where(np.isfinite(r), r, 0.0)


@dataclass(frozen=True)
class OrderIntent:
    """策略输出的**目标仓位** (权重, 不是订单流)。

    用目标权重而不是逐笔买卖单, 是为了让回测与实盘同路径: 实盘里策略同样
    是"我要保持什么仓位", 由 RiskEngine + ExecutionEngine 把它落到具体委托。
    """

    ts: pd.Timestamp
    target_weights: np.ndarray        # 长度 = BarEvent.n_assets, 单位小数
    reason: str = ""
    seq: int = field(default_factory=_next_seq, compare=False)

    def validate(self, n_assets: int) -> None:
        w = np.asarray(self.target_weights, dtype=float)
        if w.shape != (n_assets,):
            raise ValueError(
                f"target_weights 形状 {w.shape} 应为 ({n_assets},) —— "
                f"必须与 BarEvent.assets 一一对应 (顺序即 assets 顺序)")
        if not np.all(np.isfinite(w)):
            raise ValueError("target_weights 含 NaN/Inf")


@dataclass(frozen=True)
class FillEvent:
    """一笔成交 (或一次调仓的汇总)。"""

    ts: pd.Timestamp                 # 成交时刻 (= 被成交 bar 的开盘时刻)
    filled_weights: np.ndarray       # 实际成交的权重变化 (Δw)
    price: np.ndarray                # 成交价 (含滑点)
    cost: float                      # 本次成交总成本 (手续费 + 滑点, 正数)
    turnover: float                  # 本期换手 (|Δw| 之和)
    seq: int = field(default_factory=_next_seq, compare=False)


class EventBus:
    """事件总线: 严格按 (ts, seq) 派发。

    用**优先队列**而非列表排序, 因为流式回测下事件是陆续产生的; 但对同一批
    历史事件, 堆的弹出顺序与按 (ts, seq) 全排序等价 —— 这是确定性的来源。

    泄漏防护: push 时校验 event.ts >= last_dispatched_ts。不允许把时间戳更早的
    事件塞进来 (那是"往回看"), 也不允许同一 seq 重复。
    """

    def __init__(self):
        self._heap: list = []
        self._counter = itertools.count()
        self._last_dispatched: pd.Timestamp | None = None
        self.dispatched = 0

    def push(self, event) -> None:
        ts = pd.Timestamp(event.ts)
        if ts.tzinfo is None:
            raise ValueError("事件时间戳必须带时区 (统一 UTC)")
        if self._last_dispatched is not None and ts < self._last_dispatched:
            raise LeakageError(
                f"事件时间 {ts} 早于已派发的 {self._last_dispatched} —— "
                f"流式总线不允许时间倒流 (那是往回看未来)")
        heapq.heappush(self._heap, (ts, next(self._counter), event))

    def pop(self):
        """弹出下一个事件 (按 ts 严格升序); 空则 None。"""
        if not self._heap:
            return None
        ts, _, event = heapq.heappop(self._heap)
        if self._last_dispatched is not None and ts < self._last_dispatched:
            raise LeakageError("派发顺序倒流 (确定性被破坏)")
        self._last_dispatched = ts
        self.dispatched += 1
        return event

    def drain(self, handler: Callable):
        """一直派发到空。handler(event)。"""
        while True:
            ev = self.pop()
            if ev is None:
                break
            handler(ev)

    def __len__(self):
        return len(self._heap)

    def peek_ts(self):
        return self._heap[0][0] if self._heap else None