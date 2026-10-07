# -*- coding: utf-8 -*-
"""data_engine.py — DataEngine: PIT 认证面板 → BarEvent 流

设计: docs/backtest-engine-design.md (v0.1) §3/§4

职责 (只做这一件):
  把认证层 K 线面板变成**逐根 bar 的事件流**, 每个 BarEvent 带:
    - 当日 PIT 宇宙成员 (无幸存者偏差)
    - 已收盘 bar 的 OHLCV
    - data_available_at (何时可见) —— 引擎据此做最后一道 PIT 闸门
    - 下一根 bar 的开盘价 (只给 ExecutionEngine 成交用)

它不认识策略、不算收益、不做决策。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .events import BarEvent, EventBus, LeakageError

__all__ = ["BacktestData", "DataEngine"]


@dataclass
class BacktestData:
    """回测所需的全部输入 (已按 (asset, time) 排好)。

    prices: (n_time, n_asset) 的 OHLCV; assets 给出列顺序 (排序固定, 保证确定性)
    universe: (n_time, n_asset) 布尔 —— 当日是否可交易 (逐日 PIT)
    available_at: (n_time,) 每根 bar 的 data_available_at
    """

    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    available_at: pd.DatetimeIndex
    times: pd.DatetimeIndex
    assets: tuple[str, ...]
    universe: np.ndarray            # 布尔 (n_time, n_asset)
    asset_index: dict[str, int]

    @property
    def n_time(self) -> int:
        return len(self.times)

    @property
    def n_asset(self) -> int:
        return len(self.assets)


class DataEngine:
    """把面板变成事件流。只发"收盘且已可见"的 bar。"""

    def __init__(self, data: BacktestData, bus: EventBus,
                 decision_lag: pd.Timedelta = pd.Timedelta(0)):
        """decision_lag: 从 bar 收盘到决策时点的额外延迟 (默认 0 = 收盘即决策)。
        大于 0 时策略看到的是"收盘后 lag 再决策", 成交相应更晚 —— 用于模拟
        "信号要等一会儿才能执行"。"""
        self.data = data
        self.bus = bus
        self.decision_lag = pd.Timedelta(decision_lag)

    def _visible_assets(self, t: int) -> tuple[str, ...]:
        """当日 PIT 宇宙成员 (逐日快照) —— 不含当时已退市/未上市的币。"""
        mask = self.data.universe[t, :]      # 行=t (时间), 列=资产
        return tuple(a for a, ok in zip(self.data.assets, mask) if ok)

    def stream(self):
        """把全部 bar 推进总线。

        关键: 第 t 根 bar 的 BarEvent 在**收盘时刻**派发, 且 next_open 是第
        t+1 根的开盘价 —— 策略在 T 看到的数据永远不含 T+1 的价格。
        """
        d = self.data
        for t in range(d.n_time):
            assets = self._visible_assets(t)
            if not assets:
                # 当日无任何可交易资产: 不派发 (策略无事可做)
                continue
            # 只保留当日可交易资产的数据列 (逐期资产集合可变 → 无幸存者偏差)
            idx = [d.asset_index[a] for a in assets]
            sub = {
                "open": d.open[t, idx],
                "high": d.high[t, idx],
                "low": d.low[t, idx],
                "close": d.close[t, idx],
                "volume": d.volume[t, idx],
            }
            # next_open: 下一根 bar 的开盘价 (成交价来源; 最后一根没有 → None)
            nxt = d.open[t + 1, idx] if t + 1 < d.n_time else None
            # PIT 闸门: bar 的可见时刻 (available_at) 不得**倒退** —— 数据的可用时间随
            # 事件时间单调不减; 倒退说明数据底座的 avail 有问题。
            if t > 0 and pd.notna(d.available_at[t]) and \
                    pd.notna(d.available_at[t - 1]) and \
                    d.available_at[t] < d.available_at[t - 1]:
                raise LeakageError(
                    f"bar {d.times[t]} 的 data_available_at({d.available_at[t]}) "
                    f"早于上一根({d.available_at[t-1]}) —— 可用时间倒流, "
                    f"认证层异常")
            # 决策时刻 = bar 的可见时刻 (收盘); 面板网格时间 = open_time (查表用)
            decision_ts = d.available_at[t] if pd.notna(d.available_at[t]) \
                else d.times[t]
            ev = BarEvent(
                ts=decision_ts,
                available_at=d.available_at[t],
                assets=assets,
                asset_index={a: i for i, a in enumerate(assets)},
                next_open=nxt,
                bar_time=d.times[t],
                seq=t,           # 固定 seq = t, 保证确定性 (不依赖全局计数器)
                **sub,
            )
            self.bus.push(ev)


def build_data(panel, assets=None) -> BacktestData:
    """从 fields.Panel 构造 BacktestData (含逐日 PIT 宇宙)。

    panel.values: MultiIndex (base_asset, time) 的价格面板
    panel.avail : 同形状的 data_available_at
    assets     : 参与回测的资产 (默认面板里全部)
    """
    v = panel.values
    if "close" not in v.columns:
        raise KeyError("面板缺少 close 列 (回测至少需要收盘价)")
    assets = tuple(sorted(assets or v.index.get_level_values(0).unique()))
    times = pd.DatetimeIndex(sorted(v.index.get_level_values("time").unique()))
    asset_index = {a: i for i, a in enumerate(assets)}

    def mat(col, dtype=float):
        """取一列 -> (n_time, n_asset) 矩阵 (时间优先, 与引擎消费顺序一致)。"""
        if col not in v.columns:
            return np.full((len(times), len(assets)), np.nan, dtype=dtype)
        s = v[col].unstack(level="time").reindex(index=assets, columns=times)
        # unstack 给的是 (asset, time); 转成 (time, asset) 供 d.open[t, idx] 消费
        return np.ascontiguousarray(s.to_numpy(dtype=dtype, copy=True).T)

    o = mat("open")
    h = mat("high") if "high" in v.columns else o.copy()
    lo = mat("low") if "low" in v.columns else o.copy()
    c = mat("close")
    vol = mat("volume_quote") if "volume_quote" in v.columns else np.zeros_like(c)

    # available_at: 每根 bar 取该时间所有资产的 max (保守: 整根 bar 完全可见的时刻)
    av = panel.avail["close"].unstack(level="time").reindex(index=assets, columns=times)
    available_at = pd.DatetimeIndex([
        pd.Timestamp(av.iloc[:, t].max()) if pd.notna(av.iloc[:, t]).any()
        else pd.NaT for t in range(len(times))])

    # 逐日 PIT 宇宙: 面板里该时刻没有 close 的资产 = 当时不可交易 (未上市/已退市)
    universe = ~np.isnan(c)                    # (n_time, n_asset)

    return BacktestData(open=o, high=h, low=lo, close=c, volume=vol,
                        available_at=available_at, times=times, assets=assets,
                        universe=universe, asset_index=asset_index)