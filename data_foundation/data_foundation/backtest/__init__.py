# -*- coding: utf-8 -*-
"""backtest — bar 级流式 (事件驱动) 回测引擎

设计: docs/backtest-engine-design.md (v0.1)
架构借鉴: NautilusTrader (事件驱动 / 回测实盘同路径 / 确定性)

时序防泄漏是结构性的: 策略物理上看不到下一根 bar, 成交只发生在下一根开盘。
"""
from .events import BarEvent, EventBus, FillEvent, LeakageError, OrderIntent
from .data_engine import BacktestData, DataEngine, build_data
from .execution_engine import CostModel, ExecutionEngine, Portfolio
from .strategy import RiskEngine, Strategy, TopNStrategy
from .engine import BacktestEngine, BacktestResult

__all__ = ["BarEvent", "EventBus", "FillEvent", "LeakageError", "OrderIntent",
           "BacktestData", "DataEngine", "build_data",
           "CostModel", "ExecutionEngine", "Portfolio",
           "RiskEngine", "Strategy", "TopNStrategy",
           "BacktestEngine", "BacktestResult"]
