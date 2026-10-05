# -*- coding: utf-8 -*-
"""library/liquidity.py — 流动性特征 (成交额 / 笔数 / 换手强度)

来源: market_candle_spot_1h (volume_quote / trade_count) 与现货 vs 永续成交额。
加密的"流动性"多数不是价差而是**能否进出** —— 成交额强度与相对活跃度是主力。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

feature(name="volume_zscore_24h", expr="ts_zscore(volume_quote, 24)",
        category="liquidity", desc="成交额在自身 24h 的 z 分数 (放量/缩量)",
        tags=("liquidity",))
feature(name="volume_mean_24h", expr="ts_mean(volume_quote, 24)",
        category="liquidity", desc="24 小时平均成交额", tags=("liquidity", "raw"))
feature(name="liquidity_trend", expr="ts_mean(volume_quote, 24) / ts_mean(volume_quote, 168)",
        category="liquidity", desc="短期/长期成交额之比 (流动性变化趋势)",
        tags=("liquidity",))
feature(name="amihud_illiq", expr="ts_mean(pp_abs(ret_1h) / volume_quote, 24)",
        category="liquidity", desc="Amihud 非流动性: 收益绝对值/成交额 (越高越不流动)",
        tags=("liquidity", "illiquidity"))
feature(name="trade_size_mean", expr="ts_mean(volume_quote / trade_count, 24)",
        category="liquidity", desc="平均每笔成交额 (大单/小单结构)",
        tags=("liquidity", "flow"))
feature(name="liq_cs_rank", expr="cs_rank(ts_mean(volume_quote, 24))",
        category="liquidity", desc="24h 成交额的横截面排名 (流动性相对位置)",
        tags=("liquidity", "cross_section"))