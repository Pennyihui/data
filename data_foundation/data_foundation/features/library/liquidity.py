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

# --- 成交量结构 (用上次没用的 volume_base / trade_count 等) --------------------
feature(name="turnover_proxy", expr="volume_quote / ts_mean(volume_quote, 168)",
        category="liquidity", desc="当前成交额/一周均额 (换手强度)",
        tags=("liquidity", "turnover"))
feature(name="volume_base_zscore", expr="ts_zscore(volume_base, 24)",
        category="liquidity", desc="币本位成交量的 24h z 分数 (与计价口径区分)",
        tags=("liquidity",))
feature(name="volume_quote_base_ratio",
        expr="volume_quote / volume_base",
        category="liquidity", desc="计价成交额/币成交量的比值 (≈当根 VWAP)",
        tags=("liquidity", "microstructure"))
feature(name="vwap_deviation", expr="close / (volume_quote / volume_base) - 1",
        category="liquidity", desc="收盘价相对当根 VWAP 的偏离 (买卖压力)",
        tags=("liquidity", "microstructure"))
feature(name="vwap_dev_mean_24h",
        expr="ts_mean(close / (volume_quote / volume_base) - 1, 24)",
        category="liquidity", desc="24h 平均 VWAP 偏离 (持续买压/卖压)",
        tags=("liquidity", "microstructure"))
feature(name="trade_size_zscore", expr="ts_zscore(volume_quote / trade_count, 24)",
        category="liquidity", desc="平均每笔成交额的 z 分数 (大单结构变化)",
        tags=("liquidity", "flow"))
feature(name="volume_price_corr", expr="ts_corr(volume_quote, pp_abs(ret_1h), 24)",
        category="liquidity", desc="成交额与波动幅度的相关性 (放量是否伴随大波动)",
        tags=("liquidity", "correlation"))
feature(name="volume_trend_change",
        expr="ts_mean(volume_quote, 24) / ts_mean(volume_quote, 168) - ts_mean(volume_quote, 168) / ts_mean(volume_quote, 720)",
        category="liquidity", desc="流动性趋势的加速度 (短/中 vs 中/长)",
        tags=("liquidity", "trend"))

# --- 永续市场流动性 (p_* 列) ---------------------------------------------------
feature(name="perp_volume_share", expr="p_volume_quote / (p_volume_quote + volume_quote)",
        category="liquidity", desc="永续成交额占两市场合计的比例 (合约主导度)",
        tags=("liquidity", "perp"))
feature(name="perp_volume_share_trend",
        expr="ts_mean(p_volume_quote / (p_volume_quote + volume_quote), 24)",
        category="liquidity", desc="24h 平均合约主导度", tags=("liquidity", "perp"))
feature(name="perp_trade_size",
        expr="ts_mean(p_volume_quote / p_trade_count, 24)",
        category="liquidity", desc="永续平均每笔成交额 (合约大单结构)",
        tags=("liquidity", "perp", "flow"))
feature(name="perp_trade_size_ratio",
        expr="ts_mean(p_volume_quote / p_trade_count, 24) / ts_mean(volume_quote / trade_count, 24)",
        category="liquidity", desc="永续/现货 平均每笔成交额之比 (资金结构差异)",
        tags=("liquidity", "perp", "cross_market"))
feature(name="perp_volume_base_zscore", expr="ts_zscore(p_volume_base, 24)",
        category="liquidity", desc="永续币本位成交量的 24h z 分数",
        tags=("liquidity", "perp"))
feature(name="perp_taker_share",
        expr="p_taker_buy_volume_quote / p_volume_quote",
        category="liquidity", desc="永续主动买入额占比 (合约买压)",
        tags=("liquidity", "perp", "flow"))
feature(name="taker_share_gap",
        expr="taker_buy_volume_quote / volume_quote - p_taker_buy_volume_quote / p_volume_quote",
        category="liquidity",
        desc="现货买压 − 合约买压 (两个市场的买卖情绪分歧)",
        tags=("liquidity", "flow", "cross_market"))
# (现货主动买入占比 = taker_buy_volume_quote/volume_quote 已在 derivatives.py 以
#  taker_buy_share 登记; 合约口径归 derivatives, 跨市场差归 liquidity)