# -*- coding: utf-8 -*-
"""library/multiscale.py — 多时间尺度特征 (4h / 日线 / 周线 / 月线)

数据来源: 数据底座从 1h 聚合派生的 4h/1d/1w/1M 认证 K 线 (2026-10-05 全量重建,
spot 590 / perp 377 标的, 与 1h 同级覆盖)。

**为什么需要多时间尺度**: 现在库里的特征几乎全是 1h 尺度的 —— 1h 的动量、
1h 的波动率。但同一个币在不同时间尺度上的表现是**不同的信号**:
  * 1h 尺度: 微观结构、短期均值回归
  * 4h/日线: 波段趋势、机构调仓节奏
  * 周线/月线: 大周期 regime、牛熊位置
而且**跨尺度背离**本身有信息 (日线在涨但 4h 在跌 = 短期回调 vs 趋势反转)。

实现要点: 不同周期的数据在各自网格上, 交集网格语义 (引擎自动处理) 会让
"日线特征" 只在日线 bar 上出值; 需要**对齐到 1h 面板**时用 ts_backfill
(向前填充, 只用过去, PIT 安全) 或 ts_delay —— 代码里显式写出这个动作,
不靠隐式假设。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# ===========================================================================
# 一、4h 尺度 (最接近 1h 的上层周期, 换手最频繁)
# ===========================================================================
feature(name="ret_4h_candle", expr="pp_pct_change(c4_close, 1)",
        category="multiscale", desc="4h K 线的单根收益率", tags=("scale", "4h"))
feature(name="ret_4h_6bars", expr="pp_pct_change(c4_close, 6)",
        category="multiscale", desc="4h 尺度 6 根收益 (≈1 天)", tags=("scale", "4h"))
feature(name="ret_4h_42bars", expr="pp_pct_change(c4_close, 42)",
        category="multiscale", desc="4h 尺度 42 根收益 (≈7 天)", tags=("scale", "4h"))
feature(name="vol_4h", expr="ts_std(pp_pct_change(c4_close, 1), 24)",
        category="multiscale", desc="4h 收益率波动 (6 天窗口)", tags=("scale", "4h"))
feature(name="range_4h", expr="ts_mean((c4_high - c4_low) / c4_close, 12)",
        category="multiscale", desc="4h 平均振幅 (2 天窗口)", tags=("scale", "4h"))
feature(name="close_loc_4h", expr="(c4_close - c4_low) / (c4_high - c4_low)",
        category="multiscale", desc="4h 收盘在当根区间的位置", tags=("scale", "4h"))
feature(name="body_4h", expr="(c4_close - c4_open) / c4_open",
        category="multiscale", desc="4h 实体涨跌幅", tags=("scale", "4h"))
feature(name="volume_4h_zscore", expr="ts_zscore(c4_volume_quote, 24)",
        category="multiscale", desc="4h 成交额的 24 根 z 分数 (放量)",
        tags=("scale", "4h", "liquidity"))
feature(name="trade_size_4h", expr="ts_mean(c4_volume_quote / c4_trade_count, 12)",
        category="multiscale", desc="4h 平均每笔成交额 (大单结构)", tags=("scale", "4h"))
feature(name="perp_4h_vol", expr="ts_std(pp_pct_change(pc4_close, 1), 24)",
        category="multiscale", desc="永续 4h 波动率", tags=("scale", "4h", "perp"))
feature(name="perp_4h_body", expr="(pc4_close - pc4_open) / pc4_open",
        category="multiscale", desc="永续 4h 实体", tags=("scale", "4h", "perp"))
feature(name="perp_spot_4h_dispersion",
        expr="ts_std((pc4_close - pc4_open) / pc4_open - (c4_close - c4_open) / c4_open, 24)",
        category="multiscale", desc="永续与现货 4h 实体的分歧波动 (跨市场背离)",
        tags=("scale", "4h", "cross_market"))

# ===========================================================================
# 二、日线尺度
# ===========================================================================
feature(name="ret_1d_candle", expr="pp_pct_change(cd_close, 1)",
        category="multiscale", desc="日线收益率 (经典日频动量)", tags=("scale", "1d"))
feature(name="ret_3d", expr="pp_pct_change(cd_close, 3)",
        category="multiscale", desc="3 日收益", tags=("scale", "1d"))
feature(name="ret_14d", expr="pp_pct_change(cd_close, 14)",
        category="multiscale", desc="14 日收益 (两周动量)", tags=("scale", "1d"))
feature(name="ret_30d", expr="pp_pct_change(cd_close, 30)",
        category="multiscale", desc="30 日收益 (月度动量)", tags=("scale", "1d"))
feature(name="vol_1d", expr="ts_std(pp_pct_change(cd_close, 1), 14)",
        category="multiscale", desc="日线波动率 (14 日)", tags=("scale", "1d"))
feature(name="range_1d", expr="ts_mean((cd_high - cd_low) / cd_close, 14)",
        category="multiscale", desc="日线平均振幅 (14 日)", tags=("scale", "1d"))
feature(name="close_loc_1d", expr="(cd_close - cd_low) / (cd_high - cd_low)",
        category="multiscale", desc="日线收盘位置", tags=("scale", "1d"))
feature(name="body_1d", expr="(cd_close - cd_open) / cd_open",
        category="multiscale", desc="日线实体涨跌幅", tags=("scale", "1d"))
feature(name="gap_1d", expr="cd_open / ts_delay(cd_close, 1) - 1",
        category="multiscale", desc="日线跳空 (昨日收盘到今日开盘)", tags=("scale", "1d"))
feature(name="volume_1d_zscore", expr="ts_zscore(cd_volume_quote, 14)",
        category="multiscale", desc="日线成交额 z 分数 (日频放量)",
        tags=("scale", "1d", "liquidity"))
feature(name="hl_ratio_1d", expr="ts_mean(pp_log(cd_high / cd_low), 14)",
        category="multiscale", desc="日线对数振幅均值 (波动幅度)", tags=("scale", "1d"))
feature(name="daily_rank_30d", expr="ts_rank(cd_close, 30)",
        category="multiscale", desc="日线价格在 30 日内的百分位 (大周期位置)",
        tags=("scale", "1d", "position"))

# ===========================================================================
# 三、周线 / 月线尺度 (大周期 regime)
# ===========================================================================
feature(name="ret_1w_candle", expr="pp_pct_change(cw_close, 1)",
        category="multiscale", desc="周线收益率", tags=("scale", "1w"))
feature(name="ret_4w", expr="pp_pct_change(cw_close, 4)",
        category="multiscale", desc="4 周收益 (≈月度)", tags=("scale", "1w"))
feature(name="weekly_rank_12w", expr="ts_rank(cw_close, 12)",
        category="multiscale", desc="周线价格在 12 周内的百分位 (季度位置)",
        tags=("scale", "1w", "position"))
feature(name="ret_1m_candle", expr="pp_pct_change(cm_close, 1)",
        category="multiscale", desc="月线收益率 (大周期动量)", tags=("scale", "1M"))
feature(name="monthly_range", expr="(cm_high - cm_low) / cm_close",
        category="multiscale", desc="月线振幅 (月度波动范围)", tags=("scale", "1M"))
feature(name="monthly_close_loc", expr="(cm_close - cm_low) / (cm_high - cm_low)",
        category="multiscale", desc="月内收盘位置 (>0.5 = 收在月内上半区)",
        tags=("scale", "1M"))
feature(name="monthly_body", expr="(cm_close - cm_open) / cm_open",
        category="multiscale", desc="月线实体 (月度涨跌)", tags=("scale", "1M"))
feature(name="ret_3m", expr="pp_pct_change(cm_close, 3)",
        category="multiscale", desc="3 个月收益 (季度动量)", tags=("scale", "1M"))
feature(name="monthly_rank_12m", expr="ts_rank(cm_close, 12)",
        category="multiscale", desc="月线价格在 12 个月内的百分位 (年度位置)",
        tags=("scale", "1M", "position"))
feature(name="perp_monthly_body", expr="(pcm_close - ts_delay(pcm_close, 1)) / ts_delay(pcm_close, 1)",
        category="multiscale", desc="永续月线收益 (合约大周期)", tags=("scale", "1M", "perp"))

# ===========================================================================
# 四、跨尺度背离 (不同周期的信号不一致 —— 本身就是信息)
# ===========================================================================
feature(name="scale_div_1d_1h", expr="pp_pct_change(cd_close, 1) - ret_24h",
        category="multiscale", desc="日线收益 − 24h 收益 (日线 vs 小时的短期背离)",
        tags=("divergence", "scale"))
feature(name="scale_div_1w_1d", expr="pp_pct_change(cw_close, 1) - pp_pct_change(cd_close, 7)",
        category="multiscale", desc="周线收益 − 7 日收益 (周线 vs 日线背离)",
        tags=("divergence", "scale"))
feature(name="scale_div_1m_1w",
        expr="pp_pct_change(cm_close, 1) - pp_pct_change(cw_close, 4)",
        category="multiscale", desc="月线收益 − 4 周收益 (月线 vs 周线背离)",
        tags=("divergence", "scale"))
feature(name="scale_mom_align",
        expr="pp_sign(pp_pct_change(cd_close, 1)) + pp_sign(ret_24h) + pp_sign(pp_pct_change(cw_close, 1))",
        category="multiscale", desc="三尺度动量方向一致度 (-3..3, 全正=共振向上)",
        tags=("divergence", "scale", "trend"))
feature(name="scale_vol_ratio", expr="ts_std(pp_pct_change(cd_close, 1), 14) / ts_std(pp_pct_change(cd_close, 1), 60)",
        category="multiscale", desc="日线短期/长期波动率之比 (波动尺度结构)",
        tags=("scale", "volatility"))
feature(name="scale_range_ratio", expr="ts_mean((cd_high - cd_low) / cd_close, 7) / ts_mean((cd_high - cd_low) / cd_close, 30)",
        category="multiscale", desc="日线短期/长期振幅之比 (波动扩张)",
        tags=("scale", "volatility"))
feature(name="daily_vs_now", expr="cd_close / close - 1",
        category="multiscale", desc="最新价相对当日日线收盘的偏离 (日内已走多远)",
        tags=("scale", "intrabar"))
feature(name="position_in_month", expr="(close - cm_low) / (cm_high - cm_low)",
        category="multiscale", desc="当前价在**本月**区间内的位置 (月内位置)",
        tags=("scale", "1M", "position"))
feature(name="position_in_week", expr="(close - ts_min(cd_low, 7)) / (ts_max(cd_high, 7) - ts_min(cd_low, 7))",
        category="multiscale", desc="当前价在最近 7 日高低区间内的位置",
        tags=("scale", "position"))