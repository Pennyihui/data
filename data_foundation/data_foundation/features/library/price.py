# -*- coding: utf-8 -*-
"""library/price.py — 价格量衍生特征 (按数据来源组织: 设计文档 4.1)

组织原则: 特征库按**数据来源**分文件, 不按信号类别 (信号类别是因子库的事)。
本文件全部基于 market_candle_spot_1h (现货 OHLCV) 与 market_candle_perpetual_1h。

注意 (设计文档 1.4 的研究警示): 同一基础量同时提供**多口径变体** (raw /
z-score / rank), 由检验去决定哪个有效, 不预设 z-score 更好 —— 尤其资金费率,
"level 有效而 z-score 失效" 有明确文献支持。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# --- 动量 (原始水平, 不做标准化; 检验自己判断) -------------------------------
feature(name="ret_1h", expr="pp_pct_change(close, 1)", category="price",
        desc="1 小时收益率 (原始值)", tags=("momentum", "raw"))
feature(name="ret_4h", expr="pp_pct_change(close, 4)", category="price",
        desc="4 小时收益率 (原始值)", tags=("momentum", "raw"))
feature(name="ret_24h", expr="pp_pct_change(close, 24)", category="price",
        desc="24 小时收益率 (原始值)", tags=("momentum", "raw"))
feature(name="ret_7d", expr="pp_pct_change(close, 168)", category="price",
        desc="7 天收益率 (原始值)", tags=("momentum", "raw"))

# --- 动量 (标准化/排名口径) ---------------------------------------------------
feature(name="mom_zscore_24h", expr="ts_zscore(ret_1h, 168)",
        category="price", desc="1h 收益率在自身 168h 历史中的 z 分数",
        tags=("momentum", "zscore"))
feature(name="mom_rank_24h", expr="ts_rank(ret_1h, 168)",
        category="price", desc="1h 收益率在自身 168h 历史中的百分位排名",
        tags=("momentum", "rank"))

# --- 波动率 ------------------------------------------------------------------
feature(name="vol_24h", expr="ts_std(ret_1h, 24)",
        category="price", desc="24 小时收益率标准差 (小时级波动率)",
        tags=("volatility",))
feature(name="vol_ratio_7_30", expr="ts_std(ret_1h, 168) / ts_std(ret_1h, 720)",
        category="price", desc="7 日/30 日波动率之比 (波动率期限结构)",
        tags=("volatility", "regime"))
feature(name="vol_zscore_7d", expr="ts_zscore(vol_24h, 168)",
        category="price", desc="波动率的 168 期 z 分数 (波动率 regime)",
        tags=("volatility", "zscore"))
feature(name="vol_rank_30d", expr="ts_rank(vol_24h, 720)",
        category="price", desc="波动率在 30 日内的百分位", tags=("volatility", "rank"))
feature(name="vol_of_vol", expr="ts_std(ret_1h, 24) / ts_mean(vol_24h, 168)",
        category="price", desc="波动率/均值波动率 (当前波动相对自身常态)",
        tags=("volatility",))
feature(name="downside_vol", expr="ts_std(pp_clip(ret_1h, -0.1, 0.0), 24)",
        category="price", desc="下行波动 (只取负收益部分的 24h 标准差)",
        tags=("volatility", "risk"))

# --- 形态 --------------------------------------------------------------------
feature(name="hl_range_24h", expr="ts_mean((high - low) / close, 24)",
        category="price", desc="24 小时平均相对振幅 (high-low)/close",
        tags=("shape",))
feature(name="close_loc_24h",
        expr="ts_mean((close - low) / (high - low), 24)",
        category="price", desc="收盘价在当日高低区间的相对位置 (0=最低 1=最高)",
        tags=("shape",))
# (缺口率特征在 quality.py —— 按**数据来源**组织, 质量位属于质量族)

# --- 永续 (跨市场) -----------------------------------------------------------
feature(name="basis_raw", expr="p_close / close - 1",
        category="price", desc="永续/现货基差 (原始值, 跨市场拼接)",
        tags=("basis", "raw"))
feature(name="basis_zscore_7d", expr="ts_zscore(basis_raw, 168)",
        category="price", desc="基差的 168h z 分数 (拥挤度代理)",
        tags=("basis", "zscore"))
feature(name="basis_rank_cs", expr="cs_rank(basis_raw)",
        category="price", desc="基差的横截面排名", tags=("basis", "rank"))
# (基差横截面排名以 basis_rank_cs 保留在 price 族; cross_asset 族另有
#  cross_asset 专属的 cs_basis_rank 为避免重复已移除 —— 由 catalog 去重保证)

# --- 相对强弱 (cross_asset 的价格部分) --------------------------------------
feature(name="excess_ret_24h", expr="ret_24h - cs_rank(ret_24h)",
        category="price", desc="24h 超额收益的横截面偏离 (粗略 alpha proxy)",
        tags=("relative", "cross_section"))

# --- 反转 (短期超买超卖) -------------------------------------------------------
feature(name="reversal_1d", expr="-ts_zscore(ret_24h, 168)",
        category="price", desc="24h 收益的反转信号 (负 z = 近期跌幅大, 均值回归)",
        tags=("reversal",))
feature(name="reversal_5d", expr="-ts_zscore(ret_7d, 168)",
        category="price", desc="7 天收益的反转信号", tags=("reversal",))

# --- 高阶矩 (尾部风险) --------------------------------------------------------
feature(name="ret_skew_7d", expr="ts_skew(ret_1h, 168)",
        category="price", desc="7 日收益偏度 (尾部不对称)", tags=("risk",))
feature(name="ret_kurt_7d", expr="ts_kurt(ret_1h, 168)",
        category="price", desc="7 日收益峰度 (肥尾/极端事件频率)", tags=("risk",))
feature(name="ret_skew_zscore", expr="ts_zscore(ts_skew(ret_1h, 168), 336)",
        category="price", desc="偏度的 z 分数 (尾部风险 regime)",
        tags=("risk", "zscore"))

# --- 价格位置 -----------------------------------------------------------------
feature(name="price_position_30d", expr="ts_rank(close, 720)",
        category="price", desc="价格在 30 日区间内的位置 (0=最低 1=最高)",
        tags=("shape", "position"))
feature(name="drawdown_7d", expr="close / ts_max(close, 168) - 1",
        category="price", desc="相对 7 日高点的回撤", tags=("shape", "drawdown"))
feature(name="drawdown_30d", expr="close / ts_max(close, 720) - 1",
        category="price", desc="相对 30 日高点的回撤", tags=("shape", "drawdown"))

# --- 平滑/趋势持续性 -----------------------------------------------------------
feature(name="ema_gap", expr="close / ts_ewma(close, 48) - 1",
        category="price", desc="价格相对 48h 指数均线的偏离 (趋势持续性)",
        tags=("trend",))
feature(name="price_above_ema", expr="pp_sign(close - ts_ewma(close, 48))",
        category="price", desc="价格在 48h 指数均线之上(1)/之下(-1)", tags=("trend",))
feature(name="fracdiff_ret", expr="pp_frac_diff(pp_log(close), 0.5, 10)",
        category="price", desc="对数价格分数阶差分 (长记忆去趋势)",
        tags=("trend", "fracdiff"))
feature(name="detrend_ret", expr="pp_detrend(ret_1h, 24)",
        category="price", desc="收益率去线性趋势 (残差)", tags=("trend",))