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

# --- 相对强弱 (cross_asset 的价格部分) --------------------------------------
feature(name="excess_ret_24h", expr="ret_24h - cs_rank(ret_24h)",
        category="price", desc="24h 超额收益的横截面偏离 (粗略 alpha proxy)",
        tags=("relative", "cross_section"))