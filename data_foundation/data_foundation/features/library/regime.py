# -*- coding: utf-8 -*-
"""library/regime.py — 市场状态 / regime 特征 (设计文档 1.4 第 5 条的应对)

研究警示: "**regime 切换**: 2020-21 反转 regime, 2023-26 延续 regime, 系数变号"。
一个因子在两种市场里符号相反, 如果不把市场状态喂进模型, 模型就会学到平均后的
零效应 —— 这是"因子明明有逻辑却测不显著"的常见原因。

本文件的特征是**市场层面的状态量** (整条截面聚合出来的), 不带方向:
截面宽度、离散度、相关性水平、趋势强度、波动率状态。它们的作用是让模型知道
"现在是什么市场", 而不是直接预测涨跌。

写法说明: 用 cs_rank / cs_zscore 的截面聚合 (ts_mean(cs_rank(x), N) 即"宽度"),
比自己去取指数价格更省事, 而且不需要额外的基准数据。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# --- 市场宽度 (breadth): 涨的币占多少 ------------------------------------------
feature(name="mkt_breadth_24h", expr="ts_mean(cs_rank(ret_24h), 24)",
        category="regime", desc="市场宽度: 24h 收益截面排名的均值 (0.5=均衡, >0.5=普涨)",
        tags=("regime", "breadth"))
feature(name="mkt_breadth_change",
        expr="ts_mean(cs_rank(ret_24h), 24) - ts_mean(cs_rank(ret_24h), 168)",
        category="regime", desc="宽度变化 (短期宽度 vs 一周宽度, 市场转强/转弱)",
        tags=("regime", "breadth"))
feature(name="mkt_breadth_extreme",
        expr="pp_abs(ts_mean(cs_rank(ret_24h), 24) - 0.5)",
        category="regime", desc="宽度偏离均衡的程度 (单边行情的强度)",
        tags=("regime", "breadth"))
feature(name="mkt_breadth_7d", expr="ts_mean(cs_rank(ret_24h), 168)",
        category="regime", desc="一周宽度 (描述文档: 长期宽度的基准)",
        tags=("regime", "breadth"))

# --- 截面离散度 (dispersion): 币之间差多少 -------------------------------------
feature(name="mkt_dispersion_24h", expr="ts_std(cs_rank(ret_24h), 24)",
        category="regime", desc="截面动量离散度 (分化程度; 高=选股型市场)",
        tags=("regime", "dispersion"))
feature(name="mkt_dispersion_trend",
        expr="ts_std(cs_rank(ret_24h), 24) / ts_mean(ts_std(cs_rank(ret_24h), 24), 168)",
        category="regime", desc="离散度/其一周均值 (分化上升还是收敛)",
        tags=("regime", "dispersion"))
feature(name="mkt_dispersion_zscore",
        expr="ts_zscore(ts_std(cs_rank(ret_24h), 24), 336)",
        category="regime", desc="离散度的 z 分数 (相对两周常态)",
        tags=("regime", "dispersion"))

# --- 波动率状态 ---------------------------------------------------------------
feature(name="mkt_vol_state", expr="ts_mean(cs_rank(vol_24h), 24)",
        category="regime", desc="市场波动状态: 各币波动率截面排名的均值",
        tags=("regime", "volatility"))
feature(name="mkt_vol_trend", expr="ts_mean(vol_24h, 24) / ts_mean(vol_24h, 168)",
        category="regime", desc="市场平均波动率的短/长期之比 (波动放大还是收缩)",
        tags=("regime", "volatility"))
feature(name="mkt_vol_zscore", expr="ts_zscore(ts_mean(vol_24h, 24), 336)",
        category="regime", desc="市场平均波动率的 z 分数 (两周常态对比)",
        tags=("regime", "volatility"))

# --- 趋势状态 (用宽度与离散度的组合刻画) ---------------------------------------
# 注: 这两条原本写成"一大坨嵌套", 深度到 6 层 (超过决策 9 的 5 层上限);
# 拆成引用中间特征后降到 3 层, 而且中间量 (mkt_breadth_7d / mkt_vol_trend)
# 本身也是有意义的特征, 能被别的表达式复用。
feature(name="mkt_trend_strength",
        expr="(mkt_breadth_24h - 0.5) + (mkt_breadth_7d - 0.5)",
        category="regime", desc="趋势强度: 日级宽度 + 周级宽度 的均衡偏离之和",
        tags=("regime", "trend"))
feature(name="mkt_risk_on",
        expr="pp_sign(mkt_vol_trend - 1) + pp_sign(mkt_breadth_24h - 0.5)",
        category="regime", desc="风险开关: 波动扩张(+1) 与 宽度偏高(+1) 的符号和",
        tags=("regime", "risk"))

# --- 跨资产相关性水平 (共同因子强度) -------------------------------------------
feature(name="mkt_comovement_24h", expr="ts_mean(pp_abs(cs_zscore(ret_24h)), 24)",
        category="regime", desc="共同波动强度: 剔除截面均值后的绝对偏离 (高=齐涨齐跌)",
        tags=("regime", "correlation"))
feature(name="mkt_comovement_trend",
        expr="ts_mean(pp_abs(cs_zscore(ret_24h)), 24) / ts_mean(pp_abs(cs_zscore(ret_24h)), 168)",
        category="regime", desc="共同波动强度之比 (同质化程度变化)",
        tags=("regime", "correlation"))

# --- 极端行情 ----------------------------------------------------------------
feature(name="mkt_extreme_move", expr="ts_mean(pp_abs(cs_zscore(ret_24h)), 6)",
        category="regime", desc="近 6h 极端行情强度 (短期冲击检测)", tags=("regime", "risk"))
feature(name="mkt_tail_rate_7d", expr="ts_mean(pp_is_outlier(pp_abs(ret_24h), 0.15), 168)",
        category="regime", desc="7 天极端收益 bar 占比 (使用固定阈值避免自参照)",
        tags=("regime", "risk"))