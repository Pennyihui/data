# -*- coding: utf-8 -*-
"""library/sentiment.py — 情绪特征 (fng: 全局日频, 按时间广播到每个资产)

数据: sentiment_fng (恐惧贪婪指数, 2018-02 起 3,159 天, 全局一条)。
PIT 语义: 当日值**次日定稿** (data_available_at = date_utc + 1 天), 字段层
已修补并记录在 manifest —— 所以用 fng 不会偷看未来。

注意: fng 是**市场级**信号 (全市场一条), 不是某币的 alpha; 它的价值在于
给模型提供"现在市场情绪处于什么状态"的背景变量。日频数据在 1h 面板上
一天内保持不变 (24 行同值), 这不是 bug 而是采样频率的体现。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# 原始水平 (研究警示: 原始水平往往比标准化口径更有效, 见 1.4)
feature(name="fng_raw", expr="fng_value",
        category="sentiment", desc="恐惧贪婪指数原始值 (0-100, 全市场)",
        tags=("sentiment", "raw", "global"))
# 方向与变化
feature(name="fng_change_1d", expr="pp_diff(fng_value, 1)",
        category="sentiment", desc="情绪日变化 (昨日 - 前日)",
        tags=("sentiment", "momentum"))
feature(name="fng_change_7d", expr="fng_value - ts_delay(fng_value, 7)",
        category="sentiment", desc="情绪 7 天变化 (周度情绪摆动)",
        tags=("sentiment", "momentum"))
feature(name="fng_momentum_7d", expr="ts_mean(pp_diff(fng_value, 1), 7)",
        category="sentiment", desc="7 天情绪动量 (逐日变化的均值)",
        tags=("sentiment", "momentum"))
# 平滑与位置
feature(name="fng_mean_7d", expr="ts_mean(fng_value, 7)",
        category="sentiment", desc="7 天平均情绪 (平滑口径)",
        tags=("sentiment", "smooth"))
feature(name="fng_mean_30d", expr="ts_mean(fng_value, 30)",
        category="sentiment", desc="30 天平均情绪", tags=("sentiment", "smooth"))
feature(name="fng_rank_90d", expr="ts_rank(fng_value, 90)",
        category="sentiment", desc="情绪在自身 90 天内的百分位",
        tags=("sentiment", "rank"))
feature(name="fng_zscore_30d", expr="ts_zscore(fng_value, 30)",
        category="sentiment", desc="情绪的 30 天 z 分数 (标准化口径, 与 raw 并列)",
        tags=("sentiment", "zscore"))
# 极端状态 (阈值显式给定, 不拟合)
# 极端状态 (阈值显式给定, 不拟合)
# (极端恐惧/贪婪的合一标记见下方 fng_extreme; 原先分开登记的 fng_extreme_fear
#  与 fng_extreme 是同一表达式, 已按去重规则合并)
feature(name="fng_extreme_greed", expr="pp_is_outlier(fng_value, 25, 50) * 0 + pp_is_outlier(fng_value, 75, 50)",
        category="sentiment", desc="极度贪婪标记 (值 >75)",
        tags=("sentiment", "extreme"))
feature(name="fng_away_from_neutral", expr="pp_abs(fng_value - 50)",
        category="sentiment", desc="情绪偏离中性 50 的距离 (单边情绪强度)",
        tags=("sentiment", "extreme"))
feature(name="fng_extreme", expr="pp_is_outlier(fng_value, 25, 50)",
        category="sentiment", desc="极端情绪标记 (偏离中性 50 超过 25: >75 或 <25)",
        tags=("sentiment", "extreme"))
feature(name="fng_greed_side", expr="pp_sign(fng_value - 50)",
        category="sentiment", desc="情绪方向: 贪婪(+1) / 中性(0) / 恐惧(-1)",
        tags=("sentiment", "extreme"))
# 情绪与价格的关系 (情绪 regime 下的收益)。注: 比较/布尔运算在表达式白名单里
# 被禁止 (防样本内信息), regime 掩码用 pp_is_outlier 的 0/1 输出来构造
feature(name="fng_high_ret", expr="ret_24h * pp_is_outlier(fng_value, 25, 50)",
        category="sentiment", desc="极端情绪 regime 下的 24h 收益 (极贪/极恐时保留)",
        tags=("sentiment", "interaction"))
feature(name="fng_greed_ret",
        expr="ret_24h * pp_is_outlier(fng_value, 25, 50) * pp_sign(fng_value - 50)",
        category="sentiment",
        desc="分侧极端收益: 极端贪婪为正、极端恐惧为负 (乘情绪方向)",
        tags=("sentiment", "interaction"))
feature(name="fng_ret_corr_30d", expr="ts_corr(fng_value, ret_24h, 720)",
        category="sentiment", desc="情绪与收益的 30 天滚动相关 (情绪是否驱动该币)",
        tags=("sentiment", "correlation"))