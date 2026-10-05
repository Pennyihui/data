# -*- coding: utf-8 -*-
"""library/robust.py — 稳健口径与去极值特征 (补齐未被使用的算子)

设计文档 1.4 的方法论启示: **不预设哪种口径有效** —— raw / z-score / rank 都要有,
让检验去决定。本文件专门补齐"还没被任何特征用到"的那几个算子, 它们恰好是
抗离群与稳健统计的主力 (加密数据插针多、分布肥尾, 稳健口径往往比均值口径更稳):

    pp_robust / pp_quantile_bucket / pp_minmax / pp_zscore   稳健标准化与分桶
    cs_winsorize / cs_normalize / cs_scale                   截面去极值与归一
    ts_median / ts_quantile                                  抗离群的时序统计
    ts_delta / ts_pct_change / ts_backfill                   基础时序变换

不是"为了用算子而造特征": 每一条都是对已有信号换一种**更抗噪的口径**,
正好符合文档"多口径变体由检验决定"的要求。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# ===========================================================================
# 一、稳健标准化口径 (抗离群: 加密插针多, 均值/标准差口径容易被单根 bar 带偏)
# ===========================================================================
feature(name="mom_robust_24h", expr="pp_robust(ret_24h)",
        category="robust", desc="24h 动量的稳健标准化 (中位数/IQR 口径)",
        tags=("robust", "momentum"))
feature(name="funding_robust", expr="pp_robust(funding_rate_raw)",
        category="robust", desc="资金费率的稳健标准化 (截面中位数/IQR)",
        tags=("robust", "carry"))
feature(name="vol_robust_24h", expr="pp_robust(vol_24h)",
        category="robust", desc="波动率的稳健标准化", tags=("robust", "volatility"))
feature(name="volume_robust_24h", expr="pp_robust(volume_quote)",
        category="robust", desc="成交额的稳健标准化 (不被单笔巨额成交带偏)",
        tags=("robust", "liquidity"))
feature(name="mom_minmax_24h", expr="pp_minmax(ret_24h)",
        category="robust", desc="24h 动量的截面 min-max 归一到 [0,1]",
        tags=("robust", "momentum"))
feature(name="volume_minmax_24h", expr="pp_minmax(volume_quote)",
        category="robust", desc="成交额的截面 min-max 归一", tags=("robust", "liquidity"))
feature(name="vol_minmax_24h", expr="pp_minmax(vol_24h)",
        category="robust", desc="波动率的截面 min-max 归一", tags=("robust",))
feature(name="funding_zscore_cs", expr="pp_zscore(funding_rate_raw)",
        category="robust", desc="资金费率的截面 z 分数 (与滚动 z 口径并列, 由检验裁决)",
        tags=("robust", "carry"))
feature(name="volume_zscore_cs", expr="pp_zscore(volume_quote)",
        category="robust", desc="成交额的截面 z 分数", tags=("robust", "liquidity"))

# ===========================================================================
# 二、分桶 / 离散化 (把连续信号变成稳健的档位, 对非线性关系友好)
# ===========================================================================
feature(name="mom_bucket_5", expr="pp_quantile_bucket(ret_24h, 5)",
        category="robust", desc="24h 动量的截面五等分桶 (0-4)",
        tags=("robust", "bucket"))
feature(name="vol_bucket_5", expr="pp_quantile_bucket(vol_24h, 5)",
        category="robust", desc="波动率的截面五等分桶", tags=("robust", "bucket"))
feature(name="volume_bucket_5", expr="pp_quantile_bucket(volume_quote, 5)",
        category="robust", desc="成交额的截面五等分桶 (流动性档位)",
        tags=("robust", "bucket"))
feature(name="funding_bucket_5", expr="pp_quantile_bucket(funding_rate_raw, 5)",
        category="robust", desc="资金费率的截面五等分桶 (拥挤度档位)",
        tags=("robust", "bucket", "carry"))
feature(name="funding_bucket_ts", expr="pp_quantile_bucket(funding_rate_raw, 5, 'ts', 90)",
        category="robust", desc="资金费率在自身 90 期内的分位桶 (时序口径)",
        tags=("robust", "bucket", "carry"))

# ===========================================================================
# 三、截面去极值 / 归一化 (截面算子的稳健版本)
# ===========================================================================
feature(name="mom_winsor", expr="cs_winsorize(ret_24h, 3)",
        category="robust", desc="24h 动量的截面 3σ 去极值", tags=("robust", "momentum"))
feature(name="vol_winsor", expr="cs_winsorize(vol_24h, 3)",
        category="robust", desc="波动率的截面 3σ 去极值", tags=("robust",))
feature(name="funding_winsor", expr="cs_winsorize(funding_rate_raw, 3)",
        category="robust", desc="资金费率的截面 3σ 去极值", tags=("robust", "carry"))
feature(name="ret_share_cs", expr="cs_normalize(ret_24h, 'sum_abs')",
        category="robust", desc="24h 动量的截面和归一 (权重和为 ±1)",
        tags=("robust", "momentum"))
feature(name="volume_share_cs", expr="cs_normalize(volume_quote, 'sum_abs')",
        category="robust", desc="成交额的截面份额 (占比口径)",
        tags=("robust", "liquidity"))
feature(name="mom_scale_mean", expr="cs_scale(ret_24h, 'mean')",
        category="robust", desc="24h 动量按截面均值缩放", tags=("robust", "momentum"))
feature(name="vol_scale_std", expr="cs_scale(vol_24h, 'std')",
        category="robust", desc="波动率按截面标准差缩放", tags=("robust",))

# ===========================================================================
# 四、抗离群的时序统计 (中位数 / 分位 vs 均值)
# ===========================================================================
feature(name="ret_median_24h", expr="ts_median(ret_1h, 24)",
        category="robust", desc="1h 收益的 24 期**中位数** (抗单根插针的动量)",
        tags=("robust", "momentum"))
feature(name="volume_median_24h", expr="ts_median(volume_quote, 24)",
        category="robust", desc="成交额的 24 期中位数 (常态成交水平)",
        tags=("robust", "liquidity"))
feature(name="volume_mean_over_median",
        expr="ts_mean(volume_quote, 24) / ts_median(volume_quote, 24)",
        category="robust", desc="均值/中位数 之比 (放量是被少数大单推动还是普遍放大)",
        tags=("robust", "liquidity"))
feature(name="ret_q90_24h", expr="ts_quantile(ret_1h, 24, 0.9)",
        category="robust", desc="1h 收益的 24 期 90 分位 (上行尾部强度)",
        tags=("robust", "tail"))
feature(name="ret_q10_24h", expr="ts_quantile(ret_1h, 24, 0.1)",
        category="robust", desc="1h 收益的 24 期 10 分位 (下行尾部强度)",
        tags=("robust", "tail"))
feature(name="ret_tail_spread",
        expr="ts_quantile(ret_1h, 24, 0.9) - ts_quantile(ret_1h, 24, 0.1)",
        category="robust", desc="收益的 90-10 分位差 (尾部带宽, 稳健的波动度量)",
        tags=("robust", "tail", "volatility"))

# ===========================================================================
# 五、基础时序变换的口径补充
# ===========================================================================
feature(name="price_delta_24h", expr="ts_delta(close, 24)",
        category="robust", desc="24h 价格绝对变化 (价差口径, 与收益率口径并列)",
        tags=("raw", "momentum"))
feature(name="volume_delta_24h", expr="ts_delta(volume_quote, 24)",
        category="robust", desc="成交额的 24h 绝对变化", tags=("raw", "liquidity"))
feature(name="vol_pct_change_24h", expr="ts_pct_change(vol_24h, 24)",
        category="robust", desc="波动率的 24h 变化率", tags=("volatility",))
feature(name="oi_backfilled", expr="ts_backfill(oi_notional, 3)",
        category="robust", desc="OI 向前填充 (最多 3 期; 缺报时的延续口径)",
        tags=("oi", "quality"))
feature(name="funding_backfilled", expr="ts_backfill(funding_rate, 3)",
        category="robust",
        desc="资金费率向前填充 (8h 网格在 1h 面板上的延续口径)",
        tags=("carry", "quality"))

# ===========================================================================
# 六、指数加权平滑 (短窗权重更大的口径, 与等权 ts_mean 并列)
# ===========================================================================
feature(name="mom_ema_24h", expr="pp_ema(ret_1h, 24)",
        category="robust", desc="1h 收益的 EMA(24) (近期权重更大的动量)",
        tags=("smooth", "momentum"))
feature(name="mom_ema_ratio",
        expr="pp_ema(ret_1h, 6) / pp_ema(ret_1h, 24)",
        category="robust", desc="短 EMA / 长 EMA (动量加速/减速)",
        tags=("smooth", "momentum"))
feature(name="volume_ema_ratio",
        expr="pp_ema(volume_quote, 6) / pp_ema(volume_quote, 24)",
        category="robust", desc="成交额短 EMA / 长 EMA (放量加速)",
        tags=("smooth", "liquidity"))

# ===========================================================================
# 七、组内均值基准 (奖励/偏离口径)
# ===========================================================================
feature(name="mom_vs_cap_group",
        expr="ret_24h - group_mean(ret_24h, market_cap_tier)",
        category="robust", desc="24h 动量相对同市值组均值的超额 (组内 alpha)",
        tags=("group", "momentum"))
feature(name="vol_vs_age_group",
        expr="vol_24h - group_mean(vol_24h, listing_age_tier)",
        category="robust", desc="波动率相对同上市时长组均值的偏离",
        tags=("group", "volatility"))