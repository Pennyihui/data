# -*- coding: utf-8 -*-
"""library/neutral.py — 中性化与高斯化口径 (无状态预处理算子成的特征)

本文件把 2026-10-05 新增的**无状态预处理算子**落成特征:

    cs_residual       截面中性化 (对暴露量做逐期 OLS 取残差)
    cs_rank_normal    截面排名高斯化 (inverse-normal transform)
    cs_winsorize_mad  截面 MAD 去极值 (比 均值±nσ 更抗插针)
    pp_soft_threshold 软阈值去噪 (小信号置零)
    pp_savgol         端点 SG 平滑 (trailing 版, 只看过去)
    pp_boxcox         Box-Cox 幂变换 (λ 按当期截面/trailing 窗口拟合)

**为什么这些都属"无状态预处理"**: 参数只来自当期截面或 trailing 窗口, 不需要
从训练集里"记住"任何数字 —— 服务端拿新数据能原样重算 (与需要携带参数的
标准化器/PCA 有本质区别, 后者属模型研究协议)。

**口径意义**: 设计文档 1.4/决策 8 要求"同一特征的多口径变体由检验裁决"。
本文件给核心信号补上第三、第四种口径 (残差口径 / 高斯化口径 / 去噪口径),
让检验去说哪种有效, 而不是预设。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# ===========================================================================
# 一、中性化: 剔除暴露量能解释的部分 (量化最标准的预处理)
# ===========================================================================
feature(name="mom_resid_cap", expr="cs_residual(ret_24h, volume_quote)",
        category="neutral", desc="动量对成交额(规模代理)中性化后的残差",
        tags=("neutral", "momentum"))
feature(name="mom_resid_vol", expr="cs_residual(ret_24h, vol_24h)",
        category="neutral", desc="动量对自身波动率中性化 (剔除高风险高波动的解释力)",
        tags=("neutral", "momentum"))
feature(name="funding_resid_vol", expr="cs_residual(funding_zscore_30d, vol_24h)",
        category="neutral", desc="资金费率对波动率中性化后的残差",
        tags=("neutral", "carry"))
feature(name="mom_resid_liq", expr="cs_residual(ret_24h, amihud_illiq)",
        category="neutral", desc="动量对非流动性中性化 (剔除流动性溢价)",
        tags=("neutral", "momentum"))
feature(name="vol_resid_cap", expr="cs_residual(vol_24h, volume_quote)",
        category="neutral", desc="波动率对成交额中性化后的残差",
        tags=("neutral", "volatility"))
feature(name="oi_resid_vol", expr="cs_residual(oi_change_24h, vol_24h)",
        category="neutral", desc="OI 变化对波动率中性化 (剔除行情驱动)",
        tags=("neutral", "oi"))
feature(name="basis_resid_cap", expr="cs_residual(basis_raw, volume_quote)",
        category="neutral", desc="基差对成交额中性化", tags=("neutral", "basis"))

# ===========================================================================
# 二、高斯化: 排名口径的正态化 (线性模型友好, 且保留排名的抗离群性)
# ===========================================================================
feature(name="mom_gauss_cs", expr="cs_rank_normal(ret_24h)",
        category="neutral", desc="动量的截面排名高斯化",
        tags=("gauss", "momentum"))
feature(name="vol_gauss_cs", expr="cs_rank_normal(vol_24h)",
        category="neutral", desc="波动率的截面排名高斯化", tags=("gauss",))
feature(name="funding_gauss_cs", expr="cs_rank_normal(funding_rate_raw)",
        category="neutral", desc="资金费率的截面排名高斯化",
        tags=("gauss", "carry"))
feature(name="volume_gauss_cs", expr="cs_rank_normal(volume_quote)",
        category="neutral", desc="成交额的截面排名高斯化",
        tags=("gauss", "liquidity"))
feature(name="illiq_gauss_cs", expr="cs_rank_normal(amihud_illiq)",
        category="neutral", desc="非流动性的截面排名高斯化",
        tags=("gauss", "liquidity"))
feature(name="basis_gauss_cs", expr="cs_rank_normal(basis_raw)",
        category="neutral", desc="基差的截面排名高斯化", tags=("gauss", "basis"))

# ===========================================================================
# 三、MAD 去极值: 比 nσ 更抗插针的截面口径
# ===========================================================================
feature(name="mom_mad_winsor", expr="cs_winsorize_mad(ret_24h, 5)",
        category="neutral", desc="动量的截面 MAD 去极值 (中位数口径)",
        tags=("mad", "momentum"))
feature(name="vol_mad_winsor", expr="cs_winsorize_mad(vol_24h, 5)",
        category="neutral", desc="波动率的截面 MAD 去极值", tags=("mad",))
feature(name="funding_mad_winsor", expr="cs_winsorize_mad(funding_rate_raw, 5)",
        category="neutral", desc="资金费率的截面 MAD 去极值",
        tags=("mad", "carry"))
feature(name="oi_mad_winsor", expr="cs_winsorize_mad(oi_change_24h, 5)",
        category="neutral", desc="OI 变化的截面 MAD 去极值", tags=("mad", "oi"))

# ===========================================================================
# 四、软阈值去噪 (小信号置零)
# ===========================================================================
feature(name="ret_soft_denoise", expr="pp_soft_threshold(ret_1h, 0.002)",
        category="neutral", desc="1h 收益软阈值去噪 (|r|<0.2% 视为噪声置零)",
        tags=("denoise",))
feature(name="mom_soft_denoise", expr="pp_soft_threshold(ts_zscore(ret_24h, 168), 1.0)",
        category="neutral", desc="动量 z 分数的软阈值 (|z|<1 收缩到 0)",
        tags=("denoise", "momentum"))
feature(name="funding_soft_denoise",
        expr="pp_soft_threshold(ts_zscore(funding_rate, 90), 1.0)",
        category="neutral", desc="资金费率 z 分数的软阈值去噪",
        tags=("denoise", "carry"))

# ===========================================================================
# 五、SG 平滑 (端点版, 只看过去)
# ===========================================================================
feature(name="ret_savgol", expr="pp_savgol(ret_1h, 7, 2)",
        category="neutral", desc="收益的 SG 端点平滑 (7 点二次, 去单根噪声)",
        tags=("smooth",))
feature(name="mom_savgol_24h", expr="pp_savgol(pp_pct_change(close, 1), 24, 2)",
        category="neutral", desc="收益的 SG 平滑 (24 点二次) 作动量",
        tags=("smooth", "momentum"))
feature(name="volume_savgol_24h", expr="pp_savgol(volume_quote, 24, 2)",
        category="neutral", desc="成交额的 SG 平滑 (去单笔冲击)",
        tags=("smooth", "liquidity"))
feature(name="funding_savgol", expr="pp_savgol(funding_rate, 9, 2)",
        category="neutral", desc="资金费率的 SG 平滑 (跨结算点降噪)",
        tags=("smooth", "carry"))
feature(name="mom_savgol_zscore",
        expr="ts_zscore(pp_savgol(ret_1h, 24, 2), 168)",
        category="neutral", desc="平滑后动量的 168h z 分数 (去噪 + 标准化)",
        tags=("smooth", "momentum"))

# ===========================================================================
# 六、Box-Cox 幂变换 (λ 按当期截面拟合 —— 无状态)
# ===========================================================================
feature(name="volume_boxcox_cs", expr="pp_boxcox(volume_quote)",
        category="neutral", desc="成交额的 Box-Cox (λ 逐期截面拟合)",
        tags=("boxcox", "liquidity"))
feature(name="volume_boxcox_log", expr="pp_boxcox(volume_quote, 0.0)",
        category="neutral", desc="成交额的 Box-Cox λ=0 (等价对数口径, 作对照)",
        tags=("boxcox", "liquidity"))
feature(name="oi_boxcox_cs", expr="pp_boxcox(oi_notional)",
        category="neutral", desc="未平仓名义值的 Box-Cox (λ 逐期截面拟合)",
        tags=("boxcox", "oi"))
feature(name="volume_boxcox_zscore",
        expr="ts_zscore(pp_boxcox(volume_quote), 168)",
        category="neutral", desc="Box-Cox 后的成交额再滚动标准化",
        tags=("boxcox", "liquidity"))
feature(name="volume_boxcox_cs_rank", expr="cs_rank(pp_boxcox(volume_quote))",
        category="neutral", desc="Box-Cox 后成交额的截面排名 (与 raw 口径对照)",
        tags=("boxcox", "liquidity"))