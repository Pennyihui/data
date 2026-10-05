# -*- coding: utf-8 -*-
"""library/group_features.py — 分组特征 (设计文档 3.2 + 3.1 的黄金范式)

黄金范式 (设计文档 1.1 引自 WorldQuant BRAIN)::

    group_rank(ts_rank(signal, N), group)

先看信号在自身历史上的位置, 再看它在**同类组内**的相对位置 —— 这是加密版的
"行业中性"。加密没有 A 股那样的行业分类, 但有三条不依赖外部数据、且 PIT 干净的
分组轴 (features/groups.py 从宇宙快照逐日构造):

    market_cap_tier   大/中/小盘  (规模是最稳的分组轴)
    listing_age_tier  新/中/老币  (新币投机性强、老币机构持仓多)
    venue             主交易所
    quote             计价资产

**PIT 要点**: 分组标签逐日取当日宇宙快照 (2019 年的大盘股是按 2019 年的市值
排的), 不是用窗口末日的市值给整段历史打标签 —— 后者是前视。由 groups.py 保证。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# ===========================================================================
# 一、市值分层 (market_cap_tier) —— 大盘/中盘/小盘的组内相对
# ===========================================================================
feature(name="grp_cap_mom", expr="group_rank(ret_24h, market_cap_tier)",
        category="group", desc="24h 动量的市值组内排名 (同规模比强弱)",
        tags=("group", "momentum"))
feature(name="grp_cap_mom_z", expr="group_zscore(ret_24h, market_cap_tier)",
        category="group", desc="24h 动量的市值组内 z 分数", tags=("group", "momentum"))
feature(name="grp_cap_mom_neutral",
        expr="group_neutralize(ts_zscore(ret_24h, 168), market_cap_tier)",
        category="group", desc="动量的市值中性化 (剔除规模 beta 后的纯动量)",
        tags=("group", "momentum", "neutral"))
feature(name="grp_cap_vol_rank", expr="group_rank(vol_24h, market_cap_tier)",
        category="group", desc="波动率的市值组内排名", tags=("group", "volatility"))
feature(name="grp_cap_funding_rank",
        expr="group_rank(funding_zscore_30d, market_cap_tier)",
        category="group", desc="资金费率的市值组内排名 (同规模里的拥挤度)",
        tags=("group", "carry"))
feature(name="grp_cap_funding_neutral",
        expr="group_neutralize(funding_zscore_30d, market_cap_tier)",
        category="group", desc="资金费率的市值中性化", tags=("group", "carry", "neutral"))
feature(name="grp_cap_liq_rank", expr="group_rank(volume_zscore_24h, market_cap_tier)",
        category="group", desc="成交额强度的市值组内排名", tags=("group", "liquidity"))
feature(name="grp_cap_illiq_rank", expr="group_rank(amihud_illiq, market_cap_tier)",
        category="group", desc="非流动性的市值组内排名 (同规模里谁更难进出)",
        tags=("group", "liquidity"))
feature(name="grp_cap_basis_rank", expr="group_rank(basis_raw, market_cap_tier)",
        category="group", desc="基差的市值组内排名", tags=("group", "basis"))
feature(name="grp_cap_oi_rank", expr="group_rank(oi_change_24h, market_cap_tier)",
        category="group", desc="OI 变化的市值组内排名 (同规模里的加杠杆速度)",
        tags=("group", "oi"))

# ===========================================================================
# 二、上市时长分层 (listing_age_tier) —— 新币 vs 老币
# ===========================================================================
feature(name="grp_age_mom", expr="group_rank(ret_24h, listing_age_tier)",
        category="group", desc="动量的上市时长组内排名 (新币/老币分开比)",
        tags=("group", "momentum"))
feature(name="grp_age_mom_neutral",
        expr="group_neutralize(ts_zscore(ret_24h, 168), listing_age_tier)",
        category="group", desc="动量的上市时长中性化", tags=("group", "momentum", "neutral"))
feature(name="grp_age_vol_rank", expr="group_rank(vol_24h, listing_age_tier)",
        category="group", desc="波动率的上市时长组内排名", tags=("group", "volatility"))
feature(name="grp_age_funding_rank",
        expr="group_rank(funding_zscore_30d, listing_age_tier)",
        category="group", desc="资金费率的上市时长组内排名", tags=("group", "carry"))
feature(name="grp_age_reversal",
        expr="group_rank(-ts_zscore(ret_24h, 168), listing_age_tier)",
        category="group", desc="反转信号的上市时长组内排名 (新币反转更强?)",
        tags=("group", "reversal"))
feature(name="grp_age_quality", expr="group_rank(gap_rate_7d, listing_age_tier)",
        category="group", desc="数据缺口率的上市时长组内排名", tags=("group", "quality"))

# ===========================================================================
# 三、交易所分组 (venue) —— 跨所差异
# ===========================================================================
feature(name="grp_venue_mom", expr="group_rank(ret_24h, venue)",
        category="group", desc="动量的交易所组内排名 (同所内比)", tags=("group",))
feature(name="grp_venue_liq_rank", expr="group_rank(volume_zscore_24h, venue)",
        category="group", desc="成交额强度的交易所组内排名", tags=("group", "liquidity"))
feature(name="grp_venue_basis_rank", expr="group_rank(basis_raw, venue)",
        category="group", desc="基差的交易所组内排名 (同所内永续溢价位置)",
        tags=("group", "basis"))

# ===========================================================================
# 四、计价资产分组 (quote) —— USDT/USDC 等市场结构差异
# ===========================================================================
feature(name="grp_quote_mom", expr="group_rank(ret_24h, quote)",
        category="group", desc="动量的计价资产组内排名", tags=("group",))
feature(name="grp_quote_vol_rank", expr="group_rank(vol_24h, quote)",
        category="group", desc="波动率的计价资产组内排名", tags=("group",))

# ===========================================================================
# 五、组规模本身作为特征 (稀疏组 = 数据/流动性异常的信号)
# ===========================================================================
feature(name="grp_cap_size", expr="group_size(ret_24h, market_cap_tier)",
        category="group", desc="市值组内成员数 (组太小的日子, 组内排名不可靠)",
        tags=("group", "quality"))
feature(name="grp_mean_disp_24h",
        expr="group_std(ret_24h, market_cap_tier) / ts_mean(group_std(ret_24h, market_cap_tier), 168)",
        category="group", desc="组内收益离散度 / 其 168h 均值 (组内分化程度变化)",
        tags=("group", "regime"))