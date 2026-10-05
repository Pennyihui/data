# -*- coding: utf-8 -*-
"""library/cross_asset.py — 跨资产特征 (相对强弱 / 相对基准)

加密没有 A 股的"行业中性", 但**相对基准**是同等重要的一层: 个币 alpha 常常
只是 BTC beta 的噪声。cs_rel(x, ref) 把基准按 time 对齐到全体资产, 于是
"相对 BTC 的强弱"就是一行表达式 (基准列通常是 btc_close —— 见 fields 里的
基准资产约定, 或直接用任一基准资产的 close 列)。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# 说明: 这里用 cs_rank / cs_zscore 表达"相对截面强弱"; 真正相对某基准资产
# (如 BTC) 需要该基准的 close 作为列进入面板 —— 引擎在 BTC 在宇宙内时自动
# 提供 (见 engine 基准列约定)。此处先给不依赖具体基准的截面相对特征。
feature(name="cs_mom_rank", expr="cs_rank(ret_24h)",
        category="cross_asset", desc="24h 动量的横截面排名", tags=("cross_section",))
feature(name="cs_mom_zscore", expr="cs_zscore(ret_24h)",
        category="cross_asset", desc="24h 动量的横截面 z 分数", tags=("cross_section",))
feature(name="cs_vol_zscore", expr="cs_zscore(vol_24h)",
        category="cross_asset", desc="波动率的横截面 z 分数 (高波动币暴露)",
        tags=("cross_section", "risk"))
feature(name="cs_funding_rank", expr="cs_rank(funding_rate_raw)",
        category="cross_asset", desc="资金费率横截面排名 (拥挤度相对位置)",
        tags=("cross_section", "carry"))
# (基差/流动性横截面排名已在 price.py 的 basis_rank_cs 与 liquidity.py 的
#  liq_cs_rank, 此处不重复登记 —— catalog.find_duplicates 会抓到重复)
feature(name="cs_quality_rank", expr="cs_rank(is_gap)",
        category="cross_asset", desc="缺口位的横截面排名 (数据质量相对; 用 is_gap "
        "而非 pp_is_missing(close) —— 后者在只有 funding 的联合网格行上无意义)",
        tags=("cross_section", "quality"))
# (截面动量离散度 ts_std(cs_rank(ret_24h),24) 属市场状态量, 已在 regime.py 以
#  mkt_dispersion_24h 登记 —— 不重复)