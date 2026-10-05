# -*- coding: utf-8 -*-
"""library/derivatives.py — 衍生品衍生特征 (funding / OI / 标记价 / 指数 / 多空比)

来源数据集: derivatives_funding / derivatives_open_interest /
derivatives_mark_price / derivatives_index_price / derivatives_ratio_*。

PIT 注意: funding 是 8h 结算事件 (时间戳带毫秒抖动, 字段层已地板到 1h 网格),
OI/多空比是 1h; 本文件特征都在各自网格上计算 (bar 语义), 不做隐式前向填充 ——
需要"延续到下一根 bar"的口径请显式用 ts_backfill 或 F6 的延续型特征。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# --- Carry (资金费率) --------------------------------------------------------
feature(name="funding_rate_raw", expr="funding_rate",
        category="derivatives", desc="资金费率原始水平 (8h 结算值)",
        tags=("carry", "raw"))
feature(name="funding_zscore_30d", expr="ts_zscore(funding_rate, 90)",
        category="derivatives", desc="资金费率的 90 期 z 分数 (30 天)",
        tags=("carry", "zscore"))
feature(name="funding_rank_30d", expr="ts_rank(funding_rate, 90)",
        category="derivatives", desc="资金费率在自身 90 期历史中的百分位",
        tags=("carry", "rank"))
feature(name="funding_mean_7d", expr="ts_mean(funding_rate, 21)",
        category="derivatives", desc="7 天平均资金费率 (持续拥挤度)",
        tags=("carry", "raw"))

# --- 未平仓合约 (OI) ---------------------------------------------------------
feature(name="oi_notional_raw", expr="oi_notional",
        category="derivatives", desc="未平仓合约名义值 (原始)", tags=("oi", "raw"))
feature(name="oi_change_24h", expr="pp_pct_change(oi_notional, 24)",
        category="derivatives", desc="OI 24 小时变化率", tags=("oi",))
feature(name="oi_zscore_7d", expr="ts_zscore(oi_notional, 168)",
        category="derivatives", desc="OI 的 168 期 z 分数", tags=("oi", "zscore"))
feature(name="price_oi_div", expr="ret_24h - ts_zscore(pp_pct_change(oi_notional, 24), 168)",
        category="derivatives",
        desc="OI 背离: 价格动量减去 OI 动量的 z 分数 (价涨仓减=空头回补)",
        tags=("oi", "divergence"))

# --- 标记价/指数价 (抗插针与基准) ---------------------------------------------
feature(name="mark_premium", expr="mark_close / index_close - 1",
        category="derivatives", desc="标记价相对指数价的溢价 (1h)",
        tags=("mark",))
feature(name="mark_price_dev", expr="mark_close / close - 1",
        category="derivatives", desc="标记价与现货价的偏离 (插针/异常代理)",
        tags=("mark",))

# --- 多空/主动买卖 (情绪) -----------------------------------------------------
feature(name="glsr_raw", expr="glsr", category="derivatives",
        desc="全市场多空账户比 (原始)", tags=("sentiment", "raw"))
feature(name="glsr_zscore_7d", expr="ts_zscore(glsr, 168)",
        category="derivatives", desc="多空比的 168 期 z 分数", tags=("sentiment",))
feature(name="taker_imbalance_24h", expr="ts_mean(taker_ratio, 24)",
        category="derivatives", desc="24 小时主动买卖比均值 (买压)",
        tags=("flow",))
feature(name="tlsr_pos_zscore", expr="ts_zscore(tlsr_pos, 168)",
        category="derivatives", desc="大户持仓比的 168 期 z 分数",
        tags=("sentiment",))

# --- Carry 多口径扩展 (研究警示: level 有效而 z-score 可能失效, 都要有) --------
feature(name="funding_cum_7d", expr="ts_sum(funding_rate, 21)",
        category="derivatives", desc="7 天累计资金费 (carry 累积收益代理)",
        tags=("carry", "raw"))
feature(name="funding_cs_rank", expr="cs_rank(funding_rate)",
        category="derivatives", desc="资金费率横截面排名 (当前拥挤度)",
        tags=("carry", "cross_section"))
feature(name="funding_vol", expr="ts_std(funding_rate, 90)",
        category="derivatives", desc="资金费率波动 (融资情绪不稳定度)",
        tags=("carry", "risk"))
feature(name="funding_mean_weighted", expr="ts_decay_linear(funding_rate, 21)",
        category="derivatives", desc="线性衰减加权平均资金费 (近期更重)",
        tags=("carry",))

# --- OI 更多口径 ---------------------------------------------------------------
feature(name="oi_mean_7d", expr="ts_mean(oi_notional, 168)",
        category="derivatives", desc="7 天平均 OI (持仓基准)", tags=("oi", "raw"))
feature(name="oi_vol", expr="ts_std(pp_pct_change(oi_notional, 1), 168)",
        category="derivatives", desc="OI 变化率的波动 (杠杆进出剧烈度)",
        tags=("oi", "risk"))
feature(name="oi_to_vol",
        expr="ts_mean(oi_notional, 168) / (ts_mean(volume_quote, 168) * 168)",
        category="derivatives", desc="OI/成交量 杠杆密度 (拥挤度代理)",
        tags=("oi", "crowding"))

# --- 情绪更多口径 ---------------------------------------------------------------
feature(name="glsr_change_24h", expr="pp_pct_change(glsr, 24)",
        category="derivatives", desc="多空比 24h 变化 (情绪转向)", tags=("sentiment",))
feature(name="taker_imbalance_zscore", expr="ts_zscore(taker_ratio, 168)",
        category="derivatives", desc="主动买卖比的 168 期 z 分数 (买压强度)",
        tags=("flow",))
feature(name="sentiment_composite",
        expr="ts_zscore(glsr, 168) + ts_zscore(taker_ratio, 168)",
        category="derivatives", desc="情绪复合: 多空比 + 主动买压 z 分数之和",
        tags=("sentiment", "composite"))

# --- 标记价/指数价扩展 ----------------------------------------------------------
feature(name="mark_premium_zscore", expr="ts_zscore(mark_premium, 168)",
        category="derivatives", desc="标记价溢价的 168 期 z 分数", tags=("mark",))
feature(name="mark_price_dev_cs", expr="cs_rank(mark_price_dev)",
        category="derivatives", desc="标记价偏离的横截面排名 (异常标的)",
        tags=("mark", "cross_section"))

# --- 补齐未用字段 (订单流/永续量能/大户) ----------------------------------------
feature(name="taker_buy_share", expr="taker_buy_volume_quote / volume_quote",
        category="derivatives", desc="现货主动买入额占比 (买压强度 0-1)",
        tags=("flow",))
feature(name="taker_buy_share_zscore", expr="ts_zscore(taker_buy_volume_quote / volume_quote, 168)",
        category="derivatives", desc="主动买占比的 168 期 z 分数", tags=("flow",))
feature(name="tlsr_acct_zscore", expr="ts_zscore(tlsr_acct, 168)",
        category="derivatives", desc="大户多空账户比的 168 期 z 分数", tags=("sentiment",))
feature(name="oi_contracts_zscore", expr="ts_zscore(oi_contracts, 168)",
        category="derivatives", desc="未平仓合约张数的 168 期 z 分数", tags=("oi",))
feature(name="perp_vol", expr="ts_std(pp_pct_change(p_close, 1), 24)",
        category="derivatives", desc="永续 24h 波动率 (合约市场波动)", tags=("volatility",))
feature(name="perp_basis_vol", expr="ts_std(basis_raw, 24)",
        category="derivatives", desc="基差的 24h 波动 (永续定价不稳定)",
        tags=("basis", "risk"))