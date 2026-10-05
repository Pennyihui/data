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