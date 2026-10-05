# -*- coding: utf-8 -*-
"""library/intrabar.py — 币本位 K 线形态特征 (设计文档 4.4 的"币本位"类)

来源: open/high/low/close 的**组合结构**本身 (不只用收盘价), 以及永续 p_* 列。
设计文档 4.4 给的典型组合: ``pp_log(high/low)``、``ts_std(ts_delta(close))``。

这类特征在加密短周期择时上有用: 影线长度反映多空拉锯、实体方向反映承接力度、
振幅相对波动率反映"这根 bar 是否异常"。全部只用当根与过去 bar, PIT 安全。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

# --- 振幅 / 真实波幅 ---------------------------------------------------------
feature(name="range_pct", expr="(high - low) / close",
        category="intrabar", desc="当根振幅 (高-低)/收 (单 bar 波动)", tags=("intrabar",))
# (ts_mean((high-low)/close, 24) 已在 price.py 以 hl_range_24h 登记;
#  ts_mean((close-low)/(high-low), 24) 已在 price.py 以 close_loc_24h 登记 ——
#  重复定义会被 catalog.find_duplicates 抓到, 这里不重复登记)
feature(name="atr_proxy_14h", expr="ts_mean((high - low) / close, 14)",
        category="intrabar", desc="真实波幅代理 (14h)", tags=("intrabar", "volatility"))
feature(name="range_vs_vol",
        expr="ts_mean((high - low) / close, 24) / ts_std(ret_1h, 24)",
        category="intrabar", desc="振幅/已实现波动 (振幅相对波动是否异常放大)",
        tags=("intrabar", "volatility"))
feature(name="parkinson_vol", expr="ts_mean(pp_power((high - low) / close, 2), 24)",
        category="intrabar", desc="Parkinson 波动估计 (用高低价, 比收盘价法更有效)",
        tags=("intrabar", "volatility"))
feature(name="garman_klass_proxy",
        expr="hl_range_24h - close_move_24h",
        category="intrabar", desc="Garman-Klass 思路的近似 (振幅减收盘波动)",
        tags=("intrabar", "volatility"))
feature(name="close_move_24h",
        expr="ts_mean(pp_abs(pp_diff(close, 1)) / close, 24)",
        category="intrabar", desc="24h 平均单根收盘位移 (相对振幅的收盘口径波动)",
        tags=("intrabar", "volatility"))

# --- 影线 / 实体 -------------------------------------------------------------
feature(name="upper_shadow", expr="(high - ts_max(open, 1)) / close",
        category="intrabar", desc="上影线相对长度 (冲高回落压力)", tags=("intrabar",))
feature(name="lower_shadow", expr="(ts_min(open, 1) - low) / close",
        category="intrabar", desc="下影线相对长度 (探底回升承接)", tags=("intrabar",))
feature(name="shadow_ratio",
        expr="(high - ts_max(open, 1)) / (ts_min(open, 1) - low)",
        category="intrabar", desc="上下影线之比 (>1 上方压力大)", tags=("intrabar",))
feature(name="body_pct", expr="(close - open) / open",
        category="intrabar", desc="实体涨跌幅 (开收之间的真实位移)", tags=("intrabar",))
feature(name="body_vs_range", expr="(close - open) / (high - low)",
        category="intrabar", desc="实体占振幅比 (-1~1, 越大越单边)", tags=("intrabar",))
feature(name="body_abs_mean_24h", expr="ts_mean(pp_abs(close - open) / open, 24)",
        category="intrabar", desc="24h 平均实体幅度 (忽略方向)", tags=("intrabar",))
feature(name="close_loc_1h", expr="(close - low) / (high - low)",
        category="intrabar", desc="收盘在当根高低区间的位置 (0=最低 1=最高)",
        tags=("intrabar",))
feature(name="open_gap", expr="open / ts_delay(close, 1) - 1",
        category="intrabar", desc="跳空幅度 (本根开盘 vs 上根收盘)", tags=("intrabar",))
feature(name="gap_abs", expr="pp_abs(open / ts_delay(close, 1) - 1)",
        category="intrabar", desc="跳空幅度绝对值 (不分方向)", tags=("intrabar",))
feature(name="gap_abs_mean_24h", expr="ts_mean(gap_abs, 24)",
        category="intrabar", desc="24h 平均跳空幅度 (流动性缝隙)", tags=("intrabar",))

# --- 上下行分解 ---------------------------------------------------------------
feature(name="upper_wick_ratio_mean",
        expr="ts_mean((high - close) / (high - low), 24)",
        category="intrabar", desc="上影占比均值 (上方抛压的持续度)", tags=("intrabar",))
feature(name="wick_asymmetry",
        expr="ts_mean((high - close) / (high - low), 24) - ts_mean((close - low) / (high - low), 24)",
        category="intrabar", desc="影线不对称度 (正=上方压力大, 负=下方支撑强)",
        tags=("intrabar",))
feature(name="hl_ratio_log", expr="ts_mean(pp_log(high / low), 24)",
        category="intrabar", desc="对数振幅均值 (量级压缩后的波动幅度)",
        tags=("intrabar",))

# --- 永续 K 线 (p_* 列) ------------------------------------------------------
feature(name="perp_range_pct", expr="(p_high - p_low) / p_close",
        category="intrabar", desc="永续当根振幅", tags=("intrabar", "perp"))
feature(name="perp_range_24h", expr="ts_mean((p_high - p_low) / p_close, 24)",
        category="intrabar", desc="永续 24h 平均振幅", tags=("intrabar", "perp"))
feature(name="perp_body_pct", expr="(p_close - p_open) / p_open",
        category="intrabar", desc="永续实体涨跌幅", tags=("intrabar", "perp"))
feature(name="perp_close_loc", expr="(p_close - p_low) / (p_high - p_low)",
        category="intrabar", desc="永续收盘位置", tags=("intrabar", "perp"))
feature(name="perp_spot_range_ratio",
        expr="ts_mean((p_high - p_low) / p_close, 24) / ts_mean((high - low) / close, 24)",
        category="intrabar", desc="永续/现货振幅之比 (合约市场波动放大程度)",
        tags=("intrabar", "perp", "cross_market"))
feature(name="perp_spot_body_diff",
        expr="(p_close - p_open) / p_open - (close - open) / open",
        category="intrabar", desc="永续 vs 现货实体差 (合约与现货的分歧)",
        tags=("intrabar", "perp", "cross_market"))
feature(name="perp_close_gap", expr="p_close / close - 1 - basis_raw",
        category="intrabar", desc="永续收盘相对现货的偏离 (扣除同期基差后的残差)",
        tags=("intrabar", "perp"))