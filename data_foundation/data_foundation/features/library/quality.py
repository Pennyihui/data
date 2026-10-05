# -*- coding: utf-8 -*-
"""library/quality.py — 数据质量衍生特征 (清洗动作本身就是信息)

设计文档 3.3: "最后一组来自数据底座 L2 的 is_gap / is_suspect / quality_reason
—— 这些现成可用"。缺 K 线/可疑 K 线往往是**退市、故障、做市商撤单**的痕迹,
在本层把它们暴露成特征而不是静默丢弃。

注: 字段层把质量位转成 float64 (0.0/1.0/NaN) —— 外连接对齐时 bool 列会被
pandas 升成 object dtype, 混合类型序列对下游算子不友好; 质量位作为特征的语义
本就是 0/1, 直接取均值即得"缺口率/可疑率"。
"""
# -*- coding: utf-8 -*-
from ..registry import feature

feature(name="gap_rate_24h", expr="ts_mean(is_gap, 24)",
        category="quality", desc="24 小时缺口率 (缺 bar 的比例)",
        tags=("quality", "gap"))
feature(name="gap_rate_7d", expr="ts_mean(is_gap, 168)",
        category="quality", desc="7 天缺口率 (数据稳定度)", tags=("quality", "gap"))
feature(name="suspect_rate_24h", expr="ts_mean(is_suspect, 24)",
        category="quality", desc="24 小时可疑 K 线占比", tags=("quality", "suspect"))
feature(name="suspect_burst", expr="ts_mean(pp_is_outlier(is_suspect, 0.5), 24)",
        category="quality", desc="24 小时内可疑标记突发程度 (>0.5 视为突发)",
        tags=("quality", "suspect"))
feature(name="missing_rate_24h", expr="ts_mean(pp_is_missing(close), 24)",
        category="quality", desc="收盘价缺失率 (数据空洞)", tags=("quality", "missing"))
feature(name="quality_flag_24h", expr="ts_mean(is_gap + is_suspect, 24)",
        category="quality", desc="24 小时综合质量缺陷率 (缺口 + 可疑)",
        tags=("quality", "composite"))