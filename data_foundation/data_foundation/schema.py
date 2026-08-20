# -*- coding: utf-8 -*-
"""schema.py — L1/L2 统一列定义 (与设计文档一致)。"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# market_candle: 交易所 K 线事实表
# 主键: (venue_id, instrument_id, bar_interval, open_time_utc)
# ---------------------------------------------------------------------------
MARKET_CANDLE_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),          # 如 BTC-USDT (点时可追溯)
    ("symbol", "string"),                 # 源交易所 symbol, 如 BTCUSDT
    ("market_type", "string"),            # spot | perpetual
    ("base_asset", "string"),
    ("quote_asset", "string"),
    ("bar_interval", "string"),           # 1h | 1d | 1w
    ("open_time_utc", "timestamp[us, tz=UTC]"),
    ("close_time_utc", "timestamp[us, tz=UTC]"),
    ("open", "float64"),
    ("high", "float64"),
    ("low", "float64"),
    ("close", "float64"),
    ("volume_base", "float64"),
    ("volume_quote", "float64"),
    ("trade_count", "int64"),
    ("taker_buy_volume_base", "float64"),
    ("taker_buy_volume_quote", "float64"),
    ("is_closed", "bool"),                # 是否已收盘(最终)
    ("is_gap", "bool"),                   # 与前一根之间是否有缺口
    ("is_suspect", "bool"),               # 质量可疑
    ("quality_reason", "string"),         # 可疑原因 (空=正常)
    ("data_available_at", "timestamp[us, tz=UTC]"),  # PIT: 该行可用时刻
    ("source_batch_id", "string"),
]

# ---------------------------------------------------------------------------
# instrument: 交易所元数据 (点时化)
# ---------------------------------------------------------------------------
INSTRUMENT_COLUMNS = [
    ("venue_id", "string"),
    ("symbol", "string"),
    ("instrument_id", "string"),
    ("base_asset", "string"),
    ("quote_asset", "string"),
    ("market_type", "string"),
    ("contract_type", "string"),          # perpetual | spot | delivery
    ("contract_size", "float64"),
    ("tick_size", "float64"),
    ("lot_size", "float64"),
    ("min_order_size", "float64"),
    ("price_precision", "int64"),
    ("quantity_precision", "int64"),
    ("listing_time", "timestamp[us, tz=UTC]"),
    ("delisting_time", "timestamp[us, tz=UTC]"),
    ("status", "string"),
    ("settlement_asset", "string"),
    ("underlying_asset", "string"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# ---------------------------------------------------------------------------
# derivatives_snapshot: 衍生品事实表 (资金费率)
# ---------------------------------------------------------------------------
DERIVATIVES_FUNDING_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("funding_time_utc", "timestamp[us, tz=UTC]"),   # 主键组成部分
    ("funding_rate", "float64"),
    ("mark_price_at_funding", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 未平仓合约 (1h)
DERIVATIVES_OI_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("timestamp_utc", "timestamp[us, tz=UTC]"),
    ("open_interest_contracts", "float64"),
    ("open_interest_notional", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 标记价 K 线 (1h, 抗插针)
DERIVATIVES_MARK_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("open_time_utc", "timestamp[us, tz=UTC]"),
    ("mark_open", "float64"),
    ("mark_high", "float64"),
    ("mark_low", "float64"),
    ("mark_close", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 多空账户比/大户持仓比/主动买卖比 (1h) — 情绪/仓位
DERIVATIVES_RATIO_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("timestamp_utc", "timestamp[us, tz=UTC]"),
    ("metric", "string"),                 # glsr | tlsr_acct | tlsr_pos | taker
    ("long_account", "float64"),
    ("long_short_ratio", "float64"),
    ("short_account", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 数据集 -> 列定义
DATASETS = {
    "market_candle_spot_1h": MARKET_CANDLE_COLUMNS,
    "market_candle_spot_1d": MARKET_CANDLE_COLUMNS,
    "market_candle_spot_1w": MARKET_CANDLE_COLUMNS,
    "instrument": INSTRUMENT_COLUMNS,
    "derivatives_funding": DERIVATIVES_FUNDING_COLUMNS,
    "derivatives_open_interest": DERIVATIVES_OI_COLUMNS,
    "derivatives_mark_price": DERIVATIVES_MARK_COLUMNS,
    "derivatives_ratio": DERIVATIVES_RATIO_COLUMNS,
}

# 每条记录必须存在的元数据字段
MANIFEST_FIELDS = [
    "dataset", "schema_version", "source_batches", "coverage_start",
    "coverage_end", "row_count", "duplicate_count", "gap_count",
    "suspect_count", "timestamp_unit", "timezone", "certification_status",
    "certified_at", "aggregation_rules",
]
