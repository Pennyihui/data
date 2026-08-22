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
    ("volume_contracts", "float64"),      # 合约张数 (OKX swap 等, 不与其他量混用)
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
    # 审计派生列 (PIT 快照刷新时从 certified K 线重算): 该 symbol 在 certified
    # market_candle_spot_1h / market_candle_perpetual_1h 中的首末根 open_time。
    ("first_data_utc", "timestamp[us, tz=UTC]"),
    ("last_data_utc", "timestamp[us, tz=UTC]"),
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
    ("realized_rate", "float64"),                     # 实际结算费率 (OKX realizedRate)
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

# 指数价 K 线 (1h) — OKX index-candles
DERIVATIVES_INDEX_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("open_time_utc", "timestamp[us, tz=UTC]"),
    ("index_open", "float64"),
    ("index_high", "float64"),
    ("index_low", "float64"),
    ("index_close", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 基差 (1h, 永续 vs 现货) — 派生
BASIS_COLUMNS = [
    ("venue_id", "string"),
    ("instrument_id", "string"),          # 如 BTC-USDT (永续 instrument 为 -SWAP)
    ("open_time_utc", "timestamp[us, tz=UTC]"),
    ("spot_close", "float64"),
    ("swap_close", "float64"),
    ("basis", "float64"),                 # swap/spot - 1
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 稳定币供应量 (日频) — CMC 流通量
STABLECOIN_SUPPLY_COLUMNS = [
    ("venue_id", "string"),
    ("token", "string"),                  # USDT | USDC | DAI
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("circulating_supply", "float64"),
    ("rank", "float64"),
    ("source_batch_id", "string"),
]

# 稳定币交易所流向 (日频) — Ercin
STABLECOIN_FLOWS_COLUMNS = [
    ("venue_id", "string"),
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("metric", "string"),                 # exchange_inflow_total | outflow | netflow | reserve | supply_ratio
    ("value_usd", "float64"),
    ("source_batch_id", "string"),
]

# 稳定币 peg (1h) — Binance 稳定币对
STABLECOIN_PEG_COLUMNS = [
    ("venue_id", "string"),
    ("token", "string"),
    ("time_utc", "timestamp[us, tz=UTC]"),
    ("price", "float64"),
    ("peg_deviation", "float64"),         # price - 1 (美元偏离)
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 稳定币 mint/burn (日频) — 由供应量派生
STABLECOIN_MINT_BURN_COLUMNS = [
    ("venue_id", "string"),
    ("token", "string"),
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("supply_change", "float64"),         # 当日供应量变化
    ("mint", "float64"),                  # 增加量 (>=0)
    ("burn", "float64"),                  # 减少量 (>=0)
    ("source_batch_id", "string"),
]

# 链上: ERC-20 转账解码 (阶段 4)
TOKEN_TRANSFER_COLUMNS = [
    ("chain_id", "string"),
    ("token", "string"),
    ("block_number", "int64"),
    ("block_timestamp_utc", "timestamp[us, tz=UTC]"),
    ("tx_hash", "string"),
    ("log_index", "int64"),
    ("from_address", "string"),
    ("to_address", "string"),
    ("value_raw", "string"),
    ("value_decimal", "float64"),
    ("is_mint", "bool"),
    ("is_burn", "bool"),
]

# 链上日频聚合
ONCHAIN_DAILY_AGGREGATE_COLUMNS = [
    ("chain_id", "string"),
    ("token", "string"),
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("transfer_count", "int64"),
    ("unique_from", "int64"),
    ("unique_to", "int64"),
    ("volume_token", "float64"),
    ("large_transfer_count", "int64"),
    ("mint_count", "int64"),
    ("burn_count", "int64"),
]

# DEX 日频成交量 (DefiLlama)
DEX_VOLUME_COLUMNS = [
    ("venue_id", "string"),
    ("dex_name", "string"),
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("volume_usd", "float64"),
]

# BTC mempool 区块 (费率统计)
BTC_BLOCKS_COLUMNS = [
    ("venue_id", "string"),
    ("block_height", "int64"),
    ("block_timestamp_utc", "timestamp[us, tz=UTC]"),
    ("tx_count", "int64"),
    ("size", "int64"),
    ("fees_total", "float64"),
    ("fees_base", "float64"),
    ("fee_rate_avg", "float64"),
    ("fee_rate_median", "float64"),
]

# Chainlink 预言机快照
ORACLE_SNAPSHOT_COLUMNS = [
    ("venue_id", "string"),
    ("pair", "string"),
    ("price", "float64"),
    ("updated_at", "timestamp[us, tz=UTC]"),
    ("fetched_at", "timestamp[us, tz=UTC]"),
    ("round_id", "string"),               # uint256 超 int64, 用字符串
]

# Solana 链上快照 (getTokenSupply USDC 供应量; 完整转账解码需索引器, 快照级)
SOLANA_SNAPSHOT_COLUMNS = [
    ("venue_id", "string"),
    ("slot", "int64"),
    ("block_height", "int64"),
    ("usdc_supply", "float64"),           # USDC 流通量 (mint EPjFWdd5...)
    ("tps", "float64"),                   # 最近性能样本交易/秒 (可空)
    ("fetched_at", "timestamp[us, tz=UTC]"),
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

# 跨所未平仓合约 (聚合源: Coinalyze / Bybit 等, 补 Binance 500h 上限)
DERIVATIVES_OI_CROSS_COLUMNS = [
    ("venue_id", "string"),               # 交易所名 (bybit | bitget | binance ...)
    ("asset", "string"),                  # BTC | ETH ...
    ("timestamp_utc", "timestamp[us, tz=UTC]"),
    ("open_interest_contracts", "float64"),
    ("open_interest_usd", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 强平/清算聚合 (聚合源: Coinalyze 等)
DERIVATIVES_LIQUIDATION_COLUMNS = [
    ("venue_id", "string"),
    ("asset", "string"),
    ("timestamp_utc", "timestamp[us, tz=UTC]"),
    ("side", "string"),                   # long | short
    ("amount_usd", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 恐惧贪婪指数 (日频, alternative.me)
SENTIMENT_FNG_COLUMNS = [
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("value", "int64"),
    ("classification", "string"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 宏观序列 (日频收盘: DXY | SPX | NDX | VIX | GOLD | US10Y ...)
MACRO_DAILY_COLUMNS = [
    ("series", "string"),
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("close", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 资产主映射 (统一主键: 同一资产跨所 symbol/链上地址/CMC id)
ASSET_MASTER_COLUMNS = [
    ("asset", "string"),                  # 统一基础资产名 (BTC | USDT ...)
    ("venue_id", "string"),               # binance | okx | coinbase | ethereum ...
    ("market_type", "string"),            # spot | perpetual | onchain | offchain
    ("instrument_id", "string"),
    ("symbol", "string"),
    ("quote_asset", "string"),
    ("chain_id", "string"),               # 链上映射用
    ("contract_address", "string"),
    ("cmc_id", "string"),
    ("cmc_slug", "string"),
    ("listing_time", "timestamp[us, tz=UTC]"),
    ("delisting_time", "timestamp[us, tz=UTC]"),
    ("status", "string"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 上市宇宙 (Binance Vision 归档枚举, 含已下架 -> 消幸存者偏差)
LISTING_UNIVERSE_COLUMNS = [
    ("venue_id", "string"),
    ("market_type", "string"),
    ("symbol", "string"),
    ("first_period", "string"),           # YYYY-MM 首次出现
    ("last_period", "string"),            # YYYY-MM 最后出现
    ("status", "string"),                 # active | delisted
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# BTC 网络日频 (blockchain.info charts, 长格式)
BTC_NETWORK_DAILY_COLUMNS = [
    ("metric", "string"),                 # hash_rate | difficulty | active_addresses ...
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("value", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# Coin Metrics 社区版资产网络日频 (长格式; usdt/usdc/dai/eth/btc)
CM_ASSET_DAILY_COLUMNS = [
    ("asset", "string"),
    ("metric", "string"),                 # TxCnt | AdrActCnt | PriceUSD ...
    ("date_utc", "timestamp[us, tz=UTC]"),
    ("value", "float64"),
    ("data_available_at", "timestamp[us, tz=UTC]"),
    ("source_batch_id", "string"),
]

# 数据集 -> 列定义
DATASETS = {
    "market_candle_spot_1h": MARKET_CANDLE_COLUMNS,
    "market_candle_spot_1d": MARKET_CANDLE_COLUMNS,
    "market_candle_spot_1w": MARKET_CANDLE_COLUMNS,
    "market_candle_perpetual_1h": MARKET_CANDLE_COLUMNS,
    "instrument": INSTRUMENT_COLUMNS,
    "derivatives_funding": DERIVATIVES_FUNDING_COLUMNS,
    "derivatives_open_interest": DERIVATIVES_OI_COLUMNS,
    "derivatives_mark_price": DERIVATIVES_MARK_COLUMNS,
    "derivatives_index_price": DERIVATIVES_INDEX_COLUMNS,
    "basis_1h": BASIS_COLUMNS,
    "stablecoin_supply": STABLECOIN_SUPPLY_COLUMNS,
    "stablecoin_flows": STABLECOIN_FLOWS_COLUMNS,
    "stablecoin_peg": STABLECOIN_PEG_COLUMNS,
    "stablecoin_mint_burn": STABLECOIN_MINT_BURN_COLUMNS,
    "token_transfer": TOKEN_TRANSFER_COLUMNS,
    "onchain_daily_aggregate": ONCHAIN_DAILY_AGGREGATE_COLUMNS,
    "dex_volume": DEX_VOLUME_COLUMNS,
    "btc_blocks": BTC_BLOCKS_COLUMNS,
    "oracle_snapshot": ORACLE_SNAPSHOT_COLUMNS,
    "solana_snapshot": SOLANA_SNAPSHOT_COLUMNS,
    "derivatives_ratio": DERIVATIVES_RATIO_COLUMNS,
    "derivatives_oi_cross": DERIVATIVES_OI_CROSS_COLUMNS,
    "derivatives_liquidation": DERIVATIVES_LIQUIDATION_COLUMNS,
    "sentiment_fng": SENTIMENT_FNG_COLUMNS,
    "macro_daily": MACRO_DAILY_COLUMNS,
    "asset_master": ASSET_MASTER_COLUMNS,
    "listing_universe": LISTING_UNIVERSE_COLUMNS,
    "btc_network_daily": BTC_NETWORK_DAILY_COLUMNS,
    "cm_asset_daily": CM_ASSET_DAILY_COLUMNS,
}

# 每条记录必须存在的元数据字段
MANIFEST_FIELDS = [
    "dataset", "schema_version", "source_batches", "coverage_start",
    "coverage_end", "row_count", "duplicate_count", "gap_count",
    "suspect_count", "timestamp_unit", "timezone", "certification_status",
    "certified_at", "aggregation_rules",
]
