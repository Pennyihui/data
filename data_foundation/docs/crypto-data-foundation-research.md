# Crypto Data Foundation — 数据底座设计研究笔记

> 目标：面向中低频、多币种量化研究的数据底座。
> 核心原则：时间语义一致、资产/交易对可追溯、历史数据不被未来信息污染、
> 原始数据可重放、数据质量可量化认证、支持跨交易所/跨链/跨资产比较。

## 分层架构

```text
L0 Raw        原始数据，不修改，可重放
L1 Normalized 统一字段、时间、资产和交易对语义
L2 Certified  经过质量检查、PIT 处理、可直接用于研究
L3 Features   因子和研究特征，不反向污染基础数据
```

## 一、应该包含哪些数据

### 1. 现货市场数据（第一优先级，最小可用底座）

每交易所/交易对/周期至少保存：

```text
exchange, market_type, symbol, base_asset, quote_asset,
timestamp_open, timestamp_close, open, high, low, close,
volume_base, volume_quote, trade_count,
taker_buy_volume_base, taker_buy_volume_quote,
is_closed, source, ingested_at
```

周期建议：1m、5m、15m、1h、4h、1d、1w。
存储策略：
- 原始层保存 1m 或成交明细
- 标准层保存 1h
- 日线和周线由标准层重新聚合，记录聚合规则和版本

Binance 现货 K 线含 12 字段（含基础/计价成交量、成交笔数、主动买成交量）。
注意：Binance 现货自 2025-01-01 起采用微秒时间戳，合约仍可能用毫秒——
时间戳单位必须进入数据元信息，不能靠数值大小猜测。

### 2. 交易所元数据（和行情同等重要）

```text
exchange, symbol, instrument_id, base_asset, quote_asset, market_type,
contract_type, contract_size, tick_size, lot_size, min_order_size,
price_precision, quantity_precision, listing_time, delisting_time,
status, settlement_asset, underlying_asset
```

必须记录：上市/退市时间、交易对改名、合约到期交割、合约面值、结算币种、
最小价格变动、最小下单量。否则会出现：退市后零值当真实价格、
现货/永续混为一谈、不同面值成交量直接比较、用当前列表回看历史（幸存者偏差）。

### 3. 衍生品数据（中低频研究核心解释变量）

```text
funding_rate, open_interest, mark_price, index_price, basis,
liquidations, futures_volume, futures_trade_count,
contract_expiry, settlement_price
```

重点衍生变量：basis = futures/spot - 1、annualized_basis、funding_rate_8h、
open_interest_usd、open_interest_change、liquidation_volume、mark_index_spread。
注意：多数交易所历史 OI 不完整，重要指标应从现在开始持续采集，
采集开始时间记录到 manifest。

### 4. 成交明细和盘口（按研究需要分级）

基础档：1m OHLCV + 成交量 + spread + top-of-book。
增强档：逐笔成交 + L2/L3 order book。
中低频研究不必长期保存完整订单簿；1h/4h/1d 研究通常不需要 L3。

### 5. 链上数据（先做高价值子集）

原始层：区块/交易/receipt/log（含 removed 字段，处理链重组）。
解码层：ERC-20 Transfer/Mint/Burn、DEX Swap、Lending、Staking、Bridge。
研究聚合层：stablecoin supply/netflow、exchange inflow/outflow、
dex volume/liquidity、large transfer、active addresses、fees、gas、bridge flow。
可先用 Dune/Flipside 聚合源，保留查询版本与结果快照。

### 6. 稳定币和 DeFi

稳定币：USDT/USDC/DAI 按链按合约 totalSupply()，mint/burn、交易所余额、
peg deviation。DeFi：Uniswap V3 swap volume、pool liquidity、fee tier、
TVL、TWAP、Chainlink latestRoundData()（answer/updatedAt/decimals）。

### 7. 资产、交易对和 universe（点时规则）

资产主数据：asset_id、ticker、chain、contract_address、decimals、
launch/retirement_time、symbol_aliases、is_wrapped。
交易对点时成员表：as_of_time、asset_id、exchange、symbol、
is_listed、is_tradable、universe_rank、selection_reason。
（现有 universe-policy-v1/membership.csv 已具备雏形，继续沿用）

## 二、推荐数据模型

### market_candle（交易所 K 线事实表）

```text
venue_id, instrument_id, bar_interval, open_time_utc, close_time_utc,
open, high, low, close, volume_base, volume_quote, trade_count,
taker_buy_volume_base, taker_buy_volume_quote,
is_closed, is_gap, is_suspect, source_batch_id
```

主键：(venue_id, instrument_id, bar_interval, open_time_utc)。
不要用 close_time 作主键。

### derivatives_snapshot（衍生品事实表）

```text
venue_id, instrument_id, timestamp_utc, mark_price, index_price,
funding_rate, open_interest_contracts, open_interest_notional,
volume, basis, settlement_price, is_final, source_batch_id
```

open_interest_contracts 和 notional 都要保存（不同合约面值不同）。

### chain_event / token_transfer（链上事实表）

原始事件与解码事件分开两张表；解码器升级可从原始事件重放。

## 三、存储和架构

- L0 Raw：raw/{venue_id}/{dataset}/{ingest_date}/，不可变、checksum、
  记录 API/参数/抓取时间/响应时间、保留源时间戳单位、断点续传。
- L1 Normalized：Parquet（TIMESTAMP(isAdjustedToUTC=true)），
  分区 venue/dataset/date 级别（symbol 作列和排序键），UTC。
- L2 Certified：研究代码默认读取层；每数据集 manifest.json：
  schema_version、source_batches、coverage_start/end、row_count、
  duplicate/gap/suspect_count、timestamp_unit、timezone、
  certification_status、certified_at。
  认证规则：high>=max(o,c)、low<=min(o,c)、high>=low、volume>=0、
  trade_count>=0、open_time 唯一、周期边界正确、时间不超过可用时间、
  上市/退市一致。异常不静默修正，保留原值加 is_suspect/quality_reason/
  quality_rule_version。

## 四、采集策略

回填优先级：官方批量历史 > 官方 REST 分页 > WebSocket 增量 > 第三方。
增量流程：raw landing → close confirmation → REST reconciliation →
normalized → quality certification → research snapshot。
未收盘 K 线只进临时区。每天校准：重拉最近 2-3 天、对比 WS/REST、
检查缺口重复异常、更新元数据、发布 certified snapshot。

## 五、质量与研究治理

- 时间：UTC，int64 microseconds 或 timestamp[us, UTC]；L0 保留源单位。
- 特征带 feature_timestamp / data_available_at / source_timestamp；
  回测用 data_available_at <= decision_time（防前视偏差）。
- Universe 三套概念：research_universe / tradable_universe / liquid_universe。
- 多交易所价格：venue_price + venue_liquidity + venue_weight，
  构造单所价/成交量加权/跨所中位数/指数价/Chainlink 参考价/CEX-DEX spread。

## 六、实施顺序

1. 标准化现有现货底座（15-50 主流资产、1h/1d/1w、Binance spot）
2. 增加第二交易所（OKX/Coinbase）+ 衍生品（funding/OI/mark/index/basis）
3. 稳定币与资金流（USDT/USDC/DAI、supply/mint/burn/流向/peg）
4. 链上解码与 DeFi（ERC-20 Transfer、Uniswap、Chainlink）
5. 期权与微观结构（IV surface、Greeks、skew、order book、liquidations）

## 七、MVP 范围

```text
交易所：Binance spot + perpetual
资产：BTC ETH SOL BNB XRP ADA DOGE AVAX LINK LTC DOT UNI AAVE ARB
周期：1h、1d、1w
数据：spot OHLCV、perpetual OHLCV、funding rate、open interest、
      mark/index price、listing/delisting metadata
格式：Raw JSON/CSV → Normalized Parquet → DuckDB 查询 → manifest + certify
```

## 最重要的原则

1. 原始数据不可变，任何清洗都可重放。
2. open time 是 K 线主键，未收盘数据不能进入正式快照。
3. 所有资产、交易对、上市和退市状态都必须点时化。
4. 原始成交量、计价成交量、合约成交量不能混用。
5. 链上数据必须保存 block/tx/log 标识和 reorg 状态。
6. 质量异常要标记，不能静默修值。
7. 研究默认读取 certified snapshot。
8. 所有派生特征必须带 data_available_at，防止前视偏差。
9. 先把现货、衍生品和稳定币做可靠，再扩展链上和期权。
10. 数据源、抓取参数、时间单位和版本都必须写入 manifest。

## 参考

- Binance Spot API: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
- Deribit API: https://docs.deribit.com/
- Ethereum Execution APIs: https://ethereum.github.io/execution-apis/
- EIP-20: https://eips.ethereum.org/EIPS/eip-20
- Dune Data Docs: https://docs.dune.com/
- Chainlink Data Feeds: https://docs.chain.link/data-feeds
- Parquet Logical Types: https://parquet.apache.org/docs/file-format/types/logicaltypes/
