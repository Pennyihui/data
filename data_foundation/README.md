# Crypto Data Foundation

加密量化数据底座：**L0 Raw → L1 Normalized → L2 Certified**（L3 Features 不在本仓库）。
设计文档见 [docs/crypto-data-foundation-research.md](docs/crypto-data-foundation-research.md)。

## 分层

| 层 | 目录 | 说明 |
|----|------|------|
| **L0 Raw** | `data/raw/{venue_id}/{dataset}/ingest_date=YYYY-MM-DD/` | 原始 CSV/JSON，**不可变**，每批次 sha256 checksum + `.meta.json`（源/参数/抓取时间/时间戳单位） |
| **L1 Normalized** | `data/l1/{dataset}/{venue_id}/{market_type}/{instrument}/interval=.../data.parquet` | 统一 schema、UTC、微秒时间戳；1d/1w 由 1h 聚合（规则入 manifest） |
| **L2 Certified** | `data/l2/certified/{dataset}/.../data.parquet` + `manifest.json` | 质量规则认证 + 统计 + 认证状态；**研究默认读取层** |

## 数据集

- `market_candle_spot_{1h,1d,1w}` — K 线事实表（主键 venue+instrument+interval+open_time_utc；含 is_closed/is_gap/is_suspect/quality_reason/data_available_at）
- `derivatives_funding` / `derivatives_open_interest` / `derivatives_mark_price` / `derivatives_ratio_{glsr,tlsr_acct,tlsr_pos,taker}`
- `instrument` — 交易所元数据（上市时间/tick/面值/结算币等，点时化）

## 质量规则（quality_rule_version=1.0.0）

1. `high >= max(open, close)`、`low <= min(open, close)`、`high >= low`
2. `volume >= 0`、`trade_count >= 0`
3. `open_time_utc` 唯一（主键）
4. 周期边界对齐（1h 整点 / 1d 零点）
5. 时间不超过可用时间
6. 缺口检测（与前一根 > 1 周期 → `is_gap`）
7. 数值有限性、价格非负
异常**只标记不修改**：`is_suspect` + `quality_reason`。

## 使用

```bash
# 全量: L0 导入 + L1 标准化 + L2 认证
python -m data_foundation.run_pipeline --stage all --assets BTC,ETH,SOL

# 分阶段（exchangeInfo 失败可重跑 --stage l0 补齐）
python -m data_foundation.run_pipeline --stage l0
python -m data_foundation.run_pipeline --stage l1
python -m data_foundation.run_pipeline --stage l2
```

研究读取（只读 certified，支持 PIT）：

```python
from data_foundation.reader import load_candles, load_derivatives, load_manifest

df = load_candles("binance", "BTC-USDT", "1h")                # certified 1h
df = load_candles("binance", "BTC-USDT", "1h", as_of="2021-01-01")  # PIT 过滤
fund = load_derivatives("binance", "BTC-USDT", "derivatives_funding")
m = load_manifest("market_candle_spot_1h")                    # 认证 manifest
```

## MVP 覆盖（当前）

- 交易所：Binance spot + perpetual（USDT-M）
- 资产：BTC ETH SOL BNB XRP ADA DOGE AVAX LINK LTC DOT UNI AAVE ARB POL（15）
- 周期：1h（原始）、1d/1w（派生）
- 数据：spot OHLCV（2017-08 起）、funding rate 全历史、OI（近 21 天，接口上限）、mark price、多空/主动买卖比、exchangeInfo 元数据

## 已知边界

- OI/多空比等历史受 Binance 接口限制（最近 ~500 小时），需自建每日快照持续积累
- 未收盘 K 线不进入 certified（`is_closed` 标记）
- 跨交易所（OKX/Coinbase）、稳定币、链上为后续阶段
