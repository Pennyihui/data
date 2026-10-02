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

## MVP 覆盖（当前，2026-10）

- 交易所：Binance spot + perpetual（USDT-M，358 合约）、OKX、Bybit、Bitget、Coinbase
- 研究宇宙：568 币（生命周期 ≥365 天）；现货 1h 覆盖 590 个 instrument（2015-07 起）
- 周期：1h（原始）+ 4h/1d/1w（派生），现货与永续均有
- 数据：spot/perp OHLCV、funding（全历史）、OI、mark/index price（永续全历史）、
  多空比/主动买卖比、链上 token_transfer（Ethereum+Arbitrum）、稳定币、宏观、FNG、
  BTC 链上统计、期权快照、点时 instrument/asset_master/三层 universe

## 每日调度

Windows 计划任务 `DataFoundation_DailyIngest` @ 02:30 → `python -X utf8 run_daily.py`。
`run_daily.py` 注册 14 个源（binance_klines / funding / stats、okx、coinbase、stablecoins、
onchain、metadata、sentiment_macro、tron、cross_deriv、universe、**binance_perp**、rebuild），
逐源容错，失败写 `daily_manifest.json` 并经 webhook 告警。
手工单源：`python -X utf8 run_daily.py --sources universe`。

## 关键不变量（已修的坑，改代码前务必读）

- **原子写** `data_foundation/atomic.py`：所有 L1/L2 parquet 先写 `.tmp` 再 `os.replace`。
  进程被强杀不留截断文件——曾导致 universe 连续 17 天 `ArrowInvalid` 自锁。
- **合并语义** `derivatives.write_derivatives_parquet`：日增量与既有 L1 concat 后按时间列
  `keep="last"` 去重，保护 Vision 深回填历史不被日增量冲回短窗口。
- **流式重建** `onchain_stream.py`：token_transfer（5500 万行）逐文件解码→按日切分→行组
  追加，峰值 ~2GB；旧全量解码路径在 16GB 机器 OOM。
- `core_numeric_cols=[]` = 不查数值列（与 `None`=全查语义区分）。

## 体检工具

```bash
python -X utf8 _scan_corrupt.py    # 全库 parquet 完整性
python -X utf8 _coverage_scan.py   # 每数据集 行数/时间范围/新鲜度/证书
python -X utf8 _venue_scan.py      # 按 (dataset, venue) 粒度，揪"整体新但某所停更"
```

## 已知边界

- 清算量(liquidation)需 Coinalyze/CoinGlass 付费 key，免费源无法回填历史
- 期权仅有快照，历史需自行积累
- Bitget 衍生品 API 仅返回最近 100 条
- Bybit mark/index 未覆盖（仅 funding/OI/klines）
