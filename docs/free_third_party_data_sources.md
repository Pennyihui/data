# 第三方免费加密数据源调研报告

> 调研目的：找出他人已免费公开的加密市场数据（GitHub / HuggingFace / Kaggle），
> 补足自建采集的缺口（逐笔成交、1m K线、期权波动率、链上进阶指标、强平、盘口）。
> 验证时间：2026-08-18（全部经网络实测可访问/可下载）。
> 许可：除注明外均为 CC BY 4.0（商用免费，需署名）。

---

## 1. 推荐清单（按价值排序）

### ⭐ 1.1 ErcinDedeoglu/crypto-market-data（GitHub）— 链上+衍生品指标，立即可用
**https://github.com/ErcinDedeoglu/crypto-market-data**

每天自动更新的 BTC **日频指标 JSON**（约 30 个文件），内容=商业付费数据（号称 $500-2000/月）：
- **链上**：`btc_mvrv_ratio`（MVRV）、`btc_puell_multiple`（Puell）、`btc_exchange_inflow/outflow/netflow/reserve`（交易所流入流出储备）、`btc_exchange_stablecoins_ratio`（稳定币比率）、`stablecoin_exchange_inflow/outflow`（稳定币流向）、`btc_miners_position_index` / `btc_miner_netflow_total`（矿工）、`btc_exchange_whale_ratio`、`btc_fund_flow_ratio`
- **衍生品/情绪**：`btc_long_liquidations` / `btc_short_liquidations`（**多空强平**）、`btc_funding_rates`、`btc_open_interest`、`btc_taker_buy_sell_ratio`
- **溢价**：`btc_coinbase_premium_gap/index`（Coinbase 溢价=机构买盘）、`btc_korea_premium_index`（Kimchi 溢价）

**补缺**：MVRV/SOPR 类、交易所流向、强平、溢价——正是调研报告中的 P1 缺口（原需 Glassnode/CryptoQuant 付费）。
**格式**：`[{timestamp_ms, value, last_modified}]` 日频 JSON，几 MB。

### ⭐ 1.2 KEDevO/crypto-market-datasets（HuggingFace）— 微结构数据湖
**https://huggingface.co/datasets/KEDevO/crypto-market-datasets**

10 币（BTC/ETH/SOL/BNB/XRP/DOGE/ADA/AVAX/LINK/DOT）Parquet 数据湖，月更新（v202626）：
- **agg_trades 逐笔成交**：现货 BTC 2017-08 起 ~160 亿行；合约 2020 起 ~90 亿行
- **1m K线**：现货+合约全历史（BTC 4.5M 行 / ETH 等 9 币同样覆盖）
- **funding_rates / open_interest / premium_index**（BTC）
- **Deribit BTC DVOL + 期权摘要**（15 分钟）← 期权波动率！调研 P1 缺口
- **FNG / 宏观**（DXY/SPX/Gold/US10Y）
文件按 `year=YYYY/month=MM` 分区，50-300MB/个（逐笔），K线/费率文件很小。

**补缺**：逐笔成交（完全没有）、1m K线（只有 1h）、Deribit 期权波动率、OI 历史补充。
**注意**：OI 同样受 30 天窗口限制；agg_trades 总量 100G-1T，按需按年下载。

### 1.3 binance/binance-public-data（官方，GitHub）
**https://github.com/binance/binance-public-data** + **data.binance.vision**

Binance 官方月度 ZIP 归档：全市场 K线（1s~1M）、aggTrades、trades、资金费率。免费、无 key。
**用途**：作为自建采集和上述数据集的上游原始源；官方未提供 OI 历史。

### 1.4 finom/static-klines（GitHub Pages 静态 API）
**https://github.com/finom/static-klines**

Top10 USDT 对 K线（15m~1M），GitHub Pages 静态 JSON，**无限流、免注册、每日刷新**。
**用途**：零搭建的 K线获取替代；适合图表/原型。⚠️ 社区缓存，非审计级（作者声明）。

### 1.5 Kaggle 盘口/微结构数据集（一次性快照）
- [Binance BTCUSDT L3 逐笔+盘口](https://www.kaggle.com/datasets/krrdev1/binance-btcusdt-l3-market-microstructure-data)
- [BTC L2 盘口 1s（2025-11-08）](https://www.kaggle.com/datasets/fast42/btc-l2-order-book-btcusdt-1s-11825)
- [CryptoLOB-2025 盘口数据集](https://www.kaggle.com/datasets/qkxuuuu/cryptolob-2025/data)
- [L2 30 档盘口（1m/5m）](https://www.kaggle.com/datasets/adamatractor/institutional-crypto-l2-orderbook-30lvl-1m-5m/data)
- [多币 OHLCV（日线 50+ 币多年）](https://www.kaggle.com/datasets/abdullahkhan70/daily-multi-year-ohlcv-crypto-market-data/data)
**用途**：盘口/微结构研究一次性数据；OHLCV 与自建重复，仅作交叉验证。

### 其他值得关注
- [brk（Bitcoin 数据浏览器/索引器，开源）](https://explore.market.dev/ecosystems/bitcoin/projects/brk)
- [api-evangelist/glassnode（Glassnode API 规格，仍需官方 key）](https://github.com/api-evangelist/glassnode)
- [cryptoQuotes（R 包，聚合行情/情绪/期权）](https://mirrors.cstcloud.cn/CRAN/web/packages/cryptoQuotes/refman/cryptoQuotes.html)
- [iturri（质量标注的蜡烛/资金/OI/order flow API，PyPI）](https://pypi.org/project/iturri/)

---

## 2. 缺口对照表（第三方数据如何补我们的缺口）

| 我们的缺口 | 第三方免费来源 | 状态 |
|-----------|--------------|------|
| 逐笔成交 aggTrades（完全没有） | HF KEDevO（160亿行，2017 起）| ✅ 可下载 |
| 1m K线（只有 1h） | HF KEDevO（4.5M 行）| ✅ 可下载 |
| 期权波动率 DVOL/IV（完全没有） | HF KEDevO Deribit（15m）| ✅ 可下载 |
| 链上进阶指标 MVRV/Puell/交易所流向/矿工/稳定币 | ErcinDedeoglu（日频 JSON）| ✅ 已下载 |
| **强平历史**（完全没有） | ErcinDedeoglu 多空强平（日频）| ✅ 已下载 |
| Coinbase/Kimchi 溢价 | ErcinDedeoglu | ✅ 已下载 |
| OI 长历史（仅 21 天，接口上限） | ⚠️ 各处均受限（HF 也是 30 天窗）| ❌ 需自建每日快照 |
| 盘口 L2/L3 历史 | Kaggle 一次性快照 | ⚠️ 非连续 |
| 币种市值排名长历史 | CoinGecko（需免费 key）| ⚠️ 待注册 key |

---

## 3. 已执行动作

- 全部 ErcinDedeoglu 日频 JSON → `data_new/additional/third_party/`（约 30 个文件）
- HF KEDevO 资金费率 parquet（BTCUSDT 2019-2026）→ `third_party/hf_funding/`
- HF Deribit DVOL（BTC 2021 起）→ `third_party/hf_deribit_dvol/`
- 样例验证：`btc_mvrv_ratio.json`（日频 MVRV，2010 至今数据可用）

## 4. 待办（需用户决策）

1. **下载 HF 1m K线 + agg_trades**（选择币种/年份；agg_trades 每文件 50-300MB）
2. **注册 CoinGecko 免费 key** → 币种市值排名回溯 2013
3. **OI/多空比每日快照任务**（第三方也解决不了 OI 长历史，只能自攒）
