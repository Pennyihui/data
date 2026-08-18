# 加密货币交易引擎 — 数据需求调研报告

> 调研目的：为构建加密货币交易引擎（信号研究 → 回测 → 实盘执行）明确"应该获取哪些数据"。
> 调研范围：行情/微结构、衍生品、链上、情绪与另类、宏观、执行与风控、标签目标变量。
> 结论形式：按优先级分级（P0 必须 / P1 应该 / P2 可选），并给出与现有 `Data_pipeline` 的结合路线图。

---

## 0. 结论摘要（先看这里）

| 优先级 | 数据 | 现状 | 理由 |
|--------|------|------|------|
| **P0 必须** | 多周期 OHLCV | ✅ 已有（1h，可扩 1m~1M） | 一切指标/标签的基础 |
| **P0 必须** | 成交明细 aggTrades / tick 数据 | ❌ 缺 | 真实成交特征（主动买卖、大单）、精确滑点回测 |
| **P0 必须** | 订单簿 L2 快照 + 增量 | ❌ 缺 | 流动性、盘口失衡、深度特征；实盘下单的依据 |
| **P0 必须** | 最优买卖价 best bid/ask | ❌ 缺 | 执行/滑点/盘口价差的即时信号 |
| **P0 必须** | 合约：资金费率 / OI / 多空比 / 标记价 | ✅ 已有 | 情绪代理 + 展期成本 + 爆仓风险 |
| **P0 必须** | 自身账户/持仓/成交回报 | ❌ 缺 | 实盘引擎必需（下单、风控、对账） |
| **P0 必须** | 历史盘口/成交（回测用） | ❌ 缺 | OHLCV 回测会严重高估收益（无滑点/深度约束） |
| **P1 应该** | 强平流水（liquidation feed） | ❌ 缺 | 爆仓级联是波动放大器，重要择时信号 |
| **P1 应该** | 期权：IV / skew / 期限结构（Deribit） | ❌ 缺 | 市场对未来波动的定价；机构行为 |
| **P1 应该** | 链上进阶指标（MVRV/SOPR/交易所流入流出/巨鲸） | ⚠️ 只有基础 | BTC 特有 alpha 来源，与情绪/宏观互补 |
| **P1 应该** | 新闻/社交情绪（CryptoPanic / LunarCrush 等） | ⚠️ 只有 FNG | 事件驱动 alpha；FNG 太粗糙 |
| **P1 应该** | 宏观完整集（FRED：利率/CPI/M2；DXY/股/债/金/VIX） | ⚠️ 已有 Yahoo 部分 | 风险偏好/流动性周期决定 BTC 大方向 |
| **P2 可选** | 多交易所行情（OKX/Bybit/Coinbase） | ❌ | 跨所套利、价差信号、交易所迁移 |
| **P2 可选** | 预测市场（Polymarket/Kalshi） | ❌ | 新研究的波动率/宏观事件信号 |
| **P2 可选** | 稳定币供应 / mempool 费用 / Google Trends | ❌ | 流动性代理与链上活动强度 |
| **P2 可选** | 商业数据供应商（CoinAPI/Kaiko/Amberdata） | ❌ | 省自建成本，换付费 |

一句话结论：**引擎的"骨骼"是行情微结构 + 合约数据 + 自身账户；"血肉"是链上 + 情绪 + 宏观；"护栏"是历史深度/成交（回测真实性）与强平/期权（风险预判）。**

---

## 1. 行情与微结构数据（Market Data / Microstructure）

### 为什么 OHLCV 不够
OHLCV 只给"价格柱"，无法回答：当时盘口有多深？大单是主动买还是主动卖？成交价相对盘口价差多少？
对于**回测真实性**和**实盘执行**，需要更细粒度数据（[Backtest Crypto Strategies with Real Market Data (Not Just OHLCV Charts)](https://www.coinapi.io/blog/backtest-crypto-strategies-with-real-market-data)）。

| 数据项 | 用途 | 推荐源 | 频率 | 优先级 |
|--------|------|--------|------|--------|
| OHLCV K线（1m~1M 多周期） | 指标/标签基础 | Binance `/api/v3/klines`（免费） | 1m 起 | P0 ✅ |
| aggTrades（聚合成交） | 主动买卖压力、大单、真实滑点 | Binance `/api/v3/aggTrades`（免费，全历史） | 逐笔/秒 | P0 |
| 订单簿 L2 快照（20~100档） | 盘口失衡、深度、支撑/阻力 | Binance REST `/depth` + WS `depth@100ms`（免费） | 100ms~1s | P0 |
| 订单簿增量（diff depth） | 实时重建盘口 | Binance WS `diffBookDepth` | 实时 | P0 |
| best bid/ask（ticker） | 即时价差/执行价 | Binance WS `bookTicker` | 实时 | P0 |
| 多周期 tick 级历史（回测） | 事件驱动回测、订单簿回放 | 商业：CoinAPI/Kaiko/Amberdata；自建：WS 落库 | 全历史 | P1 |
| 跨交易所行情 | 价差/套利/交易所迁移信号 | OKX/Bybit/Coinbase 公开 API | 同频 | P2 |

要点：
- tick 快照 vs 增量：快照简单但数据量大；增量（diff）省带宽但需要本地重建（[Tick Data vs Order Book Snapshots](https://www.coinapi.io/blog/tick-data-vs-order-book-snapshots-complete-guide-crypto-trading)、[L2 market data for Quant Traders](https://www.coinapi.io/blog/level-2-market-data-for-quant-traders)）。
- 回测至少要有"盘口限制"模型：成交价 = 报价价 + 深度冲击（[Where to Get Historical Bid Ask Data for Crypto Backtesting](https://www.coinapi.io/blog/historical-bid-ask-data-for-crypto-backtesting)）。

---

## 2. 衍生品数据（Derivatives）

衍生品数据是加密市场**最独特、最有信号价值**的数据层，且 Binance 公开接口即可获得。

| 数据项 | 用途 | 推荐源 | 频率 | 优先级 |
|--------|------|--------|------|--------|
| 资金费率 funding rate | 多空拥挤度、展期成本、carry | Binance `/fapi/v1/fundingRate` | 8h | P0 ✅ |
| 未平仓 OI 及 OI 变化 | 持仓增减、趋势确认 | Binance `/futures/data/openInterestHist` | 1h | P0 ✅ |
| 多空账户比 / 大户持仓比 | 散户/大户方向分歧 | Binance `/futures/data/*Ratio` | 1h | P0 ✅ |
| 主动买卖量比 taker buy/sell | 实际吃单方向 | Binance `/futures/data/takerlongshortRatio` | 1h | P0 ✅ |
| 标记价/指数价 K线 | 抗插针价格、基差、资金费率计算依据 | Binance `/fapi/v1/markPriceKlines`、`indexPriceKlines` | 1h | P0 ✅ |
| **强平流水** | 爆仓级联、杠杆拥挤度、恐慌拐点 | Binance `/fapi/v1/allForceOrders`（需 key）；[CoinAPI liquidation 数据集](https://www.coinapi.io/blog/crypto-liquidation-data)；[CoinMetrics market liquidations](https://gitbook-docs.coinmetrics.io/market-data/market-data-overview/market-liquidations)；Hyperliquid feed | 实时/历史 | P1 |
| **期权：IV / skew / 期限结构 / 24h DVOL** | 市场波动率定价、机构仓位、尾部风险 | [Deribit 公开 API](https://github.com/jeromeku/cryptocurrency-derivatives-pricing-and-delta-neutral-volatility-trading)；[期权流量分析（Deribit/OKX/Bybit）](https://lobehub.com/skills/jeremylongshore-claude-code-plugins-plus-skills-analyzing-options-flow) | 实时/分钟 | P1 |
| 基差 basis（现货-永续/季度） | 套利、杠杆需求 | 由现货+合约价计算 | 1h | P1 |
| 强平地图（liquidation map） | 密集爆仓价位预测 | [py-liquidation-map](https://github.com/CRY-D/py-liquidation-map) | 实时 | P2 |

要点：
- 资金费率 + OI + 多空比是加密独有的**情绪-仓位**三件套，已在 `get_additional_data.py` 获取。
- 期权 IV 与 skew 对**波动率预测**类策略几乎是必须的（可参考 [forgequant/oracle：期权波动率+交易所流向+预测市场信号](https://github.com/forgequant/oracle)）。

---

## 3. 链上数据（On-chain）

BTC 链上数据是"基本面"，回答：真实持有者在做什么？矿工在卖吗？交易所里的币在流入还是流出？

| 数据项 | 用途 | 推荐源 | 优先级 |
|--------|------|--------|--------|
| 算力/难度/矿工收入/活跃地址/交易数/市值（12项） | 网络健康度、周期定位 | [blockchain.info charts](https://apis.io/apis/blockchain/blockchaincom-charts-stats-market-data-api/)（免费） | P0 ✅ 已有 |
| **MVRV / SOPR / 已实现市值** | 盈利盘抛压、周期顶部/底部 | [Glassnode](https://insights.glassnode.com/the-predictive-power-of-glassnode-data/)（免费额度小）；CryptoQuant | P1 |
| **交易所流入/流出、储备变化** | 抛售/囤币意图 | Glassnode / CryptoQuant（[CryptoQuant MCP/API](https://ping.mcp.so/zh/server/cryptoquant/CryptoQuant)） | P1 |
| 巨鲸转账（Whale Alert） | 大户异动 | [whale-alert.io API](https://developer.whale-alert.io/api-account/documentation)（免费额度） | P1 |
| 矿工流向（矿工卖币量） | 供给侧压力 | Glassnode / CryptoQuant | P1 |
| 稳定币供应/流入交易所 | 场外资金入场的先行指标 | Glassnode / CryptoQuant | P1/P2 |
| 休眠流通（dormant circulation）、币龄 | 老币解冻=潜在抛压 | Glassnode / CryptoQuant | P2 |
| mempool 待确认交易与费率 | 链上活动强度、网络拥堵 | [mempool.space API](https://mintlify.wiki/mempool/mempool/introduction)（免费） | P2 |

要点：
- 已有 blockchain.info 的 12 项基础指标（2010 至今）；进阶指标（MVRV/SOPR/交易所流向）被验证对 ML 价格预测有增量贡献（[Glassnode ML 策略](https://insights.glassnode.com/the-predictive-power-of-glassnode-data/)、[2026 链上+ML 预测综述](https://www.altcointrading.net/bitcoin-price-forecasting-with-ml-models/)）。
- Glassnode/CryptoQuant 免费额度有限，进阶指标需付费或申请学术 key。

---

## 4. 情绪与另类数据（Sentiment / Alternative）

| 数据项 | 用途 | 推荐源 | 优先级 |
|--------|------|--------|--------|
| 恐惧贪婪指数 FNG | 市场情绪温度计 | [alternative.me](https://github.com/rhettre/fear-and-greed-crypto)（免费） | P0 ✅ 已有 |
| 新闻流（分类+情绪打分） | 事件驱动信号 | [CryptoPanic API](https://github.com/eltonaguiar/findtorontoevents_antigravity.ca/blob/main/alpha_engine/cryptopanic_feargreed.py)；[forgequant/sentinel 情绪栈](https://github.com/forgequant/sentinel)（feargreed+news+polymarket+lunarcrush） | P1 |
| 社交情绪（Twitter/Reddit 聚合） | 散户热度 | [LunarCrush](https://github.com/forgequant/sentinel)；The TIE | P1 |
| 预测市场（Polymarket / Kalshi） | 宏观/事件概率的"真金白银"定价 | [Polymarket API](https://github.com/Rekko-AI/rekko-mcp)、[Kalshi 波动率预测研究](https://ar5iv.labs.arxiv.org/html/2604.01431) | P2 |
| Google Trends | 零售关注度 | pytrends（免费） | P2 |

要点：
- FNG 已有但粒度粗（日频、单一值）；**新闻情绪**和**社交情绪**是事件驱动 alpha 的主要来源，可并行调研 [FinRL alt_data 集成方案](https://github.com/Mattbusel/FinRL_DeepSeek_Crypto_Trading/blob/main/alt_data.py)。
- 预测市场（Polymarket/Kalshi）被研究发现能预测加密波动率（[Kalshi 论文](https://ar5iv.labs.arxiv.org/html/2604.01431)），属前沿信号。

---

## 5. 宏观数据（Macro）

BTC 本质是"流动性资产"，与美元、风险资产高度联动（[Oil, USD & Bonds: How They Secretly Move Crypto](https://blog.mexc.com/news/oil-usd-bonds-how-they-secretly-move-crypto/)）。

| 数据项 | 用途 | 推荐源 | 优先级 |
|--------|------|--------|--------|
| DXY / 标普500 / 纳斯达克 / VIX / 黄金 / 10Y美债 | 风险偏好与美元流动性 | Yahoo Finance（免费） | P0 ✅ 已有 |
| 联邦基金利率 / CPI / M2 / 缩表（FRED） | 货币政策周期、流动性拐点 | [FRED API](https://fred.stlouisfed.org/docs/api/api_key.html)（免费 key） | P1 |
| 全球 M2、逆回购规模、TGA 余额 | 全球流动性代理 | FRED / 各国央行 | P1/P2 |
| 宏观状态组合（regime panel） | 宏观过滤闸门 | TradingView FRED 组合面板思路 | P2 |

要点：已有 Yahoo 6 项；补 FRED（CPI/M2/利率）后即可构建"宏观风险开关/regime 过滤"——很多策略只在 risk-on 环境开仓。

---

## 6. 执行、回测与风控数据（Execution / Backtest / Risk）

| 数据项 | 用途 | 推荐源 | 优先级 |
|--------|------|--------|--------|
| 历史盘口/成交（tick 级） | 事件驱动回测、滑点/深度建模 | 自建 WS 落库 或 CoinAPI/Kaiko/Amberdata | P0（回测前必须） |
| 真实手续费/资金费率支付明细 | 回测净收益真实性 | 交易所费率表 + 资金费率历史 | P0 |
| 自身账户/持仓/挂单/成交回报 | 实盘状态机、风控、对账 | 交易所私密 API + WS user stream | P0（实盘） |
| 下单响应/延迟监控 | 执行质量 | 自建埋点 | P1 |
| 逐笔委托/成交流（aggTrade + 深度） | 实时特征更新 | Binance WS `aggTrade`/`depth`/`bookTicker`（[streams 文档](https://deepwiki.com/sammchardy/python-binance/4.3-socket-types-and-streams)） | P0 |

要点：
- 回测真实性是引擎可信度的生命线：必须把**手续费 + 滑点 + 深度限制**纳入（[How to Backtest a Crypto Bot: Realistic Fees, Slippage, and Paper Trading](https://paybis.com/blog/how-to-backtest-crypto-bot/)；[Bitget 回测最佳实践](https://www.bitget.com/academy/12560603877835)）。
- 实时引擎的数据层 = **WebSocket 流 + 落库存储**（时序库/Parquet），REST 只用于历史补数和快照初始化。

---

## 7. 标签与目标变量（Labels / Targets）

| 数据项 | 用途 | 现状 |
|--------|------|------|
| FTH / CT / Oracle 趋势标签 | 方向预测监督目标 | ✅ 已有（论文复现） |
| 未来波动率目标（RV/IV） | 波动率预测模型目标 | ❌ 可从 K线计算，建议补 |
| 未来收益/最大回撤/Sharpe | 组合级目标 | ❌ 可自行计算 |
| 事件标签（爆仓潮/ETF 消息/宏观数据发布） | 事件研究 | ❌ 需人工/半自动标注 |

---

## 8. 数据源与成本对照

| 数据源 | 免费额度 | 付费档 | 适合 |
|--------|---------|--------|------|
| Binance 现货/合约公开 API | 全历史 K线/aggTrades/合约统计，无限接近免费 | — | 主数据源 ✅ |
| Binance WS 实时流 | 免费 | — | 实时特征 ✅ |
| alternative.me FNG | 免费 | — | 情绪 ✅ |
| blockchain.info charts | 免费 | — | 链上基础 ✅ |
| Yahoo Finance | 免费（非官方，限流） | — | 宏观 ✅ |
| mempool.space | 免费 | — | mempool/费率 |
| whale-alert.io | 免费（~100次/天） | 付费 | 巨鲸 |
| Deribit | 公开 API 免费 | — | 期权 |
| CryptoPanic / LunarCrush | 免费额度 | 付费 | 新闻/社交 |
| Glassnode / CryptoQuant | 少量免费 | 按月付费 | 链上进阶 |
| FRED | 免费（需注册 key） | — | 宏观 |
| CoinAPI / Kaiko / Amberdata | 试用 | 按量/订阅（机构级 tick 数据，参考 [对比](https://www.coinapi.io/blog/coinapi-vs-kaiko-crypto-market-data-comparison)、[CoinAPI vs Kaiko vs Amberdata](https://canton.wiki/compare/amberdata-vs-kaiko)） | 高质量历史 tick/盘口 |

---

## 9. 实施路线图（结合现有 Data_pipeline）

**阶段 0（已完成）**：OHLCV + 合约统计（funding/OI/多空比/主动买卖）+ FNG + 链上基础 + 宏观（Yahoo）→ `data_new/additional/`。

**阶段 1（引擎数据层骨架，P0）**：
1. 新增 **WS 实时采集器**（aggTrade / depth / bookTicker / markPrice / 强平流），落库为 Parquet/时序表；REST 补历史。
2. 补 **aggTrades 历史**与 **L2 快照历史**（自建或试用 CoinAPI/Kaiko 免费档），支撑事件驱动回测。
3. 接入**私密 API**：账户/持仓/成交回报（实盘前置条件）。
4. 回测框架加入**滑点 + 深度限制 + 真实手续费**模型。

**阶段 2（信号增强，P1）**：
5. 注册 **FRED key** → 补 CPI/M2/利率，构建宏观 regime 过滤。
6. 接入 **Deribit 期权** → IV/skew/期限结构特征。
7. 链上进阶指标（Glassnode/CryptoQuant 免费额度）：MVRV、SOPR、交易所流入流出。
8. 新闻/社交情绪（CryptoPanic / LunarCrush 免费额度）。

**阶段 3（锦上添花，P2）**：预测市场、稳定币供应、mempool、多交易所、商业数据供应商。

---

## 10. 注意事项

1. **数据对齐与防未来函数**：不同频率（8h 资金费率 / 1h OI / 日频宏观）合并时必须向后填充（ffill）且 `shift(1)`，禁止用未来值（与 `calculate_indicator_v2.py` 约定一致）。
2. **网络环境**：本机需经本地代理 `127.0.0.1:7897`，间歇性不稳定——实时 WS 采集器需要断线重连 + 心跳 + 序列号校验（参考 Binance WS [流文档](https://developers.binance.com/zh-CN/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)）。
3. **数据质量**：检查交易所停摆/插针（用标记价 K线抗插针）、币种退市、成交量异常；多交易所数据注意时钟同步。
4. **存储规划**：tick/盘口数据量大，建议 Parquet 分区 + 数据库索引；OHLCV 与特征仍可 CSV。
5. **成本控制**：免费额度够研究；只有需要"机构级历史 tick/盘口"时才值得付费供应商（CoinAPI/Kaiko/Amberdata）。

---

## 参考来源

- [CoinAPI：训练 AI 交易模型的最佳市场数据](https://www.coinapi.io/blog/best-market-data-for-ai-trading-models)、[用真实数据回测（不止 OHLCV）](https://www.coinapi.io/blog/backtest-crypto-strategies-with-real-market-data)、[订单簿回放指南](https://www.coinapi.io/blog/crypto-order-book-replay)、[tick vs 快照](https://www.coinapi.io/blog/tick-data-vs-order-book-snapshots-complete-guide-crypto-trading)、[历史 bid/ask 数据](https://www.coinapi.io/blog/historical-bid-ask-data-for-crypto-backtesting)、[强平数据集](https://www.coinapi.io/blog/crypto-liquidation-data)、[CoinAPI vs Kaiko](https://www.coinapi.io/blog/coinapi-vs-kaiko-crypto-market-data-comparison)
- [Glassnode：链上数据的预测力（ML 策略）](https://insights.glassnode.com/the-predictive-power-of-glassnode-data/)、[2026 链上+ML 预测](https://www.altcointrading.net/bitcoin-price-forecasting-with-ml-models/)
- [CryptoQuant API/MCP](https://ping.mcp.so/zh/server/cryptoquant/CryptoQuant)、[whale-alert API](https://developer.whale-alert.io/api-account/documentation)、[mempool.space](https://mintlify.wiki/mempool/mempool/introduction)
- [Deribit 期权数据采集项目](https://github.com/jeromeku/cryptocurrency-derivatives-pricing-and-delta-neutral-volatility-trading)、[期权流量分析](https://lobehub.com/skills/jeremylongshore-claude-code-plugins-plus-skills-analyzing-options-flow)、[forgequant/oracle（期权+链上+预测市场）](https://github.com/forgequant/oracle)
- [forgequant/sentinel（情绪栈）](https://github.com/forgequant/sentinel)、[FinRL 另类数据](https://github.com/Mattbusel/FinRL_DeepSeek_Crypto_Trading/blob/main/alt_data.py)、[Kalshi 预测市场与加密波动率](https://ar5iv.labs.arxiv.org/html/2604.01431)
- [油/美元/债券如何影响加密](https://blog.mexc.com/news/oil-usd-bonds-how-they-secretly-move-crypto/)、[FRED API](https://fred.stlouisfed.org/docs/api/api_key.html)
- [Paybis：回测中的真实费用与滑点](https://paybis.com/blog/how-to-backtest-crypto-bot/)、[Bitget 回测最佳实践](https://www.bitget.com/academy/12560603877835)、[python-binance streams](https://deepwiki.com/sammchardy/python-binance/4.3-socket-types-and-streams)
