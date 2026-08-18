# Data Pipeline — Agent 上下文

BTC 量化数据流水线：Binance API → 多周期技术指标（200+特征） → 趋势标注（FTH/CT/Oracle）。

---

## 目录结构

```
Data_pipeline/
├── get_data.py                   # Binance 现货 K线拉取（基础数据: OHLCV）
├── get_additional_data.py        # 扩展数据拉取（合约/情绪/链上/宏观 + 1h 上下文宽表）
├── get_pair_rankings.py          # 交易对排行（现货3632对/合约742对/全局市值100币）
├── get_multi_derivatives.py      # Top100 永续币衍生品数据（资金费率/OI/多空比/标记价, 断点续传）
├── build_funding_cross_section.py # 跨币种资金费率截面特征（市场拥挤度分布, 8h）
├── build_oi_cross_section.py      # 跨币种 OI 截面特征（1h, 最近~21天）
├── build_4h_research_table.py     # 4h 研究宽表（BTC K线+资金/OI截面+FNG+宏观, 2019-09起）
├── plot_crowding.py               # 拥挤度历史可视化（资金费率截面 + OI 截面）
├── produce_csv.py                # 主线流水线（编排入口）
├── calculate_indicator_v2.py     # 技术指标库（2583行, 核心模块）
├── merge_csv.py                  # 多周期合并（4h/日/周）
├── 1_feature/                    # （预留）
├── 2_label/
│   ├── label_of_feature_trend.py # 趋势标注脚本（含鲁棒性评估）
│   ├── label.txt                 # 方向标签所依据的论文文本摘录
│   └── visualization/
│       └── daily_label_comparison.png
├── docs/
│   ├── trading_engine_data_requirements.md   # 交易引擎数据需求调研报告（P0/P1/P2 分级）
│   └── superpowers/
│       ├── specs/
│       └── plans/
├── wast/                         # 废弃版本归档
│   ├── feature.py                # v1 初版
│   ├── feature_v1_5.py           # v1.5 含未来收益率
│   ├── feature_v2.py             # v2 特征优化（PCA/SelectKBest）
│   └── h2day_resample.py         # 重采样工具
└── .claude/
    ├── settings.json
    └── settings.local.json
```

---

## 数据流

```
Binance API (1h K线)              ← get_data.py (已注释,需手动启用)
  │
  ▼
b_1h.csv                          ← 输入文件
  │
  ▼
produce_csv.py (main)
  ├─ resample → 周线/日线/4h/1h
  ├─ WeeklyIndicators.calculate()
  ├─ DailyIndicators.calculate()
  ├─ Hourly4HIndicators.calculate()
  ├─ Hourly1HIndicators.calculate()
  ├─ add_multi_timeframe_divergence()
  ├─ FeatureValidator.validate_no_future_data()
  ├─ feature_schema.csv / feature_schema.json
  └─ dataset_manifest.json
  │
  ▼
B_{tf}_with_features.csv          ← 各时间框架特征文件
  │
  ▼
merge_csv.py
  └─ prepare_multi_timeframe_data()
     ├─ merged_4h_research.csv    ← 完整研究宽表（4h主表 + lagged daily/weekly）
     └─ merged_4h_context.csv     ← 轻量上下文宽表（daily filter + weekly context）
  │
  ▼
label_of_feature_trend.py
  ├─ label_fth()   — Fixed-Time Horizon
  ├─ label_ct()    — Continuous Trend
  ├─ label_oracle()— DP 最优标签
  ├─ cumulative_return_from_labels()
  ├─ compute_robustness_profile()  — 论文鲁棒性曲线
  ├─ B_multilabeled_of_feature_trend_v2.csv
  └─ label_config.json
```

---

## 入口函数

| 脚本 | 入口 | 说明 |
|------|------|------|
| `produce_csv.py` | `main()` → `calculate_features_for_all_timeframes()` | 完整流水线：加载→重采样→4框架指标→背离→验证→保存，并输出 schema/manifest |
| `merge_csv.py` | `prepare_multi_timeframe_data()` | 生成两个4h宽表：`merged_4h_research.csv` 与 `merged_4h_context.csv`，并保留 daily/weekly 来源时间戳 |
| `label_of_feature_trend.py` | 脚本文件（直接运行） | 读取 `merged_4h_research.csv` → 标注 FTH/CT/Oracle → 收益计算 → 鲁棒性评估 → 输出标签与 `label_config.json` |

---

## 外部路径（硬编码，迁移时需更新）

| 路径 | 用途 |
|------|------|
| `/LocalSSD/p9056/TestTools_ANALY/data_new/b_1h.csv` | produce_csv.py 的输入 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/` | 当前统一的特征/合并/标签核心输出目录 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_1h_with_features.csv` | 1h 特征输出 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_4h_with_features.csv` | 4h 特征输出 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_daily_with_features.csv` | 日线特征输出 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_weekly_with_features.csv` | 周线特征输出 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/feature_schema.csv` | 特征 schema（CSV） |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/feature_schema.json` | 特征 schema（JSON） |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/dataset_manifest.json` | 数据集清单与行列统计 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/merged_4h_research.csv` | 完整研究宽表（4h主表 + lagged daily/weekly） |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/merged_4h_context.csv` | 轻量上下文宽表（daily filter + weekly context） |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_multilabeled_of_feature_trend_v2.csv` | 多时间框架方向预测标签输出 |
| `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/label_config.json` | 标签参数配置输出 |
| `/LocalSSD/p9056/TestTools_ANALY/pipeline/2_label/visualization/` | 标签可视化输出 |

---

## 扩展数据源（get_additional_data.py）

`get_data.py` 只提供基础 K 线（OHLCV），`get_additional_data.py` 在基础之上补充 4 大类免费数据，全部输出到 `data_new/additional/`：

| 类别 | 数据 | 接口 | 频率/历史 |
|------|------|------|-----------|
| A. 合约衍生品 | 资金费率 funding rate | `fapi.binance.com/fapi/v1/fundingRate` | 8h, 2019-09 至今全历史 |
| A. 合约衍生品 | 未平仓合约 open interest | `fapi.binance.com/futures/data/openInterestHist` | 1h, 最近约 500 小时 |
| A. 合约衍生品 | 全市场多空账户比 | `.../globalLongShortAccountRatio` | 1h |
| A. 合约衍生品 | 大户多空账户比/持仓比 | `.../topLongShortAccountRatio` / `topLongShortPositionRatio` | 1h |
| A. 合约衍生品 | 主动买卖量比 | `.../takerlongshortRatio` | 1h |
| A. 合约衍生品 | 标记价格 K 线（抗插针） | `fapi.binance.com/fapi/v1/markPriceKlines` | 1h |
| A. 合约衍生品 | 溢价/下一期资金费率快照 | `fapi.binance.com/fapi/v1/premiumIndex` | 实时快照 |
| B. 情绪 | 恐惧贪婪指数 FNG | `api.alternative.me/fng/` | 日, 2018-02 至今 |
| C. 链上 | 算力/难度/矿工收入/活跃地址/交易数/内存池/市值 等 12 项 | `api.blockchain.info/charts/{slug}` | 日, 2010 至今 |
| D. 宏观 | DXY/标普500/纳斯达克/VIX/黄金/10Y美债 | Yahoo Finance chart API (免 key) | 日, 近 5 年 |
| F. 宏观(可选) | CPI/M2/联邦基金利率 等 FRED 序列 | `api.stlouisfed.org/fred` (需免费 key, 环境变量 `FRED_API_KEY`) | 日, 全历史 |

核心输出 `btc_context_1h_btcusdt.csv`：把 K线 + 资金费率(8h→1h ffill) + OI + 多空比 + 主动买卖比 + FNG(日→1h) + 宏观(日→1h) 对齐到 1h 时间戳的上下文宽表，可作为 `produce_csv.py` 的补充输入列。

运行：`python get_additional_data.py`（参数：`--symbol` / `--ratio-period` / `--no-macro` / `--no-onchain` / `--no-context`），输出目录 `data_new/additional/`，并生成 `additional_data_manifest.json` 清单。

---

## 交易对排行（get_pair_rankings.py）

- 当前排行（快照）：`binance_spot_pair_ranking.csv`（3632 对）、`binance_futures_pair_ranking.csv`（742 对，附资金费率）、`coingecko_market_cap_ranking.csv`（Top100 币）
- **历史每日排名**（`--history` 模式，免费 API 无历史排名快照，用日成交额/市值重建）：
  - `spot_daily_ranking.csv` / `futures_daily_ranking.csv`：日期 × 交易对 × 成交额 × 当日排名 × 较前日排名变化
  - `spot_daily_volume_wide.csv` / `futures_daily_volume_wide.csv`：日期 × 交易对成交额矩阵
  - `coingecko_daily_marketcap_ranking.csv` / `coingecko_daily_volume_ranking.csv`：币种按日市值/成交额排名
- 用法：`python get_pair_rankings.py --history --history-days 90 --history-top 100`

---

## 运行命令

```bash
# 完整流水线（特征计算）
python produce_csv.py
# 耗时约数分钟，输出 ~700MB CSV

# 多周期合并（需先有特征文件）
python merge_csv.py

# 趋势标注（需先有 research 宽表）
cd 2_label && python label_of_feature_trend.py
```

---

## 关键代码约定

1. **无未来数据保证**：所有滚动指标使用 `.shift(1)` + `rolling(window=N, min_periods=1)`，排除当前及未来值
2. **异常容错**：每个 talib 指标计算包裹在 `try/except` 中，失败时填合理默认值（RSI=50, MACD=0 等）
3. **指标前缀**：周线 = `weekly_`，日线 = `daily_`，4h = `4h_`，1h = `1h_`
4. **背离类型编码**：`{prefix}Div_Type` — 0=无, 1=常规底, 2=常规顶, 3=隐藏底, 4=隐藏顶
5. **标签输出列**：`label_{freq}_{method}`，如 `label_4h_FTH`
6. **轻量上下文字段**：`daily_trend_bias` / `daily_filter_pass_long` / `weekly_trend_bias` / `weekly_vol_regime` 等已进入 merge 后的 context 表

---

## 核心模块速查

### calculate_indicator_v2.py 类层次

```
TechnicalIndicators (ABC)                 ← 2583行
├── calculate_all_indicators()            ← 基础指标（趋势/动量/波动率/成交量/价格行为/统计/时间特征）
├── calculate_advanced_features()         ← 高级组合特征（动量比率/波动率调整/成交量确认/烛台形态等）
├── detect_divergences()                  ← MACD柱/RSI/成交量背离（常规+隐藏）+ 综合得分
│   ├── _detect_macd_hist_divergence()
│   ├── _detect_rsi_divergence()
│   ├── _detect_volume_divergence()
│   └── _find_extrema() → 找极值点
├── calculate_ichimoku() / pivot_points() / stoch_rsi() / keltner() / donchian()
├── calculate_buy_sell_pressure() / calculate_avg_trade_size()
│
├── DailyIndicators → calculate() + calculate_daily_specific_features()
├── Hourly4HIndicators → calculate() + calculate_4h_specific_features()
├── Hourly1HIndicators → calculate() + calculate_1h_specific_features()
└── WeeklyIndicators → calculate() + calculate_weekly_specific_features()

FeatureValidator                      ← 未来数据泄露检测
└── validate_no_future_data()
    ├── _has_future_correlation()         ← 特征与未来价格相关性>0.5
    ├── _contains_future_extremes()       ← 特征极值对应未来价格极值
    └── _anomalous_before_turning_points()← 价格转折点前特征异常
```

### label_of_feature_trend.py 标注算法

当前 `label_of_feature_trend.py` 基于论文 *Optimal Trend Labeling in Financial Time Series* 实现 `FTH / CT / Oracle` 三类标签。它的定位是：

- **future direction labels（未来方向预测标签）生成模块**
- 用于未来方向预测研究与不同方向标签定义的比较
- **不是** 整个趋势交易系统全部标签的完整定义中心

| 函数 | 算法 | 参数 | 逻辑 |
|------|------|------|------|
| `label_fth()` | Fixed-Time Horizon | H (窗口), tau_mult (自适应阈值乘数) | 比较 t 与 t+H 价格 |
| `label_ct()` | Continuous Trend | omega (波动参数) | 识别连续波段的峰谷切换 |
| `label_oracle()` | Oracle DP | theta (交易费率), final_label | 动态规划最大化累计收益 |
| `cumulative_return_from_labels()` | 回测 | fee | 按标签序列模拟交易 |
| `compute_robustness_profile()` | 鲁棒性曲线 | 论文 Eq.7 | 13个ψ水平, ρ = ΔR/R / ΔACC/ACC |

---

## 相关论文

- IEEE Access 2023, Vol.11, pp.83822-83832
- *"Optimal Trend Labeling in Financial Time Series"*
- 作者: Kovačević, Merćep, Begušić, Kostanjčar

---

## 废弃模块 (wast/)

代码被重构前的实验版本，不建议使用，但保留了完整的版本演进记录：

| 文件 | 与主线的差异 |
|------|-------------|
| `feature.py` | 单时间框架，CryptoFeatureEngineer 类，无逻辑拆分 |
| `feature_v1_5.py` | 加入 `future_{period}_return` 等目标变量 |
| `feature_v2.py` | FeatureOptimizer 类，PCA/SelectKBest/特征聚类 |
| `h2day_resample.py` | 1h→日/周/月 重采样工具 |

---

## 注意事项

- `get_data.py` 末尾的调用语句已**注释**，需手动取消才可拉取新数据
- `label_of_feature_trend.py` 末尾的脚本级代码直接执行（非 `if __name__` 保护）
- 当前统一核心输出目录为 `data_new/with_features_0601/`
- `merged_4h_research.csv` 是主研究母表；`merged_4h_context.csv` 是轻量上下文表
- `merged` 表中保留了 `daily_source_timestamp` / `weekly_source_timestamp` 以便检查 higher-timeframe 对齐
- 1h 特征计算现在会保留 `quote_volume` / `trades` / `taker_buy_volume` / `taker_buy_quote_volume`，避免成交量压力类特征退化
- 所有 CSV 包含的特征数量：每小时框架约 200-300 列，周线因周期较少
