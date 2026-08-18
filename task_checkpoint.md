# task_checkpoint

## 1. 已完成功能 & 代码改动（附改动文件路径）

### 已完成的 pipeline 改造
1. **1h 原始字段保留修复**
   - 修复此前 1h 仅保留 OHLCV、导致成交量压力类特征退化的问题
   - 现在 1h 特征计算会保留：
     - `quote_volume`
     - `trades`
     - `taker_buy_volume`
     - `taker_buy_quote_volume`
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/produce_csv.py`

2. **特征 schema / manifest 输出**
   - 新增 metadata 输出：
     - `feature_schema.csv`
     - `feature_schema.json`
     - `dataset_manifest.json`
   - 增加特征分类辅助逻辑
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/produce_csv.py`

3. **K线实体方向语义修复**
   - 新增：
     - `Signed_Body`
     - `Signed_Body_Pct`
   - 修复 `weekly_Big_Red_Candle` 以前基于绝对 body 百分比、几乎无效的问题
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/calculate_indicator_v2.py`

4. **重复 detect_divergences 定义清理**
   - 删除重复 `detect_divergences()` 实现，只保留一个有效入口
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/calculate_indicator_v2.py`

5. **轻量级 daily / weekly context 特征新增**
   - 新增 daily：
     - `daily_trend_bias`
     - `daily_trend_strength`
     - `daily_filter_pass_long`
     - `daily_filter_pass_short`
   - 新增 weekly：
     - `weekly_trend_bias`
     - `weekly_trend_strength`
     - `weekly_vol_regime`
     - `weekly_above_long_ma`
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/calculate_indicator_v2.py`

6. **merge 层升级为双产物**
   - 统一合并输出目录到：
     - `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/`
   - 生成：
     - `merged_4h_research.csv`
     - `merged_4h_context.csv`
   - 新增 lineage 字段：
     - `daily_source_timestamp`
     - `weekly_source_timestamp`
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/merge_csv.py`

7. **context 宽表列名清理**
   - 将双前缀列名清理为自然命名：
     - `daily_trend_bias`
     - `daily_trend_strength`
     - `daily_filter_pass_long`
     - `daily_filter_pass_short`
     - `weekly_trend_bias`
     - `weekly_trend_strength`
     - `weekly_vol_regime`
     - `weekly_above_long_ma`
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/merge_csv.py`

8. **标签流水线重连到新 merged research 表**
   - `label_of_feature_trend.py` 现在读取：
     - `merged_4h_research.csv`
   - 输出统一到：
     - `with_features_0601/`
   - 新增：
     - `label_config.json`
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/2_label/label_of_feature_trend.py`

9. **项目文档 CLAUDE.md 更新**
   - 已把当前真实数据流、输出路径、merged 双产物、标签模块定位写入文档
   - 改动文件：
     - `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/CLAUDE.md`

### 已验证生成的核心产物目录
- `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/`

其中包括：
- `B_1h_with_features.csv`
- `B_4h_with_features.csv`
- `B_daily_with_features.csv`
- `B_weekly_with_features.csv`
- `feature_columns.csv`
- `feature_schema.csv`
- `feature_schema.json`
- `dataset_manifest.json`
- `merged_4h_research.csv`
- `merged_4h_context.csv`
- `B_multilabeled_of_feature_trend_v2.csv`
- `label_config.json`

---

## 2. 当前卡点、未解决 BUG、技术选型结论

### 当前卡点 / 未解决问题
1. **标签回测数值异常夸张**
   - `2_label/label_of_feature_trend.py` 能跑通，但打印出的累计收益与鲁棒性相关收益量级极大，不符合直觉
   - 高概率需要单独审计：
     - `cumulative_return_from_labels()` 的收益定义
     - FTH / CT / Oracle 标签与收益对齐逻辑
     - 复利计算方式是否过度放大
   - 当前结论：
     - 标签脚本“功能可运行”
     - 但“回测数值可信度”还不能直接放心用于研究结论

2. **FeatureValidator 目前几乎全量报警**
   - `validate_no_future_data()` 在真实数据上把几乎所有列都判为 suspicious
   - 当前更像启发式告警器，而不是严格可依赖的 leakage 审核器
   - 需要后续单独重构或弱化其结论权重

3. **研究表仍然很宽**
   - `merged_4h_research.csv` 列数很多，后续使用时需要按层分组，否则容易混乱
   - 这不是 bug，但会影响后续建模体验

### 技术选型结论（已明确）
1. **交易系统定位**
   - 这是一个**趋势交易系统**，不是单纯涨跌分类系统

2. **多时间框架层级**
   - `weekly` = 长期宏观层 / 场景层
   - `daily` = 一重方向 / 路况层
   - `4h` = 二重执行 / 交易层

3. **当前标签模块定位**
   - `FTH / CT / Oracle` 属于：
     - **Direction Label Family / future direction labels**
   - 它们用于未来方向预测研究
   - 它们不是整套趋势交易系统全部标签的总定义

4. **through / filter 双向逼近是后续核心方向**
   - 方向预测不只做“最可能发生什么”
   - 还要做“最不可能发生什么、因此应该过滤什么”
   - 当前代码尚未定义 pass/filter 标签族，只是新增了轻量 context 特征作为后续基础

5. **当前统一核心输出目录**
   - 所有主产物以：
     - `/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/`
     为准

6. **运行环境结论**
   - 后续应使用 conda 环境：
     - `pre_env_pe`
   - 用户明确说明这是正确环境

---

## 3. 下一步开发清单、约束、注意事项

### 下一步开发清单（推荐顺序）
1. **单独审计 `label_of_feature_trend.py` 的收益与标签对齐逻辑**
   - 重点检查：
     - `cumulative_return_from_labels()`
     - CT 标签与价格序列截取方式
     - Oracle/FTH 标签收益计算是否合理
     - 自测打印结果是否存在逻辑放大

2. **设计“标签体系蓝图”**
   - 按三层系统拆分：
     - `weekly`: 宏观状态 / regime / 状态转移标签
     - `daily`: 路况 / 方向 / pass-filter 标签
     - `4h`: 执行结构 / 方向 / pass-filter / meta-label 标签
   - 当前已有标签模块仅归入 Direction Label Family

3. **定义 pass/filter 标签族**
   - 未来建议优先从 daily / 4h 开始：
     - `daily_pass_long`
     - `daily_filter_long`
     - `daily_pass_short`
     - `daily_filter_short`
     - `4h_pass_long`
     - `4h_filter_long`
     - `4h_pass_short`
     - `4h_filter_short`

4. **补 weekly macro/state label 方案**
   - 周期 / 流动性 / 场景识别
   - 先定义状态标签，再考虑监督预测标签

5. **视需要重构 FeatureValidator**
   - 不建议在当前版本直接把它当作最终 future leakage 结论来源

### 约束
1. **不要把未来规划写成已实现事实**
   - 当前已实现的是：
     - 基础数据底座
     - merged research/context
     - direction labels(FTH/CT/Oracle)
   - 尚未实现的是：
     - state labels
     - pass/filter labels
     - execution/meta labels

2. **保持统一输出目录**
   - 继续以 `with_features_0601` 为主，不要再让 produce / merge / label 分裂到多个目录

3. **尽量保留广谱指标，不要过早大删特征**
   - 用户明确要求：
     - 现在先不删除大量泛化但不一定有用的指标
     - 因为后续还需要评价

4. **当前标签模块基于论文，继续保留**
   - 它不是错的，只是它的职责边界要清楚

### 注意事项
1. **当前会话上下文非常满（95%）**
   - 容易触发 compact / 丢失早期消息
   - 下次建议基于本 checkpoint 继续

2. **当前 project 不是 git repo**
   - 之前 subagent/worktree 模式不可用就是因为：
     - 非 git repo
     - 无 WorktreeCreate hooks
   - 后续不要默认依赖 git/worktree 流程

3. **运行时环境**
   - 正确环境是：
     - `conda activate pre_env_pe`
   - 不需要每次重复 `source` 说明，直接按该环境执行即可

---

## 4. 已阅读过的核心源码文件清单

本轮已重点阅读/修改/分析过的核心文件：

1. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/produce_csv.py`
2. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/calculate_indicator_v2.py`
3. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/merge_csv.py`
4. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/2_label/label_of_feature_trend.py`
5. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/2_label/label.txt`
6. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/CLAUDE.md`
7. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/docs/superpowers/specs/2026-06-01-mtf-pipeline-upgrade-design.md`
8. `/LocalSSD/p9056/TestTools_ANALY/agent_team/Data_pipeline/docs/superpowers/plans/2026-06-01-mtf-pipeline-upgrade.md`

---

## 附：当前最重要的一句话定位

当前项目已经完成**多时间框架趋势交易系统的数据底座升级**：
- `merged_4h_research.csv` = 主研究母表
- `merged_4h_context.csv` = 轻量上下文表
- `label_of_feature_trend.py` = 基于论文的 future direction label generator

下一阶段重点不是再修基础 merge，而是：
**审计标签收益逻辑 + 设计完整标签体系（direction / state / pass-filter / execution-meta）**。
