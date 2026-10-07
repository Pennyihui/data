# 评估协议设计（评价方法 + 内部评估服务）

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.2（已实现） |
| 日期 | 2026-10-07 |
| 状态 | **已实现**：`data_foundation/evaluation/`（metrics / significance / service / runner）＋ eval_service schema 门 ＋ 15 项测试全绿 |
| 前置 | `research-pools-timewall-design.md`（时间墙/三池）、`backtest-engine-design.md`（回测）、`label-system-design.md`（标签，阶段1） |
| 定位 | 研究链「监督学习 → 评价 → 反馈/OOS」的评价环节；承接三池纪律、供给 go/no-go 依据 |

---

## 0. 现状盘点（先回答"现在有没有评价方法"）

| 模块 | 现状 | 缺什么 |
|---|---|---|
| `eval_service.py` | **只有通道**：submit → evaluate → read_feedback 限 2 次 / OOS 人审。docstring 明说"评估函数由调用方注入，服务不内置 IC/FDR 算法" | 评价方法本体 = 0 |
| `backtest/execution_engine.py` Portfolio | 4 个基础数：total_return / sharpe / max_drawdown / total_cost | 缺 Calmar、换手、盈亏比等 |
| 预测层指标 | **不存在**：IC / rank-IC / ICIR / 命中率 / 分位组合收益，全代码库 0 匹配 | 全部 |
| 显著性检验 | **不存在**：无 PSR/DSR/多重检验修正 | 全部 |
| 开发池 (oof) 评价 | Agent 各算各的，无统一口径 → 实验不可比 | 统一评价方法 |

**结论：评价方法是研究链目前最大的空位**——通道已建好但里面没有算法。本设计补齐它。

---

## 1. 评价方法 = 三层指标电池 + 单一执行路径 + 多重检验修正

**一句话设计**：一个模型交三条成绩单——**预测层**（模型预测得准不准）、**交易层**（能不能变成钱）、**显著性层**（是不是试出来的运气）。三份成绩单由**同一个函数** `evaluate_candidate()` 产出，开发池自评和 valid/oos 服务评跑**同一份代码**；OOS 的 go/no-go 不看裸 Sharpe，看**经试验次数修正**的 DSR。

```
模型预测 (分数面板) ──┬─→ 预测层: IC / rank-IC / ICIR / 命中率 / 分位组合(成本后)
                     │       输入 = (预测分数, 标签) 对齐面板   ← 依赖标签体系+训练数据供给(阶段2)
模型信号 → 回测引擎 ──┴─→ 交易层: 年化/Sharpe/Calmar/回撤/换手/成本后净收益
                             输入 = 回测引擎 Portfolio 净值+成交   ← 只依赖回测引擎(已就绪)
交易层 Sharpe ─────────→ 显著性层: PSR / DSR / walk-forward 稳定性
                             输入 = (Sharpe, T, 偏度, 峰度, 试验次数 N, 各 fold 指标)
```

---

## 2. 五条设计原则

**原则1：单一代码路径。** 开发池自评与 valid/oos 服务评调用**同一个** `evaluate_candidate()`——服务只是"换执行环境"，不是"换评价逻辑"（与回测引擎"换数据源=换环境"同一条纪律）。不允许存在"本地版指标"和"服务版指标"两套实现。

**原则2：三层分开，缺一不可。** 预测层好而交易层差 = 信号无法变现（成本/容量问题）；交易层好而显著性差 = 运气。三层各答各的问题，不互相替代。

**原则3：成本后为准。** 交易层指标全部成本后（成本模型与回测 `CostModel`、标签 `CostParams` 同源）；分位组合收益必须扣成本，并附**零成本对照**——两数之差就是"信号被成本吃掉的部分"。

**原则4：多重检验修正，不是裸阈值。** 试 100 个模型总有 Sharpe 好看的。OOS go/no-go 用 **PSR/DSR**（López de Prado），阈值随**全局试验次数 N** 收紧；N 计入开发池里的每一次正式评估。

**原则5：反馈最小化 + 全指纹。** valid 返回**固定白名单**指标集（不允许挑指标看）；每次提交带 code_hash + 模型规格 + 特征集指纹 + 标签指纹 + 指标版本；指标定义变更 → `METRICS_VERSION` +1，旧记录不可变（与特征/标签版本化同构）。

---

## 3. 指标电池（`evaluation/metrics.py`）

### 3.1 预测层（模型质量；输入 = 预测分数×标签对齐面板，逐决策日截面）

| 指标 | 定义 | 参照 |
|---|---|---|
| `ic_mean` | 逐日截面 `corr(score_t, label_t)` 的均值 | Qlib benchmark |
| `icir` | `mean(IC)/std(IC)`（IC 信息比） | Qlib benchmark |
| `rank_ic_mean` | 逐日截面 Spearman（秩相关）的均值 | Qlib benchmark |
| `rank_icir` | `mean(rank-IC)/std(rank-IC)` | Qlib benchmark |
| `hit_rate` | `sign(score) == sign(label)` 的逐日截面命中率 | 二分类标签 |
| `quantile_spread` | 每日按分数 5 分位：top 组 − bottom 组的**成本后**平均收益（long-short 差） | Alphalens 分位组合 |
| `quantile_monotonicity` | 5 组分位组平均收益与组序 (1..5) 的 Spearman（应为 1） | Alphalens |
| `quantile_spread_zero_cost` | 同 quantile_spread 但成本=0（对照：差=被成本吃掉的量） | 原则3 |
| `top_hit_rate` | 多分类标签下 top 组的命中率 | quantile_10d |

> 为什么 rank-IC 与 IC 都要：rank-IC 抗离群，IC 反映线性强度——多口径并存，不预先假定哪个更有效（与特征底座 1.4 "z-score vs raw 都要有"同一立场）。

### 3.2 交易层（经济质量；输入 = 回测引擎 `Portfolio` 净值序列 + 成交流）

| 指标 | 定义 | 现状 |
|---|---|---|
| `ann_return_net` | 成本后年化收益 | Portfolio 有 total_return，需年化+成本后 |
| `ann_vol` | 年化波动 | 需补 |
| `sharpe` | `cagr/vol`（已有） | ✅ 已有 |
| `calmar` | 年化收益 / |最大回撤| | 需补 |
| `max_drawdown` | 最大回撤（已有） | ✅ 已有 |
| `annualized_turnover` | 年化换手率 | 需补（换手×费率=成本拆解，因子门槛指标） |
| `total_cost` | 总成本（已有） | ✅ 已有 |
| `win_rate` / `profit_factor` | 逐 bar 盈亏比 | 需补 |

### 3.3 显著性层（统计可信；输入 = 交易层 Sharpe + 试验元信息）

**PSR（Probabilistic Sharpe Ratio）**——单次试验下"Sharpe 超过基准 SR\* 的概率"（Bailey & López de Prado, JPM 2014）：

```
PSR(SR*) = Φ( (SR̂ − SR*)·√(T−1) / √(1 − γ3·SR̂ + (γ4−1)/4·SR̂²) )
    SR̂   = 样本 Sharpe（年化）     T = 观测数（bar 数）
    γ3   = 收益偏度               γ4 = 收益峰度
    Φ    = 标准正态 CDF
```

**DSR（Deflated Sharpe Ratio）**——把基准抬高到"N 次独立试验下**期望出现的最大** Sharpe"，再算 PSR：

```
SR*₀ = √(V[SR]) · [ (1−γ)·Φ⁻¹(1 − 1/N) + γ·Φ⁻¹(1 − 1/(N·e)) ]
    γ = 0.5772 (Euler–Mascheroni 常数)    V[SR] ≈ 1/(T−1)（正态近似）
DSR = PSR(SR*₀)
```

| 量 | 来源 |
|---|---|
| N（试验次数） | **全局试验计数器** = oof 正式评估次数 + valid 提交次数 + oos 提交次数（`oos_ledger.eval_count` 已有，oof/valid 部分本设计补上） |
| T、γ3、γ4 | 回测收益序列 |

**go/no-go 规则（默认，OOS 人审依据）**：

```
通过 ⇔  PSR(SR*=0) ≥ 0.95  ∧  DSR ≥ 0.95  ∧  ann_return_net > 0
```

**walk-forward 稳定性**：各 fold 的 `sharpe`/`ic_mean` 序列 → 返回 `fold_sharpe_std`、`fold_ic_std`、`worst_fold_sharpe`。防止"只有一个 fold 好"的单点运气（fold 由标签体系 §7 的切分器产出）。

---

## 4. 内部评估服务流程（valid/oos）

```
Agent 提交: submit(pool, run_id, payload)
    payload = { code_hash, model_spec, fitted_artifact_ref,
                feature_fingerprint, label_spec, params, metrics_version }

内部服务 (服务端有池内数据):
    1. 校验: payload 指纹齐全; metrics_version 与当前一致; code_hash 可解析
    2. 载入池内特征 (服务端缓存, PIT 面板)         ← Agent 拿不到
    3. 载入/服务端重算标签 (标签体系 store, 池保护)  ← Agent 拿不到
    4. 应用模型 (artifact 或 code_hash 重训) → 预测分数面板
    5. 预测 → 信号 → 回测引擎 (同一引擎, 同一成本模型)
    6. evaluate_candidate() → 三层指标 + 版本 + 指纹
    7. evaluate(evaluation_id, metrics) 记账
        valid: read_feedback 限 2 次 (已有), 返回固定白名单指标
        oos:   写 oos_ledger, Agent 永不返回, 人审 go/no-go
```

**关键不变式**：
1. valid/oos 的评估**只能由服务执行**（Agent 无数据，物理上无法自评——已有通道保证）。
2. 服务端第 2~6 步与开发池自评**逐位同一代码**（原则1）——验收测试直接对比两端输出。
3. 返回的 metrics dict 经 **schema 校验**：白名单字段 + 必需字段 + 数值域检查（|IC|≤1、sharpe 有限、指标版本匹配）——防"注入指标"绕过。

---

## 5. 开发池（oof）的评价方法

- Agent 用**同一个** `evaluate_candidate()` 自评（MCP 工具 `evaluate_candidate`，仅 oof）。
- 每次正式评估 → 写**实验账本**（experiment ledger，阶段2 的实验追踪原型）→ 计入全局试验次数 N。**开发池试验也算 N**——不然 Agent 可以在 oof 试 1000 次挑个好看的再去 valid 碰运气，DSR 的 N 就失真了（原则4 的完整性）。
- oof 结果只进实验账本，**不写** valid/oos 反馈通道（通道语义不变）。
- oof 同样跑 PSR/DSR——用池内 N 的 DSR 决定"值不值得提交 valid"，而不是凭肉眼 Sharpe。

---

## 6. 与既有体系的关系

| | 复用 / 改动 |
|---|---|
| `eval_service.py` | 通道**不动**（谁能评/谁能看/看几次）；新增 `validate_metrics()` schema 门 + 服务端 evaluate_candidate 编排 |
| `oos_ledger.py` | `eval_count` 供给 OOS 部分 N；人审 go/no-go 依据改为三层指标 |
| 回测引擎 | 交易层指标直接从 `Portfolio` 扩展；引擎本身不动 |
| `backtest.CostModel` / 标签 `CostParams` | 成本同源——三层所有"成本"是一个数 |
| 标签体系 | 供给标签面板（预测层输入）与 fold 掩码（walk-forward 稳定性） |
| 特征缓存指纹 | 提交 payload 里的 `feature_fingerprint` 复用特征缓存指纹约定 |
| 阶段2 训练数据供给 | 服务端第 4 步的"预测分数×标签对齐"由它产出（依赖，见 §10） |

---

## 7. 不做什么

- **不做**因子层单因子检验（FDR / Newey-West 单因子显著性筛选——属因子线，用户已定案不关心）。
- **不做**超参自动搜索 / 贝叶斯优化 / 算力调度（阶段2+ 的实验追踪扩展）。
- **不做**指标美化（净值曲线图、报告生成——只做数值电池）。
- **不做**实盘下单 / 资金分配（既有边界）。

---

## 8. 决策记录

| # | 决策 | 默认 | 理由 |
|---|---|---|---|
| E1 | 评价分层 | 预测层 + 交易层 + 显著性层，三层都要 | 各答各的问题：准不准 / 变不变现 / 是不是运气 |
| E2 | 代码路径 | 单一 `evaluate_candidate()`，dev 与 valid/oos 同一函数 | 与"回测实盘同路径"同一条纪律 |
| E3 | 多重检验 | DSR 阈值 = E[max SR_N]，N = 全局试验计数（**含 oof**） | 开发池过拟合也会污染下游；N 失真则 DSR 失真 |
| E4 | valid 反馈 | 固定白名单指标集，限 2 次（已有） | 不允许挑指标看 |
| E5 | go/no-go | PSR≥0.95 ∧ DSR≥0.95 ∧ 成本后年化>0 | 数值可调，但默认固定，防"为过闸改闸" |
| E6 | 分位组合 | 必须成本后 + 零成本对照 | 成本敏感性是换手门槛的直接证据 |
| E7 | 指标版本化 | `METRICS_VERSION` + schema 校验 | 指标口径改了，旧记录仍可解释 |
| E8 | 提交载荷 | code_hash + 模型规格 + 全指纹 | 服务评可复现、可审计 |

---

## 9. 实现顺序

1. `evaluation/metrics.py` **交易层** + `Portfolio` 指标扩展（只依赖回测引擎，**可立即实现**）
2. `evaluation/significance.py` PSR/DSR（交易层输出 + 试验计数输入）
3. `evaluation/metrics.py` **预测层**（依赖标签体系阶段1 落定后：标签面板对齐）
4. `validate_metrics()` schema 门 + `eval_service` 集成（服务端 evaluate_candidate 编排，阶段2 训练供给就绪后接上）
5. 实验账本 + 全局试验计数器（oof 计入 N）
6. MCP `evaluate_candidate`（oof）
7. 验收（§10）

---

## 10. 验收标准

- **手算对照**：构造 5 个决策日截面，IC / rank-IC / hit_rate 与手工计算逐位一致；PSR/DSR 用文献数值例子对照（论文 Table 样例）。
- **同输入同输出**：dev 自评与"服务端路径"对同一模型逐位一致（原则1 的测试）。
- **DSR 单调性**：固定 Sharpe，N 从 1 → 100 → 10000，DSR 单调下降（构造测试，防公式抄反）。
- **成本恒不等式**：quantile_spread ≤ quantile_spread_zero_cost（构造高换手信号验证）。
- **schema 门**：注入非法指标（缺字段/|IC|>1/旧版本）被 `validate_metrics` 拒绝。
- **池纪律不回归**：valid 限 2 次、oos Agent 拒读、`assert_can_read_data` 全链沿用。
- **全量回归**：现有 445 项测试不回归。

---

## 11. 参考资料

- Qlib 基准指标电池（IC / ICIR / Rank IC / 年化 / 信息比 / 成本后超额）：[Qlib benchmarks README](https://raw.githubusercontent.com/microsoft/qlib/main/examples/benchmarks/README.md)、[DeepWiki: Model Evaluation and Benchmarking](https://deepwiki.com/microsoft/qlib/5.4-model-evaluation-and-benchmarking)
- DSR/PSR 原始论文（Bailey & López de Prado, *The Deflated Sharpe Ratio*, JPM 2014）：[tradingstrategy.ai 词条](https://tradingstrategy.ai/glossary/probabilistic-sharpe-ratio)
- DSR 参考实现：[zostaff/ai-quant-researcher deflated_sharpe.py](https://github.com/zostaff/ai-quant-researcher/blob/main/ai_quant_lab/validation/deflated_sharpe.py)、[endgame-ml utils.sharpe](https://endgame-ml.readthedocs.io/en/latest/_modules/endgame/utils/sharpe.html)
- Alphalens 分位组合/IC/换手评价：[DeepWiki: Comparing Predictive Factors](https://deepwiki.com/quantopian/alphalens/4.3-comparing-predictive-factors)
