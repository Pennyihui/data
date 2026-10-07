# 监督学习协议设计（训练数据供给 → 模型注册 → 信号适配 → 实验追踪）

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.2（已实现） |
| 日期 | 2026-10-07 |
| 状态 | **已实现**：`data_foundation/training/`（dataset / models / registry / signal_adapter / walkforward / experiments）＋ 17 项测试 ＋ 端到端链路测试全绿 |
| 前置 | `feature-foundation-design.md`（特征/缓存/指纹）、`label-system-design.md`（标签，阶段1）、`evaluation-protocol-design.md`（评价）、`backtest-engine-design.md`（回测） |
| 定位 | 研究链第三环：目标 → 标签 → **监督学习** → 强化学习；把标签变成绩效证据的一层 |

---

## 0. 一句话设计

**监督学习协议 = 训练数据供给（X⋈y 全链唯一对齐处） + 模型注册表（可复现第一类对象） + 实验追踪（试错账本 + 试验计数 N） + ModelSignal 适配器（模型分数 → 回测信号，含仓位映射） + walk-forward 框架。**

训练**只发生在 oof**；valid/oos 只发生"服务端应用模型 + 评估"——三池纪律延伸进模型层。

---

## 1. 要解决的问题

1. **训练数据供给是泄漏重灾区**：X 与 y 的时点对齐（`X.data_available_at ≤ t`，`y` 的窗口起点 > t）现在没有任何代码做——每个实验手写对齐，静默泄漏（特征底座 `assert_no_leakage` 只保证 X 自身 PIT，**看不见对齐错误**）。
2. **模型不是第一类对象**：没有注册表，训练产物散落在脚本里 → 不可复现、不可审计、服务端无从"应用"。
3. **有状态预处理漂移**：标准器/PCA 在训练集拟合；若服务端或回测时用全样本重拟合，特征数值静默漂移而不报错（既定分界线：参数来自训练集 → 随模型携带，防训练/服务漂移）。
4. **实验追踪缺失**：试错需要账本（超参/指纹/三层指标），DSR 的试验计数 N 需要它（评价协议 E3）。
5. **仓位映射缺口**：回测文档 §9 明确"权重优化属模型协议"——分数加权仓位是留给本层的缺口。
6. **walk-forward 形态**：滚动重训的正确做法（每 fold 只用 train 训练、不滚动学习 test、不用未来数据选模型）没有载体。

---

## 2. 六条设计原则

**原则1：X 与 y 只在训练数据供给一处对齐，对齐处带双断言。** 其它任何模块都拿不到"已对齐样本"——要么原始 X，要么原始 y。

**原则2：有状态预处理随模型携带。** 标准化/降维/填充的**参数**在训练 fold 拟合、序列化进模型产物；`predict` 时用保存参数，**禁止重拟合**（漂移防线，测试强制）。

**原则3：模型是第一类对象。** 注册表 + 指纹 + 不可变版本，与特征缓存、标签存储同构。

**原则4：模型与仓位映射解耦。** 模型只输出分数面板（统一语义：越大越看好）；仓位映射（top-N 等权 / rank-linear 多空 / 分数加权）是独立适配层；**回测引擎不动**。

**原则5：训练只发生在 oof。** valid/oos 服务端只"应用"模型（不重训）；重训复现只允许在 oof 数据上做。

**原则6：每次训练/评估都留痕。** 实验账本 append-only；全局试验计数 N 供给 DSR（含 oof 试验，评价协议 E3）。

---

## 3. 架构

```
标签体系 (y 面板 + fold 掩码, 阶段1) ──┐
特征缓存 (X 面板, PIT+指纹) ───────────┼→ [训练数据供给 dataset.py] → SampleSet (双断言+指纹+落盘缓存)
                                       │
[模型家族 models.py: linear / gbdt / nn] ←── fit (只在 oof, walk-forward folds)
        ↓ 训练产物 = 权重 + 有状态预处理参数
[模型注册表 registry.py: 指纹+版本+不可变]
        ↓ predict (oof 自测 / valid-oos 服务端应用)
[预测分数面板 (asset, t)] → [signal_adapter.py] → PanelSignal(已有) → [仓位映射策略] → 回测引擎(不动)
        ↓
[实验追踪 experiments.py: 超参 + 三层指标 + 试验计数 N] → DSR (评价协议)
```

---

## 4. 训练数据供给（`training/dataset.py`）

**输入**：特征名列表 + 标签规格 + fold（标签体系 `splitter` 产出）+ `PoolScope("oof")`。

**对齐规则（全链唯一的 X⋈y 对齐处）**：

| 量 | 规则 |
|---|---|
| 决策时刻 t | 标签决策格（1D） |
| X | 特征面板在 (asset, t) 的值；**断言** `data_available_at(asset, t) ≤ t` |
| y | 标签面板在 (asset, t) 的值；**断言** `label_available_at > t`（标签是未来函数）且 `≤ fold.train_end` |
| NaN 处理 | 特征含 NaN 的样本**默认丢弃**（供给层不做填充——填充参数属有状态预处理，随模型携带，原则2） |
| 宇宙 | 仅保留 t 日 PIT 宇宙内的资产（标签体系已保证，此处复核） |

**产出**：

```python
@dataclass(frozen=True)
class SampleSet:
    X: pd.DataFrame            # (asset, t) × 特征
    y: pd.Series               # (asset, t) → 标签值
    meta: SampleMeta           # 特征指纹 + 标签指纹 + fold + 池 + 行数
    fingerprint: str           # 落盘缓存键 (与特征缓存键同构)
```

- 落盘缓存 `data/samples/`：同指纹命中直接读盘（与特征缓存同构）。
- **双断言测试**：构造一个 `data_available_at` 越过决策时刻的特征面板 → 供给层必须抛错；构造一个标签窗口起点 ≤ t 的标签 → 必须抛错。这两条断言是"对齐只在此处发生"的执行保证。

---

## 5. 模型家族与统一接口（`training/models.py`）

**统一接口（sklearn 风格薄包装，三个家族可插拔）**：

```python
class BaseModel(Protocol):
    def fit(self, X: pd.DataFrame, y: pd.Series) -> None
    def predict(self, X: pd.DataFrame) -> np.ndarray   # 分数, 越大越看好
    def state(self) -> bytes                            # 权重+预处理参数 (序列化)
    def load_state(self, b: bytes) -> None
```

**分数语义统一（决策 M7）**：regression 家族输出预测值；logistic 输出正类概率；gbdt 回归输出预测值/分类输出正类概率——全部"越大越看好"，仓位映射层不关心家族。

| 家族 | 实现 | 适用标签 |
|---|---|---|
| `linear` | sklearn Ridge / LogisticRegression | ret_10d / vol_scaled_10d / sign_10d |
| `gbdt` | LightGBM / XGBoost（已装） | 全部连续+分类 |
| `nn` | torch 简单 MLP（已装，留位） | 全部 |
| `baseline` | 零模型 / 单特征线性（对照用） | — |

**有状态预处理随模型**：`StandardScaler` / `PCA` / 简单 imputer 在 `fit` 内于**训练 fold** 拟合，序列化进 `state()`；`predict` 只用保存参数。**测试强制**：对同一样本，`predict` 与"重拟合预处理后的 predict"结果不同时，说明有人重拟合——此路径被禁止（原则2）。

**决策 M1**：首期家族 = `linear` + `gbdt` + `baseline`；`nn` 留位（阶段3 RL 时一并启用）。

---

## 6. 模型注册表（`training/registry.py`）

```python
@dataclass(frozen=True)
class ModelArtifact:
    spec: ModelSpec            # 家族+超参+seed+特征名+标签规格
    state_hash: str            # 权重+预处理参数的哈希
    feature_fingerprint: str   # 训练时特征集指纹
    label_fingerprint: str     # 标签指纹 (标签体系 §8)
    train_fold: str            # (train_start, train_end) —— 训练只用了这段数据
    train_data_fingerprint: str# 训练数据指纹 (manifest row_count/coverage/certified_at)
    metrics_version: str       # 训练期自评用的指标版本
    model_hash: str            # 以上全部 + state 的哈希 = 唯一身份
```

- 存储：`data/models/<name>__v<N>__<hash8>.pkl`（joblib）+ `meta.json`；**不可变版本**（与特征 `registry_versioning`、标签存储同构）。
- API：`register_model(artifact)` / `load_model(name, version)` / `list_models()` / `describe_model(name)`。
- **服务端应用**：valid/oos 评估时服务端 `load_model` → 校验 `feature_fingerprint` 与池内特征缓存一致 → `predict`。指纹不符 = 拒绝评估（防"训练用一套特征、评估用另一套"的静默错配）。

---

## 7. ModelSignal 适配器 + 仓位映射（`training/signal_adapter.py`）

**第一步：分数面板 → `PanelSignal`（复用已有）**。`PanelSignal` 按 `bar_time` 精确查表、不做前视填充——防泄漏第二道闸（signals.py 已有语义）直接复用，模型分数面板就是一个 Series。

**第二步：仓位映射（模型协议层新增，回测引擎不动）**：

```python
class ScoreWeightedStrategy(Strategy):     # 消费 PanelSignal, 输出 OrderIntent
    mode: "top_n" | "rank_linear"         # 决策 M2 首版两种
    top_n: int                            # top_n 模式取前 N
    long_short: bool                      # False=纯多; True=多空零成本
    rebalance_every: int                  # 调仓周期 (bar 数)
```

| mode | 权重公式 |
|---|---|
| `top_n` | 分数前 N 等权 `1/N`（等价 TopNStrategy，供**对照**） |
| `rank_linear` | 截面排名中心化后线性权重：`w ∝ (rank − 中位 rank)`，多空对称、零净敞口（纯多时只取正半） |

- 仓位经 `RiskEngine`（已有：max_weight/max_gross）修正后成交——复用回测引擎约束，不在适配层重复实现。
- 分数为 NaN 的资产不持仓（PanelSignal 已保证 NaN 不填充）。
- **决策 M2**：首版 = `top_n` 等权（对照）+ `rank_linear` 多空；`score_proportional`（分数幅度加权）留扩展位。

---

## 8. walk-forward 训练框架（`training/walkforward.py`）

- **消费标签体系 splitter 的 folds**（purge+embargo 已内置，标签体系 §7）：
  - 每个 fold：`fit(train fold 样本)` → `predict(test fold)` → 记录分数面板；
  - 汇总 = 各 fold 分数拼接 → 一次喂给回测 + 三层指标；跨 fold 稳定性（评价协议 §3.3 `fold_sharpe_std` 等）由评价层消费。
- **纪律（代码级）**：
  1. 模型在每个 test fold 只能用对应 train fold 训练（fold 结构物理隔离——作弊"偷看 test"的模型拿不到 test 样本，由供给层双断言保证）；
  2. 不做"滚动学习 test"（用 test 数据在线更新 = 泄漏）；
  3. 超参选择只允许在 train fold 内部做内层切分（同样 purge+embargo）。
- **决策 M3**：首版**不做**自动超参搜索，超参由实验者显式指定（搜索/算力调度属阶段2+）。

---

## 9. 实验追踪（`training/experiments.py`）

```python
@dataclass(frozen=True)
class ExperimentRecord:
    run_id: str                 # 全局唯一
    code_hash: str              # 实验代码哈希 (复现锚)
    model_spec: dict            # 家族+超参+seed
    feature_fingerprint / label_fingerprint
    fold_plan: str              # walk-forward 计划 (与 splitter 参数一致)
    metrics: dict               # 三层指标, 带 METRICS_VERSION
    pool: str                   # oof 自评 / valid / oos
    created_at: str
```

- 存储：`data/experiments/experiments.jsonl`（append-only，同 `oos_ledger` 风格）。
- **全局试验计数 N** = 去重 `run_id` 数（含 oof 正式评估）→ 供给 DSR（评价协议 E3）。
- oof 记录由 Agent 本地写；valid/oos 记录由服务端写并关联 `evaluation_id`（eval_service）。

---

## 10. 三池纪律集成

| 池 | 训练 | 预测/评估 |
|---|---|---|
| oof | ✅ Agent 全链可跑（fit + 自评 + 回测） | ✅ 本地 `evaluate_candidate` |
| valid | ❌（服务端**只应用**，不重训） | 服务端：载特征 → 算标签 → `load_model` → predict → 回测 → 三层指标 → 限 2 次返回 |
| oos | ❌ 同上 | 服务端同上 → 写 oos_ledger → Agent 永不返回，人审 go/no-go（PSR/DSR） |

- 提交 payload（评价协议 §4 已定义）扩展为：`{code_hash, model_artifact_ref（或完整 spec）, feature_fingerprint, label_spec, params, metrics_version}`。
- 服务端校验 `feature_fingerprint` 与池内缓存一致；`model_artifact` 的 `train_fold` 必须 ⊆ oof 范围（**训练只发生在 oof 的执行保证**）。
- 可选复现验证：服务端用 code_hash 在 **oof 数据**上重训，比对 `state_hash` 一致（审计用，不影响评估）。

---

## 11. 与既有体系的关系

| | 复用 / 改动 |
|---|---|
| 特征缓存指纹 | `feature_fingerprint` 直接复用（features/cache.py） |
| 标签体系 | y 面板、fold 掩码、标签指纹（阶段1） |
| 回测引擎 | **不动**；`PanelSignal`、`RiskEngine`、`CostModel` 原样复用 |
| 评价协议 | 三层指标 + `evaluate_candidate` + DSR 的 N（本层供给） |
| `eval_service` / `oos_ledger` | 通道不动；服务端编排新增"应用模型"步骤 |
| 三池纪律 | `assert_can_read_data` 全链沿用；训练只在 oof（原则5） |

**本层不重新实现**：取数、特征、PIT、切分、成本、指标、池纪律全部用现成的。

---

## 12. 不做什么

- **不做**超参自动搜索 / 贝叶斯优化 / 算力调度（阶段2+）。
- **不做**特征选择 / 因子检验（因子线，用户定案）。
- **不做**模型集成 / 多策略资金分配（既有边界）。
- **不做** RL（阶段3，Env 复用回测引擎）。
- **不做**实盘（LiveEngine 留位）。

---

## 13. 决策记录

| # | 决策 | 默认 | 理由 |
|---|---|---|---|
| M1 | 模型家族 | linear + gbdt + baseline 先行；nn 留位 | 最小闭环；nn 与 RL 阶段一并启用 |
| M2 | 仓位映射 | top_n 等权（对照）+ rank_linear 多空 | 回测文档 §9 的缺口；分数幅度加权留扩展 |
| M3 | 超参 | 手动指定，不做自动搜索 | 搜索属阶段2+ 算力调度；先保证可复现 |
| M4 | 训练发生地 | 只在 oof；valid/oos 只应用 | 三池纪律延伸（原则5） |
| M5 | 有状态预处理 | 随模型序列化；predict 禁重拟合 | 防训练/服务漂移（原则2，测试强制） |
| M6 | 样本 NaN | 供给层丢弃；填充留模型协议（随模型） | 填充参数属有状态预处理 |
| M7 | 分数语义 | 统一"越大越看好"（概率/预测值归一语义） | 仓位映射层与家族解耦 |

---

## 14. 实现顺序

1. `training/dataset.py`——训练数据供给 + 双断言测试
2. `training/models.py`——统一接口 + linear/gbdt/baseline + 预处理随模型（漂移测试）
3. `training/registry.py`——模型注册表（指纹/版本/不可变）
4. `training/signal_adapter.py`——ModelSignal + `ScoreWeightedStrategy`（top_n / rank_linear）
5. `training/walkforward.py` + `training/experiments.py`（试验计数 N）
6. `eval_service` 服务端编排接线（load_model → predict → 回测 → 三层指标）
7. 验收（§15）

---

## 15. 验收标准

- **双断言**：X 的 `data_available_at` 越过决策时刻、标签窗口起点 ≤ t 的样本，供给层都抛错（构造测试）。
- **预处理漂移**：`predict` 用保存参数；"重拟合后 predict"路径被测试禁止（原则2）。
- **可复现**：同 seed + 同指纹 → 同 `model_hash`；同输入 → 逐位相同预测。
- **同输入同输出**：服务端"应用模型"与 oof 自评对同一模型逐位一致（原则1/5）。
- **walk-forward 隔离**：构造"偷看 test"的作弊模型，fold 结构使其物理拿不到 test 样本。
- **端到端**：`linear` 模型 on `ret_10d` → 回测净值曲线与手工核对（小样本）。
- **池纪律**：训练代码在 valid/oos scope 内运行即抛错（`assert_can_read_data`）。
- **全量回归**：现有 445 项测试不回归。
