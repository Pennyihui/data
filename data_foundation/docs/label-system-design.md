# 标签体系设计（目标 → 标签 → 样本切分）

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.2（已实现） |
| 日期 | 2026-10-07 |
| 状态 | **已实现**：`data_foundation/labels/`（objectives / definitions / store / splitter）＋ 18 项测试全绿（`_test_labels.py` 12 + `_test_label_isolation.py` 6） |
| 前置 | `crypto-data-foundation-research.md`（数据底座）、`research-pools-timewall-design.md`（时间墙）、`feature-foundation-design.md`（特征底座）、`backtest-engine-design.md`（回测引擎） |
| 定位 | 特征底座之上、监督学习之下；研究链「目标 → 标签 → 监督学习 → 强化学习」的第二环 |

---

## 0. 一句话设计

**标签体系 = 目标注册表 + 标签库 + 标签存储 + 样本切分器。**

标签是**未来函数**：t 时刻的标签用 t 之后的价格定义。因此它被**物理隔离**在特征体系之外——隔离靠"命名空间上引用不到 + 存储上读不到"，不靠纪律。监督学习从这里取 y，特征底座只出 X，两者只在阶段 2 的「训练数据供给」一处对齐，且对齐处带断言。

---

## 1. 要解决的问题

1. **目标-标签锚点缺失**：现在"预测未来 10 天走势"没有第一类对象。每个实验各自手写标签 → 口径不一、不可比、不可复现。
2. **标签泄漏（结构性风险 #1）**：现有 `assert_no_leakage` 只查特征自身（fields.py:588），**看不见"特征引用了未来收益"**。若未来收益以字段形式混进特征库，IC 会虚高而不报错——与刚堵掉的 `next_open` 泄漏（提交 6c0dd44d）同源：都是"未来量出现在不该出现的地方"。
3. **切分泄漏（结构性风险 #2）**：10 天 horizon 下，相邻 train/test 的**标签窗口**重叠。不做 purge + embargo 的交叉验证会把验证集信息漏进训练集，所有 IC/PnL 虚高。
4. **成本幻觉**：用毛收益做标签，模型学的是"扣费后不存在的收益"。回测引擎成本模型（`CostModel`：taker 0.05%、滑点 5bps，execution_engine.py:25）已有，标签必须同源扣费。
5. **旧标签模块的继承关系**：`Data_pipeline/2_label/label_of_feature_trend.py` 的 FTH/CT/Oracle（论文 *Optimal Trend Labeling in Financial Time Series*, Kovačević et al., IEEE Access 2023）不是 PIT/成本感知的，**只作方法参照**，不在本层复用。

---

## 2. 五条设计原则

### 原则1：标签是未来函数，隔离是结构性的

特征表达式**物理上引用不到**标签名（DSL 字段白名单只由 `features/fields.py` 生成），标签**物理上**写不进特征面板（不同存储目录，特征引擎配置里没有标签路径）。继承回测引擎的教训："写『某字段不可见』的注释时，必须同时写一个断言它不可见的测试。"

### 原则2：同一目标多标签；标签随目标、成本参数、数据指纹一起版本化

"预测未来 10 天走势"是一个目标，`ret_10d / sign_10d / quantile_10d / excess_10d / vol_scaled_10d` 是它的多个标签。任一规格（口径/成本/数据指纹）变化 → 新版本，旧版本不可变（与特征 `registry_versioning` 同构）。

### 原则3：标签必须「可达」——收益口径与回测成交时点一致，并扣双边成本

回测引擎 §5 约定：信号在 t 收盘产生，成交在**下一根 bar 开盘**。标签收益必须用同一个口径定义（§6），否则"标签说能赚 1%"和"回测实际拿到 1%"之间会有一条无法解释的缝。

### 原则4：样本切分必须 purge + embargo ≥ horizon

池级隔离带（gap1/gap2 = 14 天，pool_registry.py:219）对本目标的 10 天 horizon **自洽**；切分器负责**池内** walk-forward 的 purge + embargo（§8）。

### 原则5：标签在 valid/oos 与原始数据同等敏感

标签是价格的未来函数——拿到标签 ≈ 拿到未来收益。所以三池访问策略（pool_registry.py:109）扩展到标签存储：**oof 可算可读；valid/oos 只走内部评估服务**，Agent 不得读标签（`assert_can_read_data(pool, what="标签")`）。

---

## 3. 定位与边界

```
数据底座 (L0/L1/L2 + 时间墙)
      ↓ reader + PoolScope
特征底座 (算子 + 特征 + 缓存)        ←—— X 的唯一来源
      ↓                ↘（标签名在特征 DSL 白名单之外，物理隔离）
标签体系 (目标 + 标签 + 切分)        ←—— y 的唯一来源
      ↓ 训练数据供给 (阶段2: 唯一允许 X⋈y 对齐的地方, 带断言)
监督学习 (训练/评估/模型注册)  →  ModelSignal  →  回测引擎 (决策+价格, 无标签)
```

**明确不做的三件事**（与既有文档边界一致）：

- **不进特征底座**：特征 DSL 的 `known_fields` 只由 `features/fields.py` 生成，标签名不在其中（dsl.py:142 `compile_expr`）。
- **不进回测引擎**：回测输入 =（持仓决策 + 价格），标签只进训练（用户定案）。
- **不做因子检验/选择**：标签与特征的统计关系（IC/FDR）属模型研究协议。

---

## 4. 目标注册表（`labels/objectives.py`）

```python
@dataclass(frozen=True)
class Objective:
    name: str                # 唯一名, 如 "trend_10d"
    horizon: str             # "10D"
    decision_grid: str       # "1D" —— 决策节拍 (每根日 bar 收盘决策一次)
    universe_layer: str      # "research" (PIT 宇宙快照层)
    cost: CostParams         # 标签扣费参数 (默认与回测 CostModel 同源)
    labels: tuple[str, ...]  # 本目标下注册的标签名
    desc: str
```

`REGISTRY: dict[str, Objective]` + `register_objective()` 帮助函数（风格同 pool_registry 的注册表，不用装饰器魔法）。

**首个目标**：

| 字段 | 值 | 说明 |
|---|---|---|
| `name` | `trend_10d` | 预测未来 10 天走势 |
| `horizon` | `10D` | 在 1D 格上 = **10 根日 bar**（按交易格数，不数日历日——与多周期补全的 1d 序列天然对齐） |
| `decision_grid` | `1D` | 首期只做日频决策：样本少重叠、与"10 天走势"语义一致；`4h/1h` 留作 grid 参数 |
| `cost` | `taker=0.0005, slip_bps=5` | 与 `backtest.CostModel` 默认一致 |
| `labels` | 5 个（§5 表） | |

> 为什么目标与标签分开注册：同一目标下换标签只改标签规格；将来新目标（如 `vol_forecast_10d`、`crash_risk_5d`）只加注册表条目，不动标签库代码。

---

## 5. 标签库（`labels/definitions.py`）

**决策 L1（成交时点口径，本层最关键的决策）**：

```
信号于 t 收盘 (close[t] 已知)
进场成交价 = open[t+1]          ← 与回测引擎 §5 "T+1 开盘成交" 完全一致
持有 H 根后离场, 成交价 = open[t+H+1]   ← 同样是下一根开盘
ret_10d(t) = open[t+H+1] / open[t+1] − 1
```

不用 close[t]→close[t+H]：那等于假设"看到收盘价的瞬间还能按收盘价成交"（回测引擎明确禁止的口径）；不用 open[t+1]→close[t+H]：离场价不可成交。**开-开口径是引擎唯一能逐位兑现的口径**（进场、离场都是引擎里的真实成交价）。

**决策 L2（成本感知）**：净收益 = 毛收益 − c_rt，其中

```
c_rt = 2 × (taker_fee + slippage) = 2 × (0.0005 + 0.0005) = 0.2%   (spot)
```

| 标签 | 定义 | 输出 | 用途 | 对成本敏感 |
|---|---|---|---|---|
| `ret_10d` | `open[t+H+1]/open[t+1] − 1 − c_rt` | 连续值 | 回归目标 | **是** |
| `sign_10d` | `ret_10d > 0` → 1 else 0 | {0,1} | 二分类 | **是**（阈值打在网上，扣费决定符号） |
| `quantile_10d` | 决策日 t 截面上 `ret_10d` 的 5 分位 (1..5) | 多分类 | 排序/多分类 | 否（截面秩近似成本不变——成本近似均一加在全体资产上） |
| `excess_10d` | `ret_10d − 当日截面中位数` | 连续值 | 中性化回归（吃掉大盘 beta） | 否（同上） |
| `vol_scaled_10d` | `ret_10d / 过去20日已实现波动`（trailing，只用过去） | 连续值 | 异方差稳健回归 | 半（分子扣费） |
| `triple_barrier` | López de Prado 三屏障（上下屏障+垂直屏障） | 事件标签 | 延期 | — |

**截面型标签（quantile/excess）为什么允许**：它们用的是**同一决策日 t** 的截面，全体标签在 t+H+1 之前同样不可知，不构成"标签提前可见"；且标签永远进不了特征，截面偷看无从谈起。成本近似均一，所以截面秩对成本不敏感——这正是"成本感知标签"只需要作用在连续值/符号标签上的原因。

**每条标签的 PIT 语义**：

| 量 | 值 |
|---|---|
| 决策时刻 | bar[t] 收盘 |
| 标签窗口 | [t+1, t+H+1]（进场到离场的 bar） |
| `label_available_at` | bar[t+H+1] **收盘**（保守取收盘，开盘价虽更早知道但不提前声明） |
| 可训练前提 | `label_available_at ≤ 池终点`（或 as_of） |
| 宇宙 | t 日 `universe_membership` PIT 快照内的资产才算（不用未来成分股） |

> 参考旧模块：`sign_10d` ≈ FTH（固定窗口方向标签）；`quantile_10d` 是截面化改造；CT / Oracle（DP 最优趋势标注）需要全路径，属**研究用标签**，标记 `research_only`、暂缓实现。

---

## 6. 标签与特征的物理隔离（四道闸）

| # | 闸 | 机制 | 对应测试 |
|---|---|---|---|
| 1 | **命名空间** | 特征 DSL 的 `known_fields` 只由 `features/fields.py` 生成（dsl.py:142）；标签名不在其中 → `compile_expr` 报 `ExprError: 未知字段` | 构造 `compile_expr("ret_10d", known_fields=特征字段集)` 必须失败 |
| 2 | **存储** | 标签写 `data/labels/`（与 `data/l2/certified` 平级）；特征引擎/缓存构造器配置里**没有**该路径 | 特征缓存构造器不读取 `data/labels/` 任何文件 |
| 3 | **字段源扫描** | `fields.py` 全部 `FieldSpec` 禁止正时间偏移（机械扫描 grid/lag/shift 参数）+ 全特征**未来不变性测试**沿用（现状：fields.py 无任何前视字段，已核实） | 新增字段若含正偏移 → 扫描测试拦截 |
| 4 | **池访问** | `assert_can_read_data(pool_id, what="标签")`：valid/oos 拒读（扩展三池纪律，原则5） | valid/oos scope 读标签 → `PermissionError` |

**作弊测试（继承 next_open 教训）**：写一个试图"引用标签当特征"的表达式、写一个"valid 池读标签"的调用、写一个"把标签写进特征面板"的写入——三者都必须**物理失败**，而不是"文档上说不行"。

---

## 7. 样本切分器（`labels/splitter.py`）

**公式**（决策格 t，标签窗口 [t+1, t+H+1]，H=10）：

```
purge:   训练样本必须 t + H + 1 ≤ test_start
embargo: 再加 E 根 (默认 E = H = 10) → 训练样本必须 t ≤ test_start − H − 1 − E
```

**例**（日格）：`test_start = 2023-10-01` → purge 后 train 最晚 2023-09-20；embargo 后 train 最晚 **2023-09-10**。对比：无任何处理时 train 可以到 2023-09-30——那天的标签窗口（到 10 月中旬）整个戳进测试期。

**与池隔离带的关系**：gap1/gap2 = 14 天 ≥ horizon = 10 天 → 池级边界自洽，无需改动；切分器只管**池内** walk-forward。将来若 horizon 长于 14 天，需重新评估池隔离带（写入本层验收清单）。

**API 形状**：

```python
@dataclass(frozen=True)
class Fold:
    train_start / train_end   # train_end 已含 purge+embargo 裁剪
    test_start  / test_end
    def mask(label_times: pd.Series) -> pd.Series   # 每个样本布尔掩码

def walk_forward_splits(scope: PoolScope, train_len: str, test_len: str,
                        step: str, embargo: str | None = None) -> list[Fold]
def assert_no_overlap(fold: Fold, spec: LabelSpec) -> None   # 验证闸
```

- `assert_no_overlap` 对**每条**训练样本验证：其标签窗口与测试窗（含 embargo）无交集——属性式验证，不是靠信任调用方。
- 切分器是 fold 掩码的**唯一产地**：所有实验代码必须消费它的 mask，禁止手写切分（与"时间墙不能靠约定"同一条纪律）。
- 默认 walk-forward（rolling / expanding 参数化）；首期固定 `train_len=4Y, test_len=1Y, step=1Y`。

---

## 8. 标签存储 / 指纹 / 版本化（`labels/store.py`）

```
data/labels/
  trend_10d/
    ret_10d__v1__<fp8>.parquet + meta.json      # 与 features/snapshots 同构
    sign_10d__v1__<fp8>.parquet + meta.json
    ...
```

- **指纹** = hash(标签名 + 版本 + objective + grid + horizon + 成本参数 + 输入数据指纹（1d manifest 的 row_count/coverage_end/certified_at）+ 池 + 资产集)——与特征缓存键同构（manifest.py 的字段直接复用）。
- **版本化**：规格改动 → 新版本，旧版本不可变（同 `features/registry_versioning.py`）。
- **访问**：`load_label(name, scope)` 内部先 `assert_can_read_data(scope.pool_id, "标签")`。oof 可算可读；valid/oos 由**内部评估服务**计算并保管，Agent 只有提交通道（与 eval_service 同构，训练数据供给在阶段 2 接上）。
- **MCP**：`list_labels / describe_label / compute_labels`（oof），与现有 `list_features / compute_features` 平级。

---

## 9. 与既有体系的关系

| | 复用 |
|---|---|
| 数据 | `data_foundation.reader` 读认证层 `market_candle_spot_1d`（多周期补全已回填，spot 590 标的） |
| 宇宙 | `universe_membership` 逐日 PIT 快照（pool_registry.universe_symbols） |
| 时间墙 | `PoolScope` 钳制决策日与池范围；`assert_can_read_data` 扩展 `what="标签"` |
| 成本 | `backtest.CostModel`（taker 0.0005 / 滑点 5bps）——标签与回测**成本同源**，改一个改全部 |
| 指纹 | `manifest.py` 的 row_count/coverage/certified_at；特征缓存的 fp 构造模式 |
| 版本 | `features/registry_versioning.py` 的不可变版本模式 |

**本层不重新实现**：取数、PIT 校验、宇宙门控、时间墙、指纹都用现成的。它只做"目标/标签定义 + 隔离 + 切分 + 存储"。

---

## 10. 不做什么（明确边界）

- **不做**因子检验 / 标签-特征相关性筛选 / 特征选择（模型研究协议）。
- **不做** Oracle / CT 研究用标签（全路径依赖，`research_only` 暂缓）。
- **不做** triple-barrier 事件标签（延期）。
- **不把**标签写进特征面板、特征缓存、回测引擎（三重边界）。
- **不做** perp 资金费率成本（`CostParams` 留 `funding` 扩展位；spot 先行）。

---

## 11. 决策记录

| # | 决策 | 默认 | 理由 |
|---|---|---|---|
| L1 | 收益口径 | **开-开**：`open[t+1] → open[t+H+1]` | 进场/离场都是回测引擎的真实成交价，标签-回测逐位可核对（原则3） |
| L2 | 成本 | c_rt = 2×(taker+slip) = 20bps，参数随标签规格 | 与 `CostModel` 同源；改成本→新指纹 |
| L3 | 决策节拍 | 1D 格 | "10 天走势"语义 + 样本重叠最小；4h/1h 留 grid 参数 |
| L4 | sign 阈值 | net>0 → 1 | 成本后符号；τ>0 的噪声阈值留扩展位 |
| L5 | embargo | E = H = 10 根 | 严格不弱于 horizon；池隔离带 14 天 ≥ H 自洽 |
| L6 | 标签保护 | valid/oos 视同原始数据（submit_only） | 标签≈未来收益（原则5） |
| L7 | 截面标签 | 允许 quantile/excess | 同日截面 + 标签永不进特征 → 无泄漏路径；且成本近似均一 |
| L8 | Oracle/CT | research_only 暂缓 | 需要全路径，先做无争议部分 |

---

## 12. 实现顺序

1. `labels/objectives.py`——目标注册表 + `trend_10d` 注册（含 CostParams）
2. `labels/definitions.py`——五种标签函数（1d 认证数据上计算）+ **手算对照测试**
3. `labels/store.py`——指纹 / 版本 / 池访问控制（`assert_can_read_data` 扩展）
4. `labels/splitter.py`——purge + embargo + `assert_no_overlap` 属性验证
5. 隔离四道闸 + `_test_label_isolation.py`（命名空间 / 存储 / 字段扫描 / 池访问 / 作弊测试）
6. MCP 工具（oof）+ 测试
7. 验收清单全跑（§13）

---

## 13. 验收标准

- **手算对照**：随机抽 5 个 (资产, t)，从原始 1d open 手工算 `ret_10d`，与标签库输出逐位一致（口径、成本、开-开时点）。
- **隔离**：引用标签名的特征表达式编译失败；valid/oos 读标签抛 `PermissionError`；"把标签当特征用"的作弊调用全部物理失败。
- **切分**：任意 fold，训练样本标签窗口与测试窗无交集（含 embargo）；2023-10-01 边界手工核对到 2023-09-10。
- **指纹/版本**：同规格不同数据指纹 → 不同缓存键；改成本参数 → 新版本，旧版本不可变。
- **未来不变性不回归**：特征侧全部未来不变性测试沿用；标签侧**故意**不适用未来不变性（隔离测试替代——标签本来就是未来函数）。
- **全量回归**：现有 445 项测试不回归。
