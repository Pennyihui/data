# 特征底座（Feature Foundation）设计文档

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.8（特征库 234; 补 6 个无状态预处理算子; 分组维度打通） |
| 日期 | 2026-10-05 |
| 状态 | **可实施**（决策 1-10 已定；F3 选型已验证，见 12.3） |
| 前置 | `crypto-data-foundation-research.md`（数据底座）、`research-pools-timewall-design.md`（时间墙） |
| 定位 | 数据底座之上、模型研究协议之下的一层 |

---

## 0. 一句话设计

**特征底座 = 算子层 + 特征层 + 因子层**，其中：

- **算子层**提供量化行业标准的三族算子（`ts_*` / `cs_*` / `group_*`）
- **特征层**用声明式表达式批量生成特征（候选，写了就算数）
- **因子层**只收录**通过统计检验**的特征（精选，带检验元数据）

**分界线是"检验"**：检验（IC/FDR/Newey-West）属于**模型研究协议**，不属于本层。特征选择同理。

---

## 1. 设计依据（调研结论）

### 1.1 三个参照系统的架构

**Qlib（微软）** — [Data Layer 文档](https://raw.githubusercontent.com/microsoft/qlib/main/docs/component/data.rst)
```
基础数据（磁盘只存 OHLCV）
    ↓ 表达式引擎 (Expression Engine / Operators)
特征（如 Ref($close,60)/$close）
    ↓ 处理器 (Processors) —— 与算子明确区分，处理算子做不了的复杂加工
    ↓ 数据集 (Dataset) —— 面向模型（Alpha158 / Alpha360）
```
关键：**基础数据 ≠ 特征**；算子与处理器分开；Dataset 面向模型。

**AlphaForge（加密永续）** — [GitHub](https://github.com/warren618/AlphaForge)
```
src/factor_pipeline/
├── factors/          # 因子实现（可插拔）
├── evaluator.py      # Rolling IC / FDR / Newey-West
├── combo_search.py   # 贪婪组合搜索
├── registry.py       # 装饰器自动发现
└── pipeline.py       # CLI 编排
流程：register → evaluate → pass/fail FDR → auto-register
```
关键：**注册表 + 装饰器**；检验是进因子库的门槛。

**WorldQuant BRAIN** — [算子体系](https://github.com/QuantML-Research/wq-alpha-research/blob/main/SKILL.md)

| 族 | 算子 |
|---|---|
| 截面 `cs_*` | rank, zscore, normalize, scale, winsorize |
| 时序 `ts_*` | ts_mean, ts_std_dev, ts_delta, ts_rank, ts_corr, ts_decay_linear, ts_backfill, ts_zscore |
| 分组 `group_*` | group_rank, group_neutralize, group_zscore, group_backfill |
| 条件 | if_else, trade_when |

黄金范式：`group_rank(ts_rank(signal, N), subindustry)`

### 1.2 特征 vs 因子的工程区分

| | 特征 Feature | 因子 Factor |
|---|---|---|
| 流程位置 | 算子产出（中间层） | 通过检验后进因子库 |
| 是否需验证 | 不需要 | **必须** |
| 管理方式 | 表达式定义 | 注册表 + 检验元数据 |
| 数量 | 多（Alpha158=158） | 少（Aperiodic 仅 15） |
| 分界线 | —— | **统计检验**（IC / FDR / Newey-West） |

**结论**：检验与选择属于**模型研究协议**；本层只做"特征生成 + 因子登记"。

### 1.3 加密特有因子（Aperiodic 机构级目录，15 个）

| 类别 | 数量 | 因子 |
|---|---|---|
| Momentum | 4 | Momentum / Enhanced Momentum / Instantaneous Momentum / Polaris |
| Reversal | 2 | Mean Reversion / Enhanced Mean Reversion |
| **Carry** | 1 | **Enhanced Carry**（跨所资金费率溢价） |
| Volatility | 1 | Instantaneous Volatility |
| **Liquidity** | 2 | **Relative Illiquidity** / Altair（滑点、订单失衡） |
| **Flow** | 1 | **Retail Flow**（散户流，系统性反向） |
| **Derivatives** | 2 | **Open Interest Divergence** / Margin Risk |
| **On-Chain** | 1 | Supply Velocity（通胀率） |
| Multi-Factor | 1 | 7 Factor Composite |

**加密特有**（传统股票不存在）：Carry、OI Divergence、Margin Risk、Retail Flow、Supply Velocity。

### 1.4 重要警示（严谨研究的反直觉结论）

来自 [funding-rate-alpha](https://github.com/OctopusTakopi/funding-rate-alpha)（791 合约、243 万次结算）：

1. "**The raw funding level is a carry trade far more than a price signal**"
2. "**funding does predict returns, in the level and not the z-score**" —— 通行做法（滚动 z-score）**反而失效**
3. **OI 条件的有效性在同日内对照后崩塌**：pooled +34bp → 同日内 +7.0bp（t=0.35，不显著）
4. **"No cross-validation prescription survives a control"** —— 很多通行做法经不起对照
5. **regime 切换**：2020-21 反转 regime，2023-26 延续 regime，系数变号

**方法论启示**：特征底座要**支持同一特征的多口径变体**（原始值 vs z-score vs rank），让检验去决定哪个有效，**而不是预先假定 z-score 更好**。

---

## 2. 四层架构

```
┌─────────────────────────────────────────────────────────────┐
│  数据底座 (data_foundation)  —— 已完成                        │
│  L0/L1/L2 + 时间墙 (pool_registry) + MCP + OOS 账本          │
└─────────────────────────────────────────────────────────────┘
                            ↓ reader + PoolScope
┌─────────────────────────────────────────────────────────────┐
│  特征底座 (feature_foundation)                                │
│                                                              │
│  ① 字段层 (Fields)        从数据底座取原始量                   │
│      price / derivatives / onchain / macro ...               │
│                            ↓                                 │
│  ② 算子库 (Operators)     三族算子 + 预处理算子                │
│      ts_*  |  cs_*  |  group_*  |  pp_*                      │
│                            ↓                                 │
│  ③ 特征库 (Features)      ★ 实体集合，非仅注册表               │
│      features/library/  按数据来源组织                        │
│      候选特征（写了就算数，几百个）                            │
│                            ↓                                 │
│  ④ 因子库 (Factors)       ★ 实体集合，精选                    │
│      factors/library/   按信号类别组织                        │
│      通过检验的特征（几十个，带检验元数据 + 生命周期）          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  模型研究协议 (Model Research Protocol)  —— 不在本层          │
│  特征选择 / 模型训练 / IC-FDR 检验 / 组合搜索 / 回测           │
└─────────────────────────────────────────────────────────────┘
```

**边界铁律**：
- 本层**不做特征选择**（属模型协议）
- 本层**不做统计检验**（属模型协议）
- 本层**只负责**：可 PIT 地生成特征 + 登记因子

**"库"与"注册表"的区别**（本设计明确区分）：

| | 注册表 (Registry) | 库 (Library) |
|---|---|---|
| 是什么 | 索引/目录 | 实体集合 |
| 内容 | 名字 + 元数据 | 定义本身 |
| 类比 | 图书馆检索卡 | 书架上的书 |
| 本设计 | `registry.py` | `library/` |

**特征库 ≠ 因子库**，二者是两个独立实体集合（详见第 4、5 节）：

| | 特征库 | 因子库 |
|---|---|---|
| 门槛 | 无（写了就算数） | **必须通过统计检验** |
| 组织方式 | 按**数据来源**（price/derivatives/...） | 按**信号类别**（momentum/carry/...） |
| 数量 | 几百个 | 几十个 |
| 每个的元数据 | 表达式 + 血缘 + PIT 规则 | 表达式 + 血缘 + **检验结果（分池）** + 经济解释 + 生命周期状态 |

---

## 3. 算子层设计

### 3.1 三族算子（对齐 WorldQuant）

```python
# 时序算子 ts_*  —— 单资产，跨时间
ts_mean(x, w)         ts_std(x, w)        ts_rank(x, w)
ts_delta(x, w)        ts_corr(x, y, w)    ts_zscore(x, w)
ts_decay_linear(x, w)                     # 线性衰减加权（量化特有）
ts_min(x, w)          ts_max(x, w)        ts_skew(x, w)    ts_kurt(x, w)

# 截面算子 cs_*  —— 同时刻，跨资产
cs_rank(x)            cs_zscore(x)        cs_normalize(x)
cs_scale(x)           cs_winsorize(x, n_std=4)   # 去极值

# 分组算子 group_*  —— 组内比较
group_rank(x, g)      group_zscore(x, g)  group_neutralize(x, g)
group_mean(x, g)      group_std(x, g)
```

### 3.2 加密的分组维度（替换 A 股的行业）

```python
GROUPS = {
    "sector":     板块（DeFi / L2 / Meme / AI / RWA / GameFi ...）
    "market_cap": 市值分层（大/中/小盘）
    "venue":      交易所
    "chain":      链
    "quote":      报价资产（USDT / USDC）
    "listing_age": 上市时间分层
}
```

### 3.3 预处理算子（"预处理即特征"）

```python
# 尺度变换
pp_zscore(x)      pp_minmax(x)      pp_robust(x)      # (x-median)/IQR
pp_log(x)         pp_sqrt(x)        pp_power(x, p)    pp_boxcox(x)

# 平稳化
pp_diff(x, 1)     pp_pct_change(x)  pp_frac_diff(x, d)  # 分数阶差分
pp_detrend(x)

# 离散化
pp_quantile_bucket(x, n)            pp_bucket(x, edges)

# 平滑
pp_ema(x, span)   pp_savgol(x)      pp_kalman(x)

# 数据质量衍生（★ 清洗动作暴露的信息就是特征）
pp_is_missing(x)  pp_is_gap(x)      pp_is_outlier(x)
pp_fill_count(x, w)                 pp_quality_flag(x)
```

> **最后一组来自数据底座 L2 的 `is_gap` / `is_suspect` / `quality_reason`** —— 这些现成可用。

---

## 4. 特征库设计 ★

### 4.0 定位：库 ≠ 注册表

**特征库是实体集合**（特征定义本身），不是索引。它有：

- **组织维度**：按数据来源分文件（可浏览、可复用）
- **可检索目录**（catalog）：按类别 / 来源 / 算子 / 标签多路索引
- **去重检查**：新增特征时检测是否与已有重复（表达式等价 / 血缘相同）
- **版本管理**：表达式变更即升版，旧版保留（复现历史研究）

### 4.1 目录结构

```
features/
├── registry.py            # 注册机制（装饰器 + 全局表）
├── catalog.py             # 可浏览目录（多路索引 + 检索）
├── library/               # ★ 特征库实体
│   ├── price.py             价格量衍生（动量/波动/形态）
│   ├── derivatives.py       衍生品衍生（funding/OI/basis/mark/ratio）
│   ├── liquidity.py         流动性（量、成交笔数、Amihud）
│   ├── cross_asset.py       跨资产（相对BTC、相对板块）
│   ├── quality.py           数据质量衍生（is_gap/is_suspect 的聚合）
│   └── onchain.py           链上（仅滚动OOS可用）
└── specs.py               # FeatureSpec 数据结构
```

### 4.2 特征定义（声明式）

```python
@feature(
    name="funding_rank_7d",
    expr="group_rank(ts_rank(funding_rate, 168), sector)",
    inputs=["derivatives_funding"],
    category="derivatives",
    desc="资金费率在自身168小时历史的排名，再在板块内排名",
)
```

### 4.3 多口径变体（应对 1.4 的警示）

同一基础量自动派生多口径，**由检验决定哪个有效**（不预设）：

```python
funding_rate              # 原始水平（研究表明这个才有效）
funding_rate_zscore_30d   # 滚动 z-score（通行做法，可能失效）
funding_rate_rank_cs      # 横截面排名
funding_rate_pct_30d      # 分位
```

引擎提供 `variants()` 辅助函数自动展开多口径：
```python
@feature_multi(field="funding_rate", ops=["raw","ts_zscore(30d)","cs_rank","ts_pct(30d)"])
# → 自动生成 4 个特征，命名规则: {field}_{op}
```

### 4.4 特征分类（按数据来源 × 算子组合）

| 类别 | 来源字段 | 典型算子组合 |
|---|---|---|
| 价格/量 | close, volume, trades | `ts_rank(pp_pct_change(close), 24)` |
| 衍生品 | funding, OI, mark, index, ratio | `group_rank(ts_zscore(funding, 168), sector)` |
| 币本位 | open/high/low/close 组合 | `pp_log(high/low)`, `ts_std(ts_delta(close))` |
| 流动性 | volume, trades | `ts_mean(volume,24) / ts_mean(volume,168)` |
| 链上 | onchain_daily_aggregate | `cs_rank(transfer_count)` |
| 情绪/宏观 | fng（宏观已屏蔽） | `ts_delta(fng, 7)` |
| 质量衍生 | is_gap, is_suspect | `ts_mean(pp_is_gap(x), 168)` |

### 4.5 特征注册表与目录

```python
# features/registry.py
_FEATURES: dict[str, FeatureSpec] = {}

def feature(name, expr, inputs, category="", desc="", **meta):
    def deco(fn_or_none=None):
        _FEATURES[name] = FeatureSpec(name, expr, inputs, category, desc, **meta)
        return fn_or_none
    return deco

def list_features() -> list[FeatureSpec]: ...
def get_feature(name) -> FeatureSpec: ...

# features/catalog.py —— 多路索引
def by_category(cat) -> list[FeatureSpec]: ...
def by_input(dataset) -> list[FeatureSpec]: ...      # 用了某个数据源的特征
def by_operator(op) -> list[FeatureSpec]: ...        # 用了某个算子的特征
def search(keyword) -> list[FeatureSpec]: ...
def find_duplicates() -> list[tuple]: ...            # 等价表达式检测
```

### 4.6 特征血缘规范

```python
@dataclass
class FeatureSpec:
    name: str
    expr: str                       # 表达式（人可读）
    inputs: list[str]               # 直接依赖的字段/特征
    category: str                   # price / derivatives / liquidity / ...
    lineage: list[str]              # 完整链路（自动展开）
    available_rule: str             # PIT 推导规则
    version: str                    # 版本（表达式变更即升版）
    tags: list[str]                 # 自由标签（供检索）
```

---

## 5. 表达式语言：用开源，不自研

### 6.1 决策（2026-10-04 拍板）

> **决策：表达式语言采用"开源 DSL + Python 函数式"混合模式，不自研解析器。**

理由：自研 DSL 解析器是**重复造轮子**——开源已有成熟实现，且自研会引入 parser bug（血缘解析错误 = PIT 推导错误 = 泄漏风险）。

### 6.2 可选开源项目（已调研）

| 项目 | 定位 | 适用性 |
|---|---|---|
| **[expr_codegen](https://github.com/wukan1986/expr_codegen)** | 表达式**转译**为 polars / pandas 代码 | ★ 最贴合——生成可读 Python 代码，本身即"血缘产物" |
| **[KunQuant](https://github.com/Menooker/KunQuant)** | 金融表达式**编译器 + 优化器 + 执行器**（MLIR 后端） | 高性能需求时考虑；内置 Alpha101/GTJA191 因子集 |
| **[FastPlus](https://github.com/AshSwing/FastPlus)** | 基于 WorldQuant Fast Expression 的 Alpha 表达式语言 | 语法对齐 WorldQuant |
| **[Qlib 表达式引擎](https://github.com/microsoft/qlib/blob/main/qlib/data/ops.py)** | 算子集完整（Ref/Mean/Std/Corr/Rank...） | 算子实现可参考/复用 |
| **[alphaexpr](https://github.com/loversky02/alphaexpr)** | 解析 / 校验 / **去重**公式化 alpha 表达式 | 用于特征库去重 |
| **[alpha-lab](https://explore.market.dev/ecosystems/python/projects/alpha-lab)** | 本地优先的 alpha 研究平台（受 WQ BRAIN 启发） | 架构参考 |

### 6.3 混合模式设计

```
┌──────────────────────────────────────────────┐
│ 简单特征（~80%）→ DSL 表达式                   │
│   expr="group_rank(ts_rank(funding_rate,168), sector)" │
│   → 解析器自动得到: 血缘 + 输入 + 算子链        │
├──────────────────────────────────────────────┤
│ 复杂特征（~20%）→ Python 函数（逃生口）         │
│   @feature(name="...", inputs=[...])          │
│   def f(ctx): ...                             │
│   → 血缘靠 inputs 声明（引擎校验完整性）        │
└──────────────────────────────────────────────┘
```

**引擎对两者的统一要求**：
- 必须能回答"用了哪些输入字段"（DSL 自动解析 / Python 显式声明）
- 必须能推导 `data_available_at`
- 必须能展开血缘链路

**若 Python 函数未声明 `inputs` → 引擎拒绝注册**（强制血缘完整）。

### 6.4 待定：选哪个开源项目

建议**先做一次选型验证**（各跑一个 100 特征的小规模对比），比较：
- 是否支持所需算子（ts_* / cs_* / group_*）
- 性能（1967 万行现货 K 线的计算耗时）
- 是否输出可读代码（便于审计）
- 能否与 PoolScope / PIT 引擎对接

---

## 6. 因子库设计 ★

### 7.1 定位：因子 = 通过检验的特征

```
特征库（候选，几百个）
    ↓ 模型研究协议检验（IC / FDR / Newey-West）
因子库（精选，几十个）
```

**入库门槛**：RankIC 显著 + FDR 校正通过 + 有经济解释 + 适用域明确。

### 7.2 目录结构

```
factors/
├── registry.py            # 注册机制
├── lifecycle.py           # 生命周期状态机
└── library/               # ★ 因子库实体（按 Aperiodic 九类）
    ├── momentum.py          Momentum / Enhanced Momentum / Instantaneous Momentum / Polaris
    ├── reversal.py          Mean Reversion / Enhanced Mean Reversion
    ├── carry.py             Enhanced Carry（跨所资金费率溢价）
    ├── volatility.py        Instantaneous Volatility
    ├── liquidity.py         Relative Illiquidity / Altair
    ├── flow.py              Retail Flow（散户流，反向）
    ├── derivatives.py       Open Interest Divergence / Margin Risk
    ├── onchain.py           Supply Velocity
    └── composite.py         多因子合成
```

### 7.3 因子定义（带检验元数据 + 池绑定）

```python
@factor(
    name="carry_enhanced",
    based_on="funding_rate",           # 血缘
    category="carry",
    evaluation={                        # 检验结果（模型协议写入，按池分别记录）
        "oof":   {"rank_ic": 0.032, "ir": 0.41, "turnover": 0.18,
                  "newey_west_t": 3.2, "fdr_pass": True},
        "valid": {"rank_ic": 0.014, "ir": 0.19, "fdr_pass": True},
        "oos":   None,                  # OOS 只在人审通道可见（Agent 盲）
    },
    economic_rationale="跨所资金费率溢价：不同所费率失衡反映资金面错配",
    applicable_domain="流动性前 100 币，2020 年后",
    status="validated",                 # candidate/validated/live/decayed/retired
)
```

### 7.4 因子生命周期状态机

```
candidate ──检验通过──> validated ──上线──> live
    │                                          │
    │                                     监测衰减
    │                                          ↓
    └──────检验不通过──> archived          decayed ──> retired
```

**各池表现分开记录**是"因子健康度监控"的基础：
```
同一因子:
  oof   RankIC 0.035  IR 0.45   ✅ 优秀
  valid RankIC 0.012  IR 0.18   ⚠️ 衰减
  oos   → 仅人审可见
```

### 7.5 Agent 盲的延续

因子的 **OOS 检验结果对 Agent 不可见**（与数据底座的 OOS 治理一致）：
- Agent 可查因子是否已登记、可查 oof/valid 表现
- Agent **查不到 oos 列**（MCP 工具层过滤）

---

## 7. PIT 继承机制（核心）

### 7.1 特征的 data_available_at 推导规则

> **特征在事件时间 `t` 的值，其 `data_available_at` = 该特征所用全部输入中最大的 `data_available_at`。**

```python
# 例: ts_rank(funding_rate, 168) 在 t 时刻
#   使用 funding_rate[t-167 .. t]
#   可用时间 = max(avail(funding_rate[t-167 .. t]))
#            = avail(funding_rate[t])           (通常)
#            = funding_time_utc[t]              (资金费率结算时刻)

# 例: cs_rank(close) 在 t 时刻
#   使用当日所有币的 close
#   可用时间 = 当日全部 bar 收盘后（= 当日最后一根 bar 的 close_time）
```

**这条规则由引擎自动执行，不让使用者手写**（手写必错）。

### 7.2 滚动窗口强制 shift

```python
# 引擎内部强制: 计算 t 时刻的特征，只能使用 avail <= t 的数据
# 禁止: center=True 的居中窗口、forward-fill 未来值
```

### 7.3 泄漏检测（引擎自检）

```python
def assert_no_leakage(feature_df):
    """特征值的 data_available_at 不得早于其输入的最大可用时间。"""
    assert (feature_df.data_available_at >=
            feature_df.input_max_available_at).all()
```

---

## 8. 时间墙继承

特征底座**直接复用**数据底座的 `PoolScope`：

```python
scope = PoolScope("oof")            # 绑定开发池

ff = FeatureEngine(scope=scope)
df = ff.compute(["funding_rank_7d", "oi_divergence"], 
                instruments=scope.universe(),   # 宇宙门控
                start="2018-01-01", end="2023-12-31")
# 引擎自动:
#   - as_of 双向钳制到池内
#   - 因子屏蔽（污染源如 macro_daily 自动拒绝）
#   - 输入数据的 data_available_at 过滤
#   - 特征自身的 data_available_at 正确推导
```

**这是最大的架构红利**：时间墙已建好，特征层白拿。

---

## 9. 血缘（Lineage）

因衍生特征可多层叠加（`ts_zscore(ts_rank(pp_log(x)))`），**必须记录血缘**：

```python
@dataclass
class FeatureSpec:
    name: str
    expr: str                       # 表达式（人可读）
    inputs: list[str]               # 直接依赖的字段/特征
    lineage: list[str]              # 完整链路（自动展开）
    data_available_at_rule: str     # PIT 推导规则
    version: str                    # 版本（表达式变更即升版）
```

**血缘的四个用途**：
1. **泄漏审计**：追溯每一步是否用了未来数据
2. **相关性判断**：血缘相近 → 高度相关 → 组合时注意
3. **失效归因**：基础数据变了？还是变换不适合了？
4. **可复现**：表达式 + 版本 = 精确定义

---

## 10. 与数据底座 / MCP 的接口

### 9.1 读取接口

```python
from data_foundation.reader import load_candles, load_derivatives, load_universe
from data_foundation.pool_registry import PoolScope
```
特征底座**只通过 reader 读数据**，不直接碰 parquet。

### 9.2 MCP 工具扩展（给 Agent 用）

| 工具 | 作用 | 池约束 |
|---|---|---|
| `list_features()` | 列出可用特征 | 全可见 |
| `describe_feature(name)` | 查特征定义 + 血缘 | 全可见 |
| `compute_features(names, start, end)` | 批量算特征 | **as_of 钳制到本池** |
| `list_factors()` | 列出已登记因子（Agent 不可见 OOS 列） | OOS 检验结果对 Agent 隐藏 |
| `factor_meta(name)` | 查因子元数据 | OOS 部分对 Agent 隐藏 |

**Agent 盲的延续**：因子的 OOS 检验结果，Agent 同样读不到。

---

## 11. 加密因子实现清单（映射到现有数据）

基于 Aperiodic 目录，标注你们的**数据可支撑性**：

| 因子 | 类别 | 所需数据 | 你们有吗 |
|---|---|---|---|
| Momentum | momentum | 现货/永续 K线 | ✅ 全历史 |
| Mean Reversion | reversal | K线 | ✅ |
| **Enhanced Carry** | carry | 资金费率（跨所） | ✅ Binance全史 + OKX/Bybit/Bitget |
| Instantaneous Volatility | volatility | K线 | ✅ |
| **Relative Illiquidity** | liquidity | volume, trades | ✅ |
| **Open Interest Divergence** | derivatives | OI + 价格 | ✅ OI 自 2020-09 |
| **Margin Risk** | derivatives | 标记价 + OI + 多空比 | ✅（清算数据缺，可近似） |
| **Retail Flow** | flow | 多空比 + taker 量 | ✅ |
| Supply Velocity | onchain | 代币供应（需补） | ⚠️ 需补数据 |
| Altair（订单失衡） | liquidity | 订单簿 | ❌ 无 L2 |
| Polaris（归一化动量） | momentum | K线 | ✅ |

**结论**：**15 个因子里至少 8 个你们的数据能直接支撑**，且都是加密特有类别（Carry / OID / Margin Risk / Retail Flow）。

---

## 12. 落地路线（双线：特征线 + 因子线）

> **决策（2026-10-05）**：实现分**两条独立路线**——**特征线先行**，因子线后置。
> 两条线解耦：特征线**不依赖**因子线的任何产物；因子线消费特征线的产出。

### 12.1 特征线（先行实现）

| 阶段 | 内容 | 依赖 | 状态 |
|---|---|---|---|
| **F1** | 算子库（三族 `ts_/cs_/group_` + 预处理 `pp_`） | 无 | ✅ 完成（46 算子 / 153 测试） |
| **F2** | 字段层 + **PIT 引擎**（reader 取数、`data_available_at` 自动推导、PoolScope 继承、泄漏自检） | 数据底座 | ✅ 完成（34 字段 / 51 测试） |
| **F3** | **DSL 选型验证**（expr_codegen：`group_*` 支持 + 1,967 万行性能） | 无（可并行） | ✅ 完成（见 12.3 / 66 测试） |
| **F4** | **特征库**（注册表 + library/ 按来源组织 + catalog 多路索引 + 去重） | F1+F2+F3 | ✅ 完成（80 特征 / 32 测试） |
| **F5** | **血缘系统**（DAG 直接父节点 + 审计 + 版本管理，深度≤5） | F4 | ✅ 完成（lineage.py，trace/impact/audit） |
| **F6** | **首批 50–100 个特征填充特征库**（按来源：price/derivatives/liquidity/quality/cross_asset） | F4 | ✅ 完成（80 个，全池1实算，无空特征） |
| **F7** | MCP 工具扩展（`list_features` / `describe_feature` / `compute_features` / `feature_catalog`） | F4 | ✅ 完成（墙在工具层验证） |

**特征线的交付物**：可 PIT 计算、带血缘、受时间墙约束的特征库 + 计算引擎 + MCP 工具。
**特征线的验收**：能对池1（OOF）批量算出 50+ 特征，`assert_no_leakage` 全通过，血缘完整可查。

### 12.2 因子线（后置，依赖特征线）

| 阶段 | 内容 | 依赖 | 状态 |
|---|---|---|---|
| **G1** | 因子库骨架（元数据结构 + 池绑定 + 生命周期状态机 + Agent 盲过滤） | 特征线 F4 | 后置 |
| **G2** | 因子注册表 + 与模型协议的检验接口（IC/FDR 结果写入） | G1 + 模型协议 | 后置 |
| **G3** | 加密因子首批实现（8 个：Carry/OID/MarginRisk/RetailFlow/Momentum/MR/Illiq/IVOL） | G1 + 首批标签 | 后置 |
| **G4** | MCP 工具扩展（`list_factors` / `factor_meta`，OOS 列对 Agent 隐藏） | G1 | 后置 |

**因子线的启动条件**：特征线 F4 完成 + **模型研究协议提供标签**（G2 的检验需要未来收益）。
**为什么因子线后置**：因子的定义（"通过检验的特征"）依赖检验，检验依赖标签，标签属模型协议——**标签还没建，因子线动不了**。

### 12.3 F3 选型实测结论（2026-10-05，v0.5）

决策 6 要求 F3 验证两点（`group_*` 支持 / 性能）。实测脚本 `_dsl_probe.py`，
结论存 `_dsl_probe_result.json`。把 46 个算子注册进 expr_codegen 的 `global_env`：

| 验证项 | 结论 |
|---|---|
| ① `group_*` 支持 | ✅ 注册后能转译 `group_rank(cs_zscore(ts_zscore(pp_log(close))), sector)`，生成可读分阶段代码 |
| 公共子表达式消除（CSE） | ✅ 共享子表达式只保留一次计算 |
| 分阶段生成 | ✅ 时序阶段 → 截面/分组阶段 |
| 与本项目**面板**（MultiIndex base_asset/time）执行 | ❌ `KeyError: 'base_asset'` |
| 平表执行 | ❌ `NameError: ts_zscore`（生成代码只 import `expr_codegen.pandas.ta`，不认我们的命名空间） |
| 性能口径 | 我们的 pandas 引擎：2M 行 `ts_zscore(24)`≈2s、`ts_rank(24)`≈5s、`cs_rank`≈2s；1967 万行外推为分钟级/特征 |

**结论**：expr_codegen 的**代码生成**能力（可读 + CSE + 阶段拆分）验证成立，
但它的**执行模型**（把 asset/date 当列、自行 `groupby` 逐片调用算子）与我们
**面板算子模型**（按索引层自行分组）互斥；要落地就得为 46 个算子再写一套平表
内核，且 PIT 可用时间传播必须知道每个节点的取数窗口（expr_codegen 执行时不暴露）。

因此**引擎定案**：用 Python 标准库 `ast.parse` + 严格白名单求值
（`features/dsl.py`）。决策 1 担心的"自研 parser 的 bug"在此不成立——解析交标准
库，我们只写白名单与遍历，并用与参考实现逐点对照 + 未来不变性测试兜底
（`_test_dsl.py`，66 项）。CSE、阶段化可读性、PIT 逐节点传播全部在我们引擎内
实现。

> **决策 1/6 修订（2026-10-05）**：不再以 expr_codegen 为主引擎；expr_codegen
> 降级为**可选的代码生成后端**（其可读代码仍可用于审计/血缘展示），默认引擎为
> 自带的 ast 白名单求值器。理由与实测见 12.3。

```
特征线: F1→F2→F3→F4→F5→F6→F7     （现在做）
                      ↓ 产出特征
因子线:            G1→G2→G3→G4      （等特征线 F4 + 模型协议标签）
```

- 特征线期间，因子层**只保留设计**（第 6 节），不写代码
- 特征库的 `FeatureSpec` 设计**预留** `category` / 血缘字段——因子线直接复用，不返工
- G3 的 8 个加密因子，其**特征层前身**已在特征线 F6 实现（如 `funding_rate`、`oi_divergence_raw`），G3 只是把它们"检验后晋升登记"

**不做的事**（边界）：
- 不做特征选择（模型协议）
- 不做统计检验（模型协议；因子线 G2 只提供接口）
- 不落盘缓存（特征线全程）

---

## 13. 已决策事项（2026-10-04 拍板）

| # | 问题 | 决策 | 理由 |
|---|---|---|---|
| 1 | 表达式语言 | **标准库 ast 白名单求值**（v0.5 修订；expr_codegen 降为可选审计后端） | F3 实测：expr_codegen 能生成可读分阶段代码（CSE + group_* 均通），但其执行模型把 asset/date 当列并自行 groupby，与本项目"面板算子"（按索引层自行分组）互斥；且 PIT 传播必须知道每个节点的取数窗口（expr_codegen 执行时不暴露）。决策 1 担心的"自研 parser 的 bug"在此不成立——解析交给标准库，我们只写白名单与遍历，并以逐点对照 + 未来不变性测试兜底（12.3 / `_test_dsl.py` 66 项） |
| 2 | 板块分类来源 | **接现成数据源（如 CoinGecko categories），不手工维护全表** | 手工维护不可持续（新币、主观性、时变性）；人工只做补充 |
| 3 | 特征是否落盘缓存 | **先不落盘**（F1–F3 阶段） | 过早缓存引入 PIT 一致性风险（上游修正后缓存过期）；等算力成瓶颈再引入，且缓存 key 须含 特征版本+输入批次ID+池 |
| 4 | 检验归属 | **放模型研究协议侧，本层只给接口** | IC 计算需要标签（未来收益），而标签定义属模型协议；若本层算 IC 则两层耦合。本层完全不需要知道标签存在 |
| 5 | 首批特征规模 | **50–100 个** | Google Rule #21：可学权重数 ≈ 数据量。加密有效样本少（regime 少、相邻日高相关），贪多必过拟合 |
| 6 | DSL 选型 | **F3 已验证并定案**（v0.5，详见 12.3） | 实测（`_dsl_probe.py`）：expr_codegen 0.16.6 注册我们算子后，group_* ✅、CSE ✅、阶段拆分 ✅、可读代码 ✅（审计价值真实存在，保留为可选审计/展示后端）；但其执行器喂 MultiIndex 面板会 `KeyError: 'base_asset'`，喂平表则 `NameError`（不认我们的命名空间）——与面板模型不兼容，不作默认执行引擎。默认引擎 = 自带 ast 白名单求值器 |
| 7 | 特征库规模上限 | **软上限 500 / 硬上限 1000 + 强制定期体检** | 业界成熟特征集在几百量级（Alpha158/Alpha360）；每加 100 个必须跑一轮检验，未通过的归档——让库是"活水"不是垃圾场 |
| 8 | 多口径变体展开 | **受控展开 + 白名单模板；F4 阶段先不做自动展开** | 口径选择要有理论依据（如 funding 的 raw 有效而 z-score 可能失效），穷举会淹没真信号且加重多重检验负担；先手工写 50-100 个，检验后再固化有效口径为模板 |
| 9 | 血缘存储与深度 | **DAG 存直接父节点（O(N)），完整链路用时遍历；深度上限 5 层** | 每节点冗余全链路会 O(N²)；深度过大的特征难解释、难归因、易过拟合（Alpha101 实践：有效因子嵌套通常 ≤3-4 层） |
| 10 | 因子入库门槛 | **多指标联合 + 分池递减**（详见下表） | 单一阈值会被钻空子（高 IC 但覆盖 5% 或换手 300%）；反馈池的作用是**测量衰减**不是再筛一遍 |

**因子入库门槛明细（决策 10）**：

| 指标 | 池1 (OOF) | 反馈池 (valid) | 说明 |
|---|---|---|---|
| RankIC 均值 | ≥ 0.02 | ≥ 0.01 | 反馈池衰减是常态，门槛放宽 |
| RankIR | ≥ 0.3 | ≥ 0.15 | IC 的稳定性 |
| Newey-West t | ≥ 2.0 | ≥ 1.5 | 修正自相关后的显著性 |
| 换手率 | ≤ 0.5 | ≤ 0.5 | 太高 = 交易成本吃掉收益 |
| 覆盖度 | ≥ 60% | ≥ 60% | 有效样本占比 |
| FDR 校正 | 通过 | 通过 | 多重检验（Benjamini-Hochberg） |

**OOS 不设门槛**——它是记录，不是筛选（研究人员看，Agent 看不到）。

---

## 14. 待评审的开放问题

1. ~~**开源 DSL 选型**~~ → **已拍板（决策 6）：expr_codegen 为主**。F3 阶段验证两点：① 是否支持 `group_*` 分组算子（不支持则自行补齐）② 1,967 万行现货 K 线的计算耗时
2. ~~**特征库规模上限**~~ → **已拍板（决策 7）**
3. ~~**多口径变体展开**~~ → **已拍板（决策 8）**
4. ~~**血缘展开深度**~~ → **已拍板（决策 9）**
5. ~~**因子入库门槛**~~ → **已拍板（决策 10）**

**当前无待评审开放问题。** 剩余事项均为 F3 选型验证阶段的执行项（见第 12 节落地路线）。

---

## 15. 变更日志

| 版本 | 日期 | 变更 |
|---|---|---|
| v0.1 | 2026-10-04 | 预研稿：三层架构 + 算子族 + PIT 继承 + 加密因子清单 |
| v0.2 | 2026-10-04 | **补充特征库（第4节）+ 因子库（第6节）**，明确"库≠注册表"；**表达式语言改为用开源不自研**（第5节）；5 个开放问题拍板写入第 13 节；落地路线重排（F1–F8，含 DSL 选型） |
| v0.3 | 2026-10-04 | **全部开放问题拍板关闭**：DSL 选型定为 expr_codegen（决策6）、特征库上限 500/1000（决策7）、多口径受控展开（决策8）、血缘 DAG+深度5（决策9）、因子入库多指标联合+分池递减（决策10）。第 14 节清空待评审项，文档进入可实施状态 |
| v0.4 | 2026-10-05 | **落地路线改为双线**：特征线（F1–F7，先行）+ 因子线（G1–G4，后置）；因子线启动条件 = 特征线 F4 + 模型协议提供标签；FeatureSpec 预留因子线所需字段 |
| v0.5 | 2026-10-05 | **F1-F3 已落地**。F3 选型实测后定案表达式引擎：expr_codegen 验证通（可读代码 + CSE + group_* 支持），但其执行模型与本项目面板算子模型互斥，引擎改用 **标准库 ast 白名单求值**（决策 1/6 修订，见 12.3 与第 13 节）。落地路线状态列更新（F1/F2/F3 完成）。 |
| v0.6 | 2026-10-05 | **特征线 F1-F7 全部落地**。F4 特征库（80 特征，5 类别，去重/体检/升版）；F5 血缘系统（DAG 直接父节点 + trace/impact/audit，depth≤5）；F6 全量特征池1 实算验收（交集网格语义）；F7 MCP 4 工具（墙在工具层验证）。引擎两项关键语义定案：①特征在**其全部输入的交集网格**上计算再对齐回面板（修 8h 资金费率在 1h 面板上的窗口错位）；②**无可用时间处不产生值**（修联合网格幽灵行）。测试合计 ~350 项。 |
| v0.7 | 2026-10-05 | **特征库扩到 204 个（8 类别）, 分组维度打通**。新增 `features/groups.py`：从 PIT 宇宙快照**逐日**构造四个分组维度（market_cap_tier / listing_age_tier / venue / quote），引擎在表达式用到时自动加载 —— 设计文档 3.2 的"加密版行业中性"由此可用（group_* 家族从 0 使用变成 23 个特征）。新增五类库文件：`intrabar.py`（K 线形态）、`regime.py`（市场状态）、`group_features.py`（分组截面）、`robust.py`（稳健口径）。三处工程修正：①**除零不再产生 inf**（DSL 出口拦截 + 除法置 NaN）—— 实测 shadow_ratio 曾静默产出 280 个 inf；②引擎批量 concat 中间列（修 167 列的 DataFrame 碎片化告警）；③数据缺失不再中断整个面板装载（basis/funding_mark_price 等只有部分交易所有数据）。 |
| v0.8 | 2026-10-05 | **补 6 个无状态预处理算子 + 特征库扩到 234**。新增算子（全部参数只来自当期截面或 trailing 窗口，不需要"记住"任何训练集数字）：`cs_residual`（截面中性化 OLS 残差）、`cs_rank_normal`（排名高斯化 inverse-normal）、`cs_winsorize_mad`（MAD 去极值）、`pp_soft_threshold`（软阈值去噪）、`pp_savgol`（**端点版** SG 平滑，设计文档 3.3 列的居中版会破坏未来不变性，已改造）、`pp_boxcox`（λ 按当期截面/trailing 窗口拟合，**禁止整段历史拟合**）。算子数 47 → 53。新增 `library/neutral.py`（30 个特征：残差/高斯化/MAD/去噪/SG/Box-Cox 六种口径）。**"无状态 vs 有状态"的分界由此明确**：特征底座只做无状态变换（服务端可原样重算）；需要携带参数的变换（标准化器/PCA）属模型研究协议，必须随模型保存 —— 这是防止训练/服务静默漂移的架构分界线。 |
