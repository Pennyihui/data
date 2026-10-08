# 强化学习协议设计（Env 封装 → 奖励函数 → 训练 → 评估）

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.1 |
| 日期 | 2026-10-07 |
| 状态 | 设计提案，待评审（评审通过后按 §10 实现） |
| 前置 | `backtest-engine-design.md`（回测引擎）、`supervised-learning-protocol-design.md`（监督学习，阶段2）、`evaluation-protocol-design.md`（三层指标）、`research-chain-first-run-20261007.md`（首次运行报告） |
| 定位 | 研究链第四环：目标 → 标签 → 监督学习 → **强化学习**（执行交易操作） |

---

## 0. 一句话设计

**强化学习协议 = Env 封装（复用回测引擎）+ 奖励函数库 + 训练循环 + 与监督学习同一套评价。**

**核心决定：Env 的底层就是现有的 `BacktestEngine`** —— 不是重写一个模拟器。这样"训练环境 = 回测 = 实盘"三者同路径，与回测引擎原则 2 一致。

---

## 1. 要解决的问题

1. **RL 需要可 reset、可 step 的环境**，而现有回测引擎是"一次 run 到底"的事件循环。**缺口在控制流，不在模拟逻辑**。
2. **奖励函数要与成本同源**：奖励若用毛收益，策略会学出不可执行的收益（监督学习那轮已经踩到：极端日收益 +243 倍）。
3. **RL 的评估必须复用三层指标**：否则 RL 的"高分"和监督学习的 Sharpe 不可比。
4. **RL 有超参搜索需求**（比监督学习强得多：lr / γ / 熵系数 / 网络结构），而**超参搜索会大量消耗试验次数 N** —— DSR 必须如实计入，否则多重检验修正失效。

---

## 2. 六条设计原则

**原则1：Env 复用回测引擎，不重写模拟器。**
`step(action)` = 把 action 转成 `OrderIntent` → 走**同一个** ExecutionEngine/Portfolio → 拿到成交与权益。这样 RL 学到的策略在回测里可直接复现（不需要"再实现一次回测"）。

**原则2：奖励 = 成本后收益，且默认缩尾。**
`reward = clip(Δlog(equity), -r_clip, +r_clip) - λ × turnover`。缩尾（clip）是**必需**的：首次运行报告里单期 243 倍收益会让任何梯度估计爆掉。

**原则3：动作空间 = 组合权重（与监督学习同构）。**
action = 目标权重向量 → RiskEngine 修正 → OrderIntent。这样 RL 与监督学习共用同一套仓位映射语义和同一套约束。

**原则4：状态只能来自当期可见数据。**
state = 因子面板当期值 + 当前持仓 + 现金。**物理上**不含未来——Env 的 state 构造只接收面板查询接口（同 PanelSignal 的精确查表语义）。

**原则5：训练/评估与三池纪律一致。**
训练只在 oof；valid/oos 的 RL 策略评估走服务端。

**原则6：RL 的每次训练都计入试验次数 N。**
RL 超参搜索是试验次数的主要来源之一，DSR 必须如实反映。

---

## 3. 架构

```
     ┌─────────────────── Env (本层新建) ───────────────────┐
     │  reset() -> state                                   │
     │  step(action) -> (state', reward, done, info)       │
     │       ↓                                             │
     │  action(权重) -> OrderIntent -> RiskEngine          │
     │       -> ExecutionEngine (复用!) -> Portfolio       │
     │       -> reward = 成本后收益 - λ·换手 (缩尾)         │
     └──────────────────────────────────────────────────────┘
                          ↓
              训练 (PPO / DQN 等, torch)
                          ↓
              策略权重序列 -> 三层指标 (复用 evaluation)
                          ↓
              与监督学习**同一张成绩单**比较
```

---

## 4. Env 规格（`rl/env.py`）

```python
class PortfolioEnv:
    def __init__(self, panel, factor_panel, *, cost=None, reward_scale=1.0,
                 turnover_penalty=0.0, clip_reward=0.05, max_weight=0.1,
                 max_gross=1.0, top_k=None, seed=0):
        ...
    def reset(self, start=None) -> np.ndarray:   # -> state
    def step(self, action: np.ndarray) -> tuple[np.ndarray, float, bool, dict]:
        ...
```

| 要素 | 设计 |
|---|---|
| state | `[因子当期值(截面), 当前权重, 现金]` — 只含当期可见 |
| action | 目标权重向量 (经 RiskEngine 限幅/限杠杆) |
| reward | `clip(log(equity_t/equity_{t-1}), ±clip) - λ·turnover` |
| done | 到达窗口末尾 |
| 确定性 | 同 seed + 同初始 → 同轨迹（可复现） |
| 资产数固定 | 与回测引擎契约一致（见首次运行报告缺陷3） |

**状态泄漏防护**：state 构造只允许通过 `PanelSignal` 式的**精确查表**（查不到=NaN，不前向填充）—— 沿用回测引擎的第二道闸。

---

## 5. 奖励函数库（`rl/rewards.py`）

| 奖励 | 公式 | 适用 |
|---|---|---|
| `log_return` | `clip(Δlog(equity), ±clip)` | **默认**，稳健 |
| `net_return` | `Δequity/equity - cost` | 直觉但对尾部敏感 |
| `sharpe_delta` | `Δequity / (σ·√T)` - 惩罚 | 风险调整 |
| `drawdown_penalized` | `log_return - λ·max(0, dd - dd_prev)` | 回撤厌恶 |

**成本同源**：奖励里的 cost 来自**同一个** `CostModel`（taker+滑点），与回测、标签三处一致。

---

## 6. 训练（`rl/trainer.py`）

- 算法：先 PPO（stable-baselines3 或自实现 torch PPO），Reinforce 作为对照。
- 超参：`lr / gamma / clip / n_steps / batch`。
- **每次训练写实验账本**（原则6），`trial_count` 自动纳入 RL 试验。

---

## 7. 与既有体系的关系

| | 复用 |
|---|---|
| 回测引擎 | ExecutionEngine / Portfolio / RiskEngine / CostModel / BacktestData |
| 因子面板 | FeatureEngine（RL 的 state 直接吃因子面板） |
| 三层指标 | evaluation.evaluate_candidate（同一张成绩单） |
| 模型注册 | training.registry（RL 策略也注册成 ModelArtifact） |
| 实验账本 | training.experiments（trial_count → DSR） |

**本层不重新实现**：撮合、成本、净值、PIT 校验、指标 —— 全部用现成的。

---

## 8. 不做什么

- 不做 tick/L2 微观结构（无数据）。
- 不做多策略资金分配。
- 不做在线学习/实盘（LiveEngine 留位）。
- 不重写回测引擎（Env 复用它）。

---

## 9. 决策记录

| # | 决策 | 默认 | 理由 |
|---|---|---|---|
| R1 | Env 底层 | 复用 BacktestEngine 组件 | 训练=回测=实盘同路径 |
| R2 | 奖励默认 | log_return + 缩尾 clip=0.05 | 尾部 243 倍会让梯度爆 |
| R3 | 动作空间 | 组合权重（过 RiskEngine） | 与监督学习同构 |
| R4 | state | 因子当期 + 持仓 + 现金 | 物理不含未来 |
| R5 | 算法 | 先 PPO | 样本效率高，稳定 |
| R6 | RL 计入 N | 是 | 超参搜索是试验次数主要来源 |

---

## 10. 实现顺序

1. `rl/env.py` —— PortfolioEnv（复用回测引擎；reset/step/确定性测试）
2. `rl/rewards.py` —— 四种奖励 + 成本同源测试
3. `rl/trainer.py` —— PPO 训练循环 + 写实验账本
4. MCP 工具：`train_rl` / `eval_rl_policy`
5. 验收：确定性测试 + 与监督学习同一张成绩单对比

---

## 11. 验收标准

- **确定性**：同 seed 两次训练轨迹逐位相同。
- **同路径**：RL 策略的权重序列喂给回测引擎，净值与 Env 内部一致（同一路径）。
- **奖励无泄漏**：state 构造对未来改动噪声不变（未来不变性）。
- **可比性**：RL 成绩单与监督学习同格式（同一 evaluate_candidate）。
- **N 如实**：RL 训练计入 trial_count。