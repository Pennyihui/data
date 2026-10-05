# 会话交接文档 (Session Handoff)

> 交接时间: 2026-10-05 (晚, 特征线完成后更新)
> 交出会话: `session-50ee7933-95b0-48f7-b7a3-1e5234c552b5`
> 日志地址: `C:\Users\Evan\.dsh\sessions\--D-Documents-z_python_data_analy-Quent-workspace_0817--\session-50ee7933-95b0-48f7-b7a3-1e5234c552b5.jsonl`
> 接手会话: 请读本文件后继续工作，不要重复已完成的部分

---

## ★ 特征线 F1-F7 完成状态 (2026-10-05 晚, 全部落地并推送)

| 阶段 | 内容 | 交付物 | 测试 |
|---|---|---|---|
| F1 | 算子库 | `features/operators.py` — 49 算子 (ts 18 / cs 6 / group 6 / pp 17) + 四族注册表 + 表达式 lint + PIT 静态审计 | `_test_operators.py` 157 (未来不变性 49/49×2 面板) |
| F2 | 字段层+PIT 引擎 | `fields.py` — 34 字段注册表 + `load_panel`(时间墙/宇宙门控/主交易所) + 可用时间传播 (int64ns 精确) + `assert_no_leakage` | `_test_fields.py` 52 (真实数据) |
| F3 | DSL 选型验证+引擎 | `_dsl_probe.py`(expr_codegen 实测) + `features/dsl.py`(ast 白名单求值+CSE+血缘+PIT 传播) | `_test_dsl.py` 66 |
| F4 | 特征库 | `features/{specs,registry,catalog,engine}.py` + `library/{price,derivatives,liquidity,quality,cross_asset}.py` | `_test_library.py` 32 |
| F5 | 血缘系统 | `features/lineage.py` — DAG 直接父节点 / trace / impact / audit (depth≤5) | (并入 library 测试) |
| F6 | 首批特征 | **80 个** (price 30 / derivatives 32 / liquidity 6 / quality 6 / cross_asset 6), 全部池1 实算, 无空特征, 无重复, depth≤4 | (同上) |
| F7 | MCP 工具 | `mcp_server.py` +4 工具: list_features / describe_feature / compute_features / feature_catalog | `_test_mcp_features.py` 13 |

**测试合计 ~350 项全过**; 提交: 5a600280 (F1+F2) → a383fc0e (F3) → 94330362 (F4) → (F5-F7)。

### 后续会话必须知道的四个关键定案 (都在代码注释/设计文档 v0.6 里)
1. **引擎是标准库 ast 白名单求值**, 不是 expr_codegen —— 后者执行模型与面板算子互斥
   (实测 `_dsl_probe.py`: 喂 MultiIndex 面板 KeyError, 喂平表 NameError), 但其"可读代码
   + CSE"的审计价值真实, 保留为可选审计后端。
2. **特征在其全部输入的交集网格上计算**, 再对齐回联合面板 —— 否则 8h 资金费率在
   1h 面板上的窗口语义全错 (ts_decay_linear(funding,21) 会全空)。
3. **无可用时间处不产生值** (dsl.evaluate 的 v_out.where(av_out.notna())) —— 联合网格
   的幽灵行 (只有 funding 没有 close 的时刻) 不许凭空出值。
4. PIT 传播窗口必须与算子取数窗口严格一致 (`dsl._node_avail`); ts_rank 用 shift 累加
   实现 (rolling.apply 慢 60 倍); rolling.skew/kurt 用窗口和展开 (pandas 的跨窗累积
   算法不满足未来不变性)。

### 因子线 (G1-G4) 的启动条件已满足一半
特征线 F4 ✅; 还差**模型协议提供标签**。G1 骨架可直接开工。

---

## 0. 项目根与关键路径

```
项目根:  D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline
包目录:  Data_pipeline\data_foundation\data_foundation\
数据目录: Data_pipeline\data_foundation\data\{raw,l1,l2\certified}
Python:  E:\Anaconda3\python.exe -X utf8  (必须 -X utf8)
git:     Data_pipeline 目录, remote=github.com/Pennyihui/data.git, main 分支
推送:    用 D:\Documents\z_python_data_analy\Quent\workspace_0817\_push_retry.ps1
         (单次 push >120 文件/~100MB 会卡死代理, 需按 ~120 文件分片)
```

## 1. 已完成的基础设施（勿重做）

### 1.1 数据底座 (data_foundation) — 全部完成
- 40 个认证数据集, 1.2 亿行; 全库 parquet 0 损坏 (`_scan_corrupt.py`)
- 时间墙: `pool_registry.py` — 六池 (oof 2018-2023 / gap1 / valid 2024-2025.6 / gap2 / oos 2025.7-2026.9 / rolling_oos), 双向钳制 as_of, 因子屏蔽 (FACTOR_AVAILABILITY), 污染源硬屏蔽 (REVISION_CONTAMINATED = macro_daily/cm_asset_daily/btc_network_daily; NO_PIT_COLUMN = stablecoin_supply/flows/dex_volume)
- OOS 账本: `oos_ledger.py` — Agent 盲(代码级 PermissionError) + 人审通道留痕 + eval_count(DSR)
- MCP: `mcp_server.py` 10 工具, 会话绑池不可重绑
- universe_membership 已回填至 2017-07: 830,605 行 / 3352 日 (2017-08-01 ~ 2026-10-04)
- 测试: `_test_pool_wall.py` 20 项全过; 体检 `_coverage_scan.py` / `_venue_scan.py`

### 1.2 设计文档（均已定稿，在 data_foundation/docs/）
- `research-pools-timewall-design.md` — 时间墙 v1.0（决策全拍板）
- `feature-foundation-design.md` — **特征底座 v0.4（当前工作）**，决策 1-10 全拍板，落地路线为**双线**：
  - **特征线 F1-F7 先行**（当前任务）
  - 因子线 G1-G4 后置（启动条件 = 特征线 F4 完成 + 模型协议提供标签）

## 2. 当前任务：特征线实现（用户已批准"先实现特征线"）

**重要：F1 刚开始写，`operators.py` 的 write 调用被中断（tool call aborted），文件可能不存在或不完整。接手后先检查 `data_foundation/features/` 目录实际状态，再重写。**

### 2.1 特征线 F1-F7 任务清单

| 阶段 | 内容 | 状态 |
|---|---|---|
| **F1** | 算子库: 三族 `ts_*`/`cs_*`/`group_*` + 预处理 `pp_*`，含注册表 TS_/CS_/GROUP_/PP_OPERATORS + ALL_OPERATORS | **刚开始，可能未写入** |
| **F2** | 字段层 + PIT 引擎: reader 取数、data_available_at 自动推导（= 输入中最大者）、PoolScope 继承、assert_no_leakage 泄漏自检 | 未开始 |
| **F3** | DSL 选型验证: expr_codegen（验证 group_* 支持 + 1,967 万行性能；不支持分组算子则自行补齐） | 未开始 |
| **F4** | 特征库: 注册表 + library/ 按来源组织(price/derivatives/liquidity/quality/onchain) + catalog 多路索引 + 去重 | 未开始 |
| **F5** | 血缘系统: DAG 存直接父节点(O(N))、完整链路用时遍历、深度≤5、版本管理 | 未开始 |
| **F6** | 首批 50-100 个特征填充特征库 | 未开始 |
| **F7** | MCP 工具扩展: list_features / describe_feature / compute_features | 未开始 |

### 2.2 F1 算子库设计要点（中断前的设计，按此继续）

- **文件**: `data_foundation/data_foundation/features/operators.py`
- **PIT 纪律**: 所有时序算子只用当前及过去数据（shift 后计算）；禁止居中窗口；禁止 forward-fill 未来
- **算子清单**:
  - `ts_*`: ts_mean, ts_std, ts_sum, ts_min, ts_max, ts_median, ts_delta, ts_pct_change, ts_rank(滚动pct排名), ts_zscore, ts_corr, ts_decay_linear(线性衰减加权), ts_skew, ts_kurt, ts_ewma
  - `cs_*`: cs_rank, cs_zscore, cs_normalize, cs_winsorize（截面算子按 MultiIndex 的 "time" 层分组）
  - `group_*`: group_rank, group_neutralize, group_zscore, group_mean, group_std（按 [time, group] 双键分组）
  - `pp_*`: pp_log, pp_sqrt, pp_diff, pp_pct_change, pp_frac_diff(分数阶差分), pp_quantile_bucket, pp_ema, pp_is_missing, pp_is_outlier
- **末尾注册表**: TS_OPERATORS / CS_OPERATORS / GROUP_OPERATORS / PP_OPERATORS 四个 dict + ALL_OPERATORS 合并（供 DSL/引擎遍历）
- **面板约定**: MultiIndex (instrument, time)；截面/分组算子靠 `x.index.get_level_values("time")` 分组
- 加密分组维度: sector(板块)/market_cap/venue/chain/quote/listing_age（分组映射接 CoinGecko categories，不手工维护）

### 2.3 F2 PIT 引擎核心规则（设计已定，实现时遵守）

> **特征在事件时间 t 的值，其 data_available_at = 该特征所用全部输入中最大的 data_available_at。**

- ts_rank(funding_rate,168) 在 t 时刻 → 可用时间 = avail(funding_rate[t])
- cs_rank(close) 在 t 时刻 → 可用时间 = 当日最后一根 bar 的 close_time
- 引擎自动推导，不让使用者手写；`assert_no_leakage`: 特征的 data_available_at >= 输入最大可用时间
- 必须通过 `PoolScope` 继承时间墙（双向钳制 as_of + 因子屏蔽 + 宇宙门控）
- reader 接口: `load_candles(venue, instrument, interval, as_of, cols, market_type, scope)` 返回 open_time_utc + data_available_at + 列；`load_derivatives(venue, instrument, dataset, as_of, scope)`；`load_universe(as_of, layer, scope)`

### 2.4 已拍板决策（不要重新讨论）
1. DSL = expr_codegen（开源）+ Python 函数混合，不自研 parser
2. 板块分类接 CoinGecko categories
3. 先不落盘缓存
4. 检验(IC/FDR)放模型协议侧，特征底座只给接口、不知道标签存在
5. 首批 50-100 个特征
6. 特征库软上限500/硬1000，每加100个强制体检
7. 多口径变体受控展开+白名单模板，F4 阶段不做自动展开
8. 血缘 DAG 存直接父节点，深度≤5（Alpha101 实践嵌套≤3-4层）
9. 因子入库门槛: RankIC≥0.02/IR≥0.3/NW-t≥2.0/换手≤0.5/覆盖≥60%/FDR通过（池1）；反馈池递减(0.01/0.15/1.5)；OOS 不设门槛
10. 因子线后置（G1-G4），特征线先行

## 3. 环境注意事项

- PowerShell 陷阱: `echo "x" >> .gitignore` 会通配符展开；改 .gitignore 用 `Set-Content -Encoding UTF8` 逐行写
- 代理不稳: git push 用 `_push_retry.ps1`（退避重试）；raw.githubusercontent.com 经代理不通，抓 GitHub 文件用 web_search 或 Invoke-WebRequest 走系统代理
- 修订污染: macro_daily/cm_asset_daily/btc_network_daily 历史是事后回填（lag 漂移 3-4千天），已在 pool_registry 硬屏蔽；stablecoin_supply/flows/dex_volume 无 data_available_at 列也屏蔽
- 数据现状: 核心价格/衍生品因子 PIT 干净（lag≤1h）；token_transfer 只有 62 天（链上因子仅滚动 OOS 可用）
- 中文路径: git 已设 core.quotepath false

## 4. 会话记忆 vault 提示

用 memory_recall 可查已存的关键经验（搜索 "data_foundation" / "timewall" / "silent-staleness" / "chunked-push"）。本次会话新增的关键教训已入库：
- 四类静默停更根因（冻结批次号/无nightly源/非原子写/rebuild假成功）
- chunked-push（>120文件卡代理）
- timewall 体系决策记录

## 5. 接手后的第一步

1. 检查 `data_foundation/data_foundation/features/` 目录现状（`operators.py` 是否存在/完整）
2. 若缺失，按 2.2 的设计重写 F1 算子库
3. 然后依次 F2(PIT引擎) → F3(DSL验证) → F4(特征库) → F5(血缘) → F6(首批特征) → F7(MCP)
4. 每完成一个阶段: py_compile 检查 + 小规模冒烟（用 BTC-USDT 真实数据跑通）+ git commit + `_push_retry.ps1` 推送
5. 完成后更新 `feature-foundation-design.md` 的落地路线状态列
