# -*- coding: utf-8 -*-
"""_probe_rl.py — 强化学习首次运行 (只经 MCP 工具)"""
import data_foundation.mcp_server as mcp

mcp.tool_bind_pool("oof")
print(f"MCP 工具数: {len(mcp.TOOLS)}")
print("RL 工具:", [t["name"] for t in mcp.TOOLS if t["name"].startswith(("train_rl", "eval_rl", "rl_env"))])

FEATS = ["mom_zscore_24h", "vol_24h"]
pk = mcp.tool_load_price_panel(start="2021-01-01", end="2022-06-30")
mcp.tool_compute_labels(pk["panel_key"], "ret_10d_w")
b = mcp.tool_build_training_samples(FEATS, "ret_10d_w", pk["panel_key"],
                                    train_len="365D", test_len="90D", step="90D",
                                    start="2021-01-01", end="2022-06-30")
rid = b["run_id"]
print(f"样本 run_id={rid} folds={b['n_folds']}")

print("\n=== RL: 结构化动作 (#3) vs 连续权重对照 ===")
info = mcp.tool_rl_env_info(rid)
print(f"环境: state_dim={info['state_dim']} actions={info['n_actions']} "
      f"time={info['n_time']} mode={info['action_mode']}")

t = mcp.tool_train_rl(rid, hidden=32, epochs=4, n_steps=128, batch=32, seed=7,
                      action_mode="scores", top_k=10)
print(f"scores模式: rewards={t['train_rewards']} improved={t['reward_improved']}")
e = mcp.tool_eval_rl_policy(t["model_id"])
mm = e["metrics"]
print(f"  评估: bars={e['n_bars']} mode={e['action_mode']} bankrupt={e['bankrupt']} "
      f"total={mm.get('total_return'):.4f} sharpe={mm.get('sharpe'):.3f} "
      f"trustworthy={mm.get('trustworthy')}")

t_w = mcp.tool_train_rl(rid, hidden=32, epochs=4, n_steps=128, batch=32, seed=7,
                        action_mode="weights", model_id="rl-weights-cmp")
print(f"weights对照: rewards={t_w['train_rewards']} improved={t_w['reward_improved']}")

print("\n=== 子集验证 (50 资产) ===")
all_assets = [a for a in sorted(set(mcp._LABEL_CACHE["ret_10d_w"].values.index.get_level_values(0)))]
subset = all_assets[:50]
t_s = mcp.tool_train_rl(rid, hidden=32, epochs=4, n_steps=128, batch=32, seed=7,
                        action_mode="scores", top_k=10, assets=subset,
                        model_id="rl-subset50")
print(f"50资产: n_actions={t_s['n_actions']} rewards={t_s['train_rewards']} "
      f"improved={t_s['reward_improved']}")

print("\n=== 试验计数 (RL 是否计入 N) ===")
print("N =", mcp.tool_trial_count()["n_trials"])