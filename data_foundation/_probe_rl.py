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

info = mcp.tool_rl_env_info(rid)
print(f"环境: state_dim={info['state_dim']} actions={info['n_actions']} "
      f"time={info['n_time']} clip={info['clip_reward']} lam={info['turnover_penalty']}")

t = mcp.tool_train_rl(rid, hidden=32, epochs=3, n_steps=128, batch=32, seed=7)
print(f"训练: model_id={t['model_id']} state_dim={t['state_dim']} "
      f"actions={t['n_actions']} rewards={t['train_rewards']} "
      f"improved={t['reward_improved']}")

e = mcp.tool_eval_rl_policy(t["model_id"])
mm = e["metrics"]
print(f"评估: bars={e['n_bars']} reward_total={e['reward_total']:.4f} "
      f"total={mm.get('total_return')} sharpe={mm.get('sharpe')} "
      f"mdd={mm.get('max_drawdown')} cost={mm.get('total_cost')}")

# 换手惩罚对照
t2 = mcp.tool_train_rl(rid, hidden=32, epochs=3, n_steps=128, batch=32, seed=7,
                       turnover_penalty=0.02, model_id="rl-hi-lam")
print(f"对照(高换手惩罚 λ=0.02): rewards={t2['train_rewards']}")
print("DONE")