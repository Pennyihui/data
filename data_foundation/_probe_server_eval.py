# -*- coding: utf-8 -*-
"""_probe_server_eval.py — 验证 #4: valid/oos 服务端评估通道 (只经 MCP 工具)"""
import data_foundation.mcp_server as mcp
import data_foundation.eval_service as es
from data_foundation.evaluation.metrics import build_metrics

print(f"MCP 工具数: {len(mcp.TOOLS)}")
srv = [t["name"] for t in mcp.TOOLS if "evaluate" in t["name"] or "server" in t["name"]]
print("服务端评估工具:", srv)

# oof: 建样本 + 训练 + 注册模型 (训练只发生在开发池)
mcp.tool_bind_pool("oof")
pk = mcp.tool_load_price_panel(start="2021-01-01", end="2021-12-31")
mcp.tool_compute_labels(pk["panel_key"], "ret_10d_w")
b = mcp.tool_build_training_samples(["mom_zscore_24h", "vol_24h"], "ret_10d_w",
                                    pk["panel_key"], train_len="150D",
                                    test_len="60D", step="60D",
                                    start="2021-01-01", end="2021-12-31")
t = mcp.tool_train_model(b["run_id"], "linear", "ret_10d_w", "reg", True,
                         {"alpha": 1.0}, "probe-model")
print(f"\noof 训练并注册: model_hash={t['model_hash'][:12]} v{t['version']}")

# 构造一个 valid 提交 (带 features + label_name, 服务端据此自取数)
payload = {"model_name": "probe-model", "label_name": "ret_10d_w",
           "features": ["mom_zscore_24h", "vol_24h"],
           "feature_fingerprint": "server-side", "note": "probe"}
eid = es.submit("valid", "probe-run", "hash-probe", payload)
print(f"提交 valid: {eid}")

print("\n=== #4 服务端评估 (原来这一步没有工具入口) ===")
try:
    r = mcp.tool_evaluate_submission(eid)
    m = r["metrics"]
    print(f"评估完成: status={r['status']} pool={r['pool']}")
    print(f"  prediction={m['prediction']}")
    print(f"  significance={m['significance']}")
    print(f"  trading keys={list(m['trading'])[:6]}")
    print(f"  extra={m.get('extra')}")
except Exception as e:
    import traceback
    print(f"FAIL: {type(e).__name__}: {e}")
    traceback.print_exc(limit=4)

print("\n=== 三池纪律仍然成立 ===")
st = es.status(eid)
print(f"状态: {st['status']}")
fb = es.read_feedback(eid)


def g(d, k, w=9, p=4):
    v = d.get(k)
    return f"{v:{w}.{p}f}" if isinstance(v, (int, float)) else f"{'n/a':>{w}}"


print(f"取结果 1 次: views={fb['views_used']}/{fb['views_limit']} "
      f"IC={g(fb['metrics']['prediction'], 'ic_mean')} "
      f"DSR={g(fb['metrics']['significance'], 'dsr', 8, 3)}")
try:
    es.read_feedback(eid); es.read_feedback(eid)
    print("限次: 失败(应被拒)")
except PermissionError as e:
    print(f"限次生效: {str(e)[:44]}")
print(f"RL 也计入 N: {mcp.tool_trial_count()['n_trials']}")