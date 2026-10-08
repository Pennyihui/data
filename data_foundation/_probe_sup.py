# -*- coding: utf-8 -*-
"""_probe_sup.py — 监督学习系统性研究 (只经MCP 工具)

维度: 标签口径 x 模型家族 x 目标函数(loss) x 优化器 x 仓位映射
"""
import data_foundation.mcp_server as mcp

STEP = []


def run(name, fn, *a, **kw):
    try:
        r = fn(*a, **kw)
        STEP.append((name, "OK", r))
        return r
    except Exception as e:  # noqa: BLE001
        STEP.append((name, "BLOCKED", f"{type(e).__name__}: {e}"))
        print(f"  [!!] {name}: {type(e).__name__}: {str(e)[:110]}")
        return None


mcp.tool_bind_pool("oof")
FEATS = ["mom_zscore_24h", "vol_24h", "ret_14d", "mom_rank_24h"]
pk = mcp.tool_load_price_panel(start="2021-01-01", end="2021-12-31")
print(f"面板: {pk['n_rows']:,} 行 {pk['n_assets']} 资产 {pk['time_range']}",
      flush=True)

LABELS = ["ret_10d", "ret_10d_w", "sign_10d_w"]
for lb in LABELS:
    mcp.tool_compute_labels(pk["panel_key"], lb)

# 样本: 每个标签一份 (窗口收窄以控制耗时)
SAMPLES = {}
for lb in LABELS:
    r = run(f"samples[{lb}]", mcp.tool_build_training_samples, FEATS, lb,
            pk["panel_key"], train_len="150D", test_len="60D", step="60D",
            start="2021-01-01", end="2021-12-31")
    if r:
        SAMPLES[lb] = r["run_id"]
    print(f"  样本 {lb}: {'ok' if r else 'FAIL'}", flush=True)
print(f"样本集: {len(SAMPLES)}/{len(LABELS)} 标签可用", flush=True)

# 模型矩阵: 家族 x loss x optimizer x task (有界)
CONFIGS = [
    ("baseline", "reg", {}, "baseline"),
    ("linear", "reg", {"alpha": 1.0}, "ridge"),
    ("linear", "cls", {"alpha": 1.0}, "logreg"),
    ("gbdt", "reg", {"n_estimators": 60, "max_depth": 3}, "gbdt"),
    ("nn", "reg", {"loss": "mse", "optimizer": "adam",
                   "epochs": 8, "hidden": 16}, "nn-mse-adam"),
    ("nn", "reg", {"loss": "huber", "optimizer": "adam",
                   "epochs": 8, "hidden": 16}, "nn-huber-adam"),
    ("nn", "reg", {"loss": "mse", "optimizer": "sgd", "epochs": 8,
                   "hidden": 16, "lr": 0.05}, "nn-mse-sgd"),
    ("nn", "reg", {"loss": "mse", "optimizer": "rmsprop", "epochs": 8,
                   "hidden": 16}, "nn-mse-rms"),
]

results = []
for lb, rid in SAMPLES.items():
    for fam, task, params, tag in CONFIGS:
        mid = f"{tag}@{lb}"
        t = run(f"train {mid}", mcp.tool_train_model, rid, fam, lb, task,
                False, params, mid)
        if not t:
            continue
        mcp.tool_run_backtest(mid)
        e = run(f"eval {mid}", mcp.tool_evaluate_candidate, mid, lb)
        if e:
            p, tr, s = e["prediction"], e["trading"], e["significance"]
            results.append(dict(label=lb, model=tag, family=fam, task=task,
                                ic=p.get("ic_mean"), ric=p.get("rank_ic_mean"),
                                icir=p.get("icir"), hit=p.get("hit_rate"),
                                qspread=p.get("quantile_spread"),
                                sharpe=tr.get("sharpe"), ann=tr.get("ann_return_net"),
                                mdd=tr.get("max_drawdown"), to=tr.get("annualized_turnover"),
                                psr=s.get("psr"), dsr=s.get("dsr"),
                                passed=e["decision"]["passed"]))
        print(f"  完成 {mid}", flush=True)

print("\n" + "=" * 122)
print("监督学习矩阵结果 (目标 trend_10d, oof 开发池, walk-forward)")
print("=" * 122)
hdr = (f"{'标签':<17}{'模型':<16}{'IC':>9}{'rankIC':>9}{'ICIR':>8}{'命中':>7}"
       f"{'Sharpe':>8}{'年化':>9}{'回撤':>8}{'换手':>7}{'DSR':>7}{'过闸':>5}")
print(hdr)
print("-" * 122)


def f(x, w=9, p=4):
    return f"{x:{w}.{p}f}" if isinstance(x, (int, float)) and x == x else \
        f"{'n/a':>{w}}"


for r in sorted(results, key=lambda z: -(z["ric"] if isinstance(z["ric"], float) else -9)):
    print(f"{r['label']:<17}{r['model']:<16}{f(r['ic'])}{f(r['ric'])}"
          f"{f(r['icir'], 8, 3)}{f(r['hit'], 7, 3)}{f(r['sharpe'], 8, 3)}"
          f"{f(r['ann'])}{f(r['mdd'], 8, 3)}{f(r['to'], 7, 1)}"
          f"{f(r['dsr'], 7, 3)}{'是' if r['passed'] else '否':>5}")
print("-" * 122)
best_ric = max((r for r in results if isinstance(r["ric"], float)),
               key=lambda z: z["ric"], default=None)
if best_ric:
    print(f"最佳 rank-IC: {best_ric['model']}@{best_ric['label']} = "
          f"{best_ric['ric']:.4f} (IC={best_ric['ic']:.4f})")
print(f"过闸模型数: {sum(1 for r in results if r['passed'])}/{len(results)}")
print(f"试验计数 N = {mcp.tool_trial_count()['n_trials']} (注: 训练不经账本, N 仍偏小)")
print("=" * 122)
blocked = [s for s in STEP if s[1] == "BLOCKED"]
print(f"走通 {len(STEP) - len(blocked)} / 卡住 {len(blocked)}")