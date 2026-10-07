# -*- coding: utf-8 -*-
"""_probe_mcp.py — 以 Agent 身份**只通过 MCP 工具**走一遍完整研究链

规则: 不 import 设施模块干活, 只调 mcp_server 的 tool_* 函数; 不改底座代码。
"""
import data_foundation.mcp_server as mcp

STEP = []


def run(name, fn, *a, **kw):
    try:
        r = fn(*a, **kw)
        STEP.append((name, "OK", r))
        print(f"  [OK] {name}")
        return r
    except Exception as e:  # noqa: BLE001
        STEP.append((name, "BLOCKED", f"{type(e).__name__}: {e}"))
        print(f"  [!!] {name}\n       {type(e).__name__}: {str(e)[:160]}")
        return None


def main():
    print(f"MCP 工具数: {len(mcp.TOOLS)}")
    names = [t["name"] for t in mcp.TOOLS]
    need = ["load_price_panel", "compute_labels", "build_training_samples",
            "train_model", "run_backtest", "evaluate_candidate"]
    print("研究链必需工具:", {n: (n in names) for n in need})

    print("\n=== 0) 绑定开发池 ===")
    run("bind_pool(oof)", mcp.tool_bind_pool, "oof")

    print("\n=== 1) 目标与标签 ===")
    run("list_objectives", mcp.tool_list_objectives)
    run("list_labels", mcp.tool_list_labels)

    print("\n=== 2) 价格面板 (2021 窗口先验证) ===")
    pk = run("load_price_panel(2021)", mcp.tool_load_price_panel,
             start="2021-01-01", end="2021-12-31")
    if pk:
        print(f"       panel_key={pk['panel_key']} rows={pk['n_rows']:,} "
              f"assets={pk['n_assets']} range={pk['time_range']}")
    run("compute_labels(ret_10d)", mcp.tool_compute_labels,
        pk["panel_key"], "ret_10d")
    run("compute_labels(sign_10d)", mcp.tool_compute_labels,
        pk["panel_key"], "sign_10d")

    print("\n=== 3) 训练样本 (walk-forward 折) ===")
    feats = ["mom_zscore_24h", "vol_24h", "ret_14d", "mom_rank_24h"]
    b = run("build_training_samples", mcp.tool_build_training_samples, feats,
            "ret_10d", pk["panel_key"], train_len="150D", test_len="60D",
            step="60D", start="2021-01-01", end="2021-12-31")
    if not b:
        return
    rid = b["run_id"]
    print(f"       run_id={rid} folds={b['n_folds']} "
          f"common_grid={b['n_common_grid']:,}")

    print("\n=== 4) 训练: 家族 x 目标函数 ===")
    run("linear/regression", mcp.tool_train_model, rid, "linear", "ret_10d",
        "regression", False, {"alpha": 1.0}, "m-lin-reg")
    run("linear/classification", mcp.tool_train_model, rid, "linear", "ret_10d",
        "classification", False, {"alpha": 1.0}, "m-lin-cls")
    run("gbdt/regression", mcp.tool_train_model, rid, "gbdt", "ret_10d",
        "regression", False,
        {"n_estimators": 60, "max_depth": 3, "learning_rate": 0.05},
        "m-gbdt-reg")
    run("baseline/regression", mcp.tool_train_model, rid, "baseline", "ret_10d",
        "regression", False, {}, "m-base-reg")

    print("\n=== 5) 回测: 两种仓位映射 ===")
    bt1 = run("rank_linear", mcp.tool_run_backtest, "m-lin-reg")
    bt2 = run("top_n(10, 多空)", mcp.tool_run_backtest, "m-lin-reg",
              "top_n", 10, True, 5)
    for nm, bt in (("rank_linear", bt1), ("top_n", bt2)):
        if bt:
            mm = bt["metrics"]
            print(f"       {nm}: assets={bt['n_assets']} "
                  f"(剔除极端 {bt['n_dropped_extreme']}) bars={bt['n_bars']} "
                  f"total={mm.get('total_return'):.4f} "
                  f"cagr={mm.get('cagr')} sharpe={mm.get('sharpe')} "
                  f"mdd={mm.get('max_drawdown'):.4f} "
                  f"cost={mm.get('total_cost'):.4f} "
                  f"turnover={mm.get('total_turnover'):.1f} "
                  f"病态={bt['pathological_equity']}")

    print("\n=== 6) 三层评价 ===")
    rows = []
    for mid, label in (("m-lin-reg", "linear/reg"), ("m-lin-cls", "linear/cls"),
                       ("m-gbdt-reg", "gbdt/reg"), ("m-base-reg", "baseline")):
        mm = run(f"evaluate({label})", mcp.tool_evaluate_candidate, mid)
        if mm:
            p, t, s = mm["prediction"], mm["trading"], mm["significance"]
            rows.append((label, p.get("ic_mean"), p.get("rank_ic_mean"),
                         p.get("icir"), p.get("hit_rate"),
                         p.get("quantile_spread"),
                         t.get("sharpe"), t.get("ann_return_net"),
                         t.get("max_drawdown"), s.get("dsr"),
                         mm["decision"]["passed"]))
    if rows:
        print("\n" + "-" * 108)
        print(f"{'模型':<12}{'IC':>9}{'rankIC':>9}{'ICIR':>9}{'命中':>8}"
              f"{'Q价差':>9}{'Sharpe':>9}{'年化':>9}{'回撤':>9}{'DSR':>8}{'过闸':>6}")
        print("-" * 108)
        for r in rows:
            def f(x, w=9, p=4):
                return f"{x:{w}.{p}f}" if isinstance(x, (int, float)) else \
                    f"{'n/a':>{w}}"
            print(f"{r[0]:<12}{f(r[1])}{f(r[2])}{f(r[3])}{f(r[4], 8, 3)}"
                  f"{f(r[5])}{f(r[6])}{f(r[7])}{f(r[8])}{f(r[9], 8, 3)}"
                  f"{'是' if r[10] else '否':>6}")
        print("-" * 108)
    run("trial_count", mcp.tool_trial_count)

    print("\n=== 7) 同一目标的不同标签 (验证 hit_rate 需要 sign 标签) ===")
    b2 = run("build_training_samples(sign)", mcp.tool_build_training_samples,
             feats, "sign_10d", pk["panel_key"], train_len="150D",
             test_len="60D", step="60D", start="2021-01-01", end="2021-12-31")
    if b2:
        run("train(linear/sign)", mcp.tool_train_model, b2["run_id"], "linear",
            "sign_10d", "classification", False, {"alpha": 1.0}, "m-sign-cls")
        mm2 = run("evaluate(sign_10d)", mcp.tool_evaluate_candidate,
                  "m-sign-cls", "sign_10d")
        if mm2:
            p = mm2["prediction"]
            print(f"       sign_10d: hit_rate={p.get('hit_rate')} "
                  f"ic={p.get('ic_mean')} rankIC={p.get('rank_ic_mean')}")

    print("\n=== 8) 离群值诊断: IC 高是不是被极端标签驱动的假象 ===")
    lab = mcp._LABEL_CACHE["ret_10d"].values
    q = lab.dropna()
    q1, q99 = q.quantile(0.01), q.quantile(0.99)
    trimmed = lab.clip(q1, q99)
    for nm, mid in (("linear/reg", "m-lin-reg"), ("gbdt/reg", "m-gbdt-reg"),
                    ("baseline", "m-base-reg")):
        sc = mcp._SCORES_CACHE.get(mid)
        if sc is None:
            continue
        common = sc.index.intersection(lab.index)
        s_, y_, t_ = sc.loc[common], lab.loc[common], trimmed.loc[common]
        ok = s_.notna() & y_.notna()
        raw_ic = s_[ok].groupby(level="time").corr(y_[ok]).mean()
        trim_ic = s_[ok].groupby(level="time").corr(t_[ok]).mean()
        print(f"       {nm:<12} 原始IC={raw_ic.mean():+.4f}  "
              f"缩尾IC(1%/99%)={trim_ic.mean():+.4f}  "
              f"标签|99分位|={abs(q99):.3f} max={abs(q.max()):.1f}")

    print("\n" + "=" * 60)
    blocked = [s for s in STEP if s[1] == "BLOCKED"]
    print(f"走通 {len(STEP) - len(blocked)} / 卡住 {len(blocked)}")
    for n, _, err in blocked:
        print(f"  - {n}: {err[:120]}")
    print("=" * 60)


if __name__ == "__main__":
    main()