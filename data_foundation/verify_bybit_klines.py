# -*- coding: utf-8 -*-
"""
verify_bybit_klines.py — 收尾验证 (只读):
1) 每 symbol bybit certified 行数/起止/suspect/gap
2) 与 Binance 永续日收盘抽样相关系数 (top5)
3) manifest 内容与数据目录足迹
"""
import glob
import json
import os
import sys

import pandas as pd

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation.config import MVP_ASSETS, CERTIFIED_DIR

DS = "market_candle_perpetual_1h"
L2B = os.path.join(CERTIFIED_DIR, DS, "bybit", "perpetual")

rows = []
for a in MVP_ASSETS:
    inst = f"{a}-USDT"
    p = os.path.join(L2B, inst, "interval=1h", "data.parquet")
    if not os.path.exists(p):
        rows.append({"sym": f"{a}USDT", "status": "MISSING"})
        continue
    df = pd.read_parquet(p)
    rows.append({
        "sym": f"{a}USDT",
        "rows": int(len(df)),
        "start": str(df["open_time_utc"].min())[:16],
        "end": str(df["open_time_utc"].max())[:16],
        "suspect": int(df["is_suspect"].sum()),
        "gap": int(df["is_gap"].sum()),
        "dups": int(df["open_time_utc"].duplicated().sum()),
    })

print(f"{'SYM':10s} {'rows':>7s} {'start':>16s} {'end':>16s} {'sus':>4s} {'gap':>4s} {'dup':>4s}")
for r in rows:
    if r.get("status"):
        print(f"{r['sym']:10s} {r['status']}")
    else:
        print(f"{r['sym']:10s} {r['rows']:>7d} {r['start']:>16s} {r['end']:>16s} "
              f"{r['suspect']:>4d} {r['gap']:>4d} {r['dups']:>4d}")

ok = [r for r in rows if r.get("status") is None]
print(f"\n汇总: {len(ok)}/{len(MVP_ASSETS)} 币, 总行数 {sum(r['rows'] for r in ok)}, "
      f"总 suspect {sum(r['suspect'] for r in ok)}")

# 相关系数 (与 Binance 永续, 重叠期日收盘)
def daily_close(df):
    d = df.copy()
    d["date"] = d["open_time_utc"].dt.floor("1D")
    return d.groupby("date")["close"].last()

corr = []
for r in ok:
    a = r["sym"][:-4]
    inst = f"{a}-USDT"
    bp = os.path.join(CERTIFIED_DIR, DS, "binance", "perpetual", inst,
                      "interval=1h", "data.parquet")
    if not os.path.exists(bp):
        continue
    b = pd.read_parquet(bp)
    y = pd.read_parquet(os.path.join(L2B, inst, "interval=1h", "data.parquet"))
    s = pd.concat([daily_close(b).rename("bin"), daily_close(y).rename("byb")],
                  axis=1).dropna()
    if len(s) >= 30:
        corr.append((r["sym"], float(s["bin"].corr(s["byb"])), int(len(s))))
corr.sort(key=lambda t: -t[1])
print("\n与 Binance 永续日收盘相关系数 (top5):")
for sym, c, n in corr[:5]:
    print(f"  {sym:9s} corr={c:.6f}  (n={n} 天)")

# manifest
mf = os.path.join(CERTIFIED_DIR, DS, "manifest.json")
with open(mf, encoding="utf-8") as f:
    man = json.load(f)
print("\nmanifest:", json.dumps({k: man[k] for k in
      ["row_count", "suspect_count", "coverage_start", "coverage_end",
       "certification_status", "certified_at"]}, ensure_ascii=False, indent=2))
print("source_batches:", man.get("source_batches"))
print("aggregation_rules:", man.get("aggregation_rules"))

# 足迹
def sz(p):
    t = 0
    for dp, _, fns in os.walk(p):
        for fn in fns:
            try:
                t += os.path.getsize(os.path.join(dp, fn))
            except OSError:
                pass
    return t

foot = {
    "raw": sz(os.path.join(HERE, "data", "raw", "bybit", "perpetual_klines_1h")),
    "l1": sz(os.path.join(HERE, "data", "l1", DS, "bybit")),
    "l2": sz(os.path.join(HERE, "data", "l2", "certified", DS, "bybit")),
}
print("\nbybit klines 足迹 (MB):", {k: round(v / 1e6, 2) for k, v in foot.items()},
      "合计", round(sum(foot.values()) / 1e6, 2), "MB")
