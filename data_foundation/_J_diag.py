# -*- coding: utf-8 -*-
"""分析目标币在快照中的 tradeable 表现与未达标原因。"""
import pandas as pd
import pyarrow.parquet as pq

t = pq.read_table(r"data/l2/certified/universe_membership/builder/all/data.parquet")
df = t.to_pandas()
targets = pd.read_csv("_J_targets.csv")["symbol"].tolist()

print("=== 目标币在快照中的表现 (R=research成员日 B=backtest成员日 T=tradeable成员日) ===")
rows = []
for s in targets:
    sub = df[df["symbol"] == s]
    if len(sub) == 0:
        rows.append((s, 0, 0, 0, "NOT_IN_SNAPSHOT"))
        continue
    tdays = int(sub["layer_tradeable"].sum())
    bdays = int(sub["layer_backtest"].sum())
    rdays = int(sub["layer_research"].sum())
    reason = ""
    if tdays == 0 and bdays > 0:
        last = sub.iloc[-1]
        reason = (f"vol30d={last['median_volume_30d_usd']:.3e} "
                  f"mcap={last['market_cap_usd']:.3e} "
                  f"gap={last['gap_ratio_30d']:.3f}")
    elif tdays == 0 and bdays == 0:
        reason = "从未进 backtest"
    rows.append((s, rdays, bdays, tdays, reason))

for r in sorted(rows, key=lambda x: (x[3], x[0])):
    print(f"  {r[0]:<14} R={r[1]:>5} B={r[2]:>5} T={r[3]:>5}  {r[4]}")

# 对 T=0 的币, 看其 30d 中位量的历史最大值 (诊断 volume 门槛 vs mcap 门槛)
print("\n=== T=0 目标币的诊断: 窗口内 median_volume_30d_usd 最大值 ===")
for r in rows:
    if r[3] > 0:
        continue
    s = r[0]
    sub = df[df["symbol"] == s]
    if len(sub) == 0:
        continue
    vmax = sub["median_volume_30d_usd"].max()
    mmax = sub["market_cap_usd"].max()
    age_max = sub["age_days"].max()
    hist_max = sub["_hist_days"].max() if "_hist_days" in sub.columns else None
    print(f"  {s:<14} median_vol30d_max={vmax:.3e}  mcap_max={mmax:.3e}  "
          f"age_max={age_max}d  rows={len(sub)}")
