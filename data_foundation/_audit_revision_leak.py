# -*- coding: utf-8 -*-
"""_audit_revision_leak.py — 审计泄露路径 #6: 修订泄漏 (事后修正的数据回填)

核心问题
--------
设计文档第 6 节的泄露路径 6: "用事后修订的数据回测历史"。

如果某数据源对历史做了回溯修正 (例如 CMC 对 2020 年的市值做过重新抓取/纠错),
那么 2020 年的那行 "当时可见的值" 可能已经不含 2020 年的信息了 —— 它的
data_available_at 虽然写着 2020, 但内容可能来自 2023 的修正版。

判据
----
对每个被审计数据集, 检查:
  lag = (data_available_at - date_utc).days
    - 若 lag 在全表上稳定为常数 L: 该源是"当期快照抓取", L 天后可见 —— 合法
    - 若 lag 随时间漂移 (早期 lag 大、近期 lag 小): 说明历史被回填/修正过
      —— 越久远的日期 lag 越大, 越可疑 (像是事后抓的"完整历史")
"""
from __future__ import annotations

import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from data_foundation.config import CERTIFIED_DIR  # noqa: E402

TARGETS = [
    ("cm_asset_daily", "date_utc", "Coin Metrics 资产网络"),
    ("macro_daily", "date_utc", "宏观 (Yahoo)"),
    ("btc_network_daily", "date_utc", "BTC 网络 (blockchain.info)"),
    ("sentiment_fng", "date_utc", "恐惧贪婪"),
    ("stablecoin_supply", "date_utc", "稳定币供应"),
    ("stablecoin_peg", "time_utc", "稳定币锚定"),
    ("stablecoin_flows", "date_utc", "稳定币流向"),
    ("dex_volume", "date_utc", "DEX 量"),
]


def audit(ds: str, dcol: str, label: str) -> dict:
    root = os.path.join(CERTIFIED_DIR, ds)
    if not os.path.isdir(root):
        return {"dataset": ds, "status": "missing"}
    import pyarrow.parquet as pq
    frames = []
    for dp, _dn, fns in os.walk(root):
        if "data.parquet" not in fns:
            continue
        p = os.path.join(dp, "data.parquet")
        try:
            pf = pq.ParquetFile(p)
            cols = [c for c in (dcol, "data_available_at") if c in pf.schema_arrow.names]
            if len(cols) < 2:
                continue
            frames.append(pd.read_parquet(p, columns=cols))
        except Exception:
            continue
    if not frames:
        return {"dataset": ds, "status": "no_data_available_at"}
    df = pd.concat(frames, ignore_index=True)
    d = pd.to_datetime(df[dcol], utc=True)
    a = pd.to_datetime(df["data_available_at"], utc=True)
    lag = (a - d).dt.total_seconds() / 86400.0

    out = {
        "dataset": ds, "label": label, "status": "ok",
        "rows": int(len(df)),
        "date_min": str(d.min().date()), "date_max": str(d.max().date()),
        "lag_median": round(float(lag.median()), 2),
        "lag_p05": round(float(lag.quantile(0.05)), 2),
        "lag_p95": round(float(lag.quantile(0.95)), 2),
    }
    # 早期 vs 近期 lag 漂移: 早期 - 近期
    med = d < (d.min() + (d.max() - d.min()) / 2)
    out["lag_early"] = round(float(lag[med].median()), 2)
    out["lag_late"] = round(float(lag[~med].median()), 2)
    drift = out["lag_early"] - out["lag_late"]
    out["drift"] = round(drift, 2)
    if drift > 3:
        out["verdict"] = "SUSPECT: 早期数据 lag 明显更大, 疑似事后回填"
    elif drift < -3:
        out["verdict"] = "OK(反向漂移): 近期 lag 更大, 正常"
    else:
        out["verdict"] = "OK: lag 稳定, 当期快照抓取"
    return out


if __name__ == "__main__":
    print("=" * 104)
    print("泄露路径 #6 审计 — 修订泄漏 (data_available_at 真实性)")
    print("=" * 104)
    print(f"{'数据集':<22}{'区间':<24}{'lag中位':>9}{'lag早':>8}{'lag晚':>8}"
          f"{'漂移':>8}  判定")
    print("-" * 104)
    rows = []
    for ds, dcol, label in TARGETS:
        r = audit(ds, dcol, label)
        rows.append(r)
        if r["status"] != "ok":
            print(f"{ds:<22}({r['status']})")
            continue
        rng = f"{r['date_min']}~{r['date_max']}"
        print(f"{ds:<22}{rng:<24}{r['lag_median']:>9.2f}{r['lag_early']:>8.2f}"
              f"{r['lag_late']:>8.2f}{r['drift']:>8.2f}  {r['verdict']}")
    print("=" * 104)
    sus = [r for r in rows if r.get("verdict", "").startswith("SUSPECT")]
    if sus:
        print(f"\n发现 {len(sus)} 个可疑数据集, 建议:")
        for r in sus:
            print(f"  - {r['dataset']}: 漂移 {r['drift']} 天 -> 需人工核查其历史是否被回填修正")
    else:
        print("\n未发现明显修订泄漏 (所有源 lag 稳定)")