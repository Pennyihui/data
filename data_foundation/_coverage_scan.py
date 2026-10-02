"""全量覆盖度扫描: 每个 L2 certified dataset 的行数/时间范围/新鲜度 + 证书状态。"""
import os
import sys

import pandas as pd
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from data_foundation.config import CERTIFIED_DIR  # noqa: E402

NOW = pd.Timestamp.now(tz="UTC")
TIME_PREF = ("open_time_utc", "date_utc", "timestamp", "time", "date",
             "funding_time", "data_available_at", "trade_date")


def ds_files(root):
    out = []
    for dp, _dn, fns in os.walk(root):
        for fn in fns:
            if fn.endswith(".parquet"):
                out.append(os.path.join(dp, fn))
    return out


def scan(ds_dir):
    files = ds_files(ds_dir)
    if not files:
        return None
    rows = 0
    tmin = tmax = None
    col = None
    for p in files:
        try:
            pf = pq.ParquetFile(p)
            rows += pf.metadata.num_rows
            names = pf.schema_arrow.names
            c = next((x for x in TIME_PREF if x in names), None)
            if c is None:
                continue
            if col is None or TIME_PREF.index(c) < TIME_PREF.index(col):
                col = c
        except Exception:
            continue
    if col is None:
        return {"files": len(files), "rows": rows, "min": None, "max": None,
                "staleness_d": None, "tcol": None}
    for p in files:
        try:
            t = pd.read_parquet(p, columns=[col])[col]
        except Exception:
            continue
        if len(t) == 0:
            continue
        a, b = t.min(), t.max()
        ts = lambda x: pd.to_datetime(x, utc=True)  # noqa: E731
        a, b = ts(a), ts(b)
        tmin = a if tmin is None or a < tmin else tmin
        tmax = b if tmax is None or b > tmax else tmax
    stale = (NOW - tmax).days if tmax is not None else None
    return {"files": len(files), "rows": rows, "min": tmin, "max": tmax,
            "staleness_d": stale, "tcol": col}


rows_out = []
for ds in sorted(os.listdir(CERTIFIED_DIR)):
    d = os.path.join(CERTIFIED_DIR, ds)
    if not os.path.isdir(d):
        continue
    r = scan(d)
    if r is None:
        continue
    mf = os.path.join(d, "manifest.json")
    cert = dup = susp = "-"
    if os.path.exists(mf):
        import json
        m = json.load(open(mf, encoding="utf-8"))
        cert = m.get("certification_status", "?")
        dup = m.get("duplicate_count", "?")
        susp = m.get("suspect_count", "?")
    rows_out.append((ds, r, cert, dup, susp))

print(f"{'dataset':<34}{'files':>6}{'rows':>14}  {'min':<11}{'max':<11}"
      f"{'stale_d':>8}  {'cert':<10}{'dup':>6}{'susp':>8}")
print("-" * 122)
tot = 0
for ds, r, cert, dup, susp in rows_out:
    tot += r["rows"]
    mn = str(r["min"])[:10] if r["min"] is not None else "-"
    mx = str(r["max"])[:10] if r["max"] is not None else "-"
    print(f"{ds:<34}{r['files']:>6}{r['rows']:>14,}  {mn:<11}{mx:<11}"
          f"{str(r['staleness_d']):>8}  {str(cert):<10}{str(dup):>6}{str(susp):>8}")
print("-" * 122)
print(f"TOTAL datasets={len(rows_out)}  rows={tot:,}")
