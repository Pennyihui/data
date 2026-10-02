"""按 (dataset, venue) 维度的覆盖度/新鲜度扫描, 识别'整体看起来新但某个所已停更'。"""
import os
import sys
from collections import defaultdict

import pandas as pd
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from data_foundation.config import CERTIFIED_DIR  # noqa: E402

NOW = pd.Timestamp.now(tz="UTC")
TIME_PREF = ("open_time_utc", "date_utc", "timestamp_utc", "funding_time_utc",
             "trade_date", "data_available_at", "date")
DATSETS = sys.argv[1:] or [
    "market_candle_spot_1h", "market_candle_spot_4h", "market_candle_spot_1d",
    "market_candle_perpetual_1h", "market_candle_perpetual_4h",
    "derivatives_funding", "derivatives_open_interest", "derivatives_mark_price",
    "derivatives_index_price", "instrument", "asset_master", "listing_universe",
    "derivatives_oi_cross",
]

for ds in DATSETS:
    root = os.path.join(CERTIFIED_DIR, ds)
    if not os.path.isdir(root):
        print(f"{ds}: (缺失)")
        continue
    by_venue = defaultdict(lambda: {"files": 0, "rows": 0, "max": None, "min": None,
                                    "tcol": None})
    for dp, _dn, fns in os.walk(root):
        if "data.parquet" not in fns:
            continue
        p = os.path.join(dp, "data.parquet")
        rel = os.path.relpath(dp, root)
        venue = rel.split(os.sep)[0]
        try:
            pf = pq.ParquetFile(p)
            n = pf.metadata.num_rows
            names = pf.schema_arrow.names
            c = next((x for x in TIME_PREF if x in names), None)
        except Exception:
            continue
        v = by_venue[venue]
        v["files"] += 1
        v["rows"] += n
        if not c:
            continue
        try:
            t = pd.read_parquet(p, columns=[c])[c]
        except Exception:
            continue
        if len(t) == 0:
            continue
        a, b = pd.to_datetime(t.min(), utc=True), pd.to_datetime(t.max(), utc=True)
        v["min"] = a if v["min"] is None or a < v["min"] else v["min"]
        v["max"] = b if v["max"] is None or b > v["max"] else v["max"]
        v["tcol"] = c
    print(f"\n=== {ds} ===")
    print(f"  {'venue':<12}{'files':>6}{'rows':>13}  {'min':<11}{'max':<11}{'stale_d':>8}  tcol")
    for venue, v in sorted(by_venue.items(), key=lambda kv: -(kv[1]["rows"])):
        st = (NOW - v["max"]).days if v["max"] is not None else None
        mn = str(v["min"])[:10] if v["min"] is not None else "-"
        mx = str(v["max"])[:10] if v["max"] is not None else "-"
        flag = "  <== 停更" if (st is not None and st > 5) else ""
        print(f"  {venue:<12}{v['files']:>6}{v['rows']:>13,}  {mn:<11}{mx:<11}"
              f"{str(st):>8}  {v['tcol']}{flag}")
