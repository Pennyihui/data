# -*- coding: utf-8 -*-
"""_backfill_4h.py — 为全部已有 1h K线的 instrument 派生 4h 层 (L1+L2)。"""
import os
import sys

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
sys.path.insert(0, HERE)
os.chdir(HERE)

import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from data_foundation.config import L1_DIR, CERTIFIED_DIR  # noqa: E402
from data_foundation.l1 import derive_aggregates  # noqa: E402
from data_foundation.l2 import (build_dataset_manifest, certify_candles,  # noqa: E402
                                write_certified)

ACC = {}


def acc(ds, df):
    s = ACC.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                            "gap_count": 0, "suspect_count": 0,
                            "coverage_start": None, "coverage_end": None})
    t = pd.to_datetime(df["open_time_utc"])
    s["row_count"] += len(df)
    s["suspect_count"] += int(df["is_suspect"].sum())
    lo, hi = str(t.min()), str(t.max())
    if s["coverage_start"] is None or lo < s["coverage_start"]:
        s["coverage_start"] = lo
    if s["coverage_end"] is None or hi > s["coverage_end"]:
        s["coverage_end"] = hi


def derive_one(src_parquet, ds_out, venue, market_type, inst):
    df = pq.read_table(src_parquet).to_pandas()
    need = {"open_time_utc", "open", "high", "low", "close", "volume_base",
            "volume_quote", "trade_count"}
    if not need.issubset(df.columns) or df.empty:
        return False
    agg = derive_aggregates(df, "4h")
    # L1
    root = os.path.join(L1_DIR, ds_out, venue, market_type, inst, "interval=4h")
    os.makedirs(root, exist_ok=True)
    import pyarrow as pa
    for c in agg.columns:
        if "time" in c:
            agg[c] = pd.to_datetime(agg[c], utc=True, errors="coerce") \
                .astype("datetime64[us, UTC]")
    agg["date"] = agg["open_time_utc"].dt.strftime("%Y-%m-%d")
    pa.parquet  # noqa
    pq.write_table(pa.Table.from_pandas(agg, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    # L2
    cert = certify_candles(agg)
    write_certified(cert, ds_out, venue, market_type, inst, "4h")
    acc(ds_out, cert)
    return True


def main():
    n_spot = n_perp = 0
    # 现货
    spot_root = os.path.join(L1_DIR, "market_candle_spot_1h", "binance", "spot")
    for inst in sorted(os.listdir(spot_root)):
        src = os.path.join(spot_root, inst, "interval=1h", "data.parquet")
        if not os.path.exists(src):
            continue
        if derive_one(src, "market_candle_spot_4h", "binance", "spot", inst):
            n_spot += 1
    print(f"spot 4h derived: {n_spot}")
    # 永续 (各 venue)
    perp_root = L1_DIR + "/market_candle_perpetual_1h"
    for venue in sorted(os.listdir(perp_root)):
        vd = os.path.join(perp_root, venue)
        for mt in sorted(os.listdir(vd)):
            mtd = os.path.join(vd, mt)
            for inst in sorted(os.listdir(mtd)):
                src = os.path.join(mtd, inst, "interval=1h", "data.parquet")
                if not os.path.exists(src):
                    continue
                if derive_one(src, "market_candle_perpetual_4h", venue, mt, inst):
                    n_perp += 1
    print(f"perp 4h derived: {n_perp}")
    # manifests
    for ds, extra in [("market_candle_spot_4h", "spot"),
                      ("market_candle_perpetual_4h", "perpetual")]:
        if ds in ACC:
            build_dataset_manifest(ds, "*", "*", "*", "*", ACC[ds],
                                   ["derived_from_1h_v1"],
                                   {"method": "resample 1h->4h (UTC 对齐), "
                                             "OHLC=first/max/min/last, "
                                             "量求和; 由标准层无损派生",
                                    "base_interval": "1h"})
    print("done")


if __name__ == "__main__":
    main()
