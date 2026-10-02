import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import refresh_perp_daily as r
import pandas as pd

# 只测 2 个币, 近 3 天
assets = ["BTC", "ETH"]
ing = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
print("probe fapi:", r._probe_fapi())
nb = r.fetch_and_write_raw(assets, 3, ing)
print("raw batches written:", nb)
r.rebuild_l1_l2(assets, derive_4h=True)
print("rebuild done")

# 验证
from data_foundation.config import CERTIFIED_DIR
for ds in ["market_candle_perpetual_1h", "market_candle_perpetual_4h",
           "derivatives_mark_price", "derivatives_index_price"]:
    p = os.path.join(CERTIFIED_DIR, ds, "binance", "BTC-USDT", "data.parquet")
    if not os.path.exists(p):
        p2 = os.path.join(CERTIFIED_DIR, ds, "binance", "perpetual", "BTC-USDT", "interval=1h", "data.parquet")
        p = p2 if os.path.exists(p2) else p
    if os.path.exists(p):
        d = pd.read_parquet(p)
        tc = "open_time_utc" if "open_time_utc" in d.columns else d.columns[0]
        print(f"  {ds:32} rows={len(d):>8,}  max={pd.to_datetime(d[tc].max(),utc=True)}")
    else:
        print(f"  {ds:32} (not found at {p})")
