# -*- coding: utf-8 -*-
"""收尾验证: asset_master + listing_universe + alert stub 汇总。"""
import json
import os

from data_foundation.config import CERTIFIED_DIR
from data_foundation.reader import load_asset_master

print("=" * 60)
print("asset_master")
print("=" * 60)
df = load_asset_master()
print(f"行数: {len(df)}")
print("按 venue 分布:")
print(df.groupby("venue_id").size().to_string())
cmc = df["cmc_slug"].notna()
print(f"\ncmc_slug 匹配: {int(cmc.sum())} / {len(df)} = {cmc.mean() * 100:.1f}%")
print("样例:", df.loc[cmc, ["asset", "cmc_slug"]].drop_duplicates("asset")
      .head(5).to_string(index=False))
am_manifest = json.load(open(os.path.join(
    CERTIFIED_DIR, "asset_master", "manifest.json"), encoding="utf-8"))
print("asset_master manifest:", am_manifest["certification_status"],
      "| rows:", am_manifest["row_count"])

print()
print("=" * 60)
print("listing_universe")
print("=" * 60)
lu_path = os.path.join(CERTIFIED_DIR, "listing_universe", "binance_vision",
                       "all", "data.parquet")
if os.path.exists(lu_path):
    import pandas as pd
    lu = pd.read_parquet(lu_path)
    print(f"symbol 数: {lu['symbol'].nunique()} (总行数 {len(lu)})")
    print("status 分布:", lu["status"].value_counts().to_dict())
    print(f"最早 first_period: {lu['first_period'].min()}")
    print(f"最晚 last_period:  {lu['last_period'].max()}")
    print("样例:", lu.head(3).to_string(index=False))
    lu_manifest = json.load(open(os.path.join(
        CERTIFIED_DIR, "listing_universe", "manifest.json"), encoding="utf-8"))
    print("listing_universe manifest:", lu_manifest["certification_status"],
          "| rows:", lu_manifest["row_count"])
else:
    print("certified 文件尚不存在 (枚举可能仍在运行)")

print()
print("=" * 60)
print("alert_webhook stub (无 env 应 log-only)")
print("=" * 60)
from data_foundation import alert_webhook  # noqa: E402
os.environ.pop("ALERT_WEBHOOK_URL", None)
r = alert_webhook.notify({"stub_source": {"status": "failed",
                                          "detail": "test", "batches": 0,
                                          "notes": [], "elapsed": 0.0}})
print("notify 返回:", r["sent"], r["reason"])
