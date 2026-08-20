# -*- coding: utf-8 -*-
"""
run_pipeline.py — L0 -> L1 -> L2 编排
=====================================
用法:
  python -m data_foundation.run_pipeline --stage l0|deriv|l1|l2|all
  --assets BTC,ETH,SOL (默认 MVP 15 币)
"""
from __future__ import annotations

import argparse
import json
import os

from . import ingest
from .config import (CERTIFIED_DIR, L1_DIR, MVP_ASSETS, RAW_DIR)
from .derivatives import (normalize_funding, normalize_mark_price,
                          normalize_open_interest, normalize_ratio,
                          write_derivatives_parquet)
from .l1 import (derive_aggregates, load_raw_batches, normalize_klines,
                 write_parquet)
from .l2 import (build_dataset_manifest, certify_candles,
                 write_certified)
from .manifest import load_manifest

RATIO_METRICS = ["glsr", "tlsr_acct", "tlsr_pos", "taker"]


def stage_l0(assets):
    print("== L0: 原始数据导入 ==")
    syms = [f"{a}USDT" for a in assets]
    n1 = ingest.ingest_external_klines(symbols=syms)
    n2 = ingest.ingest_external_derivatives(symbols=syms)
    print(f"  现货 K线批次 {len(n1)}, 衍生品批次 {len(n2)}")
    try:
        n3 = ingest.fetch_exchange_info()
        print(f"  元数据批次 {len(n3)}")
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] exchangeInfo 抓取失败(可重跑 --stage l0 补齐): {str(e)[:80]}")


def normalize_instruments() -> None:
    """exchangeInfo -> L1 instrument 表 (仅 MVP 交易对)。"""
    import glob

    import pandas as pd
    from .l0 import list_raw_batches
    from .l1 import instrument_id
    frames = []
    for meta in list_raw_batches("binance", "exchange_metadata"):
        mtype = "spot" if "spot" in meta["batch_id"] else "perpetual"
        ingest_date = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, "binance", "exchange_metadata", f"ingest_date={ingest_date}")
        files = [f for f in glob.glob(os.path.join(d, f"{meta['batch_id']}.*"))
                 if not f.endswith(".meta.json")]
        if not files:
            continue
        with open(files[0], encoding="utf-8") as f:
            j = json.load(f)
        for s in j.get("symbols", []):
            if not s.get("symbol", "").endswith(("USDT", "USDC")):
                continue
            frames.append({
                "venue_id": "binance", "symbol": s["symbol"],
                "instrument_id": instrument_id(s["symbol"]),
                "base_asset": s.get("baseAsset"), "quote_asset": s.get("quoteAsset"),
                "market_type": mtype,
                "contract_type": s.get("contractType") or "spot",
                "contract_size": s.get("contractSize", 1.0),
                "tick_size": (s.get("filters", [{}])[0].get("tickSize") if s.get("filters") else None),
                "lot_size": None,
                "min_order_size": None,
                "price_precision": s.get("pricePrecision"),
                "quantity_precision": s.get("quantityPrecision"),
                "listing_time": pd.to_datetime(s.get("onboardDate", 0), unit="ms", utc=True)
                if s.get("onboardDate") else None,
                "delisting_time": None,
                "status": s.get("status"),
                "settlement_asset": s.get("marginAsset") or s.get("quoteAsset"),
                "underlying_asset": s.get("underlyingAsset") or s.get("baseAsset"),
                "data_available_at": pd.Timestamp.now(tz="UTC"),
                "source_batch_id": meta["batch_id"],
            })
    df = pd.DataFrame(frames)
    if df.empty:
        print("  [warn] 无 exchange_metadata 批次, 跳过 instrument 表 (重跑 --stage l0 补齐)")
        return
    from .schema import INSTRUMENT_COLUMNS
    cols = [c for c, _ in INSTRUMENT_COLUMNS]
    df = df[[c for c in cols if c in df.columns]]
    root = os.path.join(L1_DIR, "instrument", "binance")
    os.makedirs(root, exist_ok=True)
    for c in df.columns:
        if "time" in c or c == "data_available_at":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    import pyarrow as pa
    import pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "instruments.parquet"), compression="snappy")
    print(f"  instrument 表: {len(df)} 行 -> {root}/instruments.parquet")


def stage_l1(assets):
    print("== L1: 标准化 ==")
    for a in assets:
        sym = f"{a}USDT"
        raw = load_raw_batches("binance", "spot_klines_1h", sym)
        inst = f"{a}-USDT"
        if raw:
            import pandas as pd
            frames = [pd.read_csv(p) for p in raw]
            df = pd.concat(frames, ignore_index=True)
            df["source_batch_id"] = os.path.basename(raw[0]).split(".")[0]
            norm = normalize_klines(df, "binance", "spot", sym, "1h")
            inst = norm["instrument_id"].iloc[0]
            write_parquet(norm, "market_candle_spot_1h", "binance", "spot", inst, "1h")
            for iv in ["1d", "1w"]:
                agg = derive_aggregates(norm, iv)
                write_parquet(agg, f"market_candle_spot_{iv}", "binance", "spot", inst, iv)
        else:
            print(f"  [warn] {sym} 无 L0 现货批次, 跳过 K线(仅衍生品)")
        # 衍生品
        for fn, ds, tc in [(normalize_funding, "derivatives_funding", "funding_time_utc"),
                           (normalize_open_interest, "derivatives_open_interest", "timestamp_utc"),
                           (normalize_mark_price, "derivatives_mark_price", "open_time_utc")]:
            ddf = fn("binance", sym)
            if not ddf.empty:
                write_derivatives_parquet(ddf, ds, "binance", inst, tc)
        for m in RATIO_METRICS:
            rdf = normalize_ratio("binance", sym, m)
            if not rdf.empty:
                write_derivatives_parquet(rdf, f"derivatives_ratio_{m}",
                                          "binance", inst, "timestamp_utc")
        print(f"  {sym}: 1h/1d/1w + funding/OI/mark/ratio 已标准化")
    normalize_instruments()


def stage_l2(assets):
    print("== L2: 认证 ==")
    import pandas as pd
    import pyarrow.parquet as pq
    from .l2 import (certify_candles, certify_derivatives,
                     write_certified, write_certified_derivatives)
    from .derivatives import (normalize_funding, normalize_mark_price,
                              normalize_open_interest, normalize_ratio)

    # 跨 instrument 聚合统计 (manifest 按数据集)
    accum = {}

    def acc(ds, stats):
        s = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                                  "gap_count": 0, "suspect_count": 0,
                                  "coverage_start": None, "coverage_end": None})
        s["row_count"] += stats["row_count"]
        s["duplicate_count"] += stats["duplicate_count"]
        s["gap_count"] += stats["gap_count"]
        s["suspect_count"] += stats["suspect_count"]
        if s["coverage_start"] is None or stats["coverage_start"] < s["coverage_start"]:
            s["coverage_start"] = stats["coverage_start"]
        if s["coverage_end"] is None or stats["coverage_end"] > s["coverage_end"]:
            s["coverage_end"] = stats["coverage_end"]

    for a in assets:
        sym = f"{a}USDT"
        inst = f"{a}-USDT"
        for iv in ["1h", "1d", "1w"]:
            ds = f"market_candle_spot_{iv}"
            root = os.path.join(L1_DIR, ds, "binance", "spot", inst, f"interval={iv}")
            if not os.path.isdir(root):
                continue
            df = pq.read_table(root).to_pandas()
            if df.empty:
                continue
            df = certify_candles(df)
            _, stats = write_certified(df, ds, "binance", "spot", inst, iv)
            acc(ds, stats)
        # 衍生品认证
        for fn, ds, tc, core in [
            (normalize_funding, "derivatives_funding", "funding_time_utc",
             ["funding_rate"]),
            (normalize_open_interest, "derivatives_open_interest", "timestamp_utc",
             ["open_interest_contracts", "open_interest_notional"]),
            (normalize_mark_price, "derivatives_mark_price", "open_time_utc",
             ["mark_open", "mark_high", "mark_low", "mark_close"])]:
            ddf = fn("binance", sym)
            if ddf.empty:
                continue
            ddf = certify_derivatives(ddf, tc, core_numeric_cols=core)
            write_certified_derivatives(ddf, ds, "binance", inst, tc)
            acc(ds, {"row_count": len(ddf), "duplicate_count": 0,
                     "gap_count": 0, "suspect_count": int(ddf["is_suspect"].sum()),
                     "coverage_start": str(ddf[tc].min()),
                     "coverage_end": str(ddf[tc].max())})
        for m in RATIO_METRICS:
            rdf = normalize_ratio("binance", sym, m)
            if rdf.empty:
                continue
            rdf = certify_derivatives(rdf, "timestamp_utc",
                                      core_numeric_cols=["long_account",
                                                         "long_short_ratio",
                                                         "short_account"])
            write_certified_derivatives(rdf, f"derivatives_ratio_{m}",
                                        "binance", inst, "timestamp_utc")
            acc(f"derivatives_ratio_{m}",
                {"row_count": len(rdf), "duplicate_count": 0, "gap_count": 0,
                 "suspect_count": int(rdf["is_suspect"].sum()),
                 "coverage_start": str(rdf["timestamp_utc"].min()),
                 "coverage_end": str(rdf["timestamp_utc"].max())})
        print(f"  {sym}: 1h/1d/1w + 衍生品 certified")

    # 汇总 manifest (每数据集一份, 聚合所有 instrument)
    for ds, s in accum.items():
        src_batches = [f"{a}USDT_v1" for a in assets]
        rules = None
        if ds in ("market_candle_spot_1d", "market_candle_spot_1w"):
            rules = {"method": "resample from 1h", "version": "1.0"}
        build_dataset_manifest(ds, "binance", "spot" if ds.startswith("market_candle")
                               else "perpetual", "*", "*", s, src_batches, rules)
    print("  manifests 汇总完成")


def main():
    ap = argparse.ArgumentParser(description="data_foundation 管线 L0->L1->L2")
    ap.add_argument("--stage", default="all", choices=["l0", "l1", "l2", "all"])
    ap.add_argument("--assets", default=",".join(MVP_ASSETS))
    args = ap.parse_args()
    assets = [x.strip() for x in args.assets.split(",") if x.strip()]
    print(f"资产({len(assets)}): {assets}")
    if args.stage in ("l0", "all"):
        stage_l0(assets)
    if args.stage in ("l1", "all"):
        stage_l1(assets)
    if args.stage in ("l2", "all"):
        stage_l2(assets)
    print("完成")


if __name__ == "__main__":
    main()
