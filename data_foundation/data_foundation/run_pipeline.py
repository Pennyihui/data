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


def stage_okx(assets, days=None, version="v2"):
    """OKX 接入管线: L0 摄取 -> L1 标准化 -> L2 认证 -> basis 派生。
    days=None 时回填到上市日; version 为批次版本 (v2 深回填)。"""
    import pandas as pd
    import pyarrow.parquet as pq
    from .ingest_okx import ingest_okx_all
    from .l1_okx import (normalize_okx_candles, normalize_okx_funding,
                         normalize_okx_instruments, normalize_okx_mark_index,
                         normalize_okx_oi)
    from .l2 import (certify_candles, certify_derivatives, write_certified,
                     write_certified_derivatives)
    from .schema import BASIS_COLUMNS

    print(f"== OKX ({', '.join(assets)}, days={days or '上市日至今'}) ==")
    print("-- L0 --")
    ingest_okx_all(assets, days, version=version)

    print("-- L1 --")
    for a in assets:
        sym = f"{a}USDT"
        for mtype, ds in [("spot", "market_candle_spot_1h"),
                          ("perpetual", "market_candle_perpetual_1h")]:
            df = normalize_okx_candles("okx", sym, mtype)
            if df.empty:
                continue
            inst = df["instrument_id"].iloc[0]
            write_parquet(df, ds, "okx", mtype, inst, "1h")
        inst_swap = f"{a}-USDT-SWAP"
        inst_spot = f"{a}-USDT"
        fd = normalize_okx_funding("okx", sym)
        if not fd.empty:
            write_derivatives_parquet(fd, "derivatives_funding", "okx",
                                      inst_swap, "funding_time_utc")
        mk = normalize_okx_mark_index("okx", sym, "mark")
        if not mk.empty:
            write_derivatives_parquet(mk, "derivatives_mark_price", "okx",
                                      inst_swap, "open_time_utc")
        ix = normalize_okx_mark_index("okx", sym, "index")
        if not ix.empty:
            write_derivatives_parquet(ix, "derivatives_index_price", "okx",
                                      inst_spot, "open_time_utc")
        oi = normalize_okx_oi("okx", sym)
        if not oi.empty:
            write_derivatives_parquet(oi, "derivatives_open_interest", "okx",
                                      inst_swap, "timestamp_utc")
        print(f"  okx {a}: spot/swap/funding/mark/index/OI 标准化")
    inst_df = normalize_okx_instruments()
    if not inst_df.empty:
        import pyarrow as pa
        for c in inst_df.columns:
            if "time" in c or c == "data_available_at":
                inst_df[c] = pd.to_datetime(inst_df[c], utc=True, errors="coerce") \
                    .astype("datetime64[us, UTC]")
        root = os.path.join(L1_DIR, "instrument", "okx")
        os.makedirs(root, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(inst_df, preserve_index=False),
                       os.path.join(root, "instruments.parquet"), compression="snappy")
        print(f"  okx instruments: {len(inst_df)} 行")

    print("-- L2 --")
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
        for mtype, ds in [("spot", "market_candle_spot_1h"),
                          ("perpetual", "market_candle_perpetual_1h")]:
            inst = f"{a}-USDT" if mtype == "spot" else f"{a}-USDT-SWAP"
            root = os.path.join(L1_DIR, ds, "okx", mtype, inst, "interval=1h")
            if not os.path.isdir(root):
                continue
            df = pq.read_table(root).to_pandas()
            if df.empty:
                continue
            n_unclosed = int((~df["is_closed"]).sum())
            df = df[df["is_closed"]]  # 设计: 未收盘不进正式快照
            df = certify_candles(df)
            _, stats = write_certified(df, ds, "okx", mtype, inst, "1h")
            stats["row_count"] += n_unclosed  # 未收盘行计入总量但不进快照
            acc(ds, stats)
        inst_swap = f"{a}-USDT-SWAP"
        for fn, ds, tc, core in [
            (normalize_okx_funding, "derivatives_funding", "funding_time_utc",
             ["funding_rate"]),
            (lambda v, s: normalize_okx_mark_index(v, s, "mark"),
             "derivatives_mark_price", "open_time_utc",
             ["mark_open", "mark_high", "mark_low", "mark_close"]),
            (lambda v, s: normalize_okx_mark_index(v, s, "index"),
             "derivatives_index_price", "open_time_utc",
             ["index_open", "index_high", "index_low", "index_close"]),
            (normalize_okx_oi, "derivatives_open_interest", "timestamp_utc",
             ["open_interest_contracts", "open_interest_notional"])]:
            ddf = fn("okx", f"{a}USDT")
            if ddf.empty:
                continue
            ddf = certify_derivatives(ddf, tc, core_numeric_cols=core)
            write_certified_derivatives(ddf, ds, "okx", inst_swap, tc)
            acc(ds, {"row_count": len(ddf), "duplicate_count": 0, "gap_count": 0,
                     "suspect_count": int(ddf["is_suspect"].sum()),
                     "coverage_start": str(ddf[tc].min()),
                     "coverage_end": str(ddf[tc].max())})
        print(f"  okx {a}: certified")

    print("-- basis --")
    for a in assets:
        try:
            spot = pq.read_table(os.path.join(
                CERTIFIED_DIR, "market_candle_spot_1h", "okx", "spot",
                f"{a}-USDT", "interval=1h")).to_pandas()[["open_time_utc", "close"]]
            swap = pq.read_table(os.path.join(
                CERTIFIED_DIR, "market_candle_perpetual_1h", "okx", "perpetual",
                f"{a}-USDT-SWAP", "interval=1h")).to_pandas()[["open_time_utc", "close"]]
        except Exception:  # noqa: BLE001
            continue
        spot["open_time_utc"] = pd.to_datetime(spot["open_time_utc"], utc=True)
        swap["open_time_utc"] = pd.to_datetime(swap["open_time_utc"], utc=True)
        m = spot.merge(swap, on="open_time_utc", suffixes=("_spot", "_swap"))
        m["venue_id"] = "okx"
        m["instrument_id"] = f"{a}-USDT"
        m["basis"] = m["close_swap"] / m["close_spot"] - 1
        m["data_available_at"] = m["open_time_utc"] + pd.Timedelta(hours=1)
        m["source_batch_id"] = "okx_basis_v1"
        m = m.rename(columns={"close_spot": "spot_close", "close_swap": "swap_close"})
        m = m[[c for c, _ in BASIS_COLUMNS]]
        write_certified_derivatives(m, "basis_1h", "okx", f"{a}-USDT",
                                    "open_time_utc")
        acc("basis_1h", {"row_count": len(m), "duplicate_count": 0, "gap_count": 0,
                         "suspect_count": 0,
                         "coverage_start": str(m["open_time_utc"].min()),
                         "coverage_end": str(m["open_time_utc"].max())})
        print(f"  basis {a}: {len(m)} 行")

    for ds, s in accum.items():
        src_batches = [f"{a}USDT_okx_v1" for a in assets]
        rules = {"note": "OKX 1H; basis=swap/spot-1; OI 仅当前快照(OKX 无历史 OI 接口)"}
        if ds in ("market_candle_spot_1h", "market_candle_perpetual_1h"):
            rules = {"note": "OKX 1H; 未收盘行(confirm=0)不计入 certified"}
        build_dataset_manifest(ds, "okx", "spot", "*", "*", s, src_batches, rules)
    print("OKX 管线完成")


def stage_stablecoins():
    """阶段 3: 稳定币 (供应量/流向/peg + mint/burn 派生) L0->L1->L2。"""
    import pandas as pd
    import pyarrow.parquet as pq
    from .ingest_stablecoins import ingest_stablecoins_all
    from .l1_stablecoins import (normalize_flows, normalize_peg,
                                 normalize_supply, write_stablecoin_parquet)
    from .l2 import certify_derivatives, write_certified_derivatives

    print("== 阶段3: 稳定币 ==")
    print("-- L0 --")
    ingest_stablecoins_all()

    print("-- L1 --")
    sup = normalize_supply()
    if not sup.empty:
        write_stablecoin_parquet(sup, "stablecoin_supply", "cmc", "date_utc")
        print(f"  supply: {len(sup)} 行 ({sup.token.nunique()} 币, "
              f"{sup.date_utc.min().date()}~{sup.date_utc.max().date()})")
    fl = normalize_flows()
    if not fl.empty:
        write_stablecoin_parquet(fl, "stablecoin_flows", "ercin", "date_utc")
        print(f"  flows: {len(fl)} 行 ({fl.metric.nunique()} 指标)")
    peg = normalize_peg()
    if not peg.empty:
        write_stablecoin_parquet(peg, "stablecoin_peg", "binance", "time_utc")
        print(f"  peg: {len(peg)} 行 ({peg.token.nunique()} 币)")

    print("-- L2 --")
    accum = {}

    def acc(ds, s):
        a = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                                  "gap_count": 0, "suspect_count": 0,
                                  "coverage_start": None, "coverage_end": None})
        a["row_count"] += s["row_count"]
        a["suspect_count"] += s["suspect_count"]
        if a["coverage_start"] is None or s["coverage_start"] < a["coverage_start"]:
            a["coverage_start"] = s["coverage_start"]
        if a["coverage_end"] is None or s["coverage_end"] > a["coverage_end"]:
            a["coverage_end"] = s["coverage_end"]

    for df, ds, venue, tc, core, keys in [
        (sup, "stablecoin_supply", "cmc", "date_utc", ["circulating_supply"],
         ["token", "date_utc"]),
        (fl, "stablecoin_flows", "ercin", "date_utc", ["value_usd"],
         ["metric", "date_utc"]),
        (peg, "stablecoin_peg", "binance", "time_utc",
         ["price", "peg_deviation"], ["token", "time_utc"])]:
        if df.empty:
            continue
        cdf = certify_derivatives(df, tc, core_numeric_cols=core, key_cols=keys)
        write_certified_derivatives(cdf, ds, venue, "all", tc)
        acc(ds, {"row_count": len(cdf), "duplicate_count": 0, "gap_count": 0,
                 "suspect_count": int(cdf["is_suspect"].sum()),
                 "coverage_start": str(cdf[tc].min()),
                 "coverage_end": str(cdf[tc].max())})

    # mint/burn 派生 (供应量日差)
    sup_cert = pq.read_table(os.path.join(CERTIFIED_DIR, "stablecoin_supply",
                                          "cmc")).to_pandas()
    sup_cert["date_utc"] = pd.to_datetime(sup_cert["date_utc"], utc=True)
    mb = []
    for tok, g in sup_cert.groupby("token"):
        g = g.sort_values("date_utc")
        chg = g["circulating_supply"].diff()
        d = g[["token", "date_utc"]].copy()
        d["supply_change"] = chg
        d["mint"] = chg.clip(lower=0)
        d["burn"] = (-chg).clip(lower=0)
        d["venue_id"] = "cmc"
        d["source_batch_id"] = "cmc_supply_v1"
        mb.append(d)
    mb = pd.concat(mb, ignore_index=True) if mb else pd.DataFrame()
    if not mb.empty:
        from .schema import STABLECOIN_MINT_BURN_COLUMNS
        mb = mb[[c for c, _ in STABLECOIN_MINT_BURN_COLUMNS]]
        cdf = certify_derivatives(mb, "date_utc", core_numeric_cols=["supply_change"],
                                  key_cols=["token", "date_utc"])
        write_certified_derivatives(cdf, "stablecoin_mint_burn", "cmc", "all", "date_utc")
        acc("stablecoin_mint_burn",
            {"row_count": len(cdf), "duplicate_count": 0, "gap_count": 0,
             "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())})

    for ds, s in accum.items():
        build_dataset_manifest(ds, "*", "*", "*", "*", s, ["stablecoin_v1"],
                               {"note": "阶段3 稳定币; supply=CMC流通量, "
                                "flows=Ercin日频, peg=Binance稳定币对, "
                                "mint/burn=供应量派生"})
    print("阶段3 完成")


def stage_coinbase(assets=("BTC", "ETH", "SOL", "XRP"), days=365):
    """Coinbase 接入: L0 -> L1 -> L2 (第三交易所跨所验证)。"""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .ingest_coinbase import ASSETS as CB_ASSETS, ingest_coinbase_all
    from .l1_coinbase import (normalize_coinbase_candles,
                              normalize_coinbase_instruments)
    from .l2 import certify_candles, write_certified

    print(f"== Coinbase ({', '.join(assets)}, 近 {days} 天) ==")
    ingest_coinbase_all(days=days)

    print("-- L1 --")
    for a in assets:
        product = CB_ASSETS[a]
        df = normalize_coinbase_candles("coinbase", product)
        if df.empty:
            continue
        write_parquet(df, "market_candle_spot_1h", "coinbase", "spot",
                      product, "1h")
        print(f"  coinbase {product}: {len(df)} 行")
    inst = normalize_coinbase_instruments()
    if not inst.empty:
        for c in inst.columns:
            if "time" in c or c == "data_available_at":
                inst[c] = pd.to_datetime(inst[c], utc=True, errors="coerce") \
                    .astype("datetime64[us, UTC]")
        root = os.path.join(L1_DIR, "instrument", "coinbase")
        os.makedirs(root, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(inst, preserve_index=False),
                       os.path.join(root, "instruments.parquet"), compression="snappy")
        print(f"  coinbase instruments: {len(inst)} 行")

    print("-- L2 --")
    accum = {}

    def acc(ds, s):
        a = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                                  "gap_count": 0, "suspect_count": 0,
                                  "coverage_start": None, "coverage_end": None})
        a["row_count"] += s["row_count"]
        a["suspect_count"] += s["suspect_count"]
        if a["coverage_start"] is None or s["coverage_start"] < a["coverage_start"]:
            a["coverage_start"] = s["coverage_start"]
        if a["coverage_end"] is None or s["coverage_end"] > a["coverage_end"]:
            a["coverage_end"] = s["coverage_end"]

    for a in assets:
        product = CB_ASSETS[a]
        root = os.path.join(L1_DIR, "market_candle_spot_1h", "coinbase", "spot",
                            product, "interval=1h")
        if not os.path.isdir(root):
            continue
        df = pq.read_table(root).to_pandas()
        if df.empty:
            continue
        df = certify_candles(df)
        _, stats = write_certified(df, "market_candle_spot_1h", "coinbase",
                                   "spot", product, "1h")
        acc("market_candle_spot_1h", stats)
        print(f"  coinbase {product}: certified")
    for ds, s in accum.items():
        build_dataset_manifest(ds, "coinbase", "spot", "*", "*", s,
                               ["coinbase_v1"],
                               {"note": "Coinbase REST candles 回填近 1 年, "
                                "可扩展; 无 confirm 字段(全为已收盘 bar)"})
    print("Coinbase 管线完成")


def main():
    ap = argparse.ArgumentParser(description="data_foundation 管线 L0->L1->L2")
    ap.add_argument("--stage", default="all",
                    choices=["l0", "l1", "l2", "all", "okx", "coinbase", "stablecoins"])
    ap.add_argument("--assets", default=",".join(MVP_ASSETS))
    ap.add_argument("--days", type=int, default=None, help="OKX/Coinbase 回看天数(不填=OKX回填到上市日)")
    ap.add_argument("--okx-version", default="v2", help="OKX 批次版本(v2 深回填)")
    args = ap.parse_args()
    assets = [x.strip() for x in args.assets.split(",") if x.strip()]
    print(f"资产({len(assets)}): {assets}")
    if args.stage == "okx":
        stage_okx(assets, days=args.days, version=args.okx_version)
    elif args.stage == "coinbase":
        stage_coinbase(assets, days=args.days or 365)
    elif args.stage == "stablecoins":
        stage_stablecoins()
    else:
        if args.stage in ("l0", "all"):
            stage_l0(assets)
        if args.stage in ("l1", "all"):
            stage_l1(assets)
        if args.stage in ("l2", "all"):
            stage_l2(assets)
    print("完成")


if __name__ == "__main__":
    main()
