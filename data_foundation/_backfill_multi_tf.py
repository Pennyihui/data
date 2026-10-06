# -*- coding: utf-8 -*-
"""_backfill_multi_tf.py — 从 1h 认证数据重建 4h/1d/1w/1M (数据底座)

背景: run_pipeline 当年只给 15 个主流币派生了 1d/1w; 1M 从来没有。
而 1h 认证数据覆盖 spot 590 / perpetual 377 个标的。本脚本把**多周期
派生补全到 1h 已覆盖的全部标的** —— 数据聚合属于数据底座 (分层边界:
特征底座只消费认证数据, 不自己造数据)。

做法 (对每个 instrument):
    读 certified 1h → l1.derive_aggregates(4h/1d/1w/1M)
    → l1.write_parquet (L1) → l2.certify_candles → l2.write_certified (L2)
    → 更新 manifest

幂等: 已存在且行数>0 的 certified 输出默认跳过 (--force 重算)。
用法:
    python _backfill_multi_tf.py                # 全量 (spot+perp, 4 个周期)
    python _backfill_multi_tf.py --market spot  # 只跑现货
    python _backfill_multi_tf.py --intervals 1d 1M
    python _backfill_multi_tf.py --force        # 重算已有
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.config import CERTIFIED_DIR, QUALITY_RULE_VERSION  # noqa: E402
from data_foundation.l1 import derive_aggregates, write_parquet  # noqa: E402
from data_foundation.l2 import certify_candles, write_certified  # noqa: E402
from data_foundation.manifest import (certify_manifest, empty_manifest,  # noqa: E402
                                      write_manifest)

SOURCE = {"spot": "market_candle_spot_1h", "perpetual": "market_candle_perpetual_1h"}
INTERVALS = ["4h", "1d", "1w", "1M"]


def find_instruments(market: str) -> list[tuple[str, str]]:
    """从 certified 1h 目录枚举 (venue, instrument)。"""
    root = os.path.join(CERTIFIED_DIR, SOURCE[market])
    out = []
    for venue in sorted(os.listdir(root)):
        vp = os.path.join(root, venue, market)
        if not os.path.isdir(vp):
            continue
        for inst in sorted(os.listdir(vp)):
            if os.path.isdir(os.path.join(vp, inst, "interval=1h")):
                out.append((venue, inst))
    return out


def process_one(venue: str, inst: str, market: str, intervals: list[str],
                force: bool) -> dict:
    src = os.path.join(CERTIFIED_DIR, SOURCE[market], venue, market, inst,
                       "interval=1h", "data.parquet")
    out = {"instrument": f"{venue}/{market}/{inst}", "done": [], "skip": [],
           "rows": {}}
    df1h = pd.read_parquet(src)
    if df1h.empty:
        out["error"] = "empty source"
        return out
    for iv in intervals:
        ds = f"market_candle_{market}_{iv}"
        dst = os.path.join(CERTIFIED_DIR, ds, venue, market, inst,
                           f"interval={iv}", "data.parquet")
        if not force and os.path.exists(dst):
            try:
                if os.path.getsize(dst) > 500:
                    out["skip"].append(iv)
                    continue
            except OSError:
                pass
        agg = derive_aggregates(df1h, iv)
        if agg.empty:
            out["skip"].append(iv)
            continue
        cert = certify_candles(agg)
        write_parquet(cert, ds, venue, market, inst, iv)
        _, stats = write_certified(cert, ds, venue, market, inst, iv)
        mf = certify_manifest(
            empty_manifest(ds, "1.0"),
            coverage_start=stats["coverage_start"],
            coverage_end=stats["coverage_end"],
            row_count=stats["row_count"],
            duplicate_count=stats["duplicate_count"],
            gap_count=stats["gap_count"],
            suspect_count=stats["suspect_count"],
            quality_rule_version=QUALITY_RULE_VERSION)
        mf["aggregation_rules"] = {"method": f"resample from 1h -> {iv}",
                                   "version": "1.1",
                                   "rule": {"open": "first", "high": "max",
                                            "low": "min", "close": "last",
                                            "volume_*": "sum",
                                            "data_available_at": "close_time_utc"}}
        root_ds = os.path.join(CERTIFIED_DIR, ds)
        write_manifest(root_ds, mf)
        out["done"].append(iv)
        out["rows"][iv] = stats["row_count"]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["spot", "perpetual", "all"], default="all")
    ap.add_argument("--intervals", nargs="*", default=INTERVALS)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 个 (调试)")
    args = ap.parse_args()

    markets = ["spot", "perpetual"] if args.market == "all" else [args.market]
    t0 = time.time()
    n_done = n_skip = n_err = 0
    for market in markets:
        insts = find_instruments(market)
        if args.limit:
            insts = insts[: args.limit]
        print(f"== {market}: {len(insts)} 个标的, 周期 {args.intervals} ==", flush=True)
        for i, (venue, inst) in enumerate(insts):
            try:
                r = process_one(venue, inst, market, args.intervals, args.force)
                if r.get("error"):
                    n_err += 1
                    print(f"  [err] {r['instrument']}: {r['error']}", flush=True)
                    continue
                if r["done"]:
                    n_done += 1
                    print(f"  [{i+1}/{len(insts)}] {r['instrument']}: "
                          f"{r['done']} rows={r['rows']}", flush=True)
                else:
                    n_skip += 1
            except Exception as exc:  # noqa: BLE001
                n_err += 1
                print(f"  [err] {venue}/{inst}: {type(exc).__name__} {exc}",
                      flush=True)
    print(f"== 完成: 新建/更新 {n_done}, 跳过 {n_skip}, 错误 {n_err}, "
          f"耗时 {time.time()-t0:.0f}s ==", flush=True)


if __name__ == "__main__":
    main()