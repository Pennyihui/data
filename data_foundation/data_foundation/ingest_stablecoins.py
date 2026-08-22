# -*- coding: utf-8 -*-
"""
ingest_stablecoins.py — 稳定币数据 L0 摄取 (阶段 3)
====================================================
- 供应量: CMC 流通量历史 (cmc_daily_circulating_supply_ranking.csv, 2013+)
- 交易所流向: ErcinDedeoglu stablecoin_*.json (日频, 2022-11+)
- peg: Binance USDCUSDT/DAIUSDT 1h K线
"""
from __future__ import annotations

import glob
import json
import os
import time
from datetime import datetime, timezone

import requests

from .config import RAW_DIR
from .l0 import list_raw_batches, write_raw_file

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
TOKENS = ["USDT", "USDC", "DAI"]

# 外部数据目录 (Data_pipeline 项目, 与 data_foundation 同级)
_PIPE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "data_new", "additional")
CMC_SUPPLY_CSV = os.path.join(_PIPE, "cmc_daily_circulating_supply_ranking.csv")
ERCIN_DIR = os.path.join(_PIPE, "third_party")
KLINE_1H_DIR = os.path.join(os.path.dirname(_PIPE), "klines_1h")


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def ingest_stablecoins_all() -> list[str]:
    written = []
    now = datetime.now(timezone.utc).isoformat()
    # 1) CMC 供应量历史
    if not _already("cmc", "stablecoin_supply", "cmc_supply_v1") and \
            os.path.exists(CMC_SUPPLY_CSV):
        written.append(write_raw_file(
            CMC_SUPPLY_CSV, "cmc", "stablecoin_supply", batch_id="cmc_supply_v1",
            source={"api": "coinmarketcap data-api v3 historical",
                    "note": "circulating supply 日频, 2013+", "fetched_at": now}))
    # 2) Ercin 稳定币流向
    for f in sorted(glob.glob(os.path.join(ERCIN_DIR, "stablecoin_*.json"))):
        name = os.path.basename(f)[:-5]
        if not _already("ercin", "stablecoin_flows", f"{name}_v1"):
            written.append(write_raw_file(
                f, "ercin", "stablecoin_flows", batch_id=f"{name}_v1",
                source={"api": "github ErcinDedeoglu/crypto-market-data",
                        "metric": name, "fetched_at": now}, ext="json"))
    # 3) peg K线: USDCUSDT 本地已有, DAIUSDT 需抓取
    for tok in ["USDC", "DAI"]:
        sym = f"{tok}USDT"
        src = os.path.join(KLINE_1H_DIR, f"{sym}.csv")
        if not os.path.exists(src):
            src = _fetch_klines(sym)
        if src and not _already("binance", "stablecoin_peg_klines", f"{sym}_peg_v1"):
            written.append(write_raw_file(
                src, "binance", "stablecoin_peg_klines", batch_id=f"{sym}_peg_v1",
                source={"api": "binance /api/v3/klines", "symbol": sym,
                        "interval": "1h", "fetched_at": now}))
    print(f"  稳定币 L0: {len(written)} 批次")
    return written


def _fetch_klines(sym: str) -> str | None:
    """抓 DAIUSDT 1h 全历史到临时文件。"""
    end_ms = int(time.time() * 1000)
    rows, cursor = [], 0
    last = None
    for attempt in range(8):
        try:
            while True:
                # 统一走 netpath 四级链路 (retries=1: 外层已有 8 轮退避)
                from . import netpath
                data = netpath.fetch_json(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": sym, "interval": "1h",
                            "startTime": cursor, "limit": 1000},
                    retries=1, timeout=25)
                if not data:
                    break
                rows.extend(data)
                cursor = data[-1][0] + 1
                time.sleep(0.1)
            break
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(2 * (attempt + 1), 12))
    if not rows:
        print(f"  [warn] {sym} K线抓取失败: {str(last)[:60]}")
        return None
    import csv
    tmp = os.path.join(RAW_DIR, "_tmp", f"{sym}_1h.csv")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Open Time", "Open", "High", "Low", "Close", "Volume",
                    "Close Time", "Quote Asset Volume", "Number of Trades",
                    "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
                    "Ignore"])
        for r in rows:
            w.writerow([pd_to_dt(r[0]), *r[1:5], r[5], pd_to_dt(r[6]), r[7],
                        r[8], r[9], r[10], r[11]])
    return tmp


def pd_to_dt(ms: int) -> str:
    from datetime import datetime as dt
    return dt.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
