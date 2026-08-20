# -*- coding: utf-8 -*-
"""
ingest.py — L0 摄取
===================
1) 从 Data_pipeline 已抓好的 CSV (klines_1h/, multi_symbol/) 导入 L0 raw
2) 抓取 Binance exchangeInfo (现货+合约) 元数据 -> L0 raw
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests

from .config import (EXTERNAL_KLINE_DIR, EXTERNAL_MULTI_DIR, MVP_ASSETS,
                     RAW_DIR, VENUES)
from .l0 import write_raw_file

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}

# 外部衍生品文件 -> L0 dataset 名
DERIV_FILE_MAP = {
    "funding_rate.csv": ("derivatives_funding", "8h"),
    "open_interest_1h.csv": ("derivatives_open_interest", "1h"),
    "mark_price_klines_1h.csv": ("derivatives_mark_price", "1h"),
    "global_ls_account_ratio_1h.csv": ("derivatives_ratio_glsr", "1h"),
    "top_trader_ls_account_ratio_1h.csv": ("derivatives_ratio_tlsr_acct", "1h"),
    "top_trader_ls_position_ratio_1h.csv": ("derivatives_ratio_tlsr_pos", "1h"),
    "taker_buy_sell_ratio_1h.csv": ("derivatives_ratio_taker", "1h"),
}


def ingest_external_klines(symbols=None, assets=None) -> list[str]:
    """klines_1h/{SYM}.csv -> raw/binance/spot_klines_1h/ (每个交易对一批)。"""
    symbols = symbols or [f"{a}USDT" for a in (assets or MVP_ASSETS)]
    written = []
    for sym in symbols:
        src = os.path.join(EXTERNAL_KLINE_DIR, f"{sym}.csv")
        if not os.path.exists(src):
            print(f"  [skip] {sym} 无源文件")
            continue
        dst = write_raw_file(
            src, "binance", "spot_klines_1h", batch_id=f"{sym}_v1",
            source={"api": "binance /api/v3/klines", "symbol": sym,
                    "interval": "1h", "market_type": "spot",
                    "fetched_at": datetime.now(timezone.utc).isoformat()},
            timestamp_unit="ms")
        written.append(dst)
    return written


def ingest_external_derivatives(symbols=None, assets=None) -> list[str]:
    """multi_symbol/{SYM}/ 下的衍生品 CSV -> raw/binance/{dataset}/。"""
    symbols = symbols or [f"{a}USDT" for a in (assets or MVP_ASSETS)]
    written = []
    for sym in symbols:
        d = os.path.join(EXTERNAL_MULTI_DIR, sym)
        if not os.path.isdir(d):
            print(f"  [skip] {sym} 无衍生品目录")
            continue
        for fname, (dataset, interval) in DERIV_FILE_MAP.items():
            src = os.path.join(d, fname)
            if not os.path.exists(src):
                continue
            written.append(write_raw_file(
                src, "binance", dataset, batch_id=f"{sym}_v1",
                source={"api": "binance fapi", "symbol": sym,
                        "market_type": "perpetual", "interval": interval,
                        "fetched_at": datetime.now(timezone.utc).isoformat()},
                timestamp_unit="ms"))
    return written


def _get_json(url, params, retries=8, timeout=25):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            if r.status_code == 429:
                time.sleep(5)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(1.5 * (i + 1), 12))
    raise RuntimeError(str(last)[:150])


def fetch_exchange_info() -> list[str]:
    """Binance 现货+合约 exchangeInfo -> raw/binance/exchange_metadata/。"""
    written = []
    for mtype, base, path in [("spot", VENUES["binance"]["base"],
                               VENUES["binance"]["spot_exchange_info"]),
                              ("perpetual", VENUES["binance"]["fapi"],
                               VENUES["binance"]["futures_exchange_info"])]:
        j = _get_json(f"{base}{path}", {})
        tmp = os.path.join(RAW_DIR, "_tmp", f"exchange_info_{mtype}.json")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(j, f)
        written.append(write_raw_file(
            tmp, "binance", "exchange_metadata", batch_id=f"exchange_info_{mtype}_v1",
            source={"api": f"binance {mtype} exchangeInfo",
                    "fetched_at": datetime.now(timezone.utc).isoformat()},
            timestamp_unit="ms", ext="json"))
        time.sleep(0.3)
    return written
