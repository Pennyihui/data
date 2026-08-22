# -*- coding: utf-8 -*-
"""
ingest_coinbase.py — Coinbase 现货 L0 摄取 (第三交易所验证)
============================================================
- products 元数据 (Coinbase REST 无上市时间, 记录 status)
- 1h candles: 每请求最多 300 根, 按窗口分页回填 (默认 1 年)
注意: Coinbase candle 数组顺序为 [time, low, high, open, close, volume]
"""
from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone

import requests

from .config import RAW_DIR
from .l0 import list_raw_batches, write_raw_file

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
BASE = "https://api.exchange.coinbase.com"
CANDLE_HEADERS = ["time", "low", "high", "open", "close", "volume"]
ASSETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}


def _get(path, params=None, retries=8, timeout=30):
    # 统一走 netpath 四级链路
    from . import netpath
    return netpath.fetch_json(f"{BASE}{path}", params=params, retries=retries,
                              timeout=timeout)


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def fetch_candles(product: str, days: int = 365) -> list:
    """Coinbase 1h candles: 300根/请求, 从最新往旧分窗。"""
    end_ts = int(time.time())
    start_ts = end_ts - days * 86400
    rows, w_end = [], end_ts
    pages = 0
    while w_end > start_ts and pages < 1000:
        w_start = max(start_ts, w_end - 300 * 3600)
        data = _get(f"/products/{product}/candles",
                    {"granularity": 3600, "start": w_start, "end": w_end})
        if not data:
            break
        rows.extend(data)
        oldest = min(int(r[0]) for r in data)
        w_end = oldest - 1
        pages += 1
        time.sleep(0.2)
    seen, uniq = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            uniq.append(r)
    uniq.sort(key=lambda r: r[0])
    return uniq


def ingest_coinbase_all(days: int = 365) -> list[str]:
    written = []
    now = datetime.now(timezone.utc).isoformat()
    # products 元数据
    if not _already("coinbase", "exchange_metadata", "products_v1"):
        data = _get("/products")
        tmp = os.path.join(RAW_DIR, "_tmp", "coinbase_products.json")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        written.append(write_raw_file(
            tmp, "coinbase", "exchange_metadata", batch_id="products_v1",
            source={"api": "coinbase /products", "fetched_at": now,
                    "note": "Coinbase REST 无上市时间字段"},
            timestamp_unit="ms", ext="json"))
    # candles
    for a, product in ASSETS.items():
        bid = f"{a}USD_cb_v1"
        if _already("coinbase", "spot_klines_1h", bid):
            continue
        rows = fetch_candles(product, days)
        tmp = os.path.join(RAW_DIR, "_tmp", f"cb_{a}_1h.csv")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CANDLE_HEADERS)
            w.writerows(rows)
        written.append(write_raw_file(
            tmp, "coinbase", "spot_klines_1h", batch_id=bid,
            source={"api": "coinbase /products/{id}/candles", "product": product,
                    "granularity": 3600, "days": days, "fetched_at": now}))
        print(f"  coinbase {product}: {len(rows)} 根已摄取", flush=True)
    return written
