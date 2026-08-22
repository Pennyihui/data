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


# ---------------------------------------------------------------------------
# 深回填 (子代理 B): 从 hint_ts 探到最早上市数据, 逐 300 根窗拉满到 now。
# Coinbase REST 每请求最多 300 根 (超出返回 400 "granularity too small"),
# 故窗口恒为 300*3600 秒; 输出按年份分段批次 {SYM}_deep_v2_{year}。
# ---------------------------------------------------------------------------
DEEP_WINDOW = 300 * 3600          # 300 根 1h 蜡烛的窗口跨度
DEEP_HINTS = {                    # 各产品探测起始时间戳 (UTC 秒)
    "BTC-USD": int(datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp()),
    "ETH-USD": int(datetime(2016, 1, 1, tzinfo=timezone.utc).timestamp()),
    "SOL-USD": int(datetime(2020, 8, 1, tzinfo=timezone.utc).timestamp()),
    "XRP-USD": int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp()),
}


def _get_candles_retry(product: str, start: int, end: int,
                       attempts: int = 3) -> list:
    """单窗拉取, 网络全通道失败时外层重试 (netpath 内部已处理 429/403)。"""
    for i in range(attempts):
        try:
            return _get(f"/products/{product}/candles",
                        {"granularity": 3600, "start": start, "end": end})
        except Exception as e:  # noqa: BLE001
            if i == attempts - 1:
                raise
            print(f"  [retry] {product} 窗 {start}~{end} 失败: "
                  f"{str(e)[:80]}, 5s 后重试", flush=True)
            time.sleep(5)
    return []


def fetch_candles_deep(product: str, hint_ts: int,
                       now_ts: int | None = None) -> tuple[int | None, list]:
    """深回填: 从 hint_ts 粗探最早非空窗, 再逐窗前推拉满到 now。

    返回 (earliest_ts, rows): earliest_ts 为最早一根的时间戳 (无数据则 None),
    rows 为去重升序的 [time, low, high, open, close, volume] 列表。
    """
    now_ts = now_ts or int(time.time())
    # 1) 粗探: 找第一个非空 300h 窗 (即最早数据所在窗口)
    s = hint_ts
    first_win = None
    while s < now_ts:
        data = _get_candles_retry(product, s, min(s + DEEP_WINDOW, now_ts))
        if data:
            first_win = s
            break
        s += DEEP_WINDOW
        time.sleep(0.2)
    if first_win is None:
        return None, []
    # 2) 从最早窗口向前逐窗拉满 (空窗=停牌/未上市, 继续前进)
    rows, s = [], first_win
    pages = 0
    while s < now_ts and pages < 5000:
        e = min(s + DEEP_WINDOW, now_ts)
        data = _get_candles_retry(product, s, e)
        if data:
            rows.extend(data)
        s = e
        pages += 1
        time.sleep(0.2)
    seen, uniq = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            uniq.append(r)
    uniq.sort(key=lambda r: r[0])
    earliest = min(int(r[0]) for r in uniq) if uniq else None
    return earliest, uniq


def ingest_coinbase_deep(symbols=("BTC", "ETH", "SOL", "XRP")) -> list[str]:
    """深回填到上市日: 按年份分段写 raw/coinbase/spot_klines_1h 批次
    (batch {SYM}_deep_v2_{year}), 不动已有 v1/每日批次。"""
    written = []
    now = datetime.now(timezone.utc).isoformat()
    now_ts = int(time.time())
    for a in symbols:
        product = ASSETS[a]
        earliest, rows = fetch_candles_deep(product, DEEP_HINTS[product],
                                            now_ts)
        if not rows:
            print(f"  [warn] coinbase {product}: 深回填无数据", flush=True)
            continue
        by_year: dict[int, list] = {}
        for r in rows:
            by_year.setdefault(
                datetime.fromtimestamp(int(r[0]), tz=timezone.utc).year,
                []).append(r)
        for year in sorted(by_year):
            bid = f"{a}USD_deep_v2_{year}"
            if _already("coinbase", "spot_klines_1h", bid):
                print(f"  coinbase {product} {year}: 批次 {bid} 已存在, 跳过",
                      flush=True)
                continue
            seg = by_year[year]
            tmp = os.path.join(RAW_DIR, "_tmp", f"cb_{a}_deep_{year}.csv")
            with open(tmp, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(CANDLE_HEADERS)
                w.writerows(seg)
            written.append(write_raw_file(
                tmp, "coinbase", "spot_klines_1h", batch_id=bid,
                source={"api": "coinbase /products/{id}/candles",
                        "product": product, "granularity": 3600,
                        "deep_backfill": True, "year": year,
                        "first_ts": int(seg[0][0]), "last_ts": int(seg[-1][0]),
                        "rows": len(seg), "fetched_at": now}))
            print(f"  coinbase {product} {year}: {len(seg)} 根 "
                  f"({datetime.fromtimestamp(int(seg[0][0]), tz=timezone.utc).date()}"
                  f"~{datetime.fromtimestamp(int(seg[-1][0]), tz=timezone.utc).date()})",
                  flush=True)
        if earliest:
            print(f"  coinbase {product}: 最早 {datetime.fromtimestamp(earliest, tz=timezone.utc).isoformat()}, "
                  f"共 {len(rows)} 根", flush=True)
    return written
