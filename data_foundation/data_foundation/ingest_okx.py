# -*- coding: utf-8 -*-
"""
ingest_okx.py — OKX 公开数据 L0 摄取 (跨交易所验证)
====================================================
- spot/swap 1H K线 (history-candles 分页, 近 ~2 年)
- funding-rate-history (分页)
- mark/index K线 (近期)
- OI 当前快照 (OKX 无历史 OI 接口, manifest 记录边界)
- instruments (SPOT/SWAP 元数据)
输出: raw/okx/{dataset}/ingest_date=.../  + .meta.json (与 binance 同构)
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
BASE = "https://www.okx.com/api/v5"
CANDLE_HEADERS = ["ts", "open", "high", "low", "close", "vol",
                  "volCcy", "volCcyQuote", "confirm"]
FUNDING_HEADERS = ["fundingTime", "fundingRate", "realizedRate", "instId", "method"]
MARK_HEADERS = ["ts", "open", "high", "low", "close", "confirm"]


def _get(path, params, retries=8, timeout=30):
    last = None
    for i in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", params=params, timeout=timeout, headers=UA)
            if r.status_code == 429:
                time.sleep(5)
                continue
            r.raise_for_status()
            j = r.json()
            if j.get("code") != "0":
                raise RuntimeError(f"OKX {path}: {j.get('msg')}")
            return j["data"]
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(1.5 * (i + 1), 12))
    raise RuntimeError(str(last)[:150])


def _write_csv(rows, headers, tmp_name):
    # Defensive shaping: pad/trim every row to the header width so a mixed
    # endpoint response can never corrupt the CSV into ragged columns.
    n = len(headers)
    shaped = []
    for r in rows:
        r = list(r)
        if len(r) > n:
            r = r[:n]
        elif len(r) < n:
            r = r + [""] * (n - len(r))
        shaped.append(r)
    tmp = os.path.join(RAW_DIR, "_tmp", tmp_name)
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(shaped)
    return tmp


def _ingest(tmp, dataset, batch_id, source, timestamp_unit="ms"):
    return write_raw_file(tmp, "okx", dataset, batch_id, source,
                          timestamp_unit=timestamp_unit, ext="csv")


def fetch_candles(inst_id: str, bar: str = "1H", days: int = 730,
                  history_path: str = "/market/history-candles") -> list:
    """分页拉 K 线 (OKX 数组格式)。after 为时间戳毫秒(取更早数据)。

    mark/index 端点返回 6 字段 [ts,o,h,l,c,confirm], trade 端点返回 9 字段;
    recent 段必须与 history 段走同一端点, 否则 CSV 行宽混杂。
    """
    rows, end_ts = [], int(time.time() * 1000)
    cursor = end_ts - days * 86400000
    # 先取最近一段: trade K线用 /market/candles, mark/index 用同路径(6字段)
    recent_path = "/market/candles" if history_path == "/market/history-candles" \
        else history_path
    try:
        recent = _get(recent_path, {"instId": inst_id, "bar": bar, "limit": 300})
        rows.extend(recent)
        cursor = min(int(r[0]) for r in recent) - 1
    except Exception:  # noqa: BLE001
        pass
    pages = 0
    while pages < 200:
        data = _get(history_path, {"instId": inst_id, "bar": bar, "limit": 300,
                                   "after": cursor})
        if not data:
            break
        rows.extend(data)
        cursor = int(data[-1][0]) - 1
        pages += 1
        if cursor < end_ts - days * 86400000:
            break
        time.sleep(0.15)
    seen, uniq = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            uniq.append(r)
    uniq.sort(key=lambda r: r[0])
    return uniq


def fetch_funding(inst_id: str, days: int = 730) -> list:
    rows, cursor = [], None
    end_ts = int(time.time() * 1000)
    pages = 0
    while pages < 300:
        params = {"instId": inst_id, "limit": 100}
        if cursor:
            params["after"] = cursor
        data = _get("/public/funding-rate-history", params)
        if not data:
            break
        rows.extend(data)
        cursor = int(data[-1]["fundingTime"]) - 1
        pages += 1
        if cursor < end_ts - days * 86400000:
            break
        time.sleep(0.15)
    seen, uniq = set(), []
    for r in rows:
        if r["fundingTime"] not in seen:
            seen.add(r["fundingTime"])
            uniq.append(r)
    return uniq


def _already_ingested(venue: str, dataset: str, batch_id: str, inst_key: str) -> bool:
    """断点续传: 该批次+标的已存在则跳过。"""
    for meta in list_raw_batches(venue, dataset):
        if meta.get("batch_id") == batch_id and meta.get("source", {}).get(inst_key):
            return True
    return False


def ingest_okx_all(assets: list[str], days: int = 730) -> list[str]:
    """入口: 抓取 OKX 全部数据集到 L0 (断点续传)。返回写入的批次文件。"""
    written = []
    now = datetime.now(timezone.utc).isoformat()
    # instruments
    for itype in ["SPOT", "SWAP"]:
        if _already_ingested("okx", "exchange_metadata",
                             f"instruments_{itype.lower()}_v1", "instType"):
            continue
        data = _get("/public/instruments", {"instType": itype})
        tmp = os.path.join(RAW_DIR, "_tmp", f"okx_instruments_{itype.lower()}.json")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        written.append(write_raw_file(
            tmp, "okx", "exchange_metadata", batch_id=f"instruments_{itype.lower()}_v1",
            source={"api": "okx /public/instruments", "instType": itype,
                    "fetched_at": now}, timestamp_unit="ms", ext="json"))
    for a in assets:
        spot_inst, swap_inst = f"{a}-USDT", f"{a}-USDT-SWAP"
        bid = f"{a}USDT_okx_v1"
        # spot 1H
        if not _already_ingested("okx", "spot_klines_1h", bid, "instId"):
            rows = fetch_candles(spot_inst)
            tmp = _write_csv(rows, CANDLE_HEADERS, f"okx_{a}_spot_1h.csv")
            written.append(_ingest(tmp, "spot_klines_1h", bid,
                                   {"api": "okx /market/history-candles", "instId": spot_inst,
                                    "bar": "1H", "days": days, "fetched_at": now}))
        # swap 1H
        if not _already_ingested("okx", "perpetual_klines_1h", bid, "instId"):
            rows = fetch_candles(swap_inst)
            tmp = _write_csv(rows, CANDLE_HEADERS, f"okx_{a}_swap_1h.csv")
            written.append(_ingest(tmp, "perpetual_klines_1h", bid,
                                   {"api": "okx /market/history-candles", "instId": swap_inst,
                                    "bar": "1H", "days": days, "fetched_at": now}))
        # funding
        if not _already_ingested("okx", "derivatives_funding", bid, "instId"):
            rows = fetch_funding(swap_inst, days)
            tmp = _write_csv([[r["fundingTime"], r["fundingRate"], r.get("realizedRate", ""),
                               r["instId"], r.get("method", "")] for r in rows],
                             FUNDING_HEADERS, f"okx_{a}_funding.csv")
            written.append(_ingest(tmp, "derivatives_funding", bid,
                                   {"api": "okx /public/funding-rate-history",
                                    "instId": swap_inst, "days": days, "fetched_at": now}))
        # mark / index (近期 30 天)
        for dataset, path, inst in [("derivatives_mark_price",
                                     "/market/mark-price-candles", swap_inst),
                                    ("derivatives_index_price",
                                     "/market/index-candles", spot_inst)]:
            if _already_ingested("okx", dataset, bid, "instId"):
                continue
            rows = fetch_candles(inst, days=30, history_path=path)
            tmp = _write_csv(rows, MARK_HEADERS, f"okx_{a}_{dataset.split('_')[1]}.csv")
            written.append(_ingest(tmp, dataset, bid,
                                   {"api": f"okx {path}", "instId": inst,
                                    "bar": "1H", "days": 30, "fetched_at": now}))
        # OI 当前快照
        if not _already_ingested("okx", "derivatives_open_interest", bid, "instId"):
            oi = _get("/public/open-interest", {"instId": swap_inst})
            tmp = os.path.join(RAW_DIR, "_tmp", f"okx_{a}_oi.json")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(oi, f)
            written.append(write_raw_file(
                tmp, "okx", "derivatives_open_interest", batch_id=bid,
                source={"api": "okx /public/open-interest", "instId": swap_inst,
                        "fetched_at": now, "note": "OKX 无历史 OI 接口, 仅当前快照"},
                timestamp_unit="ms", ext="json"))
        print(f"  okx {a}: spot/swap/funding/mark/index/OI 已摄取", flush=True)
    return written
