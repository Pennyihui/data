# -*- coding: utf-8 -*-
"""
ingest_deribit.py — Deribit 期权数据 L0 摄取 (阶段 5)
=====================================================
- DVOL 波动率指数历史: BTC+ETH, 最近 days 天
    * get_volatility_index_data, resolution=3600 秒 (1H 粒度)
    * 实测: 官方 resolution 枚举仅 [1, 60, 3600, 43200, "1D"] (单位: 秒),
      没有 15M 粒度; 每页上限 1000 行, 用 result.continuation 分页
    * 历史深度: DVOL 可回溯多年 (实测 20+ 页仍未见底, 至少到 2024-05 之前)
- 期权链快照: get_book_summary_by_currency kind=option (BTC+ETH)
- 指数价快照: get_index_price (btc_usd / eth_usd)
输出: raw/deribit/{dvol_15m,options_chain,index_price}/ingest_date=.../ + .meta.json
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests

from .config import RAW_DIR
from .l0 import list_raw_batches, write_raw_file

BASE = "https://www.deribit.com/api/v2/public"
UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
DVOL_RESOLUTIONS = {60: "1M", 3600: "1H", 43200: "12H", 86400: "1D"}


def _proxies():
    """本地代理 (任务要求经 127.0.0.1:7897); DERIBIT_PROXY='' 可关闭。"""
    p = os.environ.get("DERIBIT_PROXY", "http://127.0.0.1:7897").strip()
    return {"http": p, "https": p} if p else None


def _get(path, params, retries=8, timeout=30):
    """GET Deribit 公开端点 — 统一走 netpath 四级链路 (DERIBIT_PROXY 环境变量废弃)。"""
    from . import netpath
    j = netpath.fetch_json(f"{BASE}{path}", params=params, retries=retries,
                           timeout=timeout)
    if j.get("error"):
        raise RuntimeError(f"Deribit {path}: {j['error']}")
    return j


def fetch_dvol_history(currency: str, days: int = 90, resolution: int = 3600,
                       page_cap: int = 300) -> list:
    """DVOL 历史 (resolution 秒粒度, continuation 分页, 每页 ~1000 行)。

    返回按时间升序的 [ts_ms, open, high, low, close] 列表 (裁剪到 days 窗口)。
    """
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400000
    rows, cur_end, pages = [], end_ms, 0
    while pages < page_cap:
        j = _get("/get_volatility_index_data",
                 {"currency": currency, "start_timestamp": start_ms,
                  "end_timestamp": cur_end, "resolution": resolution})
        res = j.get("result") or {}
        data = res.get("data") or []
        if not data:
            break
        rows.extend(data)
        pages += 1
        oldest = min(int(r[0]) for r in data)
        cont = res.get("continuation")
        if cont is None or oldest < start_ms:
            break
        cur_end = cont
        time.sleep(0.3)
    seen = {}
    for r in rows:
        ts = int(r[0])
        if start_ms <= ts <= end_ms:
            seen[ts] = r
    return [seen[k] for k in sorted(seen)]


def fetch_chain(currency: str) -> list:
    j = _get("/get_book_summary_by_currency",
             {"currency": currency, "kind": "option"})
    return j.get("result") or []


def fetch_index_price(index_name: str) -> dict:
    j = _get("/get_index_price", {"index_name": index_name})
    return j.get("result") or {}


def _already(venue: str, dataset: str, batch_id: str,
             marker_key: str, marker_val: str) -> bool:
    """断点续传: 该批次+标的存在则跳过。"""
    for meta in list_raw_batches(venue, dataset):
        if meta.get("batch_id") == batch_id and \
                meta.get("source", {}).get(marker_key) == marker_val:
            return True
    return False


def ingest_deribit_all(days: int = 90, dvol_resolution: int = 3600,
                       currencies=("BTC", "ETH")) -> list:
    """入口: Deribit DVOL/期权链/指数价 -> L0 (断点续传)。"""
    written = []
    now = datetime.now(timezone.utc).isoformat()
    tmp_dir = os.path.join(RAW_DIR, "_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # 1) DVOL 历史 (BTC+ETH)
    for cur in currencies:
        bid = f"dvol_{cur.lower()}_v1"
        if _already("deribit", "dvol_15m", bid, "currency", cur):
            print(f"  deribit DVOL {cur}: 批次 {bid} 已存在, 跳过", flush=True)
            continue
        rows = fetch_dvol_history(cur, days=days, resolution=dvol_resolution)
        if not rows:
            print(f"  [warn] deribit DVOL {cur}: 返回空 (网络/分辨率无效?)", flush=True)
            continue
        tmp = os.path.join(tmp_dir, f"deribit_dvol_{cur.lower()}.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f)
        written.append(write_raw_file(
            tmp, "deribit", "dvol_15m", batch_id=bid,
            source={"api": "deribit /public/get_volatility_index_data",
                    "currency": cur, "resolution_seconds": dvol_resolution,
                    "resolution_label": DVOL_RESOLUTIONS.get(dvol_resolution,
                                                             str(dvol_resolution)),
                    "days": days, "fetched_at": now,
                    "note": "官方 resolution 枚举仅 [1,60,3600,43200,'1D'] 秒, "
                            "无 15M 粒度; 当前取 1H; 数据为 OHLC 数组"},
            timestamp_unit="ms", ext="json"))
        print(f"  deribit DVOL {cur}: {len(rows)} 行 "
              f"({DVOL_RESOLUTIONS.get(dvol_resolution, dvol_resolution)}, {days} 天)",
              flush=True)

    # 2) 期权链快照 (BTC+ETH)
    for cur in currencies:
        bid = f"chain_{cur.lower()}_v1"
        if _already("deribit", "options_chain", bid, "currency", cur):
            print(f"  deribit chain {cur}: 批次 {bid} 已存在, 跳过", flush=True)
            continue
        rows = fetch_chain(cur)
        if not rows:
            print(f"  [warn] deribit chain {cur}: 返回空", flush=True)
            continue
        tmp = os.path.join(tmp_dir, f"deribit_chain_{cur.lower()}.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f)
        written.append(write_raw_file(
            tmp, "deribit", "options_chain", batch_id=bid,
            source={"api": "deribit /public/get_book_summary_by_currency",
                    "currency": cur, "kind": "option", "fetched_at": now},
            timestamp_unit="ms", ext="json"))
        print(f"  deribit chain {cur}: {len(rows)} 张期权", flush=True)

    # 3) 指数价快照 (btc_usd / eth_usd)
    for idx in ("btc_usd", "eth_usd"):
        bid = f"index_{idx}_v1"
        if _already("deribit", "index_price", bid, "index_name", idx):
            print(f"  deribit index {idx}: 批次 {bid} 已存在, 跳过", flush=True)
            continue
        res = fetch_index_price(idx)
        tmp = os.path.join(tmp_dir, f"deribit_index_{idx}.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(res, f)
        written.append(write_raw_file(
            tmp, "deribit", "index_price", batch_id=bid,
            source={"api": "deribit /public/get_index_price",
                    "index_name": idx, "fetched_at": now},
            timestamp_unit="iso", ext="json"))
        print(f"  deribit index {idx}: index_price={res.get('index_price')}",
              flush=True)
    return written
