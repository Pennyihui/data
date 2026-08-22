# -*- coding: utf-8 -*-
"""
ingest_universe.py — Binance Vision 上市宇宙枚举 (listing_universe)
====================================================================
枚举 Binance Vision 现货月度 K 线归档的全部 {SYMBOL}/{YYYY-MM} 前缀,
得到每 symbol 的 first_period / last_period; 与当前现货 exchangeInfo
现役集合对比, 不在现役 -> status="delisted" (消幸存者偏差)。

端点:
  * S3 列表: https://s3-ap-northeast-1.amazonaws.com/data.binance.vision
    (prefix=data/spot/monthly/klines/, max-keys=1000, Marker 翻页);
  * exchangeInfo: https://data-api.binance.vision/api/v3/exchangeInfo。

网络 (重要实测结论):
  * 本机直连 S3 被墙 (connect timeout), 只能走本地代理端口;
  * 各代理口对 S3 host 的可达性随节点轮换漂移 (实测 2026-08-22:
    7897 曾可用 ~3.7s/页, 后整段失效; 7900 当前最快 ~0.3s 小请求/
    2.3s 大页, 其余 7902-7907/7898 多数 refused)。netpath 的失败驱动
    换通道会 2 连败后永久弃用当前通道并切到死通道, 导致全通道失败
    (实测 8 次重试耗尽 RuntimeError 中断);
  * 因此本模块对 S3 列表自建多口故障切换: 按 [7900, 7897, 7906, 7907,
    7904, 7902] 每轮全试, 缓存最后可用口并置于队首, 失败不弃用通道
    (下轮仍从缓存口开始), 自带重试/退避; exchangeInfo 仍走 netpath
    (T0 vision 直连重写可直达, 实测 0.8s/17MB 稳定)。
  * 全程只读文件元数据 (*.zip 文件名), 不下载任何数据文件。

归档层级 (实测): {SYMBOL}/ -> {INTERVAL}/ -> {SYMBOL}-{INTERVAL}-{YYYY-MM}.zip
  * 第一遍: prefix=monthly/klines/ + delimiter=/ 枚举 symbol 层 (3695 个);
  * 第二遍: 每 symbol 枚举 {SYMBOL}/1h/ 文件列表提取 YYYY-MM (响应 ~KB 级,
    ~1.1s/symbol); 无 1h 档案时回退枚举 {SYMBOL}/ 全部 interval 月份并集。
    断点续跑: 每 100 个 symbol 存 checkpoint 到 raw/_tmp/listing_universe_
    state.json, 中断后重跑自动跳过已完成的 symbol。
    (全归档单遍扫描实测 ~3.7s/页 × ~3700 页 ≈ 4h, 传输受限; 逐 symbol
    小响应经 T1 钉死 ~1.1s/个 ≈ 1.2h, 故采用后者。)

输出:
  raw/binance_vision/listing_universe (batch universe_v1, json)
  -> L1 listing_universe/"binance_vision" -> L2 certified + manifest

用法:
  python -m data_foundation.ingest_universe
"""
from __future__ import annotations

import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd
import requests

from . import netpath
from .config import RAW_DIR
from .l0 import write_raw_file
from .l1_onchain import write_onchain_parquet
from .l2 import (build_dataset_manifest, certify_derivatives,
                 write_certified_derivatives)

VISION_LIST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
PREFIX = "data/spot/monthly/klines/"
EXCHANGE_INFO = "https://data-api.binance.vision/api/v3/exchangeInfo"
PAGE_SLEEP = 0.2            # 任务约定: 翻页/请求间 sleep 0.2s
_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_MONTH_RE = re.compile(r"-(\d{4}-\d{2})\.zip$")
_STATE_FILE = os.path.join(RAW_DIR, "_tmp", "listing_universe_state.json")

# 多口故障切换 (S3 host 专用; 理由见模块 docstring)
_PORTS = [7900, 7897, 7906, 7907, 7904, 7902]
_working_port = {"port": 7900}
_PINNED = requests.Session()
_PINNED.trust_env = False
_UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}


# ---------------------------------------------------------------------------
# S3 列表 (多口故障切换抓取)
# ---------------------------------------------------------------------------
def _fetch_s3_listing(params: dict, retries: int = 6) -> str:
    """经本地代理口抓取 S3 列表 XML。

    每轮按 [缓存口, 其余口...] 全试一轮, 成功即缓存该口; 失败不弃用通道
    (下轮仍从缓存口开始), 避免 netpath 式死通道螺旋。
    """
    last = None
    for attempt in range(retries):
        port = _working_port["port"]
        order = _PORTS[_PORTS.index(port):] + _PORTS[:_PORTS.index(port)]
        for p in order:
            proxies = {"http": f"http://127.0.0.1:{p}",
                       "https": f"http://127.0.0.1:{p}"}
            try:
                r = _PINNED.get(VISION_LIST, params=params, timeout=30,
                                headers=_UA, proxies=proxies)
                r.raise_for_status()
                _working_port["port"] = p
                return r.text
            except Exception as e:  # noqa: BLE001
                last = e
        time.sleep(min(1.5 * (attempt + 1), 12))
    raise RuntimeError(f"S3 列表多口抓取失败 ({retries} 轮): {str(last)[:120]}")


def _list_page(prefix: str, marker: str | None = None,
               max_keys: int = 1000,
               delimiter: bool = False) -> tuple[list, list, str | None, bool]:
    """单页列表 -> (CommonPrefixes, Keys, NextMarker, IsTruncated)。

    delimiter=True 用于 symbol 层枚举 (CommonPrefixes);
    delimiter=False 用于文件列表 (Keys)。
    注意: ListObjects v1 仅在使用 delimiter 时返回 NextMarker; 无 delimiter
    时需用上一页最后一个 Key 作为下一页的 Marker。
    """
    params = {"prefix": prefix, "max-keys": str(max_keys)}
    if delimiter:
        params["delimiter"] = "/"
    if marker:
        params["marker"] = marker
    txt = _fetch_s3_listing(params)
    root = ET.fromstring(txt)
    prefixes = [el.text for el in root.findall(f"{_NS}CommonPrefixes/{_NS}Prefix")
                if el.text]
    keys = [el.text for el in root.findall(f"{_NS}Contents/{_NS}Key") if el.text]
    nxt = root.findtext(f"{_NS}NextMarker")
    truncated = (root.findtext(f"{_NS}IsTruncated") or "").strip() == "true"
    return prefixes, keys, nxt, truncated


def enumerate_symbols() -> list[str]:
    """枚举全部 {SYMBOL} 前缀 (delimiter=/ 只到 symbol 层, 不递归)。"""
    symbols: list[str] = []
    marker: str | None = None
    page = 0
    while True:
        page += 1
        prefixes, _, nxt, truncated = _list_page(PREFIX, marker=marker,
                                                 delimiter=True)
        for p in prefixes:
            sym = p[len(PREFIX):].strip("/")
            if sym and sym not in symbols:
                symbols.append(sym)
        print(f"  [symbols] 第 {page} 页: 累计 {len(symbols)} 个 symbol "
              f"(truncated={truncated})", flush=True)
        if not truncated or not nxt:
            break
        marker = nxt
        time.sleep(PAGE_SLEEP)
    return symbols


def symbol_months(symbol: str) -> list[str]:
    """该 symbol 的可用月份 (YYYY-MM) 排序列表。

    主路径: 枚举 {SYMBOL}/1h/ 下月度档案文件名 (只读元数据, 响应 KB 级);
    回退: 无 1h 档案时枚举 {SYMBOL}/ 全部 interval 文件取月份并集。
    """
    def _collect(prefix: str) -> list[str]:
        got: list[str] = []
        marker: str | None = None
        while True:
            _, keys, nxt, truncated = _list_page(prefix, marker=marker,
                                                 delimiter=False)
            for k in keys:
                if k.endswith(".zip") and not k.endswith(".CHECKSUM"):
                    m = _MONTH_RE.search(k)
                    if m:
                        got.append(m.group(1))
            if not truncated:
                break
            marker = keys[-1] if keys else nxt
            time.sleep(PAGE_SLEEP)
        return got

    months = _collect(f"{PREFIX}{symbol}/1h/")
    if not months:
        months = _collect(f"{PREFIX}{symbol}/")
    return sorted(set(months))


# ---------------------------------------------------------------------------
# 断点续跑
# ---------------------------------------------------------------------------
def _load_state() -> dict:
    if os.path.exists(_STATE_FILE):
        try:
            with open(_STATE_FILE, encoding="utf-8") as f:
                st = json.load(f)
            if isinstance(st, dict) and isinstance(st.get("sym_months"), dict):
                return st
        except Exception:  # noqa: BLE001
            pass
    return {"sym_months": {}, "done": []}


def _save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
    tmp = _STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, _STATE_FILE)


# ---------------------------------------------------------------------------
# 现役集合
# ---------------------------------------------------------------------------
def fetch_active_symbols() -> set[str]:
    """当前现货 exchangeInfo 中 status=TRADING 的 symbol 集合 (走 netpath)。"""
    j = netpath.fetch_json(EXCHANGE_INFO)
    active = {s["symbol"] for s in j.get("symbols", [])
              if s.get("status") == "TRADING"}
    print(f"  [exchangeInfo] 现役 TRADING symbol: {len(active)}", flush=True)
    return active


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def build_listing_universe(verbose: bool = True) -> pd.DataFrame:
    """全量枚举 listing_universe -> raw + L1 + L2 certified + manifest。

    带断点续跑: 已完成的 symbol 月份存 checkpoint, 中断后重跑自动跳过。
    """
    t0 = datetime.now(timezone.utc)
    print("== 枚举 Binance Vision 现货月度 K 线 symbol 列表 ==", flush=True)
    symbols = enumerate_symbols()
    active = fetch_active_symbols()

    state = _load_state()
    sym_months = state["sym_months"]          # {SYM: [months]}
    done = set(state.get("done", []))
    for i, sym in enumerate(symbols, 1):
        if sym in done and sym in sym_months:
            continue
        months = symbol_months(sym)
        if months:
            sym_months[sym] = months
        done.add(sym)
        if i % 100 == 0 or i == len(symbols):
            _save_state({"sym_months": sym_months, "done": sorted(done)})
            if verbose:
                print(f"  [months] {i}/{len(symbols)} symbols 完成 "
                      f"({(datetime.now(timezone.utc) - t0).total_seconds():.0f}s)",
                      flush=True)
        time.sleep(PAGE_SLEEP)
    print(f"== 月份枚举完成: {len(sym_months)} 个 symbol, "
          f"{(datetime.now(timezone.utc) - t0).total_seconds():.0f}s ==")

    now = pd.Timestamp.now(tz="UTC")
    rows = []
    for sym in sorted(sym_months):
        months = sym_months[sym]
        rows.append({
            "venue_id": "binance_vision",
            "market_type": "spot",
            "symbol": sym,
            "first_period": months[0],
            "last_period": months[-1],
            "status": "active" if sym in active else "delisted",
            "data_available_at": now,
            "source_batch_id": "universe_v1",
        })

    df = pd.DataFrame(rows)
    # L1 统一微秒 (write_onchain_parquet 不处理 data_available_at, 需显式 cast)
    df["data_available_at"] = df["data_available_at"].astype("datetime64[us, UTC]")

    # L0 raw (batch universe_v1, json)
    payload = {
        "fetched_at": now.isoformat(),
        "active_symbols": sorted(active),
        "symbols": [{**r, "data_available_at": r["data_available_at"].isoformat()}
                    for r in rows],
    }
    tmp = os.path.join(RAW_DIR, "_tmp", "listing_universe_universe_v1.json")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    dst = write_raw_file(tmp, "binance_vision", "listing_universe",
                         "universe_v1",
                         {"api": "binance vision s3 list (monthly/klines "
                          "symbol+month 前缀) + data-api.binance.vision "
                          "/api/v3/exchangeInfo",
                          "fetched_at": now.isoformat()}, ext="json")
    print(f"  [L0] {dst}")

    # L1 写入
    write_onchain_parquet(df, "listing_universe", "binance_vision",
                          "data_available_at")
    print(f"  [L1] listing_universe/binance_vision: {len(df)} 行")

    # L2 认证 + certified
    cdf = certify_derivatives(df, "data_available_at", core_numeric_cols=[],
                              key_cols=["venue_id", "market_type", "symbol"])
    write_certified_derivatives(cdf, "listing_universe", "binance_vision",
                                "all", "data_available_at")
    stats = {
        "row_count": int(len(cdf)),
        "duplicate_count": 0,
        "gap_count": 0,
        "suspect_count": int(cdf["is_suspect"].sum()),
        "coverage_start": str(cdf["data_available_at"].min()),
        "coverage_end": str(cdf["data_available_at"].max()),
    }
    build_dataset_manifest(
        "listing_universe", "*", "*", "*", "*", stats, ["universe_v1"],
        {"note": "Binance Vision 现货月度 K 线归档 {SYMBOL}/{YYYY-MM} 前缀枚举: "
                 "delimiter=/ 枚举 symbol 层 + 每 symbol 1h 档案文件名提取月份 "
                 "(无 1h 回退全部 interval 并集, 只读元数据不下数据, 断点续跑); "
                 "与 data-api.binance.vision /api/v3/exchangeInfo 现役 TRADING "
                 "集合对比定 active|delisted"})
    print(f"  [L2] listing_universe/binance_vision/all: {len(cdf)} 行")

    # 成功后清理断点
    try:
        if os.path.exists(_STATE_FILE):
            os.remove(_STATE_FILE)
    except Exception:  # noqa: BLE001
        pass
    return df


if __name__ == "__main__":
    out = build_listing_universe()
    print(f"\nlisting_universe 构建完成: {len(out)} 行")
