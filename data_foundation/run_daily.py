# -*- coding: utf-8 -*-
"""
run_daily.py — 每日增量任务调度器 (L0 Raw -> L1 Normalized -> L2 Certified)
============================================================================
设计 (与 data_foundation 现有约定一致):
  * L0 增量: 每个源每天一批, batch_id 带日期戳 {base}_{YYYYMMDD},
    写入 raw/{venue}/{dataset}/ingest_date=今天/  (sha256 + .meta.json sidecar)
  * L1/L2 重建: 复用 run_pipeline 的阶段函数 (幂等, 从 raw 全量重建, L0 因 _already 跳过)
  * 每源独立容错: 单源失败不影响其他源, 内部重试 3 次, 结果写入日志与 manifest
  * 网络: 一律经本地代理 127.0.0.1:7897, 所有请求带 8 次重试 + 退避
    (time.sleep(min(1.5*(i+1), 12)), 429 -> sleep 5)

用法:
  python run_daily.py                                   # 全量 (所有源 + L1/L2 重建)
  python run_daily.py --sources binance_klines          # 只跑部分源
  python run_daily.py --date 20260821                   # 指定日期戳 (回填语义)
  python run_daily.py --register-task                   # 注册 Windows 每日 02:30 定时任务

源列表:
  binance_klines   Binance 现货 1h K线 (15 MVP 币, 近 3 天)
  binance_funding  Binance 资金费率 (近 30 天)
  binance_stats    Binance 5 个统计接口 (OI/多空比/主动买卖, period=1h, limit=500)
  okx              OKX BTC/ETH/SOL/XRP spot+swap 1h K线(3天)/资金费率(3个月)/mark/index(3天)/OI快照
  coinbase         Coinbase BTC-USD/ETH-USD/SOL-USD/XRP-USD 1h K线 (近 3 天)
  stablecoins      DefiLlama 供应量快照 + Ercin 稳定币流向(5 文件) + Binance USDC/DAI peg K线
  onchain          链上: ERC-20 Transfer 日志(昨天00:00->今天00:00 UTC) + mempool + DEX + Chainlink
  metadata         三所合约规格 PIT 快照 (Binance 现货/永续 + OKX SPOT/SWAP + Coinbase products)
  rebuild          L1/L2 重建 (stage_l1/l2/okx/coinbase/stablecoins/onchain)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# ---- 路径与工作目录 (保证从任何 cwd 运行都一致) ----
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
os.chdir(_HERE)

# 控制台/重定向输出统一 UTF-8, 避免 GBK 控制台打印中文乱码或抛错
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        pass

# ---- 本地代理 (任务约定: 经 127.0.0.1:7897 访问外网, 不要绕过) ----
PROXY = "http://127.0.0.1:7897"
PROXIES = {"http": PROXY, "https": PROXY}
os.environ.setdefault("HTTP_PROXY", PROXY)
os.environ.setdefault("HTTPS_PROXY", PROXY)
UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}

from data_foundation.config import DATA_ROOT, MVP_ASSETS, RAW_DIR  # noqa: E402
from data_foundation import netpath  # noqa: E402  统一四级网络链路
from data_foundation.l0 import list_raw_batches, sha256_file  # noqa: E402

# ============================================================
# 全局状态
# ============================================================
_ARGS = None
_LOG = None          # 日志文件路径
_CTX = None          # {"date": "YYYYMMDD", "ingest_date": "YYYY-MM-DD", "log": ...}

# Binance 现货端点: api.binance.com 经代理经常超时, data-api.binance.vision
# 是 Binance 官方公共数据镜像 (同一 /api/v3 接口), 作为自动回退。
BINANCE_BASES = ["https://api.binance.com", "https://data-api.binance.vision"]
_binance_base = {"host": None}
_fapi_alive = {"v": None}

# ============================================================
# 日志
# ============================================================
def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if _LOG:
        try:
            with open(_LOG, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


# netpath 的通道切换日志统一走 run_daily.log (带时间戳、进日志文件)
netpath.set_logger(log)


class _Tee:
    """把子函数(阶段 print)同时写进日志文件。"""

    def write(self, s: str) -> None:
        sys.__stdout__.write(s)
        sys.__stdout__.flush()
        if _LOG:
            try:
                with open(_LOG, "a", encoding="utf-8") as f:
                    f.write(s)
            except OSError:
                pass

    def flush(self) -> None:
        sys.__stdout__.flush()


# ============================================================
# 基础 HTTP (统一走 netpath 四级链路: vision直连 -> 7897 -> 专用端口 -> socks5/钉IP)
# ============================================================
def _get_json(url, params=None, retries=8, timeout=25, headers=None):
    return netpath.fetch_json(url, params=params, retries=retries,
                              timeout=timeout, headers=headers)


def _get_text(url, params=None, retries=8, timeout=30, headers=None):
    return netpath.fetch_text(url, params=params, retries=retries,
                              timeout=timeout, headers=headers)


def _pick_binance_base() -> str:
    """Binance 现货端点: api.binance.com 优先 (vision 镜像由 netpath T0 兜底);
    全链路都不可用时退到 vision 基址写 URL (meta 记录 base_host 便于追溯)。"""
    if _binance_base["host"]:
        return _binance_base["host"]
    try:
        netpath.fetch_json(f"{BINANCE_BASES[0]}/api/v3/klines",
                           {"symbol": "BTCUSDT", "interval": "1h", "limit": 1},
                           retries=2, timeout=10)
        _binance_base["host"] = BINANCE_BASES[0]
    except Exception:  # noqa: BLE001
        _binance_base["host"] = BINANCE_BASES[1]
    log(f"  binance 现货端点选定: {_binance_base['host']}")
    return _binance_base["host"]


def _probe_fapi() -> bool:
    """fapi.binance.com 连通性探测 (资金费率/统计接口无镜像, 不可达则整源跳过)。"""
    if _fapi_alive["v"] is not None:
        return _fapi_alive["v"]
    try:
        _get_json("https://fapi.binance.com/fapi/v1/ping", {}, retries=5, timeout=20)
        _fapi_alive["v"] = True
    except Exception as e:  # noqa: BLE001
        _fapi_alive["v"] = False
        log(f"  [warn] fapi.binance.com 经代理不可达 ({str(e)[:60]}), "
            f"binance_funding/binance_stats 将记为失败")
    return _fapi_alive["v"]


# ============================================================
# L0 写入与去重 (与 l0.write_raw_file 同构, 支持显式 ingest_date)
# ============================================================
def _write_raw(src_path, venue, dataset, batch, source, ext="csv",
               ingest_date=None, timestamp_unit="ms"):
    ext = ext.lstrip(".")
    ingest_date = ingest_date or _CTX["ingest_date"]
    dst_dir = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest_date}")
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, f"{batch}.{ext}")
    shutil.copy2(src_path, dst)
    meta = {
        "batch_id": batch,
        "source_path": src_path,
        "source": source,
        # --date 回填时 ingested_at 取该日, 保证 load_raw_batches 的目录定位一致
        "ingested_at": (f"{ingest_date}T00:00:00+00:00" if _ARGS and _ARGS.date
                        else datetime.now(timezone.utc).isoformat()),
        "timestamp_unit": timestamp_unit,
        "timezone": "UTC",
        "checksum_sha256": sha256_file(dst),
        "file_size_bytes": os.path.getsize(dst),
        "immutable": True,
    }
    with open(f"{dst}.meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return dst


def _already_today(venue, dataset, batch) -> bool:
    """该批次今天已写过则跳过 (断点续传/重试幂等)。"""
    today = _CTX["ingest_date"]
    return any(m.get("batch_id") == batch and m.get("ingested_at", "")[:10] == today
               for m in list_raw_batches(venue, dataset))


def _write_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    return path


def _dt(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _dt3(ms) -> str:
    """与 v1 资金费率文件一致的 3 位毫秒格式 (%Y-%m-%d %H:%M:%S.%f)。

    pandas 2.2 对同列混合格式 (有无毫秒) 会做严格推断并抛错, 必须与 v1 一致。
    """
    d = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return f"{d:%Y-%m-%d %H:%M:%S}.{int(ms) % 1000:03d}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# 源 1: binance_klines — 现货 1h K线, 15 MVP 币, 近 3 天
# ============================================================
KLINE_HEADERS = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                 "Close Time", "Quote Asset Volume", "Number of Trades",
                 "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"]


def _fetch_binance_klines(sym: str, base: str, days: int = 3) -> list:
    end_ms = int(time.time() * 1000)
    start_ms = (end_ms - days * 86400000) // 3600000 * 3600000
    rows, cursor = [], start_ms
    while cursor < end_ms:
        data = _get_json(f"{base}/api/v3/klines",
                         {"symbol": sym, "interval": "1h", "startTime": cursor, "limit": 1000})
        if not data:
            break
        rows.extend(data)
        cursor = data[-1][0] + 1
        if len(data) < 1000:
            break
        time.sleep(0.15)
    rows = [r for r in rows if r[0] < end_ms + 3600000]
    seen, uniq = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            uniq.append(r)
    uniq.sort(key=lambda r: r[0])
    return uniq


def run_binance_klines() -> dict:
    base = _pick_binance_base()
    n = 0
    for a in MVP_ASSETS:
        sym = f"{a}USDT"
        bid = f"{sym}_daily_{_CTX['date']}"
        if _already_today("binance", "spot_klines_1h", bid):
            continue
        uniq = _fetch_binance_klines(sym, base)
        if not uniq:
            raise RuntimeError(f"{sym} K线为空")
        tmp = os.path.join(RAW_DIR, "_tmp", f"daily_{sym}_1h.csv")
        _write_csv(tmp, KLINE_HEADERS,
                   [[_dt(r[0]), r[1], r[2], r[3], r[4], r[5], _dt(r[6]),
                     r[7], r[8], r[9], r[10], r[11]] for r in uniq])
        _write_raw(tmp, "binance", "spot_klines_1h", bid,
                   {"api": f"{base}/api/v3/klines", "base_host": base, "symbol": sym,
                    "interval": "1h", "market_type": "spot", "days": 3,
                    "startTime_ms": (int(time.time() * 1000) - 3 * 86400000) // 3600000 * 3600000,
                    "endTime_ms": int(time.time() * 1000),
                    "fetched_at": _now_iso()}, timestamp_unit="ms")
        n += 1
        log(f"  {sym}: {len(uniq)} 根 -> {bid}")
    return {"batches": n, "notes": [f"binance 现货端点: {base}"]}


# ============================================================
# 源 2: binance_funding — 资金费率, 近 30 天
# ============================================================
def run_binance_funding() -> dict:
    if not _probe_fapi():
        raise RuntimeError("fapi.binance.com 经代理不可达 (超时), 资金费率未抓取")
    start_ms = int(time.time() * 1000) - 30 * 86400000
    n = 0
    for a in MVP_ASSETS:
        sym = f"{a}USDT"
        bid = f"{sym}_funding_daily_{_CTX['date']}"
        if _already_today("binance", "derivatives_funding", bid):
            continue
        data = _get_json("https://fapi.binance.com/fapi/v1/fundingRate",
                         {"symbol": sym, "startTime": start_ms})
        data = sorted(data, key=lambda r: r["fundingTime"])
        tmp = os.path.join(RAW_DIR, "_tmp", f"daily_{sym}_funding.csv")
        _write_csv(tmp, ["funding_time", "funding_rate", "mark_price"],
                   [[_dt3(r["fundingTime"]), r.get("fundingRate", ""),
                     r.get("markPrice", "")] for r in data])
        _write_raw(tmp, "binance", "derivatives_funding", bid,
                   {"api": "fapi /fapi/v1/fundingRate", "symbol": sym,
                    "days": 30, "startTime_ms": start_ms,
                    "fetched_at": _now_iso()}, timestamp_unit="ms")
        n += 1
        log(f"  {sym}: {len(data)} 条资金费率 -> {bid}")
    return {"batches": n, "notes": []}


# ============================================================
# 源 3: binance_stats — 5 个统计接口 (period=1h, limit=500)
# ============================================================
STAT_ENDPOINTS = [
    ("openInterestHist", "derivatives_open_interest", "oi"),
    ("globalLongShortAccountRatio", "derivatives_ratio_glsr", "ratio"),
    ("topLongShortAccountRatio", "derivatives_ratio_tlsr_acct", "ratio"),
    ("topLongShortPositionRatio", "derivatives_ratio_tlsr_pos", "ratio"),
    ("takerlongshortRatio", "derivatives_ratio_taker", "taker"),
]
STAT_HEADERS = {
    "oi": ["sumOpenInterest", "sumOpenInterestValue", "CMCCirculatingSupply", "time"],
    "ratio": ["longAccount", "longShortRatio", "shortAccount", "time"],
    "taker": ["buySellRatio", "sellVol", "buyVol", "time"],
}


def run_binance_stats() -> dict:
    if not _probe_fapi():
        raise RuntimeError("fapi.binance.com 经代理不可达 (超时), 统计接口未抓取")
    n = 0
    for a in MVP_ASSETS:
        sym = f"{a}USDT"
        for ep, ds, kind in STAT_ENDPOINTS:
            short = ds.replace("derivatives_", "")
            bid = f"{sym}_{short}_daily_{_CTX['date']}"
            if _already_today("binance", ds, bid):
                continue
            data = _get_json(f"https://fapi.binance.com/futures/data/{ep}",
                             {"symbol": sym, "period": "1h", "limit": 500})
            data = sorted(data, key=lambda r: r["timestamp"])
            if kind == "oi":
                rows = [[r.get("sumOpenInterest", ""), r.get("sumOpenInterestValue", ""),
                         r.get("CMCCirculatingSupply", ""), _dt(r["timestamp"])] for r in data]
            elif kind == "ratio":
                rows = [[r.get("longAccount", ""), r.get("longShortRatio", ""),
                         r.get("shortAccount", ""), _dt(r["timestamp"])] for r in data]
            else:
                rows = [[r.get("buySellRatio", ""), r.get("sellVol", ""),
                         r.get("buyVol", ""), _dt(r["timestamp"])] for r in data]
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_{sym}_{short}.csv")
            _write_csv(tmp, STAT_HEADERS[kind], rows)
            _write_raw(tmp, "binance", ds, bid,
                       {"api": f"fapi /futures/data/{ep}", "symbol": sym,
                        "period": "1h", "limit": 500, "fetched_at": _now_iso()},
                       timestamp_unit="ms")
            n += 1
    log(f"  binance_stats: 共 {n} 个批次")
    return {"batches": n, "notes": []}


# ============================================================
# 源 4: okx — BTC/ETH/SOL/XRP (spot+swap K线/资金费率/mark/index/OI)
# ============================================================
OKX_BASE = "https://www.okx.com/api/v5"
OKX_ASSETS = ["BTC", "ETH", "SOL", "XRP"]


def _okx_get(path, params, retries=8, timeout=30):
    j = netpath.fetch_json(f"{OKX_BASE}{path}", params=params, retries=retries,
                           timeout=timeout)
    if j.get("code") != "0":
        raise RuntimeError(f"OKX {path}: {j.get('msg')}")
    return j["data"]


def _okx_candles(inst, days=3, history_path="/market/history-candles"):
    """与 ingest_okx.fetch_candles 同逻辑 (recent 段与 history 段同端点, 行宽一致)。"""
    end_ts = int(time.time() * 1000)
    cursor = end_ts - days * 86400000
    rows = []
    recent_path = "/market/candles" if history_path == "/market/history-candles" \
        else history_path
    try:
        recent = _okx_get(recent_path, {"instId": inst, "bar": "1H", "limit": 300})
        rows.extend(recent)
        cursor = min(int(r[0]) for r in recent) - 1
    except Exception:  # noqa: BLE001
        pass
    pages = 0
    while pages < 50:
        data = _okx_get(history_path, {"instId": inst, "bar": "1H",
                                       "limit": 100, "after": cursor})
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


def _okx_funding(inst, days=90):
    rows, cursor, pages = [], None, 0
    end_ts = int(time.time() * 1000)
    while pages < 30:
        params = {"instId": inst, "limit": 100}
        if cursor:
            params["after"] = cursor
        data = _okx_get("/public/funding-rate-history", params)
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


def run_okx() -> dict:
    n = 0
    date = _CTX["date"]
    for a in OKX_ASSETS:
        spot_inst, swap_inst = f"{a}-USDT", f"{a}-USDT-SWAP"
        bid = f"{a}USDT_okx_daily_{date}"
        # spot 1H
        if not _already_today("okx", "spot_klines_1h", bid):
            rows = _okx_candles(spot_inst, days=3)
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_okx_{a}_spot.csv")
            _write_csv(tmp, ["ts", "open", "high", "low", "close", "vol",
                             "volCcy", "volCcyQuote", "confirm"], rows)
            _write_raw(tmp, "okx", "spot_klines_1h", bid,
                       {"api": "okx /market/history-candles", "instId": spot_inst,
                        "bar": "1H", "days": 3, "fetched_at": _now_iso()})
            n += 1
        # swap 1H
        if not _already_today("okx", "perpetual_klines_1h", bid):
            rows = _okx_candles(swap_inst, days=3)
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_okx_{a}_swap.csv")
            _write_csv(tmp, ["ts", "open", "high", "low", "close", "vol",
                             "volCcy", "volCcyQuote", "confirm"], rows)
            _write_raw(tmp, "okx", "perpetual_klines_1h", bid,
                       {"api": "okx /market/history-candles", "instId": swap_inst,
                        "bar": "1H", "days": 3, "fetched_at": _now_iso()})
            n += 1
        # funding (3 个月即可)
        if not _already_today("okx", "derivatives_funding", bid):
            rows = _okx_funding(swap_inst, days=90)
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_okx_{a}_funding.csv")
            _write_csv(tmp, ["fundingTime", "fundingRate", "realizedRate", "instId", "method"],
                       [[r["fundingTime"], r["fundingRate"], r.get("realizedRate", ""),
                         r["instId"], r.get("method", "")] for r in rows])
            _write_raw(tmp, "okx", "derivatives_funding", bid,
                       {"api": "okx /public/funding-rate-history", "instId": swap_inst,
                        "days": 90, "fetched_at": _now_iso()})
            n += 1
        # mark / index (3 天)
        for dataset, path, inst in [("derivatives_mark_price",
                                     "/market/history-mark-price-candles", swap_inst),
                                    ("derivatives_index_price",
                                     "/market/history-index-candles", spot_inst)]:
            if _already_today("okx", dataset, bid):
                continue
            rows = _okx_candles(inst, days=3, history_path=path)
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_okx_{a}_{dataset.split('_')[1]}.csv")
            _write_csv(tmp, ["ts", "open", "high", "low", "close", "confirm"], rows)
            _write_raw(tmp, "okx", dataset, bid,
                       {"api": f"okx {path}", "instId": inst, "bar": "1H",
                        "days": 3, "fetched_at": _now_iso()})
            n += 1
        # OI 当前快照
        if not _already_today("okx", "derivatives_open_interest", bid):
            oi = _okx_get("/public/open-interest", {"instId": swap_inst})
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_okx_{a}_oi.json")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(oi, f)
            _write_raw(tmp, "okx", "derivatives_open_interest", bid,
                       {"api": "okx /public/open-interest", "instId": swap_inst,
                        "fetched_at": _now_iso(),
                        "note": "OKX 无历史 OI 接口, 仅当前快照"}, ext="json")
            n += 1
        log(f"  okx {a}: spot/swap/funding/mark/index/OI 每日批次完成")
    return {"batches": n, "notes": []}


# ============================================================
# 源 5: coinbase — 4 个 USD 现货 1h K线, 近 3 天
# ============================================================
CB_BASE = "https://api.exchange.coinbase.com"
CB_ASSETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}


def _cb_get(path, params=None, retries=8, timeout=30):
    return netpath.fetch_json(f"{CB_BASE}{path}", params=params,
                              retries=retries, timeout=timeout)


def _cb_candles(product, days=3):
    end_ts = int(time.time())
    start_ts = end_ts - days * 86400
    rows, w_end = [], end_ts
    while w_end > start_ts:
        w_start = max(start_ts, w_end - 300 * 3600)
        data = _cb_get(f"/products/{product}/candles",
                       {"granularity": 3600, "start": w_start, "end": w_end})
        if not data:
            break
        rows.extend(data)
        w_end = min(int(r[0]) for r in data) - 1
        time.sleep(0.2)
    seen, uniq = set(), []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            uniq.append(r)
    uniq.sort(key=lambda r: r[0])
    return uniq


def run_coinbase() -> dict:
    n = 0
    for a, product in CB_ASSETS.items():
        bid = f"{a}USD_cb_daily_{_CTX['date']}"
        if _already_today("coinbase", "spot_klines_1h", bid):
            continue
        rows = _cb_candles(product, days=3)
        if not rows:
            raise RuntimeError(f"{product} candles 为空")
        tmp = os.path.join(RAW_DIR, "_tmp", f"daily_cb_{a}_1h.csv")
        _write_csv(tmp, ["time", "low", "high", "open", "close", "volume"],
                   [[int(r[0]), r[1], r[2], r[3], r[4], r[5]] for r in rows])
        _write_raw(tmp, "coinbase", "spot_klines_1h", bid,
                   {"api": "coinbase /products/{id}/candles", "product": product,
                    "granularity": 3600, "days": 3, "fetched_at": _now_iso()})
        n += 1
        log(f"  coinbase {product}: {len(rows)} 根 -> {bid}")
    return {"batches": n, "notes": []}


# ============================================================
# 源 6: stablecoins — 供应量快照 + Ercin 流向 + peg K线
# ============================================================
ERCIN_NAMES = ["stablecoin_exchange_inflow_total", "stablecoin_exchange_netflow",
               "stablecoin_exchange_outflow_total", "stablecoin_exchange_reserve",
               "stablecoin_exchange_supply_ratio"]
LLAMA_STABLECOIN_IDS = {"USDT": 1, "USDC": 2, "DAI": 5}


def run_stablecoins() -> dict:
    n = 0
    notes = []
    # 1) DefiLlama 当前供应量快照 -> 自建 schema 数据集 stablecoin_supply_daily
    bid = f"stablecoin_supply_daily_{_CTX['date']}"
    if not _already_today("defillama", "stablecoin_supply_daily", bid):
        rows = []
        for tok, sid in LLAMA_STABLECOIN_IDS.items():
            j = _get_json(f"https://stablecoins.llama.fi/stablecoin/{sid}",
                          retries=8, timeout=40)
            series = j.get("tokens") or []
            latest = series[-1] if series else {}
            circ = (latest.get("circulating") or {}).get("peggedUSD")
            day = (datetime.fromtimestamp(latest.get("date", time.time()), tz=timezone.utc)
                   .strftime("%Y-%m-%d") if latest else _CTX["ingest_date"])
            rows.append(["defillama", day, tok,
                         "" if circ is None else f"{float(circ):.2f}"])
        tmp = os.path.join(RAW_DIR, "_tmp", "daily_stablecoin_supply.csv")
        _write_csv(tmp, ["venue_id", "date", "token", "circulating_supply"], rows)
        _write_raw(tmp, "defillama", "stablecoin_supply_daily", bid,
                   {"api": "stablecoins.llama.fi /stablecoin/{id}",
                    "tokens": LLAMA_STABLECOIN_IDS,
                    "note": "自建 schema: venue_id/date/token/circulating_supply (L0 快照)",
                    "fetched_at": _now_iso()})
        n += 1
        log(f"  defillama 供应量快照: {rows}")
    # 2) Ercin 稳定币流向 (5 文件) — batch_id 不带日期戳以保持 L1 flows metric 干净
    for name in ERCIN_NAMES:
        if _already_today("ercin", "stablecoin_flows", name):
            continue
        txt = _get_text("https://raw.githubusercontent.com/ErcinDedeoglu/"
                        f"crypto-market-data/main/data/daily/{name}.json")
        tmp = os.path.join(RAW_DIR, "_tmp", f"daily_{name}.json")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(txt)
        _write_raw(tmp, "ercin", "stablecoin_flows", name,
                   {"api": "github ErcinDedeoglu/crypto-market-data", "metric": name,
                    "fetched_at": _now_iso()}, ext="json")
        n += 1
        log(f"  ercin {name}: 已重下载")
    # 3) Binance USDCUSDT/DAIUSDT peg K线 (3 天) — DAIUSDT 现货 2020-08 后停牌,
    #    近 3 天无交易属正常 (写空批次+注明), 不应让整个源失败
    base = _pick_binance_base()
    for sym in ["USDCUSDT", "DAIUSDT"]:
        bid = f"{sym}_peg_daily_{_CTX['date']}"
        if _already_today("binance", "stablecoin_peg_klines", bid):
            continue
        uniq = _fetch_binance_klines(sym, base, days=3)
        tmp = os.path.join(RAW_DIR, "_tmp", f"daily_{sym}_peg.csv")
        _write_csv(tmp, KLINE_HEADERS,
                   [[_dt(r[0]), r[1], r[2], r[3], r[4], r[5], _dt(r[6]),
                     r[7], r[8], r[9], r[10], r[11]] for r in uniq])
        note = "ok"
        if not uniq:
            note = "empty: 该交易对近 3 天无成交 (DAIUSDT 自 2020-08 起 Binance 现货停牌)"
            log(f"  [warn] peg {sym}: 近 3 天无数据, 写入空批次 ({note})")
        _write_raw(tmp, "binance", "stablecoin_peg_klines", bid,
                   {"api": f"{base}/api/v3/klines", "base_host": base, "symbol": sym,
                    "interval": "1h", "days": 3, "note": note,
                    "fetched_at": _now_iso()}, timestamp_unit="ms")
        n += 1
        log(f"  peg {sym}: {len(uniq)} 根 -> {bid} ({note})")
    return {"batches": n, "notes": notes}


# ============================================================
# 源 7: onchain — ERC-20 日志(昨天00:00->今天00:00 UTC) + mempool + DEX + Chainlink
# ============================================================
RPCS = ["https://rpc.mevblocker.io", "https://eth.drpc.org"]
ONCHAIN_TOKENS = {
    "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
    "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6},
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "decimals": 18},
}
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CHAINLINK_PAIRS = {
    "BTC-USD": "0x1b44F3514812d835EB1BDB0acB33d3fA3351Ee43",
    "ETH-USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
}
DEX_WHITELIST = ["Uniswap", "PancakeSwap", "Raydium", "Curve",
                 "Orca", "Trader Joe", "Aerodrome"]
MAX_USDT_LOGS = 700_000   # USDT 日志量巨大, 超限截断并在日志注明


def _rpc(method, params, retries=4, timeout=20):
    errors = []
    for i in range(retries):
        for rpc in RPCS:
            try:
                # netpath 四级链路 (RPC 域名无污染问题, 通常 T1 即通;
                # 单次不轮换, 由外层 RPCS 列表做主机级 failover)
                j = netpath.post_json(rpc, json_body={"jsonrpc": "2.0", "id": 1,
                                                      "method": method,
                                                      "params": params},
                                      timeout=timeout, retries=1)
                if "result" in j:
                    return j["result"]
                errors.append(f"{rpc}: {str(j)[:80]}")
            except Exception as e:  # noqa: BLE001
                errors.append(f"{rpc}: {str(e)[:80]}")
        time.sleep(2 * (i + 1))
    raise RuntimeError("; ".join(errors[-4:]))


def _block_ts(block: int) -> int:
    return int(_rpc("eth_getBlockByNumber", [hex(block), False])["timestamp"], 16)


def _block_at_ts(ts: int, latest: int) -> int:
    """返回时间戳 <= ts 的最近区块 (线性估计 + 精修)。"""
    lts = _block_ts(latest)
    if ts >= lts:
        return latest
    b = max(0, latest - (lts - ts) // 12)
    for _ in range(20):
        b = max(0, min(b, latest))
        t = _block_ts(b)
        if t == ts:
            return b
        step = max(1, abs(ts - t) // 12)
        b = b + step if t < ts else b - step
        if step == 1 and abs(t - ts) < 60:
            return b
    return max(0, min(b, latest))


def _fetch_logs(addr: str, frm: int, to: int, max_logs=None):
    """自适应块区间分页 (200 -> 10 减半), 超 1 万条自动减半 (RPC 限制)。"""
    all_logs, b = [], frm
    truncated = False
    while b <= to:
        span = 200
        while span >= 10:
            t = min(b + span - 1, to)
            try:
                logs = _rpc("eth_getLogs", [{"address": addr,
                                             "topics": [TRANSFER_TOPIC],
                                             "fromBlock": hex(b),
                                             "toBlock": hex(t)}])
                break
            except RuntimeError as e:
                msg = str(e)
                if any(k in msg for k in ("too many logs", "exceeds max results",
                                          "more than 10000")):
                    span //= 2
                    continue
                raise
        if logs:
            all_logs.extend(logs)
        if max_logs and len(all_logs) >= max_logs:
            truncated = True
            break
        b = t + 1
        time.sleep(0.4)
    return all_logs, truncated


def run_onchain() -> dict:
    n = 0
    notes = []
    # 窗口: 昨天 00:00 UTC -> 今天 00:00 UTC
    today_00 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_00 = today_00 - timedelta(days=1)
    latest = int(_rpc("eth_blockNumber", []), 16)
    end_block = _block_at_ts(int(today_00.timestamp()), latest)
    start_block = _block_at_ts(int(yesterday_00.timestamp()), latest)
    log(f"  链上窗口: {yesterday_00:%Y-%m-%d %H:%M} UTC -> {today_00:%Y-%m-%d %H:%M} UTC "
        f"(block {start_block} ~ {end_block}, latest={latest})")
    # 窗口边界块时间戳
    wbid = f"window_blocks_daily_{_CTX['date']}"
    if not _already_today("ethereum", "erc20_transfer_logs", wbid):
        content = json.dumps({
            "start_block": start_block, "end_block": end_block,
            "start_timestamp": _block_ts(start_block),
            "end_timestamp": _block_ts(end_block),
        })
        tmp = os.path.join(RAW_DIR, "_tmp", "daily_eth_window.json")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        _write_raw(tmp, "ethereum", "erc20_transfer_logs", wbid,
                   {"api": "eth_getBlockByNumber", "note": "每日窗口边界时间戳"}, ext="json")
        n += 1
    # ERC-20 日志 (USDT 超限/过慢时只抓 USDC+DAI 并注明)
    for tok in ("USDT", "USDC", "DAI"):
        bid = f"{tok}_transfer_logs_daily_{_CTX['date']}"
        if _already_today("ethereum", "erc20_transfer_logs", bid):
            continue
        try:
            cap = MAX_USDT_LOGS if tok == "USDT" else None
            logs, truncated = _fetch_logs(ONCHAIN_TOKENS[tok]["address"],
                                          start_block, end_block, max_logs=cap)
            tmp = os.path.join(RAW_DIR, "_tmp", f"daily_eth_{tok}_logs.json")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(logs, f)
            _write_raw(tmp, "ethereum", "erc20_transfer_logs", bid,
                       {"api": "eth_getLogs", "token": tok,
                        "address": ONCHAIN_TOKENS[tok]["address"],
                        "blocks": [start_block, end_block],
                        "fetched_at": _now_iso()}, ext="json")
            n += 1
            note = f"{tok}: {len(logs)} 条日志"
            if truncated:
                note += " (超过上限截断, 仅部分)"
                notes.append(note)
            log(f"  {note} -> {bid}")
        except Exception as e:  # noqa: BLE001
            notes.append(f"{tok}: 抓取失败 {str(e)[:80]}")
            log(f"  [warn] {tok} 日志抓取失败: {str(e)[:80]} (已跳过该币)")
    # mempool 推荐费率快照
    bid = f"fees_recommended_daily_{_CTX['date']}"
    if not _already_today("mempool", "btc_fees", bid):
        j = _get_json("https://mempool.space/api/v1/fees/recommended")
        j["fetched_at"] = _now_iso()
        tmp = os.path.join(RAW_DIR, "_tmp", "daily_mempool_fees.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(j, f)
        _write_raw(tmp, "mempool", "btc_fees", bid,
                   {"api": "mempool.space /v1/fees/recommended"}, ext="json")
        n += 1
    # mempool 近 24h 区块
    bid = f"blocks_v1_daily_{_CTX['date']}"
    if not _already_today("mempool", "btc_blocks", bid):
        blocks, height = [], None
        end_ts = time.time()
        for _ in range(40):
            path = "/api/v1/blocks" if height is None else f"/api/v1/blocks/{height}"
            page = _get_json(f"https://mempool.space{path}")
            if not page:
                break
            blocks.extend(page)
            height = page[-1]["height"] - 1
            if page[0]["timestamp"] < end_ts - 24 * 3600:
                break
            time.sleep(0.3)
        tmp = os.path.join(RAW_DIR, "_tmp", "daily_mempool_blocks.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blocks, f)
        _write_raw(tmp, "mempool", "btc_blocks", bid,
                   {"api": "mempool.space /api/v1/blocks", "hours": 24}, ext="json")
        n += 1
        log(f"  mempool blocks: {len(blocks)} 块")
    # DefiLlama DEX 量
    bid = f"dex_v1_daily_{_CTX['date']}"
    if not _already_today("defillama", "dex_volume", bid):
        last = None
        for _ in range(8):
            try:
                j = _get_json("https://api.llama.fi/overview/dexs?dataType=dailyVolume",
                              retries=2, timeout=90)
                out = {"all_dexs": j.get("totalDataChart", [])}
                for day in j.get("totalDataChartBreakdown", []):
                    if not isinstance(day, dict):
                        continue
                    ts = day.get("date")
                    dexs = day.get("dexs", {})
                    for dex in DEX_WHITELIST:
                        if dex in dexs and isinstance(dexs[dex], dict):
                            out.setdefault(dex, []).append(
                                [ts, dexs[dex].get("volume", 0)])
                out["fetched_at"] = _now_iso()
                tmp = os.path.join(RAW_DIR, "_tmp", "daily_dex_volume.json")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(out, f)
                _write_raw(tmp, "defillama", "dex_volume", bid,
                           {"api": "defillama /overview/dexs",
                            "whitelist": DEX_WHITELIST, "fetched_at": _now_iso()},
                           ext="json")
                n += 1
                break
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(10)
        else:
            raise RuntimeError(f"dex 抓取失败: {str(last)[:100]}")
    # Chainlink 快照
    bid = f"chainlink_v1_daily_{_CTX['date']}"
    if not _already_today("ethereum", "oracle_snapshot", bid):
        out = {}
        for pair, addr in CHAINLINK_PAIRS.items():
            res = _rpc("eth_call", [{"to": addr, "data": "0xfeaf968c"}, "latest"])
            hex_body = res[2:] if res.startswith("0x") else res
            words = [int(hex_body[i:i + 64], 16) for i in range(0, len(hex_body), 64)]
            if len(words) >= 5:
                out[pair] = {"roundId": words[0], "answer": words[1],
                             "startedAt": words[2], "updatedAt": words[3],
                             "answeredInRound": words[4]}
            time.sleep(0.4)
        out["fetched_at"] = _now_iso()
        tmp = os.path.join(RAW_DIR, "_tmp", "daily_chainlink.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f)
        _write_raw(tmp, "ethereum", "oracle_snapshot", bid,
                   {"api": "eth_call latestRoundData",
                    "pairs": list(CHAINLINK_PAIRS), "fetched_at": _now_iso()},
                   ext="json")
        n += 1
        log(f"  chainlink: {out}")
    if notes:
        log("  onchain 备注: " + "; ".join(notes))
    return {"batches": n, "notes": notes}


# ============================================================
# 源 8: metadata — 三所合约规格 PIT 快照 (L0 raw 日批次 + L1/L2 认证)
# ============================================================
def run_metadata() -> dict:
    """抓 4 组元数据 (Binance 现货/永续 + OKX SPOT/SWAP + Coinbase products)
    -> 写 raw 日批次 (幂等) -> metadata_pit.normalize + certify 追加进
    l1/instrument 并重认证 (PIT 快照历史)。"""
    import pandas as pd
    from data_foundation import metadata_pit as mp
    date = _CTX["date"]
    n = 0
    notes = []

    def _grab(key, venue, fetch, api_desc):
        """抓取一组元数据并写 raw 日批次 (幂等)。返回 payload 或 None。"""
        nonlocal n
        bid = f"metadata_{key}_{date}"
        if _already_today(venue, "instrument_metadata", bid):
            log(f"  metadata {key}: 今日批次已存在, 跳过抓取 (幂等)")
            return None
        payload = fetch()
        tmp = os.path.join(RAW_DIR, "_tmp", f"{bid}.json")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        _write_raw(tmp, venue, "instrument_metadata", bid,
                   {"api": api_desc, "fetched_at": _now_iso()}, ext="json")
        n += 1
        log(f"  metadata {key}: {api_desc} -> {bid}")
        return payload

    spot_p = _grab("binance_spot", "binance", mp.fetch_binance_spot,
                   "binance /api/v3/exchangeInfo (现货, 全 symbol)")
    perp_p = _grab("binance_perp", "binance", mp.fetch_binance_perp,
                   "binance fapi /fapi/v1/exchangeInfo (USDT-M 永续, 全 symbol)")
    okx_p = _grab("okx", "okx", mp.fetch_okx,
                  "okx /public/instruments (SPOT+SWAP)")
    cb_p = _grab("coinbase", "coinbase", mp.fetch_coinbase,
                 "coinbase /products (全 products)")

    new_by_venue, bids_by_venue = {}, {}

    def _norm(venue, key, payload, norm_fn):
        if payload is None:
            return
        bid = f"metadata_{key}_{date}"
        df = norm_fn(payload, pd.Timestamp.now(tz="UTC"), bid)
        new_by_venue.setdefault(venue, []).append(df)
        bids_by_venue.setdefault(venue, set()).add(bid)
        log(f"  metadata {key}: 归一化 {len(df)} 行")

    _norm("binance", "binance_spot", spot_p, mp.normalize_binance_spot)
    _norm("binance", "binance_perp", perp_p, mp.normalize_binance_perp)
    _norm("okx", "okx", okx_p, mp.normalize_okx)
    _norm("coinbase", "coinbase", cb_p, mp.normalize_coinbase)

    for venue in ("binance", "okx", "coinbase"):
        new_df = pd.concat(new_by_venue.get(venue, []), ignore_index=True) \
            if new_by_venue.get(venue) else None
        mp.certify_venue(venue, new_df=new_df,
                         batch_ids=bids_by_venue.get(venue))
    log(f"  metadata 完成: 新写批次 {n}, 各 venue PIT 已重认证")
    return {"batches": n, "notes": notes}


# ============================================================
# 源 9: rebuild — L1/L2 重建 (复用 run_pipeline 阶段函数)
# ============================================================
def run_rebuild() -> dict:
    from data_foundation.run_pipeline import (stage_coinbase, stage_l1, stage_l2,
                                              stage_okx, stage_onchain,
                                              stage_stablecoins)
    stages = [
        ("binance_l1", lambda: stage_l1(list(MVP_ASSETS))),
        ("binance_l2", lambda: stage_l2(list(MVP_ASSETS))),
        ("okx", lambda: stage_okx(["BTC", "ETH", "SOL", "XRP"], days=None, version="v2")),
        ("coinbase", lambda: stage_coinbase(["BTC", "ETH", "SOL", "XRP"], days=365)),
        ("stablecoins", lambda: stage_stablecoins()),
        ("onchain", lambda: _stage_onchain_no_l0(stage_onchain)),
    ]
    old = sys.stdout
    sys.stdout = _Tee()
    res = {}
    try:
        for name, fn in stages:
            t0 = time.time()
            try:
                fn()
                res[name] = "ok"
            except Exception as e:  # noqa: BLE001
                res[name] = f"fail: {str(e)[:200]}"
                log(f"  [rebuild:{name}] 失败: {str(e)[:200]}")
            log(f"  rebuild {name}: {res[name]} ({time.time() - t0:.0f}s)")
    finally:
        sys.stdout = old
    failed = [k for k, v in res.items() if not v.startswith("ok")]
    if failed:
        raise RuntimeError("L1/L2 重建部分阶段失败: " + ", ".join(failed)
                           + " | 详情见日志 " + str(_LOG))
    # manifest 全局汇总必须是最后一步 (防止单 venue 统计覆盖)
    try:
        from data_foundation.finalize import finalize_all
        n = len(finalize_all(verbose=False))
        log(f"  [rebuild] finalize: {n} 个数据集 manifest 已全局汇总")
    except Exception as e:  # noqa: BLE001
        log(f"  [rebuild] finalize 失败: {str(e)[:120]}")
    return {"batches": len(stages), "notes": [f"{k}={v}" for k, v in res.items()]}


def _stage_onchain_no_l0(stage_onchain) -> None:
    """调用 stage_onchain 但跳过其 L0 (ingest_onchain_all)。

    背景: ingest_onchain.py 被并行修改后新增 Arbitrum/Solana 抓取, 其 RPC
    (arb1.arbitrum.io 等) 经本地代理 127.0.0.1:7897 会卡死在 TLS 握手
    (timeout 不生效), 导致 stage_onchain 永久挂起。本调度器的链上 L0 由
    run_onchain 源独立完成 (Ethereum USDT/USDC/DAI 每日窗口), 因此这里只
    重建 L1/L2 (纯本地计算, 无网络)。运行时 monkey-patch, 不改动现有文件。
    """
    import data_foundation.ingest_onchain as _io
    _orig = _io.ingest_onchain_all
    _io.ingest_onchain_all = lambda days=1, hours=24: []
    try:
        log("  [rebuild:onchain] L0 已跳过 (每日 L0 由 run_onchain 完成; "
            "Arbitrum/Solana RPC 经代理卡死), 仅重建 L1/L2")
        stage_onchain()
    finally:
        _io.ingest_onchain_all = _orig


# ============================================================
# 源注册表
# ============================================================
ALL_SOURCES = ["binance_klines", "binance_funding", "binance_stats",
               "okx", "coinbase", "stablecoins", "onchain", "metadata",
               "rebuild"]
SOURCES = {
    "binance_klines": run_binance_klines,
    "binance_funding": run_binance_funding,
    "binance_stats": run_binance_stats,
    "okx": run_okx,
    "coinbase": run_coinbase,
    "stablecoins": run_stablecoins,
    "onchain": run_onchain,
    "metadata": run_metadata,
    "rebuild": run_rebuild,
}


# ============================================================
# 执行框架: 每源独立容错, 内部重试 3 次
# ============================================================
def _run_source(name, fn) -> dict:
    attempts = 0
    last = ""
    while attempts < 3:
        attempts += 1
        t0 = time.time()
        try:
            detail = fn()
            elapsed = time.time() - t0
            log(f"[{name}] OK ({elapsed:.0f}s)")
            return {"status": "ok", "elapsed": elapsed, "batches": detail.get("batches", 0),
                    "notes": detail.get("notes", []), "detail": ""}
        except Exception as e:  # noqa: BLE001
            last = f"{type(e).__name__}: {str(e)[:300]}"
            elapsed = time.time() - t0
            log(f"[{name}] 第 {attempts}/3 次尝试失败 ({elapsed:.0f}s): {last}")
            if attempts < 3:
                time.sleep(8)
    return {"status": "failed", "elapsed": 0.0, "batches": 0, "notes": [],
            "detail": last}


def _write_manifest(results: dict) -> str:
    path = os.path.join(DATA_ROOT, "daily_manifest.json")
    m = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                m = json.load(f)
        except Exception:  # noqa: BLE001
            m = {}
    history = [h for h in m.get("history", []) if h.get("date") != _CTX["date"]]
    history.append({"date": _CTX["date"],
                    "failed": [k for k, v in results.items() if v["status"] == "failed"]})
    m["last_run"] = datetime.now(timezone.utc).isoformat()
    m["date"] = _CTX["date"]
    m["sources"] = {
        k: {"status": v["status"], "detail": v.get("detail", ""),
            "elapsed_sec": round(v.get("elapsed", 0.0), 1),
            "batches": v.get("batches", 0), "notes": v.get("notes", [])}
        for k, v in results.items()
    }
    m["failed"] = [k for k, v in results.items() if v["status"] == "failed"]
    m["history"] = history[-30:]
    # 网络观测: 本次运行实际使用的通道与切换次数 (回溯"走的哪条道")
    try:
        m["network"] = netpath.stats_snapshot()
    except Exception:  # noqa: BLE001
        pass
    with open(path, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)
    return path


# ============================================================
# Windows 定时任务注册 (每天 02:30)
# ============================================================
def _register_task() -> bool:
    py = sys.executable or r"E:\Anaconda3\python.exe"
    script = os.path.join(_HERE, "run_daily.py")

    def _q(p: str) -> str:
        # 路径含空格才加引号 (schtasks 对 /tr 内嵌引号的解析很脆弱)
        return f'"{p}"' if " " in p else p

    tr = f"{_q(py)} {_q(script)}"
    cmd = ["schtasks", "/create", "/tn", "DataFoundation_DailyIngest",
           "/tr", tr, "/sc", "daily", "/st", "02:30", "/f"]
    log("注册 Windows 定时任务 (每天 02:30):")
    log("  " + " ".join(cmd))
    try:
        r = subprocess.run(cmd, timeout=30)
        log(f"  schtasks 退出码: {r.returncode} "
            + ("(注册成功)" if r.returncode == 0 else "(注册失败, 请手动执行上面的命令)"))
        return r.returncode == 0
    except Exception as e:  # noqa: BLE001
        log(f"  schtasks 执行失败: {str(e)[:150]} (请手动执行上面的命令)")
        return False


# ============================================================
# 入口
# ============================================================
def main() -> int:
    global _ARGS, _CTX, _LOG
    ap = argparse.ArgumentParser(
        description="data_foundation 每日增量任务调度器 (L0 增量 -> L1/L2 重建)")
    ap.add_argument("--sources", default=None,
                    help="只跑部分源, 逗号分隔: " + ",".join(ALL_SOURCES))
    ap.add_argument("--date", default=None, help="日期戳 YYYYMMDD (默认今天 UTC)")
    ap.add_argument("--register-task", action="store_true",
                    help="注册 Windows 每日 02:30 定时任务")
    _ARGS = ap.parse_args()

    if _ARGS.register_task:
        _register_task()
        return 0

    date_str = _ARGS.date or datetime.now(timezone.utc).strftime("%Y%m%d")
    if len(date_str) != 8 or not date_str.isdigit():
        print("--date 需为 YYYYMMDD 格式", file=sys.stderr)
        return 2
    _CTX = {"date": date_str,
            "ingest_date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
            "log": os.path.join(DATA_ROOT, f"daily_run_{date_str}.log")}
    _LOG = _CTX["log"]

    sources = [s.strip() for s in _ARGS.sources.split(",")] if _ARGS.sources \
        else list(ALL_SOURCES)
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        print(f"未知源: {unknown}", file=sys.stderr)
        return 2

    log("=" * 70)
    log(f"每日增量调度开始 | 日期戳={date_str} | ingest_date={_CTX['ingest_date']}")
    log(f"源: {sources}")
    log(f"代理: {PROXY} | 日志: {_LOG}")
    # 网络链路启动探测 (vision直连 -> 7897 -> 专用端口 -> socks5/钉IP), 失败驱动降级
    try:
        _pr = netpath.probe(timeout=8)
        _line = ", ".join(
            f"{k}={'%dms' % v['ms'] if v.get('ok') else '不可用'}"
            for k, v in _pr.items())
        log(f"网络链路探测: {_line}")
    except Exception as e:  # noqa: BLE001
        log(f"[warn] 网络链路探测失败: {str(e)[:80]} (各源仍会自行重试)")
    log("=" * 70)

    results = {}
    for name in sources:
        results[name] = _run_source(name, SOURCES[name])

    failed = [k for k, v in results.items() if v["status"] == "failed"]
    man = _write_manifest(results)
    log("=" * 70)
    log(f"调度完成 | 失败源: {failed if failed else '无'}")
    log(f"manifest: {man}")
    log("=" * 70)
    print("\n各源结果:")
    for k, v in results.items():
        print(f"  {k}: {v['status']}"
              + (f" (batches={v['batches']}, {v['elapsed']:.0f}s)" if v["status"] == "ok" else "")
              + (f" | {v['detail']}" if v["detail"] else ""))
    print(f"\n失败源列表: {failed if failed else '无'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
