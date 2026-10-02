# -*- coding: utf-8 -*-
"""refresh_perp_daily.py — Binance 永续 K线 / 标记价 / 指数价 每日增量刷新

背景
----
历史上 Binance 永续三件套 (market_candle_perpetual_1h / derivatives_mark_price /
derivatives_index_price) 只在一次性的 J3 回填脚本里生成过, 之后 nightly 流水线里
**没有对应的每日源**, 导致这三类数据自 2026-08-23 起冻结 (停滞 39 天)。本脚本补上
这个缺口: 抓最近 N 天 (默认 3 天, 可 --days 加大用于补历史) 写入 L0 raw 批次, 再
复用 l1.normalize_* / derivatives.normalize_* 从 raw 合并重建 L1 并认证到 L2。

安全设计 (与既有 derivatives.py 合并语义一致)
- L0 原始批次不可变, 按 ingest_date 落盘
- L1 写入用 write_derivatives_parquet (读旧 -> concat -> 按时间列去重 keep=last),
  不会因为每日增量而丢掉深回填的历史
- mark/index 的 daily 增量只覆盖最近窗口, 历史靠既有 raw 批次重放

用法
----
    python refresh_perp_daily.py                 # MVP 15 币, 近 3 天
    python refresh_perp_daily.py --assets all    # 全部已有 L1 永续合约
    python refresh_perp_daily.py --days 45       # 补 45 天历史
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from data_foundation import netpath  # noqa: E402
from data_foundation.derivatives import (  # noqa: E402
    normalize_index_price, normalize_mark_price, write_derivatives_parquet)
from data_foundation.l1 import derive_aggregates, instrument_id, normalize_klines, write_parquet  # noqa: E402
from data_foundation.l2 import (  # noqa: E402
    build_dataset_manifest, certify_candles, certify_derivatives, write_certified,
    write_certified_derivatives)
from data_foundation.config import CERTIFIED_DIR, L1_DIR, MVP_ASSETS, RAW_DIR  # noqa: E402

FAPI = "https://fapi.binance.com"
PAIRS_FOR_INDEX = None  # indexPriceKlines 用 pair(=symbol) 而非 symbol


def _log(msg: str) -> None:
    print(msg, flush=True)


def _get_json(url, params, retries=6, timeout=25):
    return netpath.fetch_json(url, params=params, retries=retries, timeout=timeout)


def _probe_fapi() -> bool:
    try:
        _get_json(f"{FAPI}/fapi/v1/ping", {}, retries=4, timeout=20)
        return True
    except Exception as e:  # noqa: BLE001
        _log(f"  [warn] fapi 不可达: {str(e)[:80]}")
        return False


def _ms(dt) -> int:
    return int(pd.Timestamp(dt).timestamp() * 1000)


def _dt_iso(ms) -> str:
    # 与既有 raw 批次格式一致 (naive UTC, 无时区后缀), 否则 derivatives.normalize_*
    # 的 pd.to_datetime(..., utc=True) 严格解析会因 '+00:00' 后缀报 ValueError
    return pd.to_datetime(ms, unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S")


def discover_perp_assets(mode: str) -> list[str]:
    """返回要刷新的合约 base 资产列表。"""
    if mode == "mvp":
        return list(MVP_ASSETS)
    # all: 扫描 L1 永续 K线目录
    root = os.path.join(L1_DIR, "market_candle_perpetual_1h", "binance", "perpetual")
    out = []
    if os.path.isdir(root):
        for inst in os.listdir(root):
            if inst.endswith("-USDT"):
                out.append(inst.split("-")[0])
    return sorted(set(out))


# ---------------------------------------------------------------------------
# 抓取 + 写 raw
# ---------------------------------------------------------------------------
def _fetch_klines(sym: str, start_ms: int, end_ms: int) -> list[list]:
    rows, cur = [], start_ms
    while cur < end_ms:
        page = _get_json(f"{FAPI}/fapi/v1/klines",
                         {"symbol": sym, "interval": "1h",
                          "startTime": cur, "limit": 1500})
        if not page:
            break
        rows.extend(page)
        nxt = int(page[-1][0]) + 3600_000
        if nxt <= cur:
            break
        cur = nxt
        if len(page) < 1500:
            break
    return [r for r in rows if start_ms <= int(r[0]) < end_ms]


def _fetch_ohlc(url: str, key: str, sym: str, start_ms: int, end_ms: int,
                limit: int = 1000) -> list[list]:
    """mark/index 价 K 线分页抓取。

    注意: markPriceKlines / indexPriceKlines 单页上限 1000 根, 超过窗口 (如 45 天
    = 1080 根) 必须用 endTime 向前翻页, 否则会**静默截断**在最早 1000 根。
    """
    rows, cur = [], start_ms
    while cur < end_ms:
        page = _get_json(url, {key: sym, "interval": "1h",
                               "startTime": cur, "endTime": end_ms, "limit": limit})
        if not page:
            break
        rows.extend(page)
        nxt = int(page[-1][0]) + 3600_000
        if nxt <= cur:
            break
        cur = nxt
        if len(page) < limit:
            break
    return [r for r in rows if start_ms <= int(r[0]) < end_ms]


def _write_raw_batch(sym: str, dataset: str, rows: list, batch: str, source: dict,
                     headers: list, ingest_date: str) -> None:
    d = os.path.join(RAW_DIR, "binance", dataset, f"ingest_date={ingest_date}")
    os.makedirs(d, exist_ok=True)
    csv = os.path.join(d, f"{batch}.csv")
    pd.DataFrame(rows, columns=headers).to_csv(csv, index=False)
    meta = {
        "batch_id": batch, "source_path": csv, "source": source,
        "ingested_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "timestamp_unit": "ms", "timezone": "UTC", "immutable": True,
        "file_size_bytes": os.path.getsize(csv),
    }
    import hashlib
    h = hashlib.sha256()
    with open(csv, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    meta["checksum_sha256"] = h.hexdigest()
    import json
    with open(f"{csv}.meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)


KLINE_RAW_HDRS = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                  "Close Time", "Quote Asset Volume", "Number of Trades",
                  "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
                  "Ignore"]

# mark/index 价 K 线 raw 表头 (与 derivatives.normalize_mark_price/index_price 对齐)
OHLC_RAW_HDRS = ["open_time", "open", "high", "low", "close"]


def _kline_row(r):
    return [_dt_iso(r[0]), r[1], r[2], r[3], r[4], r[5], _dt_iso(r[6]),
            r[7], r[8], r[9], r[10], r[11]]


def fetch_and_write_raw(assets: list[str], days: int, ingest_date: str) -> int:
    end_ms = _ms(pd.Timestamp.now(tz="UTC").floor("h"))
    start_ms = end_ms - days * 86400_000
    n_batch = 0
    for i, a in enumerate(assets, 1):
        sym = f"{a}USDT"
        try:
            # 1) 永续 K线
            rows = _fetch_klines(sym, start_ms, end_ms)
            if rows:
                _write_raw_batch(
                    sym, "perpetual_klines_1h",
                    [_kline_row(r) for r in rows],
                    f"{sym}_perp_daily_{ingest_date}",
                    {"api": f"{FAPI}/fapi/v1/klines", "symbol": sym, "interval": "1h",
                     "days": days, "start_ms": start_ms, "end_ms": end_ms,
                     "fetched_at": pd.Timestamp.now(tz="UTC").isoformat()},
                    KLINE_RAW_HDRS, ingest_date)
                n_batch += 1
            # 2) 标记价 K线
            rows = _fetch_ohlc(f"{FAPI}/fapi/v1/markPriceKlines", "symbol",
                               sym, start_ms, end_ms)
            if rows:
                _write_raw_batch(
                    sym, "derivatives_mark_price",
                    [[_dt_iso(r[0]), r[1], r[2], r[3], r[4]] for r in rows],
                    f"{sym}_mark_daily_{ingest_date}",
                    {"api": f"{FAPI}/fapi/v1/markPriceKlines", "symbol": sym,
                     "days": days, "fetched_at": pd.Timestamp.now(tz="UTC").isoformat()},
                    OHLC_RAW_HDRS, ingest_date)
                n_batch += 1
            # 3) 指数价 K线 (用 pair)
            rows = _fetch_ohlc(f"{FAPI}/fapi/v1/indexPriceKlines", "pair",
                               sym, start_ms, end_ms)
            if rows:
                _write_raw_batch(
                    sym, "derivatives_index_price",
                    [[_dt_iso(r[0]), r[1], r[2], r[3], r[4]] for r in rows],
                    f"{sym}_index_daily_{ingest_date}",
                    {"api": f"{FAPI}/fapi/v1/indexPriceKlines", "pair": sym,
                     "symbol": sym,
                     "days": days, "fetched_at": pd.Timestamp.now(tz="UTC").isoformat()},
                    OHLC_RAW_HDRS, ingest_date)
                n_batch += 1
        except Exception as e:  # noqa: BLE001
            _log(f"  [{i}/{len(assets)}] {sym}: 抓取失败 {str(e)[:80]}")
        time.sleep(0.12)  # 轻度限速, 避免触发 fapi 权重限制
    return n_batch


# ---------------------------------------------------------------------------
# raw -> L1 -> L2
# ---------------------------------------------------------------------------
def rebuild_l1_l2(assets: list[str], derive_4h: bool = True) -> dict:
    """从 raw 合并重建这三类数据的 L1 + 认证 L2。"""
    from data_foundation.derivatives import _concat_raw
    stats = {}
    for a in assets:
        sym = f"{a}USDT"
        inst = f"{a}-USDT"
        # 永续 K线
        raw = _concat_raw("binance", "perpetual_klines_1h", sym)
        if not raw.empty:
            norm = normalize_klines(raw, "binance", "perpetual", sym, "1h")
            write_derivatives_parquet(norm, "market_candle_perpetual_1h",
                                      "binance", inst, "open_time_utc")
            # 认证 L2
            cert = certify_candles(norm)
            write_certified(cert, "market_candle_perpetual_1h", "binance",
                            "perpetual", inst, "1h")
            if derive_4h:
                agg4 = derive_aggregates(norm, "4h")
                write_derivatives_parquet(agg4, "market_candle_perpetual_4h",
                                          "binance", inst, "open_time_utc")
                write_certified(certify_candles(agg4), "market_candle_perpetual_4h",
                                "binance", "perpetual", inst, "4h")
        # 标记价
        mk = normalize_mark_price("binance", sym)
        if not mk.empty:
            c = certify_derivatives(mk, "open_time_utc",
                                    core_numeric_cols=["mark_open", "mark_high",
                                                       "mark_low", "mark_close"])
            write_certified_derivatives(c, "derivatives_mark_price", "binance",
                                        inst, "open_time_utc")
        # 指数价
        ix = normalize_index_price("binance", sym)
        if not ix.empty:
            c = certify_derivatives(ix, "open_time_utc",
                                    core_numeric_cols=["index_open", "index_high",
                                                       "index_low", "index_close"])
            write_certified_derivatives(c, "derivatives_index_price", "binance",
                                        inst, "open_time_utc")
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", default="mvp", choices=["mvp", "all"],
                    help="mvp=15 核心币; all=全部已有 L1 永续合约")
    ap.add_argument("--days", type=int, default=3, help="回溯天数")
    ap.add_argument("--no-derive-4h", action="store_true")
    ap.add_argument("--rebuild-only", action="store_true",
                    help="跳过抓取, 仅从既有 raw 重建 L1/L2")
    args = ap.parse_args(argv)

    if not _probe_fapi() and not args.rebuild_only:
        return 2
    assets = discover_perp_assets(args.assets)
    _log(f"[refresh_perp] assets={len(assets)} days={args.days} mode={args.assets}")

    if not args.rebuild_only:
        ingest_date = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
        nb = fetch_and_write_raw(assets, args.days, ingest_date)
        _log(f"[refresh_perp] 写入 raw 批次 {nb} 个")

    _log("[refresh_perp] 重建 L1/L2 ...")
    rebuild_l1_l2(assets, derive_4h=not args.no_derive_4h)
    _log("[refresh_perp] 完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
