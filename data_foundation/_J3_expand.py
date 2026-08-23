# -*- coding: utf-8 -*-
"""
_J3_expand.py — 子代理 J3: Binance 永续衍生品数据扩容 (15 MVP -> 全现货宇宙∩有永续 343 币)
=========================================================================================
四类数据 (每 sym ∈ T, 全历史回填):
  1. 资金费率全历史   : /fapi/v1/fundingRate      -> raw derivatives_funding/{SYM}_funding_v3.csv.gz
  2. 标记价 1h K线    : /fapi/v1/markPriceKlines  -> raw derivatives_mark_price/{SYM}_mark_v3.csv.gz
  3. 指数价 1h K线    : /fapi/v1/indexPriceKlines -> raw derivatives_index_price/{SYM}_index_v3.csv.gz
  4. 永续 1h K线      : /fapi/v1/klines           -> raw perpetual_klines_1h/{SYM}_v3.csv.gz

L0 -> L1 (derivatives.normalize_*/l1.normalize_klines) -> L2 (certify + write_certified)。
幂等: (dataset, sym) 的 L0 批次存在即跳过抓取; L1 合并写 + L2 全量重写均安全重跑;
      _J3_state.json 记录每 sym 已完成的数据集, 中断可续跑。
红线: 不改 schema/run_daily/reader/finalize/universe_builder; 不删文件; 全程 UTC;
      不调 finalize_all; 每请求间隔 >=0.2s。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation import netpath  # noqa: E402
from data_foundation.config import RAW_DIR, CERTIFIED_DIR  # noqa: E402
from data_foundation import l1 as l1m  # noqa: E402
from data_foundation import derivatives as der  # noqa: E402
from data_foundation.l2 import (  # noqa: E402
    build_dataset_manifest, certify_candles, certify_derivatives,
    write_certified, write_certified_derivatives)

FAPI = "https://fapi.binance.com"
TARGETS_CSV = "_J3_targets.csv"
STATE_PATH = "_J3_state.json"
REPORT_PATH = "_J3_report.json"
DISK_GUARD_GB = 4.5
PAGE_SLEEP = 0.2            # 页间礼貌间隔 (>=0.2s, netpath 另有全局 0.12s 限速)
SYMBOL_SLEEP = 0.3          # symbol 间额外间隔
RETRY_429_SLEEP = 30        # 429 退避
PROGRESS_EVERY = 25
BASE_START_MS = 1567296000000   # 2019-09-01 (Binance 永续上线前; startTime=0 会被 API 当作近期窗口)

KLINE_HEADERS = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                 "Close Time", "Quote Asset Volume", "Number of Trades",
                 "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
                 "Ignore"]
FUND_HEADERS = ["funding_time", "funding_rate", "mark_price"]
OHLC_HEADERS = ["open_time", "open", "high", "low", "close"]


class EmptySymbol(Exception):
    """该 symbol 无此数据 (Invalid symbol / 404 / 空响应)。"""


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dt(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S")


def _dt3(ms: int) -> str:
    """资金费率时间格式: 与 v1 批次一致的三位毫秒 (%Y-%m-%d %H:%M:%S.%f)。"""
    d = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return f"{d:%Y-%m-%d %H:%M:%S}.{int(ms) % 1000:03d}"


def disk_free_gb() -> float:
    return shutil.disk_usage(os.path.dirname(HERE)).free / 1e9


# ---------------------------------------------------------------------------
# 数据集规格
# ---------------------------------------------------------------------------
def _funding_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame({
        "funding_time": [_dt3(r["fundingTime"]) for r in rows],
        "funding_rate": [r.get("fundingRate", "") for r in rows],
        "mark_price": [r.get("markPrice", "") for r in rows],
    })


def _ohlc_rows_to_df(rows: list[list]) -> pd.DataFrame:
    return pd.DataFrame(
        [[_dt(r[0]), r[1], r[2], r[3], r[4]] for r in rows],
        columns=OHLC_HEADERS)


def _kline_rows_to_df(rows: list[list]) -> pd.DataFrame:
    return pd.DataFrame(
        [[_dt(r[0]), r[1], r[2], r[3], r[4], r[5], _dt(r[6]), r[7], r[8],
          r[9], r[10], r[11]] for r in rows],
        columns=KLINE_HEADERS)


DATASETS: dict[str, dict] = {
    "funding": {
        "dataset": "derivatives_funding",
        "batch": lambda sym: f"{sym}_funding_v3",
        "url": f"{FAPI}/fapi/v1/fundingRate",
        "params": lambda sym, cur: {"symbol": sym, "startTime": cur,
                                    "limit": 1000},
        "cursor": lambda rows: int(rows[-1]["fundingTime"]) + 1,
        "limit": 1000,
        "to_df": _funding_rows_to_df,
        "normalize": der.normalize_funding,
        "time_col": "funding_time_utc",
        "core": ["funding_rate"],
        "kind": "derivatives",
        "raw_dir": os.path.join(RAW_DIR, "binance", "derivatives_funding"),
        "market_type": "perpetual",
    },
    "mark": {
        "dataset": "derivatives_mark_price",
        "batch": lambda sym: f"{sym}_mark_v3",
        "url": f"{FAPI}/fapi/v1/markPriceKlines",
        "params": lambda sym, cur: {"symbol": sym, "interval": "1h",
                                    "startTime": cur, "limit": 1000},
        "cursor": lambda rows: int(rows[-1][0]) + 3600_000,
        "limit": 1000,
        "to_df": _ohlc_rows_to_df,
        "normalize": der.normalize_mark_price,
        "time_col": "open_time_utc",
        "core": ["mark_open", "mark_high", "mark_low", "mark_close"],
        "kind": "derivatives",
        "raw_dir": os.path.join(RAW_DIR, "binance", "derivatives_mark_price"),
        "market_type": "perpetual",
    },
    "index": {
        "dataset": "derivatives_index_price",
        "batch": lambda sym: f"{sym}_index_v3",
        "url": f"{FAPI}/fapi/v1/indexPriceKlines",
        "params": lambda sym, cur: {"pair": sym, "interval": "1h",
                                    "startTime": cur, "limit": 1000},
        "cursor": lambda rows: int(rows[-1][0]) + 3600_000,
        "limit": 1000,
        "to_df": _ohlc_rows_to_df,
        "normalize": der.normalize_index_price,
        "time_col": "open_time_utc",
        "core": ["index_open", "index_high", "index_low", "index_close"],
        "kind": "derivatives",
        "raw_dir": os.path.join(RAW_DIR, "binance", "derivatives_index_price"),
        "market_type": "perpetual",
    },
    "perp": {
        "dataset": "market_candle_perpetual_1h",
        "batch": lambda sym: f"{sym}_v3",
        "url": f"{FAPI}/fapi/v1/klines",
        "params": lambda sym, cur: {"symbol": sym, "interval": "1h",
                                    "startTime": cur, "limit": 1500},
        "cursor": lambda rows: int(rows[-1][0]) + 3600_000,
        "limit": 1500,
        "to_df": _kline_rows_to_df,
        "normalize": None,          # perp 走 l1.normalize_klines
        "time_col": "open_time_utc",
        "core": None,
        "kind": "candles",
        "raw_dir": os.path.join(RAW_DIR, "binance", "perpetual_klines_1h"),
        "market_type": "perpetual",
    },
}


# ---------------------------------------------------------------------------
# 抓取: 游标分页 (startTime -> 最早)
# ---------------------------------------------------------------------------
def warm_channel() -> str:
    """探测并固定到可用的 fapi 通道 (本机代理池仅个别端口稳定)。"""
    for _ in range(3):
        netpath.probe(ref_urls={"*": f"{FAPI}/fapi/v1/time"})
        snap = netpath.stats_snapshot()
        ch = snap.get("current_channel")
        if ch:
            log(f"通道探测: {ch} (switches={snap.get('switches')})")
            return ch
        time.sleep(3)
    log("通道探测: 暂无可用通道 (代理池全故障)")
    return "?"


def _request_page(spec: dict, sym: str, cur: int):
    """单页请求; 429 退避 30s; 持续 4xx -> EmptySymbol; 最多 12 次尝试。

    连续失败时重新探测通道 (代理池不稳定)。netpath 对非 2xx 直接抛错,
    4xx 多为代理瞬时噪声, 重试到通道恢复; 连续 6 次 4xx 才判无数据。
    """
    bad400 = 0
    for attempt in range(12):
        try:
            r = netpath.request("GET", spec["url"],
                                params=spec["params"](sym, cur),
                                timeout=20, retries=3)
            if r.status_code == 429:
                time.sleep(RETRY_429_SLEEP)
                continue
            r.raise_for_status()
            return r.json()
        except EmptySymbol:
            raise
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if "400" in msg or "404" in msg or "Invalid symbol" in msg:
                bad400 += 1
                if bad400 >= 6:
                    raise EmptySymbol(f"{sym}: 持续 HTTP 4xx ({msg[:80]})")
                try:
                    warm_channel()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(6)
                continue
            if attempt in (3, 6, 9):
                try:
                    warm_channel()
                except Exception:  # noqa: BLE001
                    pass
            if attempt == 11:
                raise RuntimeError(
                    f"{sym} 页失败 (cur={cur}): {msg[:110]}") from e
            time.sleep(4 * (attempt + 1))


def fetch_paged(spec: dict, sym: str) -> pd.DataFrame | None:
    """游标分页到最早, 返回原始列头 DataFrame; 无数据/无效 symbol 返回 None。"""
    rows_all: list = []
    cur = BASE_START_MS
    while True:
        resp = _request_page(spec, sym, cur)
        if not isinstance(resp, list) or len(resp) == 0:
            break
        rows_all.extend(resp)
        if len(resp) < spec["limit"]:
            break
        cur = spec["cursor"](resp)
        time.sleep(PAGE_SLEEP)
    if not rows_all:
        return None
    df = spec["to_df"](rows_all)
    df = df.drop_duplicates().reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# L0 写入 (gz + meta)
# ---------------------------------------------------------------------------
def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_l0(spec: dict, sym: str, df: pd.DataFrame,
             extra_source: dict | None = None) -> str:
    batch_id = spec["batch"](sym)
    ingest = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dst_dir = os.path.join(spec["raw_dir"], f"ingest_date={ingest}")
    os.makedirs(dst_dir, exist_ok=True)
    gz_path = os.path.join(dst_dir, f"{batch_id}.csv.gz")
    tmp = gz_path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False, header=True, lineterminator="\n")
    os.replace(tmp, gz_path)
    source = {
        "api": spec["url"].replace(FAPI, "fapi.binance.com"),
        "symbol": sym,
        "interval": "1h" if spec["kind"] == "candles"
        or spec["dataset"] != "derivatives_funding" else None,
        "market_type": spec["market_type"],
        "cursor": "startTime 分页到最早 (整页 1000/1500)",
        "fetched_at": now_iso(),
    }
    if extra_source:
        source.update(extra_source)
    meta = {
        "batch_id": batch_id,
        "source": source,
        "ingested_at": now_iso(),
        "timestamp_unit": "ms",
        "timezone": "UTC",
        "checksum_sha256": _sha256(gz_path),
        "file_size_bytes": os.path.getsize(gz_path),
        "row_count": int(len(df)),
        "immutable": True,
    }
    with open(gz_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return gz_path


def l0_batch_exists(spec: dict, sym: str) -> bool:
    batch_id = spec["batch"](sym)
    for ingest in os.listdir(spec["raw_dir"]) if os.path.isdir(spec["raw_dir"]) else []:
        if not ingest.startswith("ingest_date="):
            continue
        if os.path.isfile(os.path.join(spec["raw_dir"], ingest,
                                       f"{batch_id}.csv.gz")):
            return True
    return False


# ---------------------------------------------------------------------------
# L1 + L2 (单 sym 单数据集)
# ---------------------------------------------------------------------------
def build_l1_l2(spec: dict, sym: str) -> dict:
    """读取该 sym 全部 L0 批次 -> L1 合并写 -> L2 certify + 写。返回 stats。"""
    inst = l1m.instrument_id(sym)
    if spec["kind"] == "candles":
        raw_paths = l1m.load_raw_batches("binance", "perpetual_klines_1h", sym)
        if not raw_paths:
            raise RuntimeError(f"{sym}: 无 L0 perpetual_klines 批次")
        frames = [pd.read_csv(p, compression="infer") for p in raw_paths]
        df = pd.concat(frames, ignore_index=True)
        for c in ("Open Time", "Close Time"):
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
        df = l1m.normalize_klines(df, "binance", "perpetual", sym, "1h")
        df["source_batch_id"] = spec["batch"](sym)
        l1m.write_parquet(df, spec["dataset"], "binance", "perpetual", inst, "1h")
        cdf = certify_candles(df)
        _, stats = write_certified(cdf, spec["dataset"], "binance", "perpetual",
                                   inst, "1h")
    else:
        nf = spec["normalize"]
        ddf = nf("binance", sym)
        if ddf.empty:
            raise RuntimeError(f"{sym}: {spec['dataset']} L1 标准化后为空")
        der.write_derivatives_parquet(ddf, spec["dataset"], "binance", inst,
                                      spec["time_col"])
        cdf = certify_derivatives(ddf, spec["time_col"],
                                  core_numeric_cols=spec["core"])
        write_certified_derivatives(cdf, spec["dataset"], "binance", inst,
                                    spec["time_col"])
        stats = {
            "row_count": int(len(cdf)),
            "duplicate_count": int(cdf[spec["time_col"]].duplicated().sum()),
            "gap_count": 0,
            "suspect_count": int(cdf["is_suspect"].sum()),
            "coverage_start": str(cdf[spec["time_col"]].min()),
            "coverage_end": str(cdf[spec["time_col"]].max()),
        }
    return stats


# ---------------------------------------------------------------------------
# 状态 (断点续跑)
# ---------------------------------------------------------------------------
def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"done": {}, "empty": {}, "failures": [], "accum": {},
            "last_update": None}


def save_state(state: dict):
    state["last_update"] = now_iso()
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)


def acc_stats(accum: dict, ds: str, stats: dict):
    s = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                              "gap_count": 0, "suspect_count": 0,
                              "coverage_start": None, "coverage_end": None})
    s["row_count"] += stats["row_count"]
    s["duplicate_count"] += stats["duplicate_count"]
    s["gap_count"] += stats["gap_count"]
    s["suspect_count"] += stats["suspect_count"]
    if s["coverage_start"] is None or stats["coverage_start"] < s["coverage_start"]:
        s["coverage_start"] = stats["coverage_start"]
    if s["coverage_end"] is None or stats["coverage_end"] > s["coverage_end"]:
        s["coverage_end"] = stats["coverage_end"]


# ---------------------------------------------------------------------------
# 单 symbol 处理
# ---------------------------------------------------------------------------
def process_symbol(sym: str, state: dict) -> dict:
    done = state["done"].get(sym, [])
    empty = state["empty"].get(sym, [])
    summary = {"symbol": sym, "ok": [], "empty": [], "skipped": [], "failed": []}
    for name, spec in DATASETS.items():
        ds = spec["dataset"]
        if name in done:
            summary["skipped"].append(name)
            continue
        if name in empty:
            summary["empty"].append(name)
            continue
        try:
            if not l0_batch_exists(spec, sym):
                raw = fetch_paged(spec, sym)
                if raw is None or len(raw) == 0:
                    state["empty"].setdefault(sym, []).append(name)
                    summary["empty"].append(name)
                    log(f"  {sym} {name}: 无数据, 记空")
                    continue
                gz = write_l0(spec, sym, raw)
                log(f"  {sym} {name}: L0 {len(raw)} 行 -> "
                    f"{os.path.relpath(gz, HERE)}")
            else:
                log(f"  {sym} {name}: L0 批次已存在, 跳过抓取")
            stats = build_l1_l2(spec, sym)
            stats["symbol"] = sym
            stats["dataset"] = ds
            stats["key"] = name
            acc_stats(state["accum"], ds, stats)
            done.append(name)
            state["done"][sym] = done
            summary["ok"].append(name)
            log(f"  {sym} {name}: L2 {stats['row_count']} 行 suspect="
                f"{stats['suspect_count']} "
                f"{str(stats['coverage_start'])[:16]} ~ "
                f"{str(stats['coverage_end'])[:16]}")
        except EmptySymbol as e:
            state["empty"].setdefault(sym, []).append(name)
            summary["empty"].append(name)
            log(f"  {sym} {name}: 无数据 ({str(e)[:80]})")
        except Exception as e:  # noqa: BLE001
            err = str(e)[:300]
            state["failures"].append({"symbol": sym, "dataset": name,
                                      "error": err})
            summary["failed"].append({"dataset": name, "error": err})
            log(f"  {sym} {name}: FAILED {err[:150]}")
        time.sleep(SYMBOL_SLEEP)
    save_state(state)
    return summary


# ---------------------------------------------------------------------------
# manifest (每个数据集一次, binance venue 级)
# ---------------------------------------------------------------------------
def write_manifests(state: dict, processed: list[str]):
    src_batches = [f"{s}_v3" for s in sorted(processed)]
    rules = {
        "note": ("Binance 永续衍生品扩容 (子代理 J3, v3): 现货宇宙∩有永续全部 "
                 "symbol 全历史回填; funding=8h 资金费率, mark/index=1h K线, "
                 "perp=永续 1h K线; 源=fapi REST 游标分页 (startTime=0 起); "
                 "资金费 mark_price 为结算时标记价"),
        "version": "1.0",
    }
    for ds, s in state["accum"].items():
        if not s or not s.get("row_count"):
            continue
        mtype = "perpetual"
        build_dataset_manifest(ds, "binance", mtype, "*", "*", s,
                               src_batches, rules)
        log(f"[manifest] {ds}: rows={s['row_count']} suspect={s['suspect_count']} "
            f"{str(s['coverage_start'])[:10]} ~ {str(s['coverage_end'])[:10]}")


# ---------------------------------------------------------------------------
# 收尾验证
# ---------------------------------------------------------------------------
def verify_samples(syms: list[str]):
    print("\n==== 抽样验证: 资金费起点/行数 ====", flush=True)
    for sym in syms:
        inst = l1m.instrument_id(sym)
        p = os.path.join(CERTIFIED_DIR, "derivatives_funding", "binance",
                         inst, "data.parquet")
        if not os.path.isfile(p):
            print(f"  {sym}: 无 certified funding", flush=True)
            continue
        import pyarrow.parquet as pq
        df = pq.read_table(p).to_pandas()
        df["funding_time_utc"] = pd.to_datetime(df["funding_time_utc"], utc=True)
        print(f"  {sym}: funding {len(df)} 行, "
              f"{df['funding_time_utc'].min()} ~ {df['funding_time_utc'].max()}",
              flush=True)
    total_suspect = sum(int(s.get("suspect_count", 0))
                        for s in state_accum_ref().values())
    print(f"\n==== certified suspect 总数: {total_suspect} ====", flush=True)


def state_accum_ref():
    # 简单闭包: 由 main 注入
    return _ACCUM


_ACCUM: dict = {}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", default=TARGETS_CSV)
    ap.add_argument("--limit", type=int, default=None,
                    help="仅处理前 N 个 symbol (测试用)")
    args = ap.parse_args()
    t0 = time.time()
    warm_channel()
    targets = [s.strip() for s in open(args.targets, encoding="utf-8")
               if s.strip() and not s.startswith("symbol")]
    if args.limit:
        targets = targets[:args.limit]
    log(f"目标 {len(targets)} 个 symbol")
    state = load_state()
    state["failures"] = []          # 本次运行重新计数 (done/empty 标记保留)
    global _ACCUM
    _ACCUM = state["accum"]

    results, empties = [], []
    for i, sym in enumerate(targets, 1):
        free = disk_free_gb()
        if free < DISK_GUARD_GB:
            log(f"[DISK-GUARD] 剩余 {free:.2f}GB < {DISK_GUARD_GB}GB, 停止")
            state["failures"].append({"symbol": sym, "dataset": "disk_guard",
                                      "error": "disk_guard"})
            break
        try:
            summary = process_symbol(sym, state)
            results.append(summary)
            if summary["empty"]:
                empties.append({"symbol": sym, "empty": summary["empty"]})
        except Exception as e:  # noqa: BLE001
            err = str(e)[:300]
            state["failures"].append({"symbol": sym, "dataset": "symbol",
                                      "error": err})
            results.append({"symbol": sym, "failed": [{"dataset": "symbol",
                                                       "error": err}]})
            log(f"{sym}: SYMBOL FAILED {err[:150]}")
        if i % PROGRESS_EVERY == 0 or i == len(targets):
            el = time.time() - t0
            rate = i / el if el > 0 else 0
            eta = (len(targets) - i) / rate / 60 if rate > 0 else 0
            n_ok = sum(len(r.get("ok", [])) for r in results)
            n_empty = sum(len(r.get("empty", [])) for r in results)
            n_fail = len(state["failures"])
            log(f"[PROGRESS] {i}/{len(targets)} | ok={n_ok} empty={n_empty} "
                f"fail={n_fail} | 耗时={el:.0f}s | ETA≈{eta:.0f}min")
    save_state(state)

    # manifest
    processed = [s for s in targets
                 if len(state["done"].get(s, [])) > 0]
    write_manifests(state, processed)

    ok_syms = len([s for s in targets if len(state["done"].get(s, [])) == 4])
    log(f"== 完成: {ok_syms}/{len(targets)} symbol 四数据集齐全, "
        f"failures={len(state['failures'])}, 耗时 {time.time()-t0:.0f}s ==")
    for f in state["failures"][-30:]:
        log(f"  FAIL {f['symbol']}/{f['dataset']}: {f['error'][:120]}")

    # 抽样 3 个新币
    sample = [s for s in ["ALGOUSDT", "ZECUSDT", "SUIUSDT"] if s in targets]
    if not sample:
        sample = [s for s in targets if len(state["done"].get(s, [])) >= 1][:3]
    verify_samples(sample)

    report = {
        "targets_total": len(targets),
        "ok_symbols": ok_syms,
        "failures": state["failures"],
        "empties": empties,
        "accum": state["accum"],
        "elapsed_sec": int(time.time() - t0),
        "done": state["done"],
    }
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    log(f"报告 -> {REPORT_PATH}")


if __name__ == "__main__":
    main()
