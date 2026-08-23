# -*- coding: utf-8 -*-
"""
_K_backfill.py — 子代理 K: 研究层剩余全部 symbol 的 1h K 线回填 (universe_v3)
=============================================================================
目标: listing_universe 中 quote=USDT 且尚无 certified K 线 的全部 symbol
      (~661 个: 420 active + 241 delisted, 以已下架微流动性对为主), 让三层
      宇宙的价格数据完整。

路径 (每个 symbol, 与 G/J 同模式):
  * 抓取: data.binance.vision 月度 1h klines zip (内存解压, 绝不落盘);
    [first_period..last_period] 逐月, 404 跳过 (universe 枚举自 1h 档案,
    理论上 404 罕见; 无 1h 档案的 symbol 全部月份 404 -> 记入空清单)。
  * L0: raw/binance/spot_klines_1h/{SYM}_universe_v3.csv.gz + meta
        (months 清单 + ms|us 混合说明; 时间戳 2025 起 us 16 位, 按 |v|>=1e14 判断)
  * L1: l1.normalize_klines (normalize 前时间列转 ISO 字符串) + write_parquet
  * L2: l2.certify_candles + write_certified
  * 末尾 merge manifest (合并式) + 报告 JSON。

幂等: certified 覆盖到 last_period 所在月份即跳过; L0 gz 已存在则复用;
      每 symbol try/except; 磁盘守卫 <4.5GB 即停; 中途被杀可重跑续传。
红线: 不改 schema/run_daily/reader/finalize/universe_builder; 不删已有文件;
      不调 finalize_all; 全程 UTC。
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation import netpath  # noqa: E402
from data_foundation.config import RAW_DIR, CERTIFIED_DIR  # noqa: E402
from data_foundation import l1 as l1m  # noqa: E402
from data_foundation.l2 import (certify_candles, write_certified,  # noqa: E402
                                build_dataset_manifest)
from data_foundation.manifest import load_manifest  # noqa: E402
from data_foundation.finalize import scan_dataset  # noqa: E402

UNIVERSE = r"data/l1/listing_universe/binance_vision/data.parquet"
DATASET = "market_candle_spot_1h"
VENUE = "binance"
MARKET = "spot"
INTERVAL = "1h"
DISK_GUARD_GB = 4.5
SLEEP = 0.05            # netpath 已有全局 0.12s 限速, 这里只做轻量礼貌间隔
BATCH_SUFFIX = "_universe_v3"

VISION_HEADER = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                 "Close Time", "Quote Asset Volume", "Number of Trades",
                 "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume",
                 "Ignore"]

RAW_DATASET_DIR = os.path.join(RAW_DIR, VENUE, "spot_klines_1h")
LOG_FILE = "_K_backfill.log"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# 1) 选标的
# ---------------------------------------------------------------------------
def select_targets() -> tuple[list[dict], dict]:
    """quote=USDT 且 certified 目录不存在 且 生命周期>=365天 (研究层新规则) 的全部 symbol。"""
    MIN_LIFE_DAYS = 365
    uni = pd.read_parquet(UNIVERSE)
    cert_root = os.path.join(CERTIFIED_DIR, DATASET, VENUE, MARKET)
    have = set(os.listdir(cert_root)) if os.path.isdir(cert_root) else set()
    t = uni[uni["symbol"].str.endswith("USDT")].copy()
    t["inst"] = t["symbol"].map(l1m.instrument_id)
    t = t[~t["inst"].isin(have)]
    t = t[t["first_period"].notna() & t["last_period"].notna()]
    fp = pd.to_datetime(t["first_period"] + "-01")
    lp = pd.to_datetime(t["last_period"] + "-01") + pd.offsets.MonthEnd(1)
    t["life_days"] = (lp - fp).dt.days
    before = len(t)
    t = t[t["life_days"] >= MIN_LIFE_DAYS]
    t = t.sort_values(["status", "symbol"]).reset_index(drop=True)
    rows = t[["symbol", "first_period", "last_period", "status"]].to_dict("records")
    note = (f"listing_universe quote=USDT 共 {len(uni[uni['symbol'].str.endswith('USDT')])} 个, "
            f"已有 certified {len(have)} 个, 无K线 {before} 个, "
            f"按研究层规则(生命周期>={MIN_LIFE_DAYS}天)筛后回填 {len(rows)} 个 "
            f"(active={int((t['status']=='active').sum())}, "
            f"delisted={int((t['status']=='delisted').sum())}); "
            f"全走 Vision 月度 1h 归档, batch {BATCH_SUFFIX}。")
    return rows, note


# ---------------------------------------------------------------------------
# 2) 抓取 (Vision 月度, 内存解压)
# ---------------------------------------------------------------------------
def download_month(sym: str, ym: str, timeout: int = 60) -> bytes | None:
    """单月 zip 字节; 404 返回 None; 其他异常抛出。"""
    url = (f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1h/"
           f"{sym}-1h-{ym}.zip")
    try:
        r = netpath.request("GET", url, timeout=timeout, retries=2)
        return r.content
    except Exception as e:  # noqa: BLE001
        if "404" in str(e):
            return None
        raise


def fetch_symbol_months(sym: str, first_period: str, last_period: str
                        ) -> tuple[list[pd.DataFrame], list[str]]:
    """逐月下载 -> (frames, ok_months)。zip 内存解压不落盘。"""
    months = pd.period_range(first_period, last_period, freq="M").strftime("%Y-%m").tolist()
    frames, ok_months = [], []
    for ym in months:
        z = download_month(sym, ym)
        if z is None:
            continue
        with zipfile.ZipFile(io.BytesIO(z)) as zf:
            name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            raw = zf.read(name)
        df = pd.read_csv(io.BytesIO(raw), header=None, names=VISION_HEADER)
        frames.append(df)
        ok_months.append(ym)
        time.sleep(SLEEP)
    return frames, ok_months


def _parse_epoch_iso(col: pd.Series) -> pd.Series:
    """Vision 时间戳: 2025 前 13 位 ms, 2025 起 16 位 us; 按值判断 -> ISO 字符串。

    转成 ISO 字符串后交给 normalize_klines (其内部 pd.to_datetime(utc=True)
    解析), 与现有 certified 格式一致 (datetime64[us, UTC])。
    """
    v = pd.to_numeric(col, errors="coerce")
    out = pd.Series(pd.NaT, index=v.index, dtype="datetime64[ns, UTC]")
    ms = v.abs() < 1e14
    if ms.any():
        out[ms] = pd.to_datetime(v[ms], unit="ms", utc=True, errors="coerce")
    if (~ms).any():
        out[~ms] = pd.to_datetime(v[~ms], unit="us", utc=True, errors="coerce")
    return out.dt.strftime("%Y-%m-%d %H:%M:%S+00:00")


# ---------------------------------------------------------------------------
# 3) L0 写入
# ---------------------------------------------------------------------------
def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_l0(sym: str, frames: list[pd.DataFrame], ok_months: list[str],
             first_period: str, last_period: str) -> str:
    merged = pd.concat(frames, ignore_index=True)
    batch_id = f"{sym}{BATCH_SUFFIX}"
    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    os.makedirs(RAW_DATASET_DIR, exist_ok=True)
    tmp = gz_path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        merged.to_csv(f, index=False, header=True, lineterminator="\n")
    os.replace(tmp, gz_path)
    meta = {
        "batch_id": batch_id,
        "source": {
            "api": "binance-vision monthly klines",
            "url_template": ("https://data.binance.vision/data/spot/monthly/klines/"
                             "{SYM}/1h/{SYM}-1h-{YYYY-MM}.zip"),
            "symbol": sym,
            "interval": "1h",
            "market_type": "spot",
            "months": ok_months,
            "first_period": first_period,
            "last_period": last_period,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        },
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "timestamp_unit": "ms|us (per value; Binance 2025 起改用 us)",
        "timezone": "UTC",
        "checksum_sha256": _sha256(gz_path),
        "file_size_bytes": os.path.getsize(gz_path),
        "row_count": int(len(merged)),
        "immutable": True,
    }
    with open(gz_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return gz_path


def load_l0_raw(batch_id: str) -> pd.DataFrame:
    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        return pd.read_csv(f)


# ---------------------------------------------------------------------------
# 4) L1 + L2 (单 symbol)
# ---------------------------------------------------------------------------
def build_l1_l2(sym: str, raw: pd.DataFrame, batch_id: str) -> tuple[str, dict]:
    inst = l1m.instrument_id(sym)
    df = raw.copy()
    for c in ("Open Time", "Close Time"):
        df[c] = _parse_epoch_iso(df[c])
    df = l1m.normalize_klines(df, VENUE, MARKET, sym, INTERVAL)
    df["source_batch_id"] = batch_id
    l1_root = l1m.write_parquet(df, DATASET, VENUE, MARKET, inst, INTERVAL)
    cdf = certify_candles(df)
    root, stats = write_certified(cdf, DATASET, VENUE, MARKET, inst, INTERVAL)
    return root, stats


def certified_complete(sym: str, last_period: str) -> bool:
    """certified 已覆盖到 last_period 所在月份 (Vision 归档止于最后完整月)。

    active 与 delisted 统一按此判断 (universe 的 last_period 即归档末月)。
    """
    inst = l1m.instrument_id(sym)
    p = os.path.join(CERTIFIED_DIR, DATASET, VENUE, MARKET, inst,
                     f"interval={INTERVAL}", "data.parquet")
    if not os.path.isfile(p):
        return False
    import pyarrow.parquet as pq
    t = pq.read_table(p, columns=["open_time_utc"])
    mx = t.column("open_time_utc").to_pandas().max()
    if pd.isna(mx):
        return False
    return str(pd.Timestamp(mx))[:7] == last_period


def process_symbol(row: dict) -> dict:
    sym = row["symbol"]
    first_period = row["first_period"]
    last_period = row["last_period"]
    batch_id = f"{sym}{BATCH_SUFFIX}"

    if certified_complete(sym, last_period):
        return {"symbol": sym, "status": "skipped", "batch_id": batch_id}

    gz_path = os.path.join(RAW_DATASET_DIR, f"{batch_id}.csv.gz")
    if os.path.isfile(gz_path):
        log(f"  {sym}: 复用既有 L0")
        raw = load_l0_raw(batch_id)
        if len(raw) == 0:
            return {"symbol": sym, "status": "empty", "note": "L0 存在但 0 行"}
    else:
        frames, ok_months = fetch_symbol_months(sym, first_period, last_period)
        if not frames:
            return {"symbol": sym, "status": "empty",
                    "note": f"Vision 无任何 1h 月份数据 ({first_period}~{last_period})"}
        raw = pd.concat(frames, ignore_index=True)
        if len(raw) == 0:
            return {"symbol": sym, "status": "empty", "note": "月份文件均 0 行"}
        gz = write_l0(sym, frames, ok_months, first_period, last_period)
        log(f"  {sym}: L0 -> {os.path.relpath(gz, HERE)} "
            f"({len(ok_months)} 个月, {len(raw)} 行)")

    root, stats = build_l1_l2(sym, raw, batch_id)
    stats["symbol"] = sym
    stats["status"] = "ok"
    stats["batch_id"] = batch_id
    stats["cert_root"] = os.path.relpath(root, HERE)
    stats["rows"] = stats["row_count"]
    return stats


# ---------------------------------------------------------------------------
# 5) manifest (合并式, 保留既有 venue/batch)
# ---------------------------------------------------------------------------
def merge_manifest(new_batches: list[str], note: str) -> dict:
    ds_dir = os.path.join(CERTIFIED_DIR, DATASET)
    existing = load_manifest(ds_dir) if os.path.exists(
        os.path.join(ds_dir, "manifest.json")) else {}
    src = list(existing.get("source_batches") or [])
    for b in new_batches:
        if b not in src:
            src.append(b)
    stats = scan_dataset(ds_dir)
    agg = {"interval": "1h", "note": note}
    if isinstance(existing.get("aggregation_rules"), dict):
        prev = existing["aggregation_rules"].get("note", "")
        if prev:
            agg["note"] = prev + " | " + agg["note"]
    man = build_dataset_manifest(DATASET, VENUE, MARKET, "*", "*", stats,
                                 src, agg)
    log(f"manifest: rows={stats['row_count']} suspect={stats['suspect_count']} "
        f"{str(stats['coverage_start'])[:10]} ~ {str(stats['coverage_end'])[:10]} "
        f"batches={len(src)}")
    return man


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    targets, note = select_targets()
    log(f"目标 {len(targets)} 个: "
        f"active={sum(1 for r in targets if r['status']=='active')}, "
        f"delisted={sum(1 for r in targets if r['status']=='delisted')}")
    pd.DataFrame(targets).to_csv("_K_targets.csv", index=False)
    log(f"目标清单 -> _K_targets.csv; {note}")

    results, failures, empties = [], [], []
    new_batches = []
    cum_rows = 0
    for i, row in enumerate(targets, 1):
        free_gb = shutil.disk_usage(os.path.dirname(HERE)).free / 1e9
        if free_gb < DISK_GUARD_GB:
            log(f"磁盘守卫: 剩余 {free_gb:.2f}GB < {DISK_GUARD_GB}GB, 停止后续 symbol")
            failures.append({"symbol": row["symbol"], "error": "disk_guard"})
            break
        try:
            st = process_symbol(row)
            results.append(st)
            if st.get("status") == "ok":
                cum_rows += int(st.get("row_count", 0))
                new_batches.append(st["batch_id"])
            elif st.get("status") == "empty":
                empties.append({"symbol": row["symbol"],
                                "first_period": row["first_period"],
                                "last_period": row["last_period"],
                                "note": st.get("note", "")})
            elif st.get("status") == "skipped":
                new_batches.append(st["batch_id"])
        except Exception as e:  # noqa: BLE001
            log(f"  {row['symbol']}: FAILED {str(e)[:200]}")
            failures.append({"symbol": row["symbol"], "error": str(e)[:300]})
        if i % 25 == 0 or i == len(targets):
            el = time.time() - t0
            rate = i / el if el > 0 else 0
            eta = (len(targets) - i) / rate / 60 if rate > 0 else 0
            log(f"[PROGRESS] {i}/{len(targets)} 已完成 | "
                f"ok={len([r for r in results if r.get('status')=='ok'])} "
                f"skip={len([r for r in results if r.get('status')=='skipped'])} "
                f"empty={len(empties)} fail={len(failures)} | "
                f"累计行数={cum_rows:,} | 耗时={el:.0f}s | ETA≈{eta:.0f}min")

    note_k = ("研究层全量回填 (子代理 K, universe_v3): 全部 quote=USDT 且此前无 "
              "certified K 线的 symbol, Vision 月度 1h 归档, 时间戳 ms|us 按值判断; "
              "以已下架微流动性对为主。")
    if new_batches:
        merge_manifest(new_batches, note_k)

    ok = len([r for r in results if r.get("status") == "ok"])
    sk = len([r for r in results if r.get("status") == "skipped"])
    log(f"== 完成: {ok} ok, {sk} skipped, {len(empties)} empty, "
        f"{len(failures)} failed, 耗时 {time.time()-t0:.0f}s ==")
    for f in failures:
        log(f"  FAIL {f['symbol']}: {f['error']}")
    report = {
        "targets_total": len(targets),
        "ok": ok, "skipped": sk, "empty": len(empties), "failed": len(failures),
        "cumulative_rows": cum_rows,
        "elapsed_sec": int(time.time() - t0),
        "note": note,
        "results": results,
        "failures": failures,
        "empty_symbols": empties,
    }
    with open("_K_backfill_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    with open("_K_empty_list.json", "w", encoding="utf-8") as f:
        json.dump(empties, f, ensure_ascii=False, indent=2, default=str)
    log("报告 -> _K_backfill_report.json / _K_empty_list.json")


if __name__ == "__main__":
    main()
