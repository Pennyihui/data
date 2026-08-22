# -*- coding: utf-8 -*-
"""
_vision_metrics_backfill.py — Binance Vision futures metrics 归档回填 (子代理 E)
================================================================================
把 OI 与全部多空比/主动买卖比从"最近 21 天"扩展到 2020-09 起的官方历史。

流程:
  1. 枚举: S3 列表收集全部 {SYM}-metrics-{YYYY-MM-DD}.zip Key (跳过 CHECKSUM)
  2. 下载并按月合并 -> L0 raw/binance/futures_metrics/{SYM}_metrics_{YYYYMM}.csv.gz
     (已存在批次跳过, 幂等断点续跑; 单日失败不拖垮整月)
  3. L1/L2 重建: 5min -> 1h (每小时取末行, 时间戳取整点), 与现有 21 天 API 行合并
     (现有行优先/先到先得), 写五份数据集 + certify + manifest

用法:
  python _vision_metrics_backfill.py --stage all            # 下载 + 重建
  python _vision_metrics_backfill.py --stage l0             # 仅下载
  python _vision_metrics_backfill.py --stage rebuild        # 仅 L1/L2 重建
  python _vision_metrics_backfill.py --stage verify         # 仅验证打印
  --symbols BTC,ETH   --max-files 100 (测试用, 按整月对齐)
磁盘守卫: D: 剩余 < 5GB 即停 (保存状态, 可续跑)。不调用 finalize_all, 不 git。
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import re
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import pandas as pd
import pyarrow.parquet as pq

from data_foundation import netpath
from data_foundation.config import L1_DIR, MVP_ASSETS, RAW_DIR
from data_foundation.derivatives import write_derivatives_parquet
from data_foundation.l0 import list_raw_batches, write_raw_file
from data_foundation.l1 import instrument_id
from data_foundation.l2 import (build_dataset_manifest, certify_derivatives,
                                write_certified_derivatives)

S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
VISION = "https://data.binance.vision"
DATASET = "futures_metrics"
STATE_PATH = os.path.join(_HERE, "_vision_state.json")
FILELISTS_PATH = os.path.join(_HERE, "_vision_filelists.json")
TMP_DIR = os.path.join(RAW_DIR, "_tmp")
DISK_GUARD_GB = 5.0
DOWNLOAD_SLEEP = 0.1          # 下载间 sleep (netpath 还有全局 0.12s 节流)
PROGRESS_EVERY = 500          # 每 500 个文件打印一次进度

DATASET_SPECS = [
    ("derivatives_open_interest", ["timestamp_utc"],
     ["open_interest_contracts", "open_interest_notional"]),
    ("derivatives_ratio_glsr", ["timestamp_utc", "metric"], ["long_short_ratio"]),
    ("derivatives_ratio_tlsr_acct", ["timestamp_utc", "metric"], ["long_short_ratio"]),
    ("derivatives_ratio_tlsr_pos", ["timestamp_utc", "metric"], ["long_short_ratio"]),
    ("derivatives_ratio_taker", ["timestamp_utc", "metric"], ["long_short_ratio"]),
]
RATIO_METRIC = {
    "derivatives_ratio_glsr": "glsr",
    "derivatives_ratio_tlsr_acct": "tlsr_acct",
    "derivatives_ratio_tlsr_pos": "tlsr_pos",
    "derivatives_ratio_taker": "taker",
}
RATIO_SRC_COL = {
    "derivatives_ratio_glsr": "count_long_short_ratio",
    "derivatives_ratio_tlsr_acct": "count_toptrader_long_short_ratio",
    "derivatives_ratio_tlsr_pos": "sum_toptrader_long_short_ratio",
    "derivatives_ratio_taker": "sum_taker_long_short_vol_ratio",
}
ALL_SYMS = [f"{a}USDT" for a in MVP_ASSETS]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"completed_symbols": [], "accum": {}, "downloaded_files": 0,
            "missing_days": [], "last_update": None}


def save_state(state: dict):
    state["last_update"] = now_iso()
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)


def disk_free_gb() -> float:
    return shutil.disk_usage(RAW_DIR).free / (1024 ** 3)


def check_disk() -> bool:
    free = disk_free_gb()
    if free < DISK_GUARD_GB:
        print(f"[DISK-GUARD] D: 剩余 {free:.2f}GB < {DISK_GUARD_GB}GB, 停止",
              flush=True)
        return False
    return True


# ---------------------------------------------------------------------------
# 1) 枚举文件清单
# ---------------------------------------------------------------------------
def list_s3_keys(prefix: str) -> list[str]:
    keys, marker = [], ""
    while True:
        url = f"{S3}?prefix={prefix}&max-keys=1000" + \
              (f"&marker={marker}" if marker else "")
        r = netpath.request("GET", url, timeout=30, retries=6)
        xml = r.text
        page = re.findall(r"<Key>([^<]+)</Key>", xml)
        keys.extend(page)
        if "<IsTruncated>true</IsTruncated>" not in xml:
            break
        m = re.search(r"<NextMarker>([^<]+)</NextMarker>", xml)
        marker = m.group(1) if m else page[-1]
    return keys


def enumerate_all(force: bool = False) -> dict[str, list[str]]:
    if not force and os.path.exists(FILELISTS_PATH):
        with open(FILELISTS_PATH, encoding="utf-8") as f:
            return json.load(f)
    lists = {}
    for sym in ALL_SYMS:
        prefix = f"data/futures/um/daily/metrics/{sym}/"
        keys = list_s3_keys(prefix)
        zips = sorted(k for k in keys if k.endswith(".zip")
                      and not k.endswith(".CHECKSUM"))
        lists[sym] = zips
        dates = [re.search(r"metrics-(\d{4}-\d{2}-\d{2})\.zip$", k).group(1)
                 for k in zips]
        print(f"[enum] {sym}: {len(zips)} files, {dates[0]} -> {dates[-1]}",
              flush=True)
    with open(FILELISTS_PATH, "w", encoding="utf-8") as f:
        json.dump(lists, f)
    return lists


# ---------------------------------------------------------------------------
# 2) 下载并按月合并 -> L0
# ---------------------------------------------------------------------------
def _existing_batch_ids() -> set[str]:
    return {m["batch_id"] for m in list_raw_batches("binance", DATASET)}


_DIRECT_SESSION = None


def _get_direct(url: str, timeout: int = 60):
    """直连 data.binance.vision (vision 域名直连可用, ~0.15s vs 代理 ~1.1s)。
    成功返回 Response; 网络失败返回 None; 4xx 返回 Response 由上层判断。"""
    global _DIRECT_SESSION
    if _DIRECT_SESSION is None:
        import requests as _rq
        _DIRECT_SESSION = _rq.Session()
        _DIRECT_SESSION.trust_env = False
    try:
        return _DIRECT_SESSION.get(
            url, timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (data-foundation)"})
    except Exception:           # noqa: BLE001
        return None


def _download_zip(url: str) -> bytes:
    r = _get_direct(url)
    if r is not None:
        if r.status_code == 200:
            return r.content
        if r.status_code == 404:
            raise FileNotFoundError(url)
    # 直连失败/异常 -> netpath 四级兜底
    resp = netpath.request("GET", url, timeout=60, retries=3)
    return resp.content


def download_month(sym: str, days: list[tuple[str, str]]) -> tuple[str, list[str]]:
    """逐日下载 zip -> 解 CSV -> 纵向拼接。单日失败记录 missing, 不拖垮整月。

    days: [(day 'YYYY-MM-DD', url), ...]。返回 (csv_text, missing_days)。
    """
    header = None
    bodies = []
    missing = []
    for day, url in days:
        ok = False
        for attempt in range(2):          # 立即重试一次
            try:
                content = _download_zip(url)
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    text = z.read(z.namelist()[0]).decode("utf-8",
                                                          errors="replace")
                ok = True
                break
            except Exception as e:        # noqa: BLE001
                last_err = f"{str(e)[:70]}"
                time.sleep(1.0)
        if not ok:
            missing.append(f"{day}:{last_err}")
            continue
        lines = text.splitlines()
        if not lines:
            missing.append(f"{day}:empty")
            continue
        hdr = lines[0].strip()
        if header is None:
            header = hdr
        elif hdr != header:
            missing.append(f"{day}:header_mismatch")
            continue
        bodies.append("\n".join(l for l in lines[1:] if l.strip()))
        time.sleep(DOWNLOAD_SLEEP)
    if header is None:
        raise RuntimeError(f"{sym}: 月内无任何有效 CSV (missing={len(missing)})")
    return header + "\n" + "\n".join(bodies) + "\n", missing


def stage_l0(filelists: dict, state: dict, symbols: list[str],
             max_files: int | None = None):
    os.makedirs(TMP_DIR, exist_ok=True)
    existing = _existing_batch_ids()
    dl_count = state.get("downloaded_files", 0)
    missing_days = list(state.get("missing_days", []))

    for sym in symbols:
        months: dict[str, list[tuple[str, str]]] = {}
        for k in filelists.get(sym, []):
            m = re.search(r"metrics-(\d{4}-\d{2}-\d{2})\.zip$", k)
            if not m:
                continue
            day = m.group(1)
            yyyymm = day[:4] + day[5:7]        # 2020-09-01 -> 202009
            months.setdefault(yyyymm, []).append(
                (day, f"{VISION}/{k}"))
        for yyyymm in sorted(months):
            days = sorted(months[yyyymm])
            batch_id = f"{sym}_metrics_{yyyymm}"
            if batch_id in existing:
                continue
            if max_files is not None and dl_count + len(days) > max_files:
                print(f"  [limit] dl_count={dl_count} + {len(days)} 超过 "
                      f"--max-files {max_files}, 停止", flush=True)
                state["downloaded_files"] = dl_count
                state["missing_days"] = missing_days
                save_state(state)
                return
            if not check_disk():
                state["downloaded_files"] = dl_count
                state["missing_days"] = missing_days
                save_state(state)
                print(f"[STOP] 磁盘守卫触发 (symbol={sym}, month={yyyymm})",
                      flush=True)
                return
            print(f"  [l0] {sym} {yyyymm}: {len(days)} 天, 下载中...", flush=True)
            try:
                csv_text, miss = download_month(sym, days)
            except Exception as e:        # noqa: BLE001
                print(f"  [warn] {sym} {yyyymm} 整月失败: {str(e)[:80]}",
                      flush=True)
                missing_days.extend(f"{sym}_{yyyymm}_all:{str(e)[:50]}")
                continue
            if miss:
                print(f"    [warn] {sym} {yyyymm}: {len(miss)} 天缺失: "
                      f"{miss[:5]}", flush=True)
                missing_days.extend(f"{sym}_{yyyymm}:{m}" for m in miss)
            tmp_path = os.path.join(TMP_DIR, f"{batch_id}.csv.gz")
            with open(tmp_path, "wb") as f:
                f.write(gzip.compress(csv_text.encode("utf-8")))
            source = {
                "symbol": sym,
                "url_base": f"{VISION}/data/futures/um/daily/metrics/{sym}/",
                "file_count": len(days) - len(miss),
                "date_range": [days[0][0], days[-1][0]],
                "missing_days": miss,
                "granularity": "5min",
                "fetched_at": now_iso(),
            }
            write_raw_file(tmp_path, "binance", DATASET, batch_id,
                           source=source, ext="csv.gz")
            os.remove(tmp_path)
            existing.add(batch_id)
            dl_count += len(days)
            if dl_count % PROGRESS_EVERY == 0:
                print(f"  [progress] 累计处理 {dl_count} 个文件", flush=True)
        print(f"  [l0] {sym} 完成, 累计处理 {dl_count} 个文件", flush=True)

    state["downloaded_files"] = dl_count
    state["missing_days"] = missing_days
    save_state(state)
    print(f"[l0] 下载阶段完成: 处理 {dl_count} 个文件, "
          f"缺失日 {len(missing_days)} 条", flush=True)


# ---------------------------------------------------------------------------
# 3) L1/L2 重建
# ---------------------------------------------------------------------------
def symbol_batch_paths(sym: str) -> list[str]:
    out = []
    for meta in list_raw_batches("binance", DATASET):
        if meta.get("source", {}).get("symbol") != sym:
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, "binance", DATASET, f"ingest_date={ingest}")
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append(os.path.join(d, f))
    return sorted(out)


def load_symbol_raw(sym: str) -> pd.DataFrame:
    frames = []
    for p in symbol_batch_paths(sym):
        frames.append(pd.read_csv(p, compression="gzip"))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def downsample_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """create_time -> UTC, 按小时分组取每组最后一行, 时间戳取整点。"""
    df = df.copy()
    df["ts"] = pd.to_datetime(df["create_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    if df.empty:
        return df
    df["hour"] = df["ts"].dt.floor("h")
    df = df.sort_values("ts")
    last = df.groupby("hour", as_index=False).tail(1)
    return last.sort_values("hour").reset_index(drop=True)


def build_oi(df: pd.DataFrame, sym: str, inst: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "venue_id": "binance",
        "instrument_id": inst,
        "symbol": sym,
        "timestamp_utc": pd.to_datetime(df["hour"], utc=True),
        "open_interest_contracts": pd.to_numeric(df["sum_open_interest"],
                                                 errors="coerce"),
        "open_interest_notional": pd.to_numeric(df["sum_open_interest_value"],
                                                errors="coerce"),
        "data_available_at": pd.to_datetime(df["ts"], utc=True),
        "source_batch_id": "binance_vision_oi_v1",
    })
    return out.dropna(subset=["open_interest_contracts",
                              "open_interest_notional"],
                      how="all").reset_index(drop=True)


def build_ratio(df: pd.DataFrame, sym: str, inst: str, dataset: str) -> pd.DataFrame:
    metric = RATIO_METRIC[dataset]
    src_col = RATIO_SRC_COL[dataset]
    out = pd.DataFrame({
        "venue_id": "binance",
        "instrument_id": inst,
        "symbol": sym,
        "timestamp_utc": pd.to_datetime(df["hour"], utc=True),
        "metric": metric,
        "long_account": float("nan"),
        "long_short_ratio": pd.to_numeric(df[src_col], errors="coerce"),
        "short_account": float("nan"),
        "data_available_at": pd.to_datetime(df["ts"], utc=True),
        "source_batch_id": f"binance_vision_{metric}_v1",
    })
    return out.dropna(subset=["long_short_ratio"]).reset_index(drop=True)


def merge_with_existing(new_df: pd.DataFrame, dataset: str, inst: str,
                        keys: list[str]) -> tuple[pd.DataFrame, int]:
    """与现有 L1 合并: 现有行优先 (先到先得), 新历史行按 key 去重。

    返回 (merged, dropped): dropped = 因与现有行时间戳冲突被剔除的行数
    (即新旧衔接处的重叠冲突量)。
    """
    path = os.path.join(L1_DIR, dataset, "binance", inst, "data.parquet")
    if not os.path.exists(path):
        return new_df.sort_values("timestamp_utc").reset_index(drop=True), 0
    old = pq.read_table(path).to_pandas()
    old = old.drop(columns=[c for c in ["date", "is_suspect",
                                        "quality_reason"] if c in old.columns])
    merged = pd.concat([old, new_df], ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset=keys, keep="first")
    dropped = before - len(merged)
    print(f"      merge {dataset}/{inst}: old={len(old)} new={len(new_df)} "
          f"-> {len(merged)} (去重 {dropped})", flush=True)
    return merged.sort_values("timestamp_utc").reset_index(drop=True), dropped


def acc_stats(accum: dict, dataset: str, stats: dict):
    s = accum.setdefault(dataset, {"row_count": 0, "duplicate_count": 0,
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


def rebuild_symbol(sym: str, state: dict):
    inst = instrument_id(sym)
    df = load_symbol_raw(sym)
    if df.empty:
        print(f"  [warn] {sym}: 无 L0 futures_metrics 批次, 跳过", flush=True)
        return
    h = downsample_hourly(df)
    print(f"  [rebuild] {sym}: raw 行 {len(df)} -> 1h {len(h)} 行 "
          f"({h['hour'].min()} ~ {h['hour'].max()})", flush=True)

    overlap_report = {}
    for dataset, keys, core in DATASET_SPECS:
        if dataset == "derivatives_open_interest":
            new_df = build_oi(h, sym, inst)
        else:
            new_df = build_ratio(h, sym, inst, dataset)
        time_col = "timestamp_utc"
        merged, dropped = merge_with_existing(new_df, dataset, inst, keys)
        overlap_report[dataset] = dropped
        if merged.empty:
            continue
        write_derivatives_parquet(merged, dataset, "binance", inst, time_col)
        cert = certify_derivatives(merged, time_col,
                                   core_numeric_cols=core, key_cols=keys)
        write_certified_derivatives(cert, dataset, "binance", inst, time_col)
        stats = {
            "row_count": int(len(cert)),
            "duplicate_count": int(cert[keys].duplicated().sum()),
            "gap_count": 0,
            "suspect_count": int(cert["is_suspect"].sum()),
            "coverage_start": str(cert[time_col].min()),
            "coverage_end": str(cert[time_col].max()),
        }
        acc_stats(state["accum"], dataset, stats)
        print(f"    {dataset}: {stats['row_count']} 行 suspect="
              f"{stats['suspect_count']} ({stats['coverage_start']} ~ "
              f"{stats['coverage_end']})", flush=True)
    state["overlap"] = state.get("overlap", {})
    state["overlap"][sym] = overlap_report
    state["completed_symbols"] = sorted(set(state.get("completed_symbols", []))
                                        | {sym})
    save_state(state)


def write_manifests(state: dict):
    accum = state["accum"]
    if not accum:
        return
    per_sym: dict[str, list[str]] = {}
    for meta in list_raw_batches("binance", DATASET):
        parts = meta["batch_id"].split("_")
        if len(parts) == 3 and parts[1] == "metrics":
            per_sym.setdefault(parts[0], []).append(parts[2])
    src_batches = []
    for sym in sorted(per_sym):
        months = sorted(per_sym[sym])
        src_batches.append(
            f"{sym}_metrics_{months[0]}..{months[-1]} ({len(months)} months)")
    for ds in [d for d, _, _ in DATASET_SPECS]:
        s = accum.get(ds)
        if not s:
            continue
        rules = {
            "source": "Binance Vision 官方 futures metrics 归档 "
                      "(data.binance.vision/data/futures/um/daily/metrics)",
            "resample": "源=Vision官方metrics 5分钟粒度, 重采样1h取末值 "
                        "(每小时组取最后一行, timestamp 取整点)",
            "dedup": "与现有 21 天 API 行合并: 现有行优先 (先到先得), "
                     "新历史行按主键去重",
            "merge_overlap_dropped": state.get("overlap", {}),
            "naming": "open_interest_usd 按 schema 落地为 open_interest_notional",
            "ratio_detail": "glsr=count_long_short_ratio, "
                            "tlsr_acct=count_toptrader_long_short_ratio, "
                            "tlsr_pos=sum_toptrader_long_short_ratio, "
                            "taker=sum_taker_long_short_vol_ratio; "
                            "long_account/short_account Vision 无原生字段=NaN; "
                            "ratio 无值的小时行剔除",
            "depth": f"深度回填 {s['coverage_start']} ~ {s['coverage_end']} "
                     "(2020-09 起, 各 symbol 自上市月)",
            "version": "1.0",
        }
        build_dataset_manifest(ds, "binance", "perpetual", "*", "*", s,
                               src_batches, rules)
        print(f"  [manifest] {ds}: {s['row_count']} 行, coverage "
              f"{s['coverage_start']} ~ {s['coverage_end']}", flush=True)


# ---------------------------------------------------------------------------
# 验证
# ---------------------------------------------------------------------------
def stage_verify(state: dict):
    print("\n==== 验证: 每 symbol 覆盖 (L1 OI) ====", flush=True)
    for sym in ALL_SYMS:
        inst = instrument_id(sym)
        p = os.path.join(L1_DIR, "derivatives_open_interest", "binance",
                         inst, "data.parquet")
        if not os.path.exists(p):
            print(f"  {sym}: L1 OI 缺失", flush=True)
            continue
        df = pq.read_table(p).to_pandas()
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        src = df["source_batch_id"].value_counts().to_dict()
        print(f"  {sym}: OI {len(df)} 行 "
              f"{df['timestamp_utc'].min()} ~ {df['timestamp_utc'].max()} "
              f"src={src}", flush=True)
    print("\n==== 验证: 每数据集汇总 ====", flush=True)
    for ds, _, _ in DATASET_SPECS:
        total = 0
        start = end = None
        for sym in ALL_SYMS:
            inst = instrument_id(sym)
            p = os.path.join(L1_DIR, ds, "binance", inst, "data.parquet")
            if not os.path.exists(p):
                continue
            df = pq.read_table(p).to_pandas()
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
            total += len(df)
            s0, s1 = df["timestamp_utc"].min(), df["timestamp_utc"].max()
            start = s0 if start is None or s0 < start else start
            end = s1 if end is None or s1 > end else end
        print(f"  {ds}: total {total} 行, {start} ~ {end}", flush=True)
    ov = state.get("overlap", {})
    print("\n==== 重叠区去重量 (Vision 历史 vs 旧 21 天 API 行) ====", flush=True)
    for sym, rep in sorted(ov.items()):
        print(f"  {sym}: {rep}", flush=True)
    md = state.get("missing_days", [])
    print(f"\n==== 缺失日 ({len(md)}) ====", flush=True)
    for m in md[:20]:
        print(f"  {m}", flush=True)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="all",
                    choices=["all", "l0", "rebuild", "verify"])
    ap.add_argument("--symbols", default=",".join(MVP_ASSETS))
    ap.add_argument("--force-enumerate", action="store_true")
    ap.add_argument("--max-files", type=int, default=None,
                    help="L0 测试上限 (整月对齐)")
    args = ap.parse_args()
    symbols = []
    for s in args.symbols.split(","):
        s = s.strip().upper()
        if not s:
            continue
        if not s.endswith("USDT"):
            s += "USDT"
        if s in ALL_SYMS:
            symbols.append(s)
    state = load_state()

    if args.stage in ("all", "l0"):
        filelists = enumerate_all(args.force_enumerate)
        stage_l0(filelists, state, symbols, max_files=args.max_files)
    if args.stage in ("all", "rebuild"):
        state = load_state()  # 刷新 (l0 可能刚写完)
        todo = [s for s in symbols if s not in state.get("completed_symbols", [])]
        for sym in todo:
            if not check_disk():
                print("[STOP] 磁盘守卫触发 (rebuild)", flush=True)
                break
            rebuild_symbol(sym, state)
        write_manifests(state)
    if args.stage == "verify":
        stage_verify(state)
    if args.stage == "all":
        print("\n[all] 完成。运行 --stage verify 可复查。", flush=True)


if __name__ == "__main__":
    main()
