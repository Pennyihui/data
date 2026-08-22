# -*- coding: utf-8 -*-
"""
ingest_tron.py — Tron USDT 转账流 L0 摄取 + 日频聚合 (三层底座)
=================================================================
- 源: Tronscan public API /api/token_trc20/transfers (USDT 主网合约)
  TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t, 按 timestamp(block_ts) 降序返回
- 已知深度限制 (2026-08-22 实测):
  * limit 上限 50/页; start 偏移上限 ~10000 (start=10000 起返回空)
  * total 字段恒为 10000 (封顶, 不代表窗口真实计数)
  * Tron USDT 转账流量极大 (≥10³/秒量级), 10000 条仅覆盖数分钟
  => 本脚本翻页至深度上限, 按实际到达的 UTC 日聚合, 并报告实际深度
- 输出:
  L0: raw/tronscan/token_transfers (batch tron_usdt_{YYYYMMDD}, json.gz)
  L1: l1/onchain_daily_aggregate/tron/data.parquet (合并去重后)
  L2: l2/certified/onchain_daily_aggregate/tron/all/data.parquet
      + dataset 级 manifest (跨 ethereum/arbitrum/tron 汇总统计)

用法:
  python ingest_tron.py fetch    # 翻页拉取 -> L0 raw (幂等, 按日批次去重)
  python ingest_tron.py build    # raw -> 聚合 -> L1/L2 + manifest (幂等重建)
  python ingest_tron.py all      # fetch + build
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from data_foundation import netpath
from data_foundation.config import CERTIFIED_DIR, L1_DIR, RAW_DIR
from data_foundation.l0 import list_raw_batches, write_raw_file
from data_foundation.l1_onchain import write_onchain_parquet
from data_foundation.l2 import (build_dataset_manifest, certify_derivatives,
                                write_certified_derivatives)

TRONSCAN_URL = "https://apilist.tronscanapi.com/api/token_trc20/transfers"
USDT_TOKEN = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_DECIMALS = 6
PAGE = 50                    # API limit 上限 50
MAX_DEPTH = 10050            # 探测上限 (start 最大 ~10000)
REQUEST_SLEEP = 0.3          # 请求间隔下限
LARGE_THRESHOLD = 1_000_000  # 大额转账阈值 (USDT, 与 ethereum/arbitrum 口径一致)
TARGET_DAYS = 30


def _utcnow() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def _read_raw(venue: str, dataset: str) -> list[dict]:
    """读取该 dataset 全部原始批次并合并为记录列表。"""
    out = []
    for m in list_raw_batches(venue, dataset):
        ingest = m["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if not f.startswith(m["batch_id"]) or f.endswith(".meta.json"):
                continue
            p = os.path.join(d, f)
            if p.endswith(".gz"):
                with gzip.open(p, "rt", encoding="utf-8") as fh:
                    recs = json.load(fh)
            else:
                with open(p, encoding="utf-8") as fh:
                    recs = json.load(fh)
            out.extend(recs)
    return out


def _all_batch_ids(venue: str, dataset: str) -> list[str]:
    return sorted({m["batch_id"] for m in list_raw_batches(venue, dataset)})


# ---------------------------------------------------------------------------
# 抓取
# ---------------------------------------------------------------------------
def _fetch_page(start: int, retries: int = 8) -> dict:
    """单页 token_trc20/transfers。限流/失败交给 netpath 重试; 空页返回 {}。"""
    try:
        return netpath.fetch_json(
            TRONSCAN_URL,
            params={"limit": PAGE, "start": start, "token": USDT_TOKEN},
            retries=retries, timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"  [tron] start={start} 请求失败: {str(e)[:120]}", flush=True)
        return {}


def fetch_all_transfers() -> tuple[list[dict], dict]:
    """翻页拉取至深度上限 (或连续空页), 返回 (records, stats)。"""
    records: list[dict] = []
    empty_streak = 0
    stats = {"pages": 0, "start_max": 0, "first_ts": None, "last_ts": None,
             "errors": 0}
    t0 = time.time()
    for start in range(0, MAX_DEPTH, PAGE):
        j = _fetch_page(start)
        stats["pages"] += 1
        stats["start_max"] = start
        tl = j.get("token_transfers") or []
        if not tl:
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue
        empty_streak = 0
        records.extend(tl)
        if stats["first_ts"] is None:
            stats["first_ts"] = tl[0].get("block_ts")
        stats["last_ts"] = tl[-1].get("block_ts")
        if len(records) % 1000 < PAGE:
            print(f"  [tron] 已拉 {len(records)} 条 (start={start})",
                  flush=True)
        time.sleep(REQUEST_SLEEP)
    stats["elapsed_s"] = round(time.time() - t0, 1)
    return records, stats


# ---------------------------------------------------------------------------
# 聚合
# ---------------------------------------------------------------------------
def _amount_usdt(rec: dict) -> float:
    try:
        return int(rec.get("quant") or 0) / (10 ** USDT_DECIMALS)
    except (TypeError, ValueError):
        return 0.0


def aggregate_daily(records: list[dict]) -> pd.DataFrame:
    """转账记录 -> onchain_daily_aggregate 行 (chain_id=tron, token=USDT)。

    清洗规则 (mock 源存在重复行与不可能金额, 规则写入 manifest):
      1. 按 transaction_id 去重 (保留首见) —— 实测 10000 条仅 2697 个唯一 tx;
      2. 排除金额 > 1e10 USDT 的记录 (总供应量 ~1.5e11, 单笔 >1e10 必为脏数据)。
    """
    seen: set[str] = set()
    rows = []
    for r in records:
        tid = r.get("transaction_id")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        ts = r.get("block_ts")
        if not ts:
            continue
        amt = _amount_usdt(r)
        if amt > 1e10:            # 不可能金额 (脏数据)
            continue
        rows.append({
            "date_utc": pd.to_datetime(int(ts), unit="ms", utc=True).normalize(),
            "from_address": r.get("from_address") or "",
            "to_address": r.get("to_address") or "",
            "amount_usdt": amt,
            "large": int(amt >= LARGE_THRESHOLD),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    g = df.groupby("date_utc")
    out = pd.DataFrame({
        "chain_id": "tron",
        "token": "USDT",
        "date_utc": g.size().index,
        "transfer_count": g.size().values,
        "unique_from": g["from_address"].nunique().values,
        "unique_to": g["to_address"].nunique().values,
        "volume_token": g["amount_usdt"].sum().values,
        "large_transfer_count": g["large"].sum().values,
        "mint_count": 0,
        "burn_count": 0,
    })
    out = out.sort_values("date_utc").reset_index(drop=True)
    for c in ("transfer_count", "unique_from", "unique_to",
              "large_transfer_count", "mint_count", "burn_count"):
        out[c] = out[c].astype("int64")
    out["volume_token"] = out["volume_token"].astype("float64")
    out["date_utc"] = out["date_utc"].astype("datetime64[us, UTC]")
    return out[["chain_id", "token", "date_utc", "transfer_count",
                "unique_from", "unique_to", "volume_token",
                "large_transfer_count", "mint_count", "burn_count"]]


# ---------------------------------------------------------------------------
# 管线
# ---------------------------------------------------------------------------
def phase_fetch() -> tuple[list[dict], dict]:
    """翻页拉取 -> 按 UTC 日写 raw 批次 (幂等: 已存在批次跳过)。"""
    records, stats = fetch_all_transfers()
    if not records:
        print("  [tron] 无记录, 跳过 raw 写入")
        return records, stats
    stats["records"] = len(records)
    stats["span_minutes"] = round(
        ((stats["first_ts"] or 0) - (stats["last_ts"] or 0)) / 60000.0, 2)
    # 按 UTC 日分组写批次
    by_day: dict[str, list[dict]] = {}
    for r in records:
        d = pd.to_datetime(int(r["block_ts"]), unit="ms", utc=True)
        by_day.setdefault(d.strftime("%Y%m%d"), []).append(r)
    written = []
    for day, recs in sorted(by_day.items()):
        bid = f"tron_usdt_{day}"
        if _already("tronscan", "token_transfers", bid):
            print(f"  [tron] 批次 {bid} 已存在, 跳过")
            continue
        tmp = os.path.join(RAW_DIR, "_tmp", f"{bid}.json.gz")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            json.dump(recs, f, ensure_ascii=False)
        dst = write_raw_file(
            tmp, "tronscan", "token_transfers", bid,
            source={"api": TRONSCAN_URL, "token": USDT_TOKEN,
                    "note": "token_trc20/transfers 翻页 (深度上限内)",
                    "fetched_at": _now_iso(), "records": len(recs)},
            timestamp_unit="ms", ext="json.gz")
        os.remove(tmp)
        written.append(dst)
        print(f"  [tron] {bid}: {len(recs)} 条 -> {dst}")
    return records, stats


def phase_build(verbose: bool = True) -> dict:
    """raw -> 日聚合 -> L1 (合并去重) -> L2 certified + dataset manifest。"""
    records = _read_raw("tronscan", "token_transfers")
    if not records:
        print("  [tron] raw 无记录, 先跑 fetch")
        return {}
    agg = aggregate_daily(records)
    if agg.empty:
        print("  [tron] 聚合为空")
        return {}
    # L1: 与已有 tron parquet 合并去重 (chain_id, token, date_utc)
    l1_path = os.path.join(L1_DIR, "onchain_daily_aggregate", "tron",
                           "data.parquet")
    merged = agg
    if os.path.exists(l1_path):
        old = pd.read_parquet(l1_path)
        merged = pd.concat([old, agg], ignore_index=True)
        merged = merged.drop_duplicates(["chain_id", "token", "date_utc"],
                                        keep="last")
        merged = merged.sort_values("date_utc").reset_index(drop=True)
    write_onchain_parquet(merged, "onchain_daily_aggregate", "tron",
                          "date_utc")
    print(f"  [tron] L1: {len(merged)} 行 "
          f"({merged['date_utc'].min().date()} ~ {merged['date_utc'].max().date()})")
    # L2: certified (只认证 tron 部分; 与 ethereum/arbitrum 文件互不干扰)
    cdf = certify_derivatives(agg, "date_utc",
                              core_numeric_cols=["volume_token"],
                              key_cols=["token", "date_utc"])
    write_certified_derivatives(cdf, "onchain_daily_aggregate", "tron",
                                "all", "date_utc")
    print(f"  [tron] L2 certified: {len(cdf)} 行")
    # manifest: dataset 级, 统计跨全部 venue 汇总 (ethereum+arbitrum+tron)
    stats_all = {"row_count": 0, "duplicate_count": 0, "gap_count": 0,
                 "suspect_count": 0, "coverage_start": None,
                 "coverage_end": None}
    for chain in ("ethereum", "arbitrum", "tron"):
        p = os.path.join(CERTIFIED_DIR, "onchain_daily_aggregate", chain,
                         "all", "data.parquet")
        if not os.path.exists(p):
            continue
        d = pd.read_parquet(p)
        if d.empty:
            continue
        stats_all["row_count"] += len(d)
        stats_all["suspect_count"] += int(d["is_suspect"].sum())
        cs = d["date_utc"].min()
        ce = d["date_utc"].max()
        if stats_all["coverage_start"] is None or cs < stats_all["coverage_start"]:
            stats_all["coverage_start"] = cs
        if stats_all["coverage_end"] is None or ce > stats_all["coverage_end"]:
            stats_all["coverage_end"] = ce
    stats_all["coverage_start"] = str(stats_all["coverage_start"])
    stats_all["coverage_end"] = str(stats_all["coverage_end"])
    # 保留旧 manifest 中非 finalize 字段的 source_batches 并合并 tron 批次
    old_manifest_path = os.path.join(CERTIFIED_DIR, "onchain_daily_aggregate",
                                     "manifest.json")
    old_batches: list[str] = []
    if os.path.exists(old_manifest_path):
        try:
            with open(old_manifest_path, encoding="utf-8") as f:
                old_batches = json.load(f).get("source_batches", [])
        except Exception:  # noqa: BLE001
            pass
    batches = list(dict.fromkeys(old_batches
                                 + _all_batch_ids("tronscan", "token_transfers")))
    manifest = build_dataset_manifest(
        "onchain_daily_aggregate", "*", "*", "*", "*", stats_all, batches,
        {"note": "ERC-20 (ethereum/arbitrum) + Tron TRC-20 (USDT) 日频聚合; "
                 "large>=1e6 代币面值; tron 来自 Tronscan token_trc20/transfers "
                 "深度上限内翻页 (约最近几分钟~当日); mint/burn 无标记置 0; "
                 "清洗: 按 transaction_id 去重 + 排除金额>1e10 USDT 的脏数据"})
    print(f"  [tron] manifest 更新: {len(batches)} 个 source_batches, "
          f"覆盖 {stats_all['coverage_start']} ~ {stats_all['coverage_end']}, "
          f"{stats_all['row_count']} 行 (全部 venue)")
    return stats_all


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode in ("fetch", "all"):
        phase_fetch()
    if mode in ("build", "all"):
        phase_build()


if __name__ == "__main__":
    main()
