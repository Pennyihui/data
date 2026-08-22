# -*- coding: utf-8 -*-
"""
onchain_history.py — 链上转账日志历史回填 + 日频聚合全历史重建 (Agent C)
========================================================================
1) L0 回填: 2026-08-01 → 2026-08-18 (以及 eth 08-19 / arb 08-19~08-20 的
   缺失窗口, 保证日频聚合覆盖 08-01..08-21 完整) 的 ERC-20 Transfer 日志。
   * 复用 ingest_onchain 的 RPC 列表 / token 地址 / _rpc 重试 / 自适应 span 逻辑
     (import CHAIN_ERC20, _rpc, TRANSFER, 不修改原文件)
   * 日志 JSON 以 gzip 压缩写入 raw, batch_id 沿用现有每日命名
     {TOKEN}_transfer_logs_daily_{YYYYMMDD}, 另写按日窗口边界时间戳
     window_blocks_daily_{YYYYMMDD}
   * 磁盘守卫: 写前检查 D:\\ 剩余空间, < 6GB 立即停止并报告实际回填到哪天
2) L1 重建: 解码新 raw (.json.gz) + 与现有 l1/token_transfer/{chain}/data.parquet
   合并 (concat + 按 [chain_id,token,tx_hash,log_index] 去重 + 排序 + 重写)。
   内存安全: 按 (链, 币) 分片处理, 字符串列用 pyarrow-backed dtype。
3) L2: token_transfer 全量 certify + 写 certified + manifest (汇总两链)。
4) 日频聚合: 从合并后全量 L1 计算 onchain_daily_aggregate (按
   [chain_id, token, date_utc] 分组), 阈值 large_transfer >= 100000 写入 manifest。
   + certify + 写 certified + manifest。

用法:
  python -m data_foundation.onchain_history                # 全部
  python -m data_foundation.onchain_history --skip-rebuild # 只回填 raw
  python -m data_foundation.onchain_history --skip-fetch   # 只重建 L1/L2/聚合
  python -m data_foundation.onchain_history --max-days 2   # 仅回填前 2 天 (冒烟测试)
"""
from __future__ import annotations

import argparse
import gc
import gzip
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import CERTIFIED_DIR, L1_DIR, RAW_DIR
from .ingest_onchain import CHAIN_ERC20, TRANSFER, _rpc
from .l0 import list_raw_batches, write_raw_file
from .l1_onchain import write_onchain_parquet
from .l2 import (build_dataset_manifest, certify_derivatives,
                 write_certified_derivatives)

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
BACKFILL_START = datetime(2026, 8, 1, tzinfo=timezone.utc)
BACKFILL_END = datetime(2026, 8, 18, tzinfo=timezone.utc)
# 扩展回填的缺失窗口: eth 08-19 00:00~22:42 (现有 _v1 从 08-19 22:42 起),
# arb 08-19 整天 + 08-20 00:00~13:36 (现有 _v1 从 08-20 13:36 起)。
# 这些属"缺失日期", 与已有窗口不重叠 (重叠部分会被 [chain_id,token,tx_hash,log_index] 去重吸收)。
EXTRA_DAYS = {
    "ethereum": [datetime(2026, 8, 19, tzinfo=timezone.utc)],
    "arbitrum": [datetime(2026, 8, 19, tzinfo=timezone.utc),
                 datetime(2026, 8, 20, tzinfo=timezone.utc)],
}
MIN_FREE_GB = 6.0
DISK = "D:\\"
LARGE_TRANSFER_THRESHOLD = 100_000      # 大额转账阈值 (写进 manifest)
FETCH_SLEEP = 0.25                      # 查询间隔 (任务要求 0.2-0.5s)
# Arbitrum 起始跨度放宽到 4000: 实测 ~2 条日志/块, 4000 块 ≈ 8k 条 < 1 万上限,
# 命中上限时自适应逻辑仍会逐级减半 (4000->2000->1000->...), 只是减少稀疏探测。
START_SPAN_OVERRIDE = {"arbitrum": 4000}
# Arbitrum RPC 顺序/超时覆盖: arb1.arbitrum.io 间歇性读超时 (实测 9s+/失败),
# arbitrum.drpc.org 当前稳定 (0.5s), 故 drpc 优先, 超时从 25s 降到 15s。
# 不修改 ingest_onchain.py, 仅在本模块回填时使用。
RPC_OVERRIDE = {
    "arbitrum": {"rpcs": ["https://arb1.arbitrum.io/rpc",
                          "https://arbitrum.drpc.org"],
                 # arb1 官方是唯一可靠 getLogs 源 (drpc 免费计划对大批量查询
                 # 直接报 usage limit/free plan timeout)。超时 20s、重试 2 轮,
                 # 让"超限->减半"快速生效, 避免长时间空等。
                 "timeout": 20, "retries": 2},
}
# 重试次数覆盖: 长时间回填会偶发 mevblocker 限流 (-32005)/drpc 临时错误,
# 重跑补齐失败批次时用更多重试更稳。
RETRIES_OVERRIDE = {"ethereum": 4}
BATCH_RE = re.compile(r"^(USDT|USDC|DAI)_transfer_logs_daily_(\d{8})$")
STR_COLS = ("token", "tx_hash", "from_address", "to_address", "value_raw", "chain_id")

# 汇总文件 (回填结果持久化, 供重建/验证读取)
SUMMARY_PATH = os.path.join(RAW_DIR, "_tmp", "onchain_history_summary.json")
PARTS_DIR = os.path.join(RAW_DIR, "_tmp", "parts_token_transfer")

_TS_CACHE: dict[int, int] = {}


def _cfg(chain: str) -> dict:
    """链配置: ingest_onchain.CHAIN_ERC20 + 本模块的 RPC/跨度/重试覆盖。"""
    cfg = dict(CHAIN_ERC20[chain])
    cfg.update(RPC_OVERRIDE.get(chain, {}))
    cfg["retries"] = RETRIES_OVERRIDE.get(chain, cfg.get("retries", 2))
    return cfg


def _backfill_day_set() -> set[str]:
    """本模块回填的目标日期集合 (08-01..08-18 + 各链扩展缺失窗口)。"""
    s = set()
    d = BACKFILL_START
    while d <= BACKFILL_END:
        s.add(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    for days in EXTRA_DAYS.values():
        for dd in days:
            s.add(dd.strftime("%Y%m%d"))
    return s


# ---------------------------------------------------------------------------
# RPC 辅助 (复用 ingest_onchain._rpc)
# ---------------------------------------------------------------------------
def _block_ts(cfg: dict, block: int) -> int:
    if block in _TS_CACHE:
        return _TS_CACHE[block]
    ts = int(_rpc("eth_getBlockByNumber", [hex(block), False],
                  rpcs=cfg["rpcs"], timeout=cfg["timeout"],
                  retries=cfg["retries"])["timestamp"], 16)
    _TS_CACHE[block] = ts
    return ts


def _last_block_at_ts(cfg: dict, ts: int, latest: int, lts: int, bps: float) -> int:
    """返回时间戳 <= ts 的最后一个区块 (线性估计 + 本地精修)。"""
    if ts >= lts:
        return latest
    b = max(0, min(latest, int(latest - (lts - ts) * bps)))
    for _ in range(80):
        t = _block_ts(cfg, b)
        if t <= ts:
            if b >= latest:
                return b
            tn = _block_ts(cfg, b + 1)
            if tn > ts:
                return b
            b = min(latest, b + max(1, int((ts - tn) * bps)))
        else:
            if b == 0:
                return 0
            tp = _block_ts(cfg, b - 1)
            if tp <= ts:
                return b - 1
            b = max(0, b - max(1, int((t - ts) * bps)))
    return b


def _first_block_at_ts(cfg: dict, ts: int, latest: int, lts: int, bps: float) -> int:
    """返回时间戳 >= ts 的第一个区块。"""
    b = _last_block_at_ts(cfg, ts, latest, lts, bps)
    if b >= latest:
        return b
    if _block_ts(cfg, b) == ts:
        return b
    return min(b + 1, latest)


def _chain_bounds(chain: str, days: list) -> tuple[dict, int]:
    cfg = _cfg(chain)
    latest = int(_rpc("eth_blockNumber", [], rpcs=cfg["rpcs"],
                      timeout=cfg["timeout"], retries=cfg["retries"]), 16)
    lts = _block_ts(cfg, latest)
    bps = cfg["blocks_per_day"] / 86400.0
    bounds = {}
    for day in days:
        ts0 = int(day.timestamp())
        b0 = _first_block_at_ts(cfg, ts0, latest, lts, bps)
        b1 = _first_block_at_ts(cfg, ts0 + 86400, latest, lts, bps)
        bounds[day] = (b0, b1)
    return bounds, latest


def _existing_min_block(venue: str) -> int | None:
    """该链"既有"(回填前已存在) 日志批次的最早起始块。

    用于把首个重叠日的窗口截断到既有窗口之前, 绝不重复抓已有窗口。
    注意: 必须排除本模块自己回填的批次 (BATCH_RE), 否则会把回填批次当成
    既有数据导致整链被误判为"已覆盖"。
    """
    mn = None
    for m in list_raw_batches(venue, "erc20_transfer_logs"):
        bid = m["batch_id"]
        if BATCH_RE.match(bid):
            continue   # 本模块回填的批次不算"既有"
        if not bid.startswith(("USDT_", "USDC_", "DAI_")) or "transfer_logs" not in bid:
            continue
        bl = (m.get("source") or {}).get("blocks") or []
        if bl and (mn is None or int(bl[0]) < mn):
            mn = int(bl[0])
    return mn


# ---------------------------------------------------------------------------
# L0 回填
# ---------------------------------------------------------------------------
def _days_for(chain: str) -> list:
    days = []
    d = BACKFILL_START
    while d <= BACKFILL_END:
        days.append(d)
        d += timedelta(days=1)
    days += EXTRA_DAYS.get(chain, [])
    return sorted(set(days))


def _disk_free_gb() -> float:
    return shutil.disk_usage(DISK).free / (1024 ** 3)


def _exists(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id for m in list_raw_batches(venue, dataset))


def _fetch_token_logs(cfg: dict, addr: str, frm: int, to: int) -> tuple[list, int]:
    """自适应块区间分页抓取日志 (start_span -> 命中上限/超时逐级减半到 10)。

    - "too many logs"/"exceeds max results"/"more than 10000"/"limit of 10000"
      -> 窗口减半重试 (RPC 上限)
    - 超时 (timed out/408) -> 窗口减半重试 (上游慢, 减小查询负载)
    - 减到最小 span 仍失败 -> 抛错 (批次记为失败, 可重跑补齐), 绝不静默跳块
    """
    all_logs = []
    span_hint = START_SPAN_OVERRIDE.get(cfg["venue"], cfg["start_span"])
    b, n_queries = frm, 0
    while b <= to:
        span = span_hint
        logs, t = None, min(b + span - 1, to)
        exhausted = False
        while span >= 10:
            t = min(b + span - 1, to)
            try:
                logs = _rpc("eth_getLogs",
                            [{"address": addr, "topics": [TRANSFER],
                              "fromBlock": hex(b), "toBlock": hex(t)}],
                            rpcs=cfg["rpcs"], timeout=cfg["timeout"],
                            retries=cfg["retries"])
                break
            except RuntimeError as e:
                msg = str(e)
                if any(k in msg for k in ("too many logs", "too many results",
                                          "exceeds max results", "exceeds",
                                          "logs matched", "usage limit",
                                          "more than 10000", "limit of 10000",
                                          "timed out", "Timeout", "408")):
                    span //= 2
                    continue
                raise
        if logs is None:
            exhausted = True
        n_queries += 1
        if logs:
            all_logs.extend(logs)
        if exhausted:
            raise RuntimeError(
                f"eth_getLogs span 减至 <10 仍失败 (block {b}..{t}), 批次未写, 可重跑补齐")
        # 跨度自适应: 本窗口稀疏则放宽下一窗口, 密集则保持
        if len(logs) < 5000:
            span_hint = min(span * 2, START_SPAN_OVERRIDE.get(cfg["venue"],
                                                              cfg["start_span"]))
        else:
            span_hint = span
        b = t + 1
        time.sleep(FETCH_SLEEP)
    return all_logs, n_queries


def _write_gz_logs(chain: str, day: datetime, tok: str, logs: list,
                   blocks: tuple) -> str:
    cfg = _cfg(chain)
    batch_id = f"{tok}_transfer_logs_daily_{day:%Y%m%d}"
    tmp = os.path.join(RAW_DIR, "_tmp", f"hist_{chain}_{tok}_{day:%Y%m%d}.json.gz")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(logs, f)
    src = {"api": "eth_getLogs", "token": tok, "chain": chain,
           "address": cfg["tokens"][tok]["address"],
           "blocks": list(blocks),
           "date_range_utc": [day.strftime("%Y-%m-%dT%H:%M:%SZ"),
                              (day + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")],
           "log_count": len(logs),
           "fetched_at": datetime.now(timezone.utc).isoformat(),
           "note": "历史回填 (gzip 压缩)"}
    return write_raw_file(tmp, cfg["venue"], "erc20_transfer_logs", batch_id, src,
                          timestamp_unit="ms", ext="json.gz")


def _write_window(chain: str, day: datetime, b0: int, b1: int) -> None:
    cfg = _cfg(chain)
    bid = f"window_blocks_daily_{day:%Y%m%d}"
    if _exists(cfg["venue"], "erc20_transfer_logs", bid):
        return
    content = {"chain": chain, "start_block": b0, "end_block": b1,
               "start_timestamp": _block_ts(cfg, b0),
               "end_timestamp": _block_ts(cfg, b1),
               "note": "历史回填按日窗口边界时间戳"}
    tmp = os.path.join(RAW_DIR, "_tmp", f"hist_{chain}_window_{day:%Y%m%d}.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(content, f)
    write_raw_file(tmp, cfg["venue"], "erc20_transfer_logs", bid,
                   {"api": "eth_getBlockByNumber", "chain": chain,
                    "note": "历史回填窗口边界时间戳"}, ext="json")


def backfill(max_days: int | None = None) -> tuple[list, datetime | None, float]:
    """逐日逐链逐币回填 raw。返回 (summary, 停止日, 停止时剩余GB)。"""
    summary = []
    stopped_at, stopped_free = None, None
    for chain in ("ethereum", "arbitrum"):
        cfg = _cfg(chain)
        venue = cfg["venue"]
        days = _days_for(chain)
        if max_days:
            days = days[:max_days]
        bounds, latest = _chain_bounds(chain, days)
        mn = _existing_min_block(venue)
        print(f"[backfill] {chain}: {len(days)} 天, latest={latest}, "
              f"已有最早块={mn}", flush=True)
        for day in days:
            b0, b1 = bounds[day]
            fetch_to = b1 - 1
            if mn is not None and fetch_to >= mn:
                fetch_to = mn - 1          # 截断到已有窗口之前, 不重复抓
            if fetch_to < b0:
                print(f"  {chain} {day:%Y-%m-%d}: 窗口已覆盖, 跳过", flush=True)
                continue
            free = _disk_free_gb()
            if free < MIN_FREE_GB:
                stopped_at, stopped_free = day, free
                print(f"[STOP] D: 剩余 {free:.1f}GB < {MIN_FREE_GB}GB "
                      f"(在 {chain} {day:%Y-%m-%d} 前), 停止回填", flush=True)
                _save_summary(summary, stopped_at, stopped_free)
                return summary, stopped_at, stopped_free
            _write_window(chain, day, b0, fetch_to)
            for tok, meta in cfg["tokens"].items():
                batch_id = f"{tok}_transfer_logs_daily_{day:%Y%m%d}"
                if _exists(venue, "erc20_transfer_logs", batch_id):
                    print(f"  [skip] {batch_id} 已存在", flush=True)
                    continue
                free = _disk_free_gb()
                if free < MIN_FREE_GB:
                    stopped_at, stopped_free = day, free
                    print(f"[STOP] D: 剩余 {free:.1f}GB < {MIN_FREE_GB}GB "
                          f"(在 {chain} {tok} {day:%Y-%m-%d} 前), 停止回填", flush=True)
                    _save_summary(summary, stopped_at, stopped_free)
                    return summary, stopped_at, stopped_free
                try:
                    logs, nq = _fetch_token_logs(cfg, meta["address"], b0, fetch_to)
                except RuntimeError as e:
                    print(f"  [warn] {chain} {tok} {day:%Y-%m-%d} 抓取失败: "
                          f"{str(e)[:120]} (60s 冷却后继续, 该批次可重跑补齐)",
                          flush=True)
                    time.sleep(60)   # RPC 上游限流冷却, 提高后续批次成功率
                    summary.append({"chain": chain, "token": tok,
                                    "day": str(day.date()), "logs": 0,
                                    "queries": 0, "status": "failed",
                                    "error": str(e)[:120]})
                    continue
                dst = _write_gz_logs(chain, day, tok, logs, (b0, fetch_to))
                summary.append({"chain": chain, "token": tok,
                                "day": str(day.date()), "logs": len(logs),
                                "queries": nq, "status": "ok",
                                "blocks": [b0, fetch_to],
                                "file": os.path.basename(dst)})
                _save_summary(summary, None, None)   # 增量保存, 防中断丢失
                print(f"  {chain} {tok} {day:%Y-%m-%d}: {len(logs)} 条日志 "
                      f"/ {nq} 查询 -> {os.path.basename(dst)}", flush=True)
    _save_summary(summary, stopped_at, stopped_free)
    return summary, stopped_at, stopped_free


def _save_summary(summary: list, stopped_at, stopped_free) -> None:
    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump({"summary": summary,
                   "stopped_at": str(stopped_at) if stopped_at else None,
                   "stopped_free_gb": stopped_free,
                   "generated_at": datetime.now(timezone.utc).isoformat()},
                  f, ensure_ascii=False, indent=2, default=str)


def _load_summary() -> list:
    if not os.path.exists(SUMMARY_PATH):
        return []
    with open(SUMMARY_PATH, encoding="utf-8") as f:
        return json.load(f).get("summary", [])


# ---------------------------------------------------------------------------
# L1 解码 (独立实现, 不修改 l1_onchain.py)
# ---------------------------------------------------------------------------
def _read_logs_file(path: str) -> list:
    if path.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _list_backfill_batches(venue: str) -> list[dict]:
    day_set = _backfill_day_set()
    out = []
    for m in list_raw_batches(venue, "erc20_transfer_logs"):
        mm = BATCH_RE.match(m["batch_id"])
        if not mm:
            continue
        if mm.group(2) not in day_set:
            continue   # 只解码本模块回填的日期, 跳过既有 08-21 等批次 (已并入现有 parquet)
        day = datetime.strptime(mm.group(2), "%Y%m%d").replace(tzinfo=timezone.utc)
        ingest = m["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, "erc20_transfer_logs", f"ingest_date={ingest}")
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.startswith(m["batch_id"]) and f.endswith((".json", ".json.gz")) \
                    and not f.endswith(".meta.json"):
                out.append({"token": mm.group(1), "day": day,
                            "path": os.path.join(d, f), "venue": venue})
                break
    return out


def _window_for(venue: str, day: datetime) -> dict | None:
    bid = f"window_blocks_daily_{day:%Y%m%d}"
    for m in list_raw_batches(venue, "erc20_transfer_logs"):
        if m["batch_id"] != bid:
            continue
        ingest = m["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, "erc20_transfer_logs", f"ingest_date={ingest}")
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.startswith(bid) and f.endswith(".json") and not f.endswith(".meta.json"):
                with open(os.path.join(d, f), encoding="utf-8") as fh:
                    w = json.load(fh)
                return {k: int(str(v), 0) for k, v in w.items()
                        if k in ("start_block", "end_block",
                                 "start_timestamp", "end_timestamp")}
    return None


def _decode_rows(logs: list, tok: str, dec: int, chain_id: str) -> pd.DataFrame:
    n = len(logs)
    tokens = [tok] * n
    blocks = [0] * n
    hashes = [""] * n
    logidx = [0] * n
    fr = [""] * n
    to = [""] * n
    raw = [""] * n
    val = [0.0] * n
    mint = [False] * n
    burn = [False] * n
    zero_topic = "0x" + "0" * 64
    for i, l in enumerate(logs):
        blocks[i] = int(l["blockNumber"], 16)
        hashes[i] = l["transactionHash"]
        logidx[i] = int(l["logIndex"], 16)
        fr[i] = "0x" + l["topics"][1][-40:].lower()
        to[i] = "0x" + l["topics"][2][-40:].lower()
        raw[i] = l["data"]
        val[i] = int(l["data"][:66], 16) / (10 ** dec)
        mint[i] = l["topics"][1] == zero_topic
        burn[i] = l["topics"][2] == zero_topic
    d = pd.DataFrame({"token": tokens, "block_number": blocks, "tx_hash": hashes,
                      "log_index": logidx, "from_address": fr, "to_address": to,
                      "value_raw": raw, "value_decimal": val, "is_mint": mint,
                      "is_burn": burn})
    d["chain_id"] = chain_id
    return d


def decode_backfill_chain(chain: str, token: str) -> pd.DataFrame:
    """解码该链该币全部回填批次 (含时间戳插值)。"""
    cfg = _cfg(chain)
    venue = cfg["venue"]
    dec = cfg["tokens"][token]["decimals"]
    frames = []
    for b in _list_backfill_batches(venue):
        if b["token"] != token:
            continue
        w = _window_for(venue, b["day"])
        logs = _read_logs_file(b["path"])
        if not logs:
            continue
        d = _decode_rows(logs, token, dec, chain)
        if w:
            slope = (w["end_timestamp"] - w["start_timestamp"]) / max(
                w["end_block"] - w["start_block"], 1)
            ts = pd.to_datetime(
                w["start_timestamp"] + (d["block_number"] - w["start_block"]) * slope,
                unit="s", utc=True)
            d["block_timestamp_utc"] = ts   # 默认索引对齐, 按位置匹配
        else:
            d["block_timestamp_utc"] = pd.NaT
        # 逐日先转 pyarrow string, 避免大对象串帧峰值内存
        frames.append(_coerce_arrow_strings(d))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _coerce_arrow_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in STR_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string[pyarrow]")
    return df


def _aggregate_token(df: pd.DataFrame) -> pd.DataFrame:
    """单币 token_transfer -> 日频聚合 (large_transfer 阈值 100000)。"""
    if df.empty:
        return pd.DataFrame()
    d = df.copy(deep=False)          # 浅拷贝, 只新增 date_utc 一列, 控制峰值内存
    d["date_utc"] = d["block_timestamp_utc"].dt.normalize()
    g = d.groupby(["chain_id", "token", "date_utc"])
    out = g.agg(transfer_count=("tx_hash", "count"),
                unique_from=("from_address", "nunique"),
                unique_to=("to_address", "nunique"),
                volume_token=("value_decimal", "sum"),
                mint_count=("is_mint", "sum"),
                burn_count=("is_burn", "sum")).reset_index()
    large = d[d["value_decimal"] >= LARGE_TRANSFER_THRESHOLD]
    if large.empty:
        out["large_transfer_count"] = 0
    else:
        lc = large.groupby(["chain_id", "token", "date_utc"]).size().rename(
            "large_transfer_count").reset_index()
        out = out.merge(lc, on=["chain_id", "token", "date_utc"], how="left")
        out["large_transfer_count"] = out["large_transfer_count"].fillna(0)
    out["large_transfer_count"] = out["large_transfer_count"].astype("int64")
    return out[["chain_id", "token", "date_utc", "transfer_count", "unique_from",
                "unique_to", "volume_token", "mint_count", "burn_count",
                "large_transfer_count"]]


# ---------------------------------------------------------------------------
# L1/L2 重建
# ---------------------------------------------------------------------------
def rebuild_l1() -> tuple[dict, dict]:
    """解码回填 + 合并现有 parquet (按链币分片) -> parts parquet。"""
    os.makedirs(PARTS_DIR, exist_ok=True)
    totals = {}
    agg_frames = {}
    for chain in ("ethereum", "arbitrum"):
        existing_path = os.path.join(L1_DIR, "token_transfer", chain, "data.parquet")
        existing = pd.read_parquet(existing_path) \
            if os.path.exists(existing_path) else pd.DataFrame()
        if not existing.empty:
            existing = existing.drop(columns=["date"], errors="ignore")
        cfg = _cfg(chain)
        dups, rows = 0, 0
        aggs = []
        for tok in cfg["tokens"]:
            new_tok = decode_backfill_chain(chain, tok)
            ex_tok = existing[existing["token"] == tok] if not existing.empty \
                else pd.DataFrame()
            merged = pd.concat([ex_tok, new_tok], ignore_index=True) \
                if len(ex_tok) else new_tok
            merged = _coerce_arrow_strings(merged)
            n_before = len(merged)
            merged = merged.drop_duplicates(
                ["chain_id", "token", "tx_hash", "log_index"], keep="first")
            dups += n_before - len(merged)
            merged = merged.sort_values(["block_number", "log_index"]) \
                .reset_index(drop=True)
            part = os.path.join(PARTS_DIR, f"{chain}_{tok}.parquet")
            merged.to_parquet(part, compression="snappy")
            rows += len(merged)
            ag = _aggregate_token(merged)
            if not ag.empty:
                aggs.append(ag)
            print(f"  L1[{chain}/{tok}]: {len(merged)} 行 "
                  f"(回填 {len(new_tok)}, 去重 {n_before - len(merged)})", flush=True)
            del merged, new_tok, ex_tok
            gc.collect()
        totals[chain] = {"rows": rows, "dups": dups}
        agg_frames[chain] = pd.concat(aggs, ignore_index=True) if aggs \
            else pd.DataFrame()
        del existing
    return totals, agg_frames


def _merge_parts_write(parts: list, dst: str, time_col: str) -> int:
    """pyarrow 合并 parts -> 最终 parquet (内存友好), 时间戳统一 us, 追加 date 列。"""
    tables = [pq.read_table(p) for p in parts if os.path.exists(p)]
    if not tables:
        return 0
    table = pa.concat_tables(tables)
    idx = table.schema.get_field_index(time_col)
    ts = pc.cast(table[time_col], pa.timestamp("us", tz="UTC"))
    table = table.set_column(idx, time_col, ts)
    date_col = pc.strftime(pc.cast(ts, pa.timestamp("us")), "%Y-%m-%d")
    table = table.append_column("date", date_col)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    pq.write_table(table, dst, compression="snappy")
    return table.num_rows


def assemble_l1() -> None:
    for chain in ("ethereum", "arbitrum"):
        parts = [os.path.join(PARTS_DIR, f"{chain}_{t}.parquet")
                 for t in _cfg(chain)["tokens"]]
        dst = os.path.join(L1_DIR, "token_transfer", chain, "data.parquet")
        n = _merge_parts_write(parts, dst, "block_timestamp_utc")
        print(f"  L1 写盘 {chain}: {n} 行 -> {dst}", flush=True)


def rebuild_aggregate(agg_frames: dict) -> dict:
    stats_all = {"row_count": 0, "duplicate_count": 0, "gap_count": 0,
                 "suspect_count": 0, "coverage_start": None, "coverage_end": None}
    for chain, agg in agg_frames.items():
        if agg.empty:
            print(f"  [warn] {chain} 无聚合数据", flush=True)
            continue
        agg = agg.sort_values(["chain_id", "token", "date_utc"]).reset_index(drop=True)
        write_onchain_parquet(agg, "onchain_daily_aggregate", chain, "date_utc")
        cdf = certify_derivatives(agg, "date_utc",
                                  key_cols=["chain_id", "token", "date_utc"],
                                  core_numeric_cols=["transfer_count", "unique_from",
                                                     "unique_to", "volume_token",
                                                     "large_transfer_count",
                                                     "mint_count", "burn_count"])
        write_certified_derivatives(cdf, "onchain_daily_aggregate", chain, "all",
                                    "date_utc")
        dups = int(agg[["chain_id", "token", "date_utc"]].duplicated().sum())
        s = {"row_count": int(len(cdf)),
             "duplicate_count": dups,
             "gap_count": 0,
             "suspect_count": int(cdf["is_suspect"].sum()),
             "coverage_start": str(cdf["date_utc"].min()),
             "coverage_end": str(cdf["date_utc"].max())}
        for k in stats_all:
            if k == "row_count" or k == "duplicate_count" or k == "suspect_count":
                stats_all[k] += s[k]
            elif k == "coverage_start":
                stats_all[k] = s[k] if stats_all[k] is None \
                    else min(stats_all[k], s[k])
            elif k == "coverage_end":
                stats_all[k] = s[k] if stats_all[k] is None \
                    else max(stats_all[k], s[k])
        print(f"  aggregate[{chain}]: {len(cdf)} 行 "
              f"({s['coverage_start']} ~ {s['coverage_end']}, "
              f"suspect={s['suspect_count']})", flush=True)
        print(cdf[["chain_id", "token", "date_utc", "transfer_count",
                   "large_transfer_count"]].to_string(index=False), flush=True)
    return stats_all


def rebuild_l2_token_transfer() -> dict:
    cert_parts = os.path.join(PARTS_DIR, "cert")
    os.makedirs(cert_parts, exist_ok=True)
    stats_all = {"row_count": 0, "duplicate_count": 0, "gap_count": 0,
                 "suspect_count": 0, "coverage_start": None, "coverage_end": None}
    for chain in ("ethereum", "arbitrum"):
        chain_suspect = 0
        for tok in _cfg(chain)["tokens"]:
            part = os.path.join(PARTS_DIR, f"{chain}_{tok}.parquet")
            if not os.path.exists(part):
                continue
            df = pd.read_parquet(part)
            cdf = certify_derivatives(df, "block_timestamp_utc",
                                      core_numeric_cols=["block_number",
                                                         "value_decimal"],
                                      key_cols=["chain_id", "token", "tx_hash",
                                                "log_index"])
            chain_suspect += int(cdf["is_suspect"].sum())
            cdf.to_parquet(os.path.join(cert_parts, f"{chain}_{tok}.parquet"),
                           compression="snappy")
            del df, cdf
            gc.collect()
        parts = [os.path.join(cert_parts, f"{chain}_{t}.parquet")
                 for t in _cfg(chain)["tokens"]]
        dst = os.path.join(CERTIFIED_DIR, "token_transfer", chain, "all",
                           "data.parquet")
        n = _merge_parts_write(parts, dst, "block_timestamp_utc")
        print(f"  certified token_transfer[{chain}]: {n} 行, "
              f"suspect={chain_suspect} -> {dst}", flush=True)
    return stats_all


def _source_batches() -> list:
    ids = set()
    for venue in ("ethereum", "arbitrum"):
        for m in list_raw_batches(venue, "erc20_transfer_logs"):
            ids.add(f"{venue}:{m['batch_id']}")
    return sorted(ids)


def build_manifests(stats_tt: dict, stats_agg: dict) -> None:
    src = _source_batches()
    tt_rules = {"note": "从 raw eth_getLogs 解码 (历史回填 + 每日窗口); "
                        "主键 [chain_id,token,tx_hash,log_index]; "
                        "时间戳=按日窗口边界块线性插值(±10min)"}
    build_dataset_manifest("token_transfer", "*", "*", "*", "*", stats_tt,
                           src, tt_rules)
    agg_rules = {"group_by": ["chain_id", "token", "date_utc"],
                 "metrics": ["transfer_count", "unique_from", "unique_to",
                             "volume_token", "large_transfer_count",
                             "mint_count", "burn_count"],
                 "large_transfer_threshold": LARGE_TRANSFER_THRESHOLD,
                 "note": "按 UTC 日聚合 block_timestamp_utc; "
                         "large_transfer = value_decimal >= 100000"}
    build_dataset_manifest("onchain_daily_aggregate", "*", "*", "*", "*",
                           stats_agg, src, agg_rules)
    print("  manifest 已写: token_transfer / onchain_daily_aggregate", flush=True)


# ---------------------------------------------------------------------------
# 验证
# ---------------------------------------------------------------------------
def verify(totals: dict, agg_frames: dict, summary: list) -> None:
    print("\n================ 收尾验证 ================", flush=True)
    ok = summary and all(s.get("status") == "ok" for s in summary)
    ok_rows = [s for s in summary if s.get("status") == "ok"]
    failed = [s for s in summary if s.get("status") != "ok"]
    if ok_rows:
        days = sorted({s["day"] for s in ok_rows})
        print(f"回填批次: {len(ok_rows)} 个 (链×币×日), 覆盖 {days[0]} ~ {days[-1]}")
        print(f"回填实际到达的最早日期: {days[0]}")
        print(f"回填失败批次: {len(failed)}"
              + (f" -> {[(f['chain'], f['token'], f['day']) for f in failed]}" if failed else ""))
        # 每链每币总量
        for chain in ("ethereum", "arbitrum"):
            for tok in _cfg(chain)["tokens"]:
                n = sum(s["logs"] for s in ok_rows
                        if s["chain"] == chain and s["token"] == tok)
                print(f"  回填日志数 {chain}/{tok}: {n}")
    print("\nL1 token_transfer 合并后:")
    for chain in ("ethereum", "arbitrum"):
        df = pd.read_parquet(os.path.join(L1_DIR, "token_transfer", chain,
                                          "data.parquet"))
        print(f"  {chain}: {len(df)} 行, "
              f"{df['block_timestamp_utc'].min()} ~ {df['block_timestamp_utc'].max()}, "
              f"去重去除 {totals[chain]['dups']}")
        del df
    print("\nonchain_daily_aggregate:")
    for chain in ("ethereum", "arbitrum"):
        agg = pd.read_parquet(os.path.join(L1_DIR, "onchain_daily_aggregate",
                                           chain, "data.parquet"))
        print(f"  {chain}: {len(agg)} 行, "
              f"{agg['date_utc'].min().date()} ~ {agg['date_utc'].max().date()}, "
              f"币×日组合 {agg[['token', 'date_utc']].drop_duplicates().shape[0]}")
        del agg
    print("\nL2 certified:")
    for chain in ("ethereum", "arbitrum"):
        df = pd.read_parquet(os.path.join(CERTIFIED_DIR, "token_transfer", chain,
                                          "all", "data.parquet"))
        print(f"  token_transfer[{chain}]: {len(df)} 行, "
              f"suspect={int(df['is_suspect'].sum())}")
        del df
        agg = pd.read_parquet(os.path.join(CERTIFIED_DIR,
                                           "onchain_daily_aggregate", chain,
                                           "all", "data.parquet"))
        print(f"  onchain_daily_aggregate[{chain}]: {len(agg)} 行, "
              f"suspect={int(agg['is_suspect'].sum())}")
        del agg
    print("\n交叉验证 (2026-08-19, 用 SQL 式 groupby 核对 aggregate 与 token_transfer):")
    for chain in ("ethereum", "arbitrum"):
        day = pd.Timestamp("2026-08-19", tz="UTC")
        agg = pd.read_parquet(os.path.join(L1_DIR, "onchain_daily_aggregate",
                                           chain, "data.parquet"))
        a = agg[agg["date_utc"] == day]
        sub_rows = []
        for tok in _cfg(chain)["tokens"]:
            part = os.path.join(PARTS_DIR, f"{chain}_{tok}.parquet")
            if not os.path.exists(part):
                continue
            df = pd.read_parquet(part)
            d = df[df["block_timestamp_utc"].dt.normalize() == day]
            if d.empty:
                continue
            g = d.groupby(["chain_id", "token"]).agg(
                transfer_count=("tx_hash", "count"),
                unique_from=("from_address", "nunique"),
                unique_to=("to_address", "nunique"),
                volume_token=("value_decimal", "sum"),
                large_transfer_count=("value_decimal",
                                      lambda x: int((x >= LARGE_TRANSFER_THRESHOLD).sum())),
                mint_count=("is_mint", "sum"),
                burn_count=("is_burn", "sum")).reset_index()
            sub_rows.append(g)
            del d, df
        if sub_rows:
            g = pd.concat(sub_rows, ignore_index=True)
            a2 = a[["chain_id", "token", "transfer_count", "unique_from",
                    "unique_to", "volume_token", "large_transfer_count",
                    "mint_count", "burn_count"]].merge(
                g, on=["chain_id", "token"], suffixes=("_agg", "_sql"))
            ok_all = True
            for c in ("transfer_count", "unique_from", "unique_to", "volume_token",
                      "large_transfer_count", "mint_count", "burn_count"):
                diff = (a2[f"{c}_agg"] - a2[f"{c}_sql"]).abs().sum()
                status = "OK" if diff == 0 else f"MISMATCH diff={diff}"
                if diff != 0:
                    ok_all = False
                print(f"    {chain} {c}: agg={int(a2[f'{c}_agg'].sum())} "
                      f"sql={int(a2[f'{c}_sql'].sum())} -> {status}")
            print(f"    {chain} 08-19 交叉验证: {'全部一致' if ok_all else '存在差异!'}")
        else:
            print(f"    {chain} 08-19: 无数据")
        del agg, a


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="链上日志历史回填 + 全历史重建")
    ap.add_argument("--skip-fetch", action="store_true", help="跳过 L0 回填")
    ap.add_argument("--skip-rebuild", action="store_true", help="跳过 L1/L2/聚合重建")
    ap.add_argument("--max-days", type=int, default=None,
                    help="只回填每链前 N 天 (冒烟测试)")
    args = ap.parse_args()

    t0 = time.time()
    if not args.skip_fetch:
        print("== 阶段 1: L0 历史回填 ==", flush=True)
        summary, stopped, free = backfill(max_days=args.max_days)
        if stopped:
            print(f"[STOP] 磁盘不足, 回填停止于 {stopped} (剩余 {free:.1f}GB), "
                  f"已写批次见 {SUMMARY_PATH}", flush=True)
        else:
            print(f"回填完成, 共 {len(summary)} 个批次 (含失败), 摘要 -> "
                  f"{SUMMARY_PATH}", flush=True)
    else:
        summary = _load_summary()

    if args.skip_rebuild:
        print(f"阶段 1 耗时 {time.time() - t0:.0f}s")
        return 0

    print("\n== 阶段 2: L1 重建 (解码回填 + 合并现有) ==", flush=True)
    totals, agg_frames = rebuild_l1()
    assemble_l1()

    print("\n== 阶段 3: L2 token_transfer 认证 ==", flush=True)
    rebuild_l2_token_transfer()

    print("\n== 阶段 4: 日频聚合重建 ==", flush=True)
    stats_agg = rebuild_aggregate(agg_frames)

    print("\n== 阶段 5: manifest ==", flush=True)
    stats_tt = {"row_count": sum(t["rows"] for t in totals.values()),
                "duplicate_count": sum(t["dups"] for t in totals.values()),
                "gap_count": 0, "suspect_count": 0,
                "coverage_start": None, "coverage_end": None}
    for chain in ("ethereum", "arbitrum"):
        df = pd.read_parquet(os.path.join(L1_DIR, "token_transfer", chain,
                                          "data.parquet"))
        if stats_tt["coverage_start"] is None:
            stats_tt["coverage_start"] = str(df["block_timestamp_utc"].min())
        else:
            stats_tt["coverage_start"] = min(
                stats_tt["coverage_start"], str(df["block_timestamp_utc"].min()))
        stats_tt["coverage_end"] = str(df["block_timestamp_utc"].max()) \
            if stats_tt["coverage_end"] is None else max(
                stats_tt["coverage_end"], str(df["block_timestamp_utc"].max()))
        cdf = pd.read_parquet(os.path.join(CERTIFIED_DIR, "token_transfer",
                                           chain, "all", "data.parquet"))
        stats_tt["suspect_count"] += int(cdf["is_suspect"].sum())
        del df, cdf
    build_manifests(stats_tt, stats_agg)

    print("\n== 阶段 6: 验证 ==", flush=True)
    verify(totals, agg_frames, summary)

    print(f"\n总耗时 {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
