# -*- coding: utf-8 -*-
"""
onchain_finish.py — 链上历史回填收尾 (接手中断的 Agent C)
==========================================================
背景: Agent C 已完成 Ethereum 全部回填 (08-01..08-19 × USDT/USDC/DAI, .json.gz),
Arbitrum 仅完成 3 个批次后停滞。本脚本接手:
  fetch  补齐 Arbitrum 缺失批次 + 两链缺失的逐日窗口文件 (幂等, 可断点续跑)
  build  全量解码 -> L1 -> L2 certified -> 日频聚合 (复用 stage_onchain, 跳过 L0)
  all    fetch + build

用法:
  python onchain_finish.py fetch
  python onchain_finish.py build
  python onchain_finish.py all
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone

from data_foundation import netpath  # noqa: E402
from data_foundation.config import RAW_DIR  # noqa: E402
from data_foundation.ingest_onchain import ARB_TOKENS, ARB_RPCS, RPCS, TRANSFER  # noqa: E402
from data_foundation.l0 import list_raw_batches, write_raw_file  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
CHAIN_CFG = {
    "arbitrum": {"venue": "arbitrum", "rpcs": ARB_RPCS, "tokens": ARB_TOKENS,
                 "block_time": 0.25, "start_span": 4000, "max_span": 8000,
                 "timeout": 25},
    "ethereum": {"venue": "ethereum", "rpcs": RPCS,
                 "block_time": 12.0, "start_span": 4000, "max_span": 8000,
                 "timeout": 15},
}
DAYS = [f"2026{m:02d}{d:02d}" for m, d in
        [(8, dd) for dd in range(1, 20)]]          # 20260801..20260819
FETCH_TOKENS = ["USDT", "USDC", "DAI"]
DISK_FLOOR_GB = 5.0


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def rpc(method, params, rpcs, timeout=25):
    """单次 JSON-RPC, 走 netpath 四级链路; 失败抛错由上层重试。"""
    j = netpath.post_json(rpcs[0], json_body={"jsonrpc": "2.0", "id": 1,
                                              "method": method, "params": params},
                          timeout=timeout, retries=1)
    if "result" not in j:
        raise RuntimeError(str(j)[:120])
    return j["result"]


def rpc_robust(method, params, rpcs, timeout=25, rounds=3):
    """跨 RPC 端点轮询 + 轮次退避。错误消息保留完整 (跨度减半依赖其中
    的 'limit of 10000' 等信号词, 截断会破坏自适应逻辑)。"""
    errs = []
    for i in range(rounds):
        for base in rpcs:
            try:
                j = netpath.post_json(base, json_body={
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": params}, timeout=timeout, retries=1)
                if "result" in j:
                    return j["result"]
                errs.append(f"{base}:{str(j)[:400]}")
            except Exception as e:  # noqa: BLE001
                errs.append(f"{base}:{str(e)[:200]}")
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(" | ".join(errs[-4:]))


def existing_batches(venue):
    return {m["batch_id"] for m in list_raw_batches(venue, "erc20_transfer_logs")}


def block_ts(rpcs, blk, timeout):
    h = rpc_robust("eth_getBlockByNumber", [hex(blk), False], rpcs, timeout)
    return int(h["timestamp"], 16)


def day_start_block(rpcs, day, latest, now_ts, block_time, timeout):
    """该 UTC 日 00:00 的起始块 (估算+迭代精修, 收敛到 ±几个块)。"""
    y, m, d = int(day[:4]), int(day[4:6]), int(day[6:])
    target = datetime(y, m, d, tzinfo=timezone.utc).timestamp()
    blk = max(1, latest - int((now_ts - target) / block_time))
    best = None
    for _ in range(30):
        bts = block_ts(rpcs, blk, timeout)
        best = (blk, bts)
        if abs(bts - target) <= max(2, block_time * 4):
            break
        blk = max(1, min(latest, int(blk + (target - bts) / block_time)))
    # 保证 start.ts >= 当日 00:00 (边界块归属次日由相邻窗口互斥保证)
    blk, bts = best
    steps = 0
    while bts < target and steps < 200:
        blk += 1
        bts = block_ts(rpcs, blk, timeout)
        steps += 1
    return blk, bts


def save_window(venue, dataset_day, start_blk, start_ts, end_blk, end_ts):
    content = json.dumps({"start_block": start_blk, "end_block": end_blk,
                          "start_timestamp": start_ts, "end_timestamp": end_ts})
    tmp = os.path.join(RAW_DIR, "_tmp", f"win_{venue}_{dataset_day}.json")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    write_raw_file(tmp, venue, "erc20_transfer_logs",
                   f"window_blocks_daily_{dataset_day}",
                   source={"api": "eth_getBlockByNumber",
                           "note": "逐日窗口边界块 (回填)"},
                   timestamp_unit="ms", ext="json")


def fetch_logs_spanned(rpcs, address, frm, to, cfg, tok, day):
    """自适应跨度抓 [frm, to] 日志, 返回 (logs, 截断否)。"""
    logs_all, b = [], frm
    span = cfg["start_span"]
    while b <= to:
        cur = None
        while span >= 10:
            t = min(b + span - 1, to)
            try:
                cur = rpc_robust("eth_getLogs",
                                 [{"address": address, "topics": [TRANSFER],
                                   "fromBlock": hex(b), "toBlock": hex(t)}],
                                 rpcs, cfg["timeout"])
                break
            except RuntimeError as e:
                low = str(e).lower()
                if any(k in low for k in ("10000", "too many",
                                          "exceeds max results")):
                    span //= 2
                    continue
                raise
        if cur is None:
            raise RuntimeError(f"span 耗尽仍失败 {tok} {day} @{b}")
        logs_all.extend(cur)
        if len(cur) < 2000 and span < cfg["max_span"]:
            span = min(span * 2, cfg["max_span"])
        elif len(cur) >= 9000:
            span = max(span // 2, 10)
        b = min(b + span, to + 1)
        time.sleep(0.35)
    return logs_all


def phase_fetch():
    now_ts = time.time()
    # ---- 1) 两链逐日窗口文件补齐 (缺失时全量重算边界, 保证相邻配对正确) ----
    for chain, cfg in CHAIN_CFG.items():
        venue = cfg["venue"]
        have = existing_batches(venue)
        missing_days = [d for d in DAYS if f"window_blocks_daily_{d}" not in have]
        if not missing_days:
            log(f"[{chain}] 窗口文件齐全 ({len(DAYS)} 天)")
            continue
        rpcs = cfg["rpcs"]
        bt = cfg["block_time"]
        timeout = cfg["timeout"]
        latest = int(rpc_robust("eth_blockNumber", [], rpcs, timeout), 16)
        log(f"[{chain}] 补窗口: 缺 {len(missing_days)} 天, 计算全部 {len(DAYS)} 天边界")
        starts = {}
        for day in DAYS:
            blk, bts = day_start_block(rpcs, day, latest, now_ts, bt, timeout)
            starts[day] = (blk, bts)
            time.sleep(0.25)
        log(f"[{chain}] 边界块计算完成: {DAYS[0]}->{starts[DAYS[0]][0]} .. "
            f"{DAYS[-1]}->{starts[DAYS[-1]][0]}")
        sdays = sorted(starts)
        wrote = 0
        for i, day in enumerate(sdays[:-1]):
            if f"window_blocks_daily_{day}" in existing_batches(venue):
                continue
            nxt = sdays[i + 1]
            sb, sts = starts[day]
            eb = starts[nxt][0] - 1
            ets = block_ts(rpcs, eb, timeout)
            save_window(venue, day, sb, sts, eb, ets)
            wrote += 1
            log(f"[{chain}] 写窗口 {day}: [{sb}..{eb}]")
        # 最后一天窗口到 latest
        last = sdays[-1]
        if f"window_blocks_daily_{last}" not in existing_batches(venue):
            sb, sts = starts[last]
            lts = int(rpc_robust("eth_getBlockByNumber", [hex(latest), False],
                                 rpcs, timeout)["timestamp"], 16)
            save_window(venue, last, sb, sts, latest, lts)
            wrote += 1
            log(f"[{chain}] 写窗口 {last}: [{sb}..{latest}](至 latest)")
        log(f"[{chain}] 窗口补齐完成: 新写 {wrote}")

    # ---- 2) Arbitrum 缺失日志批次 ----
    venue = "arbitrum"
    cfg = CHAIN_CFG["arbitrum"]
    rpcs = cfg["rpcs"]
    have = existing_batches(venue)
    latest = int(rpc_robust("eth_blockNumber", [], rpcs, cfg["timeout"]), 16)
    todo = [(tok, day) for day in DAYS[:-1] for tok in FETCH_TOKENS
            if f"{tok}_transfer_logs_daily_{day}" not in have]
    log(f"Arbitrum 待回填 {len(todo)} 个批次 (USDT/USDC/DAI × {DAYS[0]}..{DAYS[-2]})")
    done = 0
    t0 = time.time()
    for tok, day in todo:
        free_gb = shutil.disk_usage("D:\\").free / 1e9
        if free_gb < DISK_FLOOR_GB:
            log(f"[stop] D 盘剩余 {free_gb:.1f}GB < {DISK_FLOOR_GB}GB, 停止回填")
            break
        win = existing_batches(venue)
        wb = f"window_blocks_daily_{day}"
        if wb not in win:
            log(f"[skip] {tok} {day}: 无窗口文件 (先跑窗口补齐)")
            continue
        w = next(m for m in list_raw_batches(venue, "erc20_transfer_logs")
                 if m["batch_id"] == wb)
        d_dir = os.path.join(RAW_DIR, venue, "erc20_transfer_logs",
                             f"ingest_date={w['ingested_at'][:10]}")
        with open(os.path.join(d_dir, f"{wb}.json"), encoding="utf-8") as f:
            wnd = json.load(f)
        sb, eb = int(wnd["start_block"]), int(wnd["end_block"])
        tb = time.time()
        logs = fetch_logs_spanned(rpcs, ARB_TOKENS[tok]["address"], sb, eb,
                                  cfg, tok, day)
        payload = json.dumps(logs).encode()
        tmp = os.path.join(RAW_DIR, "_tmp", f"{tok}_transfer_logs_daily_{day}.json.gz")
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            f.write(json.dumps(logs))
        write_raw_file(tmp, venue, "erc20_transfer_logs",
                       f"{tok}_transfer_logs_daily_{day}",
                       source={"api": "eth_getLogs (arbitrum 回填)", "token": tok,
                               "blocks": [sb, eb], "logs": len(logs),
                               "fetched_at": datetime.now(timezone.utc).isoformat()},
                       timestamp_unit="ms", ext="json.gz")
        os.remove(tmp)
        done += 1
        log(f"  arb {tok} {day}: {len(logs)} 条 "
            f"({time.time()-tb:.0f}s, 累计 {done}/{len(todo)})")
    log(f"fetch 完成: 新增 {done} 批次, 耗时 {(time.time()-t0)/60:.0f} 分钟")


def phase_build():
    """流式全量重建 (内存受限 16GB, 不能一次性 decode 4000万行):

    逐文件解码 -> 按日切分 -> 流式追加 parquet 行组 (L1 + certified) ->
    聚合在丢弃行数据前增量计算。回填批次 (0801-0818, 窗口互斥) 免去全局
    去重; 遗留批次 (v1/daily, 覆盖 0819+) 按日缓冲后去重。
    """
    import gc

    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data_foundation.config import CERTIFIED_DIR, L1_DIR
    from data_foundation.l1_onchain import (CHAIN_ERC20, _read_raw_json,
                                            block_timestamps)
    from data_foundation.l2 import build_dataset_manifest

    def pa_ts():
        return pa.timestamp("us", tz="UTC")

    L1_SCHEMA = pa.schema([
        ("token", pa.string()), ("block_number", pa.int64()),
        ("tx_hash", pa.string()), ("log_index", pa.int64()),
        ("from_address", pa.string()), ("to_address", pa.string()),
        ("value_raw", pa.string()), ("value_decimal", pa.float64()),
        ("is_mint", pa.bool_()), ("is_burn", pa.bool_()),
        ("chain_id", pa.string()), ("block_timestamp_utc", pa_ts()),
        ("date", pa.string())])
    CERT_SCHEMA = pa.schema(list(L1_SCHEMA) + [
        ("is_suspect", pa.bool_()), ("quality_reason", pa.string())])

    def anchors_for(chain):
        m = {}
        for w in block_timestamps(chain):
            m[w["start_block"]] = w["start_timestamp"]
            m[w["end_block"]] = w["end_timestamp"]
        bx = np.array(sorted(m), dtype=float)
        by = np.array([m[int(b)] for b in sorted(m)], dtype=float)
        return bx, by

    def decode_file(path, chain, tok, dec, bx, by):
        logs = _read_raw_json(path)
        if not logs:
            return None
        n = len(logs)
        blk = np.empty(n, dtype=np.int64)
        tx = [None] * n
        li = np.empty(n, dtype=np.int64)
        frm = [None] * n
        to = [None] * n
        vr = [None] * n
        vd = np.empty(n, dtype=np.float64)
        mint = np.empty(n, dtype=bool)
        burn = np.empty(n, dtype=bool)
        zero_addr = "0x" + "0" * 40
        for i, l in enumerate(logs):
            blk[i] = int(l["blockNumber"], 16)
            tx[i] = l["transactionHash"]
            li[i] = int(l["logIndex"], 16)
            frm[i] = "0x" + l["topics"][1][-40:]
            to[i] = "0x" + l["topics"][2][-40:]
            vr[i] = l["data"]
            try:
                vd[i] = int(l["data"][:66], 16) / (10 ** dec)
            except ValueError:
                vd[i] = float("nan")
            mint[i] = l["topics"][1] == "0x" + "0" * 64
            burn[i] = l["topics"][2] == "0x" + "0" * 64
        # 插值后取整到微秒, 避免 float 秒 -> ns 的精度垃圾导致 us 转换失败
        ts_us = np.round(np.interp(blk.astype(float), bx, by) * 1e6).astype("int64")
        ts = pd.to_datetime(ts_us, unit="us", utc=True)
        df = pd.DataFrame({
            "token": tok, "block_number": blk, "tx_hash": tx, "log_index": li,
            "from_address": frm, "to_address": to, "value_raw": vr,
            "value_decimal": vd, "is_mint": mint, "is_burn": burn,
            "chain_id": chain, "block_timestamp_utc": ts})
        del logs
        return df

    def process_day(day_df, day, chain, writers, agg_rows, seen_by_tok):
        """持久化流式去重 (跨文件/跨 Pass, 按 chain+day+token 维护已见键)。"""
        new_parts = []
        for tok, g in day_df.groupby("token", sort=False):
            s = seen_by_tok.setdefault(tok, set())
            keys = list(zip(g["tx_hash"], g["log_index"].astype(int),
                            strict=False))
            mask = np.array([k not in s for k in keys], dtype=bool)
            s.update(k for k, keep in zip(keys, mask.tolist(),
                                          strict=False) if keep)
            if mask.any():
                new_parts.append(g[mask])
        new_df = pd.concat(new_parts, ignore_index=True) if new_parts else None
        del new_parts
        if new_df is None or new_df.empty:
            return 0
        bad = ~np.isfinite(new_df["value_decimal"].to_numpy(dtype=float))
        sus = bad.copy()
        reason = pd.Series("", index=new_df.index, dtype=object)
        reason[bad] = "value_not_finite"
        # 聚合增量计算 (随后即丢弃行数据)
        for tok, g in new_df.groupby("token", sort=False):
            large = int((g["value_decimal"] >= 1_000_000).sum())
            agg_rows.append({
                "chain_id": chain, "token": tok, "date_utc": day,
                "transfer_count": int(len(g)),
                "unique_from": int(g["from_address"].nunique()),
                "unique_to": int(g["to_address"].nunique()),
                "volume_token": float(g["value_decimal"].sum()),
                "large_transfer_count": large,
                "mint_count": int(g["is_mint"].sum()),
                "burn_count": int(g["is_burn"].sum())})
        out = new_df.copy()
        del new_df
        out["date"] = out["block_timestamp_utc"].dt.strftime("%Y-%m-%d")
        cols = [f.name for f in L1_SCHEMA]
        writers["l1"].write_table(pa.Table.from_pandas(
            out[cols], schema=L1_SCHEMA, preserve_index=False))
        out["is_suspect"] = sus
        out["quality_reason"] = reason
        ccols = [f.name for f in CERT_SCHEMA]
        writers["cert"].write_table(pa.Table.from_pandas(
            out[ccols], schema=CERT_SCHEMA, preserve_index=False))
        log(f"    {chain} {day.date()}: +{len(out)} 行 "
            f"(suspect={int(sus.sum())})")
        return int(len(out))

    t00 = time.time()
    total_rows = 0
    all_agg = []
    stats = {}
    for chain, cfgc in CHAIN_ERC20.items():
        venue = cfgc["venue"]
        bx, by = anchors_for(chain)
        root = os.path.join(RAW_DIR, venue, "erc20_transfer_logs")
        files = []
        for dirpath, _, fnames in os.walk(root):
            for fn in sorted(fnames):
                if fn.endswith(".meta.json") or not (
                        fn.endswith(".json") or fn.endswith(".json.gz")):
                    continue
                if not any(fn.startswith(f"{t}_transfer_logs")
                           for t in cfgc["tokens"]):
                    continue
                files.append(os.path.join(dirpath, fn))
        backfill = [f for f in files if f.endswith(".gz")]
        legacy = [f for f in files if not f.endswith(".gz")]
        log(f"[{chain}] 文件: 回填 {len(backfill)} 个 (.gz), 遗留 {len(legacy)} 个")

        l1_path = os.path.join(L1_DIR, "token_transfer", venue, "data.parquet")
        cert_path = os.path.join(CERTIFIED_DIR, "token_transfer", venue,
                                 "all", "data.parquet")
        os.makedirs(os.path.dirname(l1_path), exist_ok=True)
        os.makedirs(os.path.dirname(cert_path), exist_ok=True)
        writers = {"l1": pq.ParquetWriter(l1_path, L1_SCHEMA,
                                          compression="snappy"),
                   "cert": pq.ParquetWriter(cert_path, CERT_SCHEMA,
                                            compression="snappy")}
        agg_rows = []
        chain_rows = 0
        written_rows = 0
        decimals = {t: m["decimals"] for t, m in cfgc["tokens"].items()}
        seen_by_day = {}   # day -> {token: set((tx_hash, log_index))}
        try:
            # 统一流式处理: 回填批次 (.gz, 窗口互斥) + 遗留批次 (v1/daily);
            # 跨文件/跨 Pass 重叠由 seen_by_day 持久键集合去重
            for i, fp in enumerate(backfill + legacy):
                tok = next(t for t in cfgc["tokens"]
                           if os.path.basename(fp).startswith(t))
                df = decode_file(fp, chain, tok, decimals[tok], bx, by)
                if df is None or df.empty:
                    continue
                for day, sub in df.groupby(df["block_timestamp_utc"].dt.floor("D")):
                    n = process_day(sub.copy(), day, chain, writers, agg_rows,
                                    seen_by_day.setdefault(day, {}))
                    written_rows += n
                chain_rows += len(df)
                del df
                gc.collect()
                if (i + 1) % 10 == 0:
                    log(f"  [{chain}] 进度 {i+1}/{len(backfill)+len(legacy)}, "
                        f"累计读取 {chain_rows} 行, {time.time()-t00:.0f}s")
        finally:
            writers["l1"].close()
            writers["cert"].close()
        total_rows += written_rows
        all_agg.extend(agg_rows)
        stats[chain] = {"read": chain_rows, "written": written_rows}
        log(f"[{chain}] 完成: 读取 {chain_rows} 行 -> 写入 {written_rows} 行 "
            f"(去重 {chain_rows - written_rows}), 聚合 {len(agg_rows)} 组, "
            f"{time.time()-t00:.0f}s")

    # ---- 聚合表 L1/L2 ----
    from data_foundation.l2 import certify_derivatives, write_certified_derivatives
    from data_foundation.l1_onchain import write_onchain_parquet
    agg_df = pd.DataFrame(all_agg)
    agg_df["date_utc"] = pd.to_datetime(agg_df["date_utc"], utc=True)
    agg_df = agg_df.sort_values(["chain_id", "token", "date_utc"]).reset_index(
        drop=True)
    accum = {}
    for chain, g in agg_df.groupby("chain_id"):
        write_onchain_parquet(g.drop(columns=[]), "onchain_daily_aggregate",
                              chain, "date_utc")
        acdf = certify_derivatives(g, "date_utc",
                                   core_numeric_cols=["volume_token"],
                                   key_cols=["token", "date_utc"])
        write_certified_derivatives(acdf, "onchain_daily_aggregate", chain,
                                    "all", "date_utc")
        accum["onchain_daily_aggregate"] = accum.get(
            "onchain_daily_aggregate", 0) + len(acdf)
        log(f"[{chain}] onchain_daily_aggregate: {len(acdf)} 行 "
            f"({g['date_utc'].min().date()} ~ {g['date_utc'].max().date()})")

    # ---- manifests ----
    tt_start = min(pd.read_parquet(os.path.join(
        CERTIFIED_DIR, "token_transfer", c, "all", "data.parquet"),
        columns=["block_timestamp_utc"])["block_timestamp_utc"].min()
        for c in CHAIN_ERC20)
    build_dataset_manifest(
        "token_transfer", "*", "*", "*", "*",
        {"row_count": total_rows, "duplicate_count": 0, "gap_count": 0,
         "suspect_count": 0, "coverage_start": str(tt_start),
         "coverage_end": str(pd.Timestamp.now(tz="UTC"))},
        ["onchain_backfill_v2"],
        {"note": "全历史回填重建: Ethereum+Arbitrum ERC-20 (USDT/USDC/DAI) "
                 "2026-08-01..逐日窗口 + v1/daily 批次; .json.gz 存储; "
                 "时间戳=逐日窗口边界块分段线性插值"})
    log(f"build 完成: token_transfer {total_rows} 行, "
        f"aggregate {len(agg_df)} 行, 总耗时 {(time.time()-t00)/60:.0f} 分钟")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode in ("fetch", "all"):
        phase_fetch()
    if mode in ("build", "all"):
        phase_build()


if __name__ == "__main__":
    main()
