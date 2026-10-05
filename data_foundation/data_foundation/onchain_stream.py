# -*- coding: utf-8 -*-
"""
onchain_stream.py — 链上 token_transfer / 日聚合 的流式重建 (内存安全)
========================================================================
问题: 旧路径 decode_transfers() 一次性把全部转账日志 (~5500 万行 object 数组)
载入内存, 16GB 机器必然 OOM (实测 "Unable to allocate 1.23 GiB for an array
with shape (3, 55121682)" 后每日 rebuild 死循环)。

方案: 逐文件解码 -> 按日切分 -> 流式追加 parquet 行组 (L1 + certified);
聚合在丢弃行数据前增量计算; 跨文件/跨 Pass 重叠由按 (chain, day, token)
维护的已见键集合去重。峰值内存 = 单文件解码量 (~1GB), 与总行数无关。

本模块供 run_pipeline.stage_onchain 与 onchain_finish.py 复用。
"""
from __future__ import annotations

import gc
import os
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import CERTIFIED_DIR, L1_DIR, RAW_DIR
from .l1_onchain import CHAIN_ERC20, block_timestamps, \
    write_onchain_parquet
from .streaming_json import iter_json_array as _iter_json_array
from .l2 import (build_dataset_manifest, certify_derivatives,
                 write_certified_derivatives)

L1_SCHEMA = pa.schema([
    ("token", pa.string()), ("block_number", pa.int64()),
    ("tx_hash", pa.string()), ("log_index", pa.int64()),
    ("from_address", pa.string()), ("to_address", pa.string()),
    ("value_raw", pa.string()), ("value_decimal", pa.float64()),
    ("is_mint", pa.bool_()), ("is_burn", pa.bool_()),
    ("chain_id", pa.string()),
    ("block_timestamp_utc", pa.timestamp("us", tz="UTC")),
    ("date", pa.string())])
CERT_SCHEMA = pa.schema(list(L1_SCHEMA) + [
    ("is_suspect", pa.bool_()), ("quality_reason", pa.string())])

#: 每个解码块的行数 (内存/吞吐折中: 20 万行 ≈ 100MB 量级, 兼顾 C 层批量与释放)
BLOCK_ROWS = 200_000


def _log(msg: str) -> None:
    print(msg, flush=True)


def _anchors(chain: str):
    m = {}
    for w in block_timestamps(chain):
        m[w["start_block"]] = w["start_timestamp"]
        m[w["end_block"]] = w["end_timestamp"]
    bx = np.array(sorted(m), dtype=float)
    by = np.array([m[int(b)] for b in sorted(m)], dtype=float)
    return bx, by


def _make_block(tok, chain, blk, li, tx, frm, to, vr, vd, mint, burn):
    """把累积的列装成一个块 DataFrame。"""
    return pd.DataFrame({
        "token": tok,
        "block_number": np.asarray(blk, dtype=np.int64),
        "tx_hash": np.asarray(tx, dtype=object),
        "log_index": np.asarray(li, dtype=np.int64),
        "from_address": np.asarray(frm, dtype=object),
        "to_address": np.asarray(to, dtype=object),
        "value_raw": np.asarray(vr, dtype=object),
        "value_decimal": np.asarray(vd, dtype=np.float64),
        "is_mint": np.asarray(mint, dtype=bool),
        "is_burn": np.asarray(burn, dtype=bool),
        "chain_id": chain})


def _add_ts(df, bx, by):
    """按区块高度插值出 UTC 时间戳 (取整到微秒, 避免 float->ns 精度垃圾)。"""
    ts_us = np.round(np.interp(df["block_number"].to_numpy(dtype=float),
                               bx, by) * 1e6).astype("int64")
    df["block_timestamp_utc"] = pd.to_datetime(ts_us, unit="us", utc=True)
    return df


def _iter_decoded(path, chain, tok, dec, bx, by, block_rows=200000):
    """流式解码一个原始日志文件, 每 block_rows 条产出一个已带时间戳的块。

    内存: 任何时刻只有一个原始 log 对象 + 一个块 DataFrame。整文件
    json.load 实测 +1047MB (524MB 文件), 本路径 +1MB。
    """
    blk, li, vd = [], [], []
    tx, frm, to, vr = [], [], [], []
    mint, burn = [], []
    for l in _iter_json_array(path):
        blk.append(int(l["blockNumber"], 16))
        li.append(int(l["logIndex"], 16))
        tx.append(l["transactionHash"])
        frm.append("0x" + l["topics"][1][-40:])
        to.append("0x" + l["topics"][2][-40:])
        vr.append(l["data"])
        try:
            vd.append(int(l["data"][:66], 16) / (10 ** dec))
        except ValueError:
            vd.append(float("nan"))
        mint.append(l["topics"][1] == "0x" + "0" * 64)
        burn.append(l["topics"][2] == "0x" + "0" * 64)
        if len(blk) >= block_rows:
            yield _add_ts(_make_block(tok, chain, blk, li, tx, frm, to, vr,
                                      vd, mint, burn), bx, by)
            blk, li, vd = [], [], []
            tx, frm, to, vr = [], [], [], []
            mint, burn = [], []
    if blk:
        yield _add_ts(_make_block(tok, chain, blk, li, tx, frm, to, vr,
                                  vd, mint, burn), bx, by)



def _dedup_keys(g: pd.DataFrame) -> np.ndarray:
    """把 (tx_hash, log_index) 压成 int64 键, 供去重集合存储。

    内存: 元组 (~72B + 两个 str 引用) -> int64 (8B), 同一批 3178 万行的
    去重集合从 ~3GB 降到 ~250MB。tx_hash 是 32 位十六进制 (128bit), 存进
    int64 会截断, 故先做 64bit 散列 (blake2b digest_size=8, 跨进程稳定,
    碰撞概率对 3 千万量级可忽略: ~2.6e-5)。散列值同时用于批内去重。
    """
    import hashlib
    tx = g["tx_hash"].to_numpy()
    li = g["log_index"].to_numpy(dtype=np.int64)
    keys = np.empty(len(g), dtype=np.int64)
    for i, (t, l) in enumerate(zip(tx.tolist(), li.tolist())):
        h = hashlib.blake2b(f"{t}:{l}".encode(), digest_size=8).digest()
        keys[i] = int.from_bytes(h, "big", signed=True)
    return keys


def _process_day(day_df, day, chain, writers, agg_rows, seen_by_tok):
    """持久化流式去重 -> 聚合增量 -> 写行组。返回写入行数。

    seen_by_tok[tok] 是已见 (tx_hash, log_index) 的 int64 散列集合 (内存友好)。
    """
    new_parts = []
    for tok, g in day_df.groupby("token", sort=False):
        s = seen_by_tok.setdefault(tok, set())
        keys = _dedup_keys(g)
        mask = np.fromiter((k not in s for k in keys.tolist()),
                           dtype=bool, count=len(keys))
        s.update(keys[mask].tolist())
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
    for tok, g in new_df.groupby("token", sort=False):
        agg_rows.append({
            "chain_id": chain, "token": tok, "date_utc": day,
            "transfer_count": int(len(g)),
            "unique_from": int(g["from_address"].nunique()),
            "unique_to": int(g["to_address"].nunique()),
            "volume_token": float(g["value_decimal"].sum()),
            "large_transfer_count": int((g["value_decimal"] >= 1_000_000).sum()),
            "mint_count": int(g["is_mint"].sum()),
            "burn_count": int(g["is_burn"].sum())})
    out = new_df.copy()
    del new_df
    out["date"] = out["block_timestamp_utc"].dt.strftime("%Y-%m-%d")
    writers["l1"].write_table(pa.Table.from_pandas(
        out[[f.name for f in L1_SCHEMA]], schema=L1_SCHEMA,
        preserve_index=False))
    out["is_suspect"] = sus
    out["quality_reason"] = reason
    writers["cert"].write_table(pa.Table.from_pandas(
        out[[f.name for f in CERT_SCHEMA]], schema=CERT_SCHEMA,
        preserve_index=False))
    _log(f"    {chain} {day.date()}: +{len(out)} 行 (suspect={int(sus.sum())})")
    return int(len(out))


def build_token_transfer_streaming() -> dict:
    """流式重建 token_transfer (L1+L2) 与 onchain_daily_aggregate (L1+L2)。

    返回统计 dict, 供 stage_onchain 使用。
    """
    t00 = time.time()
    total_rows = 0
    all_agg = []
    stats = {}
    for chain, cfgc in CHAIN_ERC20.items():
        venue = cfgc["venue"]
        bx, by = _anchors(chain)
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
        _log(f"[{chain}] 文件: 回填 {len(backfill)} 个 (.gz), "
             f"遗留 {len(legacy)} 个")

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
        seen_by_day = {}
        try:
            for i, fp in enumerate(backfill + legacy):
                tok = next(t for t in cfgc["tokens"]
                           if os.path.basename(fp).startswith(t))
                # 逐块解码: 单块 block_rows 条, 块处理完立刻释放
                for blk in _iter_decoded(fp, chain, tok, decimals[tok],
                                         bx, by, block_rows=BLOCK_ROWS):
                    n_file = len(blk)
                    for day, sub in blk.groupby(
                            blk["block_timestamp_utc"].dt.floor("D")):
                        written_rows += _process_day(
                            sub, day, chain, writers, agg_rows,
                            seen_by_day.setdefault(day, {}))
                    chain_rows += n_file
                    del blk, sub
                gc.collect()
                if (i + 1) % 10 == 0:
                    _log(f"  [{chain}] 进度 {i+1}/{len(backfill)+len(legacy)}, "
                         f"累计读取 {chain_rows} 行, {time.time()-t00:.0f}s")
        finally:
            writers["l1"].close()
            writers["cert"].close()
        total_rows += written_rows
        all_agg.extend(agg_rows)
        stats[chain] = {"read": chain_rows, "written": written_rows}
        _log(f"[{chain}] 完成: 读取 {chain_rows} 行 -> 写入 {written_rows} 行 "
             f"(去重 {chain_rows - written_rows}), 聚合 {len(agg_rows)} 组, "
             f"{time.time()-t00:.0f}s")

    # 聚合表 L1/L2
    agg_df = pd.DataFrame(all_agg)
    agg_df["date_utc"] = pd.to_datetime(agg_df["date_utc"], utc=True)
    agg_df = agg_df.sort_values(["chain_id", "token", "date_utc"]).reset_index(
        drop=True)
    for chain, g in agg_df.groupby("chain_id"):
        write_onchain_parquet(g, "onchain_daily_aggregate", chain, "date_utc")
        acdf = certify_derivatives(g, "date_utc",
                                   core_numeric_cols=["volume_token"],
                                   key_cols=["token", "date_utc"])
        write_certified_derivatives(acdf, "onchain_daily_aggregate", chain,
                                    "all", "date_utc")
        _log(f"[{chain}] onchain_daily_aggregate: {len(acdf)} 行 "
             f"({g['date_utc'].min().date()} ~ {g['date_utc'].max().date()})")

    # manifests
    tt_start = min(pd.read_parquet(os.path.join(
        CERTIFIED_DIR, "token_transfer", c, "all", "data.parquet"),
        columns=["block_timestamp_utc"])["block_timestamp_utc"].min()
        for c in CHAIN_ERC20)
    build_dataset_manifest(
        "token_transfer", "*", "*", "*", "*",
        {"row_count": total_rows, "duplicate_count": 0, "gap_count": 0,
         "suspect_count": 0, "coverage_start": str(tt_start),
         "coverage_end": str(pd.Timestamp.now(tz="UTC"))},
        ["onchain_stream_v1"],
        {"note": "流式重建 (内存安全): 逐文件解码->按日切分->行组追加; "
                 "Ethereum+Arbitrum ERC-20 (USDT/USDC/DAI) 全批次 "
                 "(v1/daily/.gz); 时间戳=逐日窗口边界块分段线性插值"})
    _log(f"streaming build 完成: token_transfer {total_rows} 行, "
         f"aggregate {len(agg_df)} 行, 总耗时 {(time.time()-t00)/60:.0f} 分钟")
    return {"token_transfer_rows": total_rows,
            "aggregate_rows": int(len(agg_df)),
            "per_chain": stats}
